"""Bridge Telegram <-> omp: long polling, comandos, aprovações e stream."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import Any

from telegram import (
    Bot,
    BotCommand,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.error import (
    Conflict,
    InvalidToken,
    NetworkError,
    RetryAfter,
    TelegramError,
    TimedOut,
)

from . import session_index
from .config import Config
from .omp_client import (
    OmpCommandError,
    OmpError,
    OmpRpcClient,
    OmpSupervisorExhausted,
    supervisor,
)
from .streamer import TurnStreamer

log = logging.getLogger(__name__)

HELP_TEXT = """Comandos:
/status — modelo, sessão, contexto, modo
/ask [pergunta] — modo só leitura e pesquisa (não escreve nem executa)
/work — volta ao modo normal
/stop — aborta o turno atual
/new — nova sessão
/sessions — lista e troca de sessões
/name <texto> — nomeia a sessão
/model [busca] — escolhe o modelo
/thinking [nível] — escolhe o nível de raciocínio
/compact — compacta o contexto
/id — seu id de usuário e o id do chat
/help — esta ajuda

Texto simples vira prompt. Foto vira imagem para o modelo.
Documento .txt/.md (até 64 KB) vira texto."""

IMAGE_MIME = {
    ".png": "image/png",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}
MAX_DOC_BYTES = 65536
PENDING_UI_LIMIT = 50
# Ferramentas do modo /ask: leitura e pesquisa, nenhuma escrita/execução.
# `--tools=` é enforçado pelo próprio omp (verificado: write/edit/bash indisponíveis).
ASK_TOOLS = "read,glob,grep,web_search,lsp"


class Bridge:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.chat_id = config.allowed_user_id
        self.bot = Bot(token=config.token)
        self.client = OmpRpcClient(
            omp_bin=config.omp_bin,
            cwd=config.omp_cwd,
            sessions_dir=config.sessions_dir,
            approval_mode=config.approval_mode,
            model=config.model,
            stderr_path=config.omp_stderr_file,
        )

        # Modo /ask: um segundo filho do omp, com ferramentas só de leitura e
        # sessões separadas, para o agente não poder tocar no projeto.
        self.ask_client: OmpRpcClient | None = None
        self.ask_mode = False
        self._ask_task: asyncio.Task[None] | None = None

        self.streamer: TurnStreamer | None = None
        self.turn_prompt_id: str | None = None
        self.pending_ui: dict[str, dict[str, Any]] = {}
        self.pending_text: dict[int, str] = {}
        self.model_choices: list[dict[str, Any]] = []
        self.session_choices: list[dict[str, Any]] = []
        self.warned_chats: set[int] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._closing = False
        self._fatal = False
        self._poll_task: asyncio.Task[None] | None = None

    # -------------------------------------------------------------- modos

    def _active_client(self) -> OmpRpcClient:
        if self.ask_mode and self.ask_client is not None:
            return self.ask_client
        return self.client

    @property
    def ask_sessions_dir(self) -> Path:
        return self.config.data_dir / "ask-sessions"

    async def _ensure_ask_client(self) -> OmpRpcClient:
        """Sobe o filho somente-leitura na primeira vez que /ask é usado."""
        if self.ask_client is not None and self.ask_client.running:
            return self.ask_client
        client = OmpRpcClient(
            omp_bin=self.config.omp_bin,
            cwd=self.config.omp_cwd,
            sessions_dir=self.ask_sessions_dir,
            approval_mode=self.config.approval_mode,
            model=self.config.model,
            stderr_path=self.config.data_dir / "omp-ask-stderr.log",
            extra_args=[f"--tools={ASK_TOOLS}"],
        )
        await client.start()
        self.ask_client = client
        self._ask_task = asyncio.create_task(
            self._event_loop(client), name="events-ask"
        )
        log.info("modo ask: omp somente-leitura iniciado (%s)", ASK_TOOLS)
        return client

    async def _set_ask_mode(self, enabled: bool) -> None:
        if enabled == self.ask_mode:
            return
        if enabled:
            await self._ensure_ask_client()
        await self._finish_streamer()
        self.ask_mode = enabled
        self.turn_prompt_id = None

    # ------------------------------------------------------------ ciclo de vida

    async def run(self) -> None:
        try:
            async with self.bot:
                await self._serve()
        except InvalidToken as exc:
            raise SystemExit(
                "TELEGRAM_BOT_TOKEN inválido: o Telegram recusou o token do .env"
            ) from exc
        except TelegramError as exc:
            raise SystemExit(f"não consegui falar com a API do Telegram: {exc}") from exc

    async def _serve(self) -> None:
        log.info("bot conectado: @%s", self.bot.username)

        with contextlib.suppress(Exception):
            await self.bot.delete_webhook(drop_pending_updates=False)
        with contextlib.suppress(Exception):
            await self.bot.set_my_commands(
                [
                    BotCommand("status", "modelo, sessão, contexto, modo"),
                    BotCommand("ask", "modo só leitura e pesquisa"),
                    BotCommand("work", "volta ao modo normal"),
                    BotCommand("stop", "aborta o turno atual"),
                    BotCommand("new", "nova sessão"),
                    BotCommand("sessions", "lista e troca de sessões"),
                    BotCommand("name", "nomeia a sessão"),
                    BotCommand("model", "escolhe o modelo"),
                    BotCommand("thinking", "nível de raciocínio"),
                    BotCommand("compact", "compacta o contexto"),
                    BotCommand("id", "ids de usuário e chat"),
                    BotCommand("help", "ajuda"),
                ]
            )
        if self.config.allowed_user_id == 0:
            log.warning(
                "modo setup: TELEGRAM_ALLOWED_USER_ID=0 — o omp não será iniciado; "
                "envie /id no Telegram e preencha o .env"
            )
            try:
                await self._poll_loop()
            finally:
                self._closing = True
            return

        await self.client.start()
        self._tasks = [
            asyncio.create_task(self._event_loop(), name="events"),
            asyncio.create_task(self._tick_loop(), name="tick"),
            asyncio.create_task(self._supervise(), name="supervisor"),
        ]
        self._poll_task = asyncio.current_task()
        try:
            await self._poll_loop()
        finally:
            self._closing = True
            for task in self._tasks:
                task.cancel()
            if self._ask_task is not None:
                self._ask_task.cancel()
            for task in self._tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            if self._ask_task is not None:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._ask_task
            if self.ask_client is not None:
                await self.ask_client.close()
            await self.client.close()
        if self._fatal:
            raise SystemExit(1)

    async def _supervise(self) -> None:
        try:
            await supervisor(self.client, self._on_restart)
        except OmpSupervisorExhausted as exc:
            self._fatal = True
            log.critical("supervisor desistiu: %s", exc)
            with contextlib.suppress(Exception):
                await self.bot.send_message(
                    self.chat_id,
                    "⛔ o processo do omp não reiniciou; o bridge vai encerrar. "
                    "Rode o bridge de novo.",
                )
            poll_task = self._poll_task
            if poll_task is not None and not poll_task.done():
                poll_task.cancel()

    async def _on_restart(self) -> None:
        self.streamer = None
        self.turn_prompt_id = None
        self.pending_ui.clear()
        self.pending_text.clear()
        log.warning("omp reiniciado pelo supervisor")
        with contextlib.suppress(Exception):
            await self.bot.send_message(
                self.chat_id, "♻️ o processo do omp caiu e foi reiniciado."
            )
        await self._refresh_index()

    async def _event_loop(self, client: OmpRpcClient | None = None) -> None:
        source = client or self.client
        while True:
            frame = await source.events.get()
            # Frames do cliente que não está no comando agora são descartados:
            # só um modo por vez desenha no chat.
            if source is not self._active_client():
                continue
            try:
                await self._route_frame(frame)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("erro tratando frame %s", frame.get("type"))

    async def _route_frame(self, frame: dict[str, Any]) -> None:
        ftype = frame.get("type")

        if ftype == "extension_ui_request":
            await self._handle_ui_request(frame)
            return

        # Trabalho em background (bash auto-backgrounded, task, eval) pode acordar o
        # agente depois do `prompt_result`: um `agent_start` sem streamer vivo abre
        # uma mensagem nova em vez de descartar a resposta.
        if ftype == "agent_start" and (self.streamer is None or self.streamer.closed):
            self.streamer = TurnStreamer(self.bot, self.chat_id, self.config)
            self.turn_prompt_id = None
            log.info("novo streamer para execução iniciada em background")

        if self.streamer is not None and not self.streamer.closed:
            await self.streamer.consume(frame)

        if ftype == "prompt_result":
            await self._handle_prompt_result(frame)
        elif ftype == "session_settled":
            await self._on_settled()

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            streamer = self.streamer
            if streamer is not None and not streamer.closed:
                with contextlib.suppress(Exception):
                    await streamer.tick()

    async def _finish_streamer(self, state: dict[str, Any] | None = None) -> None:
        """Fecha o turno em curso: rodapé + documento, e libera o streamer."""
        streamer = self.streamer
        self.streamer = None
        self.turn_prompt_id = None
        if streamer is None or streamer.closed:
            return
        if state is None:
            try:
                state = await self._state()
            except OmpError:
                state = {}
        with contextlib.suppress(Exception):
            await streamer.finish(state)

    async def _handle_prompt_result(self, frame: dict[str, Any]) -> None:
        status = frame.get("status")
        error = frame.get("error") or {}
        message = error.get("message") if isinstance(error, dict) else str(error)

        is_turn = frame.get("id") == self.turn_prompt_id
        has_streamer = self.streamer is not None and not self.streamer.closed

        if status == "error" and (not is_turn or not has_streamer):
            await self._send(self.chat_id, f"❌ {message or 'erro desconhecido'}")
        elif status == "aborted" and (not is_turn or not has_streamer):
            await self._send(self.chat_id, "🛑 interrompido")

        if is_turn:
            await self._finish_streamer(await self._state())

    async def _on_settled(self) -> None:
        # Quiescência: qualquer streamer ainda aberto pertence a um turno que
        # terminou sem `prompt_result` casado (ex.: steer, troca de sessão).
        await self._finish_streamer()
        await self._refresh_index()

    async def _refresh_index(self) -> None:
        if self.ask_mode:
            # Sessões do modo ask são separadas e não entram em /sessions.
            return
        try:
            state = await self._state()
        except OmpError:
            return
        session_file = state.get("sessionFile")
        if not session_file:
            return
        name = state.get("sessionName")
        with contextlib.suppress(Exception):
            session_index.touch(
                self.config.index_file,
                session_path=session_file,
                label=name or None,
                messages=state.get("messageCount"),
            )

    # ---------------------------------------------------------------- polling

    async def _poll_loop(self) -> None:
        offset = self._read_offset()
        backoff = 1.0
        while not self._closing:
            try:
                updates = await self.bot.get_updates(
                    offset=offset,
                    timeout=25,
                    allowed_updates=["message", "callback_query"],
                )
                backoff = 1.0
            except RetryAfter as exc:
                await asyncio.sleep(_retry_seconds(exc) + 0.5)
                continue
            except Conflict as exc:
                log.critical("409 Conflict: outra instância está fazendo polling (%s)", exc)
                raise SystemExit(
                    "outra instância do bridge está usando este token "
                    "(409 Conflict) — mate-a antes de iniciar outra"
                ) from exc
            except (TimedOut, NetworkError) as exc:
                log.warning("falha de rede no getUpdates (%s); retry em %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue

            for update in updates:
                try:
                    await self._handle_update(update)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("erro tratando update %s", update.update_id)
                offset = update.update_id + 1
                self._write_offset(offset)

    def _read_offset(self) -> int | None:
        try:
            data = json.loads(self.config.offset_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        value = data.get("offset")
        return value if isinstance(value, int) else None

    def _write_offset(self, offset: int) -> None:
        path = self.config.offset_file
        tmp = path.with_name(path.name + ".tmp")
        with contextlib.suppress(OSError):
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps({"offset": offset}), encoding="utf-8")
            os.replace(tmp, path)

    # ---------------------------------------------------------------- updates

    async def _handle_update(self, update: Update) -> None:
        user = update.effective_user
        user_id = user.id if user is not None else None
        chat = update.effective_chat
        chat_id = chat.id if chat is not None else None

        if self.config.allowed_user_id == 0:
            # Modo setup: sem usuário autorizado, só /id é atendido.
            await self._handle_setup_update(update, user_id, chat_id)
            return

        if user_id != self.config.allowed_user_id:
            log.warning("update ignorado de user_id=%s", user_id)
            if chat_id is not None and chat_id not in self.warned_chats:
                self.warned_chats.add(chat_id)
                with contextlib.suppress(Exception):
                    await self.bot.send_message(chat_id, "não autorizado")
            return

        if update.callback_query is not None:
            await self._handle_callback(update)
            return
        if update.message is not None:
            await self._handle_message(update)

    async def _handle_setup_update(
        self, update: Update, user_id: int | None, chat_id: int | None
    ) -> None:
        if chat_id is None:
            return
        text = ""
        if update.message is not None:
            text = (update.message.text or "").strip()
        command = text[1:].split("@", 1)[0].lower() if text.startswith("/") else ""
        if command == "id":
            await self._send(
                chat_id,
                f"user_id: {user_id}\nchat_id: {chat_id}\n\n"
                "Coloque o user_id em TELEGRAM_ALLOWED_USER_ID no .env e reinicie.",
            )
            return
        if chat_id not in self.warned_chats:
            self.warned_chats.add(chat_id)
            await self._send(
                chat_id,
                "configuração pendente: envie /id e coloque o user_id em "
                "TELEGRAM_ALLOWED_USER_ID no .env (nenhum outro comando é atendido).",
            )

    async def _handle_message(self, update: Update) -> None:
        message = update.message
        assert message is not None
        chat_id = message.chat_id
        text = (message.text or "").strip()

        if chat_id in self.pending_text and text and not text.startswith("/"):
            ui_id = self.pending_text.pop(chat_id)
            await self._active_client().send_raw(
                {"type": "extension_ui_response", "id": ui_id, "value": text}
            )
            await self._send(chat_id, "✅ resposta enviada")
            return

        if text.startswith("/"):
            await self._handle_command(chat_id, text)
            return

        if message.photo:
            await self._handle_photo(message)
            return
        if message.document is not None:
            await self._handle_document(message)
            return
        if text:
            await self._prompt(chat_id, text)

    # --------------------------------------------------------------- comandos

    async def _handle_command(self, chat_id: int, raw: str) -> None:
        head, _, rest = raw.partition(" ")
        command = head[1:].split("@", 1)[0].lower()
        arg = rest.strip()

        try:
            if command in ("start", "help"):
                await self._send(chat_id, HELP_TEXT)
            elif command == "id":
                await self._send(chat_id, f"chat_id: {chat_id}\nuser_id: {self.config.allowed_user_id}")
            elif command == "status":
                await self._cmd_status(chat_id)
            elif command == "ask":
                await self._cmd_ask(chat_id, arg)
            elif command in ("work", "code"):
                await self._cmd_work(chat_id)
            elif command == "stop":
                await self._active_client().request({"type": "abort"})
                await self._send(chat_id, "🛑 abortado")
            elif command == "new":
                await self._cmd_new(chat_id)
            elif command == "compact":
                await self._active_client().request({"type": "compact"}, timeout=600)
                await self._send(chat_id, "🗜️ compactação iniciada")
            elif command == "name":
                await self._cmd_name(chat_id, arg)
            elif command == "model":
                await self._cmd_model(chat_id, arg)
            elif command == "thinking":
                await self._cmd_thinking(chat_id, arg)
            elif command == "sessions":
                await self._cmd_sessions(chat_id)
            elif command == "cancel":
                ui_id = self.pending_text.pop(chat_id, None)
                if ui_id is None:
                    await self._send(chat_id, "nada pendente")
                else:
                    await self._active_client().send_raw(
                        {"type": "extension_ui_response", "id": ui_id, "cancelled": True}
                    )
                    await self._send(chat_id, "⌛ cancelado")
            else:
                await self._send(chat_id, f"comando desconhecido: /{command}\n\n{HELP_TEXT}")
        except OmpCommandError as exc:
            await self._send(chat_id, f"❌ {exc}")
        except OmpError as exc:
            await self._send(chat_id, f"❌ {exc}")

    async def _cmd_ask(self, chat_id: int, arg: str) -> None:
        """Liga o modo somente-leitura; com argumento, já pergunta nele."""
        try:
            await self._set_ask_mode(True)
        except Exception as exc:
            await self._send(chat_id, f"❌ não consegui subir o modo ask: {exc}")
            return
        if arg:
            await self._prompt(chat_id, arg)
            return
        await self._send(
            chat_id,
            "🔍 modo ASK ligado: só leitura e pesquisa — o agente não escreve, "
            "não edita e não executa nada.\n\n"
            f"ferramentas: {ASK_TOOLS}\n"
            f"sessões próprias em: {self.ask_sessions_dir}\n"
            "/work volta ao modo normal.",
        )

    async def _cmd_work(self, chat_id: int) -> None:
        if not self.ask_mode:
            await self._send(chat_id, "já está no modo normal")
            return
        await self._set_ask_mode(False)
        await self._send(chat_id, "🛠️ modo normal: o agente pode editar e executar")

    async def _cmd_status(self, chat_id: int) -> None:
        state = await self._state()
        model = state.get("model") or {}
        session_file = state.get("sessionFile") or ""
        usage = state.get("contextUsage") or {}
        percent = usage.get("percent")
        if state.get("isStreaming"):
            status = "rodando"
        elif not state.get("isSettled", True):
            status = "aguardando"
        else:
            status = "ocioso"
        lines = [
            f"modelo: {model.get('provider', '?')}/{model.get('id', '?')}",
            f"thinking: {state.get('thinkingLevel', '?')}",
            f"sessão: {Path(session_file).name if session_file else 'sem persistência'}",
            f"mensagens: {state.get('messageCount', 0)} · na fila: {state.get('queuedMessageCount', 0)}",
        ]
        if isinstance(percent, (int, float)):
            lines.append(f"contexto: {round(percent)}%")
        lines.append(f"estado: {status}")
        lines.append(f"modo: {'🔍 ask (só leitura)' if self.ask_mode else '🛠️ normal'}")
        await self._send(chat_id, "\n".join(lines))

    async def _cmd_new(self, chat_id: int) -> None:
        frame = await self._active_client().request({"type": "new_session"}, timeout=120)
        data = frame.get("data") or {}
        if isinstance(data, dict) and data.get("cancelled"):
            await self._send(chat_id, "cancelado por extensão")
            return
        self.streamer = None
        self.turn_prompt_id = None
        state = await self._state()
        await self._refresh_index()
        await self._send(chat_id, f"🆕 nova sessão: {state.get('sessionId', '?')}")

    async def _cmd_name(self, chat_id: int, arg: str) -> None:
        if not arg:
            state = await self._state()
            await self._send(chat_id, f"nome atual: {state.get('sessionName') or '(sem nome)'}")
            return
        await self._active_client().request({"type": "set_session_name", "name": arg})
        state = await self._state()
        session_file = state.get("sessionFile")
        if session_file:
            session_index.touch(
                self.config.index_file, session_path=session_file, label=arg
            )
        await self._send(chat_id, f"🏷️ sessão nomeada: {arg}")

    async def _cmd_model(self, chat_id: int, query: str) -> None:
        frame = await self._active_client().request({"type": "get_available_models"}, timeout=90)
        models = (frame.get("data") or {}).get("models") or []
        state = await self._state()
        model = state.get("model") or {}
        current = f"{model.get('provider', '?')}/{model.get('id', '?')}"

        needle = query.lower()
        filtered = [
            m
            for m in models
            if not needle or needle in f"{m.get('provider')}/{m.get('id')}".lower()
        ][:30]
        self.model_choices = filtered

        if not filtered:
            await self._send(chat_id, f"modelo atual: {current}\nnenhum modelo para “{query}”")
            return
        keyboard = [
            [
                InlineKeyboardButton(
                    text=f"{m.get('provider')}/{m.get('id')}",
                    callback_data=f"mdl:{i}",
                )
            ]
            for i, m in enumerate(filtered)
        ]
        await self._send(
            chat_id,
            f"modelo atual: {current}\nescolha um ({len(filtered)} de {len(models)}):",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    async def _cmd_thinking(self, chat_id: int, arg: str) -> None:
        frame = await self._active_client().request({"type": "get_available_thinking_levels"}, timeout=60)
        levels = (frame.get("data") or {}).get("levels") or []
        state = await self._state()
        current = state.get("thinkingLevel", "?")
        if not arg:
            await self._send(chat_id, f"thinking atual: {current}\ndisponíveis: {', '.join(levels)}")
            return
        if arg not in levels:
            await self._send(chat_id, f"nível inválido: {arg}\ndisponíveis: {', '.join(levels)}")
            return
        await self._active_client().request({"type": "set_thinking_level", "level": arg})
        await self._send(chat_id, f"🧠 thinking: {arg}")

    async def _cmd_sessions(self, chat_id: int) -> None:
        state = await self._state()
        active = state.get("sessionFile")
        entries = session_index.recent(self.config.index_file, limit=20)
        if active and not any(e.get("path") == active for e in entries):
            entries.insert(
                0,
                {
                    "path": active,
                    "label": state.get("sessionName") or Path(active).stem,
                    "updatedAt": time.time(),
                    "messages": state.get("messageCount", 0),
                },
            )
        self.session_choices = entries
        if not entries:
            await self._send(chat_id, "nenhuma sessão registrada")
            return
        keyboard = []
        lines = ["sessões (✅ = ativa):"]
        for i, entry in enumerate(entries):
            path = entry.get("path", "")
            label = entry.get("label") or Path(path).stem
            mark = "✅ " if path == active else ""
            ago = _humanize_ago(entry.get("updatedAt"))
            lines.append(f"{mark}{label} · {entry.get('messages', 0)} msgs · {ago}")
            keyboard.append(
                [InlineKeyboardButton(text=f"{mark}{label}", callback_data=f"ses:{i}")]
            )
        await self._send(
            chat_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ------------------------------------------------------------- callbacks

    async def _handle_callback(self, update: Update) -> None:
        query = update.callback_query
        assert query is not None
        with contextlib.suppress(Exception):
            await self.bot.answer_callback_query(query.id)

        data = query.data or ""
        chat_id = query.message.chat_id if query.message is not None else self.chat_id

        if data.startswith("ui:"):
            await self._resolve_ui(chat_id, query, data)
        elif data.startswith("mdl:"):
            await self._pick_model(chat_id, query, data)
        elif data.startswith("ses:"):
            await self._pick_session(chat_id, query, data)
        else:
            with contextlib.suppress(Exception):
                await self.bot.answer_callback_query(query.id, text="expirado")

    async def _resolve_ui(self, chat_id: int, query: Any, data: str) -> None:
        _, _, tail = data.partition(":")
        token, _, index_raw = tail.partition(":")
        entry = self.pending_ui.pop(token, None)
        if entry is None:
            with contextlib.suppress(Exception):
                await self.bot.answer_callback_query(query.id, text="expirado")
            return
        try:
            index = int(index_raw)
        except ValueError:
            return

        options = entry.get("options") or []
        if index < 0 or index >= len(options):
            return
        label = options[index]
        if entry.get("confirm"):
            response = {
                "type": "extension_ui_response",
                "id": entry["id"],
                "confirmed": index == 0,
            }
        else:
            response = {"type": "extension_ui_response", "id": entry["id"], "value": label}
        await self._active_client().send_raw(response)
        # A linha de atividade já registra a ferramenta; deixar "✅ Approve" no
        # chat só acumula ruído a cada aprovação.
        await self._dismiss_ui_message(chat_id, query.message.message_id, index == 0, label)

    async def _dismiss_ui_message(
        self, chat_id: int, message_id: int, approved: bool, label: str
    ) -> None:
        try:
            await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            # Apagar pode falhar (mensagem antiga); degrada para um marcador curto.
            with contextlib.suppress(Exception):
                await self.bot.edit_message_text(
                    f"{'✅' if approved else '🚫'} {label}",
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=None,
                )

    async def _pick_model(self, chat_id: int, query: Any, data: str) -> None:
        index = _index_of(data)
        if index is None or index >= len(self.model_choices):
            with contextlib.suppress(Exception):
                await self.bot.answer_callback_query(query.id, text="expirado")
            return
        model = self.model_choices[index]
        provider = model.get("provider")
        model_id = model.get("id")
        await self._active_client().request(
            {"type": "set_model", "provider": provider, "modelId": model_id}, timeout=90
        )
        label = f"modelo: {provider}/{model_id}"
        with contextlib.suppress(Exception):
            await self.bot.edit_message_text(
                label, chat_id=chat_id, message_id=query.message.message_id, reply_markup=None
            )

    async def _pick_session(self, chat_id: int, query: Any, data: str) -> None:
        index = _index_of(data)
        if index is None or index >= len(self.session_choices):
            with contextlib.suppress(Exception):
                await self.bot.answer_callback_query(query.id, text="expirado")
            return
        entry = self.session_choices[index]
        path = entry.get("path")
        try:
            await self._active_client().request(
                {"type": "switch_session", "sessionPath": path}, timeout=120
            )
        except OmpError as exc:
            await self._send(chat_id, f"❌ {exc}")
            return
        self.streamer = None
        self.turn_prompt_id = None
        state = await self._state()
        await self._refresh_index()
        await self._send(
            chat_id,
            f"↩️ sessão trocada: {entry.get('label')} · "
            f"{state.get('messageCount', 0)} mensagens",
        )

    # -------------------------------------------------------------------- UI

    async def _handle_ui_request(self, frame: dict[str, Any]) -> None:
        method = frame.get("method")

        if method == "select":
            options = [str(o) for o in (frame.get("options") or [])]
            if not options:
                return
            await self._ask_buttons(frame, options, confirm=False)
        elif method == "confirm":
            await self._ask_buttons(frame, ["Sim", "Não"], confirm=True)
        elif method in ("input", "editor"):
            await self._ask_text(frame)
        elif method == "cancel":
            await self._cancel_ui(frame.get("targetId"))
        else:
            log.debug("extension_ui_request ignorado: method=%s", method)

    async def _ask_buttons(
        self, frame: dict[str, Any], options: list[str], *, confirm: bool
    ) -> None:
        token = secrets.token_hex(4)
        self.pending_ui[token] = {
            "id": frame.get("id"),
            "options": options,
            "confirm": confirm,
        }
        while len(self.pending_ui) > PENDING_UI_LIMIT:
            oldest = next(iter(self.pending_ui))
            self.pending_ui.pop(oldest, None)

        keyboard = [
            [InlineKeyboardButton(text=o[:60], callback_data=f"ui:{token}:{i}")]
            for i, o in enumerate(options)
        ]
        title = frame.get("title") or "Confirmação"
        await self._send(
            self.chat_id, f"❓ {title}", reply_markup=InlineKeyboardMarkup(keyboard)
        )

    async def _ask_text(self, frame: dict[str, Any]) -> None:
        self.pending_text[self.chat_id] = frame.get("id")
        title = frame.get("title") or "Entrada"
        placeholder = frame.get("placeholder")
        text = f"✏️ {title}"
        if placeholder:
            text += f"\n(ex.: {placeholder})"
        text += "\n\n/envie o texto ou /cancel"
        await self._send(
            self.chat_id, text, reply_markup=ForceReply(selective=True)
        )

    async def _cancel_ui(self, target_id: Any) -> None:
        for token, entry in list(self.pending_ui.items()):
            if entry.get("id") == target_id:
                self.pending_ui.pop(token, None)
        for chat_id, ui_id in list(self.pending_text.items()):
            if ui_id == target_id:
                self.pending_text.pop(chat_id, None)

    # ---------------------------------------------------------------- mídia

    async def _handle_photo(self, message: Any) -> None:
        photo = message.photo[-1]
        try:
            file = await self.bot.get_file(photo.file_id)
            raw = bytes(await file.download_as_bytearray())
        except Exception as exc:
            await self._send(message.chat_id, f"❌ não consegui baixar a foto: {exc}")
            return
        mime = IMAGE_MIME.get(Path(file.file_path or "").suffix.lower(), "image/jpeg")
        payload = {
            "type": "image",
            "mimeType": mime,
            "data": base64.b64encode(raw).decode("ascii"),
        }
        text = (message.caption or "").strip() or "Analise esta imagem."
        await self._prompt(message.chat_id, text, images=[payload])

    async def _handle_document(self, message: Any) -> None:
        document = message.document
        mime = document.mime_type or ""
        size = document.file_size or 0
        if not mime.startswith("text/") or size > MAX_DOC_BYTES:
            await self._send(
                message.chat_id, "não suportado (envie texto ou imagem)"
            )
            return
        try:
            file = await self.bot.get_file(document.file_id)
            raw = bytes(await file.download_as_bytearray())
        except Exception as exc:
            await self._send(message.chat_id, f"❌ não consegui baixar o arquivo: {exc}")
            return
        content = raw.decode("utf-8", errors="replace")
        caption = (message.caption or "").strip()
        text = f"[arquivo {document.file_name}]\n{content}"
        if caption:
            text = f"{caption}\n\n{text}"
        await self._prompt(message.chat_id, text)

    # ---------------------------------------------------------------- prompts

    async def _prompt(
        self, chat_id: int, text: str, *, images: list[dict[str, Any]] | None = None
    ) -> None:
        state = await self._state()
        streaming = bool(state.get("isStreaming"))
        log.info(
            "prompt: streaming=%s streamer=%s imagens=%d",
            streaming,
            "aberto" if (self.streamer and not self.streamer.closed) else "nenhum",
            len(images or []),
        )

        if streaming:
            try:
                prompt_id = await self._active_client().prompt(
                    text, images=images, streaming_behavior="steer"
                )
            except OmpError as exc:
                # O turno pode ter terminado entre o get_state e o envio.
                log.info("steer recusado (%s); enviando como prompt normal", exc)
            else:
                self._active_client().forget_prompt(prompt_id)
                await self._send(chat_id, "↩︎ steer enviado")
                return

        # Turno anterior ainda aberto sem `prompt_result` casado: fecha antes de abrir outro.
        if self.streamer is not None and not self.streamer.closed:
            await self._finish_streamer(state)

        self.streamer = TurnStreamer(self.bot, chat_id, self.config)
        prompt_id = await self._active_client().prompt(text, images=images)
        self.turn_prompt_id = prompt_id

    # ----------------------------------------------------------------- utils

    async def _state(self) -> dict[str, Any]:
        frame = await self._active_client().request({"type": "get_state"})
        data = frame.get("data")
        return data if isinstance(data, dict) else {}

    async def _send(self, chat_id: int, text: str, **kwargs: Any) -> None:
        try:
            await self.bot.send_message(chat_id, text, **kwargs)
        except RetryAfter as exc:
            await asyncio.sleep(_retry_seconds(exc) + 0.5)
            with contextlib.suppress(Exception):
                await self.bot.send_message(chat_id, text, **kwargs)


def _index_of(data: str) -> int | None:
    try:
        return int(data.split(":", 1)[1])
    except (IndexError, ValueError):
        return None


def _retry_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    return value.total_seconds() if hasattr(value, "total_seconds") else float(value)


def _humanize_ago(timestamp: Any) -> str:
    if not isinstance(timestamp, (int, float)):
        return "?"
    delta = max(0, time.time() - timestamp)
    if delta < 60:
        return "agora"
    if delta < 3600:
        return f"há {int(delta // 60)} min"
    if delta < 86400:
        return f"há {int(delta // 3600)} h"
    return f"há {int(delta // 86400)} d"


__all__ = ["Bridge"]
