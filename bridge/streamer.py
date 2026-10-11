"""Renderizador do stream do omp para mensagens do Telegram."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter
from datetime import timedelta
from typing import Any

from telegram import Bot, InputFile
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, RetryAfter

log = logging.getLogger(__name__)

ACTIVITY_HEADER = "⚙️ atividade\n"
ACTIVITY_MAX_LINES = 15
ACTIVITY_THROTTLE = 2.0
# O plano muda a cada passo; um intervalo curto mantém o acompanhamento vivo.
PLAN_THROTTLE = 2.0
TYPING_INTERVAL = 4.0
ARG_SUMMARY_LIMIT = 120
# Telegram: ~1 mensagem por segundo por chat; edições contam no mesmo limite.
MIN_SEND_INTERVAL = 1.0
# Bloco de código maior que isso nasce recolhido (<blockquote expandable>).
CODE_COLLAPSE_CHARS = 400
CODE_COLLAPSE_LINES = 12
# Fração de MAX_MSG_CHARS usada como tamanho do bloco em streaming: o resto é
# folga para as tags do HTML (negrito, código, listas de tabela) caberem.
CHUNK_HEADROOM = 0.75
# Campos que descrevem melhor a ação quando o frame não traz `intent`.
ARG_PRIORITY_KEYS = (
    "path",
    "file",
    "filePath",
    "file_path",
    "command",
    "pattern",
    "query",
    "url",
    "name",
    "prompt",
)


def _one_line(text: str) -> str:
    """Colapsa qualquer espaço/quebra de linha em espaço simples."""
    return " ".join(text.split())


def _split_table_row(line: str) -> list[str]:
    """Células de uma linha de tabela Markdown, sem os pipes das pontas."""
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in stripped.split("|")]


def _is_table_separator(line: str) -> bool:
    """Linha `|---|---|` (com alinhamento opcional) que separa cabeçalho do corpo."""
    stripped = line.strip()
    if not stripped.startswith("|") or "-" not in stripped:
        return False
    return all(
        cell and set(cell) <= {"-", ":", " "} and "-" in cell
        for cell in _split_table_row(stripped)
    )


def _tables_to_lists(text: str) -> str:
    """Converte tabelas Markdown em listas.

    O Telegram não renderiza tabelas: os pipes apareceriam literalmente. Cada
    linha vira um item — a primeira célula em negrito, o resto depois de `—`
    (ou separado por `·` quando há mais de duas colunas).
    """
    lines = text.split("\n")
    out: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line.strip().startswith("|"):
            out.append(line)
            index += 1
            continue

        # Coleta o bloco contíguo de linhas de tabela.
        block: list[str] = []
        while index < len(lines) and lines[index].strip().startswith("|"):
            block.append(lines[index])
            index += 1

        rows: list[list[str]] = []
        header: list[str] = []
        for offset, row_line in enumerate(block):
            if _is_table_separator(row_line):
                continue
            cells = _split_table_row(row_line)
            if not any(cells):
                continue
            # A primeira linha é cabeçalho quando a segunda é o separador.
            if offset == 0 and len(block) > 1 and _is_table_separator(block[1]):
                header = cells
                continue
            rows.append(cells)

        if not rows:
            out.extend(block)
            continue

        # Rótulos das colunas de valor. Sem eles, uma tabela como `| | CBD | THC |`
        # perderia o significado ("não" é de quem?) ao virar lista.
        labels = [cell for cell in header[1:]] if header else []
        for cells in rows:
            head = cells[0]
            rest = [c for c in cells[1:] if c]
            if not rest:
                out.append(f"▸ {head}")
            elif len(rest) == 1:
                out.append(f"▸ {head} — {rest[0]}")
            elif labels and len(labels) >= len(cells) - 1:
                pairs = [
                    f"{label}: {value}" if label else value
                    for label, value in zip(labels, cells[1:])
                    if value
                ]
                out.append(f"▸ {head} — " + " · ".join(pairs))
            else:
                out.append(f"▸ {head} — " + " · ".join(rest))
    return "\n".join(out)


def _render_code_block(lang: str, body: str) -> str:
    """`<pre>` com a linguagem; bloco longo vira citação colapsável.

    Um JSON de 100 linhas direto no chat é uma parede. O Telegram suporta
    `<blockquote expandable>` (Bot API 7.3+) e entidades aninhadas, então o
    bloco longo nasce recolhido e o usuário abre se quiser.
    """
    language = lang.strip().split()[0].lower() if lang.strip() else ""
    if language and re.fullmatch(r"[a-z0-9+#._-]{1,20}", language):
        inner = f'<pre><code class="language-{language}">{body}</code></pre>'
    else:
        inner = f"<pre>{body}</pre>"
    if len(body) > CODE_COLLAPSE_CHARS or body.count("\n") > CODE_COLLAPSE_LINES:
        return f"<blockquote expandable>{inner}</blockquote>"
    return inner


def _to_telegram_html(text: str) -> str:
    """Converte o Markdown do modelo para o HTML do Telegram.

    Só é usado na edição **final**: durante o stream o texto é parcial e uma
    formatação aberta quebraria o parser (`can't parse entities`). Aqui o texto
    está completo, então basta escapar o HTML e traduzir o subconjunto que o
    modelo realmente usa. Marcador desbalanceado fica literal e o HTML continua
    válido; o que o Telegram não suporta (tabelas) é reescrito.
    """
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Blocos cercados saem de cena primeiro para o conteúdo não ser reformatado.
    blocks: list[tuple[str, str]] = []

    def stash(match: re.Match[str]) -> str:
        blocks.append((match.group("lang") or "", match.group("body")))
        return f"\x00B{len(blocks) - 1}\x00"

    escaped = re.sub(
        r"```(?P<lang>[^\n`]*)\n(?P<body>.*?)```", stash, escaped, flags=re.DOTALL
    )
    escaped = _tables_to_lists(escaped)

    # Código inline também é intocável: sem isso, `x * 3` e `x * 2` na mesma
    # linha fechavam um itálico dentro de cada `<code>` (virava `x <i> 3`).
    codes: list[str] = []

    def stash_code(match: re.Match[str]) -> str:
        codes.append(match.group(1))
        return f"\x00C{len(codes) - 1}\x00"

    escaped = re.sub(r"`([^`\n]+)`", stash_code, escaped)

    escaped = re.sub(r"\*\*([^*\n]+)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"__([^_\n]+)__", r"<b>\1</b>", escaped)
    # Itálico depois do negrito, com guardas para não pegar `* item` de lista
    # nem `snake_case` (o `_` entre palavras não é ênfase).
    escaped = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?![\w*])", r"<i>\1</i>", escaped)
    escaped = re.sub(r"(?<!\w)_([^_\n]+)_(?!\w)", r"<i>\1</i>", escaped)
    escaped = re.sub(r"~~([^~\n]+)~~", r"<s>\1</s>", escaped)
    escaped = re.sub(
        r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)", r'<a href="\2">\1</a>', escaped
    )
    escaped = re.sub(r"^#{1,6}\s+(.+)$", r"<b>\1</b>", escaped, flags=re.MULTILINE)
    # `---` sozinho é régua no Markdown; no Telegram vira um separador visual.
    # `[ \t]*` (e não `\s*`) para não engolir as linhas em branco ao redor.
    escaped = re.sub(
        r"^[ \t]*(?:-{3,}|\*{3,}|_{3,})[ \t]*$", "──────", escaped, flags=re.MULTILINE
    )
    # Citação: `> texto` vira bloco citado (o `>` já está escapado).
    escaped = re.sub(
        r"(?:^&gt;[ \t]?.*(?:\n|$))+",
        lambda m: "<blockquote>"
        + re.sub(r"^&gt;[ \t]?", "", m.group(0).rstrip("\n"), flags=re.MULTILINE)
        + "</blockquote>\n",
        escaped,
        flags=re.MULTILINE,
    )
    for index, code in enumerate(codes):
        escaped = escaped.replace(f"\x00C{index}\x00", f"<code>{code}</code>")
    for index, (lang, body) in enumerate(blocks):
        escaped = escaped.replace(f"\x00B{index}\x00", _render_code_block(lang, body))
    return escaped


def _truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


# Ferramentas cujo resultado é o plano de trabalho do omp. O `todo` devolve a
# lista em `result.details.phases` (estruturada) e em `result.content[0].text`
# (já formatada); a estruturada é a fonte preferida.
TODO_TOOLS = {"todo", "set_todos"}
PLAN_HEADER = "📋 plano"
STATUS_MARKS = {
    "completed": "✅",
    "in_progress": "🔄",
    "pending": "⬜",
    "blocked": "⛔",
    "cancelled": "🚫",
}


def _extract_phases(frame: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Fases/tarefas do frame de `todo`, quando houver."""
    result = frame.get("result")
    if not isinstance(result, dict):
        return None
    details = result.get("details")
    if isinstance(details, dict):
        phases = details.get("phases")
        if isinstance(phases, list) and phases:
            return [p for p in phases if isinstance(p, dict)]
    return None


def _render_plan(phases: list[dict[str, Any]]) -> str:
    """Lista de tarefas do plano, com progresso e marcas por status."""
    tasks = [t for p in phases for t in (p.get("tasks") or []) if isinstance(t, dict)]
    done = sum(1 for t in tasks if t.get("status") == "completed")
    lines = [f"{PLAN_HEADER} · {done}/{len(tasks)}"]
    for phase in phases:
        name = str(phase.get("name") or "").strip()
        if name:
            lines.append(f"\n{name}")
        for task in phase.get("tasks") or []:
            if not isinstance(task, dict):
                continue
            mark = STATUS_MARKS.get(str(task.get("status")), "⬜")
            content = str(task.get("content") or "").strip()
            lines.append(f"  {mark} {content}")
    return "\n".join(lines)


def _split_chunk(text: str, limit: int) -> tuple[str, str]:
    """Divide o texto no último limite natural antes de `limit`.

    Cortar no meio de uma tabela ou bloco de código deixa a outra metade
    irreconhecível (a linha `|---|---|` fica sem o cabeçalho, e a metade sem o
    `|` inicial deixa de ser detectada como tabela). Por isso preferimos a
    última linha em branco, depois o último fim de linha, e só então o corte duro.
    """
    if len(text) <= limit:
        return text, ""
    window = text[:limit]
    floor = limit // 4  # não fecha blocos minúsculos só para achar um limite
    for separator in ("\n\n", "\n"):
        index = window.rfind(separator)
        if index > floor:
            cut = index + len(separator)
            return text[:cut], text[cut:]
    return text[:limit], text[limit:]


def _retry_after_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


class TurnStreamer:
    """Uma instância viva por turno do agente."""

    def __init__(self, bot: Bot, chat_id: int, config: Any) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.config = config

        self.text_buf = ""
        self.last_sent = ""
        self.full_text = ""
        # O HTML ocupa mais que o texto cru (tags); fechamos cada bloco com folga
        # para que a versão renderizada ainda caiba no limite do Telegram.
        self.chunk_chars = max(1, int(config.max_msg_chars * CHUNK_HEADROOM))
        self.active_msg_id: int | None = None
        # Última mensagem escrita: usada para pendurar o rodapé quando o texto
        # terminou exatamente num bloco (senão nasceria um "✅ concluído" solto).
        self.last_msg_id: int | None = None
        self.last_edit = 0.0
        self.started_at = time.monotonic()

        self.activity_msg_id: int | None = None
        self.activity_lines: list[str] = []
        self._activity_index: dict[str, int] = {}
        self.tool_counts: Counter[str] = Counter()
        self.tool_failures = 0
        self.last_activity_sent = ""
        self.last_activity_edit = 0.0
        # Mensagem própria para o plano (todo): fica legível e não se mistura
        # com a linha de atividade, que é passageira.
        self.plan_msg_id: int | None = None
        self.plan_text = ""
        self.last_plan_sent = ""
        self.last_plan_edit = 0.0
        self._last_chat_write = 0.0

        self.error: str | None = None
        self.aborted = False
        self.closed = False
        self._last_typing = 0.0

    # ------------------------------------------------------------- consumo

    async def consume(self, frame: dict[str, Any]) -> None:
        ftype = frame.get("type")

        if ftype == "message_update":
            event = frame.get("assistantMessageEvent") or {}
            if event.get("type") == "text_delta":
                delta = event.get("delta") or ""
                if delta:
                    self.text_buf += delta
                    self.full_text += delta
                    if len(self.text_buf) >= self.chunk_chars:
                        await self._roll_over_chunks()
            return

        if ftype == "tool_execution_start":
            name = frame.get("toolName") or "tool"
            self.tool_counts[name] += 1
            if name in TODO_TOOLS:
                # O plano tem mensagem própria; não vira linha de atividade nem
                # despeja os argumentos crus (eram um JSON enorme e ilegível).
                return
            summary = self._args_summary(frame)
            detail = f"{name}: {summary}" if summary else name
            self._activity_add(f"🔧 {detail}", frame.get("toolCallId"))
            await self._flush_activity()
            return

        if ftype == "tool_execution_end":
            name = frame.get("toolName") or "tool"
            if frame.get("isError"):
                self.tool_failures += 1
            if name in TODO_TOOLS:
                phases = _extract_phases(frame)
                if phases:
                    self.plan_text = _render_plan(phases)
                    await self._flush_plan()
                return
            mark = "✗" if frame.get("isError") else "✓"
            self._activity_complete(frame.get("toolCallId"), f"{mark} {name}")
            await self._flush_activity()
            return

        if ftype == "notice":
            message = frame.get("message") or frame.get("text") or ""
            if message:
                await self._safe_send(f"ℹ️ {message}")
            return

        if ftype == "extension_error":
            await self._safe_send(
                f"⚠️ {frame.get('event', 'erro')}: {frame.get('error', 'desconhecido')}"
            )
            return

        if ftype == "prompt_result":
            self._apply_prompt_result(frame)
            return

    async def _roll_over_chunks(self) -> None:
        """Fecha blocos enquanto o buffer passar do tamanho, renderizando cada um.

        Um bloco fechado não recebe mais deltas, então é o momento certo de
        aplicar o Markdown: antes disso o texto é parcial e uma formatação
        aberta quebraria o parser do Telegram.
        """
        while len(self.text_buf) >= self.chunk_chars:
            head, tail = _split_chunk(self.text_buf, self.chunk_chars)
            self.text_buf = head
            await self._flush_edit(force=True, render=True)
            self.active_msg_id = None
            self.text_buf = tail

    def _apply_prompt_result(self, frame: dict[str, Any]) -> None:
        status = frame.get("status")
        if status == "error":
            error = frame.get("error") or {}
            message = error.get("message") if isinstance(error, dict) else str(error)
            self.error = message or "erro desconhecido"
        elif status == "aborted":
            self.aborted = True

    @staticmethod
    def _args_summary(frame: dict[str, Any]) -> str:
        """Detalhe legível da ferramenta.

        `tool_execution_start` traz `intent` (frase do modelo, ex. "Creating
        hello.py"); preferir isso ao JSON cru, que despeja conteúdo de arquivo.
        Sem `intent`, mostra o campo mais informativo dos argumentos — nunca o
        JSON inteiro.
        """
        intent = frame.get("intent")
        if isinstance(intent, str) and intent.strip():
            return _truncate(_one_line(intent), ARG_SUMMARY_LIMIT)

        for key in ("args", "input", "arguments"):
            raw_args = frame.get(key)
            if raw_args in (None, "", {}, []):
                continue
            if isinstance(raw_args, dict):
                for preferred in ARG_PRIORITY_KEYS:
                    value = raw_args.get(preferred)
                    if isinstance(value, str) and value.strip():
                        return _truncate(_one_line(value), ARG_SUMMARY_LIMIT)
                for value in raw_args.values():
                    if isinstance(value, str) and value.strip():
                        return _truncate(_one_line(value), ARG_SUMMARY_LIMIT)
            try:
                raw = json.dumps(raw_args, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                raw = str(raw_args)
            return _truncate(_one_line(raw), ARG_SUMMARY_LIMIT)
        return ""

    # --------------------------------------------------------------- tick

    async def tick(self) -> None:
        if self.closed:
            return
        now = time.monotonic()
        if (
            self.text_buf
            and self.text_buf != self.last_sent
            and self._may_write(now, self.last_edit, self.config.stream_edit_seconds)
        ):
            await self._flush_edit()
        if (
            self.activity_lines
            and self._activity_text() != self.last_activity_sent
            and self._may_write(now, self.last_activity_edit, ACTIVITY_THROTTLE)
        ):
            await self._flush_activity()
        if (
            self.plan_text
            and self.plan_text != self.last_plan_sent
            and self._may_write(now, self.last_plan_edit, PLAN_THROTTLE)
        ):
            await self._flush_plan()
        if now - self._last_typing >= TYPING_INTERVAL:
            self._last_typing = now
            try:
                await self.bot.send_chat_action(self.chat_id, ChatAction.TYPING)
            except (BadRequest, RetryAfter):
                pass
            except Exception:
                log.debug("send_chat_action falhou", exc_info=True)

    def _may_write(self, now: float, channel_last: float, interval: float) -> bool:
        """O limite do Telegram é ~1 msg/s por chat, e edições contam junto.

        Além do intervalo de cada canal, exige um piso compartilhado entre stream
        e atividade para não estourar o limite com duas escritas seguidas.
        """
        return (
            now - channel_last >= interval
            and now - self._last_chat_write >= MIN_SEND_INTERVAL
        )

    # ----------------------------------------------------------- mensagens

    async def _safe_send(self, text: str) -> None:
        """Mensagem solta, sem throttle (usada para avisos)."""
        await self._call(self.bot.send_message, self.chat_id, text)

    async def _call(self, func, *args, **kwargs):
        """Executa uma chamada do Telegram tratando 429 e 'not modified'."""
        try:
            return await func(*args, **kwargs)
        except RetryAfter as exc:
            wait = _retry_after_seconds(exc) + 0.5
            log.warning("429 do Telegram: aguardando %.1fs", wait)
            await asyncio.sleep(wait)
            try:
                return await func(*args, **kwargs)
            except BadRequest as inner:
                if "not modified" in str(inner).lower():
                    return None
                raise
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                return None
            raise

    def _render_payload(self, text: str) -> tuple[str, dict[str, Any]]:
        """Renderiza o Markdown para HTML do Telegram, se couber no limite."""
        if not text:
            return text, {}
        candidate = _to_telegram_html(text)
        if len(candidate) <= self.config.max_msg_chars:
            return candidate, {"parse_mode": ParseMode.HTML}
        log.info(
            "render maior que o limite (%d > %d); mantendo texto puro",
            len(candidate),
            self.config.max_msg_chars,
        )
        return text, {}

    async def _deliver(self, outgoing: str, extra: dict[str, Any]) -> None:
        if self.active_msg_id is None:
            message = await self._call(self.bot.send_message, self.chat_id, outgoing, **extra)
            if message is not None:
                self.active_msg_id = message.message_id
        else:
            await self._call(
                self.bot.edit_message_text,
                outgoing,
                chat_id=self.chat_id,
                message_id=self.active_msg_id,
                **extra,
            )
        if self.active_msg_id is not None:
            self.last_msg_id = self.active_msg_id

    async def _write(self, text: str, *, render: bool) -> None:
        """Escreve/edita a mensagem ativa; com `render`, aplica o Markdown."""
        if render:
            outgoing, extra = self._render_payload(text)
            if extra:
                try:
                    await self._deliver(outgoing, extra)
                    return
                except BadRequest as exc:
                    # Markdown inválido para o parser: entrega cru, sem perder a resposta.
                    log.info("HTML recusado (%s); enviando texto puro", exc)
        await self._deliver(text, {})

    async def _flush_edit(self, *, force: bool = False, render: bool = False) -> None:
        if self.closed:
            return
        if not self.text_buf and not force:
            return

        text = self.text_buf or ("⏳ trabalhando…" if self.active_msg_id is None else self.last_sent)
        if self.active_msg_id is not None and text == self.last_sent and not render:
            return

        await self._write(text, render=render)
        self.last_sent = text
        self.last_edit = self._last_chat_write = time.monotonic()

    def _activity_text(self) -> str:
        return ACTIVITY_HEADER + "\n".join(self.activity_lines)

    def _activity_add(self, line: str, tool_call_id: Any = None) -> None:
        """Anexa uma linha; guarda o índice para atualizá-la quando a ferramenta acabar."""
        self.activity_lines.append(line)
        if isinstance(tool_call_id, str) and tool_call_id:
            self._activity_index[tool_call_id] = len(self.activity_lines) - 1
        if len(self.activity_lines) > ACTIVITY_MAX_LINES:
            removed = len(self.activity_lines) - ACTIVITY_MAX_LINES
            del self.activity_lines[:removed]
            # Os índices guardados deslizam com o corte.
            self._activity_index = {
                key: index - removed
                for key, index in self._activity_index.items()
                if index >= removed
            }

    def _activity_complete(self, tool_call_id: Any, replacement: str) -> None:
        """Troca a linha de início pela de fim (evita o par `🔧 x` + `✓ x`)."""
        index = self._activity_index.pop(tool_call_id, None) if isinstance(tool_call_id, str) else None
        if index is not None and 0 <= index < len(self.activity_lines):
            start_line = self.activity_lines[index]
            detail = start_line.split(": ", 1)[1] if ": " in start_line else ""
            self.activity_lines[index] = f"{replacement}: {detail}" if detail else replacement
            return
        self._activity_add(replacement)

    async def _flush_activity(self, *, force: bool = False) -> None:
        if self.closed or not self.activity_lines:
            # Nunca cria/edita a mensagem de atividade sem nenhuma linha.
            return
        text = self._activity_text()
        if text == self.last_activity_sent:
            return
        now = time.monotonic()
        if not force and not self._may_write(now, self.last_activity_edit, ACTIVITY_THROTTLE):
            return
        if self.activity_msg_id is None:
            message = await self._call(self.bot.send_message, self.chat_id, text)
            if message is not None:
                self.activity_msg_id = message.message_id
        else:
            await self._call(
                self.bot.edit_message_text,
                text,
                chat_id=self.chat_id,
                message_id=self.activity_msg_id,
            )
        self.last_activity_sent = text
        self.last_activity_edit = self._last_chat_write = time.monotonic()

    async def _flush_plan(self, *, force: bool = False) -> None:
        """Escreve/edita a mensagem do plano, com o mesmo throttle do chat."""
        if self.closed or not self.plan_text or self.plan_text == self.last_plan_sent:
            return
        now = time.monotonic()
        if not force and not self._may_write(now, self.last_plan_edit, PLAN_THROTTLE):
            return
        if self.plan_msg_id is None:
            message = await self._call(
                self.bot.send_message, self.chat_id, self.plan_text
            )
            if message is not None:
                self.plan_msg_id = message.message_id
        else:
            await self._call(
                self.bot.edit_message_text,
                self.plan_text,
                chat_id=self.chat_id,
                message_id=self.plan_msg_id,
            )
        self.last_plan_sent = self.plan_text
        self.last_plan_edit = self._last_chat_write = time.monotonic()

    def _activity_summary(self) -> str:
        """Resumo de uma linha: o detalhe ao vivo vira contagem no fim do turno."""
        elapsed = time.monotonic() - self.started_at
        parts = [f"{name} ×{count}" for name, count in self.tool_counts.most_common()]
        text = "⚙️ " + (" · ".join(parts) if parts else "sem ferramentas")
        text += f" · {elapsed:.0f}s"
        if self.tool_failures:
            text += f" · {self.tool_failures} falha(s)"
        return text

    async def _collapse_activity(self) -> None:
        """Troca a lista de ferramentas por um resumo, evitando a parede no chat."""
        if self.activity_msg_id is None and not self.activity_lines:
            return
        summary = self._activity_summary()
        if self.activity_msg_id is None:
            message = await self._call(self.bot.send_message, self.chat_id, summary)
            if message is not None:
                self.activity_msg_id = message.message_id
        else:
            await self._call(
                self.bot.edit_message_text,
                summary,
                chat_id=self.chat_id,
                message_id=self.activity_msg_id,
            )
        self.last_activity_sent = summary
        self.last_activity_edit = self._last_chat_write = time.monotonic()

    # ------------------------------------------------------------- fim

    def _footer(self, state: dict[str, Any]) -> str:
        elapsed = time.monotonic() - self.started_at
        model = (state.get("model") or {}).get("id") or "?"
        usage = (state.get("contextUsage") or {}).get("percent")
        parts = [f"— ✅ pronto em {elapsed:.0f}s · {model}"]
        if isinstance(usage, (int, float)):
            parts.append(f"ctx {round(usage)}%")
        if self.error:
            parts = [f"— ❌ {self.error}"]
        elif self.aborted:
            parts = ["— 🛑 interrompido"]
        return "\n\n" + " · ".join(parts)

    async def finish(self, state: dict[str, Any] | None = None) -> None:
        if self.closed:
            return
        state = state or {}
        await self._collapse_activity()
        # O plano é entregável do turno: fica visível, não entra no resumo.
        await self._flush_plan(force=True)
        if self.active_msg_id is None and not self.text_buf and self.last_msg_id is not None:
            # O texto terminou exatamente num bloco já fechado: o rodapé vai na
            # última mensagem, em vez de criar um "✅ concluído" solto.
            self.active_msg_id = self.last_msg_id
            self.text_buf = self.last_sent
        if self.active_msg_id is None and not self.text_buf:
            self.text_buf = "✅ concluído"
        await self._flush_edit(force=True)

        if self.active_msg_id is not None:
            footer = self._footer(state)
            text = self.last_sent + footer
            if len(text) > self.config.max_msg_chars:
                text = _truncate(self.last_sent, self.config.max_msg_chars - 80) + footer
            # O texto está completo: agora sim o Markdown vira formatação.
            await self._write(text, render=True)
            self.last_sent = text

        if len(self.full_text) > self.config.doc_threshold:
            await self._call(
                self.bot.send_document,
                self.chat_id,
                document=InputFile(self.full_text.encode("utf-8"), filename="resposta.md"),
                caption="resposta completa",
            )

        self.closed = True
