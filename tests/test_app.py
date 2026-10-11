"""Testes do app: autorização, diálogos de extensão e persistência de offset."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from telegram import Chat, Message, Update, User
from telegram.error import BadRequest

from bridge.app import Bridge
from bridge.config import Config


@dataclass
class SentMessage:
    message_id: int
    text: str
    kwargs: dict[str, Any]


class FakeBot:
    """Bot mínimo: grava envios e devolve mensagens com id incremental."""

    def __init__(self) -> None:
        self.sent: list[SentMessage] = []
        self.edits: list[tuple[int, str, dict[str, Any]]] = []
        self.answered: list[tuple[str, str | None]] = []
        self.deleted: list[int] = []
        self.delete_fails = False
        self.deleted_webhook = 0
        self.commands: list[Any] = []
        self._next = 100

    async def send_message(self, chat_id: int, text: str, **kwargs: Any) -> Message:
        self._next += 1
        self.sent.append(SentMessage(self._next, text, kwargs))
        return _message(self._next, chat_id, text, kwargs.get("reply_markup"))

    async def edit_message_text(self, text: str, **kwargs: Any) -> Message:
        self.edits.append((kwargs.get("message_id"), text, kwargs))
        return _message(kwargs.get("message_id"), kwargs.get("chat_id"), text)

    async def delete_message(self, chat_id: int, message_id: int) -> bool:
        if self.delete_fails:
            raise BadRequest("message can't be deleted")
        self.deleted.append(message_id)
        return True

    async def edit_message_reply_markup(self, **kwargs: Any) -> Message:
        return _message(kwargs.get("message_id"), kwargs.get("chat_id"), "")

    async def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> bool:
        self.answered.append((callback_query_id, text))
        return True

    async def delete_webhook(self, **kwargs: Any) -> bool:
        self.deleted_webhook += 1
        return True

    async def set_my_commands(self, commands: list[Any]) -> bool:
        self.commands = commands
        return True

    def texts(self) -> list[str]:
        return [m.text for m in self.sent]

    def buttons(self) -> list[list[str]]:
        result = []
        for message in self.sent:
            markup = message.kwargs.get("reply_markup")
            if markup is None:
                continue
            rows = getattr(markup, "inline_keyboard", None) or []
            result.append([b.callback_data for row in rows for b in row])
        return result


def _message(message_id: int, chat_id: int, text: str, markup: Any = None) -> Message:
    return Message(
        message_id=message_id,
        date=None,
        chat=Chat(id=chat_id, type="private"),
        text=text,
        reply_markup=markup,
    )


class StubClient:
    """Cliente RPC falso: registra requests e responde com dados programáveis."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.raw: list[dict[str, Any]] = []
        self.prompts: list[dict[str, Any]] = []
        self.responses: dict[str, dict[str, Any]] = {}
        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.running = True
        self._prompt_counter = 0

    async def request(self, cmd: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        self.requests.append(cmd)
        key = cmd["type"]
        payload = self.responses.get(key, {"success": True, "data": {}})
        if payload.get("success") is False:
            from bridge.omp_client import OmpCommandError

            raise OmpCommandError(payload.get("error") or f"comando {key} falhou")
        return {"id": "r", "type": "response", "command": key, **payload}

    async def send_raw(self, frame: dict[str, Any]) -> None:
        self.raw.append(frame)

    async def prompt(self, text: str, **kwargs: Any) -> str:
        self._prompt_counter += 1
        prompt_id = f"req_p{self._prompt_counter}"
        self.prompts.append({"text": text, "id": prompt_id, **kwargs})
        return prompt_id

    def forget_prompt(self, prompt_id: str) -> None:
        pass


def make_bridge(tmp_path: Path, allowed: int = 42) -> Bridge:
    config = Config(
        token="1:x",
        allowed_user_id=allowed,
        omp_bin="omp",
        omp_cwd=tmp_path,
        approval_mode="write",
        model=None,
        stream_edit_seconds=0.5,
        max_msg_chars=3800,
        doc_threshold=12000,
        data_dir=tmp_path / "data",
    )
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.sessions_dir.mkdir(parents=True, exist_ok=True)
    bridge = Bridge(config)
    bridge.bot = FakeBot()
    bridge.client = StubClient()
    return bridge


def text_update(chat_id: int, text: str, update_id: int = 1) -> Update:
    return Update(
        update_id=update_id,
        message=Message(
            message_id=update_id * 10,
            date=None,
            chat=Chat(id=chat_id, type="private"),
            from_user=User(id=chat_id, first_name="Joaki", is_bot=False),
            text=text,
        ),
    )


def callback_update(chat_id: int, data: str, message_id: int, update_id: int = 2) -> Update:
    message = _message(message_id, chat_id, "anterior")
    return Update(
        update_id=update_id,
        callback_query=CallbackQueryStub(data, message, chat_id),
    )


class CallbackQueryStub:
    """Objeto mínimo com a superfície usada pelo handler."""

    def __init__(self, data: str, message: Message, chat_id: int) -> None:
        self.id = "cq1"
        self.data = data
        self.message = message
        self.from_user = User(id=chat_id, first_name="Joaki", is_bot=False)

    async def answer(self, *args: Any, **kwargs: Any) -> bool:  # pragma: no cover
        return True


def test_ask_mode_routes_to_readonly_client(tmp_path: Path) -> None:
    """`/ask` usa um segundo omp com ferramentas só de leitura."""

    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        created: list[dict[str, Any]] = []

        class StubAskClient:
            def __init__(self) -> None:
                self.prompts: list[dict[str, Any]] = []
                self.requests: list[dict[str, Any]] = []
                self.raw: list[dict[str, Any]] = []
                self.events = asyncio.Queue()
                self.running = True
                created.append({"client": self})

            async def start(self) -> None:
                pass

            async def close(self) -> None:
                self.running = False

            async def request(self, cmd: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
                self.requests.append(cmd)
                return {"success": True, "data": {}}

            async def send_raw(self, frame: dict[str, Any]) -> None:
                self.raw.append(frame)

            async def prompt(self, text: str, **kwargs: Any) -> str:
                self.prompts.append({"text": text, **kwargs})
                return "ask_p1"

            def forget_prompt(self, prompt_id: str) -> None:
                pass

        async def fake_ensure() -> Any:
            client = StubAskClient()
            bridge.ask_client = client
            return client

        bridge._ensure_ask_client = fake_ensure  # type: ignore[method-assign]

        await bridge._handle_command(42, "/ask")
        assert bridge.ask_mode is True
        assert "ASK ligado" in bridge.bot.texts()[-1]
        ask = created[0]["client"]

        # Um prompt agora vai para o cliente somente-leitura.
        bridge.client.responses["get_state"] = {"success": True, "data": {"isStreaming": False}}
        await bridge._handle_message(text_update(42, "o que é isso?", update_id=5))
        assert ask.prompts and ask.prompts[0]["text"] == "o que é isso?"
        assert bridge.client.prompts == []

        # E /status lê o estado do cliente do modo ask.
        await bridge._handle_command(42, "/status")
        assert "modo: 🔍 ask (só leitura)" in bridge.bot.texts()[-1]
        assert any(c["type"] == "get_state" for c in ask.requests)

        # /work volta.
        await bridge._handle_command(42, "/work")
        assert bridge.ask_mode is False
        assert "modo normal" in bridge.bot.texts()[-1]

    asyncio.run(scenario())


def test_ask_command_can_carry_the_question(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        prompts: list[str] = []

        class StubAskClient:
            def __init__(self) -> None:
                self.events = asyncio.Queue()
                self.running = True

            async def start(self) -> None:
                pass

            async def request(self, cmd: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
                return {"success": True, "data": {"isStreaming": False}}

            async def prompt(self, text: str, **kwargs: Any) -> str:
                prompts.append(text)
                return "p1"

            def forget_prompt(self, prompt_id: str) -> None:
                pass

        async def fake_ensure() -> Any:
            client = StubAskClient()
            bridge.ask_client = client
            return client

        bridge._ensure_ask_client = fake_ensure  # type: ignore[method-assign]
        await bridge._handle_command(42, "/ask qual a capital da França?")

        assert bridge.ask_mode is True
        assert prompts == ["qual a capital da França?"]
        assert bridge.streamer is not None

    asyncio.run(scenario())


def test_ask_mode_keeps_work_sessions_out_of_index(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        bridge.ask_mode = True
        bridge.client.responses["get_state"] = {
            "success": True,
            "data": {"sessionFile": str(tmp_path / "ask.jsonl"), "messageCount": 3},
        }
        await bridge._refresh_index()
        assert not bridge.config.index_file.exists()

    asyncio.run(scenario())


def test_status_shows_normal_mode_by_default(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        bridge.client.responses["get_state"] = {"success": True, "data": {}}
        await bridge._handle_command(42, "/status")
        assert "modo: 🛠️ normal" in bridge.bot.texts()[-1]

    asyncio.run(scenario())


def test_select_approval_round_trip(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        frame = {
            "type": "extension_ui_request",
            "id": "ui_1",
            "method": "select",
            "title": "Allow tool: bash\nCommand: echo done",
            "options": ["Approve", "Deny"],
        }
        await bridge._handle_ui_request(frame)

        assert bridge.bot.texts() == ["❓ Allow tool: bash\nCommand: echo done"]
        assert len(bridge.pending_ui) == 1
        key = next(iter(bridge.pending_ui))
        approve_data = bridge.bot.buttons()[0]
        assert approve_data == [f"ui:{key}:0", f"ui:{key}:1"]

        await bridge._handle_callback(callback_update(42, f"ui:{key}:0", 1001))
        assert bridge.client.raw == [
            {"type": "extension_ui_response", "id": "ui_1", "value": "Approve"}
        ]
        assert bridge.pending_ui == {}
        assert bridge.bot.answered == [("cq1", None)]
        # A mensagem do botão sai do chat: nada de "✅ Approve" acumulando.
        assert bridge.bot.deleted == [1001]
        assert not [t for _, t, _ in bridge.bot.edits if "Approve" in t]

        # Deny no segundo pedido
        await bridge._handle_ui_request({**frame, "id": "ui_2"})
        key2 = next(iter(bridge.pending_ui))
        await bridge._handle_callback(callback_update(42, f"ui:{key2}:1", 1002))
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_2",
            "value": "Deny",
        }

    asyncio.run(scenario())


def test_approval_message_falls_back_to_marker_when_delete_fails(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        bridge.bot.delete_fails = True
        await bridge._handle_ui_request(
            {
                "type": "extension_ui_request",
                "id": "ui_x",
                "method": "select",
                "title": "Allow tool: bash",
                "options": ["Approve", "Deny"],
            }
        )
        key = next(iter(bridge.pending_ui))
        await bridge._handle_callback(callback_update(42, f"ui:{key}:1", 1001))

        assert bridge.bot.deleted == []
        assert [t for _, t, _ in bridge.bot.edits if t == "🚫 Deny"] == ["🚫 Deny"]
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_x",
            "value": "Deny",
        }

    asyncio.run(scenario())


def test_confirm_uses_boolean(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        await bridge._handle_ui_request(
            {"type": "extension_ui_request", "id": "ui_c", "method": "confirm", "title": "Continuar?"}
        )
        assert bridge.bot.texts() == ["❓ Continuar?"]
        key = next(iter(bridge.pending_ui))

        await bridge._handle_callback(callback_update(42, f"ui:{key}:0", 1001))
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_c",
            "confirmed": True,
        }

        await bridge._handle_ui_request(
            {"type": "extension_ui_request", "id": "ui_d", "method": "confirm", "title": "De novo?"}
        )
        key2 = next(iter(bridge.pending_ui))
        await bridge._handle_callback(callback_update(42, f"ui:{key2}:1", 1002))
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_d",
            "confirmed": False,
        }

    asyncio.run(scenario())


def test_input_dialog_uses_next_text_and_cancel(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        await bridge._handle_ui_request(
            {
                "type": "extension_ui_request",
                "id": "ui_i",
                "method": "input",
                "title": "Nome do branch",
                "placeholder": "feature/...",
            }
        )
        assert bridge.pending_text == {42: "ui_i"}
        assert "feature/..." in bridge.bot.texts()[-1]

        await bridge._handle_message(text_update(42, "feature/nova"))
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_i",
            "value": "feature/nova",
        }
        assert bridge.pending_text == {}

        # /cancel responde com cancelled
        await bridge._handle_ui_request(
            {"type": "extension_ui_request", "id": "ui_j", "method": "editor", "title": "Texto"}
        )
        await bridge._handle_command(42, "/cancel")
        assert bridge.client.raw[-1] == {
            "type": "extension_ui_response",
            "id": "ui_j",
            "cancelled": True,
        }
        assert bridge.pending_text == {}

    asyncio.run(scenario())


def test_cancel_request_clears_pending_entry(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        await bridge._handle_ui_request(
            {
                "type": "extension_ui_request",
                "id": "ui_k",
                "method": "select",
                "title": "Escolha",
                "options": ["a", "b"],
            }
        )
        await bridge._handle_ui_request(
            {"type": "extension_ui_request", "id": "c1", "method": "cancel", "targetId": "ui_k"}
        )
        assert bridge.pending_ui == {}

    asyncio.run(scenario())


def test_unauthorized_user_warned_once(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path, allowed=42)
        await bridge._handle_update(text_update(777, "oi"))
        await bridge._handle_update(text_update(777, "oi de novo", update_id=2))

        assert bridge.bot.texts() == ["não autorizado"]
        assert bridge.client.prompts == []

    asyncio.run(scenario())


def test_pending_ui_is_capped(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        for i in range(60):
            await bridge._handle_ui_request(
                {
                    "type": "extension_ui_request",
                    "id": f"ui_{i}",
                    "method": "select",
                    "title": f"pedido {i}",
                    "options": ["Approve", "Deny"],
                }
            )
        assert len(bridge.pending_ui) == 50
        # As mais antigas foram descartadas.
        remaining = {entry["id"] for entry in bridge.pending_ui.values()}
        assert "ui_59" in remaining
        assert "ui_0" not in remaining

    asyncio.run(scenario())


def test_offset_persisted_after_handling(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        assert bridge._read_offset() is None
        bridge._write_offset(7)
        assert bridge._read_offset() == 7
        assert json.loads(bridge.config.offset_file.read_text(encoding="utf-8")) == {"offset": 7}
        assert list(bridge.config.offset_file.parent.glob("*.tmp")) == []

    asyncio.run(scenario())


def test_expired_callback_answers_expired(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        await bridge._handle_callback(callback_update(42, "ui:naoexiste:0", 1001))
        assert bridge.bot.answered[-1] == ("cq1", "expirado")
        assert bridge.client.raw == []

    asyncio.run(scenario())


def test_status_formats_state(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        bridge.client.responses["get_state"] = {
            "success": True,
            "data": {
                "model": {"provider": "inferhub", "id": "deepseek-v4.1-flash"},
                "thinkingLevel": "high",
                "sessionFile": "C:/s/2026_abc.jsonl",
                "sessionId": "abc",
                "messageCount": 12,
                "queuedMessageCount": 1,
                "contextUsage": {"percent": 41.6},
                "isStreaming": False,
                "isSettled": True,
            },
        }
        await bridge._handle_command(42, "/status")
        text = bridge.bot.texts()[-1]
        assert "modelo: inferhub/deepseek-v4.1-flash" in text
        assert "thinking: high" in text
        assert "sessão: 2026_abc.jsonl" in text
        assert "mensagens: 12 · na fila: 1" in text
        assert "contexto: 42%" in text
        assert "estado: ocioso" in text

    asyncio.run(scenario())


def test_unknown_command_lists_help(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        await bridge._handle_command(42, "/naoexiste")
        assert "comando desconhecido: /naoexiste" in bridge.bot.texts()[-1]

    asyncio.run(scenario())


def test_omp_error_is_reported_to_chat(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        bridge.client.responses["get_state"] = {"success": False, "error": "sessão ocupada"}
        await bridge._handle_command(42, "/status")
        assert bridge.bot.texts()[-1] == "❌ sessão ocupada"

    asyncio.run(scenario())


def test_setup_mode_only_answers_id(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path, allowed=0)
        await bridge._handle_update(text_update(777, "/id"))
        assert "user_id: 777" in bridge.bot.texts()[-1]

        await bridge._handle_update(text_update(777, "oi", update_id=3))
        assert "configuração pendente" in bridge.bot.texts()[-1]
        assert bridge.client.prompts == []

    asyncio.run(scenario())


def test_photo_becomes_image_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        raw = b"\x89PNG\r\n\x1a\n"

        class FakeFile:
            file_path = "photos/foto.png"

            async def download_as_bytearray(self) -> bytearray:
                return bytearray(raw)

        async def get_file(file_id: str) -> FakeFile:
            return FakeFile()

        bridge.bot.get_file = get_file  # type: ignore[method-assign]
        bridge.client.responses["get_state"] = {"success": True, "data": {}}

        photo = type("P", (), {"file_id": "p1"})()
        message = Message(
            message_id=1,
            date=None,
            chat=Chat(id=42, type="private"),
            from_user=User(id=42, first_name="J", is_bot=False),
            photo=[photo],
            caption="descreva",
        )
        await bridge._handle_photo(message)

        assert len(bridge.client.prompts) == 1
        sent = bridge.client.prompts[0]
        assert sent["text"] == "descreva"
        image = sent["images"][0]
        assert image["type"] == "image"
        assert image["mimeType"] == "image/png"
        assert image["data"]

    asyncio.run(scenario())


def test_unsupported_document_is_rejected(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)
        document = type("D", (), {"mime_type": "application/zip", "file_size": 10, "file_id": "z"})()
        message = Message(
            message_id=1,
            date=None,
            chat=Chat(id=42, type="private"),
            from_user=User(id=42, first_name="J", is_bot=False),
            document=document,
        )
        await bridge._handle_document(message)
        assert bridge.bot.texts()[-1] == "não suportado (envie texto ou imagem)"
        assert bridge.client.prompts == []

    asyncio.run(scenario())


def test_text_document_is_inlined(tmp_path: Path) -> None:
    async def scenario() -> None:
        bridge = make_bridge(tmp_path)

        class FakeFile:
            file_path = "f/notas.md"

            async def download_as_bytearray(self) -> bytearray:
                return bytearray("conteúdo do arquivo".encode())

        async def get_file(file_id: str) -> FakeFile:
            return FakeFile()

        bridge.bot.get_file = get_file  # type: ignore[method-assign]
        bridge.client.responses["get_state"] = {"success": True, "data": {}}

        document = type(
            "D",
            (),
            {
                "mime_type": "text/markdown",
                "file_size": 100,
                "file_id": "d",
                "file_name": "notas.md",
            },
        )()
        message = Message(
            message_id=1,
            date=None,
            chat=Chat(id=42, type="private"),
            from_user=User(id=42, first_name="J", is_bot=False),
            document=document,
            caption="resuma",
        )
        await bridge._handle_document(message)

        text = bridge.client.prompts[0]["text"]
        assert text.startswith("resuma")
        assert "[arquivo notas.md]" in text
        assert "conteúdo do arquivo" in text

    asyncio.run(scenario())
