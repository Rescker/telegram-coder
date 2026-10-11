"""Servidor falso da Bot API para o smoke test ponta a ponta (descartável)."""

from __future__ import annotations

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs

_TINY_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d4944415478da63f8cfc000000301010018dd8db10000000049454e44ae426082"
)


def _parse_multipart(raw: bytes, boundary: str) -> dict[str, Any]:
    """Leitura mínima de multipart/form-data (só o que o smoke precisa)."""
    data: dict[str, Any] = {}
    if not boundary:
        return data
    for part in raw.split(b"--" + boundary.encode()):
        head, sep, body = part.partition(b"\r\n\r\n")
        if not sep:
            continue
        headers = head.decode("utf-8", errors="replace")
        name_match = re.search(r'name="([^"]+)"', headers)
        if name_match is None:
            continue
        name = name_match.group(1)
        file_match = re.search(r'filename="([^"]*)"', headers)
        body = body.rstrip(b"\r\n")
        if file_match is not None:
            data[name] = {"filename": file_match.group(1), "size": len(body)}
        else:
            value = body.decode("utf-8", errors="replace")
            try:
                data[name] = json.loads(value)
            except json.JSONDecodeError:
                data[name] = value
    for key in ("chat_id", "message_id"):
        if isinstance(data.get(key), str) and data[key].lstrip("-").isdigit():
            data[key] = int(data[key])
    return data


class FakeTelegram:
    def __init__(self, user_id: int = 42) -> None:
        self.user_id = user_id
        self.cond = threading.Condition()
        self.updates: list[dict[str, Any]] = []
        self.next_update_id = 1
        self.next_message_id = 1000
        self.messages: dict[int, str] = {}
        self.sent: list[dict[str, Any]] = []
        self.server: ThreadingHTTPServer | None = None
        self.port = 0
        self.file_bytes = _TINY_PNG
        self.last_getupdates: dict[str, Any] = {}

    # ------------------------------------------------------------ controle

    def start(self) -> str:
        state = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *args: Any) -> None:  # silencia
                pass

            def _read_payload(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                ctype = (self.headers.get("Content-Type") or "").lower()
                if "json" in ctype:
                    try:
                        return json.loads(raw or b"{}")
                    except json.JSONDecodeError:
                        return {}
                if "multipart/form-data" in ctype:
                    boundary = ""
                    match = re.search(r'boundary="?([^";]+)"?', ctype)
                    if match:
                        boundary = match.group(1)
                    return _parse_multipart(raw, boundary)
                if not raw:
                    return {}
                parsed = parse_qs(raw.decode("utf-8", errors="replace"))
                data: dict[str, Any] = {}
                for key, values in parsed.items():
                    value = values[0]
                    try:
                        data[key] = json.loads(value)
                    except json.JSONDecodeError:
                        data[key] = value
                for key in ("chat_id", "message_id"):
                    if isinstance(data.get(key), str) and data[key].lstrip("-").isdigit():
                        data[key] = int(data[key])
                return data

            def _reply(self, payload: dict[str, Any], status: int = 200) -> None:
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith("/file/bot"):
                    body = state.file_bytes
                    self.send_response(200)
                    self.send_header("Content-Type", "image/png")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                self._safe_handle({})

            def do_POST(self) -> None:  # noqa: N802
                self._safe_handle(self._read_payload())

            def _safe_handle(self, payload: dict[str, Any]) -> None:
                method = self.path.rstrip("/").split("/")[-1]
                try:
                    result, error = state.dispatch(method, payload)
                except Exception as exc:  # nunca deixa o cliente sem resposta
                    self._reply({"ok": False, "error_code": 500, "description": repr(exc)}, 500)
                    return
                if error is not None:
                    self._reply(
                        {"ok": False, "error_code": 400, "description": error}, status=400
                    )
                else:
                    self._reply({"ok": True, "result": result})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.server.block_on_close = False
        self.port = self.server.server_address[1]
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        return f"http://127.0.0.1:{self.port}/bot"

    def stop(self) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()

    # ------------------------------------------------------------- updates

    def _base_message(self, chat_id: int, message_id: int) -> dict[str, Any]:
        return {
            "message_id": message_id,
            "date": int(time.time()),
            "chat": {"id": chat_id, "type": "private"},
        }

    def push_text(self, chat_id: int, text: str, **extra: Any) -> int:
        with self.cond:
            update_id = self.next_update_id
            self.next_update_id += 1
            message = self._base_message(chat_id, update_id * 10)
            message["from"] = {
                "id": chat_id,
                "is_bot": False,
                "first_name": "Joaki",
            }
            message["text"] = text
            message.update(extra)
            self.updates.append({"update_id": update_id, "message": message})
            self.cond.notify_all()
            return update_id

    def push_callback(self, chat_id: int, data: str, message_id: int) -> int:
        with self.cond:
            update_id = self.next_update_id
            self.next_update_id += 1
            self.updates.append(
                {
                    "update_id": update_id,
                    "callback_query": {
                        "id": str(update_id),
                        "from": {"id": chat_id, "is_bot": False, "first_name": "Joaki"},
                        "chat_instance": "1",
                        "data": data,
                        "message": self._base_message(chat_id, message_id),
                    },
                }
            )
            self.cond.notify_all()
            return update_id

    def _collect(self, offset: int | None, wait: float) -> list[dict[str, Any]]:
        deadline = time.time() + wait
        with self.cond:
            while True:
                pending = [
                    u for u in self.updates if offset is None or u["update_id"] >= offset
                ]
                if pending:
                    return pending
                remaining = deadline - time.time()
                if remaining <= 0:
                    return []
                self.cond.wait(remaining)

    # -------------------------------------------------------------- métodos

    def dispatch(self, method: str, payload: dict[str, Any]) -> tuple[Any, str | None]:
        if method == "getMe":
            return (
                {
                    "id": 1,
                    "is_bot": True,
                    "first_name": "Bridge",
                    "username": "bridge_smoke_bot",
                },
                None,
            )
        if method in ("deleteWebhook", "setMyCommands", "answerCallbackQuery", "sendChatAction"):
            return True, None
        if method == "deleteMessage":
            with self.cond:
                self.sent.append(
                    {"method": method, "payload": payload, "message_id": payload.get("message_id")}
                )
            return True, None
        if method == "getUpdates":
            self.last_getupdates = payload
            offset = payload.get("offset")
            wait = min(float(payload.get("timeout") or 1.0), 1.0)
            return self._collect(offset, wait), None
        if method == "sendMessage":
            with self.cond:
                self.next_message_id += 1
                mid = self.next_message_id
                text = payload.get("text", "")
                self.messages[mid] = text
                self.sent.append({"method": method, "payload": payload, "message_id": mid})
                message = self._base_message(payload["chat_id"], mid)
                message["text"] = text
                if payload.get("reply_markup"):
                    message["reply_markup"] = payload["reply_markup"]
                return message, None
        if method == "editMessageText":
            with self.cond:
                mid = payload["message_id"]
                text = payload.get("text", "")
                if self.messages.get(mid) == text:
                    return None, "Bad Request: message is not modified"
                self.messages[mid] = text
                self.sent.append({"method": method, "payload": payload, "message_id": mid})
                message = self._base_message(payload["chat_id"], mid)
                message["text"] = text
                return message, None
        if method == "sendDocument":
            with self.cond:
                self.next_message_id += 1
                mid = self.next_message_id
                self.sent.append({"method": method, "payload": payload, "message_id": mid})
                message = self._base_message(payload["chat_id"], mid)
                message["document"] = {"file_id": "doc", "file_unique_id": "doc"}
                return message, None
        if method == "getFile":
            return (
                {
                    "file_id": payload.get("file_id"),
                    "file_unique_id": "f",
                    "file_path": "photos/photo.png",
                },
                None,
            )
        return True, None

    # ---------------------------------------------------------------- busca

    def find(self, method: str) -> list[dict[str, Any]]:
        with self.cond:
            return [s for s in self.sent if s["method"] == method]

    def count(self) -> int:
        with self.cond:
            return len(self.sent)

    def wait_for(self, predicate, timeout: float = 180.0, start: int = 0) -> tuple[int, dict[str, Any]]:
        """Aguarda a primeira entrada (a partir de `start`) que case o predicado."""
        deadline = time.time() + timeout
        index = start
        while time.time() < deadline:
            with self.cond:
                snapshot = list(self.sent)
            while index < len(snapshot):
                entry = snapshot[index]
                index += 1
                if predicate(entry):
                    return index, entry
            time.sleep(0.2)
        raise TimeoutError(f"nada casou o predicado em {timeout:.0f}s")
