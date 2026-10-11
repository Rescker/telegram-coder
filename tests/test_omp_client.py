"""Testes do cliente NDJSON do omp com um filho falso."""

from __future__ import annotations

import asyncio

import sys
from pathlib import Path

import pytest

from bridge.omp_client import (
    OmpCommandError,
    OmpProcessDied,
    OmpRpcClient,
)

FAKE_SERVER = r"""
import json
import sys

pending = {}


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def maybe_pair():
    if "hold" in pending and "go" in pending:
        send({"id": pending["go"]["id"], "type": "response", "command": "go", "success": True})
        send({"id": pending["hold"]["id"], "type": "response", "command": "hold", "success": True})


send({"type": "ready", "protocolVersion": 1, "maxFrameBytes": 1048576})

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    cmd = json.loads(line)
    ctype = cmd.get("type")
    cid = cmd.get("id")

    if ctype in ("hold", "go"):
        pending[ctype] = cmd
        maybe_pair()
        continue
    if ctype == "prompt":
        if cmd.get("message") == "local_prompt":
            send({"id": cid, "type": "response", "command": "prompt", "success": True,
                  "data": {"agentInvoked": False}})
            continue
        send({"id": cid, "type": "response", "command": "prompt", "success": True})
        send({"type": "agent_start"})
        send({"type": "message_update", "messageId": "m1",
              "assistantMessageEvent": {"type": "text_delta", "delta": "oi"}})
        send({"type": "agent_end", "messages": [], "isTerminal": True, "yielded": True})
        send({"type": "prompt_result", "id": cid, "agentInvoked": True,
              "status": "completed", "sessionSettled": True})
        send({"type": "session_settled"})
        continue
    if ctype == "set_event_filter":
        send({"id": cid, "type": "response", "command": ctype, "success": True,
              "data": {"events": cmd.get("events"), "messageUpdates": cmd.get("messageUpdates")}})
        send({"type": "notice", "message": f"filter={cmd.get('messageUpdates')}"})
        continue
    if ctype == "fail":
        send({"id": cid, "type": "response", "command": "fail", "success": False,
              "error": "nope"})
        continue
    if ctype == "boom":
        sys.exit(0)
    send({"id": cid, "type": "response", "command": ctype, "success": True})
"""


def make_client(tmp_path: Path) -> OmpRpcClient:
    return OmpRpcClient(
        omp_bin="omp",
        cwd=tmp_path,
        sessions_dir=tmp_path / "sessions",
        stderr_path=tmp_path / "omp-stderr.log",
        argv_override=[sys.executable, "-u", "-c", FAKE_SERVER],
    )


async def _drain_until(client: OmpRpcClient, ftype: str, limit: float = 5.0) -> dict:
    async with asyncio.timeout(limit):
        while True:
            frame = await client.events.get()
            if frame.get("type") == ftype:
                return frame


def test_request_correlates_out_of_order_responses(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            ready = await _drain_until(client, "ready")
            assert ready["protocolVersion"] == 1

            first = asyncio.create_task(client.request({"type": "hold"}))
            await asyncio.sleep(0.2)
            second = asyncio.create_task(client.request({"type": "go"}))

            hold_frame, go_frame = await asyncio.wait_for(
                asyncio.gather(first, second), timeout=5
            )
            # o servidor respondeu `go` antes de `hold`; cada request recebeu o seu.
            assert hold_frame["command"] == "hold"
            assert go_frame["command"] == "go"
            assert hold_frame["id"] != go_frame["id"]
        finally:
            await client.close()

    asyncio.run(scenario())


def test_prompt_result_resolves_wait_prompt(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            prompt_id = await client.prompt("diga oi")
            result = await client.wait_prompt(prompt_id, timeout=5)
            assert result["status"] == "completed"
            assert result["agentInvoked"] is True
            assert result["id"] == prompt_id

            update = await _drain_until(client, "message_update")
            assert update["assistantMessageEvent"]["delta"] == "oi"
            await _drain_until(client, "session_settled")
        finally:
            await client.close()

    asyncio.run(scenario())


def test_local_prompt_synthesizes_prompt_result(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            prompt_id = await client.prompt("local_prompt")
            result = await client.wait_prompt(prompt_id, timeout=5)
            assert result["status"] == "completed"
            assert result["agentInvoked"] is False
        finally:
            await client.close()

    asyncio.run(scenario())


def test_command_error_raises(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            with pytest.raises(OmpCommandError, match="nope"):
                await client.request({"type": "fail"})
        finally:
            await client.close()

    asyncio.run(scenario())


def test_eof_fails_pending_waiters(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            pending = asyncio.create_task(client.request({"type": "hold"}))
            await asyncio.sleep(0.3)
            await client.send_raw({"type": "boom"})

            with pytest.raises(OmpProcessDied):
                await asyncio.wait_for(pending, timeout=5)
            assert client.running is False
        finally:
            await client.close()

    asyncio.run(scenario())


def test_malformed_line_is_skipped(tmp_path: Path) -> None:
    script = (
        "import sys, json\n"
        "sys.stdout.write('not json at all\\n')\n"
        "sys.stdout.flush()\n"
        "line = sys.stdin.readline()\n"
        "cmd = json.loads(line)\n"
        "sys.stdout.write(json.dumps({'id': cmd['id'], 'type': 'response',"
        " 'command': 'ping', 'success': True}) + '\\n')\n"
        "sys.stdout.flush()\n"
        "sys.stdin.readline()\n"
    )

    async def scenario() -> None:
        client = OmpRpcClient(
            omp_bin="omp",
            cwd=tmp_path,
            sessions_dir=tmp_path / "sessions",
            argv_override=[sys.executable, "-u", "-c", script],
        )
        await client.start()
        try:
            frame = await client.request({"type": "ping"}, timeout=5)
            assert frame["command"] == "ping"
        finally:
            await client.close()

    asyncio.run(scenario())


def test_restart_after_kill_keeps_requests_working(tmp_path: Path) -> None:
    """Reiniciar o filho não pode ser derrubado pelo leitor do processo antigo."""

    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            await _drain_until(client, "ready")
            assert client._proc is not None
            first_pid = client._proc.pid
            client._proc.kill()
            assert await client.wait() is not None
            await asyncio.sleep(0.2)

            await client.start()
            try:
                assert client._proc is not None
                assert client._proc.pid != first_pid
                frame = await client.request({"type": "ping"}, timeout=10)
                assert frame["command"] == "ping"
                assert client.running is True
            finally:
                await client.close()
        finally:
            await client.close()

    asyncio.run(scenario())


def test_start_missing_binary(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = OmpRpcClient(
            omp_bin="definitely-not-here-xyz",
            cwd=tmp_path,
            sessions_dir=tmp_path / "sessions",
        )
        from bridge.omp_client import OmpStartError

        with pytest.raises(OmpStartError, match="OMP_BIN"):
            await client.start()

    asyncio.run(scenario())


def test_oversized_line_is_skipped_and_stream_survives(tmp_path: Path) -> None:
    """Um frame maior que o limite do StreamReader não pode matar o leitor."""
    script = (
        "import sys, json\n"
        "sys.stdout.write(json.dumps({'type': 'ready', 'protocolVersion': 1}) + '\\n')\n"
        "sys.stdout.write(json.dumps({'type': 'message_update', 'blob': 'x' * 500}) + '\\n')\n"
        "sys.stdout.flush()\n"
        "for line in sys.stdin:\n"
        "    cmd = json.loads(line)\n"
        "    sys.stdout.write(json.dumps({'id': cmd['id'], 'type': 'response',"
        " 'command': 'ping', 'success': True}) + '\\n')\n"
        "    sys.stdout.flush()\n"
    )

    async def scenario() -> None:
        client = OmpRpcClient(
            omp_bin="omp",
            cwd=tmp_path,
            sessions_dir=tmp_path / "sessions",
            argv_override=[sys.executable, "-u", "-c", script],
            stream_limit=256,
        )
        await client.start()
        try:
            await _drain_until(client, "ready")
            await asyncio.sleep(0.3)
            # O frame gigante foi descartado, mas o cliente continua vivo.
            assert client.running is True
            frame = await client.request({"type": "ping"}, timeout=5)
            assert frame["command"] == "ping"
        finally:
            await client.close()

    asyncio.run(scenario())


def test_delta_event_filter_is_requested(tmp_path: Path) -> None:
    async def scenario() -> None:
        client = make_client(tmp_path)
        await client.start()
        try:
            notice = await _drain_until(client, "notice")
            assert notice["message"] == "filter=delta"
        finally:
            await client.close()

    asyncio.run(scenario())


class StubClient:
    """Cliente falso para exercitar o supervisor sem subir processo."""

    def __init__(self, starts: int = 10) -> None:
        self.starts = 0
        self.remaining = starts
        self.waits = 0

    async def wait(self) -> int:
        self.waits += 1
        return 1

    async def start(self) -> None:
        self.starts += 1
        if self.remaining <= 0:
            from bridge.omp_client import OmpStartError

            raise OmpStartError("não subiu")
        self.remaining -= 1


def test_supervisor_restarts_and_notifies() -> None:
    async def scenario() -> None:
        from bridge.omp_client import supervisor

        client = StubClient(starts=1)
        restarts: list[int] = []

        async def on_restart() -> None:
            restarts.append(client.starts)
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await supervisor(client, on_restart, max_failures=5)

        assert client.starts == 1
        assert restarts == [1]

    asyncio.run(scenario())


def test_supervisor_gives_up_after_max_failures() -> None:
    async def scenario() -> None:
        from bridge.omp_client import OmpSupervisorExhausted, supervisor

        client = StubClient(starts=0)

        async def on_restart() -> None:
            raise AssertionError("não deveria reiniciar")

        with pytest.raises(OmpSupervisorExhausted):
            await supervisor(client, on_restart, max_failures=1)

        assert client.starts == 1

    asyncio.run(scenario())
