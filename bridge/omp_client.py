"""NDJSON client for `omp --mode rpc-ui` + restart supervisor."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

PROMPT_TIMEOUT = 3600.0
REQUEST_TIMEOUT = 30.0
# The RPC server limits physical frames to ~1 MiB (v1 `maxFrameBytes`); the
# asyncio default (64 KiB) would break readline on full `message_update` frames.
STREAM_LIMIT = 8 * 1024 * 1024


class OmpError(Exception):
    """Base das falhas do cliente RPC."""


class OmpStartError(OmpError):
    pass


class OmpTimeout(OmpError):
    pass


class OmpProcessDied(OmpError):
    pass


class OmpCommandError(OmpError):
    """Resposta `success: false` do servidor RPC."""


class OmpSupervisorExhausted(OmpError):
    """O filho do omp morreu e não reiniciou dentro do limite."""


class OmpRpcClient:
    def __init__(
        self,
        omp_bin: str,
        cwd: Path,
        sessions_dir: Path,
        approval_mode: str = "write",
        model: str | None = None,
        stderr_path: Path | None = None,
        argv_override: list[str] | None = None,
        stream_limit: int = STREAM_LIMIT,
        extra_args: list[str] | None = None,
    ) -> None:
        self.omp_bin = omp_bin
        self.cwd = cwd
        self.sessions_dir = sessions_dir
        self.approval_mode = approval_mode
        self.model = model
        self.stderr_path = stderr_path
        self._argv_override = argv_override
        self._stream_limit = stream_limit
        self._extra_args = list(extra_args or [])

        self.events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.protocol_version: int | None = None

        self._proc: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._config_task: asyncio.Task[None] | None = None
        self._ready_event = asyncio.Event()
        self._stderr_handle = None
        self._counter = itertools.count(1)
        self._waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._prompt_waiters: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._eof = False

    # ------------------------------------------------------------------ ciclo

    @property
    def argv(self) -> list[str]:
        if self._argv_override is not None:
            return list(self._argv_override)
        argv = [
            self.omp_bin,
            "--mode",
            "rpc-ui",
            "--no-title",
            "--cwd",
            str(self.cwd),
            "--session-dir",
            str(self.sessions_dir),
            "--approval-mode",
            self.approval_mode,
        ]
        if self.model:
            argv += ["--model", self.model]
        argv += self._extra_args
        return argv

    async def start(self) -> None:
        await self._teardown()
        self._eof = False
        self._ready_event = asyncio.Event()
        if self.stderr_path is not None:
            self.stderr_path.parent.mkdir(parents=True, exist_ok=True)
            self._stderr_handle = open(self.stderr_path, "ab", buffering=0)
            stderr_target: Any = self._stderr_handle
        else:
            stderr_target = asyncio.subprocess.DEVNULL

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *self.argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_target,
                cwd=str(self.cwd),
                limit=self._stream_limit,
            )
        except FileNotFoundError as exc:
            raise OmpStartError(
                f"binário do omp não encontrado ({self.omp_bin!r}) — ajuste OMP_BIN no .env"
            ) from exc

        self._reader_task = asyncio.create_task(self._read_loop())
        self._config_task = asyncio.create_task(self._configure_events())
        log.info("omp iniciado: %s (pid %s)", " ".join(self.argv), self._proc.pid)

    async def _teardown(self) -> None:
        """Descarta o processo anterior (leitor, filtro de eventos, stderr)."""
        for task_attr in ("_config_task", "_reader_task"):
            task = getattr(self, task_attr)
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                setattr(self, task_attr, None)
        if self._stderr_handle is not None:
            with contextlib.suppress(Exception):
                self._stderr_handle.close()
            self._stderr_handle = None
        self._proc = None

    async def _configure_events(self) -> None:
        """Pede `message_update` só com deltas: os snapshots completos estouram frames."""
        await self._ready_event.wait()
        try:
            frame = await self.request(
                {"type": "set_event_filter", "events": None, "messageUpdates": "delta"},
                timeout=REQUEST_TIMEOUT,
            )
            log.info("filtro de eventos: %s", frame.get("data"))
        except (OmpError, asyncio.CancelledError) as exc:
            log.info("set_event_filter indisponível (%s); seguindo com snapshots completos", exc)

    async def close(self) -> None:
        proc = self._proc
        if proc is None:
            return
        if proc.stdin is not None and not proc.stdin.is_closing():
            with contextlib.suppress(Exception):
                proc.stdin.close()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        except TimeoutError:
            log.warning("omp não encerrou em 5s; matando")
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
        await self._teardown()

    async def wait(self) -> int:
        """Aguarda o processo filho terminar e devolve o returncode."""
        proc = self._proc
        if proc is None:
            return -1
        return await proc.wait()

    @property
    def running(self) -> bool:
        return (
            self._proc is not None
            and self._proc.returncode is None
            and not self._eof
        )

    # ------------------------------------------------------------------ envio

    async def _write(self, frame: dict[str, Any]) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None or proc.returncode is not None:
            raise OmpProcessDied("processo do omp não está rodando")
        data = json.dumps(frame, ensure_ascii=False) + "\n"
        try:
            proc.stdin.write(data.encode("utf-8"))
            await proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise OmpProcessDied("stdin do omp fechou") from exc

    async def send_raw(self, frame: dict[str, Any]) -> None:
        """Escreve sem esperar resposta (ex.: `extension_ui_response`)."""
        await self._write(frame)

    async def request(self, cmd: dict[str, Any], timeout: float = REQUEST_TIMEOUT) -> dict[str, Any]:
        cmd = dict(cmd)
        req_id = cmd.get("id") or f"req_{next(self._counter)}"
        cmd["id"] = req_id
        if req_id in self._waiters:
            raise OmpError(f"id de request duplicado: {req_id}")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._waiters[req_id] = future
        try:
            await self._write(cmd)
            try:
                frame = await asyncio.wait_for(future, timeout=timeout)
            except TimeoutError as exc:
                raise OmpTimeout(f"timeout ({timeout:.0f}s) esperando resposta de {cmd.get('type')}") from exc
        finally:
            self._waiters.pop(req_id, None)

        if frame.get("success") is False:
            raise OmpCommandError(frame.get("error") or f"comando {cmd.get('type')} falhou")
        return frame

    # ----------------------------------------------------------------- prompt

    async def prompt(
        self,
        text: str,
        *,
        images: list[dict[str, Any]] | None = None,
        streaming_behavior: str | None = None,
    ) -> str:
        """Envia um prompt. Confirma na *admissão*, não no fim do turno."""
        cmd: dict[str, Any] = {"type": "prompt", "message": text}
        if images:
            cmd["images"] = images
        if streaming_behavior:
            cmd["streamingBehavior"] = streaming_behavior

        req_id = f"req_{next(self._counter)}"
        cmd["id"] = req_id
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
        # O app normalmente observa o `prompt_result` pelo stream de eventos; o
        # callback evita "Future exception was never retrieved" em caso de morte
        # do filho sem `wait_prompt`.
        waiter.add_done_callback(lambda f: None if f.cancelled() else f.exception())
        self._prompt_waiters[req_id] = waiter
        self._waiters[req_id] = loop.create_future()

        try:
            await self._write(cmd)
            try:
                await asyncio.wait_for(asyncio.shield(self._waiters[req_id]), timeout=REQUEST_TIMEOUT)
            except TimeoutError as exc:
                self._prompt_waiters.pop(req_id, None)
                raise OmpTimeout("timeout esperando admissão do prompt") from exc
        finally:
            self._waiters.pop(req_id, None)
        return req_id

    async def wait_prompt(self, prompt_id: str, timeout: float = PROMPT_TIMEOUT) -> dict[str, Any]:
        waiter = self._prompt_waiters.get(prompt_id)
        if waiter is None:
            raise OmpError(f"prompt desconhecido: {prompt_id}")
        try:
            return await asyncio.wait_for(asyncio.shield(waiter), timeout=timeout)
        except TimeoutError as exc:
            raise OmpTimeout(f"timeout ({timeout:.0f}s) esperando prompt_result") from exc
        finally:
            self._prompt_waiters.pop(prompt_id, None)

    def forget_prompt(self, prompt_id: str) -> None:
        self._prompt_waiters.pop(prompt_id, None)

    # ----------------------------------------------------------------- leitura

    async def _read_loop(self) -> None:
        proc = self._proc
        assert proc is not None and proc.stdout is not None
        stdout = proc.stdout
        pid = proc.pid
        try:
            while True:
                try:
                    line = await stdout.readline()
                except ValueError:
                    # Frame maior que STREAM_LIMIT: o buffer do StreamReader ainda
                    # guarda a linha inteira, então descarta até o \n e segue.
                    skipped = await self._discard_line(stdout)
                    log.warning(
                        "frame do omp maior que %d bytes descartado (%d bytes)",
                        self._stream_limit,
                        skipped,
                    )
                    continue
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    frame = json.loads(text)
                except json.JSONDecodeError:
                    log.warning("linha não-JSON do omp ignorada: %.200s", text)
                    continue
                if isinstance(frame, dict):
                    self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("erro no leitor do stdout do omp")
        finally:
            self._on_eof(pid)

    @staticmethod
    async def _discard_line(stdout: asyncio.StreamReader) -> int:
        """Consome bytes até (e incluindo) o próximo \n. `read` não aplica o limite."""
        total = 0
        while True:
            chunk = await stdout.read(65536)
            if not chunk:
                return total
            total += len(chunk)
            if b"\n" in chunk:
                return total

    def _dispatch(self, frame: dict[str, Any]) -> None:
        ftype = frame.get("type")
        req_id = frame.get("id")

        if ftype == "ready":
            self.protocol_version = frame.get("protocolVersion")
            log.info(
                "omp pronto: protocolVersion=%s maxFrameBytes=%s",
                frame.get("protocolVersion"),
                frame.get("maxFrameBytes"),
            )
            self._ready_event.set()
            self._emit(frame)
            return

        if ftype == "response":
            waiter = self._waiters.get(req_id) if isinstance(req_id, str) else None
            if waiter is not None and not waiter.done():
                waiter.set_result(frame)
            # Prompt que terminou localmente: nenhum `prompt_result` virá.
            data = frame.get("data")
            if (
                isinstance(req_id, str)
                and isinstance(data, dict)
                and data.get("agentInvoked") is False
                and req_id in self._prompt_waiters
            ):
                synthetic = {
                    "type": "prompt_result",
                    "id": req_id,
                    "agentInvoked": False,
                    "status": "completed" if frame.get("success") else "error",
                    "sessionSettled": True,
                }
                if frame.get("success") is False:
                    synthetic["error"] = {"message": frame.get("error") or "falha no prompt"}
                self._handle_prompt_result(synthetic)
            return

        if ftype == "prompt_result":
            self._handle_prompt_result(frame)
            return

        self._emit(frame)

    def _handle_prompt_result(self, frame: dict[str, Any]) -> None:
        req_id = frame.get("id")
        if isinstance(req_id, str):
            waiter = self._prompt_waiters.get(req_id)
            if waiter is not None and not waiter.done():
                waiter.set_result(frame)
        self._emit(frame)

    def _emit(self, frame: dict[str, Any]) -> None:
        self.events.put_nowait(frame)

    def _on_eof(self, pid: int | None = None) -> None:
        current = self._proc
        if pid is not None and (current is None or current.pid != pid):
            # Leitor de um processo já substituído: não pode derrubar os waiters
            # do processo atual.
            log.info("leitor do omp pid %s terminou (processo já substituído)", pid)
            return
        self._eof = True
        exc = OmpProcessDied("processo do omp terminou")
        for future in list(self._waiters.values()):
            if not future.done():
                future.set_exception(exc)
        self._waiters.clear()
        for future in list(self._prompt_waiters.values()):
            if not future.done():
                future.set_exception(exc)
        self._prompt_waiters.clear()
        log.warning("EOF do stdout do omp (pid %s): waiters pendentes falharam", pid)


async def supervisor(
    client: OmpRpcClient,
    on_restart: Callable[[], Awaitable[None]],
    *,
    max_failures: int = 5,
) -> None:
    """Reinicia o filho do omp quando ele morre (backoff 2,4,8,16,30s)."""
    failures = 0
    delay = 2.0
    while True:
        rc = await client.wait()
        log.warning("omp saiu (returncode=%s)", rc)
        restarted = False
        while failures < max_failures:
            await asyncio.sleep(delay)
            try:
                await client.start()
            except OmpStartError as exc:
                failures += 1
                delay = min(delay * 2, 30.0)
                log.error("falha ao reiniciar omp (%d/%d): %s", failures, max_failures, exc)
                continue
            failures = 0
            delay = 2.0
            restarted = True
            try:
                await on_restart()
            except Exception:
                log.exception("erro no callback on_restart")
            break
        if not restarted:
            log.critical("omp não reiniciou após %d tentativas; encerrando o bridge", max_failures)
            raise OmpSupervisorExhausted(
                f"omp não reiniciou após {max_failures} tentativas"
            )
