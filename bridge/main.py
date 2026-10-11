"""Entrada do bridge: logging, lock de instância única, selftest e run."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import logging.handlers
import os
import sys
from pathlib import Path

from .app import Bridge
from .config import load_config
from .omp_client import OmpRpcClient

log = logging.getLogger("bridge")


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    # O long polling gera uma linha por request; ruído puro.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class InstanceLock:
    """Impede duas instâncias no mesmo token (evita 409 Conflict)."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def acquire(self) -> None:
        pid = _read_pid(self.path)
        if pid is not None and _pid_alive(pid):
            raise SystemExit(
                f"já existe uma instância rodando (pid {pid}) — mate-a antes de iniciar outra"
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(os.getpid()), encoding="utf-8")

    def release(self) -> None:
        with contextlib.suppress(OSError):
            if _read_pid(self.path) == os.getpid():
                self.path.unlink()


def _read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


async def selftest() -> int:
    """Sobe o omp de verdade e faz uma chamada real ao modelo, sem Telegram."""
    config = load_config(require_token=False)
    client = OmpRpcClient(
        omp_bin=config.omp_bin,
        cwd=config.omp_cwd,
        sessions_dir=config.sessions_dir,
        approval_mode=config.approval_mode,
        model=config.model,
        stderr_path=config.omp_stderr_file,
    )
    try:
        await client.start()
    except Exception as exc:
        print(f"SELFTEST FAIL: não consegui iniciar o omp: {exc}")
        return 1

    collected: list[str] = []
    try:
        async with asyncio.timeout(120):
            prompt_id = await client.prompt("Reply with exactly: ok")
            while True:
                frame = await client.events.get()
                if frame.get("type") == "message_update":
                    event = frame.get("assistantMessageEvent") or {}
                    if event.get("type") == "text_delta":
                        collected.append(event.get("delta") or "")
                elif frame.get("type") == "prompt_result" and frame.get("id") == prompt_id:
                    if frame.get("status") != "completed":
                        print(f"SELFTEST FAIL: prompt terminou como {frame.get('status')}")
                        return 1
                    break
    except TimeoutError:
        print("SELFTEST FAIL: timeout de 120s esperando o modelo")
        return 1
    except Exception as exc:
        print(f"SELFTEST FAIL: {exc}")
        return 1
    finally:
        await client.close()

    text = "".join(collected).strip()
    print(f"resposta: {text!r}")
    if "ok" in text.lower():
        print("SELFTEST PASS")
        return 0
    print("SELFTEST FAIL: a resposta não contém 'ok'")
    return 1


async def run() -> int:
    config = load_config()
    setup_logging(config.log_file)

    lock = InstanceLock(config.lock_file)
    lock.acquire()
    try:
        bridge = Bridge(config)
        await bridge.run()
    finally:
        lock.release()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bridge", description="Bot de Telegram para o omp")
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="sobe o omp, faz um prompt real e sai (não usa Telegram)",
    )
    args = parser.parse_args(argv)

    try:
        if args.selftest:
            return asyncio.run(selftest())
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 0
    except SystemExit as exc:
        message = exc.code if isinstance(exc.code, str) else str(exc.code or "")
        if message:
            print(message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
