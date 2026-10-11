"""Smoke dos caminhos de falha (descartável): config, lock e morte do omp."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

from telegram import Bot

from bridge.app import Bridge
from bridge.config import Config
from bridge.main import InstanceLock
from smoke_server import FakeTelegram

ROOT = Path(__file__).resolve().parent
RUN_ID = time.strftime("%Y%m%d-%H%M%S")
WORK = ROOT / "data" / f"smoke-fail-{RUN_ID}"
CHAT = 42
OMP_BIN = "C:/Users/Joaki/AppData/Local/omp/omp.exe"

FAILURES: list[str] = []
CHECKS: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    CHECKS.append(name)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)
    if not ok:
        FAILURES.append(name)


def text_of(entry: dict) -> str:
    return entry["payload"].get("text", "")


def run_bridge_subprocess(cwd: Path, env_extra: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    # O cwd é o diretório de teste (para o `.env`), então o pacote `bridge`
    # precisa vir do PYTHONPATH.
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, "-m", "bridge.main"],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


async def check_omp_death_and_restart() -> None:
    """Mata o filho do omp e verifica aviso + recuperação."""
    root = WORK / "restart"
    cwd = root / "cwd"
    cwd.mkdir(parents=True, exist_ok=True)

    fake = FakeTelegram(user_id=CHAT)
    base_url = fake.start()
    config = Config(
        token="123:SMOKE",
        allowed_user_id=CHAT,
        omp_bin=OMP_BIN,
        omp_cwd=cwd,
        approval_mode="write",
        model=None,
        stream_edit_seconds=0.5,
        max_msg_chars=3800,
        doc_threshold=12000,
        data_dir=root,
    )
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.sessions_dir.mkdir(parents=True, exist_ok=True)

    bridge = Bridge(config)
    bridge.bot = Bot(
        token=config.token,
        base_url=base_url,
        base_file_url=base_url.replace("/bot", "/file/bot"),
    )
    task = asyncio.create_task(bridge.run(), name="bridge")
    try:
        await asyncio.sleep(2.5)
        check("C.1 omp vivo no início", bridge.client.running)

        proc = bridge.client._proc
        assert proc is not None
        proc.kill()
        loop = asyncio.get_running_loop()
        _, notice = await loop.run_in_executor(
            None, lambda: fake.wait_for(lambda e: "reiniciado" in text_of(e), 90, fake.count())
        )
        check("C.2 chat avisado do reinício", "♻️" in text_of(notice), text_of(notice))
        check("C.3 omp voltou a rodar", bridge.client.running)

        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        _, status = await loop.run_in_executor(
            None,
            lambda: fake.wait_for(lambda e: "estado:" in text_of(e), 90, cursor),
        )
        check("C.4 /status funciona após reinício", "modelo:" in text_of(status), text_of(status).replace("\n", " | "))
    finally:
        bridge._closing = True
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        fake.stop()


def main() -> int:
    WORK.mkdir(parents=True, exist_ok=True)

    # A) .env com token vazio
    notoken = WORK / "notoken"
    notoken.mkdir(parents=True, exist_ok=True)
    (notoken / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_BIN=omp\n", encoding="utf-8"
    )
    result = run_bridge_subprocess(notoken)
    check(
        "A.1 token vazio aponta o arquivo e o BotFather",
        result.returncode == 1
        and "TELEGRAM_BOT_TOKEN vazio" in result.stderr
        and "@BotFather" in result.stderr,
        f"rc={result.returncode} stderr={result.stderr.strip()[:120]!r}",
    )

    # A1b) .env inexistente
    noenv = WORK / "noenv"
    noenv.mkdir(parents=True, exist_ok=True)
    result = run_bridge_subprocess(noenv)
    check(
        "A.1b .env inexistente manda copiar o exemplo",
        result.returncode == 1 and ".env.example" in result.stderr,
        f"rc={result.returncode} stderr={result.stderr.strip()[:120]!r}",
    )

    # A2) id de usuário inválido
    badid = WORK / "badid"
    badid.mkdir(parents=True, exist_ok=True)
    (badid / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=abc\n", encoding="utf-8"
    )
    result = run_bridge_subprocess(badid)
    check(
        "A.2 id inválido explica como obter o id",
        result.returncode == 1 and "TELEGRAM_ALLOWED_USER_ID inválido" in result.stderr
        and "/id" in result.stderr,
        f"rc={result.returncode} stderr={result.stderr.strip()[:120]!r}",
    )

    # A3) OMP_CWD inexistente
    badcwd = WORK / "badcwd"
    badcwd.mkdir(parents=True, exist_ok=True)
    (badcwd / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_CWD=Z:/nao/existe\n",
        encoding="utf-8",
    )
    result = run_bridge_subprocess(badcwd)
    check(
        "A.3 OMP_CWD inexistente é recusado",
        result.returncode == 1 and "OMP_CWD não existe" in result.stderr,
        f"rc={result.returncode} stderr={result.stderr.strip()[:120]!r}",
    )

    # A4) token recusado pelo Telegram: mensagem limpa, sem traceback
    badtoken = WORK / "badtoken"
    badtoken.mkdir(parents=True, exist_ok=True)
    (badtoken / ".env").write_text(
        f"TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\n"
        f"OMP_CWD={badtoken}\nDATA_DIR={badtoken}\n",
        encoding="utf-8",
    )
    result = run_bridge_subprocess(badtoken)
    check(
        "A.4 token inválido falha com mensagem clara",
        result.returncode == 1
        and "TELEGRAM_BOT_TOKEN inválido" in result.stderr
        and "Traceback" not in result.stderr,
        f"rc={result.returncode} stderr={result.stderr.strip()[:140]!r}",
    )

    # B) segunda instância: lock do próprio processo já segura o arquivo
    lockdir = WORK / "lock"
    lockdir.mkdir(parents=True, exist_ok=True)
    lock = InstanceLock(lockdir / "bridge.lock")
    lock.acquire()
    try:
        (lockdir / ".env").write_text(
            f"TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\n"
            f"OMP_CWD={lockdir}\nDATA_DIR={lockdir}\n",
            encoding="utf-8",
        )
        result = run_bridge_subprocess(lockdir)
        check(
            "B.1 segunda instância recusada com o pid",
            result.returncode == 1
            and f"já existe uma instância rodando (pid {os.getpid()})" in result.stderr,
            f"rc={result.returncode} stderr={result.stderr.strip()[:160]!r}",
        )
    finally:
        lock.release()

    # B2) lock órfão (pid morto) é reaproveitado
    stale = lockdir / "bridge.lock"
    stale.write_text("999999", encoding="utf-8")
    lock2 = InstanceLock(stale)
    try:
        lock2.acquire()
        check("B.2 lock órfão é assumido", stale.read_text(encoding="utf-8") == str(os.getpid()))
    except SystemExit as exc:
        check("B.2 lock órfão é assumido", False, str(exc))
    finally:
        lock2.release()

    # C) morte do omp
    asyncio.run(check_omp_death_and_restart())

    print()
    print(f"{len(CHECKS) - len(FAILURES)}/{len(CHECKS)} checks passaram")
    if FAILURES:
        print("FALHAS: " + ", ".join(FAILURES))
        return 1
    print("SMOKE FAILURES PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
