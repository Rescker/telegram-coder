"""Smoke test ponta a ponta do bridge contra um Telegram falso (descartável).

Roda o bridge de verdade, com o omp de verdade, e injeta updates na Bot API
falsa. Cobre: stream editado, aprovação por botão (Approve/Deny), steer,
/status, /model, /thinking, /new, /name, /sessions + troca, foto, documento,
allowlist e persistência de offset.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from telegram import Bot

from bridge.app import Bridge
from bridge.config import Config
from smoke_server import FakeTelegram

ROOT = Path(__file__).resolve().parent
RUN_ID = time.strftime("%Y%m%d-%H%M%S")
SMOKE = ROOT / "data" / f"smoke-{RUN_ID}"
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


def has_button(entry: dict) -> bool:
    markup = entry["payload"].get("reply_markup") or {}
    return bool(markup.get("inline_keyboard"))


def button_data(entry: dict) -> list[str]:
    markup = entry["payload"].get("reply_markup") or {}
    return [b.get("callback_data", "") for row in markup.get("inline_keyboard", []) for b in row]


def valid_telegram_html(html: str) -> None:
    """Valida como o Telegram recebe: `<blockquote expandable>` não é XML válido."""
    import re as _re

    normalized = _re.sub(r"<blockquote expandable>", '<blockquote expandable="1">', html)
    ET.fromstring(f"<r>{normalized}</r>")


def all_text(fake: FakeTelegram) -> str:
    return "\n".join(
        text_of(e) for e in fake.find("sendMessage") + fake.find("editMessageText")
    )


def line_value(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip()
    return ""


async def wait(fake: FakeTelegram, predicate, cursor: int, timeout: float):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fake.wait_for(predicate, timeout, cursor))


async def wait_for_streaming(bridge: Bridge, timeout: float = 60.0) -> bool:
    """Espera o omp reportar `isStreaming` (a leitura é feita pelo próprio bridge)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            state = await bridge._state()
        except Exception:
            state = {}
        if state.get("isStreaming"):
            return True
        await asyncio.sleep(0.5)
    return False


class Approvals:
    """Responde automaticamente pedidos de aprovação enquanto o turno roda.

    O agente pode pedir `bash` várias vezes dentro do mesmo turno; sem isso o
    smoke ficaria esperando para sempre. Fica desligado nos passos que testam
    o botão na mão (Approve/Deny).
    """

    def __init__(self, fake: FakeTelegram) -> None:
        self.fake = fake
        self.enabled = False
        self.answered: set[int] = set()

    async def pump(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            if not self.enabled:
                continue
            for entry in self.fake.find("sendMessage"):
                if entry["message_id"] in self.answered:
                    continue
                markup = entry["payload"].get("reply_markup") or {}
                rows = markup.get("inline_keyboard") or []
                if not rows:
                    continue
                approve = rows[0][0].get("callback_data", "")
                if not approve.startswith("ui:"):
                    continue
                self.answered.add(entry["message_id"])
                self.fake.push_callback(CHAT, approve, entry["message_id"])


async def wait_footer(fake: FakeTelegram, cursor: int, timeout: float) -> tuple[int, dict[str, Any]]:
    return await wait(fake, lambda e: "pronto em" in text_of(e), cursor, timeout)

async def monitor_loop(fake: FakeTelegram, bridge: Bridge, task: asyncio.Task) -> None:
    """Imprime o estado do bridge a cada 20s para diagnóstico (stderr, fora do stdout)."""
    while True:
        await asyncio.sleep(20)
        sent = fake.find("sendMessage")
        edits = fake.find("editMessageText")
        streamer = bridge.streamer
        state = "?"
        if streamer is not None:
            state = f"buf={len(streamer.text_buf)} ativo={streamer.active_msg_id} closed={streamer.closed}"
        print(
            f"  … {time.strftime('%H:%M:%S')} sends={len(sent)} edits={len(edits)} "
            f"omp_running={bridge.client.running} streamer={state} task_done={task.done()}",
            flush=True,
            file=sys.stderr,
        )
        if task.done() and not task.cancelled():
            print(f"  !! bridge terminou: {task.exception()!r}", flush=True, file=sys.stderr)


async def main() -> int:
    cwd = SMOKE / "cwd"
    cwd.mkdir(parents=True, exist_ok=True)
    print(f"run dir: {SMOKE}", flush=True)

    fake = FakeTelegram(user_id=CHAT)
    base_url = fake.start()
    print(f"fake telegram: {base_url}", flush=True)

    config = Config(
        token="123:SMOKE",
        allowed_user_id=CHAT,
        omp_bin=OMP_BIN,
        omp_cwd=cwd,
        approval_mode="write",
        model=None,
        stream_edit_seconds=0.5,
        max_msg_chars=3800,
        doc_threshold=400,
        data_dir=SMOKE,
    )
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.sessions_dir.mkdir(parents=True, exist_ok=True)

    bridge = Bridge(config)
    bridge.bot = Bot(
        token=config.token,
        base_url=base_url,
        base_file_url=base_url.replace("/bot", "/file/bot"),
    )
    from bridge.main import setup_logging

    setup_logging(config.log_file)

    task = asyncio.create_task(bridge.run(), name="bridge")
    monitor = asyncio.create_task(monitor_loop(fake, bridge, task), name="monitor")
    approvals = Approvals(fake)
    approvals_task = asyncio.create_task(approvals.pump(), name="approvals")
    try:
        await asyncio.sleep(2.0)

        # ---------------------------------------------------- 1) write + bash
        cursor = fake.count()
        fake.push_text(
            CHAT,
            'crie hello.py com exatamente print("hi") usando a ferramenta write '
            "e depois rode esse arquivo com a ferramenta bash (python hello.py)",
        )
        cursor, approval = await wait(fake, has_button, cursor, 300)
        options = button_data(approval)
        check(
            "1.1 aprovação chega como botões",
            len(options) >= 2 and options[0].startswith("ui:"),
            f"{options[:2]} · {text_of(approval)[:70]!r}",
        )
        check(
            "1.2 título cita bash",
            "bash" in text_of(approval).lower(),
            text_of(approval)[:90].replace("\n", " "),
        )
        check(
            "1.3 atividade existe",
            bool([e for e in fake.find("sendMessage") if "atividade" in text_of(e)]),
            "⚙️ atividade",
        )
        check("1.4 Approve é o primeiro botão", options[0].endswith(":0"), options[0])

        fake.push_callback(CHAT, options[0], approval["message_id"])
        cursor, footer = await wait(fake, lambda e: "pronto em" in text_of(e), cursor, 300)
        check("1.5 rodapé final", "✅" in text_of(footer), text_of(footer)[-90:].replace("\n", " "))

        deleted = [e["message_id"] for e in fake.find("deleteMessage")]
        check(
            "1.5b mensagem do botão apagada ao aprovar",
            approval["message_id"] in deleted,
            f"apagadas={deleted}",
        )
        rich = [
            e
            for e in fake.find("editMessageText")
            if (e["payload"].get("parse_mode") or "") == "HTML"
        ]
        check(
            "1.5d resposta final renderizada em HTML",
            bool(rich),
            f"{len(rich)} edição(ões) com parse_mode=HTML",
        )
        malformed = []
        for entry in rich:
            try:
                valid_telegram_html(entry["payload"].get("text", ""))
            except ET.ParseError as exc:
                malformed.append(f"{entry['message_id']}: {exc}")
        check(
            "1.5e HTML final bem-formado (o Telegram recusaria entidades inválidas)",
            not malformed,
            "; ".join(malformed) if malformed else f"{len(rich)} validado(s)",
        )
        summaries = [
            text_of(e)
            for e in fake.find("editMessageText")
            if text_of(e).startswith("⚙️") and "\n" not in text_of(e)
        ]
        check(
            "1.5c atividade colapsada em uma linha no fim do turno",
            bool(summaries) and "×" in summaries[-1],
            summaries[-1] if summaries else "nenhum resumo",
        )

        hello = cwd / "hello.py"
        check("1.6 hello.py criado", hello.exists(), str(hello))
        if hello.exists():
            check("1.7 conteúdo correto", 'print("hi")' in hello.read_text(encoding="utf-8"))

        after = all_text(fake)
        activity_texts = [
            text_of(e)
            for e in fake.find("sendMessage") + fake.find("editMessageText")
            if text_of(e).startswith("⚙️")
        ]
        joined_activity = "\n".join(activity_texts)
        check(
            "1.8 atividade registra write e bash (ao vivo ou no resumo)",
            "write" in joined_activity and "bash" in joined_activity,
            next(
                (t for t in reversed(activity_texts) if "×" in t),
                activity_texts[-1] if activity_texts else "nenhuma",
            ),
        )
        check("1.9 saída 'hi' no stream", "hi" in after)
        print(f"      (ferramentas vistas: {sorted(set(w for w in ('write', 'edit', 'bash', 'read') if f'🔧 {w}' in after))})", flush=True)

        footer_edits = [e for e in fake.find("editMessageText") if "pronto em" in text_of(e)]
        check(
            "1.10 rodapé aplicado por edição numa única mensagem",
            len(footer_edits) >= 1 and len({e["message_id"] for e in footer_edits}) == 1,
            f"ids={sorted({e['message_id'] for e in footer_edits})}",
        )
        # Invariante central do streamer: a resposta do turno é criada por UM
        # send_message e depois só editada (nunca uma mensagem por delta).
        answer_ids = {e["message_id"] for e in footer_edits}
        sends_for_answer = [
            e for e in fake.find("sendMessage") if e["message_id"] in answer_ids
        ]
        check(
            "1.11 resposta do turno criada uma vez e só editada",
            len(sends_for_answer) == 1,
            f"sends={len(sends_for_answer)} answer_ids={sorted(answer_ids)}",
        )
        check(
            "1.12 stream não criou mensagem por delta",
            len(fake.find("sendMessage")) <= 3,
            f"{len(fake.find('sendMessage'))} sendMessage no turno",
        )

        # ---------------------------------------------------------- 2) Deny
        cursor = fake.count()
        fake.push_text(CHAT, "rode bash: echo negado-agora")
        cursor, approval2 = await wait(fake, has_button, cursor, 300)
        deny = [d for d in button_data(approval2) if d.endswith(":1")]
        check("2.1 botão Deny existe", bool(deny), str(button_data(approval2)))
        fake.push_callback(CHAT, deny[0], approval2["message_id"])
        cursor, _ = await wait(fake, lambda e: "pronto em" in text_of(e), cursor, 300)
        after_deny = all_text(fake)
        check(
            "2.2 negação aparece (linha ✗ ao vivo ou contagem de falhas no resumo)",
            "✗ bash" in after_deny or "falha(s)" in after_deny,
            next(
                (
                    line
                    for line in after_deny.splitlines()
                    if "falha(s)" in line or line.startswith("✗")
                ),
                "nenhuma indicação",
            ),
        )
        check(
            "2.3 agente menciona a negação",
            "denied" in after_deny.lower() or "negad" in after_deny.lower(),
        )

        # --------------------------------------------------------- 3) status
        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        cursor, status = await wait(fake, lambda e: "estado:" in text_of(e), cursor, 60)
        st = text_of(status)
        check(
            "3.1 /status completo",
            all(k in st for k in ("modelo:", "sessão:", "contexto:", "estado:")),
            st.replace("\n", " | "),
        )

        # ------------------------------------------------------- 4) thinking
        cursor = fake.count()
        fake.push_text(CHAT, "/thinking")
        cursor, th = await wait(fake, lambda e: "thinking atual" in text_of(e), cursor, 60)
        levels = line_value(text_of(th), "disponíveis:").split(", ")
        check("4.1 /thinking lista níveis", len(levels) >= 2, text_of(th).replace("\n", " | "))

        target_level = levels[-1]
        cursor = fake.count()
        fake.push_text(CHAT, f"/thinking {target_level}")
        cursor, th2 = await wait(fake, lambda e: "🧠" in text_of(e), cursor, 60)
        check("4.2 nível aplicado", target_level in text_of(th2), text_of(th2))

        # ------------------------------------------------------------ 5) steer
        # Enquanto uma aprovação está pendente o turno está garantidamente vivo,
        # então o texto seguinte tem que entrar como steer.
        cursor = fake.count()
        fake.push_text(CHAT, "rode bash: ping -n 10 127.0.0.1")
        cursor, approval3 = await wait(fake, has_button, cursor, 300)
        check("5.1 aprovação pendente mantém o turno vivo", await wait_for_streaming(bridge, 30))

        cursor = fake.count()
        streamer_before = bridge.streamer
        fake.push_text(CHAT, "troque para oi")
        cursor, steer = await wait(fake, lambda e: "steer enviado" in text_of(e), cursor, 90)
        check("5.2 steer confirmado", "↩︎" in text_of(steer), text_of(steer))
        check(
            "5.3 steer reusou o streamer do turno em curso",
            bridge.streamer is streamer_before and streamer_before is not None,
            f"antes={id(streamer_before)} depois={id(bridge.streamer)}",
        )

        fake.push_callback(CHAT, button_data(approval3)[0], approval3["message_id"])
        approvals.enabled = True
        cursor, _ = await wait_footer(fake, cursor, 300)
        check("5.4 turno com steer concluído", True)
        approvals.enabled = False

        # ----------------------------------------------------------- 6) /model
        cursor = fake.count()
        fake.push_text(CHAT, "/model flash")
        cursor, models_msg = await wait(
            fake, lambda e: has_button(e) and "modelo atual" in text_of(e), cursor, 120
        )
        data = button_data(models_msg)
        check("6.1 teclado de modelos", bool(data) and data[0].startswith("mdl:"), str(data[:3]))

        cursor = fake.count()
        fake.push_callback(CHAT, data[0], models_msg["message_id"])
        cursor, chosen = await wait(fake, lambda e: "modelo:" in text_of(e), cursor, 120)
        check("6.2 troca de modelo", text_of(chosen).startswith("modelo:"), text_of(chosen))

        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        cursor, st_after = await wait(fake, lambda e: "estado:" in text_of(e), cursor, 60)
        check(
            "6.3 /status reflete o modelo",
            line_value(text_of(st_after), "modelo:") == line_value(text_of(chosen), "modelo:"),
            f"{line_value(text_of(st_after), 'modelo:')} vs {line_value(text_of(chosen), 'modelo:')}",
        )

        # --------------------------------------------- 7) /new, /name, /sessions
        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        cursor, st0 = await wait(fake, lambda e: "estado:" in text_of(e), cursor, 60)
        old_session = line_value(text_of(st0), "sessão:")
        check("7.1 sessão inicial registrada", bool(old_session), old_session)

        cursor = fake.count()
        fake.push_text(CHAT, "/new")
        cursor, newmsg = await wait(fake, lambda e: "nova sessão" in text_of(e), cursor, 120)
        new_session = text_of(newmsg).split(":", 1)[1].strip()
        check("7.2 /new cria outra sessão", new_session != old_session, f"{old_session} -> {new_session}")

        cursor = fake.count()
        fake.push_text(CHAT, "/name teste")
        cursor, named = await wait(fake, lambda e: "nomeada" in text_of(e), cursor, 60)
        check("7.3 /name aplicado", "teste" in text_of(named), text_of(named))

        cursor = fake.count()
        fake.push_text(CHAT, "/sessions")
        cursor, sessions = await wait(fake, lambda e: "sessões" in text_of(e), cursor, 60)
        stext = text_of(sessions)
        check("7.4 /sessions lista teste com ✅", "teste" in stext and "✅" in stext, stext.replace("\n", " | "))
        ses_data = button_data(sessions)
        check("7.5 teclado de sessões", bool(ses_data) and ses_data[0].startswith("ses:"), str(ses_data))

        cursor = fake.count()
        fake.push_callback(CHAT, ses_data[-1], sessions["message_id"])
        cursor, switched = await wait(fake, lambda e: "sessão trocada" in text_of(e), cursor, 120)
        check("7.6 troca de sessão", "sessão trocada" in text_of(switched), text_of(switched))

        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        cursor, st_back = await wait(fake, lambda e: "estado:" in text_of(e), cursor, 60)
        check(
            "7.7 voltou para a sessão antiga",
            line_value(text_of(st_back), "sessão:") == old_session,
            f"{line_value(text_of(st_back), 'sessão:')} vs {old_session}",
        )

        # ------------------------------------------------------------ 8) /ask
        cursor = fake.count()
        fake.push_text(CHAT, "/ask")
        cursor, askmsg = await wait(fake, lambda e: "ASK ligado" in text_of(e), cursor, 120)
        check("8.1 /ask liga o modo", "ASK ligado" in text_of(askmsg), text_of(askmsg).splitlines()[0])
        check(
            "8.2 cliente ask usa ferramentas só de leitura",
            bridge.ask_client is not None
            and any(a.startswith("--tools=") for a in bridge.ask_client.argv),
            next(
                (a for a in (bridge.ask_client.argv if bridge.ask_client else []) if a.startswith("--tools=")),
                "sem --tools",
            ),
        )

        cursor = fake.count()
        fake.push_text(CHAT, "crie o arquivo invadido.txt com o texto oi")
        cursor, _ = await wait_footer(fake, cursor, 300)
        check(
            "8.3 ask não cria arquivo nenhum",
            not (cwd / "invadido.txt").exists(),
            f"{cwd}",
        )

        cursor = fake.count()
        fake.push_text(CHAT, "/status")
        cursor, st_ask = await wait(fake, lambda e: "modo:" in text_of(e), cursor, 60)
        check("8.4 /status mostra o modo ask", "ask" in line_value(text_of(st_ask), "modo:"), line_value(text_of(st_ask), "modo:"))

        cursor = fake.count()
        fake.push_text(CHAT, "/work")
        cursor, workmsg = await wait(fake, lambda e: "modo normal" in text_of(e), cursor, 60)
        check("8.5 /work volta ao normal", not bridge.ask_mode, text_of(workmsg))

        # ----------------------------------------------------------- 9) /stop
        approvals.enabled = True
        cursor = fake.count()
        fake.push_text(CHAT, "rode bash: ping -n 20 127.0.0.1")
        cursor, approval4 = await wait(fake, has_button, cursor, 300)
        fake.push_callback(CHAT, button_data(approval4)[0], approval4["message_id"])
        await wait_for_streaming(bridge, 30)
        cursor = fake.count()
        fake.push_text(CHAT, "/stop")
        cursor, stopped = await wait(fake, lambda e: "abortado" in text_of(e), cursor, 60)
        check("8.1 /stop confirma o abort", "🛑" in text_of(stopped), text_of(stopped))
        check("8.2 turno encerrado após /stop", not await wait_for_streaming(bridge, 30))
        approvals.enabled = False

        # ------------------------------------------------------- 10) documento
        approvals.enabled = True
        cursor = fake.count()
        doc = "relatório de smoke\n" + ("linha de teste\n" * 40)
        fake.push_text(
            CHAT,
            "",
            document={
                "file_id": "doc1",
                "file_unique_id": "doc1",
                "file_name": "relatorio.md",
                "mime_type": "text/markdown",
                "file_size": len(doc.encode()),
            },
            caption="resuma o arquivo em uma linha",
        )
        cursor, _ = await wait_footer(fake, cursor, 300)
        check("10.1 documento processado (turno concluído)", True)
        approvals.enabled = False

        # ----------------------------------------------------------- 11) foto
        approvals.enabled = True
        cursor = fake.count()
        fake.push_text(
            CHAT,
            "",
            photo=[
                {
                    "file_id": "p1",
                    "file_unique_id": "p1",
                    "width": 1,
                    "height": 1,
                    "file_size": 68,
                }
            ],
            caption="o que é esta imagem?",
        )
        cursor, _ = await wait_footer(fake, cursor, 300)
        check("11.1 foto processada (turno concluído)", True)
        approvals.enabled = False

        # --------------------------------------------------- 12) allowlist
        with fake.cond:
            fake.updates.append(
                {
                    "update_id": fake.next_update_id,
                    "message": {
                        "message_id": 999999,
                        "date": int(time.time()),
                        "chat": {"id": 777, "type": "private"},
                        "from": {"id": 777, "is_bot": False, "first_name": "Intruso"},
                        "text": "me deixe entrar",
                    },
                }
            )
            fake.next_update_id += 1
            fake.cond.notify_all()
        cursor = fake.count()
        cursor, denied = await wait(fake, lambda e: "não autorizado" in text_of(e), cursor, 60)
        check("12.1 usuário não autorizado recusado", "não autorizado" in text_of(denied))

        # ----------------------------------------------------- 13) offset
        check("13.1 arquivo de offset gravado", config.offset_file.exists())
        if config.offset_file.exists():
            value = json.loads(config.offset_file.read_text(encoding="utf-8")).get("offset")
            check(
                "13.2 offset persistido após tratar",
                isinstance(value, int) and value >= fake.next_update_id - 1,
                f"offset={value} last={fake.next_update_id - 1}",
            )
    finally:
        bridge._closing = True
        proc = bridge.client._proc
        monitor.cancel()
        approvals_task.cancel()
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        if proc is not None and proc.returncode is None:
            proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
        fake.stop()

    print()
    print(f"{len(CHECKS) - len(FAILURES)}/{len(CHECKS)} checks passaram")
    if FAILURES:
        print("FALHAS: " + ", ".join(FAILURES))
        return 1
    print("SMOKE PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
