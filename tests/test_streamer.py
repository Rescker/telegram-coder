"""Testes do renderizador de stream com um bot falso."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Any

import pytest
from telegram import InputFile
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter

from bridge.streamer import TurnStreamer


@dataclass
class FakeMessage:
    message_id: int


@dataclass
class FakeBot:
    calls: list[tuple[str, tuple, dict]] = field(default_factory=list)
    # (método, message_id, texto, kwargs) para correlacionar envios e edições.
    written: list[tuple[str, int, str, dict]] = field(default_factory=list)
    edits_before_success: int = 0
    edit_error: Exception | None = None
    _next_id: int = 100

    def _record(self, name: str, args: tuple, kwargs: dict) -> FakeMessage:
        self.calls.append((name, args, kwargs))
        # Edições referenciam uma mensagem existente; só envios criam id novo.
        message_id = kwargs.get("message_id")
        if message_id is None:
            self._next_id += 1
            message_id = self._next_id
        text = kwargs.get("text") or (args[1] if name == "send_message" else args[0])
        self.written.append((name, message_id, text, kwargs))
        return FakeMessage(message_id)

    async def send_message(self, chat_id: int, text: str, **kwargs: Any) -> FakeMessage:
        return self._record("send_message", (chat_id, text), kwargs)

    async def edit_message_text(self, text: str, **kwargs: Any) -> FakeMessage:
        if self.edit_error is not None:
            error = self.edit_error
            self.edit_error = None
            self._record("edit_message_text_raised", (text,), kwargs)
            raise error
        if self.edits_before_success > 0:
            self.edits_before_success -= 1
            self._record("edit_message_text_raised", (text,), kwargs)
            raise RetryAfter(3)
        return self._record("edit_message_text", (text,), kwargs)

    async def send_chat_action(self, chat_id: int, action: Any) -> bool:
        self._record("send_chat_action", (chat_id, action), {})
        return True

    async def send_document(self, chat_id: int, document: Any, **kwargs: Any) -> FakeMessage:
        return self._record("send_document", (chat_id, document), kwargs)

    def names(self) -> list[str]:
        return [name for name, _, _ in self.calls]

    def texts(self, name: str) -> list[str]:
        return [args[1] if len(args) > 1 else args[0] for n, args, _ in self.calls if n == name]


@dataclass
class FakeConfig:
    stream_edit_seconds: float = 2.0
    max_msg_chars: int = 3800
    doc_threshold: int = 12000


def delta(text: str) -> dict[str, Any]:
    return {
        "type": "message_update",
        "messageId": "m1",
        "assistantMessageEvent": {"type": "text_delta", "delta": text},
    }


def make_streamer(bot: FakeBot, config: FakeConfig | None = None) -> TurnStreamer:
    return TurnStreamer(bot=bot, chat_id=42, config=config or FakeConfig())


def test_deltas_within_window_produce_single_edit() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)

        for i in range(5):
            await streamer.consume(delta(f"a{i}"))
        await streamer.tick()
        assert bot.names().count("send_message") == 1

        # Segunda janela: mais 10 deltas, todos dentro do throttle -> 1 edit.
        for i in range(10):
            await streamer.consume(delta(f"b{i}"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        assert bot.names().count("edit_message_text") == 1
        assert bot.texts("edit_message_text")[0] == "a0a1a2a3a4b0b1b2b3b4b5b6b7b8b9"

        # Terceira janela: deltas recentes -> nenhum edit novo.
        for i in range(10):
            await streamer.consume(delta(f"c{i}"))
        streamer.last_edit = time.monotonic()
        await streamer.tick()
        assert bot.names().count("edit_message_text") == 1

    asyncio.run(scenario())


def test_shared_interval_between_stream_and_activity() -> None:
    """Edições de stream e de atividade contam no mesmo limite do Telegram."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)

        await streamer.consume(delta("texto"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        assert bot.names().count("send_message") == 1

        # Atividade logo depois: barrada pelo piso compartilhado.
        await streamer.consume({"type": "tool_execution_start", "toolName": "bash"})
        await streamer.tick()
        assert bot.names().count("send_message") == 1

        # Passado o piso, a atividade sai.
        streamer.last_activity_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        assert bot.names().count("send_message") == 2
        assert bot.texts("send_message")[1].startswith("⚙️ atividade\n🔧 bash")

    asyncio.run(scenario())


def test_overflow_starts_new_message() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot, FakeConfig(max_msg_chars=20))

        await feed(streamer, "x" * 20, step=5)
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        await feed(streamer, "y" * 5, step=5)
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()

        sent = final_messages(bot)
        assert len(sent) >= 2, f"esperava nova mensagem no overflow: {sent}"
        # O texto é repartido em blocos, mas nada se perde nem se duplica.
        joined = "".join(text for _, text in sent)
        assert joined == "x" * 20 + "y" * 5, joined
        assert streamer.active_msg_id is not None

    asyncio.run(scenario())


def test_retry_after_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        bot = FakeBot()
        bot.edits_before_success = 1
        streamer = make_streamer(bot)

        await streamer.consume(delta("oi"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        assert bot.names().count("send_message") == 1

        slept: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        await streamer.consume(delta(" tudo bem"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()

        assert slept == [3.5]
        assert bot.names().count("edit_message_text") == 1
        assert bot.texts("edit_message_text") == ["oi tudo bem"]

    asyncio.run(scenario())


def test_not_modified_bad_request_is_swallowed() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)

        await streamer.consume(delta("oi"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        bot.edit_error = BadRequest("Message is not modified")

        await streamer.consume(delta("!"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()  # não deve propagar

        assert bot.names().count("edit_message_text") == 0
        assert "edit_message_text_raised" in bot.names()

    asyncio.run(scenario())


def test_activity_accumulates_and_marks_errors() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)

        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "bash", "args": {"command": "echo hi"}}
        )
        await streamer.consume({"type": "tool_execution_end", "toolName": "bash", "isError": True})
        streamer.last_activity_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()

        activity_texts = bot.texts("send_message") + bot.texts("edit_message_text")
        text = activity_texts[-1]
        assert text.startswith("⚙️ atividade\n")
        assert "🔧 bash: echo hi" in text
        assert "✗ bash" in text

    asyncio.run(scenario())


def test_tool_line_is_updated_in_place_on_completion() -> None:
    """`🔧 write: ...` vira `✓ write: ...` — sem par duplicado de linhas."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {
                "type": "tool_execution_start",
                "toolName": "write",
                "intent": "Creating hello.py",
                "toolCallId": "call_1",
            }
        )
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "write", "toolCallId": "call_1", "isError": False}
        )
        assert streamer.activity_lines == ["✓ write: Creating hello.py"]

    asyncio.run(scenario())


def test_tool_line_marks_error_in_place() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "bash", "toolCallId": "c", "intent": "Running"}
        )
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "bash", "toolCallId": "c", "isError": True}
        )
        assert streamer.activity_lines == ["✗ bash: Running"]

    asyncio.run(scenario())


def test_completion_without_matching_start_appends() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume({"type": "tool_execution_end", "toolName": "bash", "isError": False})
        assert streamer.activity_lines == ["✓ bash"]

    asyncio.run(scenario())


def test_activity_index_survives_line_trimming() -> None:
    """O corte das últimas 15 linhas não pode desalinhar o índice guardado."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        for i in range(20):
            await streamer.consume(
                {
                    "type": "tool_execution_start",
                    "toolName": "read",
                    "toolCallId": f"c{i}",
                    "intent": f"lendo {i}",
                }
            )
        # `c19` ainda está na janela; o índice guardado tem que continuar válido.
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "read", "toolCallId": "c19", "isError": False}
        )
        assert streamer.activity_lines[-1] == "✓ read: lendo 19"
        assert len(streamer.activity_lines) == 15

    asyncio.run(scenario())


def test_activity_prefers_intent_over_raw_args() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {
                "type": "tool_execution_start",
                "toolName": "write",
                "intent": "Creating hello.py",
                "args": {"path": "hello.py", "content": "print('hi')\n"},
            }
        )
        text = bot.texts("send_message")[-1]
        assert "🔧 write: Creating hello.py" in text
        assert "content" not in text  # sem despejar o conteúdo do arquivo

    asyncio.run(scenario())


def test_activity_falls_back_to_args_without_intent() -> None:
    """Sem `intent`, mostra o campo útil — nunca o JSON cru."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "bash", "args": {"command": "ls -la ."}}
        )
        await streamer.consume(
            {
                "type": "tool_execution_start",
                "toolName": "glob",
                "args": {"path": ".", "limit": 20},
            }
        )
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "read", "args": {"path": "src/lib.rs"}}
        )

        assert streamer.activity_lines == [
            "🔧 bash: ls -la .",
            "🔧 glob: .",
            "🔧 read: src/lib.rs",
        ]

    asyncio.run(scenario())


def test_fallback_never_dumps_raw_json() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {
                "type": "tool_execution_start",
                "toolName": "read",
                "args": {"path": "a.rs", "offset": 10, "limit": 50},
            }
        )
        text = bot.texts("send_message")[-1]
        assert "{" not in text and "}" not in text

    asyncio.run(scenario())


def test_no_empty_activity_message_is_created() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        # Nenhuma ferramenta ainda: o tick não pode criar "⚙️ atividade" vazio.
        await streamer.tick()
        await streamer.consume(delta("só texto"))
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.tick()
        assert not [t for t in bot.texts("send_message") if t.startswith("⚙️")]

        # E o finish também não cria uma atividade vazia.
        await streamer.finish({})
        assert not [t for t in bot.texts("send_message") if t.startswith("⚙️")]

    asyncio.run(scenario())


def todo_frame(phases: list[dict[str, Any]], is_error: bool = False) -> dict[str, Any]:
    return {
        "type": "tool_execution_end",
        "toolName": "todo",
        "toolCallId": "t1",
        "isError": is_error,
        "result": {"details": {"op": "create", "phases": phases}},
    }


PHASES = [
    {
        "name": "1. Renomear em util.py",
        "tasks": [
            {"content": "Renomear a funcao `dobro` para `triplo`", "status": "completed"},
            {"content": "Rodar os testes", "status": "in_progress"},
        ],
    },
    {
        "name": "2. Atualizar chamadores",
        "tasks": [{"content": "Mapear todo uso via grep", "status": "pending"}],
    },
]


def test_todo_result_renders_as_readable_plan() -> None:
    """Antes o plano virava `🔧 todo: {"op": "create", ...}` — JSON cru."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(todo_frame(PHASES))

        plan = bot.texts("send_message")[-1]
        assert plan.startswith("📋 plano · 1/3")
        assert "1. Renomear em util.py" in plan
        assert "  ✅ Renomear a funcao `dobro` para `triplo`" in plan
        assert "  🔄 Rodar os testes" in plan
        assert "2. Atualizar chamadores" in plan
        assert "  ⬜ Mapear todo uso via grep" in plan

    asyncio.run(scenario())


def test_todo_does_not_leak_raw_json_into_activity() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {
                "type": "tool_execution_start",
                "toolName": "todo",
                "toolCallId": "t1",
                "args": {"op": "create", "phases": PHASES},
                "intent": "Registering plan steps",
            }
        )
        assert streamer.activity_lines == []
        assert not [t for t in bot.texts("send_message") if "⚙️" in t]

    asyncio.run(scenario())


def test_plan_is_updated_in_place_as_steps_complete() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(todo_frame(PHASES))

        done = [
            {
                "name": "1. Renomear em util.py",
                "tasks": [
                    {"content": "Renomear a funcao `dobro` para `triplo`", "status": "completed"},
                    {"content": "Rodar os testes", "status": "completed"},
                ],
            }
        ]
        streamer.last_plan_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.consume(todo_frame(done))

        ids = {mid for name, mid, _, _ in bot.written if name in ("send_message", "edit_message_text")}
        assert len(ids) == 1, f"o plano deveria viver numa mensagem só: {ids}"
        final = final_messages(bot)[-1][1]
        assert final.startswith("📋 plano · 2/2")

    asyncio.run(scenario())


def test_plan_survives_turn_end_without_being_collapsed() -> None:
    """O plano é entregável do turno: não entra no resumo de ferramentas."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(delta("Segue o plano."))
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "read", "toolCallId": "r1"}
        )
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "read", "toolCallId": "r1"}
        )
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "todo", "toolCallId": "t1"}
        )
        await streamer.consume(todo_frame(PHASES))
        await streamer.finish({})

        plans = [t for _, _, t, _ in bot.written if t.startswith("📋 plano")]
        assert plans, "o plano sumiu no fim do turno"
        assert "✅" in plans[-1]
        # O `todo` entra na contagem; o conteúdo dele é o plano, não uma linha.
        summary = [t for _, _, t, _ in bot.written if t.startswith("⚙️")]
        assert summary and "todo ×1" in summary[-1] and "read ×1" in summary[-1], summary

    asyncio.run(scenario())


def test_plan_ignores_malformed_phases() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume({"type": "tool_execution_end", "toolName": "todo", "result": {}})
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "todo", "result": {"details": {"phases": []}}}
        )
        assert not [t for _, _, t, _ in bot.written if t.startswith("📋 plano")]

    asyncio.run(scenario())


def test_chunk_split_prefers_paragraph_boundary() -> None:
    """Cortar no meio de uma tabela deixa a outra metade irreconhecível."""
    from bridge.streamer import _split_chunk

    text = "paragrafo um\n\nparagrafo dois\n\nparagrafo tres"
    head, tail = _split_chunk(text, 20)
    assert head.endswith("\n\n")
    assert head + tail == text
    assert head == "paragrafo um\n\n"
    assert tail == "paragrafo dois\n\nparagrafo tres"


def test_chunk_split_falls_back_to_line_boundary() -> None:
    from bridge.streamer import _split_chunk

    text = "linha a\nlinha b\nlinha c"
    head, tail = _split_chunk(text, 12)
    assert head.endswith("\n")
    assert head + tail == text


def test_chunk_split_hard_cuts_when_no_boundary() -> None:
    from bridge.streamer import _split_chunk

    text = "x" * 100
    head, tail = _split_chunk(text, 40)
    assert head == "x" * 40
    assert tail == "x" * 60


def test_chunk_split_keeps_short_text_whole() -> None:
    from bridge.streamer import _split_chunk

    assert _split_chunk("curto", 100) == ("curto", "")


def test_table_is_not_split_across_messages() -> None:
    """Uma tabela grande deve caber inteira numa mensagem."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot, FakeConfig(max_msg_chars=400))

        rows = "\n".join(f"| variedade {i} | {i} kg | uso {i} |" for i in range(12))
        table = f"| Variedade | Peso | Uso |\n|---|---|---|\n{rows}\n"
        await feed(streamer, "intro\n\n" + table + "\nfim\n")
        await streamer.finish({})

        for _, text in final_messages(bot):
            # Nenhuma mensagem pode conter um fragmento de tabela sem o cabeçalho
            # completo: ou a tabela está inteira, ou não aparece.
            if "|" in text and "▸" not in text and "<b>" not in text:
                assert "|---" not in text, f"fragmento de tabela cru: {text[:120]!r}"

    asyncio.run(scenario())


def test_emphasis_never_spans_code_spans() -> None:
    """`x * 3` e `x * 2` na mesma linha fechavam um itálico dentro dos `<code>`."""

    from bridge.streamer import _to_telegram_html

    out = _to_telegram_html("sugere `x * 3`, mas o corpo é `x * 2`")
    assert out == "sugere <code>x * 3</code>, mas o corpo é <code>x * 2</code>"
    assert "<i>" not in out
    assert_valid_telegram_html(out)

    out = _to_telegram_html("o `a * b` e o `c * d` e **forte**")
    assert "<code>a * b</code>" in out
    assert "<code>c * d</code>" in out
    assert "<b>forte</b>" in out
    assert_valid_telegram_html(out)


def test_markdown_inside_code_stays_literal() -> None:
    from bridge.streamer import _to_telegram_html

    assert _to_telegram_html("`**nao**` e **sim**") == "<code>**nao**</code> e <b>sim</b>"
    assert _to_telegram_html("`_x_`") == "<code>_x_</code>"
    assert _to_telegram_html("```py\nx * y\n```") == (
        '<pre><code class="language-py">x * y\n</code></pre>'
    )


def test_table_values_keep_their_column_labels() -> None:
    """Sem rótulo, `| | CBD | THC |` vira "não · sim" e perde o significado."""

    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html(
        "| | CBD | THC |\n|---|---|---|\n| Psicoativo | não | sim |\n"
        "| Risco | hepatotoxicidade | psicose |"
    )
    assert "▸ Psicoativo — CBD: não · THC: sim" in html
    assert "▸ Risco — CBD: hepatotoxicidade · THC: psicose" in html


def test_two_column_table_has_no_redundant_label() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html("| Arquivo | Papel |\n|---|---|\n| a.py | faz x |")
    assert html == "▸ a.py — faz x"


def test_three_column_table_labels_after_the_first() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html("| Tipo | CBD | THC |\n|---|---|---|\n| Psicoativo | não | sim |")
    assert html == "▸ Psicoativo — CBD: não · THC: sim"


def test_table_without_header_keeps_plain_values() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html("| a | b | c |\n| 1 | 2 | 3 |")
    assert html == "▸ a — b · c\n▸ 1 — 2 · 3"


def test_markdown_table_becomes_a_list() -> None:
    """O Telegram não renderiza tabelas: os pipes apareceriam literais."""
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html(
        "| Arquivo | Papel |\n|---|---|\n"
        "| `update_prices.py` | Motor atual (310 linhas). |\n"
        "| `omp.py` | Wrapper. |\n"
    )
    assert "|" not in html
    assert "▸ <code>update_prices.py</code> — Motor atual (310 linhas)." in html
    assert "▸ <code>omp.py</code> — Wrapper." in html


def test_table_header_is_not_repeated_as_a_row() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html("| Arquivo | Papel |\n|---|---|\n| a.py | faz x |")
    assert "Arquivo" not in html
    assert html == "▸ a.py — faz x"


def test_table_alignment_markers_are_ignored() -> None:
    from bridge.streamer import _to_telegram_html

    assert _to_telegram_html("| A | B |\n|:--|--:|\n| 1 | 2 |") == "▸ 1 — 2"


def test_table_without_separator_keeps_all_rows() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html("| A | B |\n| 1 | 2 |")
    assert html == "▸ A — B\n▸ 1 — 2"


def test_pipes_outside_a_table_are_untouched() -> None:
    from bridge.streamer import _to_telegram_html

    assert _to_telegram_html("use a | b aqui") == "use a | b aqui"


def test_multiple_tables_are_converted_independently() -> None:
    from bridge.streamer import _to_telegram_html

    html = _to_telegram_html(
        "| A | B |\n|---|---|\n| 1 | 2 |\n\ntexto\n\n| C | D |\n|---|---|\n| 3 | 4 |"
    )
    assert html == "▸ 1 — 2\n\ntexto\n\n▸ 3 — 4"


def test_blockquote_link_strike_and_rule() -> None:
    from bridge.streamer import _to_telegram_html

    assert (
        _to_telegram_html("> nota\n> segunda")
        == "<blockquote>nota\nsegunda</blockquote>\n"
    )
    assert _to_telegram_html("> nota\ntexto") == "<blockquote>nota</blockquote>\ntexto"
    assert _to_telegram_html("veja [a doc](https://x.dev/a)") == (
        'veja <a href="https://x.dev/a">a doc</a>'
    )
    assert _to_telegram_html("~~riscado~~") == "<s>riscado</s>"
    assert _to_telegram_html("a\n\n---\n\nb") == "a\n\n──────\n\nb"


def assert_valid_telegram_html(html: str) -> None:
    """Valida o HTML como o Telegram o recebe.

    O Telegram aceita `<blockquote expandable>` (atributo sem valor, como
    documentado), que o parser XML estrito rejeita — então normalizamos só para
    a checagem de balanceamento.
    """
    import xml.etree.ElementTree as ET

    normalized = re.sub(
        r"<blockquote expandable>", '<blockquote expandable="1">', html
    )
    ET.fromstring(f"<r>{normalized}</r>")


async def feed(streamer: TurnStreamer, text: str, step: int = 8) -> None:
    """Alimenta o streamer em deltas pequenos, como o omp faz de verdade."""
    for start in range(0, len(text), step):
        await streamer.consume(delta(text[start : start + step]))


def final_messages(bot: FakeBot) -> list[tuple[int, str]]:
    """Texto final de cada mensagem, na ordem em que apareceu no chat.

    Edições sobrescrevem o conteúdo, então o que interessa é o último estado.
    """
    order: list[int] = []
    texts: dict[int, str] = {}
    for name, message_id, text, _ in bot.written:
        if name not in ("send_message", "edit_message_text"):
            continue
        if message_id not in texts:
            order.append(message_id)
        texts[message_id] = text
    return [(message_id, texts[message_id]) for message_id in order]


def test_overflow_chunks_are_also_formatted() -> None:
    """Resposta longa vira várias mensagens: TODAS devem ser formatadas.

    Antes, só a última recebia render; os blocos anteriores ficavam com
    Markdown cru (`## titulo`, `**bold**`, tabelas com pipes) no chat.
    """

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot, FakeConfig(max_msg_chars=200))

        await feed(streamer, "## Primeiro\n\n**negrito** e `code`\n\n")
        await feed(streamer, "texto de enchimento " * 20)
        await feed(streamer, "\n\n## Segundo\n\nmais texto")
        await streamer.finish({})

        sent_ids = {
            message_id for name, message_id, _, _ in bot.written if name == "send_message"
        }
        assert len(sent_ids) >= 2, f"esperava overflow, ids={sent_ids}"

        formatted_ids = {
            message_id
            for name, message_id, _, kwargs in bot.written
            if name in ("send_message", "edit_message_text")
            and kwargs.get("parse_mode") == ParseMode.HTML
        }
        missing = sorted(sent_ids - formatted_ids)
        assert not missing, f"mensagens sem formatação: {missing}"

        rendered = " ".join(text for _, _, text, _ in bot.written)
        assert "<b>Primeiro</b>" in rendered
        assert "## Primeiro" not in rendered

    asyncio.run(scenario())


def test_overflow_chunk_falls_back_to_plain_when_too_long() -> None:
    """Se o render não couber, mantém texto puro em vez de estourar o limite."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot, FakeConfig(max_msg_chars=120))
        # Muito markdown inline: a versão HTML fica maior que o limite.
        await streamer.consume(delta("`a` " * 60))
        await streamer.finish({})

        for name, _, kwargs in bot.calls:
            text = kwargs.get("text") or ""
            if name in ("send_message", "edit_message_text"):
                assert len(text) <= 120, f"mensagem acima do limite: {len(text)}"

    asyncio.run(scenario())


def test_italics_are_converted_but_not_snake_case_or_bullets() -> None:
    from bridge.streamer import _to_telegram_html

    assert _to_telegram_html("*C. pepo*") == "<i>C. pepo</i>"
    assert _to_telegram_html("isto é _itálico_ aqui") == "isto é <i>itálico</i> aqui"
    # `_` entre palavras é identificador, não ênfase.
    assert _to_telegram_html("file_name_here") == "file_name_here"
    # Bullet de lista não vira itálico.
    assert _to_telegram_html("* item\n* outro") == "* item\n* outro"
    assert _to_telegram_html("**bold** e *ital*") == "<b>bold</b> e <i>ital</i>"


def test_long_code_block_is_collapsed() -> None:
    """Bloco longo nasce recolhido: o Telegram suporta citação colapsável."""

    from bridge.streamer import _to_telegram_html

    short = _to_telegram_html("```json\n{\"a\": 1}\n```")
    assert not short.startswith("<blockquote expandable>")
    assert_valid_telegram_html(short)

    long_json = "```json\n" + "\n".join(f'  "k{i}": {i},' for i in range(40)) + "\n```"
    out = _to_telegram_html(long_json)
    assert out.startswith("<blockquote expandable><pre>")
    assert out.endswith("</pre></blockquote>")
    assert_valid_telegram_html(out)


def test_many_short_lines_also_collapse() -> None:
    from bridge.streamer import _to_telegram_html

    body = "\n".join(f"linha {i}" for i in range(20))
    out = _to_telegram_html(f"```\n{body}\n```")
    assert out.startswith("<blockquote expandable>")


def test_code_language_is_kept_and_sanitized() -> None:
    from bridge.streamer import _to_telegram_html

    assert 'class="language-python"' in _to_telegram_html("```python\nx\n```")
    # Linguagem esquisita não vira atributo injetável.
    weird = _to_telegram_html('```py"><script>\nx\n```')
    assert "<script>" not in weird
    assert_valid_telegram_html(weird)


def test_html_conversion_escapes_and_keeps_unmatched_markers() -> None:
    from bridge.streamer import _to_telegram_html

    assert _to_telegram_html("a < b & c > d") == "a &lt; b &amp; c &gt; d"
    assert _to_telegram_html("**negrito**") == "<b>negrito</b>"
    assert _to_telegram_html("**sem fim") == "**sem fim"
    assert _to_telegram_html("`code`") == "<code>code</code>"
    assert _to_telegram_html("```\nx < y\n```") == "<pre>x &lt; y\n</pre>"
    assert _to_telegram_html("```py\nx < y\n```") == (
        '<pre><code class="language-py">x &lt; y\n</code></pre>'
    )
    assert _to_telegram_html("__forte__") == "<b>forte</b>"
    assert _to_telegram_html("## Título") == "<b>Título</b>"
    # HTML escrito pelo modelo não pode virar tag de verdade.
    assert _to_telegram_html("<b>falso</b>") == "&lt;b&gt;falso&lt;/b&gt;"


def test_html_conversion_never_produces_unbalanced_tags() -> None:
    """Marcadores desbalanceados não podem gerar HTML inválido."""
    import xml.etree.ElementTree as ET

    from bridge.streamer import _to_telegram_html

    samples = [
        "***x***",
        "**a **b** c**",
        "`a**b`",
        "```\nsem fim",
        "**`code`**",
        "# `x` e **y**",
        "a & b < c > d `e` ```f```",
    ]
    for sample in samples:
        html = _to_telegram_html(sample)
        ET.fromstring(f"<r>{html}</r>")  # levanta se estiver desbalanceado


def test_markdown_is_rendered_as_html_in_final_edit() -> None:
    """O Markdown do modelo vira HTML no fim do turno (não aparece literal)."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            delta("Use `print(hi)` e **negrito**.\n\n```python\nprint(1)\n```\n\n# Título\n")
        )
        await streamer.finish({})

        rich_edits = [
            kwargs
            for name, _, kwargs in bot.calls
            if name == "edit_message_text" and kwargs.get("parse_mode") == ParseMode.HTML
        ]
        assert rich_edits, "a edição final deveria usar parse_mode=HTML"
        text = bot.texts("edit_message_text")[-1]
        assert "<code>print(hi)</code>" in text
        assert "<b>negrito</b>" in text
        assert '<pre><code class="language-python">print(1)\n</code></pre>' in text
        assert "<b>Título</b>" in text
        assert "**" not in text

    asyncio.run(scenario())


def test_final_edit_falls_back_to_plain_text_on_bad_markdown() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(delta("resposta com `código`"))
        bot.edit_error = BadRequest("Can't parse entities")
        await streamer.finish({})

        assert "edit_message_text_raised" in bot.names()
        plain_edits = [
            kwargs
            for name, _, kwargs in bot.calls
            if name == "edit_message_text" and kwargs.get("parse_mode") is None
        ]
        assert plain_edits, "deveria cair para texto puro"
        text = bot.texts("edit_message_text")[-1]
        assert "resposta com `código`" in text  # markdown literal, sem tags
        assert "<code>" not in text

    asyncio.run(scenario())


def test_streaming_edits_stay_plain() -> None:
    """Durante o stream não pode haver parse_mode: o texto é parcial."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(delta("parcial `com crase aberta"))
        await streamer.tick()
        assert bot.names().count("send_message") == 1
        streamer.last_edit = time.monotonic() - 5
        streamer._last_chat_write = time.monotonic() - 5
        await streamer.consume(delta(" mais"))
        await streamer.tick()

        for name, _, kwargs in bot.calls:
            if name in ("send_message", "edit_message_text"):
                assert kwargs.get("parse_mode") is None

    asyncio.run(scenario())


def test_finish_collapses_activity_to_one_line() -> None:
    """No fim do turno a lista de ferramentas vira um resumo de uma linha."""

    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(delta("resposta"))
        for i, name in enumerate(["bash", "bash", "read"]):
            await streamer.consume(
                {"type": "tool_execution_start", "toolName": name, "toolCallId": f"c{i}"}
            )
            await streamer.consume(
                {"type": "tool_execution_end", "toolName": name, "toolCallId": f"c{i}"}
            )
        await streamer.finish({})

        activity = [t for t in bot.texts("edit_message_text") if t.startswith("⚙️")]
        assert len(activity) == 1, activity
        assert activity[0].startswith("⚙️ bash ×2 · read ×1 · ")
        assert "\n" not in activity[0]

    asyncio.run(scenario())


def test_collapsed_summary_counts_failures() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {"type": "tool_execution_start", "toolName": "bash", "toolCallId": "c1"}
        )
        await streamer.consume(
            {"type": "tool_execution_end", "toolName": "bash", "toolCallId": "c1", "isError": True}
        )
        await streamer.finish({})
        summary = [t for t in bot.texts("edit_message_text") if t.startswith("⚙️")][-1]
        assert "1 falha(s)" in summary

    asyncio.run(scenario())


def test_finish_without_tools_creates_no_activity_message() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(delta("só texto"))
        await streamer.finish({})
        assert not [t for t in bot.texts("send_message") if t.startswith("⚙️")]
        assert not [t for t in bot.texts("edit_message_text") if t.startswith("⚙️")]

    asyncio.run(scenario())


def test_finish_appends_footer_and_sends_document() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot, FakeConfig(doc_threshold=10))
        await streamer.consume(delta("resposta longa o suficiente"))
        await streamer.consume(
            {"type": "prompt_result", "id": "req_1", "status": "completed"}
        )
        state = {"model": {"id": "inferhub/deepseek-v4.1-flash"}, "contextUsage": {"percent": 42.4}}
        await streamer.finish(state)

        final = bot.texts("edit_message_text")[-1]
        assert "— ✅ pronto em" in final
        assert "inferhub/deepseek-v4.1-flash" in final
        assert "ctx 42%" in final
        assert bot.names().count("send_document") == 1
        doc = [args[1] for n, args, _ in bot.calls if n == "send_document"][0]
        assert isinstance(doc, InputFile)
        assert doc.filename == "resposta.md"
        assert doc.input_file_content == b"resposta longa o suficiente"

    asyncio.run(scenario())


def test_finish_reports_error_and_abort() -> None:
    async def scenario() -> None:
        bot = FakeBot()
        streamer = make_streamer(bot)
        await streamer.consume(
            {
                "type": "prompt_result",
                "id": "req_1",
                "status": "error",
                "error": {"message": "provider caiu"},
            }
        )
        await streamer.finish({})
        assert "— ❌ provider caiu" in bot.texts("edit_message_text")[-1]

        bot2 = FakeBot()
        streamer2 = make_streamer(bot2)
        await streamer2.consume(delta("parcial"))
        await streamer2.consume({"type": "prompt_result", "id": "r", "status": "aborted"})
        await streamer2.finish({})
        assert "— 🛑 interrompido" in bot2.texts("edit_message_text")[-1]

    asyncio.run(scenario())
