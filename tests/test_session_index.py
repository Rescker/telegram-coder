"""Testes do índice de sessões."""

from __future__ import annotations

import json
from pathlib import Path

from bridge import session_index


def test_load_missing_or_corrupt_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    assert session_index.load(path) == {"sessions": []}

    path.write_text("{isso não é json", encoding="utf-8")
    assert session_index.load(path) == {"sessions": []}

    path.write_text(json.dumps(["lista"]), encoding="utf-8")
    assert session_index.load(path) == {"sessions": []}


def test_touch_upserts_and_preserves_label(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    session_index.touch(path, session_path="/s/1.jsonl", messages=3)
    session_index.touch(path, session_path="/s/1.jsonl", label="trabalho")
    session_index.touch(path, session_path="/s/1.jsonl", messages=9)

    entries = session_index.recent(path)
    assert len(entries) == 1
    assert entries[0]["label"] == "trabalho"
    assert entries[0]["messages"] == 9


def test_default_label_is_file_stem(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    session_index.touch(path, session_path="/s/2026-01-02T03-04-05_abc.jsonl")
    assert session_index.recent(path)[0]["label"] == "2026-01-02T03-04-05_abc"


def test_recent_orders_by_updated_at_desc_and_limits(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    session_index.save(
        path,
        {
            "sessions": [
                {"path": "/a", "label": "a", "updatedAt": 100, "messages": 1},
                {"path": "/b", "label": "b", "updatedAt": 300, "messages": 2},
                {"path": "/c", "label": "c", "updatedAt": 200, "messages": 3},
                {"path": "", "label": "inválida", "updatedAt": 999},
            ]
        },
    )
    assert [e["path"] for e in session_index.recent(path)] == ["/b", "/c", "/a"]
    assert [e["path"] for e in session_index.recent(path, limit=2)] == ["/b", "/c"]


def test_save_is_atomic_leaving_no_tmp(tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    session_index.save(path, {"sessions": []})
    assert path.exists()
    assert list(tmp_path.glob("*.tmp")) == []
