"""Índice local de sessões do bridge (não é o formato JSONL do omp)."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def load(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except FileNotFoundError:
        return {"sessions": []}
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("índice de sessões ilegível (%s): %s", path, exc)
        return {"sessions": []}
    if not isinstance(data, dict) or not isinstance(data.get("sessions"), list):
        return {"sessions": []}
    return data


def save(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def touch(
    path: Path,
    *,
    session_path: str,
    label: str | None = None,
    messages: int | None = None,
) -> None:
    data = load(path)
    sessions = data.setdefault("sessions", [])
    now = time.time()
    for entry in sessions:
        if entry.get("path") == session_path:
            entry["updatedAt"] = now
            if label is not None:
                entry["label"] = label
            if messages is not None:
                entry["messages"] = messages
            break
    else:
        sessions.append(
            {
                "path": session_path,
                "label": label or Path(session_path).stem,
                "updatedAt": now,
                "messages": messages or 0,
            }
        )
    save(path, data)


def recent(path: Path, limit: int = 20) -> list[dict[str, Any]]:
    data = load(path)
    sessions = [s for s in data.get("sessions", []) if isinstance(s, dict) and s.get("path")]
    sessions.sort(key=lambda s: s.get("updatedAt") or 0, reverse=True)
    return sessions[:limit]
