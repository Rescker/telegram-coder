"""Testes de carregamento/validação da configuração."""

from __future__ import annotations

from pathlib import Path

import pytest

from bridge.config import load_config


def write_env(path: Path, content: str) -> Path:
    env_file = path / ".env"
    env_file.write_text(content, encoding="utf-8")
    return env_file


def test_parses_values_with_quotes_and_comments(tmp_path: Path) -> None:
    data_dir = tmp_path / "dados"
    env = write_env(
        tmp_path,
        "\n".join(
            [
                "# comentário",
                "",
                'TELEGRAM_BOT_TOKEN="123:ABC"',
                "TELEGRAM_ALLOWED_USER_ID=7",
                f"OMP_CWD='{tmp_path}'",
                "OMP_MODEL=''",
                "STREAM_EDIT_SECONDS=0.25",
                "MAX_MSG_CHARS=1000",
                "DOC_THRESHOLD=5000",
                f"DATA_DIR={data_dir}",
            ]
        ),
    )
    config = load_config(env)

    assert config.token == "123:ABC"
    assert config.allowed_user_id == 7
    assert config.omp_cwd == tmp_path
    assert config.model is None
    assert config.stream_edit_seconds == 0.25
    assert config.max_msg_chars == 1000
    assert config.doc_threshold == 5000
    assert config.data_dir == data_dir
    assert config.sessions_dir == data_dir / "sessions"
    assert config.offset_file == data_dir / "telegram-offset.json"
    assert config.index_file == data_dir / "session-index.json"
    assert config.lock_file == data_dir / "bridge.lock"
    assert config.log_file == data_dir / "bridge.log"
    assert config.data_dir.is_dir()
    assert config.sessions_dir.is_dir()


def test_env_var_overrides_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=do-arquivo\nTELEGRAM_ALLOWED_USER_ID=1\n")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "do-ambiente")
    assert load_config(env).token == "do-ambiente"


def test_inline_comment_is_stripped_when_unquoted(tmp_path: Path) -> None:
    env = write_env(
        tmp_path,
        f"TELEGRAM_BOT_TOKEN=123:ABC  # token do BotFather\n"
        f"TELEGRAM_ALLOWED_USER_ID=99  # meu id\n"
        f"OMP_CWD={tmp_path}  # projeto alvo\n",
    )
    config = load_config(env)
    assert config.token == "123:ABC"
    assert config.allowed_user_id == 99
    assert config.omp_cwd == tmp_path


def test_hash_inside_quotes_is_kept(tmp_path: Path) -> None:
    env = write_env(
        tmp_path,
        f'TELEGRAM_BOT_TOKEN="123:AB#C"\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_CWD="{tmp_path}"\n',
    )
    assert load_config(env).token == "123:AB#C"


def test_empty_token_points_at_the_file_to_fill(tmp_path: Path) -> None:
    env = write_env(tmp_path, "TELEGRAM_ALLOWED_USER_ID=1\n")
    with pytest.raises(SystemExit, match=r"TELEGRAM_BOT_TOKEN vazio.*@BotFather"):
        load_config(env)


def test_missing_env_file_tells_you_to_copy_the_example(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match=r"não existe: copie \.env\.example para \.env"):
        load_config(tmp_path / "nao-existe.env")


def test_non_numeric_user_id(tmp_path: Path) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=abc\n")
    with pytest.raises(SystemExit, match="/id"):
        load_config(env)


def test_negative_user_id_rejected(tmp_path: Path) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=-3\n")
    with pytest.raises(SystemExit, match="inválido"):
        load_config(env)


def test_missing_cwd_is_rejected(tmp_path: Path) -> None:
    env = write_env(
        tmp_path,
        f"TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_CWD={tmp_path}/nao-existe\n",
    )
    with pytest.raises(SystemExit, match="OMP_CWD não existe"):
        load_config(env)


def test_empty_cwd_falls_back_to_process_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\n")
    monkeypatch.chdir(tmp_path)
    assert load_config(env).omp_cwd == Path.cwd()


def test_invalid_numbers_are_rejected(tmp_path: Path) -> None:
    env = write_env(
        tmp_path,
        "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\nSTREAM_EDIT_SECONDS=muito\n",
    )
    with pytest.raises(SystemExit, match="STREAM_EDIT_SECONDS inválido"):
        load_config(env)


def test_data_dir_is_resolved_to_absolute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Relativo quebraria: o filho do omp roda com cwd=OMP_CWD."""
    monkeypatch.chdir(tmp_path)
    env = write_env(
        tmp_path,
        "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_CWD=.\nDATA_DIR=./dados\n",
    )
    config = load_config(env)
    assert config.data_dir.is_absolute()
    assert config.sessions_dir.is_absolute()
    assert config.data_dir == (tmp_path / "dados").resolve()
    assert config.sessions_dir == (tmp_path / "dados" / "sessions").resolve()


def test_relative_cwd_is_resolved_to_absolute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`OMP_CWD` relativo vira absoluto: é usado como cwd do filho e em --cwd."""
    (tmp_path / "alvo").mkdir()
    monkeypatch.chdir(tmp_path)
    env = write_env(
        tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=1\nOMP_CWD=./alvo\n"
    )
    config = load_config(env)
    assert config.omp_cwd.is_absolute()
    assert config.omp_cwd == (tmp_path / "alvo").resolve()


def test_zero_user_id_is_allowed_for_setup(tmp_path: Path) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=0\n")
    assert load_config(env).allowed_user_id == 0


def test_empty_user_id_means_setup_mode(tmp_path: Path) -> None:
    env = write_env(tmp_path, "TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_ALLOWED_USER_ID=\n")
    assert load_config(env).allowed_user_id == 0


def test_token_can_be_omitted_for_selftest(tmp_path: Path) -> None:
    env = write_env(tmp_path, "OMP_CWD=.\n")
    config = load_config(env, require_token=False)
    assert config.token == ""
    assert config.allowed_user_id == 0


def test_require_token_false_still_validates_the_rest(tmp_path: Path) -> None:
    env = write_env(tmp_path, "OMP_CWD=Z:/nao/existe\n")
    with pytest.raises(SystemExit, match="OMP_CWD não existe"):
        load_config(env, require_token=False)
