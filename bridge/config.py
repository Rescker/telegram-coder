"""Configuração do bridge: leitura de .env + validação."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_env_file(env_file: Path) -> dict[str, str]:
    """Parser próprio de .env (sem python-dotenv).

    Ignora linhas vazias e comentários, corta no primeiro `=`, remove aspas
    simples/duplas das pontas do valor. Em valores sem aspas, um `#` precedido
    de espaço inicia comentário (mesma regra do python-dotenv), para que
    `TELEGRAM_ALLOWED_USER_ID=123 # meu id` não vire um erro de parse.
    """
    values: dict[str, str] = {}
    if not env_file.exists():
        return values
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].strip()
        if key:
            values[key] = value
    return values


def _get(values: dict[str, str], key: str, default: str = "") -> str:
    """Variáveis de ambiente reais do processo têm precedência sobre o arquivo."""
    env_value = os.environ.get(key)
    if env_value is not None and env_value != "":
        return env_value
    return values.get(key, default)


@dataclass(frozen=True)
class Config:
    token: str
    allowed_user_id: int
    omp_bin: str
    omp_cwd: Path
    approval_mode: str
    model: str | None
    stream_edit_seconds: float
    max_msg_chars: int
    doc_threshold: int
    data_dir: Path

    @property
    def sessions_dir(self) -> Path:
        return self.data_dir / "sessions"

    @property
    def offset_file(self) -> Path:
        return self.data_dir / "telegram-offset.json"

    @property
    def index_file(self) -> Path:
        return self.data_dir / "session-index.json"

    @property
    def lock_file(self) -> Path:
        return self.data_dir / "bridge.lock"

    @property
    def log_file(self) -> Path:
        return self.data_dir / "bridge.log"

    @property
    def omp_stderr_file(self) -> Path:
        return self.data_dir / "omp-stderr.log"


def _parse_float(values: dict[str, str], key: str, default: float) -> float:
    raw = _get(values, key, str(default)).strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f"{key} inválido: {raw!r} (esperado número, ex. 2.0)")


def _parse_int(values: dict[str, str], key: str, default: int) -> int:
    raw = _get(values, key, str(default)).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f"{key} inválido: {raw!r} (esperado inteiro, ex. 3800)")


def load_config(env_file: Path = Path(".env"), *, require_token: bool = True) -> Config:
    """Lê e valida a configuração.

    `require_token=False` é usado pelo `--selftest`, que exercita só a ponte com
    o omp e não fala com o Telegram.
    """
    values = _parse_env_file(env_file)

    token = _get(values, "TELEGRAM_BOT_TOKEN").strip()
    if not token and require_token:
        if env_file.exists():
            raise SystemExit(
                f"TELEGRAM_BOT_TOKEN vazio em {env_file}: preencha com o token do @BotFather"
            )
        raise SystemExit(
            f"{env_file} não existe: copie .env.example para .env e preencha o token"
        )

    raw_user_id = _get(values, "TELEGRAM_ALLOWED_USER_ID").strip()
    if not raw_user_id:
        # Vazio = modo setup (ninguém autorizado), o padrão fail-safe: nenhum
        # usuário passa pela allowlist até o id ser preenchido.
        allowed_user_id = 0
    else:
        try:
            allowed_user_id = int(raw_user_id)
        except ValueError:
            raise SystemExit(
                "TELEGRAM_ALLOWED_USER_ID inválido: "
                f"{raw_user_id!r} — use o id numérico do usuário. Para descobri-lo, "
                "rode o bridge com TELEGRAM_ALLOWED_USER_ID=0 e envie /id para o bot."
            )
        if allowed_user_id < 0:
            raise SystemExit(f"TELEGRAM_ALLOWED_USER_ID inválido: {allowed_user_id}")

    omp_bin = _get(values, "OMP_BIN", "omp").strip() or "omp"

    raw_cwd = _get(values, "OMP_CWD").strip()
    # Absoluto é obrigatório: o caminho é usado como cwd do processo filho *e*
    # em `--cwd`, então um valor relativo seria resolvido duas vezes.
    omp_cwd = (Path(raw_cwd).expanduser() if raw_cwd else Path.cwd()).resolve()
    if not omp_cwd.is_dir():
        raise SystemExit(f"OMP_CWD não existe: {omp_cwd}")

    approval_mode = _get(values, "OMP_APPROVAL_MODE", "write").strip() or "write"

    model = _get(values, "OMP_MODEL").strip() or None

    # Absoluto: o filho do omp roda com `cwd=OMP_CWD`, então um caminho relativo
    # seria resolvido contra o diretório errado (sessões cairiam dentro do alvo).
    data_dir = (
        Path(_get(values, "DATA_DIR", "./data").strip() or "./data").expanduser().resolve()
    )

    config = Config(
        token=token,
        allowed_user_id=allowed_user_id,
        omp_bin=omp_bin,
        omp_cwd=omp_cwd,
        approval_mode=approval_mode,
        model=model,
        stream_edit_seconds=_parse_float(values, "STREAM_EDIT_SECONDS", 2.0),
        max_msg_chars=_parse_int(values, "MAX_MSG_CHARS", 3800),
        doc_threshold=_parse_int(values, "DOC_THRESHOLD", 12000),
        data_dir=data_dir,
    )

    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.sessions_dir.mkdir(parents=True, exist_ok=True)
    return config
