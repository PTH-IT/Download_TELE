from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def resolve_session_dir() -> Path:
    override = os.getenv("SESSION_DIR") or os.getenv("SESSIONS_DIR")
    if override:
        path = Path(override).expanduser()
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path
    return (PROJECT_ROOT / "sessions").resolve()


def resolve_session_file() -> Path:
    return resolve_session_dir() / "session_string.txt"
