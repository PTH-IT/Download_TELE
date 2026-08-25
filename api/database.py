"""api/database.py"""
import asyncio
import logging
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .models import Base

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://tgcopy:tgcopy_secret@localhost:5432/tgcopy")

def _engine_kwargs(url: str) -> dict:
    # SQLite dùng NullPool/StaticPool, không nhận pool_size/max_overflow
    if url.startswith("sqlite"):
        return {"echo": False}
    return {"echo": False, "pool_size": 10, "max_overflow": 20, "pool_pre_ping": True}


engine = create_async_engine(DATABASE_URL, **_engine_kwargs(DATABASE_URL))
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

# create_all chỉ tạo bảng còn thiếu, không sửa bảng đã tồn tại.
# Các câu lệnh dưới đây là migration nhẹ, idempotent, cho DB đã chạy từ trước.
_MIGRATIONS = (
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS media_kind VARCHAR(16) DEFAULT 'video'",
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS downloaded_bytes BIGINT DEFAULT 0",
    "CREATE INDEX IF NOT EXISTS ix_tasks_job_id ON tasks (job_id)",
)


async def _run_migrations(conn) -> None:
    for stmt in _MIGRATIONS:
        try:
            await conn.execute(text(stmt))
        except Exception as exc:  # pragma: no cover - phụ thuộc dialect
            log.warning("Bỏ qua migration %r: %s", stmt, exc)


async def init_db(retries: int = 10, delay_s: float = 1.5):
    last_exc = None
    for i in range(1, retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with engine.begin() as conn:
                await _run_migrations(conn)
            return
        except Exception as e:  # Connection refused / DB not ready
            last_exc = e
            log.warning("init_db lần %s thất bại: %s", i, e)
            await asyncio.sleep(delay_s * i)
    # If still failing, re-raise with context
    raise last_exc


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
