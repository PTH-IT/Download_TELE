"""api/database.py"""
import os
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from .models import Base

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://tgcopy:tgcopy_secret@localhost:5432/tgcopy")

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=10, max_overflow=20)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


async def init_db(retries: int = 10, delay_s: float = 1.5):
    last_exc = None
    for i in range(1, retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            return
        except Exception as e:  # Connection refused / DB not ready
            last_exc = e
            # small exponential-ish backoff
            await __import__("asyncio").sleep(delay_s * i)
    # If still failing, re-raise with context
    raise last_exc



async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
