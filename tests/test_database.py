import importlib
import os

import pytest
from sqlalchemy import select

from api.models import Job


@pytest.mark.asyncio
async def test_init_db_supports_sqlite(tmp_path):
    db_path = tmp_path / "test.db"
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"

    import api.database as database

    database = importlib.reload(database)
    await database.init_db(retries=1, delay_s=0)

    async with database.AsyncSessionLocal() as session:
        result = await session.execute(select(Job))
        assert result.scalars().all() == []
