import importlib
import os

import pytest
from sqlalchemy import select

from api.models import Job, Task
from shared.constants import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_DONE,
    JOB_STATUS_RUNNING,
    TASK_STATUS_DONE,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_UPLOADING,
)
from shared.job_state import refresh_job_counters, update_task_status


async def make_session_factory(tmp_path):
    """Không dùng async fixture: pytest-asyncio 0.23 lỗi với pytest 8.x."""
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path / 'state.db'}"
    import api.database as database

    database = importlib.reload(database)
    await database.init_db(retries=1, delay_s=0)
    return database.AsyncSessionLocal


async def _seed(db, statuses, job_status=JOB_STATUS_RUNNING):
    job = Job(src_link="-1001", dst_link="-1002", status=job_status)
    db.add(job)
    await db.flush()
    for i, status in enumerate(statuses):
        db.add(Task(job_id=job.id, msg_id=100 + i, status=status))
    await db.commit()
    return job.id


@pytest.mark.asyncio
async def test_update_task_status_sets_datetime_not_float(tmp_path):
    session_factory = await make_session_factory(tmp_path)
    """Regression: truyền float vào updated_at làm chết vòng lặp upload."""
    async with session_factory() as db:
        job_id = await _seed(db, [TASK_STATUS_UPLOADING])
        task_id = (await db.execute(select(Task.id).where(Task.job_id == job_id))).scalar_one()

        await update_task_status(db, task_id, TASK_STATUS_DONE, speed_up=1234.5)
        await db.commit()

        task = await db.get(Task, task_id, populate_existing=True)
        assert task.status == TASK_STATUS_DONE
        assert task.speed_up == pytest.approx(1234.5)
        assert task.updated_at is not None and hasattr(task.updated_at, "year")


@pytest.mark.asyncio
async def test_refresh_job_counters_counts_and_closes_job(tmp_path):
    session_factory = await make_session_factory(tmp_path)
    async with session_factory() as db:
        job_id = await _seed(db, [TASK_STATUS_DONE, TASK_STATUS_DONE, TASK_STATUS_FAILED])

        await refresh_job_counters(db, job_id)
        await db.commit()

        job = await db.get(Job, job_id, populate_existing=True)
        assert (job.total, job.done, job.failed) == (3, 2, 1)
        assert job.status == JOB_STATUS_DONE


@pytest.mark.asyncio
async def test_refresh_job_counters_keeps_job_running_while_tasks_pending(tmp_path):
    session_factory = await make_session_factory(tmp_path)
    async with session_factory() as db:
        job_id = await _seed(db, [TASK_STATUS_DONE, TASK_STATUS_PENDING])

        await refresh_job_counters(db, job_id)
        await db.commit()

        job = await db.get(Job, job_id, populate_existing=True)
        assert (job.total, job.done, job.failed) == (2, 1, 0)
        assert job.status == JOB_STATUS_RUNNING


@pytest.mark.asyncio
async def test_refresh_job_counters_does_not_resurrect_cancelled_job(tmp_path):
    session_factory = await make_session_factory(tmp_path)
    async with session_factory() as db:
        job_id = await _seed(db, [TASK_STATUS_DONE], job_status=JOB_STATUS_CANCELLED)

        await refresh_job_counters(db, job_id)
        await db.commit()

        job = await db.get(Job, job_id, populate_existing=True)
        assert job.status == JOB_STATUS_CANCELLED


@pytest.mark.asyncio
async def test_retried_task_reopens_finished_job(tmp_path):
    """Task failed được retry -> pending: job đã done phải mở lại thành running."""
    session_factory = await make_session_factory(tmp_path)
    async with session_factory() as db:
        job_id = await _seed(db, [TASK_STATUS_DONE, TASK_STATUS_FAILED])

        await refresh_job_counters(db, job_id)
        await db.commit()
        job = await db.get(Job, job_id, populate_existing=True)
        assert job.status == JOB_STATUS_DONE

        # retry: task failed quay lại pending
        failed_id = (
            await db.execute(select(Task.id).where(Task.status == TASK_STATUS_FAILED))
        ).scalar_one()
        await update_task_status(db, failed_id, TASK_STATUS_PENDING)
        await refresh_job_counters(db, job_id)
        await db.commit()

        job = await db.get(Job, job_id, populate_existing=True)
        assert job.status == JOB_STATUS_RUNNING
        assert (job.total, job.done, job.failed) == (2, 1, 0)
