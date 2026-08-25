"""api/routes/stats.py"""
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Task
from ..redis_client import get_redis
from shared.constants import (
    QUEUE_DOWNLOAD,
    QUEUE_UPLOAD,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_DONE,
    TASK_STATUS_DOWNLOADING,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_UPLOADING,
)

router = APIRouter(prefix="/api/stats", tags=["stats"])

_STATUSES = (
    TASK_STATUS_PENDING,
    TASK_STATUS_DOWNLOADING,
    TASK_STATUS_UPLOADING,
    TASK_STATUS_DONE,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
)


@router.get("/realtime")
async def realtime_stats(
    job_id: Optional[int] = None,
    redis=Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    dl_queue = await redis.zcard(QUEUE_DOWNLOAD)
    up_queue = await redis.zcard(QUEUE_UPLOAD)

    # Một truy vấn GROUP BY thay cho 5 lần COUNT
    q = select(Task.status, func.count()).group_by(Task.status)
    if job_id is not None:
        q = q.where(Task.job_id == job_id)
    result = await db.execute(q)
    grouped = {status: int(n) for status, n in result.all()}

    return {
        "queue_download": dl_queue,
        "queue_upload": up_queue,
        "tasks": {status: grouped.get(status, 0) for status in _STATUSES},
    }


@router.get("/history")
async def history(limit: int = 50, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Task)
        .where(Task.status == TASK_STATUS_DONE)
        .order_by(Task.updated_at.desc())
        .limit(limit)
    )
    tasks = result.scalars().all()
    return [
        {
            "msg_id": t.msg_id,
            "job_id": t.job_id,
            "size_mb": round(t.size_bytes / (1 << 20), 1) if t.size_bytes else 0,
            "done_at": t.updated_at.isoformat() if t.updated_at else None,
        }
        for t in tasks
    ]
