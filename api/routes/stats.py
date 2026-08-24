"""api/routes/stats.py"""
from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Task
from ..redis_client import get_redis
from shared.constants import QUEUE_DOWNLOAD, QUEUE_UPLOAD

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/realtime")
async def realtime_stats(
    redis=Depends(get_redis),
    db: AsyncSession = Depends(get_db),
):
    dl_queue = await redis.zcard(QUEUE_DOWNLOAD)
    up_queue = await redis.zcard(QUEUE_UPLOAD)

    # đếm theo trạng thái
    counts = {}
    for status in ("pending", "downloading", "uploading", "done", "failed"):
        r = await db.execute(
            select(func.count()).where(Task.status == status)
        )
        counts[status] = r.scalar()

    return {
        "queue_download": dl_queue,
        "queue_upload": up_queue,
        "tasks": counts,
    }


@router.get("/history")
async def history(limit: int = 50, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Task)
        .where(Task.status == "done")
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
