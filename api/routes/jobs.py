"""api/routes/jobs.py"""
import json
import time
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Job, Task
from ..redis_client import get_redis
from ..task_utils import can_retry_task_status
from shared.constants import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_RUNNING,
    QUEUE_DOWNLOAD,
    QUEUE_NEW_JOB,
    QUEUE_UPLOAD,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    WORKER_HEARTBEAT,
    WORKER_STATUS,
)
from shared.peers import normalize_peer

router = APIRouter(prefix="/api/jobs", tags=["jobs"])

HEARTBEAT_TIMEOUT = 30


class JobCreate(BaseModel):
    src_link: str
    dst_link: str
    from_msg_id: Optional[int] = None
    to_msg_id: Optional[int] = None


class JobOut(BaseModel):
    id: int
    src_link: str
    dst_link: str
    src_title: Optional[str]
    dst_title: Optional[str]
    status: str
    total: int
    done: int
    failed: int

    class Config:
        from_attributes = True


def _job_dict(job: Job) -> dict:
    return {
        "id": job.id,
        "src_link": job.src_link,
        "dst_link": job.dst_link,
        "src_title": job.src_title,
        "dst_title": job.dst_title,
        "status": job.status,
        "total": job.total or 0,
        "done": job.done or 0,
        "failed": job.failed or 0,
    }


async def _ready_workers(redis) -> int:
    """Worker còn heartbeat VÀ đã đăng nhập Telegram xong."""
    heartbeats = await redis.hgetall(WORKER_HEARTBEAT)
    statuses = await redis.hgetall(WORKER_STATUS)
    now = time.time()
    ready = 0
    for worker_id, ts in heartbeats.items():
        try:
            if now - float(ts) >= HEARTBEAT_TIMEOUT:
                continue
        except (TypeError, ValueError):
            continue
        try:
            state = json.loads(statuses.get(worker_id) or "{}")
        except ValueError:
            state = {}
        if state.get("session") == "ready":
            ready += 1
    return ready


@router.get("", response_model=List[JobOut])
async def list_jobs(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).order_by(Job.created_at.desc()).limit(100))
    return [_job_dict(job) for job in result.scalars().all()]


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
    return _job_dict(job)


@router.post("", response_model=JobOut, status_code=201)
async def create_job(
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
):
    # Validate sớm: link sai định dạng sẽ làm worker fail từng task một cách khó hiểu
    try:
        normalize_peer(body.src_link)
        normalize_peer(body.dst_link)
    except ValueError as exc:
        raise HTTPException(400, f"Link chat không hợp lệ: {exc}")

    if body.from_msg_id is not None and body.to_msg_id is None:
        raise HTTPException(400, "Cần nhập cả from_msg_id và to_msg_id, hoặc bỏ trống cả hai")
    if body.to_msg_id is not None and body.from_msg_id is None:
        raise HTTPException(400, "Cần nhập cả from_msg_id và to_msg_id, hoặc bỏ trống cả hai")

    if await _ready_workers(redis) == 0:
        raise HTTPException(
            503,
            "Chưa có worker nào sẵn sàng — kiểm tra worker đã chạy và đã đăng nhập Telegram chưa",
        )

    job = Job(src_link=body.src_link, dst_link=body.dst_link, status=JOB_STATUS_RUNNING)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Gửi lệnh tới worker qua Redis — worker sẽ scan history và enqueue tasks
    payload = {
        "job_id": job.id,
        "src_link": body.src_link,
        "dst_link": body.dst_link,
        "from_msg_id": body.from_msg_id,
        "to_msg_id": body.to_msg_id,
    }
    await redis.lpush(QUEUE_NEW_JOB, json.dumps(payload))
    return _job_dict(job)


@router.delete("/{job_id}/cancel")
async def cancel_job(job_id: int, db: AsyncSession = Depends(get_db), redis=Depends(get_redis)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")

    result = await db.execute(
        select(Task.id).where(Task.job_id == job_id, Task.status == TASK_STATUS_PENDING)
    )
    pending_ids = [str(row[0]) for row in result.all()]

    job.status = JOB_STATUS_CANCELLED
    await db.execute(
        update(Task)
        .where(Task.job_id == job_id, Task.status == TASK_STATUS_PENDING)
        .values(status=TASK_STATUS_CANCELLED)
    )
    await db.commit()

    # Gỡ luôn khỏi hàng đợi Redis, nếu không worker vẫn pop ra rồi mới bỏ
    if pending_ids:
        await redis.zrem(QUEUE_DOWNLOAD, *pending_ids)
        await redis.zrem(QUEUE_UPLOAD, *pending_ids)

    return {"ok": True, "cancelled": len(pending_ids)}


@router.post("/{job_id}/tasks/{task_id}/retry")
async def retry_task(
    job_id: int,
    task_id: int,
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
):
    task = await db.get(Task, task_id)
    if not task or task.job_id != job_id:
        raise HTTPException(404, "Task không tồn tại")
    if not can_retry_task_status(task.status):
        raise HTTPException(400, "Task này không thể retry")

    task.status = TASK_STATUS_PENDING
    task.error = None
    task.worker_id = None
    task.attempt = 0
    await db.commit()

    await redis.zadd(QUEUE_DOWNLOAD, {str(task.id): 0})
    return {"ok": True, "task_id": task_id}


@router.post("/{job_id}/retry_failed")
async def retry_failed_tasks(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
):
    """Đẩy lại toàn bộ task failed/cancelled của job vào hàng đợi."""
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")

    result = await db.execute(
        select(Task.id).where(
            Task.job_id == job_id,
            Task.status.in_([TASK_STATUS_FAILED, TASK_STATUS_CANCELLED]),
        )
    )
    task_ids = [row[0] for row in result.all()]
    if not task_ids:
        return {"ok": True, "requeued": 0}

    await db.execute(
        update(Task)
        .where(Task.id.in_(task_ids))
        .values(status=TASK_STATUS_PENDING, error=None, worker_id=None, attempt=0)
    )
    job.status = JOB_STATUS_RUNNING
    await db.commit()

    await redis.zadd(QUEUE_DOWNLOAD, {str(tid): 0 for tid in task_ids})
    return {"ok": True, "requeued": len(task_ids)}


@router.get("/{job_id}/tasks")
async def job_tasks(
    job_id: int,
    status: Optional[str] = None,
    limit: int = 200,
    db: AsyncSession = Depends(get_db),
):
    q = select(Task).where(Task.job_id == job_id)
    if status:
        q = q.where(Task.status == status.lower())
    q = q.order_by(Task.id.desc()).limit(limit)
    result = await db.execute(q)
    tasks = result.scalars().all()
    return [
        {
            "id": t.id,
            "msg_id": t.msg_id,
            "status": t.status,
            "worker_id": t.worker_id,
            "speed_dl": round(t.speed_dl / (1 << 20), 2) if t.speed_dl else 0,
            "speed_up": round(t.speed_up / (1 << 20), 2) if t.speed_up else 0,
            "size_mb": round(t.size_bytes / (1 << 20), 1) if t.size_bytes else 0,
            "error": t.error,
            "updated_at": t.updated_at.isoformat() if t.updated_at else None,
        }
        for t in tasks
    ]
