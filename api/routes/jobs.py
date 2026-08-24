"""api/routes/jobs.py"""
import json
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import Job, Task, Transferred
from ..redis_client import get_redis
from ..task_utils import can_retry_task_status, normalize_status
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))
from shared.constants import (
    JOB_STATUS_CANCELLED, TASK_STATUS_CANCELLED,
    TASK_STATUS_PENDING, QUEUE_DOWNLOAD,
)

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


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


@router.get("", response_model=List[JobOut])
async def list_jobs(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Job).order_by(Job.created_at.desc()).limit(100))
    jobs = result.scalars().all()
    return [
        {
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
        for job in jobs
    ]


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
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


@router.post("", response_model=JobOut, status_code=201)
async def create_job(
    body: JobCreate,
    db: AsyncSession = Depends(get_db),
    redis=Depends(get_redis),
):
    job = Job(src_link=body.src_link, dst_link=body.dst_link, status="running")
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
    await redis.lpush("queue:new_job", json.dumps(payload))
    return job


@router.delete("/{job_id}/cancel")
async def cancel_job(job_id: int, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(404, "Job không tồn tại")
    job.status = JOB_STATUS_CANCELLED
    await db.execute(
        update(Task)
        .where(Task.job_id == job_id, Task.status == TASK_STATUS_PENDING)
        .values(status=TASK_STATUS_CANCELLED)
    )
    await db.commit()
    return {"ok": True}


@router.post("/{job_id}/tasks/{task_id}/retry")
async def retry_task(job_id: int, task_id: int, db: AsyncSession = Depends(get_db), redis=Depends(get_redis)):
    task = await db.get(Task, task_id)
    if not task or task.job_id != job_id:
        raise HTTPException(404, "Task không tồn tại")
    if not can_retry_task_status(task.status):
        raise HTTPException(400, "Task này không thể retry")

    task.status = TASK_STATUS_PENDING
    task.error = None
    task.worker_id = None
    task.updated_at = None
    await db.commit()

    await redis.zadd(QUEUE_DOWNLOAD, {str(task.id): 0})
    return {"ok": True, "task_id": task_id}


@router.get("/{job_id}/tasks")
async def job_tasks(
    job_id: int,
    status: Optional[str] = None,
    limit: int = 200,
    db: AsyncSession = Depends(get_db),
):
    q = select(Task).where(Task.job_id == job_id).order_by(Task.id.desc()).limit(limit)
    if status:
        q = q.where(Task.status == status)
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
