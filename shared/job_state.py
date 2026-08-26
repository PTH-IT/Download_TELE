"""shared/job_state.py — cập nhật trạng thái task/job trong DB.

Tách riêng khỏi worker.py để test được mà không cần import Pyrogram.
"""
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import Job, Task
from shared.constants import (
    JOB_STATUS_CANCELLED,
    JOB_STATUS_DONE,
    JOB_STATUS_RUNNING,
    TASK_STATUS_DONE,
    TASK_STATUS_DOWNLOADING,
    TASK_STATUS_FAILED,
    TASK_STATUS_PENDING,
    TASK_STATUS_UPLOADING,
)


async def update_task_status(db: AsyncSession, task_id: int, status: str, **fields):
    """Đổi trạng thái task.

    `updated_at` là cột DateTime — luôn truyền datetime, không bao giờ truyền
    time.time() (float), vì driver sẽ ném lỗi và làm hỏng cả transaction.
    """
    values = {"status": status, "updated_at": datetime.utcnow()}
    values.update(fields)
    await db.execute(update(Task).where(Task.id == task_id).values(**values))


async def job_is_cancelled(db: AsyncSession, job_id: int) -> bool:
    """CHỈ trạng thái cancelled mới được phép bỏ việc đang làm.

    Không dùng "có phải đang running không": job bị đóng sớm thành done (hoặc
    vừa retry xong chưa kịp mở lại) sẽ khiến task tải xong 100% rồi bị đánh
    cancelled, vứt luôn file vừa tải.
    """
    res = await db.execute(select(Job.status).where(Job.id == job_id))
    return res.scalar_one_or_none() == JOB_STATUS_CANCELLED


async def count_task_statuses(db: AsyncSession, job_id: int) -> dict[str, int]:
    res = await db.execute(
        select(Task.status, func.count()).where(Task.job_id == job_id).group_by(Task.status)
    )
    return {status: int(n) for status, n in res.all()}


async def refresh_job_counters(db: AsyncSession, job_id: int) -> dict[str, int]:
    """Tính lại total/done/failed từ bảng tasks — luôn khớp, không bị lệch."""
    counts = await count_task_statuses(db, job_id)
    total = sum(counts.values())
    active = (
        counts.get(TASK_STATUS_PENDING, 0)
        + counts.get(TASK_STATUS_DOWNLOADING, 0)
        + counts.get(TASK_STATUS_UPLOADING, 0)
    )
    values = {
        "total": total,
        "done": counts.get(TASK_STATUS_DONE, 0),
        "failed": counts.get(TASK_STATUS_FAILED, 0),
        "updated_at": datetime.utcnow(),
    }

    stmt = update(Job).where(Job.id == job_id)
    if total > 0 and active == 0:
        # chỉ đóng job đang chạy, không đụng vào job đã cancel
        values["status"] = JOB_STATUS_DONE
        stmt = stmt.where(Job.status == JOB_STATUS_RUNNING)
    elif active > 0:
        # Task failed được retry sẽ quay lại pending. Nếu job đã bị đóng trước
        # đó thì phải mở lại, nếu không dashboard báo "done" trong khi worker
        # vẫn đang chạy hàng trăm task.
        values["status"] = JOB_STATUS_RUNNING
        stmt = stmt.where(Job.status == JOB_STATUS_DONE)
        await db.execute(stmt.values(**values))
        # câu lệnh trên chỉ chạm job đang done; cập nhật counter cho phần còn lại
        values.pop("status")
        stmt = update(Job).where(Job.id == job_id)

    await db.execute(stmt.values(**values))
    return values
