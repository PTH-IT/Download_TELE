"""api/models.py — SQLAlchemy async models"""
from datetime import datetime
from sqlalchemy import (
    BigInteger, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    src_link   = Column(Text, nullable=False)
    dst_link   = Column(Text, nullable=False)
    src_title  = Column(String(255))
    dst_title  = Column(String(255))
    status     = Column(String(32), default="running", nullable=False)
    total      = Column(Integer, default=0)
    done       = Column(Integer, default=0)
    failed     = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    tasks = relationship("Task", back_populates="job", lazy="dynamic")


class Task(Base):
    __tablename__ = "tasks"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    job_id     = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    msg_id     = Column(BigInteger, nullable=False, index=True)
    caption    = Column(Text, default="")
    filename   = Column(String(512))
    file_path  = Column(String(512))
    status     = Column(String(32), default="pending", nullable=False, index=True)
    worker_id  = Column(String(128))
    attempt    = Column(Integer, default=0)
    error      = Column(Text)
    speed_dl   = Column(Float, default=0.0)   # bytes/sec
    speed_up   = Column(Float, default=0.0)
    size_bytes = Column(BigInteger, default=0)
    downloaded_bytes = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    job = relationship("Job", back_populates="tasks")


class Transferred(Base):
    """Lịch sử msg_id đã chuyển thành công — thay thế da_chuyen.txt"""
    __tablename__ = "transferred"

    msg_id    = Column(BigInteger, primary_key=True)
    job_id    = Column(Integer, ForeignKey("jobs.id"))
    done_at   = Column(DateTime, default=func.now())
