"""worker/worker.py — tải media từ Telegram và upload sang chat đích.

Mỗi container chạy 1 process worker. Nhiều worker cùng lấy việc từ 2 hàng đợi
Redis (QUEUE_DOWNLOAD / QUEUE_UPLOAD) nên có thể scale ngang bằng
`docker compose up -d --scale worker=N`.
"""
import asyncio
import json
import logging
import os
import socket
import time
from typing import Optional, Union

import redis.asyncio as aioredis
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import AsyncSessionLocal, init_db
from api.models import Job, Task, Transferred
from api.session_paths import resolve_session_dir, resolve_session_file
from shared.constants import (
    QUEUE_DOWNLOAD,
    QUEUE_UPLOAD,
    QUEUE_NEW_JOB,
    LOCK_PREFIX,
    AUTH_LOCK_KEY,
    AUTH_SESSION_KEY,
    AUTH_OTP_QUEUE,
    AUTH_OTP_REQ_QUEUE,
    WORKER_HEARTBEAT,
    WORKER_STATUS,
    PUBSUB_PROGRESS,
    TASK_STATUS_PENDING,
    TASK_STATUS_DOWNLOADING,
    TASK_STATUS_UPLOADING,
    TASK_STATUS_DONE,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
    JOB_STATUS_CANCELLED,
    JOB_STATUS_RUNNING,
)
from shared.job_state import refresh_job_counters, update_task_status
from shared.peers import normalize_peer
from shared.phone import InvalidPhoneNumber, mask_phone, normalize_phone

try:
    from .media_utils import (
        get_media_extension,
        get_media_kind,
        get_media_size,
        guess_kind_from_path,
        is_supported_media,
    )
except ImportError:  # pragma: no cover - chạy trực tiếp `python worker/worker.py`
    from media_utils import (
        get_media_extension,
        get_media_kind,
        get_media_size,
        guess_kind_from_path,
        is_supported_media,
    )

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("worker")

# ---- Config from env ----
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# WORKER_ID phải là duy nhất cho mỗi process. Khi scale bằng docker compose,
# hostname chính là container id nên mặc định này luôn khác nhau. Nếu hard-code
# WORKER_ID trong compose thì mọi replica sẽ ghi đè heartbeat của nhau và
# dashboard chỉ thấy đúng 1 worker.
WORKER_ID = os.getenv("WORKER_ID") or f"worker-{socket.gethostname()}"

# Bắt buộc lấy từ biến môi trường — không hard-code credentials trong source.
# Tạo app tại https://my.telegram.org/apps để có API_ID / API_HASH.
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = (os.getenv("API_HASH") or "").strip()


def _require_credentials() -> int:
    if not API_ID_RAW or not API_HASH:
        raise SystemExit(
            "Thiếu API_ID / API_HASH. Đặt chúng trong file .env "
            "(xem .env.example) hoặc trong environment của service worker."
        )
    try:
        return int(API_ID_RAW)
    except ValueError:
        raise SystemExit(f"API_ID phải là số, nhận được: {API_ID_RAW!r}")

# Cho phép mỗi worker dùng một tài khoản Telegram riêng. Dùng chung 1 session
# string cho nhiều worker có thể bị Telegram huỷ auth key (AUTH_KEY_DUPLICATED).
SESSION_STRING_ENV = (os.getenv("SESSION_STRING") or "").strip()

SESSION_DIR = str(resolve_session_dir())
SESSION_FILE = str(resolve_session_file())

DEFAULT_MAX_DL = int(os.getenv("MAX_DL", "2"))
DEFAULT_MAX_UP = int(os.getenv("MAX_UP", "4"))

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/app/downloads")
DELETE_AFTER_UPLOAD = os.getenv("DELETE_AFTER_UPLOAD", "1") not in ("0", "false", "False")

MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
LOCK_TTL = int(os.getenv("TASK_LOCK_TTL", "3600"))
SCAN_CAP = int(os.getenv("SCAN_CAP", "200"))
PROGRESS_INTERVAL = float(os.getenv("PROGRESS_INTERVAL", "1.0"))
CAPTION_LIMIT = 1024

JANITOR_LOCK = "lock:janitor"
AUTH_ERROR_KEY = "auth:last_error"
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "")
# Yêu cầu OTP cũ hơn ngần này bị bỏ qua — nếu không, worker khởi động lại sẽ
# phát lại yêu cầu tồn trong Redis và gửi OTP ngoài ý muốn.
AUTH_REQUEST_MAX_AGE = int(os.getenv("AUTH_REQUEST_MAX_AGE", "120"))
# TTL ngắn + gia hạn liên tục: auth master chết thì worker khác tiếp quản sau
# tối đa 60 giây thay vì kẹt 5 phút.
AUTH_LOCK_TTL = int(os.getenv("AUTH_LOCK_TTL", "60"))
# Heartbeat cũ hơn ngần này bị xoá khỏi dashboard
WORKER_STALE_AFTER = int(os.getenv("WORKER_STALE_AFTER", "600"))


class AdaptiveSemaphore:
    """Semaphore có thể đổi limit lúc đang chạy (lệnh set_limits từ dashboard)."""

    def __init__(self, limit: int):
        self._limit = max(1, int(limit))
        self._count = 0
        self._cond = asyncio.Condition()

    async def acquire(self):
        async with self._cond:
            while self._count >= self._limit:
                await self._cond.wait()
            self._count += 1

    async def release(self):
        async with self._cond:
            self._count = max(0, self._count - 1)
            self._cond.notify_all()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # release phải được await; nếu fire-and-forget bằng create_task thì khi
        # loop đang shutdown counter sẽ không bao giờ được trả lại.
        await self.release()

    async def set_limit(self, new_limit: int):
        async with self._cond:
            self._limit = max(1, int(new_limit))
            self._cond.notify_all()

    @property
    def limit(self) -> int:
        return self._limit


class WorkerStats:
    """Số liệu hiển thị trên dashboard."""

    def __init__(self):
        self.session_state = "waiting_auth"
        self.dl_speed = 0.0
        self.up_speed = 0.0
        self.total_done = 0
        self.total_failed = 0
        self.active_dl = 0
        self.active_up = 0
        self.current_task: Optional[int] = None

    def snapshot(self, dl_limit: int, up_limit: int) -> dict:
        return {
            "session": self.session_state,
            "current_task": self.current_task,
            "dl_speed_mbs": round(self.dl_speed / (1 << 20), 2),
            "up_speed_mbs": round(self.up_speed / (1 << 20), 2),
            "total_done": self.total_done,
            "total_failed": self.total_failed,
            "active_dl": self.active_dl,
            "active_up": self.active_up,
            "max_dl": dl_limit,
            "max_up": up_limit,
        }


# --------------------------------------------------------------------------
# Redis helpers
# --------------------------------------------------------------------------
_PEER_CACHE: dict[str, Union[int, str]] = {}


async def resolve_chat(app: Client, raw) -> Union[int, str]:
    """Đổi link/username/id thành chat id mà send_* dùng được.

    Link mời (t.me/+hash) chỉ có get_chat hiểu được, send_video thì không.
    Gọi get_chat còn có tác dụng nạp peer vào session của chính worker này —
    worker khác chưa từng thấy chat sẽ báo PEER_ID_INVALID nếu bỏ qua bước đó.
    """
    key = str(raw)
    if key in _PEER_CACHE:
        return _PEER_CACHE[key]

    peer = normalize_peer(raw)
    try:
        chat = await app.get_chat(peer)
        resolved = chat.id
    except Exception as exc:
        log.warning("Không resolve được chat %r qua get_chat: %s", raw, exc)
        resolved = peer
    _PEER_CACHE[key] = resolved
    return resolved


async def enqueue_progress(r: aioredis.Redis, payload: dict):
    try:
        await r.publish(PUBSUB_PROGRESS, json.dumps(payload, ensure_ascii=False))
    except Exception as exc:  # pub/sub hỏng không được phép giết task
        log.debug("publish progress lỗi: %s", exc)


async def try_acquire_lock(r: aioredis.Redis, lock_key: str, ttl_sec: int = LOCK_TTL) -> bool:
    return bool(await r.set(lock_key, WORKER_ID, nx=True, ex=ttl_sec))


def _is_stale_request(payload: dict) -> bool:
    ts = payload.get("timestamp")
    if not ts:
        return False
    try:
        return (time.time() - float(ts)) > AUTH_REQUEST_MAX_AGE
    except (TypeError, ValueError):
        return False


async def release_lock(r: aioredis.Redis, lock_key: str):
    try:
        await r.delete(lock_key)
    except Exception:
        pass


async def heartbeat_loop(
    r: aioredis.Redis,
    stats: WorkerStats,
    dl_sem: AdaptiveSemaphore,
    up_sem: AdaptiveSemaphore,
):
    while True:
        try:
            await r.hset(WORKER_HEARTBEAT, WORKER_ID, str(time.time()))
            await r.hset(
                WORKER_STATUS,
                WORKER_ID,
                json.dumps(stats.snapshot(dl_sem.limit, up_sem.limit), ensure_ascii=False),
            )
            log.debug("Heartbeat %s", WORKER_ID)
        except Exception as exc:
            log.warning("Heartbeat lỗi: %s", exc)
        await asyncio.sleep(5)


async def command_listener(
    r: aioredis.Redis,
    dl_sem: AdaptiveSemaphore,
    up_sem: AdaptiveSemaphore,
    stop_event: asyncio.Event,
):
    pubsub = r.pubsub()
    # Kênh riêng theo worker + kênh broadcast cho toàn bộ worker
    await pubsub.subscribe(f"cmd:{WORKER_ID}", "cmd:all")
    try:
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            try:
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                payload = json.loads(data)
            except Exception:
                payload = {}

            if payload.get("action") == "stop":
                log.info("Nhận lệnh stop")
                stop_event.set()
                return

            max_dl = payload.get("max_dl")
            max_up = payload.get("max_up")
            if max_dl is not None:
                await dl_sem.set_limit(int(max_dl))
            if max_up is not None:
                await up_sem.set_limit(int(max_up))
            if max_dl is not None or max_up is not None:
                log.info("Đổi limit: max_dl=%s max_up=%s", dl_sem.limit, up_sem.limit)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("command_listener dừng: %s", exc)
    finally:
        try:
            await pubsub.unsubscribe(f"cmd:{WORKER_ID}", "cmd:all")
        except Exception:
            pass
        try:
            await pubsub.aclose()
        except Exception:
            pass


# --------------------------------------------------------------------------
# DB helpers
# --------------------------------------------------------------------------
async def fail_or_retry(
    r: aioredis.Redis,
    db: AsyncSession,
    task: Task,
    queue: str,
    error: str,
    stats: WorkerStats,
):
    """Task lỗi: còn lượt thì đẩy lại hàng đợi, hết lượt thì đánh failed."""
    attempt = (task.attempt or 0) + 1
    if attempt < MAX_ATTEMPTS:
        await update_task_status(
            db, task.id, TASK_STATUS_PENDING, attempt=attempt, error=error[:2000]
        )
        await db.commit()
        await r.zadd(queue, {str(task.id): time.time() + 30})
        log.warning("Task %s lỗi (lần %s/%s), sẽ thử lại: %s", task.id, attempt, MAX_ATTEMPTS, error)
        return

    await update_task_status(db, task.id, TASK_STATUS_FAILED, attempt=attempt, error=error[:2000])
    await refresh_job_counters(db, task.job_id)
    await db.commit()
    stats.total_failed += 1
    log.error("Task %s failed sau %s lần: %s", task.id, attempt, error)
    await enqueue_progress(
        r,
        {
            "worker_id": WORKER_ID,
            "type": "task_failed",
            "task_id": task.id,
            "job_id": task.job_id,
            "error": error[:300],
        },
    )


def make_progress_cb(r: aioredis.Redis, kind: str, task_id: int, job_id: int, stats: WorkerStats):
    """Progress callback có throttle — không spam Redis mỗi chunk."""
    state = {"last": 0.0, "start": time.time()}

    async def _cb(current: int, total: int):
        now = time.time()
        if now - state["last"] < PROGRESS_INTERVAL and current != total:
            return
        state["last"] = now
        speed = current / max(0.001, now - state["start"])
        if kind == "dl":
            stats.dl_speed = speed
        else:
            stats.up_speed = speed
        await enqueue_progress(
            r,
            {
                "worker_id": WORKER_ID,
                "type": kind,
                "task_id": task_id,
                "job_id": job_id,
                "current": current,
                "total": total,
                "speed": speed,
            },
        )

    return _cb


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
async def _handle_download(
    app: Client,
    r: aioredis.Redis,
    db: AsyncSession,
    dl_sem: AdaptiveSemaphore,
    stats: WorkerStats,
    task_id: int,
):
    task = await db.get(Task, task_id)
    if not task:
        log.warning("Task %s không tồn tại, bỏ qua", task_id)
        return
    if task.status in (TASK_STATUS_CANCELLED, TASK_STATUS_DONE):
        return

    job = await db.get(Job, task.job_id)
    if not job:
        await update_task_status(db, task_id, TASK_STATUS_FAILED, error="Job không tồn tại")
        await db.commit()
        return
    if job.status == JOB_STATUS_CANCELLED:
        await update_task_status(db, task_id, TASK_STATUS_CANCELLED)
        await db.commit()
        return

    await update_task_status(db, task_id, TASK_STATUS_DOWNLOADING, worker_id=WORKER_ID, error=None)
    await db.commit()
    stats.current_task = task_id

    try:
        # job.src_link là chuỗi ("-1001234..." hoặc link t.me). Truyền thẳng cho
        # Pyrogram sẽ bị hiểu là username -> PEER_ID_INVALID.
        src_peer = await resolve_chat(app, job.src_link)
        msg = await app.get_messages(src_peer, task.msg_id)
        if not msg or getattr(msg, "empty", False):
            raise RuntimeError(f"Không lấy được message {task.msg_id} từ {job.src_link}")
        if not is_supported_media(msg):
            raise RuntimeError(f"Message {task.msg_id} không phải media hỗ trợ")

        media_kind = get_media_kind(msg)
        # Tên file có prefix job_id: cùng msg_id ở 2 job khác nhau sẽ không ghi đè nhau
        filename = task.filename or f"{job.id}_{task.msg_id}{get_media_extension(msg)}"
        local_path = os.path.join(DOWNLOAD_DIR, filename)
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        stats.active_dl += 1
        try:
            async with dl_sem:
                start = time.time()
                path = await app.download_media(
                    msg,
                    file_name=local_path,
                    progress=make_progress_cb(r, "dl", task_id, job.id, stats),
                )
        finally:
            stats.active_dl -= 1

        # download_media nuốt exception và trả None khi tải lỗi
        if not path or not os.path.exists(path):
            raise RuntimeError("Tải file thất bại (Pyrogram trả về None)")

        elapsed = max(0.001, time.time() - start)
        size = os.path.getsize(path)
        speed = size / elapsed
        stats.dl_speed = speed

        await update_task_status(
            db,
            task_id,
            TASK_STATUS_UPLOADING,
            file_path=path,
            filename=os.path.basename(path),
            media_kind=media_kind,
            speed_dl=speed,
            size_bytes=size or get_media_size(msg),
            downloaded_bytes=size,
            caption=(msg.caption or task.caption or "")[:CAPTION_LIMIT],
        )
        await db.commit()

        # Job có thể bị huỷ trong lúc đang tải — bỏ file thay vì upload thừa
        if not await _job_is_active(db, job.id):
            await update_task_status(db, task_id, TASK_STATUS_CANCELLED)
            await db.commit()
            try:
                os.remove(path)
            except OSError:
                pass
            log.info("Job %s đã huỷ, bỏ task %s sau khi tải", job.id, task_id)
            return

        # Đẩy sang hàng đợi upload dùng chung -> worker nào rảnh cũng upload được
        await r.zadd(QUEUE_UPLOAD, {str(task_id): time.time()})
        await enqueue_progress(
            r, {"worker_id": WORKER_ID, "type": "dl_done", "task_id": task_id, "job_id": job.id}
        )
        log.info("Tải xong task %s (%.1f MB, %.2f MB/s)", task_id, size / (1 << 20), speed / (1 << 20))

    except FloodWait as e:
        wait = int(getattr(e, "value", 30)) + 1
        log.warning("FloodWait %ss khi tải task %s", wait, task_id)
        await db.rollback()
        await update_task_status(db, task_id, TASK_STATUS_PENDING)
        await db.commit()
        await r.zadd(QUEUE_DOWNLOAD, {str(task_id): time.time() + wait})
    except asyncio.CancelledError:
        await db.rollback()
        await r.zadd(QUEUE_DOWNLOAD, {str(task_id): time.time()})
        raise
    except Exception as e:
        await db.rollback()
        task = await db.get(Task, task_id)
        if task:
            await fail_or_retry(r, db, task, QUEUE_DOWNLOAD, f"{type(e).__name__}: {e}", stats)
    finally:
        stats.current_task = None


async def _next_queue_item(r: aioredis.Redis, queue: str) -> Optional[int]:
    """Lấy task kế tiếp từ ZSET, tôn trọng score dùng làm 'không chạy trước lúc'."""
    item = await r.zpopmin(queue, count=1)
    if not item:
        return None

    member, score = item[0]
    if isinstance(member, bytes):
        member = member.decode("utf-8")
    try:
        task_id = int(member)
    except ValueError:
        return None

    if float(score) > time.time():
        # chưa tới lượt (retry/FloodWait) -> trả lại hàng đợi
        await r.zadd(queue, {member: score})
        await asyncio.sleep(0.5)
        return None
    return task_id


async def download_worker(
    app: Client,
    r: aioredis.Redis,
    dl_sem: AdaptiveSemaphore,
    stop_event: asyncio.Event,
    stats: WorkerStats,
):
    while not stop_event.is_set():
        try:
            task_id = await _next_queue_item(r, QUEUE_DOWNLOAD)
        except Exception as exc:
            log.warning("Đọc queue download lỗi: %s", exc)
            await asyncio.sleep(1)
            continue

        if task_id is None:
            await asyncio.sleep(0.3)
            continue

        lock_key = f"{LOCK_PREFIX}{task_id}"
        if not await try_acquire_lock(r, lock_key):
            # worker khác đang giữ task này -> hoãn lại, tránh vòng lặp nóng
            await r.zadd(QUEUE_DOWNLOAD, {str(task_id): time.time() + 30})
            await asyncio.sleep(0.5)
            continue

        try:
            # Session mới cho mỗi task: tránh identity-map trả về dữ liệu cũ và
            # tránh transaction hỏng lây sang task kế tiếp.
            async with AsyncSessionLocal() as db:
                await _handle_download(app, r, db, dl_sem, stats, task_id)
        except asyncio.CancelledError:
            await release_lock(r, lock_key)
            raise
        except Exception as exc:
            log.exception("download_worker lỗi ngoài dự kiến ở task %s: %s", task_id, exc)
        finally:
            await release_lock(r, lock_key)


# --------------------------------------------------------------------------
# Upload
# --------------------------------------------------------------------------
async def _send_media(
    app: Client,
    dst_peer: Union[int, str],
    kind: str,
    path: str,
    caption: str,
    progress,
):
    if kind == "video":
        return await app.send_video(
            dst_peer, video=path, caption=caption, supports_streaming=True, progress=progress
        )
    if kind == "photo":
        return await app.send_photo(dst_peer, photo=path, caption=caption, progress=progress)
    if kind == "audio":
        return await app.send_audio(dst_peer, audio=path, caption=caption, progress=progress)
    return await app.send_document(dst_peer, document=path, caption=caption, progress=progress)


async def _mark_transferred(db: AsyncSession, job_id: int, msg_id: int):
    """Ghi lịch sử đã chuyển, an toàn với bản ghi trùng."""
    existing = await db.get(Transferred, (job_id, msg_id))
    if existing:
        return
    try:
        async with db.begin_nested():
            db.add(Transferred(job_id=job_id, msg_id=msg_id))
            # flush trong savepoint để IntegrityError bị bắt tại đây,
            # không làm hỏng transaction chính lúc commit
            await db.flush()
    except Exception as exc:  # race giữa 2 worker
        log.debug("Transferred đã tồn tại (%s/%s): %s", job_id, msg_id, exc)


async def _handle_upload(
    app: Client,
    r: aioredis.Redis,
    db: AsyncSession,
    up_sem: AdaptiveSemaphore,
    stats: WorkerStats,
    task_id: int,
):
    task = await db.get(Task, task_id)
    if not task:
        return
    if task.status in (TASK_STATUS_CANCELLED, TASK_STATUS_DONE):
        return

    job = await db.get(Job, task.job_id)
    if not job:
        await update_task_status(db, task_id, TASK_STATUS_FAILED, error="Job không tồn tại")
        await db.commit()
        return
    if job.status == JOB_STATUS_CANCELLED:
        await update_task_status(db, task_id, TASK_STATUS_CANCELLED)
        await db.commit()
        return

    path = task.file_path
    if not path or not os.path.exists(path):
        # file mất -> tải lại từ đầu thay vì fail thẳng
        await fail_or_retry(
            r, db, task, QUEUE_DOWNLOAD, f"File không tồn tại để upload: {path}", stats
        )
        return

    await update_task_status(db, task_id, TASK_STATUS_UPLOADING, worker_id=WORKER_ID)
    await db.commit()
    stats.current_task = task_id

    try:
        dst_peer = await resolve_chat(app, job.dst_link)
        caption = (task.caption or "")[:CAPTION_LIMIT]
        kind = task.media_kind or guess_kind_from_path(path)

        stats.active_up += 1
        try:
            async with up_sem:
                start = time.time()
                await _send_media(
                    app,
                    dst_peer,
                    kind,
                    path,
                    caption,
                    make_progress_cb(r, "up", task_id, job.id, stats),
                )
        finally:
            stats.active_up -= 1

        elapsed = max(0.001, time.time() - start)
        size = os.path.getsize(path) if os.path.exists(path) else (task.size_bytes or 0)
        speed = size / elapsed
        stats.up_speed = speed

        # LƯU Ý: updated_at là cột DateTime. Truyền time.time() (float) vào đây
        # sẽ ném lỗi ở tầng driver, rồi commit trong except gây PendingRollbackError
        # và giết luôn vòng lặp upload — đó là lý do upload "im lặng" không chạy.
        await update_task_status(db, task_id, TASK_STATUS_DONE, speed_up=speed, error=None)
        await _mark_transferred(db, task.job_id, task.msg_id)
        await refresh_job_counters(db, task.job_id)
        await db.commit()

        stats.total_done += 1

        if DELETE_AFTER_UPLOAD:
            try:
                os.remove(path)
            except OSError:
                pass

        await enqueue_progress(
            r, {"worker_id": WORKER_ID, "type": "up_done", "task_id": task_id, "job_id": job.id}
        )
        log.info("Upload xong task %s (%.2f MB/s)", task_id, speed / (1 << 20))

    except FloodWait as e:
        wait = int(getattr(e, "value", 30)) + 1
        log.warning("FloodWait %ss khi upload task %s", wait, task_id)
        await db.rollback()
        await update_task_status(db, task_id, TASK_STATUS_UPLOADING)
        await db.commit()
        await r.zadd(QUEUE_UPLOAD, {str(task_id): time.time() + wait})
    except asyncio.CancelledError:
        await db.rollback()
        await r.zadd(QUEUE_UPLOAD, {str(task_id): time.time()})
        raise
    except Exception as e:
        await db.rollback()
        task = await db.get(Task, task_id)
        if task:
            await fail_or_retry(r, db, task, QUEUE_UPLOAD, f"{type(e).__name__}: {e}", stats)
    finally:
        stats.current_task = None


async def upload_worker(
    app: Client,
    r: aioredis.Redis,
    up_sem: AdaptiveSemaphore,
    stop_event: asyncio.Event,
    stats: WorkerStats,
):
    while not stop_event.is_set():
        try:
            task_id = await _next_queue_item(r, QUEUE_UPLOAD)
        except Exception as exc:
            log.warning("Đọc queue upload lỗi: %s", exc)
            await asyncio.sleep(1)
            continue

        if task_id is None:
            await asyncio.sleep(0.3)
            continue

        lock_key = f"{LOCK_PREFIX}{task_id}"
        if not await try_acquire_lock(r, lock_key):
            await r.zadd(QUEUE_UPLOAD, {str(task_id): time.time() + 30})
            await asyncio.sleep(0.5)
            continue

        try:
            async with AsyncSessionLocal() as db:
                await _handle_upload(app, r, db, up_sem, stats, task_id)
        except asyncio.CancelledError:
            await release_lock(r, lock_key)
            raise
        except Exception as exc:
            log.exception("upload_worker lỗi ngoài dự kiến ở task %s: %s", task_id, exc)
        finally:
            await release_lock(r, lock_key)


# --------------------------------------------------------------------------
# Nhận job mới từ API
# --------------------------------------------------------------------------
async def _iter_messages(app: Client, peer, start: int, end: int):
    """Lấy message theo lô 200 id — nhanh hơn rất nhiều so với gọi từng cái."""
    ids = list(range(start, end + 1))
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        try:
            messages = await app.get_messages(peer, chunk)
        except FloodWait as e:
            await asyncio.sleep(int(getattr(e, "value", 30)) + 1)
            try:
                messages = await app.get_messages(peer, chunk)
            except RPCError as exc:
                log.error("get_messages lỗi cho lô %s-%s: %s", chunk[0], chunk[-1], exc)
                continue
        except RPCError as exc:
            log.error("get_messages lỗi cho lô %s-%s: %s", chunk[0], chunk[-1], exc)
            continue
        for msg in messages or []:
            yield msg


async def _job_is_active(db: AsyncSession, job_id: int) -> bool:
    res = await db.execute(select(Job.status).where(Job.id == job_id))
    status = res.scalar_one_or_none()
    return status == JOB_STATUS_RUNNING


async def _create_task_row(db: AsyncSession, r: aioredis.Redis, job_id: int, msg) -> bool:
    existing = await db.execute(
        select(Task.id).where(Task.job_id == job_id, Task.msg_id == msg.id).limit(1)
    )
    if existing.scalars().first():
        return False

    task = Task(
        job_id=job_id,
        msg_id=msg.id,
        caption=(msg.caption or "")[:CAPTION_LIMIT],
        status=TASK_STATUS_PENDING,
        filename=f"{job_id}_{msg.id}{get_media_extension(msg)}",
        media_kind=get_media_kind(msg),
        size_bytes=get_media_size(msg),
        worker_id=None,
    )
    db.add(task)
    try:
        await db.flush()
        await db.commit()
    except Exception as exc:  # unique constraint -> worker khác đã tạo
        await db.rollback()
        log.debug("Task đã tồn tại (job=%s msg=%s): %s", job_id, msg.id, exc)
        return False

    await r.zadd(QUEUE_DOWNLOAD, {str(task.id): 0})
    return True


async def _resolve_job_chats(app: Client, db: AsyncSession, r: aioredis.Redis, job_id: int, src_peer, dst_peer) -> bool:
    """Kiểm tra quyền truy cập 2 chat trước, báo lỗi sớm thay vì fail từng task."""
    titles = {}
    for field, peer in (("src_title", src_peer), ("dst_title", dst_peer)):
        try:
            chat = await app.get_chat(peer)
            titles[field] = (
                getattr(chat, "title", None) or getattr(chat, "username", None) or str(peer)
            )[:255]
        except Exception as exc:
            log.error("Job %s: không truy cập được chat %r: %s", job_id, peer, exc)
            await db.execute(
                update(Job).where(Job.id == job_id).values(status=JOB_STATUS_CANCELLED)
            )
            await db.commit()
            await enqueue_progress(
                r,
                {
                    "worker_id": WORKER_ID,
                    "type": "job_error",
                    "job_id": job_id,
                    "error": f"Không truy cập được {peer}: {exc}",
                },
            )
            return False

    await db.execute(update(Job).where(Job.id == job_id).values(**titles))
    await db.commit()
    return True


async def _process_new_job(app: Client, r: aioredis.Redis, job_req: dict, stop_event: asyncio.Event):
    job_id = int(job_req["job_id"])
    from_msg_id = job_req.get("from_msg_id")
    to_msg_id = job_req.get("to_msg_id")

    try:
        src_peer = normalize_peer(job_req["src_link"])
        dst_peer = normalize_peer(job_req["dst_link"])
    except ValueError as exc:
        log.error("Job %s có link không hợp lệ: %s", job_id, exc)
        return

    log.info("Job %s: src=%r dst=%r range=%s..%s", job_id, src_peer, dst_peer, from_msg_id, to_msg_id)

    async with AsyncSessionLocal() as db:
        if not await _resolve_job_chats(app, db, r, job_id, src_peer, dst_peer):
            return

        res = await db.execute(select(Transferred.msg_id).where(Transferred.job_id == job_id))
        transferred_ids = {int(x[0]) for x in res.all() if x and x[0] is not None}

        created = 0
        if from_msg_id is not None and to_msg_id is not None:
            start = int(min(from_msg_id, to_msg_id))
            end = int(max(from_msg_id, to_msg_id))
            async for msg in _iter_messages(app, src_peer, start, end):
                if stop_event.is_set():
                    break
                if not msg or getattr(msg, "empty", False):
                    continue
                if int(msg.id) in transferred_ids or not is_supported_media(msg):
                    continue
                if await _create_task_row(db, r, job_id, msg):
                    created += 1
                    # Quét dải msg_id rộng có thể mất rất lâu; cập nhật counter
                    # dọc đường để dashboard không đứng ở total = 0.
                    if created % 50 == 0:
                        await refresh_job_counters(db, job_id)
                        await db.commit()
                        if not await _job_is_active(db, job_id):
                            log.info("Job %s đã bị huỷ, dừng quét", job_id)
                            break
        else:
            scanned = 0
            async for msg in app.get_chat_history(src_peer, limit=SCAN_CAP):
                if stop_event.is_set() or scanned >= SCAN_CAP:
                    break
                scanned += 1
                if not msg or getattr(msg, "empty", False):
                    continue
                if int(msg.id) in transferred_ids or not is_supported_media(msg):
                    continue
                if await _create_task_row(db, r, job_id, msg):
                    created += 1
                    if created % 50 == 0:
                        await refresh_job_counters(db, job_id)
                        await db.commit()
                        if not await _job_is_active(db, job_id):
                            log.info("Job %s đã bị huỷ, dừng quét", job_id)
                            break

        await refresh_job_counters(db, job_id)
        await db.commit()

    log.info("Job %s: tạo %s task", job_id, created)
    await enqueue_progress(
        r, {"worker_id": WORKER_ID, "type": "job_enqueued", "job_id": job_id, "created": created}
    )


async def new_job_consumer(app: Client, r: aioredis.Redis, stop_event: asyncio.Event):
    log.info("new_job_consumer sẵn sàng")
    while not stop_event.is_set():
        try:
            payload = await r.brpop(QUEUE_NEW_JOB, timeout=2)
        except Exception as exc:
            log.warning("BRPOP lỗi: %s", exc)
            await asyncio.sleep(1)
            continue
        if not payload:
            continue

        _queue, raw = payload
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            job_req = json.loads(raw)
        except Exception as exc:
            log.error("Job JSON hỏng: %s", exc)
            continue

        try:
            await _process_new_job(app, r, job_req, stop_event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("Xử lý job mới thất bại: %s", exc)


# --------------------------------------------------------------------------
# Janitor: cứu task bị kẹt khi worker chết giữa chừng
# --------------------------------------------------------------------------
async def _requeue_orphans(r: aioredis.Redis):
    """Task ở trạng thái downloading/uploading nhưng không còn lock -> worker đã chết."""
    async with AsyncSessionLocal() as db:
        res = await db.execute(
            select(Task.id, Task.status).where(
                Task.status.in_([TASK_STATUS_DOWNLOADING, TASK_STATUS_UPLOADING])
            )
        )
        recovered = 0
        for task_id, status in res.all():
            if await r.exists(f"{LOCK_PREFIX}{task_id}"):
                continue
            in_dl = await r.zscore(QUEUE_DOWNLOAD, str(task_id))
            in_up = await r.zscore(QUEUE_UPLOAD, str(task_id))
            if in_dl is not None or in_up is not None:
                continue

            if status == TASK_STATUS_UPLOADING:
                await r.zadd(QUEUE_UPLOAD, {str(task_id): time.time()})
            else:
                await update_task_status(db, task_id, TASK_STATUS_PENDING)
                await r.zadd(QUEUE_DOWNLOAD, {str(task_id): time.time()})
            recovered += 1

        if recovered:
            await db.commit()
            log.info("Janitor đưa lại %s task bị kẹt vào hàng đợi", recovered)


async def _prune_dead_workers(r: aioredis.Redis):
    """Xoá heartbeat của worker đã tắt từ lâu, tránh dashboard đầy worker ma."""
    heartbeats = await r.hgetall(WORKER_HEARTBEAT)
    now = time.time()
    dead = []
    for worker_id, ts in heartbeats.items():
        try:
            if now - float(ts) > WORKER_STALE_AFTER:
                dead.append(worker_id)
        except (TypeError, ValueError):
            dead.append(worker_id)
    if dead:
        await r.hdel(WORKER_HEARTBEAT, *dead)
        await r.hdel(WORKER_STATUS, *dead)
        log.info("Janitor xoá %s worker đã chết: %s", len(dead), ", ".join(dead))


async def janitor_loop(r: aioredis.Redis, stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            # chỉ 1 worker chạy janitor mỗi phút
            if await try_acquire_lock(r, JANITOR_LOCK, ttl_sec=55):
                await _requeue_orphans(r)
                await _prune_dead_workers(r)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("janitor lỗi: %s", exc)
        await asyncio.sleep(60)


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------
def _write_session_file(session_str: str):
    os.makedirs(SESSION_DIR, exist_ok=True)
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        f.write(session_str)


def _read_session_file() -> Optional[str]:
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except (FileNotFoundError, IOError):
        return None


async def _load_session(r: aioredis.Redis) -> Optional[str]:
    if SESSION_STRING_ENV:
        return SESSION_STRING_ENV
    session = await r.get(AUTH_SESSION_KEY)
    if isinstance(session, bytes):
        session = session.decode("utf-8")
    if session:
        return session
    session = _read_session_file()
    if session:
        # session không nên có TTL: hết hạn key là toàn bộ worker mất auth
        await r.set(AUTH_SESSION_KEY, session)
    return session


async def wait_for_session(r: aioredis.Redis, stop_event: asyncio.Event) -> Optional[str]:
    """Chờ session; nếu auth master biến mất thì tự đứng ra làm master."""
    while not stop_event.is_set():
        session = await _load_session(r)
        if session:
            return session
        if await try_acquire_lock(r, AUTH_LOCK_KEY, ttl_sec=AUTH_LOCK_TTL):
            session = await run_auth_master(r, stop_event)
            if session:
                return session
        await asyncio.sleep(1)
    return None


async def run_auth_master(r: aioredis.Redis, stop_event: asyncio.Event) -> Optional[str]:
    """Worker giữ AUTH_LOCK sẽ nhận số điện thoại/OTP từ web và tạo session."""
    log.info("Worker %s làm auth master — chờ OTP từ web", WORKER_ID)
    session_str = None
    app = Client(
        f"tgcopy_auth_{WORKER_ID}",
        api_id=_require_credentials(),
        api_hash=API_HASH,
        in_memory=True,
    )
    await app.connect()
    try:
        while not stop_event.is_set() and not session_str:
            # gia hạn lock, tránh worker khác cũng nhảy vào làm master
            await r.expire(AUTH_LOCK_KEY, AUTH_LOCK_TTL)

            req_payload = await r.brpop(AUTH_OTP_REQ_QUEUE, timeout=2)
            if req_payload:
                _, raw = req_payload
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                phone = ""
                try:
                    req = json.loads(raw)
                    phone = normalize_phone(req.get("phone_number"), DEFAULT_COUNTRY_CODE)
                    if _is_stale_request(req):
                        log.info("Bỏ qua yêu cầu OTP cũ cho %s", mask_phone(phone))
                        phone = ""
                    if phone:
                        sent = await app.send_code(phone)
                        await r.set(f"auth:phone_code_hash:{phone}", sent.phone_code_hash, ex=300)
                        await r.delete(AUTH_ERROR_KEY)
                        log.info("Đã gửi OTP tới %s", mask_phone(phone))
                except InvalidPhoneNumber as exc:
                    log.error("Số điện thoại không hợp lệ: %s", exc)
                    await r.set(AUTH_ERROR_KEY, str(exc), ex=300)
                except Exception as exc:
                    log.error("Gửi OTP tới %s lỗi: %s", mask_phone(phone), exc)
                    hint = ""
                    if "PHONE_NUMBER_INVALID" in str(exc):
                        hint = (
                            " — Telegram không nhận số này. Kiểm tra lại mã quốc gia,"
                            " nhập dạng +84987654321."
                        )
                    await r.set(AUTH_ERROR_KEY, f"Gửi OTP lỗi: {exc}{hint}", ex=300)

            verify_payload = await r.brpop(AUTH_OTP_QUEUE, timeout=1)
            if not verify_payload:
                continue

            _, raw = verify_payload
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            try:
                data = json.loads(raw)
                phone = normalize_phone(data.get("phone_number"), DEFAULT_COUNTRY_CODE)
                otp = data.get("otp", "")
                password = data.get("password") or ""
                if not (phone and otp):
                    continue
                if _is_stale_request(data):
                    log.info("Bỏ qua OTP cũ cho %s", phone)
                    continue

                phone_code_hash = await r.get(f"auth:phone_code_hash:{phone}")
                if not phone_code_hash:
                    await r.set(AUTH_ERROR_KEY, "OTP hết hạn, hãy gửi lại mã", ex=300)
                    continue
                if isinstance(phone_code_hash, bytes):
                    phone_code_hash = phone_code_hash.decode("utf-8")

                try:
                    await app.sign_in(
                        phone_number=phone, phone_code_hash=phone_code_hash, phone_code=otp
                    )
                except Exception as exc:
                    if type(exc).__name__ == "SessionPasswordNeeded":
                        if not password:
                            await r.set(
                                AUTH_ERROR_KEY, "Tài khoản bật 2FA — cần nhập mật khẩu", ex=300
                            )
                            continue
                        await app.check_password(password)
                    else:
                        raise

                session_str = await app.export_session_string()
                _write_session_file(session_str)
                await r.set(AUTH_SESSION_KEY, session_str)
                await r.delete(AUTH_ERROR_KEY)
                log.info("Đăng nhập thành công, đã lưu session cho %s", mask_phone(phone))
            except InvalidPhoneNumber as exc:
                log.error("Số điện thoại không hợp lệ: %s", exc)
                await r.set(AUTH_ERROR_KEY, str(exc), ex=300)
            except RPCError as exc:
                log.error("Xác thực OTP lỗi RPC: %s", exc)
                await r.set(AUTH_ERROR_KEY, f"OTP không hợp lệ: {exc}", ex=300)
            except Exception as exc:
                log.error("Xác thực OTP thất bại: %s", exc)
                await r.set(AUTH_ERROR_KEY, str(exc), ex=300)
    finally:
        try:
            await app.disconnect()
        except Exception:
            pass
        # nhả lock để lần đăng nhập sau không bị kẹt 5 phút
        await release_lock(r, AUTH_LOCK_KEY)
    return session_str


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    stop_event = asyncio.Event()

    log.info("Khởi động worker %s", WORKER_ID)
    api_id = _require_credentials()

    dl_sem = AdaptiveSemaphore(DEFAULT_MAX_DL)
    up_sem = AdaptiveSemaphore(DEFAULT_MAX_UP)
    stats = WorkerStats()

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    # DB và các vòng lặp không phụ thuộc Telegram được bật trước khi đăng nhập:
    # worker chưa auth vẫn hiện trên dashboard (session = "waiting_auth") và vẫn
    # dọn được task mồ côi / heartbeat của worker đã chết.
    await init_db()
    hb_task = asyncio.create_task(heartbeat_loop(r, stats, dl_sem, up_sem), name="heartbeat")
    janitor_task = asyncio.create_task(janitor_loop(r, stop_event), name="janitor")

    session_str = await _load_session(r)
    if not session_str:
        log.info("Worker %s chờ đăng nhập Telegram", WORKER_ID)
        session_str = await wait_for_session(r, stop_event)
    if not session_str or stop_event.is_set():
        hb_task.cancel()
        janitor_task.cancel()
        return

    # Tên client phải khác nhau giữa các worker để không đụng file session
    app = Client(
        f"tgcopy_{WORKER_ID}",
        session_string=session_str,
        api_id=api_id,
        api_hash=API_HASH,
        workers=int(os.getenv("PYROGRAM_WORKERS", "8")),
        max_concurrent_transmissions=max(DEFAULT_MAX_DL, DEFAULT_MAX_UP),
    )
    await app.start()
    me = await app.get_me()
    log.info("Đăng nhập Telegram: %s (id=%s)", me.username or me.first_name, me.id)

    stats.session_state = "ready"

    tasks = [
        hb_task,
        janitor_task,
        asyncio.create_task(command_listener(r, dl_sem, up_sem, stop_event), name="cmd"),
        asyncio.create_task(new_job_consumer(app, r, stop_event), name="new_jobs"),
    ]
    tasks += [
        asyncio.create_task(download_worker(app, r, dl_sem, stop_event, stats), name=f"dl-{i}")
        for i in range(max(1, DEFAULT_MAX_DL))
    ]
    tasks += [
        asyncio.create_task(upload_worker(app, r, up_sem, stop_event, stats), name=f"up-{i}")
        for i in range(max(1, DEFAULT_MAX_UP))
    ]

    await r.hset(WORKER_HEARTBEAT, WORKER_ID, str(time.time()))
    await r.hset(WORKER_STATUS, WORKER_ID, json.dumps(stats.snapshot(dl_sem.limit, up_sem.limit)))
    log.info(
        "Worker %s chạy %s download loop / %s upload loop", WORKER_ID, DEFAULT_MAX_DL, DEFAULT_MAX_UP
    )

    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await r.hdel(WORKER_HEARTBEAT, WORKER_ID)
            await r.hdel(WORKER_STATUS, WORKER_ID)
        except Exception:
            pass
        try:
            await app.stop()
        except Exception:
            pass
        try:
            await r.aclose()
        except Exception:
            pass
        log.info("Worker %s đã dừng", WORKER_ID)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
