import os
import json
import time
import asyncio
import logging
from typing import Any

import redis.asyncio as aioredis
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from api.database import init_db
from api.session_paths import resolve_session_dir, resolve_session_file

from api.models import Job, Task, Transferred

try:
    from .media_utils import get_media_extension, is_supported_media
except ImportError:  # pragma: no cover - direct script execution in Docker
    from media_utils import get_media_extension, is_supported_media
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

from shared.constants import (
    QUEUE_DOWNLOAD,
    QUEUE_UPLOAD,
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
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

# ---- Config from env ----
WORKER_ID = os.getenv("WORKER_ID", "worker-1")
API_ID = int(os.getenv("API_ID", ""))
API_HASH = os.getenv("API_HASH", "")

SESSION_DIR = str(resolve_session_dir())
SESSION_FILE = str(resolve_session_file())

DEFAULT_MAX_DL = int(os.getenv("MAX_DL", "2"))
DEFAULT_MAX_UP = int(os.getenv("MAX_UP", "4"))

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/app/downloads")

# concurrency tuning
QUEUE_BACKPRESSURE_UPLOAD = int(os.getenv("UPLOAD_QUEUE_BACKPRESSURE", "4"))

# ---- Redis PubSub commands: cmd:{worker_id} ----
CMD_LIMITS = "set_limits"
CMD_STOP = "stop"


class AdaptiveSemaphore:
    def __init__(self, limit: int):
        self._limit = max(1, int(limit))
        self._count = 0
        self._cond = asyncio.Condition()

    async def acquire(self):
        async with self._cond:
            while self._count >= self._limit:
                await self._cond.wait()
            self._count += 1

    def release(self):
        async def _rel():
            async with self._cond:
                self._count -= 1
                self._cond.notify_all()

        try:
            asyncio.create_task(_rel())
        except RuntimeError:
            pass

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    def set_limit(self, new_limit: int):
        new_limit = max(1, int(new_limit))

        async def _set():
            async with self._cond:
                self._limit = new_limit
                self._cond.notify_all()

        asyncio.create_task(_set())

    @property
    def limit(self):
        return self._limit


def pub_progress(data: Any):
    # progress is pushed via redis publish from async context
    return json.dumps(data, ensure_ascii=False)


async def heartbeat_loop(r: aioredis.Redis):
    while True:
        await r.hset(WORKER_HEARTBEAT, WORKER_ID, str(time.time()))
        log.info("Heartbeat sent")
        # status set by main loop
        await asyncio.sleep(5)


async def command_listener(r: aioredis.Redis, dl_sem: AdaptiveSemaphore, up_sem: AdaptiveSemaphore, stop_event: asyncio.Event):
    pubsub = r.pubsub()
    await pubsub.subscribe(f"cmd:{WORKER_ID}")
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

            action = payload.get("action")
            if action == "stop":
                stop_event.set()
                return

            # set_limits
            max_dl = payload.get("max_dl")
            max_up = payload.get("max_up")
            if max_dl is not None:
                dl_sem.set_limit(int(max_dl))
            if max_up is not None:
                up_sem.set_limit(int(max_up))
    finally:
        try:
            await pubsub.unsubscribe(f"cmd:{WORKER_ID}")
        except Exception:
            pass
        try:
            await pubsub.aclose()
        except Exception:
            pass


async def update_task_status(db: AsyncSession, task_id: int, status: str, **fields):
    values = {"status": status}
    values.update(fields)
    await db.execute(update(Task).where(Task.id == task_id).values(**values))


async def try_acquire_lock(r: aioredis.Redis, lock_key: str, ttl_sec: int = 3600) -> bool:
    # simple SET NX
    ok = await r.set(lock_key, WORKER_ID, nx=True, ex=ttl_sec)
    return bool(ok)


async def enqueue_progress(r: aioredis.Redis, payload: dict):
    # payload is JSON string
    await r.publish(PUBSUB_PROGRESS, pub_progress(payload))


async def download_worker(app: Client, r: aioredis.Redis, db: AsyncSession, dl_sem: AdaptiveSemaphore, up_queue: asyncio.Queue, stop_event: asyncio.Event):
    while not stop_event.is_set():
        # Take next download task from ZSET by priority
        # Using ZPOPMIN (Redis >=5). If not available in your redis, we fall back later.
        item = await r.zpopmin(QUEUE_DOWNLOAD, count=1)
        if not item:
            await asyncio.sleep(0.2)
            continue

        # item = [(member, score)] but encoding depends; normalize
        member, _score = item[0]
        if isinstance(member, bytes):
            member = member.decode("utf-8")
        task_id = int(member)

        # verify task state
        t = await db.get(Task, task_id)
        if not t:
            continue
        if t.status in (TASK_STATUS_CANCELLED, TASK_STATUS_DONE, TASK_STATUS_FAILED):
            continue

        # distributed lock
        lock_key = f"{LOCK_PREFIX}{task_id}"
        got = await try_acquire_lock(r, lock_key)
        if not got:
            # someone else processing
            await r.zadd(QUEUE_DOWNLOAD, {str(task_id): int(time.time())})
            continue

        try:
            await update_task_status(db, task_id, TASK_STATUS_DOWNLOADING)
            await db.commit()

            # Download using saved filename/path in Task
            # Note: Task must contain enough info. In current DB model, caption/filename/file_path exist.
            # We'll assume Task.filename holds a deterministic desired filename.
            filename = t.filename or f"{task_id}.mp4"
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            # app.download_media expects a message reference; our Task only has msg_id/job_id.
            # We need source/dst resolution from Job stored in DB.
            job = await db.get(Job, t.job_id)
            if not job:
                raise RuntimeError("Job not found")

            src_chat = job.src_link  # for real system we should store chat id; for now keep link
            msg = await app.get_messages(src_chat, t.msg_id)
            if not msg or not is_supported_media(msg):
                raise RuntimeError("Message is not a supported media message")

            await asyncio.sleep(0)  # yield

            async def dl_progress(current, total):
                await enqueue_progress(r, {
                    "worker_id": WORKER_ID,
                    "type": "dl",
                    "task_id": task_id,
                    "current": current,
                    "total": total,
                })

            async with dl_sem:
                start = time.time()
                path = await app.download_media(
                    msg,
                    file_name=local_path,
                    progress=dl_progress,
                )
                elapsed = max(0.001, time.time() - start)
                speed = os.path.getsize(path) / elapsed if os.path.exists(path) else 0

            await update_task_status(db, task_id, TASK_STATUS_UPLOADING, file_path=path, speed_dl=speed)
            await db.commit()

            # push to upload queue for this worker process
            await up_queue.put(task_id)

            await enqueue_progress(r, {"worker_id": WORKER_ID, "type": "dl_done", "task_id": task_id})

        except asyncio.CancelledError:
            raise
        except FloodWait as e:
            wait = int(e.value) + 1
            # requeue
            await update_task_status(db, task_id, TASK_STATUS_PENDING)
            await db.commit()
            await asyncio.sleep(wait)
            await r.zadd(QUEUE_DOWNLOAD, {str(task_id): 999999999})
        except Exception as e:
            await update_task_status(db, task_id, TASK_STATUS_FAILED, error=str(e))
            await db.commit()
        finally:
            # release lock by deleting
            try:
                await r.delete(lock_key)
            except Exception:
                pass


async def upload_worker(app: Client, r: aioredis.Redis, db: AsyncSession, up_sem: AdaptiveSemaphore, up_queue: asyncio.Queue, stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            task_id = await asyncio.wait_for(up_queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        t = await db.get(Task, task_id)
        if not t:
            up_queue.task_done()
            continue
        if t.status == TASK_STATUS_CANCELLED:
            up_queue.task_done()
            continue

        lock_key = f"{LOCK_PREFIX}{task_id}"
        # reuse lock not strictly needed for upload because we have download lock lifecycle,
        # but keep a best-effort lock.
        await try_acquire_lock(r, lock_key, ttl_sec=3600)

        try:
            await update_task_status(db, task_id, TASK_STATUS_UPLOADING)
            await db.commit()

            job = await db.get(Job, t.job_id)
            if not job:
                raise RuntimeError("Job not found")

            dst_chat = job.dst_link
            caption = t.caption or ""
            path = t.file_path
            if not path or not os.path.exists(path):
                raise RuntimeError(f"File not found: {path}")

            async def up_progress(current, total):
                await enqueue_progress(r, {
                    "worker_id": WORKER_ID,
                    "type": "up",
                    "task_id": task_id,
                    "current": current,
                    "total": total,
                })

            async with up_sem:
                start = time.time()
                await app.send_video(
                    dst_chat,
                    video=path,
                    caption=caption,
                    supports_streaming=True,
                    progress=up_progress,
                )
                elapsed = max(0.001, time.time() - start)
                speed = os.path.getsize(path) / elapsed if os.path.exists(path) else 0

            await update_task_status(db, task_id, TASK_STATUS_DONE, speed_up=speed, updated_at=time.time())
            await db.commit()

            # store transferred history
            # msg_id in Task
            db_obj = await db.get(Transferred, t.msg_id)
            if not db_obj:
                db.add(Transferred(msg_id=t.msg_id, job_id=t.job_id))
                await db.commit()

            # optionally delete file
            try:
                os.remove(path)
            except Exception:
                pass

            await enqueue_progress(r, {"worker_id": WORKER_ID, "type": "up_done", "task_id": task_id})

        except Exception as e:
            await update_task_status(db, task_id, TASK_STATUS_FAILED, error=str(e))
            await db.commit()
        finally:
            try:
                await r.delete(lock_key)
            except Exception:
                pass
            up_queue.task_done()


async def new_job_consumer(app: Client, r: aioredis.Redis, dl_sem: AdaptiveSemaphore, stop_event: asyncio.Event):
    log.info("Starting new_job_consumer loop")
    while not stop_event.is_set():
        log.info(f"Polling queue:new_job")
        payload = await r.brpop("queue:new_job", timeout=2)
        log.info(f"BRPOP returned: {payload}")
        if not payload:
            continue
        _queue, raw = payload
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            job_req = json.loads(raw)
            log.info(f"Parsed job_req: {job_req}")
        except Exception as e:
            log.error(f"Failed to parse job JSON: {e}")
            continue

        job_id = int(job_req["job_id"])
        log.info(f"Processing job {job_id}")
        # from/to optional: if missing, we will scan recent history until we hit a reasonable limit.
        from_msg_id = job_req.get("from_msg_id")
        to_msg_id = job_req.get("to_msg_id")
        log.info(f"from_msg_id={from_msg_id}, to_msg_id={to_msg_id}")

        # resolve chats (links) via Pyrogram
        src_peer = job_req["src_link"]
        dst_peer = job_req["dst_link"]
        # Convert numeric string to int for Telegram API
        if isinstance(src_peer, str) and src_peer.startswith("-") and src_peer.lstrip("-").isdigit():
            src_peer = int(src_peer)

        # Use a fresh db session
        log.info(f"Getting DB session for job {job_id}")
        async for db in _db_session_iter():
            log.info(f"Got DB session")
            transferred_ids = set()
            try:
                res = await db.execute(select(Transferred.msg_id).where(Transferred.job_id == job_id))
                transferred_ids = {int(x[0]) for x in res.all() if x and x[0] is not None}
                log.info(f"transferred_ids: {transferred_ids}")
            except Exception as e:
                log.error(f"DB error for job {job_id}: {e}")
                break

            # Decide scanning range strategy
            # If both provided => scan in [from,to]
            # Else => scan a capped amount from newest backwards
            cap_messages = 200  # configurable later if needed

            created_count = 0
            if from_msg_id is not None and to_msg_id is not None:
                log.info(f"Entered range branch")
                start = int(min(from_msg_id, to_msg_id))
                end = int(max(from_msg_id, to_msg_id))
                log.info(f"Range: {start} to {end}")
                for msg_id in range(start, end + 1):
                    log.info(f"Checking msg_id {msg_id}")
                    if stop_event.is_set():
                        break
                    if msg_id in transferred_ids:
                        continue

                    try:
                        log.info(f"Calling get_messages({src_peer}, {msg_id})")
                        msg = await app.get_messages(src_peer, msg_id)
                        log.info(f"Got message: {msg is not None}")
                    except Exception as e:
                        log.error(f"get_messages error for {msg_id}: {e}")
                        continue

                    if not msg:
                        log.info(f"msg is None for {msg_id}")
                        continue
                    if not is_supported_media(msg):
                        log.info(f"unsupported media for {msg_id}, content_type={type(msg).__name__}")
                        continue

                    # caption
                    caption = msg.caption or ""

                    # Insert Task if not exists
                    existing = await db.execute(
                        select(Task).where(Task.job_id == job_id, Task.msg_id == msg_id).limit(1)
                    )
                    if existing.scalars().first():
                        continue

                    media = getattr(msg, "video", None) or getattr(msg, "document", None) or getattr(msg, "photo", None)
                    t = Task(
                        job_id=job_id,
                        msg_id=msg_id,
                        caption=caption,
                        status=TASK_STATUS_PENDING,
                        filename=f"{msg_id}{get_media_extension(msg)}",
                        worker_id=WORKER_ID,
                        size_bytes=getattr(media, "file_size", 0) or 0,
                    )
                    db.add(t)
                    await db.flush()
                    await db.commit()

                    # Lower score => higher priority download
                    await r.zadd(QUEUE_DOWNLOAD, {str(t.id): 0})
                    created_count += 1
            else:
                # scan recent history limited by cap_messages
                scanned = 0
                async for msg in app.get_chat_history(src_peer, limit=cap_messages):
                    if stop_event.is_set():
                        break
                    if scanned >= cap_messages:
                        break
                    scanned += 1

                    if not msg or not is_supported_media(msg):
                        continue
                    if int(msg.id) in transferred_ids:
                        continue

                    existing = await db.execute(
                        select(Task).where(Task.job_id == job_id, Task.msg_id == msg.id).limit(1)
                    )
                    if existing.scalars().first():
                        continue

                    caption = msg.caption or ""
                    t = Task(
                        job_id=job_id,
                        msg_id=msg.id,
                        caption=caption,
                        status=TASK_STATUS_PENDING,
                        filename=f"{msg.id}.mp4",
                        worker_id=WORKER_ID,
                    )
                    db.add(t)
                    await db.flush()
                    await db.commit()
                    await r.zadd(QUEUE_DOWNLOAD, {str(t.id): 0})
                    created_count += 1

            # best-effort: update job status to cancelled check is not implemented in this skeleton
            await enqueue_progress(r, {"worker_id": WORKER_ID, "type": "job_enqueued", "job_id": job_id, "created": created_count})
            break


async def _db_session_iter():
    # helper yields one session
    from api.database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        yield session


async def _read_session_file() -> str | None:
    try:
        with open(SESSION_FILE, "r") as f:
            return f.read().strip()
    except (FileNotFoundError, IOError):
        return None


def _write_session_file(session_str: str):
    os.makedirs(SESSION_DIR, exist_ok=True)
    with open(SESSION_FILE, "w") as f:
        f.write(session_str)


async def wait_for_session(r: aioredis.Redis, stop_event: asyncio.Event) -> str:
    """Wait for session string to be available from Redis or file."""
    while not stop_event.is_set():
        # Check Redis first (shared session)
        session = await r.get(AUTH_SESSION_KEY)
        if session:
            if isinstance(session, bytes):
                session = session.decode("utf-8")
            return session
        # Check if file exists (persistent volume)
        try:
            with open(SESSION_FILE, "r") as f:
                file_session = f.read().strip()
                if file_session:
                    await r.set(AUTH_SESSION_KEY, file_session, ex=86400)
                    return file_session
        except (FileNotFoundError, IOError):
            pass
        await asyncio.sleep(1)
    raise RuntimeError("stop_event set while waiting for session")


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    stop_event = asyncio.Event()
    session_str = None

    # Check if session already exists (from file or Redis)
    session_str = await r.get(AUTH_SESSION_KEY)
    if isinstance(session_str, bytes):
        session_str = session_str.decode("utf-8")
    if not session_str:
        try:
            with open(SESSION_FILE, "r") as f:
                session_str = f.read().strip()
                if session_str:
                    await r.set(AUTH_SESSION_KEY, session_str, ex=86400)
        except (FileNotFoundError, IOError):
            pass

    # If no session exists, try to become auth master
    is_auth_master = False
    if not session_str:
        is_auth_master = await try_acquire_lock(r, AUTH_LOCK_KEY, ttl_sec=300)

        if is_auth_master:
            log.info(f"Worker {WORKER_ID} is auth master - waiting for OTP via web")

            app = Client(
                f"tgcopy_auth_{WORKER_ID}",
                api_id=API_ID,
                api_hash=API_HASH,
            )

            # Use connect() instead of start() to avoid phone prompt
            await app.connect()
            log.info("Client connected in auth master mode")

            # Now wait for OTP requests
            while not stop_event.is_set() and not session_str:
                req_payload = await r.brpop(AUTH_OTP_REQ_QUEUE, timeout=2)
                if req_payload:
                    _, raw = req_payload
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    try:
                        req_data = json.loads(raw)
                        phone = req_data.get("phone_number", "")
                        if phone:
                            code = await app.send_code(phone)
                            await r.set(f"auth:phone_code_hash:{phone}", code.phone_code_hash, ex=300)
                            log.info(f"OTP sent to {phone}")
                    except Exception as e:
                        log.error(f"Failed to send OTP: {e}")

                verify_payload = await r.brpop(AUTH_OTP_QUEUE, timeout=0.5)
                if verify_payload:
                    _, raw = verify_payload
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    try:
                        req_data = json.loads(raw)
                        req_phone = req_data.get("phone_number", "")
                        otp = req_data.get("otp", "")
                        if req_phone and otp:
                            phone_code_hash = await r.get(f"auth:phone_code_hash:{req_phone}")
                            if phone_code_hash:
                                if isinstance(phone_code_hash, bytes):
                                    phone_code_hash = phone_code_hash.decode("utf-8")
                                await app.sign_in(phone_number=req_phone, phone_code_hash=phone_code_hash, phone_code=otp)
                                session_str = await app.export_session_string()
                                if isinstance(session_str, bytes):
                                    session_str = session_str.decode("utf-8")
                                _write_session_file(session_str)
                                await r.set(AUTH_SESSION_KEY, session_str, ex=86400)
                                log.info(f"Session authenticated and saved for {req_phone}")
                    except RPCError as e:
                        log.error(f"OTP verify RPC error: {e}")
                    except Exception as e:
                        log.error(f"OTP verify failed: {e}")

            try:
                await app.disconnect()
            except Exception:
                pass

    # Wait for session to be available (for non-master workers or if master failed)
    if not session_str:
        log.info(f"Worker {WORKER_ID} waiting for auth session from master")
        session_str = await wait_for_session(r, stop_event)

    if stop_event.is_set():
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    await init_db()

    dl_sem = AdaptiveSemaphore(DEFAULT_MAX_DL)
    up_sem = AdaptiveSemaphore(DEFAULT_MAX_UP)

    # Telegram client with session
    app = Client(
        "tgcopy_worker",
        session_string=session_str,
        api_id=API_ID,
        api_hash=API_HASH,
        workers=16,
    )

    await app.start()
    log.info("App started, creating tasks")

    # heartbeat / command
    hb = asyncio.create_task(heartbeat_loop(r))
    cmd = asyncio.create_task(command_listener(r, dl_sem, up_sem, stop_event))

    # upload queue within this worker process
    up_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=0)

    # start download/upload consumers
    async def download_loop():
        async for db in _db_session_iter():
            await download_worker(app, r, db, dl_sem, up_queue, stop_event)
            break

    async def upload_loop():
        async for db in _db_session_iter():
            await upload_worker(app, r, db, up_sem, up_queue, stop_event)
            break

    download_loops = max(1, dl_sem.limit)
    upload_loops = max(1, up_sem.limit)

    downloads = [asyncio.create_task(download_loop()) for _ in range(download_loops)]
    uploads = [asyncio.create_task(upload_loop()) for _ in range(upload_loops)]
    log.info(f"Started {len(downloads)} download loops, {len(uploads)} upload loops")

    # new job consumer
    new_jobs = asyncio.create_task(new_job_consumer(app, r, dl_sem, stop_event))

    # init status
    await r.hset(WORKER_STATUS, WORKER_ID, json.dumps({"session": "ready", "max_dl": dl_sem.limit, "max_up": up_sem.limit}))

    await stop_event.wait()

    # shutdown
    for t in downloads + uploads:
        t.cancel()
    new_jobs.cancel()
    cmd.cancel()
    hb.cancel()

    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())