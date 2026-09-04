"""api/routes/auth.py — Telegram authentication endpoints"""
import json
import os
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ..redis_client import get_redis
from ..session_paths import resolve_session_dir, resolve_session_file
from shared.phone import InvalidPhoneNumber, normalize_phone
from shared.constants import (
    AUTH_LOCK_KEY,
    AUTH_OTP_QUEUE,
    AUTH_OTP_REQ_QUEUE,
    AUTH_SESSION_KEY,
    WORKER_HEARTBEAT,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])

SESSIONS_DIR = resolve_session_dir()
SESSION_FILE = str(resolve_session_file())
AUTH_ERROR_KEY = "auth:last_error"
# Mã vùng mặc định cho số nội địa bắt đầu bằng 0 (ví dụ "84"). Để trống thì
# bắt buộc nhập số ở định dạng quốc tế.
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "")


HEARTBEAT_TIMEOUT = 30


async def _require_live_worker(redis):
    """Không có worker nào sống thì OTP sẽ nằm im trong hàng đợi mãi mãi."""
    heartbeats = await redis.hgetall(WORKER_HEARTBEAT)
    now = time.time()
    for ts in heartbeats.values():
        try:
            if now - float(ts) < HEARTBEAT_TIMEOUT:
                return
        except (TypeError, ValueError):
            continue
    raise HTTPException(
        503,
        "Chưa có worker nào chạy — worker mới là bên gửi OTP. "
        "Khởi động worker rồi thử lại (docker compose up -d worker).",
    )


def _clean_phone(raw: str) -> str:
    try:
        return normalize_phone(raw, DEFAULT_COUNTRY_CODE)
    except InvalidPhoneNumber as exc:
        raise HTTPException(400, str(exc))


def _read_session_string() -> Optional[str]:
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except (FileNotFoundError, IOError):
        return None


class RequestOTP(BaseModel):
    phone_number: str


class VerifyOTP(BaseModel):
    phone_number: str
    otp: str
    password: Optional[str] = None  # mật khẩu 2FA nếu tài khoản có bật


@router.post("/request")
async def request_otp(body: RequestOTP, redis=Depends(get_redis)):
    """Send OTP code to Telegram. Worker with auth lock will process it."""
    phone = _clean_phone(body.phone_number)
    await _require_live_worker(redis)

    await redis.delete(AUTH_ERROR_KEY)
    payload = {
        "action": "request_otp",
        "phone_number": phone,
        "timestamp": time.time(),
    }
    await redis.lpush(AUTH_OTP_REQ_QUEUE, json.dumps(payload))
    return {"status": "otp_sent", "phone_number": phone}


@router.post("/verify")
async def verify_otp(body: VerifyOTP, redis=Depends(get_redis)):
    """Verify OTP code. Worker will set session_string on success."""
    phone = _clean_phone(body.phone_number)
    otp = body.otp.strip()
    if not otp:
        raise HTTPException(400, "Chưa nhập mã OTP")
    await _require_live_worker(redis)

    await redis.delete(AUTH_ERROR_KEY)
    payload = {
        "action": "verify_otp",
        "phone_number": phone,
        "otp": otp,
        "password": (body.password or "").strip(),
        "timestamp": time.time(),
    }
    await redis.lpush(AUTH_OTP_QUEUE, json.dumps(payload))
    return {"status": "otp_queued"}


@router.get("/state")
async def get_auth_state(redis=Depends(get_redis)):
    """Check if session string exists and is saved."""
    error = await redis.get(AUTH_ERROR_KEY)

    session_str = _read_session_string()
    if not session_str:
        session_str = await redis.get(AUTH_SESSION_KEY)

    if session_str:
        return {
            "authenticated": True,
            "session_exists": True,
            "session_length": len(session_str),
            "error": None,
        }
    return {"authenticated": False, "session_exists": False, "error": error}


@router.post("/clear")
async def clear_session(redis=Depends(get_redis)):
    """Clear saved session (logout)."""
    try:
        os.remove(SESSION_FILE)
    except FileNotFoundError:
        pass
    await redis.delete(AUTH_SESSION_KEY)
    await redis.delete(AUTH_LOCK_KEY)
    await redis.delete(AUTH_ERROR_KEY)
    return {"status": "cleared"}


@router.get("/status")
async def get_auth_status(redis=Depends(get_redis)):
    """Get auth lock status for monitoring."""
    return {
        "auth_lock": await redis.get(AUTH_LOCK_KEY),
        "waiting_workers": await redis.llen(AUTH_OTP_REQ_QUEUE),
        "last_error": await redis.get(AUTH_ERROR_KEY),
    }
