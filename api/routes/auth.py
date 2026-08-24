"""api/routes/auth.py — Telegram authentication endpoints"""
import os
import json
import time
from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from ..redis_client import get_redis
from shared.constants import LOCK_PREFIX, AUTH_SESSION_KEY, AUTH_LOCK_KEY, AUTH_OTP_REQ_QUEUE, AUTH_OTP_QUEUE
from ..session_paths import resolve_session_dir, resolve_session_file

router = APIRouter(prefix="/api/auth", tags=["auth"])

SESSIONS_DIR = resolve_session_dir()
SESSION_FILE = str(resolve_session_file())


def _write_session_string(session_str: str):
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    with open(SESSION_FILE, "w", encoding="utf-8") as f:
        f.write(session_str)


def _read_session_string() -> Optional[str]:
    try:
        with open(SESSION_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    except (FileNotFoundError, IOError):
        return None


class RequestOTP(BaseModel):
    phone_number: str


class VerifyOTP(BaseModel):
    phone_number: str
    otp: str


@router.post("/request")
async def request_otp(body: RequestOTP, redis=Depends(get_redis)):
    """Send OTP code to Telegram. Worker with auth lock will process it."""
    phone = body.phone_number.strip()
    if not phone:
        raise HTTPException(400, "phone_number required")
    
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
    phone = body.phone_number.strip()
    otp = body.otp.strip()
    if not phone or not otp:
        raise HTTPException(400, "phone_number and otp required")
    
    payload = {
        "action": "verify_otp",
        "phone_number": phone,
        "otp": otp,
        "timestamp": time.time(),
    }
    await redis.lpush(AUTH_OTP_QUEUE, json.dumps(payload))
    return {"status": "otp_queued"}


@router.get("/state")
async def get_auth_state(redis=Depends(get_redis)):
    """Check if session string exists and is saved."""
    session_str = _read_session_string()
    if session_str:
        return {
            "authenticated": True,
            "session_exists": True,
            "session_length": len(session_str),
        }
    redis_session = await redis.get(AUTH_SESSION_KEY)
    if redis_session:
        return {
            "authenticated": True,
            "session_exists": True,
            "session_length": len(redis_session),
        }
    return {"authenticated": False, "session_exists": False}


@router.post("/clear")
async def clear_session(redis=Depends(get_redis)):
    """Clear saved session (logout)."""
    try:
        os.remove(SESSION_FILE)
    except FileNotFoundError:
        pass
    await redis.delete(AUTH_SESSION_KEY)
    await redis.delete(AUTH_LOCK_KEY)
    return {"status": "cleared"}


@router.get("/status")
async def get_auth_status(redis=Depends(get_redis)):
    """Get auth lock status for monitoring."""
    lock_holder = await redis.get(AUTH_LOCK_KEY)
    return {
        "auth_lock": lock_holder,
        "waiting_workers": await redis.llen(AUTH_OTP_REQ_QUEUE),
    }