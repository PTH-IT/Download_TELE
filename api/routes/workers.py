"""api/routes/workers.py"""
import json, time
from fastapi import APIRouter, Depends
from ..redis_client import get_redis
from shared.constants import WORKER_HEARTBEAT, WORKER_STATUS

router = APIRouter(prefix="/api/workers", tags=["workers"])

HEARTBEAT_TIMEOUT = 30  # giây — worker im lặng quá lâu coi là offline


@router.get("")
async def list_workers(redis=Depends(get_redis)):
    heartbeats = await redis.hgetall(WORKER_HEARTBEAT)
    statuses   = await redis.hgetall(WORKER_STATUS)
    now = time.time()
    workers = []
    for wid, ts in heartbeats.items():
        age = now - float(ts)
        online = age < HEARTBEAT_TIMEOUT
        status = {}
        if wid in statuses:
            try:
                status = json.loads(statuses[wid])
            except Exception:
                pass
        workers.append({
            "id": wid,
            "online": online,
            "last_seen_ago": round(age, 1),
            "session": status.get("session", "?"),
            "current_task": status.get("current_task"),
            "dl_speed_mbs": status.get("dl_speed_mbs", 0),
            "up_speed_mbs": status.get("up_speed_mbs", 0),
            "total_done": status.get("total_done", 0),
            "max_dl": status.get("max_dl", 2),
            "max_up": status.get("max_up", 4),
        })
    workers.sort(key=lambda w: (not w["online"], w["id"]))
    return workers


@router.post("/{worker_id}/set_limits")
async def set_worker_limits(
    worker_id: str,
    max_dl: int = 2,
    max_up: int = 4,
    redis=Depends(get_redis),
):
    """Gửi lệnh điều chỉnh concurrency cho 1 worker cụ thể."""
    await redis.publish(f"cmd:{worker_id}", json.dumps({"max_dl": max_dl, "max_up": max_up}))
    return {"ok": True}


@router.delete("/{worker_id}/stop")
async def stop_worker(worker_id: str, redis=Depends(get_redis)):
    await redis.publish(f"cmd:{worker_id}", json.dumps({"action": "stop"}))
    return {"ok": True}
