"""api/routes/websocket.py — stream tiến độ real-time qua Redis pub/sub"""
import asyncio, json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from ..redis_client import get_redis
from shared.constants import PUBSUB_PROGRESS

router = APIRouter(tags=["websocket"])


@router.websocket("/ws/progress")
async def ws_progress(ws: WebSocket):
    await ws.accept()
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(PUBSUB_PROGRESS)
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            await ws.send_text(message["data"])
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        await pubsub.unsubscribe(PUBSUB_PROGRESS)
        await pubsub.aclose()


@router.websocket("/ws/workers")
async def ws_workers(ws: WebSocket):
    """Push worker list mỗi 2 giây"""
    await ws.accept()
    r = await get_redis()
    import time

    try:
        while True:
            heartbeats = await r.hgetall("workers:heartbeat")
            statuses   = await r.hgetall("workers:status")
            now = time.time()
            workers = []
            for wid, ts in heartbeats.items():
                age = now - float(ts)
                status = {}
                if wid in statuses:
                    try:
                        status = json.loads(statuses[wid])
                    except Exception:
                        pass
                workers.append({
                    "id": wid,
                    "online": age < 30,
                    "session": status.get("session", "?"),
                    "current_task": status.get("current_task"),
                    "dl_speed_mbs": status.get("dl_speed_mbs", 0),
                    "up_speed_mbs": status.get("up_speed_mbs", 0),
                    "total_done": status.get("total_done", 0),
                })
            await ws.send_text(json.dumps(workers))
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        pass
