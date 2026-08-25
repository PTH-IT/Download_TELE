from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import init_db
from .redis_client import close_redis
from .routes.auth import router as auth_router
from .routes.jobs import router as jobs_router
from .routes.stats import router as stats_router
from .routes.websocket import router as websocket_router
from .routes.workers import router as workers_router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await init_db()
    yield
    await close_redis()


app = FastAPI(title="tgcopy", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs_router)
app.include_router(workers_router)
app.include_router(stats_router)
app.include_router(websocket_router)
app.include_router(auth_router)


@app.get("/health")
async def health():
    return {"ok": True}
