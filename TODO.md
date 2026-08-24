# TODO - Integrate Telegram worker + Parallel download/upload + Web/API control

## Step 0 — Gather & verify repo structure
- [x] Read FastAPI routes and models: jobs/tasks/workers/stats/websocket
- [x] Read shared Redis constants for queues/locks/progress

## Step 1 — Decide worker architecture
- [x] Confirm worker type: **2) N process worker**

## Step 2 — Implement Telegram worker process
- [ ] Create worker file (new): `api/worker_tg.py`
- [ ] Consume commands from Redis:
  - [ ] `queue:new_job` for new jobs
  - [ ] `cmd:{worker_id}` for set limits / stop
- [ ] Implement task lifecycle with Redis + DB:
  - [ ] enqueue download tasks into `queue:download` (ZSET priority)
  - [ ] download media concurrently (semaphore), update DB state to `downloading`
  - [ ] enqueue upload tasks into `queue:upload` (ZSET)
  - [ ] upload concurrently (semaphore), update DB state to `uploading/done/failed`
  - [ ] distributed lock per task id (`lock:task:{...}`)
- [ ] Push progress updates to Redis pub/sub `progress`
- [ ] Update worker heartbeat/status hashes `workers:heartbeat` and `workers:status`

## Step 3 — Enforce priority: download first
- [ ] Ensure download workers always have priority over upload when both are runnable
- [ ] Add backpressure: pause download when upload backlog too large

## Step 4 — Add runnable entrypoint / instructions
- [ ] Provide command(s) to start multiple workers (e.g., env var WORKER_ID)
- [ ] Ensure folders exist: `downloads/`

## Step 5 — Testing
- [ ] Start Redis + Postgres (if not running)
- [ ] Start API (FastAPI)
- [ ] Start 2-3 worker processes with different WORKER_ID
- [ ] Create a job via API and verify realtime queues/progress

