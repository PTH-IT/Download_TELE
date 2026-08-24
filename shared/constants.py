# shared/constants.py
# Keys Redis dùng chung giữa API và Worker

QUEUE_DOWNLOAD   = "queue:download"    # ZSET, score = priority (nhỏ = cao)
QUEUE_UPLOAD     = "queue:upload"      # ZSET
QUEUE_RETRY      = "queue:retry"       # LIST dead-letter
PUBSUB_PROGRESS  = "progress"          # channel pub/sub cho WebSocket
LOCK_PREFIX      = "lock:task:"        # distributed lock mỗi task
WORKER_HEARTBEAT = "workers:heartbeat" # HASH worker_id -> timestamp
WORKER_STATUS    = "workers:status"    # HASH worker_id -> json status

AUTH_LOCK_KEY    = "lock:auth_session"  # Redis lock for auth master
AUTH_SESSION_KEY = "auth:session_string"  # Stored session string
AUTH_OTP_QUEUE   = "auth:otp"         # BRPOP for OTP verify requests
AUTH_OTP_REQ_QUEUE = "auth:otp_request"  # LPUSH for OTP send requests

TASK_STATUS_PENDING     = "pending"
TASK_STATUS_DOWNLOADING = "downloading"
TASK_STATUS_UPLOADING   = "uploading"
TASK_STATUS_DONE        = "done"
TASK_STATUS_FAILED      = "failed"
TASK_STATUS_CANCELLED   = "cancelled"

JOB_STATUS_RUNNING    = "running"
JOB_STATUS_DONE       = "done"
JOB_STATUS_CANCELLED  = "cancelled"
JOB_STATUS_PAUSED     = "paused"

PRIORITY_DOWNLOAD = 0   # xử lý trước
PRIORITY_UPLOAD   = 1
