from shared.constants import TASK_STATUS_CANCELLED, TASK_STATUS_FAILED, TASK_STATUS_PENDING


RETRYABLE_TASK_STATUSES = {TASK_STATUS_FAILED, TASK_STATUS_CANCELLED}


def can_retry_task_status(status: str | None) -> bool:
    return (status or "").lower() in RETRYABLE_TASK_STATUSES


def normalize_status(status: str | None) -> str:
    if not status:
        return TASK_STATUS_PENDING
    return status.lower()
