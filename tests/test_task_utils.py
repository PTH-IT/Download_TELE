from api.task_utils import can_retry_task_status


def test_can_retry_task_status_for_failed_and_cancelled():
    assert can_retry_task_status("failed") is True
    assert can_retry_task_status("cancelled") is True
    assert can_retry_task_status("pending") is False
    assert can_retry_task_status("done") is False
