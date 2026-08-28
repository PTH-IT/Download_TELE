"""shared/transfer.py — canh chừng transfer bị treo.

Pyrogram không có timeout cho download_media/send_*. Khi kết nối chết giữa
chừng, coroutine treo vĩnh viễn: task đứng ở uploading, lock lại được gia hạn
đều đặn nên janitor cũng không thu hồi được. Thực tế đã thấy 8 task treo 18
tiếng không nhúc nhích.
"""
import asyncio
import os
import time

# Không có byte nào nhúc nhích trong ngần này giây thì huỷ transfer để thử lại
TRANSFER_STALL_TIMEOUT = int(os.getenv("TRANSFER_STALL_TIMEOUT", "180"))
# Chu kỳ watchdog kiểm tra; nhỏ hơn nhiều so với stall timeout
_POLL_INTERVAL = float(os.getenv("TRANSFER_POLL_INTERVAL", "15"))


class TransferProgress:
    """Mốc thời gian byte cuối cùng nhận/gửi được — dùng cho watchdog."""

    def __init__(self):
        self.started = time.time()
        self.last_move = time.time()
        self.current = 0

    def touch(self, current: int):
        # Chỉ tính là "có tiến triển" khi số byte thực sự tăng; Pyrogram vẫn
        # gọi callback đều đặn cả khi kết nối đã đứng hình.
        if current > self.current:
            self.current = current
            self.last_move = time.time()

    @property
    def stalled_for(self) -> float:
        return time.time() - self.last_move


async def run_with_stall_guard(
    coro,
    progress: "TransferProgress",
    label: str,
    stall_timeout: int = TRANSFER_STALL_TIMEOUT,
    poll_interval: float = _POLL_INTERVAL,
):
    """Chạy transfer, huỷ nếu không nhúc nhích quá stall_timeout."""
    task = asyncio.create_task(coro)
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=poll_interval)
            if task in done:
                return task.result()
            if progress.stalled_for > stall_timeout:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
                raise TimeoutError(
                    f"{label} đứng yên {int(progress.stalled_for)}s "
                    f"(đã truyền {progress.current} byte) — huỷ để thử lại"
                )
    finally:
        if not task.done():
            task.cancel()


