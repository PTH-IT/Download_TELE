import asyncio

import pytest

from shared.transfer import TransferProgress, run_with_stall_guard


def test_progress_only_moves_forward():
    p = TransferProgress()
    p.touch(100)
    first = p.last_move
    # Pyrogram vẫn gọi callback khi kết nối đứng hình — số byte không tăng
    p.touch(100)
    p.touch(50)
    assert p.current == 100
    assert p.last_move == first


@pytest.mark.asyncio
async def test_returns_result_when_transfer_finishes():
    progress = TransferProgress()

    async def transfer():
        progress.touch(10)
        return "/tmp/file.mp4"

    result = await run_with_stall_guard(
        transfer(), progress, "test", stall_timeout=5, poll_interval=0.01
    )
    assert result == "/tmp/file.mp4"


@pytest.mark.asyncio
async def test_cancels_transfer_that_stops_moving():
    """Đúng kịch bản đã gặp: coroutine treo vĩnh viễn, không byte nào nhúc nhích."""
    progress = TransferProgress()
    started = asyncio.Event()

    async def hung_transfer():
        started.set()
        await asyncio.sleep(3600)

    with pytest.raises(TimeoutError) as exc:
        await run_with_stall_guard(
            hung_transfer(), progress, "upload task 1", stall_timeout=0, poll_interval=0.01
        )
    assert "upload task 1" in str(exc.value)
    assert started.is_set()


@pytest.mark.asyncio
async def test_keeps_waiting_while_bytes_keep_moving():
    progress = TransferProgress()

    async def slow_but_alive():
        for i in range(1, 6):
            progress.touch(i * 1000)
            await asyncio.sleep(0.02)
        return "xong"

    result = await run_with_stall_guard(
        slow_but_alive(), progress, "test", stall_timeout=1, poll_interval=0.01
    )
    assert result == "xong"


@pytest.mark.asyncio
async def test_propagates_transfer_error():
    progress = TransferProgress()

    async def broken():
        raise OSError("Broken pipe")

    with pytest.raises(OSError):
        await run_with_stall_guard(
            broken(), progress, "test", stall_timeout=5, poll_interval=0.01
        )
