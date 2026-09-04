"""shared/peers.py — chuẩn hoá định danh chat của Telegram.

Người dùng có thể nhập chat theo nhiều dạng khác nhau (id số, link t.me,
@username...). Pyrogram chỉ chấp nhận `int` (chat id) hoặc `str` (username).
Nếu truyền chuỗi "-1001234567890" thì Pyrogram hiểu đó là *username* và ném
PEER_ID_INVALID — đây là nguyên nhân khiến download/upload fail.
"""
from __future__ import annotations

import re
from typing import Union

PeerId = Union[int, str]

_TME_PRIVATE = re.compile(r"(?:https?://)?t\.me/c/(\d+)(?:/\d+)?/?$", re.IGNORECASE)
_TME_PUBLIC = re.compile(r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,})(?:/\d+)?/?$", re.IGNORECASE)


def normalize_peer(value: PeerId | None) -> PeerId:
    """Trả về chat id dạng int hoặc username dạng str mà Pyrogram hiểu được."""
    if value is None:
        raise ValueError("peer rỗng")
    if isinstance(value, int):
        return value

    raw = str(value).strip()
    if not raw:
        raise ValueError("peer rỗng")

    # "-1001234567890" / "123456789"
    if re.fullmatch(r"-?\d+", raw):
        return int(raw)

    # https://t.me/c/1234567890/55  -> supergroup id -1001234567890
    m = _TME_PRIVATE.match(raw)
    if m:
        return int(f"-100{m.group(1)}")

    # https://t.me/username/55 -> "username"
    m = _TME_PUBLIC.match(raw)
    if m:
        return m.group(1)

    # "@username" -> "username"
    if raw.startswith("@"):
        return raw[1:]

    return raw


def extract_message_id(value: str | None) -> int | None:
    """Lấy msg_id từ link dạng https://t.me/c/123/456 hoặc https://t.me/user/456."""
    if not value:
        return None
    m = re.search(r"t\.me/(?:c/\d+|[A-Za-z][A-Za-z0-9_]{3,})/(\d+)", str(value), re.IGNORECASE)
    return int(m.group(1)) if m else None
