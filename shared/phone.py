"""shared/phone.py — chuẩn hoá số điện thoại trước khi gửi cho Telegram.

Telegram chỉ nhận số ở định dạng quốc tế. Truyền "0987 379 557" hay
"0987-379-557" sẽ nhận về PHONE_NUMBER_INVALID mà không nói rõ sai ở đâu.
"""
from __future__ import annotations

import re

# Telegram/E.164: 8–15 chữ số sau dấu +
_MIN_DIGITS = 8
_MAX_DIGITS = 15

_SEPARATORS = re.compile(r"[\s\-.() ]")


class InvalidPhoneNumber(ValueError):
    """Số điện thoại không dùng được với Telegram."""


def normalize_phone(raw: str | None, default_country_code: str = "") -> str:
    """Trả về số dạng +<quốc gia><số>, hoặc ném InvalidPhoneNumber.

    default_country_code (ví dụ "84") chỉ được dùng cho số nội địa bắt đầu
    bằng 0. Nếu để trống thì số nội địa bị từ chối kèm hướng dẫn, an toàn hơn
    là đoán sai quốc gia rồi gửi OTP tới nhầm người.
    """
    if raw is None:
        raise InvalidPhoneNumber("Chưa nhập số điện thoại")

    value = _SEPARATORS.sub("", str(raw)).strip()
    if not value:
        raise InvalidPhoneNumber("Chưa nhập số điện thoại")

    # 0084... -> +84...
    if value.startswith("00"):
        value = "+" + value[2:]

    cc = _SEPARATORS.sub("", default_country_code or "").lstrip("+")

    if value.startswith("+"):
        digits = value[1:]
    elif value.startswith("0"):
        if not cc:
            raise InvalidPhoneNumber(
                "Cần nhập số ở định dạng quốc tế, ví dụ +84987654321 "
                "(hoặc đặt DEFAULT_COUNTRY_CODE trong .env để tự thêm mã vùng)"
            )
        digits = cc + value.lstrip("0")
    else:
        digits = value

    if not digits.isdigit():
        raise InvalidPhoneNumber(
            f"Số điện thoại chỉ được chứa chữ số và dấu + ở đầu: {raw!r}"
        )
    if not (_MIN_DIGITS <= len(digits) <= _MAX_DIGITS):
        raise InvalidPhoneNumber(
            f"Số điện thoại phải có {_MIN_DIGITS}–{_MAX_DIGITS} chữ số, "
            f"đang có {len(digits)}: {raw!r}"
        )

    return "+" + digits


def mask_phone(phone: str | None) -> str:
    """Che bớt số khi ghi log."""
    if not phone:
        return "?"
    tail = phone[-3:]
    return f"{phone[:3]}***{tail}" if len(phone) > 6 else "***"
