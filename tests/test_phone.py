import pytest

from shared.phone import InvalidPhoneNumber, mask_phone, normalize_phone


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("+84987654321", "+84987654321"),
        ("  +84 987 654 321 ", "+84987654321"),
        ("+84-987-654-321", "+84987654321"),
        ("+84 (987) 654.321", "+84987654321"),
        ("0084987654321", "+84987654321"),
        ("84987654321", "+84987654321"),
    ],
)
def test_normalize_phone_accepts_common_formats(raw, expected):
    assert normalize_phone(raw) == expected


def test_local_number_needs_country_code():
    with pytest.raises(InvalidPhoneNumber) as exc:
        normalize_phone("0987654321")
    assert "quốc tế" in str(exc.value)


def test_local_number_with_default_country_code():
    assert normalize_phone("0987654321", "84") == "+84987654321"
    assert normalize_phone("0987654321", "+84") == "+84987654321"


@pytest.mark.parametrize("raw", [None, "", "   ", "abc", "+84abc123456", "+8498765432112345678"])
def test_normalize_phone_rejects_garbage(raw):
    with pytest.raises(InvalidPhoneNumber):
        normalize_phone(raw)


def test_too_short_number_is_rejected():
    with pytest.raises(InvalidPhoneNumber) as exc:
        normalize_phone("+8498")
    assert "chữ số" in str(exc.value)


def test_mask_phone_hides_middle():
    masked = mask_phone("+84987654321")
    assert masked.startswith("+84") and masked.endswith("321")
    assert "987654" not in masked
