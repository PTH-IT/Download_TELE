import pytest

from shared.peers import extract_message_id, normalize_peer


@pytest.mark.parametrize(
    "raw,expected",
    [
        (-1001234567890, -1001234567890),
        ("-1001234567890", -1001234567890),
        ("123456789", 123456789),
        ("https://t.me/c/1234567890/55", -1001234567890),
        ("https://t.me/c/1234567890", -1001234567890),
        ("@mychannel", "mychannel"),
        ("t.me/mychannel", "mychannel"),
        ("https://t.me/mychannel/9", "mychannel"),
        ("mychannel", "mychannel"),
    ],
)
def test_normalize_peer(raw, expected):
    assert normalize_peer(raw) == expected


def test_normalize_peer_numeric_string_is_not_treated_as_username():
    # Đây chính là bug cũ: Pyrogram nhận "-100..." dạng str -> PEER_ID_INVALID
    assert isinstance(normalize_peer("-1001234567890"), int)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_normalize_peer_rejects_empty(raw):
    with pytest.raises(ValueError):
        normalize_peer(raw)


def test_extract_message_id():
    assert extract_message_id("https://t.me/c/1234567890/55") == 55
    assert extract_message_id("https://t.me/mychannel/9") == 9
    assert extract_message_id("mychannel") is None
    assert extract_message_id(None) is None
