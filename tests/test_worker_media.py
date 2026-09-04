from types import SimpleNamespace

from worker.media_utils import is_supported_media, get_media_extension


def test_is_supported_media_accepts_document_and_photo_messages():
    document_message = SimpleNamespace(document=SimpleNamespace(file_size=123))
    photo_message = SimpleNamespace(photo=SimpleNamespace(file_size=456))

    assert is_supported_media(document_message) is True
    assert is_supported_media(photo_message) is True


def test_get_media_extension_uses_document_or_photo_extension():
    document_message = SimpleNamespace(document=SimpleNamespace(mime_type="application/pdf"))
    photo_message = SimpleNamespace(photo=SimpleNamespace())

    assert get_media_extension(document_message) == ".pdf"
    assert get_media_extension(photo_message) == ".jpg"
