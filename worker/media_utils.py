from typing import Any, Optional

# Thứ tự ưu tiên khi 1 message có nhiều loại media
_MEDIA_ATTRS = ("video", "document", "photo", "audio", "animation", "voice", "video_note")


def get_media(msg: Any) -> Optional[Any]:
    for attr in _MEDIA_ATTRS:
        media = getattr(msg, attr, None)
        if media:
            return media
    return None


def is_supported_media(msg: Any) -> bool:
    return get_media(msg) is not None


def get_media_size(msg: Any) -> int:
    media = get_media(msg)
    return int(getattr(media, "file_size", 0) or 0)


def get_media_extension(msg: Any) -> str:
    media = getattr(msg, "document", None) or getattr(msg, "photo", None) or getattr(msg, "video", None)
    if not media:
        return ".mp4"

    mime_type = getattr(media, "mime_type", "") or ""
    if getattr(msg, "photo", None) is not None:
        return ".jpg"
    if mime_type.startswith("image/"):
        return ".jpg"
    if mime_type.startswith("video/"):
        return ".mp4"
    if mime_type == "application/pdf":
        return ".pdf"
    if mime_type.startswith("audio/"):
        return ".mp3"
    return ".bin"


def get_media_kind(msg: Any) -> str:
    """Loại media dùng để chọn đúng hàm send_* khi upload."""
    if getattr(msg, "video", None) or getattr(msg, "animation", None) or getattr(msg, "video_note", None):
        return "video"
    if getattr(msg, "photo", None):
        return "photo"
    if getattr(msg, "audio", None) or getattr(msg, "voice", None):
        return "audio"

    doc = getattr(msg, "document", None)
    if doc:
        mime = (getattr(doc, "mime_type", "") or "").lower()
        if mime.startswith("video/"):
            return "video"
        if mime.startswith("image/"):
            return "photo"
        if mime.startswith("audio/"):
            return "audio"
    return "document"


_EXT_KIND = {
    ".mp4": "video", ".mkv": "video", ".mov": "video", ".avi": "video", ".webm": "video",
    ".jpg": "photo", ".jpeg": "photo", ".png": "photo", ".webp": "photo",
    ".mp3": "audio", ".m4a": "audio", ".ogg": "audio", ".flac": "audio",
}


def guess_kind_from_path(path: str) -> str:
    """Fallback khi task đã lưu trong DB và không còn object message gốc."""
    import os

    ext = os.path.splitext(path or "")[1].lower()
    return _EXT_KIND.get(ext, "document")
