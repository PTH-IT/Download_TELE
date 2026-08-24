from typing import Any


def is_supported_media(msg: Any) -> bool:
    return bool(
        getattr(msg, "video", None)
        or getattr(msg, "document", None)
        or getattr(msg, "photo", None)
    )


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
