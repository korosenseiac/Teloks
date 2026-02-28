"""
utils/media.py — Shared media classification helpers.

Centralises file-extension lookups, MIME type guessing, and type
classification used by both the TeraBox and MediaFire pipelines.
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Extension sets
# ---------------------------------------------------------------------------

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v", ".ts"}
AUDIO_EXTS = {".mp3", ".flac", ".aac", ".ogg", ".m4a", ".wav", ".opus"}
ARCHIVE_EXTS = {".zip", ".rar"}
MEDIA_EXTS = PHOTO_EXTS | VIDEO_EXTS

# ---------------------------------------------------------------------------
# Max file size (Telegram upload limit via raw API)
# ---------------------------------------------------------------------------

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ext(name: str) -> str:
    """Return the lower-cased file extension including the dot."""
    return os.path.splitext(name)[1].lower()


def classify(name: str) -> str:
    """Return 'photo', 'video', 'audio', or 'document'."""
    e = ext(name)
    if e in PHOTO_EXTS:
        return "photo"
    if e in VIDEO_EXTS:
        return "video"
    if e in AUDIO_EXTS:
        return "audio"
    return "document"


def mime(name: str) -> str:
    """Best-effort MIME type from extension."""
    e = ext(name)
    mapping = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
        ".gif": "image/gif", ".mp4": "video/mp4",
        ".mkv": "video/x-matroska", ".avi": "video/x-msvideo",
        ".mov": "video/quicktime", ".webm": "video/webm",
        ".flv": "video/x-flv", ".m4v": "video/x-m4v",
        ".ts": "video/mp2t", ".mp3": "audio/mpeg",
        ".flac": "audio/flac", ".aac": "audio/aac",
        ".ogg": "audio/ogg", ".m4a": "audio/x-m4a",
        ".wav": "audio/wav", ".opus": "audio/opus",
        ".pdf": "application/pdf",
        ".zip": "application/zip",
        ".rar": "application/x-rar-compressed",
    }
    return mapping.get(e, "application/octet-stream")


def is_media(name: str) -> bool:
    """Return True if the filename has a photo or video extension."""
    return ext(name) in MEDIA_EXTS


def is_archive(name: str) -> bool:
    """Return True if the filename has an archive extension (.zip / .rar)."""
    return ext(name) in ARCHIVE_EXTS
