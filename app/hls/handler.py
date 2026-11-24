"""
hls/handler.py — Telegram handlers for m3u8/HLS stream downloads.

Mirrors ``app/direct/handler.py``: the link is guarded, a per-user job slot is
reserved while the caption prompt is on screen, and the finished MP4 is uploaded
through the very same Direct pipeline (backup group → copy to user, custom
thumbnail, captions, automatic splitting for oversized files).

The only HLS-specific part is the download+remux step, which lives in
``app/hls/downloader.py``.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import inspect
import os
import re
import shutil
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from pyrogram import Client
from pyrogram.types import Message

import app.config as _cfg
from app.config import (
    BACKUP_GROUP_ID,
    HLS_BROWSER_ENABLED,
    HLS_BROWSER_TIMEOUT,
    HLS_ENABLED,
    HLS_MAX_HEIGHT,
    HLS_PAGE_EXTRACT,
    HLS_PAGE_HINT,
    HLS_PAGE_MAX_BYTES,
    HLS_PAGE_MAX_CANDIDATES,
    HLS_PROXY_FALLBACK_DIRECT,
)
from app.bot.session_manager import manager
from app.database.db import (
    get_user_session,
    get_user_profile,
    log_forward,
)
from app.direct.handler import (
    EXCLUSIVE_DOMAINS,
    _generate_video_thumb,
    _get_backup_group_peer,
    _get_video_metadata,
    _resize_thumb_high_quality,
    _send_to_user,
    _split_video_part,
    _upload_file_to_backup,
)
from app.hls.browser import browser_available, resolve_stream_with_browser
from app.hls.client import AUTO_PROXY, HlsClient, default_headers, read_proxy_url
from app.hls.downloader import (
    R_DOWNLOAD_FAILED,
    R_NOT_HLS,
    download_hls_to_mp4,
    reason_message,
)
from app.hls.extract import resolve_stream_from_page
from app.hls.playlist import HLS_LINK_PATTERN

from app.mediafire.streamer import FileStreamer
from app.terabox.progress import ProgressTracker
from app.utils.caption import (
    generate_video_caption,
    get_caption_choice_keyboard,
    set_caption_state,
)
from app.utils.media import MAX_FILE_SIZE, MAX_FILE_SIZE_PREMIUM
from app.utils.message import safe_edit, clear_reply_markup
from app.utils.streamer import SessionInvalidError
from app.warp import WarpProxyError, ensure_proxy_ready, is_proxy_enabled_async


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: A link that *might* be a video page: any http(s) URL no other pipeline owns.
#: The negative lookaheads stop this handler from ever stealing traffic from the
#: HLS (``.m3u8``), torrent/magnet, terabox, mediafire and ``t.me`` handlers, and
#: from wasting a page fetch on an obvious file download — those keep working
#: exactly as before. The patterns of the sibling pipelines are re-checked at
#: run time too (``_owned_by_other_pipeline``), so the list can never drift.
PAGE_LINK_PATTERN = re.compile(
    r"https?://"
    r"(?!(?:" + "|".join(re.escape(d) for d in sorted(EXCLUSIVE_DOMAINS)) + r"))"
    r"(?![^\s<>\"']*\.m3u8?(?:[?#]|$))"
    r"(?![^\s<>\"']*\.(?:torrent|mp4|mkv|avi|mov|webm|flv|wmv|ts|mp3|m4a|aac|"
    r"zip|rar|7z|tar|gz|pdf|docx?|xlsx?|apk|exe|iso|jpg|jpeg|png|gif|webp|"
    r"srt|vtt|ass)(?:[?#]|$))"
    r"[^\s<>\"']+",
    re.IGNORECASE,
)

#: Page slugs that say nothing about the video.
_USELESS_SLUGS = {
    "video", "videos", "watch", "view", "embed", "player", "play", "index",
    "page", "post", "v", "e", "w", "id", "download", "file", "stream",
}


def _owned_by_other_pipeline(text: str) -> bool:
    """True when another link handler must keep this message.

    Imported lazily (all of these modules are already loaded by the bot) so the
    only thing that has to stay in sync with them is this one list.
    """
    from app.mediafire.handler import MEDIAFIRE_LINK_PATTERN
    from app.terabox.handler import TERABOX_LINK_PATTERN
    from app.torrent.handler import MAGNET_LINK_PATTERN, TORRENT_URL_PATTERN

    return any(
        pattern.search(text)
        for pattern in (
            HLS_LINK_PATTERN, MAGNET_LINK_PATTERN, TORRENT_URL_PATTERN,
            TERABOX_LINK_PATTERN, MEDIAFIRE_LINK_PATTERN,
        )
    )


#: Webpage extensions stripped when deriving a display name from a page URL.
_PAGE_EXTS = {".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cfm", ".shtml"}

#: Headers stripped before passing captured browser/page headers into aiohttp/ffmpeg.
_STRIPPED_HEADERS = {
    "host", "content-length", "connection", "transfer-encoding",
    "accept-encoding", "te", "upgrade", "keep-alive", "proxy-connection",
}

#: Canonical casing for standard HTTP headers we merge/override.
_CANONICAL_HEADER_NAMES = {
    "user-agent": "User-Agent",
    "referer": "Referer",
    "origin": "Origin",
    "accept": "Accept",
    "accept-language": "Accept-Language",
    "authorization": "Authorization",
    "sec-fetch-dest": "Sec-Fetch-Dest",
    "sec-fetch-mode": "Sec-Fetch-Mode",
    "sec-fetch-site": "Sec-Fetch-Site",
    "sec-ch-ua": "Sec-Ch-Ua",
    "sec-ch-ua-mobile": "Sec-Ch-Ua-Mobile",
    "sec-ch-ua-platform": "Sec-Ch-Ua-Platform",
}

_URL_QUERY_RE = re.compile(r"(https?://[^\s<>\"'?]+)\?[^\s<>\"']+")
_PROXY_CRED_RE = re.compile(r"([a-zA-Z0-9+.-]+://)[^/@\s]+:[^/@\s]*@")


def _clean_extracted_url(raw: str) -> str:
    """Strip whitespace and trailing prose punctuation from a matched URL."""
    url = (raw or "").strip()
    while url:
        if url[-1] in ".,;:'\"!?\\":
            url = url[:-1]
            continue
        if url[-1] == ")" and url.count(")") > url.count("("):
            url = url[:-1]
            continue
        if url[-1] == "]" and url.count("]") > url.count("["):
            url = url[:-1]
            continue
        if url[-1] == "}" and url.count("}") > url.count("{"):
            url = url[:-1]
            continue
        break
    return url


def _origin_of(url: Optional[str]) -> str:
    """Return ``scheme://netloc`` for *url*, or ``""`` when invalid."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def _sanitize_filename(name: str, fallback: str = "stream") -> str:
    """Replace filesystem-unsafe characters and trim excessive length."""
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "-", unquote(name or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-_")
    return (cleaned or fallback)[:120]


def _sanitize_error_text(text: Any) -> str:
    """Strip query tokens and proxy credentials from user-facing error text."""
    cleaned = _PROXY_CRED_RE.sub(r"\1***@", str(text or ""))
    cleaned = _URL_QUERY_RE.sub(r"\1", cleaned)
    return cleaned.strip()


_AUTH_HTTP_RE = re.compile(r"\bHTTP\s+(?:401|403)\b", re.IGNORECASE)
_EXPIRED_PLAYLIST_HTTP_RE = re.compile(r"playlist:.*\bHTTP\s+(?:404|410)\b", re.IGNORECASE)

# slot_sid -> (created_ts, metadata dict) so app/bot/main.py needs no signature change.
_STREAM_META_TTL = 1800.0
_PENDING_STREAM_META: Dict[str, Tuple[float, Dict[str, Any]]] = {}


def _prune_stream_meta() -> None:
    now = time.time()
    for sid, (ts, _) in list(_PENDING_STREAM_META.items()):
        if now - ts > _STREAM_META_TTL:
            _PENDING_STREAM_META.pop(sid, None)


def _store_stream_meta(sid: Optional[str], meta: Dict[str, Any]) -> None:
    if not sid or not meta:
        return
    _prune_stream_meta()
    _PENDING_STREAM_META[sid] = (time.time(), dict(meta))


def _pop_stream_meta(sid: Optional[str]) -> Dict[str, Any]:
    if not sid:
        return {}
    _prune_stream_meta()
    item = _PENDING_STREAM_META.pop(sid, None)
    return dict(item[1]) if item else {}


async def _safe_is_warp_active() -> bool:
    try:
        return bool(await is_proxy_enabled_async())
    except Exception:
        return False


def _is_auth_http_failure(result: Any) -> bool:
    if getattr(result, "converted", False):
        return False
    if getattr(result, "reason", "") != R_DOWNLOAD_FAILED:
        return False
    detail = str(getattr(result, "detail", "") or "")
    return bool(_AUTH_HTTP_RE.search(detail))


def _should_reextract(result: Any) -> bool:
    """True when a page-resolved download likely failed due to an expired stream token."""
    if getattr(result, "converted", False):
        return False
    if _is_auth_http_failure(result):
        return True
    reason = getattr(result, "reason", "")
    detail = str(getattr(result, "detail", "") or "")
    if reason == R_NOT_HLS:
        return True
    if reason == R_DOWNLOAD_FAILED and _EXPIRED_PLAYLIST_HTTP_RE.search(detail):
        return True
    return False


def _format_hls_error(
    reason: str,
    detail: str = "",
    *,
    from_page: bool = False,
    warp_active: bool = False,
) -> str:
    """Render a user-facing HLS failure message with sanitized detail and actionable hints."""
    clean_detail = _sanitize_error_text(detail)
    try:
        params = inspect.signature(reason_message).parameters
    except (TypeError, ValueError):
        params = {}
    if len(params) >= 2 or "detail" in params:
        msg = _sanitize_error_text(reason_message(reason, clean_detail))
    else:
        base = reason_message(reason)
        if clean_detail and clean_detail not in base:
            msg = f"{base}\nℹ️ `{clean_detail[:200]}`"
        else:
            msg = base

    if reason == R_DOWNLOAD_FAILED and _AUTH_HTTP_RE.search(clean_detail):
        tips = []
        if from_page:
            tips.append(
                "💡 Pelayan strim menyekat permintaan (HTTP 401/403) walaupun selepas mengambil semula halaman."
            )
        else:
            tips.append(
                "💡 Pautan `.m3u8` mungkin mempunyai token tamat tempoh atau terikat pada IP pelayar anda — "
                "cuba hantar pautan halaman video/embed asal dan bukannya URL `.m3u8` terus."
            )
        if warp_active:
            tips.append(
                "🛡️ WARP sedang aktif — jika CDN menyekat IP WARP, cuba `/warp_off` dan muat turun semula."
            )
        msg = f"{msg}\n\n" + "\n".join(tips)
    elif reason == R_NOT_HLS and not from_page:
        msg = (
            f"{msg}\n\n"
            "💡 Pautan ini mungkin sudah tamat tempoh atau memerlukan halaman asalnya — "
            "cuba hantar pautan halaman video asal."
        )
    return msg


def _parse_cookie_header(raw_cookie: str) -> Dict[str, str]:
    """Parse an HTTP ``Cookie`` header string into ``{name: value}``."""
    cookies: Dict[str, str] = {}
    for part in (raw_cookie or "").split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            cookies[k] = v
    return cookies


def _merge_request_context(
    url: str,
    page_url: Optional[str],
    captured_headers: Optional[Dict[str, str]],
    captured_cookies: Optional[Dict[str, str]],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build case-deduplicated headers and cookies for the HLS download."""
    merged: Dict[str, str] = {}
    lower_to_key: Dict[str, str] = {}
    cookies: Dict[str, str] = {}

    def _put(key: str, value: str) -> None:
        if not key or value is None:
            return
        low = key.strip().lower()
        if low.startswith(":") or low in _STRIPPED_HEADERS:
            return
        if low == "cookie":
            cookies.update(_parse_cookie_header(str(value)))
            return
        canonical = _CANONICAL_HEADER_NAMES.get(low, key.strip())
        old_key = lower_to_key.get(low)
        if old_key and old_key != canonical:
            merged.pop(old_key, None)
        merged[canonical] = str(value).strip()
        lower_to_key[low] = canonical

    if captured_headers:
        for k, v in default_headers(url).items():
            if k.lower() in ("user-agent", "accept", "accept-language"):
                _put(k, v)
        for k, v in captured_headers.items():
            if not k or not v:
                continue
            _put(k, v)
    elif page_url:
        for k, v in default_headers(url, referer=page_url).items():
            _put(k, v)
    else:
        for k, v in default_headers(url).items():
            _put(k, v)

    if captured_cookies:
        for k, v in captured_cookies.items():
            if k and v is not None:
                cookies[str(k)] = str(v)

    return merged, cookies


def _name_from_url(url: str) -> str:
    """Derive a display name from an m3u8 URL (falls back to ``stream.m3u8``)."""
    path = unquote(url.split("?", 1)[0]).rstrip("/")
    base = os.path.basename(path)
    if not base or base in (".", "/"):
        return "stream.m3u8"
    stem, ext = os.path.splitext(base)
    if not stem:
        return "stream.m3u8"
    # Paths like /hls/my_show/master.m3u8 → "master" is useless; use the parent segment.
    parent = os.path.basename(os.path.dirname(path))
    if stem.lower() in ("index", "playlist", "master", "manifest", "out") and parent:
        return f"{_sanitize_filename(parent)}.m3u8"
    if ext.lower() in (".m3u8", ".m3u"):
        return f"{_sanitize_filename(stem)}{ext.lower()}"
    return f"{_sanitize_filename(stem)}.m3u8"


def _name_from_page(page_url: Optional[str]) -> Optional[str]:
    """Display name from a page URL (``/video/135123-fc2-ppv-4332967.html``).

    A page-derived job would otherwise be named after the CDN path
    (``upnuod4v9rr3_,n,.urlset``), which tells the user nothing. Returns ``None``
    when the slug carries no information, so the caller falls back to the
    manifest URL.
    """
    if not page_url:
        return None
    try:
        slug = unquote(os.path.basename(urlparse(page_url).path.rstrip("/")))
    except ValueError:
        # Malformed URL (e.g. an unmatched '[' in the host): no name from it.
        return None
    base, ext = os.path.splitext(slug)
    stem = base if ext.lower() in _PAGE_EXTS else slug
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-._")
    if len(stem) < 3 or stem.lower() in _USELESS_SLUGS:
        return None
    return stem[:120]


def _name_from_title(title: Optional[str]) -> Optional[str]:
    """Clean an extracted video title into a filesystem-safe stem, or ``None``."""
    if not title or not isinstance(title, str):
        return None
    cleaned = _sanitize_filename(title, fallback="")
    if len(cleaned) < 2 or cleaned.lower() in _USELESS_SLUGS:
        return None
    return cleaned[:120]


def _choose_job_filename(
    url: str,
    page_url: Optional[str],
    title: Optional[str],
) -> str:
    """Choose display filename preserving dots (e.g. ``.1080p``) before ``.mp4`` conversion."""
    title_stem = _name_from_title(title)
    if title_stem:
        return f"{title_stem}.m3u8"
    page_stem = _name_from_page(page_url)
    if page_stem:
        return f"{page_stem}.m3u8"
    return _name_from_url(url)


async def _load_custom_thumb(bot: Client, message: Message) -> Optional[bytes]:
    """Load the user's custom thumbnail (document > photo), like Direct does."""
    source = None
    if (message.document and message.document.mime_type
            and message.document.mime_type.startswith("image/")):
        source = "document"
    elif message.photo:
        source = "photo"
    if not source:
        return None

    try:
        if source == "document":
            thumb_path = await message.download()
        else:
            thumb_path = await bot.download_media(message.photo.file_id)
        if not thumb_path:
            return None
        with open(thumb_path, "rb") as fh:
            thumb_raw = fh.read()
        try:
            os.remove(thumb_path)
        except OSError:
            pass
        thumb_raw = _resize_thumb_high_quality(thumb_raw)
        print(f"[HLS] Custom thumbnail loaded ({source}): {len(thumb_raw)} bytes")
        return thumb_raw
    except Exception as e:
        print(f"[HLS] Failed to load custom thumbnail: {e}")
        return None


# ---------------------------------------------------------------------------
# Entry point — called by the bot's message handler
# ---------------------------------------------------------------------------

async def _ensure_user_ready(message: Message) -> Optional[int]:
    """Verify slot capacity, Telegram session and profile; reply on failure."""
    from app.bot.main import has_free_slot, process_limit_message

    user = getattr(message, "from_user", None)
    if not user:
        return None
    user_id = user.id

    if not has_free_slot(user_id):
        await message.reply_text(process_limit_message())
        return None

    user_session = await get_user_session(user_id)
    if not user_session:
        await message.reply_text("❌ Belum login. Sila /start untuk login.")
        return None

    user_client = await manager.get_client(user_id)
    if not user_client:
        await message.reply_text("❌ Sesi tidak sah. Sila login semula.")
        return None

    user_profile = await get_user_profile(user_id)
    if not user_profile:
        await message.reply_text(
            "⚠️ **Profile belum lengkap!**\n\n"
            "Sila set profile anda terlebih dahulu.\n\n"
            "👇 **Pilih jantina anda:**"
        )
        return None

    return user_id


async def _begin_hls_job(
    bot: Client,
    message: Message,
    url: str,
    *,
    page_url: Optional[str] = None,
    force_direct: bool = False,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    stream_type: str = "hls",
    title: str = "",
    audio_url: Optional[str] = None,
    audio_headers: Optional[Dict[str, str]] = None,
    manifest_text: str = "",
    cookie_entries: Optional[List[Dict[str, Any]]] = None,
    fallback_urls: Optional[List[str]] = None,
    extractor: str = "",
    orig_page_url: Optional[str] = None,
    _user_verified: bool = False,
) -> bool:
    """Guard, reserve a slot and ask about captions for *url*.

    Shared by both entry points (a raw ``.m3u8`` link, and a page we extracted a
    stream from). Returns True when the message is handled — a job was started,
    or the user was told why it cannot start — and False when HLS is disabled,
    in which case the caller leaves the message to the Direct handler.
    """
    from app.bot.main import process_limit_message, reserve_process

    if not HLS_ENABLED:
        # Nothing to do: the caller leaves the message to the Direct handler.
        return False

    user = getattr(message, "from_user", None)
    if not user:
        return False

    if _user_verified:
        user_id = user.id
    else:
        verified = await _ensure_user_ready(message)
        if verified is None:
            return True
        user_id = verified

    # Never log a signed URL in full: its query string is the credential.
    shown = url.split("?", 1)[0]
    if page_url:
        print(
            f"[HLS] user={user_id} type={stream_type} via={extractor or 'page'} "
            f"page={page_url.split('?', 1)[0]} -> {shown[:100]}"
        )
    else:
        print(f"[HLS] user={user_id} type={stream_type} url={shown[:100]}")

    # Reserve the slot *before* the caption prompt: the prompt is already a job,
    # so spamming links can never start more jobs than allowed.
    slot = reserve_process(user_id, "hls", prompt_msg=message)
    if slot is None:
        await message.reply_text(process_limit_message())
        return True

    prompt_heading = (
        "🎬 **Tetapan Caption Video (HLS/m3u8)**"
        if stream_type == "hls"
        else "🎬 **Tetapan Caption Video**"
    )
    status_msg = await message.reply_text(
        f"{prompt_heading}\n\n"
        "Adakah anda mahu meletakkan nama fail sebagai caption pada video?\n\n"
        "💡 *Video/strim akan dimuat turun dan ditukar ke MP4 secara automatik "
        "(stream copy — tanpa turun kualiti).*\n"
        "💡 *Jika Ya, nama fail akan dijadikan caption (format fail seperti "
        ".m3u8/.mp4 akan dibuang secara automatik).*",
        reply_markup=get_caption_choice_keyboard(slot.sid),
    )
    slot.status_msg = status_msg

    _store_stream_meta(
        slot.sid,
        {
            "stream_type": stream_type or "hls",
            "title": title or "",
            "audio_url": audio_url,
            "audio_headers": dict(audio_headers) if audio_headers else None,
            "manifest_text": manifest_text or "",
            "cookie_entries": list(cookie_entries) if cookie_entries else [],
            "fallback_urls": list(fallback_urls) if fallback_urls else [],
            "extractor": extractor or "",
            "orig_page_url": orig_page_url or page_url or "",
        },
    )

    set_caption_state(
        user_id=user_id,
        flow_type="hls",
        message=message,
        status_msg=status_msg,
        sid=slot.sid,
        url=url,
        page_url=page_url or "",
        force_direct=force_direct,
        headers=dict(headers) if headers else None,
        cookies=dict(cookies) if cookies else None,
    )
    return True


async def hls_link_handler(bot: Client, message: Message) -> None:
    """Validate an m3u8 link, reserve a job slot and ask about captions.

    The download itself only starts once the caption flow is answered (see
    ``process_hls_download``), exactly like the Direct and Torrent handlers.
    """
    if not getattr(message, "from_user", None):
        return
    message_text = message.text or message.caption or ""
    match = HLS_LINK_PATTERN.search(message_text)
    if not match:
        await message.reply_text("❌ Tidak dapat mengesan URL m3u8 yang sah.")
        return
    url = _clean_extracted_url(match.group(0))
    if not url:
        await message.reply_text("❌ Tidak dapat mengesan URL m3u8 yang sah.")
        return
    await _begin_hls_job(bot, message, url)


@dataclass
class _ResolvedPage:
    """Outcome of resolving a video page/embed URL into a stream + request context."""
    url: str = ""
    page_url: str = ""
    force_direct: bool = False
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    player_page: bool = False
    stage1_error: bool = False
    stream_type: str = "hls"
    title: str = ""
    audio_url: Optional[str] = None
    audio_headers: Optional[Dict[str, str]] = None
    manifest_text: str = ""
    cookie_entries: List[Dict[str, Any]] = field(default_factory=list)
    fallback_urls: List[str] = field(default_factory=list)
    extractor: str = ""


def _resolved_from_scan(
    scan: Any,
    *,
    default_page_url: str,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    force_direct: bool = False,
    player_page: bool = False,
    extractor: str = "",
) -> _ResolvedPage:
    raw_entries = getattr(scan, "cookie_entries", None)
    raw_fallbacks = getattr(scan, "fallback_urls", None)
    raw_audio_hdrs = getattr(scan, "audio_headers", None)
    return _ResolvedPage(
        url=getattr(scan, "url", "") or "",
        page_url=getattr(scan, "page_url", "") or default_page_url,
        force_direct=force_direct or bool(getattr(scan, "force_direct", False)),
        headers=dict(headers if headers is not None else (getattr(scan, "headers", None) or {})),
        cookies=dict(cookies if cookies is not None else (getattr(scan, "cookies", None) or {})),
        player_page=player_page or bool(getattr(scan, "player_page", False)),
        stage1_error=False,
        stream_type=(getattr(scan, "stream_type", None) or "hls").strip().lower() or "hls",
        title=getattr(scan, "title", "") or "",
        audio_url=getattr(scan, "audio_url", None) or None,
        audio_headers=dict(raw_audio_hdrs) if isinstance(raw_audio_hdrs, dict) else None,
        manifest_text=getattr(scan, "manifest_text", "") or "",
        cookie_entries=list(raw_entries) if isinstance(raw_entries, list) else [],
        fallback_urls=list(raw_fallbacks) if isinstance(raw_fallbacks, list) else [],
        extractor=getattr(scan, "extractor", "") or extractor,
    )


async def _ytdlp_stage(
    page_url: str,
    fallback_page: str,
) -> Optional[_ResolvedPage]:
    """Tier 2: resolve known video sites via yt-dlp without launching Chromium."""
    if not getattr(_cfg, "HLS_YTDLP_ENABLED", True):
        return None
    try:
        from app.hls import ytdlp as _ytdlp_mod
    except Exception:
        return None
    avail_fn = getattr(_ytdlp_mod, "ytdlp_available", None)
    extract_fn = getattr(_ytdlp_mod, "extract_with_ytdlp", None)
    if not callable(extract_fn) or (callable(avail_fn) and not avail_fn()):
        return None

    proxy_url: Optional[str] = None
    warp_cfg = await ensure_proxy_ready(
        require_http=False, require_socks5=True, feature="HLS",
    )
    if warp_cfg is not None:
        proxy_url = warp_cfg.socks5_url
    else:
        proxy_url = read_proxy_url()

    timeout = float(getattr(_cfg, "HLS_YTDLP_TIMEOUT", 25.0) or 25.0)
    try:
        stream = await extract_fn(
            page_url,
            proxy=proxy_url,
            max_height=HLS_MAX_HEIGHT,
            timeout=timeout,
        )
    except (asyncio.CancelledError, WarpProxyError):
        raise
    except Exception as e:
        print(f"[HLS] yt-dlp stage failed for {page_url.split('?', 1)[0]}: {type(e).__name__}: {e}")
        return None

    if stream is not None and getattr(stream, "url", None):
        return _resolved_from_scan(
            stream,
            default_page_url=fallback_page or page_url,
            extractor="yt-dlp",
        )
    return None


async def _browser_stage_resolved(
    message: Message,
    page_url: str,
    fallback_page: str,
    *,
    notify_status: bool = True,
) -> Optional[_ResolvedPage]:
    """Tier 3: run the page in headless Chromium and return a full ``_ResolvedPage``."""
    if not HLS_BROWSER_ENABLED or not browser_available():
        return None

    status_msg = None
    if notify_status:
        try:
            # This stage can take a while, so say something instead of staying quiet.
            status_msg = await message.reply_text("🌐 Membuka halaman di pelayar…")
        except Exception:
            status_msg = None

    try:
        scan = await resolve_stream_with_browser(page_url, timeout=HLS_BROWSER_TIMEOUT)
    except WarpProxyError:
        raise
    except Exception as e:
        print(f"[HLS] Browser stage failed for {page_url.split('?', 1)[0]}: "
              f"{type(e).__name__}: {e}")
        return None
    finally:
        if status_msg is not None:
            try:
                await status_msg.delete()
            except Exception:
                pass

    if scan.url:
        used_warp = bool(getattr(scan, "used_warp", False))
        force_direct = (not used_warp) and (not scan.used_proxy) and (read_proxy_url() is not None)
        if force_direct:
            print("[HLS] Browser had to go direct while proxy.txt is configured - "
                  "this job will download directly too, so the minted URL matches "
                  "the address it was minted for")
        return _resolved_from_scan(
            scan,
            default_page_url=page_url,
            force_direct=force_direct,
            extractor="browser",
        )
    detail = f" - {scan.detail}" if scan.detail else ""
    print(f"[HLS] Browser stage: no manifest in {page_url.split('?', 1)[0]} "
          f"({scan.reason or 'unknown'}{detail})")
    return None


async def _browser_stage(
    message: Message,
    page_url: str,
    fallback_page: str,
    *,
    notify_status: bool = True,
) -> Tuple[str, str, bool, Dict[str, str], Dict[str, str]]:
    """Backward-compatible wrapper around :func:`_browser_stage_resolved`."""
    resolved = await _browser_stage_resolved(
        message, page_url, fallback_page, notify_status=notify_status,
    )
    if resolved is not None and resolved.url:
        return (
            resolved.url,
            resolved.page_url,
            resolved.force_direct,
            resolved.headers,
            resolved.cookies,
        )
    return "", fallback_page, False, {}, {}


async def _scan_page_with_client(
    page_url: str,
    page_headers: Dict[str, str],
    *,
    proxy_url: Any = AUTO_PROXY,
) -> Tuple[Any, Dict[str, str], Dict[str, str]]:
    """Run ``resolve_stream_from_page`` and preserve any captured headers/cookies."""
    client = HlsClient(page_headers, proxy_url=proxy_url)
    try:
        scan = await resolve_stream_from_page(
            client,
            page_url,
            max_bytes=HLS_PAGE_MAX_BYTES,
            limit=HLS_PAGE_MAX_CANDIDATES,
        )
        scan_headers = dict(getattr(scan, "headers", None) or {})
        scan_cookies = dict(getattr(scan, "cookies", None) or {})
        if not scan_cookies and hasattr(client, "export_cookies"):
            try:
                target = scan.url or getattr(scan, "page_url", "") or page_url
                scan_cookies = dict(client.export_cookies(target) or {})
            except Exception:
                pass
        if not getattr(scan, "cookie_entries", None) and hasattr(client, "export_cookie_entries"):
            try:
                entries = client.export_cookie_entries()
                if entries and hasattr(scan, "cookie_entries"):
                    scan.cookie_entries = list(entries)
            except Exception:
                pass
        return scan, scan_headers, scan_cookies
    finally:
        await client.close()


async def _resolve_page_manifest(
    message: Message,
    page_url: str,
    *,
    notify_browser: bool = True,
) -> _ResolvedPage:
    """Resolve *page_url* across Tier 0/1 (HTTP), Tier 2 (yt-dlp), Tier 3 (browser), & direct fallback."""
    # Do not send fabricated Origin/self-Referer on initial GET page navigation (WAF red flag).
    page_headers = {
        k: v for k, v in default_headers(page_url).items()
        if k.lower() not in ("origin", "referer")
    }

    try:
        scan, scan_headers, scan_cookies = await _scan_page_with_client(
            page_url, page_headers, proxy_url=AUTO_PROXY,
        )
    except (asyncio.CancelledError, WarpProxyError):
        raise
    except Exception as e:
        print(f"[HLS] Page scan failed for {page_url.split('?', 1)[0]}: "
              f"{type(e).__name__}: {e}")
        return _ResolvedPage(page_url=page_url, stage1_error=True)

    url = scan.url or ""
    page_used = getattr(scan, "page_url", "") or page_url
    player_page = bool(getattr(scan, "player_page", False))
    scan_title = getattr(scan, "title", "") or ""

    if url:
        if not scan_headers:
            scan_headers = dict(default_headers(url, referer=page_used))
        return _resolved_from_scan(
            scan,
            default_page_url=page_used,
            headers=scan_headers,
            cookies=scan_cookies,
            player_page=player_page,
            extractor="http",
        )

    detail = f" - {scan.detail}" if getattr(scan, "detail", "") else ""
    print(f"[HLS] Page scan: no manifest in {page_url.split('?', 1)[0]} "
          f"({scan.reason}, {scan.candidates} candidate(s){detail})")

    if scan.reason != "not-a-page":
        # Tier 2: yt-dlp (fast, no Chromium RAM overhead for 1,800+ supported sites).
        yt_resolved = await _ytdlp_stage(page_url, page_used)
        if yt_resolved is not None and yt_resolved.url:
            if not yt_resolved.title and scan_title:
                yt_resolved.title = scan_title
            return yt_resolved

        # Tier 3: Headless Chromium sniffer.
        br_resolved = await _browser_stage_resolved(
            message, page_url, page_used, notify_status=notify_browser,
        )
        if br_resolved is not None and br_resolved.url:
            if not br_resolved.title and scan_title:
                br_resolved.title = scan_title
            return br_resolved

        # Direct HTTP retry if proxy.txt failed and WARP is OFF.
        if (
            scan.reason == "error"
            and HLS_PROXY_FALLBACK_DIRECT
            and read_proxy_url() is not None
            and not await is_proxy_enabled_async()
        ):
            try:
                direct_scan, direct_hdrs, direct_cookies = (
                    await _scan_page_with_client(
                        page_url, page_headers, proxy_url=None,
                    )
                )
                if direct_scan.url:
                    print("[HLS] Page scan succeeded directly after proxy error")
                    eff_page = getattr(direct_scan, "page_url", "") or page_url
                    eff_hdrs = direct_hdrs or dict(default_headers(direct_scan.url, referer=eff_page))
                    return _resolved_from_scan(
                        direct_scan,
                        default_page_url=eff_page,
                        headers=eff_hdrs,
                        cookies=direct_cookies,
                        force_direct=True,
                        player_page=player_page,
                        extractor="http-direct",
                    )
            except (asyncio.CancelledError, WarpProxyError):
                raise
            except Exception as e:
                print(f"[HLS] Direct page retry failed: {type(e).__name__}: {e}")

    return _ResolvedPage(
        url="",
        page_url=page_used,
        force_direct=False,
        headers=dict(scan_headers or {}),
        cookies=dict(scan_cookies or {}),
        player_page=player_page,
        stage1_error=False,
        title=scan_title,
    )


async def page_hls_handler(bot: Client, message: Message) -> bool:
    """Start an HLS/video job from a *page* link, using the stream it embeds.

    This is the cure for CDNs that only honour playlist URLs minted for the
    client that loaded the page (the token carries that client's IP/ASN, so a
    URL copied out of a browser gets 403 from this server whatever headers it
    sends). Loading the page ourselves — same proxy, same headers as the
    download — makes the site mint the playlist for *us*, and the child URLs
    inside that master are then signed for us too.

    Returns True when the message is claimed (a job started, or the user was
    told why it cannot start) and False when the link should keep flowing to the
    Direct handler — which is the case for every link that turns out not to be a
    video page.
    """
    if not (HLS_ENABLED and HLS_PAGE_EXTRACT):
        return False
    if not getattr(message, "from_user", None):
        return False

    message_text = message.text or message.caption or ""
    match = PAGE_LINK_PATTERN.search(message_text)
    if not match or _owned_by_other_pipeline(message_text):
        return False
    page_url = _clean_extracted_url(match.group(0))
    if not page_url:
        return False

    # Never spend a page fetch or browser launch on a user who cannot start a job.
    if await _ensure_user_ready(message) is None:
        return True

    try:
        resolved = await _resolve_page_manifest(message, page_url, notify_browser=True)
    except WarpProxyError as e:
        print(f"[HLS] WARP proxy error during page scan for {page_url.split('?', 1)[0]}: {e}")
        try:
            await message.reply_text(f"❌ {e}")
        except Exception:
            pass
        return True

    if resolved.stage1_error:
        return False

    if not resolved.url:
        if HLS_PAGE_HINT and resolved.player_page:
            warp_active = await _safe_is_warp_active()
            hint_text = (
                "🔎 Halaman ini mengandungi pemain video tetapi pautan strimnya tidak "
                "dapat dibaca secara automatik.\n\n"
                "💡 Sila cuba hantar pautan halaman video/embed terus (`player`/`embed` URL).\n"
                "⚠️ Elakkan menyalin URL `.m3u8` terus dari pelayar (DevTools) kerana "
                "token biasanya terikat pada IP pelayar anda dan akan gagal dengan ralat HTTP 403."
            )
            if warp_active:
                hint_text += (
                    "\n🛡️ WARP sedang aktif — jika halaman/CDN menyekat WARP, cuba `/warp_off` dan hantar semula."
                )
            try:
                await message.reply_text(hint_text)
            except Exception:
                pass
            return True
        return False

    print(
        f"[HLS] Page scan: {resolved.stream_type} stream found via "
        f"{resolved.extractor or 'page'} ({resolved.page_url.split('?', 1)[0]})"
    )
    return await _begin_hls_job(
        bot,
        message,
        resolved.url,
        page_url=resolved.page_url,
        force_direct=resolved.force_direct,
        headers=resolved.headers,
        cookies=resolved.cookies,
        stream_type=resolved.stream_type,
        title=resolved.title,
        audio_url=resolved.audio_url,
        audio_headers=resolved.audio_headers,
        manifest_text=resolved.manifest_text,
        cookie_entries=resolved.cookie_entries,
        fallback_urls=resolved.fallback_urls,
        extractor=resolved.extractor,
        orig_page_url=page_url,
        _user_verified=True,
    )


async def _call_download_hls(
    url: str,
    file_name: str,
    *,
    headers: Dict[str, str],
    cookies: Dict[str, str],
    status_cb: Any,
    on_chunk: Any,
    on_meta: Any,
    out_dir: str,
    proxy_url: Any,
    cookie_entries: Optional[List[Dict[str, Any]]] = None,
    stream_type: str = "hls",
    audio_url: Optional[str] = None,
    audio_headers: Optional[Dict[str, str]] = None,
    fallback_urls: Optional[List[str]] = None,
    initial_playlist_text: str = "",
    page_url: str = "",
) -> Any:
    """Invoke ``download_hls_to_mp4``, propagating cookies and stream metadata safely."""
    dl_kwargs: Dict[str, Any] = {
        "headers": headers,
        "status_cb": status_cb,
        "on_chunk": on_chunk,
        "on_meta": on_meta,
        "out_dir": out_dir,
        "proxy_url": proxy_url,
    }
    try:
        params = inspect.signature(download_hls_to_mp4).parameters
    except (TypeError, ValueError):
        params = {}
    if "cookies" in params:
        dl_kwargs["cookies"] = cookies or None
    elif cookies:
        cookie_hdr = "; ".join(f"{k}={v}" for k, v in cookies.items())
        if cookie_hdr:
            dl_kwargs["headers"] = {**headers, "Cookie": cookie_hdr}

    optional_args: Dict[str, Any] = {
        "cookie_entries": cookie_entries or None,
        "stream_type": stream_type or "hls",
        "audio_url": audio_url,
        "audio_headers": audio_headers,
        "fallback_urls": fallback_urls or None,
        "initial_playlist_text": initial_playlist_text or "",
        "page_url": page_url or "",
    }
    for k, v in optional_args.items():
        if k in params and (v is not None and v != "" and (k != "stream_type" or v != "hls")):
            dl_kwargs[k] = v
    return await download_hls_to_mp4(url, file_name, **dl_kwargs)


# ---------------------------------------------------------------------------
# Job — download the HLS stream, remux to MP4, upload it
# ---------------------------------------------------------------------------

async def process_hls_download(
    bot: Client,
    user_id: int,
    message: Message,
    status_msg: Message,
    *,
    url: Optional[str] = None,
    page_url: Optional[str] = None,
    force_direct: bool = False,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    enable_caption: bool = False,
    exclude_words: Optional[str] = None,
    slot_sid: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Download an HLS stream and deliver the resulting MP4 to the user.

    Reuses the Direct pipeline for everything after the download: backup-group
    upload, custom/generated thumbnail, caption, multi-part splitting, delivery
    to the user and logging.
    """
    from app.bot.main import (
        begin_process, get_process_slot, has_free_slot, is_cancelled,
        process_cancel_keyboard, process_limit_message, release_process,
    )

    user_id = user_id or message.from_user.id
    stream_meta = _pop_stream_meta(slot_sid)
    url = url or kwargs.get("url")
    page_url = page_url or kwargs.get("page_url")
    force_direct = bool(force_direct or kwargs.get("force_direct"))
    captured_headers = headers if headers is not None else kwargs.get("headers")
    captured_cookies = cookies if cookies is not None else kwargs.get("cookies")
    stream_type = kwargs.get("stream_type") or stream_meta.get("stream_type") or "hls"
    title = kwargs.get("title") or stream_meta.get("title") or ""
    audio_url = kwargs.get("audio_url") or stream_meta.get("audio_url")
    audio_headers = kwargs.get("audio_headers") or stream_meta.get("audio_headers")
    manifest_text = kwargs.get("manifest_text") or stream_meta.get("manifest_text") or ""
    cookie_entries = kwargs.get("cookie_entries") or stream_meta.get("cookie_entries") or []
    fallback_urls = kwargs.get("fallback_urls") or stream_meta.get("fallback_urls") or []
    orig_page_url = kwargs.get("orig_page_url") or stream_meta.get("orig_page_url") or page_url or ""
    if not url:
        await safe_edit(status_msg, "❌ URL tidak sah.")
        return

    is_premium = getattr(message.from_user, "is_premium", False) or False
    size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE

    user_client = await manager.get_client(user_id)
    if not user_client:
        await safe_edit(status_msg, "❌ Sesi tidak sah. Sila login semula.")
        return

    # Adopt the slot reserved when the link was accepted.
    slot = begin_process(user_id, slot_sid, "hls", status_msg)
    if slot is None:
        if slot_sid and get_process_slot(slot_sid) is None:
            print(f"[HLS] job {slot_sid} was cancelled before it started")
            await safe_edit(status_msg, "🚫 **Proses dibatalkan.**")
        elif not has_free_slot(user_id):
            await safe_edit(status_msg, process_limit_message())
        return

    cancel_kb = process_cancel_keyboard(slot.sid)
    if status_msg is None:
        status_msg = await message.reply_text("🔄 Sedang Diproses..", reply_markup=cancel_kb)
    await safe_edit(
        status_msg, "🔄 Sedang Diproses..",
        reply_markup=cancel_kb,
    )
    slot.status_msg = status_msg

    # Preserve dots like `.1080p` in titles/slugs by giving them a synthetic
    # `.m3u8` extension before _mp4_name()/splitext runs.
    file_name = _choose_job_filename(url, page_url, title)
    mp4_name = f"{os.path.splitext(file_name)[0]}.mp4"
    req_headers, req_cookies = _merge_request_context(
        url, page_url, captured_headers, captured_cookies,
    )

    tracker: Optional[ProgressTracker] = None
    temp_mp4: Optional[str] = None
    work_dir: Optional[str] = None

    try:
        # ---- Custom thumbnail from the message that carried the link -------
        thumb_raw = await _load_custom_thumb(bot, message)

        # The progress bar can only be sized once the playlist has been parsed,
        # so the downloader tells us the estimate via on_meta and routes every
        # downloaded byte through on_chunk.
        holder: Dict[str, Optional[ProgressTracker]] = {"tracker": None}

        def _on_meta(estimated: int) -> None:
            if holder["tracker"] is not None:
                return
            est = max(0, int(estimated or 0))
            holder["tracker"] = ProgressTracker(
                status_msg, mp4_name, est,
                reply_markup=cancel_kb,
            )
            holder["tracker"].start()

        def _on_chunk(n: int) -> None:
            current = holder["tracker"]
            if current is not None:
                current.add_downloaded(n)

        async def _status_cb(text: str) -> None:
            # Don't clobber the active progress bar right after on_meta starts it.
            if holder["tracker"] is not None and text.startswith("⬇️ Memuat turun"):
                return
            await safe_edit(status_msg, text, reply_markup=cancel_kb)

        try:
            backup_peer = await _get_backup_group_peer()
        except Exception as e:
            await safe_edit(status_msg, f"❌ Gagal mendapat backup group: {_sanitize_error_text(e)}")
            return

        work_dir = tempfile.mkdtemp(prefix="hls_job_")

        async def _invoke_download() -> Any:
            call_kwargs: Dict[str, Any] = {
                "headers": req_headers,
                "cookies": req_cookies,
                "status_cb": _status_cb,
                "on_chunk": _on_chunk,
                "on_meta": _on_meta,
                "out_dir": work_dir,
                # A page resolved by a browser that had to go direct must download
                # directly too: its token is bound to that address.
                "proxy_url": None if force_direct else AUTO_PROXY,
            }
            if cookie_entries:
                call_kwargs["cookie_entries"] = cookie_entries
            if stream_type and stream_type != "hls":
                call_kwargs["stream_type"] = stream_type
            if audio_url:
                call_kwargs["audio_url"] = audio_url
            if audio_headers:
                call_kwargs["audio_headers"] = audio_headers
            if fallback_urls:
                call_kwargs["fallback_urls"] = fallback_urls
            if manifest_text:
                call_kwargs["initial_playlist_text"] = manifest_text
            if orig_page_url:
                call_kwargs["page_url"] = orig_page_url
            try:
                call_params = inspect.signature(_call_download_hls).parameters
                if not any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in call_params.values()
                ):
                    call_kwargs = {k: v for k, v in call_kwargs.items() if k in call_params}
            except (TypeError, ValueError):
                pass
            return await _call_download_hls(url, file_name, **call_kwargs)

        result = await _invoke_download()

        tracker = holder["tracker"]

        if is_cancelled(user_id):
            raise asyncio.CancelledError()

        if not result.converted and page_url and _should_reextract(result):
            if tracker is not None:
                await tracker.stop()
                tracker = None
                holder["tracker"] = None
            if work_dir and os.path.exists(work_dir):
                shutil.rmtree(work_dir, ignore_errors=True)
            work_dir = tempfile.mkdtemp(prefix="hls_job_")

            await safe_edit(
                status_msg,
                "🔄 Token strim tamat tempoh — mengambil semula pautan strim…",
                reply_markup=cancel_kb,
            )
            if is_cancelled(user_id):
                raise asyncio.CancelledError()

            reextract_target = orig_page_url or page_url
            resolved: Optional[_ResolvedPage] = None
            try:
                resolved = await _resolve_page_manifest(
                    message, reextract_target, notify_browser=False,
                )
            except (asyncio.CancelledError, WarpProxyError):
                raise
            except Exception as re_exc:
                print(
                    f"[HLS] Re-extraction failed for {reextract_target.split('?', 1)[0]}: "
                    f"{type(re_exc).__name__}: {re_exc}"
                )

            if is_cancelled(user_id):
                raise asyncio.CancelledError()

            if resolved is not None and not resolved.stage1_error and resolved.url:
                url = resolved.url
                page_url = resolved.page_url or page_url
                force_direct = resolved.force_direct
                stream_type = resolved.stream_type or "hls"
                title = resolved.title or title
                audio_url = resolved.audio_url
                audio_headers = resolved.audio_headers
                manifest_text = resolved.manifest_text
                cookie_entries = resolved.cookie_entries
                fallback_urls = resolved.fallback_urls
                file_name = _choose_job_filename(url, page_url, title)
                mp4_name = f"{os.path.splitext(file_name)[0]}.mp4"
                req_headers, req_cookies = _merge_request_context(
                    url, page_url, resolved.headers, resolved.cookies,
                )
                result = await _invoke_download()
                tracker = holder["tracker"]
                if is_cancelled(user_id):
                    raise asyncio.CancelledError()

        if not result.converted:
            err_detail = getattr(result, "detail", "") or ""
            print(f"[HLS] Download failed for user {user_id}: {result.reason} "
                  f"{_sanitize_error_text(err_detail)}".rstrip())
            warp_active = await _safe_is_warp_active()
            err_msg = _format_hls_error(
                result.reason,
                err_detail,
                from_page=bool(page_url),
                warp_active=warp_active,
            )
            if tracker is not None:
                await tracker.stop(err_msg)
            else:
                await safe_edit(status_msg, err_msg)
            return

        temp_mp4 = result.path
        file_name = result.name
        file_size = result.size

        video_meta = await _get_video_metadata(temp_mp4)
        if not video_meta.get("duration") and getattr(result, "duration", 0):
            video_meta["duration"] = int(result.duration)
        print(f"[HLS] Video metadata: {video_meta}")

        # ---- Oversized MP4: reuse the Direct multi-part splitting flow -----
        if file_size > size_limit:
            if tracker is not None:
                await tracker.stop()
                tracker = None

            import math
            target_part_size = int(size_limit * 0.95)
            num_parts = math.ceil(file_size / target_part_size)
            total_duration = video_meta.get("duration", 0)
            part_duration = total_duration / num_parts if total_duration > 0 else 3600

            await safe_edit(
                status_msg,
                f"✂️ Fail video melebihi had ({file_size / (1024 ** 3):.2f} GB > "
                f"{size_limit / (1024 ** 3):.1f} GB).\n"
                f"Memotong video kepada {num_parts} bahagian…",
                reply_markup=cancel_kb,
            )

            custom_thumb_raw = thumb_raw
            split_dir = tempfile.mkdtemp(prefix="hls_split_")
            base_name, ext_str = os.path.splitext(file_name)
            delivered_count = 0

            try:
                for i in range(num_parts):
                    if is_cancelled(user_id):
                        raise asyncio.CancelledError()

                    part_num = i + 1
                    part_filename = f"{base_name} (Part {part_num}){ext_str}"
                    part_path = os.path.join(split_dir, part_filename)

                    start_sec = i * part_duration
                    dur_sec = (
                        part_duration if part_num < num_parts
                        else (total_duration - start_sec)
                    )
                    if dur_sec <= 0:
                        dur_sec = part_duration

                    await safe_edit(
                        status_msg,
                        f"✂️ Memotong Bahagian {part_num}/{num_parts}: "
                        f"`{part_filename}`…",
                        reply_markup=cancel_kb,
                    )

                    ok = await _split_video_part(temp_mp4, start_sec, dur_sec, part_path)
                    if not ok:
                        raise Exception(f"Gagal memotong bahagian {part_num}")

                    part_size = os.path.getsize(part_path)

                    # A custom thumbnail is only used for Part 1; later parts get
                    # a snapshot from their own clip.
                    if part_num == 1 and custom_thumb_raw:
                        part_thumb = custom_thumb_raw
                    else:
                        part_thumb = await _generate_video_thumb(part_path, int(dur_sec))

                    part_meta = await _get_video_metadata(part_path)
                    part_tracker = ProgressTracker(
                        status_msg, part_filename, part_size,
                        reply_markup=cancel_kb,
                    )
                    part_tracker.start()

                    part_streamer = FileStreamer(part_path, part_filename)
                    part_caption = (
                        generate_video_caption(part_filename, exclude_words)
                        if enable_caption else None
                    )
                    msg_id = None
                    is_sent_to_bot = False
                    try:
                        msg_id, is_sent_to_bot = await _upload_file_to_backup(
                            bot, user_client, backup_peer, part_streamer,
                            part_filename, part_size,
                            tracker=part_tracker,
                            thumb_raw=part_thumb,
                            video_meta=part_meta,
                            caption=part_caption,
                        )
                    except BaseException:
                        await part_tracker.stop()
                        raise
                    finally:
                        if hasattr(part_streamer, "close"):
                            await part_streamer.close()

                    if not msg_id:
                        await part_tracker.stop(
                            f"❌ Gagal memuat naik bahagian {part_num}."
                        )
                        return

                    await part_tracker.stop(
                        f"⬆️ Menghantar bahagian {part_num}/{num_parts}…"
                    )
                    delivered = await _send_to_user(
                        bot, user_id, msg_id, is_sent_to_bot, caption=part_caption,
                    )

                    if delivered:
                        delivered_count += 1
                        try:
                            channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                            link = (
                                f"https://t.me/c/{channel_id_str}/{msg_id}"
                                if not is_sent_to_bot else None
                            )
                            await log_forward(
                                message.from_user.username or "Unknown",
                                msg_id,
                                part_size,
                                f"HLS/{part_filename}",
                                link,
                            )
                        except Exception as e:
                            print(f"[HLS] Part logging error: {e}")

                    if os.path.exists(part_path):
                        try:
                            os.remove(part_path)
                        except OSError:
                            pass

                if delivered_count == num_parts:
                    try:
                        await status_msg.delete()
                    except Exception:
                        pass
                else:
                    await safe_edit(
                        status_msg,
                        f"⚠️ Selesai! {delivered_count}/{num_parts} bahagian berjaya dihantar.",
                    )
                return
            finally:
                if os.path.exists(split_dir):
                    shutil.rmtree(split_dir, ignore_errors=True)

        # ---- Single-file flow: keep tracker running for live upload bar ----
        if tracker is None:
            tracker = ProgressTracker(
                status_msg, file_name, max(1, file_size),
                reply_markup=cancel_kb,
            )
            tracker.downloaded = file_size
            tracker.start()
        else:
            tracker.file_name = file_name
            tracker.file_size = max(1, file_size)
            tracker.downloaded = file_size

        # ---- Thumbnail (custom or generated) --------------------------------
        if not thumb_raw:
            thumb_raw = await _generate_video_thumb(
                temp_mp4, video_meta.get("duration", 0)
            )
            if thumb_raw:
                print(f"[HLS] Thumbnail generated: {len(thumb_raw)} bytes")
        else:
            print("[HLS] Using the user-supplied thumbnail")

        # The download is already fully counted, so FileStreamer gets no
        # on_download_chunk callback — only the upload side moves the bar.
        streamer = FileStreamer(temp_mp4, file_name)

        video_caption = (
            generate_video_caption(file_name, exclude_words) if enable_caption else None
        )

        try:
            msg_id, is_sent_to_bot = await _upload_file_to_backup(
                bot, user_client, backup_peer, streamer, file_name, file_size,
                tracker=tracker,
                thumb_raw=thumb_raw,
                video_meta=video_meta,
                caption=video_caption,
            )
        finally:
            if hasattr(streamer, "close"):
                await streamer.close()

        if not msg_id:
            await tracker.stop("❌ Gagal memuat naik ke grup sandaran.")
            return

        await tracker.stop("⬆️ Menghantar ke anda…")
        delivered = await _send_to_user(
            bot, user_id, msg_id, is_sent_to_bot, caption=video_caption,
        )

        if delivered:
            try:
                channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                link = (
                    f"https://t.me/c/{channel_id_str}/{msg_id}"
                    if not is_sent_to_bot else None
                )
                await log_forward(
                    message.from_user.username or "Unknown",
                    msg_id,
                    file_size,
                    f"HLS/{file_name}",
                    link,
                )
            except Exception as e:
                print(f"[HLS] Logging error: {e}")

            try:
                await status_msg.delete()
            except Exception:
                pass
        else:
            await safe_edit(
                status_msg,
                "⚠️ Selesai! Fail telah dimuat naik ke grup sandaran tetapi "
                "gagal dihantar.",
            )

    except asyncio.CancelledError:
        if tracker:
            await tracker.stop("❌ Dibatalkan oleh pengguna.")
        else:
            await safe_edit(status_msg, "❌ Dibatalkan oleh pengguna.")
    except SessionInvalidError as e:
        print(f"[HLS] Session invalid for user {user_id}: {e}")
        if tracker:
            await tracker.stop("❌ Sesi anda telah tamat. Sila /start dan login semula.")
        else:
            await safe_edit(status_msg, "❌ Sesi anda telah tamat. Sila /start dan login semula.")
    except WarpProxyError as e:
        print(f"[HLS] WARP proxy error for user {user_id}: {e}")
        if tracker:
            await tracker.stop(f"❌ {e}")
        else:
            try:
                await safe_edit(status_msg, f"❌ {e}")
            except Exception:
                pass
    except Exception as e:
        safe_err = _sanitize_error_text(e)
        print(f"[HLS] Handler error: {safe_err}")
        import traceback
        traceback.print_exc()
        if tracker:
            await tracker.stop(f"❌ Ralat: {safe_err}")
        else:
            try:
                await safe_edit(status_msg, f"❌ Ralat: {safe_err}")
            except Exception:
                pass
    finally:
        if tracker:
            await tracker.stop()
        # Release only this job's slot — a second job of the same user keeps its
        # own slot, cancellation state and HTTP session.
        release_process(slot)
        # 🧹 Always remove the downloaded MP4 and its workspace, on every exit
        # path (success, split return, cancellation, error).
        if temp_mp4 and os.path.exists(temp_mp4):
            try:
                os.remove(temp_mp4)
            except OSError as e:
                print(f"[HLS] Temp MP4 cleanup error: {e}")
        if work_dir and os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)
        await clear_reply_markup(status_msg)

