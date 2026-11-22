"""
hls/downloader.py — HLS download + MP4 remux orchestration.

Turns an m3u8/HLS URL into a local, Telegram-ready MP4 with the SAME method the
bot already uses for MKV -> MP4 (``app/utils/video_compressor.py``): ffmpeg
**stream copy** (``-c copy``) — no re-encode, no quality loss, ~0 CPU and I/O
bound wall time, plus a cheap audio-only AAC fallback for streams whose audio
codec MP4 cannot hold (AC-3/E-AC-3/FLAC are common on HLS).

Two engines, same output recipe
-------------------------------
* **Engine A (segments)** — fetch the playlists (preserving post-redirect base
  URLs and variant fallbacks), download keys, fMP4 init sections, video segments
  and separate audio segments concurrently with an IDM-style worker pool over
  the shared HTTP keep-alive/proxy pool, decrypt AES-128 (C-backed in Python
  when available, or via ffmpeg C crypto otherwise), strip disguised fake image
  headers, merge in-order via a memory-bounded sink into a contiguous stream
  file (or local playlist for discontinuities), and remux to faststart MP4.
* **Engine B (ffmpeg)** — hand the remote playlist straight to ffmpeg when
  Engine A cannot fetch the playlist/segments (respecting headers, cookies and
  HTTP proxies, or direct fallback when allowed).

Failure/cleanup policy (identical in spirit to ``video_compressor``)
-------------------------------------------------------------------
* ffmpeg always writes a private ``.hls_<rand>.mp4`` scratch file which is only
  published with ``os.replace`` after the output was verified, so a half-written
  MP4 can never be uploaded.
* Any failure, timeout or cancellation cancels worker tasks, kills subprocesses,
  closes file handles and removes the scratch file and per-job download dir.
* ``cleanup_orphaned_hls_dirs()`` (called once at startup) sweeps leftovers from
  a hard kill (SIGKILL / OOM).

Self-test (needs network + ffmpeg):

    python -m app.hls.downloader
"""
from __future__ import annotations

import asyncio
import glob
import inspect
import json
import os
import random
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse, urlsplit, urlunsplit

import app.config as _cfg
import app.hls.client as _client_mod
from app.config import (
    HLS_ENABLED,
    HLS_ENGINE,
    HLS_MAX_DURATION,
    HLS_MAX_HEIGHT,
    HLS_REENCODE_FALLBACK,
    HLS_SEGMENT_RETRIES,
    HLS_SEGMENT_WORKERS,
    HLS_TEMP_DIR,
    HLS_TIMEOUT,
)
from app.hls.client import (
    AUTO_PROXY,
    HlsClient,
    default_headers,
    ffmpeg_header_args,
    ffmpeg_proxy_args,
    is_socks_proxy,
)
from app.hls.playlist import (
    MediaPlaylist,
    MediaSegment,
    is_drm,
    is_hls_playlist,
    is_master_playlist,
    parse_master,
    parse_media,
    rank_variants,
    select_audio_track,
)
from app.utils.video_compressor import (
    MP4_SAFE_AUDIO,
    MP4_SAFE_VIDEO,
    convert_semaphore,
)
from app.warp import (
    WarpProxyError,
    check_proxy_reachable,
    ensure_proxy_ready,
    format_warp_proxy_error,
)

# ---------------------------------------------------------------------------
# Outcome reason codes
# ---------------------------------------------------------------------------

R_DISABLED = "disabled"
R_NO_FFMPEG = "no-ffmpeg"
R_NOT_HLS = "not-hls"
R_DRM = "drm"
R_NO_SEGMENTS = "no-segments"
R_LOW_DISK = "low-disk"
R_DOWNLOAD_FAILED = "download-failed"
R_REMUX_FAILED = "remux-failed"

# Memory caps for the ordered contiguous segment sink (VPS MemoryMax safety).
_MAX_REORDER_RAM_BYTES = 32 * 1024 * 1024
_MAX_SINGLE_RAM_BYTES = 12 * 1024 * 1024

_ISO_BMFF_BOXES = (
    b"ftyp", b"styp", b"moof", b"sidx", b"emsg", b"free", b"mdat", b"moov", b"prft",
)
_HTTP_STATUS_RE = re.compile(r"\bHTTP\s+(\d{3})\b", re.IGNORECASE)
_URL_QUERY_RE = re.compile(r"(https?://[^\s?#\"'<>]+)\?[^\s\"'<>]+")
_URL_CREDS_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^:@\s]+:[^@\s]+@")
_COOKIE_HDR_RE = re.compile(r"(Cookie|Authorization)\s*:\s*[^\r\n]+", re.IGNORECASE)


@dataclass
class HlsResult:
    """Outcome of an HLS download.

    When ``converted`` is False no usable MP4 exists and ``reason`` (plus
    optional sanitized ``detail``) explains why; handlers must report the error
    instead of uploading anything.
    """
    path: str = ""
    name: str = ""
    size: int = 0
    converted: bool = False
    reason: str = ""
    engine: str = ""
    duration: float = 0.0
    is_live: bool = False
    estimated_size: int = 0
    detail: str = ""


@dataclass
class _Prepared:
    """Everything resolved from the playlists before media bytes are fetched."""
    video_url: str = ""
    video_text: str = ""
    video_pl: Optional[MediaPlaylist] = None
    audio_url: str = ""
    audio_text: str = ""
    audio_pl: Optional[MediaPlaylist] = None
    duration: float = 0.0
    bandwidth: int = 0
    is_live: bool = True


# ---------------------------------------------------------------------------
# Config & client compatibility helpers
# ---------------------------------------------------------------------------

def _cfg_workers() -> int:
    val = getattr(_cfg, "HLS_SEGMENT_WORKERS", HLS_SEGMENT_WORKERS)
    try:
        return max(1, int(val))
    except (TypeError, ValueError):
        return 16


def _cfg_retries() -> int:
    val = getattr(_cfg, "HLS_SEGMENT_RETRIES", HLS_SEGMENT_RETRIES)
    try:
        return max(1, int(val))
    except (TypeError, ValueError):
        return 5


def _cfg_connect_timeout() -> Optional[float]:
    val = getattr(_cfg, "HLS_CONNECT_TIMEOUT", None)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _cfg_segment_timeout() -> Optional[float]:
    val = getattr(_cfg, "HLS_SEGMENT_TIMEOUT", None)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _cfg_proxy_fallback_direct() -> bool:
    val = getattr(_cfg, "HLS_PROXY_FALLBACK_DIRECT", True)
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return bool(val)


def _merge_cookie_header(headers: Dict[str, str], cookies: Dict[str, str]) -> None:
    """Merge *cookies* into a ``Cookie`` header without duplicates."""
    if not cookies:
        return
    existing_key = next((k for k in headers if k.lower() == "cookie"), "Cookie")
    existing = headers.get(existing_key, "").strip()
    pairs: Dict[str, str] = {}
    if existing:
        for item in existing.split(";"):
            if "=" in item:
                k, _, v = item.partition("=")
                pairs[k.strip()] = v.strip()
    for k, v in cookies.items():
        if k:
            pairs[str(k)] = str(v)
    if pairs:
        headers[existing_key] = "; ".join(f"{k}={v}" for k, v in pairs.items())


def _normalize_headers(url: str, headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Ensure baseline browser defaults exist without re-injecting omitted Referer/Origin."""
    defaults = default_headers(url)
    if not headers:
        return defaults
    merged = dict(headers)
    lower_keys = {k.lower() for k in merged}
    for key in ("User-Agent", "Accept", "Accept-Language"):
        if key.lower() not in lower_keys and key in defaults:
            merged[key] = defaults[key]
    return merged


def _supports_kwarg(fn: Any, kw: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD or param.name == kw:
            return True
    return False


def _header_value_ci(headers: Optional[Dict[str, str]], name: str) -> str:
    low = name.lower()
    for k, v in (headers or {}).items():
        if k and v is not None and k.lower() == low:
            return str(v).strip()
    return ""


def _origin_of_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    if parts.scheme and parts.netloc:
        return f"{parts.scheme}://{parts.netloc}"
    return ""


def _inherit_query(child_url: str, parent_url: str) -> Optional[str]:
    """Append *parent_url*'s query string to *child_url* if *child_url* has none."""
    if not child_url or not parent_url:
        return None
    try:
        parent_parts = urlsplit(parent_url)
        child_parts = urlsplit(child_url)
    except ValueError:
        return None
    if not parent_parts.query or child_parts.query:
        return None
    return urlunsplit((
        child_parts.scheme,
        child_parts.netloc,
        child_parts.path,
        parent_parts.query,
        child_parts.fragment,
    ))


def _header_fallback_profiles(
    current_headers: Dict[str, str],
    target_url: str,
) -> List[Dict[str, Optional[str]]]:
    """Ordered, deduplicated Referer/Origin overrides to probe only on HTTP 401/403."""
    cur_ref = _header_value_ci(current_headers, "Referer")
    cur_orig = _header_value_ci(current_headers, "Origin")
    ref_origin = _origin_of_url(cur_ref)
    target_origin = _origin_of_url(target_url)

    seen_pairs = {(cur_ref or None, cur_orig or None)}
    profiles: List[Dict[str, Optional[str]]] = []

    def _add(ref_val: Optional[str], orig_val: Optional[str], override: Dict[str, Optional[str]]) -> None:
        pair = (ref_val or None, orig_val or None)
        if pair in seen_pairs:
            return
        seen_pairs.add(pair)
        profiles.append(override)

    # 1) Origin-only Referer + Origin (Chrome strict-origin-when-cross-origin).
    if cur_ref and ref_origin:
        _add(f"{ref_origin}/", ref_origin, {"Referer": f"{ref_origin}/", "Origin": ref_origin})

    # 2) Current Referer without Origin.
    if cur_ref:
        _add(cur_ref, None, {"Origin": None})

    # 3) Origin-only Referer without Origin.
    if cur_ref and ref_origin:
        _add(f"{ref_origin}/", None, {"Referer": f"{ref_origin}/", "Origin": None})

    # 4) Target/stream origin as Referer without Origin, then with Origin.
    if target_origin:
        _add(f"{target_origin}/", None, {"Referer": f"{target_origin}/", "Origin": None})
        _add(f"{target_origin}/", target_origin, {"Referer": f"{target_origin}/", "Origin": target_origin})

    # 5) Clean no-Referer, no-Origin.
    _add(None, None, {"Referer": None, "Origin": None})
    return profiles


def _apply_header_dict_inplace(
    target: Dict[str, str], overrides: Dict[str, Optional[str]]
) -> None:
    key_map = {k.lower(): k for k in list(target.keys()) if k}
    for k, v in overrides.items():
        if not k:
            continue
        low = k.lower()
        if low in key_map:
            target.pop(key_map.pop(low), None)
        if v is not None:
            target[k] = v
            key_map[low] = k


def _current_headers(client: Any, base_headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    ch = getattr(client, "headers", None)
    if isinstance(ch, dict) and ch:
        return dict(ch)
    return dict(base_headers or {})


def _promote_header_overrides(
    client: Any,
    base_headers: Optional[Dict[str, str]],
    overrides: Optional[Dict[str, Optional[str]]],
) -> None:
    if not overrides:
        return
    apply_fn = getattr(client, "apply_header_overrides", None)
    if callable(apply_fn):
        apply_fn(overrides)
    else:
        hdrs = getattr(client, "headers", None)
        if isinstance(hdrs, dict):
            _apply_header_dict_inplace(hdrs, overrides)
    if isinstance(base_headers, dict):
        _apply_header_dict_inplace(base_headers, overrides)


def _make_client(
    headers: Dict[str, str],
    proxy_url: Any,
    cookies: Optional[Dict[str, str]],
    workers: int,
) -> HlsClient:
    """Instantiate HlsClient cleanly across pre/post-upgrade client.py."""
    params = inspect.signature(HlsClient.__init__).parameters
    kwargs: Dict[str, Any] = {"proxy_url": proxy_url}
    hdrs = dict(headers)
    if cookies:
        if "cookies" in params:
            kwargs["cookies"] = dict(cookies)
        else:
            _merge_cookie_header(hdrs, cookies)
    kwargs["headers"] = hdrs
    if "max_connections" in params:
        kwargs["max_connections"] = max(32, workers * 2)
    conn_t = _cfg_connect_timeout()
    if conn_t is not None and "connect_timeout" in params:
        kwargs["connect_timeout"] = conn_t
    seg_t = _cfg_segment_timeout()
    if seg_t is not None and "segment_timeout" in params:
        kwargs["segment_timeout"] = seg_t
    return HlsClient(**kwargs)


def _client_ffmpeg_headers(
    client: HlsClient,
    base_headers: Dict[str, str],
    cookies: Optional[Dict[str, str]],
    url: str,
) -> List[str]:
    """Build ffmpeg header args including session cookies when supported."""
    merged_cookies: Dict[str, str] = dict(cookies or {})
    export_fn = getattr(client, "export_cookies", None)
    if callable(export_fn):
        try:
            exported = export_fn(url)
            if isinstance(exported, dict):
                merged_cookies.update(exported)
        except Exception:
            pass

    params = inspect.signature(ffmpeg_header_args).parameters
    if "cookies" in params:
        return ffmpeg_header_args(base_headers, cookies=merged_cookies or None)
    hdrs = dict(base_headers)
    if merged_cookies:
        _merge_cookie_header(hdrs, merged_cookies)
    return ffmpeg_header_args(hdrs)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    """``print`` that can never raise (unsafe stdout encodings must not break us)."""
    try:
        print(msg)
    except Exception:
        pass


async def _notify(status_cb: Optional[Callable[[str], object]], text: str) -> None:
    """Best-effort status message; never lets a UI failure break the pipeline."""
    if status_cb is None:
        return
    try:
        res = status_cb(text)
        if inspect.isawaitable(res):
            await res
    except Exception as e:
        _log(f"[HLS] status_cb failed: {e}")


def _safe_size(path: str) -> int:
    """``os.path.getsize`` that returns 0 instead of raising."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _mp4_name(file_name: str) -> str:
    """Replace *file_name*'s extension with ``.mp4``."""
    base, _ = os.path.splitext(file_name or "")
    base = base or "stream"
    return f"{base}.mp4"


def _has_disk_space(directory: str, size: int) -> bool:
    """True when *directory* can hold roughly twice *size* bytes."""
    try:
        free = shutil.disk_usage(directory).free
    except OSError:
        return True
    return free >= int(size * 2.05) + 300 * 1024 * 1024


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _is_m3u8_path(src: str) -> bool:
    return (src or "").split("?", 1)[0].lower().endswith((".m3u8", ".m3u"))


def _extension_of(url: str, default: str = ".ts") -> str:
    """Return the (safely recognised) extension of *url*, or *default*."""
    ext = os.path.splitext((url or "").split("?", 1)[0])[1].lower()
    if 3 <= len(ext) <= 5 and ext[1:].isalnum():
        return ext
    return default


def _segment_filename(index: int, url: str) -> str:
    """Local file name for a segment, keeping known media extensions."""
    path = (url or "").split("?", 1)[0]
    ext = os.path.splitext(path)[1].lower()
    allowed = {".ts", ".m4s", ".mp4", ".aac", ".mp3", ".cmfv", ".cmfa", ".vtt", ".key"}
    if ext not in allowed:
        ext = ".ts"
    return f"seg_{index:05d}{ext}"


def _sanitize_detail(text: str, max_len: int = 200) -> str:
    """Strip query tokens, proxy creds and auth headers from error strings."""
    if not text:
        return ""
    cleaned = _URL_CREDS_RE.sub(r"\1***@", str(text))
    cleaned = _URL_QUERY_RE.sub(r"\1?...", cleaned)
    cleaned = _COOKIE_HDR_RE.sub(r"\1: ***", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > max_len:
        cleaned = cleaned[: max_len - 3].rstrip() + "..."
    return cleaned


# ---------------------------------------------------------------------------
# Error classification & resilient retries
# ---------------------------------------------------------------------------

def _extract_http_status(exc: BaseException) -> Optional[int]:
    status = getattr(exc, "status", None)
    if isinstance(status, int) and status > 0:
        return status
    m = _HTTP_STATUS_RE.search(str(exc))
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _is_proxy_error(exc: BaseException) -> bool:
    proxy_cls = getattr(_client_mod, "HlsProxyError", None)
    if proxy_cls is not None and isinstance(exc, proxy_cls):
        return True
    cur: Optional[BaseException] = exc
    seen = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        name = type(cur).__name__.lower()
        if "proxy" in name:
            return True
        cur = cur.__cause__ or cur.__context__
    msg = str(exc).lower()
    return "proxy" in msg and ("connect" in msg or "socks" in msg or "timed out" in msg or "refused" in msg)


def _looks_like_ffmpeg_warp_proxy_error(err_text: str, proxy_url: Optional[str]) -> bool:
    """Return True if ffmpeg stderr suggests the local WARP HTTP proxy failed."""
    msg = (err_text or "").lower()
    if not msg:
        return False
    if proxy_url:
        from urllib.parse import urlparse

        parsed = urlparse(proxy_url)
        if parsed.hostname and parsed.port:
            endpoint = f"{parsed.hostname.lower()}:{parsed.port}"
            if endpoint in msg:
                return True
    return "proxy" in msg and any(
        tok in msg
        for tok in (
            "refused",
            "failed",
            "error",
            "timed out",
            "unreachable",
            "cannot",
            "not on whitelist",
        )
    )


def _is_permanent_error(exc: BaseException) -> bool:
    if getattr(exc, "is_permanent", False):
        status = _extract_http_status(exc)
        if status not in (401, 403):
            return True
    status = _extract_http_status(exc)
    return status in (400, 404, 405, 410, 451)


def _max_attempts_for(exc: BaseException, configured_attempts: int, attempt: int) -> bool:
    """True when another retry should be attempted after *exc*."""
    if attempt >= configured_attempts:
        return False
    if _is_proxy_error(exc):
        return False
    if _is_permanent_error(exc):
        return False
    status = _extract_http_status(exc)
    # A 401/403 is usually permanent, but a single short retry catches CDN edge blips.
    if status in (401, 403) and attempt >= 2:
        return False
    return True


def _retry_delay(attempt: int, exc: BaseException) -> float:
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, (int, float)) and retry_after > 0:
        return min(float(retry_after), 15.0)
    status = _extract_http_status(exc)
    if status in (401, 403):
        return 0.35
    if status == 429:
        return min(1.5 * (2 ** (attempt - 1)), 12.0) + random.uniform(0.0, 0.4)
    return min(0.35 * (2 ** (attempt - 1)), 6.0) + random.uniform(0.0, 0.25)


async def _retry_call(
    op_name: str,
    fn: Callable[[], Any],
    *,
    max_attempts: Optional[int] = None,
) -> Any:
    """Run an async fetch operation with smart status-aware backoff."""
    attempts = max(1, int(max_attempts)) if max_attempts is not None else _cfg_retries()
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except (asyncio.CancelledError, WarpProxyError):
            raise
        except Exception as exc:
            last_exc = exc
            if not _max_attempts_for(exc, attempts, attempt):
                raise
            delay = _retry_delay(attempt, exc)
            _log(f"[HLS] {op_name} retry {attempt}/{attempts - 1} in {delay:.2f}s: {_sanitize_detail(str(exc))}")
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc


async def _fetch_text_resilient(
    client: HlsClient,
    url: str,
    *,
    headers: Optional[Dict[str, Optional[str]]] = None,
    max_attempts: Optional[int] = None,
) -> Tuple[str, str]:
    """Fetch playlist text + final post-redirect URL with retries."""
    fetch_with_url = getattr(client, "fetch_text_with_url", None)

    async def _do() -> Tuple[str, str]:
        if callable(fetch_with_url):
            if headers is not None and _supports_kwarg(fetch_with_url, "headers"):
                return await fetch_with_url(url, headers=headers)
            return await fetch_with_url(url)
        if headers is not None and _supports_kwarg(client.fetch_text, "headers"):
            text = await client.fetch_text(url, headers=headers)
        else:
            text = await client.fetch_text(url)
        return text, url

    return await _retry_call("playlist", _do, max_attempts=max_attempts)


async def _fetch_bytes_resilient(
    client: HlsClient,
    url: str,
    *,
    byte_range: Optional[Tuple[int, int]] = None,
    on_chunk: Optional[Callable[[int], None]] = None,
    label: str = "segment",
    headers: Optional[Dict[str, Optional[str]]] = None,
    max_attempts: Optional[int] = None,
) -> bytes:
    """Fetch binary bytes (segment, key, or init map) with retries & range guard."""
    download_bytes_fn = getattr(client, "download_bytes", None)

    async def _do() -> bytes:
        if callable(download_bytes_fn):
            if headers is not None and _supports_kwarg(download_bytes_fn, "headers"):
                data = await download_bytes_fn(
                    url, byte_range=byte_range, on_chunk=on_chunk, headers=headers,
                )
            else:
                data = await download_bytes_fn(
                    url, byte_range=byte_range, on_chunk=on_chunk,
                )
        else:
            if headers is not None and _supports_kwarg(client.fetch_bytes, "headers"):
                data = await client.fetch_bytes(url, byte_range=byte_range, headers=headers)
            else:
                data = await client.fetch_bytes(url, byte_range=byte_range)
            if byte_range is not None:
                offset, length = byte_range
                if len(data) > length:
                    data = data[offset:offset + length]
                if len(data) != length:
                    raise ValueError(
                        f"Byte-range segment truncated: got {len(data)} of {length} bytes for {url}"
                    )
            if not data:
                raise ValueError(f"Empty {label} response for {url}")
            if on_chunk is not None and data:
                try:
                    on_chunk(len(data))
                except Exception as e:
                    _log(f"[HLS] progress callback failed: {e}")
        if not data:
            raise ValueError(f"Empty {label} response for {url}")
        return data

    return await _retry_call(label, _do, max_attempts=max_attempts)


class _PlaylistAuthState:
    """Shared per-playlist 401/403 recovery state so workers don't stampede probes."""

    def __init__(
        self,
        client: Any,
        *,
        parent_url: str = "",
        base_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.client = client
        self.parent_url = parent_url
        self.base_headers = base_headers
        self.lock = asyncio.Lock()
        self.generation: int = 0
        self.inherit_query: bool = False
        self.winning_overrides: Optional[Dict[str, Optional[str]]] = None
        self.exhausted: bool = False


def _client_supports_binary_headers(client: Any) -> bool:
    download_bytes_fn = getattr(client, "download_bytes", None)
    if callable(download_bytes_fn):
        return _supports_kwarg(download_bytes_fn, "headers")
    fetch_bytes_fn = getattr(client, "fetch_bytes", None)
    if callable(fetch_bytes_fn):
        return _supports_kwarg(fetch_bytes_fn, "headers")
    return False


def _client_supports_text_headers(client: Any) -> bool:
    fetch_with_url = getattr(client, "fetch_text_with_url", None)
    if callable(fetch_with_url):
        return _supports_kwarg(fetch_with_url, "headers")
    fetch_text_fn = getattr(client, "fetch_text", None)
    if callable(fetch_text_fn):
        return _supports_kwarg(fetch_text_fn, "headers")
    return False


async def _fetch_media_bytes_resilient(
    client: HlsClient,
    url: str,
    *,
    byte_range: Optional[Tuple[int, int]] = None,
    on_chunk: Optional[Callable[[int], None]] = None,
    label: str = "segment",
    auth_state: Optional[_PlaylistAuthState] = None,
    validator: Optional[Callable[[bytes], bool]] = None,
) -> bytes:
    """Fetch key/init/segment bytes with shared 401/403 query & header fallback."""
    if auth_state is None:
        return await _fetch_bytes_resilient(
            client, url, byte_range=byte_range, on_chunk=on_chunk, label=label,
        )

    eff_url = (_inherit_query(url, auth_state.parent_url) or url) if auth_state.inherit_query else url
    eff_headers = auth_state.winning_overrides
    start_gen = auth_state.generation

    try:
        return await _fetch_bytes_resilient(
            client, eff_url, byte_range=byte_range, on_chunk=on_chunk, label=label,
            headers=eff_headers,
        )
    except (asyncio.CancelledError, WarpProxyError):
        raise
    except Exception as exc:
        if _is_proxy_error(exc) or _extract_http_status(exc) not in (401, 403):
            raise

        retry_after_unlock = False
        retry_url = url
        retry_headers: Optional[Dict[str, Optional[str]]] = None

        async with auth_state.lock:
            if auth_state.generation != start_gen:
                retry_after_unlock = True
                retry_url = (
                    (_inherit_query(url, auth_state.parent_url) or url)
                    if auth_state.inherit_query else url
                )
                retry_headers = (
                    dict(auth_state.winning_overrides)
                    if auth_state.winning_overrides is not None else None
                )
            elif auth_state.exhausted:
                raise
            else:
                inherited = (
                    _inherit_query(url, auth_state.parent_url)
                    if (auth_state.parent_url and not auth_state.inherit_query)
                    else None
                )
                inherited_alive = inherited is not None

                if inherited is not None:
                    try:
                        data = await _fetch_bytes_resilient(
                            client, inherited, byte_range=byte_range, on_chunk=on_chunk,
                            label=label, headers=auth_state.winning_overrides, max_attempts=1,
                        )
                        if validator is None or validator(data):
                            auth_state.inherit_query = True
                            auth_state.generation += 1
                            return data
                    except (asyncio.CancelledError, WarpProxyError):
                        raise
                    except Exception as inh_exc:
                        if _is_proxy_error(inh_exc):
                            raise
                        if _extract_http_status(inh_exc) not in (401, 403):
                            inherited_alive = False

                if _client_supports_binary_headers(client):
                    profiles = _header_fallback_profiles(
                        _current_headers(client, auth_state.base_headers), url,
                    )
                    for profile in profiles:
                        cand_urls = [url]
                        if inherited_alive and inherited is not None:
                            cand_urls.append(inherited)
                        for idx_c, cand_url in enumerate(cand_urls):
                            try:
                                data = await _fetch_bytes_resilient(
                                    client, cand_url, byte_range=byte_range, on_chunk=on_chunk,
                                    label=label, headers=profile, max_attempts=1,
                                )
                                if validator is not None and not validator(data):
                                    continue
                                _promote_header_overrides(client, auth_state.base_headers, profile)
                                auth_state.winning_overrides = dict(profile)
                                if idx_c == 1:
                                    auth_state.inherit_query = True
                                auth_state.generation += 1
                                return data
                            except (asyncio.CancelledError, WarpProxyError):
                                raise
                            except Exception as p_exc:
                                if _is_proxy_error(p_exc):
                                    raise
                                if idx_c == 1 and _extract_http_status(p_exc) not in (401, 403):
                                    inherited_alive = False
                                continue

                auth_state.exhausted = True
                raise

        if retry_after_unlock:
            return await _fetch_bytes_resilient(
                client, retry_url, byte_range=byte_range, on_chunk=on_chunk, label=label,
                headers=retry_headers,
            )


async def _fetch_playlist_with_fallback(
    client: HlsClient,
    url: str,
    *,
    parent_url: str = "",
    base_headers: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """Fetch a playlist, falling back on 401/403 to query inheritance and header profiles."""
    try:
        return await _fetch_text_resilient(client, url)
    except (asyncio.CancelledError, WarpProxyError):
        raise
    except Exception as exc:
        if _is_proxy_error(exc) or _extract_http_status(exc) not in (401, 403):
            raise

        inherited = _inherit_query(url, parent_url) if parent_url else None
        inherited_alive = inherited is not None

        if inherited is not None:
            try:
                text, final_url = await _fetch_text_resilient(client, inherited, max_attempts=1)
                if is_hls_playlist(text):
                    return text, final_url
            except (asyncio.CancelledError, WarpProxyError):
                raise
            except Exception as inh_exc:
                if _is_proxy_error(inh_exc):
                    raise
                if _extract_http_status(inh_exc) not in (401, 403):
                    inherited_alive = False

        if _client_supports_text_headers(client):
            profiles = _header_fallback_profiles(_current_headers(client, base_headers), url)
            for profile in profiles:
                cand_urls = [url]
                if inherited_alive and inherited is not None:
                    cand_urls.append(inherited)
                for idx_c, cand_url in enumerate(cand_urls):
                    try:
                        text, final_url = await _fetch_text_resilient(
                            client, cand_url, headers=profile, max_attempts=1,
                        )
                        if not is_hls_playlist(text):
                            continue
                        _promote_header_overrides(client, base_headers, profile)
                        return text, final_url
                    except (asyncio.CancelledError, WarpProxyError):
                        raise
                    except Exception as p_exc:
                        if _is_proxy_error(p_exc):
                            raise
                        if idx_c == 1 and _extract_http_status(p_exc) not in (401, 403):
                            inherited_alive = False
                        continue

        raise


# ---------------------------------------------------------------------------
# AES-128 decryption & disguised segment sanitization
# ---------------------------------------------------------------------------

def _detect_aes_backend() -> Optional[str]:
    """Return C-backed AES-128 library name, or None (never slow pure-Python pyaes)."""
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes  # noqa: F401
        return "cryptography"
    except Exception:
        pass
    try:
        from Crypto.Cipher import AES  # noqa: F401
        return "pycryptodome"
    except Exception:
        pass
    return None


def _compute_aes_iv(iv_attr: str, sequence: int) -> bytes:
    if iv_attr:
        cleaned = iv_attr.strip().lower()
        if cleaned.startswith("0x"):
            cleaned = cleaned[2:]
        try:
            raw = bytes.fromhex(cleaned.zfill(32))
            return raw[-16:]
        except ValueError:
            pass
    return int(max(0, sequence)).to_bytes(16, "big")


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if 1 <= pad <= 16 and len(data) >= pad and data[-pad:] == bytes([pad]) * pad:
        return data[:-pad]
    return data


def _decrypt_aes128_cbc(data: bytes, key: bytes, iv: bytes, backend: str) -> bytes:
    """Decrypt AES-128-CBC bytes using a C-accelerated backend."""
    if not data:
        return data
    if len(data) % 16 != 0:
        raise ValueError(f"Encrypted segment length {len(data)} is not a multiple of 16")
    if backend == "cryptography":
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
        out = decryptor.update(data) + decryptor.finalize()
        return _pkcs7_unpad(out)
    if backend == "pycryptodome":
        from Crypto.Cipher import AES
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return _pkcs7_unpad(cipher.decrypt(data))
    raise RuntimeError(f"Unsupported AES backend: {backend}")


def _find_ts_sync_offset(data: bytes, max_scan: int = 8192) -> Optional[int]:
    """Locate MPEG-TS 188-byte packet alignment (``0x47`` sync bytes)."""
    n = len(data)
    limit = min(n, max_scan)
    for pos in range(limit):
        if data[pos] != 0x47:
            continue
        if pos + 376 < n:
            if data[pos + 188] == 0x47 and data[pos + 376] == 0x47:
                return pos
        elif pos + 188 < n:
            if data[pos + 188] == 0x47:
                return pos
        elif n - pos == 188 and pos == 0:
            return 0
    return None


def _find_bmff_offset(data: bytes, max_scan: int = 1024) -> Optional[int]:
    """Locate ISO-BMFF box start (``ftyp``/``styp``/``moof``/``sidx``)."""
    n = len(data)
    if n < 8:
        return None
    if data[4:8] in _ISO_BMFF_BOXES:
        return 0
    head = data[:min(n, max_scan)]
    best: Optional[int] = None
    for tag in (b"ftyp", b"styp", b"moof", b"sidx"):
        idx = head.find(tag)
        if idx >= 4:
            box_start = idx - 4
            box_size = int.from_bytes(data[box_start:idx], "big")
            if 8 <= box_size <= n - box_start:
                if best is None or box_start < best:
                    best = box_start
    return best


def _is_probably_error_page(data: bytes) -> bool:
    """Detect HTTP 200 HTML/WAF/JSON error pages masquerading as media segments."""
    if not data:
        return True
    head = data[:512].lstrip().lower()
    if not head.startswith((b"<!doctype", b"<html", b"<head", b"<?xml", b"{")):
        return False
    # A disguised TS/fMP4 payload that happens to start with '<' would still
    # reveal TS packet syncs or BMFF boxes nearby.
    if _find_ts_sync_offset(data, max_scan=4096) is not None:
        return False
    if _find_bmff_offset(data, max_scan=1024) is not None:
        return False
    return True


def _sanitize_segment_bytes(
    data: bytes, *, is_fmp4: bool = False,
) -> Tuple[bytes, str]:
    """Strip fake image/junk prefixes from disguised segments and detect format.

    Many HLS CDNs disguise MPEG-TS or fMP4 segments as ``.png``/``.jpg``/``.gif``
    and prepend a dummy image header before the real media stream, which causes
    ffmpeg's demuxer to abort with ``detected format png_pipe``.
    """
    if not data:
        return data, ".m4s" if is_fmp4 else ".ts"

    # 1. Clean ISO-BMFF / fMP4 box at byte 0.
    if len(data) >= 8 and data[4:8] in _ISO_BMFF_BOXES:
        return data, ".m4s"

    # 2. Clean MPEG-TS at byte 0.
    if data[0] == 0x47 and (len(data) < 189 or data[188] == 0x47):
        return data, ".ts"

    # 3. Raw ADTS AAC / ID3 audio segment (used by audio-only HLS groups).
    if data[:3] == b"ID3":
        return data, ".aac"
    if len(data) >= 4 and data[0] == 0xFF and (data[1] & 0xF6) in (0xF0, 0xF2):
        # Verify it's not a JPEG (FF D8 FF) or accidental match before a TS header.
        ts_pos = _find_ts_sync_offset(data)
        if ts_pos is None:
            return data, ".aac"

    # 4. Strip disguised prefix before MPEG-TS sync alignment.
    ts_pos = _find_ts_sync_offset(data)
    if ts_pos is not None:
        return (data[ts_pos:] if ts_pos > 0 else data), ".ts"

    # 5. Strip disguised prefix before ISO-BMFF fMP4 boxes.
    bmff_pos = _find_bmff_offset(data)
    if bmff_pos is not None:
        return (data[bmff_pos:] if bmff_pos > 0 else data), ".m4s"

    return data, ".m4s" if is_fmp4 else ".ts"


def _validate_aes_key(data: bytes, url: str) -> bytes:
    """Ensure a fetched AES-128 key is 16 raw bytes, not an HTML error page."""
    if len(data) == 16:
        return data
    if _is_probably_error_page(data):
        raise ValueError(f"AES-128 key URL returned HTML/JSON error page for {url}")
    raise ValueError(f"Invalid AES-128 key length ({len(data)} bytes) for {url}")


# ---------------------------------------------------------------------------
# ffmpeg plumbing
# ---------------------------------------------------------------------------

def _build_remux_args(
    inputs: List[str],
    dst: str,
    *,
    local: bool,
    mode: str = "copy",
    extra_input_args: Optional[List[str]] = None,
    limit_seconds: int = 0,
    with_progress: bool = False,
) -> List[str]:
    """Build the ffmpeg argv for the HLS -> MP4 remux.

    Deliberately mirrors ``app/utils/video_compressor.py::_build_ffmpeg_args``:
    ``-c copy`` (never a video re-encode unless *mode* asks for it), the same
    timestamp normalisation and the same ``+faststart`` so Telegram can stream
    and seek immediately.

    ``mode`` is ``"copy"``, ``"copy-adtstoasc"``, ``"audio-aac"`` (video copied,
    audio re-encoded — the cheap rescue pass for AC-3/E-AC-3/FLAC audio) or
    ``"reencode"``.
    """
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
    if with_progress:
        args += ["-progress", "pipe:1", "-nostats"]

    for src in inputs:
        if local:
            # -allowed_extensions is private to the hls demuxer: passing it for
            # a merged .ts/.mp4 input fails with "Option allowed_extensions not found".
            if _is_m3u8_path(src):
                args += [
                    "-allowed_extensions", "ALL",
                    "-protocol_whitelist", "file,crypto,data",
                ]
        else:
            args += [
                "-allowed_extensions", "ALL",
                "-protocol_whitelist", "file,http,https,httpproxy,tcp,tls,crypto",
                "-reconnect", "1", "-reconnect_streamed", "1",
                "-reconnect_delay_max", "10",
            ]
            if extra_input_args:
                args += list(extra_input_args)
        args += ["-i", src]

    # Only the first video stream + audio stream: drops attachment/subtitle
    # streams the MP4 muxer could choke on. "?" tolerates audio-only or
    # video-only inputs safely.
    if len(inputs) > 1:
        args += ["-map", "0:v:0?", "-map", "1:a:0?"]
    else:
        args += ["-map", "0:v:0?", "-map", "0:a?"]
    args += ["-sn", "-map_chapters", "-1"]

    if mode == "audio-aac":
        args += ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]
    elif mode == "reencode":
        args += [
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
        ]
    elif mode == "copy-adtstoasc":
        args += ["-c", "copy", "-bsf:a", "aac_adtstoasc"]
    else:
        args += ["-c", "copy"]

    if limit_seconds and limit_seconds > 0:
        args += ["-t", str(int(limit_seconds))]

    args += [
        "-fflags", "+genpts", "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "1024",
        "-movflags", "+faststart",
        dst,
    ]
    return args


async def _read_progress(proc: asyncio.subprocess.Process, on_chunk) -> None:
    """Consume ffmpeg's ``-progress pipe:1`` output and report byte deltas."""
    if proc.stdout is None:
        return
    total = 0
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace").strip()
            if text.startswith("total_size="):
                try:
                    new_total = int(text.split("=", 1)[1])
                except (ValueError, IndexError):
                    continue
                delta = new_total - total
                if delta > 0:
                    total = new_total
                    try:
                        on_chunk(delta)
                    except Exception as e:
                        _log(f"[HLS] progress callback failed: {e}")
            elif text == "progress=end":
                break
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _log(f"[HLS] progress reader error: {e}")


async def _read_stderr_capped(
    proc: asyncio.subprocess.Process, limit: int = 8192,
) -> str:
    """Drain stderr without risking pipe buffer deadlock; keep last *limit* bytes."""
    if proc.stderr is None:
        return ""
    buf = bytearray()
    try:
        while True:
            chunk = await proc.stderr.read(4096)
            if not chunk:
                break
            buf.extend(chunk)
            if len(buf) > limit * 2:
                del buf[:-limit]
    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    return bytes(buf[-limit:]).decode("utf-8", "replace").strip()


async def _run_ffmpeg_detailed(
    args: List[str],
    dst: str,
    timeout: int,
    on_progress=None,
) -> Tuple[bool, str]:
    """Run ffmpeg, returning ``(ok, error_detail)``."""
    proc = None
    reader = None
    stderr_task = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=(
                asyncio.subprocess.PIPE if on_progress is not None
                else asyncio.subprocess.DEVNULL
            ),
            stderr=asyncio.subprocess.PIPE,
        )
        if on_progress is not None:
            reader = asyncio.create_task(_read_progress(proc, on_progress))
        stderr_task = asyncio.create_task(_read_stderr_capped(proc))

        await asyncio.wait_for(proc.wait(), timeout=max(60, timeout))

        if reader is not None:
            try:
                await asyncio.wait_for(reader, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                reader.cancel()

        stderr_text = ""
        if stderr_task is not None:
            try:
                stderr_text = await asyncio.wait_for(stderr_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                stderr_task.cancel()

        if proc.returncode != 0:
            err_line = stderr_text.splitlines()[-1] if stderr_text else f"exit code {proc.returncode}"
            _log(f"[HLS] ffmpeg exited with code {proc.returncode}: {err_line}")
            return False, err_line
        if not os.path.exists(dst) or os.path.getsize(dst) < 1024:
            _log("[HLS] ffmpeg produced no usable output")
            return False, "ffmpeg produced empty output"
        return True, ""
    except asyncio.TimeoutError:
        _log(f"[HLS] ffmpeg timed out after {timeout}s - killing process")
        return False, f"ffmpeg timed out after {timeout}s"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _log(f"[HLS] ffmpeg error: {e}")
        return False, str(e)
    finally:
        for task in (reader, stderr_task):
            if task is not None and not task.done():
                task.cancel()
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except BaseException:
                pass
        for task in (reader, stderr_task):
            if task is not None:
                try:
                    await task
                except BaseException:
                    pass


async def _run_ffmpeg(
    args: List[str],
    dst: str,
    timeout: int,
    on_progress=None,
) -> bool:
    """Run ffmpeg, returning True only when a usable output file was produced."""
    ok, _ = await _run_ffmpeg_detailed(args, dst, timeout, on_progress=on_progress)
    return ok


async def _remux_with_fallbacks(
    inputs: List[str],
    dst: str,
    *,
    local: bool,
    timeout: int,
    extra_input_args: Optional[List[str]] = None,
    limit_seconds: int = 0,
    on_progress=None,
    modes: Optional[List[str]] = None,
    error_out: Optional[List[str]] = None,
) -> bool:
    """Remux with ``-c copy``, then retry with progressively safer modes."""
    if not modes:
        attempts = ["copy", "audio-aac"]
    else:
        attempts = list(modes)
    if HLS_REENCODE_FALLBACK and "reencode" not in attempts:
        attempts.append("reencode")

    last_err = ""
    for mode in attempts:
        args = _build_remux_args(
            inputs, dst, local=local, mode=mode,
            extra_input_args=extra_input_args, limit_seconds=limit_seconds,
            with_progress=(on_progress is not None),
        )
        ok, err = await _run_ffmpeg_detailed(args, dst, timeout, on_progress=on_progress)
        if ok:
            if mode not in ("copy", "copy-adtstoasc"):
                _log(f"[HLS] Remux succeeded with mode '{mode}'")
            return True
        if err:
            last_err = err
        _log(f"[HLS] Remux mode '{mode}' failed")
        # Remove partial output from a failed attempt before retrying.
        if os.path.exists(dst):
            try:
                os.remove(dst)
            except OSError:
                pass
    if error_out is not None and last_err:
        error_out.append(last_err)
    return False


async def _probe_streams(inputs: List[str], local: bool) -> Dict[str, List[str]]:
    """Best-effort ffprobe of the remux inputs -> ``{"video": [...], "audio": [...]}``."""
    video: List[str] = []
    audio: List[str] = []
    for src in inputs:
        args = ["ffprobe", "-v", "error"]
        if local and _is_m3u8_path(src):
            args += [
                "-allowed_extensions", "ALL",
                "-protocol_whitelist", "file,crypto,data",
            ]
        args += [
            "-show_entries", "stream=codec_type,codec_name",
            "-of", "json", src,
        ]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
            if proc.returncode != 0 or not stdout:
                continue
            data = json.loads(stdout.decode("utf-8", "replace"))
            for stream in data.get("streams") or []:
                name = (stream.get("codec_name") or "").lower()
                kind = (stream.get("codec_type") or "").lower()
                if not name:
                    continue
                if kind == "video":
                    video.append(name)
                elif kind == "audio":
                    audio.append(name)
        except asyncio.CancelledError:
            if proc is not None and proc.returncode is None:
                proc.kill()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except BaseException:
                    pass
            raise
        except Exception as e:
            _log(f"[HLS] probe failed for {src}: {e}")
            if proc is not None and proc.returncode is None:
                proc.kill()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except BaseException:
                    pass
    return {"video": video, "audio": audio}


async def _choose_remux_modes(
    inputs: List[str], local: bool,
) -> Optional[List[str]]:
    """Order the remux modes so the first attempt matches the actual codecs."""
    probe = await _probe_streams(inputs, local)
    video, audio = probe["video"], probe["audio"]
    if not video and not audio:
        return None

    default = ["copy", "audio-aac"]

    if video and video[0] not in MP4_SAFE_VIDEO:
        if HLS_REENCODE_FALLBACK:
            return ["reencode"] + default
        _log(f"[HLS] Video codec {video[0]} may not be MP4-safe; keeping -c copy")

    if audio and any(codec not in MP4_SAFE_AUDIO for codec in audio):
        _log(f"[HLS] Audio codecs {audio} need an AAC transcode")
        return ["audio-aac", "copy"]

    # Apply aac_adtstoasc only when AAC audio is present (passing it on MP3 fails
    # ffmpeg), while keeping plain copy as immediate fallback.
    if audio and all(codec == "aac" for codec in audio):
        return ["copy-adtstoasc", "copy", "audio-aac"]

    return default


# ---------------------------------------------------------------------------
# Engine A — fast parallel segment downloader + contiguous merge / playlist
# ---------------------------------------------------------------------------

def _truncate(pl: MediaPlaylist, limit_seconds: int) -> MediaPlaylist:
    """Return *pl* limited to *limit_seconds* (a no-op when unset/already short).

    Segments are only ever dropped from the END, so the media sequence — and
    therefore implicit AES-128 IV derivation — stays valid.
    """
    if not limit_seconds or limit_seconds <= 0:
        return pl
    if pl.total_duration and pl.total_duration <= limit_seconds:
        return pl

    kept: List[MediaSegment] = []
    total = 0.0
    for seg in pl.segments:
        if kept and total >= limit_seconds:
            break
        kept.append(seg)
        total += seg.duration
    return MediaPlaylist(
        segments=kept,
        keys=pl.keys,
        init_url=pl.init_url,
        init_byte_range=pl.init_byte_range,
        media_sequence=pl.media_sequence,
        target_duration=pl.target_duration,
        version=pl.version,
        is_live=False,
        is_fmp4=pl.is_fmp4,
        total_duration=total,
    )


def _write_local_playlist(
    pl: MediaPlaylist,
    dst_path: str,
    filenames: List[str],
    key_files: Dict[str, str],
    init_name: Union[str, Dict[Tuple[str, Optional[Tuple[int, int]]], str]] = "",
    *,
    already_decrypted: bool = False,
) -> None:
    """Write a self-contained local playlist pointing at downloaded files."""
    init_map: Dict[Tuple[str, Optional[Tuple[int, int]]], str]
    default_init = ""
    if isinstance(init_name, dict):
        init_map = init_name
    else:
        init_map = {}
        default_init = init_name

    has_init = bool(default_init or init_map)
    version = max(pl.version or 3, 7 if (pl.is_fmp4 or has_init) else 3)
    lines = [
        "#EXTM3U",
        f"#EXT-X-VERSION:{version}",
        f"#EXT-X-TARGETDURATION:{pl.target_duration or 10}",
    ]
    if pl.media_sequence:
        lines.append(f"#EXT-X-MEDIA-SEQUENCE:{pl.media_sequence}")
    lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
    if default_init:
        lines.append(f'#EXT-X-MAP:URI="{default_init}"')

    current_key: Optional[int] = None
    current_init_name = default_init
    for seg, filename in zip(pl.segments, filenames):
        if seg.discontinuity:
            lines.append("#EXT-X-DISCONTINUITY")
            current_init_name = ""
        if init_map and seg.init_url:
            mapped = init_map.get((seg.init_url, seg.init_byte_range), "")
            if mapped and mapped != current_init_name:
                lines.append(f'#EXT-X-MAP:URI="{mapped}"')
                current_init_name = mapped
        if not already_decrypted and seg.key_index != current_key:
            key = pl.keys[seg.key_index] if seg.key_index < len(pl.keys) else None
            if key is not None and key.is_aes128:
                local_key = key_files.get(key.url)
                if not local_key:
                    raise ValueError(f"No local key for {key.url}")
                iv = f",IV={key.iv}" if key.iv else ""
                lines.append(f'#EXT-X-KEY:METHOD=AES-128,URI="{local_key}"{iv}')
            else:
                lines.append("#EXT-X-KEY:METHOD=NONE")
            current_key = seg.key_index
        lines.append(f"#EXTINF:{seg.duration:.3f},")
        lines.append(filename)

    lines.append("#EXT-X-ENDLIST")
    with open(dst_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _write_bytes_sync(path: str, data: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(data)


def _read_head_sync(path: str, n: int = 8192) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(n)


def _copy_and_unlink_sync(src_path: str, dst_fh) -> None:
    try:
        with open(src_path, "rb") as src_fh:
            shutil.copyfileobj(src_fh, dst_fh, 1024 * 1024)
    finally:
        try:
            os.remove(src_path)
        except OSError:
            pass


def _infer_merged_ext(
    first_chunk: bytes, init_bytes: Optional[bytes], is_fmp4: bool,
) -> str:
    if init_bytes and _find_bmff_offset(init_bytes) is not None:
        return ".mp4"
    _, ext = _sanitize_segment_bytes(first_chunk, is_fmp4=is_fmp4)
    if ext == ".m4s":
        return ".mp4"
    return ext


async def _fetch_keys_and_inits(
    client: HlsClient,
    pl: MediaPlaylist,
    *,
    sem: asyncio.Semaphore,
    on_chunk=None,
    parent_url: str = "",
    base_headers: Optional[Dict[str, str]] = None,
    auth_state: Optional[_PlaylistAuthState] = None,
) -> Tuple[Dict[str, bytes], Dict[Tuple[str, Optional[Tuple[int, int]]], bytes]]:
    """Fetch all distinct AES-128 keys and #EXT-X-MAP init sections concurrently."""
    for key in pl.keys:
        if key.method and key.method not in ("NONE", "AES-128"):
            raise _HlsError(R_DRM, f"Unsupported encryption method: {key.method}")
        if key.is_aes128 and not key.url:
            raise ValueError("AES-128 key missing URI")

    if auth_state is None:
        auth_state = _PlaylistAuthState(
            client, parent_url=parent_url, base_headers=base_headers,
        )

    unique_key_urls: List[str] = []
    for key in pl.keys:
        if key.is_aes128 and key.url and key.url not in unique_key_urls:
            unique_key_urls.append(key.url)

    unique_inits: List[Tuple[str, Optional[Tuple[int, int]]]] = []
    for spec in pl.init_sections:
        if spec not in unique_inits:
            unique_inits.append(spec)
    if pl.init_url and (pl.init_url, pl.init_byte_range) not in unique_inits:
        unique_inits.append((pl.init_url, pl.init_byte_range))

    raw_keys: Dict[str, bytes] = {}
    raw_inits: Dict[Tuple[str, Optional[Tuple[int, int]]], bytes] = {}

    async def _get_key(url: str) -> None:
        async with sem:
            data = await _fetch_media_bytes_resilient(
                client, url, label="AES-128 key",
                auth_state=auth_state, validator=lambda d: len(d) == 16,
            )
            raw_keys[url] = _validate_aes_key(data, url)

    async def _get_init(spec: Tuple[str, Optional[Tuple[int, int]]]) -> None:
        init_url, init_range = spec
        async with sem:
            data = await _fetch_media_bytes_resilient(
                client, init_url, byte_range=init_range,
                on_chunk=on_chunk, label="init segment",
                auth_state=auth_state, validator=lambda d: not _is_probably_error_page(d),
            )
            if _is_probably_error_page(data):
                raise ValueError(f"Init segment returned HTML/error page for {init_url}")
            bmff_pos = _find_bmff_offset(data)
            if bmff_pos is not None and bmff_pos > 0:
                data = data[bmff_pos:]
            raw_inits[spec] = data

    tasks = [asyncio.create_task(_get_key(u)) for u in unique_key_urls] + [
        asyncio.create_task(_get_init(s)) for s in unique_inits
    ]
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    return raw_keys, raw_inits


async def _localize_playlist(
    client: HlsClient,
    pl: MediaPlaylist,
    work_dir: str,
    on_chunk=None,
    *,
    sem: Optional[asyncio.Semaphore] = None,
    parent_url: str = "",
    base_headers: Optional[Dict[str, str]] = None,
) -> str:
    """Download every segment/key/init of *pl* into *work_dir*.

    When possible (unencrypted or C-backed Python AES-128 decryption, no
    discontinuities, <=1 init map), segments are sanitized and merged in exact
    index order into a single contiguous file (``stream.ts`` / ``stream.mp4`` /
    ``stream.aac``) via a memory-bounded sink — eliminating thousands of
    per-segment open/write/close cycles and speeding up ffmpeg remuxing.

    Otherwise, writes clean local segment files + ``index.m3u8`` (with C-speed
    ffmpeg decryption if no Python C-AES backend is installed).
    """
    os.makedirs(work_dir, exist_ok=True)
    if not pl.segments:
        raise ValueError("playlist has no segments")

    worker_count = _cfg_workers()
    if sem is None:
        sem = asyncio.Semaphore(worker_count)

    auth_state = _PlaylistAuthState(
        client, parent_url=parent_url, base_headers=base_headers,
    )

    raw_keys, raw_inits = await _fetch_keys_and_inits(
        client, pl, sem=sem, on_chunk=on_chunk,
        parent_url=parent_url, base_headers=base_headers, auth_state=auth_state,
    )

    aes_backend = _detect_aes_backend() if pl.is_encrypted else None
    can_python_decrypt = (not pl.is_encrypted) or (aes_backend is not None)
    has_discontinuity = any(s.discontinuity for s in pl.segments)
    use_contiguous = (
        can_python_decrypt and not has_discontinuity and len(raw_inits) <= 1
    )

    loop = asyncio.get_running_loop()
    total = len(pl.segments)
    cursor = 0
    first_error: Optional[BaseException] = None

    def _decrypt_and_clean(idx: int, seg: MediaSegment, data: bytes) -> Tuple[bytes, str]:
        key = pl.keys[seg.key_index] if seg.key_index < len(pl.keys) else None
        is_seg_enc = key is not None and key.is_aes128
        if is_seg_enc:
            if not can_python_decrypt:
                return data, (".m4s" if (pl.is_fmp4 or seg.init_url) else ".ts")
            iv = _compute_aes_iv(key.iv, seg.sequence)
            data = _decrypt_aes128_cbc(data, raw_keys[key.url], iv, aes_backend)
        if _is_probably_error_page(data):
            raise ValueError(f"Segment {idx} returned HTML/JSON error page for {seg.url}")
        return _sanitize_segment_bytes(data, is_fmp4=(pl.is_fmp4 or bool(seg.init_url)))

    if use_contiguous:
        init_bytes = next(iter(raw_inits.values()), b"")
        pending: Dict[int, Union[bytes, str]] = {}
        pending_ram = 0
        next_write = 0
        drain_lock = asyncio.Lock()
        out_path: Optional[str] = None
        out_fh = None
        active_io: Optional[asyncio.Future] = None

        async def _run_io(fn, *args) -> Any:
            nonlocal active_io
            fut = loop.run_in_executor(None, fn, *args)
            active_io = fut
            try:
                return await fut
            finally:
                if active_io is fut:
                    active_io = None

        async def _worker_contiguous() -> None:
            nonlocal cursor, first_error, pending_ram, next_write, out_path, out_fh
            while first_error is None:
                if cursor >= total:
                    return
                idx = cursor
                cursor += 1
                seg = pl.segments[idx]
                try:
                    async with sem:
                        if first_error is not None:
                            return
                        raw = await _fetch_media_bytes_resilient(
                            client, seg.url, byte_range=seg.byte_range,
                            on_chunk=on_chunk, label=f"segment {idx}",
                            auth_state=auth_state,
                            validator=(
                                (lambda d: not _is_probably_error_page(d))
                                if not pl.is_encrypted else None
                            ),
                        )
                    if aes_backend is not None and pl.keys[seg.key_index].is_aes128:
                        data, _ = await loop.run_in_executor(
                            None, _decrypt_and_clean, idx, seg, raw,
                        )
                    else:
                        data, _ = _decrypt_and_clean(idx, seg, raw)

                    if idx != next_write and (
                        pending_ram + len(data) > _MAX_REORDER_RAM_BYTES
                        or len(data) > _MAX_SINGLE_RAM_BYTES
                    ):
                        spill_path = os.path.join(work_dir, f"spill_{idx:05d}.part")
                        await loop.run_in_executor(None, _write_bytes_sync, spill_path, data)
                        pending[idx] = spill_path
                    else:
                        pending[idx] = data
                        pending_ram += len(data)

                    async with drain_lock:
                        while next_write in pending:
                            item = pending.pop(next_write)
                            if isinstance(item, bytes):
                                pending_ram -= len(item)
                            if out_fh is None:
                                sample = (
                                    item if isinstance(item, bytes)
                                    else await _run_io(_read_head_sync, item, 8192)
                                )
                                ext = _infer_merged_ext(sample, init_bytes, pl.is_fmp4)
                                out_path = os.path.join(work_dir, f"stream{ext}")
                                out_fh = open(out_path, "wb", buffering=1024 * 1024)
                                if init_bytes:
                                    await _run_io(out_fh.write, init_bytes)
                            if isinstance(item, bytes):
                                await _run_io(out_fh.write, item)
                            else:
                                await _run_io(_copy_and_unlink_sync, item, out_fh)
                            next_write += 1
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                    return

        num_workers = min(worker_count, total)
        tasks = [asyncio.create_task(_worker_contiguous()) for _ in range(num_workers)]
        try:
            await asyncio.gather(*tasks)
            if first_error is not None:
                raise first_error
        except BaseException:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            if active_io is not None and not active_io.done():
                try:
                    await asyncio.wait_for(asyncio.shield(active_io), timeout=5)
                except BaseException:
                    pass
            if out_fh is not None:
                try:
                    out_fh.flush()
                except Exception:
                    pass
                try:
                    out_fh.close()
                except Exception:
                    pass
            pending.clear()

        if not out_path or _safe_size(out_path) == 0:
            raise ValueError("Downloaded stream is empty")
        return out_path

    # ---- Fallback path: local segment files + index.m3u8 -----------------
    key_files: Dict[str, str] = {}
    for idx_k, (key_url, key_bytes) in enumerate(raw_keys.items()):
        name = f"key_{idx_k}.bin"
        _write_bytes_sync(os.path.join(work_dir, name), key_bytes)
        key_files[key_url] = name

    init_files: Dict[Tuple[str, Optional[Tuple[int, int]]], str] = {}
    for idx_m, (spec, init_bytes) in enumerate(raw_inits.items()):
        name = f"init_{idx_m}" + _extension_of(spec[0], default=".mp4")
        _write_bytes_sync(os.path.join(work_dir, name), init_bytes)
        init_files[spec] = name

    filenames: List[str] = [""] * total

    async def _worker_files() -> None:
        nonlocal cursor, first_error
        while first_error is None:
            if cursor >= total:
                return
            idx = cursor
            cursor += 1
            seg = pl.segments[idx]
            try:
                async with sem:
                    if first_error is not None:
                        return
                    raw = await _fetch_media_bytes_resilient(
                        client, seg.url, byte_range=seg.byte_range,
                        on_chunk=on_chunk, label=f"segment {idx}",
                        auth_state=auth_state,
                        validator=(
                            (lambda d: not _is_probably_error_page(d))
                            if not pl.is_encrypted else None
                        ),
                    )
                if can_python_decrypt:
                    if aes_backend is not None and pl.keys[seg.key_index].is_aes128:
                        data, ext = await loop.run_in_executor(
                            None, _decrypt_and_clean, idx, seg, raw,
                        )
                    else:
                        data, ext = _decrypt_and_clean(idx, seg, raw)
                    name = f"seg_{idx:05d}{ext}"
                else:
                    # Encrypted without Python C-AES: keep raw ciphertext for ffmpeg.
                    data = raw
                    name = f"seg_{idx:05d}" + (".m4s" if (pl.is_fmp4 or seg.init_url) else ".ts")
                await loop.run_in_executor(
                    None, _write_bytes_sync, os.path.join(work_dir, name), data,
                )
                filenames[idx] = name
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                return

    num_workers = min(worker_count, total)
    tasks = [asyncio.create_task(_worker_files()) for _ in range(num_workers)]
    try:
        await asyncio.gather(*tasks)
        if first_error is not None:
            raise first_error
    except BaseException:
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    playlist_path = os.path.join(work_dir, "index.m3u8")
    _write_local_playlist(
        pl, playlist_path, filenames, key_files, init_files,
        already_decrypted=can_python_decrypt,
    )
    return playlist_path


async def _engine_segments(
    client: HlsClient,
    prep: _Prepared,
    temp_dir: str,
    limit_seconds: int,
    on_chunk=None,
    *,
    base_headers: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Engine A: download video (and audio concurrently) and return ffmpeg inputs."""
    video_pl = _truncate(prep.video_pl, limit_seconds)
    if not video_pl.segments:
        raise _HlsError(R_NO_SEGMENTS, "video playlist has no segments")

    shared_sem = asyncio.Semaphore(_cfg_workers())
    video_dir = os.path.join(temp_dir, "video")

    audio_pl: Optional[MediaPlaylist] = None
    if prep.audio_pl and prep.audio_pl.segments:
        candidate_audio = _truncate(prep.audio_pl, limit_seconds)
        if candidate_audio.segments:
            audio_pl = candidate_audio

    if audio_pl is None:
        video_input = await _localize_playlist(
            client, video_pl, video_dir, on_chunk=on_chunk, sem=shared_sem,
            parent_url=prep.video_url, base_headers=base_headers,
        )
        return [video_input]

    audio_dir = os.path.join(temp_dir, "audio")

    async def _download_audio_safe() -> Optional[str]:
        try:
            return await _localize_playlist(
                client, audio_pl, audio_dir, on_chunk=on_chunk, sem=shared_sem,
                parent_url=prep.audio_url, base_headers=base_headers,
            )
        except (asyncio.CancelledError, WarpProxyError):
            raise
        except Exception as exc:
            _log(f"[HLS] Audio track download failed (continuing video-only): {_sanitize_detail(str(exc))}")
            return None

    video_task = asyncio.create_task(
        _localize_playlist(
            client, video_pl, video_dir, on_chunk=on_chunk, sem=shared_sem,
            parent_url=prep.video_url, base_headers=base_headers,
        )
    )
    audio_task = asyncio.create_task(_download_audio_safe())

    try:
        video_input = await video_task
    except BaseException:
        if not audio_task.done():
            audio_task.cancel()
        await asyncio.gather(audio_task, return_exceptions=True)
        raise

    audio_input = await audio_task
    return [video_input, audio_input] if audio_input else [video_input]


# ---------------------------------------------------------------------------
# Playlist preparation (parsing only — no media downloads yet)
# ---------------------------------------------------------------------------

class _HlsError(Exception):
    """Internal error carrying a public reason code."""

    def __init__(self, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


_REASON_MESSAGES = {
    R_DISABLED: "❌ Ciri muat turun HLS dimatikan pada pelayan ini.",
    R_NO_FFMPEG: "❌ ffmpeg tidak dipasang pada pelayan ini.",
    R_NOT_HLS: "❌ URL ini bukan playlist HLS (.m3u8) yang sah.",
    R_DRM: "❌ Strim ini dilindungi DRM (SAMPLE-AES/Widevine) dan tidak boleh dimuat turun.",
    R_NO_SEGMENTS: "❌ Playlist HLS tidak mengandungi segmen video.",
    R_LOW_DISK: "❌ Ruang cakera tidak mencukupi untuk memuat turun strim ini.",
    R_DOWNLOAD_FAILED: "❌ Gagal memuat turun segmen HLS. Sila cuba lagi.",
    R_REMUX_FAILED: "❌ Gagal menukar strim HLS ke MP4.",
}


def reason_message(reason: str, detail: str = "") -> str:
    """User-facing (Malay) explanation for a failed HLS download."""
    base = _REASON_MESSAGES.get(reason, f"❌ Ralat HLS ({reason}).")
    clean = _sanitize_detail(detail)
    if clean:
        return f"{base}\nℹ️ `{clean}`"
    return base


async def _prepare(
    client: HlsClient,
    url: str,
    max_height: int,
    *,
    base_headers: Optional[Dict[str, str]] = None,
) -> _Prepared:
    """Resolve a master/media playlist (with variant fallback & redirect base URL)."""
    prep = _Prepared(video_url=url)

    text, current_url = await _fetch_playlist_with_fallback(
        client, url, base_headers=base_headers,
    )
    prep.video_url = current_url

    if not is_hls_playlist(text):
        raise _HlsError(R_NOT_HLS, "response does not start with #EXTM3U")
    if is_drm(text):
        raise _HlsError(R_DRM, "playlist uses unsupported DRM key method")

    # Resolve master playlist (up to 2 hops for rare nested masters), trying
    # lower-ranked variants if the top edge returns 403/404.
    media_text = text
    media_url = current_url
    audios = []
    audios_base_url = current_url
    chosen_audio_group = ""

    for _hop in range(2):
        if not is_master_playlist(media_text):
            break
        variants, hop_audios = parse_master(media_text, media_url)
        if hop_audios:
            audios = hop_audios
            audios_base_url = media_url
        ordered = rank_variants(variants, max_height)
        if not ordered:
            break

        last_exc: Optional[Exception] = None
        picked = False
        for variant in ordered:
            try:
                v_text, v_final_url = await _fetch_playlist_with_fallback(
                    client, variant.url, parent_url=media_url, base_headers=base_headers,
                )
            except (asyncio.CancelledError, WarpProxyError):
                raise
            except Exception as exc:
                if _is_proxy_error(exc):
                    raise
                last_exc = exc
                _log(f"[HLS] Variant failed ({_sanitize_detail(variant.url)}): {_sanitize_detail(str(exc))}")
                continue
            if not is_hls_playlist(v_text):
                last_exc = ValueError("variant is not an m3u8 playlist")
                continue
            if is_drm(v_text):
                raise _HlsError(R_DRM, "variant playlist uses DRM")
            media_text = v_text
            media_url = v_final_url
            prep.video_url = v_final_url
            prep.bandwidth = variant.rank_bandwidth
            chosen_audio_group = variant.audio_group
            picked = True
            break

        if not picked:
            if last_exc is not None:
                raise last_exc
            break

    if chosen_audio_group and audios:
        track = select_audio_track(audios, chosen_audio_group)
        if track is not None and track.url:
            prep.audio_url = track.url

    if is_drm(media_text):
        raise _HlsError(R_DRM, "media playlist uses DRM")

    prep.video_text = media_text
    if is_master_playlist(media_text):
        return prep

    prep.video_pl = parse_media(media_text, prep.video_url)
    prep.duration = prep.video_pl.total_duration
    prep.is_live = prep.video_pl.is_live

    if not prep.audio_url:
        return prep

    try:
        audio_text, audio_final_url = await _fetch_playlist_with_fallback(
            client, prep.audio_url, parent_url=audios_base_url, base_headers=base_headers,
        )
        prep.audio_url = audio_final_url
        if is_drm(audio_text):
            raise _HlsError(R_DRM, "audio playlist uses DRM")
        prep.audio_text = audio_text
        if not is_master_playlist(audio_text):
            prep.audio_pl = parse_media(audio_text, audio_final_url)
    except (_HlsError, WarpProxyError):
        raise
    except Exception as e:
        if _is_proxy_error(e):
            raise
        _log(f"[HLS] Could not read audio playlist {_sanitize_detail(prep.audio_url)}: {_sanitize_detail(str(e))}")
        prep.audio_url = ""

    return prep


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def download_hls_to_mp4(
    url: str,
    file_name: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    status_cb: Optional[Callable[[str], object]] = None,
    on_chunk: Optional[Callable[[int], None]] = None,
    on_meta: Optional[Callable[[int], None]] = None,
    out_dir: Optional[str] = None,
    max_height: Optional[int] = None,
    max_duration: Optional[int] = None,
    proxy_url: Any = AUTO_PROXY,
) -> HlsResult:
    """Download an HLS stream and remux it into a local MP4."""
    result = HlsResult(name=_mp4_name(file_name))

    if not HLS_ENABLED:
        result.reason = R_DISABLED
        return result
    if not _ffmpeg_available():
        result.reason = R_NO_FFMPEG
        return result

    workers = _cfg_workers()
    base_headers = _normalize_headers(url, headers)
    client = _make_client(base_headers, proxy_url, cookies, workers)

    height_cap = HLS_MAX_HEIGHT if max_height is None else max_height
    duration_cap = HLS_MAX_DURATION if max_duration is None else max_duration
    engine = HLS_ENGINE if HLS_ENGINE in ("auto", "segments", "ffmpeg") else "auto"

    temp_dir: Optional[str] = None
    scratch: Optional[str] = None
    published = False

    async def _switch_client_direct() -> None:
        nonlocal client
        switcher = getattr(client, "switch_to_direct", None)
        if callable(switcher):
            await switcher()
        else:
            await client.close()
            client = _make_client(base_headers, None, cookies, workers)

    try:
        # ---- 1. Parse the playlists --------------------------------------
        prep: Optional[_Prepared] = None
        prep_error: str = ""
        prep_detail: str = ""
        if engine in ("auto", "segments"):
            await _notify(status_cb, "🔍 Membaca playlist HLS…")
            for proxy_try in (0, 1):
                try:
                    prep = await _prepare(client, url, height_cap, base_headers=base_headers)
                    prep_error = ""
                    prep_detail = ""
                    break
                except _HlsError as e:
                    if e.reason in (R_DRM, R_NOT_HLS):
                        result.reason = e.reason
                        result.detail = _sanitize_detail(str(e))
                        return result
                    prep_error = e.reason or R_DOWNLOAD_FAILED
                    raw_msg = str(e)
                    if not raw_msg.lower().startswith("playlist:"):
                        raw_msg = f"playlist: {raw_msg}"
                    prep_detail = _sanitize_detail(raw_msg)
                    _log(f"[HLS] Prepare failed: {prep_detail}")
                    break
                except (asyncio.CancelledError, WarpProxyError):
                    raise
                except Exception as e:
                    if (
                        proxy_try == 0
                        and not getattr(client, "using_warp", False)
                        and _is_proxy_error(e)
                        and client.proxy_url is not None
                        and _cfg_proxy_fallback_direct()
                    ):
                        _log(
                            f"[HLS] Proxy unreachable ({_sanitize_detail(str(e))}) "
                            "— switching to direct connection"
                        )
                        await _switch_client_direct()
                        continue
                    prep_error = R_DOWNLOAD_FAILED
                    raw_msg = str(e)
                    if not raw_msg.lower().startswith("playlist:"):
                        raw_msg = f"playlist: {raw_msg}"
                    prep_detail = _sanitize_detail(raw_msg)
                    _log(f"[HLS] Prepare failed: {prep_detail}")
                    break

        if prep is not None:
            result.duration = prep.duration
            result.is_live = prep.is_live
            if prep.bandwidth and prep.duration:
                result.estimated_size = int(prep.bandwidth * prep.duration / 8)
            if prep.video_pl is not None and not prep.video_pl.segments:
                result.reason = R_NO_SEGMENTS
                result.detail = "playlist has 0 segments"
                return result

        # ---- 2. Workspace + guards ---------------------------------------
        dest_dir = out_dir or HLS_TEMP_DIR or tempfile.gettempdir()
        try:
            os.makedirs(dest_dir, exist_ok=True)
        except OSError as e:
            _log(f"[HLS] Cannot create output dir {dest_dir}: {e}")
            result.reason = R_REMUX_FAILED
            result.detail = _sanitize_detail(str(e))
            return result

        temp_dir = tempfile.mkdtemp(prefix="hls_", dir=dest_dir)
        if result.estimated_size and not _has_disk_space(
            dest_dir, result.estimated_size
        ):
            _log(f"[HLS] Not enough free disk for ~{result.estimated_size} bytes")
            result.reason = R_LOW_DISK
            return result

        final_path = os.path.join(dest_dir, result.name)
        scratch = os.path.join(
            dest_dir, f".hls_{random.randint(10 ** 9, 10 ** 10)}.mp4"
        )

        limit_seconds = 0
        if duration_cap and duration_cap > 0:
            if prep is None or prep.is_live or (
                prep.duration and prep.duration > duration_cap
            ):
                limit_seconds = int(duration_cap)

        if on_meta is not None:
            try:
                on_meta(result.estimated_size or 0)
            except Exception as e:
                _log(f"[HLS] on_meta callback failed: {e}")

        # ---- 3. Engine A: local segment downloader -----------------------
        local_inputs: List[str] = []
        download_error = ""
        download_detail = ""
        segments_downloaded = False
        remux_errors: List[str] = []

        if engine in ("auto", "segments") and prep is not None and (
            prep.video_pl is not None and prep.video_pl.segments
        ):
            for seg_try in (0, 1):
                try:
                    await _notify(status_cb, "⬇️ Memuat turun segmen HLS…")
                    local_inputs = await _engine_segments(
                        client, prep, temp_dir, limit_seconds, on_chunk=on_chunk,
                        base_headers=base_headers,
                    )
                    segments_downloaded = bool(local_inputs)
                    break
                except (asyncio.CancelledError, WarpProxyError):
                    raise
                except _HlsError as e:
                    if e.reason in (R_DRM, R_NO_SEGMENTS):
                        result.reason = e.reason
                        result.detail = _sanitize_detail(str(e))
                        return result
                    download_error = e.reason or R_DOWNLOAD_FAILED
                    download_detail = _sanitize_detail(str(e))
                    _log(f"[HLS] Engine A failed: {download_detail}")
                    break
                except Exception as e:
                    if (
                        seg_try == 0
                        and not getattr(client, "using_warp", False)
                        and _is_proxy_error(e)
                        and client.proxy_url is not None
                        and _cfg_proxy_fallback_direct()
                    ):
                        _log(
                            f"[HLS] Proxy failed during segment download ({_sanitize_detail(str(e))}) "
                            "— switching to direct connection"
                        )
                        await _switch_client_direct()
                        shutil.rmtree(temp_dir, ignore_errors=True)
                        os.makedirs(temp_dir, exist_ok=True)
                        continue
                    download_error = R_DOWNLOAD_FAILED
                    download_detail = _sanitize_detail(str(e))
                    _log(f"[HLS] Engine A failed: {download_detail}")
                    break

        if local_inputs:
            await _notify(status_cb, "🔄 Menukar ke MP4 (stream copy)…")
            modes = await _choose_remux_modes(local_inputs, local=True)
            async with convert_semaphore():
                ok = await _remux_with_fallbacks(
                    local_inputs, scratch, local=True,
                    timeout=HLS_TIMEOUT, limit_seconds=limit_seconds,
                    modes=modes, error_out=remux_errors,
                )
            if ok:
                os.replace(scratch, final_path)
                published = True
                result.path = final_path
                result.size = _safe_size(final_path)
                result.converted = True
                result.engine = "segments"
                _log(f"[HLS] OK {result.name} ({result.size} bytes) via segments")
                return result
            _log(f"[HLS] Engine A remux failed: {remux_errors[-1] if remux_errors else 'unknown'}")

        # ---- 4. Engine B: let ffmpeg fetch and remux ---------------------
        can_run_ffmpeg = (
            engine == "ffmpeg"
            or (
                engine == "auto"
                and not segments_downloaded
                and (
                    getattr(client, "using_warp", False)
                    or not is_socks_proxy(client.proxy_url)
                )
            )
        )
        if can_run_ffmpeg:
            ffmpeg_proxy_url = client.proxy_url
            ffmpeg_using_warp = False
            if proxy_url is AUTO_PROXY:
                warp_http_cfg = await ensure_proxy_ready(
                    require_http=True,
                    require_socks5=False,
                    feature="HLS",
                )
                if warp_http_cfg is not None:
                    ffmpeg_proxy_url = warp_http_cfg.http_url
                    ffmpeg_using_warp = True

            await _notify(status_cb, "⬇️ Memuat turun & menukar ke MP4 (ffmpeg)…")
            target_url = prep.video_url if prep and prep.video_url else url
            inputs: List[str] = [target_url]
            if prep is not None and prep.audio_url:
                inputs.append(prep.audio_url)
            extra = (
                _client_ffmpeg_headers(client, base_headers, cookies, target_url)
                + ffmpeg_proxy_args(ffmpeg_proxy_url)
            )
            async with convert_semaphore():
                ok = await _remux_with_fallbacks(
                    inputs, scratch, local=False, timeout=HLS_TIMEOUT,
                    extra_input_args=extra, limit_seconds=limit_seconds,
                    on_progress=on_chunk, error_out=remux_errors,
                )
            if ok:
                os.replace(scratch, final_path)
                published = True
                result.path = final_path
                result.size = _safe_size(final_path)
                result.converted = True
                result.engine = "ffmpeg"
                _log(f"[HLS] OK {result.name} ({result.size} bytes) via ffmpeg")
                return result
            if ffmpeg_using_warp:
                last_ffmpeg_err = remux_errors[-1] if remux_errors else ""
                proxy_still_up = await check_proxy_reachable(
                    require_http=True,
                    require_socks5=False,
                    timeout=1.5,
                )
                if not proxy_still_up or _looks_like_ffmpeg_warp_proxy_error(
                    last_ffmpeg_err, ffmpeg_proxy_url
                ):
                    raise WarpProxyError(
                        format_warp_proxy_error(
                            RuntimeError(last_ffmpeg_err or "wireproxy HTTP listener unreachable"),
                            feature="HLS",
                        )
                    )

        # ---- 5. Accurate failure classification --------------------------
        if segments_downloaded:
            result.reason = R_REMUX_FAILED
            result.detail = _sanitize_detail(remux_errors[-1] if remux_errors else "")
        elif prep is not None and prep.video_pl is not None and not prep.video_pl.segments:
            result.reason = R_NO_SEGMENTS
        elif download_error:
            result.reason = download_error
            result.detail = download_detail
        elif prep_error:
            result.reason = prep_error
            result.detail = prep_detail
        else:
            result.reason = R_DOWNLOAD_FAILED
            result.detail = _sanitize_detail(remux_errors[-1] if remux_errors else "")
        return result
    finally:
        try:
            await client.close()
        except Exception:
            pass
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
        if not published and scratch and os.path.exists(scratch):
            try:
                os.remove(scratch)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Startup housekeeping
# ---------------------------------------------------------------------------

def cleanup_orphaned_hls_dirs() -> None:
    """Remove leftover HLS scratch from a previous crash."""
    removed = 0
    cutoff = time.time() - 6 * 3600

    def _stale(path: str) -> bool:
        try:
            return os.path.getmtime(path) < cutoff
        except OSError:
            return False

    def _sweep(directory: str) -> None:
        nonlocal removed
        for d in glob.glob(os.path.join(directory, "hls_*")):
            if os.path.isdir(d) and _stale(d):
                shutil.rmtree(d, ignore_errors=True)
                removed += 1
        for f in glob.glob(os.path.join(directory, ".hls_*.mp4")):
            if os.path.isfile(f) and _stale(f):
                try:
                    os.remove(f)
                    removed += 1
                except OSError:
                    pass

    _sweep(tempfile.gettempdir())
    if HLS_TEMP_DIR and os.path.isdir(HLS_TEMP_DIR):
        _sweep(HLS_TEMP_DIR)

    if removed:
        _log(f"[HLS] Cleaned up {removed} orphaned temp item(s)")


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import subprocess
    import sys

    TEST_URL = "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8"

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    def _probe(path: str) -> dict:
        out = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "stream=codec_type,codec_name,width,height",
             "-show_entries", "format=format_name,duration",
             "-of", "json", path],
            capture_output=True, text=True,
        )
        if out.returncode != 0 or not out.stdout:
            return {}
        return json.loads(out.stdout)

    def _faststart_ok(path: str) -> bool:
        """True when the moov atom precedes mdat (what Telegram needs to stream)."""
        with open(path, "rb") as fh:
            head = fh.read(1024 * 1024)
        moov, mdat = head.find(b"moov"), head.find(b"mdat")
        return moov != -1 and (mdat == -1 or moov < mdat)

    async def _run_case() -> None:
        work = tempfile.mkdtemp(prefix="hls_selftest_")
        res = await download_hls_to_mp4(
            TEST_URL, "selftest.m3u8", out_dir=work, max_duration=20,
        )
        _check("converted", res.converted, f"reason={res.reason!r} detail={res.detail!r}")
        if not res.converted:
            shutil.rmtree(work, ignore_errors=True)
            return

        _check("engine reported", res.engine in ("segments", "ffmpeg"), res.engine)
        _check("output exists", os.path.exists(res.path), res.path)
        _check("name is .mp4", res.name == "selftest.mp4", res.name)
        _check("size matches file", res.size == _safe_size(res.path), str(res.size))
        _check("size > 100 KB", res.size > 100 * 1024, str(res.size))

        meta = _probe(res.path)
        fmt = (meta.get("format") or {}).get("format_name", "")
        streams = meta.get("streams") or []
        vcodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "video"]
        acodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "audio"]
        _check("container is mp4", "mp4" in fmt, fmt)
        _check("video stream is h264", "h264" in vcodecs, str(vcodecs))
        _check("audio stream present (not silent)", bool(acodecs), str(acodecs))
        _check("faststart (moov before mdat)", _faststart_ok(res.path))
        dur = float((meta.get("format") or {}).get("duration", 0) or 0)
        _check("duration ~= 20s cap", abs(dur - 20) < 6, str(dur))
        _check("no .hls_* scratch left",
               not [f for f in os.listdir(work) if f.startswith(".hls_")],
               str(os.listdir(work)))
        _check("no hls_* temp dir left",
               not [d for d in os.listdir(work) if d.startswith("hls_")],
               str(os.listdir(work)))
        shutil.rmtree(work, ignore_errors=True)

    async def _cancel_case() -> None:
        """A cancelled job must leave neither a temp dir nor a scratch file."""
        work = tempfile.mkdtemp(prefix="hls_selftest_")
        task = asyncio.create_task(
            download_hls_to_mp4(TEST_URL, "cancel.m3u8", out_dir=work)
        )
        await asyncio.sleep(2.0)
        task.cancel()
        cancelled = False
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        _check("cancellation propagates", cancelled)
        _check("cancel: no hls_* temp dir left",
               not [d for d in os.listdir(work) if d.startswith("hls_")],
               str(os.listdir(work)))
        _check("cancel: no .hls_* scratch left",
               not [f for f in os.listdir(work) if f.startswith(".hls_")],
               str(os.listdir(work)))
        shutil.rmtree(work, ignore_errors=True)

    print("--- HLS download + remux ---")
    asyncio.run(_run_case())
    print("--- cancellation cleanup ---")
    asyncio.run(_cancel_case())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
