"""
hls/extract.py — Universal static HTTP stream discovery & JS deobfuscation.

Why this exists
---------------
Some CDNs only accept the playlist URL that *they* minted for the client that
loaded the page (the token carries the requester's IP/ASN/TTL), and most modern
video hosts hide their stream URL behind JS packers (Dean Edwards ``p,a,c,k,e,d``),
VOE obfuscated JSON blobs, base64/hex/unicode encodings, nested ``<iframe>``
chains, JS/meta redirects, or extensionless endpoints.

This module implements Tier 0 (direct-stream probe) and Tier 1 (static HTTP +
generic JS deobfuscation + multi-hop embed following + Content-Type/body
validation for HLS, DASH, and direct MP4/WebM fallbacks).

Self-test (offline — only a loopback server is started, no network needed):

    python -m app.hls.extract
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import html as _html
import inspect
import json
import re
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import SplitResult, unquote, urljoin, urlsplit

import app.config as _app_config
from app.config import (
    HLS_PAGE_FOLLOW_EMBED,
    HLS_PAGE_MAX_BYTES,
    HLS_PAGE_MAX_CANDIDATES,
)
from app.hls.client import (
    DEFAULT_USER_AGENT,
    HlsClient,
    HlsHttpError,
    PageFetchResult,
    default_headers,
    redact_url,
    sniff_stream_bytes,
)
from app.hls.playlist import is_hls_playlist
from app.warp import WarpProxyError

# ---------------------------------------------------------------------------
# Unified handoff contract (extractor -> handler -> downloader)
# ---------------------------------------------------------------------------


@dataclass
class ExtractedStream:
    """Normalized stream extraction outcome shared across all extraction tiers."""

    url: Optional[str] = None
    #: "hls" | "direct" | "dash"
    stream_type: str = "hls"
    audio_url: Optional[str] = None
    audio_headers: Optional[Dict[str, str]] = None
    page_url: str = ""
    title: str = ""
    manifest_text: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)
    cookie_entries: List[Dict[str, Any]] = field(default_factory=list)
    fallback_urls: List[str] = field(default_factory=list)
    #: "direct-probe" | "http" | "yt-dlp" | "browser" | ""
    extractor: str = ""
    used_proxy: bool = False
    used_warp: bool = False
    force_direct: bool = False
    #: "ok" | "not-a-page" | "no-manifest" | "no-valid-manifest" | "no-browser" | "error"
    reason: str = ""
    detail: str = ""
    player_page: bool = False


# ---------------------------------------------------------------------------
# What counts as a manifest / stream / embed
# ---------------------------------------------------------------------------

#: Playlists this pipeline prefers first (HLS).
HLS_EXTENSIONS = (".m3u8", ".m3u", ".m3u8?")
#: DASH and progressive direct video formats (kept as ranked fallbacks).
DASH_EXTENSIONS = (".mpd",)
DIRECT_VIDEO_EXTENSIONS = (".mp4", ".webm", ".mov", ".mkv", ".flv", ".m4v")
OTHER_STREAM_EXTENSIONS = (".mpd", ".mp4", ".webm", ".flv", ".mov", ".mkv", ".m4v")

#: Static assets that are never a video stream or player embed.
_STATIC_ASSET_EXTS = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp", ".avif",
    ".css", ".js", ".mjs", ".map", ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".vtt", ".srt", ".ass", ".ssa", ".pdf", ".zip", ".rar", ".7z", ".gz",
)

#: Trailing punctuation that web pages glue onto URLs.
_TRAILING_JUNK = " \"'`,;)]}.!?\\"

#: Characters that can never appear raw inside a URL we would fetch.
_URL_STOP = r"""\s\"'<>\\`\[\]{}$"""

#: Absolute and protocol-relative URLs anywhere in the page.
_ABS_URL = re.compile(r"""(?:https?:)?//[^""" + _URL_STOP + r"""]+""", re.IGNORECASE)

#: URL-encoded absolute URLs inside query parameters or attributes.
_PCT_URL = re.compile(r"""https?%3A%2F%2F[^""" + _URL_STOP + r"""&]+""", re.IGNORECASE)

#: JS/JSON player config properties pointing to an m3u8 URL even if ``.m3u8``
#: sits in a query param or non-terminal path segment.
_PLAYER_URL_KV = re.compile(
    r"""(?:["']?(?:file|hls|hlsUrl|hls_url|stream|streamUrl|stream_url|manifest|manifestUrl|playlist|playlistUrl|m3u8|m3u8Url)["']?\s*[:=]|["'](?:src|source|url)["']?\s*:)\s*"""
    r"""(?:"([^"\\<>]+\.m3u8?[^"\\<>]*)"|'([^'\\<>]+\.m3u8?[^'\\<>]*)')""",
    re.IGNORECASE,
)

#: Explicit player config keys that may hold an extensionless HLS/DASH/MP4 URL.
_PLAYER_ANY_URL_KV = re.compile(
    r"""["']?(?:file|hls|hlsUrl|hls_url|stream|streamUrl|stream_url|manifest|manifestUrl|playlist|playlistUrl|m3u8|m3u8Url|videoUrl|video_url|contentUrl|dash|mpd|mp4|video_src|src_hls)["']?\s*[:=]\s*"""
    r"""(?:"((?:https?:)?//[^"\\<>\s]+|/[^"\\<>\s]+)"|'((?:https?:)?//[^'\\<>\s]+|/[^'\\<>\s]+)')""",
    re.IGNORECASE,
)

#: ``<video>`` and ``<source>`` tags.
_MEDIA_TAG_RE = re.compile(
    r"""<(?:video|source)\b([^>]+)>""",
    re.IGNORECASE,
)
_ATTR_SRC_RE = re.compile(
    r"""\b(?:data-)?src\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)
_ATTR_TYPE_RE = re.compile(
    r"""\btype\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)

#: Page-relative manifest/video paths, e.g. ``/hls/1234/master.m3u8``.
_REL_MANIFEST = re.compile(
    r"""(?<![\w/])(/[^""" + _URL_STOP + r"""()]*\.(?:m3u8?|mpd|mp4|webm)[^"""
    + _URL_STOP + r"""()]*)""",
    re.IGNORECASE,
)

#: ``<iframe src=...>`` / ``<embed src=...>`` (plus ``data-src=`` / ``data-embed=`` lazy embeds).
_EMBED = re.compile(
    r"""<(?:iframe|embed)\b[^>]*?\b(?:data-(?:src|embed|url|iframe|player)|src)\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)

#: JS redirects and dynamic iframe src assignments.
_JS_REDIRECT_RE = re.compile(
    r"""(?:(?:window|self|top)\.)?location(?:\.href)?\s*=\s*(?:"([^"\\<>]+)"|'([^'\\<>]+)')"""
    r"""|(?:(?:window|self|top)\.)?location\.(?:replace|assign)\s*\(\s*(?:"([^"\\<>]+)"|'([^'\\<>]+)')\s*\)""",
    re.IGNORECASE,
)
_JS_IFRAME_SRC_RE = re.compile(
    r"""\b(?:iframe|player|embed)\w*\.src\s*=\s*(?:"([^"\\<>]+)"|'([^'\\<>]+)')""",
    re.IGNORECASE,
)
_META_REFRESH_RE = re.compile(
    r"""<meta\b[^>]*\bhttp-equiv\s*=\s*["']?refresh["']?[^>]*\bcontent\s*=\s*(?:"([^"]+)"|'([^']+)')""",
    re.IGNORECASE,
)
_META_TAG_RE = re.compile(r"""<meta\b([^>]+)>""", re.IGNORECASE)
_META_PROP_RE = re.compile(
    r"""\b(?:property|name)\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)
_META_CONTENT_RE = re.compile(
    r"""\bcontent\s*=\s*(?:"([^"]*)"|'([^']*)')""",
    re.IGNORECASE,
)
_TITLE_TAG_RE = re.compile(r"""<title\b[^>]*>(.*?)</title>""", re.IGNORECASE | re.DOTALL)

#: Base64 blobs (some players ship the manifest URL encoded).
_B64_BLOB = re.compile(r"""[A-Za-z0-9+/_-]{40,}={0,2}""")

#: Explicit ``atob("...")`` calls (catches shorter base64 URLs too).
_ATOB_RE = re.compile(r"""\batob\s*\(\s*(?:"([A-Za-z0-9+/=_-]{16,4096})"|'([A-Za-z0-9+/=_-]{16,4096})')\s*\)""")

#: ``String.fromCharCode(104, 116, 116, 112, ...)``
_FROM_CHAR_CODE_RE = re.compile(
    r"""String\.fromCharCode\s*\(\s*((?:(?:0x[0-9a-fA-F]+|\d+)\s*,\s*){7,}(?:0x[0-9a-fA-F]+|\d+))\s*\)"""
)

#: Adjacent JS string concatenation like ``"https://" + "cdn.x/a.m3u8"``.
_JS_CONCAT_RE = re.compile(
    r"""(?:(?:"[^"\\\r\n]{1,200}"|'[^'\\\r\n]{1,200}')\s*\+\s*){1,12}(?:"[^"\\\r\n]{1,200}"|'[^'\\\r\n]{1,200}')"""
)

#: Reversed URL fragments like ``8u3m.retsam/slh/moc.ndc//:sptth``.
_REV_URL_RE = re.compile(r"""[^\s"'<>\\`\[\]{}]{12,512}//:s?ptth""", re.IGNORECASE)

_JSON_SCRIPT_RE = re.compile(
    r"""<script\b[^>]*\btype\s*=\s*(?:"application/(?:ld\+)?json"|'application/(?:ld\+)?json')[^>]*>(.*?)</script>""",
    re.IGNORECASE | re.DOTALL,
)
_NEXT_DATA_RE = re.compile(
    r"""<script\b[^>]*\bid\s*=\s*(?:"__NEXT_DATA__"|'__NEXT_DATA__')[^>]*>(.*?)</script>""",
    re.IGNORECASE | re.DOTALL,
)

_VOE_ROT13_TRANS = str.maketrans(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    "NOPQRSTUVWXYZABCDEFGHIJKLMnopqrstuvwxyzabcdefghijklm",
)
_VOE_MARKERS = ("@$", "^^", "~@", "%?", "*~", "!!", "#&")
_VOE_MAX_BLOCK_CHARS = 128 * 1024

#: Full JS unicode / hex escape matchers for _unescape().
_UNICODE_BRACE_RE = re.compile(r"""\\u\{([0-9a-fA-F]{1,6})\}""")
_UNICODE_HEX_RE = re.compile(r"""\\u([0-9a-fA-F]{4})""")
_HEX_ESC_RE = re.compile(r"""\\x([0-9a-fA-F]{2})""")

#: Markers that a page embeds a player (used only for the user-facing hint).
_PLAYER_MARKERS = (
    "m3u8", "hls.js", "hlsjs", "jwplayer", "videojs", "plyr", "shaka",
    "artplayer", "dplayer", "clappr", "fluidplayer", "flowplayer", "bitmovin",
    "<video", "player", "<iframe", "p,a,c,k,e,d",
)

#: Known ad/tracker/captcha/widget hosts to ignore for embed hops & deprioritize for streams.
_AD_HOST_PARTS = (
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adsbygoogle", "adservice", "popads", "popcash", "propellerads",
    "exoclick", "juicyads", "trafficjunky", "adsterra", "hilltopads",
    "facebook.net", "criteo", "taboola", "outbrain", "openfpcdn",
    "histats", "yandex.ru/metrika", "mc.yandex", "adnxs", "pubmatic",
    "rubiconproject", "smartadserver", "mgid.com", "revcontent",
)

_NON_PLAYER_EMBED_PARTS = (
    "challenges.cloudflare.com", "hcaptcha.com", "recaptcha.net",
    "google.com/recaptcha", "facebook.com/plugins", "platform.twitter.com",
    "disqus.com", "accounts.google.com", "cookielaw.org", "onetrust.com",
    "consentmanager", "cookiebot.com", "chatango.com", "tawk.to",
    "intercom.io", "zendesk.com", "crisp.chat", "stripe.com",
)

_AD_STREAM_MARKERS = (
    "/vast", "preroll", "/ads/", "adserver", "doubleclick", "googlesyndication",
    "preview.", "/preview/", "trailer", "teaser", "/thumbs/", "sprite",
)

_USELESS_TITLES = {
    "just a moment...", "attention required!", "403 forbidden", "404 not found",
    "access denied", "error", "loading", "loading...", "embed", "player",
    "video", "video player", "watch", "stream", "index", "home", "untitled",
}

_STREAM_JSON_KEYS = (
    "source", "sources", "hls", "hlsurl", "hls_url", "file", "files",
    "manifest", "manifesturl", "playlist", "playlisturl", "m3u8", "m3u8url",
    "stream", "streamurl", "stream_url", "videourl", "video_url",
    "contenturl", "dash", "mpd", "mp4", "src", "url",
)
_EMBED_JSON_KEYS = ("embedurl", "embed_url", "iframe", "iframeurl", "playerurl", "embed")


# ---------------------------------------------------------------------------
# Generic JS deobfuscation & unescaping
# ---------------------------------------------------------------------------


def _unescape(text: str) -> str:
    """Undo the encodings sites use when they embed a URL.

    Handles JSON/JS escaped slashes (``\\/``), full ``\\xHH``, ``\\uHHHH``,
    ``\\u{H+}`` escapes for printable ASCII/URL characters, and HTML entities.
    """
    if not text:
        return ""
    text = text.replace("\\\\/", "/").replace("\\/", "/")

    if "\\u{" in text:
        def _repl_ubrace(m: re.Match) -> str:
            cp = int(m.group(1), 16)
            return chr(cp) if 0x20 <= cp <= 0x7E else m.group(0)
        text = _UNICODE_BRACE_RE.sub(_repl_ubrace, text)

    if "\\u" in text:
        def _repl_uhex(m: re.Match) -> str:
            cp = int(m.group(1), 16)
            return chr(cp) if 0x20 <= cp <= 0x7E else m.group(0)
        text = _UNICODE_HEX_RE.sub(_repl_uhex, text)

    if "\\x" in text:
        def _repl_xhex(m: re.Match) -> str:
            cp = int(m.group(1), 16)
            return chr(cp) if 0x20 <= cp <= 0x7E else m.group(0)
        text = _HEX_ESC_RE.sub(_repl_xhex, text)

    return _html.unescape(text)


_PACKER_HEADER_RE = re.compile(
    r"""eval\s*\(\s*function\s*\(\s*p\s*,\s*a\s*,\s*c\s*,\s*k\s*,\s*e\s*,\s*[a-zA-Z_$][\w$]*\s*\)""",
    re.IGNORECASE,
)
_PACKER_WORD_RE = re.compile(r"""\b\w+\b""")
_BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _encode_base(num: int, radix: int) -> str:
    """Encode non-negative *num* in *radix* (Dean Edwards packer convention)."""
    if radix <= 1:
        return str(num)
    if radix <= 62:
        if num == 0:
            return "0"
        digits: List[str] = []
        n = num
        while n > 0:
            n, rem = divmod(n, radix)
            digits.append(_BASE62_ALPHABET[rem])
        return "".join(reversed(digits))
    # Rare base > 62 (e.g. 95): ASCII starting at 32.
    if num == 0:
        return " "
    digits = []
    n = num
    while n > 0:
        n, rem = divmod(n, radix)
        digits.append(chr(rem + 32))
    return "".join(reversed(digits))


def _read_js_string(src: str, start: int) -> Tuple[Optional[str], int]:
    """Read a single- or double-quoted JS string literal starting at *start*."""
    n = len(src)
    while start < n and src[start].isspace():
        start += 1
    if start >= n or src[start] not in ("'", '"'):
        return None, start
    quote = src[start]
    i = start + 1
    out: List[str] = []
    while i < n:
        ch = src[i]
        if ch == "\\":
            if i + 1 < n:
                nxt = src[i + 1]
                if nxt == "n":
                    out.append("\n")
                elif nxt == "r":
                    out.append("\r")
                elif nxt == "t":
                    out.append("\t")
                elif nxt in ("'", '"', "\\", "/"):
                    out.append(nxt)
                else:
                    out.append("\\")
                    out.append(nxt)
                i += 2
                continue
            break
        if ch == quote:
            return "".join(out), i + 1
        out.append(ch)
        i += 1
    return None, start


def _unpack_one_packer(src: str, start_pos: int) -> Tuple[Optional[str], int]:
    """Parse and unpack one Dean Edwards ``eval(function(p,a,c,k,e,d){...}(...))`` block."""
    # Find the end of the function body ``}(`` within a bounded window.
    search_end = min(len(src), start_pos + 2048)
    body_end = src.find("}(", start_pos, search_end)
    if body_end == -1:
        # Allow whitespace between '}' and '('
        m_end = re.search(r"""\}\s*\(""", src[start_pos:search_end])
        if not m_end:
            return None, start_pos + 1
        arg_pos = start_pos + m_end.end()
    else:
        arg_pos = body_end + 2

    payload, pos = _read_js_string(src, arg_pos)
    if payload is None or len(payload) > 512 * 1024:
        return None, arg_pos
    m_nums = re.match(r"""\s*,\s*(\d{1,3})\s*,\s*(\d{1,6})\s*,\s*""", src[pos:pos + 64])
    if not m_nums:
        return None, pos
    radix = int(m_nums.group(1))
    count = int(m_nums.group(2))
    pos += m_nums.end()

    sym_str, pos = _read_js_string(src, pos)
    if sym_str is None:
        return None, pos

    # Expect ``.split('|')`` (or another single-char separator).
    m_split = re.match(r"""\s*\.split\s*\(\s*(?:"([^"]{1,4})"|'([^']{1,4})')\s*\)""", src[pos:pos + 64])
    if not m_split:
        return None, pos
    sep = m_split.group(1) or m_split.group(2) or "|"
    pos += m_split.end()

    symtab = sym_str.split(sep)
    if not symtab or radix < 2 or radix > 95 or count > 100000:
        return None, pos

    lookup: Dict[str, str] = {}
    limit_c = min(count, len(symtab))
    for i in range(limit_c):
        word = symtab[i]
        if word:
            lookup[_encode_base(i, radix)] = word

    if not lookup:
        return payload, pos

    unpacked = _PACKER_WORD_RE.sub(lambda m: lookup.get(m.group(0), m.group(0)), payload)
    return unpacked, pos


def unpack_packer(html: str, *, max_blocks: int = 12, max_depth: int = 3) -> List[str]:
    """Unpack Dean Edwards ``p,a,c,k,e,d`` JS blocks (including nested packs)."""
    if not html or "p,a,c,k,e" not in html:
        return []
    results: List[str] = []
    queue: deque[Tuple[str, int]] = deque([(html, 0)])
    while queue and len(results) < max_blocks:
        chunk, depth = queue.popleft()
        if depth >= max_depth:
            continue
        cursor = 0
        while len(results) < max_blocks:
            m = _PACKER_HEADER_RE.search(chunk, cursor)
            if not m:
                break
            unpacked, next_pos = _unpack_one_packer(chunk, m.start())
            cursor = max(m.end(), next_pos)
            if unpacked:
                results.append(unpacked)
                if "p,a,c,k,e" in unpacked and depth + 1 < max_depth:
                    queue.append((unpacked, depth + 1))
    return results


def _decode_js_tricks(text: str, limit: int = 16) -> List[str]:
    """Reconstruct URLs hidden via atob(), fromCharCode(), string concat, or reversal."""
    if not text or limit <= 0:
        return []
    out: List[str] = []

    def _keep(candidate: str) -> None:
        if len(out) >= limit or not candidate:
            return
        low = candidate.lower()
        if (
            "http://" in low
            or "https://" in low
            or ".m3u8" in low
            or ".mp4" in low
            or ".mpd" in low
            or "/hls/" in low
            or "/embed/" in low
        ):
            out.append(_unescape(candidate))

    for m in _ATOB_RE.finditer(text):
        if len(out) >= limit:
            break
        blob = m.group(1) or m.group(2) or ""
        if not blob:
            continue
        padded = blob + "=" * (-len(blob) % 4)
        try:
            raw = base64.b64decode(padded, validate=False)
            _keep(raw.decode("utf-8", "ignore"))
        except (binascii.Error, ValueError):
            continue

    for m in _FROM_CHAR_CODE_RE.finditer(text):
        if len(out) >= limit:
            break
        parts = [p.strip() for p in m.group(1).split(",") if p.strip()]
        if len(parts) > 2048:
            continue
        try:
            chars = []
            for p in parts:
                val = int(p, 16) if p.lower().startswith("0x") else int(p)
                if 0 < val <= 0x10FFFF:
                    chars.append(chr(val))
            _keep("".join(chars))
        except ValueError:
            continue

    for m in _JS_CONCAT_RE.finditer(text):
        if len(out) >= limit:
            break
        expr = m.group(0)
        pieces = re.findall(r""""([^"\\\r\n]*)"|'([^'\\\r\n]*)'""", expr)
        joined = "".join(a or b for a, b in pieces)
        _keep(joined)

    for m in _REV_URL_RE.finditer(text):
        if len(out) >= limit:
            break
        _keep(m.group(0)[::-1])

    return out


def _safe_urlsplit(url: str) -> Optional[SplitResult]:
    """``urlsplit`` that never raises on page junk."""
    try:
        return urlsplit(url)
    except ValueError:
        return None


def _origin_of(url: str) -> str:
    parts = _safe_urlsplit(url)
    if parts is None or not parts.scheme or not parts.netloc:
        return ""
    return f"{parts.scheme}://{parts.netloc}"


def _clean_candidate(raw: str, base_url: str) -> str:
    """Turn a raw match into an absolute URL (or ``""`` when unusable)."""
    url = (raw or "").strip().strip("\"'").rstrip(_TRAILING_JUNK)
    if not url:
        return ""
    low = url.lower()
    if low.startswith(("javascript:", "mailto:", "data:", "blob:", "about:", "#")):
        return ""
    if low.startswith(("http://", "https://")):
        return url if _safe_urlsplit(url) else ""
    if url.startswith("//"):
        base = _safe_urlsplit(base_url)
        if base is None or not base.netloc:
            return ""
        return f"{base.scheme or 'https'}:{url}"
    if url.startswith("/"):
        base = _safe_urlsplit(base_url)
        if base is None or not base.scheme or not base.netloc:
            return ""
        return f"{base.scheme}://{base.netloc}{url}"
    try:
        joined = urljoin(base_url, url)
    except ValueError:
        return ""
    return joined if _safe_urlsplit(joined) else ""


def _is_static_asset_url(url: str) -> bool:
    parts = _safe_urlsplit(url)
    if parts is None:
        return True
    path_low = (parts.path or "").lower()
    return path_low.endswith(_STATIC_ASSET_EXTS)


def _is_ad_url(url: str) -> bool:
    parts = _safe_urlsplit(url)
    if parts is None:
        return True
    host = (parts.hostname or "").lower()
    return any(part in host for part in _AD_HOST_PARTS)


def _is_non_player_embed(url: str) -> bool:
    if _is_ad_url(url):
        return True
    low = url.lower()
    return any(part in low for part in _NON_PLAYER_EMBED_PARTS)


def _looks_like_stream(url: str, *, allow_query_m3u8: bool = False) -> bool:
    """True when the URL looks like a playable source."""
    parts = _safe_urlsplit(url)
    if parts is None:
        return False
    path_low = parts.path.lower()
    if path_low.endswith(HLS_EXTENSIONS + OTHER_STREAM_EXTENSIONS):
        return True
    if allow_query_m3u8 and (".m3u8" in url.lower() or ".m3u" in path_low):
        return True
    return False


def _looks_like_extensionless_stream(url: str) -> bool:
    """True for plausible extensionless player-config URLs (not static assets/HTML)."""
    parts = _safe_urlsplit(url)
    if parts is None or parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        return False
    if _is_ad_url(url) or _is_static_asset_url(url):
        return False
    path_low = (parts.path or "").lower()
    if path_low in ("", "/") or path_low.endswith((".html", ".htm", ".php", ".asp", ".aspx")):
        # Allow .php only when it looks like a stream endpoint with query params.
        if not (path_low.endswith(".php") and parts.query):
            return False
    return True


def _rank(url: str) -> Tuple[int, int, int, int, int]:
    """Sort key: HLS > extensionless player stream > DASH > direct video; non-ad first."""
    parts = _safe_urlsplit(url)
    path = (parts.path if parts is not None else "").lower()
    host = (parts.hostname if parts is not None else "") or ""
    low_full = url.lower()
    if path.endswith(HLS_EXTENSIONS) or ".m3u8" in low_full:
        tier = 0
    elif path.endswith(DASH_EXTENSIONS):
        tier = 2
    elif path.endswith(DIRECT_VIDEO_EXTENSIONS):
        tier = 3
    else:
        # Extensionless player-config endpoint (could be disguised HLS or direct video).
        tier = 1
    is_junk = (
        1
        if (
            any(part in host for part in _AD_HOST_PARTS)
            or any(marker in low_full for marker in _AD_STREAM_MARKERS)
        )
        else 0
    )
    term = path.rsplit("/", 1)[-1]
    is_master = 0 if ("master" in term or "manifest" in term or "playlist" in term) else 1
    has_query = 0 if "?" in url else 1
    return (tier, is_junk, is_master, has_query, len(url))


def _decoded_blobs(text: str, limit: int = 12) -> List[str]:
    """Text of up to *limit* base64 blobs that decode to something URL-ish."""
    out: List[str] = []
    for match in _B64_BLOB.finditer(text):
        if len(out) >= limit:
            break
        blob = match.group(0)
        padded = blob + "=" * (-len(blob) % 4)
        try:
            raw = base64.b64decode(padded, validate=False)
        except (binascii.Error, ValueError):
            continue
        decoded = raw.decode("utf-8", "ignore")
        low = decoded.lower()
        if "." in decoded and (
            "m3u8" in low or "http" in low or ".mp4" in low or ".mpd" in low or "/hls/" in low
        ):
            out.append(_unescape(decoded))
    return out


def _decode_voe_payload(encoded: Any) -> Optional[str]:
    if not isinstance(encoded, str) or not encoded or len(encoded) > _VOE_MAX_BLOCK_CHARS:
        return None
    try:
        step = encoded.translate(_VOE_ROT13_TRANS)
        for tok in _VOE_MARKERS:
            step = step.replace(tok, "")
        raw1 = base64.b64decode(step + "=" * (-len(step) % 4), validate=False)
        shifted = bytes((b - 3) % 256 for b in raw1)
        rev = shifted[::-1]
        raw2 = base64.b64decode(rev + b"=" * (-len(rev) % 4), validate=False)
        decoded = raw2.decode("utf-8")
        obj = json.loads(decoded)
        if isinstance(obj, (dict, list)):
            return decoded
    except Exception:
        return None
    return None


def _decoded_voe_json_blobs(html: str, limit: int = 6) -> List[str]:
    """Decode bounded VOE ``<script type="application/json">`` config blobs."""
    if not html or limit <= 0:
        return []
    out: List[str] = []
    scanned = 0
    max_scan = max(limit * 4, 24)
    for match in _JSON_SCRIPT_RE.finditer(html):
        scanned += 1
        if scanned > max_scan or len(out) >= limit:
            break
        raw_block = (match.group(1) or "").strip()
        if not raw_block or len(raw_block) > _VOE_MAX_BLOCK_CHARS:
            continue
        try:
            arr = json.loads(raw_block)
        except Exception:
            continue
        if not isinstance(arr, list):
            continue
        for item in arr[:4]:
            decoded = _decode_voe_payload(item)
            if decoded:
                out.append(decoded)
                if len(out) >= limit:
                    break
    return out


def _iter_voe_json_urls(obj: Any, depth: int = 0) -> Iterable[str]:
    if depth > 4:
        return
    if isinstance(obj, str):
        yield obj
        return
    if isinstance(obj, list):
        for item in obj[:16]:
            yield from _iter_voe_json_urls(item, depth + 1)
        return
    if isinstance(obj, dict):
        priority = ("source", "hls", "file", "src", "url")
        for key in priority:
            if key in obj:
                yield from _iter_voe_json_urls(obj[key], depth + 1)
        for key, val in obj.items():
            if key not in priority:
                yield from _iter_voe_json_urls(val, depth + 1)


def _iter_structured_json_urls(
    obj: Any,
    *,
    keys: Tuple[str, ...],
    depth: int = 0,
) -> Iterable[str]:
    if depth > 6:
        return
    if isinstance(obj, list):
        for item in obj[:24]:
            yield from _iter_structured_json_urls(item, keys=keys, depth=depth + 1)
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            klow = str(k).lower()
            if klow in keys:
                if isinstance(v, str) and v.strip():
                    yield v.strip()
                elif isinstance(v, (list, dict)):
                    for s in _iter_voe_json_urls(v, depth=0):
                        yield s
            elif isinstance(v, (dict, list)):
                yield from _iter_structured_json_urls(v, keys=keys, depth=depth + 1)


def _extract_json_scripts(html: str, limit: int = 8) -> List[Any]:
    out: List[Any] = []
    for regex in (_JSON_SCRIPT_RE, _NEXT_DATA_RE):
        for m in regex.finditer(html):
            if len(out) >= limit:
                return out
            raw = (m.group(1) or "").strip()
            if not raw or len(raw) > 512 * 1024:
                continue
            try:
                out.append(json.loads(raw))
            except Exception:
                continue
    return out


def _parse_meta_tags(html: str) -> Dict[str, List[str]]:
    """Extract ``property``/``name`` -> ``content`` values from ``<meta>`` tags."""
    meta: Dict[str, List[str]] = {}
    if not html or "<meta" not in html.lower():
        return meta
    for m in _META_TAG_RE.finditer(html):
        attrs = m.group(1)
        pm = _META_PROP_RE.search(attrs)
        cm = _META_CONTENT_RE.search(attrs)
        if not pm or not cm:
            continue
        key = (pm.group(1) or pm.group(2) or pm.group(3) or "").strip().lower()
        val = _html.unescape((cm.group(1) if cm.group(1) is not None else cm.group(2) or "").strip())
        if key and val:
            meta.setdefault(key, []).append(val)
    return meta


def extract_title(html: str) -> str:
    """Extract a clean human-readable video title from *html*, or ``""``."""
    if not html:
        return ""
    meta = _parse_meta_tags(html)
    candidates: List[str] = []
    for key in ("og:title", "twitter:title", "title"):
        candidates.extend(meta.get(key, []))

    for obj in _extract_json_scripts(html, limit=4):
        if isinstance(obj, dict):
            typ = str(obj.get("@type") or "").lower()
            if any(t in typ for t in ("video", "movie", "episode", "clip")):
                name = obj.get("name") or obj.get("headline")
                if isinstance(name, str) and name.strip():
                    candidates.append(name.strip())

    tm = _TITLE_TAG_RE.search(html)
    if tm:
        candidates.append(_html.unescape(tm.group(1)))

    for raw in candidates:
        cleaned = re.sub(r"\s+", " ", (raw or "")).strip(" -|:\t\r\n")
        if not cleaned or len(cleaned) < 2:
            continue
        if cleaned.lower() in _USELESS_TITLES:
            continue
        return cleaned[:160]
    return ""


def _scan_text_for_streams(
    text: str,
    base_url: str,
    add_fn: Any,
    *,
    allow_extensionless: bool = True,
) -> None:
    """Scan a single decoded/unpacked text block for stream URLs."""
    for match in _ABS_URL.finditer(text):
        add_fn(match.group(0))
    for match in _REL_MANIFEST.finditer(text):
        add_fn(match.group(1))
    for match in _PLAYER_URL_KV.finditer(text):
        raw_kv = match.group(1) or match.group(2) or ""
        if "%3a%2f%2f" in raw_kv.lower():
            continue
        add_fn(raw_kv, allow_query_m3u8=True)
    for match in _PCT_URL.finditer(text):
        add_fn(unquote(match.group(0)), allow_query_m3u8=True)
    if allow_extensionless:
        for match in _PLAYER_ANY_URL_KV.finditer(text):
            raw_kv = match.group(1) or match.group(2) or ""
            if "%3a%2f%2f" in raw_kv.lower():
                continue
            add_fn(raw_kv, allow_query_m3u8=True, allow_extensionless=True)


def find_manifests(html: str, base_url: str, limit: int = 3) -> List[str]:
    """Every stream/manifest URL referenced by *html*, best candidate first.

    Handles plain URLs, escaped slashes, hex/unicode escapes, HTML entities,
    percent-encoded URLs, protocol/page-relative paths, base64 blobs, VOE JSON
    configs, Dean Edwards ``p,a,c,k,e,d`` JS packers, ``atob``/``fromCharCode``/
    string concatenation/reversed URLs, JSON-LD / ``__NEXT_DATA__``, OpenGraph
    video tags, ``<video>/<source>`` tags, and extensionless player-config URLs.
    """
    if not html:
        return []

    text = _unescape(html)
    found: List[str] = []
    seen: set[str] = set()

    def _add(
        raw: str,
        *,
        allow_query_m3u8: bool = False,
        allow_extensionless: bool = False,
    ) -> None:
        try:
            url = _clean_candidate(raw, base_url)
        except ValueError:
            return
        if not url or url in seen:
            return
        ok = _looks_like_stream(url, allow_query_m3u8=allow_query_m3u8)
        if not ok and allow_extensionless:
            ok = _looks_like_extensionless_stream(url)
        if not ok:
            return
        seen.add(url)
        found.append(url)

    # 1. Raw unescaped HTML / JS
    _scan_text_for_streams(text, base_url, _add, allow_extensionless=True)

    # 2. <video> / <source> tags (allow extensionless URLs unless type is non-media)
    for m in _MEDIA_TAG_RE.finditer(text):
        attrs = m.group(1)
        sm = _ATTR_SRC_RE.search(attrs)
        if not sm:
            continue
        raw_src = sm.group(1) or sm.group(2) or sm.group(3) or ""
        tm = _ATTR_TYPE_RE.search(attrs)
        mtype = (tm.group(1) or tm.group(2) or tm.group(3) or "").lower() if tm else ""
        if mtype.startswith(("image/", "text/", "audio/")):
            continue
        _add(raw_src, allow_query_m3u8=True, allow_extensionless=True)

    # 3. OpenGraph / Twitter meta video tags
    meta = _parse_meta_tags(html)
    for mkey in ("og:video", "og:video:url", "og:video:secure_url"):
        for val in meta.get(mkey, []):
            _add(val, allow_query_m3u8=True, allow_extensionless=False)

    # 4. Dean Edwards p,a,c,k,e,d unpacked scripts
    unpacked_blocks = unpack_packer(html)
    for block in unpacked_blocks:
        unesc_block = _unescape(block)
        _scan_text_for_streams(unesc_block, base_url, _add, allow_extensionless=True)
        for trick in _decode_js_tricks(unesc_block, limit=8):
            _scan_text_for_streams(trick, base_url, _add, allow_extensionless=True)
        for blob in _decoded_blobs(unesc_block, limit=6):
            _scan_text_for_streams(blob, base_url, _add, allow_extensionless=True)

    # 5. Generic JS tricks (atob, String.fromCharCode, string concat, reversed URLs)
    for trick in _decode_js_tricks(text):
        _scan_text_for_streams(trick, base_url, _add, allow_extensionless=True)

    # 6. Base64 config blobs
    for blob in _decoded_blobs(text):
        _scan_text_for_streams(blob, base_url, _add, allow_extensionless=True)

    # 7. Structured JSON (JSON-LD, __NEXT_DATA__, application/json)
    for obj in _extract_json_scripts(html):
        for candidate in _iter_structured_json_urls(obj, keys=_STREAM_JSON_KEYS):
            _add(
                _unescape(candidate),
                allow_query_m3u8=True,
                allow_extensionless=False,
            )

    # 8. VOE obfuscated JSON config blobs
    for vblob in _decoded_voe_json_blobs(html):
        try:
            vobj = json.loads(vblob)
        except Exception:
            vobj = None
        if vobj is not None:
            for candidate in _iter_voe_json_urls(vobj):
                if ".m3u8" in candidate.lower() or candidate.lower().startswith(("http://", "https://", "/")):
                    _add(candidate, allow_query_m3u8=True, allow_extensionless=False)
        _scan_text_for_streams(_unescape(vblob), base_url, _add, allow_extensionless=False)

    found.sort(key=_rank)
    return found[:max(1, limit)]


def find_embed_urls(html: str, base_url: str, limit: int = 4) -> List[str]:
    """Embedded player/iframe/redirect URLs (the second place a manifest hides)."""
    if not html:
        return []

    text = _unescape(html)
    ranked: List[Tuple[int, int, str]] = []
    seen: set[str] = set()
    base_clean = (base_url or "").split("#", 1)[0]

    def _add_embed(raw: str, *, boost: int = 0) -> None:
        try:
            url = _clean_candidate(raw, base_url)
        except ValueError:
            return
        if not url or url in seen or not url.lower().startswith(("http://", "https://")):
            return
        if url.split("#", 1)[0] == base_clean:
            return
        if _is_static_asset_url(url) or _is_non_player_embed(url):
            return
        # Don't treat direct stream URLs as HTML embeds.
        if _looks_like_stream(url):
            return
        seen.add(url)
        low = url.lower()
        if any(k in low for k in ("/embed", "/player", "/e/", "/v/", "/play", "embed", "player", "stream", "video")):
            priority = 0 - boost
        else:
            priority = 1 - boost
        ranked.append((priority, len(url), url))

    # 1. <iframe> / <embed> tags (src, data-src, data-embed, data-player, data-url)
    for match in _EMBED.finditer(text):
        raw = match.group(1) or match.group(2) or match.group(3) or ""
        _add_embed(raw)

    # 2. <meta http-equiv="refresh"> redirects
    for match in _META_REFRESH_RE.finditer(html):
        content = match.group(1) or match.group(2) or ""
        um = re.search(r"""url\s*=\s*['"]?([^'">\s;]+)""", content, re.IGNORECASE)
        if um:
            _add_embed(_html.unescape(um.group(1)), boost=1)

    # 3. OpenGraph / Twitter player embeds
    meta = _parse_meta_tags(html)
    for mkey in ("twitter:player", "og:video", "og:video:url", "og:video:secure_url"):
        for val in meta.get(mkey, []):
            _add_embed(val, boost=1)

    # 4. JS redirects & dynamic iframe.src (both raw and unpacked p,a,c,k,e,d)
    js_sources = [text] + [_unescape(b) for b in unpack_packer(html, max_blocks=6)]
    for js in js_sources:
        for match in _JS_REDIRECT_RE.finditer(js):
            raw = next((g for g in match.groups() if g), "")
            _add_embed(raw, boost=1)
        for match in _JS_IFRAME_SRC_RE.finditer(js):
            raw = match.group(1) or match.group(2) or ""
            _add_embed(raw, boost=1)

    # 5. Structured JSON embedUrl
    for obj in _extract_json_scripts(html, limit=6):
        for candidate in _iter_structured_json_urls(obj, keys=_EMBED_JSON_KEYS):
            _add_embed(_unescape(candidate), boost=1)

    ranked.sort(key=lambda item: (item[0], item[1]))
    return [url for _, _, url in ranked[:max(1, limit)]]


def looks_like_player_page(html: str) -> bool:
    """True when the page probably had a player but no readable manifest."""
    if not html:
        return False
    low = html.lower()
    return any(marker in low for marker in _PLAYER_MARKERS)


# ---------------------------------------------------------------------------
# Page scan
# ---------------------------------------------------------------------------


@dataclass
class PageScan:
    """Outcome of looking for a stream inside a page."""

    #: The stream/manifest URL to hand to the downloader (None when nothing was found).
    url: Optional[str] = None
    #: The page that actually carried it (may be an embed or redirect target)
    #: — used as the browser-like ``Referer`` for the stream.
    page_url: str = ""
    #: "ok" | "not-a-page" | "no-manifest" | "no-valid-manifest" | "error"
    reason: str = ""
    #: How many candidate URLs the page offered.
    candidates: int = 0
    #: True when the page embedded a player (drives the user-facing hint).
    player_page: bool = False
    #: Free-text detail for the log line.
    detail: str = ""
    #: Request headers that worked for the stream (UA, Referer, Origin).
    headers: Dict[str, str] = field(default_factory=dict)
    #: Cookies captured during page/embed/stream loading.
    cookies: Dict[str, str] = field(default_factory=dict)
    #: New backward-compatible contract fields:
    stream_type: str = "hls"
    title: str = ""
    audio_url: Optional[str] = None
    audio_headers: Optional[Dict[str, str]] = None
    manifest_text: str = ""
    cookie_entries: List[Dict[str, Any]] = field(default_factory=list)
    fallback_urls: List[str] = field(default_factory=list)

    def to_extracted_stream(
        self,
        *,
        extractor: str = "http",
        used_proxy: bool = False,
        used_warp: bool = False,
        force_direct: bool = False,
    ) -> ExtractedStream:
        return ExtractedStream(
            url=self.url,
            stream_type=self.stream_type or "hls",
            audio_url=self.audio_url,
            audio_headers=dict(self.audio_headers) if self.audio_headers else None,
            page_url=self.page_url,
            title=self.title,
            manifest_text=self.manifest_text,
            headers=dict(self.headers or {}),
            cookies=dict(self.cookies or {}),
            cookie_entries=list(self.cookie_entries or []),
            fallback_urls=list(self.fallback_urls or []),
            extractor=extractor,
            used_proxy=used_proxy,
            used_warp=used_warp,
            force_direct=force_direct,
            reason=self.reason,
            detail=self.detail,
            player_page=self.player_page,
        )


def _supports_kwarg(fn: Any, kw: str) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.VAR_KEYWORD or param.name == kw:
            return True
    return False


async def _fetch_page_compat(
    client: Any,
    url: str,
    max_bytes: int,
    headers: Optional[Dict[str, str]] = None,
) -> PageFetchResult:
    """Fetch *url* using ``fetch_page_probe`` on real ``HlsClient``, else legacy ``fetch_page``."""
    # Respect unit tests that monkeypatch client.fetch_page on an HlsClient instance.
    use_probe = (
        isinstance(client, HlsClient)
        and "fetch_page" not in getattr(client, "__dict__", {})
        and "fetch_page_text" not in getattr(client, "__dict__", {})
    )
    if use_probe:
        return await client.fetch_page_probe(url, max_bytes=max_bytes, headers=headers)

    fetch_page_fn = getattr(client, "fetch_page", None)
    if callable(fetch_page_fn):
        kwargs: Dict[str, Any] = {"max_bytes": max_bytes}
        if headers and _supports_kwarg(fetch_page_fn, "headers"):
            kwargs["headers"] = headers
        res = await fetch_page_fn(url, **kwargs)
        if res is None:
            return PageFetchResult(kind="not-a-page", final_url=url)
        html, final_url = res
        final_url = final_url or url
        if is_hls_playlist((html or "").lstrip("\ufeff \t\r\n")):
            return PageFetchResult(kind="hls", text=html, final_url=final_url)
        return PageFetchResult(kind="page", text=html, final_url=final_url)

    fetch_text_fn = client.fetch_page_text
    kwargs = {"max_bytes": max_bytes}
    if headers and _supports_kwarg(fetch_text_fn, "headers"):
        kwargs["headers"] = headers
    html = await fetch_text_fn(url, **kwargs)
    if html is None:
        return PageFetchResult(kind="not-a-page", final_url=url)
    if is_hls_playlist((html or "").lstrip("\ufeff \t\r\n")):
        return PageFetchResult(kind="hls", text=html, final_url=url)
    return PageFetchResult(kind="page", text=html, final_url=url)


async def _fetch_text_compat(
    client: Any,
    url: str,
    headers: Optional[Dict[str, Optional[str]]] = None,
) -> Tuple[str, str]:
    fn = getattr(client, "fetch_text_with_url", None)
    if callable(fn):
        if headers and _supports_kwarg(fn, "headers"):
            return await fn(url, headers=headers)
        return await fn(url)

    fn_text = client.fetch_text
    if headers and _supports_kwarg(fn_text, "headers"):
        text = await fn_text(url, headers=headers)
    else:
        text = await fn_text(url)
    return text, url


def _header_attempts(referer_url: str, candidate_url: str) -> List[Dict[str, Optional[str]]]:
    """Ordered browser-like header sets to validate a candidate stream."""
    attempts: List[Dict[str, Optional[str]]] = []
    ref_origin = _origin_of(referer_url)
    cand_origin = _origin_of(candidate_url)

    if referer_url:
        first: Dict[str, Optional[str]] = {"Referer": referer_url}
        if ref_origin:
            first["Origin"] = ref_origin
        attempts.append(first)

        if ref_origin and referer_url != f"{ref_origin}/":
            attempts.append({"Referer": f"{ref_origin}/", "Origin": ref_origin})

        attempts.append({"Referer": referer_url, "Origin": None})

    if cand_origin and cand_origin != ref_origin:
        attempts.append({"Referer": f"{cand_origin}/", "Origin": None})

    clean_attempt: Dict[str, Optional[str]] = {"Referer": None, "Origin": None}
    if clean_attempt not in attempts:
        attempts.append(clean_attempt)
    return attempts


def _build_winning_headers(
    client: Any,
    referer_url: str,
    used_attempt: Dict[str, Optional[str]],
) -> Dict[str, str]:
    ua = getattr(client, "user_agent", None)
    if not ua:
        hdrs = getattr(client, "headers", None) or {}
        ua = hdrs.get("User-Agent") or hdrs.get("user-agent") or DEFAULT_USER_AGENT
    out: Dict[str, str] = {
        "User-Agent": ua,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if "Referer" in used_attempt:
        ref = used_attempt["Referer"]
    else:
        ref = referer_url
    if ref:
        out["Referer"] = ref
    orig = used_attempt.get("Origin")
    if orig:
        out["Origin"] = orig
    return out


def _collect_cookies(
    client: Any,
    manifest_url: str,
    page_url: str,
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    cookies: Dict[str, str] = {}
    entries: List[Dict[str, Any]] = []

    export_entries_fn = getattr(client, "export_cookie_entries", None)
    if callable(export_entries_fn):
        try:
            raw_entries = export_entries_fn()
            if isinstance(raw_entries, list):
                entries = raw_entries
        except Exception:
            entries = []

    export_fn = getattr(client, "export_cookies", None)
    if not callable(export_fn):
        return cookies, entries
    try:
        page_cookies = export_fn(page_url) if page_url else {}
        manifest_cookies = export_fn(manifest_url) if manifest_url else {}
        if isinstance(page_cookies, dict) and page_cookies:
            mp = _safe_urlsplit(manifest_url)
            pp = _safe_urlsplit(page_url)
            mhost = (mp.hostname or "").lower() if mp else ""
            phost = (pp.hostname or "").lower() if pp else ""
            if mhost and phost and (
                mhost == phost
                or mhost.split(".")[-2:] == phost.split(".")[-2:]
                or mhost.split(".")[-3:] == phost.split(".")[-3:]
            ):
                cookies.update(page_cookies)
        if isinstance(manifest_cookies, dict):
            cookies.update(manifest_cookies)
    except Exception:
        return {}, entries
    return cookies, entries


def _is_non_hls_media_url(url: str) -> bool:
    parts = _safe_urlsplit(url)
    path_low = (parts.path if parts is not None else url.split("?", 1)[0]).lower()
    if ".m3u8" in url.lower() or path_low.endswith((".m3u8", ".m3u")):
        return False
    return path_low.endswith(OTHER_STREAM_EXTENSIONS)


def _is_dash_manifest_text(text: str) -> bool:
    head = (text or "").lstrip("\ufeff \t\r\n")[:2048].upper()
    return "<MPD" in head and (head.startswith("<MPD") or head.startswith("<?XML"))


async def _first_valid_stream(
    client: Any,
    urls: Sequence[str],
    *,
    referer_url: str = "",
) -> Tuple[Optional[str], str, Dict[str, str], str, str, List[str]]:
    """Validate candidates in rank order: prefer verified HLS, fall back to verified direct/DASH.

    Returns ``(url, detail, winning_headers, stream_type, manifest_text, fallback_urls)``.
    """
    detail = ""
    probe_fn = getattr(client, "probe_media", None)
    can_probe = callable(probe_fn) and isinstance(client, HlsClient) and (
        "fetch_text" not in getattr(client, "__dict__", {})
        and "fetch_text_with_url" not in getattr(client, "__dict__", {})
    )

    fetcher = getattr(client, "fetch_text_with_url", None)
    if not callable(fetcher):
        fetcher = getattr(client, "fetch_text", None)
    supports_headers = _supports_kwarg(fetcher, "headers")

    best_fallback: Optional[Tuple[str, Dict[str, str], str, str]] = None
    fallback_urls: List[str] = []

    for idx_url, url in enumerate(urls):
        short = redact_url(url)
        is_direct_hint = _is_non_hls_media_url(url)
        attempts = _header_attempts(referer_url, url) if supports_headers else [{}]

        if is_direct_hint and not can_probe:
            detail = f"skipped non-HLS candidate ({short})"
            print(f"[HLS] Candidate skipped ({short}): non-HLS media")
            continue

        for idx, req_hdrs in enumerate(attempts):
            try:
                if is_direct_hint and can_probe:
                    probe = await probe_fn(url, headers=req_hdrs if supports_headers else None)
                    winning_hdrs = _build_winning_headers(client, referer_url, req_hdrs)
                    final_u = probe.url or url
                    if probe.stream_type == "hls" and probe.text_preview:
                        remaining = [u for u in urls[idx_url + 1:] if u != final_u]
                        return final_u, "", winning_hdrs, "hls", probe.text_preview, remaining
                    if probe.stream_type in ("direct", "dash"):
                        if best_fallback is None:
                            best_fallback = (final_u, winning_hdrs, probe.stream_type, probe.text_preview)
                        elif final_u not in fallback_urls:
                            fallback_urls.append(final_u)
                        break
                    detail = f"{short} is not a video stream ({probe.content_type or 'unknown'})"
                    break

                text, final_url = await _fetch_text_compat(
                    client, url, headers=req_hdrs if supports_headers else None
                )
            except WarpProxyError:
                raise
            except HlsHttpError as e:
                detail = f"{type(e).__name__}: {e}"
                if e.status in (401, 403) and idx + 1 < len(attempts):
                    continue
                print(f"[HLS] Candidate rejected ({short}): {detail}")
                break
            except ValueError as ve:
                # HlsHttpError is handled above; here ValueError means non-playlist Content-Type or size.
                if can_probe and "Non-playlist Content-Type" in str(ve):
                    try:
                        probe = await probe_fn(url, headers=req_hdrs if supports_headers else None)
                        if probe.stream_type in ("direct", "dash"):
                            winning_hdrs = _build_winning_headers(client, referer_url, req_hdrs)
                            final_u = probe.url or url
                            if best_fallback is None:
                                best_fallback = (final_u, winning_hdrs, probe.stream_type, probe.text_preview)
                            elif final_u not in fallback_urls:
                                fallback_urls.append(final_u)
                            break
                    except WarpProxyError:
                        raise
                    except Exception:
                        pass
                detail = f"{type(ve).__name__}: {ve}"
                print(f"[HLS] Candidate rejected ({short}): {detail}")
                break
            except Exception as e:
                detail = f"{type(e).__name__}: {e}"
                status = getattr(e, "status", None)
                if status in (401, 403) and idx + 1 < len(attempts):
                    continue
                print(f"[HLS] Candidate rejected ({short}): {detail}")
                break

            stripped = text.lstrip("\ufeff \t\r\n")
            if is_hls_playlist(stripped) or stripped[:7].upper() == "#EXTM3U":
                winning_hdrs = _build_winning_headers(client, referer_url, req_hdrs)
                final_u = final_url or url
                remaining = [u for u in urls[idx_url + 1:] if u != final_u]
                if best_fallback and best_fallback[0] not in remaining and best_fallback[0] != final_u:
                    remaining.append(best_fallback[0])
                return final_u, "", winning_hdrs, "hls", text, remaining

            if _is_dash_manifest_text(stripped):
                winning_hdrs = _build_winning_headers(client, referer_url, req_hdrs)
                final_u = final_url or url
                if best_fallback is None:
                    best_fallback = (final_u, winning_hdrs, "dash", text)
                elif final_u not in fallback_urls:
                    fallback_urls.append(final_u)
                break

            detail = f"{short} is not a playlist ({len(text)} B)"
            print(f"[HLS] Candidate rejected ({detail})")
            break

    if best_fallback is not None:
        fb_url, fb_hdrs, fb_type, fb_text = best_fallback
        return fb_url, "", fb_hdrs, fb_type, fb_text, fallback_urls

    return None, detail, {}, "hls", "", []


async def _first_valid(
    client: Any,
    urls: Sequence[str],
    *,
    referer_url: str = "",
) -> Tuple[Optional[str], str, Dict[str, str]]:
    """Backward-compatible 3-tuple wrapper around :func:`_first_valid_stream`."""
    url, detail, winning_hdrs, _stype, _mtext, _fallbacks = await _first_valid_stream(
        client, urls, referer_url=referer_url
    )
    return url, detail, winning_hdrs


async def resolve_stream_from_page(
    client,
    page_url: str,
    *,
    max_bytes: int = HLS_PAGE_MAX_BYTES,
    limit: int = HLS_PAGE_MAX_CANDIDATES,
    follow_embed: bool = HLS_PAGE_FOLLOW_EMBED,
    max_embed_depth: Optional[int] = None,
) -> PageScan:
    """Load *page_url* and return the stream a real player would use.

    Handles Tier 0 direct-stream responses (extensionless shortlinks returning
    ``#EXTM3U``, ``<MPD``, or inline ``video/*``), Tier 1 static HTML + JS
    deobfuscation (Dean Edwards ``p,a,c,k,e,d``, VOE, base64/atob/hex/unicode,
    JSON-LD, OpenGraph), and multi-hop iframe/redirect chains up to
    ``HLS_PAGE_EMBED_DEPTH`` (default 3).
    """
    try:
        probe = await _fetch_page_compat(client, page_url, max_bytes=max_bytes)
    except WarpProxyError:
        raise
    except Exception as e:
        return PageScan(
            page_url=page_url,
            reason="error",
            detail=f"{type(e).__name__}: {e}",
        )

    final_page_url = probe.final_url or page_url

    # Tier 0: The submitted URL itself directly returned a stream (shortlink / extensionless endpoint).
    if probe.kind in ("hls", "dash", "direct"):
        winning_hdrs = _build_winning_headers(client, final_page_url, {"Referer": None, "Origin": None})
        cookies, cookie_entries = _collect_cookies(client, final_page_url, final_page_url)
        return PageScan(
            url=final_page_url,
            page_url=final_page_url,
            reason="ok",
            candidates=1,
            headers=winning_hdrs,
            cookies=cookies,
            stream_type=probe.kind,
            manifest_text=probe.text if probe.kind in ("hls", "dash") else "",
            cookie_entries=cookie_entries,
        )

    if probe.kind == "not-a-page":
        return PageScan(page_url=final_page_url, reason="not-a-page")

    html = probe.text
    root_title = extract_title(html)
    root_player_page = looks_like_player_page(html)

    candidates = find_manifests(html, final_page_url, limit=limit)
    detail = ""
    if candidates:
        url, detail, winning_hdrs, stype, mtext, fallbacks = await _first_valid_stream(
            client, candidates, referer_url=final_page_url
        )
        if url:
            cookies, cookie_entries = _collect_cookies(client, url, final_page_url)
            return PageScan(
                url=url,
                page_url=final_page_url,
                reason="ok",
                candidates=len(candidates),
                player_page=root_player_page,
                headers=winning_hdrs,
                cookies=cookies,
                stream_type=stype,
                title=root_title,
                manifest_text=mtext,
                cookie_entries=cookie_entries,
                fallback_urls=fallbacks,
            )

    if follow_embed:
        depth_cap = (
            int(max_embed_depth)
            if max_embed_depth is not None
            else max(1, int(getattr(_app_config, "HLS_PAGE_EMBED_DEPTH", 3) or 3))
        )
        visited: set[str] = {page_url.split("#", 1)[0], final_page_url.split("#", 1)[0]}
        # Queue of (embed_url, referer_url, depth)
        queue: deque[Tuple[str, str, int]] = deque()
        for embed in find_embed_urls(html, final_page_url, limit=4):
            key = embed.split("#", 1)[0]
            if key not in visited:
                visited.add(key)
                queue.append((embed, final_page_url, 1))

        fetches_done = 0
        max_embed_fetches = 6
        any_player_page = root_player_page

        while queue and fetches_done < max_embed_fetches:
            embed, ref_url, depth = queue.popleft()
            fetches_done += 1
            try:
                sub_probe = await _fetch_page_compat(
                    client,
                    embed,
                    max_bytes=max_bytes,
                    headers={"Referer": ref_url},
                )
            except WarpProxyError:
                raise
            except Exception as e:
                print(f"[HLS] Embed hop failed ({redact_url(embed)}): {e}")
                continue

            embed_final_url = sub_probe.final_url or embed
            visited.add(embed_final_url.split("#", 1)[0])

            if sub_probe.kind in ("hls", "dash", "direct"):
                winning_hdrs = _build_winning_headers(
                    client, ref_url, {"Referer": ref_url, "Origin": _origin_of(ref_url) or None}
                )
                cookies, cookie_entries = _collect_cookies(client, embed_final_url, ref_url)
                return PageScan(
                    url=embed_final_url,
                    page_url=ref_url,
                    reason="ok",
                    candidates=1,
                    player_page=True,
                    headers=winning_hdrs,
                    cookies=cookies,
                    stream_type=sub_probe.kind,
                    title=root_title,
                    manifest_text=sub_probe.text if sub_probe.kind in ("hls", "dash") else "",
                    cookie_entries=cookie_entries,
                )

            if sub_probe.kind != "page" or not sub_probe.text:
                continue

            sub = sub_probe.text
            sub_title = root_title or extract_title(sub)
            if looks_like_player_page(sub):
                any_player_page = True

            sub_candidates = find_manifests(sub, embed_final_url, limit=limit)
            if sub_candidates:
                url, sub_detail, winning_hdrs, stype, mtext, fallbacks = await _first_valid_stream(
                    client, sub_candidates, referer_url=embed_final_url
                )
                if url:
                    cookies, cookie_entries = _collect_cookies(client, url, embed_final_url)
                    return PageScan(
                        url=url,
                        page_url=embed_final_url,
                        reason="ok",
                        candidates=len(sub_candidates),
                        player_page=True,
                        headers=winning_hdrs,
                        cookies=cookies,
                        stream_type=stype,
                        title=sub_title,
                        manifest_text=mtext,
                        cookie_entries=cookie_entries,
                        fallback_urls=fallbacks,
                    )
                detail = sub_detail

            if depth < depth_cap:
                for next_embed in find_embed_urls(sub, embed_final_url, limit=3):
                    key = next_embed.split("#", 1)[0]
                    if key not in visited:
                        visited.add(key)
                        queue.append((next_embed, embed_final_url, depth + 1))

        root_player_page = any_player_page

    return PageScan(
        page_url=final_page_url,
        reason="no-manifest" if not candidates else "no-valid-manifest",
        candidates=len(candidates),
        player_page=root_player_page,
        detail=detail,
        title=root_title,
    )


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import hashlib
    import hmac
    import sys

    from aiohttp import web

    import app.hls.client as _client_mod
    _client_mod.HLS_DISABLE_PROXY = True

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    BASE = "https://site.example.com/watch/135123-fc2"

    print("--- find_manifests: encodings & deobfuscation ---")
    escaped = r'<script>var s = "https:\/\/cdn.example.com\/hls\/master.m3u8?t=1";</script>'
    _check("JS-escaped slashes",
           find_manifests(escaped, BASE) == ["https://cdn.example.com/hls/master.m3u8?t=1"],
           str(find_manifests(escaped, BASE)))

    hex_escaped = r'<script>var s = "https:\x3a\x2f\x2fcdn.example.com\x2fhls\x2fhex.m3u8?a=1\x26b=2";</script>'
    _check("JS hex-escaped slashes",
           find_manifests(hex_escaped, BASE) == ["https://cdn.example.com/hls/hex.m3u8?a=1&b=2"],
           str(find_manifests(hex_escaped, BASE)))

    pct_escaped = '<iframe src="https://p.example.com/?url=https%3A%2F%2Fcdn.example.com%2Fpct%2Fmaster.m3u8%3Ft%3D7&autoplay=1"></iframe>'
    _check("percent-encoded URL in attribute",
           find_manifests(pct_escaped, BASE) == ["https://cdn.example.com/pct/master.m3u8?t=7"],
           str(find_manifests(pct_escaped, BASE)))

    entity = '<video src="https://cdn.example.com/a/master.m3u8?t=1&amp;e=2"></video>'
    _check("HTML entity in query",
           find_manifests(entity, BASE) == ["https://cdn.example.com/a/master.m3u8?t=1&e=2"],
           str(find_manifests(entity, BASE)))

    proto = '<div data-src="//cdn.example.com/b/index.m3u8"></div>'
    _check("protocol-relative -> page scheme",
           find_manifests(proto, BASE) == ["https://cdn.example.com/b/index.m3u8"],
           str(find_manifests(proto, BASE)))

    rel = '<source src="/hls/v1/master.m3u8">'
    _check("page-relative -> page origin",
           find_manifests(rel, BASE) == ["https://site.example.com/hls/v1/master.m3u8"],
           str(find_manifests(rel, BASE)))

    _check("JSON key",
           find_manifests('{"file":"https://cdn.example.com/j/master.m3u8?t=9"}', BASE)
           == ["https://cdn.example.com/j/master.m3u8?t=9"])

    packed_js = (
        "<script>eval(function(p,a,c,k,e,d){while(c--)if(k[c])p=p.replace("
        "new RegExp('\\\\b'+c.toString(a)+'\\\\b','g'),k[c]);return p;}"
        "('0.1({2:[{3:\"4://5.6.7/8/9.a?b=c\"}]});',16,13,"
        "'jwplayer|setup|sources|file|https|cdn|filemoon|example|hls|master|m3u8|t|packed'.split('|'),0,{}))</script>"
    )
    _check("Dean Edwards p,a,c,k,e,d unpacked",
           find_manifests(packed_js, BASE) == ["https://cdn.filemoon.example/hls/master.m3u8?t=packed"],
           str(find_manifests(packed_js, BASE)))

    atob_js = "<script>var u = atob('aHR0cHM6Ly9jZG4uZXhhbXBsZS5jb20vYXRvYi9tYXN0ZXIubTN1OA==');</script>"
    _check("atob() decoded",
           find_manifests(atob_js, BASE) == ["https://cdn.example.com/atob/master.m3u8"],
           str(find_manifests(atob_js, BASE)))

    concat_js = '<script>var u = "https://" + "cdn.example.com" + "/concat/master.m3u8?k=1";</script>'
    _check("JS string concatenation decoded",
           find_manifests(concat_js, BASE)[0] == "https://cdn.example.com/concat/master.m3u8?k=1",
           str(find_manifests(concat_js, BASE)))

    char_url = "https://cdn.example.com/fcc/master.m3u8"
    fcc_js = f"<script>var u = String.fromCharCode({','.join(str(ord(c)) for c in char_url)});</script>"
    _check("String.fromCharCode decoded",
           find_manifests(fcc_js, BASE) == [char_url],
           str(find_manifests(fcc_js, BASE)))

    extless_js = '<script>jwplayer().setup({file: "https://cdn.example.com/stream/abc123?token=xyz"});</script>'
    _check("extensionless player config URL captured",
           find_manifests(extless_js, BASE) == ["https://cdn.example.com/stream/abc123?token=xyz"],
           str(find_manifests(extless_js, BASE)))

    jsonld = '<script type="application/ld+json">{"@type":"VideoObject","name":"My Great Clip","contentUrl":"https://cdn.example.com/ld/master.m3u8"}</script>'
    _check("JSON-LD contentUrl + title extracted",
           find_manifests(jsonld, BASE) == ["https://cdn.example.com/ld/master.m3u8"]
           and extract_title(jsonld) == "My Great Clip",
           f"{find_manifests(jsonld, BASE)} / {extract_title(jsonld)!r}")

    import base64 as _b64
    blob = _b64.b64encode(
        b'{"src":"https:\\/\\/cdn.example.com\\/b64\\/master.m3u8?t=8","poster":"cover.jpg"}'
    ).decode()
    _check("base64 config blob with escaped slashes",
           find_manifests(f"<script>window.cfg='{blob}';</script>", BASE)
           == ["https://cdn.example.com/b64/master.m3u8?t=8"],
           str(find_manifests(f"<script>window.cfg='{blob}';</script>", BASE)))

    print("--- find_manifests: ranking / limits / negatives ---")
    mixed = (
        '<video poster="https://cdn.example.com/p/1.jpg"></video>'
        '<source src="https://cdn.example.com/x/index.m3u8?t=2">'
        '<a href="https://cdn.example.com/x/clip.mp4">dl</a>'
        '<script>load("https://cdn.example.com/x/master.m3u8?t=1")</script>'
    )
    order = find_manifests(mixed, BASE)
    _check("master.m3u8 ranked first",
           order[0] == "https://cdn.example.com/x/master.m3u8?t=1", str(order))
    _check("variant second, .mp4 last",
           order[1] == "https://cdn.example.com/x/index.m3u8?t=2"
           and order[-1].endswith("clip.mp4"), str(order))
    _check("poster image ignored", all("1.jpg" not in u for u in order), str(order))
    _check("limit honoured", len(find_manifests(mixed, BASE, limit=2)) == 2)
    ad_vs_real = (
        '<script>vast="https://ads.example.com/preroll/master.m3u8?t=1";'
        'main="https://cdn.example.com/real/index.m3u8?t=2";</script>'
    )
    _check("ad manifest ranked behind real manifest",
           find_manifests(ad_vs_real, BASE)[0] == "https://cdn.example.com/real/index.m3u8?t=2",
           str(find_manifests(ad_vs_real, BASE)))
    _check("trailing CSS/JSON junk stripped",
           find_manifests("background:url(https://cdn.example.com/y/master.m3u8?t=3);", BASE)
           == ["https://cdn.example.com/y/master.m3u8?t=3"],
           str(find_manifests(
               "background:url(https://cdn.example.com/y/master.m3u8?t=3);", BASE)))
    _check("duplicate URL once",
           len(find_manifests('a "https://cdn.example.com/z/master.m3u8?t=4" '
                              'b "https://cdn.example.com/z/master.m3u8?t=4"', BASE)) == 1)
    _check("no manifest -> []", find_manifests("<html><body>hello</body></html>", BASE) == [])
    _check("empty page -> []", find_manifests("", BASE) == [])

    print("--- embeds / redirects / player detection ---")
    embeds = find_embed_urls(
        '<iframe src="https://doubleclick.net/banner.html"></iframe>'
        '<iframe src="https://challenges.cloudflare.com/turnstile"></iframe>'
        '<iframe data-src="https://site.example.com/embed/player-123"></iframe>', BASE)
    _check("ad & captcha filtered, embed ranked first",
           embeds == ["https://site.example.com/embed/player-123"],
           str(embeds))
    js_redir = "<script>window.location.href = 'https://mirror.example.com/e/abc99';</script>"
    _check("JS window.location redirect discovered",
           find_embed_urls(js_redir, BASE) == ["https://mirror.example.com/e/abc99"],
           str(find_embed_urls(js_redir, BASE)))
    _check("player page detected",
           looks_like_player_page('<script src="hls.js"></script><video id="v">'))
    _check("plain page not a player page",
           not looks_like_player_page("<html><body><p>blog post</p></body></html>"))

    print("--- hostile page junk never aborts the scan ---")
    junk_page = (
        "<html><head><script>var tpl = `//${cdnHost}[${videoId}]/v`;"
        "var re = /a[b]/; var x = 'https://host]x/path/master.m3u8';</script>"
        "</head><body>"
        '<script>var src = "https://cdn.example.com/hls/master.m3u8?t=real";</script>'
        "</body></html>"
    )
    junk_base = "https://jamesbornmain.com/e/upnuod4v9rr3"
    _check("template-literal junk skipped, real manifest still found",
           find_manifests(junk_page, junk_base)
           == ["https://cdn.example.com/hls/master.m3u8?t=real"],
           str(find_manifests(junk_page, junk_base)))
    _check("unmatched brackets dropped (no ValueError)",
           find_manifests("x //a[b]c y //[abc z", BASE) == [],
           str(find_manifests("x //a[b]c y //[abc z", BASE)))
    _check("sole closing bracket dropped",
           find_manifests('<a href="https://host]x/path/master.m3u8">a</a>', BASE) == [],
           str(find_manifests('<a href="https://host]x/path/master.m3u8">a</a>', BASE)))
    _check("malformed base URL tolerated (manifests)",
           find_manifests('<source src="/hls/master.m3u8">', "https://[x/e/1") == [],
           str(find_manifests('<source src="/hls/master.m3u8">', "https://[x/e/1")))
    _check("malformed base URL tolerated (embeds)",
           find_embed_urls('<iframe src="/embed/1"></iframe>', "https://[x/e/1") == [],
           str(find_embed_urls('<iframe src="/embed/1"></iframe>', "https://[x/e/1")))

    print("--- page scan against loopback server (Tier 0 + Tier 1 + multi-hop + direct MP4) ---")

    SECRET = "s3cr3t"
    FOREIGN_UA = "ForeignBrowser/1.0"

    def token_for(user_agent: str) -> str:
        return hmac.new(SECRET.encode(), (user_agent or "").encode(),
                        hashlib.sha256).hexdigest()[:16]

    async def _run_scan_checks() -> None:
        async def page(request):
            ua = request.headers.get("User-Agent", "")
            body = (
                "<html><head><title>Awesome Sample Video</title></head><body>"
                '<video id="v"></video><script src="/static/hls.js"></script>'
                '<script>var src = "http:\\/\\/127.0.0.1:%d\\/media\\/master.m3u8'
                '?t=%s";</script></body></html>' % (request.url.port, token_for(ua))
            )
            resp = web.Response(text=body, content_type="text/html")
            resp.set_cookie("cdn_sess", "tok_page_99", path="/")
            return resp

        async def master(request):
            token = token_for(request.headers.get("User-Agent", ""))
            if request.query.get("t") != token:
                return web.Response(status=403, text="forbidden")
            body = ("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\n"
                    f"index.m3u8?t={token}\n")
            return web.Response(text=body,
                                content_type="application/vnd.apple.mpegurl")

        async def disguised_manifest(request):
            return web.Response(
                body=b"#EXTM3U\n#EXTINF:4,\nseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="image/png",
            )

        async def extless_page(request):
            return web.Response(
                text='<html><body><script>jwplayer().setup({file: "/stream/hidden-pl?tok=1"});</script></body></html>',
                content_type="text/html",
            )

        async def mp4_page(request):
            return web.Response(
                text='<html><head><meta property="og:title" content="Direct Clip"></head><body><video src="/media/clip.mp4"></video></body></html>',
                content_type="text/html",
            )

        async def mp4_file(request):
            # Minimal ISO BMFF ftyp header
            body = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2" + (b"\x00" * 1024)
            return web.Response(body=body, content_type="video/mp4")

        async def real_file(request):
            return web.Response(
                body=b"PK\x03\x04" + b"0" * 512, content_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="x.zip"'})

        async def hop1(request):
            return web.Response(
                text='<html><head><title>Nested Show</title></head><body><iframe src="/embed/hop2"></iframe></body></html>',
                content_type="text/html",
            )

        async def hop2(request):
            return web.Response(
                text='<html><body><script>window.location.replace("/embed/hop3");</script></body></html>',
                content_type="text/html",
            )

        async def hop3(request):
            return web.Response(
                text='<html><body><source src="/stream/hidden-pl"></body></html>',
                content_type="text/html",
            )

        async def error_page(request):
            return web.Response(status=403, text="blocked")

        app = web.Application()
        app.router.add_get("/page", page)
        app.router.add_get("/media/master.m3u8", master)
        app.router.add_get("/stream/hidden-pl", disguised_manifest)
        app.router.add_get("/extless-page", extless_page)
        app.router.add_get("/mp4-page", mp4_page)
        app.router.add_get("/media/clip.mp4", mp4_file)
        app.router.add_get("/file.zip", real_file)
        app.router.add_get("/hop1", hop1)
        app.router.add_get("/embed/hop2", hop2)
        app.router.add_get("/embed/hop3", hop3)
        app.router.add_get("/blocked", error_page)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        client: Optional[HlsClient] = None

        try:
            client = HlsClient(headers={"User-Agent": "BotFetcher/1.0"})

            scan = await resolve_stream_from_page(client, f"{base}/page")
            _check("page scan found manifest + title + manifest_text + cookies",
                   scan.reason == "ok"
                   and bool(scan.url)
                   and scan.title == "Awesome Sample Video"
                   and "#EXTM3U" in scan.manifest_text
                   and scan.cookies.get("cdn_sess") == "tok_page_99",
                   str(scan))

            scan_t0 = await resolve_stream_from_page(
                client, f"{base}/media/master.m3u8?t={token_for('BotFetcher/1.0')}"
            )
            _check("Tier 0 direct mpegurl endpoint resolved directly as HLS",
                   scan_t0.reason == "ok" and scan_t0.stream_type == "hls" and "#EXTM3U" in scan_t0.manifest_text,
                   str(scan_t0))

            scan_extless = await resolve_stream_from_page(client, f"{base}/extless-page")
            _check("extensionless disguised image/png #EXTM3U playlist resolved",
                   scan_extless.reason == "ok"
                   and scan_extless.stream_type == "hls"
                   and "/stream/hidden-pl" in (scan_extless.url or ""),
                   str(scan_extless))

            scan_mp4 = await resolve_stream_from_page(client, f"{base}/mp4-page")
            _check("direct MP4 fallback page resolved with stream_type='direct' & title",
                   scan_mp4.reason == "ok"
                   and scan_mp4.stream_type == "direct"
                   and scan_mp4.title == "Direct Clip"
                   and (scan_mp4.url or "").endswith("/media/clip.mp4"),
                   str(scan_mp4))

            scan_hops = await resolve_stream_from_page(client, f"{base}/hop1")
            _check("3-hop iframe + JS redirect chain resolved",
                   scan_hops.reason == "ok"
                   and scan_hops.stream_type == "hls"
                   and scan_hops.title == "Nested Show",
                   str(scan_hops))

            scan_zip = await resolve_stream_from_page(client, f"{base}/file.zip")
            _check("attachment zip reports not-a-page",
                   scan_zip.url is None and scan_zip.reason == "not-a-page",
                   str(scan_zip))

            scan_err = await resolve_stream_from_page(client, f"{base}/blocked")
            _check("HTTP error page reports error",
                   scan_err.url is None and scan_err.reason == "error",
                   str(scan_err))
        finally:
            if client is not None:
                await client.close()
            await runner.cleanup()

    asyncio.run(_run_scan_checks())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
