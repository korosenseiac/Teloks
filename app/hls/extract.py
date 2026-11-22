"""
hls/extract.py — Find the real manifest URL inside a video *page*.

Why this exists
---------------
Some CDNs only accept the playlist URL that *they* minted for the client that
loaded the page. The token in the query string carries the requester's identity
(``i=103.179`` = IP prefix, ``asn=56231`` = the viewer's ISP, ``s``/``e`` = mint
time and TTL), and the master playlist body then carries child URLs signed for
that same client. Copying such an URL out of a browser therefore only works on
the machine that minted it: this bot's VPS — or its proxy — gets HTTP 403 for
the variant and for every segment, whatever headers it sends.

The cure is to stop reusing somebody else's signed URL: load the page ourselves,
let the site mint a playlist URL for OUR egress, and use that. Because the token
is issued per requester, the child URLs inside the master are then signed for us
too.

Everything here is dependency-free and bounded: the page is read up to
``HLS_PAGE_MAX_BYTES``, only a handful of candidates are returned, and every
candidate is verified with a real fetch (:func:`is_hls_playlist`) before it is
used — a dead, foreign-signed or DRM-only candidate is skipped instead of
starting a job that cannot finish.

Self-test (offline — only a loopback server is started, no network needed):

    python -m app.hls.extract
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import html as _html
import inspect
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import SplitResult, unquote, urljoin, urlsplit

from app.config import (
    HLS_PAGE_FOLLOW_EMBED,
    HLS_PAGE_MAX_BYTES,
    HLS_PAGE_MAX_CANDIDATES,
)
from app.hls.client import (
    DEFAULT_USER_AGENT,
    HlsHttpError,
    default_headers,
    redact_url,
)
from app.hls.playlist import is_hls_playlist
from app.warp import WarpProxyError

# ---------------------------------------------------------------------------
# What counts as a manifest
# ---------------------------------------------------------------------------

#: Playlists this pipeline can actually download (HLS).
HLS_EXTENSIONS = (".m3u8", ".m3u", ".m3u8?")
#: Other player sources: kept as candidates (they still mean "this is a player
#: page") but ranked last, because the downloader only understands HLS.
OTHER_STREAM_EXTENSIONS = (".mpd", ".mp4", ".webm", ".flv")

#: Trailing punctuation that web pages glue onto URLs: JSON/JS string ends,
#: CSS ``url(...)`` closers, prose commas and sentence dots.
_TRAILING_JUNK = " \"'`,;)]}.!?\\"

#: Characters that can never appear raw inside a URL we would fetch. Brackets and
#: braces matter most: a JS template literal such as ``//${host}[${id}]`` reaches
#: ``urlsplit()`` as an unmatched ``[`` and used to abort the whole page scan with
#: "Invalid IPv6 URL" — even when the real manifest was sitting further down the
#: page.
_URL_STOP = r"""\s\"'<>\\`\[\]{}$"""

#: Absolute and protocol-relative URLs anywhere in the page.
_ABS_URL = re.compile(r"""(?:https?:)?//[^""" + _URL_STOP + r"""]+""", re.IGNORECASE)

#: URL-encoded absolute URLs inside query parameters or attributes.
_PCT_URL = re.compile(r"""https?%3A%2F%2F[^""" + _URL_STOP + r"""&]+""", re.IGNORECASE)

#: JS/JSON player config properties pointing to an m3u8 URL even if ``.m3u8``
#: sits in a query param or non-terminal path segment.
_PLAYER_URL_KV = re.compile(
    r"""(?:["']?(?:file|hls|hlsUrl|stream|streamUrl|manifest|playlist)["']?\s*[:=]|["'](?:src|source|url)["']?\s*:)\s*"""
    r"""(?:"([^"\\<>]+\.m3u8?[^"\\<>]*)"|'([^'\\<>]+\.m3u8?[^'\\<>]*)')""",
    re.IGNORECASE,
)

#: Page-relative manifest paths, e.g. ``/hls/1234/master.m3u8``.
#: The lookbehind stops it from re-matching the path part of a full URL.
_REL_MANIFEST = re.compile(
    r"""(?<![\w/])(/[^""" + _URL_STOP + r"""()]*\.(?:m3u8?|mpd|mp4)[^"""
    + _URL_STOP + r"""()]*)""",
    re.IGNORECASE,
)

#: ``<iframe src=...>`` / ``<embed src=...>`` (plus ``data-src=`` lazy embeds).
_EMBED = re.compile(
    r"""<(?:iframe|embed)\b[^>]*?\b(?:data-)?src\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)

#: Base64 blobs (some players ship the manifest URL encoded). 40 chars catches
#: short URLs (~30 bytes) while keeping the search cheap.
_B64_BLOB = re.compile(r"""[A-Za-z0-9+/_-]{40,}={0,2}""")

#: Markers that a page embeds a player (used only for the user-facing hint).
_PLAYER_MARKERS = (
    "m3u8", "hls.js", "hlsjs", "jwplayer", "videojs", "plyr", "shaka",
    "<video", "player", "<iframe",
)

#: Known ad/tracker hosts to ignore for embed hops and deprioritize for streams.
_AD_HOST_PARTS = (
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adsbygoogle", "adservice", "popads", "popcash", "propellerads",
    "exoclick", "juicyads", "trafficjunky", "adsterra", "hilltopads",
    "facebook.net", "criteo", "taboola", "outbrain", "openfpcdn",
)

_AD_STREAM_MARKERS = (
    "/vast", "preroll", "/ads/", "adserver", "doubleclick", "googlesyndication",
    "preview.", "/preview/", "trailer", "teaser",
)


def _unescape(text: str) -> str:
    """Undo the encodings sites use when they embed a URL.

    HTML entities (``&amp;``), JSON/JS escaped slashes (``\\/``), hex escapes
    (``\\x2f``) and escaped ``\\uXXXX`` characters for ``/ = & ? : %``.
    """
    text = text.replace("\\\\/", "/").replace("\\/", "/")
    for code, char in (
        ("002F", "/"), ("002f", "/"),
        ("003D", "="), ("003d", "="),
        ("0026", "&"),
        ("003F", "?"), ("003f", "?"),
        ("003A", ":"), ("003a", ":"),
        ("0025", "%"),
    ):
        text = text.replace(f"\\u{code}", char)
    for code, char in (
        ("2F", "/"), ("2f", "/"),
        ("3D", "="), ("3d", "="),
        ("26", "&"),
        ("3F", "?"), ("3f", "?"),
        ("3A", ":"), ("3a", ":"),
        ("25", "%"),
    ):
        text = text.replace(f"\\x{code}", char)
    return _html.unescape(text)


def _safe_urlsplit(url: str) -> Optional[SplitResult]:
    """``urlsplit`` that never raises on page junk.

    Python's parser rejects an unmatched ``[`` / ``]`` in the host — which is
    exactly what a JS template literal (``//${host}[${id}]``) or a mangled URL
    looks like — and a broken page must never take a job down, so every parse in
    this module goes through here. ``None`` means "not a URL we can use".
    """
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
    if url.lower().startswith(("http://", "https://")):
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


def _rank(url: str) -> Tuple[int, int, int, int, int]:
    """Sort key: HLS before other sources, non-ad first, master before variant, token first.

    Lower is better. A ``master.m3u8`` wins over an ``index.m3u8``, an `m3u8`
    with a query string (the freshly minted, signed one) wins over a bare path,
    and ``.mpd``/``.mp4`` come last because the pipeline cannot download them.
    """
    parts = _safe_urlsplit(url)
    path = (parts.path if parts is not None else "").lower()
    host = (parts.hostname if parts is not None else "") or ""
    low_full = url.lower()
    is_hls = 0 if (path.endswith(HLS_EXTENSIONS) or ".m3u8" in low_full) else 1
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
    return (is_hls, is_junk, is_master, has_query, len(url))


def _decoded_blobs(text: str, limit: int = 12) -> List[str]:
    """Text of up to *limit* base64 blobs that decode to something URL-ish.

    Best-effort and heavily bounded: players that store the source in a base64
    config are covered without turning the scan into a decoder for the page.
    """
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
        if "." in decoded and ("m3u8" in decoded.lower() or "http" in decoded.lower()):
            out.append(decoded)
    return out


def find_manifests(html: str, base_url: str, limit: int = 3) -> List[str]:
    """Every stream/manifest URL referenced by *html*, best candidate first.

    Handles the encodings seen in the wild: plain URLs in attributes or JS
    strings, escaped slashes (``https:\\/\\/…``), ``\\x2f`` hex escapes,
    ``&amp;`` entities, percent-encoded URLs, protocol-relative and
    page-relative paths, and base64 config blobs.
    """
    if not html:
        return []

    text = _unescape(html)
    found: List[str] = []
    seen = set()

    def _add(raw: str, *, allow_query_m3u8: bool = False) -> None:
        try:
            url = _clean_candidate(raw, base_url)
        except ValueError:
            # A hostile/broken page must never abort the scan; skip the junk.
            return
        if not url or url in seen or not _looks_like_stream(url, allow_query_m3u8=allow_query_m3u8):
            return
        seen.add(url)
        found.append(url)

    for match in _ABS_URL.finditer(text):
        _add(match.group(0))
    for match in _REL_MANIFEST.finditer(text):
        _add(match.group(1))
    for match in _PLAYER_URL_KV.finditer(text):
        raw_kv = match.group(1) or match.group(2) or ""
        if "%3a%2f%2f" in raw_kv.lower():
            continue
        _add(raw_kv, allow_query_m3u8=True)
    for match in _PCT_URL.finditer(text):
        _add(unquote(match.group(0)), allow_query_m3u8=True)
    for blob in _decoded_blobs(text):
        for match in _ABS_URL.finditer(blob):
            _add(match.group(0))
        for match in _REL_MANIFEST.finditer(blob):
            _add(match.group(1))
        for match in _PLAYER_URL_KV.finditer(blob):
            raw_kv = match.group(1) or match.group(2) or ""
            _add(raw_kv, allow_query_m3u8=True)

    found.sort(key=_rank)
    return found[:max(1, limit)]


def _is_ad_url(url: str) -> bool:
    parts = _safe_urlsplit(url)
    if parts is None:
        return True
    host = (parts.hostname or "").lower()
    return any(part in host for part in _AD_HOST_PARTS)


def find_embed_urls(html: str, base_url: str, limit: int = 2) -> List[str]:
    """Embedded player/iframe URLs (the second place a manifest likes to hide)."""
    if not html:
        return []

    text = _unescape(html)
    ranked: List[Tuple[int, int, str]] = []
    seen = set()
    for match in _EMBED.finditer(text):
        raw = match.group(1) or match.group(2) or match.group(3) or ""
        try:
            url = _clean_candidate(raw, base_url)
        except ValueError:
            continue
        if not url or url in seen or not url.lower().startswith(("http://", "https://")):
            continue
        low = url.lower()
        if low.endswith((".jpg", ".png", ".gif", ".webp", ".svg", ".css", ".js")):
            continue
        if _is_ad_url(url):
            continue
        seen.add(url)
        priority = 0 if any(k in low for k in ("embed", "player", "stream", "video")) else 1
        ranked.append((priority, len(url), url))

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
    """Outcome of looking for a manifest inside a page."""

    #: The manifest URL to hand to the downloader (None when nothing was found).
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
    #: Request headers that worked for the manifest (UA, Referer, Origin).
    headers: Dict[str, str] = field(default_factory=dict)
    #: Cookies captured during page/embed/manifest loading.
    cookies: Dict[str, str] = field(default_factory=dict)


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
) -> Optional[Tuple[str, str]]:
    fetch_page_fn = getattr(client, "fetch_page", None)
    if callable(fetch_page_fn):
        kwargs: Dict[str, Any] = {"max_bytes": max_bytes}
        if headers and _supports_kwarg(fetch_page_fn, "headers"):
            kwargs["headers"] = headers
        return await fetch_page_fn(url, **kwargs)

    fetch_text_fn = client.fetch_page_text
    kwargs = {"max_bytes": max_bytes}
    if headers and _supports_kwarg(fetch_text_fn, "headers"):
        kwargs["headers"] = headers
    html = await fetch_text_fn(url, **kwargs)
    if html is None:
        return None
    return html, url


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
    """Ordered browser-like header sets to validate a candidate playlist."""
    attempts: List[Dict[str, Optional[str]]] = []
    ref_origin = _origin_of(referer_url)
    cand_origin = _origin_of(candidate_url)

    if referer_url:
        # 1) Full document Referer + Origin (standard browser XHR from page/embed).
        first: Dict[str, Optional[str]] = {"Referer": referer_url}
        if ref_origin:
            first["Origin"] = ref_origin
        attempts.append(first)

        # 2) Origin-only Referer + Origin (Chrome default cross-origin policy:
        #    strict-origin-when-cross-origin strips path/query on Referer).
        if ref_origin and referer_url != f"{ref_origin}/":
            attempts.append({"Referer": f"{ref_origin}/", "Origin": ref_origin})

        # 3) Referer without Origin (some non-CORS or legacy servers reject Origin).
        attempts.append({"Referer": referer_url, "Origin": None})

    # 4) Stream's own origin as Referer (fallback for CDNs enforcing own-origin hotlink).
    if cand_origin and cand_origin != ref_origin:
        attempts.append({"Referer": f"{cand_origin}/", "Origin": None})

    if not attempts:
        attempts.append({})
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
    ref = used_attempt.get("Referer") or referer_url
    if ref:
        out["Referer"] = ref
    orig = used_attempt.get("Origin")
    if orig:
        out["Origin"] = orig
    return out


def _collect_cookies(client: Any, manifest_url: str, page_url: str) -> Dict[str, str]:
    export_fn = getattr(client, "export_cookies", None)
    if not callable(export_fn):
        return {}
    cookies: Dict[str, str] = {}
    try:
        page_cookies = export_fn(page_url) if page_url else {}
        manifest_cookies = export_fn(manifest_url) if manifest_url else {}
        if isinstance(page_cookies, dict) and page_cookies:
            # Share cookies when manifest & page share a parent domain.
            mp = _safe_urlsplit(manifest_url)
            pp = _safe_urlsplit(page_url)
            mhost = (mp.hostname or "").lower() if mp else ""
            phost = (pp.hostname or "").lower() if pp else ""
            if mhost and phost and mhost.split(".")[-2:] == phost.split(".")[-2:]:
                cookies.update(page_cookies)
        if isinstance(manifest_cookies, dict):
            cookies.update(manifest_cookies)
    except Exception:
        return {}
    return cookies


async def _first_valid(
    client: Any,
    urls: Sequence[str],
    *,
    referer_url: str = "",
) -> Tuple[Optional[str], str, Dict[str, str]]:
    """First candidate that really is an HLS playlist, plus why the rest failed.

    Returns ``(url, detail, winning_headers)``.
    """
    detail = ""
    for url in urls:
        short = redact_url(url)
        attempts = _header_attempts(referer_url, url)
        # If client doesn't support per-request headers (mock), run once.
        supports_headers = _supports_kwarg(
            getattr(client, "fetch_text_with_url", client.fetch_text), "headers"
        )
        if not supports_headers:
            attempts = [{}]

        for idx, req_hdrs in enumerate(attempts):
            try:
                text, final_url = await _fetch_text_compat(
                    client, url, headers=req_hdrs if supports_headers else None
                )
            except WarpProxyError:
                raise
            except Exception as e:
                detail = f"{type(e).__name__}: {e}"
                status = getattr(e, "status", None)
                if status in (401, 403) and idx + 1 < len(attempts):
                    continue
                print(f"[HLS] Candidate rejected ({short}): {detail}")
                break

            if is_hls_playlist(text.lstrip("\ufeff \t\r\n")):
                winning_hdrs = _build_winning_headers(client, referer_url, req_hdrs)
                return final_url or url, "", winning_hdrs

            detail = f"{short} is not a playlist ({len(text)} B)"
            print(f"[HLS] Candidate rejected ({detail})")
            break

    return None, detail, {}


async def resolve_stream_from_page(
    client,
    page_url: str,
    *,
    max_bytes: int = HLS_PAGE_MAX_BYTES,
    limit: int = HLS_PAGE_MAX_CANDIDATES,
    follow_embed: bool = HLS_PAGE_FOLLOW_EMBED,
) -> PageScan:
    """Load *page_url* and return the manifest a real player would use.

    The page is fetched with the same client — therefore the same proxy, UA and
    headers — as the download, so any URL the site mints is minted for us. Every
    candidate is verified before it is returned, so callers can trust
    ``scan.url``; ``scan.reason`` explains a miss.
    """
    try:
        fetched = await _fetch_page_compat(client, page_url, max_bytes=max_bytes)
    except WarpProxyError:
        raise
    except Exception as e:
        return PageScan(
            page_url=page_url,
            reason="error",
            detail=f"{type(e).__name__}: {e}",
        )
    if fetched is None:
        return PageScan(page_url=page_url, reason="not-a-page")

    html, final_page_url = fetched
    final_page_url = final_page_url or page_url

    candidates = find_manifests(html, final_page_url, limit=limit)
    detail = ""
    if candidates:
        url, detail, winning_hdrs = await _first_valid(
            client, candidates, referer_url=final_page_url
        )
        if url:
            cookies = _collect_cookies(client, url, final_page_url)
            return PageScan(
                url=url,
                page_url=final_page_url,
                reason="ok",
                candidates=len(candidates),
                headers=winning_hdrs,
                cookies=cookies,
            )

    if follow_embed:
        for embed in find_embed_urls(html, final_page_url):
            try:
                sub_fetched = await _fetch_page_compat(
                    client,
                    embed,
                    max_bytes=max_bytes,
                    headers={"Referer": final_page_url},
                )
            except WarpProxyError:
                raise
            except Exception as e:
                print(f"[HLS] Embed hop failed ({redact_url(embed)}): {e}")
                continue
            if not sub_fetched:
                continue
            sub, embed_final_url = sub_fetched
            embed_final_url = embed_final_url or embed
            sub_candidates = find_manifests(sub, embed_final_url, limit=limit)
            if not sub_candidates:
                continue
            url, sub_detail, winning_hdrs = await _first_valid(
                client, sub_candidates, referer_url=embed_final_url
            )
            if url:
                cookies = _collect_cookies(client, url, embed_final_url)
                return PageScan(
                    url=url,
                    page_url=embed_final_url,
                    reason="ok",
                    candidates=len(sub_candidates),
                    headers=winning_hdrs,
                    cookies=cookies,
                )
            detail = sub_detail

    return PageScan(
        page_url=final_page_url,
        reason="no-manifest" if not candidates else "no-valid-manifest",
        candidates=len(candidates),
        player_page=looks_like_player_page(html),
        detail=detail,
    )


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import hashlib
    import hmac
    import sys

    from aiohttp import web

    # The loopback checks below must never be routed through proxy.txt.
    import app.hls.client as _client_mod
    _client_mod.HLS_DISABLE_PROXY = True
    from app.hls.client import HlsClient

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    BASE = "https://site.example.com/watch/135123-fc2"

    print("--- find_manifests: encodings ---")
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

    import base64 as _b64
    blob = _b64.b64encode(
        b'{"src":"https://cdn.example.com/b64/master.m3u8?t=8","poster":"cover.jpg"}'
    ).decode()
    _check("base64 config blob",
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

    print("--- embeds / player detection ---")
    embeds = find_embed_urls(
        '<iframe src="https://doubleclick.net/banner.html"></iframe>'
        '<iframe src="https://ads.example.com/banner.html"></iframe>'
        '<iframe data-src="https://site.example.com/embed/player-123"></iframe>', BASE)
    _check("ad host filtered & embed/player host ranked first",
           embeds[0] == "https://site.example.com/embed/player-123"
           and all("doubleclick" not in u for u in embeds),
           str(embeds))
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

    print("--- page scan against a client-bound CDN (loopback) ---")

    SECRET = "s3cr3t"
    FOREIGN_UA = "ForeignBrowser/1.0"

    def token_for(user_agent: str) -> str:
        """Stand-in for the CDN token: bound to the client that asked for it."""
        return hmac.new(SECRET.encode(), (user_agent or "").encode(),
                        hashlib.sha256).hexdigest()[:16]

    async def _run_scan_checks() -> None:
        async def page(request):
            ua = request.headers.get("User-Agent", "")
            body = (
                "<html><head><title>Video</title></head><body>"
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

        async def media(request):
            token = token_for(request.headers.get("User-Agent", ""))
            if request.query.get("t") != token:
                return web.Response(status=403)
            return web.Response(
                text=f"#EXTM3U\n#EXTINF:4,\nseg0.ts?t={token}\n#EXT-X-ENDLIST\n")

        async def segment(request):
            if request.query.get("t") != token_for(request.headers.get("User-Agent", "")):
                return web.Response(status=403)
            return web.Response(body=b"\x47" * 376, content_type="video/mp2t")

        async def real_file(request):
            return web.Response(
                body=b"PK\x03\x04" + b"0" * 512, content_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="x.zip"'})

        async def page_bad_charset(request):
            return web.Response(
                text='<html><body><script>var src = "/media/master-cp1252.m3u8";'
                     "</script></body></html>",
                content_type="text/html")

        async def master_bad_charset(request):
            body = (b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\n"
                    b"# caf\xe9\nindex.m3u8\n")
            return web.Response(body=body,
                                content_type="application/vnd.apple.mpegurl")

        async def page_junk_candidate(request):
            return web.Response(
                text='<html><body><script>var src = "/media/notaplaylist.m3u8";'
                     "</script></body></html>",
                content_type="text/html")

        async def not_a_playlist(request):
            return web.Response(body=b"<html>caf\xe9 not a playlist</html>",
                                content_type="application/vnd.apple.mpegurl")

        async def redirect_page(request):
            raise web.HTTPFound("/embed-target/watch")

        async def embed_target(request):
            return web.Response(
                text='<html><body><iframe src="/player/frame-1"></iframe></body></html>',
                content_type="text/html",
            )

        async def player_frame(request):
            return web.Response(
                text='<html><body><script>var s = "stream.m3u8";</script>'
                     '<source src="/player/strict.m3u8"></body></html>',
                content_type="text/html",
            )

        async def strict_m3u8(request):
            ref = request.headers.get("Referer", "")
            orig = request.headers.get("Origin", "")
            if not ref.endswith("/player/frame-1") or not orig:
                return web.Response(status=403, text="bad referer/origin")
            return web.Response(
                text="#EXTM3U\n#EXTINF:4,\nseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def error_page(request):
            return web.Response(status=403, text="blocked")

        app = web.Application()
        app.router.add_get("/page", page)
        app.router.add_get("/media/master.m3u8", master)
        app.router.add_get("/media/index.m3u8", media)
        app.router.add_get("/media/seg0.ts", segment)
        app.router.add_get("/file.zip", real_file)
        app.router.add_get("/page-bad-charset", page_bad_charset)
        app.router.add_get("/media/master-cp1252.m3u8", master_bad_charset)
        app.router.add_get("/page-junk-candidate", page_junk_candidate)
        app.router.add_get("/media/notaplaylist.m3u8", not_a_playlist)
        app.router.add_get("/redir", redirect_page)
        app.router.add_get("/embed-target/watch", embed_target)
        app.router.add_get("/player/frame-1", player_frame)
        app.router.add_get("/player/strict.m3u8", strict_m3u8)
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
            foreign = f"{base}/media/master.m3u8?t={token_for(FOREIGN_UA)}"
            blocked = ""
            try:
                await client.fetch_text(foreign)
            except Exception as e:
                blocked = str(e)
            _check("foreign-signed URL rejected (403) like the real CDN",
                   "403" in blocked, blocked or "request unexpectedly succeeded")

            scan = await resolve_stream_from_page(client, f"{base}/page")
            _check("page scan found a manifest", bool(scan.url),
                   f"{scan.reason} {scan.detail}")
            _check("scan reason ok", scan.reason == "ok", scan.reason)
            _check("minted for us, not for the browser",
                   bool(scan.url) and token_for(FOREIGN_UA) not in scan.url,
                   str(scan.url))
            _check("cookies captured on PageScan",
                   scan.cookies.get("cdn_sess") == "tok_page_99",
                   str(scan.cookies))
            _check("headers captured on PageScan",
                   "Referer" in scan.headers and "Origin" in scan.headers,
                   str(scan.headers))

            master_text = await client.fetch_text(scan.url) if scan.url else ""
            _check("minted master is a playlist", is_hls_playlist(master_text))
            child = urljoin(scan.url or "", master_text.splitlines()[-1]) if master_text else ""
            child_text = await client.fetch_text(child) if child else ""
            _check("minted child variant works", is_hls_playlist(child_text), child)
            seg = urljoin(child, child_text.splitlines()[-2]) if child_text else ""
            seg_body = await client.fetch_bytes(seg) if seg else b""
            _check("minted segment works", len(seg_body) > 0, seg)

            playlist_as_page = f"{base}/media/master.m3u8?t={token_for('BotFetcher/1.0')}"
            scan2 = await resolve_stream_from_page(client, playlist_as_page)
            _check("non-page response reports not-a-page",
                   scan2.url is None and scan2.reason == "not-a-page", str(scan2))

            scan_err = await resolve_stream_from_page(client, f"{base}/blocked")
            _check("HTTP error page reports error (not not-a-page)",
                   scan_err.url is None and scan_err.reason == "error", str(scan_err))

            scan_embed = await resolve_stream_from_page(client, f"{base}/redir")
            _check("redirect + embed hop sets embed Referer & Origin",
                   scan_embed.reason == "ok"
                   and scan_embed.page_url == f"{base}/player/frame-1"
                   and scan_embed.headers.get("Referer") == f"{base}/player/frame-1"
                   and scan_embed.headers.get("Origin") == base,
                   str(scan_embed))

            file_text = await client.fetch_page_text(f"{base}/file.zip")
            _check("attachment/binary page refused", file_text is None, str(file_text))

            scan_bad = await resolve_stream_from_page(client, f"{base}/page-bad-charset")
            _check("non-utf8 playlist with no charset still accepted",
                   scan_bad.reason == "ok" and bool(scan_bad.url),
                   f"{scan_bad.reason} {scan_bad.detail!r}")
            raw = await client.fetch_text(f"{base}/media/master-cp1252.m3u8")
            _check("such a body decodes into a playlist", is_hls_playlist(raw),
                   repr(raw[:32]))

            scan_junk = await resolve_stream_from_page(
                client, f"{base}/page-junk-candidate")
            _check("non-playlist candidate reports why",
                   scan_junk.url is None and scan_junk.reason == "no-valid-manifest"
                   and "not a playlist" in scan_junk.detail,
                   f"{scan_junk.reason} {scan_junk.detail!r}")
        finally:
            if client is not None:
                await client.close()
            await runner.cleanup()

    asyncio.run(_run_scan_checks())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
