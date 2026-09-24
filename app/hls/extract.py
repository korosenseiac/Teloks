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
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple
from urllib.parse import SplitResult, urljoin, urlsplit

from app.config import (
    HLS_PAGE_FOLLOW_EMBED,
    HLS_PAGE_MAX_BYTES,
    HLS_PAGE_MAX_CANDIDATES,
)
from app.hls.playlist import is_hls_playlist

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

#: Page-relative manifest paths, e.g. ``/hls/1234/master.m3u8``.
#: The lookbehind stops it from re-matching the path part of a full URL.
_REL_MANIFEST = re.compile(
    r"""(?<![\w/])(/[^""" + _URL_STOP + r"""()]*\.(?:m3u8?|mpd|mp4)[^"""
    + _URL_STOP + r"""()]*)""",
    re.IGNORECASE,
)

#: ``<iframe src=...>`` / ``<embed src=...>`` — a player page often hides the
#: manifest behind one embed hop.
_EMBED = re.compile(
    r"""<(?:iframe|embed)\b[^>]*?\bsrc\s*=\s*(?:"([^"]+)"|'([^']+)'|([^\s"'>]+))""",
    re.IGNORECASE,
)

#: Base64 blobs (some players ship the manifest URL encoded). 80 chars is long
#: enough to be a URL and short enough to keep the search cheap.
_B64_BLOB = re.compile(r"""[A-Za-z0-9+/_-]{80,}={0,2}""")

#: Markers that a page embeds a player (used only for the user-facing hint).
_PLAYER_MARKERS = (
    "m3u8", "hls.js", "hlsjs", "jwplayer", "videojs", "plyr", "shaka",
    "<video", "player", "<iframe",
)


def _unescape(text: str) -> str:
    """Undo the encodings sites use when they embed a URL.

    HTML entities (``&amp;``), JSON/JS escaped slashes (``\\/``) and escaped
    ``\\uXXXX`` characters for ``/ = & ?`` — all of which would otherwise hide a
    perfectly good manifest URL from the scanner.
    """
    text = text.replace("\\/", "/")
    for code, char in (("002F", "/"), ("002f", "/"), ("003D", "="),
                       ("0026", "&"), ("003F", "?"), ("003A", ":")):
        text = text.replace(f"\\u{code}", char)
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


def _looks_like_stream(url: str) -> bool:
    """True when the URL path ends in a playable source extension."""
    parts = _safe_urlsplit(url)
    if parts is None:
        return False
    return parts.path.lower().endswith(HLS_EXTENSIONS + OTHER_STREAM_EXTENSIONS)


def _rank(url: str) -> Tuple[int, int, int, int]:
    """Sort key: HLS before other sources, master before variant, token first.

    Lower is better. A ``master.m3u8`` wins over an ``index.m3u8``, an `m3u8`
    with a query string (the freshly minted, signed one) wins over a bare path,
    and ``.mpd``/``.mp4`` come last because the pipeline cannot download them.
    """
    parts = _safe_urlsplit(url)
    path = (parts.path if parts is not None else "").lower()
    is_hls = 0 if path.endswith(HLS_EXTENSIONS) else 1
    term = path.rsplit("/", 1)[-1]
    is_master = 0 if ("master" in term or "manifest" in term) else 1
    has_query = 0 if "?" in url else 1
    return (is_hls, is_master, has_query, len(url))

def _decoded_blobs(text: str, limit: int = 8) -> List[str]:
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
    strings, escaped slashes (``https:\\/\\/…``), ``&amp;`` entities,
    protocol-relative and page-relative paths, and base64 config blobs.
    """
    if not html:
        return []

    text = _unescape(html)
    found: List[str] = []
    seen = set()

    def _add(raw: str) -> None:
        try:
            url = _clean_candidate(raw, base_url)
        except ValueError:
            # A hostile/broken page must never abort the scan; skip the junk.
            return
        if not url or url in seen or not _looks_like_stream(url):
            return
        seen.add(url)
        found.append(url)

    for match in _ABS_URL.finditer(text):
        _add(match.group(0))
    for match in _REL_MANIFEST.finditer(text):
        _add(match.group(1))
    for blob in _decoded_blobs(text):
        for match in _ABS_URL.finditer(blob):
            _add(match.group(0))
        for match in _REL_MANIFEST.finditer(blob):
            _add(match.group(1))

    found.sort(key=_rank)
    return found[:max(1, limit)]


def find_embed_urls(html: str, base_url: str, limit: int = 2) -> List[str]:
    """Embedded player/iframe URLs (the second place a manifest likes to hide)."""
    if not html:
        return []

    text = _unescape(html)
    ranked: List[Tuple[int, str]] = []
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
        seen.add(url)
        ranked.append((0 if ("embed" in low or "player" in low) else 1, url))

    ranked.sort(key=lambda item: (item[0], len(item[1])))
    return [url for _, url in ranked[:max(1, limit)]]


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
    #: The page that actually carried it (may be an embed followed from the
    #: submitted page) — used as the browser-like ``Referer`` for the stream.
    page_url: str = ""
    #: "ok" | "not-a-page" | "no-manifest" | "no-valid-manifest" | "error"
    reason: str = ""
    #: How many candidate URLs the page offered.
    candidates: int = 0
    #: True when the page embedded a player (drives the user-facing hint).
    player_page: bool = False
    #: Free-text detail for the log line.
    detail: str = ""


async def _first_valid(client, urls: Sequence[str]) -> Optional[str]:
    """First candidate that really is an HLS playlist."""
    for url in urls:
        try:
            text = await client.fetch_text(url)
        except Exception as e:
            print(f"[HLS] Candidate rejected ({e})")
            continue
        if is_hls_playlist(text):
            return url
    return None


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
        html = await client.fetch_page_text(page_url, max_bytes=max_bytes)
    except Exception as e:
        return PageScan(page_url=page_url, reason="not-a-page", detail=str(e))
    if html is None:
        return PageScan(page_url=page_url, reason="not-a-page")

    candidates = find_manifests(html, page_url, limit=limit)
    if candidates:
        url = await _first_valid(client, candidates)
        if url:
            return PageScan(
                url=url, page_url=page_url, reason="ok", candidates=len(candidates),
            )

    if follow_embed:
        for embed in find_embed_urls(html, page_url):
            try:
                sub = await client.fetch_page_text(embed, max_bytes=max_bytes)
            except Exception as e:
                print(f"[HLS] Embed hop failed ({embed.split('?')[0]}): {e}")
                continue
            if not sub:
                continue
            sub_candidates = find_manifests(sub, embed, limit=limit)
            if not sub_candidates:
                continue
            url = await _first_valid(client, sub_candidates)
            if url:
                return PageScan(
                    url=url, page_url=embed, reason="ok",
                    candidates=len(sub_candidates),
                )

    return PageScan(
        page_url=page_url,
        reason="no-manifest" if not candidates else "no-valid-manifest",
        candidates=len(candidates),
        player_page=looks_like_player_page(html),
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
        '<iframe src="https://ads.example.com/banner.html"></iframe>'
        '<iframe src="https://site.example.com/embed/player-123"></iframe>', BASE)
    _check("embed/player host ranked first",
           embeds[0] == "https://site.example.com/embed/player-123", str(embeds))
    _check("player page detected",
           looks_like_player_page('<script src="hls.js"></script><video id="v">'))
    _check("plain page not a player page",
           not looks_like_player_page("<html><body><p>blog post</p></body></html>"))

    print("--- hostile page junk never aborts the scan ---")
    # The production shape that broke the scan: a JS template literal with an
    # unmatched '[' reached urlsplit() as a bad host ("Invalid IPv6 URL") and
    # killed the whole page, even though the real manifest was further down.
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
    FOREIGN_UA = "ForeignBrowser/1.0"   # the browser that minted the copied URL

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
            return web.Response(text=body, content_type="text/html")

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
            # Real CDNs sign every segment for the requesting client, so each
            # line carries its own token.
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

        app = web.Application()
        app.router.add_get("/page", page)
        app.router.add_get("/media/master.m3u8", master)
        app.router.add_get("/media/index.m3u8", media)
        app.router.add_get("/media/seg0.ts", segment)
        app.router.add_get("/file.zip", real_file)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        client: Optional[HlsClient] = None

        try:
            # 1) The URL a user copies out of a browser is signed for THAT
            #    client: replaying it from here must fail, exactly like the
            #    real CDN did for the reported site.
            client = HlsClient(headers={"User-Agent": "BotFetcher/1.0"})
            foreign = f"{base}/media/master.m3u8?t={token_for(FOREIGN_UA)}"
            blocked = ""
            try:
                await client.fetch_text(foreign)
            except Exception as e:
                blocked = str(e)
            _check("foreign-signed URL rejected (403) like the real CDN",
                   "403" in blocked, blocked or "request unexpectedly succeeded")

            # 2) Loading the page OURSELVES mints a URL bound to our client.
            scan = await resolve_stream_from_page(client, f"{base}/page")
            _check("page scan found a manifest", bool(scan.url),
                   f"{scan.reason} {scan.detail}")
            _check("scan reason ok", scan.reason == "ok", scan.reason)
            _check("minted for us, not for the browser",
                   bool(scan.url) and token_for(FOREIGN_UA) not in scan.url,
                   str(scan.url))

            # 3) ...and the whole chain works with that freshly minted token.
            master_text = await client.fetch_text(scan.url) if scan.url else ""
            _check("minted master is a playlist", is_hls_playlist(master_text))
            child = urljoin(scan.url or "", master_text.splitlines()[-1]) if master_text else ""
            child_text = await client.fetch_text(child) if child else ""
            _check("minted child variant works", is_hls_playlist(child_text), child)
            seg = urljoin(child, child_text.splitlines()[-2]) if child_text else ""
            seg_body = await client.fetch_bytes(seg) if seg else b""
            _check("minted segment works", len(seg_body) > 0, seg)

            # 4) A playlist served where a page was expected -> clean "no page".
            playlist_as_page = f"{base}/media/master.m3u8?t={token_for('BotFetcher/1.0')}"
            scan2 = await resolve_stream_from_page(client, playlist_as_page)
            _check("non-page response reports not-a-page",
                   scan2.url is None and scan2.reason == "not-a-page", str(scan2))

            # 5) A real file is never downloaded twice by the sniffer.
            file_text = await client.fetch_page_text(f"{base}/file.zip")
            _check("attachment/binary page refused", file_text is None, str(file_text))
        finally:
            if client is not None:
                await client.close()
            await runner.cleanup()

    asyncio.run(_run_scan_checks())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)

