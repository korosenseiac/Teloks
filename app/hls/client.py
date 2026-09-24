"""
hls/client.py — Proxy-aware HTTP client for HLS playlists, segments and keys.

Thin ``aiohttp`` wrapper that reuses the exact same ``proxy.txt`` SOCKS5 setup as
``app.direct.client.DirectLinkClient``: playlists, media segments and AES-128 key
files must all travel through the same route as every other download in this bot,
otherwise geo-restricted HLS streams fail even though the rest of the bot works.

ffmpeg cannot use a SOCKS5 proxy, which is why the pipeline downloads through
this client first (Engine A) and only asks ffmpeg to fetch remotely when no
SOCKS5 route is configured (Engine B). HTTP(S) proxies are additionally exposed
as ``-http_proxy`` arguments for that ffmpeg path.

Self-test (offline, no network needed):

    python -m app.hls.client
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote, urlparse

import aiohttp

from app.config import HLS_DISABLE_PROXY

# ---------------------------------------------------------------------------
# Constants (kept in sync with app/direct/client.py)
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Timeout for playlist / metadata requests.
_TIMEOUT = aiohttp.ClientTimeout(total=60, sock_connect=30)

# Timeout for segment downloads (generous — a segment can be several MB).
_SEGMENT_TIMEOUT = aiohttp.ClientTimeout(total=300, sock_connect=60, sock_read=120)

# Flush buffered bytes to disk once this many are pending.
_FLUSH_THRESHOLD = 4 * 1024 * 1024

# Headers for a *page* fetch (manifest extraction): a browser asks for HTML and
# accepts the JSON/script wrappers some players use instead of plain markup.
_PAGE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,"
              "text/plain;q=0.8,*/*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9",
}
_PAGE_CONTENT_TYPES = {
    "application/xhtml+xml", "application/json", "application/ld+json",
    "application/javascript", "application/x-javascript",
}


# ---------------------------------------------------------------------------
# Proxy configuration
# ---------------------------------------------------------------------------

#: Sentinel for ``HlsClient(proxy_url=...)``: "decide from proxy.txt" (default).
#: Pass ``None`` to force a direct connection for one job — used when the page
#: was resolved by a browser that could not use the configured proxy, so the
#: token it minted is bound to the direct address.
AUTO_PROXY = object()


def read_proxy_url() -> Optional[str]:
    """Build a proxy URL from ``proxy.txt`` (or the deployed fallback path).

    Mirrors ``DirectLinkClient._get_proxy_url``: the project file uses a
    ``host: / port: / username: / password:`` layout and always resolves to a
    ``socks5://`` URL. Returns ``None`` when no usable proxy is configured.
    """
    if HLS_DISABLE_PROXY:
        return None

    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    proxy_file = os.path.join(root_dir, "proxy.txt")

    if not os.path.exists(proxy_file):
        if os.path.exists("/opt/telegram-forwarder-bot/proxy.txt"):
            proxy_file = "/opt/telegram-forwarder-bot/proxy.txt"
        else:
            return None

    try:
        with open(proxy_file, "r", encoding="utf-8") as fh:
            content = fh.read()

        # A single line that already is a proxy URL is accepted as-is.
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "://" in line and ":" not in line.split("://", 1)[0]:
                return line

        host = port = username = password = None
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            key, val = line.split(":", 1)
            key, val = key.strip().lower(), val.strip()
            if key == "host":
                host = val
            elif key == "port":
                port = val
            elif key == "username":
                username = val
            elif key == "password":
                password = val

        if host and port:
            if username and password:
                return f"socks5://{quote(username)}:{quote(password)}@{host}:{port}"
            return f"socks5://{host}:{port}"
    except Exception as e:
        print(f"[HLS] Error reading proxy.txt: {e}")
    return None


def is_socks_proxy(proxy_url: Optional[str]) -> bool:
    """True for socks4/socks5 proxy URLs (usable by aiohttp, not by ffmpeg)."""
    if not proxy_url:
        return False
    return urlparse(proxy_url).scheme.lower().startswith("socks")


def ffmpeg_proxy_args(proxy_url: Optional[str]) -> List[str]:
    """``-http_proxy`` arguments for ffmpeg, or ``[]`` for SOCKS5/no proxy.

    ffmpeg's HTTP protocol only understands HTTP(S) proxies; a SOCKS5 route is
    handled by :class:`HlsClient` instead (see the module docstring).
    """
    if not proxy_url:
        return []
    if urlparse(proxy_url).scheme.lower() in ("http", "https"):
        return ["-http_proxy", proxy_url]
    return []


# ---------------------------------------------------------------------------
# Request headers
# ---------------------------------------------------------------------------

def default_headers(url: str) -> Dict[str, str]:
    """Browser-like headers, including the ``Referer`` most HLS CDNs expect.

    The origin of the stream URL is the best proxy for the page that embedded
    the player, and it is what makes a large number of otherwise-403 streams
    work. A malformed URL is tolerated (no ``Referer``) rather than raising:
    page URLs come from chat messages and junk must never break a job.
    """
    headers: Dict[str, str] = {"User-Agent": _USER_AGENT, "Accept": "*/*"}
    try:
        parsed = urlparse(url)
    except ValueError:
        return headers
    if parsed.scheme and parsed.netloc:
        headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    return headers


def ffmpeg_header_args(headers: Dict[str, str]) -> List[str]:
    """Convert request headers into ffmpeg ``-user_agent`` / ``-headers`` args."""
    args: List[str] = []
    ua = headers.get("User-Agent") or headers.get("user-agent")
    if ua:
        args += ["-user_agent", ua]
    block = "".join(
        f"{k}: {v}\r\n" for k, v in headers.items() if k.lower() != "user-agent"
    )
    if block:
        args += ["-headers", block]
    return args


def _append_bytes(path: str, data: bytes) -> None:
    """Append bytes to *path* (blocking — always called via an executor)."""
    with open(path, "ab") as fh:
        fh.write(data)


# ---------------------------------------------------------------------------
# HlsClient
# ---------------------------------------------------------------------------

class HlsClient:
    """HTTP client for HLS playlists, segments and AES-128 key files.

    A client is created per job and closed by the job's ``finally`` block, so a
    finished job can never close the session of a second concurrent job.
    """

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        proxy_url: Any = AUTO_PROXY,
    ) -> None:
        self.headers = dict(headers or {})
        self._session: Optional[aiohttp.ClientSession] = None
        # AUTO_PROXY (the default) follows proxy.txt; None forces a direct
        # connection for this client only (see app/hls/browser.py).
        self._proxy_url: Optional[str] = (
            read_proxy_url() if proxy_url is AUTO_PROXY else proxy_url
        )

    # ------------------------------------------------------------- session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session

        connector: aiohttp.BaseConnector
        if self._proxy_url:
            try:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(
                    self._proxy_url, limit_per_host=8, limit=64,
                )
            except ImportError:
                print("[HLS] 'aiohttp_socks' not installed. Ignoring SOCKS5 proxy.")
                connector = aiohttp.TCPConnector(limit_per_host=8, limit=64)
        else:
            connector = aiohttp.TCPConnector(limit_per_host=8, limit=64)

        self._session = aiohttp.ClientSession(
            headers=self.headers or {"User-Agent": _USER_AGENT},
            connector=connector,
            read_bufsize=1024 * 1024,
        )
        return self._session

    async def close(self) -> None:
        """Close the underlying session (safe to call more than once)."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    @property
    def proxy_url(self) -> Optional[str]:
        """The proxy this client (and therefore every fetch) uses."""
        return self._proxy_url

    # ------------------------------------------------------------ fetching

    async def fetch_text(self, url: str) -> str:
        """Fetch a playlist document and return it as text."""
        session = await self._get_session()
        async with session.get(
            url, timeout=_TIMEOUT, allow_redirects=True, ssl=False,
        ) as resp:
            if resp.status >= 400:
                raise ValueError(f"HTTP {resp.status} for {url}")
            return await resp.text()

    async def fetch_bytes(
        self, url: str, byte_range: Optional[tuple] = None
    ) -> bytes:
        """Fetch small binary data (AES-128 key files, fMP4 init segments)."""
        session = await self._get_session()
        headers: Dict[str, str] = {}
        if byte_range is not None:
            offset, length = byte_range
            headers["Range"] = f"bytes={offset}-{offset + length - 1}"
        async with session.get(
            url, headers=headers, timeout=_TIMEOUT, allow_redirects=True, ssl=False,
        ) as resp:
            if resp.status >= 400:
                raise ValueError(f"HTTP {resp.status} for {url}")
            return await resp.read()


    async def fetch_page_text(
        self, url: str, max_bytes: int = 4 * 1024 * 1024
    ) -> Optional[str]:
        """Fetch an HTML/JSON *page* for manifest extraction.

        Returns ``None`` — instead of raising — for anything that is not a page
        (a file download, an image, a playlist), so the caller can simply step
        aside and let the normal handlers deal with the link. The body is read
        up to *max_bytes*, so a huge page can never stall a job.
        """
        session = await self._get_session()
        async with session.get(
            url, headers=_PAGE_HEADERS, timeout=_TIMEOUT,
            allow_redirects=True, ssl=False,
        ) as resp:
            if resp.status >= 400:
                raise ValueError(f"HTTP {resp.status} for {url.split('?', 1)[0]}")
            if "attachment" in (resp.headers.get("Content-Disposition") or "").lower():
                return None
            ctype = (resp.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
            if ctype and not (
                ctype.startswith("text/")
                or ctype in _PAGE_CONTENT_TYPES
                or ctype.endswith(("+json", "+xml"))
            ):
                return None

            body = bytearray()
            async for chunk in resp.content.iter_chunked(64 * 1024):
                body.extend(chunk)
                if len(body) >= max_bytes:
                    break
            encoding = resp.get_encoding() or "utf-8"
            try:
                return bytes(body).decode(encoding, "ignore")
            except LookupError:
                return bytes(body).decode("utf-8", "ignore")

    async def download_to(
        self,
        url: str,
        path: str,
        on_chunk: Optional[Callable[[int], None]] = None,
        byte_range: Optional[tuple] = None,
    ) -> int:
        """Download *url* to *path*, returning the number of bytes written.

        Bytes are buffered and flushed through an executor so the event loop is
        never blocked by disk I/O. ``on_chunk(n)`` fires for every flushed chunk
        so the caller can drive a progress bar.

        The target file is recreated on every call, so a retry always starts from
        a clean file.
        """
        session = await self._get_session()
        loop = asyncio.get_running_loop()

        headers: Dict[str, str] = {}
        expected: Optional[int] = None
        if byte_range is not None:
            offset, length = byte_range
            headers["Range"] = f"bytes={offset}-{offset + length - 1}"
            expected = length

        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass

        written = 0
        buf = bytearray()

        async def _flush() -> None:
            nonlocal written
            if not buf:
                return
            data = bytes(buf)
            buf.clear()
            await loop.run_in_executor(None, _append_bytes, path, data)
            written += len(data)
            if on_chunk is not None:
                try:
                    on_chunk(len(data))
                except Exception as e:
                    print(f"[HLS] progress callback failed: {e}")

        async with session.get(
            url,
            headers=headers,
            timeout=_SEGMENT_TIMEOUT,
            allow_redirects=True,
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                raise ValueError(f"HTTP {resp.status} for {url}")
            async for chunk in resp.content.iter_chunked(1024 * 1024):
                if not chunk:
                    continue
                buf.extend(chunk)
                if len(buf) >= _FLUSH_THRESHOLD:
                    await _flush()
            await _flush()

        if expected is not None and written != expected:
            # A truncated byte-range segment would corrupt the mux (and break AES
            # decryption), so treat it as a hard failure and let the downloader
            # retry it instead of shipping a broken MP4.
            raise ValueError(
                f"Byte-range segment truncated: got {written} of {expected} "
                f"bytes for {url}"
            )
        return written


# ---------------------------------------------------------------------------
# Self-test / manual smoke check (offline — no network required)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    print("--- headers ---")
    hdrs = default_headers("https://cdn.example.com/hls/master.m3u8?token=1")
    _check("Referer derived from origin",
           hdrs.get("Referer") == "https://cdn.example.com/", str(hdrs))
    _check("UA present", "Mozilla" in hdrs.get("User-Agent", ""), str(hdrs))
    args = ffmpeg_header_args(hdrs)
    _check("-user_agent emitted", "-user_agent" in args, str(args))
    _check("-headers block CRLF terminated",
           args[args.index("-headers") + 1].endswith("\r\n"), str(args))
    _check("UA not duplicated into -headers",
           "User-Agent" not in args[args.index("-headers") + 1], str(args))
    _check("page fetch asks for HTML",
           _PAGE_HEADERS["Accept"].startswith("text/html"), str(_PAGE_HEADERS))

    print("--- proxy ---")
    _check("socks5 detected", is_socks_proxy("socks5://u:p@h:1080"))
    _check("socks5 -> no ffmpeg args", ffmpeg_proxy_args("socks5://u:p@h:1080") == [])
    _check("http -> ffmpeg args",
           ffmpeg_proxy_args("http://h:8080") == ["-http_proxy", "http://h:8080"])
    _check("no proxy -> no args", ffmpeg_proxy_args(None) == [])

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)

