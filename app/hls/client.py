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
import datetime
import email.utils
import http.cookies
import os
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import quote, urlparse

import aiohttp
from yarl import URL

import app.config as _app_config
from app.config import HLS_DISABLE_PROXY
from app.warp import (
    WarpProxyConfig,
    WarpProxyError,
    create_aiohttp_connector,
    ensure_proxy_ready,
    format_warp_proxy_error,
    get_active_socks5_url,
    is_warp_proxy_error,
)

# ---------------------------------------------------------------------------
# Constants (kept in sync with app/direct/client.py and app/hls/browser.py)
# ---------------------------------------------------------------------------

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_USER_AGENT = DEFAULT_USER_AGENT

_DEFAULT_CONNECT_TIMEOUT = 15.0
_DEFAULT_SEGMENT_TIMEOUT = 120.0
_DEFAULT_WORKERS = 16

# Flush buffered bytes to disk once this many are pending.
_FLUSH_THRESHOLD = 1024 * 1024
_CHUNK_READ_SIZE = 256 * 1024
_MAX_IGNORED_RANGE_SKIP = 32 * 1024 * 1024

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

_PERMANENT_HTTP_STATUSES = frozenset({401, 403, 404, 410, 451})


# ---------------------------------------------------------------------------
# Errors & URL sanitization
# ---------------------------------------------------------------------------

def redact_url(url: str) -> str:
    """Strip query tokens and fragments before placing a URL in logs/errors."""
    if not url:
        return ""
    return url.split("?", 1)[0].split("#", 1)[0]


def _redact_proxy_url(proxy_url: Optional[str]) -> str:
    if not proxy_url:
        return ""
    try:
        parsed = urlparse(proxy_url)
    except ValueError:
        return "<proxy>"
    if not parsed.hostname:
        return "<proxy>"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        secs = float(raw)
        return max(0.5, min(60.0, secs))
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        delta = (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        return max(0.5, min(60.0, delta))
    except Exception:
        return None


class HlsHttpError(ValueError):
    """HTTP >= 400 response with structured status and Retry-After metadata."""

    def __init__(
        self,
        status: int,
        url: str,
        *,
        retry_after: Optional[float] = None,
        detail: str = "",
    ) -> None:
        self.status = int(status)
        self.url = redact_url(url)
        self.retry_after = retry_after
        self.detail = detail
        msg = f"HTTP {self.status} for {self.url}"
        if detail:
            msg = f"{msg} ({detail})"
        super().__init__(msg)

    @property
    def is_permanent(self) -> bool:
        """True for status codes unlikely to recover on blind retry."""
        return self.status in _PERMANENT_HTTP_STATUSES


class HlsProxyError(ValueError):
    """Proxy connection/handshake failure (separate from stream HTTP errors)."""

    def __init__(
        self,
        proxy_url: Optional[str],
        cause: Optional[BaseException] = None,
    ) -> None:
        self.proxy_url = _redact_proxy_url(proxy_url)
        self.cause = cause
        detail = f"{type(cause).__name__}: {cause}" if cause is not None else "unreachable"
        super().__init__(f"Proxy connection failed ({self.proxy_url}): {detail}")


def _is_proxy_exception(exc: BaseException) -> bool:
    """True when *exc* (or its chain) indicates the proxy itself failed."""
    try:
        import aiohttp_socks
        socks_errors: Tuple[type, ...] = (
            aiohttp_socks.ProxyError,
            aiohttp_socks.ProxyConnectionError,
            aiohttp_socks.ProxyTimeoutError,
        )
    except Exception:
        socks_errors = ()

    seen: set[int] = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, aiohttp.ClientProxyConnectionError):
            return True
        if socks_errors and isinstance(cur, socks_errors):
            return True
        cls_name = type(cur).__name__
        if cls_name in ("ProxyError", "ProxyConnectionError", "ProxyTimeoutError"):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


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

def _origin_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return ""


def default_headers(
    url: str,
    *,
    referer: Optional[str] = None,
    origin: Optional[str] = None,
) -> Dict[str, str]:
    """Browser-like headers, including ``Referer`` (and ``Origin`` when given).

    The origin of the stream URL is the best proxy for the page that embedded
    the player, and it is what makes a large number of otherwise-403 streams
    work. When *referer* is passed explicitly, ``Origin`` is derived from it
    unless *origin* overrides it. A malformed URL is tolerated rather than
    raising: page URLs come from chat messages and junk must never break a job.
    """
    headers: Dict[str, str] = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer
        derived_origin = origin if origin is not None else _origin_from_url(referer)
        if derived_origin:
            headers["Origin"] = derived_origin
        return headers

    if origin:
        headers["Origin"] = origin

    stream_origin = _origin_from_url(url)
    if stream_origin:
        headers["Referer"] = f"{stream_origin}/"
    return headers


def _sanitize_header_part(text: str) -> str:
    return text.replace("\r", "").replace("\n", "").replace("\x00", "")


def ffmpeg_header_args(
    headers: Dict[str, str],
    cookies: Optional[Union[Dict[str, str], str]] = None,
) -> List[str]:
    """Convert request headers (and optional cookies) into ffmpeg args."""
    args: List[str] = []
    ua = ""
    has_cookie = False
    lines: List[str] = []

    for key, val in (headers or {}).items():
        if not key or val is None:
            continue
        k = _sanitize_header_part(str(key)).strip()
        v = _sanitize_header_part(str(val)).strip()
        if not k or not v:
            continue
        low = k.lower()
        if low == "user-agent":
            ua = v
            continue
        if low == "cookie":
            has_cookie = True
        lines.append(f"{k}: {v}\r\n")

    if not has_cookie and cookies:
        if isinstance(cookies, str):
            cookie_str = _sanitize_header_part(cookies).strip()
        else:
            cookie_str = "; ".join(
                f"{_sanitize_header_part(str(k)).strip()}={_sanitize_header_part(str(v)).strip()}"
                for k, v in cookies.items()
                if k and v is not None
            )
        if cookie_str:
            lines.append(f"Cookie: {cookie_str}\r\n")

    if ua:
        args += ["-user_agent", ua]
    block = "".join(lines)
    if block:
        args += ["-headers", block]
    return args


def _write_bytes(path: str, data: bytes, append: bool) -> None:
    """Write/append bytes to *path* (blocking — always called via an executor)."""
    mode = "ab" if append else "wb"
    with open(path, mode) as fh:
        fh.write(data)


def decode_text(body: bytes, encoding: Optional[str] = None) -> str:
    """Decode a fetched body as text *without* ever raising on bad bytes.

    Real sites lie about charsets. A body that advertises (or defaults to)
    utf-8 but actually carries cp1252/latin-1 text — or that is not text at all
    — used to blow up inside ``str.decode`` as ``UnicodeDecodeError``, which the
    page scan then reported as "Candidate rejected": the log hid whether the
    candidate was a playlist at all, and a genuine manifest sitting in a
    mis-declared page would have been thrown away.

    Tolerating the bad bytes costs nothing: an HLS playlist is pure ASCII, so a
    real one still starts with ``#EXTM3U`` after this. Tries the declared
    charset strictly, then utf-8, then cp1252 (which accepts almost every byte),
    and finally replaces whatever is left.
    """
    for enc in (encoding, "utf-8", "cp1252"):
        if not enc:
            continue
        try:
            return body.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue
    return body.decode("utf-8", "replace")


def _parse_cookie_header(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in raw.split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        if k:
            out[k] = v.strip()
    return out


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
        *,
        cookies: Optional[Union[Dict[str, str], Iterable[Dict[str, Any]]]] = None,
        max_connections: Optional[int] = None,
        connect_timeout: Optional[float] = None,
        segment_timeout: Optional[float] = None,
    ) -> None:
        raw_headers = dict(headers or {})
        self._pending_cookies: Dict[str, str] = {}
        self._pending_cookie_entries: List[Dict[str, Any]] = []
        self._pending_morsels: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self.headers: Dict[str, str] = {}

        for k, v in raw_headers.items():
            if not k or v is None:
                continue
            if str(k).lower() == "cookie":
                self._pending_cookies.update(_parse_cookie_header(str(v)))
            else:
                self.headers[str(k)] = str(v)

        if "User-Agent" not in self.headers and "user-agent" not in {
            k.lower() for k in self.headers
        }:
            self.headers["User-Agent"] = DEFAULT_USER_AGENT

        if cookies:
            self._ingest_cookies(cookies)

        self._session: Optional[aiohttp.ClientSession] = None
        # AUTO_PROXY (the default) uses WARP when ON, falling back to proxy.txt;
        # explicit values (e.g. None for direct) bypass WARP/proxy.txt.
        self._proxy_mode: str = "auto" if proxy_url is AUTO_PROXY else "explicit"
        self._explicit_proxy_url: Optional[str] = None if proxy_url is AUTO_PROXY else proxy_url
        self._session_proxy_key: Optional[tuple] = None
        self._using_warp: bool = False
        self._warp_cfg: Optional[WarpProxyConfig] = None
        if proxy_url is AUTO_PROXY:
            active_warp = get_active_socks5_url()
            self._proxy_url: Optional[str] = active_warp if active_warp else read_proxy_url()
        else:
            self._proxy_url = proxy_url
        self._max_connections = max_connections
        self._connect_timeout = connect_timeout
        self._segment_timeout = segment_timeout

    # -------------------------------------------------------------- cookies

    def _ingest_cookies(
        self, cookies: Union[Dict[str, str], Iterable[Dict[str, Any]]]
    ) -> None:
        if isinstance(cookies, dict):
            for k, v in cookies.items():
                if k and v is not None:
                    self._pending_cookies[str(k)] = str(v)
        else:
            for item in cookies:
                if isinstance(item, dict) and item.get("name"):
                    self._pending_cookie_entries.append(dict(item))

    def _apply_pending_cookies(self, jar: aiohttp.CookieJar) -> None:
        if self._pending_morsels:
            exp_fn = getattr(jar, "_expire_cookie", None)
            for item in self._pending_morsels.values():
                name = str(item.get("name") or "")
                if not name:
                    continue
                morsel: http.cookies.Morsel = http.cookies.Morsel()
                morsel.__setstate__({
                    "key": name,
                    "value": item.get("value", ""),
                    "coded_value": item.get("coded_value", item.get("value", "")),
                })
                for attr, val in (item.get("attrs") or {}).items():
                    morsel[attr] = val
                path = str(item.get("path") or "/")
                if path:
                    morsel["path"] = path
                domain = str(item.get("domain") or "")
                host_only = bool(item.get("host_only"))
                if domain and not host_only:
                    morsel["domain"] = domain
                else:
                    morsel["domain"] = ""
                resp_url = URL()
                if domain:
                    try:
                        resp_url = URL.build(scheme="https", host=domain)
                    except ValueError:
                        resp_url = URL()
                        morsel["domain"] = domain
                jar.update_cookies({name: morsel}, response_url=resp_url)
                if "expires_ts" in item and callable(exp_fn):
                    try:
                        exp_fn(float(item["expires_ts"]), domain, path.rstrip("/"), name)
                    except Exception:
                        pass
        if self._pending_cookies:
            jar.update_cookies(self._pending_cookies)
        if self._pending_cookie_entries:
            for item in self._pending_cookie_entries:
                name = str(item.get("name") or "")
                if not name:
                    continue
                val = str(item.get("value") or "")
                sc = http.cookies.SimpleCookie()
                sc[name] = val
                domain = str(item.get("domain") or "")
                path = str(item.get("path") or "")
                host_only = bool(item.get("host_only"))
                if path:
                    sc[name]["path"] = path
                if domain and not host_only:
                    sc[name]["domain"] = domain
                resp_url = URL()
                if domain and host_only:
                    try:
                        resp_url = URL.build(scheme="https", host=domain)
                    except ValueError:
                        sc[name]["domain"] = domain
                jar.update_cookies(sc, response_url=resp_url)

    def _save_session_cookies(self) -> None:
        if self._session is None or self._session.closed:
            return
        jar = getattr(self._session, "cookie_jar", None)
        if jar is None:
            return
        try:
            morsels = list(jar)
        except Exception:
            morsels = []
        if not morsels:
            if not isinstance(jar, aiohttp.CookieJar):
                self._pending_cookies.update(self.export_cookies())
            return
        host_only = set(getattr(jar, "host_only_cookies", ()) or ())
        expirations = getattr(jar, "_expirations", {}) or {}
        saved: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for morsel in morsels:
            if not isinstance(morsel, http.cookies.Morsel):
                self._pending_cookies.update(self.export_cookies())
                return
            domain = str(morsel["domain"] or "")
            path = str(morsel["path"] or "/")
            name = str(morsel.key)
            is_host_only = (domain, name) in host_only
            attrs: Dict[str, Any] = {}
            for attr in ("expires", "max-age", "secure", "httponly", "samesite", "comment", "version"):
                val = morsel.get(attr)
                if val:
                    attrs[attr] = val
            entry: Dict[str, Any] = {
                "name": name,
                "value": morsel.value,
                "coded_value": morsel.coded_value,
                "domain": domain,
                "path": path,
                "host_only": is_host_only,
                "attrs": attrs,
            }
            exp = expirations.get((domain, path.rstrip("/"), name))
            if exp is not None:
                entry["expires_ts"] = exp
            saved[(domain, path, name)] = entry
        self._pending_morsels.update(saved)
        self._pending_cookies.clear()
        self._pending_cookie_entries.clear()

    def update_cookies(
        self,
        cookies: Union[Dict[str, str], Iterable[Dict[str, Any]]],
        url: Optional[str] = None,
    ) -> None:
        """Add or update cookies on this client (and its open session jar)."""
        if not cookies:
            return
        if self._session is not None and not self._session.closed:
            jar = self._session.cookie_jar
            if isinstance(cookies, dict):
                cleaned = {str(k): str(v) for k, v in cookies.items() if k and v is not None}
                if url:
                    jar.update_cookies(cleaned, response_url=URL(url))
                else:
                    jar.update_cookies(cleaned)
                return
        if isinstance(cookies, dict) and url:
            try:
                parsed = URL(url)
                host = parsed.raw_host or ""
                req_path = parsed.path or "/"
                cookie_path = (
                    "/" + req_path[1:req_path.rfind("/")]
                    if req_path.startswith("/") and "/" in req_path[1:]
                    else "/"
                )
            except Exception:
                host = ""
                cookie_path = "/"
            if host:
                for k, v in cookies.items():
                    if k and v is not None:
                        self._pending_cookie_entries.append({
                            "name": str(k),
                            "value": str(v),
                            "domain": host,
                            "path": cookie_path or "/",
                            "host_only": True,
                        })
                return
        self._ingest_cookies(cookies)
        if self._session is not None and not self._session.closed:
            self._apply_pending_cookies(self._session.cookie_jar)

    def export_cookies(self, url: Optional[str] = None) -> Dict[str, str]:
        """Return cookies as ``{name: value}`` (filtered by *url* if given)."""
        if self._session is not None and not self._session.closed:
            jar = self._session.cookie_jar
            if url:
                try:
                    filtered = jar.filter_cookies(URL(url))
                    return {k: m.value for k, m in filtered.items() if m.value is not None}
                except Exception:
                    pass
            out: Dict[str, str] = {}
            for cookie in jar:
                out[cookie.key] = cookie.value
            return out

        if url and (self._pending_morsels or self._pending_cookie_entries or self._pending_cookies):
            try:
                loop = asyncio.get_running_loop()
                tmp_jar = aiohttp.CookieJar(unsafe=True, loop=loop)
                self._apply_pending_cookies(tmp_jar)
                filtered = tmp_jar.filter_cookies(URL(url))
                return {k: m.value for k, m in filtered.items() if m.value is not None}
            except RuntimeError:
                pass
            except Exception:
                pass

        out = dict(self._pending_cookies)
        for item in self._pending_cookie_entries:
            name = item.get("name")
            if name:
                out[str(name)] = str(item.get("value") or "")
        for item in self._pending_morsels.values():
            name = item.get("name")
            if name:
                out[str(name)] = str(item.get("value") or "")
        return out

    # ------------------------------------------------------------- session

    def _effective_connect_timeout(self) -> float:
        raw = (
            self._connect_timeout
            if self._connect_timeout is not None
            else getattr(_app_config, "HLS_CONNECT_TIMEOUT", _DEFAULT_CONNECT_TIMEOUT)
        )
        return max(3.0, float(raw))

    def _effective_segment_timeout(self) -> float:
        raw = (
            self._segment_timeout
            if self._segment_timeout is not None
            else getattr(_app_config, "HLS_SEGMENT_TIMEOUT", _DEFAULT_SEGMENT_TIMEOUT)
        )
        return max(10.0, float(raw))

    def _meta_timeout(self) -> aiohttp.ClientTimeout:
        connect_to = self._effective_connect_timeout()
        return aiohttp.ClientTimeout(total=45.0, sock_connect=connect_to, sock_read=30.0)

    def _segment_client_timeout(self) -> aiohttp.ClientTimeout:
        connect_to = self._effective_connect_timeout()
        seg_to = self._effective_segment_timeout()
        return aiohttp.ClientTimeout(
            total=seg_to,
            sock_connect=connect_to,
            sock_read=min(60.0, seg_to),
        )

    def _pool_limits(self) -> Tuple[int, int]:
        workers = int(getattr(_app_config, "HLS_SEGMENT_WORKERS", _DEFAULT_WORKERS) or _DEFAULT_WORKERS)
        per_host = max(32, int(self._max_connections or (workers * 2)))
        total = max(128, per_host * 2)
        return per_host, total

    async def _get_session(self) -> aiohttp.ClientSession:
        warp_cfg: Optional[WarpProxyConfig] = None
        if self._proxy_mode == "auto":
            warp_cfg = await ensure_proxy_ready(
                require_http=False,
                require_socks5=True,
                feature="HLS",
            )
            if warp_cfg is not None:
                desired_proxy: Optional[str] = warp_cfg.socks5_url
                desired_key: tuple = ("warp", warp_cfg.socks5_url)
            else:
                desired_proxy = read_proxy_url()
                desired_key = ("proxy_txt", desired_proxy) if desired_proxy else ("direct", None)
        else:
            desired_proxy = self._explicit_proxy_url
            desired_key = ("explicit", desired_proxy) if desired_proxy else ("direct", None)

        if self._session is not None and not self._session.closed:
            if self._session_proxy_key == desired_key:
                return self._session
            self._save_session_cookies()
            await self.close()

        per_host, total = self._pool_limits()
        connector: aiohttp.BaseConnector
        if warp_cfg is not None:
            try:
                try:
                    connector = create_aiohttp_connector(
                        limit_per_host=per_host,
                        limit=total,
                        ttl_dns_cache=300,
                        keepalive_timeout=60,
                        enable_cleanup_closed=True,
                    )
                except TypeError:
                    connector = create_aiohttp_connector(
                        limit_per_host=per_host,
                        limit=total,
                    )
                if connector is None:
                    from aiohttp_socks import ProxyConnector
                    try:
                        connector = ProxyConnector.from_url(
                            warp_cfg.socks5_url,
                            rdns=True,
                            limit_per_host=per_host,
                            limit=total,
                            ttl_dns_cache=300,
                            keepalive_timeout=60,
                            enable_cleanup_closed=True,
                        )
                    except TypeError:
                        connector = ProxyConnector.from_url(
                            warp_cfg.socks5_url,
                            rdns=True,
                            limit_per_host=per_host,
                            limit=total,
                        )
            except WarpProxyError:
                raise
            except Exception as exc:
                raise WarpProxyError(
                    format_warp_proxy_error(exc, feature="HLS")
                ) from exc
        elif desired_proxy:
            try:
                from aiohttp_socks import ProxyConnector
                try:
                    connector = ProxyConnector.from_url(
                        desired_proxy,
                        rdns=True,
                        limit_per_host=per_host,
                        limit=total,
                        ttl_dns_cache=300,
                        keepalive_timeout=60,
                        enable_cleanup_closed=True,
                    )
                except TypeError:
                    connector = ProxyConnector.from_url(
                        desired_proxy,
                        rdns=True,
                        limit_per_host=per_host,
                        limit=total,
                    )
            except ImportError:
                print("[HLS] 'aiohttp_socks' not installed. Ignoring SOCKS5 proxy.")
                desired_proxy = None
                desired_key = ("direct", None)
                connector = aiohttp.TCPConnector(
                    limit_per_host=per_host,
                    limit=total,
                    ttl_dns_cache=300,
                    keepalive_timeout=60,
                    enable_cleanup_closed=True,
                )
        else:
            connector = aiohttp.TCPConnector(
                limit_per_host=per_host,
                limit=total,
                ttl_dns_cache=300,
                keepalive_timeout=60,
                enable_cleanup_closed=True,
            )

        jar = aiohttp.CookieJar(unsafe=True)
        self._apply_pending_cookies(jar)

        try:
            self._session = aiohttp.ClientSession(
                connector=connector,
                cookie_jar=jar,
                read_bufsize=2 * 1024 * 1024,
                trust_env=False,
            )
        except Exception:
            await connector.close()
            raise
        self._pending_cookies.clear()
        self._pending_cookie_entries.clear()
        self._pending_morsels.clear()
        self._using_warp = warp_cfg is not None
        self._warp_cfg = warp_cfg
        self._proxy_url = desired_proxy
        self._session_proxy_key = desired_key
        return self._session

    async def switch_to_direct(self) -> None:
        """Drop the configured proxy and reconnect directly (keeping cookies)."""
        if self._session is not None and not self._session.closed:
            self._save_session_cookies()
            await self._session.close()
        self._session = None
        self._session_proxy_key = None
        self._proxy_mode = "explicit"
        self._explicit_proxy_url = None
        self._proxy_url = None
        self._using_warp = False
        self._warp_cfg = None

    async def close(self) -> None:
        """Close the underlying session (safe to call more than once)."""
        if self._session is not None and not self._session.closed:
            self._save_session_cookies()
        session = self._session
        self._session = None
        self._session_proxy_key = None
        self._using_warp = False
        self._warp_cfg = None
        if session is not None and not session.closed:
            await session.close()

    @property
    def using_warp(self) -> bool:
        """Whether the active HTTP session is routed through WARP wireproxy."""
        return self._using_warp

    @property
    def warp_http_proxy_url(self) -> Optional[str]:
        """Active WARP HTTP proxy URL when using WARP, else ``None``."""
        if self._using_warp and self._warp_cfg is not None:
            return self._warp_cfg.http_url
        return None

    @property
    def proxy_url(self) -> Optional[str]:
        """The proxy this client (and therefore every fetch) uses."""
        return self._proxy_url

    @property
    def user_agent(self) -> str:
        """The active User-Agent string sent by this client."""
        for k, v in self.headers.items():
            if k.lower() == "user-agent" and v:
                return v
        return DEFAULT_USER_AGENT

    def _merged_headers(
        self, extra: Optional[Dict[str, Optional[str]]] = None
    ) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        key_map: Dict[str, str] = {}
        for k, v in self.headers.items():
            if k and v is not None:
                low = k.lower()
                key_map[low] = k
                merged[k] = v
        if extra:
            for k, v in extra.items():
                if not k:
                    continue
                low = k.lower()
                if low in key_map:
                    merged.pop(key_map.pop(low), None)
                if v is not None:
                    key_map[low] = k
                    merged[k] = v
        return merged

    def apply_header_overrides(
        self, overrides: Optional[Dict[str, Optional[str]]]
    ) -> None:
        """Update ``self.headers`` case-insensitively (``None`` removes a key)."""
        if not overrides:
            return
        self.headers = self._merged_headers(overrides)

    def _raise_if_proxy_error(self, exc: BaseException) -> None:
        if self._using_warp and is_warp_proxy_error(exc):
            raise WarpProxyError(
                format_warp_proxy_error(exc, feature="HLS")
            ) from exc
        if self._proxy_url and _is_proxy_exception(exc):
            raise HlsProxyError(self._proxy_url, exc) from exc

    @staticmethod
    def _raise_for_status(resp: aiohttp.ClientResponse, url: str) -> None:
        if resp.status >= 400:
            retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
            raise HlsHttpError(
                resp.status,
                str(resp.url or url),
                retry_after=retry_after,
            )

    # ------------------------------------------------------------ fetching

    async def fetch_text_with_url(
        self,
        url: str,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> Tuple[str, str]:
        """Fetch a playlist document and return ``(text, final_url)``."""
        session = await self._get_session()
        req_headers = self._merged_headers(headers)
        try:
            async with session.get(
                url,
                headers=req_headers,
                timeout=self._meta_timeout(),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                self._raise_for_status(resp, url)
                body = await resp.read()
                final_url = str(resp.url or url)
                return decode_text(body, resp.get_encoding()), final_url
        except (HlsHttpError, HlsProxyError, WarpProxyError):
            raise
        except Exception as exc:
            self._raise_if_proxy_error(exc)
            raise

    async def fetch_text(
        self,
        url: str,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> str:
        """Fetch a playlist document and return it as text.

        Decoding is deliberately lenient (see :func:`decode_text`).
        """
        text, _ = await self.fetch_text_with_url(url, headers=headers)
        return text

    async def _read_segment_response(
        self,
        resp: aiohttp.ClientResponse,
        url: str,
        byte_range: Optional[Tuple[int, int]],
        on_piece: Callable[[bytes], Any],
    ) -> int:
        expected: Optional[int] = None
        offset = 0
        if byte_range is not None:
            offset, expected = int(byte_range[0]), int(byte_range[1])

        total_written = 0
        if (
            byte_range is not None
            and resp.status == 200
            and not resp.headers.get("Content-Range")
            and not (
                offset == 0
                and resp.content_length is not None
                and resp.content_length == expected
            )
        ):
            # Server ignored Range: and returned the full object starting at 0.
            if offset > _MAX_IGNORED_RANGE_SKIP:
                raise ValueError(
                    f"Server ignored Range header (200 OK) at offset {offset} for {redact_url(url)}"
                )
            end = offset + (expected or 0)
            pos = 0
            async for chunk in resp.content.iter_chunked(_CHUNK_READ_SIZE):
                if not chunk:
                    continue
                chunk_end = pos + len(chunk)
                if chunk_end > offset and pos < end:
                    start_idx = max(0, offset - pos)
                    stop_idx = min(len(chunk), end - pos)
                    piece = chunk[start_idx:stop_idx]
                    if piece:
                        res = on_piece(piece)
                        if asyncio.iscoroutine(res):
                            await res
                        total_written += len(piece)
                pos = chunk_end
                if pos >= end:
                    break
        else:
            async for chunk in resp.content.iter_chunked(_CHUNK_READ_SIZE):
                if not chunk:
                    continue
                res = on_piece(chunk)
                if asyncio.iscoroutine(res):
                    await res
                total_written += len(chunk)

        if expected is not None:
            if total_written != expected:
                raise ValueError(
                    f"Byte-range segment truncated: got {total_written} of {expected} "
                    f"bytes for {redact_url(url)}"
                )
        else:
            if total_written == 0:
                raise ValueError(f"Empty response (0 bytes) for {redact_url(url)}")
            cl = resp.content_length
            if (
                cl is not None
                and cl > 0
                and not resp.headers.get("Content-Encoding")
                and total_written < cl
            ):
                raise ValueError(
                    f"Segment truncated: got {total_written} of {cl} bytes for {redact_url(url)}"
                )
        return total_written

    async def fetch_bytes(
        self,
        url: str,
        byte_range: Optional[Tuple[int, int]] = None,
        on_chunk: Optional[Callable[[int], None]] = None,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> bytes:
        """Fetch binary data (AES-128 key files, init segments, or media chunks)."""
        session = await self._get_session()
        extra: Dict[str, Optional[str]] = dict(headers or {})
        if byte_range is not None:
            offset, length = int(byte_range[0]), int(byte_range[1])
            extra["Range"] = f"bytes={offset}-{offset + length - 1}"
        req_headers = self._merged_headers(extra)

        try:
            async with session.get(
                url,
                headers=req_headers,
                timeout=self._meta_timeout() if byte_range is None and on_chunk is None else self._segment_client_timeout(),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                self._raise_for_status(resp, url)
                if byte_range is None and on_chunk is None:
                    return await resp.read()
                buf = bytearray()

                def _collect(piece: bytes) -> None:
                    buf.extend(piece)
                    if on_chunk is not None:
                        try:
                            on_chunk(len(piece))
                        except Exception as e:
                            print(f"[HLS] progress callback failed: {e}")

                await self._read_segment_response(resp, url, byte_range, _collect)
                return bytes(buf)
        except (HlsHttpError, HlsProxyError, WarpProxyError):
            raise
        except Exception as exc:
            self._raise_if_proxy_error(exc)
            raise

    async def download_bytes(
        self,
        url: str,
        byte_range: Optional[Tuple[int, int]] = None,
        on_chunk: Optional[Callable[[int], None]] = None,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> bytes:
        """Stream a segment into memory with Range/truncation checks & progress."""
        session = await self._get_session()
        extra: Dict[str, Optional[str]] = dict(headers or {})
        if byte_range is not None:
            offset, length = int(byte_range[0]), int(byte_range[1])
            extra["Range"] = f"bytes={offset}-{offset + length - 1}"
        req_headers = self._merged_headers(extra)

        try:
            async with session.get(
                url,
                headers=req_headers,
                timeout=self._segment_client_timeout(),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                self._raise_for_status(resp, url)
                buf = bytearray()

                def _collect(piece: bytes) -> None:
                    buf.extend(piece)
                    if on_chunk is not None:
                        try:
                            on_chunk(len(piece))
                        except Exception as e:
                            print(f"[HLS] progress callback failed: {e}")

                await self._read_segment_response(resp, url, byte_range, _collect)
                return bytes(buf)
        except (HlsHttpError, HlsProxyError, WarpProxyError):
            raise
        except Exception as exc:
            self._raise_if_proxy_error(exc)
            raise

    async def fetch_page(
        self,
        url: str,
        max_bytes: int = 4 * 1024 * 1024,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> Optional[Tuple[str, str]]:
        """Fetch an HTML/JSON *page* and return ``(text, final_url)`` or ``None``.

        Returns ``None`` — instead of raising — for anything that is not a page
        (a file download, an image, a playlist), so the caller can simply step
        aside and let the normal handlers deal with the link. The body is read
        up to *max_bytes*, so a huge page can never stall a job.
        """
        session = await self._get_session()
        extra: Dict[str, Optional[str]] = dict(_PAGE_HEADERS)
        if headers:
            extra.update(headers)
        req_headers = self._merged_headers(extra)
        try:
            async with session.get(
                url,
                headers=req_headers,
                timeout=self._meta_timeout(),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                self._raise_for_status(resp, url)
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
                final_url = str(resp.url or url)
                return decode_text(bytes(body), resp.get_encoding()), final_url
        except (HlsHttpError, HlsProxyError, WarpProxyError):
            raise
        except Exception as exc:
            self._raise_if_proxy_error(exc)
            raise

    async def fetch_page_text(
        self,
        url: str,
        max_bytes: int = 4 * 1024 * 1024,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> Optional[str]:
        """Fetch an HTML/JSON *page* for manifest extraction (text only)."""
        res = await self.fetch_page(url, max_bytes=max_bytes, headers=headers)
        return res[0] if res is not None else None

    async def download_to(
        self,
        url: str,
        path: str,
        on_chunk: Optional[Callable[[int], None]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        *,
        headers: Optional[Dict[str, Optional[str]]] = None,
    ) -> int:
        """Download *url* to *path*, returning the number of bytes written.

        Bytes are buffered and flushed through an executor so the event loop is
        never blocked by disk I/O. ``on_chunk(n)`` fires for every flushed chunk
        so the caller can drive a progress bar.
        """
        session = await self._get_session()
        loop = asyncio.get_running_loop()

        extra: Dict[str, Optional[str]] = dict(headers or {})
        if byte_range is not None:
            offset, length = int(byte_range[0]), int(byte_range[1])
            extra["Range"] = f"bytes={offset}-{offset + length - 1}"
        req_headers = self._merged_headers(extra)

        written = 0
        has_flushed = False
        buf = bytearray()

        async def _flush() -> None:
            nonlocal written, has_flushed
            if not buf:
                return
            data = bytes(buf)
            buf.clear()
            await loop.run_in_executor(None, _write_bytes, path, data, has_flushed)
            has_flushed = True
            written += len(data)
            if on_chunk is not None:
                try:
                    on_chunk(len(data))
                except Exception as e:
                    print(f"[HLS] progress callback failed: {e}")

        async def _on_piece(piece: bytes) -> None:
            buf.extend(piece)
            if len(buf) >= _FLUSH_THRESHOLD:
                await _flush()

        try:
            async with session.get(
                url,
                headers=req_headers,
                timeout=self._segment_client_timeout(),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                self._raise_for_status(resp, url)
                await self._read_segment_response(resp, url, byte_range, _on_piece)
                await _flush()
            return written
        except Exception as exc:
            if has_flushed and os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass
            if isinstance(exc, (HlsHttpError, HlsProxyError, WarpProxyError)):
                raise
            self._raise_if_proxy_error(exc)
            raise


# ---------------------------------------------------------------------------
# Self-test / manual smoke check (offline — no network required)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    from app.hls.playlist import is_hls_playlist

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
    hdrs_ref = default_headers(
        "https://cdn.example.com/hls/master.m3u8",
        referer="https://player.example.com/embed/1",
    )
    _check("Origin derived from explicit referer",
           hdrs_ref.get("Origin") == "https://player.example.com"
           and hdrs_ref.get("Referer") == "https://player.example.com/embed/1",
           str(hdrs_ref))
    args = ffmpeg_header_args(hdrs, cookies={"cf_bm": "tok123"})
    _check("-user_agent emitted", "-user_agent" in args, str(args))
    _check("-headers block CRLF terminated",
           args[args.index("-headers") + 1].endswith("\r\n"), str(args))
    _check("UA not duplicated into -headers",
           "User-Agent" not in args[args.index("-headers") + 1], str(args))
    _check("Cookie merged into -headers",
           "Cookie: cf_bm=tok123\r\n" in args[args.index("-headers") + 1], str(args))
    _check("page fetch asks for HTML",
           _PAGE_HEADERS["Accept"].startswith("text/html"), str(_PAGE_HEADERS))

    print("--- proxy & errors ---")
    _check("socks5 detected", is_socks_proxy("socks5://u:p@h:1080"))
    _check("socks5 -> no ffmpeg args", ffmpeg_proxy_args("socks5://u:p@h:1080") == [])
    _check("http -> ffmpeg args",
           ffmpeg_proxy_args("http://h:8080") == ["-http_proxy", "http://h:8080"])
    _check("no proxy -> no args", ffmpeg_proxy_args(None) == [])
    err_403 = HlsHttpError(403, "https://cdn.example.com/x.ts?token=secret", retry_after=5.0)
    _check("HlsHttpError subclasses ValueError & redacts query",
           isinstance(err_403, ValueError)
           and err_403.is_permanent
           and err_403.retry_after == 5.0
           and "secret" not in str(err_403),
           str(err_403))
    perr = HlsProxyError("socks5://user:secretpass@127.0.0.1:1080", RuntimeError("down"))
    _check("HlsProxyError redacts proxy credentials",
           isinstance(perr, ValueError) and "secretpass" not in str(perr),
           str(perr))

    print("--- lenient decoding (production: 'Candidate rejected' on a good page) ---")
    playlist_bytes = b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\n# caf\xe9\nv.m3u8\n"
    _check("no declared charset + cp1252 byte -> decoded",
           is_hls_playlist(decode_text(playlist_bytes)), repr(decode_text(playlist_bytes)))
    _check("mis-declared utf-8 -> decoded",
           is_hls_playlist(decode_text(playlist_bytes, "utf-8")))
    _check("bogus charset name -> decoded, not LookupError",
           is_hls_playlist(decode_text(playlist_bytes, "not-a-real-charset")))
    _check("pure ASCII unchanged",
           decode_text(b"#EXTM3U\n") == "#EXTM3U\n", repr(decode_text(b"#EXTM3U\n")))
    binary_body = bytes(range(256))
    _check("binary body survives decoding",
           not is_hls_playlist(decode_text(binary_body, "utf-8")),
           repr(decode_text(binary_body, "utf-8")[:24]))

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
