"""
direct/client.py — HTTP client wrapper for direct-link downloads.

Validates URLs, extracts metadata, and provides streaming downloads
for any HTTP/HTTPS URL.
"""
from __future__ import annotations

import os
import re
from typing import AsyncGenerator, Dict, Optional
from urllib.parse import urlparse, unquote

import aiohttp

from app.warp import (
    WarpProxyError,
    create_aiohttp_connector,
    ensure_proxy_ready,
    format_warp_proxy_error,
    is_warp_proxy_error,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Timeout for HEAD requests / initial checks
_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Timeout for streaming downloads (generous — large files)
# sock_read increased to handle slower proxy connections without timeout
_STREAM_TIMEOUT = aiohttp.ClientTimeout(total=0, sock_connect=60, sock_read=300)


# ---------------------------------------------------------------------------
# DirectLinkClient
# ---------------------------------------------------------------------------

class DirectLinkClient:
    """
    HTTP client for direct-link downloads.
    
    Validates URLs, extracts metadata (file size, filename),
    and provides streaming download capability.
    """

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_proxy_key: Optional[tuple] = None
        self._using_warp: bool = False

    @property
    def using_warp(self) -> bool:
        """Whether the active HTTP session is routed through WARP wireproxy."""
        return self._using_warp

    async def _get_session(self) -> aiohttp.ClientSession:
        warp_cfg = await ensure_proxy_ready(
            require_http=False,
            require_socks5=True,
            feature="Direct",
        )

        if warp_cfg is not None:
            desired_key = ("warp", warp_cfg.socks5_url)
            proxy_url = None
        else:
            proxy_url = self._get_proxy_url()
            desired_key = ("proxy_txt", proxy_url) if proxy_url else ("direct", None)

        if self._session is not None and not self._session.closed:
            if self._session_proxy_key == desired_key:
                return self._session
            await self.close()

        if warp_cfg is not None:
            try:
                connector = create_aiohttp_connector(
                    limit_per_host=10,
                    limit=100,
                )
                if connector is None:
                    from aiohttp_socks import ProxyConnector
                    connector = ProxyConnector.from_url(
                        warp_cfg.socks5_url,
                        rdns=True,
                        limit_per_host=10,
                        limit=100,
                    )
            except WarpProxyError:
                raise
            except Exception as exc:
                raise WarpProxyError(
                    format_warp_proxy_error(exc, feature="Direct")
                ) from exc

            try:
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": _USER_AGENT},
                    connector=connector,
                    read_bufsize=2 * 1024 * 1024,  # 2MB read buffer for better throughput
                    trust_env=False,
                )
            except Exception:
                await connector.close()
                raise
            self._using_warp = True
            self._session_proxy_key = desired_key
            return self._session

        # WARP proxy is OFF — preserve existing proxy.txt / direct behavior
        if proxy_url:
            try:
                from aiohttp_socks import ProxyConnector
                connector = ProxyConnector.from_url(
                    proxy_url,
                    limit_per_host=10,  # Increased from 5 for better throughput
                    limit=100,  # Total connection limit
                )
            except ImportError:
                print("[DirectLink] 'aiohttp_socks' not installed. Ignoring SOCKS5 proxy.")
                connector = aiohttp.TCPConnector(
                    limit_per_host=10,
                    limit=100,
                )
        else:
            connector = aiohttp.TCPConnector(
                limit_per_host=10,
                limit=100,
            )

        try:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": _USER_AGENT},
                connector=connector,
                read_bufsize=2 * 1024 * 1024,  # 2MB read buffer for better throughput
            )
        except Exception:
            await connector.close()
            raise
        self._using_warp = False
        self._session_proxy_key = desired_key
        return self._session

    async def close(self) -> None:
        """Close the session and its connector cleanly."""
        session = self._session
        self._session = None
        self._session_proxy_key = None
        self._using_warp = False
        if session is not None and not session.closed:
            await session.close()

    def _get_proxy_url(self) -> Optional[str]:
        """Read proxy.txt from project root and construct proxy URL."""
        # Project root is 3 levels up from app/direct/client.py
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        proxy_file = os.path.join(root_dir, "proxy.txt")
        
        if not os.path.exists(proxy_file):
            # Fallback for deployed path if needed
            if os.path.exists("/opt/telegram-forwarder-bot/proxy.txt"):
                proxy_file = "/opt/telegram-forwarder-bot/proxy.txt"
            else:
                return None
        
        try:
            with open(proxy_file, "r") as f:
                content = f.read()
            
            host = port = username = password = None
            for line in content.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    key, val = line.split(":", 1)
                    key = key.strip().lower()
                    val = val.strip()
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
                    from urllib.parse import quote
                    user_quoted = quote(username)
                    pass_quoted = quote(password)
                    return f"socks5://{user_quoted}:{pass_quoted}@{host}:{port}"
                return f"socks5://{host}:{port}"
        except Exception as e:
            print(f"[DirectLink] Error reading proxy.txt: {e}")
        return None

    # ---------------------------------------------------------------- resolve

    async def resolve(self, url: str) -> Dict[str, object]:
        """
        Resolve a URL into download metadata.

        Parameters
        ----------
        url : str
            The HTTP/HTTPS URL to resolve.

        Returns
        -------
        dict with keys:
            url       : str  — the final URL (after redirects)
            filename  : str  — extracted filename
            size      : int  — file size in bytes

        Raises
        ------
        ValueError  if the URL is invalid or inaccessible.
        aiohttp.ClientError  if the HTTP request fails.
        """
        # Validate URL format before opening any network session
        parsed = urlparse(url)
        if not parsed.scheme or parsed.scheme not in ("http", "https"):
            raise ValueError(f"Invalid URL scheme: {parsed.scheme}")
        if not parsed.netloc:
            raise ValueError(f"Invalid URL: no hostname")

        session = await self._get_session()

        # HEAD with redirects to find final URL and metadata
        try:
            async with session.head(
                url,
                timeout=_TIMEOUT,
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status >= 400:
                    raise ValueError(
                        f"HTTP {resp.status} for {url}"
                    )

                # Get final URL (after redirects)
                final_url = str(resp.url)

                # Extract file size from Content-Length
                file_size = 0
                if "Content-Length" in resp.headers:
                    try:
                        file_size = int(resp.headers["Content-Length"])
                    except (ValueError, TypeError):
                        pass

                # Extract filename
                filename = self._extract_filename(
                    resp, url, final_url
                )

                return {
                    "url": final_url,
                    "filename": filename,
                    "size": file_size,
                }

        except WarpProxyError:
            raise
        except Exception as e:
            if self._using_warp and is_warp_proxy_error(e):
                raise WarpProxyError(
                    format_warp_proxy_error(e, feature="Direct")
                ) from e
            if isinstance(e, aiohttp.ClientError):
                raise ValueError(f"Failed to resolve URL: {e}") from e
            raise

    # --------------------------------------------------------- stream download

    async def download_stream(
        self, url: str, chunk_size: int = 1024 * 1024, start_offset: int = 0
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream download the file from the URL.

        Parameters
        ----------
        url : str
            The HTTP/HTTPS URL to download.
        chunk_size : int
            Size of chunks to yield (default 1 MB).
        start_offset : int
            Offset in bytes to start downloading from.

        Yields
        ------
        bytes
            Chunks of file data.

        Raises
        ------
        aiohttp.ClientError  if the HTTP request fails.
        WarpProxyError  if WARP proxy is ON and wireproxy fails.
        """
        session = await self._get_session()

        headers = {}
        if start_offset > 0:
            headers["Range"] = f"bytes={start_offset}-"

        try:
            async with session.get(
                url,
                headers=headers,
                timeout=_STREAM_TIMEOUT,
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status >= 400:
                    raise ValueError(f"HTTP {resp.status} for {url}")

                # Stream data in chunks
                async for chunk in resp.content.iter_chunked(chunk_size):
                    if chunk:
                        yield chunk
        except WarpProxyError:
            raise
        except Exception as e:
            if self._using_warp and is_warp_proxy_error(e):
                raise WarpProxyError(
                    format_warp_proxy_error(e, feature="Direct")
                ) from e
            raise

    # --------------------------------------------------------- helpers

    def _extract_filename(
        self, resp: aiohttp.ClientResponse, original_url: str, final_url: str
    ) -> str:
        """
        Extract filename from Content-Disposition header, URL path, or use default.
        """
        # 1. Try Content-Disposition header
        content_disp = resp.headers.get("Content-Disposition", "")
        if content_disp:
            match = re.search(r'filename\*?=(?:"([^"]*)"|([^;\s]*))', content_disp)
            if match:
                filename = match.group(1) or match.group(2)
                filename = unquote(filename).strip()
                if filename:
                    return filename

        # 2. Try URL path
        parsed = urlparse(final_url)
        if parsed.path:
            filename = parsed.path.split("/")[-1]
            # Remove query string
            if "?" in filename:
                filename = filename.split("?")[0]
            filename = unquote(filename).strip()
            if filename and filename != "/":
                return filename

        # 3. Fallback to generic name with guessed extension
        content_type = resp.headers.get("Content-Type", "")
        ext = self._guess_extension(content_type)
        return f"file{ext}"

    def _guess_extension(self, content_type: str) -> str:
        """Guess file extension from Content-Type header."""
        type_map = {
            "application/pdf": ".pdf",
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "video/mp4": ".mp4",
            "video/mpeg": ".mpg",
            "audio/mpeg": ".mp3",
            "audio/wav": ".wav",
            "application/zip": ".zip",
            "application/x-rar-compressed": ".rar",
            "text/plain": ".txt",
        }
        for mime, ext in type_map.items():
            if mime in content_type.lower():
                return ext
        return ""
