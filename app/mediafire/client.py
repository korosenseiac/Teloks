"""
mediafire/client.py — Lightweight MediaFire link resolver and download streamer.

No authentication required. Works by:
1. Fetching the MediaFire file page with a browser User-Agent.
2. Extracting the direct download URL via regex.
3. Streaming the file via standard HTTP GET.

Reference: https://github.com/Locon213/gopeed-extension-mediafire
"""
from __future__ import annotations

import re
from typing import AsyncGenerator, Dict, Optional

import aiohttp

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Primary regex: matches the CDN direct-download URL on the page
_DIRECT_URL_RE = re.compile(r'https://download\d+\.mediafire\.com/[^"\'<>\s]+')

# Fallback: look for any href containing "download"
_FALLBACK_URL_RE = re.compile(r'href="([^"]*download[^"]*)"', re.IGNORECASE)

# Title extraction (filename is in <title>TEXT | MediaFire</title>)
_TITLE_RE = re.compile(r"<title>([^<]+)</title>", re.IGNORECASE)

# Timeout for page fetch / HEAD request
_TIMEOUT = aiohttp.ClientTimeout(total=30)

# Timeout for streaming download (generous — large files)
_STREAM_TIMEOUT = aiohttp.ClientTimeout(total=0, sock_connect=30, sock_read=120)


# ---------------------------------------------------------------------------
# MediaFireClient
# ---------------------------------------------------------------------------

class MediaFireClient:
    """
    Resolves a MediaFire share link into a direct download URL and
    provides an async streaming download generator.
    """

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": _USER_AGENT},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------------------------------------------------------------- resolve

    async def resolve(self, url: str) -> Dict[str, object]:
        """
        Resolve a MediaFire share URL into download metadata.

        Returns
        -------
        dict with keys:
            direct_url : str  — CDN download URL
            filename   : str  — original file name
            size       : int  — file size in bytes (from Content-Length)

        Raises
        ------
        ValueError  if the direct download URL cannot be extracted.
        """
        session = await self._get_session()

        # 1. Fetch the MediaFire page
        async with session.get(url, timeout=_TIMEOUT, allow_redirects=True) as resp:
            if resp.status != 200:
                raise ValueError(
                    f"MediaFire page returned HTTP {resp.status}"
                )
            html = await resp.text()

        # 2. Extract direct download URL
        direct_url = self._extract_direct_url(html)
        if not direct_url:
            raise ValueError(
                "Tidak dapat mengekstrak link muat turun dari halaman MediaFire."
            )

        # 3. Extract filename — prefer the original share URL (has extension),
        #    then <title>, then the CDN direct-download URL.
        filename = (
            self._filename_from_share_url(url)
            or self._extract_filename(html)
            or self._filename_from_url(direct_url)
        )

        # If the title-derived name lacks an extension but the URL-derived
        # name has one, merge them: use the title as the base name + URL ext.
        if filename and "." not in filename:
            url_name = self._filename_from_share_url(url) or self._filename_from_url(direct_url)
            if url_name and "." in url_name:
                from os.path import splitext
                _, ext = splitext(url_name)
                filename = filename + ext

        # 4. GET file size via Content-Length header (follow redirects)
        size = 0
        try:
            async with session.head(
                direct_url, timeout=_TIMEOUT, allow_redirects=True
            ) as head_resp:
                cl = head_resp.headers.get("Content-Length")
                if cl:
                    size = int(cl)
                    
                # If HEAD doesn't return Content-Length, try a range GET
                if size == 0:
                    async with session.get(
                        direct_url, timeout=_TIMEOUT, allow_redirects=True,
                        headers={"Range": "bytes=0-0"}
                    ) as range_resp:
                        cr = range_resp.headers.get("Content-Range", "")
                        # Content-Range: bytes 0-0/12345678
                        if "/" in cr:
                            try:
                                size = int(cr.split("/")[-1])
                            except (ValueError, IndexError):
                                pass
        except Exception as e:
            print(f"[MediaFire] HEAD/size check error: {e}")

        return {
            "direct_url": direct_url,
            "filename": filename,
            "size": size,
        }

    # --------------------------------------------------------- download_stream

    async def download_stream(
        self,
        direct_url: str,
        chunk_size: int = 2 * 1024 * 1024,
    ) -> AsyncGenerator[bytes, None]:
        """
        Async generator that yields *chunk_size* byte chunks from *direct_url*.

        The caller is responsible for consuming all chunks; the underlying
        HTTP response is closed automatically when the generator is exhausted
        or garbage-collected.
        """
        session = await self._get_session()
        async with session.get(
            direct_url, timeout=_STREAM_TIMEOUT, allow_redirects=True
        ) as resp:
            if resp.status not in (200, 206):
                raise ValueError(f"MediaFire download returned HTTP {resp.status}")
            async for chunk in resp.content.iter_chunked(chunk_size):
                if chunk:
                    yield chunk

    # -------------------------------------------------------- private helpers

    @staticmethod
    def _extract_direct_url(html: str) -> Optional[str]:
        """Try primary then fallback regex to find the direct download URL."""
        m = _DIRECT_URL_RE.search(html)
        if m:
            return m.group(0)
        m = _FALLBACK_URL_RE.search(html)
        if m:
            return m.group(1)
        return None

    @staticmethod
    def _extract_filename(html: str) -> Optional[str]:
        """Extract filename from the <title> tag."""
        m = _TITLE_RE.search(html)
        if m:
            raw = m.group(1).strip()
            # Remove "… | MediaFire" suffix added by the site
            for suffix in (" - MediaFire", " | MediaFire"):
                if raw.endswith(suffix):
                    raw = raw[: -len(suffix)].strip()
            if raw:
                return raw
        return None

    @staticmethod
    def _filename_from_share_url(url: str) -> Optional[str]:
        """
        Extract filename from a MediaFire share URL.

        Typical pattern:  /file/<id>/<filename>/file
        The filename segment (with extension) is the second-to-last part.
        """
        try:
            from urllib.parse import urlparse, unquote
            parts = [p for p in urlparse(url).path.split("/") if p]
            # e.g. ['file', 'k7x4ekou4owf26e', 'CWL_Liya_Punk.zip', 'file']
            if len(parts) >= 3 and parts[0] == "file":
                candidate = unquote(parts[2])
                if "." in candidate:  # must have an extension
                    return candidate
        except Exception:
            pass
        return None

    @staticmethod
    def _filename_from_url(url: str) -> str:
        """Fallback: derive a filename from the URL path."""
        try:
            from urllib.parse import urlparse, unquote
            path = urlparse(url).path
            name = unquote(path.split("/")[-1])
            if name:
                return name
        except Exception:
            pass
        return "mediafire_download"
