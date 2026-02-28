"""
TeraBoxClient — async Python port of the TeraBox JS API (TeraBoxApp class).

Reference: https://seiya-npm.github.io/terabox-api/html/api.js.html
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import ssl
import urllib.parse
from typing import Any, Dict, List, Optional

import aiohttp


# ---------------------------------------------------------------------------
# SSL context that excludes ECDHE-RSA-AES128-SHA (matches JS buildConnector)
# ---------------------------------------------------------------------------

def _build_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    # Exclude the cipher that causes issues matching JS ':!ECDHE-RSA-AES128-SHA'
    try:
        ciphers = ssl._DEFAULT_CIPHERS if hasattr(ssl, "_DEFAULT_CIPHERS") else "DEFAULT"
        # Build a list without the problematic cipher
        excluded = "ECDHE-RSA-AES128-SHA"
        ciphers_str = ":".join(
            c for c in ctx.get_ciphers() if c.get("name") != excluded
        )
        if ciphers_str:
            ctx.set_ciphers(ciphers_str)
    except Exception:
        pass  # If cipher manipulation fails, continue with defaults
    return ctx


_TERABOX_SSL_CTX = _build_ssl_context()

# ---------------------------------------------------------------------------
# Connector factories
# ---------------------------------------------------------------------------

def _default_connector() -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(ssl=None)  # use default ssl


def _restricted_connector() -> aiohttp.TCPConnector:
    """Used for short_url_info / short_url_list (cipher-restricted TLS)."""
    return aiohttp.TCPConnector(ssl=_TERABOX_SSL_CTX)


# ---------------------------------------------------------------------------
# TeraBoxClient
# ---------------------------------------------------------------------------

class TeraBoxClient:
    """
    Async client for TeraBox public API.

    Usage::

        client = TeraBoxClient(ndus_token="…")
        await client.update_app_data()
        await client.check_login()
        info = await client.short_url_info("1abcXYZ")
    """

    DEFAULT_DOMAIN = "https://www.terabox.com"
    USER_AGENT = (
        "terabox;1.40.0.132;PC;PC-Windows;10.0.26100;WindowsTeraBox"
    )

    # ------------------------------------------------------------------ init

    def __init__(self, ndus_token: str) -> None:
        self.ndus_token = ndus_token
        self.domain = self.DEFAULT_DOMAIN
        # Cookie string sent with every request
        self._cookie_str = f"lang=en; ndus={ndus_token}"
        # App-level tokens obtained from update_app_data()
        self.js_token: str = ""
        self.bds_token: str = ""
        self.logid: str = ""
        self.csrf: str = ""
        self.pcf_token: str = ""
        self.account_id: str = ""
        # Shared aiohttp session (created lazily)
        self._session: Optional[aiohttp.ClientSession] = None

    # ---------------------------------------------------------------- helpers

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.USER_AGENT,
            "Cookie": self._cookie_str,
            "Referer": self.domain + "/",
        }

    def _session_for(self, restricted: bool = False) -> aiohttp.ClientSession:
        """Return a fresh one-shot session (each call creates a new one)."""
        connector = _restricted_connector() if restricted else _default_connector()
        return aiohttp.ClientSession(
            connector=connector,
            headers=self._headers,
        )

    async def close(self) -> None:
        """Close the shared session if open."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # -------------------------------------------------------- update_app_data

    async def update_app_data(self, custom_path: Optional[str] = None) -> bool:
        """
        Fetch the TeraBox main page and extract JS/BD tokens.
        Mirrors TeraBoxApp#updateAppData (lines 491–575 of api.js).
        """
        path = custom_path or "main"
        url = f"{self.domain}/{path}"
        try:
            async with self._session_for() as session:
                async with session.get(
                    url,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    # Handle redirect to different hostname
                    if resp.url.host != urllib.parse.urlparse(self.domain).hostname:
                        self.domain = f"{resp.url.scheme}://{resp.url.host}"
                        self._cookie_str = f"lang=en; ndus={self.ndus_token}"

                    html = await resp.text(errors="replace")

            # Extract jsToken
            js_token_match = re.search(r'jsToken\s*=\s*["\']([^"\']+)["\']', html)
            if js_token_match:
                self.js_token = js_token_match.group(1)

            # Extract templateData JSON blob
            template_match = re.search(
                r'locals\.templateData\s*=\s*(\{.*?\});\s*\n', html, re.DOTALL
            )
            if not template_match:
                # Fallback: look for window.templateData
                template_match = re.search(
                    r'window\.templateData\s*=\s*(\{.*?\});', html, re.DOTALL
                )
            if template_match:
                try:
                    data = json.loads(template_match.group(1))
                    self.csrf = data.get("csrf", "")
                    self.pcf_token = data.get("pcfToken", "")
                    self.bds_token = data.get("bdstoken", "")
                    self.account_id = str(data.get("uk", ""))
                    self.logid = data.get("logid", "")
                except json.JSONDecodeError:
                    pass

            # Alternative: look for bdstoken directly
            if not self.bds_token:
                bds_match = re.search(r'"bdstoken"\s*:\s*"([^"]+)"', html)
                if bds_match:
                    self.bds_token = bds_match.group(1)

            return True

        except Exception as e:
            print(f"[TeraBox] update_app_data error: {e}")
            return False

    # ----------------------------------------------------------- check_login

    async def check_login(self) -> bool:
        """
        Verify the NDUS token is still valid.
        Mirrors TeraBoxApp#checkLogin (lines 696–729 of api.js).
        Returns True if logged in, False otherwise.
        """
        url = f"{self.domain}/api/check/login"
        try:
            async with self._session_for() as session:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=15),
                    allow_redirects=False,
                ) as resp:
                    # 302 may carry region-domain-prefix header
                    new_prefix = resp.headers.get("x-redirect-domain", "")
                    if new_prefix:
                        self.domain = f"https://{new_prefix}.terabox.com"
                    data = await resp.json(content_type=None)
                    errno = data.get("errno", -1)
                    return errno == 0
        except Exception as e:
            print(f"[TeraBox] check_login error: {e}")
            return False

    # -------------------------------------------------------- short_url_info

    async def short_url_info(self, surl: str) -> Optional[Dict[str, Any]]:
        """
        Fetch share metadata (share_id, uk, file list).
        Mirrors TeraBoxApp#shortUrlInfo (lines 2018–2050 of api.js).
        Uses cipher-restricted TLS connector.
        """
        url = (
            f"{self.domain}/api/shorturlinfo"
            f"?shorturl=1{urllib.parse.quote(surl)}&root=1"
        )
        try:
            async with self._session_for(restricted=True) as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] short_url_info error: {e}")
            return None

    # -------------------------------------------------------- short_url_list

    async def short_url_list(
        self, surl: str, remote_dir: str = "", page: int = 1
    ) -> Optional[Dict[str, Any]]:
        """
        List files in a TeraBox share.
        Mirrors TeraBoxApp#shortUrlList (lines 2061–2112 of api.js).
        Fetches jsToken first if not already obtained.
        """
        if not self.js_token:
            await self.update_app_data()

        params = {
            "app_id": "250528",
            "web": "1",
            "channel": "dubox",
            "clienttype": "0",
            "jsToken": self.js_token,
            "shorturl": surl,
            "by": "name",
            "order": "asc",
            "num": "20000",
            "dir": remote_dir,
            "page": str(page),
        }
        if not remote_dir:
            params["root"] = "1"

        url = f"{self.domain}/share/list?" + urllib.parse.urlencode(params)
        try:
            async with self._session_for(restricted=True) as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] short_url_list error: {e}")
            return None

    # -------------------------------------------------------- share_transfer

    async def share_transfer(
        self,
        share_id: str,
        from_uk: str,
        fs_ids: List[int],
        dest_path: str = "/",
    ) -> Optional[Dict[str, Any]]:
        """
        Copy shared files into *your* TeraBox account at dest_path.
        Mirrors TeraBoxApp#shareTransfer (lines 2496–2545 of api.js).
        Retries once on errno=400810 (token refresh needed).
        """
        for attempt in range(2):
            if not self.js_token or not self.bds_token:
                await self.update_app_data()

            params = urllib.parse.urlencode(
                {
                    "app_id": "250528",
                    "web": "1",
                    "channel": "dubox",
                    "clienttype": "0",
                    "jsToken": self.js_token,
                    "shareid": share_id,
                    "from": from_uk,
                    "ondup": "newcopy",
                    "async": "1",
                    "bdstoken": self.bds_token,
                    "logid": self.logid,
                }
            )
            url = f"{self.domain}/share/transfer?{params}"
            body = urllib.parse.urlencode(
                {
                    "fsidlist": json.dumps(fs_ids),
                    "path": dest_path,
                }
            )
            try:
                async with self._session_for() as session:
                    async with session.post(
                        url,
                        data=body,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=aiohttp.ClientTimeout(total=60),
                    ) as resp:
                        result = await resp.json(content_type=None)
                        errno = result.get("errno", -1)
                        if errno == 400810 and attempt == 0:
                            # Refresh tokens and retry
                            self.js_token = ""
                            self.bds_token = ""
                            await self.update_app_data()
                            continue
                        return result
            except Exception as e:
                print(f"[TeraBox] share_transfer error: {e}")
                if attempt == 1:
                    return None
        return None

    # --------------------------------------------------------------- create_dir

    async def create_dir(self, remote_dir: str) -> Optional[Dict[str, Any]]:
        """
        Create a directory in your TeraBox account.
        Mirrors TeraBoxApp#createDir (lines 1698–1729 of api.js).
        """
        params = urllib.parse.urlencode(
            {
                "a": "commit",
                "app_id": "250528",
                "web": "1",
                "channel": "dubox",
                "clienttype": "0",
                "jsToken": self.js_token,
                "bdstoken": self.bds_token,
            }
        )
        url = f"{self.domain}/api/create?{params}"
        body = urllib.parse.urlencode(
            {
                "path": remote_dir,
                "isdir": "1",
                "block_list": "[]",
            }
        )
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] create_dir error: {e}")
            return None

    # -------------------------------------------------------------- get_remote_dir

    async def get_remote_dir(
        self, remote_dir: str, page: int = 1
    ) -> Optional[Dict[str, Any]]:
        """
        List files inside one of your own TeraBox directories.
        Mirrors TeraBoxApp#getRemoteDir (lines 1230–1263 of api.js).
        """
        params = urllib.parse.urlencode(
            {
                "app_id": "250528",
                "web": "1",
                "channel": "dubox",
                "clienttype": "0",
                "jsToken": self.js_token,
                "bdstoken": self.bds_token,
            }
        )
        url = f"{self.domain}/api/list?{params}"
        body = urllib.parse.urlencode(
            {
                "order": "name",
                "desc": "0",
                "dir": remote_dir,
                "num": "20000",
                "page": str(page),
                "showempty": "0",
            }
        )
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] get_remote_dir error: {e}")
            return None

    # ----------------------------------------------------------- get_home_info

    async def get_home_info(self) -> Optional[Dict[str, Any]]:
        """
        Fetch sign data needed for download link generation.
        Mirrors TeraBoxApp#getHomeInfo (lines 2223–2248 of api.js).
        """
        url = f"{self.domain}/api/home/info"
        try:
            async with self._session_for() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] get_home_info error: {e}")
            return None

    # --------------------------------------------------------- sign_download

    @staticmethod
    def sign_download(s1: str, s2: str) -> str:
        """
        RC4-like signing algorithm.
        Python port of TeraBoxApp.signDownload (lines 108–141 of api.js).

        Parameters
        ----------
        s1 : str   Input data string (sign1 from home/info).
        s2 : str   Key string (sign3 from home/info).

        Returns
        -------
        str        Base64-encoded signed bytes.
        """
        # Key-scheduling algorithm (KSA)
        d = bytearray(range(256))
        b = bytearray(ord(s2[i % len(s2)]) for i in range(256))

        j = 0
        for i in range(256):
            j = (j + d[i] + b[i]) % 256
            d[i], d[j] = d[j], d[i]

        # Pseudo-random generation algorithm (PRGA) XOR with s1
        f = bytearray()
        k = 0
        ll = 0
        for i in range(len(s1)):
            ll = (ll + 1) % 256
            k = (k + d[ll]) % 256
            d[ll], d[k] = d[k], d[ll]
            f.append(ord(s1[i]) ^ d[(d[ll] + d[k]) % 256])

        return base64.b64encode(bytes(f)).decode()

    # -------------------------------------------------------------- download

    async def download(self, fs_ids: List[int]) -> Optional[Dict[str, Any]]:
        """
        Get direct download links (dlink) for a list of your own fs_ids.
        Mirrors TeraBoxApp#download (lines 2258–2297 of api.js).
        """
        home_info = await self.get_home_info()
        if not home_info:
            return None

        sign1 = home_info.get("sign1", "")
        sign3 = home_info.get("sign3", "")
        timestamp = home_info.get("timestamp", "")
        sign_b = self.sign_download(sign3, sign1)

        params = urllib.parse.urlencode(
            {
                "app_id": "250528",
                "web": "1",
                "channel": "dubox",
                "clienttype": "0",
                "jsToken": self.js_token,
                "bdstoken": self.bds_token,
            }
        )
        url = f"{self.domain}/api/download?{params}"
        body = urllib.parse.urlencode(
            {
                "fidlist": json.dumps(fs_ids),
                "type": "dlink",
                "vip": "2",
                "sign": sign_b,
                "timestamp": str(timestamp),
                "need_speed": "1",
            }
        )
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] download error: {e}")
            return None

    # -------------------------------------------------------------- filemanager

    async def filemanager(
        self, operation: str, fm_params: List[Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Perform a file-manager operation (e.g. delete) on your account.
        Mirrors TeraBoxApp#filemanager (lines 1824–1884 of api.js).

        Examples
        --------
        Delete a folder::

            await client.filemanager("delete", ["/terabox_temp_abc123"])
        """
        params = urllib.parse.urlencode(
            {
                "app_id": "250528",
                "web": "1",
                "channel": "dubox",
                "clienttype": "0",
                "opera": operation,
                "jsToken": self.js_token,
                "bdstoken": self.bds_token,
                "logid": self.logid,
            }
        )
        url = f"{self.domain}/api/filemanager?{params}"
        body = urllib.parse.urlencode({"filelist": json.dumps(fm_params)})
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    return await resp.json(content_type=None)
        except Exception as e:
            print(f"[TeraBox] filemanager error: {e}")
            return None

    # -------------------------------------------------- download_file_stream

    async def download_file_stream(
        self, dlink: str, chunk_size: int = 512 * 1024
    ):
        """
        Async generator that yields raw bytes chunks from a TeraBox dlink URL.

        Usage::

            async for chunk in client.download_file_stream(dlink):
                process(chunk)
        """
        headers = {
            "User-Agent": self.USER_AGENT,
            "Cookie": self._cookie_str,
            "Referer": self.domain + "/",
        }
        async with self._session_for() as session:
            async with session.get(
                dlink,
                headers=headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=None, connect=30, sock_read=60),
            ) as resp:
                resp.raise_for_status()
                async for chunk in resp.content.iter_chunked(chunk_size):
                    if chunk:
                        yield chunk
