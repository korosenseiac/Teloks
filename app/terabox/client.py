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
import yarl


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
        # Saved cookie jar from successful share-page scrape
        self._share_jar: Optional[aiohttp.CookieJar] = None
        # randsk token from share verification (needed as cookie for share/list)
        self._randsk: str = ""
        # Domain that actually served the share data (after redirects)
        self._share_domain: str = ""
        # Share auth tokens from shorturlinfo (needed for share/list)
        self._share_uk: str = ""
        self._share_id: str = ""
        self._share_sign: str = ""
        self._share_timestamp: str = ""

    # ---------------------------------------------------------------- helpers

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.USER_AGENT,
            "Cookie": self._cookie_str,
            "Referer": self.domain + "/",
            "Accept-Encoding": "identity",
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

    # --------------------------------------------------------- _extract_js_token

    @staticmethod
    def _extract_js_token(html: str) -> str:
        """
        Extract the actual jsToken value from an HTML page.

        TeraBox embeds jsToken in two known formats:
          1. jsToken = "HEXSTRING"                                       → direct
          2. jsToken = "function%20fn%28a%29...fn%28%22HEXSTRING%22%29"  → URL-encoded wrapper

        In case (2) the value is URL-encoded inside the quotes, so we
        URL-decode it first, then extract the fn() argument.
        """
        # Step 0: Handle eval(decodeURIComponent(`...jsToken...`))
        duri_match = re.search(r'decodeURIComponent\(`([^`]*jsToken[^`]*)`\)', html)
        if duri_match:
            decoded_block = urllib.parse.unquote(duri_match.group(1))
            inner = re.search(r'fn\("([A-Fa-f0-9]{32,})"\)', decoded_block)
            if inner:
                return inner.group(1)

        # Step 1: Try fn("HEX") directly in the raw HTML (unlikely but free)
        fn_match = re.search(r'fn\(\\?"([A-Fa-f0-9]{32,})\\?"\)', html)
        if fn_match:
            return fn_match.group(1)

        # Step 2: Broad capture of jsToken = "..." (may be URL-encoded wrapper)
        m = (
            re.search(r'jsToken\s*=\s*"([^"]+)"', html)
            or re.search(r"jsToken\s*=\s*'([^']+)'", html)
            or re.search(r'"jsToken"\s*:\s*"([^"]+)"', html)
        )
        if not m:
            return ""

        raw = m.group(1)

        # Step 3: URL-decode the captured value (may be encoded once or twice)
        decoded = urllib.parse.unquote(raw)
        decoded2 = urllib.parse.unquote(decoded)
        print(f"[TB] _extract_js_token raw_len={len(raw)} decoded_preview={decoded2[:80]!r}")

        # Step 4: Look for fn("HEXSTRING") in the decoded value
        for candidate in (decoded2, decoded, raw):
            inner = re.search(r'fn\("([A-Fa-f0-9]{32,})"\)', candidate)
            if inner:
                return inner.group(1)

        # Step 5: If decoded value is purely hex (direct token), return it
        if re.fullmatch(r'[A-Fa-f0-9]{32,}', decoded2):
            return decoded2

        # Step 6: If it looks like code / not a token, reject it.
        if "function" in decoded2 or "{" in decoded2:
            # Last resort: try to find any long hex string in the decoded value
            hex_match = re.search(r'([A-Fa-f0-9]{64,})', decoded2)
            if hex_match:
                return hex_match.group(1)
            return ""

        return raw

    # -------------------------------------------------------- update_app_data

    async def update_app_data(self, custom_path: Optional[str] = None) -> bool:
        """
        Fetch the TeraBox main page and extract JS/BD tokens.
        Mirrors TeraBoxApp#updateAppData (lines 491–575 of api.js).
        """
        path = custom_path or "main"
        url = f"{self.domain}/{path}"
        print(f"[TB] update_app_data → GET {url}")
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
                        print(f"[TB] update_app_data → domain redirected to {self.domain}")

                    html = await resp.text(errors="replace")
                    print(f"[TB] update_app_data ← HTTP {resp.status} | html_len={len(html)}")

            # Extract jsToken — try multiple patterns
            self.js_token = self._extract_js_token(html)

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

            print(
                f"[TB] update_app_data → tokens: "
                f"js_token={'OK' if self.js_token else 'MISSING'} "
                f"bds_token={'OK' if self.bds_token else 'MISSING'} "
                f"logid={self.logid!r} csrf={self.csrf!r}"
            )

            # If jsToken still missing, try fetching /disk/home page as fallback
            if not self.js_token and not custom_path:
                print("[TB] update_app_data → jsToken MISSING, retrying with /disk/home")
                await self.update_app_data("disk/home")

            return True

        except Exception as e:
            print(f"[TB] update_app_data error: {e}")
            return False

    # ----------------------------------------------------------- check_login

    async def check_login(self) -> bool:
        """
        Verify the NDUS token is still valid.
        Mirrors TeraBoxApp#checkLogin (lines 696–729 of api.js).
        Returns True if logged in, False otherwise.
        """
        url = f"{self.domain}/api/check/login"
        print(f"[TB] check_login → GET {url}")
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
                        print(f"[TB] check_login → domain redirected to {self.domain}")
                    data = await resp.json(content_type=None)
                    print(f"[TB] check_login ← HTTP {resp.status} | {data}")
                    errno = data.get("errno", -1)
                    return errno == 0
        except Exception as e:
            print(f"[TB] check_login error: {e}")
            return False

    # -------------------------------------------------------- short_url_info

    async def short_url_info(
        self, surl: str, share_host: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch share metadata (share_id, uk, file list).

        Multi-strategy approach to bypass datacenter IP anti-bot:
        1. Browser-session scrape with cookie chain (WAP, sharing/link, /s/)
        2. REST API with different configurations (UA, clienttype, jsToken)

        Parameters
        ----------
        surl : str
            The raw code from the share URL path.
        share_host : str, optional
            Hostname from the original share URL.
        """
        base = f"https://{share_host}" if share_host else self.domain

        # -------- Strategy 1: Browser-session scrape --------
        page_data = await self._scrape_share_page(base, surl)
        if page_data and page_data.get("shareid"):
            print(f"[TB] short_url_info → got data from scrape")
            self._apply_randsk(page_data)
            return {**page_data, "errno": 0}

        # -------- Strategy 2: REST API with multiple configs --------
        browser_ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )
        # Ensure jsToken is available
        if not self.js_token:
            if share_host and share_host not in self.domain:
                await self._fetch_js_token_from(base)
            else:
                await self.update_app_data()

        api_configs = [
            # (extra_params, user_agent, description)
            (
                {"jsToken": self.js_token, "web": "1", "channel": "dubox", "clienttype": "0"},
                self.USER_AGENT, "desktop+jsToken",
            ),
            ({}, browser_ua, "browser_minimal"),
            ({"clienttype": "5"}, browser_ua, "mobile_ct"),
        ]

        last_data = None
        for extra, ua, desc in api_configs:
            params = {"app_id": "250528", "shorturl": surl, "root": "1", **extra}
            url = f"{base}/api/shorturlinfo?" + urllib.parse.urlencode(params)
            print(f"[TB] short_url_info({desc}) → GET {url}")
            try:
                req_headers = {
                    "User-Agent": ua,
                    "Cookie": self._cookie_str,
                    "Referer": base + "/",
                    "Accept-Encoding": "identity",
                }
                async with self._session_for(restricted=True) as session:
                    async with session.get(
                        url, headers=req_headers,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        data = await resp.json(content_type=None)
                        errno = data.get("errno", -1)
                        print(f"[TB] short_url_info({desc}) ← HTTP {resp.status} | errno={errno} | keys={list(data.keys())}")
                        if errno == 0:
                            return data
                        last_data = data
                        if errno != 400210:
                            return data  # Non-verify error, stop trying
            except Exception as e:
                print(f"[TB] short_url_info({desc}) error: {e}")
                continue

        print(f"[TB] short_url_info → all strategies exhausted")
        return last_data

    def _apply_randsk(self, data: Dict[str, Any]) -> None:
        """Extract share auth tokens from an API response and store them."""
        randsk = data.get("randsk", "")
        if randsk:
            self._randsk = randsk
            print(f"[TB] _apply_share_tokens → stored randsk (len={len(randsk)})")
            if self._share_jar and self._share_domain:
                decoded_randsk = urllib.parse.unquote(randsk)
                self._share_jar.update_cookies(
                    {"TSID": decoded_randsk, "randsk": decoded_randsk},
                    yarl.URL(self._share_domain),
                )
                print(f"[TB] _apply_share_tokens → injected into jar for {self._share_domain}")

        # Store share identifiers needed for share/list
        for src, attr in [
            ("uk", "_share_uk"), ("uk_str", "_share_uk"),
            ("shareid", "_share_id"),
            ("sign", "_share_sign"),
            ("timestamp", "_share_timestamp"),
        ]:
            val = data.get(src)
            if val and not getattr(self, attr, ""):
                setattr(self, attr, str(val))

        print(
            f"[TB] _apply_share_tokens → uk={self._share_uk} "
            f"shareid={self._share_id} sign={self._share_sign[:8] if self._share_sign else '?'}... "
            f"ts={self._share_timestamp}"
        )

    async def _scrape_share_page(
        self, base_url: str, surl: str
    ) -> Optional[Dict[str, Any]]:
        """
        Multi-strategy scrape of TeraBox share pages.

        Uses browser-like sessions with cookie accumulation to avoid
        triggering anti-bot protections.  Tries multiple endpoints
        (WAP, sharing/link, /s/) across multiple domains.
        """
        browser_ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )
        mobile_ua = (
            "Mozilla/5.0 (Linux; Android 13; SM-S918B) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Mobile Safari/537.36"
        )

        # Build list of domains to try
        domains = [base_url]
        for alt in ("https://www.terabox.com", "https://1024terabox.com"):
            if alt.rstrip("/") != base_url.rstrip("/"):
                domains.append(alt)

        for domain in domains:
            print(f"[TB] _scrape → domain {domain}")
            result = await self._scrape_one_domain(domain, surl, browser_ua, mobile_ua)
            if result and result.get("shareid"):
                return result

        print(f"[TB] _scrape → all domains/endpoints failed")
        return None

    async def _scrape_one_domain(
        self, domain: str, surl: str,
        browser_ua: str, mobile_ua: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Try multiple share-page endpoints on *one* TeraBox domain,
        all sharing the same ``CookieJar`` so that session cookies
        accumulate naturally (like a real browser).
        """
        # Cookie jar pre-seeded with NDUS
        jar = aiohttp.CookieJar(unsafe=True)
        jar.update_cookies(
            {"lang": "en", "ndus": self.ndus_token},
            yarl.URL(domain),
        )

        connector = aiohttp.TCPConnector(ssl=None)
        base_headers = {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "identity",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }

        try:
            async with aiohttp.ClientSession(
                connector=connector,
                cookie_jar=jar,
                headers=base_headers,
            ) as session:
                # ------ Step 1: Warm up — visit domain root for session cookies ------
                print(f"[TB]   warmup → GET {domain}/")
                try:
                    async with session.get(
                        f"{domain}/",
                        headers={"User-Agent": browser_ua},
                        allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        warmup_html = await resp.text(errors="replace")
                        real_domain = f"{resp.url.scheme}://{resp.url.host}"
                        cookie_count = sum(1 for _ in jar)
                        print(
                            f"[TB]   warmup ← HTTP {resp.status} | len={len(warmup_html)} "
                            f"| final={real_domain} | cookies={cookie_count}"
                        )
                        # Harvest jsToken from warmup page
                        js = self._extract_js_token(warmup_html)
                        if js:
                            self.js_token = js
                            print(f"[TB]   warmup → got jsToken")
                except Exception as e:
                    print(f"[TB]   warmup error: {e}")

                # ------ Step 2: Try share-page endpoints ------
                endpoints = [
                    # WAP mobile endpoint — usually server-side rendered
                    (f"{domain}/wap/share/filelist?surl={surl}", mobile_ua, "WAP"),
                    # sharing/link desktop page
                    (f"{domain}/sharing/link?surl={surl}&root=1&path=%2F", browser_ua, "sharing"),
                    # Original /s/ page
                    (f"{domain}/s/{surl}", browser_ua, "/s/"),
                ]

                # Collect redirect-target domains seen during page visits
                seen_domains: list[str] = []

                for url, ua, desc in endpoints:
                    print(f"[TB]   try {desc} → GET {url}")
                    try:
                        async with session.get(
                            url,
                            headers={"User-Agent": ua},
                            allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=25),
                        ) as resp:
                            html = await resp.text(errors="replace")
                            resp_domain = f"{resp.url.scheme}://{resp.url.host}"
                            if resp_domain not in seen_domains:
                                seen_domains.append(resp_domain)
                            print(
                                f"[TB]   {desc} ← HTTP {resp.status} | len={len(html)} "
                                f"| final={resp.url}"
                            )
                            if len(html) < 500:
                                print(f"[TB]   {desc} → too short, skipping")
                                continue
                    except Exception as e:
                        print(f"[TB]   {desc} error: {e}")
                        continue

                    # Always extract jsToken from every fetched page
                    js = self._extract_js_token(html)
                    if js:
                        self.js_token = js
                        print(f"[TB]   {desc} → got jsToken")

                    result = self._parse_share_html(html)
                    if result and result.get("shareid"):
                        print(f"[TB]   {desc} → SUCCESS shareid={result['shareid']}")
                        self._share_jar = jar
                        self._share_domain = resp_domain
                        return result

                # ------ Step 3: Try REST API on redirect-target domains ------
                # Pages redirected to different domains (e.g. 1024terabox.com →
                # www.1024tera.com).  Cookies in the jar are keyed by the domain
                # that *set* them, so we must hit the same domain for the API call.
                api_domains = list(dict.fromkeys(seen_domains + [domain]))
                print(f"[TB]   API domains to try: {api_domains}")

                # Also build an explicit Cookie header from the jar as fallback
                all_cookies = {c.key: c.value for c in jar}
                cookie_str = "; ".join(f"{k}={v}" for k, v in all_cookies.items())
                print(f"[TB]   jar cookies: {list(all_cookies.keys())}")

                api_params: Dict[str, str] = {
                    "app_id": "250528",
                    "shorturl": surl,
                    "root": "1",
                }
                if self.js_token:
                    api_params["jsToken"] = self.js_token

                for api_domain in api_domains:
                    api_url = f"{api_domain}/api/shorturlinfo"
                    print(f"[TB]   try API → {api_url}")
                    try:
                        async with session.get(
                            api_url,
                            params=api_params,
                            headers={
                                "User-Agent": browser_ua,
                                "Cookie": cookie_str,
                                "Referer": f"{api_domain}/",
                            },
                            timeout=aiohttp.ClientTimeout(total=20),
                        ) as resp:
                            data = await resp.json(content_type=None)
                            errno = data.get("errno", -1)
                            final_domain = f"{resp.url.scheme}://{resp.url.host}"
                            print(f"[TB]   API ← HTTP {resp.status} | errno={errno} | final={final_domain}")
                            if errno == 0:
                                self._share_jar = jar
                                self._share_domain = final_domain
                                self._apply_randsk(data)
                                return data
                    except Exception as e:
                        print(f"[TB]   API error ({api_domain}): {e}")

        except Exception as e:
            print(f"[TB] _scrape_one_domain session error: {e}")

        return None

    # ----------------------------------------------------- _parse_share_html

    @staticmethod
    def _parse_share_html(html: str) -> Optional[Dict[str, Any]]:
        """
        Extract share data (shareid, uk, file_list) from TeraBox HTML.
        Tries multiple known patterns.
        """
        result: Dict[str, Any] = {}

        # Pattern 1: window.__INITIAL_STATE__ = {...}
        for pat in (
            r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});\s*</script',
            r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});\s*\n',
        ):
            m = re.search(pat, html, re.DOTALL)
            if m:
                try:
                    state = json.loads(m.group(1))
                    print(f"[TB]   found __INITIAL_STATE__ keys={list(state.keys())[:12]}")
                    share = state
                    for k, v in state.items():
                        if isinstance(v, dict) and ("shareid" in v or "share_id" in v):
                            share = v
                            break
                    result["shareid"] = str(share.get("shareid") or share.get("share_id", ""))
                    result["uk"] = str(share.get("uk") or share.get("owner_id", ""))
                    fl = share.get("file_list") or share.get("list", [])
                    if isinstance(fl, dict):
                        fl = fl.get("list", [])
                    result["file_list"] = fl
                    if result.get("shareid"):
                        return result
                except json.JSONDecodeError as e:
                    print(f"[TB]   __INITIAL_STATE__ JSON error: {e}")
                break

        # Pattern 2: locals.init({...})
        if not result.get("shareid"):
            m = re.search(r'locals\.init\((\{.+?\})\)', html, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    print(f"[TB]   found locals.init keys={list(data.keys())[:12]}")
                    result["shareid"] = str(data.get("shareid", ""))
                    result["uk"] = str(data.get("uk", ""))
                    result["file_list"] = data.get("file_list", [])
                    if result.get("shareid"):
                        return result
                except json.JSONDecodeError:
                    pass

        # Pattern 3: window.jsData = {...}
        if not result.get("shareid"):
            m = re.search(r'window\.jsData\s*=\s*(\{.+?\})\s*;', html, re.DOTALL)
            if m:
                try:
                    data = json.loads(m.group(1))
                    print(f"[TB]   found jsData keys={list(data.keys())[:12]}")
                    result["shareid"] = str(data.get("shareid") or data.get("share_id", ""))
                    result["uk"] = str(data.get("uk") or data.get("owner_id", ""))
                    fl = data.get("file_list") or data.get("list", [])
                    if isinstance(fl, dict):
                        fl = fl.get("list", [])
                    result["file_list"] = fl
                    if result.get("shareid"):
                        return result
                except json.JSONDecodeError:
                    pass

        # Pattern 4: Any large JSON object in <script> with shareid
        if not result.get("shareid"):
            for script_m in re.finditer(
                r'<script[^>]*>([^<]{200,})</script>', html, re.DOTALL
            ):
                content = script_m.group(1)
                for json_m in re.finditer(r'=\s*(\{.+?\})\s*;', content, re.DOTALL):
                    try:
                        obj = json.loads(json_m.group(1))
                        if isinstance(obj, dict) and (
                            "shareid" in obj or "share_id" in obj
                        ):
                            print(f"[TB]   found shareid in script JSON")
                            result["shareid"] = str(
                                obj.get("shareid") or obj.get("share_id", "")
                            )
                            result["uk"] = str(
                                obj.get("uk") or obj.get("owner_id", "")
                            )
                            fl = obj.get("file_list") or obj.get("list", [])
                            if isinstance(fl, dict):
                                fl = fl.get("list", [])
                            result["file_list"] = fl
                            if result.get("shareid"):
                                return result
                    except (json.JSONDecodeError, ValueError):
                        continue

        # Pattern 5: Regex extraction (last resort)
        if not result.get("shareid"):
            sid = re.search(r'"shareid"\s*:\s*"?(\d+)"?', html)
            uk = re.search(r'"uk"\s*:\s*"?(\d+)"?', html)
            if sid:
                result["shareid"] = sid.group(1)
            if uk:
                result["uk"] = uk.group(1)
            if result.get("shareid"):
                print(f"[TB]   regex fallback: shareid={result['shareid']} uk={result.get('uk')}")
                return result

        # Debug: log script tags
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
        print(
            f"[TB]   parse FAILED — {len(scripts)} scripts | "
            f"html[:200]={html[:200]!r}"
        )
        for i, s in enumerate(scripts[:5]):
            preview = s.strip()[:120].replace("\n", " ")
            print(f"[TB]     script[{i}] len={len(s)} {preview!r}")
        return None

    async def _fetch_js_token_from(self, base_url: str) -> None:
        """
        Fetch jsToken from a specific TeraBox domain (e.g. 1024terabox.com).
        Only updates jsToken (and bdstoken if found); does NOT change self.domain.
        """
        for path in ("main", "disk/home", ""):
            try:
                url = f"{base_url}/{path}" if path else base_url
                print(f"[TB] _fetch_js_token_from → GET {url}")
                async with self._session_for() as session:
                    # Override cookie domain for this request
                    custom_headers = {
                        "User-Agent": self.USER_AGENT,
                        "Cookie": f"lang=en; ndus={self.ndus_token}",
                        "Referer": base_url + "/",
                        "Accept-Encoding": "identity",
                    }
                    async with session.get(
                        url,
                        headers=custom_headers,
                        allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        html = await resp.text(errors="replace")
                        print(f"[TB] _fetch_js_token_from ← HTTP {resp.status} | html_len={len(html)}")

                js_match = self._extract_js_token(html)
                if js_match:
                    self.js_token = js_match
                    print(f"[TB] _fetch_js_token_from → jsToken OK (from {path or 'root'})")

                bds_match = re.search(r'"bdstoken"\s*:\s*"([^"]+)"', html)
                if bds_match and not self.bds_token:
                    self.bds_token = bds_match.group(1)

                if self.js_token:
                    return  # Got what we need
            except Exception as e:
                print(f"[TB] _fetch_js_token_from error ({path}): {e}")

    # -------------------------------------------------------- short_url_list

    async def short_url_list(
        self, surl: str, remote_dir: str = "", page: int = 1,
        share_host: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        List files in a TeraBox share.

        Passes all required share auth tokens (uk, shareid, sign, timestamp,
        sekey) as query params AND sekey/ndus as explicit Cookie header to
        survive cross-domain redirects.
        """
        base = f"https://{share_host}" if share_host else self.domain

        params: Dict[str, str] = {
            "app_id": "250528",
            "web": "1",
            "channel": "dubox",
            "clienttype": "0",
            "shorturl": surl,
            "by": "name",
            "order": "asc",
            "num": "20000",
            "dir": remote_dir,
            "page": str(page),
        }
        if self.js_token:
            params["jsToken"] = self.js_token
        if not remote_dir:
            params["root"] = "1"

        # Include share auth tokens from shorturlinfo response
        if self._share_uk:
            params["uk"] = self._share_uk
        if self._share_id:
            params["shareid"] = self._share_id
        if self._share_sign:
            params["sign"] = self._share_sign
        if self._share_timestamp:
            params["timestamp"] = self._share_timestamp

        decoded_randsk = urllib.parse.unquote(self._randsk) if self._randsk else ""
        if decoded_randsk:
            params["sekey"] = decoded_randsk

        # Build explicit cookie string from ALL jar cookies — the explicit
        # Cookie header overrides the jar so we must include everything.
        cookie_map: Dict[str, str] = {}
        if self._share_jar:
            for c in self._share_jar:
                cookie_map[c.key] = c.value
        # Ensure essential cookies are present
        cookie_map.setdefault("lang", "en")
        cookie_map["ndus"] = self.ndus_token
        if decoded_randsk:
            cookie_map["TSID"] = decoded_randsk
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookie_map.items())

        browser_ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        )

        print(
            f"[TB] short_url_list → params: uk={params.get('uk','?')} "
            f"shareid={params.get('shareid','?')} sign={params.get('sign','?')[:8]}... "
            f"ts={params.get('timestamp','?')} sekey_len={len(decoded_randsk)} "
            f"dir={remote_dir!r} | cookies={list(cookie_map.keys())}"
        )

        # Try multiple domains — the jar domain, the share base, and self.domain
        domains_to_try = []
        if self._share_domain:
            domains_to_try.append(self._share_domain)
        if base not in domains_to_try:
            domains_to_try.append(base)

        for try_base in domains_to_try:
            url = f"{try_base}/share/list?" + urllib.parse.urlencode(params)
            print(f"[TB] short_url_list → GET {try_base}/share/list (dir={remote_dir!r})")
            try:
                connector = aiohttp.TCPConnector(ssl=None)
                jar = self._share_jar or aiohttp.CookieJar(unsafe=True)
                async with aiohttp.ClientSession(
                    connector=connector,
                    cookie_jar=jar,
                ) as session:
                    async with session.get(
                        url,
                        headers={
                            "User-Agent": browser_ua,
                            "Accept-Encoding": "identity",
                            "Cookie": cookie_str,
                            "Referer": try_base + "/",
                        },
                        allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=20),
                    ) as resp:
                        data = await resp.json(content_type=None)
                        errno = data.get("errno", -1)
                        file_count = len(data.get("list", []))
                        final = f"{resp.url.scheme}://{resp.url.host}"
                        print(f"[TB] short_url_list ← HTTP {resp.status} | errno={errno} | files={file_count} | final={final}")
                        if errno == 0:
                            return data
                        print(f"[TB] short_url_list → errno={errno}, trying next domain...")
            except Exception as e:
                print(f"[TB] short_url_list error ({try_base}): {e}")

        print(f"[TB] short_url_list → all domains failed")
        return data if 'data' in dir() else None

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
            print(f"[TB] share_transfer → POST {url}  (attempt={attempt+1}, fs_ids={fs_ids}, dest={dest_path})")
            try:
                async with self._session_for() as session:
                    async with session.post(
                        url,
                        data=body,
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        timeout=aiohttp.ClientTimeout(total=60),
                    ) as resp:
                        result = await resp.json(content_type=None)
                        print(f"[TB] share_transfer ← HTTP {resp.status} | {result}")
                        errno = result.get("errno", -1)
                        if errno == 400810 and attempt == 0:
                            print("[TB] share_transfer: errno=400810 → refreshing tokens and retrying")
                            self.js_token = ""
                            self.bds_token = ""
                            await self.update_app_data()
                            continue
                        return result
            except Exception as e:
                print(f"[TB] share_transfer error (attempt={attempt+1}): {e}")
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
        print(f"[TB] create_dir → POST {url}  (path={remote_dir})")
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    data = await resp.json(content_type=None)
                    print(f"[TB] create_dir ← HTTP {resp.status} | {data}")
                    return data
        except Exception as e:
            print(f"[TB] create_dir error: {e}")
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
        print(f"[TB] get_remote_dir → POST {url}  (dir={remote_dir})")
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    data = await resp.json(content_type=None)
                    file_count = len(data.get("list", []))
                    print(f"[TB] get_remote_dir ← HTTP {resp.status} | errno={data.get('errno')} | files={file_count}")
                    return data
        except Exception as e:
            print(f"[TB] get_remote_dir error: {e}")
            return None

    # ----------------------------------------------------------- get_home_info

    async def get_home_info(self) -> Optional[Dict[str, Any]]:
        """
        Fetch sign data needed for download link generation.
        Mirrors TeraBoxApp#getHomeInfo (lines 2223–2248 of api.js).
        """
        url = f"{self.domain}/api/home/info"
        print(f"[TB] get_home_info → GET {url}")
        try:
            async with self._session_for() as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    data = await resp.json(content_type=None)
                    print(f"[TB] get_home_info ← HTTP {resp.status} | errno={data.get('errno')} | has_sign={bool(data.get('sign1'))}")
                    return data
        except Exception as e:
            print(f"[TB] get_home_info error: {e}")
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
        print(f"[TB] download → POST {url}  (fs_ids={fs_ids})")
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    data = await resp.json(content_type=None)
                    link_count = len(data.get("dlink", []))
                    print(f"[TB] download ← HTTP {resp.status} | errno={data.get('errno')} | dlinks={link_count}")
                    return data
        except Exception as e:
            print(f"[TB] download error: {e}")
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
        print(f"[TB] filemanager → POST {url}  (opera={operation}, params={fm_params})")
        try:
            async with self._session_for() as session:
                async with session.post(
                    url,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    data = await resp.json(content_type=None)
                    print(f"[TB] filemanager ← HTTP {resp.status} | {data}")
                    return data
        except Exception as e:
            print(f"[TB] filemanager error: {e}")
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
