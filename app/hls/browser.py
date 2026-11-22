"""
hls/browser.py — last-resort stream resolution with a real headless browser.

Why this exists
---------------
Some hosts build the playlist URL *inside* their JavaScript and never put an
``.m3u8`` in the HTML: the loader is a javascript-obfuscator bundle with a string
table, the player config is assembled at runtime, and the request is gated by
device/GPU fingerprinting and bot detection (``openfpcdn.io/botd``). No amount of
regex, header or proxy work can reach that URL — something has to *run* the page.

So this module launches Playwright's Chromium, opens the page, and returns the
first HLS manifest the page asks for along with the browser's effective
User-Agent, Referer, Origin, custom request headers, frame/final page URL, and
cookies so downstream segment downloads match the exact browser session:

* **Network capture** — requests and responses are watched for ``.m3u8`` (or
  ``mpegurl`` content types); a successful ``<400`` manifest response wins
  immediately, while ad/blocked requests are ignored.
* **JS tap** (fallback) — an init script wraps ``jwplayer``, ``Hls.prototype``,
  ``HTMLMediaElement.prototype.src``, ``fetch`` and ``XMLHttpRequest`` across all
  frames, so a URL that is *built* but never requested is still recovered.
* **Ad/weight blocking** — images, fonts, non-manifest media and known ad hosts
  are aborted to keep the scan fast and cheap; stylesheets, scripts and XHR
  pass so layout-checking players still initialize.

The browser is a module-level singleton: one Chromium process, one isolated
context per job, serialised by a lock (``HLS_BROWSER_MAX_CONCURRENCY``), and
closed automatically after ``HLS_BROWSER_IDLE`` seconds of inactivity so a small
VPS never keeps ~400 MB of Chromium resident between jobs.

Everything degrades gracefully: without the ``playwright`` package (or without
its Chromium download) the module reports itself unavailable and the page scan
simply keeps its previous behaviour.

Install (optional, on the VPS):

    pip install playwright && python -m playwright install chromium

Self-test:

    python -m app.hls.browser
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

from app.config import (
    HLS_BROWSER_BLOCK_ADS,
    HLS_BROWSER_IDLE,
    HLS_BROWSER_MAX_CONCURRENCY,
    HLS_BROWSER_PROXY,
    HLS_BROWSER_TIMEOUT,
)
from app.hls.client import DEFAULT_USER_AGENT, default_headers, read_proxy_url
from app.warp import (
    WarpProxyError,
    check_proxy_reachable,
    ensure_proxy_ready,
    format_warp_proxy_error,
)

#: Chromium switches that make it survive on a small headless VPS.
_LAUNCH_ARGS = (
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--mute-audio",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-renderer-backgrounding",
    "--disable-features=Translate,MediaRouter,OptimizationHints",
    "--autoplay-policy=no-user-gesture-required",
    "--disable-blink-features=AutomationControlled",
)

#: Resource types that can never carry the manifest and only cost time.
#: Keep stylesheets unblocked so players checking layout/offsetWidth still boot.
#: Non-manifest media is aborted in _block_heavy, while manifest media passes.
_BLOCKED_RESOURCE_TYPES = {"image", "font"}

#: Ad/tracker hosts that are aborted by default (the page's own player never
#: lives on them, and they are the main reason these pages are slow).
_BLOCKED_HOST_PARTS = (
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adsbygoogle", "adservice", "popads", "popcash", "propellerads",
    "exoclick", "juicyads", "trafficjunky", "adsterra", "hilltopads",
    "facebook.net", "criteo", "taboola", "outbrain", "openfpcdn",
)

_AD_PATH_MARKERS = ("/vast", "preroll", "/ads/", "adserver")

#: Bot root (two levels above ``<bot>/app/hls/browser.py``).
_BOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_CACHE_CANDIDATES = (
    os.path.join(_BOT_DIR, "ms-playwright"),
    "/opt/telegram-forwarder-bot/ms-playwright",
)


def _has_browser(path: str) -> bool:
    """True when ``path`` holds at least one Chromium build (cheap, no import)."""
    try:
        return any(
            name.startswith(("chromium", "chrome")) for name in os.listdir(path)
        )
    except Exception:
        return False


def _home_masked() -> bool:
    """True when this process cannot see its own home directory."""
    for home in (os.environ.get("HOME"), os.path.expanduser("~")):
        if home and home.startswith(("/home/", "/root")) and not os.path.isdir(home):
            return True
    return False


def _resolve_browser_cache() -> None:
    """Point Playwright at a Chromium it can actually reach."""
    if os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        return
    if _has_browser(os.path.expanduser("~/.cache/ms-playwright")):
        return
    for candidate in _CACHE_CANDIDATES:
        if _has_browser(candidate):
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = candidate
            print(f"[HLS] Browser: using the Chromium cache at {candidate}")
            return


def browser_available() -> bool:
    """True when the ``playwright`` package is importable."""
    global _AVAILABLE
    if _AVAILABLE is None:
        _AVAILABLE = importlib.util.find_spec("playwright") is not None
        if not _AVAILABLE:
            print(f"[HLS] Browser engine unavailable: 'playwright' is not installed "
                  f"in {sys.executable} — install it as the user that runs the bot:\n"
                  f"      {sys.executable} -m pip install playwright\n"
                  f"{_install_hint()}")
    return _AVAILABLE


_AVAILABLE: Optional[bool] = None
_HINT_LOGGED = False


@dataclass
class BrowserScan:
    """What the headless browser found on the page."""

    #: The manifest URL, or None.
    url: Optional[str] = None
    #: Where it came from: "network" | "player-config" | "".
    source: str = ""
    #: "ok" | "no-manifest" | "no-browser" | "not-a-page" | "error"
    reason: str = ""
    #: Free-text detail for the log line.
    detail: str = ""
    #: Requests the page made while we watched.
    requests: int = 0
    #: True when Chromium itself went through the configured proxy.
    used_proxy: bool = False
    #: True when Chromium went through the local WARP wireproxy HTTP listener.
    used_warp: bool = False
    #: Effective page/frame URL (after redirects or inside embed iframe).
    page_url: str = ""
    #: Captured request headers (User-Agent, Referer, Origin, auth headers).
    headers: Dict[str, str] = field(default_factory=dict)
    #: Browser cookies captured for the manifest/page context.
    cookies: Dict[str, str] = field(default_factory=dict)


def _looks_like_manifest(url: str) -> bool:
    """True for an HLS manifest URL (query string preserved)."""
    if ".m3u8" not in url.lower():
        return False
    return url.lower().split("?", 1)[0].endswith((".m3u8", ".m3u"))


def _is_blocked_url(url: str) -> bool:
    """True for ad/tracker hosts we never need."""
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return True
    return any(part in host for part in _BLOCKED_HOST_PARTS)


def _is_ad_manifest(url: str) -> bool:
    if _is_blocked_url(url):
        return True
    low = url.lower()
    return any(marker in low for marker in _AD_PATH_MARKERS)


#: Installed before any page script runs: records the first HLS URL the page
#: *builds*, whether or not the request ever leaves the browser (autoplay
#: blocked, player paused, request aborted by our own ad filter...).
_TAP_JS = r"""
(() => {
  if (window.__hlsTapInstalled) return;
  window.__hlsTapInstalled = true;
  try {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  } catch (e) {}
  window.__hlsTapList = window.__hlsTapList || [];
  const isAd = (u) => {
    const low = String(u).toLowerCase();
    return low.indexOf('doubleclick') !== -1 || low.indexOf('googlesyndication') !== -1
      || low.indexOf('/vast') !== -1 || low.indexOf('preroll') !== -1;
  };
  const note = (u) => {
    try {
      if (typeof u === 'string' && u.indexOf('.m3u8') !== -1 && !isAd(u)) {
        if (!window.__hlsTap) window.__hlsTap = u;
        if (window.__hlsTapList.indexOf(u) === -1) window.__hlsTapList.push(u);
      }
    } catch (e) {}
  };
  const walk = (o, depth) => {
    try {
      if (!o || depth > 6) return;
      if (typeof o === 'string') { note(o); return; }
      if (Array.isArray(o)) { for (const v of o) walk(v, depth + 1); return; }
      if (typeof o === 'object') { for (const k of Object.keys(o)) walk(o[k], depth + 1); }
    } catch (e) {}
  };
  const xo = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (m, u) { note(u); return xo.apply(this, arguments); };
  const of = window.fetch;
  if (of) {
    window.fetch = function (input) {
      try { note(typeof input === 'string' ? input : (input && input.url)); } catch (e) {}
      return of.apply(this, arguments);
    };
  }
  try {
    const desc = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
    if (desc && desc.set) {
      Object.defineProperty(HTMLMediaElement.prototype, 'src', {
        configurable: true,
        enumerable: desc.enumerable,
        get: desc.get,
        set(v) { note(v); return desc.set.call(this, v); },
      });
    }
  } catch (e) {}
  let jw = window.jwplayer;
  const wrapSetup = (obj) => {
    try {
      if (obj && typeof obj.setup === 'function' && !obj.__hlsTapped) {
        const orig = obj.setup.bind(obj);
        obj.setup = function (cfg) { walk(cfg, 0); return orig(cfg); };
        obj.__hlsTapped = true;
      }
    } catch (e) {}
    return obj;
  };
  try {
    Object.defineProperty(window, 'jwplayer', {
      configurable: true,
      get() { return jw; },
      set(v) {
        jw = (typeof v === 'function')
          ? function () { return wrapSetup(v.apply(this, arguments)); }
          : v;
      },
    });
  } catch (e) {}
  try {
    let H = window.Hls;
    const hookHls = (v) => {
      if (v && v.prototype && typeof v.prototype.loadSource === 'function' && !v.prototype.__hlsTapped) {
        const orig = v.prototype.loadSource;
        v.prototype.loadSource = function (u) { note(u); return orig.apply(this, arguments); };
        v.prototype.__hlsTapped = true;
      }
      if (typeof v === 'function' && typeof v.loadSource === 'function' && !v.__hlsTapped) {
        const OL = v.loadSource;
        v.loadSource = function (u) { note(u); return OL.apply(this, arguments); };
        v.__hlsTapped = true;
      }
    };
    if (H) hookHls(H);
    Object.defineProperty(window, 'Hls', {
      configurable: true,
      get() { return H; },
      set(v) { hookHls(v); H = v; },
    });
  } catch (e) {}
})();
"""

# ---------------------------------------------------------------------------
# Browser lifecycle (one Chromium process, reused, auto-closed when idle)
# ---------------------------------------------------------------------------

_pw: Any = None
_browser: Any = None
_active = 0
_browser_last_used = 0.0
_browser_used: bool = False
_browser_proxy_key: Optional[tuple] = None
_browser_using_warp: bool = False
_sem: Optional[asyncio.Semaphore] = None
_idle_task: Optional[asyncio.Task] = None


def _browser_used_proxy() -> bool:
    """True when the running Chromium was launched through the proxy."""
    return _browser_used


async def _resolve_browser_proxy() -> Tuple[Optional[Dict[str, str]], tuple, bool]:
    """Resolve Playwright proxy config, preferring WARP HTTP proxy when ON."""
    warp_cfg = await ensure_proxy_ready(
        require_http=True,
        require_socks5=False,
        feature="HLS",
    )
    if warp_cfg is not None:
        return ({"server": warp_cfg.http_url}, ("warp", warp_cfg.http_url), True)
    proxy = _playwright_proxy()
    if proxy:
        return (
            proxy,
            ("proxy_txt", proxy.get("server", ""), proxy.get("username", "")),
            False,
        )
    return (None, ("direct", None), False)


def _playwright_proxy() -> Optional[Dict[str, str]]:
    """Playwright proxy config built from ``proxy.txt`` (``none`` bypasses it)."""
    if str(HLS_BROWSER_PROXY).strip().lower() in ("", "none", "off", "no", "false", "disabled"):
        return None
    proxy = read_proxy_url()
    if not proxy:
        return None
    parsed = urlparse(proxy)
    if not parsed.hostname:
        return None
    scheme = (parsed.scheme or "http").lower()
    config: Dict[str, str] = {"server": f"{scheme}://{parsed.hostname}:{parsed.port}"}
    if parsed.username:
        if scheme.startswith("socks"):
            # Chromium takes SOCKS credentials itself, not from Playwright, so an
            # authenticated SOCKS tunnel cannot be used here.
            print("[HLS] Browser: proxy.txt is an authenticated SOCKS5 proxy, which "
                  "Chromium cannot use — the browser will go direct "
                  "(set HLS_BROWSER_PROXY=none to silence this).")
            return None
        config["username"] = parsed.username
        config["password"] = parsed.password or ""
    return config


def _concurrency() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(max(1, HLS_BROWSER_MAX_CONCURRENCY))
    return _sem


async def _get_browser(
    proxy: Optional[Dict[str, str]] = None,
    proxy_key: Optional[tuple] = None,
    using_warp: bool = False,
) -> Tuple[Any, bool]:
    """Start Chromium on first use and reuse it for matching proxy settings."""
    global _pw, _browser, _browser_last_used, _browser_used, _browser_proxy_key, _browser_using_warp, _idle_task
    if proxy_key is None:
        proxy = _playwright_proxy()
        proxy_key = (
            ("proxy_txt", proxy.get("server", ""), proxy.get("username", ""))
            if proxy
            else ("direct", None)
        )

    if _browser is not None:
        try:
            if _browser.is_connected():
                if _browser_proxy_key == proxy_key:
                    _browser_last_used = time.time()
                    return _browser, False
                if _active <= 1:
                    try:
                        await _browser.close()
                    except Exception:
                        pass
                    _browser = None
                    _browser_used = False
                    _browser_proxy_key = None
                    _browser_using_warp = False
                else:
                    from playwright.async_api import async_playwright

                    _resolve_browser_cache()
                    if _pw is None:
                        _pw = await async_playwright().start()
                    eph_kwargs: Dict[str, Any] = {"headless": True, "args": list(_LAUNCH_ARGS)}
                    if proxy:
                        eph_kwargs["proxy"] = proxy
                    eph_browser = await _pw.chromium.launch(**eph_kwargs)
                    print(
                        f"[HLS] Ephemeral browser started (proxy={'yes' if proxy else 'no'}, "
                        f"warp={'yes' if using_warp else 'no'})"
                    )
                    return eph_browser, True
        except Exception:
            _browser = None
            _browser_used = False
            _browser_proxy_key = None
            _browser_using_warp = False

    from playwright.async_api import async_playwright

    # Before the driver starts: it is the driver that resolves the browser path,
    # so a cache outside the (sandboxed) home has to be advertised here.
    _resolve_browser_cache()

    if _pw is None:
        _pw = await async_playwright().start()

    kwargs: Dict[str, Any] = {"headless": True, "args": list(_LAUNCH_ARGS)}
    if proxy:
        kwargs["proxy"] = proxy
    _browser = await _pw.chromium.launch(**kwargs)
    _browser_used = proxy is not None
    _browser_proxy_key = proxy_key
    _browser_using_warp = using_warp
    _browser_last_used = time.time()
    print(
        f"[HLS] Browser started (proxy={'yes' if proxy else 'no'}, "
        f"warp={'yes' if using_warp else 'no'})"
    )

    if _idle_task is None or _idle_task.done():
        _idle_task = asyncio.create_task(_idle_watchdog())
    return _browser, False


async def _idle_watchdog() -> None:
    """Free Chromium's ~400 MB footprint once no job needs it."""
    global _idle_task
    step = max(5, min(30, HLS_BROWSER_IDLE))
    while True:
        await asyncio.sleep(step)
        if _browser is None:
            break
        if _active or _browser_last_used == 0.0:
            continue
        if time.time() - _browser_last_used >= HLS_BROWSER_IDLE:
            print(f"[HLS] Browser idle {HLS_BROWSER_IDLE}s - closing Chromium")
            await close_browser()
            break
    _idle_task = None


async def close_browser() -> None:
    """Shut Chromium and Playwright down (safe to call at any time)."""
    global _browser, _pw, _browser_last_used, _browser_used, _browser_proxy_key, _browser_using_warp
    if _active:
        return
    if _browser is not None:
        try:
            await _browser.close()
        except Exception as e:
            print(f"[HLS] Browser close failed: {e}")
        _browser = None
    _browser_used = False
    _browser_proxy_key = None
    _browser_using_warp = False
    if _pw is not None:
        try:
            await _pw.stop()
        except Exception:
            pass
        _pw = None


async def _block_heavy(route, request) -> None:
    """Abort ads, images, fonts and non-manifest media; let player assets pass."""
    try:
        rtype = request.resource_type
        url = request.url
        if _is_blocked_url(url):
            await route.abort()
        elif rtype in _BLOCKED_RESOURCE_TYPES:
            await route.abort()
        elif rtype == "media" and not _looks_like_manifest(url):
            await route.abort()
        else:
            await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass


async def _read_tap(page) -> Tuple[str, str]:
    """Read any tapped or DOM-attached manifest URL across all page frames."""
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]
    if page.main_frame not in frames:
        frames.insert(0, page.main_frame)

    for frame in frames:
        try:
            value = await frame.evaluate(
                """() => {
                  if (window.__hlsTap) return window.__hlsTap;
                  if (Array.isArray(window.__hlsTapList) && window.__hlsTapList.length) {
                    return window.__hlsTapList[0];
                  }
                  const els = document.querySelectorAll('video, source');
                  for (const el of els) {
                    const src = el.getAttribute('src') || el.currentSrc || el.src || '';
                    if (src && src.indexOf('.m3u8') !== -1) return src;
                  }
                  return '';
                }"""
            )
            if isinstance(value, str) and value:
                f_url = ""
                try:
                    f_url = frame.url or ""
                except Exception:
                    f_url = ""
                return value, f_url
        except Exception:
            continue
    return "", ""


async def _try_wake_player(page) -> None:
    """Best-effort click on common player overlays for click-to-play embeds."""
    selectors = (
        ".jw-icon-display",
        ".vjs-big-play-button",
        ".plyr__control--overlaid",
        "video",
    )
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]
    for frame in frames:
        for sel in selectors:
            try:
                el = await frame.query_selector(sel)
                if el is not None and await el.is_visible():
                    await el.click(timeout=300, no_wait_after=True)
                    return
            except Exception:
                continue


async def _extract_request_headers(req, effective_page_url: str) -> Dict[str, str]:
    raw: Dict[str, str] = {}
    try:
        raw = await req.all_headers()
    except Exception:
        try:
            raw = dict(req.headers or {})
        except Exception:
            raw = {}

    out: Dict[str, str] = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    for k, v in raw.items():
        if not k or not v:
            continue
        low = k.lower()
        if low == "user-agent":
            out["User-Agent"] = v
        elif low == "referer":
            out["Referer"] = v
        elif low == "origin":
            out["Origin"] = v
        elif low == "authorization":
            out["Authorization"] = v
        elif low.startswith("x-") and low not in ("x-client-data", "x-requested-with"):
            out[k] = v

    if "Referer" not in out and effective_page_url:
        out["Referer"] = effective_page_url
    if "Origin" not in out and out.get("Referer"):
        parsed = urlparse(out["Referer"])
        if parsed.scheme and parsed.netloc:
            out["Origin"] = f"{parsed.scheme}://{parsed.netloc}"
    return out


async def _extract_cookies(context, manifest_url: str, page_url: str) -> Dict[str, str]:
    cookies: Dict[str, str] = {}
    if manifest_url and manifest_url.startswith(("http://", "https://")):
        try:
            m_items = await context.cookies([manifest_url])
            for item in m_items or []:
                name = item.get("name")
                val = item.get("value")
                if name and val is not None:
                    cookies[str(name)] = str(val)
        except Exception:
            pass
    if not cookies and page_url and page_url.startswith(("http://", "https://")):
        try:
            mp = urlparse(manifest_url)
            pp = urlparse(page_url)
            mhost = (mp.hostname or "").lower()
            phost = (pp.hostname or "").lower()
            if mhost and phost and mhost.split(".")[-2:] == phost.split(".")[-2:]:
                p_items = await context.cookies([page_url])
                for item in p_items or []:
                    name = item.get("name")
                    val = item.get("value")
                    if name and val is not None:
                        cookies[str(name)] = str(val)
        except Exception:
            pass
    return cookies


def _install_hint() -> str:
    """The exact commands that install Chromium for the account running the bot."""
    cache = os.path.join(_BOT_DIR, "ms-playwright")
    try:
        import pwd
        me = pwd.getpwuid(os.getuid()).pw_name
    except Exception:
        me = os.environ.get("USER") or os.environ.get("LOGNAME") or "botuser"
    return (
        f"        sudo -H -u {me} env PLAYWRIGHT_BROWSERS_PATH={cache} \\\n"
        f"          {sys.executable} -m playwright install chromium\n"
        f"      Chromium also needs its system libraries, installed as root:\n"
        f"        sudo {sys.executable} -m playwright install-deps chromium\n"
        f"      Or run both steps at once, from an admin user:\n"
        f"        sudo bash deploy/install-browser.sh"
    )


_MISSING_EXE = re.compile(r"Executable doesn't exist at (.+)", re.MULTILINE)


def _describe_cache(wanted: str) -> str:
    """One log line: the build Playwright wants vs. what the cache holds."""
    if not wanted:
        return "      cache contains:   (unknown - Playwright did not name a path)"
    build = os.path.basename(os.path.dirname(os.path.dirname(wanted)))
    cache = os.path.dirname(os.path.dirname(os.path.dirname(wanted)))
    if not os.path.isdir(cache):
        if _home_masked():
            return (f"      cache contains:   {cache} is not visible to this process\n"
                    f"                        (the service sandbox hides /home)")
        return f"      cache contains:   {cache} does not exist (download never ran)"
    try:
        builds = sorted(
            name for name in os.listdir(cache)
            if name.startswith(("chromium", "chrome", "ffmpeg"))
        )
    except Exception as e:
        return f"      cache contains:   unreadable ({type(e).__name__}: {e})"
    if not builds:
        return f"      cache contains:   nothing in {cache} (download never completed)"
    stale = "" if build in builds else "  <- OLDER than this Playwright wants"
    return f"      cache contains:   {', '.join(builds)}{stale}"


def _sandbox_hint(cache: str) -> str:
    """The way out of the ``ProtectHome=true`` trap (see the unit in install.sh)."""
    return (
        f"      ProtectHome=true hides /home from the service, so a Chromium under\n"
        f"      {cache} can never be used by the bot, however often it is reinstalled.\n"
        f"      Easiest fix (installs outside /home, where the service can read it):\n"
        f"        sudo bash deploy/install-browser.sh\n"
        f"      Or keep the current cache and let the service see it:\n"
        f"        sudo mkdir -p /etc/systemd/system/telegram-forwarder.service.d\n"
        f"        printf '[Service]\\nEnvironment=PLAYWRIGHT_BROWSERS_PATH=%s\\n' {cache} \\\n"
        f"          | sudo tee /etc/systemd/system/telegram-forwarder.service.d/browser-cache.conf\n"
        f"        sudo systemctl daemon-reload && sudo systemctl restart telegram-forwarder\n"
    )


def _log_launch_failure(err: Exception) -> None:
    """One clear log line per failure mode, with the fix where we know it."""
    global _HINT_LOGGED
    message = str(err)
    if "Executable doesn't exist" in message or "playwright install" in message:
        if not _HINT_LOGGED:
            _HINT_LOGGED = True
            match = _MISSING_EXE.search(message)
            wanted = (match.group(1).strip() if match else "")
            cache = (
                os.path.dirname(os.path.dirname(os.path.dirname(wanted)))
                if wanted else os.path.expanduser("~/.cache/ms-playwright")
            )
            print(f"[HLS] Browser engine: Chromium is not downloaded for this user "
                  f"(cache: {cache}).\n"
                  f"      playwright wants: {wanted or '(not named in the error)'}\n"
                  f"{_describe_cache(wanted)}\n"
                  f"{_sandbox_hint(cache) if _home_masked() else _install_hint()}")
        return
    if "shared librar" in message.lower() or "libnss" in message.lower():
        print("[HLS] Browser engine: Chromium cannot start - missing system "
              "libraries.\n"
              f"      Install them as root, then restart the bot:\n"
              f"        sudo {sys.executable} -m playwright install-deps chromium")
        return
    print(f"[HLS] Browser engine unavailable: {type(err).__name__}: {err}")


async def resolve_stream_with_browser(
    page_url: str,
    *,
    timeout: int = HLS_BROWSER_TIMEOUT,
) -> BrowserScan:
    """Load *page_url* in Chromium and return the first HLS manifest it needs.

    Returns ``url=None`` (never raises) when the browser is unavailable, the page
    has no manifest, or anything else goes wrong — the caller keeps its previous
    behaviour. Jobs are serialised by ``HLS_BROWSER_MAX_CONCURRENCY`` so a small
    VPS never runs more Chromium contexts than it can afford.
    """
    global _active, _browser_last_used

    if not browser_available():
        return BrowserScan(reason="no-browser", detail="playwright not installed")

    timeout = max(5, int(timeout or HLS_BROWSER_TIMEOUT))
    started = time.time()

    async with _concurrency():
        _active += 1
        context = None
        page = None
        browser = None
        is_ephemeral = False
        requests_seen = 0
        last_request_at = time.time()
        try:
            proxy_cfg, proxy_key, using_warp = await _resolve_browser_proxy()
            try:
                browser, is_ephemeral = await _get_browser(
                    proxy=proxy_cfg,
                    proxy_key=proxy_key,
                    using_warp=using_warp,
                )
            except WarpProxyError:
                raise
            except Exception as e:
                _log_launch_failure(e)
                return BrowserScan(reason="no-browser", detail=f"{type(e).__name__}: {e}")

            loop = asyncio.get_running_loop()
            verified_hit: asyncio.Future = loop.create_future()
            fallback_req: Any = None
            fallback_url: str = ""
            fallback_seen_at: float = 0.0
            failed_urls: set[str] = set()

            try:
                context = await browser.new_context(
                    user_agent=DEFAULT_USER_AGENT,
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                    ignore_https_errors=True,
                )
                page = await context.new_page()
                page.set_default_timeout(timeout * 1000)

                def _on_request(request) -> None:
                    nonlocal requests_seen, last_request_at, fallback_req, fallback_url, fallback_seen_at
                    requests_seen += 1
                    last_request_at = time.time()
                    try:
                        req_url = request.url
                        if _is_ad_manifest(req_url):
                            return
                        if _looks_like_manifest(req_url) and (
                            fallback_req is None or fallback_req.url in failed_urls
                        ):
                            fallback_req = request
                            fallback_url = urljoin(page_url, req_url)
                            fallback_seen_at = time.time()
                    except Exception:
                        pass

                def _on_response(response) -> None:
                    try:
                        resp_url = response.url
                        if _is_ad_manifest(resp_url):
                            return
                        ctype = (response.headers.get("content-type") or "").lower()
                        is_manifest = _looks_like_manifest(resp_url) or ("mpegurl" in ctype)
                        if not is_manifest:
                            return
                        if 200 <= response.status < 300:
                            if not verified_hit.done():
                                verified_hit.set_result((urljoin(page_url, resp_url), response.request))
                        elif response.status >= 400:
                            failed_urls.add(resp_url)
                    except Exception:
                        pass

                page.on("request", _on_request)
                page.on("response", _on_response)
                if HLS_BROWSER_BLOCK_ADS:
                    await page.route("**/*", _block_heavy)
                await page.add_init_script(_TAP_JS)

                loaded = False
                try:
                    await page.goto(
                        page_url, wait_until="domcontentloaded", timeout=timeout * 1000,
                    )
                    loaded = True
                except Exception as e:
                    if using_warp:
                        err_low = str(e).lower()
                        proxy_err_marker = any(
                            m in err_low
                            for m in (
                                "err_proxy_connection_failed",
                                "err_tunnel_connection_failed",
                                "err_socks_connection_failed",
                            )
                        )
                        if proxy_err_marker or not await check_proxy_reachable(
                            require_http=True, require_socks5=False, timeout=1.5
                        ):
                            raise WarpProxyError(
                                format_warp_proxy_error(e, feature="HLS")
                            ) from e
                    if not verified_hit.done():
                        print(f"[HLS] Browser: page load issue ({type(e).__name__}: {e})")

                url = ""
                source = ""
                winning_req = None
                effective_page_url = page.url or page_url
                if not effective_page_url.startswith(("http://", "https://")):
                    effective_page_url = page_url

                quiet = max(5, min(15, timeout // 4))
                deadline = loop.time() + timeout
                tried_click = False

                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        url, winning_req = await asyncio.wait_for(
                            asyncio.shield(verified_hit), timeout=min(0.5, remaining),
                        )
                        source = "network"
                        break
                    except asyncio.TimeoutError:
                        pass

                    if loaded and not tried_click and fallback_req is None:
                        # If page has a click-to-play overlay, wake it once.
                        if (time.time() - last_request_at) >= 1.5:
                            tried_click = True
                            await _try_wake_player(page)

                    if (
                        fallback_req is not None
                        and fallback_req.url not in failed_urls
                        and (time.time() - fallback_seen_at) >= 1.5
                    ):
                        url = fallback_url
                        winning_req = fallback_req
                        source = "network"
                        break

                    if (loaded and requests_seen
                            and (time.time() - last_request_at) >= quiet):
                        break

                if not url and fallback_req is not None and fallback_url:
                    url = fallback_url
                    winning_req = fallback_req
                    source = "network"

                headers: Dict[str, str] = {}
                if url and winning_req is not None:
                    try:
                        frame_url = winning_req.frame.url if winning_req.frame else ""
                        if frame_url and frame_url.startswith(("http://", "https://")):
                            effective_page_url = frame_url
                    except Exception:
                        pass
                    headers = await _extract_request_headers(winning_req, effective_page_url)

                if not url:
                    tapped_url, frame_url = await _read_tap(page)
                    if tapped_url:
                        base_for_tap = (
                            frame_url
                            if frame_url and frame_url.startswith(("http://", "https://"))
                            else effective_page_url
                        )
                        tapped_url = urljoin(base_for_tap, tapped_url)
                        if tapped_url.lower().startswith(("http://", "https://")):
                            url = tapped_url
                            source = "player-config"
                            effective_page_url = base_for_tap
                            headers = default_headers(url, referer=effective_page_url)

                elapsed = time.time() - started
                if url:
                    cookies = await _extract_cookies(context, url, effective_page_url)
                    print(f"[HLS] Browser: manifest via {source} after {elapsed:.1f}s "
                          f"({requests_seen} requests)")
                    return BrowserScan(
                        url=url,
                        source=source,
                        reason="ok",
                        requests=requests_seen,
                        used_proxy=(using_warp or proxy_cfg is not None),
                        used_warp=using_warp,
                        page_url=effective_page_url,
                        headers=headers,
                        cookies=cookies,
                    )
                print(f"[HLS] Browser: no manifest after {elapsed:.1f}s "
                      f"({requests_seen} requests)")
                return BrowserScan(
                    reason="no-manifest",
                    requests=requests_seen,
                    page_url=effective_page_url,
                )
            except WarpProxyError:
                raise
            except Exception as e:
                return BrowserScan(
                    reason="error", detail=f"{type(e).__name__}: {e}",
                    requests=requests_seen,
                )
            finally:
                for closer in (page, context):
                    if closer is not None:
                        try:
                            await closer.close()
                        except Exception:
                            pass
                if is_ephemeral and browser is not None:
                    try:
                        await browser.close()
                    except Exception:
                        pass
        finally:
            _active -= 1
            _browser_last_used = time.time()


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    from aiohttp import web

    # The loopback pages below must never be routed through proxy.txt.
    import app.hls.client as _client_mod
    _client_mod.HLS_DISABLE_PROXY = True

    failures: list = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    print("--- manifest / ad-host predicates (no browser needed) ---")
    _check("m3u8 with query is a manifest",
           _looks_like_manifest("https://cdn/x/master.m3u8?t=abc&e=1"))
    _check("m3u/js/vtt are not", not _looks_like_manifest("https://cdn/x/a.js")
           and not _looks_like_manifest("https://cdn/x/master.m3u?t=1")
           and not _looks_like_manifest("https://cdn/x/seg.ts"))
    _check("ad host blocked", _is_blocked_url("https://pagead2.googlesyndication.com/x.png")
           and _is_blocked_url("https://openfpcdn.io/botd/v1"))
    _check("cdn host not blocked",
           not _is_blocked_url("https://ugc-cdn-caching-x.cloudwindow-route.com/a.m3u8"))
    _check("proxy config: none -> no proxy", _playwright_proxy() is None)

    if not browser_available():
        print("--- browser checks SKIPPED (playwright not installed) ---")
        print("    install with: pip install playwright && "
              "python -m playwright install chromium")
        print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
        sys.exit(1 if failures else 0)

    print("--- JS-only page, loopback (needs Chromium) ---")

    JS_PAGE = """<html><body><video id="v"></video><script>
      fetch('/api/config').then(r => r.json()).then(cfg => fetch(cfg.file));
    </script></body></html>"""

    TAP_PAGE = """<html><body><video id="v"></video><script>
      window.jwplayer = function () {
        return { setup: function (cfg) { window.__cfg = cfg; } };
      };
      window.jwplayer().setup({
        playlist: [{ sources: [{ file: '/cdn/tapped.m3u8?t=tap', type: 'hls' }] }],
      });
    </script></body></html>"""

    IFRAME_HLS_PAGE = """<html><body>
      <iframe src="/embed-player"></iframe>
    </body></html>"""

    EMBED_PLAYER_HTML = """<html><body><script>
      window.Hls = function() {};
      window.Hls.prototype.loadSource = function(u) { fetch(u); };
      var h = new window.Hls();
      h.loadSource('/cdn/iframe.m3u8?t=frm');
    </script></body></html>"""

    BARE_PAGE = """<html><body><video id="v"></video><p>nothing here</p></body></html>"""

    async def _run_browser_checks() -> None:
        app = web.Application()

        async def js_page(request):
            resp = web.Response(text=JS_PAGE, content_type="text/html")
            resp.set_cookie("br_tok", "cookie_42", path="/")
            return resp

        async def tap_page(request):
            return web.Response(text=TAP_PAGE, content_type="text/html")

        async def iframe_page(request):
            return web.Response(text=IFRAME_HLS_PAGE, content_type="text/html")

        async def embed_player(request):
            return web.Response(text=EMBED_PLAYER_HTML, content_type="text/html")

        async def bare_page(request):
            return web.Response(text=BARE_PAGE, content_type="text/html")

        async def api(request):
            return web.json_response({"file": "/cdn/real.m3u8?t=js"})

        async def manifest(request):
            return web.Response(
                text="#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nindex.m3u8\n",
                content_type="application/vnd.apple.mpegurl")

        app.router.add_get("/js-only", js_page)
        app.router.add_get("/tap-only", tap_page)
        app.router.add_get("/iframe-page", iframe_page)
        app.router.add_get("/embed-player", embed_player)
        app.router.add_get("/bare", bare_page)
        app.router.add_get("/api/config", api)
        app.router.add_get("/cdn/real.m3u8", manifest)
        app.router.add_get("/cdn/tapped.m3u8", manifest)
        app.router.add_get("/cdn/iframe.m3u8", manifest)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        base = f"http://127.0.0.1:{port}"

        try:
            scan = await resolve_stream_with_browser(f"{base}/js-only", timeout=25)
            _check("JS-built manifest captured from the network",
                   scan.url == f"{base}/cdn/real.m3u8?t=js", f"{scan.reason} {scan.url}")
            _check("source reported as network", scan.source == "network", scan.source)
            _check("page made requests", scan.requests > 0, str(scan.requests))
            _check("matching UA & Referer captured",
                   scan.headers.get("User-Agent") == DEFAULT_USER_AGENT
                   and scan.headers.get("Referer") == f"{base}/js-only",
                   str(scan.headers))
            _check("cookies captured from browser context",
                   scan.cookies.get("br_tok") == "cookie_42",
                   str(scan.cookies))

            scan2 = await resolve_stream_with_browser(f"{base}/tap-only", timeout=6)
            _check("URL built but never requested is tapped",
                   scan2.url == f"{base}/cdn/tapped.m3u8?t=tap",
                   f"{scan2.reason} {scan2.url}")
            _check("source reported as player-config",
                   scan2.source == "player-config", scan2.source)
            _check("tapped scan carries headers",
                   scan2.headers.get("Referer") == f"{base}/tap-only",
                   str(scan2.headers))

            scan_frm = await resolve_stream_with_browser(f"{base}/iframe-page", timeout=15)
            _check("iframe Hls.prototype.loadSource manifest + frame Referer captured",
                   scan_frm.url == f"{base}/cdn/iframe.m3u8?t=frm"
                   and scan_frm.page_url == f"{base}/embed-player"
                   and scan_frm.headers.get("Referer") == f"{base}/embed-player",
                   str(scan_frm))

            scan3 = await resolve_stream_with_browser(f"{base}/bare", timeout=6)
            _check("page without a manifest reports no-manifest",
                   scan3.url is None and scan3.reason == "no-manifest",
                   f"{scan3.reason} {scan3.url}")
            _check("no job left active", _active == 0, str(_active))

            await close_browser()
            _check("browser closed on request", _browser is None)
        finally:
            await close_browser()
            await runner.cleanup()

    asyncio.run(_run_browser_checks())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
