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
first HLS manifest the page asks for:

* **Network capture** — every request Playwright reports is checked for an
  ``.m3u8``; the first hit wins and the page is closed immediately (we do not
  wait for playback, and no video data is downloaded).
* **JS tap** (fallback) — an init script wraps ``jwplayer``, ``fetch`` and
  ``XMLHttpRequest``, so a URL that is *built* but never requested (autoplay
  blocked, player paused) is still recovered.
* **Ad/weight blocking** — images, fonts, stylesheets and known ad hosts are
  aborted to keep the scan fast and cheap; the document, scripts and XHR pass.

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
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urljoin, urlparse

from app.config import (
    HLS_BROWSER_BLOCK_ADS,
    HLS_BROWSER_IDLE,
    HLS_BROWSER_MAX_CONCURRENCY,
    HLS_BROWSER_PROXY,
    HLS_BROWSER_TIMEOUT,
)
from app.hls.client import read_proxy_url

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
)

#: Resource types that can never carry the manifest and only cost time.
_BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}

#: Ad/tracker hosts that are aborted by default (the page's own player never
#: lives on them, and they are the main reason these pages are slow).
_BLOCKED_HOST_PARTS = (
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adsbygoogle", "adservice", "popads", "popcash", "propellerads",
    "exoclick", "juicyads", "trafficjunky", "adsterra", "hilltopads",
    "facebook.net", "criteo", "taboola", "outbrain", "openfpcdn",
)


def browser_available() -> bool:
    """True when the ``playwright`` package is importable.

    The Chromium download is checked at launch time (Playwright reports a clear
    "Executable doesn't exist" error, which :func:`resolve_stream_with_browser`
    turns into an actionable log line), so this stays a cheap, cached check.
    """
    global _AVAILABLE
    if _AVAILABLE is None:
        _AVAILABLE = importlib.util.find_spec("playwright") is not None
        if not _AVAILABLE:
            print("[HLS] Browser engine unavailable: 'playwright' is not installed "
                  "(pip install playwright && python -m playwright install chromium)")
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
    #: Requests the page made while we watched (a rough "the page really
    #: loaded" signal, and useful when a scan fails).
    requests: int = 0
    #: True when Chromium itself went through the configured proxy. The caller
    #: uses this to keep the download on the same egress as the page.
    used_proxy: bool = False


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

#: Installed before any page script runs: records the first HLS URL the page
#: *builds*, whether or not the request ever leaves the browser (autoplay
#: blocked, player paused, request aborted by our own ad filter...).
_TAP_JS = r"""
(() => {
  if (window.__hlsTapInstalled) return;
  window.__hlsTapInstalled = true;
  const note = (u) => {
    try {
      if (typeof u === 'string' && u.indexOf('.m3u8') !== -1 && !window.__hlsTap) {
        window.__hlsTap = u;
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
    Object.defineProperty(window, 'Hls', {
      configurable: true,
      get() { return H; },
      set(v) {
        if (typeof v === 'function' && typeof v.loadSource === 'function') {
          const OL = v.loadSource;
          v.loadSource = function (u) { note(u); return OL.apply(this, arguments); };
        }
        H = v;
      },
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
_sem: Optional[asyncio.Semaphore] = None
_idle_task: Optional[asyncio.Task] = None


def _browser_used_proxy() -> bool:
    """True when the running Chromium was launched through the proxy."""
    return _browser_used


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


async def _get_browser():
    """Start Chromium on first use and reuse it for every later job."""
    global _pw, _browser, _browser_last_used, _browser_used, _idle_task
    if _browser is not None:
        try:
            if _browser.is_connected():
                _browser_last_used = time.time()
                return _browser
        except Exception:
            pass
        _browser = None

    from playwright.async_api import async_playwright

    if _pw is None:
        _pw = await async_playwright().start()

    kwargs: Dict[str, Any] = {"headless": True, "args": list(_LAUNCH_ARGS)}
    proxy = _playwright_proxy()
    if proxy:
        kwargs["proxy"] = proxy
    _browser = await _pw.chromium.launch(**kwargs)
    _browser_used = proxy is not None
    _browser_last_used = time.time()
    print(f"[HLS] Browser started (proxy={'yes' if proxy else 'no'})")

    if _idle_task is None or _idle_task.done():
        _idle_task = asyncio.create_task(_idle_watchdog())
    return _browser


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
            print(f"[HLS] Browser idle {HLS_BROWSER_IDLE}s — closing Chromium")
            await close_browser()
            break
    _idle_task = None


async def close_browser() -> None:
    """Shut Chromium and Playwright down (safe to call at any time)."""
    global _browser, _pw, _browser_last_used
    if _active:
        return
    if _browser is not None:
        try:
            await _browser.close()
        except Exception as e:
            print(f"[HLS] Browser close failed: {e}")
        _browser = None
    if _pw is not None:
        try:
            await _pw.stop()
        except Exception:
            pass
        _pw = None


async def _block_heavy(route, request) -> None:
    """Abort ads, images, fonts and stylesheets; let everything else through."""
    try:
        if request.resource_type in _BLOCKED_RESOURCE_TYPES or _is_blocked_url(request.url):
            await route.abort()
        else:
            await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass


async def _read_tap(page) -> str:
    """Read the URL the page's JS built but never requested."""
    try:
        value = await page.evaluate("() => window.__hlsTap || ''")
    except Exception:
        return ""
    return value if isinstance(value, str) else ""


def _log_launch_failure(err: Exception) -> None:
    """One clear log line per failure mode, with the fix where we know it."""
    global _HINT_LOGGED
    message = str(err)
    if "Executable doesn't exist" in message or "playwright install" in message:
        if not _HINT_LOGGED:
            _HINT_LOGGED = True
            print("[HLS] Browser engine: Chromium is not downloaded — run "
                  "'python -m playwright install chromium' in the bot's venv.")
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
        requests_seen = 0
        last_request_at = time.time()
        try:
            try:
                browser = await _get_browser()
            except Exception as e:
                _log_launch_failure(e)
                return BrowserScan(reason="no-browser", detail=f"{type(e).__name__}: {e}")

            loop = asyncio.get_running_loop()
            first_hit: asyncio.Future = loop.create_future()

            try:
                context = await browser.new_context(
                    viewport={"width": 1280, "height": 720},
                    locale="en-US",
                    ignore_https_errors=True,
                )
                page = await context.new_page()
                page.set_default_timeout(timeout * 1000)

                def _on_request(request) -> None:
                    nonlocal requests_seen, last_request_at
                    requests_seen += 1
                    last_request_at = time.time()
                    try:
                        if not first_hit.done() and _looks_like_manifest(request.url):
                            # Playwright reports absolute URLs, but a page can
                            # still emit a protocol-relative one.
                            first_hit.set_result(urljoin(page_url, request.url))
                    except Exception:
                        pass

                page.on("request", _on_request)
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
                    # A slow, partly blocked page can still have produced it.
                    if not first_hit.done():
                        print(f"[HLS] Browser: page load issue ({type(e).__name__}: {e})")

                url = ""
                source = ""
                # Wait for the manifest, but stop early once the page has loaded
                # and its network has been quiet for a while — a page that will
                # never ask for a playlist should not burn the whole timeout.
                quiet = max(5, min(15, timeout // 4))
                deadline = loop.time() + timeout
                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        url = await asyncio.wait_for(
                            asyncio.shield(first_hit), timeout=min(0.5, remaining),
                        )
                        source = "network"
                        break
                    except asyncio.TimeoutError:
                        pass
                    if (loaded and requests_seen
                            and (time.time() - last_request_at) >= quiet):
                        break

                if not url:
                    url = await _read_tap(page)
                    if url:
                        # The JS tap can hand back a page-relative path.
                        url = urljoin(page_url, url)
                        if not url.lower().startswith(("http://", "https://")):
                            url = ""
                        else:
                            source = "player-config"

                elapsed = time.time() - started
                if url:
                    print(f"[HLS] Browser: manifest via {source} after {elapsed:.1f}s "
                          f"({requests_seen} requests)")
                    return BrowserScan(
                        url=url, source=source, reason="ok", requests=requests_seen,
                        used_proxy=_browser_used_proxy(),
                    )
                print(f"[HLS] Browser: no manifest after {elapsed:.1f}s "
                      f"({requests_seen} requests)")
                return BrowserScan(reason="no-manifest", requests=requests_seen)
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

    BARE_PAGE = """<html><body><video id="v"></video><p>nothing here</p></body></html>"""

    async def _run_browser_checks() -> None:
        app = web.Application()

        async def js_page(request):
            return web.Response(text=JS_PAGE, content_type="text/html")

        async def tap_page(request):
            return web.Response(text=TAP_PAGE, content_type="text/html")

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
        app.router.add_get("/bare", bare_page)
        app.router.add_get("/api/config", api)
        app.router.add_get("/cdn/real.m3u8", manifest)
        app.router.add_get("/cdn/tapped.m3u8", manifest)

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

            scan2 = await resolve_stream_with_browser(f"{base}/tap-only", timeout=6)
            _check("URL built but never requested is tapped",
                   scan2.url == f"{base}/cdn/tapped.m3u8?t=tap",
                   f"{scan2.reason} {scan2.url}")
            _check("source reported as player-config",
                   scan2.source == "player-config", scan2.source)

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

    _browser_last_used = 0.0
