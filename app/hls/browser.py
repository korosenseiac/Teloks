"""
hls/browser.py — Universal last-resort stream sniffer with headless Chromium.

Why this exists
---------------
Some hosts build the stream URL inside obfuscated JS behind bot/device
fingerprinting, ad-click overlays, multi-iframe players, or disguised
extensionless endpoints. When Tier 1 (HTTP) and Tier 2 (yt-dlp) miss, this
module opens the page in Playwright's Chromium and captures the real stream:

* **Body-sniffed network capture** — inspects 2xx/206 responses (including
  extensionless/disguised ``text/plain``, ``application/octet-stream``,
  ``image/*`` XHRs) for ``#EXTM3U`` (master preferred over variant; subtitle/ad
  manifests deprioritized; HTML 200 error pages rejected), ``<MPD`` (DASH), and
  direct progressive ``video/mp4`` / ``video/webm`` streams.
* **Popup killer & multi-pass click-through** — auto-closes spawned ad tabs,
  neutralizes ``window.open``, dismisses transparent full-screen ad overlays,
  and performs up to 3 click passes across common players & iframes.
* **Universal JS tap** — hooks ``Hls.prototype.loadSource`` (unconditionally),
  ``jwplayer``, ``videojs``, ``Plyr``, ``shaka``, ``Artplayer``, ``DPlayer``,
  ``Clappr``, ``dashjs``, ``HTMLMediaElement``, ``HTMLSourceElement``,
  ``setAttribute('src')``, ``fetch`` and ``XMLHttpRequest`` across all frames.
* **CORS-safe stealth & VPS memory caps** — uses CDP
  ``Emulation.setUserAgentOverride`` (so ``Sec-CH-UA`` matches Chrome without
  breaking cross-origin CORS preflights), hides headless markers, waits past
  Cloudflare interstitials, and caps Chromium renderer memory for 700 MB VPSes.

Self-test:

    python -m app.hls.browser
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from app.config import (
    HLS_BROWSER_BLOCK_ADS,
    HLS_BROWSER_IDLE,
    HLS_BROWSER_MAX_CONCURRENCY,
    HLS_BROWSER_PROXY,
    HLS_BROWSER_TIMEOUT,
)
from app.hls.client import (
    DEFAULT_USER_AGENT,
    decode_text,
    default_headers,
    read_proxy_url,
    sniff_stream_bytes,
)
from app.hls.extract import ExtractedStream
from app.warp import (
    WarpProxyError,
    check_proxy_reachable,
    ensure_proxy_ready,
    format_warp_proxy_error,
)

#: Chromium switches that make it survive on a small headless VPS (MemoryMax=700M).
_LAUNCH_ARGS = (
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
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
    "--window-size=1280,720",
    "--renderer-process-limit=2",
    "--js-flags=--max-old-space-size=256",
)

#: Resource types that never carry a video stream and only cost RAM/bandwidth.
#: Keep stylesheets, scripts, XHR/fetch and media unblocked so players boot and
#: progressive MP4 / extensionless HLS requests can be observed.
_BLOCKED_RESOURCE_TYPES = {"image", "font"}

#: Ad/tracker hosts aborted by default.
_BLOCKED_HOST_PARTS = (
    "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
    "adsbygoogle", "adservice", "popads", "popcash", "propellerads",
    "exoclick", "juicyads", "trafficjunky", "adsterra", "hilltopads",
    "facebook.net", "criteo", "taboola", "outbrain", "histats",
    "mc.yandex", "adnxs", "pubmatic", "rubiconproject", "smartadserver",
    "mgid.com", "revcontent", "onclickalgo", "acscdn", "alwingulla",
)

_AD_PATH_MARKERS = ("/vast", "preroll", "/ads/", "adserver", "/popunder", "doubleclick", "googlesyndication")

_PREVIEW_PATH_MARKERS = ("/preview/", "preview.", "trailer", "teaser", "/thumbs/", "sprite")

_STATIC_NON_STREAM_EXTS = (
    ".js", ".mjs", ".css", ".map", ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".svg", ".ico", ".vtt", ".srt", ".ass", ".ssa",
)

_DIRECT_VIDEO_EXTS = (".mp4", ".webm", ".mov", ".mkv", ".flv", ".m4v")
_DIRECT_VIDEO_CTYPES = (
    "video/mp4", "video/webm", "video/quicktime", "video/x-matroska",
    "video/x-flv", "video/ogg", "video/3gpp",
)
_MIN_DIRECT_VIDEO_BYTES = 256 * 1024
_MAX_SNIFF_BODY_BYTES = 512 * 1024

_CHALLENGE_TITLE_MARKERS = (
    "just a moment", "checking your browser", "attention required",
    "ddos-guard", "please wait", "verify you are human", "security check",
)
_USELESS_TITLES = {
    "just a moment...", "attention required!", "403 forbidden", "404 not found",
    "access denied", "error", "loading", "loading...", "embed", "player",
    "video", "video player", "watch", "stream", "index", "home", "untitled",
}

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


def _find_cached_chromium_executable() -> Optional[str]:
    """Locate an existing cached Chromium executable, preferring newer headless-shell builds."""
    roots: list[str] = []
    env_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env_path:
        roots.append(env_path)
    for candidate in _CACHE_CANDIDATES:
        if candidate not in roots:
            roots.append(candidate)
    user_cache = os.path.expanduser("~/.cache/ms-playwright")
    if user_cache not in roots:
        roots.append(user_cache)
    local_app = os.environ.get("LOCALAPPDATA")
    if local_app:
        win_cache = os.path.join(local_app, "ms-playwright")
        if win_cache not in roots:
            roots.append(win_cache)

    candidates: list[Tuple[int, int, str]] = []
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        try:
            entries = os.listdir(root)
        except Exception:
            continue
        for entry in entries:
            m = re.match(r"^(chromium_headless_shell|chromium)-(\d+)$", entry)
            if not m:
                continue
            kind, rev_s = m.group(1), m.group(2)
            tier = 0 if kind == "chromium_headless_shell" else 1
            rev = int(rev_s)
            build_dir = os.path.join(root, entry)
            if not os.path.isdir(build_dir):
                continue

            rel_targets: list[Tuple[str, ...]] = []
            if kind == "chromium_headless_shell":
                rel_targets.extend([
                    ("chrome-headless-shell-linux64", "chrome-headless-shell"),
                    ("chrome-headless-shell-linux-arm64", "chrome-headless-shell"),
                    ("chrome-headless-shell-win64", "chrome-headless-shell.exe"),
                ])
                try:
                    for sub in sorted(os.listdir(build_dir)):
                        if sub.startswith("chrome-headless-shell-mac"):
                            rel_targets.append((sub, "chrome-headless-shell"))
                except Exception:
                    pass
            else:
                rel_targets.extend([
                    ("chrome-linux64", "chrome"),
                    ("chrome-linux", "chrome"),
                    ("chrome-win64", "chrome.exe"),
                    ("chrome-win", "chrome.exe"),
                ])
                try:
                    for sub in sorted(os.listdir(build_dir)):
                        if sub.startswith("chrome-mac"):
                            rel_targets.append((
                                sub,
                                "Google Chrome for Testing.app",
                                "Contents",
                                "MacOS",
                                "Google Chrome for Testing",
                            ))
                            rel_targets.append((sub, "chrome"))
                except Exception:
                    pass

            for parts in rel_targets:
                exe = os.path.join(build_dir, *parts)
                if os.path.isfile(exe):
                    candidates.append((tier, -rev, exe))
                    break

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


async def _launch_chromium_with_fallback(pw: Any, launch_kwargs: Dict[str, Any]) -> Any:
    """Launch Playwright Chromium, falling back to an existing cached build if missing."""
    try:
        return await pw.chromium.launch(**launch_kwargs)
    except Exception as orig_err:
        if "Executable doesn't exist" not in str(orig_err):
            raise
        fallback_exe = _find_cached_chromium_executable()
        if not fallback_exe:
            raise
        print(
            f"[HLS] Browser: Playwright expected build missing — falling back to cached "
            f"Chromium {fallback_exe}; update with 'sudo bash deploy/install-browser.sh'"
        )
        try:
            return await pw.chromium.launch(**{**launch_kwargs, "executable_path": fallback_exe})
        except Exception:
            raise orig_err


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

    #: The stream/manifest URL, or None.
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
    #: New backward-compatible contract fields:
    stream_type: str = "hls"
    title: str = ""
    audio_url: Optional[str] = None
    audio_headers: Optional[Dict[str, str]] = None
    manifest_text: str = ""
    cookie_entries: List[Dict[str, Any]] = field(default_factory=list)
    fallback_urls: List[str] = field(default_factory=list)

    def to_extracted_stream(self, *, force_direct: bool = False) -> ExtractedStream:
        return ExtractedStream(
            url=self.url,
            stream_type=self.stream_type or "hls",
            audio_url=self.audio_url,
            audio_headers=dict(self.audio_headers) if self.audio_headers else None,
            page_url=self.page_url,
            title=self.title,
            manifest_text=self.manifest_text,
            headers=dict(self.headers or {}),
            cookies=dict(self.cookies or {}),
            cookie_entries=list(self.cookie_entries or []),
            fallback_urls=list(self.fallback_urls or []),
            extractor="browser",
            used_proxy=self.used_proxy,
            used_warp=self.used_warp,
            force_direct=force_direct,
            reason=self.reason,
            detail=self.detail,
            player_page=bool(self.url),
        )


def _looks_like_manifest(url: str) -> bool:
    """True for an HLS manifest URL (query string preserved)."""
    if ".m3u8" not in url.lower():
        return False
    return url.lower().split("?", 1)[0].endswith((".m3u8", ".m3u"))


def _looks_like_dash(url: str) -> bool:
    return url.lower().split("?", 1)[0].endswith(".mpd")


def _looks_like_direct_video(url: str) -> bool:
    return url.lower().split("?", 1)[0].endswith(_DIRECT_VIDEO_EXTS)


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


def _is_preview_url(url: str) -> bool:
    low = url.lower()
    return any(marker in low for marker in _PREVIEW_PATH_MARKERS)


def _chromium_major_version(browser: Any) -> str:
    """Return Chromium's major version string (fallback ``"131"`` if unknown)."""
    raw = getattr(browser, "version", "")
    if isinstance(raw, str):
        m = re.match(r"^\s*(\d+)(?:\.|$)", raw.strip())
        if m:
            return m.group(1)
    return "131"


def _build_browser_identity(browser: Any) -> Tuple[str, str, Dict[str, str]]:
    """Build consistent UA + Client-Hint headers matching *browser*'s Chromium version."""
    major = _chromium_major_version(browser)
    if major == "131" and "Chrome/131." in DEFAULT_USER_AGENT:
        ua = DEFAULT_USER_AGENT
    else:
        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{major}.0.0.0 Safari/537.36"
        )
    sec_ch_ua = (
        f'"Not:A-Brand";v="99", "Google Chrome";v="{major}", "Chromium";v="{major}"'
    )
    extra_headers = {
        "Sec-CH-UA": sec_ch_ua,
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
    }
    return major, ua, extra_headers


def _build_cdp_ua_override(major: str, ua: str) -> Dict[str, Any]:
    """CDP ``Emulation.setUserAgentOverride`` payload (CORS-safe Client Hints)."""
    full = f"{major}.0.0.0"
    return {
        "userAgent": ua,
        "acceptLanguage": "en-US,en;q=0.9",
        "platform": "Win32",
        "userAgentMetadata": {
            "brands": [
                {"brand": "Not:A-Brand", "version": "99"},
                {"brand": "Google Chrome", "version": str(major)},
                {"brand": "Chromium", "version": str(major)},
            ],
            "fullVersionList": [
                {"brand": "Not:A-Brand", "version": "99.0.0.0"},
                {"brand": "Google Chrome", "version": full},
                {"brand": "Chromium", "version": full},
            ],
            "fullVersion": full,
            "platform": "Windows",
            "platformVersion": "10.0.0",
            "architecture": "x86",
            "model": "",
            "mobile": False,
            "bitness": "64",
            "wow64": False,
        },
    }


#: Installed before any page script runs: stealth patches, popup neutralizer,
#: and universal player/network tap across all frames.
_TAP_JS_TEMPLATE = r"""
(() => {
  if (window.__hlsTapInstalled) return;
  window.__hlsTapInstalled = true;

  // --- Stealth patches ---
  try {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
  } catch (e) {}
  try {
    if (!window.chrome) {
      window.chrome = { runtime: {}, app: {}, csi: () => ({}), loadTimes: () => ({}) };
    }
  } catch (e) {}
  try {
    if (!navigator.plugins || navigator.plugins.length === 0) {
      const fakePlugins = [
        { name: 'PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
        { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
      ];
      Object.defineProperty(navigator, 'plugins', { get: () => fakePlugins });
    }
  } catch (e) {}
  try {
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
  } catch (e) {}
  try {
    if (!navigator.hardwareConcurrency || navigator.hardwareConcurrency < 2) {
      Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
    }
    if (!navigator.deviceMemory) {
      Object.defineProperty(navigator, 'deviceMemory', { get: () => 8 });
    }
  } catch (e) {}
  try {
    if (window.outerWidth === 0) Object.defineProperty(window, 'outerWidth', { get: () => 1280 });
    if (window.outerHeight === 0) Object.defineProperty(window, 'outerHeight', { get: () => 720 });
  } catch (e) {}
  try {
    const origGetParam = WebGLRenderingContext && WebGLRenderingContext.prototype.getParameter;
    if (origGetParam) {
      WebGLRenderingContext.prototype.getParameter = function (p) {
        if (p === 37445) return 'Google Inc. (NVIDIA)';
        if (p === 37446) return 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)';
        return origGetParam.call(this, p);
      };
    }
  } catch (e) {}
  try {
    const major = __HLS_CHROMIUM_MAJOR__;
    const full = major + '.0.0.0';
    const brands = [
      { brand: 'Not:A-Brand', version: '99' },
      { brand: 'Google Chrome', version: major },
      { brand: 'Chromium', version: major },
    ];
    const fullVersionList = [
      { brand: 'Not:A-Brand', version: '99.0.0.0' },
      { brand: 'Google Chrome', version: full },
      { brand: 'Chromium', version: full },
    ];
    const uaData = {
      brands,
      mobile: false,
      platform: 'Windows',
      toJSON() {
        return { brands: this.brands, mobile: this.mobile, platform: this.platform };
      },
      getHighEntropyValues(hints) {
        const all = {
          architecture: 'x86',
          bitness: '64',
          brands,
          fullVersionList,
          mobile: false,
          model: '',
          platform: 'Windows',
          platformVersion: '10.0.0',
          uaFullVersion: full,
          wow64: false,
        };
        if (!Array.isArray(hints)) return Promise.resolve(all);
        const out = { brands, mobile: false, platform: 'Windows' };
        for (const h of hints) {
          if (Object.prototype.hasOwnProperty.call(all, h)) out[h] = all[h];
        }
        return Promise.resolve(out);
      },
    };
    try {
      Object.defineProperty(Navigator.prototype, 'userAgentData', {
        configurable: true,
        get: () => uaData,
      });
    } catch (e) {
      Object.defineProperty(navigator, 'userAgentData', {
        configurable: true,
        get: () => uaData,
      });
    }
  } catch (e) {}

  // Neutralize ad popups/popunders so clicks reach the actual player.
  try {
    window.open = function () {
      return { closed: false, close() {}, focus() {}, blur() {}, location: { href: '' } };
    };
  } catch (e) {}

  // --- Universal stream tap ---
  window.__hlsTapList = window.__hlsTapList || [];
  window.__hlsTapDirectList = window.__hlsTapDirectList || [];
  window.__hlsTapDashList = window.__hlsTapDashList || [];

  const asStr = (u) => {
    if (typeof u === 'string') return u;
    if (u && typeof u.url === 'string') return u.url;
    if (u && typeof u.href === 'string') return u.href;
    return '';
  };
  const isAd = (u) => {
    const low = String(u).toLowerCase();
    return low.indexOf('doubleclick') !== -1 || low.indexOf('googlesyndication') !== -1
      || low.indexOf('/vast') !== -1 || low.indexOf('preroll') !== -1
      || low.indexOf('adserver') !== -1 || low.indexOf('/ads/') !== -1;
  };
  const recordHls = (raw) => {
    try {
      const u = asStr(raw).trim();
      if (!u || u.startsWith('blob:') || u.startsWith('data:') || isAd(u)) return;
      if (!window.__hlsTap) window.__hlsTap = u;
      if (window.__hlsTapList.indexOf(u) === -1) window.__hlsTapList.push(u);
    } catch (e) {}
  };
  const note = (raw, forcedKind) => {
    try {
      const u = asStr(raw).trim();
      if (!u || u.startsWith('blob:') || u.startsWith('data:') || isAd(u)) return;
      if (forcedKind === 'hls') { recordHls(u); return; }
      if (forcedKind === 'dash') {
        if (window.__hlsTapDashList.indexOf(u) === -1) window.__hlsTapDashList.push(u);
        return;
      }
      const low = u.toLowerCase();
      const basePath = low.split('?')[0];
      if (low.indexOf('.m3u8') !== -1 || basePath.endsWith('.m3u')) {
        recordHls(u);
      } else if (basePath.endsWith('.mpd') || low.indexOf('.mpd?') !== -1) {
        if (window.__hlsTapDashList.indexOf(u) === -1) window.__hlsTapDashList.push(u);
      } else if (
        basePath.endsWith('.mp4') || basePath.endsWith('.webm')
        || basePath.endsWith('.mov') || basePath.endsWith('.m4v')
      ) {
        if (window.__hlsTapDirectList.indexOf(u) === -1) window.__hlsTapDirectList.push(u);
      }
    } catch (e) {}
  };
  const walk = (o, depth) => {
    try {
      if (!o || depth > 7) return;
      if (typeof o === 'string') { note(o); return; }
      if (Array.isArray(o)) { for (const v of o) walk(v, depth + 1); return; }
      if (typeof o === 'object') {
        const typ = String(o.type || o.mimeType || '').toLowerCase();
        const isHlsObj = typ.indexOf('hls') !== -1 || typ.indexOf('m3u8') !== -1 || typ.indexOf('mpegurl') !== -1;
        const isDashObj = typ.indexOf('dash') !== -1 || typ.indexOf('mpd') !== -1;
        for (const k of Object.keys(o)) {
          const val = o[k];
          if (typeof val === 'string' && (k === 'file' || k === 'src' || k === 'url' || k === 'source' || k === 'hls')) {
            if (isHlsObj || k === 'hls') note(val, 'hls');
            else if (isDashObj) note(val, 'dash');
            else note(val);
          } else {
            walk(val, depth + 1);
          }
        }
      }
    } catch (e) {}
  };

  // XHR & fetch
  const xo = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function (m, u) { note(u); return xo.apply(this, arguments); };
  const of = window.fetch;
  if (of) {
    window.fetch = function (input) {
      try { note(input); } catch (e) {}
      return of.apply(this, arguments);
    };
  }

  // HTMLMediaElement / HTMLSourceElement / setAttribute('src')
  for (const proto of [HTMLMediaElement.prototype, HTMLSourceElement.prototype]) {
    try {
      const desc = Object.getOwnPropertyDescriptor(proto, 'src');
      if (desc && desc.set) {
        Object.defineProperty(proto, 'src', {
          configurable: true,
          enumerable: desc.enumerable,
          get: desc.get,
          set(v) { note(v); return desc.set.call(this, v); },
        });
      }
    } catch (e) {}
  }
  try {
    const origSetAttr = Element.prototype.setAttribute;
    Element.prototype.setAttribute = function (k, v) {
      try {
        if (k && String(k).toLowerCase() === 'src' && (this.tagName === 'VIDEO' || this.tagName === 'SOURCE')) {
          note(v);
        }
      } catch (e) {}
      return origSetAttr.apply(this, arguments);
    };
  } catch (e) {}

  // JWPlayer
  let jw = window.jwplayer;
  const wrapSetup = (obj) => {
    try {
      if (obj && typeof obj.setup === 'function' && !obj.__hlsTapped) {
        const orig = obj.setup.bind(obj);
        obj.setup = function (cfg) { walk(cfg, 0); return orig(cfg); };
        obj.__hlsTapped = true;
      }
      if (obj && typeof obj.load === 'function' && !obj.__hlsLoadTapped) {
        const origL = obj.load.bind(obj);
        obj.load = function (cfg) { walk(cfg, 0); return origL(cfg); };
        obj.__hlsLoadTapped = true;
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

  // hls.js (UNCONDITIONAL capture — anything passed to loadSource is HLS!)
  try {
    let H = window.Hls;
    const hookHls = (v) => {
      if (v && v.prototype && typeof v.prototype.loadSource === 'function' && !v.prototype.__hlsTapped) {
        const orig = v.prototype.loadSource;
        v.prototype.loadSource = function (u) { recordHls(u); return orig.apply(this, arguments); };
        v.prototype.__hlsTapped = true;
      }
      if (typeof v === 'function' && typeof v.loadSource === 'function' && !v.__hlsTapped) {
        const OL = v.loadSource;
        v.loadSource = function (u) { recordHls(u); return OL.apply(this, arguments); };
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

  // Video.js
  try {
    let VJS = window.videojs;
    const wrapVjsPlayer = (p) => {
      try {
        if (p && typeof p.src === 'function' && !p.__hlsTapped) {
          const origSrc = p.src.bind(p);
          p.src = function (arg) { if (arg) walk(arg, 0); return origSrc.apply(this, arguments); };
          p.__hlsTapped = true;
        }
      } catch (e) {}
      return p;
    };
    const hookVjs = (fn) => {
      if (typeof fn !== 'function') return fn;
      const wrapped = function () {
        for (const a of arguments) walk(a, 0);
        return wrapVjsPlayer(fn.apply(this, arguments));
      };
      Object.assign(wrapped, fn);
      return wrapped;
    };
    if (VJS) VJS = hookVjs(VJS);
    Object.defineProperty(window, 'videojs', {
      configurable: true,
      get() { return VJS; },
      set(v) { VJS = hookVjs(v); },
    });
  } catch (e) {}

  // Shaka Player
  try {
    let SH = window.shaka;
    const hookShaka = (s) => {
      try {
        if (s && s.Player && s.Player.prototype && typeof s.Player.prototype.load === 'function' && !s.Player.prototype.__hlsTapped) {
          const orig = s.Player.prototype.load;
          s.Player.prototype.load = function (u) {
            const su = asStr(u);
            if (su.toLowerCase().indexOf('.mpd') !== -1) note(su, 'dash');
            else recordHls(su);
            return orig.apply(this, arguments);
          };
          s.Player.prototype.__hlsTapped = true;
        }
      } catch (e) {}
    };
    if (SH) hookShaka(SH);
    Object.defineProperty(window, 'shaka', {
      configurable: true,
      get() { return SH; },
      set(v) { hookShaka(v); SH = v; },
    });
  } catch (e) {}

  // Artplayer / DPlayer / Clappr / Plyr config constructors
  for (const ctorName of ['Artplayer', 'DPlayer', 'Plyr']) {
    try {
      let C = window[ctorName];
      const wrapCtor = (fn) => {
        if (typeof fn !== 'function') return fn;
        return new Proxy(fn, {
          construct(target, args) {
            for (const a of args) walk(a, 0);
            return Reflect.construct(target, args);
          },
          apply(target, thisArg, args) {
            for (const a of args) walk(a, 0);
            return Reflect.apply(target, thisArg, args);
          },
        });
      };
      if (C) C = wrapCtor(C);
      Object.defineProperty(window, ctorName, {
        configurable: true,
        get() { return C; },
        set(v) { C = wrapCtor(v); },
      });
    } catch (e) {}
  }
})();
"""


def _build_tap_js(major: str = "131") -> str:
    return _TAP_JS_TEMPLATE.replace("__HLS_CHROMIUM_MAJOR__", json.dumps(str(major or "131")))


_TAP_JS = _build_tap_js("131")

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
                    eph_browser = await _launch_chromium_with_fallback(_pw, eph_kwargs)
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

    _resolve_browser_cache()

    if _pw is None:
        _pw = await async_playwright().start()

    kwargs: Dict[str, Any] = {"headless": True, "args": list(_LAUNCH_ARGS)}
    if proxy:
        kwargs["proxy"] = proxy
    _browser = await _launch_chromium_with_fallback(_pw, kwargs)
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
    """Abort ads, images and fonts; let player assets and media streams pass."""
    try:
        rtype = request.resource_type
        url = request.url
        if _is_blocked_url(url) or _is_ad_manifest(url):
            await route.abort()
        elif rtype in _BLOCKED_RESOURCE_TYPES:
            await route.abort()
        else:
            await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass


async def _read_tap(page) -> Tuple[str, str, str]:
    """Read any tapped or DOM-attached stream URL across all page frames.

    Returns ``(url, frame_url, stream_type)``.
    """
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]
    if getattr(page, "main_frame", None) and page.main_frame not in frames:
        frames.insert(0, page.main_frame)

    fallback_direct: Tuple[str, str, str] = ("", "", "")
    fallback_dash: Tuple[str, str, str] = ("", "", "")

    for frame in frames:
        try:
            value = await frame.evaluate(
                """() => {
                  const hls = [];
                  if (window.__hlsTap) hls.push(window.__hlsTap);
                  if (Array.isArray(window.__hlsTapList)) {
                    for (const u of window.__hlsTapList) if (hls.indexOf(u) === -1) hls.push(u);
                  }
                  const dash = Array.isArray(window.__hlsTapDashList) ? window.__hlsTapDashList.slice() : [];
                  const direct = Array.isArray(window.__hlsTapDirectList) ? window.__hlsTapDirectList.slice() : [];
                  const els = document.querySelectorAll('video, source');
                  for (const el of els) {
                    const src = el.getAttribute('src') || el.currentSrc || el.src || '';
                    if (!src || src.startsWith('blob:') || src.startsWith('data:')) continue;
                    const low = src.toLowerCase();
                    if (low.indexOf('.m3u8') !== -1) {
                      if (hls.indexOf(src) === -1) hls.push(src);
                    } else if (low.indexOf('.mpd') !== -1) {
                      if (dash.indexOf(src) === -1) dash.push(src);
                    } else if (low.indexOf('.mp4') !== -1 || low.indexOf('.webm') !== -1 || el.tagName === 'VIDEO' || el.tagName === 'SOURCE') {
                      if (direct.indexOf(src) === -1) direct.push(src);
                    }
                  }
                  return { hls, dash, direct };
                }"""
            )
            f_url = ""
            try:
                f_url = frame.url or ""
            except Exception:
                f_url = ""

            if isinstance(value, str) and value:
                return value, f_url, "hls"
            if isinstance(value, dict):
                hls_list = value.get("hls") or []
                if isinstance(hls_list, list) and hls_list:
                    first_hls = str(hls_list[0] or "")
                    if first_hls:
                        return first_hls, f_url, "hls"
                dash_list = value.get("dash") or []
                if not fallback_dash[0] and isinstance(dash_list, list) and dash_list:
                    first_dash = str(dash_list[0] or "")
                    if first_dash:
                        fallback_dash = (first_dash, f_url, "dash")
                direct_list = value.get("direct") or []
                if not fallback_direct[0] and isinstance(direct_list, list) and direct_list:
                    first_dir = str(direct_list[0] or "")
                    if first_dir:
                        fallback_direct = (first_dir, f_url, "direct")
        except Exception:
            continue

    if fallback_dash[0]:
        return fallback_dash
    if fallback_direct[0]:
        return fallback_direct
    return "", "", ""


async def _dismiss_ad_overlays(page) -> None:
    """Hide transparent full-screen ad-shield overlays sitting above players."""
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]
    js = """() => {
      try {
        const vw = window.innerWidth || 1280;
        const vh = window.innerHeight || 720;
        const els = document.querySelectorAll('div, a, section');
        let hidden = 0;
        for (const el of els) {
          if (hidden > 12) break;
          if (el.querySelector('video, iframe')) continue;
          const st = window.getComputedStyle(el);
          if (st.position !== 'fixed' && st.position !== 'absolute') continue;
          const z = parseInt(st.zIndex || '0', 10);
          if (isNaN(z) || z < 999) continue;
          const r = el.getBoundingClientRect();
          if (r.width >= vw * 0.5 && r.height >= vh * 0.5) {
            const bg = st.backgroundColor || '';
            const op = parseFloat(st.opacity || '1');
            const looksBlank = (op < 0.25) || bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)'
              || (el.innerText || '').trim().length === 0;
            if (looksBlank) {
              el.style.pointerEvents = 'none';
              el.style.display = 'none';
              hidden++;
            }
          }
        }
      } catch (e) {}
    }"""
    for frame in frames:
        try:
            await frame.evaluate(js)
        except Exception:
            continue


async def _try_wake_player(page, pass_idx: int = 0) -> None:
    """Multi-pass click-to-play waking across common players, overlays & iframes."""
    if pass_idx >= 1:
        await _dismiss_ad_overlays(page)

    selectors = (
        ".jw-icon-display",
        ".jw-display-icon-container",
        ".vjs-big-play-button",
        ".plyr__control--overlaid",
        ".art-icon-state",
        ".dplayer-play-icon",
        ".fluid_initial_play",
        ".play-wrapper",
        ".mejs__overlay-play",
        ".fp-playbtn",
        "[aria-label*='play' i]",
        "button[class*='play' i]",
        "[class*='play-btn' i]",
        "[class*='play_btn' i]",
        "[id*='play' i]",
        "video",
    )
    try:
        frames = list(page.frames)
    except Exception:
        frames = [page]

    clicked = False
    for frame in frames:
        if not hasattr(frame, "query_selector"):
            continue
        for sel in selectors:
            try:
                el = await frame.query_selector(sel)
                if el is not None and await el.is_visible():
                    await el.click(timeout=350, no_wait_after=True, force=(pass_idx >= 1))
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            break

    # Fallback: click the center of the largest visible iframe, or viewport center.
    if not clicked and hasattr(page, "mouse"):
        try:
            box = None
            if hasattr(page, "query_selector_all"):
                best_area = 0.0
                for ifr in await page.query_selector_all("iframe"):
                    try:
                        b = await ifr.bounding_box()
                        if b and b["width"] >= 200 and b["height"] >= 150:
                            area = b["width"] * b["height"]
                            if area > best_area:
                                best_area = area
                                box = b
                    except Exception:
                        continue
            if box is not None:
                await page.mouse.click(
                    box["x"] + box["width"] / 2.0,
                    box["y"] + box["height"] / 2.0,
                )
            elif pass_idx >= 1:
                await page.mouse.click(640, 360)
        except Exception:
            pass


async def _extract_page_title(page) -> str:
    """Read a clean video title from ``og:title`` or ``document.title``."""
    try:
        raw = await page.evaluate(
            """() => {
              const m = document.querySelector('meta[property="og:title"], meta[name="twitter:title"]');
              if (m && m.content && m.content.trim()) return m.content.trim();
              return (document.title || '').trim();
            }"""
        )
        if not isinstance(raw, str):
            return ""
        cleaned = re.sub(r"\s+", " ", raw).strip(" -|:\t\r\n")
        if not cleaned or len(cleaned) < 2 or cleaned.lower() in _USELESS_TITLES:
            return ""
        if any(k in cleaned.lower() for k in _CHALLENGE_TITLE_MARKERS):
            return ""
        return cleaned[:160]
    except Exception:
        return ""


async def _is_challenge_page(page) -> bool:
    """True when the page currently shows a Cloudflare/DDoS-Guard JS interstitial."""
    try:
        title = await page.evaluate("() => (document.title || '').toLowerCase()")
        if isinstance(title, str) and any(m in title for m in _CHALLENGE_TITLE_MARKERS):
            return True
    except Exception:
        pass
    return False


_PRESERVED_HEADERS = {
    "user-agent": "User-Agent",
    "accept": "Accept",
    "accept-language": "Accept-Language",
    "referer": "Referer",
    "origin": "Origin",
    "authorization": "Authorization",
    "sec-fetch-dest": "Sec-Fetch-Dest",
    "sec-fetch-mode": "Sec-Fetch-Mode",
    "sec-fetch-site": "Sec-Fetch-Site",
    "sec-ch-ua": "Sec-Ch-Ua",
    "sec-ch-ua-mobile": "Sec-Ch-Ua-Mobile",
    "sec-ch-ua-platform": "Sec-Ch-Ua-Platform",
}


async def _extract_request_headers(
    req,
    effective_page_url: str,
    *,
    fallback_ua: str = DEFAULT_USER_AGENT,
) -> Dict[str, str]:
    raw: Dict[str, str] = {}
    try:
        raw = await req.all_headers()
    except Exception:
        try:
            raw = dict(req.headers or {})
        except Exception:
            raw = {}

    out: Dict[str, str] = {
        "User-Agent": fallback_ua or DEFAULT_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    has_raw = False
    for k, v in (raw or {}).items():
        if not k or v is None:
            continue
        val = str(v).strip()
        if not val:
            continue
        has_raw = True
        low = k.strip().lower()
        canonical = _PRESERVED_HEADERS.get(low)
        if canonical:
            out[canonical] = val
        elif low.startswith("x-") and low not in ("x-client-data", "x-requested-with"):
            out[k.strip()] = val

    if not has_raw and effective_page_url:
        out["Referer"] = effective_page_url
    return out


async def _extract_cookies_full(
    context,
    manifest_url: str,
    page_url: str,
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """Capture both flat cookies (compat) and full domain/path-scoped cookie entries."""
    cookies: Dict[str, str] = {}
    entries: List[Dict[str, Any]] = []
    seen_entries: set[Tuple[str, str, str]] = set()

    all_items: List[Dict[str, Any]] = []
    try:
        # Playwright context.cookies() with no args returns ALL context cookies.
        res = await context.cookies()
        if isinstance(res, list):
            all_items = res
    except TypeError:
        # Compatibility with unit-test fakes expecting cookies(urls).
        target_urls = [
            u for u in (manifest_url, page_url)
            if u and u.startswith(("http://", "https://"))
        ]
        if target_urls:
            try:
                res = await context.cookies(target_urls)
                if isinstance(res, list):
                    all_items = res
            except Exception:
                all_items = []
    except Exception:
        all_items = []

    for item in all_items:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        val = item.get("value")
        if not name or val is None:
            continue
        domain = str(item.get("domain") or "")
        path = str(item.get("path") or "/")
        key = (domain.lower(), path, str(name))
        if key not in seen_entries:
            seen_entries.add(key)
            entry: Dict[str, Any] = {
                "name": str(name),
                "value": str(val),
                "domain": domain,
                "path": path,
                "host_only": not domain.startswith("."),
                "secure": bool(item.get("secure")),
                "httpOnly": bool(item.get("httpOnly")),
            }
            exp = item.get("expires")
            if isinstance(exp, (int, float)) and exp > 0:
                entry["expires"] = exp
            entries.append(entry)

    # Flat cookies: collect for manifest_url, plus page_url if same/parent domain.
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
    if page_url and page_url.startswith(("http://", "https://")):
        try:
            mp = urlparse(manifest_url)
            pp = urlparse(page_url)
            mhost = (mp.hostname or "").lower()
            phost = (pp.hostname or "").lower()
            if (not cookies) or (
                mhost and phost and (
                    mhost == phost
                    or mhost.split(".")[-2:] == phost.split(".")[-2:]
                )
            ):
                p_items = await context.cookies([page_url])
                for item in p_items or []:
                    name = item.get("name")
                    val = item.get("value")
                    if name and val is not None and str(name) not in cookies:
                        cookies[str(name)] = str(val)
        except Exception:
            pass

    return cookies, entries


async def _extract_cookies(context, manifest_url: str, page_url: str) -> Dict[str, str]:
    """Backward-compatible wrapper returning flat ``{name: value}`` cookies."""
    flat, _ = await _extract_cookies_full(context, manifest_url, page_url)
    return flat


def _is_subtitle_or_thumb_playlist(text: str) -> bool:
    """True when an #EXTM3U media playlist only lists subtitles/thumbnails."""
    if "#EXT-X-STREAM-INF" in text:
        return False
    uris = [
        line.strip().lower().split("?", 1)[0]
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not uris:
        return False
    return all(u.endswith((".vtt", ".webvtt", ".srt", ".jpg", ".jpeg", ".png", ".webp")) for u in uris)


def _estimate_playlist_duration(text: str) -> float:
    total = 0.0
    for m in re.finditer(r"#EXTINF\s*:\s*([0-9.]+)", text):
        try:
            total += float(m.group(1))
        except ValueError:
            pass
    return total


def _parse_int_header(headers: Dict[str, str], name: str) -> Optional[int]:
    val = headers.get(name) or headers.get(name.lower()) or ""
    if not val:
        return None
    try:
        n = int(str(val).strip())
        return n if n >= 0 else None
    except ValueError:
        return None


def _parse_range_total(headers: Dict[str, str]) -> Optional[int]:
    raw = headers.get("content-range") or headers.get("Content-Range") or ""
    if "/" not in raw:
        return None
    tail = raw.rsplit("/", 1)[-1].strip()
    if not tail or tail == "*":
        return None
    try:
        n = int(tail)
        return n if n >= 0 else None
    except ValueError:
        return None


@dataclass
class _SniffedCandidate:
    url: str
    stream_type: str  # "hls" | "dash" | "direct"
    request: Any
    score: int        # Lower is better: 0=master HLS, 10=media HLS, 20=short/preview HLS, 25=DASH, 30=direct video
    manifest_text: str = ""
    seen_at: float = 0.0


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


def _mismatch_hint(cache: str) -> str:
    """Actionable hint when *cache* exists outside /home but holds an older build."""
    return (
        f"      {cache} exists and is readable, but holds an older Chromium revision\n"
        f"      than the installed Playwright package expects.\n"
        f"      Update Chromium to match Playwright, then restart the service:\n"
        f"        sudo bash deploy/install-browser.sh   (or: bot browser)\n"
        f"        sudo systemctl restart telegram-forwarder"
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
            use_sandbox_hint = _home_masked() and (
                cache.startswith(("/home/", "/root")) or not os.path.isdir(cache)
            )
            if use_sandbox_hint:
                hint = _sandbox_hint(cache)
            elif os.path.isdir(cache) and _has_browser(cache):
                hint = _mismatch_hint(cache)
            else:
                hint = _install_hint()
            print(f"[HLS] Browser engine: Chromium is not downloaded for this user "
                  f"(cache: {cache}).\n"
                  f"      playwright wants: {wanted or '(not named in the error)'}\n"
                  f"{_describe_cache(wanted)}\n"
                  f"{hint}")
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
    """Load *page_url* in Chromium and return the best stream (HLS / DASH / direct MP4).

    Never raises (except ``WarpProxyError``) when the browser is unavailable,
    the page has no stream, or anything else goes wrong.
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
        cdp = None
        browser = None
        is_ephemeral = False
        requests_seen = 0
        last_request_at = time.time()
        pending_sniffs: set[asyncio.Task] = set()
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
            master_hit: asyncio.Future = loop.create_future()
            candidates: List[_SniffedCandidate] = []
            seen_cand_urls: set[str] = set()
            fallback_req: Any = None
            fallback_url: str = ""
            fallback_seen_at: float = 0.0
            failed_urls: set[str] = set()

            def _record_candidate(cand: _SniffedCandidate) -> None:
                if cand.url in failed_urls:
                    return
                if cand.url in seen_cand_urls:
                    # Upgrade score/manifest_text if a better inspection arrived.
                    for existing in candidates:
                        if existing.url == cand.url and cand.score < existing.score:
                            existing.score = cand.score
                            existing.manifest_text = cand.manifest_text or existing.manifest_text
                    return
                seen_cand_urls.add(cand.url)
                candidates.append(cand)
                if cand.score == 0 and not master_hit.done():
                    master_hit.set_result(cand)

            try:
                major, ua, extra_hdrs = _build_browser_identity(browser)
                ctx_kwargs: Dict[str, Any] = {
                    "user_agent": ua,
                    "viewport": {"width": 1280, "height": 720},
                    "locale": "en-US",
                    "ignore_https_errors": True,
                }
                # Only pass extra_http_headers if CDP UA override is unavailable
                # (avoids CORS preflight failures on strict cross-origin CDNs).
                context = await browser.new_context(**ctx_kwargs)
                page = await context.new_page()
                page.set_default_timeout(timeout * 1000)

                # Immediately close any ad popup/popunder tabs spawned after main page exists.
                if hasattr(context, "on"):
                    def _on_popup(popup_page) -> None:
                        async def _close_popup() -> None:
                            try:
                                if popup_page is not page:
                                    await popup_page.close()
                            except Exception:
                                pass
                        t = asyncio.create_task(_close_popup())
                        pending_sniffs.add(t)
                        t.add_done_callback(pending_sniffs.discard)
                    try:
                        context.on("page", _on_popup)
                    except Exception:
                        pass

                # Apply CORS-safe native Client Hints via CDP when available.
                if hasattr(context, "new_cdp_session"):
                    try:
                        cdp = await context.new_cdp_session(page)
                        await cdp.send(
                            "Emulation.setUserAgentOverride",
                            _build_cdp_ua_override(major, ua),
                        )
                    except Exception:
                        cdp = None

                def _on_request(request) -> None:
                    nonlocal requests_seen, last_request_at, fallback_req, fallback_url, fallback_seen_at
                    requests_seen += 1
                    try:
                        req_url = request.url
                        if _is_blocked_url(req_url) or _is_ad_manifest(req_url):
                            return
                        last_request_at = time.time()
                        cand_abs = urljoin(page_url, req_url)
                        if _looks_like_manifest(req_url) and (
                            fallback_req is None
                            or fallback_req.url in failed_urls
                            or fallback_url in failed_urls
                        ):
                            fallback_req = request
                            fallback_url = cand_abs
                            fallback_seen_at = time.time()
                    except Exception:
                        pass

                async def _inspect_response(response) -> None:
                    try:
                        resp_url = response.url
                        if not resp_url or _is_ad_manifest(resp_url):
                            return
                        abs_url = urljoin(page_url, resp_url)
                        headers_map = dict(getattr(response, "headers", None) or {})
                        ctype = (headers_map.get("content-type") or headers_map.get("Content-Type") or "").split(";", 1)[0].strip().lower()
                        cl = _parse_int_header(headers_map, "content-length")
                        total_len = _parse_range_total(headers_map) or cl
                        req = getattr(response, "request", None)
                        rtype = ""
                        try:
                            rtype = str(getattr(req, "resource_type", "") or "").lower()
                        except Exception:
                            rtype = ""

                        # 1. Direct progressive video (never call response.body() on huge videos!).
                        is_direct_ctype = ctype in _DIRECT_VIDEO_CTYPES or (
                            ctype.startswith("video/") and ctype != "video/mp2t"
                        )
                        if is_direct_ctype or (
                            _looks_like_direct_video(resp_url)
                            and (ctype == "application/octet-stream" or response.status == 206 or not ctype)
                        ):
                            if total_len is not None and total_len < _MIN_DIRECT_VIDEO_BYTES and response.status != 206:
                                return
                            score = 35 if _is_preview_url(abs_url) else 30
                            _record_candidate(_SniffedCandidate(
                                url=abs_url,
                                stream_type="direct",
                                request=req,
                                score=score,
                                seen_at=time.time(),
                            ))
                            return

                        # 2. Decide whether to body-sniff this response for #EXTM3U / <MPD.
                        url_is_m3u8 = _looks_like_manifest(resp_url)
                        url_is_dash = _looks_like_dash(resp_url)
                        ctype_is_hls = "mpegurl" in ctype
                        ctype_is_dash = "dash+xml" in ctype
                        path_low = resp_url.lower().split("?", 1)[0]

                        should_sniff = (
                            url_is_m3u8
                            or url_is_dash
                            or ctype_is_hls
                            or ctype_is_dash
                        )
                        if not should_sniff:
                            # Disguised/extensionless HLS over XHR/fetch/other/media
                            if (
                                rtype in ("xhr", "fetch", "other", "media", "")
                                and not path_low.endswith(_STATIC_NON_STREAM_EXTS)
                                and (cl is None or cl <= _MAX_SNIFF_BODY_BYTES)
                                and (
                                    not ctype
                                    or ctype in ("text/plain", "application/octet-stream", "application/json")
                                    or ctype.startswith("image/")
                                )
                            ):
                                should_sniff = True

                        if not should_sniff:
                            return

                        raw_body: Optional[bytes] = None
                        body_fn = getattr(response, "body", None)
                        if callable(body_fn):
                            try:
                                maybe_coro = body_fn()
                                if asyncio.iscoroutine(maybe_coro):
                                    res_bytes = await asyncio.wait_for(maybe_coro, timeout=3.0)
                                    if isinstance(res_bytes, (bytes, bytearray)):
                                        raw_body = bytes(res_bytes)
                            except Exception:
                                raw_body = None

                        if raw_body is not None:
                            if len(raw_body) > _MAX_SNIFF_BODY_BYTES:
                                # Large binary disguised URL: check direct video magic.
                                if sniff_stream_bytes(raw_body[:64], ctype) == "direct":
                                    _record_candidate(_SniffedCandidate(
                                        url=abs_url,
                                        stream_type="direct",
                                        request=req,
                                        score=30,
                                        seen_at=time.time(),
                                    ))
                                return

                            kind = sniff_stream_bytes(raw_body[:65536], ctype)
                            if kind == "hls":
                                text = decode_text(raw_body)
                                if _is_subtitle_or_thumb_playlist(text):
                                    failed_urls.add(resp_url)
                                    failed_urls.add(abs_url)
                                    return
                                is_master = "#EXT-X-STREAM-INF" in text
                                if is_master and not _is_preview_url(abs_url):
                                    score = 0
                                elif _is_preview_url(abs_url):
                                    score = 22
                                else:
                                    dur = _estimate_playlist_duration(text)
                                    # Short < 18s VOD clip may be a preroll ad/preview; wait briefly.
                                    if "#EXT-X-ENDLIST" in text and 0 < dur < 18.0:
                                        score = 20
                                    else:
                                        score = 10
                                _record_candidate(_SniffedCandidate(
                                    url=abs_url,
                                    stream_type="hls",
                                    request=req,
                                    score=score,
                                    manifest_text=text,
                                    seen_at=time.time(),
                                ))
                                return

                            if kind == "dash":
                                text = decode_text(raw_body)
                                _record_candidate(_SniffedCandidate(
                                    url=abs_url,
                                    stream_type="dash",
                                    request=req,
                                    score=25,
                                    manifest_text=text,
                                    seen_at=time.time(),
                                ))
                                return

                            if kind == "direct" and len(raw_body) >= _MIN_DIRECT_VIDEO_BYTES:
                                _record_candidate(_SniffedCandidate(
                                    url=abs_url,
                                    stream_type="direct",
                                    request=req,
                                    score=30,
                                    seen_at=time.time(),
                                ))
                                return

                            # Genuine body was NOT a manifest (e.g. 200 OK HTML error page at .m3u8).
                            if url_is_m3u8 or ctype_is_hls:
                                failed_urls.add(resp_url)
                                failed_urls.add(abs_url)
                            return

                        # No body bytes available (e.g. unit-test fake response): trust URL/Content-Type.
                        if url_is_m3u8 or ctype_is_hls:
                            _record_candidate(_SniffedCandidate(
                                url=abs_url,
                                stream_type="hls",
                                request=req,
                                score=0 if ("master" in path_low or "manifest" in path_low) else 5,
                                seen_at=time.time(),
                            ))
                        elif url_is_dash or ctype_is_dash:
                            _record_candidate(_SniffedCandidate(
                                url=abs_url,
                                stream_type="dash",
                                request=req,
                                score=25,
                                seen_at=time.time(),
                            ))
                    except Exception:
                        pass

                def _on_response(response) -> None:
                    try:
                        resp_url = response.url
                        if _is_ad_manifest(resp_url):
                            return
                        status = int(getattr(response, "status", 0) or 0)
                        if 200 <= status < 300:
                            # Fast-path for synchronous unit-test mocks without async body()
                            body_fn = getattr(response, "body", None)
                            if not callable(body_fn):
                                ctype = (getattr(response, "headers", None) or {}).get("content-type", "").lower()
                                if _looks_like_manifest(resp_url) or ("mpegurl" in ctype):
                                    _record_candidate(_SniffedCandidate(
                                        url=urljoin(page_url, resp_url),
                                        stream_type="hls",
                                        request=getattr(response, "request", None),
                                        score=0,
                                        seen_at=time.time(),
                                    ))
                                return
                            t = asyncio.create_task(_inspect_response(response))
                            pending_sniffs.add(t)
                            t.add_done_callback(pending_sniffs.discard)
                        elif status >= 400:
                            ctype = ((getattr(response, "headers", None) or {}).get("content-type") or "").lower()
                            is_manifest = _looks_like_manifest(resp_url) or ("mpegurl" in ctype)
                            chain_urls: list[str] = []
                            req = getattr(response, "request", None)
                            seen_reqs: set[int] = set()
                            for _ in range(10):
                                if req is None or id(req) in seen_reqs:
                                    break
                                seen_reqs.add(id(req))
                                r_url = getattr(req, "url", None)
                                if isinstance(r_url, str) and r_url:
                                    chain_urls.append(r_url)
                                req = getattr(req, "redirected_from", None)
                            chain_has_manifest = any(
                                _looks_like_manifest(u) and not _is_ad_manifest(u)
                                for u in chain_urls
                            )
                            if not is_manifest and not chain_has_manifest:
                                return
                            for u in [resp_url, *chain_urls]:
                                if isinstance(u, str) and u:
                                    failed_urls.add(u)
                                    if page_url:
                                        failed_urls.add(urljoin(page_url, u))
                    except Exception:
                        pass

                page.on("request", _on_request)
                page.on("response", _on_response)
                if HLS_BROWSER_BLOCK_ADS:
                    await page.route("**/*", _block_heavy)
                await page.add_init_script(_build_tap_js(major))

                loaded = False
                loaded_at = time.time()
                try:
                    await page.goto(
                        page_url, wait_until="domcontentloaded", timeout=timeout * 1000,
                    )
                    loaded = True
                    loaded_at = time.time()
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
                    if not master_hit.done() and not candidates:
                        print(f"[HLS] Browser: page load issue ({type(e).__name__}: {e})")

                url = ""
                source = ""
                stream_type = "hls"
                manifest_text = ""
                winning_req = None
                headers: Dict[str, str] = {}
                effective_page_url = page.url or page_url
                if not effective_page_url.startswith(("http://", "https://")):
                    effective_page_url = page_url

                quiet = max(5, min(15, timeout // 4))
                deadline = loop.time() + timeout
                click_passes = 0
                last_click_at = 0.0
                last_tap_check = 0.0

                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        winner_cand = await asyncio.wait_for(
                            asyncio.shield(master_hit), timeout=min(0.4, remaining),
                        )
                        url = winner_cand.url
                        stream_type = winner_cand.stream_type
                        manifest_text = winner_cand.manifest_text
                        winning_req = winner_cand.request
                        source = "network"
                        break
                    except asyncio.TimeoutError:
                        pass

                    now = time.time()

                    # Check if a verified non-master candidate has settled past its grace window.
                    valid_cands = [c for c in candidates if c.url not in failed_urls]
                    if valid_cands:
                        valid_cands.sort(key=lambda c: (c.score, c.seen_at))
                        best = valid_cands[0]
                        grace = 1.5 if best.score <= 15 else 3.5
                        if (now - best.seen_at) >= grace:
                            url = best.url
                            stream_type = best.stream_type
                            manifest_text = best.manifest_text
                            winning_req = best.request
                            source = "network"
                            break

                    # Multi-pass click-to-play waking (up to 3 passes to penetrate ad overlays).
                    if (
                        loaded
                        and not valid_cands
                        and fallback_req is None
                        and click_passes < 3
                        and (now - loaded_at) >= (1.5 + click_passes * 2.2)
                        and (now - last_click_at) >= 2.0
                    ):
                        if not await _is_challenge_page(page):
                            last_click_at = now
                            await _try_wake_player(page, pass_idx=click_passes)
                            click_passes += 1

                    if (
                        not valid_cands
                        and fallback_req is not None
                        and fallback_req.url not in failed_urls
                        and fallback_url not in failed_urls
                        and (now - fallback_seen_at) >= 2.0
                        and not pending_sniffs
                    ):
                        url = fallback_url
                        winning_req = fallback_req
                        source = "network"
                        break

                    if (
                        loaded
                        and not valid_cands
                        and fallback_req is None
                        and (now - loaded_at) >= 4.5
                        and (now - last_tap_check) >= 1.5
                    ):
                        last_tap_check = now
                        tapped_url, frame_url, tapped_type = await _read_tap(page)
                        if tapped_url:
                            base_for_tap = (
                                frame_url
                                if frame_url and frame_url.startswith(("http://", "https://"))
                                else effective_page_url
                            )
                            tapped_abs = urljoin(base_for_tap, tapped_url)
                            if (
                                tapped_abs.lower().startswith(("http://", "https://"))
                                and not _is_ad_manifest(tapped_abs)
                                and tapped_abs not in failed_urls
                            ):
                                if tapped_type == "hls" or (now - loaded_at) >= 7.0:
                                    url = tapped_abs
                                    stream_type = tapped_type or "hls"
                                    source = "player-config"
                                    effective_page_url = base_for_tap
                                    headers = default_headers(url, referer=effective_page_url)
                                    headers["User-Agent"] = ua
                                    break

                    if loaded and requests_seen and (now - last_request_at) >= quiet:
                        if not await _is_challenge_page(page):
                            break

                # Wait briefly for any in-flight body sniffs before final selection.
                if not url and pending_sniffs:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*list(pending_sniffs), return_exceptions=True),
                            timeout=1.5,
                        )
                    except Exception:
                        pass

                # Pick best verified sniffed candidate if loop exited by timeout/quiet.
                valid_cands = [c for c in candidates if c.url not in failed_urls]
                valid_cands.sort(key=lambda c: (c.score, c.seen_at))
                if not url and valid_cands:
                    best = valid_cands[0]
                    url = best.url
                    stream_type = best.stream_type
                    manifest_text = best.manifest_text
                    winning_req = best.request
                    source = "network"

                if (
                    not url
                    and fallback_req is not None
                    and fallback_url
                    and fallback_req.url not in failed_urls
                    and fallback_url not in failed_urls
                ):
                    url = fallback_url
                    winning_req = fallback_req
                    source = "network"

                if url and winning_req is not None:
                    try:
                        frame_url = winning_req.frame.url if winning_req.frame else ""
                        if frame_url and frame_url.startswith(("http://", "https://")):
                            effective_page_url = frame_url
                    except Exception:
                        pass
                    headers = await _extract_request_headers(
                        winning_req, effective_page_url, fallback_ua=ua,
                    )

                if not url:
                    tapped_url, frame_url, tapped_type = await _read_tap(page)
                    if tapped_url:
                        base_for_tap = (
                            frame_url
                            if frame_url and frame_url.startswith(("http://", "https://"))
                            else effective_page_url
                        )
                        tapped_url = urljoin(base_for_tap, tapped_url)
                        if (
                            tapped_url.lower().startswith(("http://", "https://"))
                            and not _is_ad_manifest(tapped_url)
                            and tapped_url not in failed_urls
                        ):
                            url = tapped_url
                            stream_type = tapped_type or "hls"
                            source = "player-config"
                            effective_page_url = base_for_tap
                            headers = default_headers(url, referer=effective_page_url)
                            headers["User-Agent"] = ua

                title = await _extract_page_title(page)
                fallback_urls = [c.url for c in valid_cands if c.url != url][:5]

                elapsed = time.time() - started
                if url:
                    cookies, cookie_entries = await _extract_cookies_full(
                        context, url, effective_page_url
                    )
                    print(f"[HLS] Browser: {stream_type} via {source} after {elapsed:.1f}s "
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
                        stream_type=stream_type,
                        title=title,
                        manifest_text=manifest_text,
                        cookie_entries=cookie_entries,
                        fallback_urls=fallback_urls,
                    )
                print(f"[HLS] Browser: no manifest after {elapsed:.1f}s "
                      f"({requests_seen} requests)")
                return BrowserScan(
                    reason="no-manifest",
                    requests=requests_seen,
                    page_url=effective_page_url,
                    title=title,
                )
            except WarpProxyError:
                raise
            except Exception as e:
                return BrowserScan(
                    reason="error", detail=f"{type(e).__name__}: {e}",
                    requests=requests_seen,
                )
            finally:
                for task in list(pending_sniffs):
                    if not task.done():
                        task.cancel()
                if cdp is not None:
                    try:
                        await cdp.detach()
                    except Exception:
                        pass
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
    _check("ad host blocked & openfpcdn unblocked",
           _is_blocked_url("https://pagead2.googlesyndication.com/x.png")
           and not _is_blocked_url("https://openfpcdn.io/botd/v1"))
    _check("cdn host not blocked",
           not _is_blocked_url("https://ugc-cdn-caching-x.cloudwindow-route.com/a.m3u8"))
    _check("proxy config: none -> no proxy", _playwright_proxy() is None)

    if not browser_available():
        print("--- browser checks SKIPPED (playwright not installed) ---")
        print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
        sys.exit(1 if failures else 0)

    print("--- Live Chromium loopback checks ---")

    JS_PAGE = """<html><head><title>JS Test Show</title></head><body><video id="v"></video><script>
      fetch('/api/config').then(r => r.json()).then(cfg => fetch(cfg.file));
    </script></body></html>"""

    DISGUISED_PAGE = """<html><head><title>Disguised HLS</title></head><body><script>
      fetch('/cdn/stream-token-99?k=1');
    </script></body></html>"""

    DIRECT_MP4_PAGE = """<html><head><title>Direct MP4 Video</title></head><body>
      <video id="v" autoplay muted src="/cdn/movie.mp4"></video>
    </body></html>"""

    FAKE_M3U8_HTML_PAGE = """<html><body><script>
      fetch('/cdn/fake-error.m3u8').then(() => fetch('/cdn/real.m3u8?t=after'));
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

        async def disguised_page(request):
            return web.Response(text=DISGUISED_PAGE, content_type="text/html")

        async def disguised_stream(request):
            return web.Response(
                body=b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1200000\nindex.txt\n",
                content_type="text/plain",
            )

        async def mp4_page(request):
            return web.Response(text=DIRECT_MP4_PAGE, content_type="text/html")

        async def mp4_ep(request):
            body = b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2" + (b"\x00" * (300 * 1024))
            return web.Response(body=body, content_type="video/mp4")

        async def fake_m3u8_page(request):
            return web.Response(text=FAKE_M3U8_HTML_PAGE, content_type="text/html")

        async def fake_m3u8_html(request):
            return web.Response(text="<!DOCTYPE html><html><body>Access Denied</body></html>", content_type="text/html")

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
        app.router.add_get("/disguised", disguised_page)
        app.router.add_get("/cdn/stream-token-99", disguised_stream)
        app.router.add_get("/mp4-page", mp4_page)
        app.router.add_get("/cdn/movie.mp4", mp4_ep)
        app.router.add_get("/fake-m3u8-page", fake_m3u8_page)
        app.router.add_get("/cdn/fake-error.m3u8", fake_m3u8_html)
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
            _check("JS-built manifest captured + title + manifest_text",
                   scan.url == f"{base}/cdn/real.m3u8?t=js"
                   and scan.title == "JS Test Show"
                   and "#EXTM3U" in scan.manifest_text,
                   f"{scan.reason} {scan.url} title={scan.title!r}")
            expected_major = _chromium_major_version(_browser)
            sec_ch_ua = scan.headers.get("Sec-Ch-Ua", "")
            _check("CDP native UA + Sec-Ch-Ua (no HeadlessChrome) & Referer captured",
                   f"Chrome/{expected_major}.0.0.0" in scan.headers.get("User-Agent", "")
                   and "HeadlessChrome" not in sec_ch_ua
                   and f'"{expected_major}"' in sec_ch_ua
                   and scan.headers.get("Referer") == f"{base}/js-only",
                   str(scan.headers))
            _check("cookies + cookie_entries captured",
                   scan.cookies.get("br_tok") == "cookie_42"
                   and any(c.get("name") == "br_tok" for c in scan.cookie_entries),
                   f"{scan.cookies} / {scan.cookie_entries}")

            scan_disg = await resolve_stream_with_browser(f"{base}/disguised", timeout=15)
            _check("extensionless text/plain #EXTM3U sniffed from body",
                   scan_disg.reason == "ok"
                   and scan_disg.stream_type == "hls"
                   and scan_disg.url == f"{base}/cdn/stream-token-99?k=1",
                   str(scan_disg))

            scan_mp4 = await resolve_stream_with_browser(f"{base}/mp4-page", timeout=15)
            _check("direct MP4 <video> captured with stream_type='direct'",
                   scan_mp4.reason == "ok"
                   and scan_mp4.stream_type == "direct"
                   and scan_mp4.url == f"{base}/cdn/movie.mp4",
                   str(scan_mp4))

            scan_fake = await resolve_stream_with_browser(f"{base}/fake-m3u8-page", timeout=15)
            _check("200 OK HTML at fake .m3u8 rejected, real .m3u8 captured",
                   scan_fake.reason == "ok"
                   and scan_fake.url == f"{base}/cdn/real.m3u8?t=after",
                   str(scan_fake))

            scan2 = await resolve_stream_with_browser(f"{base}/tap-only", timeout=8)
            _check("URL built but never requested is tapped",
                   scan2.url == f"{base}/cdn/tapped.m3u8?t=tap" and scan2.source == "player-config",
                   f"{scan2.reason} {scan2.source} {scan2.url}")

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
