"""
hls/ytdlp.py — Optional Tier 2 extractor & Engine C downloader powered by yt-dlp.

Provides universal extraction across 1,800+ video websites (YouTube, Vimeo,
Dailymotion, Twitch, TikTok, X/Twitter, Reddit, Ok.ru, VK, Streamtape,
Doodstream, Mixdrop, Filemoon, generic HTML5/HLS/DASH pages, etc.) without
launching a 400 MB headless Chromium process.

Everything degrades gracefully when ``yt-dlp`` is not installed or disabled
via ``HLS_YTDLP_ENABLED=false``.

Self-test:

    python -m app.hls.ytdlp
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import app.config as _app_config
from app.hls.client import (
    AUTO_PROXY,
    DEFAULT_USER_AGENT,
    read_proxy_url,
    redact_url,
)
from app.hls.extract import ExtractedStream
from app.warp import get_active_socks5_url

_YTDLP_SPEC_CACHE: Optional[bool] = None


def ytdlp_available() -> bool:
    """True when ``yt-dlp`` is enabled in config and importable."""
    global _YTDLP_SPEC_CACHE
    if not bool(getattr(_app_config, "HLS_YTDLP_ENABLED", True)):
        return False
    if _YTDLP_SPEC_CACHE is None:
        _YTDLP_SPEC_CACHE = importlib.util.find_spec("yt_dlp") is not None
    return _YTDLP_SPEC_CACHE


def _parse_cookie_str(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (raw or "").split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            out[k] = v
    return out


def write_netscape_cookiefile(
    path: str,
    *,
    cookies: Optional[Dict[str, str]] = None,
    cookie_entries: Optional[List[Dict[str, Any]]] = None,
    default_url: str = "",
) -> bool:
    """Write cookies to *path* in Netscape cookie-jar format for yt-dlp.

    Returns True when at least one cookie line was written.
    """
    lines: List[str] = ["# Netscape HTTP Cookie File", ""]
    seen: set[Tuple[str, str, str]] = set()
    default_host = ""
    if default_url:
        try:
            default_host = (urlparse(default_url).hostname or "").lower()
        except ValueError:
            default_host = ""

    def _emit(
        domain: str,
        cookie_path: str,
        secure: bool,
        expires: Any,
        name: str,
        value: str,
    ) -> None:
        if not name or not domain:
            return
        d = domain.strip()
        if not d:
            return
        include_sub = "TRUE" if d.startswith(".") else "FALSE"
        p = (cookie_path or "/").strip() or "/"
        sec = "TRUE" if secure else "FALSE"
        try:
            exp_int = int(float(expires)) if expires else 0
            if exp_int < 0:
                exp_int = 0
        except (TypeError, ValueError):
            exp_int = 0
        key = (d.lower(), p, name)
        if key in seen:
            return
        seen.add(key)
        clean_val = str(value or "").replace("\r", "").replace("\n", "").replace("\t", " ")
        clean_name = str(name).replace("\r", "").replace("\n", "").replace("\t", "")
        lines.append(f"{d}\t{include_sub}\t{p}\t{sec}\t{exp_int}\t{clean_name}\t{clean_val}")

    for item in cookie_entries or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        val = str(item.get("value") or "")
        dom = str(item.get("domain") or default_host)
        if dom and not item.get("host_only", False) and not dom.startswith("."):
            dom = "." + dom
        _emit(
            dom,
            str(item.get("path") or "/"),
            bool(item.get("secure")),
            item.get("expires"),
            name,
            val,
        )

    if cookies and default_host:
        for k, v in cookies.items():
            if k and v is not None:
                _emit(default_host, "/", False, 0, str(k), str(v))

    if len(lines) <= 2:
        return False
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return True


def _resolve_proxy(proxy: Any) -> Optional[str]:
    if proxy is AUTO_PROXY:
        warp = get_active_socks5_url()
        return warp if warp else read_proxy_url()
    if isinstance(proxy, str) and proxy.strip():
        return proxy.strip()
    return None


def _format_selector(max_height: int) -> str:
    h = int(max_height or 0)
    if h > 0:
        return (
            f"bestvideo[height<={h}][vcodec^=avc]+bestaudio[acodec^=mp4a]/"
            f"best[height<={h}][ext=mp4]/"
            f"bestvideo[height<={h}]+bestaudio/"
            f"best[height<={h}]/best"
        )
    return (
        "bestvideo[vcodec^=avc]+bestaudio[acodec^=mp4a]/"
        "best[ext=mp4]/"
        "bestvideo+bestaudio/"
        "best"
    )


def build_ytdlp_opts(
    *,
    proxy: Any = None,
    headers: Optional[Dict[str, str]] = None,
    cookiefile: Optional[str] = None,
    max_height: int = 0,
    outtmpl: Optional[str] = None,
    timeout: float = 25.0,
    for_download: bool = False,
) -> Dict[str, Any]:
    """Build a hardened ``yt_dlp.YoutubeDL`` options dictionary."""
    sock_to = max(5.0, min(30.0, float(timeout or 25.0)))
    req_headers: Dict[str, str] = {"User-Agent": DEFAULT_USER_AGENT}
    lower_keys: Dict[str, str] = {"user-agent": "User-Agent"}
    for k, v in (headers or {}).items():
        if not k or v is None:
            continue
        k_str = str(k).strip()
        if not k_str:
            continue
        k_low = k_str.lower()
        if cookiefile and k_low == "cookie":
            continue
        old_key = lower_keys.get(k_low)
        if old_key and old_key != k_str:
            req_headers.pop(old_key, None)
        req_headers[k_str] = str(v)
        lower_keys[k_low] = k_str

    opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "noplaylist": True,
        "playlistend": 1,
        "ignoreerrors": False,
        "nocheckcertificate": True,
        "geo_bypass": True,
        "socket_timeout": sock_to,
        "retries": 3 if for_download else 1,
        "fragment_retries": 4 if for_download else 1,
        "extractor_retries": 1,
        "http_headers": req_headers,
        "format": _format_selector(max_height),
    }
    resolved_proxy = _resolve_proxy(proxy)
    if resolved_proxy:
        opts["proxy"] = resolved_proxy
    if cookiefile:
        opts["cookiefile"] = cookiefile

    if for_download:
        workers = max(1, min(8, int(getattr(_app_config, "HLS_SEGMENT_WORKERS", 8) or 8)))
        opts["concurrent_fragment_downloads"] = workers
        opts["merge_output_format"] = "mp4"
        opts["postprocessor_args"] = {"merger": ["-movflags", "+faststart"]}
        if outtmpl:
            opts["outtmpl"] = outtmpl

    return opts


def _classify_protocol(fmt: Dict[str, Any]) -> str:
    proto = str(fmt.get("protocol") or "").lower()
    url = str(fmt.get("url") or "").lower()
    ext = str(fmt.get("ext") or "").lower()
    if "m3u8" in proto or ".m3u8" in url or ext == "m3u8":
        return "hls"
    if "dash" in proto or proto == "mpd" or ".mpd" in url or ext == "mpd":
        return "dash"
    if proto in ("http", "https", ""):
        return "direct"
    return ""


def _extract_headers_and_cookies(
    fmt: Dict[str, Any], info: Dict[str, Any]
) -> Tuple[Dict[str, str], Dict[str, str]]:
    hdrs: Dict[str, str] = {}
    cookies: Dict[str, str] = {}
    for src in (info.get("http_headers"), fmt.get("http_headers")):
        if isinstance(src, dict):
            for k, v in src.items():
                if not k or v is None:
                    continue
                if str(k).lower() == "cookie":
                    cookies.update(_parse_cookie_str(str(v)))
                else:
                    hdrs[str(k)] = str(v)
    for c_raw in (info.get("cookies"), fmt.get("cookies")):
        if isinstance(c_raw, str) and c_raw.strip():
            cookies.update(_parse_cookie_str(c_raw))
    return hdrs, cookies


def _collect_fallback_urls(
    info: Dict[str, Any], primary_url: str, max_height: int
) -> List[str]:
    formats = info.get("formats")
    if not isinstance(formats, list):
        return []
    scored: List[Tuple[int, int, str]] = []
    seen: set[str] = {primary_url}
    for fmt in formats:
        if not isinstance(fmt, dict):
            continue
        u = fmt.get("url")
        if not isinstance(u, str) or not u.startswith(("http://", "https://")) or u in seen:
            continue
        # Skip audio-only or video-only split fragments as standalone fallbacks.
        vcodec = str(fmt.get("vcodec") or "")
        acodec = str(fmt.get("acodec") or "")
        if vcodec == "none":
            continue
        stype = _classify_protocol(fmt)
        if not stype:
            continue
        if stype == "direct" and acodec == "none":
            continue
        height = int(fmt.get("height") or 0)
        if max_height > 0 and height > max_height:
            continue
        seen.add(u)
        tier = 0 if stype == "hls" else (1 if stype == "direct" else 2)
        scored.append((tier, -height, u))
    scored.sort(key=lambda x: (x[0], x[1]))
    return [u for _, _, u in scored[:5]]


def _info_to_extracted(
    info: Dict[str, Any],
    source_url: str,
    *,
    max_height: int = 0,
    used_proxy: bool = False,
) -> ExtractedStream:
    # Unwrap single-entry playlists/wrappers.
    if isinstance(info.get("entries"), list) and info["entries"]:
        first_entry = next((e for e in info["entries"] if isinstance(e, dict)), None)
        if first_entry is not None:
            info = first_entry

    title = str(info.get("title") or info.get("fulltitle") or "").strip()[:160]
    page_url = str(info.get("webpage_url") or source_url)

    req_fmts = info.get("requested_formats")
    if isinstance(req_fmts, list) and len(req_fmts) >= 2:
        vid_fmt = next(
            (f for f in req_fmts if isinstance(f, dict) and str(f.get("vcodec") or "") != "none"),
            req_fmts[0],
        )
        aud_fmt = next(
            (f for f in req_fmts if isinstance(f, dict) and f is not vid_fmt),
            req_fmts[1],
        )
        v_url = str(vid_fmt.get("url") or "")
        a_url = str(aud_fmt.get("url") or "")
        stype = _classify_protocol(vid_fmt)
        if v_url.startswith(("http://", "https://")) and stype:
            v_hdrs, v_cookies = _extract_headers_and_cookies(vid_fmt, info)
            a_hdrs, a_cookies = _extract_headers_and_cookies(aud_fmt, info)
            merged_cookies = {**v_cookies, **a_cookies}
            fallbacks = _collect_fallback_urls(info, v_url, max_height)
            return ExtractedStream(
                url=v_url,
                stream_type=stype,
                audio_url=a_url if a_url.startswith(("http://", "https://")) else None,
                audio_headers=a_hdrs or None,
                page_url=page_url,
                title=title,
                headers=v_hdrs,
                cookies=merged_cookies,
                fallback_urls=fallbacks,
                extractor="yt-dlp",
                used_proxy=used_proxy,
                reason="ok",
            )

    direct_url = str(info.get("url") or "")
    if direct_url.startswith(("http://", "https://")):
        stype = _classify_protocol(info) or "direct"
        hdrs, cookies = _extract_headers_and_cookies(info, info)
        fallbacks = _collect_fallback_urls(info, direct_url, max_height)
        return ExtractedStream(
            url=direct_url,
            stream_type=stype,
            page_url=page_url,
            title=title,
            headers=hdrs,
            cookies=cookies,
            fallback_urls=fallbacks,
            extractor="yt-dlp",
            used_proxy=used_proxy,
            reason="ok",
        )

    # Fallback: pick best combined format from info["formats"] if top-level url missing.
    formats = info.get("formats")
    if isinstance(formats, list):
        candidates: List[Tuple[int, int, Dict[str, Any]]] = []
        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            u = fmt.get("url")
            if not isinstance(u, str) or not u.startswith(("http://", "https://")):
                continue
            if str(fmt.get("vcodec") or "") == "none":
                continue
            stype = _classify_protocol(fmt)
            if not stype:
                continue
            height = int(fmt.get("height") or 0)
            if max_height > 0 and height > max_height:
                continue
            tier = 0 if stype == "hls" else (1 if stype == "direct" else 2)
            candidates.append((tier, -height, fmt))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            best = candidates[0][2]
            best_url = str(best["url"])
            hdrs, cookies = _extract_headers_and_cookies(best, info)
            fallbacks = [str(c[2]["url"]) for c in candidates[1:6] if str(c[2]["url"]) != best_url]
            return ExtractedStream(
                url=best_url,
                stream_type=_classify_protocol(best) or "direct",
                page_url=page_url,
                title=title,
                headers=hdrs,
                cookies=cookies,
                fallback_urls=fallbacks,
                extractor="yt-dlp",
                used_proxy=used_proxy,
                reason="ok",
            )

    return ExtractedStream(
        page_url=page_url,
        title=title,
        extractor="yt-dlp",
        used_proxy=used_proxy,
        reason="no-manifest",
        detail="yt-dlp returned no playable HTTP/HLS/DASH URL",
    )


async def extract_with_ytdlp(
    url: str,
    *,
    proxy: Any = None,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    cookie_entries: Optional[List[Dict[str, Any]]] = None,
    max_height: int = 0,
    timeout: Optional[float] = None,
) -> ExtractedStream:
    """Resolve *url* via ``yt-dlp`` without downloading the media.

    Degrades gracefully when ``yt-dlp`` is not installed or disabled.
    """
    if not ytdlp_available():
        return ExtractedStream(
            page_url=url,
            extractor="yt-dlp",
            reason="no-ytdlp",
            detail="yt-dlp not installed or disabled",
        )

    eff_timeout = max(
        5.0,
        float(
            timeout
            if timeout is not None
            else getattr(_app_config, "HLS_YTDLP_TIMEOUT", 25)
        ),
    )
    resolved_proxy = _resolve_proxy(proxy)
    cookie_path: Optional[str] = None
    try:
        if cookies or cookie_entries:
            fd, cookie_path = tempfile.mkstemp(prefix="hls_ytc_", suffix=".txt")
            os.close(fd)
            if not write_netscape_cookiefile(
                cookie_path,
                cookies=cookies,
                cookie_entries=cookie_entries,
                default_url=url,
            ):
                os.remove(cookie_path)
                cookie_path = None

        opts = build_ytdlp_opts(
            proxy=resolved_proxy,
            headers=headers,
            cookiefile=cookie_path,
            max_height=max_height,
            timeout=eff_timeout,
            for_download=False,
        )

        def _run_extract() -> Any:
            import yt_dlp

            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)

        loop = asyncio.get_running_loop()
        started = time.time()
        info = await asyncio.wait_for(
            loop.run_in_executor(None, _run_extract),
            timeout=eff_timeout,
        )
        elapsed = time.time() - started
        if not isinstance(info, dict):
            return ExtractedStream(
                page_url=url,
                extractor="yt-dlp",
                used_proxy=bool(resolved_proxy),
                reason="no-manifest",
                detail="yt-dlp returned empty info",
            )
        res = _info_to_extracted(
            info, url, max_height=max_height, used_proxy=bool(resolved_proxy)
        )
        if res.url:
            print(
                f"[HLS] yt-dlp: resolved {res.stream_type} stream in {elapsed:.1f}s "
                f"({redact_url(res.url)[:80]})"
            )
        return res
    except asyncio.TimeoutError:
        return ExtractedStream(
            page_url=url,
            extractor="yt-dlp",
            used_proxy=bool(resolved_proxy),
            reason="error",
            detail=f"yt-dlp timed out after {eff_timeout:.0f}s",
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        msg = str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__
        return ExtractedStream(
            page_url=url,
            extractor="yt-dlp",
            used_proxy=bool(resolved_proxy),
            reason="no-manifest" if "Unsupported URL" in msg else "error",
            detail=msg,
        )
    finally:
        if cookie_path and os.path.exists(cookie_path):
            try:
                os.remove(cookie_path)
            except OSError:
                pass


class _YtDlpCancelled(Exception):
    pass


async def download_with_ytdlp(
    url: str,
    out_dir: str,
    base_name: str,
    *,
    proxy: Any = None,
    headers: Optional[Dict[str, str]] = None,
    cookies: Optional[Dict[str, str]] = None,
    cookie_entries: Optional[List[Dict[str, Any]]] = None,
    max_height: int = 0,
    timeout: float = 1800.0,
    on_progress: Optional[Callable[[int, int], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> Tuple[bool, str, str]:
    """Download *url* via yt-dlp into *out_dir* (Engine C fallback for downloader).

    Returns ``(ok, downloaded_file_path, detail)``.
    """
    if not ytdlp_available():
        return False, "", "yt-dlp not installed or disabled"

    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(base_name)[0] or "stream"
    outtmpl = os.path.join(out_dir, f"{stem}.%(ext)s")
    resolved_proxy = _resolve_proxy(proxy)
    cookie_path: Optional[str] = None
    final_files: List[str] = []
    stop_flag = [False]

    try:
        if cookies or cookie_entries:
            fd, cookie_path = tempfile.mkstemp(prefix="hls_ytdl_", suffix=".txt")
            os.close(fd)
            if not write_netscape_cookiefile(
                cookie_path,
                cookies=cookies,
                cookie_entries=cookie_entries,
                default_url=url,
            ):
                os.remove(cookie_path)
                cookie_path = None

        opts = build_ytdlp_opts(
            proxy=resolved_proxy,
            headers=headers,
            cookiefile=cookie_path,
            max_height=max_height,
            outtmpl=outtmpl,
            timeout=min(30.0, timeout),
            for_download=True,
        )

        loop = asyncio.get_running_loop()

        def _safe_on_progress(dl: int, tot: int) -> None:
            if on_progress is None:
                return
            try:
                on_progress(dl, tot)
            except Exception:
                pass

        def _hook(d: Dict[str, Any]) -> None:
            if stop_flag[0] or (cancel_check is not None and cancel_check()):
                raise _YtDlpCancelled("cancelled")
            if d.get("status") == "downloading" and on_progress is not None:
                dl = int(d.get("downloaded_bytes") or 0)
                tot = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                try:
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(_safe_on_progress, dl, tot)
                except RuntimeError:
                    pass
            elif d.get("status") == "finished":
                fn = d.get("filename")
                if isinstance(fn, str) and fn:
                    final_files.append(fn)

        opts["progress_hooks"] = [_hook]

        def _run_download() -> Optional[str]:
            import yt_dlp

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                if isinstance(info, dict):
                    req = info.get("requested_downloads")
                    if isinstance(req, list) and req:
                        fp = req[0].get("filepath")
                        if isinstance(fp, str) and os.path.isfile(fp):
                            return fp
                    prepared = ydl.prepare_filename(info)
                    mp4_candidate = os.path.splitext(prepared)[0] + ".mp4"
                    if os.path.isfile(mp4_candidate):
                        return mp4_candidate
                    if os.path.isfile(prepared):
                        return prepared
            return None

        result_path = await asyncio.wait_for(
            loop.run_in_executor(None, _run_download),
            timeout=max(30.0, float(timeout)),
        )
        if not result_path:
            for candidate in reversed(final_files):
                if os.path.isfile(candidate):
                    result_path = candidate
                    break
        if not result_path:
            # Scan out_dir for the largest produced media file.
            best_f = ""
            best_sz = 0
            for entry in os.listdir(out_dir):
                full = os.path.join(out_dir, entry)
                if os.path.isfile(full) and not entry.endswith((".part", ".ytdl", ".tmp")):
                    sz = os.path.getsize(full)
                    if sz > best_sz:
                        best_sz = sz
                        best_f = full
            result_path = best_f

        if result_path and os.path.isfile(result_path) and os.path.getsize(result_path) > 0:
            return True, result_path, ""
        return False, "", "yt-dlp produced no output file"
    except _YtDlpCancelled:
        stop_flag[0] = True
        raise asyncio.CancelledError()
    except asyncio.CancelledError:
        stop_flag[0] = True
        raise
    except asyncio.TimeoutError:
        stop_flag[0] = True
        return False, "", f"yt-dlp download timed out after {int(timeout)}s"
    except Exception as exc:
        msg = str(exc).splitlines()[0][:200] if str(exc) else type(exc).__name__
        return False, "", msg
    finally:
        stop_flag[0] = True
        if cookie_path and os.path.exists(cookie_path):
            try:
                os.remove(cookie_path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Self-test / offline smoke check
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    print("--- yt-dlp module checks (offline) ---")
    _check("ytdlp_available() returns bool", isinstance(ytdlp_available(), bool))

    opts = build_ytdlp_opts(
        proxy="socks5://127.0.0.1:40000",
        headers={"Referer": "https://site.example/watch/1", "Cookie": "sid=abc", "user-agent": "CustomUA/1.0"},
        cookiefile="/tmp/c.txt",
        max_height=720,
        for_download=True,
    )
    _check("build_ytdlp_opts strips Cookie when cookiefile set & dedups UA case-insensitively",
           "Cookie" not in opts["http_headers"]
           and opts["proxy"] == "socks5://127.0.0.1:40000"
           and sum(1 for k in opts["http_headers"] if k.lower() == "user-agent") == 1
           and opts["http_headers"].get("user-agent") == "CustomUA/1.0",
           str(opts))
    _check("build_ytdlp_opts respects max_height=720", "height<=720" in opts["format"], opts["format"])

    sample_hls = {
        "title": "Sample HLS Episode",
        "webpage_url": "https://site.example/v/1",
        "url": "https://cdn.example/hls/master.m3u8?tok=1",
        "protocol": "m3u8_native",
        "http_headers": {"Referer": "https://site.example/", "Cookie": "cf_bm=tok"},
        "formats": [
            {"url": "https://cdn.example/hls/720.m3u8", "protocol": "m3u8_native", "height": 720, "vcodec": "avc1"},
            {"url": "https://cdn.example/video.mp4", "protocol": "https", "height": 720, "vcodec": "avc1", "acodec": "mp4a"},
        ],
    }
    ext_hls = _info_to_extracted(sample_hls, "https://site.example/v/1")
    _check("_info_to_extracted HLS + cookies + title + fallbacks",
           ext_hls.reason == "ok"
           and ext_hls.stream_type == "hls"
           and ext_hls.title == "Sample HLS Episode"
           and ext_hls.cookies.get("cf_bm") == "tok"
           and len(ext_hls.fallback_urls) == 2,
           str(ext_hls))

    sample_split = {
        "title": "Split Stream",
        "webpage_url": "https://yt.example/watch?v=1",
        "requested_formats": [
            {"url": "https://cdn.example/v.mp4", "protocol": "https", "vcodec": "avc1", "acodec": "none", "http_headers": {"User-Agent": "UA1"}},
            {"url": "https://cdn.example/a.m4a", "protocol": "https", "vcodec": "none", "acodec": "mp4a", "http_headers": {"User-Agent": "UA2"}},
        ],
    }
    ext_split = _info_to_extracted(sample_split, "https://yt.example/watch?v=1")
    _check("_info_to_extracted split video+audio",
           ext_split.reason == "ok"
           and ext_split.stream_type == "direct"
           and ext_split.url == "https://cdn.example/v.mp4"
           and ext_split.audio_url == "https://cdn.example/a.m4a"
           and (ext_split.audio_headers or {}).get("User-Agent") == "UA2",
           str(ext_split))

    fd, tmp_c = tempfile.mkstemp(suffix=".txt")
    os.close(fd)
    try:
        wrote = write_netscape_cookiefile(
            tmp_c,
            cookie_entries=[{"name": "sid", "value": "xyz", "domain": "example.com", "path": "/", "secure": True}],
            default_url="https://cdn.example.com/v",
        )
        content = open(tmp_c, encoding="utf-8").read()
        _check("Netscape cookiefile written cleanly",
               wrote and ".example.com\tTRUE\t/\tTRUE\t0\tsid\txyz" in content,
               content)
    finally:
        if os.path.exists(tmp_c):
            os.remove(tmp_c)

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)
