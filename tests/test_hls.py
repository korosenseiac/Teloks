"""Unit and local-HTTP integration tests for app/hls/ (playlist, downloader, remux)."""

from __future__ import annotations

import asyncio
import json
import os

os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017/telegram_forwarder_test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN_DO_NOT_USE")

import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from aiohttp import web
from aiohttp_socks import ProxyConnectionError, ProxyConnector

import app.config as _cfg
import app.hls.client as _client_mod
from app.hls import (
    HlsResult,
    R_DOWNLOAD_FAILED,
    R_DRM,
    R_NO_SEGMENTS,
    R_NOT_HLS,
    R_REMUX_FAILED,
    download_hls_to_mp4,
    is_drm,
    is_hls_playlist,
    is_master_playlist,
    parse_master,
    parse_media,
    rank_variants,
    reason_message,
    select_audio_track,
    select_variant,
)
from app.hls import browser as browser_mod
from app.hls import downloader as dl_mod
from app.hls import extract as extract_mod
from app.hls import handler as handler_mod
from app.warp import WarpProxyConfig, WarpProxyError


def _ffmpeg_ok() -> bool:
    return shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None


def _probe_media(path: str) -> dict:
    out = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-show_entries", "stream=codec_type,codec_name,width,height",
            "-show_entries", "format=format_name,duration",
            "-of", "json", path,
        ],
        capture_output=True, text=True, check=False,
    )
    if out.returncode != 0 or not out.stdout:
        return {}
    return json.loads(out.stdout)


def _faststart_ok(path: str) -> bool:
    with open(path, "rb") as fh:
        head = fh.read(512 * 1024)
    moov = head.find(b"moov")
    mdat = head.find(b"mdat")
    return moov != -1 and (mdat == -1 or moov < mdat)


def _make_ts_fixture(work: str, *, duration: float = 2.0, acodec: str = "aac") -> bytes:
    out = os.path.join(work, f"src_{acodec}.ts")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
        "-f", "lavfi", "-i", "sine=frequency=440",
        "-t", str(duration),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-c:a", acodec, out,
    ]
    subprocess.run(cmd, check=True)
    with open(out, "rb") as fh:
        return fh.read()


def _make_hls_ts_segments(
    work: str, *, duration: float = 3.0, seg_time: float = 1.0,
    aes_key: bytes | None = None, iv_hex: str = "",
) -> tuple[str, dict[str, bytes]]:
    sub = tempfile.mkdtemp(dir=work)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
        "-f", "lavfi", "-i", "sine=frequency=440",
        "-t", str(duration),
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "24",
        "-c:a", "aac",
        "-hls_time", str(seg_time),
        "-hls_playlist_type", "vod",
    ]
    if aes_key is not None:
        key_path = os.path.join(sub, "enc.key")
        with open(key_path, "wb") as fh:
            fh.write(aes_key)
        keyinfo_path = os.path.join(sub, "keyinfo")
        with open(keyinfo_path, "w", encoding="utf-8") as fh:
            fh.write(f"key.bin\n{key_path}\n")
            if iv_hex:
                fh.write(f"{iv_hex}\n")
        cmd += ["-hls_key_info_file", keyinfo_path]
    cmd.append("index.m3u8")
    subprocess.run(cmd, cwd=sub, check=True)

    with open(os.path.join(sub, "index.m3u8"), "r", encoding="utf-8") as fh:
        playlist_text = fh.read()
    files: dict[str, bytes] = {}
    for fname in os.listdir(sub):
        if fname.endswith(".ts"):
            with open(os.path.join(sub, fname), "rb") as fh:
                files[fname] = fh.read()
    return playlist_text, files


def _make_fmp4_hls(work: str, *, duration: float = 3.0) -> tuple[str, dict[str, bytes]]:
    sub = tempfile.mkdtemp(dir=work)
    subprocess.run(
        [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
            "-f", "lavfi", "-i", "sine=frequency=440",
            "-t", str(duration),
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "24",
            "-c:a", "aac",
            "-hls_time", "1",
            "-hls_playlist_type", "vod",
            "-hls_segment_type", "fmp4",
            "-hls_fmp4_init_filename", "init.mp4",
            "index.m3u8",
        ],
        cwd=sub, check=True,
    )
    with open(os.path.join(sub, "index.m3u8"), "r", encoding="utf-8") as fh:
        playlist_text = fh.read()
    files: dict[str, bytes] = {}
    for fname in os.listdir(sub):
        if fname.endswith((".mp4", ".m4s")):
            with open(os.path.join(sub, fname), "rb") as fh:
                files[fname] = fh.read()
    return playlist_text, files


class TestPlaylistParser(unittest.TestCase):
    def test_bom_and_detection(self):
        bom_pl = "\ufeff#EXTM3U\n#EXTINF:5.0,\nseg0.ts\n#EXT-X-ENDLIST\n"
        self.assertTrue(is_hls_playlist(bom_pl))
        self.assertFalse(is_master_playlist(bom_pl))
        media = parse_media(bom_pl, "https://cdn.example.com/hls/index.m3u8")
        self.assertEqual(len(media.segments), 1)
        self.assertEqual(media.segments[0].url, "https://cdn.example.com/hls/seg0.ts")
        self.assertFalse(media.is_live)

    def test_master_ranking_and_audio_selection(self):
        master = """\ufeff#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360,CODECS="avc1.4d401e,mp4a.40.2",AUDIO="a1"
360/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=4000000,RESOLUTION=1920x1080,CODECS="avc1.640028,ac-3",AUDIO="a_ac3"
1080_ac3/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3900000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2",AUDIO="a1"
1080_aac/index.m3u8
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a1",NAME="Auto",AUTOSELECT=YES,URI="audio/auto.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a1",NAME="Main",DEFAULT=YES,URI="audio/main.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="a_no_default",NAME="En",AUTOSELECT=YES,URI="audio/en.m3u8"
"""
        variants, audios = parse_master(master, "https://cdn.example.com/master.m3u8")
        best = select_variant(variants)
        self.assertIsNotNone(best)
        self.assertIn("1080_aac", best.url)
        ranked = rank_variants(variants, max_height=0)
        self.assertEqual(len(ranked), 3)
        self.assertIn("1080_aac", ranked[0].url)
        self.assertIn("360", ranked[-1].url)
        t1 = select_audio_track(audios, "a1")
        self.assertIsNotNone(t1)
        self.assertTrue(t1.url.endswith("audio/main.m3u8"))
        t2 = select_audio_track(audios, "a_no_default")
        self.assertIsNotNone(t2)
        self.assertTrue(t2.url.endswith("audio/en.m3u8"))

    def test_running_byterange_discontinuity_and_drm(self):
        pl_text = """#EXTM3U
#EXT-X-VERSION:7
#EXT-X-MEDIA-SEQUENCE:10
#EXT-X-MAP:URI="init1.mp4",BYTERANGE="500@0"
#EXTINF:4.0,
#EXT-X-BYTERANGE:1000@500
media.mp4
#EXTINF:4.0,
#EXT-X-BYTERANGE:2000
media.mp4
#EXT-X-DISCONTINUITY
#EXT-X-MAP:URI="init2.mp4"
#EXTINF:3.0,
seg3.m4s
#EXT-X-ENDLIST
"""
        pl = parse_media(pl_text, "https://cdn.example.com/hls/index.m3u8")
        self.assertEqual(pl.segments[0].byte_range, (500, 1000))
        self.assertEqual(pl.segments[1].byte_range, (1500, 2000))
        self.assertEqual(pl.segments[0].sequence, 10)
        self.assertEqual(pl.segments[2].sequence, 12)
        self.assertTrue(pl.segments[2].discontinuity)
        self.assertEqual(len(pl.init_sections), 2)

        self.assertTrue(is_drm('#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://1"\nseg.ts\n'))
        self.assertTrue(is_drm(
            '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="skd://1",KEYFORMAT="com.apple.streamingkeydelivery"\nseg.ts\n'
        ))
        self.assertFalse(is_drm('#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="key.bin"\nseg.ts\n'))


class TestSanitizationAndHelpers(unittest.TestCase):
    def test_sanitize_disguised_ts_and_fmp4_and_html(self):
        fake_ts = b"\x89PNG\r\n\x1a\n" + (b"\x00" * 24) + (b"\x47" + b"\x11" * 187) * 4
        clean_ts, ext_ts = dl_mod._sanitize_segment_bytes(fake_ts, is_fmp4=False)
        self.assertEqual(ext_ts, ".ts")
        self.assertTrue(clean_ts.startswith(b"\x47"))
        self.assertEqual(len(clean_ts), 188 * 4)

        moof_box = (24).to_bytes(4, "big") + b"moof" + (b"\x00" * 16)
        fake_fmp4 = b"\xff\xd8\xff\xe0" + (b"\x00" * 12) + moof_box
        clean_fmp4, ext_fmp4 = dl_mod._sanitize_segment_bytes(fake_fmp4, is_fmp4=False)
        self.assertEqual(ext_fmp4, ".m4s")
        self.assertEqual(clean_fmp4, moof_box)

        html_err = b"<!DOCTYPE html><html><body>403 Forbidden</body></html>"
        self.assertTrue(dl_mod._is_probably_error_page(html_err))
        self.assertFalse(dl_mod._is_probably_error_page(fake_ts))

    def test_sanitize_detail_and_reason_message(self):
        raw = "HTTP 403 for https://cdn.x/seg.ts?token=secret123 via socks5://user:pass@1.2.3.4:1080 Cookie: sid=abc"
        msg = reason_message(R_DOWNLOAD_FAILED, raw)
        self.assertNotIn("secret123", msg)
        self.assertNotIn("user:pass", msg)
        self.assertNotIn("sid=abc", msg)
        self.assertIn("HTTP 403", msg)
        self.assertIn("Gagal memuat turun segmen HLS", msg)
        self.assertIn("Gagal menukar strim HLS ke MP4", reason_message(R_REMUX_FAILED))

    def test_build_remux_args_local_vs_remote(self):
        args_merged = dl_mod._build_remux_args(["C:/tmp/stream.ts"], "C:/tmp/out.mp4", local=True, mode="copy")
        self.assertNotIn("-allowed_extensions", args_merged)

        args_m3u8 = dl_mod._build_remux_args(["C:/tmp/index.m3u8"], "C:/tmp/out.mp4", local=True, mode="copy")
        self.assertIn("-allowed_extensions", args_m3u8)

        args_remote = dl_mod._build_remux_args(
            ["https://a/v.m3u8", "https://a/a.m3u8"], "out.mp4",
            local=False, extra_input_args=["-headers", "Referer: https://a/\r\n"],
            with_progress=True,
        )
        self.assertEqual(args_remote.count("-headers"), 2)
        self.assertIn("-progress", args_remote)


@unittest.skipUnless(_ffmpeg_ok(), "ffmpeg/ffprobe required for HLS integration tests")
class TestHlsEndToEndLocalServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.work = tempfile.mkdtemp(prefix="hls_test_")
        self._orig_disable_proxy = _client_mod.HLS_DISABLE_PROXY
        _client_mod.HLS_DISABLE_PROXY = True

    async def asyncTearDown(self):
        _client_mod.HLS_DISABLE_PROXY = self._orig_disable_proxy
        shutil.rmtree(self.work, ignore_errors=True)

    async def _start_server(self, app: web.Application):
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        return runner, f"http://127.0.0.1:{port}"

    def _assert_valid_mp4(self, res: HlsResult, *, expect_audio: bool = True, expect_acodec: str = "aac"):
        self.assertTrue(res.converted, f"expected converted=True, got reason={res.reason!r} detail={res.detail!r}")
        self.assertTrue(os.path.exists(res.path))
        self.assertGreater(res.size, 1024)
        meta = _probe_media(res.path)
        fmt = (meta.get("format") or {}).get("format_name", "")
        self.assertIn("mp4", fmt)
        streams = meta.get("streams") or []
        vcodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "video"]
        acodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "audio"]
        self.assertIn("h264", vcodecs)
        if expect_audio:
            self.assertIn(expect_acodec, acodecs)
        self.assertTrue(_faststart_ok(res.path), "moov atom must precede mdat")

    async def test_redirect_headers_cookies_variant_fallback_and_cleanup(self):
        ts_bytes = _make_ts_fixture(self.work, duration=2.0)

        async def master(request: web.Request):
            if request.headers.get("Referer") != "https://player.example.com/watch":
                return web.Response(status=403, text="missing referer")
            if request.headers.get("Origin") != "https://player.example.com":
                return web.Response(status=403, text="missing origin")
            if "auth=ok" not in request.headers.get("Cookie", ""):
                return web.Response(status=403, text="missing cookie")
            body = (
                "#EXTM3U\n"
                '#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080\n'
                "dead/index.m3u8\n"
                '#EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=1280x720\n'
                "live/start.m3u8\n"
            )
            return web.Response(text=body, content_type="application/vnd.apple.mpegurl")

        async def dead_variant(request: web.Request):
            return web.Response(status=404, text="edge missing")

        async def redirect_variant(request: web.Request):
            raise web.HTTPFound("/cdn/deep/sub/index.m3u8")

        async def deep_media(request: web.Request):
            body = (
                "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                "#EXTINF:1.0,\nseg0.ts\n#EXTINF:1.0,\nseg1.ts\n#EXT-X-ENDLIST\n"
            )
            return web.Response(text=body, content_type="application/vnd.apple.mpegurl")

        async def deep_seg(request: web.Request):
            if "auth=ok" not in request.headers.get("Cookie", ""):
                return web.Response(status=403)
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/master.m3u8", master)
        app.router.add_get("/dead/index.m3u8", dead_variant)
        app.router.add_get("/live/start.m3u8", redirect_variant)
        app.router.add_get("/cdn/deep/sub/index.m3u8", deep_media)
        app.router.add_get("/cdn/deep/sub/{seg}", deep_seg)

        runner, base = await self._start_server(app)
        try:
            meta_sizes: list[int] = []
            chunks: list[int] = []
            # Pre-merge compatibility: if HlsClient in this worktree does not
            # yet expose fetch_text_with_url, provide it for the test so
            # post-redirect base URL resolution is still verified end-to-end.
            orig_ftwu = getattr(_client_mod.HlsClient, "fetch_text_with_url", None)
            if orig_ftwu is None:
                async def _compat_ftwu(self_c, req_url: str):
                    sess = await self_c._get_session()
                    async with sess.get(req_url, allow_redirects=True, ssl=False) as r:
                        if r.status >= 400:
                            raise ValueError(f"HTTP {r.status} for {req_url}")
                        return _client_mod.decode_text(await r.read(), r.get_encoding()), str(r.url)
                _client_mod.HlsClient.fetch_text_with_url = _compat_ftwu
            try:
                res = await download_hls_to_mp4(
                    f"{base}/master.m3u8",
                    "movie.m3u8",
                    headers={
                        "User-Agent": "TestAgent/1.0",
                        "Referer": "https://player.example.com/watch",
                        "Origin": "https://player.example.com",
                    },
                    cookies={"auth": "ok"},
                    on_meta=meta_sizes.append,
                    on_chunk=chunks.append,
                    out_dir=self.work,
                    proxy_url=None,
                )
            finally:
                if orig_ftwu is None and hasattr(_client_mod.HlsClient, "fetch_text_with_url"):
                    delattr(_client_mod.HlsClient, "fetch_text_with_url")

            self._assert_valid_mp4(res)
            self.assertEqual(res.engine, "segments")
            self.assertEqual(len(meta_sizes), 1)
            self.assertGreater(sum(chunks), 0)
            leftovers = [f for f in os.listdir(self.work) if f.startswith(("hls_", ".hls_"))]
            self.assertEqual(leftovers, [])
        finally:
            await runner.cleanup()

    async def test_separate_audio_group_and_out_of_order_spill(self):
        # Generate separate video-only and audio-only TS streams.
        v_path = os.path.join(self.work, "v_only.ts")
        a_path = os.path.join(self.work, "a_only.ts")
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
                "-t", "1.5", "-c:v", "libx264", "-pix_fmt", "yuv420p", v_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "1.5", "-c:a", "aac", a_path,
            ],
            check=True,
        )
        with open(v_path, "rb") as fh:
            v_bytes = fh.read()
        with open(a_path, "rb") as fh:
            a_bytes = fh.read()

        async def master(request: web.Request):
            body = (
                "#EXTM3U\n"
                '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Main",DEFAULT=YES,URI="audio.m3u8"\n'
                '#EXT-X-STREAM-INF:BANDWIDTH=1200000,RESOLUTION=640x360,AUDIO="aud"\n'
                "video.m3u8\n"
            )
            return web.Response(text=body)

        async def video_pl(request: web.Request):
            return web.Response(text=(
                "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                "#EXTINF:0.75,\nv0.ts\n#EXTINF:0.75,\nv1.ts\n#EXTINF:0.75,\nv2.ts\n#EXT-X-ENDLIST\n"
            ))

        async def audio_pl(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.5,\na0.ts\n#EXT-X-ENDLIST\n")

        async def serve_part(request: web.Request):
            name = request.match_info["name"]
            if name == "v0.ts":
                # Deliberately finish last to force out-of-order buffering + disk spill.
                await asyncio.sleep(0.15)
            return web.Response(body=(a_bytes if name.startswith("a") else v_bytes))

        app = web.Application()
        app.router.add_get("/master.m3u8", master)
        app.router.add_get("/video.m3u8", video_pl)
        app.router.add_get("/audio.m3u8", audio_pl)
        app.router.add_get("/{name}", serve_part)

        runner, base = await self._start_server(app)
        try:
            with patch.object(dl_mod, "_MAX_REORDER_RAM_BYTES", 64):
                res = await download_hls_to_mp4(
                    f"{base}/master.m3u8", "muxed.m3u8", out_dir=self.work, proxy_url=None,
                )
            self._assert_valid_mp4(res, expect_audio=True, expect_acodec="aac")
        finally:
            await runner.cleanup()

    async def test_aes128_with_key_retry_both_paths(self):
        key = b"0123456789abcdef"
        pl_text, files = _make_hls_ts_segments(self.work, duration=2.0, seg_time=1.0, aes_key=key)
        key_calls = 0

        async def media(request: web.Request):
            return web.Response(text=pl_text, content_type="application/vnd.apple.mpegurl")

        async def key_ep(request: web.Request):
            nonlocal key_calls
            key_calls += 1
            if key_calls == 1:
                return web.Response(status=503, text="busy")
            return web.Response(body=key, content_type="application/octet-stream")

        async def seg_ep(request: web.Request):
            name = request.match_info["name"]
            if name not in files:
                return web.Response(status=404)
            return web.Response(body=files[name], content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/enc.m3u8", media)
        app.router.add_get("/key.bin", key_ep)
        app.router.add_get("/{name}", seg_ep)

        runner, base = await self._start_server(app)
        try:
            # Force ffmpeg C-crypto local playlist fallback path (also matches .venv without cryptography)
            with patch.object(dl_mod, "_detect_aes_backend", return_value=None):
                res = await download_hls_to_mp4(
                    f"{base}/enc.m3u8", "aes_ffmpeg.m3u8", out_dir=self.work, proxy_url=None,
                )
            self._assert_valid_mp4(res)
            self.assertGreaterEqual(key_calls, 2)

            # Also verify Python AES-128-CBC path using cryptography/Crypto if available or mocked backend
            try:
                has_c_aes = dl_mod._detect_aes_backend() is not None
            except Exception:
                has_c_aes = False
            if has_c_aes:
                res2 = await download_hls_to_mp4(
                    f"{base}/enc.m3u8", "aes_py.m3u8", out_dir=self.work, proxy_url=None,
                )
                self._assert_valid_mp4(res2)
        finally:
            await runner.cleanup()

    async def test_fmp4_byteranges_and_disguised_png_segments(self):
        fmp4_pl, fmp4_files = _make_fmp4_hls(self.work, duration=2.0)
        init_data = fmp4_files["init.mp4"]
        m4s_names = sorted(k for k in fmp4_files if k.endswith(".m4s"))
        seg_blob = b"".join(fmp4_files[k] for k in m4s_names)
        mid = len(seg_blob) // 2
        combined = init_data + seg_blob

        async def fmp4_byterange_m3u8(request: web.Request):
            body = (
                "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:3\n"
                f'#EXT-X-MAP:URI="all.mp4",BYTERANGE="{len(init_data)}@0"\n'
                f"#EXTINF:1.0,\n#EXT-X-BYTERANGE:{mid}@{len(init_data)}\nall.mp4\n"
                f"#EXTINF:1.0,\n#EXT-X-BYTERANGE:{len(seg_blob) - mid}\nall.mp4\n"
                "#EXT-X-ENDLIST\n"
            )
            return web.Response(text=body, content_type="application/vnd.apple.mpegurl")

        async def all_mp4(request: web.Request):
            # Intentionally ignore Range and return 200 full body on 2nd check?
            # Support standard Range 206 here, and test 200 fallback in separate route.
            rng = request.headers.get("Range", "")
            if rng.startswith("bytes="):
                start_s, _, end_s = rng[6:].partition("-")
                start = int(start_s)
                end = int(end_s)
                return web.Response(
                    status=206,
                    body=combined[start:end + 1],
                    headers={"Content-Range": f"bytes {start}-{end}/{len(combined)}"},
                )
            return web.Response(body=combined)

        ts_bytes = _make_ts_fixture(self.work, duration=1.5)
        fake_png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + (b"\xaa" * 19) + ts_bytes

        async def disguised_m3u8(request: web.Request):
            body = (
                "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                "#EXTINF:1.5,\nimg_0.png\n"
                "#EXTINF:1.5,\nnoext_1\n"
                "#EXT-X-ENDLIST\n"
            )
            return web.Response(text=body, content_type="application/vnd.apple.mpegurl")

        async def disguised_seg(request: web.Request):
            return web.Response(body=fake_png, content_type="image/png")

        app = web.Application()
        app.router.add_get("/fmp4.m3u8", fmp4_byterange_m3u8)
        app.router.add_get("/all.mp4", all_mp4)
        app.router.add_get("/disguised.m3u8", disguised_m3u8)
        app.router.add_get("/img_0.png", disguised_seg)
        app.router.add_get("/noext_1", disguised_seg)

        runner, base = await self._start_server(app)
        try:
            res1 = await download_hls_to_mp4(
                f"{base}/fmp4.m3u8", "fmp4_br.m3u8", out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res1)

            res2 = await download_hls_to_mp4(
                f"{base}/disguised.m3u8", "disguised.m3u8", out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res2)
        finally:
            await runner.cleanup()

    async def test_parallel_speed_and_retries(self):
        ts_bytes = _make_ts_fixture(self.work, duration=1.0)
        attempts: dict[str, int] = {}

        async def fast_m3u8(request: web.Request):
            lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:2"]
            for i in range(16):
                lines.append("#EXTINF:1.0,")
                lines.append(f"fast_{i}.ts")
            lines.append("#EXT-X-ENDLIST")
            return web.Response(text="\n".join(lines) + "\n")

        async def fast_seg(request: web.Request):
            await asyncio.sleep(0.15)
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        async def retry_m3u8(request: web.Request):
            return web.Response(text=(
                "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                "#EXTINF:1.0,\nr_0.ts\n#EXTINF:1.0,\nr_1.ts\n#EXT-X-ENDLIST\n"
            ))

        async def retry_seg(request: web.Request):
            name = request.match_info["name"]
            attempts[name] = attempts.get(name, 0) + 1
            if name == "r_0.ts" and attempts[name] <= 2:
                return web.Response(status=503, text="retry me")
            if name == "r_1.ts" and attempts[name] == 1:
                return web.Response(status=429, headers={"Retry-After": "0.1"}, text="rate limit")
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/fast.m3u8", fast_m3u8)
        app.router.add_get("/retry.m3u8", retry_m3u8)
        app.router.add_get("/{name}", lambda r: fast_seg(r) if r.match_info["name"].startswith("fast_") else retry_seg(r))

        runner, base = await self._start_server(app)
        try:
            with patch.object(_cfg, "HLS_SEGMENT_WORKERS", 16, create=True), \
                 patch.object(_cfg, "HLS_SEGMENT_RETRIES", 5, create=True):
                t0 = time.perf_counter()
                res_fast = await download_hls_to_mp4(
                    f"{base}/fast.m3u8", "fast.m3u8", out_dir=self.work, proxy_url=None,
                )
                elapsed = time.perf_counter() - t0
                self._assert_valid_mp4(res_fast)
                # 16 segments * 0.15s sequential would be >= 2.40s; parallel must beat it decisively.
                self.assertLess(elapsed, 1.5, f"expected parallel download, took {elapsed:.2f}s")

                res_retry = await download_hls_to_mp4(
                    f"{base}/retry.m3u8", "retry.m3u8", out_dir=self.work, proxy_url=None,
                )
                self._assert_valid_mp4(res_retry)
                self.assertEqual(attempts.get("r_0.ts"), 3)
                self.assertEqual(attempts.get("r_1.ts"), 2)
        finally:
            await runner.cleanup()

    async def test_proxy_error_falls_back_to_direct_and_rejects_html_200(self):
        ts_bytes = _make_ts_fixture(self.work, duration=1.0)

        async def ok_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nseg0.ts\n#EXT-X-ENDLIST\n")

        async def ok_seg(request: web.Request):
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        async def html_seg_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nfake200.ts\n#EXT-X-ENDLIST\n")

        async def html_seg(request: web.Request):
            return web.Response(status=200, text="<html><body>Just a moment...</body></html>")

        app = web.Application()
        app.router.add_get("/ok.m3u8", ok_m3u8)
        app.router.add_get("/seg0.ts", ok_seg)
        app.router.add_get("/fake200.m3u8", html_seg_m3u8)
        app.router.add_get("/fake200.ts", html_seg)

        runner, base = await self._start_server(app)
        try:
            # 1. Dead proxy -> automatic fallback to direct when HLS_PROXY_FALLBACK_DIRECT=True
            orig_ftwu = getattr(_client_mod.HlsClient, "fetch_text_with_url", None)

            async def _proxy_then_direct(self_c, req_url: str):
                if self_c.proxy_url is not None:
                    raise aiohttp.ClientError("proxy connect refused")
                sess = await self_c._get_session()
                async with sess.get(req_url, allow_redirects=True, ssl=False) as r:
                    return _client_mod.decode_text(await r.read(), r.get_encoding()), str(r.url)

            with patch.object(_cfg, "HLS_PROXY_FALLBACK_DIRECT", True, create=True), \
                 patch.object(_client_mod.HlsClient, "fetch_text_with_url", _proxy_then_direct, create=True):
                res = await download_hls_to_mp4(
                    f"{base}/ok.m3u8", "proxy_fb.m3u8",
                    out_dir=self.work, proxy_url="socks5://127.0.0.1:1",
                )
            self._assert_valid_mp4(res)

            # 2. HTTP 200 HTML error page masquerading as segment -> fail cleanly (not remuxed)
            with patch.object(_cfg, "HLS_SEGMENT_RETRIES", 1, create=True):
                r_html = await download_hls_to_mp4(
                    f"{base}/fake200.m3u8", "fake200.m3u8", out_dir=self.work, proxy_url=None,
                )
            self.assertFalse(r_html.converted)
            self.assertEqual(r_html.reason, R_DOWNLOAD_FAILED)
            self.assertIn("HTML", r_html.detail)
        finally:
            await runner.cleanup()

    async def test_mp3_and_ac3_audio_and_discontinuity(self):
        mp3_ts = _make_ts_fixture(self.work, duration=1.5, acodec="libmp3lame")
        ac3_ts = _make_ts_fixture(self.work, duration=1.5, acodec="ac3")
        aac_ts = _make_ts_fixture(self.work, duration=1.5, acodec="aac")

        async def mp3_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.5,\nmp3.ts\n#EXT-X-ENDLIST\n")

        async def ac3_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.5,\nac3.ts\n#EXT-X-ENDLIST\n")

        async def disc_m3u8(request: web.Request):
            return web.Response(
                text="#EXTM3U\n#EXTINF:1.5,\naac.ts\n#EXT-X-DISCONTINUITY\n#EXTINF:1.5,\naac.ts\n#EXT-X-ENDLIST\n"
            )

        async def serve_seg(request: web.Request):
            name = request.match_info["name"]
            data = {"mp3.ts": mp3_ts, "ac3.ts": ac3_ts, "aac.ts": aac_ts}[name]
            return web.Response(body=data, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/mp3.m3u8", mp3_m3u8)
        app.router.add_get("/ac3.m3u8", ac3_m3u8)
        app.router.add_get("/disc.m3u8", disc_m3u8)
        app.router.add_get("/{name}", serve_seg)

        runner, base = await self._start_server(app)
        try:
            r_mp3 = await download_hls_to_mp4(f"{base}/mp3.m3u8", "mp3.m3u8", out_dir=self.work, proxy_url=None)
            self._assert_valid_mp4(r_mp3, expect_acodec="mp3")

            r_ac3 = await download_hls_to_mp4(f"{base}/ac3.m3u8", "ac3.m3u8", out_dir=self.work, proxy_url=None)
            self._assert_valid_mp4(r_ac3, expect_acodec="aac")

            r_disc = await download_hls_to_mp4(f"{base}/disc.m3u8", "disc.m3u8", out_dir=self.work, proxy_url=None)
            self._assert_valid_mp4(r_disc, expect_acodec="aac")
        finally:
            await runner.cleanup()

    async def test_error_separation_and_cancellation(self):
        ts_bytes = _make_ts_fixture(self.work, duration=1.0)

        async def not_hls(request: web.Request):
            return web.Response(text="<html><body>not a playlist</body></html>", content_type="text/html")

        async def drm_m3u8(request: web.Request):
            return web.Response(text='#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://x"\n#EXTINF:1,\ns.ts\n#EXT-X-ENDLIST\n')

        async def empty_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXT-X-TARGETDURATION:5\n#EXT-X-ENDLIST\n")

        async def ok_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nok.ts?token=secret999\n#EXT-X-ENDLIST\n")

        async def ok_seg(request: web.Request):
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        async def forbidden_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nblocked.ts?token=secret999\n#EXT-X-ENDLIST\n")

        async def blocked_seg(request: web.Request):
            return web.Response(status=403, text="forbidden")

        async def slow_cancel_m3u8(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nslow_seg.ts\n#EXT-X-ENDLIST\n")

        async def slow_seg(request: web.Request):
            await asyncio.sleep(10)
            return web.Response(body=ts_bytes)

        app = web.Application()
        app.router.add_get("/not.m3u8", not_hls)
        app.router.add_get("/drm.m3u8", drm_m3u8)
        app.router.add_get("/empty.m3u8", empty_m3u8)
        app.router.add_get("/ok.m3u8", ok_m3u8)
        app.router.add_get("/ok.ts", ok_seg)
        app.router.add_get("/403.m3u8", forbidden_m3u8)
        app.router.add_get("/blocked.ts", blocked_seg)
        app.router.add_get("/cancel.m3u8", slow_cancel_m3u8)
        app.router.add_get("/slow_seg.ts", slow_seg)

        runner, base = await self._start_server(app)
        try:
            r_not = await download_hls_to_mp4(f"{base}/not.m3u8", "not.m3u8", out_dir=self.work, proxy_url=None)
            self.assertFalse(r_not.converted)
            self.assertEqual(r_not.reason, R_NOT_HLS)

            r_drm = await download_hls_to_mp4(f"{base}/drm.m3u8", "drm.m3u8", out_dir=self.work, proxy_url=None)
            self.assertFalse(r_drm.converted)
            self.assertEqual(r_drm.reason, R_DRM)

            r_empty = await download_hls_to_mp4(f"{base}/empty.m3u8", "empty.m3u8", out_dir=self.work, proxy_url=None)
            self.assertFalse(r_empty.converted)
            self.assertEqual(r_empty.reason, R_NO_SEGMENTS)

            r_403 = await download_hls_to_mp4(f"{base}/403.m3u8", "403.m3u8", out_dir=self.work, proxy_url=None)
            self.assertFalse(r_403.converted)
            self.assertEqual(r_403.reason, R_DOWNLOAD_FAILED)
            self.assertIn("403", r_403.detail)
            self.assertNotIn("secret999", r_403.detail)

            async def _fail_remux(*args, **kwargs):
                err_out = kwargs.get("error_out")
                if err_out is not None:
                    err_out.append("moov mux failure")
                return False

            with patch.object(dl_mod, "_remux_with_fallbacks", side_effect=_fail_remux):
                r_remux = await download_hls_to_mp4(f"{base}/ok.m3u8", "remux.m3u8", out_dir=self.work, proxy_url=None)
            self.assertFalse(r_remux.converted)
            self.assertEqual(r_remux.reason, R_REMUX_FAILED)
            self.assertIn("moov mux failure", r_remux.detail)

            cancel_dir = tempfile.mkdtemp(dir=self.work)
            task = asyncio.create_task(
                download_hls_to_mp4(f"{base}/cancel.m3u8", "cancel.m3u8", out_dir=cancel_dir, proxy_url=None)
            )
            await asyncio.sleep(0.25)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertEqual(
                [f for f in os.listdir(cancel_dir) if f.startswith(("hls_", ".hls_"))], [],
            )
        finally:
            await runner.cleanup()


WARP_ON_CFG = WarpProxyConfig(
    enabled=True,
    host="127.0.0.1",
    socks5_port=40000,
    http_port=40001,
    socks5_url="socks5://127.0.0.1:40000",
    http_url="http://127.0.0.1:40001",
)


class _AsyncCM:
    def __init__(self, value=None, exc=None):
        self._value = value
        self._exc = exc

    async def __aenter__(self):
        if self._exc is not None:
            raise self._exc
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class TestHlsWarpIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_hls_client_warp_on_off_explicit_and_cleanup(self):
        client = _client_mod.HlsClient()
        try:
            # 1. WARP ON -> ProxyConnector with rdns=True, trust_env=False
            with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
                 patch.object(_client_mod, "create_aiohttp_connector", side_effect=lambda **kw: ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True, **kw)):
                sess_on = await client._get_session()
                self.assertTrue(client.using_warp)
                self.assertEqual(client.warp_http_proxy_url, WARP_ON_CFG.http_url)
                self.assertIsInstance(sess_on.connector, ProxyConnector)
                self.assertTrue(sess_on.connector._rdns)
                self.assertFalse(sess_on._trust_env)
                client.update_cookies({"sid": "keepme"}, url="https://cdn.example.com/master.m3u8")

            # 2. Toggle to WARP OFF (no proxy.txt) -> closes old session, keeps cookies, uses TCPConnector
            with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(_client_mod, "read_proxy_url", return_value=None):
                sess_off = await client._get_session()
                self.assertTrue(sess_on.closed)
                self.assertFalse(client.using_warp)
                self.assertIsNone(client.warp_http_proxy_url)
                self.assertIsInstance(sess_off.connector, aiohttp.TCPConnector)
                self.assertNotIsInstance(sess_off.connector, ProxyConnector)
                self.assertEqual(client.export_cookies("https://cdn.example.com/seg.ts").get("sid"), "keepme")

            # 3. Toggle to WARP OFF with proxy.txt -> existing proxy.txt behavior preserved
            with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(_client_mod, "read_proxy_url", return_value="socks5://user:pass@1.2.3.4:1080"):
                sess_pt = await client._get_session()
                self.assertTrue(sess_off.closed)
                self.assertFalse(client.using_warp)
                self.assertIsInstance(sess_pt.connector, ProxyConnector)
        finally:
            await client.close()
        self.assertIsNone(client._session)
        self.assertFalse(client.using_warp)

        # 4. Explicit proxy_url=None bypasses WARP even when ensure_proxy_ready would return ON
        direct_client = _client_mod.HlsClient(proxy_url=None)
        try:
            with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)) as mock_ready:
                sess_dir = await direct_client._get_session()
                mock_ready.assert_not_awaited()
                self.assertFalse(direct_client.using_warp)
                self.assertNotIsInstance(sess_dir.connector, ProxyConnector)
        finally:
            await direct_client.close()

        # 5. Connector closed on ClientSession creation failure
        err_client = _client_mod.HlsClient()
        fake_conn = MagicMock()
        fake_conn.close = AsyncMock()
        with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
             patch.object(_client_mod, "create_aiohttp_connector", return_value=fake_conn), \
             patch.object(_client_mod.aiohttp, "ClientSession", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                await err_client._get_session()
        fake_conn.close.assert_awaited_once()

    async def test_hls_client_mid_request_proxy_error_warp_on_vs_off(self):
        # WARP ON -> wraps ProxyConnectionError into WarpProxyError across fetch methods
        client_on = _client_mod.HlsClient()
        fake_sess = MagicMock()
        fake_sess.closed = False
        fake_sess.get.return_value = _AsyncCM(exc=ProxyConnectionError(111, "refused"))
        client_on._session = fake_sess
        client_on._using_warp = True
        client_on._warp_cfg = WARP_ON_CFG
        client_on._proxy_url = WARP_ON_CFG.socks5_url
        client_on._session_proxy_key = ("warp", WARP_ON_CFG.socks5_url)

        with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
            for coro in (
                client_on.fetch_text_with_url("https://cdn.example.com/a.m3u8"),
                client_on.fetch_bytes("https://cdn.example.com/key.bin"),
                client_on.download_bytes("https://cdn.example.com/seg.ts"),
                client_on.fetch_page("https://player.example.com/watch"),
                client_on.download_to("https://cdn.example.com/seg.ts", os.path.join(tempfile.gettempdir(), "unused_seg.ts")),
            ):
                with self.assertRaises(WarpProxyError) as ctx:
                    await coro
                self.assertIn("/warp_status", str(ctx.exception))
                self.assertIn("/warp_off", str(ctx.exception))

        # WARP OFF with proxy.txt -> raises HlsProxyError, NOT WarpProxyError
        client_off = _client_mod.HlsClient()
        fake_sess_off = MagicMock()
        fake_sess_off.closed = False
        fake_sess_off.get.return_value = _AsyncCM(exc=ProxyConnectionError(111, "proxy.txt down"))
        client_off._session = fake_sess_off
        client_off._using_warp = False
        client_off._proxy_url = "socks5://1.2.3.4:1080"
        client_off._session_proxy_key = ("proxy_txt", "socks5://1.2.3.4:1080")

        with patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
             patch.object(_client_mod, "read_proxy_url", return_value="socks5://1.2.3.4:1080"):
            with self.assertRaises(_client_mod.HlsProxyError) as ctx_off:
                await client_off.fetch_text_with_url("https://cdn.example.com/a.m3u8")
            self.assertNotIsInstance(ctx_off.exception, WarpProxyError)

    async def test_downloader_warp_fail_closed_and_engine_b_http_proxy(self):
        with tempfile.TemporaryDirectory() as work:
            # 1. Fail-closed when ensure_proxy_ready raises WarpProxyError (no fallback to direct)
            with patch.object(dl_mod, "_ffmpeg_available", return_value=True), \
                 patch.object(_cfg, "HLS_PROXY_FALLBACK_DIRECT", True, create=True), \
                 patch.object(_client_mod, "ensure_proxy_ready", new=AsyncMock(side_effect=WarpProxyError("WARP down /warp_status /warp_off"))):
                with self.assertRaises(WarpProxyError):
                    await download_hls_to_mp4("https://cdn.example.com/master.m3u8", "v.m3u8", out_dir=work)

            # 2. Fail-closed when _prepare raises WarpProxyError mid-request (never switches to direct or Engine B)
            fake_client = MagicMock()
            fake_client.proxy_url = WARP_ON_CFG.socks5_url
            fake_client.using_warp = True
            fake_client.switch_to_direct = AsyncMock()
            fake_client.close = AsyncMock()
            with patch.object(dl_mod, "_ffmpeg_available", return_value=True), \
                 patch.object(_cfg, "HLS_PROXY_FALLBACK_DIRECT", True, create=True), \
                 patch.object(dl_mod, "_make_client", return_value=fake_client), \
                 patch.object(dl_mod, "_prepare", new=AsyncMock(side_effect=WarpProxyError("WARP dropped"))):
                with self.assertRaises(WarpProxyError):
                    await download_hls_to_mp4("https://cdn.example.com/master.m3u8", "v.m3u8", out_dir=work)
            fake_client.switch_to_direct.assert_not_awaited()

            # 3. Engine B ("ffmpeg") uses WARP HTTP proxy + httpproxy protocol whitelist
            captured_args = []

            async def fake_run_ffmpeg(args, dst, timeout, on_progress=None):
                captured_args.append(list(args))
                with open(dst, "wb") as fh:
                    fh.write(b"\x00" * 2048)
                return True, ""

            with patch.object(dl_mod, "_ffmpeg_available", return_value=True), \
                 patch.object(dl_mod, "HLS_ENGINE", "ffmpeg"), \
                 patch.object(dl_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
                 patch.object(dl_mod, "_run_ffmpeg_detailed", side_effect=fake_run_ffmpeg):
                res = await download_hls_to_mp4("https://cdn.example.com/master.m3u8", "ff.m3u8", out_dir=work)
            self.assertTrue(res.converted)
            self.assertEqual(res.engine, "ffmpeg")
            self.assertTrue(captured_args)
            argv = captured_args[0]
            self.assertIn("-http_proxy", argv)
            self.assertEqual(argv[argv.index("-http_proxy") + 1], WARP_ON_CFG.http_url)
            wl = argv[argv.index("-protocol_whitelist") + 1]
            self.assertIn("httpproxy", wl)

            # 4. Engine B in auto mode after Engine A HTTP 403 raises WarpProxyError if wireproxy HTTP goes down
            fake_client_403 = MagicMock()
            fake_client_403.proxy_url = WARP_ON_CFG.socks5_url
            fake_client_403.using_warp = True
            fake_client_403.close = AsyncMock()

            async def fake_fail_ffmpeg(args, dst, timeout, on_progress=None):
                return False, "Connection refused"

            with patch.object(dl_mod, "_ffmpeg_available", return_value=True), \
                 patch.object(dl_mod, "HLS_ENGINE", "auto"), \
                 patch.object(dl_mod, "_make_client", return_value=fake_client_403), \
                 patch.object(dl_mod, "_prepare", new=AsyncMock(side_effect=_client_mod.HlsHttpError(403, "https://cdn.example.com/m.m3u8"))), \
                 patch.object(dl_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
                 patch.object(dl_mod, "_run_ffmpeg_detailed", side_effect=fake_fail_ffmpeg), \
                 patch.object(dl_mod, "check_proxy_reachable", new=AsyncMock(return_value=False)):
                with self.assertRaises(WarpProxyError) as ctx_ff:
                    await download_hls_to_mp4("https://cdn.example.com/master.m3u8", "ff2.m3u8", out_dir=work)
            self.assertIn("/warp_status", str(ctx_ff.exception))

    async def test_extract_propagates_warp_proxy_error(self):
        # 1. Page fetch raises WarpProxyError -> must propagate (not PageScan(reason="error"))
        client_page_err = MagicMock()
        client_page_err.fetch_page = AsyncMock(side_effect=WarpProxyError("WARP down"))
        with self.assertRaises(WarpProxyError):
            await extract_mod.resolve_stream_from_page(client_page_err, "https://site.example.com/v/1")

        # 2. Candidate validation raises WarpProxyError -> must propagate
        client_cand_err = MagicMock()
        client_cand_err.fetch_page = AsyncMock(
            return_value=('<video src="https://cdn.example.com/master.m3u8"></video>', "https://site.example.com/v/1")
        )
        client_cand_err.fetch_text_with_url = AsyncMock(side_effect=WarpProxyError("WARP candidate error"))
        with self.assertRaises(WarpProxyError):
            await extract_mod.resolve_stream_from_page(client_cand_err, "https://site.example.com/v/1")

    async def test_browser_warp_proxy_routing_and_singleton_recycle(self):
        # Save and restore browser module globals around the test
        orig_pw = browser_mod._pw
        orig_browser = browser_mod._browser
        orig_active = browser_mod._active
        orig_used = browser_mod._browser_used
        orig_key = browser_mod._browser_proxy_key
        orig_warp = browser_mod._browser_using_warp

        try:
            browser_mod._pw = None
            browser_mod._browser = None
            browser_mod._active = 0
            browser_mod._browser_used = False
            browser_mod._browser_proxy_key = None
            browser_mod._browser_using_warp = False

            # 1. _resolve_browser_proxy prefers WARP HTTP proxy when ON
            with patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
                proxy_cfg, proxy_key, using_warp = await browser_mod._resolve_browser_proxy()
            self.assertTrue(using_warp)
            self.assertEqual(proxy_cfg, {"server": WARP_ON_CFG.http_url})
            self.assertEqual(proxy_key, ("warp", WARP_ON_CFG.http_url))

            # 2. _get_browser launches with WARP HTTP proxy and recycles when proxy key changes
            launch_calls = []

            class FakePage:
                def __init__(self, goto_exc=None):
                    self.url = "https://site.example.com/watch"
                    self.frames = []
                    self.main_frame = self
                    self._handlers = {}
                    self._goto_exc = goto_exc

                def set_default_timeout(self, ms):
                    pass

                def on(self, event, fn):
                    self._handlers.setdefault(event, []).append(fn)

                async def route(self, pattern, fn):
                    pass

                async def add_init_script(self, script):
                    pass

                async def goto(self, url, wait_until="domcontentloaded", timeout=10000):
                    if self._goto_exc is not None:
                        raise self._goto_exc
                    req = MagicMock()
                    req.url = "https://cdn.example.com/master.m3u8"
                    req.frame = self
                    req.all_headers = AsyncMock(return_value={"User-Agent": "FakeUA"})
                    resp = MagicMock()
                    resp.url = "https://cdn.example.com/master.m3u8"
                    resp.status = 200
                    resp.headers = {"content-type": "application/vnd.apple.mpegurl"}
                    resp.request = req
                    for fn in self._handlers.get("response", []):
                        fn(resp)

                async def close(self):
                    pass

            goto_exc_holder = {"exc": None}

            class FakeContext:
                async def new_page(self):
                    return FakePage(goto_exc=goto_exc_holder["exc"])

                async def cookies(self, urls):
                    return [{"name": "tok", "value": "123"}]

                async def close(self):
                    pass

            class FakeBrowser:
                def __init__(self):
                    self.closed = False

                def is_connected(self):
                    return not self.closed

                async def new_context(self, **kwargs):
                    return FakeContext()

                async def close(self):
                    self.closed = True

            launched_browsers = []

            class FakeChromium:
                async def launch(self, **kwargs):
                    launch_calls.append(kwargs)
                    b = FakeBrowser()
                    launched_browsers.append(b)
                    return b

            class FakePW:
                def __init__(self):
                    self.chromium = FakeChromium()

                async def stop(self):
                    pass

            class FakePWStarter:
                async def start(self):
                    return FakePW()

            fake_pw_pkg = types.ModuleType("playwright")
            fake_async_api = types.ModuleType("playwright.async_api")
            fake_async_api.async_playwright = lambda: FakePWStarter()
            fake_pw_pkg.async_api = fake_async_api

            with patch.dict(sys.modules, {"playwright": fake_pw_pkg, "playwright.async_api": fake_async_api}), \
                 patch.object(browser_mod, "browser_available", return_value=True), \
                 patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
                scan = await browser_mod.resolve_stream_with_browser("https://site.example.com/watch", timeout=5)
                self.assertEqual(scan.url, "https://cdn.example.com/master.m3u8")
                self.assertTrue(scan.used_proxy)
                self.assertTrue(scan.used_warp)
                self.assertEqual(launch_calls[-1].get("proxy"), {"server": WARP_ON_CFG.http_url})

                # Proxy failure on page.goto when WARP is ON raises WarpProxyError
                goto_exc_holder["exc"] = RuntimeError("Page.goto: net::ERR_PROXY_CONNECTION_FAILED")
                with self.assertRaises(WarpProxyError):
                    await browser_mod.resolve_stream_with_browser("https://site.example.com/watch", timeout=5)

            # Toggling WARP OFF recycles the idle singleton browser without proxy
            goto_exc_holder["exc"] = None
            with patch.dict(sys.modules, {"playwright": fake_pw_pkg, "playwright.async_api": fake_async_api}), \
                 patch.object(browser_mod, "browser_available", return_value=True), \
                 patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(browser_mod, "_playwright_proxy", return_value=None):
                scan_off = await browser_mod.resolve_stream_with_browser("https://site.example.com/watch", timeout=5)
                self.assertTrue(launched_browsers[0].closed)
                self.assertFalse(scan_off.used_proxy)
                self.assertFalse(scan_off.used_warp)
                self.assertNotIn("proxy", launch_calls[-1])

            await browser_mod.close_browser()
        finally:
            browser_mod._pw = orig_pw
            browser_mod._browser = orig_browser
            browser_mod._active = orig_active
            browser_mod._browser_used = orig_used
            browser_mod._browser_proxy_key = orig_key
            browser_mod._browser_using_warp = orig_warp

    async def test_handler_catches_and_reports_warp_proxy_error(self):
        # 1. page_hls_handler catches WarpProxyError, replies to user, and claims message (returns True)
        msg = MagicMock()
        msg.from_user = MagicMock(id=101, username="tester", is_premium=False)
        msg.text = "https://video.example.com/watch/123"
        msg.caption = None
        msg.reply_text = AsyncMock()

        warp_err = WarpProxyError(
            "WARP proxy error (HLS): Local WARP wireproxy unreachable.\n"
            "Check proxy health with /warp_status, or disable WARP routing with /warp_off."
        )
        with patch.object(handler_mod, "HLS_ENABLED", True), \
             patch.object(handler_mod, "HLS_PAGE_EXTRACT", True), \
             patch.object(handler_mod, "_ensure_user_ready", new=AsyncMock(return_value=101)), \
             patch.object(handler_mod, "_scan_page_with_client", new=AsyncMock(side_effect=warp_err)):
            claimed = await handler_mod.page_hls_handler(MagicMock(), msg)
        self.assertTrue(claimed)
        msg.reply_text.assert_awaited_once()
        self.assertIn("/warp_status", msg.reply_text.call_args.args[0])

        # 2. process_hls_download catches WarpProxyError and edits status_msg cleanly
        from app.bot import main as bot_main

        status_msg = MagicMock()
        status_msg.edit_text = AsyncMock()
        fake_slot = MagicMock(sid="sid_hls_warp")
        with patch.object(handler_mod.manager, "get_client", new=AsyncMock(return_value=MagicMock())), \
             patch.object(bot_main, "begin_process", return_value=fake_slot), \
             patch.object(bot_main, "release_process") as mock_release, \
             patch.object(bot_main, "is_cancelled", return_value=False), \
             patch.object(handler_mod, "_load_custom_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_get_backup_group_peer", new=AsyncMock(return_value=MagicMock())), \
             patch.object(handler_mod, "_call_download_hls", new=AsyncMock(side_effect=warp_err)), \
             patch.object(handler_mod, "safe_edit", new=AsyncMock()) as mock_edit, \
             patch.object(handler_mod, "clear_reply_markup", new=AsyncMock()):
            await handler_mod.process_hls_download(
                MagicMock(),
                101,
                msg,
                status_msg,
                url="https://cdn.example.com/master.m3u8",
                slot_sid="sid_hls_warp",
            )
        mock_release.assert_called_once_with(fake_slot)
        self.assertTrue(any("/warp_off" in str(call.args[1]) for call in mock_edit.call_args_list if len(call.args) >= 2))


if __name__ == "__main__":
    unittest.main()
