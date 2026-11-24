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


class TestHls403FixesAndStealth(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.work = tempfile.mkdtemp(prefix="hls_403_test_")
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

    async def test_header_absence_preserved_extract_to_downloader(self):
        # 1. Unit checks for _merge_request_context and _normalize_headers
        direct_hdrs, _ = handler_mod._merge_request_context(
            "https://cdn.example.com/hls/index.m3u8", None, None, None,
        )
        self.assertNotIn("Origin", direct_hdrs)
        self.assertEqual(direct_hdrs.get("Referer"), "https://cdn.example.com/")

        page_hdrs, _ = handler_mod._merge_request_context(
            "https://cdn.example.com/hls/index.m3u8", "https://site.example/watch/1", None, None,
        )
        self.assertEqual(page_hdrs.get("Referer"), "https://site.example/watch/1")
        self.assertEqual(page_hdrs.get("Origin"), "https://site.example")

        cap_hdrs, _ = handler_mod._merge_request_context(
            "https://cdn.example.com/hls/index.m3u8",
            "https://site.example/watch/1",
            {
                "user-agent": "CustomUA/1.0",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "cross-site",
                "sec-ch-ua": '"Chromium";v="153"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
            None,
        )
        self.assertNotIn("Origin", cap_hdrs)
        self.assertNotIn("Referer", cap_hdrs)
        self.assertEqual(cap_hdrs.get("Sec-Fetch-Dest"), "empty")
        self.assertEqual(cap_hdrs.get("Sec-Ch-Ua"), '"Chromium";v="153"')
        norm_cap = dl_mod._normalize_headers("https://cdn.example.com/hls/index.m3u8", cap_hdrs)
        self.assertNotIn("Origin", norm_cap)
        self.assertNotIn("Referer", norm_cap)

        if not _ffmpeg_ok():
            return

        # 2. End-to-end Attempt 3 (Origin omitted, Referer kept) and Attempt 5 (both omitted)
        ts_bytes = _make_ts_fixture(self.work, duration=1.5)
        seen_download_hdrs: list[dict[str, str]] = []

        async def page_no_origin(request: web.Request):
            return web.Response(
                text='<html><body><source src="/cdn3/master.m3u8"></body></html>',
                content_type="text/html",
            )

        async def master_no_origin(request: web.Request):
            if "Origin" in request.headers:
                return web.Response(status=403, text="origin forbidden")
            if not request.headers.get("Referer", "").endswith("/page3"):
                return web.Response(status=403, text="referer required")
            seen_download_hdrs.append(dict(request.headers))
            return web.Response(
                text="#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000\nindex.m3u8\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def media_no_origin(request: web.Request):
            if "Origin" in request.headers or not request.headers.get("Referer", "").endswith("/page3"):
                return web.Response(status=403)
            seen_download_hdrs.append(dict(request.headers))
            return web.Response(
                text="#EXTM3U\n#EXTINF:1.5,\nseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def seg_no_origin(request: web.Request):
            if "Origin" in request.headers or not request.headers.get("Referer", "").endswith("/page3"):
                return web.Response(status=403)
            seen_download_hdrs.append(dict(request.headers))
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        async def page_clean(request: web.Request):
            return web.Response(
                text='<html><body><source src="/cdn5/index.m3u8"></body></html>',
                content_type="text/html",
            )

        async def media_clean(request: web.Request):
            if "Origin" in request.headers or "Referer" in request.headers:
                return web.Response(status=403, text="ref/origin forbidden")
            return web.Response(
                text="#EXTM3U\n#EXTINF:1.5,\nseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def seg_clean(request: web.Request):
            if "Origin" in request.headers or "Referer" in request.headers:
                return web.Response(status=403)
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/page3", page_no_origin)
        app.router.add_get("/cdn3/master.m3u8", master_no_origin)
        app.router.add_get("/cdn3/index.m3u8", media_no_origin)
        app.router.add_get("/cdn3/seg0.ts", seg_no_origin)
        app.router.add_get("/page5", page_clean)
        app.router.add_get("/cdn5/index.m3u8", media_clean)
        app.router.add_get("/cdn5/seg0.ts", seg_clean)

        runner, base = await self._start_server(app)
        try:
            client = _client_mod.HlsClient(proxy_url=None)
            try:
                scan3 = await extract_mod.resolve_stream_from_page(client, f"{base}/page3")
                self.assertEqual(scan3.reason, "ok")
                self.assertNotIn("Origin", scan3.headers)
                self.assertEqual(scan3.headers.get("Referer"), f"{base}/page3")

                merged3, cookies3 = handler_mod._merge_request_context(
                    scan3.url, scan3.page_url, scan3.headers, scan3.cookies,
                )
                self.assertNotIn("Origin", merged3)
                self.assertEqual(merged3.get("Referer"), f"{base}/page3")

                seen_download_hdrs.clear()
                res3 = await download_hls_to_mp4(
                    scan3.url, "attempt3.m3u8", headers=merged3, cookies=cookies3,
                    out_dir=self.work, proxy_url=None,
                )
                self.assertTrue(res3.converted, res3.detail)
                self.assertTrue(seen_download_hdrs)
                self.assertTrue(all("Origin" not in h for h in seen_download_hdrs))

                scan5 = await extract_mod.resolve_stream_from_page(client, f"{base}/page5")
                self.assertEqual(scan5.reason, "ok")
                self.assertNotIn("Origin", scan5.headers)
                self.assertNotIn("Referer", scan5.headers)

                merged5, cookies5 = handler_mod._merge_request_context(
                    scan5.url, scan5.page_url, scan5.headers, scan5.cookies,
                )
                self.assertNotIn("Origin", merged5)
                self.assertNotIn("Referer", merged5)

                res5 = await download_hls_to_mp4(
                    scan5.url, "attempt5.m3u8", headers=merged5, cookies=cookies5,
                    out_dir=self.work, proxy_url=None,
                )
                self.assertTrue(res5.converted, res5.detail)
            finally:
                await client.close()
        finally:
            await runner.cleanup()

    async def test_downloader_403_query_inheritance_and_header_fallback(self):
        # 1. Helper unit checks
        self.assertEqual(
            dl_mod._inherit_query("https://cdn/hls/index.m3u8#frag", "https://cdn/hls/master.m3u8?t=sec123"),
            "https://cdn/hls/index.m3u8?t=sec123#frag",
        )
        self.assertIsNone(dl_mod._inherit_query("https://cdn/hls/index.m3u8?x=1", "https://cdn/hls/master.m3u8?t=sec"))
        self.assertIsNone(dl_mod._inherit_query("https://cdn/hls/index.m3u8", "https://cdn/hls/master.m3u8"))

        profiles = dl_mod._header_fallback_profiles(
            {"Referer": "https://site.example/e/1", "Origin": "https://site.example"},
            "https://cdn.example/hls/index.m3u8",
        )
        self.assertEqual(profiles[-1], {"Referer": None, "Origin": None})
        self.assertIn({"Origin": None}, profiles)
        self.assertEqual(len(profiles), len({(p.get("Referer", "__KEEP__"), p.get("Origin", "__KEEP__")) for p in profiles}))

        if not _ffmpeg_ok():
            return

        # 2. Local server: master ?t=secret123 -> relative index-v1-a1.m3u8 requiring query + no Origin -> segments
        ts_bytes = _make_ts_fixture(self.work, duration=1.5)
        seg_calls: list[tuple[str, str]] = []

        async def master(request: web.Request):
            if request.query.get("t") != "secret123":
                return web.Response(status=403)
            return web.Response(
                text="#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=900000\nindex-v1-a1.m3u8\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def variant(request: web.Request):
            if request.query.get("t") != "secret123" or "Origin" in request.headers:
                return web.Response(status=403, text="need token and no origin")
            return web.Response(
                text=(
                    "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n"
                    "#EXTINF:0.5,\nseg0.ts\n#EXTINF:0.5,\nseg1.ts\n"
                    "#EXTINF:0.5,\nseg2.ts\n#EXTINF:0.5,\nseg3.ts\n#EXT-X-ENDLIST\n"
                ),
                content_type="application/vnd.apple.mpegurl",
            )

        async def seg(request: web.Request):
            name = request.match_info["name"]
            tok = request.query.get("t", "")
            seg_calls.append((name, tok))
            if tok != "secret123" or "Origin" in request.headers:
                return web.Response(status=403)
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        async def direct_strict_m3u8(request: web.Request):
            if "Origin" in request.headers or "Referer" in request.headers:
                return web.Response(status=403, text="clean headers only")
            return web.Response(
                text="#EXTM3U\n#EXTINF:1.5,\ndirect_seg.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def direct_strict_seg(request: web.Request):
            if "Origin" in request.headers or "Referer" in request.headers:
                return web.Response(status=403)
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/master.m3u8", master)
        app.router.add_get("/index-v1-a1.m3u8", variant)
        app.router.add_get("/{name:seg\\d+\\.ts}", seg)
        app.router.add_get("/direct_strict.m3u8", direct_strict_m3u8)
        app.router.add_get("/direct_seg.ts", direct_strict_seg)

        runner, base = await self._start_server(app)
        try:
            with patch.object(_cfg, "HLS_SEGMENT_RETRIES", 1, create=True), \
                 patch.object(_cfg, "HLS_SEGMENT_WORKERS", 2, create=True):
                res_var = await download_hls_to_mp4(
                    f"{base}/master.m3u8?t=secret123",
                    "inherited.m3u8",
                    headers={
                        "User-Agent": "TestUA/1.0",
                        "Referer": "https://embed.example/e/1",
                        "Origin": "https://embed.example",
                    },
                    out_dir=self.work,
                    proxy_url=None,
                )
            self.assertTrue(res_var.converted, res_var.detail)
            # Only 1-2 initial concurrent workers could hit 403 before shared auth_state promotes inherit_query.
            failed_seg_probes = [c for c in seg_calls if c[1] != "secret123"]
            self.assertLessEqual(len(failed_seg_probes), 2, seg_calls)

            res_direct = await download_hls_to_mp4(
                f"{base}/direct_strict.m3u8",
                "direct_strict.m3u8",
                headers=None,
                out_dir=self.work,
                proxy_url=None,
            )
            self.assertTrue(res_direct.converted, res_direct.detail)
        finally:
            await runner.cleanup()

    async def test_browser_failed_urls_header_capture_and_stealth(self):
        # 1. openfpcdn is unblocked
        self.assertFalse(browser_mod._is_blocked_url("https://openfpcdn.io/botd/v1"))
        self.assertTrue(browser_mod._is_blocked_url("https://pagead2.googlesyndication.com/ad.js"))

        # 2. _extract_request_headers preserves Sec-Fetch-*, Sec-Ch-Ua*, Accept without fabricating omitted Origin/Referer
        req_with_raw = MagicMock()
        req_with_raw.all_headers = AsyncMock(return_value={
            "user-agent": "Mozilla/5.0 Chrome/153.0.0.0 Safari/537.36",
            "accept": "application/vnd.apple.mpegurl,*/*",
            "accept-language": "en-US,en;q=0.8",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "sec-ch-ua": '"Chromium";v="153"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "x-custom-token": "tok_ok",
            "x-client-data": "drop_me",
            "x-requested-with": "XMLHttpRequest",
        })
        hdrs = await browser_mod._extract_request_headers(req_with_raw, "https://site.example/watch")
        self.assertEqual(hdrs.get("Accept"), "application/vnd.apple.mpegurl,*/*")
        self.assertEqual(hdrs.get("Accept-Language"), "en-US,en;q=0.8")
        self.assertEqual(hdrs.get("Sec-Fetch-Dest"), "empty")
        self.assertEqual(hdrs.get("Sec-Fetch-Mode"), "cors")
        self.assertEqual(hdrs.get("Sec-Fetch-Site"), "cross-site")
        self.assertEqual(hdrs.get("Sec-Ch-Ua"), '"Chromium";v="153"')
        self.assertEqual(hdrs.get("Sec-Ch-Ua-Mobile"), "?0")
        self.assertEqual(hdrs.get("Sec-Ch-Ua-Platform"), '"Windows"')
        self.assertEqual(hdrs.get("x-custom-token"), "tok_ok")
        self.assertNotIn("x-client-data", hdrs)
        self.assertNotIn("x-requested-with", hdrs)
        self.assertNotIn("Referer", hdrs)
        self.assertNotIn("Origin", hdrs)

        req_empty = MagicMock()
        req_empty.all_headers = AsyncMock(return_value={})
        req_empty.headers = {}
        hdrs_empty = await browser_mod._extract_request_headers(req_empty, "https://site.example/watch")
        self.assertEqual(hdrs_empty.get("Referer"), "https://site.example/watch")
        self.assertNotIn("Origin", hdrs_empty)

        # 3. 403 failed manifest URL must NOT be returned as BrowserScan(reason="ok")
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

            class Fake403Page:
                def __init__(self):
                    self.url = "https://site.example/watch"
                    self.frames = []
                    self.main_frame = self
                    self._handlers = {}

                def set_default_timeout(self, ms):
                    pass

                def on(self, event, fn):
                    self._handlers.setdefault(event, []).append(fn)

                async def route(self, pattern, fn):
                    pass

                async def add_init_script(self, script):
                    pass

                async def evaluate(self, expr):
                    return ""

                async def goto(self, url, wait_until="domcontentloaded", timeout=5000):
                    req = MagicMock()
                    req.url = "https://cdn.example/blocked.m3u8"
                    req.frame = self
                    for fn in self._handlers.get("request", []):
                        fn(req)
                    resp = MagicMock()
                    resp.url = "https://cdn.example/blocked.m3u8"
                    resp.status = 403
                    resp.headers = {"content-type": "text/html"}
                    resp.request = req
                    for fn in self._handlers.get("response", []):
                        fn(resp)

                async def close(self):
                    pass

            class Fake403Context:
                async def new_page(self):
                    return Fake403Page()

                async def cookies(self, urls):
                    return []

                async def close(self):
                    pass

            class Fake403Browser:
                version = "153.0.8010.12"

                def is_connected(self):
                    return True

                async def new_context(self, **kwargs):
                    self._ctx_kwargs = kwargs
                    return Fake403Context()

                async def close(self):
                    pass

            class Fake403Chromium:
                async def launch(self, **kwargs):
                    return Fake403Browser()

            class Fake403PW:
                chromium = Fake403Chromium()

                async def stop(self):
                    pass

            class Fake403Starter:
                async def start(self):
                    return Fake403PW()

            fake_pw_pkg = types.ModuleType("playwright")
            fake_async_api = types.ModuleType("playwright.async_api")
            fake_async_api.async_playwright = lambda: Fake403Starter()
            fake_pw_pkg.async_api = fake_async_api

            with patch.dict(sys.modules, {"playwright": fake_pw_pkg, "playwright.async_api": fake_async_api}), \
                 patch.object(browser_mod, "browser_available", return_value=True), \
                 patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(browser_mod, "_playwright_proxy", return_value=None):
                scan_403 = await browser_mod.resolve_stream_with_browser(
                    "https://site.example/watch", timeout=5,
                )
                await browser_mod.close_browser()
            self.assertIsNone(scan_403.url)
            self.assertEqual(scan_403.reason, "no-manifest")
        finally:
            browser_mod._pw = orig_pw
            browser_mod._browser = orig_browser
            browser_mod._active = orig_active
            browser_mod._browser_used = orig_used
            browser_mod._browser_proxy_key = orig_key
            browser_mod._browser_using_warp = orig_warp

        # 4. Live Chromium loopback header check (no HeadlessChrome & matching Chrome major version)
        if browser_mod.browser_available():
            captured_wire_headers: list[dict[str, str]] = []
            captured_ua_data: list[str] = []
            app = web.Application()

            async def page_ep(request: web.Request):
                captured_wire_headers.append(dict(request.headers))
                return web.Response(
                    text=(
                        '<html><body><script>'
                        '(async () => {'
                        '  const d = navigator.userAgentData;'
                        '  const payload = d ? JSON.stringify({brands: d.brands, platform: d.platform, mobile: d.mobile}) : "{}";'
                        '  await fetch("/ua-check?d=" + encodeURIComponent(payload));'
                        '  await fetch("/stream/master.m3u8");'
                        '})();'
                        '</script></body></html>'
                    ),
                    content_type="text/html",
                )

            async def ua_check_ep(request: web.Request):
                captured_ua_data.append(request.query.get("d", ""))
                return web.Response(text="ok")

            async def m3u8_ep(request: web.Request):
                captured_wire_headers.append(dict(request.headers))
                return web.Response(
                    text="#EXTM3U\n#EXTINF:1.0,\nseg0.ts\n#EXT-X-ENDLIST\n",
                    content_type="application/vnd.apple.mpegurl",
                )

            app.router.add_get("/watch", page_ep)
            app.router.add_get("/ua-check", ua_check_ep)
            app.router.add_get("/stream/master.m3u8", m3u8_ep)
            runner, base = await self._start_server(app)
            try:
                with patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                     patch.object(browser_mod, "_playwright_proxy", return_value=None):
                    scan_live = await browser_mod.resolve_stream_with_browser(
                        f"{base}/watch", timeout=15,
                    )
                    expected_major = browser_mod._chromium_major_version(browser_mod._browser)
                    await browser_mod.close_browser()
                self.assertEqual(scan_live.reason, "ok")
                self.assertGreaterEqual(len(captured_wire_headers), 2)
                for wire in captured_wire_headers:
                    sec_ua = wire.get("sec-ch-ua", "") or wire.get("Sec-CH-UA", "")
                    ua = wire.get("User-Agent", "")
                    self.assertNotIn("HeadlessChrome", sec_ua)
                    self.assertIn(f"Chrome/{expected_major}.0.0.0", ua)
                    self.assertIn(f'"{expected_major}"', sec_ua)
                self.assertTrue(captured_ua_data)
                ua_js = json.loads(captured_ua_data[0])
                self.assertEqual(ua_js.get("platform"), "Windows")
                self.assertFalse(ua_js.get("mobile"))
                brands_str = json.dumps(ua_js.get("brands", []))
                self.assertNotIn("HeadlessChrome", brands_str)
                self.assertIn(expected_major, brands_str)
            finally:
                await runner.cleanup()

    async def test_handler_reextract_on_403_hint_claim_and_error_ux(self):
        from app.bot import main as bot_main

        # 1. _format_hls_error backward compatibility + actionable 403 tips + token redaction
        err_direct = handler_mod._format_hls_error(
            R_DOWNLOAD_FAILED, "playlist: HTTP 403 for https://cdn.x/a.m3u8?t=topsecret",
        )
        self.assertIn("Gagal memuat turun segmen HLS", err_direct)
        self.assertIn("HTTP 403", err_direct)
        self.assertNotIn("topsecret", err_direct)
        self.assertIn("halaman video", err_direct)
        self.assertNotIn("/warp_off", err_direct)

        err_page_warp = handler_mod._format_hls_error(
            R_DOWNLOAD_FAILED,
            "HTTP 403 for https://cdn.x/seg.ts?t=secret2",
            from_page=True,
            warp_active=True,
        )
        self.assertNotIn("secret2", err_page_warp)
        self.assertIn("/warp_off", err_page_warp)

        err_remux = handler_mod._format_hls_error(R_REMUX_FAILED, "moov error")
        self.assertNotIn("/warp_off", err_remux)
        self.assertNotIn("halaman video", err_remux)

        # 2. page_hls_handler claims message (returns True) when HLS_PAGE_HINT is sent
        msg = MagicMock()
        msg.from_user = MagicMock(id=202, username="tester", is_premium=False)
        msg.text = "https://player.example.com/v/abc"
        msg.caption = None
        msg.reply_text = AsyncMock()
        msg.photo = None
        msg.document = None

        with patch.object(handler_mod, "HLS_ENABLED", True), \
             patch.object(handler_mod, "HLS_PAGE_EXTRACT", True), \
             patch.object(handler_mod, "HLS_PAGE_HINT", True), \
             patch.object(handler_mod, "_ensure_user_ready", new=AsyncMock(return_value=202)), \
             patch.object(handler_mod, "is_proxy_enabled_async", new=AsyncMock(return_value=True)), \
             patch.object(
                 handler_mod, "_resolve_page_manifest",
                 new=AsyncMock(return_value=handler_mod._ResolvedPage(
                     url="", page_url="https://player.example.com/v/abc", player_page=True,
                 )),
             ):
            claimed = await handler_mod.page_hls_handler(MagicMock(), msg)
        self.assertTrue(claimed)
        msg.reply_text.assert_awaited_once()
        hint_sent = msg.reply_text.call_args.args[0]
        self.assertIn("403", hint_sent)
        self.assertIn("/warp_off", hint_sent)

        # 3. process_hls_download re-extracts once on 403 when page_url is set and succeeds on retry
        status_msg = MagicMock()
        status_msg.edit_text = AsyncMock()
        status_msg.delete = AsyncMock()
        status_msg.chat = MagicMock()
        status_msg.chat.send_action = AsyncMock()
        fake_slot = MagicMock(sid="sid_reextract")
        out_mp4 = os.path.join(self.work, "fresh.mp4")
        with open(out_mp4, "wb") as fh:
            fh.write(b"\x00" * 4096)

        dl_calls: list[dict] = []

        async def fake_call_dl(url, file_name, **kw):
            dl_calls.append({"url": url, "headers": kw["headers"], "cookies": kw["cookies"]})
            if len(dl_calls) == 1:
                return HlsResult(
                    converted=False,
                    reason=R_DOWNLOAD_FAILED,
                    detail="playlist: HTTP 403 for https://cdn.x/old.m3u8?t=expired",
                )
            return HlsResult(
                path=out_mp4,
                name="watch.mp4",
                size=4096,
                converted=True,
                engine="segments",
                duration=2.0,
            )

        re_resolved = handler_mod._ResolvedPage(
            url="https://cdn.x/fresh.m3u8?t=fresh99",
            page_url="https://player.example.com/v/abc",
            force_direct=False,
            headers={"User-Agent": "FreshUA/1.0", "Referer": "https://player.example.com/v/abc"},
            cookies={"session": "fresh_cookie"},
        )

        with patch.object(handler_mod.manager, "get_client", new=AsyncMock(return_value=MagicMock())), \
             patch.object(bot_main, "begin_process", return_value=fake_slot), \
             patch.object(bot_main, "release_process") as mock_release, \
             patch.object(bot_main, "is_cancelled", return_value=False), \
             patch.object(handler_mod, "_load_custom_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_get_backup_group_peer", new=AsyncMock(return_value=MagicMock())), \
             patch.object(handler_mod, "_call_download_hls", side_effect=fake_call_dl), \
             patch.object(handler_mod, "_resolve_page_manifest", new=AsyncMock(return_value=re_resolved)) as mock_resolve, \
             patch.object(handler_mod, "_get_video_metadata", new=AsyncMock(return_value={"duration": 2, "width": 160, "height": 120})), \
             patch.object(handler_mod, "_generate_video_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_upload_file_to_backup", new=AsyncMock(return_value=(99, False))), \
             patch.object(handler_mod, "_send_to_user", new=AsyncMock(return_value=True)), \
             patch.object(handler_mod, "log_forward", new=AsyncMock()), \
             patch.object(handler_mod, "safe_edit", new=AsyncMock()), \
             patch.object(handler_mod, "clear_reply_markup", new=AsyncMock()):
            await handler_mod.process_hls_download(
                MagicMock(),
                202,
                msg,
                status_msg,
                url="https://cdn.x/old.m3u8?t=expired",
                page_url="https://player.example.com/v/abc",
                slot_sid="sid_reextract",
            )
        mock_resolve.assert_awaited_once()
        self.assertEqual(len(dl_calls), 2)
        self.assertEqual(dl_calls[1]["url"], "https://cdn.x/fresh.m3u8?t=fresh99")
        self.assertEqual(dl_calls[1]["cookies"], {"session": "fresh_cookie"})
        mock_release.assert_called_once_with(fake_slot)

        # 4. process_hls_download stops after ONE re-extract if the retry also 403s
        always_403 = HlsResult(
            converted=False,
            reason=R_DOWNLOAD_FAILED,
            detail="playlist: HTTP 403 for https://cdn.x/still_blocked.m3u8?t=secret",
        )
        with patch.object(handler_mod.manager, "get_client", new=AsyncMock(return_value=MagicMock())), \
             patch.object(bot_main, "begin_process", return_value=fake_slot), \
             patch.object(bot_main, "release_process"), \
             patch.object(bot_main, "is_cancelled", return_value=False), \
             patch.object(handler_mod, "_load_custom_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_get_backup_group_peer", new=AsyncMock(return_value=MagicMock())), \
             patch.object(handler_mod, "_call_download_hls", new=AsyncMock(return_value=always_403)) as mock_dl_403, \
             patch.object(handler_mod, "_resolve_page_manifest", new=AsyncMock(return_value=re_resolved)) as mock_res_once, \
             patch.object(handler_mod, "is_proxy_enabled_async", new=AsyncMock(return_value=True)), \
             patch.object(handler_mod, "safe_edit", new=AsyncMock()) as mock_edit_403, \
             patch.object(handler_mod, "clear_reply_markup", new=AsyncMock()):
            await handler_mod.process_hls_download(
                MagicMock(),
                202,
                msg,
                status_msg,
                url="https://cdn.x/old.m3u8?t=expired",
                page_url="https://player.example.com/v/abc",
                slot_sid="sid_reextract",
            )
        mock_res_once.assert_awaited_once()
        self.assertEqual(mock_dl_403.await_count, 2)
        final_err_texts = [str(c.args[1]) for c in mock_edit_403.call_args_list if len(c.args) >= 2]
        self.assertTrue(any("HTTP 403" in t and "/warp_off" in t for t in final_err_texts), final_err_texts)

    async def test_engine_b_socks5_guard_and_client_reliability(self):
        # 1. Healthy SOCKS5 proxy + HTTP 403 in _prepare must NOT leak VPS IP via direct Engine B ffmpeg
        fake_socks_client = MagicMock()
        fake_socks_client.proxy_url = "socks5://u:p@1.2.3.4:1080"
        fake_socks_client.using_warp = False
        fake_socks_client.headers = {"User-Agent": "TestUA"}
        fake_socks_client.close = AsyncMock()

        with patch.object(dl_mod, "_ffmpeg_available", return_value=True), \
             patch.object(dl_mod, "HLS_ENGINE", "auto"), \
             patch.object(_cfg, "HLS_PROXY_FALLBACK_DIRECT", True, create=True), \
             patch.object(dl_mod, "_make_client", return_value=fake_socks_client), \
             patch.object(dl_mod, "_prepare", new=AsyncMock(side_effect=_client_mod.HlsHttpError(403, "https://cdn.example.com/m.m3u8"))), \
             patch.object(dl_mod, "_run_ffmpeg_detailed", new=AsyncMock()) as mock_ffmpeg:
            res_socks = await download_hls_to_mp4(
                "https://cdn.example.com/m.m3u8", "socks.m3u8", out_dir=self.work,
            )
        mock_ffmpeg.assert_not_awaited()
        self.assertFalse(res_socks.converted)
        self.assertEqual(res_socks.reason, R_DOWNLOAD_FAILED)
        self.assertTrue(res_socks.detail.startswith("playlist: HTTP 403"))

        # 2. HlsClient fetch_text_with_url & fetch_page must NOT double-wrap HlsProxyError
        inner_perr = _client_mod.HlsProxyError("socks5://1.2.3.4:1080", ProxyConnectionError(111, "down"))
        client_wrap = _client_mod.HlsClient(proxy_url="socks5://1.2.3.4:1080")
        client_wrap._proxy_url = "socks5://1.2.3.4:1080"
        fake_sess = MagicMock()
        fake_sess.closed = False
        fake_sess.get.return_value = _AsyncCM(exc=inner_perr)
        with patch.object(client_wrap, "_get_session", new=AsyncMock(return_value=fake_sess)):
            with self.assertRaises(_client_mod.HlsProxyError) as ctx_t:
                await client_wrap.fetch_text_with_url("https://cdn.example.com/a.m3u8")
            self.assertIs(ctx_t.exception, inner_perr)
            with self.assertRaises(_client_mod.HlsProxyError) as ctx_p:
                await client_wrap.fetch_page("https://site.example.com/watch")
            self.assertIs(ctx_p.exception, inner_perr)

        # 3. HlsClient cookie domain/path scoping preserved across session switches
        app = web.Application()

        async def login_ep(request: web.Request):
            resp = web.Response(text="ok")
            resp.set_cookie("scoped_tok", "only_a_hls", path="/hls")
            return resp

        app.router.add_get("/hls/login", login_ep)
        runner, base = await self._start_server(app)
        try:
            cookie_client = _client_mod.HlsClient(proxy_url=None)
            try:
                await cookie_client.fetch_text(f"{base}/hls/login")
                cookie_client.update_cookies({"b_tok": "only_b"}, url="https://b.example.com/v/1")
                await cookie_client.switch_to_direct()
                # Reopen session under new proxy key
                await cookie_client._get_session()
                self.assertEqual(
                    cookie_client.export_cookies(f"{base}/hls/seg0.ts").get("scoped_tok"),
                    "only_a_hls",
                )
                self.assertIsNone(cookie_client.export_cookies(f"{base}/other/seg0.ts").get("scoped_tok"))
                self.assertIsNone(cookie_client.export_cookies("https://b.example.com/hls/seg0.ts").get("scoped_tok"))
                self.assertEqual(
                    cookie_client.export_cookies("https://b.example.com/v/seg0.ts").get("b_tok"),
                    "only_b",
                )
            finally:
                await cookie_client.close()
        finally:
            await runner.cleanup()

    async def test_concurrent_auth_fallback_retries_outside_lock(self):
        initial_failures = 0
        all_initial_failed = asyncio.Event()
        probe_done = False
        active_retries = 0
        max_active_retries = 0
        both_overlapping = asyncio.Event()

        class FakeConcurrentClient:
            def __init__(self):
                self.headers = {
                    "User-Agent": "TestUA/1.0",
                    "Referer": "https://site.example/e/1",
                    "Origin": "https://site.example",
                }

            def apply_header_overrides(self, overrides):
                for k, v in (overrides or {}).items():
                    low = k.lower()
                    for existing in [ek for ek in self.headers if ek.lower() == low]:
                        self.headers.pop(existing, None)
                    if v is not None:
                        self.headers[k] = v

            async def download_bytes(self, url, byte_range=None, on_chunk=None, *, headers=None):
                nonlocal initial_failures, probe_done, active_retries, max_active_retries
                eff = dict(self.headers)
                if headers:
                    for k, v in headers.items():
                        low = k.lower()
                        for existing in [ek for ek in eff if ek.lower() == low]:
                            eff.pop(existing, None)
                        if v is not None:
                            eff[k] = v

                if "Origin" in eff:
                    initial_failures += 1
                    if initial_failures == 3:
                        all_initial_failed.set()
                    raise _client_mod.HlsHttpError(403, url)

                if not probe_done:
                    probe_done = True
                    await asyncio.wait_for(all_initial_failed.wait(), timeout=2.0)
                    return b"probe_ok"

                active_retries += 1
                max_active_retries = max(max_active_retries, active_retries)
                if active_retries == 2:
                    both_overlapping.set()
                try:
                    await asyncio.wait_for(both_overlapping.wait(), timeout=2.0)
                finally:
                    active_retries -= 1
                return b"retry_ok"

        client = FakeConcurrentClient()
        auth_state = dl_mod._PlaylistAuthState(
            client, parent_url="https://cdn.example/index.m3u8", base_headers=dict(client.headers),
        )
        with patch.object(_cfg, "HLS_SEGMENT_RETRIES", 1, create=True):
            results = await asyncio.gather(
                dl_mod._fetch_media_bytes_resilient(
                    client, "https://cdn.example/seg0.ts", label="segment 0", auth_state=auth_state,
                ),
                dl_mod._fetch_media_bytes_resilient(
                    client, "https://cdn.example/seg1.ts", label="segment 1", auth_state=auth_state,
                ),
                dl_mod._fetch_media_bytes_resilient(
                    client, "https://cdn.example/seg2.ts", label="segment 2", auth_state=auth_state,
                ),
            )
        self.assertEqual(sorted(results), [b"probe_ok", b"retry_ok", b"retry_ok"])
        self.assertEqual(max_active_retries, 2)

    async def test_browser_redirect_to_403_not_ok(self):
        # 1. Mocked redirect chain: /master.m3u8 -> 302 -> /blocked.html (403, non-.m3u8 target)
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

            class FakeRedirPage:
                def __init__(self):
                    self.url = "https://site.example/watch"
                    self.frames = []
                    self.main_frame = self
                    self._handlers = {}

                def set_default_timeout(self, ms):
                    pass

                def on(self, event, fn):
                    self._handlers.setdefault(event, []).append(fn)

                async def route(self, pattern, fn):
                    pass

                async def add_init_script(self, script):
                    pass

                async def evaluate(self, expr):
                    return ""

                async def goto(self, url, wait_until="domcontentloaded", timeout=5000):
                    req_init = MagicMock()
                    req_init.url = "https://site.example/master.m3u8"
                    req_init.frame = self
                    req_init.redirected_from = None

                    req_final = MagicMock()
                    req_final.url = "https://cdn.example/blocked.html"
                    req_final.frame = self
                    req_final.redirected_from = req_init

                    for fn in self._handlers.get("request", []):
                        fn(req_init)
                        fn(req_final)

                    resp = MagicMock()
                    resp.url = "https://cdn.example/blocked.html"
                    resp.status = 403
                    resp.headers = {"content-type": "text/html"}
                    resp.request = req_final
                    for fn in self._handlers.get("response", []):
                        fn(resp)

                async def close(self):
                    pass

            class FakeRedirContext:
                async def new_page(self):
                    return FakeRedirPage()

                async def cookies(self, urls):
                    return []

                async def close(self):
                    pass

            class FakeRedirBrowser:
                version = "153.0.8010.12"

                def is_connected(self):
                    return True

                async def new_context(self, **kwargs):
                    return FakeRedirContext()

                async def close(self):
                    pass

            class FakeRedirChromium:
                async def launch(self, **kwargs):
                    return FakeRedirBrowser()

            class FakeRedirPW:
                chromium = FakeRedirChromium()

                async def stop(self):
                    pass

            class FakeRedirStarter:
                async def start(self):
                    return FakeRedirPW()

            fake_pw_pkg = types.ModuleType("playwright")
            fake_async_api = types.ModuleType("playwright.async_api")
            fake_async_api.async_playwright = lambda: FakeRedirStarter()
            fake_pw_pkg.async_api = fake_async_api

            with patch.dict(sys.modules, {"playwright": fake_pw_pkg, "playwright.async_api": fake_async_api}), \
                 patch.object(browser_mod, "browser_available", return_value=True), \
                 patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(browser_mod, "_playwright_proxy", return_value=None):
                scan_redir = await browser_mod.resolve_stream_with_browser(
                    "https://site.example/watch", timeout=5,
                )
                await browser_mod.close_browser()
            self.assertIsNone(scan_redir.url)
            self.assertEqual(scan_redir.reason, "no-manifest")
        finally:
            browser_mod._pw = orig_pw
            browser_mod._browser = orig_browser
            browser_mod._active = orig_active
            browser_mod._browser_used = orig_used
            browser_mod._browser_proxy_key = orig_key
            browser_mod._browser_using_warp = orig_warp

        # 2. Real Playwright loopback 302 -> 403 verification
        if browser_mod.browser_available():
            app = web.Application()

            async def watch_ep(request: web.Request):
                return web.Response(
                    text='<html><body><script>fetch("/start/master.m3u8");</script></body></html>',
                    content_type="text/html",
                )

            async def redir_m3u8(request: web.Request):
                raise web.HTTPFound("/edge/forbidden")

            async def forbidden_ep(request: web.Request):
                return web.Response(status=403, text="forbidden", content_type="text/html")

            app.router.add_get("/watch", watch_ep)
            app.router.add_get("/start/master.m3u8", redir_m3u8)
            app.router.add_get("/edge/forbidden", forbidden_ep)
            runner, base = await self._start_server(app)
            try:
                with patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                     patch.object(browser_mod, "_playwright_proxy", return_value=None):
                    scan_live_redir = await browser_mod.resolve_stream_with_browser(
                        f"{base}/watch", timeout=6,
                    )
                    await browser_mod.close_browser()
                self.assertIsNone(scan_live_redir.url)
                self.assertEqual(scan_live_redir.reason, "no-manifest")
            finally:
                await runner.cleanup()

    async def test_extract_skips_mp4_and_decodes_voe_json(self):
        import base64

        def _encode_voe(payload_obj) -> str:
            raw2 = json.dumps(payload_obj).encode("utf-8")
            rev = base64.b64encode(raw2)
            shifted = rev[::-1]
            raw1 = bytes((b + 3) % 256 for b in shifted)
            b64_1 = base64.b64encode(raw1).decode("ascii")
            # Inject VOE junk tokens, then ROT13.
            obf = f"{b64_1[:8]}@${b64_1[8:16]}^^{b64_1[16:]}"
            return obf.translate(extract_mod._VOE_ROT13_TRANS)

        encoded_cfg = _encode_voe({
            "fallback": "https://cdn.example.com/clip.mp4",
            "source": "https://stream.example.com/hls/master.m3u8?token=abc123",
        })
        oversized_block = json.dumps(["A" * (extract_mod._VOE_MAX_BLOCK_CHARS + 50)])
        html = (
            '<html><body>'
            '<video src="https://cdn.example.com/Big_Buck_Bunny_1080_10s_5MB.mp4"></video>'
            '<script type="application/json">["not-valid-voe!!@@"]</script>'
            f'<script type="application/json">{oversized_block}</script>'
            f'<script type="application/json">{json.dumps([encoded_cfg])}</script>'
            '</body></html>'
        )

        decoded_blobs = extract_mod._decoded_voe_json_blobs(html)
        self.assertEqual(len(decoded_blobs), 1)
        self.assertIn("master.m3u8?token=abc123", decoded_blobs[0])

        manifests = extract_mod.find_manifests(html, "https://voe.example/e/abc123", limit=5)
        self.assertGreaterEqual(len(manifests), 1)
        self.assertEqual(
            manifests[0],
            "https://stream.example.com/hls/master.m3u8?token=abc123",
        )

        self.assertTrue(extract_mod._is_non_hls_media_url("https://cdn.example.com/video.mp4"))
        self.assertTrue(extract_mod._is_non_hls_media_url("https://cdn.example.com/video.webm?a=1"))
        self.assertFalse(extract_mod._is_non_hls_media_url("https://cdn.example.com/master.m3u8?a=1"))
        self.assertFalse(extract_mod._is_non_hls_media_url("https://cdn.example.com/play.mp4?u=a.m3u8"))

        class FakeClient:
            def __init__(self):
                self.fetched = []

            async def fetch_text_with_url(self, url, *, headers=None):
                self.fetched.append(url)
                if "master.m3u8" in url:
                    return "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\nseg0.ts\n", url
                raise AssertionError(f"Unexpected URL fetch: {url}")

        fc = FakeClient()
        only_mp4_url, mp4_detail, _ = await extract_mod._first_valid(
            fc,
            ["https://cdn.example.com/Big_Buck_Bunny_1080_10s_5MB.mp4"],
            referer_url="https://voe.example/e/abc123",
        )
        self.assertIsNone(only_mp4_url)
        self.assertIn("skipped non-HLS candidate", mp4_detail)
        self.assertEqual(fc.fetched, [])

        fc2 = FakeClient()
        picked, _, _ = await extract_mod._first_valid(
            fc2,
            [
                "https://cdn.example.com/Big_Buck_Bunny_1080_10s_5MB.mp4",
                "https://stream.example.com/hls/master.m3u8?token=abc123",
            ],
            referer_url="https://voe.example/e/abc123",
        )
        self.assertEqual(picked, "https://stream.example.com/hls/master.m3u8?token=abc123")
        self.assertEqual(fc2.fetched, ["https://stream.example.com/hls/master.m3u8?token=abc123"])

    async def test_client_fetch_text_rejects_binary_and_oversized(self):
        chunks_written = 0
        octet_chunks_written = 0
        app = web.Application()

        async def mp4_ep(request: web.Request):
            return web.Response(
                body=b"\x00\x00\x00\x20ftypmp42" * 20,
                content_type="video/mp4",
            )

        async def octet_ep(request: web.Request):
            return web.Response(
                body=b"\x00\x01\x02\x03",
                content_type="application/octet-stream",
            )

        async def ok_octet_m3u8_ep(request: web.Request):
            return web.Response(
                body=b"\xef\xbb\xbf  #EXTM3U\n#EXTINF:1.0,\nseg.ts\n#EXT-X-ENDLIST\n",
                content_type="application/octet-stream",
            )

        async def stream_octet_bin_ep(request: web.Request):
            nonlocal octet_chunks_written
            resp = web.StreamResponse(
                status=200,
                headers={"Content-Type": "application/octet-stream"},
            )
            await resp.prepare(request)
            try:
                first = b"\x00\x00\x00\x18ftypmp42" + b"\x00" * 1008
                for idx in range(20):
                    octet_chunks_written += 1
                    await resp.write(first if idx == 0 else b"\x00" * 1024)
                    await asyncio.sleep(0.01)
            except Exception:
                pass
            return resp

        async def hls_ep(request: web.Request):
            return web.Response(
                text="#EXTM3U\n#EXT-X-TARGETDURATION:6\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def big_cl_ep(request: web.Request):
            return web.Response(
                body=b"X" * 4096,
                content_type="text/plain",
            )

        async def huge_stream_ep(request: web.Request):
            nonlocal chunks_written
            resp = web.StreamResponse(
                status=200,
                headers={"Content-Type": "text/plain"},
            )
            await resp.prepare(request)
            try:
                for _ in range(20):
                    chunks_written += 1
                    await resp.write(b"Z" * 1024)
                    await asyncio.sleep(0.01)
            except Exception:
                pass
            return resp

        app.router.add_get("/video.mp4", mp4_ep)
        app.router.add_get("/blob.bin", octet_ep)
        app.router.add_get("/ok_octet.m3u8", ok_octet_m3u8_ep)
        app.router.add_get("/stream_octet.bin", stream_octet_bin_ep)
        app.router.add_get("/ok.m3u8", hls_ep)
        app.router.add_get("/big_cl.txt", big_cl_ep)
        app.router.add_get("/huge_stream.txt", huge_stream_ep)

        runner, base = await self._start_server(app)
        client = _client_mod.HlsClient(proxy_url=None)
        try:
            with self.assertRaises(ValueError):
                await client.fetch_text_with_url(f"{base}/video.mp4")

            with self.assertRaises(ValueError):
                await client.fetch_text_with_url(f"{base}/blob.bin")

            octet_txt, octet_u = await client.fetch_text_with_url(f"{base}/ok_octet.m3u8")
            self.assertIn("#EXTM3U", octet_txt)
            self.assertIn("#EXT-X-ENDLIST", octet_txt)
            self.assertTrue(octet_u.endswith("/ok_octet.m3u8"))

            octet_chunks_written = 0
            with self.assertRaises(ValueError):
                await client.fetch_text_with_url(f"{base}/stream_octet.bin")
            self.assertLess(octet_chunks_written, 20)

            txt, final_u = await client.fetch_text_with_url(f"{base}/ok.m3u8")
            self.assertIn("#EXTM3U", txt)
            self.assertTrue(final_u.endswith("/ok.m3u8"))

            with self.assertRaises(ValueError):
                await client.fetch_text_with_url(f"{base}/big_cl.txt", max_bytes=1024)

            chunks_written = 0
            with self.assertRaises(ValueError):
                await client.fetch_text_with_url(f"{base}/huge_stream.txt", max_bytes=1500)
            self.assertLess(chunks_written, 20)
        finally:
            await client.close()
            await runner.cleanup()

    async def test_browser_mismatch_hint_cached_fallback_and_fast_tap(self):
        import contextlib
        import io

        # 1. _log_launch_failure hint routing: non-/home cache prints mismatch hint (not ProtectHome)
        non_home_cache = os.path.join(self.work, "opt-ms-playwright")
        os.makedirs(os.path.join(non_home_cache, "chromium-1243"), exist_ok=True)
        wanted_missing = os.path.join(
            non_home_cache, "chromium-1255", "chrome-linux64", "chrome",
        )
        orig_hint_logged = browser_mod._HINT_LOGGED
        try:
            browser_mod._HINT_LOGGED = False
            buf = io.StringIO()
            with patch.object(browser_mod, "_home_masked", return_value=True), \
                 contextlib.redirect_stdout(buf):
                browser_mod._log_launch_failure(
                    RuntimeError(f"Executable doesn't exist at {wanted_missing}")
                )
            out_mismatch = buf.getvalue()
            self.assertIn("older Chromium revision", out_mismatch)
            self.assertIn("deploy/install-browser.sh", out_mismatch)
            self.assertNotIn("ProtectHome=true", out_mismatch)

            # Masked /home path still prints _sandbox_hint
            browser_mod._HINT_LOGGED = False
            buf2 = io.StringIO()
            with patch.object(browser_mod, "_home_masked", return_value=True), \
                 contextlib.redirect_stdout(buf2):
                browser_mod._log_launch_failure(
                    RuntimeError(
                        "Executable doesn't exist at "
                        "/home/botuser/.cache/ms-playwright/chromium-1255/chrome-linux64/chrome"
                    )
                )
            self.assertIn("ProtectHome=true", buf2.getvalue())
        finally:
            browser_mod._HINT_LOGGED = orig_hint_logged

        # 2. _find_cached_chromium_executable + _get_browser executable_path fallback
        pw_cache = os.path.join(self.work, "pw-cache")
        old_full = os.path.join(pw_cache, "chromium-1240", "chrome-linux64", "chrome")
        new_full = os.path.join(pw_cache, "chromium-1243", "chrome-linux64", "chrome")
        old_hs = os.path.join(
            pw_cache, "chromium_headless_shell-1240", "chrome-headless-shell-win64",
            "chrome-headless-shell.exe",
        )
        new_hs = os.path.join(
            pw_cache, "chromium_headless_shell-1243", "chrome-headless-shell-linux64",
            "chrome-headless-shell",
        )
        for p in (old_full, new_full, old_hs, new_hs):
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as fh:
                fh.write(b"fake")

        await browser_mod.close_browser()
        fake_browser = MagicMock()
        fake_browser.is_connected.return_value = True
        fake_browser.close = AsyncMock(return_value=None)
        launch_calls = []

        async def _fake_launch(**kwargs):
            launch_calls.append(dict(kwargs))
            if "executable_path" not in kwargs:
                raise RuntimeError(f"Executable doesn't exist at {wanted_missing}")
            return fake_browser

        fake_pw = types.SimpleNamespace(
            chromium=types.SimpleNamespace(launch=_fake_launch),
            stop=AsyncMock(return_value=None),
        )
        fake_cm = MagicMock()
        fake_cm.start = AsyncMock(return_value=fake_pw)

        env_patch = {
            "PLAYWRIGHT_BROWSERS_PATH": pw_cache,
            "LOCALAPPDATA": "",
        }
        with patch.dict(os.environ, env_patch, clear=False), \
             patch.object(browser_mod, "_CACHE_CANDIDATES", ()), \
             patch("os.path.expanduser", return_value=os.path.join(self.work, "missing-home-cache")):
            picked_exe = browser_mod._find_cached_chromium_executable()
            self.assertEqual(os.path.normpath(picked_exe or ""), os.path.normpath(new_hs))

            with patch("playwright.async_api.async_playwright", return_value=fake_cm):
                try:
                    got_browser, is_eph = await browser_mod._get_browser(
                        proxy=None, proxy_key=("direct", None), using_warp=False,
                    )
                    self.assertIs(got_browser, fake_browser)
                    self.assertFalse(is_eph)
                finally:
                    if browser_mod._idle_task is not None:
                        browser_mod._idle_task.cancel()
                        try:
                            await browser_mod._idle_task
                        except BaseException:
                            pass
                        browser_mod._idle_task = None
                    await browser_mod.close_browser()

        self.assertEqual(len(launch_calls), 2)
        self.assertNotIn("executable_path", launch_calls[0])
        self.assertEqual(
            os.path.normpath(launch_calls[1].get("executable_path", "")),
            os.path.normpath(new_hs),
        )

        # 3. Blocked ad requests do not stall quiet wait; early _read_tap returns fast
        if browser_mod.browser_available():
            app = web.Application()

            async def tap_ad_watch(request: web.Request):
                return web.Response(
                    text=(
                        "<html><body><video id='v'></video><script>"
                        "window.jwplayer = function(){ return { setup: function(cfg){} }; };"
                        "window.jwplayer().setup({ playlist: [{ sources: [{ file: '/cdn/tapped.m3u8?k=v' }] }] });"
                        "let i = 0;"
                        "setInterval(function(){ i++; "
                        "fetch('https://pagead2.googlesyndication.com/pagead.js?n=' + i).catch(function(){}); "
                        "}, 450);"
                        "</script></body></html>"
                    ),
                    content_type="text/html",
                )

            app.router.add_get("/watch", tap_ad_watch)
            runner, base = await self._start_server(app)
            try:
                t0 = time.monotonic()
                with patch.object(browser_mod, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                     patch.object(browser_mod, "_playwright_proxy", return_value=None):
                    scan_tap = await browser_mod.resolve_stream_with_browser(
                        f"{base}/watch", timeout=15,
                    )
                    await browser_mod.close_browser()
                elapsed = time.monotonic() - t0
                self.assertEqual(scan_tap.reason, "ok")
                self.assertEqual(scan_tap.source, "player-config")
                self.assertEqual(scan_tap.url, f"{base}/cdn/tapped.m3u8?k=v")
                self.assertLess(elapsed, 11.0)
            finally:
                await runner.cleanup()


class TestUniversalHlsUpgrade(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.work = tempfile.mkdtemp(prefix="hls_univ_test_")
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

    def _assert_valid_mp4(self, res: HlsResult, *, expect_audio: bool = True):
        self.assertTrue(res.converted, f"expected converted=True, got reason={res.reason!r} detail={res.detail!r}")
        self.assertTrue(os.path.exists(res.path))
        self.assertGreater(res.size, 1024)
        meta = _probe_media(res.path)
        fmt = (meta.get("format") or {}).get("format_name", "")
        self.assertIn("mp4", fmt)
        streams = meta.get("streams") or []
        vcodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "video"]
        acodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "audio"]
        self.assertTrue(vcodecs, f"no video stream in {meta}")
        if expect_audio:
            self.assertTrue(acodecs, f"expected audio stream in {meta}")
        self.assertTrue(_faststart_ok(res.path), "moov atom must precede mdat")

    async def test_playlist_inline_data_key_multikey_audio_only_and_init_key(self):
        import base64
        from app.hls import playlist as pl_mod

        key_raw = b"0123456789abcdef"
        key_b64 = base64.b64encode(key_raw).decode("ascii")
        data_uri = f"data:application/octet-stream;base64,{key_b64}"

        self.assertEqual(pl_mod.decode_data_key_uri(data_uri), key_raw)
        self.assertIsNone(pl_mod.decode_data_key_uri("data:text/plain;base64,AAAA"))

        # Inline 16-byte data: key is NOT DRM; short PSSH data: key IS DRM.
        inline_pl = f'#extm3u\n#EXT-X-KEY:METHOD=AES-128,URI="{data_uri}"\n#EXTINF:5,\nseg0.ts\n#EXT-X-ENDLIST\n'
        self.assertTrue(is_hls_playlist(inline_pl))
        self.assertFalse(is_drm(inline_pl))

        # Multi-key playlist with AES-128 + FairPlay SAMPLE-AES for the same segment -> NOT DRM, picks AES-128.
        multikey_pl = (
            "#EXTM3U\n"
            '#EXT-X-KEY:METHOD=AES-128,URI="key.bin"\n'
            '#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://fp",KEYFORMAT="com.apple.streamingkeydelivery"\n'
            '#EXT-X-MAP:URI="init.mp4"\n'
            "#EXTINF:4.0,\nseg0.m4s\n#EXT-X-ENDLIST\n"
        )
        self.assertFalse(is_drm(multikey_pl))
        parsed_mk = parse_media(multikey_pl, "https://cdn.example.com/hls/index.m3u8")
        self.assertEqual(parsed_mk.keys[parsed_mk.segments[0].key_index].method, "AES-128")
        self.assertEqual(parsed_mk.init_key_index, parsed_mk.segments[0].key_index)
        self.assertEqual(
            parsed_mk.init_section_keys.get(("https://cdn.example.com/hls/init.mp4", None)),
            parsed_mk.segments[0].key_index,
        )

        # Audio-only STREAM-INF deprioritized even when max_height caps out.
        master_ao = (
            "#EXTM3U\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=128000,CODECS="mp4a.40.2"\n'
            "audio_only.m3u8\n"
            '#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2"\n'
            "1080p.m3u8\n"
        )
        variants, _ = parse_master(master_ao, "https://cdn.example.com/master.m3u8")
        picked = select_variant(variants, max_height=720)
        self.assertIsNotNone(picked)
        self.assertTrue(picked.url.endswith("1080p.m3u8"))
        ranked = rank_variants(variants, max_height=720)
        self.assertTrue(ranked[0].url.endswith("1080p.m3u8"))
        self.assertTrue(ranked[-1].url.endswith("audio_only.m3u8"))

    async def test_large_disguised_preambles_tail_align_and_query_merge(self):
        # 40 KB PNG preamble + 3 TS packets + 7 trailing junk bytes
        png_blob = b"\x89PNG\r\n\x1a\n" + (b"\x01" * 40000) + b"IEND\xae\x42\x60\x82"
        ts_packets = (b"\x47" + b"\x22" * 187) * 3
        clean_ts, ext_ts = dl_mod._sanitize_segment_bytes(png_blob + ts_packets + b"JUNK123", is_fmp4=False)
        self.assertEqual(ext_ts, ".ts")
        self.assertEqual(clean_ts, ts_packets)

        # 20 KB JPEG preamble before fMP4 moof box
        moof_box = (32).to_bytes(4, "big") + b"moof" + (b"\x00" * 24)
        jpg_blob = b"\xff\xd8\xff\xe0" + (b"\x11" * 20000) + b"\xff\xd9"
        clean_fmp4, ext_fmp4 = dl_mod._sanitize_segment_bytes(jpg_blob + moof_box, is_fmp4=False)
        self.assertEqual(ext_fmp4, ".m4s")
        self.assertEqual(clean_fmp4, moof_box)

        # _merge_missing_query keeps child's existing params and appends missing parent auth params
        merged = dl_mod._merge_missing_query(
            "https://cdn.example.com/seg0.ts?part=1",
            "https://cdn.example.com/index.m3u8?part=99&token=abc&sig=xyz",
        )
        self.assertIsNotNone(merged)
        self.assertIn("part=1", merged)
        self.assertNotIn("part=99", merged)
        self.assertIn("token=abc", merged)
        self.assertIn("sig=xyz", merged)

    async def test_inline_data_aes_disguised_playlist_audio_fallback_and_dead_seg(self):
        if not _ffmpeg_ok():
            return
        import base64

        key = b"fedcba9876543210"
        key_b64 = base64.b64encode(key).decode("ascii")
        pl_text, enc_files = _make_hls_ts_segments(self.work, duration=2.0, seg_time=1.0, aes_key=key)
        # Rewrite key URI to inline data: URI
        pl_inline = pl_text.replace('URI="key.bin"', f'URI="data:application/octet-stream;base64,{key_b64}"')

        # Build video-only & audio-only TS for silent-video audio recovery + audio fallback
        v_path = os.path.join(self.work, "v_silent.ts")
        a_path = os.path.join(self.work, "a_only2.ts")
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
                "-t", "1.0", "-c:v", "libx264", "-pix_fmt", "yuv420p", v_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "1.0", "-c:a", "aac", a_path,
            ],
            check=True,
        )
        with open(v_path, "rb") as fh:
            v_bytes = fh.read()
        with open(a_path, "rb") as fh:
            a_bytes = fh.read()
        ts_full = _make_ts_fixture(self.work, duration=0.5)

        async def inline_aes_ep(request: web.Request):
            # Disguised Content-Type: image/png on playlist!
            return web.Response(text=pl_inline, content_type="image/png")

        async def enc_html_m3u8(request: web.Request):
            return web.Response(text=pl_inline, content_type="application/vnd.apple.mpegurl")

        async def enc_seg(request: web.Request):
            name = request.match_info["name"]
            if name in enc_files:
                return web.Response(body=enc_files[name], content_type="video/mp2t")
            return web.Response(status=404)

        async def enc_html_seg(request: web.Request):
            return web.Response(status=200, text="<html><body>WAF block</body></html>")

        async def silent_master(request: web.Request):
            # Deliberately omits AUDIO="..." on STREAM-INF even though standalone AUDIO exists
            return web.Response(
                text=(
                    "#EXTM3U\n"
                    '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Dead",DEFAULT=YES,URI="dead_audio.m3u8"\n'
                    '#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Backup",AUTOSELECT=YES,URI="good_audio.m3u8"\n'
                    "#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=160x120\n"
                    "silent_video.m3u8\n"
                ),
                content_type="application/vnd.apple.mpegurl",
            )

        async def dead_audio(request: web.Request):
            return web.Response(status=404)

        async def good_audio(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\na0.ts\n#EXT-X-ENDLIST\n")

        async def silent_video(request: web.Request):
            return web.Response(text="#EXTM3U\n#EXTINF:1.0,\nv0.ts\n#EXT-X-ENDLIST\n")

        async def dead_tail_m3u8(request: web.Request):
            lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:1"]
            for i in range(32):
                lines.append("#EXTINF:0.5,")
                lines.append(f"dt_{i}.ts")
            lines.append("#EXT-X-ENDLIST")
            return web.Response(text="\n".join(lines) + "\n")

        async def dead_tail_seg(request: web.Request):
            idx = int(request.match_info["idx"])
            if idx == 31:
                return web.Response(status=404, text="missing trailing segment")
            return web.Response(body=ts_full, content_type="video/mp2t")

        async def serve_v0(request: web.Request):
            return web.Response(body=v_bytes)

        async def serve_a0(request: web.Request):
            return web.Response(body=a_bytes)

        app = web.Application()
        app.router.add_get("/inline_aes.png", inline_aes_ep)
        app.router.add_get("/enc_html/index.m3u8", enc_html_m3u8)
        app.router.add_get("/enc_html/{name}", enc_html_seg)
        app.router.add_get("/silent_master.m3u8", silent_master)
        app.router.add_get("/dead_audio.m3u8", dead_audio)
        app.router.add_get("/good_audio.m3u8", good_audio)
        app.router.add_get("/silent_video.m3u8", silent_video)
        app.router.add_get("/v0.ts", serve_v0)
        app.router.add_get("/a0.ts", serve_a0)
        app.router.add_get("/dead_tail.m3u8", dead_tail_m3u8)
        app.router.add_get("/dt_{idx:\\d+}.ts", dead_tail_seg)
        app.router.add_get("/{name}", enc_seg)

        runner, base = await self._start_server(app)
        try:
            # 1. Inline data: AES-128 key + disguised Content-Type: image/png playlist
            res_inline = await download_hls_to_mp4(
                f"{base}/inline_aes.png", "inline_aes.m3u8", out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_inline)

            # 2. Encrypted segment returning HTTP 200 HTML error page fails as R_DOWNLOAD_FAILED even without C-AES
            with patch.object(dl_mod, "_detect_aes_backend", return_value=None), \
                 patch.object(_cfg, "HLS_SEGMENT_RETRIES", 1, create=True):
                res_html = await download_hls_to_mp4(
                    f"{base}/enc_html/index.m3u8", "enc_html.m3u8", out_dir=self.work, proxy_url=None,
                )
            self.assertFalse(res_html.converted)
            self.assertEqual(res_html.reason, R_DOWNLOAD_FAILED)
            self.assertIn("HTML", res_html.detail)

            # 3. Silent video recovers audio from fallback audio rendition
            res_rec = await download_hls_to_mp4(
                f"{base}/silent_master.m3u8", "recovered.m3u8", out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_rec, expect_audio=True)

            # 4. Large VOD (32 segments) survives 1 persistent 404 trailing segment
            with patch.object(_cfg, "HLS_SEGMENT_RETRIES", 1, create=True):
                res_dt = await download_hls_to_mp4(
                    f"{base}/dead_tail.m3u8", "dead_tail.m3u8", out_dir=self.work, proxy_url=None,
                )
            self._assert_valid_mp4(res_dt)
        finally:
            await runner.cleanup()

    async def test_direct_video_split_audio_fallbacks_and_initial_playlist(self):
        if not _ffmpeg_ok():
            return

        # Build valid MP4, video-only MP4, and audio-only M4A fixtures
        mp4_path = os.path.join(self.work, "direct_src.mp4")
        v_mp4_path = os.path.join(self.work, "split_v.mp4")
        a_m4a_path = os.path.join(self.work, "split_a.m4a")
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "1.5", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac",
                "-movflags", "+faststart", mp4_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
                "-t", "1.5", "-c:v", "libx264", "-pix_fmt", "yuv420p", v_mp4_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "1.5", "-c:a", "aac", a_m4a_path,
            ],
            check=True,
        )
        with open(mp4_path, "rb") as fh:
            mp4_bytes = fh.read()
        with open(v_mp4_path, "rb") as fh:
            v_mp4_bytes = fh.read()
        with open(a_m4a_path, "rb") as fh:
            a_m4a_bytes = fh.read()
        ts_bytes = _make_ts_fixture(self.work, duration=1.0)

        async def mp4_ep(request: web.Request):
            if request.headers.get("Referer") != "https://watch.example/v/1":
                return web.Response(status=403)
            return web.Response(body=mp4_bytes, content_type="video/mp4")

        async def split_v_ep(request: web.Request):
            return web.Response(body=v_mp4_bytes, content_type="video/mp4")

        async def split_a_ep(request: web.Request):
            if request.headers.get("X-Audio-Token") != "ok":
                return web.Response(status=403)
            return web.Response(body=a_m4a_bytes, content_type="audio/mp4")

        async def dead_hls_ep(request: web.Request):
            return web.Response(status=403, text="blocked")

        async def one_time_m3u8(request: web.Request):
            return web.Response(status=403, text="token already consumed")

        async def once_seg(request: web.Request):
            return web.Response(body=ts_bytes, content_type="video/mp2t")

        app = web.Application()
        app.router.add_get("/video.mp4", mp4_ep)
        app.router.add_get("/split_v.mp4", split_v_ep)
        app.router.add_get("/split_a.m4a", split_a_ep)
        app.router.add_get("/dead.m3u8", dead_hls_ep)
        app.router.add_get("/once/index.m3u8", one_time_m3u8)
        app.router.add_get("/once/seg0.ts", once_seg)

        runner, base = await self._start_server(app)
        try:
            # 1. Direct progressive MP4 with headers + faststart pass-through
            res_direct = await dl_mod.download_stream_to_mp4(
                f"{base}/video.mp4", "My Movie.mp4",
                stream_type="direct",
                headers={"Referer": "https://watch.example/v/1"},
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_direct)
            self.assertEqual(res_direct.engine, "direct")

            # 2. Split direct video + separate audio with distinct audio_headers
            res_split = await dl_mod.download_stream_to_mp4(
                f"{base}/split_v.mp4", "Split.mp4",
                stream_type="direct",
                audio_url=f"{base}/split_a.m4a",
                audio_headers={"X-Audio-Token": "ok"},
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_split, expect_audio=True)

            # 3. Fallback candidate: dead HLS -> falls back to direct MP4
            res_fb = await dl_mod.download_stream_to_mp4(
                f"{base}/dead.m3u8", "Fallback.m3u8",
                stream_type="hls",
                fallback_urls=[f"{base}/video.mp4"],
                headers={"Referer": "https://watch.example/v/1"},
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_fb)
            self.assertEqual(res_fb.engine, "direct")

            # 4. Pre-fetched initial_playlist_text avoids re-fetching a single-use playlist URL
            res_once = await dl_mod.download_stream_to_mp4(
                f"{base}/once/index.m3u8", "Once.m3u8",
                stream_type="hls",
                initial_playlist_text="#EXTM3U\n#EXTINF:1.0,\nseg0.ts\n#EXT-X-ENDLIST\n",
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_once)
        finally:
            await runner.cleanup()

    async def test_handler_tier2_ytdlp_metadata_store_and_broadened_reextract(self):
        from app.bot import main as bot_main

        # 1. _resolve_page_manifest falls through Tier 1 -> Tier 2 (yt-dlp) and skips Chromium
        msg = MagicMock()
        msg.from_user = MagicMock(id=303, username="tester", is_premium=False)
        msg.text = "https://video.example.com/watch/cool-ep"
        msg.caption = None
        msg.reply_text = AsyncMock()
        msg.photo = None
        msg.document = None

        miss_scan = extract_mod.PageScan(page_url="https://video.example.com/watch/cool-ep", reason="no-manifest")
        yt_hit = handler_mod._ResolvedPage(
            url="https://cdn.example.com/vid.mp4",
            page_url="https://video.example.com/watch/cool-ep",
            stream_type="direct",
            title="Cool Episode 1080p",
            audio_url="https://cdn.example.com/aud.m4a",
            headers={"Referer": "https://video.example.com/"},
            cookies={"s": "1"},
            cookie_entries=[{"name": "s", "value": "1", "domain": ".example.com", "path": "/"}],
            fallback_urls=["https://cdn.example.com/alt.m3u8"],
            extractor="yt-dlp",
        )
        with patch.object(handler_mod, "_scan_page_with_client", new=AsyncMock(return_value=(miss_scan, {}, {}))), \
             patch.object(handler_mod, "_ytdlp_stage", new=AsyncMock(return_value=yt_hit)) as mock_yt, \
             patch.object(handler_mod, "_browser_stage_resolved", new=AsyncMock()) as mock_br:
            resolved = await handler_mod._resolve_page_manifest(msg, "https://video.example.com/watch/cool-ep")
        mock_yt.assert_awaited_once()
        mock_br.assert_not_awaited()
        self.assertEqual(resolved.stream_type, "direct")
        self.assertEqual(resolved.title, "Cool Episode 1080p")
        self.assertEqual(resolved.audio_url, "https://cdn.example.com/aud.m4a")

        # 2. _store_stream_meta / _pop_stream_meta forwards rich metadata into process_hls_download
        handler_mod._store_stream_meta(
            "sid_meta_1",
            {
                "stream_type": "direct",
                "title": "Cool.Episode.1080p",
                "audio_url": "https://cdn.example.com/aud.m4a",
                "cookie_entries": [{"name": "s", "value": "1", "domain": ".example.com"}],
                "fallback_urls": ["https://cdn.example.com/alt.m3u8"],
                "orig_page_url": "https://video.example.com/watch/cool-ep",
            },
        )
        out_mp4 = os.path.join(self.work, "Cool.Episode.1080p.mp4")
        with open(out_mp4, "wb") as fh:
            fh.write(b"\x00" * 4096)

        captured_dl: list[dict] = []

        async def fake_dl(url, file_name, **kw):
            captured_dl.append({"url": url, "file_name": file_name, **kw})
            if len(captured_dl) == 1:
                # Simulate caption-delay token expiry returning 200 HTML -> R_NOT_HLS
                return HlsResult(converted=False, reason=R_NOT_HLS, detail="response does not start with #EXTM3U")
            return HlsResult(path=out_mp4, name="Cool.Episode.1080p.mp4", size=4096, converted=True, engine="direct", duration=2.0)

        status_msg = MagicMock()
        status_msg.edit_text = AsyncMock()
        status_msg.delete = AsyncMock()
        fake_slot = MagicMock(sid="sid_meta_1")

        with patch.object(handler_mod.manager, "get_client", new=AsyncMock(return_value=MagicMock())), \
             patch.object(bot_main, "begin_process", return_value=fake_slot), \
             patch.object(bot_main, "release_process"), \
             patch.object(bot_main, "is_cancelled", return_value=False), \
             patch.object(handler_mod, "_load_custom_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_get_backup_group_peer", new=AsyncMock(return_value=MagicMock())), \
             patch.object(handler_mod, "_call_download_hls", side_effect=fake_dl), \
             patch.object(handler_mod, "_resolve_page_manifest", new=AsyncMock(return_value=yt_hit)) as mock_re, \
             patch.object(handler_mod, "_get_video_metadata", new=AsyncMock(return_value={"duration": 2, "width": 160, "height": 120})), \
             patch.object(handler_mod, "_generate_video_thumb", new=AsyncMock(return_value=None)), \
             patch.object(handler_mod, "_upload_file_to_backup", new=AsyncMock(return_value=(77, False))), \
             patch.object(handler_mod, "_send_to_user", new=AsyncMock(return_value=True)), \
             patch.object(handler_mod, "log_forward", new=AsyncMock()), \
             patch.object(handler_mod, "safe_edit", new=AsyncMock()), \
             patch.object(handler_mod, "clear_reply_markup", new=AsyncMock()):
            await handler_mod.process_hls_download(
                MagicMock(), 303, msg, status_msg,
                url="https://cdn.example.com/expired.m3u8",
                page_url="https://video.example.com/watch/cool-ep",
                slot_sid="sid_meta_1",
            )
        mock_re.assert_awaited_once()
        self.assertEqual(len(captured_dl), 2)
        self.assertEqual(captured_dl[0]["file_name"], "Cool.Episode.1080p.m3u8")
        self.assertEqual(captured_dl[0].get("stream_type"), "direct")
        self.assertEqual(captured_dl[0].get("audio_url"), "https://cdn.example.com/aud.m4a")
        self.assertEqual(captured_dl[1]["url"], "https://cdn.example.com/vid.mp4")

        # 3. _should_reextract triggers on HTTP 410 playlist expiry and R_NOT_HLS
        self.assertTrue(handler_mod._should_reextract(HlsResult(converted=False, reason=R_NOT_HLS)))
        self.assertTrue(
            handler_mod._should_reextract(
                HlsResult(converted=False, reason=R_DOWNLOAD_FAILED, detail="playlist: HTTP 410 for https://cdn/a.m3u8")
            )
        )
        self.assertFalse(handler_mod._should_reextract(HlsResult(converted=False, reason=R_DRM)))

    async def test_extract_dean_edwards_packed_js_and_title(self):
        # Verify extractor's Dean Edwards p,a,c,k,e,d unpacker + title extraction end-to-end
        packed_html = (
            "<html><head><title>Awesome Anime Episode 5 - StreamWish</title></head><body><script>"
            "eval(function(p,a,c,k,e,d){while(c--)if(k[c])p=p.replace(new RegExp('\\\\b'+c.toString(a)+'\\\\b','g'),k[c]);return p;}"
            "('0.1({2:[{3:\"4://5.6/7/8.9?a=b\"}]});',12,12,"
            "'jwplayer|setup|sources|file|https|cdn|example|hls|master|m3u8|t|tok1'.split('|'),0,{}))"
            "</script></body></html>"
        )
        manifests = extract_mod.find_manifests(packed_html, "https://streamwish.example/e/abc", limit=5)
        self.assertIn("https://cdn.example/hls/master.m3u8?t=tok1", manifests)
        title = extract_mod.extract_title(packed_html)
        self.assertIn("Awesome Anime Episode 5", title)

    async def test_split_hls_external_audio_hls_and_direct(self):
        if not _ffmpeg_ok():
            return

        # Build video-only TS, audio-only TS, and direct M4A fixtures
        v_ts_path = os.path.join(self.work, "split_hls_v.ts")
        a_ts_path = os.path.join(self.work, "split_hls_a.ts")
        a_m4a_path = os.path.join(self.work, "split_hls_a.m4a")
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "testsrc=size=160x120:rate=24",
                "-t", "1.5", "-c:v", "libx264", "-pix_fmt", "yuv420p", v_ts_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "1.5", "-c:a", "aac", a_ts_path,
            ],
            check=True,
        )
        subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "sine=frequency=660",
                "-t", "1.5", "-c:a", "aac", a_m4a_path,
            ],
            check=True,
        )
        with open(v_ts_path, "rb") as fh:
            v_ts_bytes = fh.read()
        with open(a_ts_path, "rb") as fh:
            a_ts_bytes = fh.read()
        with open(a_m4a_path, "rb") as fh:
            a_m4a_bytes = fh.read()

        async def vid_m3u8(request: web.Request):
            if request.headers.get("X-Video-Key") != "vid-ok":
                return web.Response(status=403)
            return web.Response(
                text="#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXTINF:1.5,\nvseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def vid_seg(request: web.Request):
            if request.headers.get("X-Video-Key") != "vid-ok":
                return web.Response(status=403)
            return web.Response(body=v_ts_bytes, content_type="video/mp2t")

        async def aud_m3u8(request: web.Request):
            if request.headers.get("X-Audio-Key") != "hls-secret":
                return web.Response(status=403)
            return web.Response(
                text="#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXTINF:1.5,\naseg0.ts\n#EXT-X-ENDLIST\n",
                content_type="application/vnd.apple.mpegurl",
            )

        async def aud_seg(request: web.Request):
            if request.headers.get("X-Audio-Key") != "hls-secret":
                return web.Response(status=403)
            return web.Response(body=a_ts_bytes, content_type="video/mp2t")

        async def aud_direct(request: web.Request):
            if request.headers.get("X-Audio-Key") != "direct-secret":
                return web.Response(status=403)
            return web.Response(body=a_m4a_bytes, content_type="audio/mp4")

        app = web.Application()
        app.router.add_get("/v/index.m3u8", vid_m3u8)
        app.router.add_get("/v/vseg0.ts", vid_seg)
        app.router.add_get("/a/index.m3u8", aud_m3u8)
        app.router.add_get("/a/aseg0.ts", aud_seg)
        app.router.add_get("/a/track.m4a", aud_direct)

        runner, base = await self._start_server(app)
        try:
            # 1. Video-only HLS + separate HLS audio_url with distinct audio_headers
            res_hls_a = await dl_mod.download_stream_to_mp4(
                f"{base}/v/index.m3u8", "split_hls_audio.m3u8",
                stream_type="hls",
                headers={"X-Video-Key": "vid-ok"},
                audio_url=f"{base}/a/index.m3u8",
                audio_headers={"X-Audio-Key": "hls-secret"},
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_hls_a, expect_audio=True)

            # 2. Video-only HLS + separate direct .m4a audio_url with distinct audio_headers
            res_dir_a = await dl_mod.download_stream_to_mp4(
                f"{base}/v/index.m3u8", "split_hls_direct_audio.m3u8",
                stream_type="hls",
                headers={"X-Video-Key": "vid-ok"},
                audio_url=f"{base}/a/track.m4a",
                audio_headers={"X-Audio-Key": "direct-secret"},
                out_dir=self.work, proxy_url=None,
            )
            self._assert_valid_mp4(res_dir_a, expect_audio=True)
        finally:
            await runner.cleanup()

    async def test_encrypted_ext_x_map_local_playlist_key_before_map_without_aes_backend(self):
        from app.hls.client import HlsClient

        raw_key = b"0123456789abcdef"
        fake_init = b"\x00\x00\x00\x18ftypmp42" + (b"\xaa" * 20)
        fake_seg = b"\x00\x00\x00\x20moof" + (b"\xbb" * 24)
        pl_text = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:7\n"
            "#EXT-X-TARGETDURATION:4\n"
            '#EXT-X-KEY:METHOD=AES-128,URI="key.bin",IV=0x00000000000000000000000000000001\n'
            '#EXT-X-MAP:URI="init.mp4"\n'
            "#EXTINF:4.0,\n"
            "seg0.m4s\n"
            "#EXT-X-ENDLIST\n"
        )

        async def serve_key(request: web.Request):
            return web.Response(body=raw_key, content_type="application/octet-stream")

        async def serve_init(request: web.Request):
            return web.Response(body=fake_init, content_type="video/mp4")

        async def serve_seg(request: web.Request):
            return web.Response(body=fake_seg, content_type="video/iso.segment")

        app = web.Application()
        app.router.add_get("/key.bin", serve_key)
        app.router.add_get("/init.mp4", serve_init)
        app.router.add_get("/seg0.m4s", serve_seg)

        runner, base = await self._start_server(app)
        client = HlsClient(proxy_url=None)
        try:
            pl = parse_media(pl_text, f"{base}/index.m3u8")
            self.assertEqual(pl.init_key_index, pl.segments[0].key_index)
            self.assertEqual(pl.keys[pl.init_key_index].method, "AES-128")
            loc_dir = os.path.join(self.work, "loc_enc_map")
            with patch.object(dl_mod, "_detect_aes_backend", return_value=None):
                local_m3u8 = await dl_mod._localize_playlist(
                    client, pl, loc_dir,
                    parent_url=f"{base}/index.m3u8",
                    base_headers={},
                )
            with open(local_m3u8, "r", encoding="utf-8") as fh:
                rendered = fh.read()
            key_pos = rendered.find("#EXT-X-KEY:METHOD=AES-128,")
            map_pos = rendered.find("#EXT-X-MAP:URI=")
            self.assertNotEqual(key_pos, -1, f"missing #EXT-X-KEY in:\n{rendered}")
            self.assertNotEqual(map_pos, -1, f"missing #EXT-X-MAP in:\n{rendered}")
            self.assertLess(
                key_pos, map_pos,
                f"#EXT-X-KEY must precede #EXT-X-MAP when already_decrypted=False:\n{rendered}",
            )
        finally:
            await client.close()
            await runner.cleanup()

    async def test_strip_outer_image_wrapper_multiple_of_16_header(self):
        # 64-byte PNG wrapper (multiple of 16) + 32-byte ciphertext (multiple of 16) -> total 96 bytes (% 16 == 0)
        png_sig = b"\x89PNG\r\n\x1a\n"
        iend = b"IEND\xae\x42\x60\x82"
        png_pad = b"\x00" * (64 - len(png_sig) - len(iend))
        png_wrapper = png_sig + png_pad + iend
        self.assertEqual(len(png_wrapper), 64)
        cipher = bytes(range(32))
        wrapped_png = png_wrapper + cipher
        self.assertEqual(len(wrapped_png) % 16, 0)
        self.assertEqual(dl_mod._strip_outer_image_wrapper(wrapped_png), cipher)

        # 32-byte JPEG wrapper (multiple of 16) + 32-byte ciphertext -> total 64 bytes (% 16 == 0)
        jpg_wrapper = b"\xff\xd8\xff\xe0" + (b"\x11" * 26) + b"\xff\xd9"
        self.assertEqual(len(jpg_wrapper), 32)
        self.assertEqual(dl_mod._strip_outer_image_wrapper(jpg_wrapper + cipher), cipher)

        # Raw non-image 32-byte ciphertext is returned untouched
        self.assertEqual(dl_mod._strip_outer_image_wrapper(cipher), cipher)

    async def test_browser_subtitle_m3u8_rejected_not_leaked_via_fallback_req(self):
        sub_url = "https://cdn.example/subs.m3u8"
        sub_body = b"#EXTM3U\n#EXT-X-TARGETDURATION:5\n#EXTINF:5.0,\nseg0.vtt\n#EXT-X-ENDLIST\n"

        class _FakeReq:
            def __init__(self, url: str):
                self.url = url
                self.resource_type = "xhr"
                self.headers = {"user-agent": "FakeUA"}
                self.redirected_from = None

            async def all_headers(self):
                return dict(self.headers)

        class _FakeResp:
            def __init__(self, req: _FakeReq):
                self.url = req.url
                self.status = 200
                self.headers = {"content-type": "application/vnd.apple.mpegurl"}
                self.request = req

            async def body(self):
                return sub_body

        class _FakePage:
            def __init__(self):
                self.url = "https://watch.example/v/1"
                self._handlers: dict[str, list] = {}

            def set_default_timeout(self, ms: int):
                pass

            def on(self, event: str, cb):
                self._handlers.setdefault(event, []).append(cb)

            async def route(self, pattern: str, cb):
                pass

            async def add_init_script(self, script: str):
                pass

            async def goto(self, url: str, **kwargs):
                req = _FakeReq(sub_url)
                resp = _FakeResp(req)
                for cb in self._handlers.get("request", []):
                    cb(req)
                for cb in self._handlers.get("response", []):
                    cb(resp)

            async def evaluate(self, script: str):
                return ""

            async def close(self):
                pass

        class _FakeContext:
            def __init__(self):
                self.page = _FakePage()

            async def new_page(self):
                return self.page

            async def cookies(self, urls=None):
                return []

            async def close(self):
                pass

        class _FakeBrowser:
            version = "130.0.0.0"

            async def new_context(self, **kwargs):
                return _FakeContext()

            async def close(self):
                pass

        with patch.object(browser_mod, "browser_available", return_value=True), \
             patch.object(browser_mod, "_resolve_browser_proxy", new=AsyncMock(return_value=(None, ("direct", None), False))), \
             patch.object(browser_mod, "_get_browser", new=AsyncMock(return_value=(_FakeBrowser(), False))):
            scan = await browser_mod.resolve_stream_with_browser("https://watch.example/v/1", timeout=5)

        self.assertIsNone(scan.url, f"subtitle playlist leaked as scan.url={scan.url!r}")
        self.assertEqual(scan.reason, "no-manifest")


if __name__ == "__main__":
    unittest.main()
