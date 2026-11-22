"""Unit & integration tests for WARP proxy integration in app/direct/ and app/mediafire/."""

from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017/telegram_forwarder_test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN_DO_NOT_USE")

import aiohttp
from aiohttp_socks import ProxyConnectionError, ProxyConnector

from app.direct.client import DirectLinkClient
from app.direct.streamer import DirectLinkStreamer
from app.mediafire.client import MediaFireClient
from app.mediafire.streamer import MediaFireStreamer
from app.warp import WarpProxyConfig, WarpProxyError


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


class _FakeContent:
    def __init__(self, chunks, exc_after=None):
        self._chunks = list(chunks)
        self._exc_after = exc_after

    async def iter_chunked(self, size):
        for chunk in self._chunks:
            yield chunk
        if self._exc_after is not None:
            raise self._exc_after


class TestDirectLinkWarpIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_session_on_off_and_toggle_cleanup(self):
        client = DirectLinkClient()
        try:
            # 1. WARP ON -> ProxyConnector with rdns=True, trust_env=False
            with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
                 patch("app.direct.client.create_aiohttp_connector", side_effect=lambda **kw: ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True, **kw)):
                sess_on = await client._get_session()
                self.assertTrue(client.using_warp)
                self.assertIsInstance(sess_on.connector, ProxyConnector)
                self.assertTrue(sess_on.connector._rdns)
                self.assertFalse(sess_on._trust_env)

            # 2. Toggle to WARP OFF (with proxy.txt disabled) -> old session closed, TCPConnector used
            with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(DirectLinkClient, "_get_proxy_url", return_value=None):
                sess_off = await client._get_session()
                self.assertTrue(sess_on.closed)
                self.assertFalse(client.using_warp)
                self.assertIsInstance(sess_off.connector, aiohttp.TCPConnector)
                self.assertNotIsInstance(sess_off.connector, ProxyConnector)

            # 3. Toggle to WARP OFF with proxy.txt -> existing proxy.txt behavior preserved
            with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=None)), \
                 patch.object(DirectLinkClient, "_get_proxy_url", return_value="socks5://user:pass@1.2.3.4:1080"):
                sess_pt = await client._get_session()
                self.assertTrue(sess_off.closed)
                self.assertFalse(client.using_warp)
                self.assertIsInstance(sess_pt.connector, ProxyConnector)
        finally:
            await client.close()
        self.assertIsNone(client._session)

    async def test_connector_closed_on_session_init_failure(self):
        client = DirectLinkClient()
        fake_conn = MagicMock()
        fake_conn.close = AsyncMock()
        with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
             patch("app.direct.client.create_aiohttp_connector", return_value=fake_conn), \
             patch("app.direct.client.aiohttp.ClientSession", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                await client._get_session()
        fake_conn.close.assert_awaited_once()

    async def test_resolve_validates_url_before_session(self):
        client = DirectLinkClient()
        with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock()) as mock_ready:
            with self.assertRaises(ValueError):
                await client.resolve("ftp://example.com/file.mp4")
            with self.assertRaises(ValueError):
                await client.resolve("https:///nohost.mp4")
            mock_ready.assert_not_awaited()

    async def test_mid_request_proxy_error_warp_on_vs_off(self):
        # WARP ON -> wraps ProxyConnectionError into WarpProxyError with guidance
        client_on = DirectLinkClient()
        fake_sess = MagicMock()
        fake_sess.closed = False
        fake_sess.head.return_value = _AsyncCM(exc=ProxyConnectionError(111, "refused"))
        fake_sess.get.return_value = _AsyncCM(exc=ProxyConnectionError(111, "refused"))
        client_on._session = fake_sess
        client_on._using_warp = True
        client_on._session_proxy_key = ("warp", WARP_ON_CFG.socks5_url)

        with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
            with self.assertRaises(WarpProxyError) as ctx1:
                await client_on.resolve("https://example.com/video.mp4")
            self.assertIn("/warp_status", str(ctx1.exception))
            self.assertIn("/warp_off", str(ctx1.exception))

            with self.assertRaises(WarpProxyError) as ctx2:
                async for _ in client_on.download_stream("https://example.com/video.mp4"):
                    pass
            self.assertIn("/warp_off", str(ctx2.exception))

        # WARP OFF (using proxy.txt) -> does NOT misclassify as WarpProxyError
        client_off = DirectLinkClient()
        fake_sess_off = MagicMock()
        fake_sess_off.closed = False
        fake_sess_off.head.return_value = _AsyncCM(exc=ProxyConnectionError(111, "proxy.txt down"))
        client_off._session = fake_sess_off
        client_off._using_warp = False
        client_off._session_proxy_key = ("proxy_txt", "socks5://1.2.3.4:1080")

        with patch("app.direct.client.ensure_proxy_ready", new=AsyncMock(return_value=None)), \
             patch.object(DirectLinkClient, "_get_proxy_url", return_value="socks5://1.2.3.4:1080"):
            with self.assertRaises(ProxyConnectionError):
                await client_off.resolve("https://example.com/video.mp4")

    async def test_direct_streamer_propagates_warp_error_and_closes_without_deadlock(self):
        # 1. Error propagation without retry
        calls = 0

        class _ErrClient:
            async def download_stream(self, url, chunk_size=1024, start_offset=0):
                nonlocal calls
                calls += 1
                yield b"part1"
                raise WarpProxyError("WARP down /warp_status /warp_off")

        streamer = DirectLinkStreamer(_ErrClient(), "https://example.com/a.mp4", 10, "a.mp4")
        self.assertEqual(await streamer.read(), b"part1")
        with self.assertRaises(WarpProxyError):
            await streamer.read()
        self.assertEqual(calls, 1)
        await streamer.close()

        # 2. Close when bounded queue is full (no deadlock)
        class _EndlessClient:
            async def download_stream(self, url, chunk_size=1024, start_offset=0):
                for _ in range(100):
                    yield b"x" * 16

        s2 = DirectLinkStreamer(_EndlessClient(), "https://example.com/b.mp4", 1600, "b.mp4")
        await s2.start_download()
        await asyncio.sleep(0.05)  # allow queue (maxsize=4) to fill
        await asyncio.wait_for(s2.close(), timeout=1.0)


class TestMediaFireWarpIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_mediafire_session_on_off_and_resolve(self):
        client = MediaFireClient()
        try:
            # WARP ON
            with patch("app.mediafire.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
                 patch("app.mediafire.client.create_aiohttp_connector", side_effect=lambda **kw: ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True, **kw)):
                sess_on = await client._get_session()
                self.assertTrue(client.using_warp)
                self.assertIsInstance(sess_on.connector, ProxyConnector)
                self.assertTrue(sess_on.connector._rdns)
                self.assertFalse(sess_on._trust_env)

            # WARP OFF
            with patch("app.mediafire.client.ensure_proxy_ready", new=AsyncMock(return_value=None)):
                sess_off = await client._get_session()
                self.assertTrue(sess_on.closed)
                self.assertFalse(client.using_warp)
                self.assertNotIsInstance(sess_off.connector, ProxyConnector)
        finally:
            await client.close()

    async def test_mediafire_resolve_head_proxy_error_not_swallowed(self):
        client = MediaFireClient()
        page_resp = MagicMock()
        page_resp.status = 200
        page_resp.text = AsyncMock(
            return_value='<title>sample.mp4 | MediaFire</title><a href="https://download123.mediafire.com/abc/sample.mp4">DL</a>'
        )
        fake_sess = MagicMock()
        fake_sess.closed = False
        fake_sess.get.return_value = _AsyncCM(value=page_resp)
        fake_sess.head.return_value = _AsyncCM(exc=ProxyConnectionError(111, "refused"))

        client._session = fake_sess
        client._using_warp = True
        client._session_proxy_key = ("warp", WARP_ON_CFG.socks5_url)

        with patch("app.mediafire.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
            with self.assertRaises(WarpProxyError) as ctx:
                await client.resolve("https://www.mediafire.com/file/abc123/sample.mp4/file")
            self.assertIn("/warp_status", str(ctx.exception))
            self.assertIn("/warp_off", str(ctx.exception))

    async def test_mediafire_download_stream_and_streamer(self):
        client = MediaFireClient()
        dl_resp = MagicMock()
        dl_resp.status = 200
        dl_resp.content = _FakeContent([b"hello"], exc_after=ProxyConnectionError(111, "dropped"))
        fake_sess = MagicMock()
        fake_sess.closed = False
        fake_sess.get.return_value = _AsyncCM(value=dl_resp)

        client._session = fake_sess
        client._using_warp = True
        client._session_proxy_key = ("warp", WARP_ON_CFG.socks5_url)

        with patch("app.mediafire.client.ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)):
            streamer = MediaFireStreamer(client, "https://download1.mediafire.com/a/b.mp4", 10, "b.mp4")
            self.assertEqual(await streamer.read(), b"hello")
            with self.assertRaises(WarpProxyError):
                await streamer.read()
            await streamer.close()

        # Full queue close without deadlock
        class _EndlessMF:
            async def download_stream(self, direct_url, chunk_size=1024):
                for _ in range(100):
                    yield b"y" * 16

        s2 = MediaFireStreamer(_EndlessMF(), "https://download1.mediafire.com/a/c.mp4", 1600, "c.mp4")
        await s2.start_download()
        await asyncio.sleep(0.05)
        await asyncio.wait_for(s2.close(), timeout=1.0)


if __name__ == "__main__":
    unittest.main()
