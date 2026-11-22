"""Unit & integration tests for WARP proxy integration in app/torrent/."""

from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017/telegram_forwarder_test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN_DO_NOT_USE")

from aiohttp_socks import ProxyConnectionError, ProxyConnector

import app.torrent as torrent_pkg
from app.torrent.client import (
    Aria2Client,
    Aria2Error,
    build_aria2_proxy_options,
    download_torrent_file_via_warp,
    is_aria2_proxy_error,
)
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
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_chunked(self, size):
        for chunk in self._chunks:
            yield chunk


class TestAria2ProxyOptions(unittest.TestCase):
    def test_build_options_on_off_and_protocol_validation(self):
        on = build_aria2_proxy_options("http://127.0.0.1:40001")
        self.assertEqual(on["all-proxy"], "http://127.0.0.1:40001")
        self.assertEqual(on["proxy-method"], "tunnel")
        self.assertEqual(on["http-proxy"], "")
        self.assertEqual(on["https-proxy"], "")
        self.assertEqual(on["ftp-proxy"], "")

        off = build_aria2_proxy_options(None)
        self.assertEqual(off["all-proxy"], "")
        self.assertEqual(off["http-proxy"], "")
        self.assertEqual(off["https-proxy"], "")
        self.assertEqual(off["ftp-proxy"], "")
        self.assertEqual(off["proxy-method"], "get")

        # Must reject SOCKS5 URL because aria2c only supports HTTP/HTTPS proxies
        with self.assertRaises(ValueError):
            build_aria2_proxy_options("socks5://127.0.0.1:40000")

    def test_is_aria2_proxy_error(self):
        self.assertTrue(is_aria2_proxy_error(Aria2Error("Proxy connection failed")))
        self.assertTrue(
            is_aria2_proxy_error(
                Aria2Error("Could not connect to 127.0.0.1:40001"),
                "http://127.0.0.1:40001",
            )
        )
        self.assertFalse(is_aria2_proxy_error(Aria2Error("InfoHash is already registered", code=12)))
        self.assertFalse(is_aria2_proxy_error(Aria2Error("aria2 error 3: Resource not found", code=3)))


class TestAria2ClientAndWarpFetch(unittest.IsolatedAsyncioTestCase):
    async def test_set_proxy_and_per_download_opts(self):
        client = Aria2Client()
        rpc_calls = []

        async def fake_rpc(method, params=None):
            rpc_calls.append((method, params))
            return "gid-123"

        client._rpc = fake_rpc

        # Turn ON
        await client.set_proxy("http://127.0.0.1:40001")
        self.assertEqual(client.active_proxy_url, "http://127.0.0.1:40001")
        self.assertEqual(rpc_calls[-1][0], "aria2.changeGlobalOption")
        self.assertEqual(rpc_calls[-1][1][0]["all-proxy"], "http://127.0.0.1:40001")
        self.assertEqual(rpc_calls[-1][1][0]["proxy-method"], "tunnel")

        # add_magnet inherits active_proxy_url when unset
        await client.add_magnet("magnet:?xt=urn:btih:" + "a" * 40, "/tmp/d")
        opts = rpc_calls[-1][1][1]
        self.assertEqual(opts["all-proxy"], "http://127.0.0.1:40001")
        self.assertEqual(opts["proxy-method"], "tunnel")

        # Turn OFF clears global options and per-download options
        await client.set_proxy(None)
        self.assertIsNone(client.active_proxy_url)
        self.assertEqual(rpc_calls[-1][1][0]["all-proxy"], "")
        self.assertEqual(rpc_calls[-1][1][0]["proxy-method"], "get")

        await client.add_torrent_url("https://example.com/a.torrent", "/tmp/d", proxy_url=None)
        opts_off = rpc_calls[-1][1][1]
        self.assertEqual(opts_off["all-proxy"], "")
        self.assertEqual(opts_off["proxy-method"], "get")

        # add_torrent passes explicit proxy_url
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(b"d8:announce19:http://t.example/a4:infod4:name1:aee")
            tpath = tf.name
        try:
            await client.add_torrent(tpath, "/tmp/d", proxy_url="http://127.0.0.1:40001")
            opts_t = rpc_calls[-1][1][2]
            self.assertEqual(opts_t["all-proxy"], "http://127.0.0.1:40001")
        finally:
            os.unlink(tpath)

    async def test_sync_aria2_proxy_does_not_spawn_when_stopped(self):
        orig = torrent_pkg._client_instance
        try:
            torrent_pkg._client_instance = None
            updated = await torrent_pkg.sync_aria2_proxy("http://127.0.0.1:40001")
            self.assertFalse(updated)
            self.assertIsNone(torrent_pkg._client_instance)

            fake_client = MagicMock()
            fake_client.ping = AsyncMock(return_value=True)
            fake_client.set_proxy = AsyncMock()
            torrent_pkg._client_instance = fake_client

            updated2 = await torrent_pkg.sync_aria2_proxy("http://127.0.0.1:40001")
            self.assertTrue(updated2)
            fake_client.set_proxy.assert_awaited_once_with("http://127.0.0.1:40001")
        finally:
            torrent_pkg._client_instance = orig

    async def test_download_torrent_file_via_warp_success_and_redirects(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = os.path.join(tmp, "out.torrent")
            bencode_payload = b"d8:announce20:http://tracker/ann4:infod4:name4:testee"

            # 1. HTTP 302 -> valid .torrent
            resp1 = MagicMock(status=302, headers={"Location": "/real.torrent"})
            resp2 = MagicMock(status=200, headers={"Content-Length": str(len(bencode_payload))})
            resp2.content = _FakeContent([bencode_payload])

            fake_sess = MagicMock()
            fake_sess.__aenter__ = AsyncMock(return_value=fake_sess)
            fake_sess.__aexit__ = AsyncMock(return_value=False)
            fake_sess.get = MagicMock(side_effect=[_AsyncCM(resp1), _AsyncCM(resp2)])

            created_connectors = []

            def make_conn(**kw):
                c = ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True, **kw)
                created_connectors.append(c)
                return c

            with patch("app.torrent.client.get_active_socks5_url", return_value=WARP_ON_CFG.socks5_url), \
                 patch("app.torrent.client.create_aiohttp_connector", side_effect=make_conn), \
                 patch("app.torrent.client.aiohttp.ClientSession", return_value=fake_sess) as mock_cs:
                kind, path = await download_torrent_file_via_warp(
                    "https://example.com/start.torrent",
                    dest,
                    socks5_url=WARP_ON_CFG.socks5_url,
                )
                self.assertEqual(kind, "torrent_file")
                self.assertEqual(path, dest)
                with open(dest, "rb") as fh:
                    self.assertEqual(fh.read(), bencode_payload)
                self.assertFalse(mock_cs.call_args.kwargs["trust_env"])
            for c in created_connectors:
                await c.close()

            # 2. HTTP 302 -> magnet: redirect
            resp_mag = MagicMock(status=302, headers={"Location": "magnet:?xt=urn:btih:" + "b" * 40})
            fake_sess2 = MagicMock()
            fake_sess2.__aenter__ = AsyncMock(return_value=fake_sess2)
            fake_sess2.__aexit__ = AsyncMock(return_value=False)
            fake_sess2.get = MagicMock(return_value=_AsyncCM(resp_mag))

            c2 = ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True)
            try:
                with patch("app.torrent.client.get_active_socks5_url", return_value=WARP_ON_CFG.socks5_url), \
                     patch("app.torrent.client.create_aiohttp_connector", return_value=c2), \
                     patch("app.torrent.client.aiohttp.ClientSession", return_value=fake_sess2):
                    kind2, val2 = await download_torrent_file_via_warp(
                        "https://example.com/redir.torrent", dest, socks5_url=WARP_ON_CFG.socks5_url
                    )
                    self.assertEqual(kind2, "magnet")
                    self.assertTrue(val2.startswith("magnet:"))
            finally:
                await c2.close()

            # 3. Non-bencode HTML body rejected
            resp_html = MagicMock(status=200, headers={})
            resp_html.content = _FakeContent([b"<html>blocked</html>"])
            fake_sess3 = MagicMock()
            fake_sess3.__aenter__ = AsyncMock(return_value=fake_sess3)
            fake_sess3.__aexit__ = AsyncMock(return_value=False)
            fake_sess3.get = MagicMock(return_value=_AsyncCM(resp_html))

            c3 = ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True)
            try:
                with patch("app.torrent.client.get_active_socks5_url", return_value=WARP_ON_CFG.socks5_url), \
                     patch("app.torrent.client.create_aiohttp_connector", return_value=c3), \
                     patch("app.torrent.client.aiohttp.ClientSession", return_value=fake_sess3):
                    with self.assertRaises(Aria2Error):
                        await download_torrent_file_via_warp(
                            "https://example.com/bad.torrent", dest, socks5_url=WARP_ON_CFG.socks5_url
                        )
            finally:
                await c3.close()

            # 4. ProxyConnectionError -> WarpProxyError
            fake_sess4 = MagicMock()
            fake_sess4.__aenter__ = AsyncMock(return_value=fake_sess4)
            fake_sess4.__aexit__ = AsyncMock(return_value=False)
            fake_sess4.get = MagicMock(return_value=_AsyncCM(exc=ProxyConnectionError(111, "refused")))

            c4 = ProxyConnector.from_url(WARP_ON_CFG.socks5_url, rdns=True)
            try:
                with patch("app.torrent.client.get_active_socks5_url", return_value=WARP_ON_CFG.socks5_url), \
                     patch("app.torrent.client.create_aiohttp_connector", return_value=c4), \
                     patch("app.torrent.client.aiohttp.ClientSession", return_value=fake_sess4):
                    with self.assertRaises(WarpProxyError) as ctx:
                        await download_torrent_file_via_warp(
                            "https://example.com/x.torrent", dest, socks5_url=WARP_ON_CFG.socks5_url
                        )
                    self.assertIn("/warp_off", str(ctx.exception))
            finally:
                await c4.close()

    async def test_torrent_handler_fail_fast_without_spawning_aria2(self):
        # Stub app.database.db if not loaded (no MONGO_URI on dev machine)
        fake_db = types.ModuleType("app.database.db")
        fake_db.log_forward = AsyncMock()
        fake_db.get_user_session = AsyncMock()
        fake_db.get_user_profile = AsyncMock()
        fake_db.delete_user_session = AsyncMock()

        fake_bot_main = types.ModuleType("app.bot.main")
        fake_bot_main.get_backup_group_peer = AsyncMock()
        fake_bot_main.is_cancelled = MagicMock(return_value=False)

        with patch.dict(sys.modules, {"app.database.db": fake_db, "app.bot.main": fake_bot_main}):
            import importlib
            th = importlib.import_module("app.torrent.handler")

            with tempfile.TemporaryDirectory() as tmp:
                with patch.object(th, "ensure_proxy_ready", new=AsyncMock(side_effect=WarpProxyError("WARP unreachable /warp_status /warp_off"))), \
                     patch.object(torrent_pkg, "get_aria2_client", new=AsyncMock()) as mock_get_aria2:
                    with self.assertRaises(WarpProxyError):
                        await th._process_torrent(
                            MagicMock(),
                            MagicMock(),
                            MagicMock(),
                            MagicMock(),
                            123,
                            "magnet:?xt=urn:btih:" + "c" * 40,
                            "magnet",
                            tmp,
                        )
                    mock_get_aria2.assert_not_awaited()

    async def test_torrent_handler_on_torrent_url_and_off_clears_proxy(self):
        import importlib

        th = importlib.import_module("app.torrent.handler")

        # 1. WARP ON + torrent_url -> downloads via SOCKS5, sets HTTP proxy on aria2, converts mid-download proxy crash
        fake_aria2 = MagicMock()
        fake_aria2.set_proxy = AsyncMock()
        fake_aria2.add_torrent = AsyncMock(return_value="gid-on")
        fake_aria2.get_status = AsyncMock(return_value={"status": "active", "bittorrent": {"info": {"name": "demo"}}})
        fake_aria2.wait_for_download = AsyncMock(side_effect=Aria2Error("aria2 error 19: name resolution failed", code=19))

        fake_bot_main = types.ModuleType("app.bot.main")
        fake_bot_main.get_backup_group_peer = AsyncMock(return_value=MagicMock())
        fake_bot_main.is_cancelled = MagicMock(return_value=False)

        with tempfile.TemporaryDirectory() as tmp, \
             patch.dict(sys.modules, {"app.bot.main": fake_bot_main}), \
             patch.object(th, "ensure_proxy_ready", new=AsyncMock(return_value=WARP_ON_CFG)), \
             patch.object(th, "download_torrent_file_via_warp", new=AsyncMock(return_value=("torrent_file", os.path.join(tmp, "remote.torrent")))) as mock_dl, \
             patch.object(torrent_pkg, "get_aria2_client", new=AsyncMock(return_value=fake_aria2)), \
             patch.object(th, "check_proxy_reachable", new=AsyncMock(return_value=False)), \
             patch.object(th, "safe_edit", new=AsyncMock()), \
             patch.object(th.asyncio, "sleep", new=AsyncMock()):
            with self.assertRaises(WarpProxyError) as ctx:
                await th._process_torrent(
                    MagicMock(),
                    MagicMock(),
                    MagicMock(),
                    MagicMock(),
                    123,
                    "https://example.com/file.torrent",
                    "torrent_url",
                    tmp,
                )
            self.assertIn("/warp_off", str(ctx.exception))
            mock_dl.assert_awaited_once()
            fake_aria2.set_proxy.assert_awaited_once_with(WARP_ON_CFG.http_url)
            fake_aria2.add_torrent.assert_awaited_once()
            self.assertEqual(fake_aria2.add_torrent.call_args.kwargs["proxy_url"], WARP_ON_CFG.http_url)

        # 2. WARP OFF + magnet -> explicitly clears proxy to None
        fake_aria2_off = MagicMock()
        fake_aria2_off.set_proxy = AsyncMock()
        fake_aria2_off.add_magnet = AsyncMock(return_value="gid-off")
        fake_aria2_off.get_status = AsyncMock(return_value={"status": "active"})
        fake_aria2_off.wait_for_download = AsyncMock(side_effect=Aria2Error("Download cancelled by user"))

        with tempfile.TemporaryDirectory() as tmp, \
             patch.dict(sys.modules, {"app.bot.main": fake_bot_main}), \
             patch.object(th, "ensure_proxy_ready", new=AsyncMock(return_value=None)), \
             patch.object(torrent_pkg, "get_aria2_client", new=AsyncMock(return_value=fake_aria2_off)), \
             patch.object(th, "safe_edit", new=AsyncMock()), \
             patch.object(th.asyncio, "sleep", new=AsyncMock()):
            await th._process_torrent(
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
                123,
                "magnet:?xt=urn:btih:" + "d" * 40,
                "magnet",
                tmp,
            )
            fake_aria2_off.set_proxy.assert_awaited_once_with(None)
            fake_aria2_off.add_magnet.assert_awaited_once()
            self.assertIsNone(fake_aria2_off.add_magnet.call_args.kwargs["proxy_url"])


if __name__ == "__main__":
    unittest.main()
