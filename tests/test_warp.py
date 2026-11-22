"""Comprehensive unit & integration tests for app/warp/ and deploy/install-warp.sh."""

from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp_socks import ProxyConnectionError, ProxyConnector, ProxyError

from app.warp import (
    AFFECTED_FEATURES,
    DEFAULT_WARP_ENDPOINT,
    FALLBACK_WARP_IPV4_ENDPOINT,
    WarpConfig,
    WarpNotInstalledError,
    WarpProxyConfig,
    WarpProxyError,
    WarpTraceResult,
    WarpUnsupportedPlatformError,
    build_systemd_unit,
    build_wg_profile,
    build_wireproxy_conf,
    create_aiohttp_connector,
    detect_platform,
    disable_proxy,
    enable_proxy,
    ensure_loopback_host,
    ensure_proxy_ready,
    format_warp_proxy_error,
    get_active_http_url,
    get_active_proxy_config,
    get_active_socks5_url,
    get_warp_status,
    install_warp,
    is_proxy_enabled,
    is_proxy_enabled_async,
    is_warp_proxy_error,
    load_proxy_state,
    parse_cloudflare_trace,
    parse_wgcf_profile,
    reset_runtime_state,
    validate_loopback_host,
    validate_port,
    validate_service_name,
)
from app.warp import installer as warp_installer
from app.warp import manager as warp_manager


SAMPLE_WGCF_PROFILE = """[Interface]
PrivateKey = cHJpdmF0ZV9rZXlfZHVtbXlfMzJfYnl0ZXNfMDAwMDA=
Address = 172.16.0.2/32
Address = 2606:4700:110:8a36:1234:5678:9abc:def0/128
DNS = 1.1.1.1
MTU = 1280

[Peer]
PublicKey = bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo=
AllowedIPs = 0.0.0.0/0
AllowedIPs = ::/0
Endpoint = engage.cloudflareclient.com:2408
"""


def _make_cfg(tmp_dir: str, **overrides) -> WarpConfig:
    defaults = dict(
        bind_host="127.0.0.1",
        socks5_port=40000,
        http_port=40001,
        data_dir=tmp_dir,
        service_name="warp-wireproxy",
        mtu=1280,
        keepalive=25,
        endpoint=DEFAULT_WARP_ENDPOINT,
        trace_timeout=12,
    )
    defaults.update(overrides)
    return WarpConfig(**defaults)


class TestWarpConfig(unittest.TestCase):
    def test_validate_loopback_host_accepts_loopback(self):
        self.assertEqual(validate_loopback_host("127.0.0.1"), "127.0.0.1")
        self.assertEqual(validate_loopback_host("localhost"), "127.0.0.1")
        self.assertEqual(validate_loopback_host("::1"), "::1")
        self.assertEqual(validate_loopback_host("[::1]"), "::1")
        self.assertEqual(validate_loopback_host("127.0.0.2"), "127.0.0.2")

    def test_validate_loopback_host_rejects_non_loopback(self):
        for bad in ("0.0.0.0", "::", "192.168.1.10", "8.8.8.8", "", "example.com"):
            with self.assertRaises(ValueError):
                validate_loopback_host(bad)

    def test_ensure_loopback_host_clamps(self):
        self.assertEqual(ensure_loopback_host("0.0.0.0"), "127.0.0.1")
        self.assertEqual(ensure_loopback_host("10.0.0.5"), "127.0.0.1")
        self.assertEqual(ensure_loopback_host("::1"), "::1")

    def test_validate_port_and_service_name(self):
        self.assertEqual(validate_port(40000), 40000)
        for bad_port in (0, -1, 65536, "abc"):
            with self.assertRaises(ValueError):
                validate_port(bad_port)

        self.assertEqual(validate_service_name("warp-wireproxy.service"), "warp-wireproxy")
        for bad_svc in ("", "../evil", "foo;rm -rf /", "foo bar"):
            with self.assertRaises(ValueError):
                validate_service_name(bad_svc)

    def test_parse_and_build_wg_profile(self):
        parsed = parse_wgcf_profile(SAMPLE_WGCF_PROFILE)
        rendered = build_wg_profile(parsed, mtu=1280, keepalive=25)
        self.assertIn("PrivateKey = cHJpdmF0ZV9rZXlfZHVtbXlfMzJfYnl0ZXNfMDAwMDA=", rendered)
        self.assertIn("Address = 172.16.0.2/32, 2606:4700:110:8a36:1234:5678:9abc:def0/128", rendered)
        self.assertIn("AllowedIPs = 0.0.0.0/0, ::/0", rendered)
        self.assertIn("PersistentKeepalive = 25", rendered)

        rendered_ka = build_wg_profile(parsed, keepalive=25, keepalive_key="KeepAlive")
        self.assertIn("KeepAlive = 25", rendered_ka)

        rendered_no_ka = build_wg_profile(parsed, keepalive=0)
        self.assertNotIn("Keepalive", rendered_no_ka)

        # Optional PreSharedKey preservation
        with_psk = SAMPLE_WGCF_PROFILE + "PreSharedKey = cHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHM=\n"
        rendered_psk = build_wg_profile(parse_wgcf_profile(with_psk))
        self.assertIn("PreSharedKey = cHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHNrcHM=", rendered_psk)

    def test_build_wireproxy_conf_localhost_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = _make_cfg(tmp)
            conf = build_wireproxy_conf(cfg)
            self.assertIn("[Socks5]\nBindAddress = 127.0.0.1:40000", conf)
            self.assertIn("[http]\nBindAddress = 127.0.0.1:40001", conf)

            bad_cfg = _make_cfg(tmp, bind_host="0.0.0.0")
            with self.assertRaises(ValueError):
                build_wireproxy_conf(bad_cfg)

            dup_port_cfg = _make_cfg(tmp, socks5_port=40000, http_port=40000)
            with self.assertRaises(ValueError):
                build_wireproxy_conf(dup_port_cfg)

    def test_build_systemd_unit_protect_home_and_injection(self):
        opt_cfg = _make_cfg("/opt/telegram-forwarder-bot/warp")
        unit_opt = build_systemd_unit(opt_cfg, "/opt/telegram-forwarder-bot/warp/bin/wireproxy", run_user="botuser")
        self.assertIn("ProtectHome=true", unit_opt)
        self.assertIn("ProtectSystem=strict", unit_opt)
        self.assertIn("NoNewPrivileges=true", unit_opt)
        self.assertIn("User=botuser", unit_opt)

        home_cfg = _make_cfg("/home/ubuntu/bot/warp")
        unit_home = build_systemd_unit(home_cfg, "/home/ubuntu/bot/warp/bin/wireproxy", run_user="ubuntu")
        self.assertIn("ProtectHome=read-only", unit_home)

        root_cfg = _make_cfg("/root/bot/warp")
        unit_root = build_systemd_unit(root_cfg, "/root/bot/warp/bin/wireproxy")
        self.assertIn("ProtectHome=read-only", unit_root)

        with self.assertRaises(ValueError):
            build_systemd_unit(opt_cfg, "/opt/bin/wireproxy\nExecStartPre=/bin/evil")
        with self.assertRaises(ValueError):
            build_systemd_unit(opt_cfg, "/opt/bin/wireproxy", run_user="root;evil")


class TestWarpErrorsAndTrace(unittest.TestCase):
    def test_is_warp_proxy_error_detection(self):
        self.assertTrue(is_warp_proxy_error(WarpProxyError("down")))
        self.assertTrue(is_warp_proxy_error(ProxyConnectionError(111, "refused")))
        self.assertTrue(is_warp_proxy_error(ProxyError("socks failure")))
        self.assertTrue(is_warp_proxy_error(ConnectionRefusedError("connect 127.0.0.1:40000 failed")))
        self.assertFalse(is_warp_proxy_error(ValueError("bad url")))
        self.assertFalse(is_warp_proxy_error(ConnectionRefusedError("connect 93.184.216.34:443 failed")))

        # Chained cause
        outer = RuntimeError("wrapper")
        outer.__cause__ = ProxyConnectionError(111, "refused")
        self.assertTrue(is_warp_proxy_error(outer))

    def test_format_warp_proxy_error(self):
        msg = format_warp_proxy_error(ConnectionRefusedError("refused"), feature="Direct")
        self.assertIn("Direct", msg)
        self.assertIn("/warp_status", msg)
        self.assertIn("/warp_off", msg)

        not_inst = format_warp_proxy_error(WarpNotInstalledError("missing"), feature="Torrent")
        self.assertIn("/warp_install", not_inst)
        self.assertIn("/warp_off", not_inst)

        # Already-formatted error does not duplicate guidance
        double = format_warp_proxy_error(WarpProxyError(msg), feature="Direct")
        self.assertEqual(double.count("/warp_status"), 1)

    def test_parse_cloudflare_trace(self):
        raw = "fl=123f\nh=www.cloudflare.com\nip=104.28.1.2\nloc=SG\ncolo=SIN\nwarp=on\n"
        parsed = parse_cloudflare_trace(raw)
        self.assertEqual(parsed["warp"], "on")
        self.assertEqual(parsed["ip"], "104.28.1.2")
        self.assertEqual(parsed["loc"], "SG")
        self.assertEqual(parsed["colo"], "SIN")


class TestWarpStateAndLifecycle(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_runtime_state()
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = _make_cfg(self.tmp.name)

    def tearDown(self):
        reset_runtime_state()
        self.tmp.cleanup()

    async def test_enable_fails_when_not_installed_and_ports_down(self):
        with patch.object(warp_manager, "get_warp_config", return_value=self.cfg), \
             patch.object(warp_manager, "_has_mongo_uri", return_value=False), \
             patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=False)), \
             patch.object(warp_manager, "find_wireproxy_binary", return_value=None), \
             patch.object(warp_manager, "find_wireproxy_config", return_value=None):
            res = await enable_proxy(cfg=self.cfg)
            self.assertFalse(res.ok)
            self.assertIn("/warp_install", res.message)
            self.assertFalse(is_proxy_enabled(self.cfg))

    async def test_enable_disable_persistence_and_consumer_helpers(self):
        db_store = {}

        async def fake_save(enabled: bool, meta=None):
            db_store["doc"] = {"enabled": enabled, **(meta or {})}

        async def fake_get():
            return db_store.get("doc")

        fake_db = types.ModuleType("app.database.db")
        fake_db.save_warp_proxy_state = fake_save
        fake_db.get_warp_proxy_state = fake_get

        with patch.object(warp_manager, "get_warp_config", return_value=self.cfg), \
             patch.object(warp_manager, "_has_mongo_uri", return_value=True), \
             patch.dict(sys.modules, {"app.database.db": fake_db}), \
             patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=True)):

            res_on = await enable_proxy(cfg=self.cfg)
            self.assertTrue(res_on.ok)
            self.assertTrue(res_on.enabled)
            self.assertEqual(AFFECTED_FEATURES, ("Direct", "Torrent", "MediaFire", "HLS"))
            self.assertIn("HLS", res_on.message)
            self.assertTrue(is_proxy_enabled(self.cfg))
            self.assertEqual(get_active_socks5_url(), "socks5://127.0.0.1:40000")
            self.assertEqual(get_active_http_url(), "http://127.0.0.1:40001")
            pcfg = get_active_proxy_config()
            self.assertIsInstance(pcfg, WarpProxyConfig)
            self.assertTrue(pcfg.enabled)

            conn = create_aiohttp_connector(limit=10)
            self.assertIsInstance(conn, ProxyConnector)
            self.assertTrue(conn._rdns)
            await conn.close()

            # Verify persisted to both DB and local state.json
            self.assertTrue(db_store["doc"]["enabled"])
            with open(self.cfg.state_file, "r", encoding="utf-8") as fh:
                self.assertTrue(json.load(fh)["enabled"])

            # Simulate bot restart: reset memory, load from DB
            reset_runtime_state()
            loaded = await load_proxy_state(self.cfg)
            self.assertTrue(loaded)

            # Simulate DB outage: falls back to local state.json
            reset_runtime_state()
            fake_db_err = types.ModuleType("app.database.db")
            fake_db_err.get_warp_proxy_state = AsyncMock(side_effect=RuntimeError("db down"))
            fake_db_err.save_warp_proxy_state = fake_save
            with patch.dict(sys.modules, {"app.database.db": fake_db_err}):
                loaded_fallback = await load_proxy_state(self.cfg)
                self.assertTrue(loaded_fallback)

            # Disable persists OFF and consumer helpers return None
            res_off = await disable_proxy(cfg=self.cfg)
            self.assertTrue(res_off.ok)
            self.assertFalse(res_off.enabled)
            self.assertIn("HLS", res_off.message)
            self.assertFalse(is_proxy_enabled(self.cfg))
            self.assertIsNone(get_active_socks5_url())
            self.assertIsNone(get_active_http_url())
            self.assertIsNone(get_active_proxy_config())
            self.assertIsNone(create_aiohttp_connector())
            self.assertFalse(db_store["doc"]["enabled"])

    async def test_ensure_proxy_ready_off_on_and_unreachable(self):
        with patch.object(warp_manager, "get_warp_config", return_value=self.cfg), \
             patch.object(warp_manager, "_has_mongo_uri", return_value=False):
            # OFF -> returns None
            self.assertIsNone(await ensure_proxy_ready(feature="Direct", cfg=self.cfg))

            # Turn ON with ports reachable
            with patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=True)):
                await enable_proxy(cfg=self.cfg)
                ready = await ensure_proxy_ready(require_http=True, require_socks5=True, feature="Torrent", cfg=self.cfg)
                self.assertIsNotNone(ready)
                self.assertEqual(ready.http_url, "http://127.0.0.1:40001")

            # Now ports go down and not installed -> raises WarpProxyError with guidance
            with patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=False)), \
                 patch.object(warp_manager, "is_wireproxy_installed", return_value=False):
                with self.assertRaises(WarpProxyError) as ctx:
                    await ensure_proxy_ready(require_http=True, require_socks5=True, feature="Torrent", cfg=self.cfg)
                self.assertIn("/warp_off", str(ctx.exception))
                self.assertIn("/warp_install", str(ctx.exception))

    async def test_explicit_cfg_isolation(self):
        other_tmp = tempfile.TemporaryDirectory()
        try:
            other_cfg = _make_cfg(other_tmp.name)
            with patch.object(warp_manager, "_has_mongo_uri", return_value=False), \
                 patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=True)):
                await enable_proxy(cfg=self.cfg)
                self.assertTrue(await is_proxy_enabled_async(self.cfg))
                # other_cfg has no state.json -> must evaluate to False, not leak self.cfg's True
                self.assertFalse(await is_proxy_enabled_async(other_cfg))
                st = await get_warp_status(check_trace=False, cfg=other_cfg)
                self.assertFalse(st.proxy_enabled)
        finally:
            other_tmp.cleanup()


class TestWarpInstaller(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_runtime_state()
        self.tmp = tempfile.TemporaryDirectory()
        self.cfg = _make_cfg(self.tmp.name)

    def tearDown(self):
        reset_runtime_state()
        self.tmp.cleanup()

    def _build_tarball(self, members: list[tuple[str, bytes, Optional[str]]]) -> str:
        tar_path = os.path.join(self.tmp.name, "test.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tf:
            for name, data, linkname in members:
                info = tarfile.TarInfo(name=name)
                if linkname is not None:
                    info.type = tarfile.SYMTYPE
                    info.linkname = linkname
                    tf.addfile(info)
                else:
                    info.size = len(data)
                    tf.addfile(info, io.BytesIO(data))
        return tar_path

    def test_tar_slip_and_elf_validation(self):
        dest = os.path.join(self.tmp.name, "bin", "wireproxy")

        # Reject path traversal
        bad_tar = self._build_tarball([("../wireproxy", b"\x7fELFdummy", None)])
        with self.assertRaises(RuntimeError):
            warp_installer._safe_extract_wireproxy_tarball(bad_tar, dest)

        # Reject absolute path
        abs_tar = self._build_tarball([("/usr/bin/wireproxy", b"\x7fELFdummy", None)])
        with self.assertRaises(RuntimeError):
            warp_installer._safe_extract_wireproxy_tarball(abs_tar, dest)

        # Reject symlink
        sym_tar = self._build_tarball([("wireproxy", b"", "/bin/sh")])
        with self.assertRaises(RuntimeError):
            warp_installer._safe_extract_wireproxy_tarball(sym_tar, dest)

        # Reject non-ELF binary
        not_elf = self._build_tarball([("wireproxy", b"#!/bin/sh\necho hi", None)])
        with self.assertRaises(RuntimeError):
            warp_installer._safe_extract_wireproxy_tarball(not_elf, dest)

        # Accept clean ELF tarball
        good_tar = self._build_tarball([("./wireproxy", b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 64, None)])
        warp_installer._safe_extract_wireproxy_tarball(good_tar, dest)
        self.assertTrue(os.path.isfile(dest))

    def test_detect_platform_variants(self):
        with patch.object(warp_installer.sys, "platform", "win32"):
            with self.assertRaises(WarpUnsupportedPlatformError):
                detect_platform()

        with patch.object(warp_installer.sys, "platform", "linux"), \
             patch.object(warp_installer.platform, "machine", return_value="x86_64"):
            self.assertEqual(detect_platform(), ("linux", "amd64"))

        with patch.object(warp_installer.sys, "platform", "linux"), \
             patch.object(warp_installer.platform, "machine", return_value="aarch64"):
            self.assertEqual(detect_platform(), ("linux", "arm64"))

        with patch.object(warp_installer.sys, "platform", "linux"), \
             patch.object(warp_installer.platform, "machine", return_value="mips64"):
            with self.assertRaises(WarpUnsupportedPlatformError):
                detect_platform()

    async def test_install_warp_non_linux_graceful(self):
        with patch.object(warp_installer.sys, "platform", "win32"), \
             patch.object(warp_manager, "_has_mongo_uri", return_value=False), \
             patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=False)):
            res = await install_warp(cfg=self.cfg)
            self.assertFalse(res.ok)
            self.assertIn("Linux", res.message)

    async def test_keepalive_fallback_in_config_validation(self):
        os.makedirs(self.cfg.data_dir, exist_ok=True)
        with open(self.cfg.wgcf_profile_path, "w", encoding="utf-8") as fh:
            fh.write(SAMPLE_WGCF_PROFILE)

        calls = []

        async def fake_run_exec(*args, **kwargs):
            with open(self.cfg.wgcf_profile_path, "r", encoding="utf-8") as fh:
                content = fh.read()
            calls.append(content)
            if "PersistentKeepalive" in content:
                return (1, "", "unknown key PersistentKeepalive")
            return (0, "OK", "")

        with patch.object(warp_installer, "run_exec", side_effect=fake_run_exec):
            changed = await warp_installer._write_and_validate_configs(self.cfg, "/fake/wireproxy")
            self.assertTrue(changed)
            self.assertEqual(len(calls), 2)
            self.assertIn("KeepAlive = 25", calls[1])

    async def test_deploy_reuses_active_systemd_in_sandbox(self):
        with patch.object(warp_installer, "can_manage_systemd", new=AsyncMock(return_value=(False, False))), \
             patch.object(warp_installer, "systemd_unit_exists", return_value=True), \
             patch.object(warp_installer, "is_systemd_active", new=AsyncMock(return_value=True)), \
             patch.object(warp_installer, "check_proxy_reachable", new=AsyncMock(return_value=True)):
            # Idempotent run (no config change) reuses systemd without port collision
            mode = await warp_installer._deploy_and_start_service(
                self.cfg, "/fake/wireproxy", None, config_changed=False
            )
            self.assertEqual(mode, "systemd")

            # Config changed -> raises actionable sudo systemctl restart message
            with self.assertRaises(RuntimeError) as ctx:
                await warp_installer._deploy_and_start_service(
                    self.cfg, "/fake/wireproxy", None, config_changed=True
                )
            self.assertIn("sudo systemctl restart warp-wireproxy", str(ctx.exception))

    async def test_install_warp_mocked_linux_with_ipv4_fallback(self):
        progress_msgs: list[str] = []

        async def on_prog(text: str):
            progress_msgs.append(text)

        async def fake_ensure_wp(session, cfg, arch, force, on_progress):
            os.makedirs(cfg.bin_dir, exist_ok=True)
            with open(cfg.wireproxy_bin, "wb") as fh:
                fh.write(b"\x7fELFdummy")
            return cfg.wireproxy_bin

        async def fake_ensure_wgcf(session, cfg, arch, force, on_progress):
            os.makedirs(cfg.bin_dir, exist_ok=True)
            with open(cfg.wgcf_bin, "wb") as fh:
                fh.write(b"\x7fELFdummy")
            return cfg.wgcf_bin

        async def fake_register(cfg, wgcf_bin, force, on_progress):
            with open(cfg.wgcf_profile_path, "w", encoding="utf-8") as fh:
                fh.write(SAMPLE_WGCF_PROFILE)

        trace_calls = [
            WarpTraceResult(ok=False, warp_on=False, error="timeout"),
            WarpTraceResult(ok=False, warp_on=False, error="timeout"),
            WarpTraceResult(ok=False, warp_on=False, error="timeout"),
            WarpTraceResult(ok=True, warp_on=True, warp_state="on", exit_ip="104.28.1.9", location="SG", colo="SIN"),
        ]

        async def fake_trace(cfg, timeout=None):
            return trace_calls.pop(0) if trace_calls else WarpTraceResult(
                ok=True, warp_on=True, warp_state="on", exit_ip="104.28.1.9", location="SG"
            )

        with patch.object(warp_installer, "detect_platform", return_value=("linux", "amd64")), \
             patch.object(warp_installer, "_ensure_wireproxy_binary", side_effect=fake_ensure_wp), \
             patch.object(warp_installer, "_ensure_wgcf_binary", side_effect=fake_ensure_wgcf), \
             patch.object(warp_installer, "_register_warp_account", side_effect=fake_register), \
             patch.object(warp_installer, "run_exec", new=AsyncMock(return_value=(0, "OK", ""))), \
             patch.object(warp_installer, "_deploy_and_start_service", new=AsyncMock(return_value="managed")), \
             patch.object(warp_installer, "wait_for_ports", new=AsyncMock(return_value=(True, True))), \
             patch.object(warp_installer, "check_cloudflare_trace", side_effect=fake_trace), \
             patch.object(warp_installer.asyncio, "sleep", new=AsyncMock()), \
             patch.object(warp_manager, "_has_mongo_uri", return_value=False), \
             patch.object(warp_manager, "check_tcp_port", new=AsyncMock(return_value=True)):
            res = await install_warp(on_progress=on_prog, cfg=self.cfg)
            self.assertTrue(res.ok, msg=res.message)
            self.assertIn("104.28.1.9", res.message)
            self.assertTrue(any("162.159.192.1:2408" in m for m in progress_msgs))
            # Verify fallback endpoint was written to profile
            with open(self.cfg.wgcf_profile_path, "r", encoding="utf-8") as fh:
                self.assertIn(FALLBACK_WARP_IPV4_ENDPOINT, fh.read())

    def test_install_warp_shell_script_lf_and_syntax(self):
        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "deploy",
            "install-warp.sh",
        )
        with open(script_path, "rb") as fh:
            raw = fh.read()
        self.assertNotIn(b"\r\n", raw, "deploy/install-warp.sh must use LF line endings")
        self.assertIn(b"WARP_BIND_HOST=\"127.0.0.1\"", raw)

        git_bash = r"C:\Program Files\Git\bin\bash.exe"
        bash_bin = git_bash if os.path.isfile(git_bash) else (shutil.which("bash") if os.name != "nt" else None)
        if bash_bin:
            proc = subprocess.run([bash_bin, "-n", script_path], capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

        # Regression: ViRb3/wgcf has no --version flag; must use --help instead
        text = raw.decode("utf-8")
        self.assertNotIn("WGCF_BIN\" --version", text)
        self.assertIn("WGCF_BIN\" --help", text)
        self.assertIn("register --accept-tos", text)
        self.assertIn("\"$WGCF_BIN\" generate", text)
        self.assertIn("\"$WIREPROXY_BIN\" -v", text)
        self.assertIn("\"$WIREPROXY_BIN\" -c \"$WIREPROXY_CONF\" -n", text)

    async def test_wgcf_cli_uses_help_not_version_and_retries_cleanly(self):
        # 1. Reuse path uses --help (never --version)
        os.makedirs(self.cfg.bin_dir, exist_ok=True)
        with open(self.cfg.wgcf_bin, "wb") as fh:
            fh.write(b"\x7fELFdummy")

        seen_calls = []

        async def fake_exec(*args, **kwargs):
            seen_calls.append(args)
            self.assertNotIn("--version", args, "ViRb3/wgcf does not support --version")
            return (0, "Usage:\n  wgcf [flags]", "")

        with patch.object(warp_installer, "run_exec", side_effect=fake_exec):
            bin_path = await warp_installer._ensure_wgcf_binary(
                MagicMock(), self.cfg, "amd64", force=False, on_progress=None
            )
            self.assertEqual(bin_path, self.cfg.wgcf_bin)
            self.assertEqual(seen_calls[-1], (self.cfg.wgcf_bin, "--help"))

        # 2. Fresh-download self-check also uses --help
        seen_calls.clear()

        async def fake_dl(session, url, dest):
            with open(dest, "wb") as fh:
                fh.write(b"\x7fELFdummy")
            return 9

        with patch.object(warp_installer, "_resolve_release_asset", new=AsyncMock(return_value=("https://example/wgcf", "v2.3.1"))), \
             patch.object(warp_installer, "_download_to_path", side_effect=fake_dl), \
             patch.object(warp_installer, "run_exec", side_effect=fake_exec):
            bin_path2 = await warp_installer._ensure_wgcf_binary(
                MagicMock(), self.cfg, "amd64", force=True, on_progress=None
            )
            self.assertEqual(bin_path2, self.cfg.wgcf_bin)
            self.assertEqual(seen_calls[-1], (self.cfg.wgcf_bin, "--help"))

        # 3. _register_warp_account cleans partial wgcf-account.toml between register retries
        #    and retries transient wgcf generate failures
        reg_attempts = 0
        gen_attempts = 0

        async def fake_reg_gen(*args, **kwargs):
            nonlocal reg_attempts, gen_attempts
            if "register" in args:
                self.assertIn("--accept-tos", args)
                reg_attempts += 1
                if reg_attempts == 1:
                    # Simulate partial file left behind on failed first register attempt
                    with open(self.cfg.wgcf_account_path, "w", encoding="utf-8") as fh:
                        fh.write("partial = true\n")
                    return (1, "", "HTTP 429")
                # Must have been cleaned before retry #2
                self.assertFalse(os.path.exists(self.cfg.wgcf_account_path))
                with open(self.cfg.wgcf_account_path, "w", encoding="utf-8") as fh:
                    fh.write("device_id = 'ok'\n")
                return (0, "ok", "")
            if "generate" in args:
                gen_attempts += 1
                if gen_attempts == 1:
                    return (1, "", "transient 502")
                with open(self.cfg.wgcf_profile_path, "w", encoding="utf-8") as fh:
                    fh.write(SAMPLE_WGCF_PROFILE)
                return (0, "ok", "")
            raise AssertionError(f"Unexpected command: {args}")

        with patch.object(warp_installer, "run_exec", side_effect=fake_reg_gen), \
             patch.object(warp_installer.asyncio, "sleep", new=AsyncMock()):
            await warp_installer._register_warp_account(
                self.cfg, self.cfg.wgcf_bin, force=True, on_progress=None
            )
            self.assertEqual(reg_attempts, 2)
            self.assertEqual(gen_attempts, 2)
            self.assertTrue(os.path.isfile(self.cfg.wgcf_account_path))
            self.assertTrue(os.path.isfile(self.cfg.wgcf_profile_path))


if __name__ == "__main__":
    unittest.main()
