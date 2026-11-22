"""Unit & integration tests for WARP bot commands, Command Guide, auth, and main.py lifecycle."""

from __future__ import annotations

import os
import sys

os.environ.setdefault("MONGO_URI", "mongodb://127.0.0.1:27017/telegram_forwarder_test")
os.environ.setdefault("API_ID", "12345")
os.environ.setdefault("API_HASH", "0123456789abcdef0123456789abcdef")
os.environ.setdefault("BOT_TOKEN", "123456:TEST_TOKEN_DO_NOT_USE")

import ast
import asyncio
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.bot import main as bot_main
from app.warp import WarpActionResult, WarpStatus, WarpTraceResult


def _sample_status(**overrides) -> WarpStatus:
    defaults = dict(
        installed=True,
        wireproxy_binary="/opt/telegram-forwarder-bot/warp/bin/wireproxy",
        config_exists=True,
        service_active=True,
        service_manager="systemd",
        socks5_listening=True,
        http_listening=True,
        ports_listening=True,
        proxy_enabled=False,
        socks5_url="socks5://127.0.0.1:40000",
        http_url="http://127.0.0.1:40001",
        warp_on=True,
        warp_state="on",
        exit_ip="104.28.1.42",
        location="SG",
        colo="SIN",
        platform="Linux (x86_64)",
        detail="Healthy (systemd); routing is OFF.",
        error=None,
    )
    defaults.update(overrides)
    return WarpStatus(**defaults)


def _extract_registered_commands_ast() -> set[str]:
    """Parse every Python file under app/ and extract all filters.command(...) command names."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    app_dir = os.path.join(root, "app")
    found: set[str] = set()

    for dirpath, _, filenames in os.walk(app_dir):
        for fname in filenames:
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            with open(path, "r", encoding="utf-8") as fh:
                tree = ast.parse(fh.read(), filename=path)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func = node.func
                    if (
                        isinstance(func, ast.Attribute)
                        and func.attr == "command"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "filters"
                        and node.args
                    ):
                        arg = node.args[0]
                        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                            found.add(arg.value)
                        elif isinstance(arg, (ast.List, ast.Tuple, ast.Set)):
                            for elt in arg.elts:
                                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                    found.add(elt.value)
    return found


def _make_message(user_id: int | None, text: str = "/warp", command: list[str] | None = None):
    msg = MagicMock()
    if user_id is None:
        msg.from_user = None
    else:
        msg.from_user = MagicMock(id=user_id, username=f"user_{user_id}")
    msg.text = text
    msg.command = command if command is not None else text.lstrip("/").split()
    reply_msg = MagicMock()
    reply_msg.edit_text = AsyncMock()
    reply_msg.edit = AsyncMock()
    msg.reply_text = AsyncMock(return_value=reply_msg)
    return msg, reply_msg


def _make_callback(user_id: int | None, data: str, with_message: bool = True):
    cb = MagicMock()
    cb.data = data
    cb.from_user = None if user_id is None else MagicMock(id=user_id)
    cb.answer = AsyncMock()
    if with_message:
        cb.message = MagicMock()
        cb.message.edit_text = AsyncMock()
        cb.message.edit = AsyncMock()
    else:
        cb.message = None
    return cb


class TestCommandGuideAndBotCommands(unittest.TestCase):
    def test_ast_commands_match_bot_commands_and_guide_1_to_1(self):
        registered = _extract_registered_commands_ast()
        self.assertGreaterEqual(len(registered), 15)

        # 1. BOT_COMMANDS exact 1:1 match (no missing, no hallucinated, no duplicates)
        bot_cmd_names = [c.command for c in bot_main.BOT_COMMANDS]
        self.assertEqual(len(bot_cmd_names), len(set(bot_cmd_names)), "Duplicate entries in BOT_COMMANDS")
        self.assertEqual(set(bot_cmd_names), registered)

        # Telegram BotCommand constraints
        for bc in bot_main.BOT_COMMANDS:
            self.assertRegex(bc.command, r"^[a-z0-9_]{1,32}$")
            self.assertTrue(3 <= len(bc.description) <= 256)

        # 2. Command Guide documents EVERY registered command and hallucinates none
        guide = bot_main.build_command_guide()
        self.assertLessEqual(len(guide), bot_main.TELEGRAM_MAX_MESSAGE_LENGTH)

        # Extract all backtick-quoted slash commands like `/foo` (ignore subcommand tokens after space)
        documented_tokens = set(re.findall(r"`/([a-z0-9_]+)(?:\s[^`]*)?`", guide))
        # /skip is explicitly documented as an inline modifier (SKIP_PATTERN), not filters.command
        self.assertIn("skip", documented_tokens)
        documented_slash_cmds = documented_tokens - {"skip"}
        self.assertEqual(
            documented_slash_cmds,
            registered,
            f"Guide vs registered mismatch: missing={registered - documented_slash_cmds}, extra={documented_slash_cmds - registered}",
        )
        # Ensure MediaFire folder claim is absent
        self.assertNotIn("folder MediaFire", guide)

    def test_message_splitting_and_truncation_4096(self):
        long_text = "\n\n".join(f"Section {i}: " + ("a" * 500) for i in range(15))
        chunks = bot_main.split_telegram_message(long_text, max_length=4096)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), 4096)
            self.assertGreater(len(chunk), 0)

        # Unbreakable long string without whitespace
        solid = "x" * 9000
        solid_chunks = bot_main.split_telegram_message(solid, max_length=4096)
        self.assertEqual("".join(solid_chunks), solid)
        self.assertTrue(all(len(c) <= 4096 for c in solid_chunks))

        # Truncation never exceeds 4096
        trunc = bot_main.truncate_telegram_text("y" * 5000, max_length=4096)
        self.assertLessEqual(len(trunc), 4096)
        self.assertTrue(trunc.endswith("(truncated)"))


class TestWarpBotHandlersAndAuth(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot_main._warp_install_in_progress = False

    def tearDown(self):
        bot_main._warp_install_in_progress = False

    async def test_fail_closed_owner_auth(self):
        # OWNER_ID == 0 must deny everyone (fail-closed)
        with patch.object(bot_main, "OWNER_ID", 0):
            self.assertFalse(bot_main.is_owner_user(0))
            self.assertFalse(bot_main.is_owner_user(999))
            self.assertFalse(bot_main.is_owner_user(None))

            msg, _ = _make_message(999, "/warp_install")
            with patch.object(bot_main, "install_warp", new=AsyncMock()) as mock_inst:
                await bot_main.warp_install_handler(MagicMock(), msg)
                mock_inst.assert_not_awaited()
                self.assertIn("OWNER_ID", msg.reply_text.call_args.args[0])

        # OWNER_ID > 0 allows only OWNER_ID, denies others and None
        with patch.object(bot_main, "OWNER_ID", 111222):
            self.assertTrue(bot_main.is_owner_user(111222))
            self.assertFalse(bot_main.is_owner_user(999999))
            self.assertFalse(bot_main.is_owner_user(None))

            for handler, cmd_text in (
                (bot_main.warp_command_handler, "/warp on"),
                (bot_main.warp_install_handler, "/warp_install"),
                (bot_main.warp_on_handler, "/warp_on"),
                (bot_main.warp_off_handler, "/warp_off"),
                (bot_main.warp_status_handler, "/warp_status"),
            ):
                msg, _ = _make_message(999999, cmd_text)
                with patch.object(bot_main, "enable_proxy", new=AsyncMock()) as m_on, \
                     patch.object(bot_main, "disable_proxy", new=AsyncMock()) as m_off, \
                     patch.object(bot_main, "install_warp", new=AsyncMock()) as m_inst, \
                     patch.object(bot_main, "get_warp_status", new=AsyncMock()) as m_st:
                    await handler(MagicMock(), msg)
                    m_on.assert_not_awaited()
                    m_off.assert_not_awaited()
                    m_inst.assert_not_awaited()
                    m_st.assert_not_awaited()
                    self.assertIn("Akses ditolak", msg.reply_text.call_args.args[0])

            # Unauthorized callback query denied with alert
            cb = _make_callback(999999, "warp_on")
            with patch.object(bot_main, "enable_proxy", new=AsyncMock()) as m_on:
                await bot_main.warp_callback_handler(MagicMock(), cb)
                m_on.assert_not_awaited()
                cb.answer.assert_awaited_once()
                self.assertTrue(cb.answer.call_args.kwargs.get("show_alert"))

    async def test_warp_install_live_progress_and_final_summary(self):
        with patch.object(bot_main, "OWNER_ID", 42):
            msg, status_msg = _make_message(42, "/warp_install", ["warp_install"])
            edits: list[str] = []

            async def fake_safe_edit(m, text, **kwargs):
                edits.append(text)

            async def fake_install(on_progress=None, force=False):
                self.assertFalse(force)
                await on_progress("Downloading wireproxy...")
                await on_progress("Registering WARP account...")
                await on_progress("Verifying Cloudflare trace...")
                return WarpActionResult(
                    ok=True,
                    enabled=False,
                    message="Installed",
                    socks5_url="socks5://127.0.0.1:40000",
                    http_url="http://127.0.0.1:40001",
                    status=_sample_status(),
                    trace=WarpTraceResult(ok=True, warp_on=True, warp_state="on", exit_ip="104.28.1.42", location="SG", colo="SIN"),
                )

            with patch.object(bot_main, "install_warp", side_effect=fake_install), \
                 patch.object(bot_main, "safe_edit", side_effect=fake_safe_edit):
                await bot_main.warp_install_handler(MagicMock(), msg)

            self.assertEqual(len(edits), 4)  # 3 progress updates + 1 final summary
            self.assertIn("Downloading wireproxy...", edits[0])
            self.assertIn("Verifying Cloudflare trace...", edits[2])
            self.assertIn("Berjaya Dipasang", edits[-1])
            self.assertIn("104.28.1.42", edits[-1])
            self.assertFalse(bot_main._warp_install_in_progress)

    async def test_warp_callback_double_click_race_and_none_msg(self):
        with patch.object(bot_main, "OWNER_ID", 42):
            # 1. msg is None handled gracefully
            cb_none = _make_callback(42, "warp_refresh", with_message=False)
            await bot_main.warp_callback_handler(MagicMock(), cb_none)
            cb_none.answer.assert_awaited_once()
            self.assertTrue(cb_none.answer.call_args.kwargs.get("show_alert"))

            # 2. Rapid double-click on warp_install shares same msg -> only 1 install, click #2 alerts without clobbering msg
            shared_msg = MagicMock()
            cb1 = _make_callback(42, "warp_install")
            cb2 = _make_callback(42, "warp_install")
            cb1.message = shared_msg
            cb2.message = shared_msg

            install_calls = 0
            edits: list[str] = []

            async def slow_install(on_progress=None, force=False):
                nonlocal install_calls
                install_calls += 1
                await asyncio.sleep(0.05)
                return WarpActionResult(
                    ok=True,
                    enabled=False,
                    message="OK",
                    status=_sample_status(),
                )

            async def fake_safe_edit(m, text, **kwargs):
                edits.append(text)

            with patch.object(bot_main, "install_warp", side_effect=slow_install), \
                 patch.object(bot_main, "safe_edit", side_effect=fake_safe_edit):
                await asyncio.gather(
                    bot_main.warp_callback_handler(MagicMock(), cb1),
                    bot_main.warp_callback_handler(MagicMock(), cb2),
                )

            self.assertEqual(install_calls, 1)
            self.assertTrue(cb2.answer.call_args.kwargs.get("show_alert"))
            self.assertFalse(any("sedang berjalan" in e for e in edits))
            self.assertFalse(bot_main._warp_install_in_progress)

    async def test_warp_on_off_status_and_aria2_sync(self):
        with patch.object(bot_main, "OWNER_ID", 42):
            # /warp_on syncs aria2 with http_url
            msg_on, _ = _make_message(42, "/warp_on")
            on_res = WarpActionResult(
                ok=True,
                enabled=True,
                message="ON",
                socks5_url="socks5://127.0.0.1:40000",
                http_url="http://127.0.0.1:40001",
                status=_sample_status(proxy_enabled=True),
            )
            with patch.object(bot_main, "enable_proxy", new=AsyncMock(return_value=on_res)), \
                 patch.object(bot_main, "sync_aria2_proxy", new=AsyncMock()) as mock_sync, \
                 patch.object(bot_main, "safe_edit", new=AsyncMock()) as mock_edit:
                await bot_main.warp_on_handler(MagicMock(), msg_on)
                mock_sync.assert_awaited_once_with("http://127.0.0.1:40001")
                self.assertIn("WARP Proxy: ON", mock_edit.call_args.args[1])

            # /warp_off syncs aria2 with None
            msg_off, _ = _make_message(42, "/warp_off")
            off_res = WarpActionResult(
                ok=True,
                enabled=False,
                message="OFF",
                status=_sample_status(proxy_enabled=False),
            )
            with patch.object(bot_main, "disable_proxy", new=AsyncMock(return_value=off_res)), \
                 patch.object(bot_main, "sync_aria2_proxy", new=AsyncMock()) as mock_sync2, \
                 patch.object(bot_main, "safe_edit", new=AsyncMock()) as mock_edit2:
                await bot_main.warp_off_handler(MagicMock(), msg_off)
                mock_sync2.assert_awaited_once_with(None)
                self.assertIn("WARP Proxy: OFF", mock_edit2.call_args.args[1])

            # /commands works for regular (non-owner) users
            msg_help, _ = _make_message(777, "/commands")
            await bot_main.commands_guide_handler(MagicMock(), msg_help)
            msg_help.reply_text.assert_awaited()
            self.assertIn("/warp_install", msg_help.reply_text.call_args.args[0])

    async def test_main_startup_and_shutdown_lifecycle(self):
        import main as root_main

        events: list[str] = []

        def _rec(name, ret=None):
            events.append(name)
            return ret

        fake_bot = MagicMock()
        fake_bot.start = AsyncMock(side_effect=lambda: _rec("bot_start"))
        fake_bot.stop = AsyncMock(side_effect=lambda: _rec("bot_stop"))

        with patch.object(root_main, "bot_app", fake_bot), \
             patch.object(root_main, "register_bot_commands", new=AsyncMock(side_effect=lambda c: _rec("reg_cmds", True))), \
             patch.object(root_main, "init_warp", new=AsyncMock(side_effect=lambda: _rec("init_warp", True))), \
             patch.object(root_main, "get_backup_group_peer", new=AsyncMock(return_value=None)), \
             patch.object(root_main, "idle", new=AsyncMock(side_effect=lambda: _rec("idle"))), \
             patch.object(root_main, "shutdown_aria2_client", new=AsyncMock(side_effect=lambda: _rec("shutdown_aria2"))), \
             patch.object(root_main, "shutdown_warp", new=AsyncMock(side_effect=lambda: _rec("shutdown_warp"))):
            await root_main.start_services()

        self.assertEqual(
            events,
            ["bot_start", "reg_cmds", "init_warp", "idle", "shutdown_aria2", "shutdown_warp", "bot_stop"],
        )


if __name__ == "__main__":
    unittest.main()
