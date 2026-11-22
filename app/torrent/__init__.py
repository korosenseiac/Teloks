"""
Torrent integration package.

Provides torrent/magnet link download support via aria2c (JSON-RPC).
Exposes a module-level singleton Aria2Client that is lazily initialised
on first access and manages the aria2c subprocess lifecycle.
"""
from __future__ import annotations

import asyncio
import glob
import os
import shutil
import tempfile
from typing import Optional

_client_instance: Optional["Aria2Client"] = None  # noqa: F821
_init_lock: Optional[asyncio.Lock] = None
_init_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_init_lock() -> asyncio.Lock:
    """Return an asyncio.Lock bound to the current running event loop."""
    global _init_lock, _init_lock_loop
    loop = asyncio.get_running_loop()
    if _init_lock is None or _init_lock_loop is not loop:
        _init_lock = asyncio.Lock()
        _init_lock_loop = loop
    return _init_lock


def get_existing_aria2_client() -> Optional["Aria2Client"]:  # noqa: F821
    """Return the existing Aria2Client singleton without spawning aria2c."""
    return _client_instance


async def get_aria2_client() -> "Aria2Client":  # noqa: F821
    """Return (and lazily create) the singleton Aria2Client."""
    from app.torrent.client import Aria2Client

    global _client_instance

    async with _get_init_lock():
        if _client_instance is not None:
            # Quick liveness check
            try:
                alive = await _client_instance.ping()
                if alive:
                    return _client_instance
            except Exception as e:
                print(f"[Torrent] singleton liveness check failed: {e}")

            # Dead — discard old instance
            try:
                await _client_instance.shutdown()
            except Exception:
                pass
            _client_instance = None

        print("[Torrent] singleton: initialising new Aria2Client")
        client = Aria2Client()
        await client.start()
        _client_instance = client
        return _client_instance


async def sync_aria2_proxy(proxy_url: Optional[str]) -> bool:
    """Update proxy settings on the running Aria2Client singleton, if alive.

    Does not spawn ``aria2c`` if it has not been started yet. Returns True if
    an active daemon was updated, False otherwise.
    """
    global _client_instance

    client = _client_instance
    if client is None:
        return False

    try:
        if not await client.ping():
            return False
        await client.set_proxy(proxy_url)
        return True
    except Exception as e:
        print(f"[Torrent] sync_aria2_proxy failed: {e}")
        return False


async def shutdown_aria2_client() -> None:
    """Gracefully shut down the singleton Aria2Client if one is running."""
    global _client_instance

    async with _get_init_lock():
        if _client_instance is not None:
            try:
                await _client_instance.shutdown()
            finally:
                _client_instance = None


def cleanup_orphaned_torrent_dirs() -> None:
    """Remove any leftover torrent_* temp dirs from previous crashes.

    Called once at bot startup (synchronous, before event-loop is busy).
    """
    tmp = tempfile.gettempdir()
    pattern = os.path.join(tmp, "torrent_*")
    for d in glob.glob(pattern):
        try:
            if os.path.isdir(d):
                shutil.rmtree(d, ignore_errors=True)
                print(f"[Torrent] cleaned orphaned dir: {d}")
        except Exception:
            pass
