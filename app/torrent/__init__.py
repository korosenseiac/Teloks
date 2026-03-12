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
_init_lock = asyncio.Lock()


async def get_aria2_client() -> "Aria2Client":  # noqa: F821
    """Return (and lazily create) the singleton Aria2Client."""
    from app.torrent.client import Aria2Client

    global _client_instance

    async with _init_lock:
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
