"""
TeraBox integration package.

Exposes a module-level singleton TeraBoxClient that is lazily initialised on
first access and automatically re-validated whenever the NDUS token expires.
"""
from __future__ import annotations

import asyncio
from typing import Optional

_client_instance: Optional["TeraBoxClient"] = None  # noqa: F821
_init_lock = asyncio.Lock()


async def get_terabox_client() -> "TeraBoxClient":  # noqa: F821
    """Return (and lazily create) the singleton TeraBoxClient."""
    from app.config import TERABOX_NDUS
    from app.terabox.client import TeraBoxClient

    global _client_instance

    async with _init_lock:
        if _client_instance is not None:
            # Quick liveness check — re-init if login fails
            try:
                ok = await _client_instance.check_login()
                if ok:
                    return _client_instance
            except Exception as e:
                print(f"[TB] singleton liveness check failed: {e}")
            print("[TB] singleton: token expired or invalid — re-initialising")
            # Token expired — discard old instance
            try:
                await _client_instance.close()
            except Exception:
                pass
            _client_instance = None

        if not TERABOX_NDUS:
            raise RuntimeError(
                "TERABOX_NDUS is not set in the environment. "
                "Add it to your .env file."
            )

        print(f"[TB] singleton: initialising new TeraBoxClient (ndus len={len(TERABOX_NDUS)})")
        client = TeraBoxClient(TERABOX_NDUS)
        await client.update_app_data()
        ok = await client.check_login()
        print(f"[TB] singleton: check_login result = {ok}")
        _client_instance = client
        return _client_instance
