"""
torrent/client.py — aria2c JSON-RPC wrapper.

Manages the aria2c subprocess and provides an async interface for
adding magnet URIs, .torrent files, and HTTP URLs to .torrent files.
Communicates over JSON-RPC via aiohttp.

Memory-safe: aria2c runs as a **separate process** so its memory is
isolated from the Python bot.  Key flags keep resource usage low:
  --seed-time=0          (no seeding after download)
  --file-allocation=none (no pre-allocation → instant start)
  --disk-cache=16M       (small RAM footprint)
  --max-concurrent-downloads=2
"""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
from typing import Any, Dict, List, Optional

import aiohttp

from app.config import (
    ARIA2_RPC_PORT,
    ARIA2_RPC_SECRET,
    TORRENT_DOWNLOAD_DIR,
)


class Aria2Error(Exception):
    """Raised when aria2c RPC returns an error."""


class Aria2Client:
    """Async wrapper around the aria2c JSON-RPC interface."""

    def __init__(self) -> None:
        self.port: int = ARIA2_RPC_PORT
        self.secret: str = ARIA2_RPC_SECRET
        self.rpc_url: str = f"http://127.0.0.1:{self.port}/jsonrpc"
        self._process: Optional[asyncio.subprocess.Process] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._req_id: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the aria2c daemon and wait until it answers RPC pings."""
        # Find aria2c binary
        aria2c_bin = shutil.which("aria2c")
        if aria2c_bin is None:
            raise FileNotFoundError(
                "aria2c not found. Install it with: apt install aria2 (Linux) "
                "or choco install aria2 (Windows)"
            )

        download_dir = TORRENT_DOWNLOAD_DIR
        os.makedirs(download_dir, exist_ok=True)

        cmd = [
            aria2c_bin,
            "--enable-rpc",
            f"--rpc-listen-port={self.port}",
            "--rpc-listen-all=false",
            "--seed-time=0",
            "--max-concurrent-downloads=2",
            "--max-overall-download-limit=0",
            "--file-allocation=none",
            "--disk-cache=16M",
            "--bt-stop-timeout=600",         # stop stalled torrents after 10 min
            "--bt-tracker-connect-timeout=10",
            "--bt-tracker-timeout=10",
            "--summary-interval=0",          # no periodic console output
            "--auto-save-interval=0",
            "--console-log-level=warn",
            f"--dir={download_dir}",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--check-integrity=false",
        ]

        if self.secret:
            cmd.append(f"--rpc-secret={self.secret}")

        # On Windows, use CREATE_NO_WINDOW to suppress consoles
        kwargs: dict = {}
        if sys.platform == "win32":
            kwargs["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            **kwargs,
        )

        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )

        # Wait for aria2c to become ready (up to 10 seconds)
        for _ in range(40):
            await asyncio.sleep(0.25)
            try:
                if await self.ping():
                    print(f"[Torrent] aria2c started (pid={self._process.pid}, port={self.port})")
                    return
            except Exception:
                pass

        raise RuntimeError("aria2c failed to start within 10 seconds")

    async def shutdown(self) -> None:
        """Gracefully stop aria2c and close the HTTP session."""
        try:
            await self._rpc("aria2.shutdown")
        except Exception:
            pass

        if self._process and self._process.returncode is None:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass

        if self._session and not self._session.closed:
            await self._session.close()

    async def ping(self) -> bool:
        """Return True if aria2c answers RPC."""
        result = await self._rpc("aria2.getVersion")
        return "version" in result

    # ------------------------------------------------------------------
    # RPC transport
    # ------------------------------------------------------------------

    async def _rpc(self, method: str, params: Optional[list] = None) -> Any:
        """Send a JSON-RPC 2.0 request to aria2c and return the result."""
        if self._session is None or self._session.closed:
            raise Aria2Error("HTTP session not initialised")

        self._req_id += 1
        rpc_params: list = []
        if self.secret:
            rpc_params.append(f"token:{self.secret}")
        if params:
            rpc_params.extend(params)

        payload = {
            "jsonrpc": "2.0",
            "id": str(self._req_id),
            "method": method,
            "params": rpc_params,
        }

        async with self._session.post(self.rpc_url, json=payload) as resp:
            data = await resp.json()

        if "error" in data:
            raise Aria2Error(data["error"].get("message", str(data["error"])))

        return data.get("result", {})

    # ------------------------------------------------------------------
    # Download methods
    # ------------------------------------------------------------------

    async def add_magnet(self, magnet_uri: str, download_dir: str) -> str:
        """Add a magnet URI. Returns the GID."""
        opts = {"dir": download_dir, "seed-time": "0"}
        result = await self._rpc("aria2.addUri", [[magnet_uri], opts])
        return result  # GID string

    async def add_torrent(self, torrent_path: str, download_dir: str) -> str:
        """Add a local .torrent file. Returns the GID."""
        import base64
        with open(torrent_path, "rb") as f:
            torrent_b64 = base64.b64encode(f.read()).decode("ascii")
        opts = {"dir": download_dir, "seed-time": "0"}
        result = await self._rpc("aria2.addTorrent", [torrent_b64, [], opts])
        return result

    async def add_torrent_url(self, url: str, download_dir: str) -> str:
        """Add an HTTP URL to a .torrent file. Returns the GID."""
        opts = {"dir": download_dir, "seed-time": "0"}
        result = await self._rpc("aria2.addUri", [[url], opts])
        return result

    async def get_status(self, gid: str) -> Dict[str, Any]:
        """Get download status for a GID.

        Returns dict with keys:
          status, totalLength, completedLength, downloadSpeed, files,
          bittorrent (name, etc.), errorCode, errorMessage, followedBy
        """
        keys = [
            "status", "totalLength", "completedLength", "downloadSpeed",
            "uploadSpeed", "files", "bittorrent", "errorCode", "errorMessage",
            "followedBy", "dir", "gid",
        ]
        return await self._rpc("aria2.tellStatus", [gid, keys])

    async def get_files(self, gid: str) -> List[Dict[str, Any]]:
        """Return list of files in the download."""
        return await self._rpc("aria2.getFiles", [gid])

    async def cancel(self, gid: str) -> None:
        """Force-remove (cancel) a download."""
        try:
            await self._rpc("aria2.forceRemove", [gid])
        except Aria2Error:
            pass  # already removed

    async def remove_result(self, gid: str) -> None:
        """Remove completed/error/removed download result."""
        try:
            await self._rpc("aria2.removeDownloadResult", [gid])
        except Aria2Error:
            pass

    # ------------------------------------------------------------------
    # High-level: wait for download completion
    # ------------------------------------------------------------------

    async def wait_for_download(
        self,
        gid: str,
        *,
        poll_interval: float = 2.0,
        on_progress: Optional[Any] = None,  # callable(completed, total, speed)
        cancel_check: Optional[Any] = None,  # callable() -> bool
    ) -> Dict[str, Any]:
        """Poll until download completes, errors, or is cancelled.

        Parameters
        ----------
        on_progress : callable(completed: int, total: int, speed: int) | None
            Called each poll with bytes completed, total, and download speed.
        cancel_check : callable() -> bool | None
            If returns True, the download is force-cancelled.

        Returns
        -------
        dict  — final status from ``get_status()``.

        Raises
        ------
        Aria2Error  — on download error or cancellation.
        """
        active_gid = gid

        while True:
            await asyncio.sleep(poll_interval)

            # Check cancellation
            if cancel_check and cancel_check():
                await self.cancel(active_gid)
                raise Aria2Error("Download cancelled by user")

            status = await self.get_status(active_gid)
            state = status.get("status", "")

            total = int(status.get("totalLength", 0))
            completed = int(status.get("completedLength", 0))
            speed = int(status.get("downloadSpeed", 0))

            if on_progress and total > 0:
                on_progress(completed, total, speed)

            if state == "complete":
                # Magnet links first resolve metadata, then spawn a
                # "followed-by" GID for the actual data download.
                followed = status.get("followedBy")
                if followed:
                    active_gid = followed[0]
                    continue
                return status

            if state == "error":
                code = status.get("errorCode", "?")
                msg = status.get("errorMessage", "Unknown error")
                raise Aria2Error(f"aria2 error {code}: {msg}")

            if state == "removed":
                raise Aria2Error("Download was removed/cancelled")

            # state in ("active", "waiting", "paused") — keep polling
