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

Peer discovery is tuned for reliability:
  --enable-dht=true / --enable-peer-exchange=true / --bt-enable-lpd=true
  --dht-entry-point=<bootstrap node> × N   (explicit DHT bootstrap)
  --bt-tracker=<public tracker> × N         (fallback for bare magnet links)
  --bt-stop-timeout=0                        (never auto-abort a stalled torrent)
  --disable-ipv6=true                        (avoid IPv6 timeouts → "no peers")
"""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from app.config import (
    ARIA2_RPC_PORT,
    ARIA2_RPC_SECRET,
    TORRENT_DOWNLOAD_DIR,
    TORRENT_LISTEN_PORT,
    TORRENT_DISABLE_IPV6,
    TORRENT_TRACKERS,
)


# Public BitTorrent trackers used as a fallback for magnet links that carry no
# `tr=` parameters. Giving aria2 a bootstrap set of trackers dramatically
# improves peer discovery and torrent-metadata resolution. Override the whole
# list with the TORRENT_TRACKERS env var (comma-separated announce URLs).
_DEFAULT_BT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "https://opentracker.i2p.rocks:443/announce",
    "udp://open.stealth.si:80/announce",
    "https://tracker.gbitt.info/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.bittor.pw:1337/announce",
    "udp://tracker.cyberia.is:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://public.tracker.volemon.me:6969/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "udp://tracker1.bt.moack.co.kr:80/announce",
    "udp://tracker.birkenfeld.one:6969/announce",
]


def _resolve_bt_trackers() -> List[str]:
    """Return the fallback tracker list (env override wins)."""
    if TORRENT_TRACKERS:
        return [t.strip() for t in TORRENT_TRACKERS.split(",") if t.strip()]
    return list(_DEFAULT_BT_TRACKERS)


BT_TRACKERS = _resolve_bt_trackers()

# Well-known DHT bootstrap nodes. `--enable-dht=true` alone relies on aria2's
# bundled (and often stale) bootstrap nodes, which makes DHT slow to come
# online. Explicit entry points let the DHT routing table populate within
# seconds, which is the main peer source for bare magnet links.
DHT_ENTRY_POINTS = [
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
]


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
            # Peer discovery: DHT + peer exchange + local peer discovery.
            # A single FIXED port is used for both BitTorrent (TCP, incoming
            # peer connections) and DHT (UDP). The classic 6881-6999 range is
            # avoided because ISPs commonly throttle it and the random
            # per-start port would make firewall rules impossible to maintain.
            "--enable-dht=true",
            f"--listen-port={TORRENT_LISTEN_PORT}",
            f"--dht-listen-port={TORRENT_LISTEN_PORT}",
            "--enable-peer-exchange=true",
            "--bt-enable-lpd=true",
            "--bt-save-metadata=true",   # cache magnet metadata → faster re-adds
            "--bt-max-peers=0",          # 0 = unlimited peer connections
            "--bt-tracker-connect-timeout=60",
            "--bt-tracker-timeout=60",
            "--bt-stop-timeout=0",       # never auto-abort a stalled torrent
            "--summary-interval=0",      # no periodic console output
            "--auto-save-interval=0",
            "--console-log-level=warn",
            f"--dir={download_dir}",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--check-integrity=false",
        ]
        # Disable IPv6 unless the host has working IPv6 (default true). On an
        # IPv4-only VPS, aria2 burns time on AAAA lookups / IPv6 connection
        # timeouts before falling back — which presents as "no peers" even when
        # the swarm is full.
        cmd.append(f"--disable-ipv6={'true' if TORRENT_DISABLE_IPV6 else 'false'}")
        # Explicit DHT bootstrap nodes so the DHT table populates quickly.
        for entry in DHT_ENTRY_POINTS:
            cmd.append(f"--dht-entry-point={entry}")
        # Fallback public trackers — a single comma-separated value registers
        # every tracker on all aria2 builds (safe, unambiguous).
        if BT_TRACKERS:
            cmd.append("--bt-tracker=" + ",".join(BT_TRACKERS))

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

    async def get_peers(self, gid: str) -> List[Dict[str, Any]]:
        """Return the list of peers connected to a download (diagnostics)."""
        return await self._rpc("aria2.getPeers", [gid])

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
        stall_timeout: float = 600.0,   # abort after N seconds with zero activity
        total_timeout: float = 7200.0,  # abort after N seconds overall
    ) -> Tuple[Dict[str, Any], str]:
        """Poll until download completes, errors, or is cancelled.

        Magnet links and `.torrent` URLs make aria2 spawn a "followed-by"
        download once the metadata (or the .torrent file) has been fetched.
        This method transparently follows that chain and reports the status of
        the GID that actually finished the download.

        Parameters
        ----------
        on_progress : callable(completed: int, total: int, speed: int) | None
            Called each poll with bytes completed, total, and download speed.
        cancel_check : callable() -> bool | None
            If returns True, the download is force-cancelled.
        stall_timeout : float
            Abort with an error after this many seconds with no activity
            (no bytes downloaded and no download speed). Prevents torrents
            that can't find peers/metadata from hanging forever.
        total_timeout : float
            Abort with an error after this many seconds overall. Set 0 to
            disable.

        Returns
        -------
        (final_status, final_gid)
            The final status dict from ``get_status()`` and the GID that
            actually completed (so callers can clean the correct download).
            The status dict also includes ``completedLength``/``totalLength``
            in bytes, ``errorCode``/``errorMessage``, and ``dir``.

        Raises
        ------
        Aria2Error  — on download error, cancellation, stall, or timeout.
        """
        active_gid = gid
        seen_gids: set = set()

        start_time = time.monotonic()
        last_activity = start_time
        last_completed = -1
        last_peer_log = start_time

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

            now = time.monotonic()

            # Overall timeout
            if total_timeout > 0 and now - start_time > total_timeout:
                await self.cancel(active_gid)
                mins = int((now - start_time) / 60)
                raise Aria2Error(f"Torrent timed out after {mins} min.")

            # Stall detection (no bytes downloaded and no speed for a while)
            if speed > 0 or completed != last_completed:
                last_activity = now
                last_completed = completed
            elif stall_timeout > 0 and now - last_activity > stall_timeout:
                await self.cancel(active_gid)
                mins = int(stall_timeout / 60)
                raise Aria2Error(
                    f"Torrent tersekat (tiada kemajuan selama {mins} minit). "
                    "Sila cuba torrent lain atau semak rangkaian."
                )
            elif now - last_activity > 30 and now - last_peer_log > 60:
                # Diagnostics: while stalled, log how many peers aria2 has
                # actually connected to, so "no peers" issues become visible
                # in the bot logs instead of a silent 10-minute wait.
                last_peer_log = now
                try:
                    peers = await self.get_peers(active_gid)
                    print(
                        f"[Torrent] WARNING: no progress for {int(now - last_activity)}s — "
                        f"connected peers: {len(peers)}"
                    )
                except Exception:
                    pass

            if on_progress and total > 0:
                on_progress(completed, total, speed)

            if state == "complete":
                # Magnet links first resolve metadata, then spawn a
                # "followed-by" GID for the actual data download. Keep
                # following until we reach a GID that completed on its own.
                followed = status.get("followedBy")
                if followed:
                    next_gid = followed[0]
                    if next_gid == active_gid or next_gid in seen_gids:
                        # Safety net: never loop forever on a follow cycle.
                        return status, active_gid
                    seen_gids.add(active_gid)
                    active_gid = next_gid
                    # Fresh activity clock for the new download
                    last_activity = now
                    last_completed = -1
                    continue
                return status, active_gid

            if state == "error":
                code = status.get("errorCode", "?")
                msg = status.get("errorMessage", "Unknown error")
                raise Aria2Error(f"aria2 error {code}: {msg}")

            if state == "removed":
                raise Aria2Error("Download was removed/cancelled")

            # state in ("active", "waiting", "paused") — keep polling
