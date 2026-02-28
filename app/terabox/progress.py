"""
terabox/progress.py — Cute live-progress display for TeraBox downloads/uploads.

Tracks bytes flowing through the download (TeraBox → server) and upload
(server → Telegram) pipelines and periodically edits a Telegram status
message with a nice visual.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

from pyrogram.types import Message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _human_bytes(b: float) -> str:
    """Pretty-print byte count: 1.23 MB, 456 KB, etc."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}" if b >= 10 else f"{b:.2f} {unit}"
        b /= 1024
    return f"{b:.2f} TB"


def _human_speed(bps: float) -> str:
    """Pretty-print speed: 12.3 MB/s, etc."""
    return f"{_human_bytes(bps)}/s"


def _bar(fraction: float, length: int = 14) -> str:
    """
    Build a cute progress bar using block characters.

    Example:  ▓▓▓▓▓▓▓▓░░░░░░  57%
    """
    fraction = max(0.0, min(1.0, fraction))
    filled = round(fraction * length)
    empty = length - filled
    return "▓" * filled + "░" * empty


def _eta(remaining_bytes: float, speed: float) -> str:
    """Estimated time remaining as human string."""
    if speed <= 0:
        return "∞"
    secs = remaining_bytes / speed
    if secs < 60:
        return f"{int(secs)}s"
    elif secs < 3600:
        m, s = divmod(int(secs), 60)
        return f"{m}m {s}s"
    else:
        h, remainder = divmod(int(secs), 3600)
        m = remainder // 60
        return f"{h}h {m}m"


# ---------------------------------------------------------------------------
# ProgressTracker
# ---------------------------------------------------------------------------

class ProgressTracker:
    """
    Shared mutable state that the streamer/uploader write into,
    and a background task reads from to update the Telegram message.

    Usage
    -----
    >>> tracker = ProgressTracker(status_msg, file_name, file_size)
    >>> tracker.start()                       # spawns updater task
    >>> ...                                   # worker code calls
    >>> tracker.add_downloaded(len(chunk))     #   download side
    >>> tracker.add_uploaded(len(part))        #   upload side
    >>> await tracker.stop()                  # cancel updater & final edit
    """

    EDIT_INTERVAL: float = 2.5  # seconds between message edits (avoid flood)

    def __init__(
        self,
        status_msg: Message,
        file_name: str,
        file_size: int,
        file_index: int = 1,
        file_total: int = 1,
    ) -> None:
        self.status_msg = status_msg
        self.file_name = file_name
        self.file_size = file_size
        self.file_index = file_index
        self.file_total = file_total

        # Counters (written by workers from any coroutine — single-threaded asyncio is safe)
        self.downloaded: int = 0
        self.uploaded: int = 0

        # Speed tracking
        self._start_time: float = 0.0
        self._dl_samples: list[tuple[float, int]] = []
        self._ul_samples: list[tuple[float, int]] = []

        self._task: Optional[asyncio.Task] = None
        self._stopped = False

    # ------------------------------------------------------------------ API

    def add_downloaded(self, n: int) -> None:
        now = time.monotonic()
        self.downloaded += n
        self._dl_samples.append((now, self.downloaded))
        # Keep last 30 samples for a rolling average
        if len(self._dl_samples) > 30:
            self._dl_samples = self._dl_samples[-30:]

    def add_uploaded(self, n: int) -> None:
        now = time.monotonic()
        self.uploaded += n
        self._ul_samples.append((now, self.uploaded))
        if len(self._ul_samples) > 30:
            self._ul_samples = self._ul_samples[-30:]

    # ---------------------------------------------------------------- Speed

    @staticmethod
    def _rolling_speed(samples: list[tuple[float, int]], window: float = 8.0) -> float:
        """Compute rolling average speed (bytes/sec) over last *window* seconds."""
        if len(samples) < 2:
            return 0.0
        now = samples[-1][0]
        cutoff = now - window
        # Find the first sample within the window
        for i, (t, _) in enumerate(samples):
            if t >= cutoff:
                t0, b0 = samples[i][0], samples[i][1]
                t1, b1 = samples[-1][0], samples[-1][1]
                dt = t1 - t0
                if dt <= 0:
                    return 0.0
                return (b1 - b0) / dt
        return 0.0

    # ------------------------------------------------------------ Rendering

    def _render(self) -> str:
        """Build the cute progress message text."""
        dl_frac = self.downloaded / self.file_size if self.file_size else 0
        ul_frac = self.uploaded / self.file_size if self.file_size else 0

        dl_speed = self._rolling_speed(self._dl_samples)
        ul_speed = self._rolling_speed(self._ul_samples)

        dl_remaining = max(0, self.file_size - self.downloaded)
        ul_remaining = max(0, self.file_size - self.uploaded)

        # Truncate long file names
        display_name = self.file_name
        if len(display_name) > 30:
            display_name = display_name[:27] + "…"

        # Pick a cute icon based on progress
        if ul_frac >= 1.0:
            phase_icon = "✅"
        elif ul_frac > 0.6:
            phase_icon = "🚀"
        elif ul_frac > 0.3:
            phase_icon = "⚡"
        elif dl_frac > 0:
            phase_icon = "📡"
        else:
            phase_icon = "🌐"

        lines = [
            f"{phase_icon} **Fail {self.file_index}/{self.file_total}**",
            f"📄 `{display_name}`",
            f"📦 {_human_bytes(self.file_size)}",
            "",
            f"⬇️ Muat Turun  {_bar(dl_frac)}  {dl_frac*100:.0f}%",
            f"    {_human_bytes(self.downloaded)} • {_human_speed(dl_speed)} • ETA {_eta(dl_remaining, dl_speed)}",
            "",
            f"⬆️ Muat Naik   {_bar(ul_frac)}  {ul_frac*100:.0f}%",
            f"    {_human_bytes(self.uploaded)} • {_human_speed(ul_speed)} • ETA {_eta(ul_remaining, ul_speed)}",
        ]

        elapsed = time.monotonic() - self._start_time
        if elapsed >= 1:
            mins, secs = divmod(int(elapsed), 60)
            lines.append("")
            lines.append(f"⏱ Masa: {mins}m {secs}s")

        return "\n".join(lines)

    # -------------------------------------------------------- Background task

    def start(self) -> None:
        """Spawn the background message-updater."""
        self._start_time = time.monotonic()
        self._stopped = False
        self._task = asyncio.create_task(self._updater_loop())

    async def _updater_loop(self) -> None:
        """Periodically edit the status message."""
        while not self._stopped:
            await asyncio.sleep(self.EDIT_INTERVAL)
            if self._stopped:
                break
            try:
                text = self._render()
                await self.status_msg.edit(text)
            except Exception:
                pass  # FloodWait, message deleted, etc.

    async def stop(self, final_text: str | None = None) -> None:
        """Cancel the updater and optionally edit one last time."""
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if final_text:
            try:
                await self.status_msg.edit(final_text)
            except Exception:
                pass
