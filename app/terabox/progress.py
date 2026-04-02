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

from app.utils.message import safe_edit


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
    Milestone-based progress tracker.
    Updates are sent only at major percentage changes (0%, 25%, 50%, 75%, 100%)
    to avoid triggering Telegram FloodWait.
    """

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

        self.downloaded: int = 0
        self.uploaded: int = 0

        self._start_time: float = 0.0
        self._dl_samples: list[tuple[float, int]] = []
        self._ul_samples: list[tuple[float, int]] = []

        # Tracks last reported percentage milestones
        self._last_dl_milestone = -1
        self._last_ul_milestone = -1
        self._stopped = False

    # ------------------------------------------------------------------ API

    def add_downloaded(self, n: int) -> None:
        if self._stopped: return
        now = time.monotonic()
        self.downloaded += n
        self._dl_samples.append((now, self.downloaded))
        if len(self._dl_samples) > 20:
            self._dl_samples = self._dl_samples[-20:]
        
        # Check for milestone (every 25%)
        pct = int((self.downloaded / self.file_size * 100) / 25) * 25 if self.file_size else 0
        if pct > self._last_dl_milestone:
            self._last_dl_milestone = pct
            asyncio.create_task(self._trigger_update())

    def add_uploaded(self, n: int) -> None:
        if self._stopped: return
        now = time.monotonic()
        self.uploaded += n
        self._ul_samples.append((now, self.uploaded))
        if len(self._ul_samples) > 20:
            self._ul_samples = self._ul_samples[-20:]

        pct = int((self.uploaded / self.file_size * 100) / 25) * 25 if self.file_size else 0
        if pct > self._last_ul_milestone:
            self._last_ul_milestone = pct
            asyncio.create_task(self._trigger_update())

    # ------------------------------------------------------------ Rendering

    async def _trigger_update(self) -> None:
        """Helper to fire-and-forget an edit to the status message."""
        try:
            text = self._render()
            await safe_edit(self.status_msg, text)
        except:
            pass

    @staticmethod
    def _rolling_speed(samples: list[tuple[float, int]], window: float = 10.0) -> float:
        if len(samples) < 2: return 0.0
        t0, b0 = samples[0]
        t1, b1 = samples[-1]
        dt = t1 - t0
        return (b1 - b0) / dt if dt > 0 else 0.0

    def _render(self) -> str:
        dl_frac = self.downloaded / self.file_size if self.file_size else 0
        ul_frac = self.uploaded / self.file_size if self.file_size else 0
        
        display_name = self.file_name
        if len(display_name) > 30:
            display_name = display_name[:27] + "…"

        lines = [
            f"**Fail {self.file_index}/{self.file_total}**",
            f"📄 `{display_name}`",
            f"📦 {_human_bytes(self.file_size)}",
            "",
            f"⬇️ Muat Turun: {int(dl_frac*100)}%",
            f"⬆️ Muat Naik: {int(ul_frac*100)}%",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------- Control

    def start(self) -> None:
        """Initialize start time."""
        self._start_time = time.monotonic()
        self._stopped = False

    async def stop(self, final_text: str | None = None) -> None:
        """Stop tracking and optionally send final text."""
        self._stopped = True
        if final_text:
            try:
                await safe_edit(self.status_msg, final_text)
            except:
