"""
torrent/streamer.py — Queue-based file streamer for torrent downloads.

Reads a local file (downloaded by aria2c) through an ``asyncio.Queue``
so that ``upload_stream()`` can consume it with the same interface as
the MediaFire/TeraBox streamers.

Memory-safe: only ``maxsize × CHUNK_SIZE`` bytes are buffered at any
time (default: 4 × 1 MB = 4 MB max in the queue).
"""
from __future__ import annotations

import asyncio
import os
from typing import Callable, Optional


class TorrentFileStreamer:
    """
    Reads a local file through a bounded ``asyncio.Queue`` for
    upload to Telegram via ``upload_stream()``.

    Parameters
    ----------
    file_path : str
        Absolute path to the file on disk.
    file_name : str
        Display name passed through to ``upload_stream()``.
    on_read_chunk : callable(int) | None
        Progress callback invoked with byte count after each chunk
        is read from disk (acts as the "download" progress for the
        upload pipeline).
    """

    CHUNK_SIZE: int = 1024 * 1024  # 1 MB — same as MediaFire FileStreamer

    def __init__(
        self,
        file_path: str,
        file_name: str,
        on_read_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.file_path = file_path
        self.file_size: int = os.path.getsize(file_path)
        self.name = file_name
        self.on_read_chunk = on_read_chunk

        # Bounded queue: 4 × 1 MB = 4 MB max buffered in memory
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        self.current_offset: int = 0
        self.download_task: Optional[asyncio.Task] = None
        self.is_downloading: bool = True
        self.chunk_size: int = self.CHUNK_SIZE

    # ------------------------------------------------------------------ start

    async def start_download(self) -> None:
        """Launch the background reader task."""
        self.download_task = asyncio.create_task(self._reader())

    # ----------------------------------------------------------------- reader

    async def _reader(self) -> None:
        """Read the file in chunks and enqueue them one at a time.

        Reads via ``run_in_executor`` so that blocking disk I/O doesn't
        stall the event loop.  Memory stays bounded to
        ``queue.maxsize × CHUNK_SIZE``.
        """
        loop = asyncio.get_running_loop()
        try:
            fh = await loop.run_in_executor(
                None, lambda: open(self.file_path, "rb")
            )
            try:
                while True:
                    data = await loop.run_in_executor(
                        None, fh.read, self.CHUNK_SIZE
                    )
                    if not data:
                        break
                    await self.queue.put(data)
                    if self.on_read_chunk:
                        self.on_read_chunk(len(data))
            finally:
                await loop.run_in_executor(None, fh.close)
        except Exception as e:
            print(f"[TorrentFileStreamer] Read error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None)  # EOF sentinel

    # ------------------------------------------------------------------- read

    async def read(self, size: int = -1) -> bytes:
        """Return the next chunk of data, or ``b""`` at EOF."""
        if self.download_task is None:
            await self.start_download()

        if self.queue.empty() and not self.is_downloading:
            return b""

        chunk = await self.queue.get()
        if chunk is None:
            return b""

        self.current_offset += len(chunk)
        return chunk

    # ------------------------------------------------------------------- tell

    def tell(self) -> int:
        return self.current_offset

    # ------------------------------------------------------------------- seek

    def seek(self, offset: int, whence: int = 0) -> None:
        pass  # streaming — no arbitrary seeking

    # ------------------------------------------------------------------ close

    async def close(self) -> None:
        """Cancel the background reader task if it exists."""
        if self.download_task is not None and not self.download_task.done():
            self.download_task.cancel()
            try:
                await self.download_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[TorrentFileStreamer] close error: {e}")
        self.download_task = None
