"""
TeraBoxMediaStreamer — drop-in replacement for MediaStreamer.

Streams a TeraBox dlink URL through an asyncio.Queue into Telegram's upload
pipeline (upload_stream in app/utils/streamer.py) without buffering the
whole file in memory.

The public interface (file_size, read(), tell(), seek()) is identical to
MediaStreamer so that upload_stream() can consume either class unchanged.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.terabox.client import TeraBoxClient


class TeraBoxMediaStreamer:
    """
    Bridges TeraBox HTTP download ↔ Telegram upload via a bounded queue.

    Parameters
    ----------
    terabox_client : TeraBoxClient
        Initialised client used to open the streaming download.
    dlink : str
        Direct download URL obtained from TeraBoxClient.download().
    file_size : int
        Known file size in bytes (required by upload_stream for big-file
        part calculation).
    file_name : str
        File name (used by upload_stream as the InputFile name).
    """

    CHUNK_SIZE: int = 512 * 1024  # 512 KB — same as MediaStreamer

    def __init__(
        self,
        terabox_client: "TeraBoxClient",
        dlink: str,
        file_size: int,
        file_name: str,
    ) -> None:
        self.client = terabox_client
        self.dlink = dlink
        self.file_size = file_size
        self.name = file_name

        self.queue: asyncio.Queue = asyncio.Queue(maxsize=5)
        self.current_offset: int = 0
        self.download_task: asyncio.Task | None = None
        self.is_downloading: bool = True
        # Expose chunk_size so callers can inspect it (mirrors MediaStreamer)
        self.chunk_size: int = self.CHUNK_SIZE

    # ----------------------------------------------------------------- start

    async def start_download(self) -> None:
        """Launch the background downloader task."""
        self.download_task = asyncio.create_task(self._downloader())

    # --------------------------------------------------------------- download

    async def _downloader(self) -> None:
        """
        Pull chunks from the TeraBox dlink and enqueue them.
        Sends a None sentinel when finished or on error.
        """
        try:
            async for chunk in self.client.download_file_stream(
                self.dlink, chunk_size=self.CHUNK_SIZE
            ):
                await self.queue.put(chunk)
        except Exception as e:
            print(f"[TeraBoxMediaStreamer] Download error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None)  # EOF sentinel

    # ------------------------------------------------------------------- read

    async def read(self, size: int = -1) -> bytes:
        """
        Return the next chunk from the queue.

        ``size`` is accepted for interface compatibility but ignored —
        upload_stream() works with whatever chunk size the streamer provides.
        """
        if self.download_task is None:
            await self.start_download()

        if self.queue.empty() and not self.is_downloading:
            return b""

        chunk = await self.queue.get()
        if chunk is None:
            return b""

        self.current_offset += len(chunk)
        return chunk

    # -------------------------------------------------------------------- tell

    def tell(self) -> int:
        """Return how many bytes have been consumed so far."""
        return self.current_offset

    # -------------------------------------------------------------------- seek

    def seek(self, offset: int, whence: int = 0) -> None:
        """No-op — streaming does not support arbitrary seeking."""
        pass
