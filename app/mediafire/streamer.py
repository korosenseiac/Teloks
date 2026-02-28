"""
mediafire/streamer.py — Queue-based streamers for the MediaFire pipeline.

Two classes:

* **MediaFireStreamer** — bridges an HTTP download from a MediaFire CDN URL
  into the ``upload_stream()`` pipeline via an ``asyncio.Queue``.  Interface
  is identical to ``TeraBoxMediaStreamer``.

* **FileStreamer** — reads a local file through the same queue interface so
  that files extracted from an archive can be fed into ``upload_stream()``
  without changing any upload logic.
"""
from __future__ import annotations

import asyncio
import os
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from app.mediafire.client import MediaFireClient


# ---------------------------------------------------------------------------
# MediaFireStreamer  (remote HTTP → Telegram upload)
# ---------------------------------------------------------------------------

class MediaFireStreamer:
    """
    Bridges MediaFire HTTP download ↔ Telegram upload via a bounded queue.

    Parameters
    ----------
    mf_client : MediaFireClient
        Client instance used to open the streaming download.
    direct_url : str
        CDN download URL obtained from ``MediaFireClient.resolve()``.
    file_size : int
        Known file size in bytes (needed by ``upload_stream``).
    file_name : str
        File name passed through to ``upload_stream``.
    on_download_chunk : callable(int) | None
        Progress callback invoked with byte count after each chunk.
    """

    CHUNK_SIZE: int = 1024 * 1024  # 1 MB — keep low for 2GB RAM VPS

    def __init__(
        self,
        mf_client: "MediaFireClient",
        direct_url: str,
        file_size: int,
        file_name: str,
        on_download_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.client = mf_client
        self.direct_url = direct_url
        self.file_size = file_size
        self.name = file_name
        self.on_download_chunk = on_download_chunk

        # Keep queue small to limit memory: 4 * 2MB = 8MB max buffered
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        self.current_offset: int = 0
        self.download_task: Optional[asyncio.Task] = None
        self.is_downloading: bool = True
        self.chunk_size: int = self.CHUNK_SIZE

    # ------------------------------------------------------------------ start

    async def start_download(self) -> None:
        """Launch the background downloader task."""
        self.download_task = asyncio.create_task(self._downloader())

    # ---------------------------------------------------------------- download

    async def _downloader(self) -> None:
        try:
            async for chunk in self.client.download_stream(
                self.direct_url, chunk_size=self.CHUNK_SIZE
            ):
                await self.queue.put(chunk)
                if self.on_download_chunk:
                    self.on_download_chunk(len(chunk))
        except Exception as e:
            print(f"[MediaFireStreamer] Download error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None)  # EOF sentinel

    # ------------------------------------------------------------------- read

    async def read(self, size: int = -1) -> bytes:
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


# ---------------------------------------------------------------------------
# FileStreamer  (local file → Telegram upload)
# ---------------------------------------------------------------------------

class FileStreamer:
    """
    Reads a local file through an ``asyncio.Queue`` so that
    ``upload_stream()`` can consume it with the same interface as any
    remote streamer.

    Parameters
    ----------
    file_path : str
        Absolute path to the local file.
    file_name : str
        Display name passed to ``upload_stream``.
    on_download_chunk : callable(int) | None
        Progress callback (called as chunks are read from disk).
    """

    CHUNK_SIZE: int = 1024 * 1024  # 1 MB — keep low for 2GB RAM VPS

    def __init__(
        self,
        file_path: str,
        file_name: str,
        on_download_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.file_path = file_path
        self.file_size = os.path.getsize(file_path)
        self.name = file_name
        self.on_download_chunk = on_download_chunk

        # Keep queue small to limit memory: 4 * 2MB = 8MB max buffered
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=4)
        self.current_offset: int = 0
        self.download_task: Optional[asyncio.Task] = None
        self.is_downloading: bool = True
        self.chunk_size: int = self.CHUNK_SIZE

    # ------------------------------------------------------------------ start

    async def start_download(self) -> None:
        self.download_task = asyncio.create_task(self._reader())

    # ----------------------------------------------------------------- reader

    async def _reader(self) -> None:
        """Read the file in chunks and enqueue them one at a time.

        IMPORTANT: We read one chunk at a time via run_in_executor
        instead of loading the whole file into a list. This keeps
        memory usage bounded to (queue_maxsize * CHUNK_SIZE).
        """
        loop = asyncio.get_running_loop()
        try:
            # Open file handle once, read chunks one at a time
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
                    if self.on_download_chunk:
                        self.on_download_chunk(len(data))
            finally:
                await loop.run_in_executor(None, fh.close)
        except Exception as e:
            print(f"[FileStreamer] Read error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None)

    # ------------------------------------------------------------------- read

    async def read(self, size: int = -1) -> bytes:
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
        pass
