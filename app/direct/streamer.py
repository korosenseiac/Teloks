"""
direct/streamer.py — Queue-based streamer for direct-link downloads.

Bridges HTTP downloads from direct links into the upload_stream() pipeline
via an asyncio.Queue, using the same interface as other streamers.
"""
from __future__ import annotations

import asyncio
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from app.direct.client import DirectLinkClient


# ---------------------------------------------------------------------------
# DirectLinkStreamer  (remote HTTP → Telegram upload)
# ---------------------------------------------------------------------------

class DirectLinkStreamer:
    """
    Bridges direct-link HTTP download ↔ Telegram upload via a bounded queue.

    Parameters
    ----------
    client : DirectLinkClient
        Client instance used to open the streaming download.
    url : str
        The HTTP/HTTPS URL to download from.
    file_size : int
        Known file size in bytes (needed by ``upload_stream``).
    file_name : str
        File name passed through to ``upload_stream``.
    on_download_chunk : callable(int) | None
        Progress callback invoked with byte count after each chunk.
    """

    CHUNK_SIZE: int = 1024 * 1024  # 1 MB

    def __init__(
        self,
        client: "DirectLinkClient",
        url: str,
        file_size: int,
        file_name: str,
        on_download_chunk: Optional[Callable[[int], None]] = None,
    ) -> None:
        self.client = client
        self.url = url
        self.file_size = file_size
        self.name = file_name
        self.on_download_chunk = on_download_chunk

        # Keep queue small to limit memory: 4 * 1MB = 4MB max buffered
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
        """Download the file in chunks and put them in the queue."""
        try:
            async for chunk in self.client.download_stream(
                self.url, chunk_size=self.CHUNK_SIZE
            ):
                await self.queue.put(chunk)
                if self.on_download_chunk:
                    self.on_download_chunk(len(chunk))
        except Exception as e:
            print(f"[DirectLink] Download error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None)  # EOF sentinel

    # ------------------------------------------------------------------- read

    async def read(self, size: int = -1) -> bytes:
        """
        Read the next chunk from the download stream.
        
        Parameters
        ----------
        size : int
            Ignored — always returns next available chunk or empty bytes.
        
        Returns
        -------
        bytes
            Next chunk of file data, or empty bytes when done.
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

    # ------------------------------------------------------------------- tell

    def tell(self) -> int:
        """Return current byte offset in the stream."""
        return self.current_offset

    # ------------------------------------------------------------------- seek

    def seek(self, offset: int, whence: int = 0) -> None:
        """No-op — streaming doesn't support seeking."""
        pass
