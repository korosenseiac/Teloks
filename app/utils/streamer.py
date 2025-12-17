import asyncio
import math
import random
from pyrogram import Client
from pyrogram.file_id import FileId, PHOTO_TYPES
from pyrogram.raw.functions.upload import GetFile, SaveFilePart, SaveBigFilePart
from pyrogram.raw.types import InputFileLocation, InputFile, InputFileBig, InputDocumentFileLocation, InputPhotoFileLocation

class MediaStreamer:
    """
    A custom file-like object that bridges the gap between 
    Pyrogram's download stream and upload stream.
    """
    def __init__(self, client: Client, message, file_size: int):
        self.client = client
        self.message = message
        self.file_size = file_size
        self.queue = asyncio.Queue(maxsize=5) # Buffer of 5 chunks to save RAM
        self.current_offset = 0
        self.download_task = None
        self.is_downloading = True
        # Get media object from any media type
        media_obj = (message.document or message.video or message.audio or 
                     message.photo or message.voice or message.video_note or 
                     message.animation or message.sticker)
        self.name = getattr(media_obj, "file_name", None) or "unknown_file"
        self.chunk_size = 512 * 1024 # 512KB chunks for better stability

    async def start_download(self):
        """Starts the download process in a background task."""
        self.download_task = asyncio.create_task(self._downloader())

    async def _downloader(self):
        """Downloads chunks from Telegram and puts them into the queue."""
        try:
            # Get media from any media type
            media = (self.message.document or self.message.video or self.message.audio or 
                     self.message.photo or self.message.voice or self.message.video_note or
                     self.message.animation or self.message.sticker)
            if not media:
                return

            # Decode FileId to get location
            file_id_obj = FileId.decode(media.file_id)
            
            if file_id_obj.file_type in PHOTO_TYPES:
                location = InputPhotoFileLocation(
                    id=file_id_obj.media_id,
                    access_hash=file_id_obj.access_hash,
                    file_reference=file_id_obj.file_reference,
                    thumb_size=file_id_obj.thumbnail_size
                )
            else:
                location = InputDocumentFileLocation(
                    id=file_id_obj.media_id,
                    access_hash=file_id_obj.access_hash,
                    file_reference=file_id_obj.file_reference,
                    thumb_size=""
                )
            
            offset = 0
            
            while True:
                # Stop if we've reached the known file size
                if self.file_size and offset >= self.file_size:
                    break

                try:
                    # Invoke GetFile directly to stream chunks
                    response = await self.client.invoke(
                        GetFile(
                            location=location,
                            offset=offset,
                            limit=self.chunk_size
                        ),
                        retries=3,
                        timeout=60
                    )
                except Exception as e:
                    # Handle OFFSET_INVALID as EOF if we are at the end
                    if "OFFSET_INVALID" in str(e):
                        break
                    raise e
                
                chunk = response.bytes
                if not chunk:
                    break
                
                await self.queue.put(chunk)
                offset += len(chunk)
                
        except Exception as e:
            print(f"Download Error: {e}")
        finally:
            self.is_downloading = False
            await self.queue.put(None) # Sentinel to signal EOF

    async def read(self, size: int = -1):
        """
        Called by the Uploader. Returns bytes.
        Note: 'size' argument is often ignored by Pyrogram's uploader 
        if we just yield chunks, but we must implement the interface.
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

    # Required for file-like objects
    def tell(self):
        return self.current_offset

    def seek(self, offset, whence=0):
        pass # Streaming doesn't support seeking

async def upload_stream(client: Client, streamer: MediaStreamer, file_name: str):
    """
    Manually uploads a stream to Telegram using raw API calls.
    Returns an InputFile or InputFileBig.
    
    Note: For files > 10MB (big files), Telegram requires file_total_parts to be known upfront.
    We calculate this from the known file_size.
    """
    file_id = random.randint(0, 1000000000)
    part_count = 0
    file_size = streamer.file_size
    is_big = file_size > 10 * 1024 * 1024
    chunk_size = 512 * 1024  # 512KB - Telegram requirement
    
    # Calculate total parts from file_size (required for SaveBigFilePart)
    total_parts = math.ceil(file_size / chunk_size) if file_size > 0 else 1
    
    buffer = b""
    bytes_uploaded = 0

    while True:
        chunk = await streamer.read()
        if not chunk:
            # Upload remaining buffer if any (this is the last part, can be smaller)
            if buffer:
                if is_big:
                    await client.invoke(
                        SaveBigFilePart(
                            file_id=file_id,
                            file_part=part_count,
                            file_total_parts=total_parts,
                            bytes=buffer
                        ),
                        retries=3,
                        timeout=60
                    )
                else:
                    await client.invoke(
                        SaveFilePart(
                            file_id=file_id,
                            file_part=part_count,
                            bytes=buffer
                        ),
                        retries=3,
                        timeout=60
                    )
                bytes_uploaded += len(buffer)
                part_count += 1
            break
        
        buffer += chunk
        
        # Upload complete chunks of exactly chunk_size
        while len(buffer) >= chunk_size:
            part_data = buffer[:chunk_size]
            buffer = buffer[chunk_size:]
            
            if is_big:
                await client.invoke(
                    SaveBigFilePart(
                        file_id=file_id,
                        file_part=part_count,
                        file_total_parts=total_parts,
                        bytes=part_data
                    ),
                    retries=3,
                    timeout=60
                )
            else:
                await client.invoke(
                    SaveFilePart(
                        file_id=file_id,
                        file_part=part_count,
                        bytes=part_data
                    ),
                    retries=3,
                    timeout=60
                )
            bytes_uploaded += len(part_data)
            part_count += 1
    
    # Verify we uploaded the expected number of parts
    if part_count != total_parts:
        print(f"WARNING: Part count mismatch! Expected {total_parts}, got {part_count}. File size: {file_size}, uploaded: {bytes_uploaded}")

    if is_big:
        return InputFileBig(
            id=file_id,
            parts=part_count,
            name=file_name
        )
    else:
        return InputFile(
            id=file_id,
            parts=part_count,
            name=file_name,
            md5_checksum=""
        )
