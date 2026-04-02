"""
direct/handler.py — Handler for direct HTTP/HTTPS link downloads.

Processes direct-link downloads to Telegram using the same streaming
and upload pipeline as other handlers (mediafire, terabox, torrent).
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import shutil
import tempfile
from typing import Dict, List, Optional, Tuple

from pyrogram import Client
from app.utils.message import safe_edit
from pyrogram.types import (
    Message,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
)
from pyrogram.raw.functions.messages import SendMedia
from pyrogram.raw.functions.upload import SaveFilePart
from pyrogram.raw.types import (
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputFile,
    UpdateShortSentMessage,
    UpdateShort,
    UpdateNewMessage,
    UpdateNewChannelMessage,
)
from pyrogram.errors import FloodWait

from app.config import BACKUP_GROUP_ID
from app.database.db import (
    save_backup_group_cache,
    get_backup_group_cache,
    get_user_session,
    get_user_profile,
    log_forward,
)
from app.bot.session_manager import manager
from app.utils.streamer import upload_stream
from app.utils.media import (
    classify as _classify,
    mime as _mime,
    ext as _ext,
    MAX_FILE_SIZE,
    MAX_FILE_SIZE_PREMIUM,
    VIDEO_EXTS,
    is_archive,
    is_media,
    MEDIA_EXTS,
)
from app.direct.client import DirectLinkClient
from app.direct.streamer import DirectLinkStreamer
from app.terabox.progress import ProgressTracker
from app.mediafire.streamer import FileStreamer
from app.mediafire.archive import (
    count_media_in_archive,
    iter_extract_media,
)

# ---------------------------------------------------------------------------
# Pattern matching for direct links
# ---------------------------------------------------------------------------

# Match HTTP(S) URLs that are not known services
# Excluded: mediafire.com, terabox.com, t.me, magnet, .torrent URLs
EXCLUSIVE_DOMAINS = {
    "mediafire.com",
    "1drv.ms",
    "mega.nz",
    "cloud.mail.ru",
    "terabox.com",
    "freeterabox.com",
    "1024terabox.com",
    "t.me",
    "telegram.me",
}

DIRECT_LINK_PATTERN = re.compile(
    r"https?://(?!(?:" + "|".join(re.escape(d) for d in EXCLUSIVE_DOMAINS) + r"))"
    r"[^\s<>\"']+",
    re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Tracking
# ---------------------------------------------------------------------------

_direct_client = DirectLinkClient()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


async def _get_backup_group_peer():
    """Get the backup group peer, caching if needed."""
    # Try cache first
    cache = await get_backup_group_cache()
    if cache and cache.get("group_id"):
        from pyrogram.raw.types import InputPeerChannel
        return InputPeerChannel(
            channel_id=int(str(cache["group_id"]).replace("-100", "")),
            access_hash=cache.get("access_hash", 0),
        )

    # Fallback to direct ID
    from pyrogram.raw.types import InputPeerChannel
    channel_id = int(str(BACKUP_GROUP_ID).replace("-100", ""))
    return InputPeerChannel(channel_id=channel_id, access_hash=0)


async def _generate_video_thumb(video_path: str, duration_sec: int = 0) -> Optional[bytes]:
    """Generate a thumbnail for a video file using ffmpeg.
    
    Seeks to 10% of video duration (or 1 second if duration unknown).
    """
    try:
        thumb_path = video_path + ".thumb.jpg"
        # If duration is known, seek to 10% of the video
        seek_time = max(1, int(duration_sec * 0.1)) if duration_sec > 0 else 1
        
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", str(seek_time),
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "2",
            "-vf", "scale='min(320,iw)':-2",
            thumb_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()

        if proc.returncode == 0 and os.path.exists(thumb_path):
            with open(thumb_path, "rb") as f:
                data = f.read()
            os.remove(thumb_path)
            if len(data) > 100:
                return data
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
    except Exception as e:
        print(f"[DirectLink] Error generating thumbnail: {e}")
    return None


def _resize_thumb_high_quality(raw: bytes, max_side: int = 320) -> bytes:
    """Resize thumbnail to max_side px on longest side, using high JPEG quality."""
    from io import BytesIO
    try:
        from PIL import Image
        img = Image.open(BytesIO(raw))
        # Convert RGBA/palette to RGB for JPEG
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        # Resize preserving aspect ratio
        img.thumbnail((max_side, max_side), Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=90)
        return buf.getvalue()
    except ImportError:
        print("[DirectLink] Pillow not installed, returning raw thumbnail")
        return raw
    except Exception as e:
        print(f"[DirectLink] Thumbnail resize error: {e}")
        return raw


async def _get_video_metadata(video_path: str) -> Dict[str, int]:
    """Get video duration, width, height using ffprobe."""
    meta = {"duration": 0, "width": 0, "height": 0}
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,duration",
            "-show_entries", "format=duration",
            "-of", "csv=p=0:s=,",
            video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0 and stdout:
            lines = stdout.decode().strip().splitlines()
            if lines:
                parts = lines[0].split(",")
                if len(parts) >= 2:
                    try:
                        meta["width"] = int(parts[0])
                    except (ValueError, TypeError):
                        pass
                    try:
                        meta["height"] = int(parts[1])
                    except (ValueError, TypeError):
                        pass
                    if len(parts) >= 3:
                        try:
                            meta["duration"] = int(float(parts[2]))
                        except (ValueError, TypeError):
                            pass
                if meta["duration"] == 0 and len(lines) > 1:
                    try:
                        meta["duration"] = int(float(lines[1].strip().rstrip(",")))
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"[DirectLink] Error getting video metadata: {e}")
    return meta


async def _upload_thumb_to_telegram(bot: Client, thumb_raw: bytes) -> Optional[InputFile]:
    """Upload thumbnail bytes and return InputFile."""
    try:
        thumb_file_id = random.randint(0, 2 ** 63 - 1)
        await bot.invoke(
            SaveFilePart(
                file_id=thumb_file_id,
                file_part=0,
                bytes=thumb_raw,
            )
        )
        return InputFile(
            id=thumb_file_id,
            parts=1,
            name="thumb.jpg",
            md5_checksum="",
        )
    except Exception as e:
        print(f"[DirectLink] Error uploading thumbnail: {e}")
    return None


async def _download_video_to_temp(
    client: DirectLinkClient,
    url: str,
    file_name: str,
    file_size: int,
    on_download_chunk: Optional[callable] = None,
) -> str:
    """
    Download video file to temp directory for metadata/thumbnail extraction.
    Fires on_download_chunk(n) after each chunk so the progress tracker updates
    during the actual network download.
    Returns path to the temp file.

    Uses producer-consumer pattern to avoid TCP buffer drain:
    - Producer: Downloads chunks into a queue (non-blocking)
    - Consumer: Writes chunks from queue to disk (parallel)
    """
    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"direct_link_{random.randint(100000, 999999)}_{file_name}"
    )
    loop = asyncio.get_running_loop()

    # Buffer queue: allows download to continue while disk I/O happens
    # 32 chunks * 1MB = 32MB max buffer - prevents TCP buffer drain
    write_queue: asyncio.Queue = asyncio.Queue(maxsize=32)
    write_error: dict = {"error": None}
    write_done = asyncio.Event()

    async def _disk_writer():
        """Consumer: writes chunks to disk from the queue."""
        try:
            while True:
                item = await write_queue.get()
                if item is None:  # Sentinel for EOF
                    break
                chunk, chunk_offset = item
                await loop.run_in_executor(None, _append_bytes, temp_file, chunk)
                write_queue.task_done()
        except Exception as e:
            write_error["error"] = e
        finally:
            write_done.set()

    retries = 3
    offset = 0

    while retries > 0:
        # Start the disk writer task
        writer_task = asyncio.create_task(_disk_writer())

        try:
            async for chunk in client.download_stream(url, start_offset=offset):
                # Check if writer encountered an error
                if write_error["error"]:
                    raise write_error["error"]

                # Put chunk in queue (will block if queue is full, providing back-pressure)
                await write_queue.put((chunk, offset))
                offset += len(chunk)
                if on_download_chunk:
                    on_download_chunk(len(chunk))

            # Signal EOF to writer and wait for it to finish
            await write_queue.put(None)
            await writer_task

            # Check for write errors
            if write_error["error"]:
                raise write_error["error"]

            # Successfully downloaded
            break

        except Exception as e:
            # Cancel writer task on error
            if not writer_task.done():
                await write_queue.put(None)
                try:
                    await asyncio.wait_for(writer_task, timeout=5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    writer_task.cancel()

            import aiohttp
            if isinstance(e, (aiohttp.ClientError, ValueError)):
                print(f"[DirectLink] Temp video download dropped (offset {offset}): {e}. Retries left: {retries - 1}")
                retries -= 1
                write_error["error"] = None  # Reset error for retry
                if retries > 0:
                    await asyncio.sleep(2)
                    continue
            print(f"[DirectLink] Unrecoverable temp video download error: {e}")
            raise e

    return temp_file


def _append_bytes(path: str, data: bytes) -> None:
    """Append bytes to file (blocking — call via executor)."""
    with open(path, "ab") as f:
        f.write(data)


async def _upload_file_to_backup(
    bot: Client,
    user_client: Client,
    backup_peer,
    streamer,
    file_name: str,
    file_size: int,
    tracker: Optional[ProgressTracker] = None,
    thumb_raw: Optional[bytes] = None,
    video_meta: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[int], bool]:
    """Upload file to backup group (or bot chat if large) and return (message_id, is_sent_to_bot)."""
    try:
        on_ul = tracker.add_uploaded if tracker else None
        
        # Use bot session (sends directly to backup group) for files <= 2GB.
        # Use user session (sends to bot chat, then forwards) for files > 2GB.
        if file_size <= 2 * 1024 * 1024 * 1024:
            upload_client = bot
        else:
            upload_client = user_client
        
        # If using user_client, send to the bot instead of the backup group (Option 2)
        is_sent_to_bot = (upload_client == user_client)
        upload_peer = bot.me.username if is_sent_to_bot else backup_peer
        
        input_file = await upload_stream(upload_client, streamer, file_name, on_upload_chunk=on_ul)
        kind = _classify(file_name)
        mime_type = _mime(file_name)

        if kind == "photo":
            media = InputMediaUploadedPhoto(file=input_file)
        elif kind == "video":
            vm = video_meta or {}
            duration = vm.get("duration", 0)
            width = vm.get("width", 0)
            height = vm.get("height", 0)

            # Upload thumbnail if available
            thumb_input_file = None
            if thumb_raw:
                thumb_input_file = await _upload_thumb_to_telegram(upload_client, thumb_raw)
                if thumb_input_file:
                    print(f"[DirectLink] Thumbnail uploaded for {file_name} ({len(thumb_raw)} bytes)")

            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=[
                    DocumentAttributeVideo(
                        duration=duration, w=width, h=height,
                        supports_streaming=True,
                    ),
                    DocumentAttributeFilename(file_name=file_name),
                ],
                thumb=thumb_input_file,
            )
        else:
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=[DocumentAttributeFilename(file_name=file_name)],
            )

        # Register interceptor future if sending to bot
        fut = None
        uid = None
        if is_sent_to_bot:
            from app.bot.main import pending_bot_uploads
            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            me = await user_client.get_me()
            uid = me.id
            if uid not in pending_bot_uploads:
                pending_bot_uploads[uid] = []
            pending_bot_uploads[uid].append((file_name, fut))

        # Send to target peer with FloodWait handling
        for attempt in range(1, 4):
            try:
                # Use resolve_peer ONLY if upload_peer is a string or int (username, chat_id).
                # If it's already an InputPeer (from backup group), use it directly.
                target_peer = (
                    await upload_client.resolve_peer(upload_peer)
                    if isinstance(upload_peer, (str, int))
                    else upload_peer
                )
                
                updates = await upload_client.invoke(
                    SendMedia(
                        peer=target_peer,
                        media=media,
                        message="",
                        random_id=random.randint(0, 2 ** 63 - 1),
                    )
                )
                break
            except FloodWait as fw:
                wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
                print(f"[DirectLink] FloodWait {wait}s (attempt {attempt}/3)")
                await asyncio.sleep(wait + 1)
        else:
            print(f"[DirectLink] SendMedia failed after retries: {file_name}")
            return None

        # Extract message_id
        msg_id = _extract_msg_id(updates)
        
        if is_sent_to_bot:
            try:
                bot_msg = await asyncio.wait_for(fut, timeout=120)
                backup_msg_id = None
                try:
                    fw_msg = await bot.forward_messages(
                        chat_id=BACKUP_GROUP_ID,
                        from_chat_id=bot_msg.chat.id,
                        message_ids=bot_msg.id
                    )
                    if fw_msg:
                        backup_msg_id = fw_msg.id
                except Exception as e:
                    print(f"[DirectLink] Failed to forward bot message {bot_msg.id}: {e}")
                
                if backup_msg_id:
                    return backup_msg_id, True
                else:
                    print("[DirectLink] Failed to forward message to backup group")
                    return None, True
            except asyncio.TimeoutError:
                print("[DirectLink] Timeout waiting for bot to receive the message")
                if uid in pending_bot_uploads:
                    pending_bot_uploads[uid] = [p for p in pending_bot_uploads[uid] if p[1] != fut]
                return None, True

        if msg_id:
            return msg_id, False

        # Fallback for BACKUP_GROUP_ID directly
        print(f"[DirectLink] WARNING: Could not extract msg_id, searching recent messages")
        try:
            recent = await bot.get_messages(BACKUP_GROUP_ID, list(range(-1, -4, -1)))
            if not isinstance(recent, list):
                recent = [recent]
            for msg in recent:
                if msg and not getattr(msg, "empty", False):
                    fname = getattr(msg.document or msg.video, "file_name", None)
                    if fname == file_name:
                        return msg.id, False
        except Exception as e:
            print(f"[DirectLink] Fallback search failed: {e}")

        return None, False
    except Exception as e:
        print(f"[DirectLink] Upload error: {e}")
        import traceback
        traceback.print_exc()
        return None, False


def _extract_msg_id(updates) -> Optional[int]:
    """Extract message_id from Telegram response."""
    if isinstance(updates, UpdateShortSentMessage):
        return updates.id
    if isinstance(updates, UpdateShort):
        upd = updates.update
        if isinstance(upd, (UpdateNewMessage, UpdateNewChannelMessage)):
            return upd.message.id
    if hasattr(updates, "updates"):
        for upd in updates.updates:
            if isinstance(upd, (UpdateNewMessage, UpdateNewChannelMessage)):
                return upd.message.id
    return None


async def _safe_send(coro_factory, retries: int = 3):
    """Call coro_factory() with FloodWait handling."""
    for attempt in range(1, retries + 1):
        try:
            return await coro_factory()
        except FloodWait as fw:
            wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
            if wait > 300:
                print(f"[DirectLink] FloodWait {wait}s too long, skipping...")
                return None
            print(f"[DirectLink] FloodWait {wait}s (attempt {attempt}/{retries})")
            await asyncio.sleep(wait + 1)
        except Exception as e:
            print(f"[DirectLink] Error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(2)
    return None


async def _send_album_to_user(
    bot: Client,
    user_id: int,
    items: List[Tuple[int, str, str, int]],
    delivered_mids: set,
) -> None:
    """
    Send *items* ``(backup_msg_id, kind, name, size)`` to the user as
    media-group albums (max 8 per album). Tracks delivered message IDs
    in *delivered_mids*.
    """
    if not items:
        return

    CHUNK = 8

    async def _send_single(mid: int) -> bool:
        actual_from_id = await get_backup_group_actual_id()
        r = await _safe_send(
            lambda _mid=mid: bot.copy_message(
                chat_id=user_id,
                from_chat_id=actual_from_id,
                message_id=_mid,
            )
        )
        if r:
            delivered_mids.add(mid)
            return True

            from app.bot.session_manager import manager as local_manager
            uc = await local_manager.get_client(user_id)
            if uc:
                try:
                    me = await bot.get_me()
                    await uc.forward_messages(
                        chat_id=me.username,
                        from_chat_id=actual_from_id,
                        message_ids=mid
                    )
                    delivered_mids.add(mid)
                    return True
                except Exception as e:
                    print(f"[Fallback] user_client failed: {e}")
            return False

    for i in range(0, len(items), CHUNK):
        chunk = items[i : i + CHUNK]
        chunk_mids = [mid for mid, *_ in chunk]

        actual_group_id = await get_backup_group_actual_id()
        backup_msgs = await _safe_send(
            lambda _ids=chunk_mids: bot.get_messages(actual_group_id, _ids)
        )
        if backup_msgs is None:
            for mid, kind, name, size in chunk:
                await _send_single(mid)
                await asyncio.sleep(0.5)
            continue

        if not isinstance(backup_msgs, list):
            backup_msgs = [backup_msgs]

        media_list = []
        valid_mids = []
        for msg in backup_msgs:
            if not msg or getattr(msg, "empty", False):
                continue
            if msg.photo:
                media_list.append(InputMediaPhoto(msg.photo.file_id))
                valid_mids.append(msg.id)
            elif msg.video:
                media_list.append(InputMediaVideo(msg.video.file_id))
                valid_mids.append(msg.id)
            elif msg.document:
                media_list.append(InputMediaDocument(msg.document.file_id))
                valid_mids.append(msg.id)

        if not media_list:
            for mid, kind, name, size in chunk:
                await _send_single(mid)
                await asyncio.sleep(0.5)
            continue

        if len(media_list) == 1:
            await _send_single(valid_mids[0])
        else:
            r = await _safe_send(
                lambda _ml=media_list: bot.send_media_group(user_id, _ml)
            )
            if r:
                actual = len(r) if isinstance(r, list) else 0
                if actual == len(media_list):
                    delivered_mids.update(valid_mids)
                else:
                    print(f"[DirectLink] Album partial: sent {actual}/{len(media_list)}")
                    for vm in valid_mids[:actual]:
                        delivered_mids.add(vm)
            else:
                  fallback_album = False
                  try:
                      from app.bot.session_manager import manager as local_manager
                      from app.bot.main import get_backup_group_actual_id
                      uc = await local_manager.get_client(user_id)
                      if uc:
                          actual_from_id = await get_backup_group_actual_id()
                          me = await bot.get_me() if "bot" in locals() else await client.get_me()
                          await uc.forward_messages(me.username, actual_from_id, valid_mids)
                          fallback_album = True
                          for vm in valid_mids:
                              delivered_mids.add(vm)
                  except Exception as e:
                      pass
                      
                  if not fallback_album:
                      print("[DirectLink] Album send failed, falling back to individual sends")
                      for mid in valid_mids:
                          await _send_single(mid)
                          await asyncio.sleep(0.5)
        await asyncio.sleep(1.5)


async def _deliver_to_user_multi(
    bot: Client,
    user_id: int,
    uploaded: List[Tuple[int, str, str, int]],
    status_msg: Message,
) -> None:
    """
    Deliver all uploaded backup-group files to the user.
    Photos are sent as album(s) first, then videos as album(s).
    Includes a safety-net pass for any missed deliveries.
    """
    await safe_edit(status_msg, "⬆️ Menghantar ke anda…")

    delivered_mids: set = set()

    async def _send_single(mid: int) -> bool:
        r = await _safe_send(
            lambda _mid=mid: bot.copy_message(
                chat_id=user_id,
                from_chat_id=BACKUP_GROUP_ID,
                message_id=_mid,
            )
        )
        if r:
            delivered_mids.add(mid)
            return True
        return False

    photos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "photo"]
    videos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "video"]
    others = [(mid, k, n, s) for mid, k, n, s in uploaded
              if k not in ("photo", "video")]

    # Send: photos album first
    await _send_album_to_user(bot, user_id, photos, delivered_mids)

    # Send: videos album next
    await _send_album_to_user(bot, user_id, videos, delivered_mids)

    # Send: anything else individually
    for mid, k, n, s in others:
        await _send_single(mid)
        await asyncio.sleep(0.5)

    # Safety net — resend anything not confirmed delivered
    all_mids = {mid for mid, k, n, s in uploaded}
    missing_mids = all_mids - delivered_mids

    if missing_mids:
        print(
            f"[DirectLink] Safety net: {len(missing_mids)} file(s) not "
            "confirmed delivered, resending individually"
        )
        await asyncio.sleep(2)
        for mid in missing_mids:
            await _send_single(mid)
            await asyncio.sleep(1)

    total_to_send = len(uploaded)
    sent_count = len(delivered_mids)

    if sent_count < total_to_send:
        try:
            await safe_edit(status_msg, 
                f"⚠️ Selesai! {sent_count}/{total_to_send} fail berjaya dihantar."
            )
        except Exception:
            pass
    else:
        try:
            await status_msg.delete()
        except Exception:
            pass


async def _send_to_user(
    bot: Client,
    user_id: int,
    msg_id: int,
    is_sent_to_bot: bool,
) -> bool:
    """Deliver file to user and ensure it's in the backup group."""
    try:
        if is_sent_to_bot:
            # The file was uploaded by the user_client directly to the bot chat.
            # It's already fully visible to the user as if they sent it themselves.
            # `_upload_file_to_backup` has ALSO already forwarded it to the backup group.
            # We don't need to do any further forwarding here.
            return True
        else:
            # File is in backup group. Copy to user.
            r = await _safe_send(
                lambda: bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=BACKUP_GROUP_ID,
                    message_id=msg_id,
                ),
                retries=3,
            )
            return r is not None
    except Exception as e:
        print(f"[DirectLink] Failed to send to user: {e}")
        return False




# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def _download_archive_to_temp(
    direct_client: DirectLinkClient,
    url: str,
    filename: str,
    file_size: int,
    dest_dir: str,
    tracker: Optional[ProgressTracker] = None,
) -> str:
    """
    Stream-download the archive from Direct Link into *dest_dir*.
    Returns the path to the downloaded archive file.

    Uses producer-consumer pattern to avoid TCP buffer drain:
    - Producer: Downloads chunks into a queue (non-blocking)
    - Consumer: Writes chunks from queue to disk (parallel)
    """
    archive_path = os.path.join(dest_dir, filename)
    loop = asyncio.get_running_loop()

    # Buffer queue: allows download to continue while disk I/O happens
    # 32 chunks * 1MB = 32MB max buffer - prevents TCP buffer drain
    write_queue: asyncio.Queue = asyncio.Queue(maxsize=32)
    write_error: dict = {"error": None}

    async def _disk_writer():
        """Consumer: writes chunks to disk from the queue."""
        try:
            while True:
                item = await write_queue.get()
                if item is None:  # Sentinel for EOF
                    break
                chunk = item
                await loop.run_in_executor(None, _append_bytes, archive_path, chunk)
                write_queue.task_done()
        except Exception as e:
            write_error["error"] = e

    # Start the disk writer task
    writer_task = asyncio.create_task(_disk_writer())

    try:
        async for chunk in direct_client.download_stream(url):
            # Check if writer encountered an error
            if write_error["error"]:
                raise write_error["error"]

            # Put chunk in queue (will block if queue is full, providing back-pressure)
            await write_queue.put(chunk)
            if tracker:
                tracker.add_downloaded(len(chunk))

        # Signal EOF to writer and wait for it to finish
        await write_queue.put(None)
        await writer_task

        # Check for write errors
        if write_error["error"]:
            raise write_error["error"]

    except Exception as e:
        # Cancel writer task on error
        if not writer_task.done():
            await write_queue.put(None)
            try:
                await asyncio.wait_for(writer_task, timeout=5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                writer_task.cancel()
        raise e

    return archive_path


async def _handle_archive(
    bot: Client,
    direct_client: DirectLinkClient,
    backup_peer,
    message: Message,
    status_msg: Message,
    user_id: int,
    url: str,
    filename: str,
    file_size: int,
    skip_non_videos: bool = False,
    is_premium: bool = False,
) -> None:
    """Download archive, extract media ONE AT A TIME, upload each, send albums to user."""
    import gc
    import time as _time
    from app.terabox.progress import _human_bytes, _human_speed, _bar, _eta
    from app.bot.main import is_cancelled

    temp_dir = tempfile.mkdtemp(prefix="direct_archive_")
    size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE

    try:
        # ----- Phase 1: Download the archive -----
        await safe_edit(status_msg, 
            f"📥 Memuat turun arkib: `{filename}`\n"
            f"📦 Saiz: {_human_bytes(file_size)}"
        )

        dl_tracker = ProgressTracker(
            status_msg=status_msg,
            file_name=filename,
            file_size=file_size,
        )
        dl_tracker.start()

        archive_path = await _download_archive_to_temp(
            direct_client, url, filename, file_size, temp_dir,
            tracker=dl_tracker,
        )

        await dl_tracker.stop()

        # Check for cancellation after download
        if is_cancelled(user_id):
            await safe_edit(status_msg, "🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
            return

        # ----- Phase 2: Count media files (metadata only, no extraction) -----
        await safe_edit(status_msg, "📂 Mengimbas fail media dalam arkib…")

        loop = asyncio.get_running_loop()
        total_files = await loop.run_in_executor(
            None, count_media_in_archive, archive_path, skip_non_videos
        )

        if total_files == 0:
            msg = "❌ Tiada fail video dijumpai dalam arkib." if skip_non_videos else "❌ Tiada fail media (foto/video) dijumpai dalam arkib."
            await safe_edit(status_msg, msg)
            return

        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        await safe_edit(status_msg, 
            f"📤 Memuat naik {total_files} fail media ke Telegram…"
        )

        # ----- Phase 3: Extract & upload (photos parallel, videos sequential) -----
        uploaded: List[Tuple[int, str, str, int]] = []
        MAX_RETRIES = 3
        idx = 0

        # Batch settings for concurrent photo uploads
        _PHOTO_BATCH = 5
        _PHOTO_WORKERS = 3
        _photo_batch: List[Dict[str, Any]] = []

        _batch_state = {
            "done": 0, "downloaded": 0, "uploaded": 0,
            "_dl_samples": [], "_ul_samples": [],
            "batch_total_size": 0, "batch_count": 0,
        }
        _batch_stopped = {"v": False}
        _batch_start = _time.monotonic()

        def _batch_add_dl(n: int):
            now = _time.monotonic()
            _batch_state["downloaded"] += n
            _batch_state["_dl_samples"].append((now, _batch_state["downloaded"]))
            if len(_batch_state["_dl_samples"]) > 60:
                _batch_state["_dl_samples"] = _batch_state["_dl_samples"][-60:]

        def _batch_add_ul(n: int):
            now = _time.monotonic()
            _batch_state["uploaded"] += n
            _batch_state["_ul_samples"].append((now, _batch_state["uploaded"]))
            if len(_batch_state["_ul_samples"]) > 60:
                _batch_state["_ul_samples"] = _batch_state["_ul_samples"][-60:]

        def _rolling_speed(samples, window=8.0):
            if len(samples) < 2:
                return 0.0
            now = samples[-1][0]
            cutoff = now - window
            for i, (t, _) in enumerate(samples):
                if t >= cutoff:
                    t0, b0 = samples[i]
                    t1, b1 = samples[-1]
                    dt = t1 - t0
                    return (b1 - b0) / dt if dt > 0 else 0.0
            return 0.0

        async def _batch_updater_loop():
            # Milestone-based reporting for Direct batches
            _last_pct = -1
            while not _batch_stopped["v"]:
                await asyncio.sleep(10)
                if _batch_stopped["v"]:
                    break
                try:
                    done = _batch_state["done"]
                    total = _batch_state["batch_count"]
                    pct = int((done / total * 100) / 25) * 25 if total else 0
                    if pct > _last_pct:
                        _last_pct = pct
                        text = (
                            f"📷 **Proses Fail: {done}/{total}**\n"
                            f"📊 Progress: {pct}%"
                        )
                        await safe_edit(status_msg, text)
                except Exception:
                    pass

        _batch_updater_task = asyncio.create_task(_batch_updater_loop())

        async def _flush_photo_batch():
            """Upload accumulated photos concurrently then clean up."""
            if not _photo_batch:
                return
            batch = list(_photo_batch)
            _photo_batch.clear()

            _batch_state["batch_total_size"] += sum(e["size"] for e in batch)
            _batch_state["batch_count"] += len(batch)

            _sem = asyncio.Semaphore(_PHOTO_WORKERS)

            _ul_proxy = type("_Proxy", (), {
                "add_downloaded": staticmethod(lambda n: None),
                "add_uploaded": staticmethod(_batch_add_ul),
            })()

            async def _upload_one_photo(entry):
                async with _sem:
                    if is_cancelled(user_id):
                        return None

                    # Use size limit check for extracted photos
                    if entry["size"] > size_limit:
                        print(f"[DirectArchive] Skipping oversized photo: {entry['name']} ({_human_bytes(entry['size'])})")
                        return None

                    _bmid = None
                    for _att in range(1, MAX_RETRIES + 1):
                        streamer = FileStreamer(
                            entry["path"], entry["name"],
                            on_download_chunk=_batch_add_dl,
                        )
                        _bmid, _is_sent_to_bot = await _upload_file_to_backup(
                            bot, None, backup_peer, streamer,
                            entry["name"], entry["size"],
                            tracker=_ul_proxy,
                        )
                        if _bmid:
                            break
                        if _att < MAX_RETRIES:
                            await asyncio.sleep(1)

                    try:
                        os.remove(entry["path"])
                    except OSError:
                        pass

                    _batch_state["done"] += 1
                    await asyncio.sleep(0.3)
                    if _bmid:
                        return (_bmid, entry["kind"], entry["name"], entry["size"])
                    return None

            results = await asyncio.gather(
                *[_upload_one_photo(e) for e in batch],
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    print(f"[DirectArchive] Photo batch exception: {r}")
                    continue
                if r:
                    uploaded.append(r)
            gc.collect()

        async for mf in iter_extract_media(archive_path, extract_dir, skip_non_videos):
            idx += 1

            if is_cancelled(user_id):
                await safe_edit(status_msg, "🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
                return

            if mf["kind"] == "photo":
                _photo_batch.append(mf)
                if len(_photo_batch) >= _PHOTO_BATCH:
                    await _flush_photo_batch()
            else:
                await _flush_photo_batch()

                if mf["size"] > size_limit:
                    print(f"[DirectArchive] Skipping oversized file: {mf['name']} ({_human_bytes(mf['size'])})")
                    try:
                        os.remove(mf["path"])
                    except OSError:
                        pass
                    continue

                thumb_raw = None
                video_meta = None
                if mf["kind"] == "video":
                    thumb_raw = await _generate_video_thumb(mf["path"])
                    video_meta = await _get_video_metadata(mf["path"])

                tracker = ProgressTracker(
                    status_msg=status_msg,
                    file_name=mf["name"],
                    file_size=mf["size"],
                    file_index=idx,
                    file_total=total_files,
                )
                tracker.start()

                bmid = None
                for attempt in range(1, MAX_RETRIES + 1):
                    streamer = FileStreamer(
                        mf["path"], mf["name"],
                        on_download_chunk=tracker.add_downloaded,
                    )
                    bmid, _is_sent_to_bot = await _upload_file_to_backup(
                        bot, None, backup_peer, streamer,
                        mf["name"], mf["size"],
                        tracker=tracker,
                        thumb_raw=thumb_raw,
                        video_meta=video_meta,
                    )
                    if bmid:
                        break
                    if attempt < MAX_RETRIES:
                        tracker.downloaded = 0
                        tracker.uploaded = 0
                        tracker._dl_samples.clear()
                        tracker._ul_samples.clear()
                        await asyncio.sleep(3)

                await tracker.stop()

                if bmid:
                    uploaded.append((bmid, mf["kind"], mf["name"], mf["size"]))
                
                try:
                    os.remove(mf["path"])
                except OSError:
                    pass
                gc.collect()

                if idx < total_files:
                    await asyncio.sleep(2)

        await _flush_photo_batch()

        _batch_stopped["v"] = True
        _batch_updater_task.cancel()
        try:
            await _batch_updater_task
        except (asyncio.CancelledError, Exception):
            pass

        try:
            os.remove(archive_path)
        except OSError:
            pass

        if not uploaded:
            await safe_edit(status_msg, "❌ Semua fail gagal dimuat naik.")
            return

        await _deliver_to_user_multi(bot, user_id, uploaded, status_msg)

    finally:
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass
        gc.collect()


async def direct_link_handler(bot: Client, message: Message) -> None:
    """
    Handler for direct HTTP/HTTPS link downloads.
    
    Flow:
    1. Parse URL from message
    2. Resolve metadata (file size, filename)
    3. Validate size limits (based on premium status)
    4. Stream download from URL
    5. Upload to backup group
    6. Copy/send to user
    """
    # Avoid circular import by importing here
    from app.bot.main import (
        active_user_processes, reset_cancel, is_cancelled,
        get_backup_group_actual_id
    )
    
    user_id = message.from_user.id
    tracker: Optional[ProgressTracker] = None

    # Guard: check if user already has active process
    if active_user_processes.get(user_id):
        await message.reply_text(
            "⚠️ **Ada proses yang sedang berjalan!**\n\n"
            "Sila tunggu proses sebelumnya selesai sebelum menghantar link baru."
        )
        return

    # Check if user is logged in
    user_session = await get_user_session(user_id)
    if not user_session:
        await message.reply_text("❌ Belum login. Sila /start untuk login.")
        return

    user_client = await manager.get_client(user_id)
    if not user_client:
        await message.reply_text("❌ Sesi tidak sah. Sila login semula.")
        return

    # Check if user has completed profile setup
    user_profile = await get_user_profile(user_id)
    if not user_profile:
        await message.reply_text(
            "⚠️ **Profile belum lengkap!**\n\n"
            "Sila set profile anda terlebih dahulu.\n\n"
            "👇 **Pilih jantina anda:**"
        )
        return

    # Extract URL from message
    message_text = message.text or message.caption or ""
    match = DIRECT_LINK_PATTERN.search(message_text)
    if not match:
        await message.reply_text("❌ Tidak dapat mengesan URL yang sah.")
        return

    url = match.group(0).strip()

    # Mark user as active
    active_user_processes[user_id] = asyncio.current_task()
    reset_cancel(user_id)

    status_msg = await message.reply_text("🔄 Sedang Diproses..")

    try:
        # Resolve URL metadata
        await safe_edit(status_msg, "🔍 Mengekstrak metadata URL...")
        try:
            metadata = await _direct_client.resolve(url)
        except Exception as e:
            await safe_edit(status_msg, f"❌ Tidak dapat mengakses URL: {e}")
            return

        file_size = metadata.get("size", 0)
        file_name = metadata.get("filename", "unknown_file")
        final_url = metadata.get("url", url)

        # Check file size limit
        is_premium = getattr(message.from_user, "is_premium", False) or False
        size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE
        
        # [MODIFIED] Move size limit check for archives to extraction phase
        # Allow archives up to 10 GB during initial download
        is_arch = is_archive(file_name)
        hard_limit = 10 * 1024 * 1024 * 1024  # 10 GB
        
        effective_limit = hard_limit if is_arch else size_limit

        if file_size > effective_limit:
            limit_gb = effective_limit / (1024 * 1024 * 1024)
            actual_gb = file_size / (1024 * 1024 * 1024)
            msg = (
                f"❌ **Fail terlalu besar!**\n\n"
                f"Had Maksimum: {limit_gb:.1f} GB\n"
                f"Fail ini: {actual_gb:.2f} GB"
            )
            if is_arch:
                msg += "\n(Saiz arkib melebihi had sistem 10GB)"
            else:
                user_type = "Premium" if is_premium else "Biasa"
                msg += f"\n(User {user_type})"
                
            await safe_edit(status_msg, msg)
            return

        # Get backup group peer
        try:
            backup_peer = await _get_backup_group_peer()
        except Exception as e:
            await safe_edit(status_msg, f"❌ Gagal mendapat backup group: {e}")
            return

        # Check if file is an archive
        if is_archive(file_name):
            await _handle_archive(
                bot, _direct_client, backup_peer, message, status_msg,
                user_id, final_url, file_name, file_size,
                is_premium=is_premium
            )
            return

        # Create progress tracker
        tracker = ProgressTracker(status_msg, file_name, file_size)
        tracker.start()

        # Check if file is a video
        is_video = _ext(file_name) in VIDEO_EXTS
        thumb_raw = None
        video_meta: Optional[Dict[str, int]] = None
        temp_video_path = None

        # Load user-provided custom thumbnail if present
        # Prefer document (full quality) over photo (Telegram-compressed)
        custom_thumb_source = None
        if message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
            custom_thumb_source = "document"
        elif message.photo:
            custom_thumb_source = "photo"

        if custom_thumb_source:
            try:
                await safe_edit(status_msg, "🖼 Menyediakan gambar kecil (thumbnail)...")
                if custom_thumb_source == "document":
                    # Document = full quality, no Telegram compression
                    custom_thumb_path = await message.download()
                else:
                    # Photo = download the largest available size
                    custom_thumb_path = await bot.download_media(message.photo.file_id)
                if custom_thumb_path:
                    with open(custom_thumb_path, "rb") as f:
                        thumb_raw = f.read()
                    os.remove(custom_thumb_path)
                    # Resize to 320px with high JPEG quality
                    thumb_raw = _resize_thumb_high_quality(thumb_raw)
                    print(f"[DirectLink] Custom thumbnail loaded ({custom_thumb_source}): {len(thumb_raw)} bytes")
            except Exception as e:
                print(f"[DirectLink] Failed to download custom thumbnail: {e}")

        if is_video:
            # For videos: download to temp first (needed for ffprobe + thumbnail),
            # then upload from the local temp file.
            # Tracker is already started — we pass add_downloaded so the
            # download bar updates in real-time during the network fetch.
            try:
                temp_video_path = await _download_video_to_temp(
                    _direct_client, final_url, file_name, file_size,
                    on_download_chunk=tracker.add_downloaded,
                )

                # Extract metadata with ffprobe
                video_meta = await _get_video_metadata(temp_video_path)
                print(f"[DirectLink] Video metadata: {video_meta}")

                # Generate thumbnail at 10% of duration
                duration = video_meta.get("duration", 0)
                if not thumb_raw:
                    thumb_raw = await _generate_video_thumb(temp_video_path, duration)
                    if thumb_raw:
                        print(f"[DirectLink] Thumbnail generated: {len(thumb_raw)} bytes")
                else:
                    print(f"[DirectLink] Skipping thumbnail generation, using custom thumbnail.")

                # Upload from temp file — download is already fully counted,
                # so don't pass on_download_chunk to FileStreamer.
                streamer = FileStreamer(temp_video_path, file_name)
            except Exception as e:
                print(f"[DirectLink] Video processing error: {e}")
                await tracker.stop(f"❌ Ralat pemprosesan video: {e}")
                if temp_video_path and os.path.exists(temp_video_path):
                    os.remove(temp_video_path)
                return
        else:
            # For non-videos: stream directly from URL (no temp file needed)
            streamer = DirectLinkStreamer(
                _direct_client,
                final_url,
                file_size,
                file_name,
                on_download_chunk=tracker.add_downloaded,
            )

        # Upload to backup group with metadata and thumbnail
        msg_id, is_sent_to_bot = await _upload_file_to_backup(
            bot, user_client, backup_peer, streamer, file_name, file_size, 
            tracker=tracker,
            thumb_raw=thumb_raw,
            video_meta=video_meta
        )

        if hasattr(streamer, "close"):
            await streamer.close()

        # Clean up temp video file if created
        if temp_video_path and os.path.exists(temp_video_path):
            try:
                os.remove(temp_video_path)
            except Exception as e:
                print(f"[DirectLink] Cleanup error: {e}")

        if not msg_id:
            await tracker.stop("❌ Gagal memuat naik ke grup sandaran.")
            return

        # Send to user
        await tracker.stop("⬆️ Menghantar ke anda…")
        delivered = await _send_to_user(bot, user_id, msg_id, is_sent_to_bot)

        if delivered:
            # Log the upload
            try:
                channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                link = f"https://t.me/c/{channel_id_str}/{msg_id}" if not is_sent_to_bot else None
                await log_forward(
                    message.from_user.username or "Unknown",
                    msg_id,
                    file_size,
                    f"DirectLink/{file_name}",
                    link
                )
            except Exception as e:
                print(f"[DirectLink] Logging error: {e}")

            try:
                await status_msg.delete()
            except Exception:
                pass
        else:
            await safe_edit(status_msg, "⚠️ Selesai! Fail telah dimuat naik ke grup sandaran tetapi gagal dihantar.")

    except asyncio.CancelledError:
        if tracker:
            await tracker.stop("❌ Dibatalkan oleh pengguna.")
        else:
            await safe_edit(status_msg, "❌ Dibatalkan oleh pengguna.")
    except Exception as e:
        print(f"[DirectLink] Handler error: {e}")
        import traceback
        traceback.print_exc()
        if tracker:
            await tracker.stop(f"❌ Ralat: {e}")
        else:
            try:
                await safe_edit(status_msg, f"❌ Ralat: {e}")
            except Exception:
                pass
    finally:
        if tracker:
            await tracker.stop()
        active_user_processes.pop(user_id, None)
        await _direct_client.close()

