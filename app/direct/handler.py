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
import tempfile
from typing import Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.types import Message
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
from app.utils.media import classify as _classify, mime as _mime, ext as _ext, MAX_FILE_SIZE, MAX_FILE_SIZE_PREMIUM, VIDEO_EXTS
from app.direct.client import DirectLinkClient
from app.direct.streamer import DirectLinkStreamer
from app.terabox.progress import ProgressTracker
from app.mediafire.streamer import FileStreamer

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
            "-q:v", "5",
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
    """
    temp_file = os.path.join(
        tempfile.gettempdir(),
        f"direct_link_{random.randint(100000, 999999)}_{file_name}"
    )
    loop = asyncio.get_running_loop()
    
    retries = 3
    offset = 0

    while retries > 0:
        try:
            async for chunk in client.download_stream(url, start_offset=offset):
                # Write chunk to file via executor to avoid blocking the event loop
                await loop.run_in_executor(None, _append_bytes, temp_file, chunk)
                offset += len(chunk)
                if on_download_chunk:
                    on_download_chunk(len(chunk))
            
            # Successfully downloaded
            break
            
        except Exception as e:
            import aiohttp
            if isinstance(e, (aiohttp.ClientError, ValueError)):
                print(f"[DirectLink] Temp video download dropped (offset {offset}): {e}. Retries config left: {retries - 1}")
                retries -= 1
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
        
        # User requested Option 2 (user client -> bot chat) for ALL files
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
                updates = await upload_client.invoke(
                    SendMedia(
                        peer=await upload_client.resolve_peer(upload_peer),
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
            print(f"[DirectLink] FloodWait {wait}s (attempt {attempt}/{retries})")
            await asyncio.sleep(wait + 1)
        except Exception as e:
            print(f"[DirectLink] Error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(2)
    return None


async def _send_to_user(
    bot: Client,
    user_id: int,
    msg_id: int,
    is_sent_to_bot: bool,
) -> bool:
    """Deliver file to user and ensure it's in the backup group."""
    try:
        if is_sent_to_bot:
            # File is ALREADY in the user's chat (they sent it to the bot)
            # We just need to forward/copy it to the backup group to keep a record.
            # Using forward_messages so it retains its identity
            await _safe_send(
                lambda: bot.forward_messages(
                    chat_id=BACKUP_GROUP_ID,
                    from_chat_id=user_id,
                    message_ids=msg_id,
                ),
                retries=3,
            )
            # For the user, it is delivered.
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
    from app.bot.main import active_user_processes, reset_cancel, is_cancelled
    
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
    match = DIRECT_LINK_PATTERN.search(message.text)
    if not match:
        await message.reply_text("❌ Tidak dapat mengesan URL yang sah.")
        return

    url = match.group(0).strip()

    # Mark user as active
    active_user_processes[user_id] = True
    reset_cancel(user_id)

    status_msg = await message.reply_text("🔄 Sedang Diproses..")

    try:
        # Resolve URL metadata
        await status_msg.edit("🔍 Mengekstrak metadata URL...")
        try:
            metadata = await _direct_client.resolve(url)
        except Exception as e:
            await status_msg.edit(f"❌ Tidak dapat mengakses URL: {e}")
            return

        file_size = metadata.get("size", 0)
        file_name = metadata.get("filename", "unknown_file")
        final_url = metadata.get("url", url)

        # Check file size limit
        is_premium = getattr(message.from_user, "is_premium", False) or False
        size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE

        if file_size > size_limit:
            limit_gb = size_limit / (1024 * 1024 * 1024)
            actual_gb = file_size / (1024 * 1024 * 1024)
            user_type = "Premium" if is_premium else "Biasa"
            await status_msg.edit(
                f"❌ **Fail terlalu besar!**\n\n"
                f"User {user_type}: Max {limit_gb:.1f} GB\n"
                f"Fail ini: {actual_gb:.2f} GB"
            )
            return

        # Get backup group peer
        try:
            backup_peer = await _get_backup_group_peer()
        except Exception as e:
            await status_msg.edit(f"❌ Gagal mendapat backup group: {e}")
            return

        # Create progress tracker
        tracker = ProgressTracker(status_msg, file_name, file_size)
        tracker.start()

        # Check if file is a video
        is_video = _ext(file_name) in VIDEO_EXTS
        thumb_raw = None
        video_meta: Optional[Dict[str, int]] = None
        temp_video_path = None

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
                thumb_raw = await _generate_video_thumb(temp_video_path, duration)
                if thumb_raw:
                    print(f"[DirectLink] Thumbnail generated: {len(thumb_raw)} bytes")

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
            await status_msg.edit("⚠️ Selesai! Fail telah dimuat naik ke grup sandaran tetapi gagal dihantar.")

    except asyncio.CancelledError:
        if tracker:
            await tracker.stop("❌ Dibatalkan oleh pengguna.")
        else:
            await status_msg.edit("❌ Dibatalkan oleh pengguna.")
    except Exception as e:
        print(f"[DirectLink] Handler error: {e}")
        import traceback
        traceback.print_exc()
        if tracker:
            await tracker.stop(f"❌ Ralat: {e}")
        else:
            try:
                await status_msg.edit(f"❌ Ralat: {e}")
            except Exception:
                pass
    finally:
        if tracker:
            await tracker.stop()
        active_user_processes.pop(user_id, None)
        await _direct_client.close()

