"""
mediafire/handler.py — Telegram bot handler for MediaFire file links.

Flow
----
1. Detect a ``mediafire.com/file/…`` link in the user's message.
2. Resolve the direct-download URL (no auth needed).
3. If the file is an archive (.zip/.rar):
   a. Stream-download to a temp file.
   b. Extract only media files (photos + videos).
   c. Stream-upload each extracted media to the Telegram backup group.
4. If the file is a direct photo/video:
   a. Stream from MediaFire CDN → Telegram backup group.
5. Send albums to the user (photos first, then videos).
6. Clean up temp files.
"""
from __future__ import annotations

import asyncio
import gc
import os
import random
import re
import shutil
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.raw.functions.messages import SendMedia
from pyrogram.raw.functions.upload import SaveFilePart
from pyrogram.raw.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    InputFile,
    UpdateNewChannelMessage,
    UpdateNewMessage,
    UpdateShort,
    UpdateShortSentMessage,
)
from pyrogram.types import (
    Message,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
)

from app.config import BACKUP_GROUP_ID
from app.database.db import log_forward, get_user_session, get_user_profile
from app.utils.streamer import upload_stream
from app.utils.media import (
    PHOTO_EXTS, VIDEO_EXTS, MAX_FILE_SIZE,
    ext as _ext, classify as _classify, mime as _mime,
    is_media, is_archive,
)
from app.mediafire.client import MediaFireClient
from app.mediafire.streamer import MediaFireStreamer, FileStreamer
from app.mediafire.archive import extract_media_from_archive, iter_extract_media, count_media_in_archive
from app.terabox.progress import ProgressTracker

# ---------------------------------------------------------------------------
# Link pattern — single-file links only
# ---------------------------------------------------------------------------

MEDIAFIRE_LINK_PATTERN = re.compile(
    r"https?://(?:www\.)?mediafire\.com/file/([a-zA-Z0-9]+)(?:/[^\s]*)?"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_size(b: int) -> str:
    """Human-readable byte size."""
    if b < 1024:
        return f"{b} B"
    elif b < 1024 ** 2:
        return f"{b / 1024:.2f} KB"
    elif b < 1024 ** 3:
        return f"{b / 1024 ** 2:.2f} MB"
    else:
        return f"{b / 1024 ** 3:.2f} GB"


# ---------------------------------------------------------------------------
# Video thumbnail & metadata helpers (ffmpeg / ffprobe)
# ---------------------------------------------------------------------------


async def _generate_video_thumb(video_path: str) -> Optional[bytes]:
    """
    Use ffmpeg to extract a JPEG thumbnail from the video at ~1 second.
    Returns raw JPEG bytes or None on failure.
    """
    try:
        thumb_path = video_path + ".thumb.jpg"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-i", video_path,
            "-ss", "1",          # seek to 1 second
            "-frames:v", "1",    # single frame
            "-q:v", "5",         # JPEG quality (lower = better, 2-31)
            "-vf", "scale='min(320,iw)':-2",  # max 320px wide, keep ratio
            thumb_path,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()

        if proc.returncode == 0 and os.path.exists(thumb_path):
            with open(thumb_path, "rb") as f:
                data = f.read()
            os.remove(thumb_path)
            if len(data) > 100:  # sanity check
                return data
        # Clean up on failure
        if os.path.exists(thumb_path):
            os.remove(thumb_path)
    except Exception as e:
        print(f"[MediaFire] _generate_video_thumb error: {e}")
    return None


async def _get_video_metadata(video_path: str) -> Dict[str, int]:
    """
    Use ffprobe to get duration, width, height of a video file.
    Returns {"duration": int, "width": int, "height": int}.
    """
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
            # Output lines: stream line (w,h,dur) then format line (dur)
            lines = stdout.decode().strip().splitlines()
            if lines:
                # Parse stream line: "1920,1080,123.456" or "1920,1080,N/A"
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
                    # Duration from stream
                    if len(parts) >= 3:
                        try:
                            meta["duration"] = int(float(parts[2]))
                        except (ValueError, TypeError):
                            pass
                # If stream duration missing, try format duration (second line)
                if meta["duration"] == 0 and len(lines) > 1:
                    try:
                        meta["duration"] = int(float(lines[1].strip().rstrip(",")))
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"[MediaFire] _get_video_metadata error: {e}")
    return meta


async def _upload_thumb_to_telegram(
    bot: Client, thumb_raw: bytes
) -> Optional[InputFile]:
    """Upload thumbnail bytes via SaveFilePart and return an InputFile."""
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
        print(f"[MediaFire] _upload_thumb_to_telegram error: {e}")
    return None


# ---------------------------------------------------------------------------
# Upload a single file to the backup group via raw Telegram API
# ---------------------------------------------------------------------------

async def _upload_file_to_backup(
    bot: Client,
    backup_peer,
    streamer,
    file_name: str,
    file_size: int,
    tracker: Optional[ProgressTracker] = None,
    thumb_raw: Optional[bytes] = None,
    video_meta: Optional[Dict[str, int]] = None,
) -> Optional[int]:
    """
    Upload *streamer* to the backup Telegram group.
    Returns the Telegram ``message_id`` in the backup group, or ``None``.

    For videos, *thumb_raw* (JPEG bytes) and *video_meta* (duration/w/h)
    are used to set the thumbnail and video attributes.
    """
    try:
        on_ul = tracker.add_uploaded if tracker else None
        input_file = await upload_stream(bot, streamer, file_name, on_upload_chunk=on_ul)

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
                thumb_input_file = await _upload_thumb_to_telegram(bot, thumb_raw)
                if thumb_input_file:
                    print(f"[MediaFire] Thumbnail uploaded for {file_name} ({len(thumb_raw)} bytes)")

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

        # SendMedia with FloodWait handling
        for _send_attempt in range(1, 4):
            try:
                updates = await bot.invoke(
                    SendMedia(
                        peer=backup_peer,
                        media=media,
                        message="",
                        random_id=random.randint(0, 2 ** 63 - 1),
                    )
                )
                break
            except FloodWait as fw:
                wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
                print(f"[MediaFire] SendMedia FloodWait {wait}s (attempt {_send_attempt}/3)")
                await asyncio.sleep(wait + 1)
        else:
            print(f"[MediaFire] SendMedia failed after 3 FloodWait retries for {file_name}")
            return None

        # Extract message_id from various Telegram response types
        msg_id = _extract_msg_id(updates)

        if msg_id:
            return msg_id

        # Fallback — scan recent messages in the backup group
        print(
            f"[MediaFire] WARNING: Could not extract msg_id from SendMedia "
            f"response type={type(updates).__name__} for {file_name}"
        )
        try:
            recent = await bot.get_messages(
                BACKUP_GROUP_ID, list(range(-1, -4, -1))
            )
            if not isinstance(recent, list):
                recent = [recent]
            for msg in recent:
                if msg and not getattr(msg, "empty", False):
                    fname = None
                    if msg.document:
                        fname = msg.document.file_name
                    elif msg.video:
                        fname = msg.video.file_name
                    if fname == file_name:
                        return msg.id
        except Exception as fb_err:
            print(f"[MediaFire] Fallback search failed: {fb_err}")

        return None
    except Exception as e:
        print(f"[MediaFire] _upload_file_to_backup error ({file_name}): {e}")
        import traceback
        traceback.print_exc()
        return None


def _extract_msg_id(updates) -> Optional[int]:
    """Dig out the message_id from Telegram's polymorphic Updates response."""
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


# ---------------------------------------------------------------------------
# Reliable send helpers (FloodWait-aware)
# ---------------------------------------------------------------------------


async def _safe_send(coro_factory, retries: int = 3):
    """Call *coro_factory()* up to *retries* times, handling FloodWait."""
    for attempt in range(1, retries + 1):
        try:
            return await coro_factory()
        except FloodWait as fw:
            wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
            print(f"[MediaFire] FloodWait {wait}s (attempt {attempt}/{retries})")
            await asyncio.sleep(wait + 1)
        except Exception as e:
            print(f"[MediaFire] _safe_send error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Album sending (photos first, then videos) — mirrors TeraBox logic
# ---------------------------------------------------------------------------


async def _send_album_to_user(
    bot: Client,
    user_id: int,
    items: List[Tuple[int, str, str, int]],
    delivered_mids: set,
) -> None:
    """
    Send *items* ``(backup_msg_id, kind, name, size)`` to the user as
    media-group albums (max 8 per album).  Tracks delivered message IDs
    in *delivered_mids*.
    """
    if not items:
        return

    CHUNK = 8

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

    for i in range(0, len(items), CHUNK):
        chunk = items[i : i + CHUNK]
        chunk_mids = [mid for mid, *_ in chunk]

        backup_msgs = await _safe_send(
            lambda _ids=chunk_mids: bot.get_messages(BACKUP_GROUP_ID, _ids)
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
                    print(f"[MediaFire] Album partial: sent {actual}/{len(media_list)}")
                    for vm in valid_mids[:actual]:
                        delivered_mids.add(vm)
            else:
                print("[MediaFire] Album send failed, falling back to individual sends")
                for mid in valid_mids:
                    await _send_single(mid)
                    await asyncio.sleep(0.5)

        await asyncio.sleep(1.5)


# ---------------------------------------------------------------------------
# Send uploaded files to user grouped by type
# ---------------------------------------------------------------------------


async def _deliver_to_user(
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
    await status_msg.edit("⬆️ Menghantar ke anda…")

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
    for mid, *_ in others:
        await _send_single(mid)
        await asyncio.sleep(0.5)

    # Safety net — resend anything not confirmed delivered
    all_mids = {mid for mid, *_ in uploaded}
    missing_mids = all_mids - delivered_mids

    if missing_mids:
        print(
            f"[MediaFire] Safety net: {len(missing_mids)} file(s) not "
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
            await status_msg.edit(
                f"⚠️ Selesai! {sent_count}/{total_to_send} fail berjaya dihantar."
            )
        except Exception:
            pass
    else:
        try:
            await status_msg.delete()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Stream-download an archive to a temp file
# ---------------------------------------------------------------------------


async def _download_archive_to_temp(
    mf_client: MediaFireClient,
    direct_url: str,
    filename: str,
    file_size: int,
    dest_dir: str,
    tracker: Optional[ProgressTracker] = None,
) -> str:
    """
    Stream-download the archive from MediaFire into *dest_dir*.
    Returns the path to the downloaded archive file.
    """
    archive_path = os.path.join(dest_dir, filename)
    downloaded = 0

    async for chunk in mf_client.download_stream(direct_url):
        # Write chunk to file (run in executor to avoid blocking)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, _append_bytes, archive_path, chunk
        )
        downloaded += len(chunk)
        if tracker:
            tracker.add_downloaded(len(chunk))

    return archive_path


def _append_bytes(path: str, data: bytes) -> None:
    """Append *data* to *path* (blocking — call via executor)."""
    with open(path, "ab") as f:
        f.write(data)


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def mediafire_link_handler(bot: Client, message: Message) -> None:
    """Handler called when a user sends a MediaFire file link."""
    from app.bot.main import active_user_processes, get_backup_group_peer

    user_id = message.from_user.id

    # ---------------------------------------------------------------- Guards
    if active_user_processes.get(user_id):
        await message.reply_text(
            "⚠️ **Ada proses yang sedang berjalan!**\n\n"
            "Sila tunggu proses sebelumnya selesai sebelum menghantar link baru."
        )
        return

    user_session = await get_user_session(user_id)
    if not user_session:
        await message.reply_text("❌ Belum login. Sila /start untuk login.")
        return

    user_profile = await get_user_profile(user_id)
    if not user_profile:
        from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from app.bot.states import user_profile_states, ProfileStep

        await message.reply_text(
            "⚠️ **Profile belum lengkap!**\n\nSila set profile anda terlebih dahulu.\n\n"
            "👇 **Pilih jantina anda:**",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "👨 Lelaki", callback_data="profile_gender_lelaki"
                        ),
                        InlineKeyboardButton(
                            "👩 Perempuan",
                            callback_data="profile_gender_perempuan",
                        ),
                    ]
                ]
            ),
        )
        user_profile_states[user_id] = {"step": ProfileStep.ASK_GENDER, "data": {}}
        return

    # ---------------------------------------------------------------- Parse link
    match = MEDIAFIRE_LINK_PATTERN.search(message.text)
    if not match:
        return

    url = match.group(0)  # full URL
    print(f"[MediaFire] user={user_id} url={url!r}")

    # ---------------------------------------------------------------- Start
    active_user_processes[user_id] = True
    status_msg = await message.reply_text("🔍 Menyelesaikan link MediaFire…")

    temp_dir: Optional[str] = None
    mf_client = MediaFireClient()

    try:
        # 1. Resolve the direct download URL
        try:
            info = await mf_client.resolve(url)
        except ValueError as e:
            await status_msg.edit(f"❌ {e}")
            return

        direct_url = info["direct_url"]
        filename = info["filename"]
        file_size = info["size"]

        print(
            f"[MediaFire] Resolved: filename={filename!r} "
            f"size={file_size} direct_url={direct_url[:80]}…"
        )

        # 2. Validate size
        if file_size > MAX_FILE_SIZE:
            await status_msg.edit(
                f"❌ Fail terlalu besar! ({_format_size(file_size)})\n"
                f"Had maksimum: {_format_size(MAX_FILE_SIZE)}"
            )
            return

        # 3. Resolve backup group peer
        backup_peer = await get_backup_group_peer(bot)
        if not backup_peer:
            await status_msg.edit("❌ Backup group tidak dijumpai.")
            return

        # 4. Branch based on file type
        if is_archive(filename):
            await _handle_archive(
                bot, mf_client, backup_peer,
                message, status_msg, user_id,
                direct_url, filename, file_size,
            )
        elif is_media(filename):
            await _handle_direct_media(
                bot, mf_client, backup_peer,
                message, status_msg, user_id,
                direct_url, filename, file_size,
            )
        else:
            await status_msg.edit(
                "❌ **Fail ini bukan media yang disokong.**\n\n"
                "Bot hanya menyokong fail foto, video, atau arkib (ZIP/RAR) "
                "yang mengandungi foto/video."
            )

    except Exception as e:
        print(f"[MediaFire] Handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            await status_msg.edit(f"❌ Ralat tidak dijangka: {e}")
        except Exception:
            pass

    finally:
        await mf_client.close()
        active_user_processes.pop(user_id, None)


# ---------------------------------------------------------------------------
# Handle direct media (single photo/video)
# ---------------------------------------------------------------------------


async def _handle_direct_media(
    bot: Client,
    mf_client: MediaFireClient,
    backup_peer,
    message: Message,
    status_msg: Message,
    user_id: int,
    direct_url: str,
    filename: str,
    file_size: int,
) -> None:
    """Stream a single media file from MediaFire → backup group → user."""
    await status_msg.edit(
        f"📥 Memuat turun & memuat naik: `{filename}`\n"
        f"📦 Saiz: {_format_size(file_size)}"
    )

    tracker = ProgressTracker(
        status_msg=status_msg,
        file_name=filename,
        file_size=file_size,
        file_index=1,
        file_total=1,
    )
    tracker.start()

    try:
        streamer = MediaFireStreamer(
            mf_client, direct_url, file_size, filename,
            on_download_chunk=tracker.add_downloaded,
        )

        bmid = await _upload_file_to_backup(
            bot, backup_peer, streamer, filename, file_size, tracker=tracker,
        )

        await tracker.stop()

        if not bmid:
            await status_msg.edit("❌ Gagal memuat naik fail ke Telegram.")
            return

        kind = _classify(filename)

        # Log the upload
        channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
        link = f"https://t.me/c/{channel_id_str}/{bmid}"
        await log_forward(
            message.from_user.username, bmid, file_size,
            f"MediaFire/{filename}", link,
        )

        # Deliver to user
        uploaded = [(bmid, kind, filename, file_size)]
        await _deliver_to_user(bot, user_id, uploaded, status_msg)

    except Exception as e:
        await tracker.stop()
        raise


# ---------------------------------------------------------------------------
# Handle archive (ZIP/RAR → extract media → upload each)
# ---------------------------------------------------------------------------


async def _handle_archive(
    bot: Client,
    mf_client: MediaFireClient,
    backup_peer,
    message: Message,
    status_msg: Message,
    user_id: int,
    direct_url: str,
    filename: str,
    file_size: int,
) -> None:
    """Download archive, extract media ONE AT A TIME, upload each, send albums to user.

    Memory-efficient approach for low-RAM VPS:
    1. Stream-download the archive to disk.
    2. Count media files inside (metadata scan, no extraction).
    3. Extract one file → upload it → delete it → next file.
    4. Delete the archive after all files are processed.
    5. Send albums to user.
    """
    import gc

    temp_dir = tempfile.mkdtemp(prefix="mf_archive_")

    try:
        # ----- Phase 1: Download the archive -----
        await status_msg.edit(
            f"📥 Memuat turun arkib: `{filename}`\n"
            f"📦 Saiz: {_format_size(file_size)}"
        )

        dl_tracker = ProgressTracker(
            status_msg=status_msg,
            file_name=filename,
            file_size=file_size,
            file_index=1,
            file_total=1,
        )
        dl_tracker.start()

        archive_path = await _download_archive_to_temp(
            mf_client, direct_url, filename, file_size, temp_dir,
            tracker=dl_tracker,
        )

        await dl_tracker.stop()

        # Close the MediaFire HTTP session early to free resources
        await mf_client.close()

        # ----- Phase 2: Count media files (metadata only, no extraction) -----
        await status_msg.edit("📂 Mengimbas fail media dalam arkib…")

        loop = asyncio.get_running_loop()
        total_files = await loop.run_in_executor(
            None, count_media_in_archive, archive_path
        )

        if total_files == 0:
            await status_msg.edit(
                "❌ Tiada fail media (foto/video) dijumpai dalam arkib."
            )
            return

        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        await status_msg.edit(
            f"📤 Memuat naik {total_files} fail media ke Telegram…"
        )

        # ----- Phase 3: Extract & upload ONE AT A TIME -----
        uploaded: List[Tuple[int, str, str, int]] = []
        MAX_RETRIES = 3
        idx = 0

        async for mf in iter_extract_media(archive_path, extract_dir):
            idx += 1

            # Validate individual file size
            if mf["size"] > MAX_FILE_SIZE:
                print(f"[MediaFire] Skipping oversized file: {mf['name']} ({_format_size(mf['size'])})")
                try:
                    os.remove(mf["path"])
                except OSError:
                    pass
                continue

            # Generate thumbnail & metadata for videos (ffmpeg)
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

                bmid = await _upload_file_to_backup(
                    bot, backup_peer, streamer,
                    mf["name"], mf["size"],
                    tracker=tracker,
                    thumb_raw=thumb_raw,
                    video_meta=video_meta,
                )
                if bmid:
                    break

                print(
                    f"[MediaFire] Upload failed for {mf['name']} "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                if attempt < MAX_RETRIES:
                    # Reset tracker counters for retry
                    tracker.downloaded = 0
                    tracker.uploaded = 0
                    tracker._dl_samples.clear()
                    tracker._ul_samples.clear()
                    await asyncio.sleep(3)

            await tracker.stop()

            if bmid:
                uploaded.append((bmid, mf["kind"], mf["name"], mf["size"]))
                channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                link = f"https://t.me/c/{channel_id_str}/{bmid}"
                await log_forward(
                    message.from_user.username, bmid, mf["size"],
                    f"MediaFire/{filename}/{mf['name']}", link,
                )
            else:
                print(f"[MediaFire] Skipping {mf['name']} — upload failed after {MAX_RETRIES} attempts.")

            # Delete extracted file IMMEDIATELY after upload to free disk/memory
            try:
                os.remove(mf["path"])
            except OSError:
                pass

            # Release thumbnail memory
            thumb_raw = None
            video_meta = None

            # Hint garbage collector to reclaim memory between files
            gc.collect()

            # Delay between uploads to avoid Telegram rate limits
            if idx < total_files:
                await asyncio.sleep(2)

        # Delete the archive now that all files are processed
        try:
            os.remove(archive_path)
        except OSError:
            pass

        if not uploaded:
            await status_msg.edit("❌ Semua fail gagal dimuat naik.")
            return

        # ----- Phase 4: Send to user -----
        await _deliver_to_user(bot, user_id, uploaded, status_msg)

    finally:
        # Always clean up temp directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            print(f"[MediaFire] Failed to clean temp dir {temp_dir}: {e}")
        gc.collect()
