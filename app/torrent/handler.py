"""
torrent/handler.py — Telegram bot handler for magnet links, .torrent files,
and HTTP URLs pointing to .torrent files.

Flow
----
1. User sends a magnet URI, a .torrent file, or an HTTP link to a .torrent.
2. aria2c downloads the torrent to a temp directory.
3. Each supported media file is stream-uploaded to Telegram backup group.
4. Files are forwarded/copied to the user.
5. Temp directory is cleaned up.

Memory-safe:
  - aria2c runs as a separate process (isolated memory).
  - Upload reads from disk in bounded-queue 1 MB chunks (4 MB max buffered).
  - Files are deleted from disk immediately after upload.
  - gc.collect() after each file.
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

from app.config import BACKUP_GROUP_ID, TORRENT_MAX_SIZE
from app.database.db import log_forward, get_user_session, get_user_profile
from app.utils.streamer import upload_stream
from app.utils.media import (
    PHOTO_EXTS, VIDEO_EXTS, AUDIO_EXTS,
    MAX_FILE_SIZE, MAX_FILE_SIZE_PREMIUM,
    ext as _ext, classify as _classify, mime as _mime,
)
from app.torrent.client import Aria2Error
from app.torrent.streamer import TorrentFileStreamer
from app.terabox.progress import ProgressTracker
from app.bot.session_manager import manager

# ---------------------------------------------------------------------------
# Link patterns
# ---------------------------------------------------------------------------

# Magnet URIs — e.g.  magnet:?xt=urn:btih:abc123…
MAGNET_LINK_PATTERN = re.compile(
    r"magnet:\?xt=urn:[a-z0-9]+:[a-zA-Z0-9]+"
)

# HTTP(S) URLs ending in .torrent (with optional query string)
TORRENT_URL_PATTERN = re.compile(
    r"https?://[^\s]+\.torrent(?:\?[^\s]*)?"
)

# File extensions we'll upload to Telegram from a torrent
_UPLOADABLE_EXTS = PHOTO_EXTS | VIDEO_EXTS | AUDIO_EXTS | {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".txt", ".srt", ".ass", ".sub", ".epub",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    elif b < 1024 ** 2:
        return f"{b / 1024:.2f} KB"
    elif b < 1024 ** 3:
        return f"{b / 1024 ** 2:.2f} MB"
    else:
        return f"{b / 1024 ** 3:.2f} GB"


def _is_uploadable(name: str) -> bool:
    """Return True if the file extension is in the allowed upload set."""
    return _ext(name) in _UPLOADABLE_EXTS


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
# Video thumbnail & metadata helpers (ffmpeg / ffprobe)
# ---------------------------------------------------------------------------

async def _generate_video_thumb(video_path: str) -> Optional[bytes]:
    """Use ffmpeg to extract a JPEG thumbnail at ~1 second."""
    try:
        thumb_path = video_path + ".thumb.jpg"
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y",
            "-ss", "1",
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
        print(f"[Torrent] _generate_video_thumb error: {e}")
    return None


async def _get_video_metadata(video_path: str) -> Dict[str, int]:
    """Use ffprobe to get duration, width, height."""
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
        print(f"[Torrent] _get_video_metadata error: {e}")
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
        print(f"[Torrent] _upload_thumb_to_telegram error: {e}")
    return None


# ---------------------------------------------------------------------------
# Upload a single file to the backup group
# ---------------------------------------------------------------------------

async def _upload_file_to_backup(
    bot: Client,
    user_client: Client,
    backup_peer,
    streamer,
    file_name: str,
    file_size: int,
    tracker=None,
    thumb_raw: Optional[bytes] = None,
    video_meta: Optional[Dict[str, int]] = None,
) -> Tuple[Optional[int], bool]:
    """Upload *streamer* to backup group. Returns (message_id, is_sent_to_bot)."""
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
            thumb_input_file = None
            if thumb_raw:
                thumb_input_file = await _upload_thumb_to_telegram(upload_client, thumb_raw)
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=[
                    DocumentAttributeVideo(
                        duration=vm.get("duration", 0),
                        w=vm.get("width", 0),
                        h=vm.get("height", 0),
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

        # SendMedia with FloodWait handling
        for _attempt in range(1, 4):
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
                print(f"[Torrent] SendMedia FloodWait {wait}s (attempt {_attempt}/3)")
                await asyncio.sleep(wait + 1)
        else:
            print(f"[Torrent] SendMedia failed after 3 FloodWait retries for {file_name}")
            return None

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
                    print(f"[Torrent] Failed to forward bot message {bot_msg.id}: {e}")
                
                if backup_msg_id:
                    return backup_msg_id, True
                else:
                    print("[Torrent] Failed to forward message to backup group")
                    return None, True
            except asyncio.TimeoutError:
                print("[Torrent] Timeout waiting for bot to receive the message")
                if uid in pending_bot_uploads:
                    pending_bot_uploads[uid] = [p for p in pending_bot_uploads[uid] if p[1] != fut]
                return None, True

        if msg_id:
            return msg_id, False

        # Fallback — scan recent messages
        print(f"[Torrent] WARNING: Could not extract msg_id for {file_name}")
        try:
            recent = await bot.get_messages(BACKUP_GROUP_ID, list(range(-1, -4, -1)))
            if not isinstance(recent, list):
                recent = [recent]
            for msg in recent:
                if msg and not getattr(msg, "empty", False):
                    fname = getattr(msg.document or msg.video, "file_name", None)
                    if fname == file_name:
                        return msg.id, False
        except Exception as fb_err:
            print(f"[Torrent] Fallback search failed: {fb_err}")

        return None, False
    except Exception as e:
        print(f"[Torrent] _upload_file_to_backup error ({file_name}): {e}")
        import traceback
        traceback.print_exc()
        return None, False


# ---------------------------------------------------------------------------
# Safe send with FloodWait handling
# ---------------------------------------------------------------------------

async def _safe_send(coro_factory, retries: int = 3):
    for attempt in range(1, retries + 1):
        try:
            return await coro_factory()
        except FloodWait as fw:
            wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
            print(f"[Torrent] FloodWait {wait}s (attempt {attempt}/{retries})")
            await asyncio.sleep(wait + 1)
        except Exception as e:
            print(f"[Torrent] _safe_send error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                await asyncio.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Album sending (same logic as MediaFire)
# ---------------------------------------------------------------------------

async def _send_album_to_user(
    bot: Client,
    user_id: int,
    items: List[Tuple[int, str, str, int, bool]],
    delivered_mids: set,
) -> None:
    if not items:
        return
    CHUNK = 8

    async def _send_single(mid: int, is_sent_to_bot: bool) -> bool:
        if is_sent_to_bot:
            # It's already in the user's chat, forward/copy to backup group
            r = await _safe_send(
                lambda _mid=mid: bot.forward_messages(
                    chat_id=BACKUP_GROUP_ID,
                    from_chat_id=user_id,
                    message_ids=_mid,
                )
            )
            if r:
                delivered_mids.add(mid)
                return True
            return False
        else:
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
        
        # Cannot group nicely if mixed between backup and user chat, so we filter
        # For simplify, if any in chunk is_sent_to_bot, send individually
        if any(is_bot for _, _, _, _, is_bot in chunk):
            for mid, kind, name, size, is_sent_to_bot in chunk:
                await _send_single(mid, is_sent_to_bot)
                await asyncio.sleep(0.5)
            continue

        backup_msgs = await _safe_send(
            lambda _ids=chunk_mids: bot.get_messages(BACKUP_GROUP_ID, _ids)
        )
        if backup_msgs is None:
            for mid, kind, name, size, is_sent_to_bot in chunk:
                await _send_single(mid, is_sent_to_bot)
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
            for mid, kind, name, size, is_sent_to_bot in chunk:
                await _send_single(mid, is_sent_to_bot)
                await asyncio.sleep(0.5)
            continue

        if len(media_list) == 1:
            await _send_single(valid_mids[0], False)
        else:
            r = await _safe_send(
                lambda _ml=media_list: bot.send_media_group(user_id, _ml)
            )
            if r:
                actual = len(r) if isinstance(r, list) else 0
                if actual == len(media_list):
                    delivered_mids.update(valid_mids)
                else:
                    for vm in valid_mids[:actual]:
                        delivered_mids.add(vm)
            else:
                for mid in valid_mids:
                    await _send_single(mid)
                    await asyncio.sleep(0.5)

        await asyncio.sleep(1.5)


# ---------------------------------------------------------------------------
# Deliver uploaded files to user
# ---------------------------------------------------------------------------

async def _deliver_to_user(
    bot: Client,
    user_id: int,
    uploaded: List[Tuple[int, str, str, int, bool]],
    status_msg: Message,
) -> None:
    await status_msg.edit("⬆️ Menghantar ke anda…")

    delivered_mids: set = set()

    async def _send_single(mid: int, is_sent_to_bot: bool) -> bool:
        if is_sent_to_bot:
            r = await _safe_send(
                lambda _mid=mid: bot.copy_message(
                    chat_id=BACKUP_GROUP_ID,
                    from_chat_id=user_id,
                    message_id=_mid,
                )
            )
            if r:
                delivered_mids.add(mid)
                return True
            return False
        else:
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

    photos = [(mid, k, n, s, is_bot) for mid, k, n, s, is_bot in uploaded if k == "photo"]
    videos = [(mid, k, n, s, is_bot) for mid, k, n, s, is_bot in uploaded if k == "video"]
    others = [(mid, k, n, s, is_bot) for mid, k, n, s, is_bot in uploaded if k not in ("photo", "video")]

    await _send_album_to_user(bot, user_id, photos, delivered_mids)
    await _send_album_to_user(bot, user_id, videos, delivered_mids)

    for mid, k, n, s, is_bot in others:
        await _send_single(mid, is_bot)
        await asyncio.sleep(0.5)

    # Safety net
    all_mids_map = {mid: is_bot for mid, k, n, s, is_bot in uploaded}
    missing = set(all_mids_map.keys()) - delivered_mids
    if missing:
        print(f"[Torrent] Safety net: {len(missing)} file(s) resending")
        await asyncio.sleep(2)
        for mid in missing:
            await _send_single(mid, all_mids_map[mid])
            await asyncio.sleep(1)

    sent_count = len(delivered_mids)
    total_to_send = len(uploaded)
    if sent_count < total_to_send:
        try:
            await status_msg.edit(f"⚠️ Selesai! {sent_count}/{total_to_send} fail berjaya dihantar.")
        except Exception:
            pass
    else:
        try:
            await status_msg.delete()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Collect files from completed torrent download
# ---------------------------------------------------------------------------

def _collect_torrent_files(download_dir: str) -> List[Dict[str, Any]]:
    """Walk the download directory and return uploadable files.

    Returns list of dicts: {"path": str, "name": str, "size": int, "kind": str}
    Sorted by: photos first, then videos, then others.
    """
    files: List[Dict[str, Any]] = []
    for root, _dirs, filenames in os.walk(download_dir):
        for fname in filenames:
            fpath = os.path.join(root, fname)
            if not os.path.isfile(fpath):
                continue
            fsize = os.path.getsize(fpath)
            if fsize == 0:
                continue
            if not _is_uploadable(fname):
                continue
            files.append({
                "path": fpath,
                "name": fname,
                "size": fsize,
                "kind": _classify(fname),
            })

    # Sort: photos → videos → audio → documents
    order = {"photo": 0, "video": 1, "audio": 2, "document": 3}
    files.sort(key=lambda f: (order.get(f["kind"], 99), f["name"]))
    return files


# ---------------------------------------------------------------------------
# Get torrent name from aria2 status
# ---------------------------------------------------------------------------

def _get_torrent_name(status: Dict[str, Any]) -> str:
    """Extract a human-readable name from aria2 status."""
    bt = status.get("bittorrent", {})
    if bt:
        info = bt.get("info", {})
        name = info.get("name", "")
        if name:
            return name
    # Fallback: use first file name
    files = status.get("files", [])
    if files:
        path = files[0].get("path", "")
        if path:
            return os.path.basename(path)
    return "Unknown Torrent"


# ===========================================================================
# MAIN HANDLER — magnet links and .torrent URLs (text messages)
# ===========================================================================

async def torrent_link_handler(bot: Client, message: Message) -> None:
    """Handler for magnet: URIs and HTTP .torrent URLs sent as text."""
    from app.bot.main import (
        active_user_processes, get_backup_group_peer,
        is_cancelled, reset_cancel,
    )
    from app.torrent import get_aria2_client

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
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👨 Lelaki", callback_data="profile_gender_lelaki"),
                InlineKeyboardButton("👩 Perempuan", callback_data="profile_gender_perempuan"),
            ]]),
        )
        user_profile_states[user_id] = {"step": ProfileStep.ASK_GENDER, "data": {}}
        return

    user_client = await manager.get_client(user_id)
    if not user_client:
        await message.reply_text("❌ Sesi tidak sah. Sila login semula.")
        return

    # ---------------------------------------------------------------- Detect link type
    text = message.text or ""
    magnet_match = MAGNET_LINK_PATTERN.search(text)
    torrent_url_match = TORRENT_URL_PATTERN.search(text)

    if magnet_match:
        link = magnet_match.group(0)
        # Grab the full magnet URI (everything up to first whitespace)
        full_match = re.search(r"magnet:\?[^\s]+", text)
        if full_match:
            link = full_match.group(0)
        link_type = "magnet"
    elif torrent_url_match:
        link = torrent_url_match.group(0)
        link_type = "torrent_url"
    else:
        return

    print(f"[Torrent] user={user_id} type={link_type} link={link[:80]}…")

    # ---------------------------------------------------------------- Start
    active_user_processes[user_id] = asyncio.current_task()
    reset_cancel(user_id)
    status_msg = await message.reply_text("🧲 Memulakan muat turun torrent…")
    temp_dir = tempfile.mkdtemp(prefix="torrent_")

    try:
        await _process_torrent(
            bot, user_client, message, status_msg, user_id,
            link, link_type, temp_dir,
        )
    except Aria2Error as e:
        print(f"[Torrent] Aria2 error: {e}")
        try:
            await status_msg.edit(f"❌ Ralat torrent: {e}")
        except Exception:
            pass
    except asyncio.CancelledError:
        print(f"[Torrent] Handler cancelled for user {user_id}")
    except Exception as e:
        print(f"[Torrent] Handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            await status_msg.edit(f"❌ Ralat tidak dijangka: {e}")
        except Exception:
            pass
    finally:
        # Cleanup temp directory
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            print(f"[Torrent] Failed to clean temp dir {temp_dir}: {e}")
        active_user_processes.pop(user_id, None)
        reset_cancel(user_id)
        gc.collect()


# ===========================================================================
# HANDLER — .torrent file uploads
# ===========================================================================

async def torrent_file_handler(bot: Client, message: Message) -> None:
    """Handler for .torrent files uploaded to the bot."""
    from app.bot.main import (
        active_user_processes, get_backup_group_peer,
        is_cancelled, reset_cancel,
    )
    from app.torrent import get_aria2_client

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
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("👨 Lelaki", callback_data="profile_gender_lelaki"),
                InlineKeyboardButton("👩 Perempuan", callback_data="profile_gender_perempuan"),
            ]]),
        )
        user_profile_states[user_id] = {"step": ProfileStep.ASK_GENDER, "data": {}}
        return

    user_client = await manager.get_client(user_id)
    if not user_client:
        await message.reply_text("❌ Sesi tidak sah. Sila login semula.")
        return

    # ---------------------------------------------------------------- Download .torrent file
    active_user_processes[user_id] = asyncio.current_task()
    reset_cancel(user_id)
    status_msg = await message.reply_text("🧲 Memuat turun fail .torrent…")
    temp_dir = tempfile.mkdtemp(prefix="torrent_")

    try:
        # Download the .torrent file from Telegram
        torrent_file_path = os.path.join(temp_dir, "input.torrent")
        await bot.download_media(message, file_name=torrent_file_path)

        if not os.path.exists(torrent_file_path):
            await status_msg.edit("❌ Gagal memuat turun fail .torrent.")
            return

        await _process_torrent(
            bot, user_client, message, status_msg, user_id,
            torrent_file_path, "torrent_file", temp_dir,
        )
    except Aria2Error as e:
        print(f"[Torrent] Aria2 error: {e}")
        try:
            await status_msg.edit(f"❌ Ralat torrent: {e}")
        except Exception:
            pass
    except asyncio.CancelledError:
        print(f"[Torrent] Handler cancelled for user {user_id}")
    except Exception as e:
        print(f"[Torrent] Handler error: {e}")
        import traceback
        traceback.print_exc()
        try:
            await status_msg.edit(f"❌ Ralat tidak dijangka: {e}")
        except Exception:
            pass
    finally:
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            print(f"[Torrent] Failed to clean temp dir {temp_dir}: {e}")
        active_user_processes.pop(user_id, None)
        reset_cancel(user_id)
        gc.collect()


# ===========================================================================
# Core processing logic (shared by both handlers)
# ===========================================================================

async def _process_torrent(
    bot: Client,
    user_client: Client,
    message: Message,
    status_msg: Message,
    user_id: int,
    link_or_path: str,
    link_type: str,   # "magnet", "torrent_url", "torrent_file"
    temp_dir: str,
) -> None:
    """Download torrent via aria2c, then upload all media files to Telegram."""
    from app.bot.main import (
        get_backup_group_peer, is_cancelled,
    )
    from app.torrent import get_aria2_client

    download_dir = os.path.join(temp_dir, "files")
    os.makedirs(download_dir, exist_ok=True)

    # 1. Start aria2c client
    aria2 = await get_aria2_client()

    # 2. Submit download
    if link_type == "magnet":
        gid = await aria2.add_magnet(link_or_path, download_dir)
    elif link_type == "torrent_url":
        gid = await aria2.add_torrent_url(link_or_path, download_dir)
    elif link_type == "torrent_file":
        gid = await aria2.add_torrent(link_or_path, download_dir)
    else:
        raise ValueError(f"Unknown link type: {link_type}")

    print(f"[Torrent] Download started: gid={gid} type={link_type}")

    # 3. Wait for metadata (for magnet links, aria2 first fetches metadata)
    await status_msg.edit("🔍 Mendapatkan maklumat torrent…")

    # Give aria2 a moment to resolve metadata
    await asyncio.sleep(2)

    # Check initial status to get torrent name
    try:
        init_status = await aria2.get_status(gid)
        torrent_name = _get_torrent_name(init_status)
        # For magnet links, if there's a followedBy, track that GID
        followed = init_status.get("followedBy")
        if followed:
            gid = followed[0]
            init_status = await aria2.get_status(gid)
            torrent_name = _get_torrent_name(init_status) or torrent_name
    except Exception:
        torrent_name = "Unknown"

    # 4. Create progress tracker for download phase
    # We'll update it during polling
    download_tracker = ProgressTracker(
        status_msg=status_msg,
        file_name=torrent_name[:30] if len(torrent_name) > 30 else torrent_name,
        file_size=1,  # Will be updated once we know total size
        file_index=1,
        file_total=1,
    )

    total_known = False

    def _on_progress(completed: int, total: int, speed: int):
        nonlocal total_known
        if total > 0 and not total_known:
            download_tracker.file_size = total
            total_known = True
        if total > 0:
            # Manually set downloaded bytes (not additive — aria2 gives absolute values)
            delta = completed - download_tracker.downloaded
            if delta > 0:
                download_tracker.add_downloaded(delta)

    download_tracker.start()

    try:
        # 5. Wait for download to complete
        final_status = await aria2.wait_for_download(
            gid,
            poll_interval=2.0,
            on_progress=_on_progress,
            cancel_check=lambda: is_cancelled(user_id),
        )
    except Aria2Error as e:
        await download_tracker.stop()
        if "cancelled" in str(e).lower():
            await status_msg.edit("🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
        else:
            raise
        return

    await download_tracker.stop()

    # Check total size against limit
    total_size = int(final_status.get("totalLength", 0))
    if TORRENT_MAX_SIZE > 0 and total_size > TORRENT_MAX_SIZE:
        await status_msg.edit(
            f"❌ Torrent terlalu besar! ({_format_size(total_size)})\n"
            f"Had maksimum: {_format_size(TORRENT_MAX_SIZE)}"
        )
        return

    torrent_name = _get_torrent_name(final_status) or torrent_name
    print(f"[Torrent] Download complete: {torrent_name} ({_format_size(total_size)})")

    # Clean the download result from aria2's memory
    await aria2.remove_result(gid)

    # 6. Collect uploadable files
    if is_cancelled(user_id):
        await status_msg.edit("🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
        return

    files = _collect_torrent_files(download_dir)

    if not files:
        await status_msg.edit(
            "❌ **Tiada fail yang boleh dimuat naik dijumpai dalam torrent ini.**\n\n"
            "Bot menyokong: foto, video, audio, dan dokumen biasa."
        )
        return

    # Filter out files exceeding Telegram's limit
    # 4 GB for Premium users, 2 GB for regular users
    is_premium = getattr(message.from_user, "is_premium", False) or False
    size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE
    print(f"[Torrent] User premium={is_premium}, size_limit={_format_size(size_limit)}")

    skipped = []
    valid_files = []
    for f in files:
        if f["size"] > size_limit:
            skipped.append(f)
        else:
            valid_files.append(f)

    if skipped:
        skipped_names = ", ".join(f["name"] for f in skipped[:3])
        if len(skipped) > 3:
            skipped_names += f" +{len(skipped) - 3} lagi"
        print(f"[Torrent] Skipping {len(skipped)} oversized files: {skipped_names}")

    if not valid_files:
        await status_msg.edit(
            f"❌ Semua {len(files)} fail melebihi had saiz Telegram ({_format_size(size_limit)}).\n\n"
            f"Fail terbesar: {files[0]['name']} ({_format_size(files[0]['size'])})"
        )
        return

    # 7. Get backup peer
    backup_peer = await get_backup_group_peer(bot)
    if not backup_peer:
        await status_msg.edit("❌ Backup group tidak dijumpai.")
        return

    # 8. Upload each file
    total_files = len(valid_files)
    await status_msg.edit(
        f"📤 Memuat naik {total_files} fail ke Telegram…\n"
        f"🧲 {torrent_name}"
    )

    uploaded: List[Tuple[int, str, str, int]] = []
    MAX_RETRIES = 3

    for idx, finfo in enumerate(valid_files, 1):
        if is_cancelled(user_id):
            await status_msg.edit("🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
            return

        file_path = finfo["path"]
        file_name = finfo["name"]
        file_size = finfo["size"]
        file_kind = finfo["kind"]

        # Generate video metadata if applicable
        thumb_raw = None
        video_meta = None
        if file_kind == "video":
            thumb_raw = await _generate_video_thumb(file_path)
            video_meta = await _get_video_metadata(file_path)

        tracker = ProgressTracker(
            status_msg=status_msg,
            file_name=file_name,
            file_size=file_size,
            file_index=idx,
            file_total=total_files,
        )
        tracker.start()

        bmid = None
        is_sent_to_bot = False
        for attempt in range(1, MAX_RETRIES + 1):
            streamer = TorrentFileStreamer(
                file_path, file_name,
                on_read_chunk=tracker.add_downloaded,
            )
            bmid, is_sent_to_bot = await _upload_file_to_backup(
                bot, user_client, backup_peer, streamer,
                file_name, file_size,
                tracker=tracker,
                thumb_raw=thumb_raw,
                video_meta=video_meta,
            )
            if bmid:
                break
            print(f"[Torrent] Upload failed for {file_name} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                tracker.downloaded = 0
                tracker.uploaded = 0
                tracker._dl_samples.clear()
                tracker._ul_samples.clear()
                await asyncio.sleep(3)

        await tracker.stop()

        if bmid:
            uploaded.append((bmid, file_kind, file_name, file_size, is_sent_to_bot))
            channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
            link = f"https://t.me/c/{channel_id_str}/{bmid}" if not is_sent_to_bot else None
            await log_forward(
                message.from_user.username, bmid, file_size,
                f"Torrent/{torrent_name}/{file_name}", link
            )
        else:
            print(f"[Torrent] Skipping {file_name} — upload failed after {MAX_RETRIES} attempts.")

        if hasattr(streamer, "close"):
            await streamer.close()

        # Delete the file from disk immediately after upload
        try:
            os.remove(file_path)
        except OSError:
            pass

        thumb_raw = None
        video_meta = None
        gc.collect()

        # Brief pause between files to avoid flood
        if idx < total_files:
            await asyncio.sleep(2)

    if not uploaded:
        await status_msg.edit("❌ Semua fail gagal dimuat naik.")
        return

    # 9. Send to user
    if skipped:
        skip_msg = f"\n\n⚠️ {len(skipped)} fail dilangkau (melebihi {_format_size(size_limit)})"
    else:
        skip_msg = ""

    await _deliver_to_user(bot, user_id, uploaded, status_msg)

    # If there were skipped files, notify user
    if skipped:
        try:
            skip_text = (
                f"⚠️ **{len(skipped)} fail dilangkau** kerana melebihi had saiz "
                f"Telegram ({_format_size(size_limit)}):\n\n"
            )
            for sf in skipped[:5]:
                skip_text += f"• `{sf['name']}` ({_format_size(sf['size'])})\n"
            if len(skipped) > 5:
                skip_text += f"• … dan {len(skipped) - 5} lagi"
            await bot.send_message(user_id, skip_text)
        except Exception:
            pass
