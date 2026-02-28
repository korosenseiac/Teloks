"""
terabox/handler.py — Telegram bot handler for TeraBox share links.

Flow
----
1.  Parse surl from the share link.
2.  Enumerate all files in the share (recursive for folders).
3.  Transfer files into a temporary folder on the bot owner's TeraBox account.
4.  Obtain dlink URLs for the transferred files.
5.  Stream each file from TeraBox and upload to Telegram (backup group) via
    the existing upload_stream pipeline.
6.  Send photos as an album, videos as an album, then other files individually
    to the requesting user.
7.  Delete the temporary folder from TeraBox.
"""
from __future__ import annotations

import os
import random
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.raw.functions.messages import SendMedia
from pyrogram.raw.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    UpdateNewChannelMessage,
    UpdateNewMessage,
)
from pyrogram.types import Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument

from app.config import BACKUP_GROUP_ID
from app.database.db import log_forward, get_user_session, get_user_profile
from app.utils.streamer import upload_stream
from app.terabox.streamer import TeraBoxMediaStreamer

# ---------------------------------------------------------------------------
# Multi-domain regex for TeraBox share links
# ---------------------------------------------------------------------------

TERABOX_LINK_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:terabox\.com|1024terabox\.com|freeterabox\.com"
    r"|terabox\.app|teraboxapp\.com)/s/([a-zA-Z0-9_-]+)"
)

# ---------------------------------------------------------------------------
# File classification helpers
# ---------------------------------------------------------------------------

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".m4v", ".ts"}
AUDIO_EXTS = {".mp3", ".flac", ".aac", ".ogg", ".m4a", ".wav", ".opus"}

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB


def _ext(name: str) -> str:
    return os.path.splitext(name)[1].lower()


def _classify(name: str) -> str:
    """Return 'photo', 'video', 'audio', or 'document'."""
    e = _ext(name)
    if e in PHOTO_EXTS:
        return "photo"
    if e in VIDEO_EXTS:
        return "video"
    if e in AUDIO_EXTS:
        return "audio"
    return "document"


def _mime(name: str, category: int = 0) -> str:
    """Best-effort MIME type from extension / TeraBox category code."""
    e = _ext(name)
    mapping = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".webp": "image/webp",
        ".gif": "image/gif", ".mp4": "video/mp4",
        ".mkv": "video/x-matroska", ".avi": "video/x-msvideo",
        ".mov": "video/quicktime", ".webm": "video/webm",
        ".flv": "video/x-flv", ".m4v": "video/x-m4v",
        ".ts": "video/mp2t", ".mp3": "audio/mpeg",
        ".flac": "audio/flac", ".aac": "audio/aac",
        ".ogg": "audio/ogg", ".m4a": "audio/x-m4a",
        ".wav": "audio/wav", ".opus": "audio/opus",
        ".pdf": "application/pdf",
        ".zip": "application/zip",
    }
    return mapping.get(e, "application/octet-stream")


# ---------------------------------------------------------------------------
# Helper: recursively collect all files from a TeraBox share
# ---------------------------------------------------------------------------

async def _collect_files(
    tb_client,
    surl: str,
    remote_dir: str = "",
    depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Return a flat list of file entries from the share.
    Each entry keeps: fs_id, server_filename, size, isdir, category, path.
    """
    if depth > 5:
        return []  # Safety: don't recurse too deep

    result = await tb_client.short_url_list(surl, remote_dir=remote_dir)
    if not result or result.get("errno", -1) != 0:
        return []

    files: List[Dict[str, Any]] = []
    for entry in result.get("list", []):
        if entry.get("isdir"):
            sub = await _collect_files(
                tb_client, surl,
                remote_dir=entry.get("path", entry.get("server_filename", "")),
                depth=depth + 1,
            )
            files.extend(sub)
        else:
            files.append(entry)

    return files


# ---------------------------------------------------------------------------
# Helper: send one TeraBox file to backup group via raw API
# ---------------------------------------------------------------------------

async def _upload_terabox_file_to_backup(
    bot: Client,
    tb_client,
    backup_peer,
    dlink: str,
    file_size: int,
    file_name: str,
) -> Optional[int]:
    """
    Download from TeraBox and upload to the backup Telegram group.
    Returns the Telegram message_id in the backup group, or None on failure.
    """
    try:
        streamer = TeraBoxMediaStreamer(tb_client, dlink, file_size, file_name)
        input_file = await upload_stream(bot, streamer, file_name)

        kind = _classify(file_name)
        mime = _mime(file_name)

        if kind == "photo":
            media = InputMediaUploadedPhoto(file=input_file)
        elif kind == "video":
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime,
                attributes=[
                    DocumentAttributeVideo(duration=0, w=0, h=0, supports_streaming=True),
                    DocumentAttributeFilename(file_name=file_name),
                ],
            )
        elif kind == "audio":
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime,
                attributes=[
                    DocumentAttributeAudio(duration=0),
                    DocumentAttributeFilename(file_name=file_name),
                ],
            )
        else:
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime,
                attributes=[DocumentAttributeFilename(file_name=file_name)],
            )

        updates = await bot.invoke(
            SendMedia(
                peer=backup_peer,
                media=media,
                message="",
                random_id=random.randint(0, 2 ** 63 - 1),
            )
        )

        for update in updates.updates:
            if isinstance(update, (UpdateNewMessage, UpdateNewChannelMessage)):
                return update.message.id

        return None

    except Exception as e:
        print(f"[TeraBox] _upload_terabox_file_to_backup error ({file_name}): {e}")
        import traceback; traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

async def terabox_link_handler(bot: Client, message: Message) -> None:
    """Handler called when a user sends a TeraBox share link."""
    # -- Import here to avoid circular import (app.bot.main imports this file)
    from app.bot.main import active_user_processes, get_backup_group_peer
    from app.terabox import get_terabox_client

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

    # ---------------------------------------------------------------- Parse link
    match = TERABOX_LINK_PATTERN.search(message.text)
    if not match:
        return
    surl = match.group(1)

    # ---------------------------------------------------------------- Start
    active_user_processes[user_id] = True
    status_msg = await message.reply_text("🔍 Parsing share link…")

    temp_folder: Optional[str] = None

    try:
        # 1. Initialise TeraBox client
        try:
            tb = await get_terabox_client()
        except RuntimeError as e:
            active_user_processes.pop(user_id, None)
            await status_msg.edit(f"❌ TeraBox tidak dikonfigurasi: {e}")
            return

        # 2. Get share metadata
        info = await tb.short_url_info(surl)
        if not info or info.get("errno", -1) != 0:
            errno = info.get("errno") if info else "?"
            await status_msg.edit(
                f"❌ Link tidak sah atau telah tamat tempoh. (errno={errno})"
            )
            return

        share_id = str(info.get("shareid", ""))
        from_uk = str(info.get("uk", ""))

        # 3. Enumerate all files (recursive for folders)
        await status_msg.edit("📂 Mengimbas fail dalam share…")
        all_files = await _collect_files(tb, surl)
        if not all_files:
            await status_msg.edit("❌ Tiada fail dijumpai dalam share ini.")
            return

        total = len(all_files)

        # 4. Validate sizes
        oversized = [f for f in all_files if f.get("size", 0) > MAX_FILE_SIZE]
        if oversized:
            names = "\n".join(
                f"• `{f['server_filename']}` ({f['size'] / 1e9:.2f} GB)"
                for f in oversized
            )
            await status_msg.edit(
                f"❌ **{len(oversized)} fail melebihi had 2 GB:**\n\n{names}"
            )
            return

        # 5. Create temp folder on your TeraBox account
        temp_folder = f"/terabox_temp_{uuid.uuid4().hex[:12]}"
        await status_msg.edit(f"📁 Mencipta folder sementara…")
        cd_result = await tb.create_dir(temp_folder)
        if not cd_result or cd_result.get("errno", -1) != 0:
            await status_msg.edit(
                "❌ Gagal mencipta folder sementara di TeraBox. "
                f"(errno={cd_result.get('errno') if cd_result else '?'})"
            )
            return

        # 6. Transfer share files into temp folder
        await status_msg.edit(
            f"➡️ Memindahkan {total} fail ke akaun TeraBox anda…"
        )
        fs_ids = [int(f["fs_id"]) for f in all_files]
        transfer_result = await tb.share_transfer(share_id, from_uk, fs_ids, temp_folder)
        if not transfer_result or transfer_result.get("errno", -1) != 0:
            errno = transfer_result.get("errno") if transfer_result else "?"
            await status_msg.edit(
                f"❌ Gagal memindahkan fail. (errno={errno})"
            )
            return

        # 7. List the newly transferred files (to get YOUR account's fs_ids)
        await status_msg.edit("🔗 Mendapatkan link muat turun…")
        dir_result = await tb.get_remote_dir(temp_folder)
        if not dir_result or dir_result.get("errno", -1) != 0:
            await status_msg.edit("❌ Gagal menyenaraikan fail yang dipindahkan.")
            return

        transferred_files: List[Dict[str, Any]] = dir_result.get("list", [])
        if not transferred_files:
            await status_msg.edit("❌ Tiada fail selepas pemindahan.")
            return

        # 8. Get dlink for each transferred file
        my_fs_ids = [int(f["fs_id"]) for f in transferred_files]
        dl_result = await tb.download(my_fs_ids)
        if not dl_result or dl_result.get("errno", -1) != 0:
            await status_msg.edit("❌ Gagal mendapatkan link muat turun.")
            return

        # Build fs_id → dlink mapping
        dlink_map: Dict[int, str] = {}
        for item in dl_result.get("dlink", []):
            fid = int(item.get("fs_id", 0))
            dlink_map[fid] = item.get("dlink", "")

        # Build enriched file list: {name, size, dlink, kind}
        enriched: List[Dict[str, Any]] = []
        for tf in transferred_files:
            fid = int(tf["fs_id"])
            dlink = dlink_map.get(fid, "")
            if not dlink:
                continue
            enriched.append({
                "name": tf.get("server_filename", "file"),
                "size": int(tf.get("size", 0)),
                "dlink": dlink,
                "kind": _classify(tf.get("server_filename", "file")),
            })

        if not enriched:
            await status_msg.edit("❌ Tiada link muat turun yang sah.")
            return

        # 9. Resolve backup group peer
        backup_peer = await get_backup_group_peer(bot)
        if not backup_peer:
            await status_msg.edit("❌ Backup group tidak dijumpai.")
            return

        # 10. Upload all files to backup group one by one
        # Collect: (backup_msg_id, kind, name, size)
        uploaded: List[Tuple[int, str, str, int]] = []
        total_up = len(enriched)

        for idx, entry in enumerate(enriched, 1):
            await status_msg.edit(
                f"⬇️ Muat naik {idx}/{total_up}: `{entry['name']}`…"
            )
            bmid = await _upload_terabox_file_to_backup(
                bot, tb, backup_peer,
                entry["dlink"], entry["size"], entry["name"],
            )
            if bmid:
                uploaded.append((bmid, entry["kind"], entry["name"], entry["size"]))
                # Log each upload
                channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                link = f"https://t.me/c/{channel_id_str}/{bmid}"
                await log_forward(
                    message.from_user.username, bmid, entry["size"],
                    f"TeraBox/{surl}", link
                )
            else:
                print(f"[TeraBox] Skipping {entry['name']} — upload failed.")

        if not uploaded:
            await status_msg.edit("❌ Semua fail gagal dimuat naik.")
            return

        # 11. Separate by type for album grouping
        photos  = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "photo"]
        videos  = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "video"]
        audios  = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "audio"]
        others  = [(mid, k, n, s) for mid, k, n, s in uploaded
                   if k not in ("photo", "video", "audio")]

        await status_msg.edit("⬆️ Menghantar ke anda…")

        # ---------- Helper: send media group to user from backup file_ids ----
        async def _send_album_to_user(items: List[Tuple[int, str, str, int]]) -> None:
            """Fetch backup msgs and forward as album to the user."""
            if not items:
                return
            # Build media list — Telegram caps groups at 10
            CHUNK = 10
            for i in range(0, len(items), CHUNK):
                chunk = items[i : i + CHUNK]
                media_list = []
                backup_msgs = await bot.get_messages(
                    BACKUP_GROUP_ID, [mid for mid, *_ in chunk]
                )
                if not isinstance(backup_msgs, list):
                    backup_msgs = [backup_msgs]
                for msg in backup_msgs:
                    if not msg:
                        continue
                    if msg.photo:
                        media_list.append(InputMediaPhoto(msg.photo.file_id))
                    elif msg.video:
                        media_list.append(InputMediaVideo(msg.video.file_id))
                    elif msg.document:
                        media_list.append(InputMediaDocument(msg.document.file_id))
                if media_list:
                    if len(media_list) == 1:
                        # send_media_group requires ≥2 items
                        m = media_list[0]
                        if isinstance(m, InputMediaPhoto):
                            await bot.send_photo(user_id, m.media)
                        elif isinstance(m, InputMediaVideo):
                            await bot.send_video(user_id, m.media)
                        else:
                            await bot.send_document(user_id, m.media)
                    else:
                        await bot.send_media_group(user_id, media_list)

        # ---------- Send: photos album first ---------------------------------
        await _send_album_to_user(photos)

        # ---------- Send: videos album next ----------------------------------
        await _send_album_to_user(videos)

        # ---------- Send: audio files individually ---------------------------
        for mid, *_ in audios:
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=BACKUP_GROUP_ID,
                    message_id=mid,
                    caption="",
                )
            except Exception as e:
                print(f"[TeraBox] Failed to forward audio {mid}: {e}")

        # ---------- Send: other documents individually -----------------------
        for mid, *_ in others:
            try:
                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=BACKUP_GROUP_ID,
                    message_id=mid,
                    caption="",
                )
            except Exception as e:
                print(f"[TeraBox] Failed to forward doc {mid}: {e}")

        await status_msg.delete()

    except Exception as e:
        print(f"[TeraBox] Handler error: {e}")
        import traceback; traceback.print_exc()
        try:
            await status_msg.edit(f"❌ Ralat tidak dijangka: {e}")
        except Exception:
            pass

    finally:
        # Clean up temp folder from TeraBox
        if temp_folder:
            try:
                from app.terabox import get_terabox_client
                tb_cleanup = await get_terabox_client()
                await tb_cleanup.filemanager("delete", [temp_folder])
                print(f"[TeraBox] Deleted temp folder: {temp_folder}")
            except Exception as e:
                print(f"[TeraBox] Failed to delete temp folder {temp_folder}: {e}")

        active_user_processes.pop(user_id, None)
