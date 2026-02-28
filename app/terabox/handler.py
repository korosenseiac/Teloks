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

import asyncio
import os
import random
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.raw.functions.messages import SendMedia
from pyrogram.raw.functions.upload import SaveFilePart
from pyrogram.raw.types import (
    DocumentAttributeFilename,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    InputMediaUploadedDocument,
    InputMediaUploadedPhoto,
    InputFile,
    UpdateNewChannelMessage,
    UpdateNewMessage,
)
from pyrogram.types import Message, InputMediaPhoto, InputMediaVideo, InputMediaDocument

from app.config import BACKUP_GROUP_ID
from app.database.db import log_forward, get_user_session, get_user_profile
from app.utils.streamer import upload_stream
from app.terabox.streamer import TeraBoxMediaStreamer
from app.terabox.progress import ProgressTracker

# ---------------------------------------------------------------------------
# Multi-domain regex for TeraBox share links
# ---------------------------------------------------------------------------

TERABOX_LINK_PATTERN = re.compile(
    r"https?://(?:www\.)?(terabox\.com|1024terabox\.com|freeterabox\.com"
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
# Helper: recursively collect files from YOUR OWN TeraBox directory
# ---------------------------------------------------------------------------

async def _collect_own_files(
    tb_client,
    remote_dir: str,
    depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Recursively list files inside your own TeraBox directory (api/list).
    Handles pagination to ensure ALL files are collected.
    """
    if depth > 5:
        return []

    files: List[Dict[str, Any]] = []
    page = 1

    while True:
        result = await tb_client.get_remote_dir(remote_dir, page=page)
        if not result or result.get("errno", -1) != 0:
            break

        entries = result.get("list", [])
        if not entries:
            break

        for entry in entries:
            if str(entry.get("isdir", "0")) != "0":
                sub = await _collect_own_files(
                    tb_client,
                    remote_dir=entry.get("path", ""),
                    depth=depth + 1,
                )
                files.extend(sub)
            else:
                files.append(entry)

        # Check if there are more pages
        has_more = result.get("has_more", 0)
        if not has_more:
            break
        page += 1

    return files


# ---------------------------------------------------------------------------
# Helper: send one TeraBox file to backup group via raw API
# ---------------------------------------------------------------------------

async def _download_thumb_bytes(thumb_url: str, cookie_str: str) -> Optional[bytes]:
    """Download a thumbnail image from TeraBox. Returns raw bytes or None."""
    if not thumb_url:
        return None
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Cookie": cookie_str,
            "Referer": "https://www.terabox.com/",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                thumb_url, headers=headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    if len(data) > 100:  # sanity check
                        return data
    except Exception as e:
        print(f"[TeraBox] _download_thumb_bytes error: {e}")
    return None


async def _upload_thumb_to_telegram(bot: Client, thumb_raw: bytes) -> Optional[InputFile]:
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
        print(f"[TeraBox] _upload_thumb_to_telegram error: {e}")
    return None


async def _upload_terabox_file_to_backup(
    bot: Client,
    tb_client,
    backup_peer,
    dlink: str,
    file_size: int,
    file_name: str,
    thumb_url: str = "",
    tracker: Optional[ProgressTracker] = None,
) -> Optional[int]:
    """
    Download from TeraBox and upload to the backup Telegram group.
    Returns the Telegram message_id in the backup group, or None on failure.
    """
    try:
        # Build callbacks for progress tracking
        on_dl = tracker.add_downloaded if tracker else None
        on_ul = tracker.add_uploaded if tracker else None

        streamer = TeraBoxMediaStreamer(
            tb_client, dlink, file_size, file_name,
            on_download_chunk=on_dl,
        )
        input_file = await upload_stream(bot, streamer, file_name, on_upload_chunk=on_ul)

        kind = _classify(file_name)
        mime = _mime(file_name)

        if kind == "photo":
            media = InputMediaUploadedPhoto(file=input_file)
        elif kind == "video":
            # Download and upload thumbnail for video
            thumb_input_file = None
            if thumb_url:
                thumb_raw = await _download_thumb_bytes(thumb_url, tb_client._cookie_str)
                if thumb_raw:
                    thumb_input_file = await _upload_thumb_to_telegram(bot, thumb_raw)
                    if thumb_input_file:
                        print(f"[TeraBox] Thumbnail uploaded for {file_name} ({len(thumb_raw)} bytes)")

            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime,
                attributes=[
                    DocumentAttributeVideo(duration=0, w=0, h=0, supports_streaming=True),
                    DocumentAttributeFilename(file_name=file_name),
                ],
                thumb=thumb_input_file,
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
    share_host = match.group(1)   # e.g. "1024terabox.com"
    surl = match.group(2)         # e.g. "12VSvUMj_3xxS35TG63_lHQ"
    print(f"[TB:handler] user={user_id} raw_link={message.text.strip()!r} share_host={share_host!r} surl={surl!r}")

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
        info = await tb.short_url_info(surl, share_host=share_host)
        print(f"[TB:handler] short_url_info full response: {info}")
        if not info or info.get("errno", -1) != 0:
            errno = info.get("errno") if info else "?"
            await status_msg.edit(
                f"❌ Link tidak sah atau telah tamat tempoh. (errno={errno})"
            )
            return

        share_id = str(info.get("shareid", ""))
        from_uk = str(info.get("uk", ""))
        print(f"[TB:handler] share_id={share_id!r} from_uk={from_uk!r}")

        # 3. Transfer share items into a temp folder on YOUR TeraBox account.
        #    Inspired by TeraFetch: skip share/list (errno=105 on datacenter IPs)
        #    and transfer root items directly, then list our own files.
        await status_msg.edit("📁 Mencipta folder sementara…")
        temp_folder = f"/terabox_temp_{uuid.uuid4().hex[:12]}"
        cd_result = await tb.create_dir(temp_folder)
        if not cd_result or cd_result.get("errno", -1) != 0:
            await status_msg.edit(
                "❌ Gagal mencipta folder sementara di TeraBox. "
                f"(errno={cd_result.get('errno') if cd_result else '?'})"
            )
            return

        root_items = info.get("file_list") or info.get("list", [])
        root_fs_ids = [int(f["fs_id"]) for f in root_items]
        root_count = len(root_items)
        print(f"[TB:handler] transferring {root_count} root items: {root_fs_ids}")

        await status_msg.edit(
            f"➡️ Memindahkan {root_count} item ke akaun TeraBox anda…"
        )
        transfer_result = await tb.share_transfer(share_id, from_uk, root_fs_ids, temp_folder)
        print(f"[TB:handler] share_transfer result: {transfer_result}")
        if not transfer_result or transfer_result.get("errno", -1) != 0:
            errno = transfer_result.get("errno") if transfer_result else "?"
            extra = transfer_result.get("task_id", "") if transfer_result else ""
            # errno=12 means "already exists" — that's OK, continue
            if transfer_result and transfer_result.get("errno") == 12:
                print(f"[TB:handler] share_transfer errno=12 (already exists), continuing")
            else:
                await status_msg.edit(
                    f"❌ Gagal memindahkan fail. (errno={errno}) {extra}"
                )
                return

        # 4. List OUR temp folder recursively (api/list, not share/list)
        await status_msg.edit("📂 Mengimbas fail yang dipindahkan…")
        all_files = await _collect_own_files(tb, temp_folder)
        print(f"[TB:handler] collected {len(all_files)} file(s) from own dir")
        if all_files:
            for f in all_files[:5]:
                print(f"  └ {f.get('server_filename')} size={f.get('size')} fs_id={f.get('fs_id')}")
        if not all_files:
            await status_msg.edit("❌ Tiada fail dijumpai selepas pemindahan.")
            return

        total = len(all_files)

        # 5. Validate sizes
        oversized = [f for f in all_files if int(f.get("size", 0)) > MAX_FILE_SIZE]
        if oversized:
            names = "\n".join(
                f"• `{f['server_filename']}` ({int(f['size']) / 1e9:.2f} GB)"
                for f in oversized
            )
            await status_msg.edit(
                f"❌ **{len(oversized)} fail melebihi had 2 GB:**\n\n{names}"
            )
            return

        # 6. Get dlink for each transferred file (batched to avoid API limits)
        await status_msg.edit("🔗 Mendapatkan link muat turun…")
        my_fs_ids = [int(f["fs_id"]) for f in all_files]

        DLINK_BATCH = 5  # TeraBox may limit how many dlinks per request
        dlink_map: Dict[int, str] = {}

        for batch_start in range(0, len(my_fs_ids), DLINK_BATCH):
            batch_ids = my_fs_ids[batch_start : batch_start + DLINK_BATCH]
            dl_result = await tb.download(batch_ids)
            if not dl_result or dl_result.get("errno", -1) != 0:
                print(f"[TB:handler] download batch failed: errno={dl_result.get('errno') if dl_result else 'None'}")
                continue
            # TeraBox returns dlinks under "dlink" or "info" depending on version
            dlink_list = dl_result.get("dlink", []) or dl_result.get("info", [])
            if isinstance(dlink_list, list):
                for item in dlink_list:
                    fid = int(item.get("fs_id", 0))
                    dl = item.get("dlink", "")
                    if fid and dl:
                        dlink_map[fid] = dl

        print(f"[TB:handler] dlink_map has {len(dlink_map)} entries for {len(my_fs_ids)} files")

        # Build enriched file list: {name, size, dlink, kind, thumb_url, fs_id}
        enriched: List[Dict[str, Any]] = []
        for tf in all_files:
            fid = int(tf["fs_id"])
            dlink = dlink_map.get(fid, "")
            # Extract best thumbnail URL from TeraBox api/list thumbs
            thumbs = tf.get("thumbs", {})
            thumb_url = (
                thumbs.get("url3", "")
                or thumbs.get("url2", "")
                or thumbs.get("url1", "")
                or thumbs.get("icon", "")
            ) if isinstance(thumbs, dict) else ""
            enriched.append({
                "name": tf.get("server_filename", "file"),
                "size": int(tf.get("size", 0)),
                "dlink": dlink,
                "kind": _classify(tf.get("server_filename", "file")),
                "thumb_url": thumb_url,
                "fs_id": fid,
            })

        if not enriched:
            await status_msg.edit("❌ Tiada fail untuk dimuat naik.")
            return

        # 9. Resolve backup group peer
        backup_peer = await get_backup_group_peer(bot)
        if not backup_peer:
            await status_msg.edit("❌ Backup group tidak dijumpai.")
            return

        # Helper: fetch a fresh dlink for a single fs_id
        async def _refresh_dlink(fs_id: int) -> str:
            """Re-fetch a single dlink (handles expiry)."""
            try:
                r = await tb.download([fs_id])
                if r and r.get("errno", -1) == 0:
                    items = r.get("dlink", []) or r.get("info", [])
                    if items and isinstance(items, list):
                        return items[0].get("dlink", "")
            except Exception as e:
                print(f"[TeraBox] _refresh_dlink error for fs_id={fs_id}: {e}")
            return ""

        # 10. Upload all files to backup group one by one
        # Collect: (backup_msg_id, kind, name, size)
        uploaded: List[Tuple[int, str, str, int]] = []
        total_up = len(enriched)
        MAX_RETRIES = 2

        for idx, entry in enumerate(enriched, 1):
            # Create and start progress tracker for this file
            tracker = ProgressTracker(
                status_msg=status_msg,
                file_name=entry["name"],
                file_size=entry["size"],
                file_index=idx,
                file_total=total_up,
            )
            tracker.start()

            bmid = None
            dlink = entry["dlink"]

            for attempt in range(1, MAX_RETRIES + 1):
                # Ensure we have a dlink (may be missing from initial batch or expired)
                if not dlink:
                    print(f"[TeraBox] No dlink for {entry['name']}, fetching fresh one (attempt {attempt})")
                    dlink = await _refresh_dlink(entry["fs_id"])
                    if not dlink:
                        print(f"[TeraBox] Still no dlink for {entry['name']}")
                        break  # give up on this file

                bmid = await _upload_terabox_file_to_backup(
                    bot, tb, backup_peer,
                    dlink, entry["size"], entry["name"],
                    thumb_url=entry.get("thumb_url", ""),
                    tracker=tracker,
                )
                if bmid:
                    break  # success

                # Failed — dlink may have expired, refresh and retry
                print(f"[TeraBox] Upload failed for {entry['name']} (attempt {attempt}/{MAX_RETRIES}), refreshing dlink")
                dlink = await _refresh_dlink(entry["fs_id"])
                # Reset tracker counters for retry
                tracker.downloaded = 0
                tracker.uploaded = 0
                tracker._dl_samples.clear()
                tracker._ul_samples.clear()

            await tracker.stop()
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

        await status_msg.edit("⬆️ Menghantar ke anda…")

        # ---------- Reliable send helper with FloodWait + retry --------------
        async def _safe_send(coro_factory, retries: int = 3):
            """
            Call coro_factory() up to *retries* times, handling FloodWait.
            Returns the result on success, None on permanent failure.
            """
            for attempt in range(1, retries + 1):
                try:
                    return await coro_factory()
                except FloodWait as fw:
                    wait = fw.value if hasattr(fw, "value") else getattr(fw, "x", 10)
                    print(f"[TeraBox] FloodWait {wait}s (attempt {attempt}/{retries})")
                    await asyncio.sleep(wait + 1)
                except Exception as e:
                    print(f"[TeraBox] _safe_send error (attempt {attempt}/{retries}): {e}")
                    if attempt < retries:
                        await asyncio.sleep(2)
            return None

        # Track which backup message IDs have been confirmed delivered
        delivered_mids: set = set()

        # ---------- Helper: send one file individually -----------------------
        async def _send_single(mid: int) -> bool:
            """Send a single backup message to the user. Returns True on success."""
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

        # ---------- Helper: send album chunk to user -------------------------
        async def _send_album_to_user(
            items: List[Tuple[int, str, str, int]],
        ) -> None:
            """
            Send a list of backup-group items to the user as albums (max 8).
            Tracks delivered mids in the outer `delivered_mids` set.
            """
            if not items:
                return

            # Use 8 instead of 10 to stay safely within Telegram limits
            CHUNK = 8

            for i in range(0, len(items), CHUNK):
                chunk = items[i : i + CHUNK]
                chunk_mids = [mid for mid, *_ in chunk]

                # Fetch backup messages
                backup_msgs = await _safe_send(
                    lambda _ids=chunk_mids: bot.get_messages(BACKUP_GROUP_ID, _ids)
                )
                if backup_msgs is None:
                    # Fallback: send each individually
                    for mid, kind, name, size in chunk:
                        await _send_single(mid)
                        await asyncio.sleep(0.5)
                    continue

                if not isinstance(backup_msgs, list):
                    backup_msgs = [backup_msgs]

                # Build media list for album
                media_list = []
                valid_mids = []  # track which mids made it into media_list
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
                    # get_messages returned nothing useful — send individually
                    for mid, kind, name, size in chunk:
                        await _send_single(mid)
                        await asyncio.sleep(0.5)
                    continue

                if len(media_list) == 1:
                    # send_media_group requires ≥2 items — send single file directly
                    await _send_single(valid_mids[0])
                else:
                    # Try sending as album
                    r = await _safe_send(
                        lambda _ml=media_list: bot.send_media_group(user_id, _ml)
                    )
                    if r:
                        # Verify: count actual messages returned
                        actual = len(r) if isinstance(r, list) else 0
                        if actual == len(media_list):
                            # All delivered
                            delivered_mids.update(valid_mids)
                        else:
                            # Partial delivery — mark what we can, remainder
                            # will be caught by safety-net pass below
                            print(f"[TeraBox] Album partial: sent {actual}/{len(media_list)}")
                            for vm in valid_mids[:actual]:
                                delivered_mids.add(vm)
                    else:
                        # Album failed — fallback: send each individually
                        print(f"[TeraBox] Album send failed, falling back to individual sends")
                        for mid in valid_mids:
                            await _send_single(mid)
                            await asyncio.sleep(0.5)

                # Delay between chunks to avoid FloodWait
                await asyncio.sleep(1.5)

        # 11. Separate by type for album grouping
        photos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "photo"]
        videos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "video"]
        audios = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "audio"]
        others = [(mid, k, n, s) for mid, k, n, s in uploaded
                  if k not in ("photo", "video", "audio")]

        # Send: photos album first
        await _send_album_to_user(photos)

        # Send: videos album next
        await _send_album_to_user(videos)

        # Send: audio files individually
        for mid, *_ in audios:
            await _send_single(mid)
            await asyncio.sleep(0.5)

        # Send: other documents individually
        for mid, *_ in others:
            await _send_single(mid)
            await asyncio.sleep(0.5)

        # ---------- SAFETY NET: resend any files not confirmed delivered ------
        all_mids = {mid for mid, *_ in uploaded}
        missing_mids = all_mids - delivered_mids

        if missing_mids:
            print(f"[TeraBox] Safety net: {len(missing_mids)} file(s) not confirmed delivered, resending individually")
            await asyncio.sleep(2)  # extra breathing room
            for mid in missing_mids:
                await _send_single(mid)
                await asyncio.sleep(1)

        # Final count
        total_to_send = len(uploaded)
        sent_count = len(delivered_mids)

        # Summary
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
                del_result = await tb_cleanup.filemanager("delete", [temp_folder])
                del_errno = del_result.get("errno", "?") if del_result else "?"
                if del_result and del_result.get("errno", -1) == 0:
                    print(f"[TeraBox] Deleted temp folder: {temp_folder}")
                else:
                    print(f"[TeraBox] Failed to delete temp folder {temp_folder}: errno={del_errno} | {del_result}")
            except Exception as e:
                print(f"[TeraBox] Failed to delete temp folder {temp_folder}: {e}")

        active_user_processes.pop(user_id, None)
