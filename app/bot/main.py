import re
import random
import math
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatPrivileges
from pyrogram.raw.functions.messages import SendMedia
from pyrogram.raw.functions.upload import SaveFilePart
from pyrogram.raw.types import (
    InputMediaUploadedDocument, 
    InputMediaUploadedPhoto,
    DocumentAttributeFilename, 
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeAnimated,
    UpdateNewMessage, 
    UpdateNewChannelMessage, 
    InputPeerChannel,
    InputFile
)
from app.config import API_ID, API_HASH, BOT_TOKEN, BACKUP_GROUP_ID
from app.database.db import add_user, save_user_session, log_forward, get_user_session, save_backup_group_cache, get_backup_group_cache, get_user_profile
from app.bot.session_manager import manager
from app.utils.streamer import MediaStreamer, upload_stream
from app.bot.auth import handle_login_command, handle_auth_message, handle_login_callback, cancel_login, handle_main_menu_callback, handle_profile_callback, handle_profile_age_message, start_profile_setup
from app.bot.states import user_profile_states, ProfileStep
from app.utils.message import safe_edit
from app.terabox.handler import terabox_link_handler, TERABOX_LINK_PATTERN
from app.mediafire.handler import mediafire_link_handler, MEDIAFIRE_LINK_PATTERN
from app.torrent.handler import (
    torrent_link_handler, torrent_file_handler,
    MAGNET_LINK_PATTERN, TORRENT_URL_PATTERN,
)
from app.direct.handler import direct_link_handler, DIRECT_LINK_PATTERN
from app.utils.media import is_torrent, is_archive, classify, mime, PHOTO_EXTS, VIDEO_EXTS
from app.mediafire.archive import iter_extract_media, count_media_in_archive
from app.mediafire.streamer import FileStreamer
import asyncio
import tempfile
import shutil
import os
import gc

# Track active processes per user (user_id: True if processing)
active_user_processes = {}

# Cancellation events per user (user_id: asyncio.Event)
cancel_events = {}

# Track pending bot uploads for Option 2 (user_id -> list of (file_name, asyncio.Future))
pending_bot_uploads = {}

def is_cancelled(user_id: int) -> bool:
    """Check if the user's process has been cancelled."""
    event = cancel_events.get(user_id)
    return event.is_set() if event else False

def reset_cancel(user_id: int):
    """Reset/clear the cancellation event for a user."""
    if user_id in cancel_events:
        cancel_events[user_id].clear()

def request_cancel(user_id: int):
    """Request cancellation for a user's running process."""
    if user_id not in cancel_events:
        cancel_events[user_id] = asyncio.Event()
    cancel_events[user_id].set()
    
    task = active_user_processes.get(user_id)
    if isinstance(task, asyncio.Task):
        task.cancel()

# File size limit (2GB in bytes)
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

def format_file_size(size_bytes):
    """Format file size in human readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def get_media_file_size(msg):
    """Get file size from a message's media object."""
    media_obj = (msg.document or msg.video or msg.audio or 
                 msg.photo or msg.voice or msg.video_note or
                 msg.animation or msg.sticker)
    return getattr(media_obj, "file_size", 0)

# Main menu keyboard
def get_main_menu_keyboard(is_logged_in=False):
    """Generate main menu keyboard based on login status."""
    if is_logged_in:
        keyboard = [
            [InlineKeyboardButton("📤 Cara Guna", callback_data="menu_help")],
            [InlineKeyboardButton("🔄 Re-Login", callback_data="menu_login")],
        ]
    else:
        keyboard = [
            [InlineKeyboardButton("🔐 Login", callback_data="menu_login")],
            [InlineKeyboardButton("📖 Help", callback_data="menu_help")],
        ]
    return InlineKeyboardMarkup(keyboard)

app = Client(
    "bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Cache for backup group peer
backup_group_peer = None
backup_group_actual_id = None  # Store the actual working ID

async def get_backup_group_peer(client: Client):
    """Get and cache the backup group peer."""
    global backup_group_peer, backup_group_actual_id
    
    if backup_group_peer:
        return backup_group_peer
    
    # Try to load from database first (persistent cache)
    cached = await get_backup_group_cache()
    if cached:
        try:
            # Reconstruct the peer from cached data
            backup_group_peer = InputPeerChannel(
                channel_id=cached["group_id"],
                access_hash=cached["access_hash"]
            )
            backup_group_actual_id = int(f"-100{cached['group_id']}")
            print(f"✅ Backup group loaded from database cache: {backup_group_actual_id}")
            return backup_group_peer
        except Exception as e:
            print(f"DEBUG: Failed to load from cache: {e}")
    
    # Prepare ID variations to try
    raw_id = str(BACKUP_GROUP_ID)
    if raw_id.startswith("-100"):
        channel_id = int(raw_id[4:])
    elif raw_id.startswith("-"):
        channel_id = int(raw_id[1:])
    else:
        channel_id = int(raw_id)
    
    ids_to_try = [
        BACKUP_GROUP_ID,                    # As configured
        int(f"-100{channel_id}"),           # Supergroup format
        -channel_id,                         # Basic group format
    ]
    # Remove duplicates while preserving order
    ids_to_try = list(dict.fromkeys(ids_to_try))
    
    print(f"DEBUG: Will try these IDs: {ids_to_try}")
    
    for test_id in ids_to_try:
        # Method 1: Try get_chat first (this can work if bot received updates from the group)
        try:
            chat = await client.get_chat(test_id)
            backup_group_peer = await client.resolve_peer(test_id)
            backup_group_actual_id = test_id
            # Save to database for persistence
            if hasattr(backup_group_peer, 'channel_id') and hasattr(backup_group_peer, 'access_hash'):
                await save_backup_group_cache(backup_group_peer.channel_id, backup_group_peer.access_hash)
            print(f"✅ Backup group resolved via get_chat: {test_id} ({chat.title})")
            return backup_group_peer
        except Exception as e:
            print(f"DEBUG: get_chat({test_id}) failed: {e}")
        
        # Method 2: Try resolve_peer directly
        try:
            backup_group_peer = await client.resolve_peer(test_id)
            backup_group_actual_id = test_id
            # Save to database for persistence
            if hasattr(backup_group_peer, 'channel_id') and hasattr(backup_group_peer, 'access_hash'):
                await save_backup_group_cache(backup_group_peer.channel_id, backup_group_peer.access_hash)
            print(f"✅ Backup group resolved via resolve_peer: {test_id}")
            return backup_group_peer
        except Exception as e:
            print(f"DEBUG: resolve_peer({test_id}) failed: {e}")
        
        # Method 3: Try sending a test message
        try:
            test_msg = await client.send_message(test_id, "🤖 Bot initialized - this message can be deleted.")
            await test_msg.delete()
            backup_group_peer = await client.resolve_peer(test_id)
            backup_group_actual_id = test_id
            # Save to database for persistence
            if hasattr(backup_group_peer, 'channel_id') and hasattr(backup_group_peer, 'access_hash'):
                await save_backup_group_cache(backup_group_peer.channel_id, backup_group_peer.access_hash)
            print(f"✅ Backup group resolved after send_message: {test_id}")
            return backup_group_peer
        except Exception as e:
            print(f"DEBUG: send_message({test_id}) failed: {e}")
    
    print("Error")

    return None


async def get_backup_group_actual_id():
    """Returns the actual working ID (integer) of the backup group, even if it was not resolved."""
    global backup_group_actual_id
    if backup_group_actual_id:
        return backup_group_actual_id
    
    # Fallback to config but try to ensure -100 prefix if it looks like a supergroup
    from app.config import BACKUP_GROUP_ID
    raw_id_str = str(BACKUP_GROUP_ID)
    if not raw_id_str.startswith("-"):
        # If it's a positive number, it's likely a channel/supergroup ID without prefix
        return int(f"-100{raw_id_str}")
    return BACKUP_GROUP_ID


# Regex to extract chat_id and message_id from Telegram links
# Private: https://t.me/c/1234567890/123
# Public:  https://t.me/username/123
LINK_PATTERN = re.compile(r"https://t\.me/(?:c/(\d+)|([a-zA-Z][a-zA-Z0-9_]{3,}))/(\d+)")

# Handler to cache any group the bot is in (runs on ANY message in groups)
@app.on_message(filters.group, group=-1)
async def cache_group_handler(client: Client, message: Message):
    """This handler runs on every group message to cache the peer."""
    global backup_group_peer, backup_group_actual_id
    chat_id = message.chat.id
    
    # Check if this is our backup group
    if chat_id == BACKUP_GROUP_ID or str(chat_id) == str(BACKUP_GROUP_ID):
        if not backup_group_peer:
            try:
                backup_group_peer = await client.resolve_peer(chat_id)
                backup_group_actual_id = chat_id
                # Save to database for persistence across restarts
                if hasattr(backup_group_peer, 'channel_id') and hasattr(backup_group_peer, 'access_hash'):
                    await save_backup_group_cache(backup_group_peer.channel_id, backup_group_peer.access_hash)
                print(f"✅ Backup group auto-cached from message: {chat_id} ({message.chat.title})")
            except Exception as e:
                print(f"DEBUG: Failed to cache group {chat_id}: {e}")

@app.on_message(filters.command("checkgroup") & filters.group)
async def check_group_handler(client: Client, message: Message):
    """Debug command to check backup group status - only works in groups."""
    global backup_group_peer, backup_group_actual_id
    
    # Only allow in backup group
    if message.chat.id != BACKUP_GROUP_ID:
        return  # Silently ignore if not in backup group
    
    response = f"**Backup Group Debug Info**\n\n"
    response += f"Configured ID: `{BACKUP_GROUP_ID}`\n"
    response += f"Cached Peer: `{backup_group_peer}`\n"
    response += f"Actual ID: `{backup_group_actual_id}`\n\n"
    
    response += f"**This Chat:**\n"
    response += f"ID: `{message.chat.id}`\n"
    response += f"Title: {message.chat.title}\n"
    response += f"Type: {message.chat.type}\n\n"
    
    # Try to cache this group and save to database
    try:
        backup_group_peer = await client.resolve_peer(message.chat.id)
        backup_group_actual_id = message.chat.id
        # Save to database for persistence across restarts
        if hasattr(backup_group_peer, 'channel_id') and hasattr(backup_group_peer, 'access_hash'):
            await save_backup_group_cache(backup_group_peer.channel_id, backup_group_peer.access_hash)
        response += "✅ **Backup group peer cached and saved to database.**"
    except Exception as e:
        response += f"❌ Failed to cache: {e}"
    
    await message.reply_text(response)

@app.on_message(filters.command("promote") & filters.group)
async def promote_to_admin_handler(client: Client, message: Message):
    """Promote a user to admin with all privileges - only works in backup group."""
    global backup_group_peer, backup_group_actual_id
    
    # Only allow in backup group
    if message.chat.id != BACKUP_GROUP_ID:
        return  # Silently ignore if not in backup group
    
    # Check if user replied to a message or provided user ID/username
    target_user = None
    
    if message.reply_to_message:
        # Get user from replied message
        target_user = message.reply_to_message.from_user
    elif len(message.command) > 1:
        # Get user from command argument (user_id or @username)
        user_input = message.command[1]
        try:
            # Try to get user by ID or username
            target_user = await client.get_users(user_input)
        except Exception as e:
            await message.reply_text(f"❌ User tidak dijumpai: {e}")
            return
    else:
        await message.reply_text(
            "❌ **Cara guna:**\n\n"
            "1. Reply kepada mesej user yang ingin dipromote:\n"
            "   `/promote`\n\n"
            "2. Atau gunakan user ID/username:\n"
            "   `/promote @username`\n"
            "   `/promote 123456789`"
        )
        return
    
    if not target_user:
        await message.reply_text("❌ User tidak dijumpai.")
        return
    
    try:
        # Create ChatPrivileges with all admin permissions enabled
        # Note: can_post_messages and can_edit_messages are for channels only
        privileges = ChatPrivileges(
            can_manage_chat=True,
            can_delete_messages=True,
            can_manage_video_chats=True,
            can_restrict_members=True,
            can_promote_members=True,
            can_change_info=True,
            can_invite_users=True,
            can_pin_messages=True,
            is_anonymous=False
        )
        
        # Promote the user
        await client.promote_chat_member(
            chat_id=message.chat.id,
            user_id=target_user.id,
            privileges=privileges
        )
        
        # Build user display name
        user_name = target_user.first_name
        if target_user.last_name:
            user_name += f" {target_user.last_name}"
        if target_user.username:
            user_name += f" (@{target_user.username})"
        
        await message.reply_text(
            f"✅ **Berjaya!**\n\n"
            f"User **{user_name}** telah dipromote sebagai admin dengan semua kebenaran:\n\n"
            f"• Manage Chat ✓\n"
            f"• Delete Messages ✓\n"
            f"• Manage Video Chats ✓\n"
            f"• Restrict Members ✓\n"
            f"• Promote Members ✓\n"
            f"• Change Info ✓\n"
            f"• Invite Users ✓\n"
            f"• Pin Messages ✓"
        )
        
    except Exception as e:
        await message.reply_text(f"❌ Gagal promote user: {e}")

@app.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    await add_user(message.from_user.id, message.from_user.username)
    
    # Check if user is already logged in
    existing_session = await get_user_session(message.from_user.id)
    is_logged_in = existing_session is not None
    
    if is_logged_in:
        # Check if profile is complete
        user_profile = await get_user_profile(message.from_user.id)
        if not user_profile:
            # Prompt for profile setup
            await message.reply_text(
                "👋 **Hye!**\n\n"
                "✅ Dah login, tapi profile belum lengkap.\n\n"
                "Sila set profile anda terlebih dahulu.\n\n"
                "👇 **Pilih jantina anda:**",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("👨 Lelaki", callback_data="profile_gender_lelaki"),
                        InlineKeyboardButton("👩 Perempuan", callback_data="profile_gender_perempuan"),
                    ]
                ])
            )
            user_profile_states[message.from_user.id] = {
                "step": ProfileStep.ASK_GENDER,
                "data": {}
            }
            return
        
        welcome_text = (
            "👋 **Hye!**\n\n"
            "✅ Dah login. Follow @telokschannel. Kalau bot kena remove, admin akan update baru disitu.\n\n"
            "**Cara guna:**\n"
            "Copy dan paste mesej link dari channel/group. Contohnya:\n"
            "`https://t.me/c/1234567890/123` (private)\n"
            "`https://t.me/contoh/14720` (public)\n\n"
            "Nanti bot akan forward contentnya."
        )
    else:
        welcome_text = (
            "👋 **Hye! Follow @telokschannel. Kalau bot kena remove, admin akan update baru disitu.**\n\n"
            "Bot ni boleh forward restricted content dari private channels/group untuk korang.\n\n"
            "**Untuk mula:**\n"
            "Korang perlu login dengan akaun Telegram dalam bot ni.\n\n"
            "👇 **Tekan butang kat bawah untuk login:**"
        )
    
    await message.reply_text(
        welcome_text,
        reply_markup=get_main_menu_keyboard(is_logged_in)
    )

@app.on_callback_query(filters.regex(r"^menu_"))
async def main_menu_callback_handler(client: Client, callback_query):
    await handle_main_menu_callback(client, callback_query)

@app.on_message(filters.command("login"))
async def login_handler(client: Client, message: Message):
    await handle_login_command(client, message)

@app.on_message(filters.command("cancel"))
async def cancel_handler(client: Client, message: Message):
    user_id = message.from_user.id

    # Check if user has an active process (terabox/mediafire/forwarding)
    if active_user_processes.get(user_id):
        request_cancel(user_id)
        await message.reply_text(
            "🚫 **Membatalkan proses...**\n\n"
            "Proses telah dibatalkan dengan serta-merta.\n"
            "💾 Folder sementara telah dibersihkan."
        )
        return

    # Otherwise, try to cancel login flow
    await cancel_login(client, message)

@app.on_callback_query(filters.regex(r"^login_"))
async def login_callback_handler(client: Client, callback_query):
    await handle_login_callback(client, callback_query)

@app.on_callback_query(filters.regex(r"^cancel_"))
async def cancel_callback_handler(client: Client, callback_query):
    """Handle cancel button callbacks."""
    from app.bot.auth import cancel_login_callback
    await cancel_login_callback(client, callback_query)

@app.on_callback_query(filters.regex(r"^profile_"))
async def profile_callback_handler(client: Client, callback_query):
    """Handle profile setup callbacks (gender selection)."""
    await handle_profile_callback(client, callback_query)

@app.on_message(filters.text & filters.private, group=1)
async def auth_message_handler(client: Client, message: Message):
    # Check if this message is part of the profile setup flow (age input)
    if await handle_profile_age_message(client, message):
        message.stop_propagation()
        return
    
    # Check if this message is part of the auth flow
    if await handle_auth_message(client, message):
        message.stop_propagation()

@app.on_message(filters.regex(TERABOX_LINK_PATTERN) & filters.private)
async def terabox_handler(client: Client, message: Message):
    await terabox_link_handler(client, message)


@app.on_message(filters.regex(MEDIAFIRE_LINK_PATTERN) & filters.private)
async def mediafire_handler(client: Client, message: Message):
    await mediafire_link_handler(client, message)


@app.on_message(filters.regex(MAGNET_LINK_PATTERN) & filters.private)
async def torrent_magnet_handler(client: Client, message: Message):
    await torrent_link_handler(client, message)


@app.on_message(filters.regex(TORRENT_URL_PATTERN) & filters.private)
async def torrent_url_handler(client: Client, message: Message):
    await torrent_link_handler(client, message)


async def _is_pending_upload(_, __, message: Message):
    user_id = message.from_user.id if message.from_user else None
    return bool(user_id and user_id in pending_bot_uploads and pending_bot_uploads[user_id])

pending_upload_filter = filters.create(_is_pending_upload)

@app.on_message(filters.media & filters.private & pending_upload_filter, group=0)
async def bot_media_interceptor(client: Client, message: Message):
    """Intercept media messages sent by the user to the bot during Option 2 uploads."""
    user_id = message.from_user.id
    if user_id in pending_bot_uploads and pending_bot_uploads[user_id]:
        media_obj = (message.document or message.video or message.audio or 
                     message.photo or message.animation or message.voice)
        if not media_obj:
            return
        file_name = getattr(media_obj, "file_name", None)
        
        for i, (fname, fut) in enumerate(pending_bot_uploads[user_id]):
            # if name matches, or if it's a photo without a name and we expect a photo
            # fallback: if there is only 1 pending upload, just resolve it to avoid timeouts.
            if fname == file_name or (file_name is None and message.photo) or len(pending_bot_uploads[user_id]) == 1:
                if not fut.done():
                    fut.set_result(message)
                pending_bot_uploads[user_id].pop(i)
                message.stop_propagation()
                return


@app.on_message(filters.document & filters.private, group=2)
async def torrent_file_upload_handler(client: Client, message: Message):
    """Handle uploaded .torrent files."""
    doc = message.document
    if not doc:
        return
    fname = getattr(doc, "file_name", "") or ""
    mime = getattr(doc, "mime_type", "") or ""
    if is_torrent(fname) or mime == "application/x-bittorrent":
        await torrent_file_handler(client, message)


@app.on_message(filters.regex(DIRECT_LINK_PATTERN) & filters.private)
async def direct_link_message_handler(client: Client, message: Message):
    await direct_link_handler(client, message)


# ---------------------------------------------------------------------------
# Archive processing helper for telegram forwarder
# ---------------------------------------------------------------------------

async def _archive_get_video_metadata(video_path: str) -> dict:
    """Use ffprobe to get duration, width, height for videos."""
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
                # Try format duration if stream duration not found
                if meta["duration"] == 0 and len(lines) > 1:
                    try:
                        meta["duration"] = int(float(lines[1].strip().rstrip(",")))
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"[Archive] _archive_get_video_metadata error: {e}")
    return meta


async def _archive_generate_video_thumb(video_path: str, duration: int = 0) -> bytes | None:
    """
    Use ffmpeg to extract a JPEG thumbnail from the video.
    Tries multiple positions to avoid black frames:
    1. 10% of duration (avoids black intro)
    2. 1 second
    3. 0.1 second (for very short videos)
    """
    thumb_path = video_path + ".thumb.jpg"

    # Calculate seek positions to try
    seek_positions = []
    if duration > 0:
        # Try 10% of duration first (avoids black intro/outro)
        ten_percent = max(0.1, min(duration * 0.1, 5.0))  # Cap at 5 seconds
        seek_positions.append(str(ten_percent))
    seek_positions.extend(["1", "0.5", "0.1"])  # Fallback positions

    for seek_time in seek_positions:
        try:
            # Clean up any previous attempt
            if os.path.exists(thumb_path):
                os.remove(thumb_path)

            proc = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y",
                "-ss", seek_time,
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

                # Check if thumbnail is valid (not too small, indicating black/empty frame)
                if len(data) > 500:  # Good thumbnails are usually > 500 bytes
                    os.remove(thumb_path)
                    return data
                # If thumbnail is very small, it might be a black frame - try next position
                print(f"[Archive] Thumbnail at {seek_time}s too small ({len(data)} bytes), trying next position")

        except Exception as e:
            print(f"[Archive] Thumbnail generation error at {seek_time}s: {e}")

    # Clean up
    if os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except Exception:
            pass
    return None


async def _process_archive_from_message(
    bot: Client,
    user_client: Client,
    target_msg: Message,
    message: Message,
    status_msg: Message,
    user_id: int,
    source_name: str,
    backup_peer,
    skip_non_videos: bool = False,
):
    """
    Handle archive files (.zip/.rar) from forwarded messages.
    Downloads the archive, extracts media files, uploads them, and sends to user.

    If skip_non_videos is True, only videos will be extracted (photos skipped).
    """
    from pyrogram.types import InputMediaPhoto, InputMediaVideo

    doc = target_msg.document
    file_name = getattr(doc, "file_name", "archive.zip")
    file_size = getattr(doc, "file_size", 0)

    temp_dir = None

    try:
        # Create temp directory
        temp_dir = tempfile.mkdtemp(prefix="tg_archive_")
        archive_path = os.path.join(temp_dir, file_name)
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        # Download the archive file
        await safe_edit(status_msg, f"⬇️ Memuat turun arkib ({format_file_size(file_size)})...")

        downloaded_path = await user_client.download_media(
            target_msg,
            file_name=archive_path,
        )

        if not downloaded_path or not os.path.exists(archive_path):
            await safe_edit(status_msg, "❌ Gagal memuat turun arkib.")
            return

        # Check for cancellation after download
        if is_cancelled(user_id):
            await safe_edit(status_msg, "🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
            return

        # Count media files in archive
        media_type_msg = "video" if skip_non_videos else "media"
        await safe_edit(status_msg, f"📂 Mengimbas fail {media_type_msg} dalam arkib...")

        loop = asyncio.get_running_loop()
        total_files = await loop.run_in_executor(
            None, count_media_in_archive, archive_path, skip_non_videos
        )

        if total_files == 0:
            if skip_non_videos:
                await safe_edit(status_msg, "❌ Tiada fail video dijumpai dalam arkib.")
            else:
                await safe_edit(status_msg, "❌ Tiada fail media (foto/video) dijumpai dalam arkib.")
            return

        await safe_edit(status_msg, f"📤 Memuat naik {total_files} fail {media_type_msg} ke Telegram...")

        # Extract and upload each media file
        uploaded = []  # List of (backup_msg_id, kind, name, size)
        idx = 0

        async for mf in iter_extract_media(archive_path, extract_dir, skip_non_videos):
            idx += 1

            if is_cancelled(user_id):
                await safe_edit(status_msg, "🚫 **Proses dibatalkan!**\n\n💾 Folder sementara sedang dibersihkan...")
                return

            file_path = mf["path"]
            name = mf["name"]
            size = mf["size"]
            kind = mf["kind"]

            # Skip files > 2GB
            if size > MAX_FILE_SIZE:
                print(f"[Archive] Skipping {name}: {size} bytes exceeds limit")
                try:
                    os.remove(file_path)
                except Exception:
                    pass
                continue

            await safe_edit(status_msg, f"⬆️ Memuat naik {idx}/{total_files}: {name}...")

            try:
                # For videos, get metadata and thumbnail BEFORE streaming
                video_meta = None
                thumb_input_file = None
                if kind == "video":
                    video_meta = await _archive_get_video_metadata(file_path)
                    duration = video_meta.get("duration", 0) if video_meta else 0
                    thumb_raw = await _archive_generate_video_thumb(file_path, duration)
                    if thumb_raw and len(thumb_raw) > 100:
                        # Upload thumbnail using raw API
                        try:
                            from pyrogram.raw.functions.upload import SaveFilePart
                            thumb_file_id = random.randint(0, 2**63 - 1)
                            await bot.invoke(
                                SaveFilePart(
                                    file_id=thumb_file_id,
                                    file_part=0,
                                    bytes=thumb_raw,
                                )
                            )
                            thumb_input_file = InputFile(
                                id=thumb_file_id,
                                parts=1,
                                name="thumb.jpg",
                                md5_checksum=""
                            )
                        except Exception as e:
                            print(f"[Archive] Failed to upload thumbnail: {e}")

                # Create FileStreamer for the extracted file
                streamer = FileStreamer(file_path, name)
                input_file = await upload_stream(bot, streamer, name)

                # Determine media type and upload to backup group
                if kind == "photo":
                    media = InputMediaUploadedPhoto(file=input_file)
                elif kind == "video":
                    # Use actual video metadata
                    duration = video_meta.get("duration", 0) if video_meta else 0
                    width = video_meta.get("width", 0) if video_meta else 0
                    height = video_meta.get("height", 0) if video_meta else 0

                    video_attrs = [
                        DocumentAttributeVideo(
                            duration=duration,
                            w=width,
                            h=height,
                            supports_streaming=True,
                        ),
                        DocumentAttributeFilename(file_name=name),
                    ]
                    media = InputMediaUploadedDocument(
                        file=input_file,
                        mime_type=mime(name),
                        attributes=video_attrs,
                        thumb=thumb_input_file,
                    )
                else:
                    # Document
                    media = InputMediaUploadedDocument(
                        file=input_file,
                        mime_type=mime(name),
                        attributes=[DocumentAttributeFilename(file_name=name)],
                    )

                # Send to backup group with retry logic
                result = None
                for attempt in range(3):
                    try:
                        result = await bot.invoke(
                            SendMedia(
                                peer=backup_peer,
                                media=media,
                                message="",
                                random_id=random.randint(0, 2**63 - 1),
                            )
                        )
                        break
                    except FloodWait as fw:
                        wait = getattr(fw, "value", getattr(fw, "x", 10))
                        if wait > 120:
                            print(f"FloodWait {wait}s too long, skipping file")
                            break
                        print(f"FloodWait {wait}s on archive upload (attempt {attempt+1}/3)")
                        await asyncio.sleep(wait + 1)

                if result:
                    # Extract message ID from result
                    backup_msg_id = None
                    for upd in getattr(result, "updates", []):
                        if isinstance(upd, (UpdateNewMessage, UpdateNewChannelMessage)):
                            backup_msg_id = upd.message.id
                            break

                    if backup_msg_id:
                        uploaded.append((backup_msg_id, kind, name, size))

                        # Log to database
                        channel_id = str(BACKUP_GROUP_ID).replace("-100", "")
                        link = f"https://t.me/c/{channel_id}/{backup_msg_id}"
                        await log_forward(
                            message.from_user.username, backup_msg_id, size,
                            f"Archive/{file_name}/{name}", link
                        )

            except Exception as e:
                print(f"[Archive] Error uploading {name}: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Clean up extracted file immediately
                try:
                    os.remove(file_path)
                except Exception:
                    pass

            gc.collect()

        if not uploaded:
            await safe_edit(status_msg, "❌ Gagal memuat naik fail media dari arkib.")
            return

        # Group files by type for album sending
        photos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "photo"]
        videos = [(mid, k, n, s) for mid, k, n, s in uploaded if k == "video"]
        others = [(mid, k, n, s) for mid, k, n, s in uploaded if k not in ("photo", "video")]

        await safe_edit(status_msg, "⬆️ Menghantar ke anda...")

        # Helper to send as album chunks
        async def send_album_chunk(items, media_type):
            CHUNK = 8  # Telegram max 10 per album, use 8 for safety

            for i in range(0, len(items), CHUNK):
                chunk = items[i:i+CHUNK]

                # Fetch messages from backup group
                try:
                    msg_ids = [item[0] for item in chunk]
                    backup_msgs = await bot.get_messages(BACKUP_GROUP_ID, msg_ids)

                    if len(backup_msgs) == 1:
                        # Single file - use copy_message
                        for attempt in range(3):
                            try:
                                await bot.copy_message(
                                    chat_id=user_id,
                                    from_chat_id=BACKUP_GROUP_ID,
                                    message_id=backup_msgs[0].id
                                )
                                break
                            except FloodWait as fw:
                                wait = getattr(fw, "value", getattr(fw, "x", 10))
                                if wait > 60:
                                    break
                                await asyncio.sleep(wait + 1)
                    else:
                        # Multiple files - build album
                        media_list = []
                        for msg in backup_msgs:
                            if msg.photo:
                                media_list.append(InputMediaPhoto(msg.photo.file_id))
                            elif msg.video:
                                media_list.append(InputMediaVideo(msg.video.file_id))
                            elif msg.document:
                                # For documents, use copy_message individually
                                pass

                        if media_list:
                            for attempt in range(3):
                                try:
                                    await bot.send_media_group(chat_id=user_id, media=media_list)
                                    break
                                except FloodWait as fw:
                                    wait = getattr(fw, "value", getattr(fw, "x", 10))
                                    if wait > 60:
                                        break
                                    await asyncio.sleep(wait + 1)
                except Exception as e:
                    print(f"[Archive] Error sending album: {e}")

        # Send photos as album(s)
        if photos:
            await send_album_chunk(photos, "photo")

        # Send videos as album(s)
        if videos:
            await send_album_chunk(videos, "video")

        # Send other files individually
        for mid, kind, name, size in others:
            try:
                for attempt in range(3):
                    try:
                        await bot.copy_message(
                            chat_id=user_id,
                            from_chat_id=BACKUP_GROUP_ID,
                            message_id=mid
                        )
                        break
                    except FloodWait as fw:
                        wait = getattr(fw, "value", getattr(fw, "x", 10))
                        if wait > 60:
                            break
                        await asyncio.sleep(wait + 1)
            except Exception as e:
                print(f"[Archive] Error sending {name}: {e}")

        total_size = sum(s for _, _, _, s in uploaded)
        await safe_edit(
            status_msg,
            f"✅ **Selesai!**\n\n"
            f"📂 Arkib: `{file_name}`\n"
            f"📁 Fail: {len(uploaded)} media\n"
            f"📦 Jumlah: {format_file_size(total_size)}"
        )

    except Exception as e:
        print(f"[Archive] Error processing archive: {e}")
        import traceback
        traceback.print_exc()
        await safe_edit(status_msg, f"❌ Gagal memproses arkib: {e}")

    finally:
        # Clean up temp directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass
        gc.collect()


@app.on_message(filters.regex(LINK_PATTERN) & filters.private)
async def link_handler(client: Client, message: Message):
    user_id = message.from_user.id
    
    # Check if user already has an active process
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
    
    # Check if user has completed profile setup
    user_profile = await get_user_profile(user_id)
    if not user_profile:
        # Start profile setup
        await message.reply_text(
            "⚠️ **Profile belum lengkap!**\n\n"
            "Sila set profile anda terlebih dahulu.\n\n"
            "👇 **Pilih jantina anda:**",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("👨 Lelaki", callback_data="profile_gender_lelaki"),
                    InlineKeyboardButton("👩 Perempuan", callback_data="profile_gender_perempuan"),
                ]
            ])
        )
        user_profile_states[user_id] = {
            "step": ProfileStep.ASK_GENDER,
            "data": {}
        }
        return
    
    match = LINK_PATTERN.search(message.text)
    if not match:
        return

    # Determine chat_id based on link type
    if match.group(1):
        # Private channel link: t.me/c/<channel_id>/<msg_id>
        chat_id = int("-100" + match.group(1))
        is_public_link = False
        print(f"DEBUG: Private Link ID: {match.group(1)} -> Chat ID: {chat_id}")
    else:
        # Public channel/group link: t.me/<username>/<msg_id>
        chat_id = match.group(2)  # Use username string directly
        is_public_link = True
        print(f"DEBUG: Public Link Username: @{chat_id}")
    msg_id = int(match.group(3))

    print(f"DEBUG: Message ID: {msg_id}")
    print(f"DEBUG: Backup Group ID: {BACKUP_GROUP_ID}")

    # Mark user as having an active process
    active_user_processes[user_id] = asyncio.current_task()
    reset_cancel(user_id)

    status_msg = await message.reply_text(f"🔄 Sedang Diproses..")

    # 1. Get User Client
    user_client = await manager.get_client(user_id)
    if not user_client:
        active_user_processes.pop(user_id, None)
        await safe_edit(status_msg, "❌ Belum login.")
        return

    try:
        # 2. Fetch Message using User Client
        try:
            target_msg = await user_client.get_messages(chat_id, msg_id)
        except Exception as e:
            if is_public_link:
                # For public links, try resolving with @ prefix
                try:
                    await safe_edit(status_msg, f"🔄 Resolving @{chat_id}...")
                    target_msg = await user_client.get_messages(f"@{chat_id}", msg_id)
                except Exception as e2:
                    await safe_edit(status_msg, f"❌ Tidak dapat akses @{chat_id}: {e2}")
                    return
            else:
                await safe_edit(status_msg, f"🔄 Scanning... ({e})")
                
                found_chat = None
                debug_ids = []
                try:
                    async for dialog in user_client.get_dialogs():
                        d_id = dialog.chat.id
                        if len(debug_ids) < 5:
                            debug_ids.append(str(d_id))
                        
                        # Check exact match
                        if d_id == chat_id:
                            found_chat = dialog.chat
                            break
                        
                        # Check loose match (if ID format differs)
                        # e.g. if d_id is -100123 and raw_id is 123
                        raw_id = int(str(chat_id).replace("-100", ""))
                        if str(d_id).endswith(str(raw_id)):
                            found_chat = dialog.chat
                            chat_id = d_id # Update chat_id to the one found
                            break
                    
                    if not found_chat:
                        ids_sample = ", ".join(debug_ids)
                        await safe_edit(status_msg, f"❌ Chat {chat_id} tidak dijumpai. First 5 IDs: {ids_sample}")
                        return

                    # Try again with the found chat_id
                    target_msg = await user_client.get_messages(chat_id, msg_id)
                except Exception as e2:
                    await safe_edit(status_msg, f"❌ Error: {e2}")
                    return
        
        if not target_msg or not target_msg.media:
            await safe_edit(status_msg, "❌ Bukan media/file.")
            return

        # Get source chat name
        source_name = target_msg.chat.title if target_msg.chat and target_msg.chat.title else "Unknown"

        # Check if target is an archive file - extract and send media
        if target_msg.document:
            doc = target_msg.document
            doc_name = getattr(doc, "file_name", "") or ""
            if is_archive(doc_name):
                # Check if user wants to skip photos (only videos)
                skip_non_videos = "/skip" in message.text.lower()

                # Get backup group peer
                backup_peer = await get_backup_group_peer(client)
                if not backup_peer:
                    await safe_edit(status_msg, "❌ Backup group tidak dijumpai.")
                    return

                await _process_archive_from_message(
                    bot=client,
                    user_client=user_client,
                    target_msg=target_msg,
                    message=message,
                    status_msg=status_msg,
                    user_id=user_id,
                    source_name=source_name,
                    backup_peer=backup_peer,
                    skip_non_videos=skip_non_videos,
                )
                return

        # Check if this message is part of a media group (album)
        messages_to_process = []
        if target_msg.media_group_id:
            await safe_edit(status_msg, "📂 Detected media group, memproses semua files...")
            # Fetch all messages in the media group
            media_group_msgs = await user_client.get_media_group(chat_id, msg_id)
            messages_to_process = media_group_msgs
            print(f"DEBUG: Memproses {len(messages_to_process)} files")
        else:
            messages_to_process = [target_msg]

        # Check file size limit for all files before processing
        oversized_files = []
        for msg in messages_to_process:
            file_size = get_media_file_size(msg)
            if file_size > MAX_FILE_SIZE:
                media_obj = (msg.document or msg.video or msg.audio or 
                             msg.photo or msg.voice or msg.video_note or
                             msg.animation or msg.sticker)
                file_name = getattr(media_obj, "file_name", None) or "file"
                oversized_files.append((file_name, file_size))
        
        if oversized_files:
            error_msg = "❌ **File melebihi had saiz (600MB)!**\n\n"
            for fname, fsize in oversized_files:
                error_msg += f"📁 `{fname}`: {format_file_size(fsize)}\n"
            error_msg += f"\n⚠️ Had maksimum: 600MB"
            await safe_edit(status_msg, error_msg)
            return

        total_files = len(messages_to_process)
        backup_msg_ids = []
        
        # Check if this is a media group that should be sent together
        is_media_group = target_msg.media_group_id is not None and len(messages_to_process) > 1

        if is_media_group:
            # Process as media group - upload all files and send as album to both backup and user
            from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
            
            uploaded_media = []  # List of (file_id, media_type, file_size, metadata)
            
            for idx, msg_to_process in enumerate(messages_to_process, 1):
                # Check for cancellation
                if is_cancelled(user_id):
                    await safe_edit(status_msg, "🚫 **Proses dibatalkan!**")
                    return

                await safe_edit(status_msg, f"⬇️ Memuat naik {idx}/{total_files}...")
                
                # Upload each file and get file_id
                result = await upload_single_media_for_group(
                    client, user_client, msg_to_process, idx, total_files
                )
                
                if result:
                    uploaded_media.append(result)
            
            if not uploaded_media:
                await safe_edit(status_msg, "❌ Gagal memproses media/file.")
                return
            
            # Build InputMedia list using file_ids
            backup_media_list = []
            for file_id, media_type, file_size, metadata in uploaded_media:
                if media_type == "photo":
                    backup_media_list.append(InputMediaPhoto(file_id))
                elif media_type == "video":
                    backup_media_list.append(InputMediaVideo(
                        file_id,
                        duration=metadata.get("duration", 0),
                        width=metadata.get("width", 0),
                        height=metadata.get("height", 0)
                    ))
                elif media_type == "audio":
                    backup_media_list.append(InputMediaAudio(
                        file_id,
                        duration=metadata.get("duration", 0),
                        title=metadata.get("title", ""),
                        performer=metadata.get("performer", "")
                    ))
                else:
                    backup_media_list.append(InputMediaDocument(file_id))
            
            # Send as media group to backup group
            await safe_edit(status_msg, f"📤 ...")

            try:
                # Send to backup with FloodWait handling
                backup_msgs = None
                actual_group_id = await get_backup_group_actual_id()
                for attempt in range(3):
                    try:
                        backup_msgs = await client.send_media_group(
                            chat_id=actual_group_id,
                            media=backup_media_list
                        )
                        break
                    except FloodWait as fw:
                        wait = getattr(fw, "value", getattr(fw, "x", 10))
                        if wait > 300:  # Skip if wait > 5 minutes
                            print(f"FloodWait too long ({wait}s) for backup send, skipping...")
                            raise
                        print(f"FloodWait {wait}s on backup send (attempt {attempt+1}/3)")
                        await asyncio.sleep(wait + 1)

                if not backup_msgs:
                    raise Exception("Failed to send media group after retries")
                
                # Collect all message IDs
                for backup_msg in backup_msgs:
                    backup_msg_ids.append(backup_msg.id)
                
                # Log only the first message ID (represents the album)
                # Calculate total file size for the album
                total_album_size = sum(item[2] for item in uploaded_media)
                first_msg_id = backup_msgs[0].id
                channel_id = str(BACKUP_GROUP_ID).replace("-100", "")
                backup_message_link = f"https://t.me/c/{channel_id}/{first_msg_id}"
                await log_forward(message.from_user.username, first_msg_id, total_album_size, source_name, backup_message_link)
                
                # Forward as album to user
                await safe_edit(status_msg, f"⬆️ Menghantar album ke anda...")

                # Build media list using file_ids from backup messages (no re-upload)
                user_media_list = []
                for backup_msg in backup_msgs:
                    if backup_msg.photo:
                        user_media_list.append(InputMediaPhoto(backup_msg.photo.file_id))
                    elif backup_msg.video:
                        user_media_list.append(InputMediaVideo(backup_msg.video.file_id))
                    elif backup_msg.audio:
                        user_media_list.append(InputMediaAudio(backup_msg.audio.file_id))
                    elif backup_msg.document:
                        user_media_list.append(InputMediaDocument(backup_msg.document.file_id))

                # Try send_media_group first (preserves album), fallback to copy_message
                if user_media_list:
                    album_sent = False
                    for attempt in range(3):
                        try:
                            await client.send_media_group(chat_id=user_id, media=user_media_list)
                            album_sent = True
                            break
                        except FloodWait as fw:
                            wait = getattr(fw, "value", getattr(fw, "x", 10))
                            if wait > 60:  # If wait > 1 minute, fall back to copy_message
                                print(f"FloodWait {wait}s too long for album, using copy_message fallback...")
                                break
                            print(f"FloodWait {wait}s on user album (attempt {attempt+1}/3)")
                            await asyncio.sleep(wait + 1)

                    # Fallback: copy messages individually if album send failed
                    if not album_sent:
                        actual_from_id = await get_backup_group_actual_id()
                        
                        fallback_album = False
                        try:
                            from app.bot.session_manager import manager as local_manager
                            uc = await local_manager.get_client(user_id)
                            if uc:
                                me = await client.get_me()
                                valid_mids = [msg.id for msg in backup_msgs]
                                await uc.forward_messages(me.username, actual_from_id, valid_mids)
                                fallback_album = True
                        except Exception as e:
                            pass
                            
                        if not fallback_album:
                            for backup_msg in backup_msgs:
                                for copy_attempt in range(3):
                                    try:
                                        await client.copy_message(
                                            chat_id=user_id,
                                            from_chat_id=actual_from_id,
                                            message_id=backup_msg.id
                                        )
                                        break
                                    except FloodWait as fw:
                                        wait = getattr(fw, "value", getattr(fw, "x", 10))
                                        if wait > 300:
                                            print(f"FloodWait {wait}s too long for copy, skipping...")
                                            
                                            # User fallback mechanism
                                            try:
                                                from app.bot.session_manager import manager as local_manager
                                                uc = await local_manager.get_client(user_id)
                                                if uc:
                                                    me = await client.get_me()
                                                    await uc.forward_messages(
                                                        chat_id=me.username,
                                                        from_chat_id=actual_from_id,
                                                        message_ids=backup_msg.id
                                                    )
                                            except Exception as e:
                                                print(f"Fallback copy error: {e}")
                                                
                                            break
                                        await asyncio.sleep(wait + 1)
                            await asyncio.sleep(0.5)
                    
            except Exception as e:
                print(f"DEBUG: Failed to send media group to backup: {e}")
                import traceback
                traceback.print_exc()
                await safe_edit(status_msg, f"❌ Gagal menghantar album: {e}")
                return
        else:
            # Process single file individually
            for idx, msg_to_process in enumerate(messages_to_process, 1):
                # Check for cancellation
                if is_cancelled(user_id):
                    await safe_edit(status_msg, "🚫 **Proses dibatalkan!**")
                    return

                await safe_edit(status_msg, f"⬇️ Memproses {idx}/{total_files}...")
                
                # Process single media file
                backup_msg_id, file_name, file_size = await process_single_media(
                    client, user_client, msg_to_process, message, status_msg, idx, total_files
                )
                
                if backup_msg_id:
                    backup_msg_ids.append(backup_msg_id)
                    # Construct backup message link
                    channel_id = str(BACKUP_GROUP_ID).replace("-100", "")
                    backup_message_link = f"https://t.me/c/{channel_id}/{backup_msg_id}"
                    # Log each file
                    await log_forward(message.from_user.username, backup_msg_id, file_size, source_name, backup_message_link)

            if not backup_msg_ids:
                await safe_edit(status_msg, "❌ Gagal memproses media/file.")
                return

            # Forward individual messages to user
            await safe_edit(status_msg, f"⬆️ Mengirim {len(backup_msg_ids)} file(s) ke Anda...")
            actual_from_id = await get_backup_group_actual_id()
            for backup_msg_id in backup_msg_ids:
                success = False
                try:
                    await client.copy_message(
                        chat_id=user_id,
                        from_chat_id=actual_from_id,
                        message_id=backup_msg_id,
                        caption=""
                    )
                    success = True
                except Exception as e:
                    print(f"DEBUG: Failed to copy message {backup_msg_id}: {e}")
                    
                if not success:
                    try:
                        from app.bot.session_manager import manager as local_manager
                        uc = await local_manager.get_client(user_id)
                        if uc:
                            me = await client.get_me()
                            await uc.forward_messages(
                                chat_id=me.username,
                                from_chat_id=actual_from_id,
                                message_ids=backup_msg_id
                            )
                    except Exception as e:
                        print(f"Fallback msg {backup_msg_id} failed: {e}")
        
        await status_msg.delete()

    except asyncio.CancelledError:
        print(f"[Main] Process cancelled by user {user_id}")
    except Exception as e:
        await safe_edit(status_msg, f"....")
        import traceback
        traceback.print_exc()
    finally:
        # Always clear the active process flag when done
        active_user_processes.pop(user_id, None)
        reset_cancel(user_id)


async def upload_single_media_for_group(client: Client, user_client: Client, target_msg: Message,
                                         current_idx: int, total_count: int):
    """Upload a single media file to Telegram and return file_id with media type info.
    Returns (file_id, media_type, file_size, metadata) or None if failed."""
    try:
        from io import BytesIO
        import tempfile
        import os
        
        # Get media object and file info
        media_obj = (target_msg.document or target_msg.video or target_msg.audio or 
                     target_msg.photo or target_msg.voice or target_msg.video_note or
                     target_msg.animation or target_msg.sticker)
        file_size = getattr(media_obj, "file_size", 0)
        file_name = getattr(media_obj, "file_name", None) or "file"
        
        # For large files (>50MB), use temp file to avoid memory issues
        # For small files, use BytesIO (in-memory)
        MEMORY_THRESHOLD = 50 * 1024 * 1024  # 50MB
        
        temp_file_path = None
        media_source = None
        
        if file_size > MEMORY_THRESHOLD:
            # Use temporary file for large media
            suffix = os.path.splitext(file_name)[1] or ".tmp"
            temp_fd, temp_file_path = tempfile.mkstemp(suffix=suffix)
            os.close(temp_fd)
            await user_client.download_media(target_msg, file_name=temp_file_path)
            media_source = temp_file_path
        else:
            # Use in_memory download for small media (returns BytesIO)
            media_source = await user_client.download_media(target_msg, in_memory=True)
            if media_source:
                media_source.name = file_name if not target_msg.photo else "photo.jpg"
        
        # Send to backup group to get file_id, then delete
        sent_msg = None
        
        try:
            if target_msg.photo:
                sent_msg = await client.send_photo(BACKUP_GROUP_ID, photo=media_source)
                media_type = "photo"
                metadata = {}
                file_id = sent_msg.photo.file_id if sent_msg and sent_msg.photo else None
            elif target_msg.video:
                video = target_msg.video
                # Download thumbnail from source video
                thumb_data = None
                try:
                    if video.thumbs:
                        thumb_data = await user_client.download_media(
                            video.thumbs[0].file_id, in_memory=True
                        )
                except Exception as e:
                    print(f"DEBUG: Failed to download video thumbnail: {e}")
                    thumb_data = None
                
                sent_msg = await client.send_video(
                    BACKUP_GROUP_ID, 
                    video=media_source,
                    thumb=thumb_data,
                    duration=video.duration or 0,
                    width=video.width or 0,
                    height=video.height or 0,
                    supports_streaming=True
                )
                media_type = "video"
                metadata = {"duration": video.duration or 0, "width": video.width or 0, "height": video.height or 0}
                file_id = sent_msg.video.file_id if sent_msg and sent_msg.video else None
            elif target_msg.audio:
                audio = target_msg.audio
                sent_msg = await client.send_audio(
                    BACKUP_GROUP_ID,
                    audio=media_source,
                    duration=audio.duration or 0,
                    title=audio.title or "",
                    performer=audio.performer or ""
                )
                media_type = "audio"
                metadata = {"duration": audio.duration or 0, "title": audio.title or "", "performer": audio.performer or ""}
                file_id = sent_msg.audio.file_id if sent_msg and sent_msg.audio else None
            else:
                # Document
                sent_msg = await client.send_document(BACKUP_GROUP_ID, document=media_source)
                media_type = "document"
                metadata = {}
                file_id = sent_msg.document.file_id if sent_msg and sent_msg.document else None
            
            # Delete the temporary message from backup group
            if sent_msg:
                await client.delete_messages(BACKUP_GROUP_ID, sent_msg.id)
            
            if file_id:
                return (file_id, media_type, file_size, metadata)
            
            return None
            
        finally:
            # Clean up temp file if used
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
    except Exception as e:
        print(f"DEBUG: Error uploading media {current_idx}/{total_count}: {e}")
        import traceback
        traceback.print_exc()
        return None


async def process_single_media(client: Client, user_client: Client, target_msg: Message, 
                                original_message: Message, status_msg: Message, 
                                current_idx: int, total_count: int):
    """Process a single media file and upload to backup group. Returns (backup_msg_id, file_name, file_size)."""
    try:
        user_id = original_message.from_user.id
        
        # Create the streamer object
        media_obj = (target_msg.document or target_msg.video or target_msg.audio or 
                     target_msg.photo or target_msg.voice or target_msg.video_note or
                     target_msg.animation or target_msg.sticker)
        file_size = getattr(media_obj, "file_size", 0)
        file_name = getattr(media_obj, "file_name", None) or "file"

        streamer = MediaStreamer(user_client, target_msg, file_size)

        # Determine file name based on media type BEFORE uploading
        # Photos need a proper extension for Telegram to accept them
        if target_msg.photo:
            file_name = "photo.jpg"
        
        # Use manual upload to support async streaming
        input_file = await upload_stream(client, streamer, file_name)
        
        # Determine media type and create appropriate InputMedia
        if target_msg.photo:
            # Upload as photo
            media = InputMediaUploadedPhoto(
                file=input_file
            )
        elif target_msg.video:
            # Upload as video with proper attributes
            video = target_msg.video
            attributes = [
                DocumentAttributeVideo(
                    duration=video.duration or 0,
                    w=video.width or 0,
                    h=video.height or 0,
                    supports_streaming=True
                ),
                DocumentAttributeFilename(file_name=file_name)
            ]
            mime_type = video.mime_type or "video/mp4"
            
            # Download and upload thumbnail for video
            thumb_input_file = None
            try:
                if video.thumbs:
                    thumb_bytes_io = await user_client.download_media(
                        video.thumbs[0].file_id, in_memory=True
                    )
                    if thumb_bytes_io:
                        if isinstance(thumb_bytes_io, BytesIO):
                            thumb_bytes_io.seek(0)
                            thumb_raw = thumb_bytes_io.read()
                        else:
                            thumb_raw = thumb_bytes_io
                        
                        if thumb_raw and len(thumb_raw) > 0:
                            # Upload thumbnail using raw API SaveFilePart
                            thumb_file_id_raw = random.randint(0, 2**63 - 1)
                            await client.invoke(
                                SaveFilePart(
                                    file_id=thumb_file_id_raw,
                                    file_part=0,
                                    bytes=thumb_raw
                                )
                            )
                            thumb_input_file = InputFile(
                                id=thumb_file_id_raw,
                                parts=1,
                                name="thumb.jpg",
                                md5_checksum=""
                            )
            except Exception as e:
                print(f"DEBUG: Failed to download/upload thumbnail for raw API: {e}")
                import traceback
                traceback.print_exc()
                thumb_input_file = None
            
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes,
                thumb=thumb_input_file
            )
        elif target_msg.audio:
            # Upload as audio with proper attributes
            audio = target_msg.audio
            attributes = [
                DocumentAttributeAudio(
                    duration=audio.duration or 0,
                    title=audio.title or "",
                    performer=audio.performer or ""
                ),
                DocumentAttributeFilename(file_name=file_name)
            ]
            mime_type = audio.mime_type or "audio/mpeg"
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )
        elif target_msg.voice:
            # Upload as voice message
            voice = target_msg.voice
            attributes = [
                DocumentAttributeAudio(
                    duration=voice.duration or 0,
                    voice=True
                )
            ]
            mime_type = voice.mime_type or "audio/ogg"
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )
        elif target_msg.video_note:
            # Upload as video note (round video)
            video_note = target_msg.video_note
            attributes = [
                DocumentAttributeVideo(
                    duration=video_note.duration or 0,
                    w=video_note.length or 240,
                    h=video_note.length or 240,
                    round_message=True
                )
            ]
            mime_type = "video/mp4"
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )
        elif target_msg.animation:
            # Upload as GIF/animation
            animation = target_msg.animation
            attributes = [
                DocumentAttributeVideo(
                    duration=animation.duration or 0,
                    w=animation.width or 0,
                    h=animation.height or 0
                ),
                DocumentAttributeAnimated(),
                DocumentAttributeFilename(file_name=file_name or "animation.gif")
            ]
            mime_type = animation.mime_type or "video/mp4"
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )
        elif target_msg.sticker:
            # Upload as sticker
            sticker = target_msg.sticker
            attributes = [
                DocumentAttributeFilename(file_name=file_name or "sticker.webp")
            ]
            mime_type = sticker.mime_type or "image/webp"
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )
        else:
            # Upload as regular document
            attributes = [DocumentAttributeFilename(file_name=file_name)]
            mime_type = getattr(media_obj, "mime_type", "application/octet-stream")
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
            )

        # Resolve peer using helper function
        peer = await get_backup_group_peer(client)
        
        if not peer:
            return None, file_name, file_size

        # Send using raw API
        updates = await client.invoke(
            SendMedia(
                peer=peer,
                media=media,
                message="",
                random_id=random.randint(0, 2**63 - 1)
            )
        )

        # Extract message ID from updates
        backup_msg_id = None
        for update in updates.updates:
            if isinstance(update, (UpdateNewMessage, UpdateNewChannelMessage)):
                backup_msg_id = update.message.id
                break
        
        return backup_msg_id, file_name, file_size

    except Exception as e:
        print(f"DEBUG: Error processing media {current_idx}/{total_count}: {e}")
        import traceback
        traceback.print_exc()
        return None, "unknown", 0
