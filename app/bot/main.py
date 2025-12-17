import re
import random
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ChatPrivileges
from pyrogram.raw.functions.messages import SendMedia, SendMultiMedia
from pyrogram.raw.types import (
    InputMediaUploadedDocument, 
    InputMediaUploadedPhoto,
    InputSingleMedia,
    DocumentAttributeFilename, 
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeAnimated,
    UpdateNewMessage, 
    UpdateNewChannelMessage, 
    InputPeerChannel
)
from app.config import API_ID, API_HASH, BOT_TOKEN, BACKUP_GROUP_ID
from app.database.db import add_user, save_user_session, log_forward, get_user_session, save_backup_group_cache, get_backup_group_cache, get_user_profile
from app.bot.session_manager import manager
from app.utils.streamer import MediaStreamer, upload_stream
from app.bot.auth import handle_login_command, handle_auth_message, handle_login_callback, cancel_login, handle_main_menu_callback, handle_profile_callback, handle_profile_age_message, start_profile_setup
from app.bot.states import user_profile_states, ProfileStep
import asyncio

# Track active processes per user (user_id: True if processing)
active_user_processes = {}

# File size limit (600MB in bytes)
MAX_FILE_SIZE = 600 * 1024 * 1024  # 600MB

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

# Regex to extract chat_id and message_id from private links
# https://t.me/c/1234567890/123
LINK_PATTERN = re.compile(r"https://t\.me/c/(\d+)/(\d+)")

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
            "Copy dan paste mesej link dari private channel/group. Contohnya:\n"
            "`https://t.me/c/1234567890/123`\n\n"
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

    chat_id = int("-100" + match.group(1)) # Private channel IDs usually start with -100 when accessed via API
    msg_id = int(match.group(2))

    print(f"DEBUG: Link ID: {match.group(1)} -> Chat ID: {chat_id}")
    print(f"DEBUG: Backup Group ID: {BACKUP_GROUP_ID}")

    # Mark user as having an active process
    active_user_processes[user_id] = True

    status_msg = await message.reply_text(f"🔄 Sedang Diproses..")

    # 1. Get User Client
    user_client = await manager.get_client(user_id)
    if not user_client:
        active_user_processes.pop(user_id, None)
        await status_msg.edit("❌ Belum login.")
        return

    try:
        # 2. Fetch Message using User Client
        try:
            target_msg = await user_client.get_messages(chat_id, msg_id)
        except Exception as e:
            await status_msg.edit(f"🔄 Scanning... ({e})")
            
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
                    await status_msg.edit(f"❌ Chat {chat_id} tidak dijumpai. First 5 IDs: {ids_sample}")
                    return

                # Try again with the found chat_id
                target_msg = await user_client.get_messages(chat_id, msg_id)
            except Exception as e2:
                await status_msg.edit(f"❌ Error: {e2}")
                return
        
        if not target_msg or not target_msg.media:
            await status_msg.edit("❌ Bukan media/file.")
            return

        # Get source chat name
        source_name = target_msg.chat.title if target_msg.chat and target_msg.chat.title else "Unknown"

        # Check if this message is part of a media group (album)
        messages_to_process = []
        if target_msg.media_group_id:
            await status_msg.edit("📂 Detected media group, memproses semua files...")
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
            await status_msg.edit(error_msg)
            return

        total_files = len(messages_to_process)
        backup_msg_ids = []
        
        # Check if this is a media group that should be sent together
        is_media_group = target_msg.media_group_id is not None and len(messages_to_process) > 1

        if is_media_group:
            # Process media group - upload all files and send as album
            await status_msg.edit(f"📂 Memproses album ({total_files} files)...")
            
            backup_msg_ids, total_size = await process_media_group(
                client, user_client, messages_to_process, message, status_msg
            )
            
            if backup_msg_ids:
                # Log each file in the group
                channel_id = str(BACKUP_GROUP_ID).replace("-100", "")
                for backup_msg_id in backup_msg_ids:
                    backup_message_link = f"https://t.me/c/{channel_id}/{backup_msg_id}"
                    await log_forward(message.from_user.username, backup_msg_id, total_size // len(backup_msg_ids), source_name, backup_message_link)
        else:
            # Process each message individually (single file)
            for idx, target_msg in enumerate(messages_to_process, 1):
                await status_msg.edit(f"⬇️ Memproses {idx}/{total_files}...")
                
                # Process single media file
                backup_msg_id, file_name, file_size = await process_single_media(
                    client, user_client, target_msg, message, status_msg, idx, total_files
                )
                
                if backup_msg_id:
                    backup_msg_ids.append(backup_msg_id)
                    # Construct backup message link
                    # BACKUP_GROUP_ID format is -100XXXXXXXXXX, we need to extract the channel ID
                    channel_id = str(BACKUP_GROUP_ID).replace("-100", "")
                    backup_message_link = f"https://t.me/c/{channel_id}/{backup_msg_id}"
                    # Log each file
                    await log_forward(message.from_user.username, backup_msg_id, file_size, source_name, backup_message_link)

        if not backup_msg_ids:
            await status_msg.edit("❌ Gagal memproses media/file.")
            return

        # 4. Forward all to User (Clean)
        await status_msg.edit(f"⬆️ Mengirim {len(backup_msg_ids)} file(s) ke Anda...")
        
        if is_media_group and len(backup_msg_ids) > 1:
            # Forward as media group (album) to preserve grouping
            try:
                await client.copy_media_group(
                    chat_id=user_id,
                    from_chat_id=BACKUP_GROUP_ID,
                    message_id=backup_msg_ids[0],  # Any message ID from the group works
                    captions=""  # Strip all captions
                )
            except Exception as e:
                print(f"DEBUG: Failed to copy media group: {e}")
                # Fallback to individual copy if media group copy fails
                for backup_msg_id in backup_msg_ids:
                    try:
                        await client.copy_message(
                            chat_id=user_id,
                            from_chat_id=BACKUP_GROUP_ID,
                            message_id=backup_msg_id,
                            caption=""
                        )
                    except Exception as e2:
                        print(f"DEBUG: Failed to copy message {backup_msg_id}: {e2}")
        else:
            # Forward individual messages
            for backup_msg_id in backup_msg_ids:
                try:
                    await client.copy_message(
                        chat_id=user_id,
                        from_chat_id=BACKUP_GROUP_ID,
                        message_id=backup_msg_id,
                        caption="" # Strip caption
                    )
                except Exception as e:
                    print(f"DEBUG: Failed to copy message {backup_msg_id}: {e}")
        
        await status_msg.delete()

    except Exception as e:
        await status_msg.edit(f"....")
        import traceback
        traceback.print_exc()
    finally:
        # Always clear the active process flag when done
        active_user_processes.pop(user_id, None)


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
            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=mime_type,
                attributes=attributes
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


async def process_media_group(client: Client, user_client: Client, messages: list, 
                               original_message: Message, status_msg: Message):
    """
    Process a media group (album) and upload to backup group as a grouped album.
    Returns (list of backup_msg_ids, total_file_size).
    """
    try:
        total_files = len(messages)
        total_size = 0
        uploaded_media = []  # List of (InputSingleMedia, file_size) tuples
        
        # Step 1: Upload all files first
        for idx, target_msg in enumerate(messages, 1):
            await status_msg.edit(f"⬇️ Uploading {idx}/{total_files}...")
            
            media_obj = (target_msg.document or target_msg.video or target_msg.audio or 
                         target_msg.photo or target_msg.voice or target_msg.video_note or
                         target_msg.animation or target_msg.sticker)
            file_size = getattr(media_obj, "file_size", 0)
            file_name = getattr(media_obj, "file_name", None) or "file"
            total_size += file_size
            
            # Determine file name based on media type
            if target_msg.photo:
                file_name = f"photo_{idx}.jpg"
            
            # Create streamer and upload
            streamer = MediaStreamer(user_client, target_msg, file_size)
            input_file = await upload_stream(client, streamer, file_name)
            
            # Create appropriate InputMedia based on type
            if target_msg.photo:
                media = InputMediaUploadedPhoto(file=input_file)
            elif target_msg.video:
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
                media = InputMediaUploadedDocument(
                    file=input_file,
                    mime_type=mime_type,
                    attributes=attributes
                )
            elif target_msg.document:
                attributes = [DocumentAttributeFilename(file_name=file_name)]
                mime_type = getattr(media_obj, "mime_type", "application/octet-stream")
                media = InputMediaUploadedDocument(
                    file=input_file,
                    mime_type=mime_type,
                    attributes=attributes
                )
            elif target_msg.audio:
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
            else:
                # Fallback for other types
                attributes = [DocumentAttributeFilename(file_name=file_name)]
                mime_type = getattr(media_obj, "mime_type", "application/octet-stream")
                media = InputMediaUploadedDocument(
                    file=input_file,
                    mime_type=mime_type,
                    attributes=attributes
                )
            
            # Create InputSingleMedia for the multi-media message
            single_media = InputSingleMedia(
                media=media,
                random_id=random.randint(0, 2**63 - 1),
                message="",  # No caption
                entities=[]  # Required empty list for entities
            )
            uploaded_media.append(single_media)
        
        # Step 2: Get backup group peer
        peer = await get_backup_group_peer(client)
        if not peer:
            print("DEBUG: Failed to get backup group peer")
            return [], 0
        
        # Step 3: Send all media as a group using SendMultiMedia
        await status_msg.edit(f"📤 Sending album ({total_files} files)...")
        
        updates = await client.invoke(
            SendMultiMedia(
                peer=peer,
                multi_media=uploaded_media
            )
        )
        
        # Step 4: Extract message IDs from updates
        backup_msg_ids = []
        for update in updates.updates:
            if isinstance(update, (UpdateNewMessage, UpdateNewChannelMessage)):
                backup_msg_ids.append(update.message.id)
        
        print(f"DEBUG: Media group uploaded, got {len(backup_msg_ids)} message IDs")
        return backup_msg_ids, total_size
        
    except Exception as e:
        print(f"DEBUG: Error processing media group: {e}")
        import traceback
        traceback.print_exc()
        return [], 0