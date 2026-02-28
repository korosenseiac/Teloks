from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import SessionPasswordNeeded, PhoneCodeInvalid, PasswordHashInvalid, PhoneCodeExpired
from app.bot.states import user_login_states, LoginStep, user_profile_states, ProfileStep
from app.database.db import save_user_session, get_user_session, save_user_profile, get_user_profile
import asyncio

# We need a way to keep track of the temporary client for each user during login
# user_id -> Client
temp_clients = {}

def get_cancel_keyboard():
    """Get keyboard with just a cancel button."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_login")]
    ])

def get_gender_keyboard():
    """Get keyboard for gender selection."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👨 Lelaki", callback_data="profile_gender_lelaki"),
            InlineKeyboardButton("👩 Perempuan", callback_data="profile_gender_perempuan"),
        ]
    ])

def get_login_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("1", callback_data="login_1"),
            InlineKeyboardButton("2", callback_data="login_2"),
            InlineKeyboardButton("3", callback_data="login_3"),
        ],
        [
            InlineKeyboardButton("4", callback_data="login_4"),
            InlineKeyboardButton("5", callback_data="login_5"),
            InlineKeyboardButton("6", callback_data="login_6"),
        ],
        [
            InlineKeyboardButton("7", callback_data="login_7"),
            InlineKeyboardButton("8", callback_data="login_8"),
            InlineKeyboardButton("9", callback_data="login_9"),
        ],
        [
            InlineKeyboardButton("DEL", callback_data="login_del"),
            InlineKeyboardButton("0", callback_data="login_0"),
            InlineKeyboardButton("✅ Done", callback_data="login_done"),
        ],
        [
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_login"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

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

async def handle_main_menu_callback(client: Client, callback_query: CallbackQuery):
    """Handle main menu button callbacks."""
    user_id = callback_query.from_user.id
    action = callback_query.data.split("_")[1]
    
    if action == "login":
        await callback_query.answer()
        # Start login process
        await start_login_process(client, callback_query.message, user_id)
    
    elif action == "help":
        await callback_query.answer()
        help_text = (
            "📖 **Cara guna bot ni**\n\n"
            "1️⃣ **Login** - Mula-mula korang kena login account Telegram dalam bot ni dulu\n\n"
            "2️⃣ **Dapatkan Link** - Copy mesej link dari private channel/group. Contohnya:\n"
            "`https://t.me/c/1234567890/123`\n\n"
            "3️⃣ **Send Link** - Paste link yang korang copy kat sini. Nanti bot akan forward contentnya\n\n"
            "📦 **MediaFire** - Boleh juga hantar link MediaFire:\n"
            "`https://www.mediafire.com/file/abc123/file.zip/file`\n"
            "Bot akan muat turun dan hantar media (foto/video). "
            "Kalau fail ZIP/RAR, bot akan extract dan hantar media sahaja.\n\n"
        )
        existing_session = await get_user_session(user_id)
        is_logged_in = existing_session is not None
        
        await callback_query.message.edit_text(
            help_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Menu", callback_data="menu_back")]
            ])
        )
    
    elif action == "back":
        await callback_query.answer()
        existing_session = await get_user_session(user_id)
        is_logged_in = existing_session is not None
        
        if is_logged_in:
            welcome_text = (
                "👋 **Hye!**\n\n"
                "✅ Dah login.\n\n"
                "**Cara guna:**\n"
                "Copy dan paste mesej link dari private channel/group. Contohnya:\n"
                "`https://t.me/c/1234567890/123`\n\n"
                "Nanti bot akan forward contentnya."
            )
        else:
            welcome_text = (
                "👋 **Hye!**\n\n"
                "Bot ni boleh forward restricted content dari private channels/group untuk korang.\n\n"
                "**Untuk mula:**\n"
                "Korang perlu login dengan akaun Telegram dalam bot ni.\n\n"
                "👇 **Klik butang kat bawah untuk login:**"
            )
        
        await callback_query.message.edit_text(
            welcome_text,
            reply_markup=get_main_menu_keyboard(is_logged_in)
        )

async def start_login_process(client: Client, message: Message, user_id: int):
    """Start the login process (can be called from button or command)."""
    # Check if already logged in
    existing_session = await get_user_session(user_id)
    
    user_login_states[user_id] = {
        "step": LoginStep.ASK_API_ID,
        "data": {}
    }
    
    if existing_session:
        await message.reply_text(
            "⚠️ **Dah login!**\n\n"
            "Login baharu akan replace session lama.\n"
            "Hantar **API ID** disini.\n\n"
            "👇 Tekan butang untuk cancel:",
            reply_markup=get_cancel_keyboard()
        )
    else:
        await message.reply_text(
            "🔐 **Login**\n\n"
            "Follow video tutorial [disini](https://t.me/telokschannel/14). Korang perlukan **API ID** dan **API HASH** dari my.telegram.org.\n\n"
            "1️⃣ Paste **API ID** dan click Send.\n\n"
            "👇 Tekan butang untuk cancel:",
            reply_markup=get_cancel_keyboard()
        )

async def handle_login_command(client: Client, message: Message):
    user_id = message.from_user.id
    await start_login_process(client, message, user_id)

async def cancel_login(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in user_login_states:
        if user_id in temp_clients:
            await temp_clients[user_id].disconnect()
            del temp_clients[user_id]
        del user_login_states[user_id]
        
        existing_session = await get_user_session(user_id)
        is_logged_in = existing_session is not None
        
        await message.reply_text(
            "❌ Login proses dibatalkan.",
            reply_markup=get_main_menu_keyboard(is_logged_in)
        )
    else:
        existing_session = await get_user_session(user_id)
        is_logged_in = existing_session is not None
        
        await message.reply_text(
            "ℹ️ Tidak di dalam proses login.",
            reply_markup=get_main_menu_keyboard(is_logged_in)
        )

async def cancel_login_callback(client: Client, callback_query: CallbackQuery):
    """Handle cancel button callback."""
    user_id = callback_query.from_user.id
    await callback_query.answer("Login dibatalkan")
    
    if user_id in user_login_states:
        if user_id in temp_clients:
            await temp_clients[user_id].disconnect()
            del temp_clients[user_id]
        del user_login_states[user_id]
    
    existing_session = await get_user_session(user_id)
    is_logged_in = existing_session is not None
    
    if is_logged_in:
        welcome_text = (
            "❌ **Login dibatalkan.**\n\n"
            "✅ Session lama masih aktif.\n\n"
            "**Cara guna:**\n"
            "Copy dan paste mesej link dari private channel/group. Contohnya:\n"
            "`https://t.me/c/1234567890/123`"
        )
    else:
        welcome_text = (
            "❌ **Login dibatalkan.**\n\n"
            "Anda perlu login untuk menggunakan bot ini.\n\n"
            "👇 **Tekan butang dibawah untuk login:**"
        )
    
    await callback_query.message.edit_text(
        welcome_text,
        reply_markup=get_main_menu_keyboard(is_logged_in)
    )

async def handle_auth_message(client: Client, message: Message):
    user_id = message.from_user.id
    state = user_login_states.get(user_id)
    
    if not state:
        return False # Not handling auth

    step = state["step"]
    text = message.text.strip()

    if text == "/cancel":
        if user_id in temp_clients:
            await temp_clients[user_id].disconnect()
            del temp_clients[user_id]
        del user_login_states[user_id]
        
        existing_session = await get_user_session(user_id)
        is_logged_in = existing_session is not None
        
        await message.reply_text(
            "❌ Proses login dibatalkan.",
            reply_markup=get_main_menu_keyboard(is_logged_in)
        )
        return True

    try:
        if step == LoginStep.ASK_API_ID:
            if not text.isdigit():
                await message.reply_text(
                    "⚠️ API ID mesti nombor. Sila cuba lagi.",
                    reply_markup=get_cancel_keyboard()
                )
                return True
            state["data"]["api_id"] = int(text)
            state["step"] = LoginStep.ASK_API_HASH
            await message.reply_text(
                "2️⃣ Bagus! Sekarang hantar **API HASH**.",
                reply_markup=get_cancel_keyboard()
            )
        elif step == LoginStep.ASK_API_HASH:
            state["data"]["api_hash"] = text
            state["step"] = LoginStep.ASK_PHONE
            await message.reply_text(
                "3️⃣ Hantar **Nombor Telefon**",
                reply_markup=get_cancel_keyboard()
            )
        elif step == LoginStep.ASK_PHONE:
            # Sanitize phone number
            phone_number = text.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            state["data"]["phone"] = phone_number
            
            await message.reply_text(
                f"🔄 Menghubungkan ke server Telegram dengan {phone_number}...",
                reply_markup=get_cancel_keyboard()
            )
            
            # Initialize Temp Client
            temp_client = Client(
                name=f"login_{user_id}",
                api_id=state["data"]["api_id"],
                api_hash=state["data"]["api_hash"],
                in_memory=True
            )
            await temp_client.connect()
            temp_clients[user_id] = temp_client

            try:
                sent_code = await temp_client.send_code(phone_number)
                state["data"]["phone_code_hash"] = sent_code.phone_code_hash
                state["step"] = LoginStep.ASK_CODE
                state["data"]["temp_code"] = "" # Initialize temp code
                
                await message.reply_text(
                    "4️⃣ **Last Part**\n"
                    "Masukkan kod yang Telegram bagi.\n\n"
                    "⚠️ **JANGAN HANTAR KOD SEBAGAI TEKS** (Nanti Error).\n"
                    "👇 **Guna butang bawah ni untuk masukkan kod:**\n"
                    "Kod: `_ _ _ _ _`",
                    reply_markup=get_login_keyboard()
                )
            except Exception as e:
                await message.reply_text(
                    f"❌ Error hantar kod: {e}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Try Again", callback_data="menu_login")],
                        [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
                    ])
                )
                await temp_client.disconnect()
                del temp_clients[user_id]
                del user_login_states[user_id]

        elif step == LoginStep.ASK_CODE:
            # Delete the user's message to prevent code from being sent as text
            try:
                await message.delete()
            except:
                pass  # In case bot doesn't have delete permission
            
            # Update the display with a warning
            current_code = state["data"].get("temp_code", "")
            display = " ".join(list(current_code)) + " _" * (5 - len(current_code)) if current_code else "_ _ _ _ _"
            
            await message.reply_text(
                "4️⃣ **Last Part**\n"
                "Masukkan kod yang Telegram bagi.\n\n"
                "⚠️ **JANGAN HANTAR KOD SEBAGAI TEKS** (Nanti Error).\n"
                "👇 **Guna butang bawah ni untuk masukkan kod:**\n"
                f"Kod: `{display}`\n\n"
                "🚫 **Mesej anda telah dipadam.**\n"
                "Sila guna butang sahaja!",
                reply_markup=get_login_keyboard()
            )

        elif step == LoginStep.ASK_PASSWORD:
            temp_client = temp_clients.get(user_id)
            try:
                await temp_client.check_password(password=text)
                await finalize_login(user_id, message, temp_client, state)
            except PasswordHashInvalid:
                await message.reply_text(
                    "❌ Password salah. Sila cuba lagi.",
                    reply_markup=get_cancel_keyboard()
                )
            except Exception as e:
                await message.reply_text(
                    f"❌ Error: {e}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔄 Try Again", callback_data="menu_login")],
                        [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
                    ])
                )
                await cleanup_login(user_id)

    except Exception as e:
        await message.reply_text(
            f"❌ Error: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="menu_login")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
            ])
        )
        await cleanup_login(user_id)

    return True

async def handle_login_callback(client: Client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    state = user_login_states.get(user_id)
    
    if not state or state["step"] != LoginStep.ASK_CODE:
        await callback_query.answer("Sesi tamat atau keadaan tidak sah.", show_alert=True)
        return

    data = callback_query.data
    action = data.split("_")[1]
    
    current_code = state["data"].get("temp_code", "")
    
    if action.isdigit():
        if len(current_code) < 5:
            current_code += action
            state["data"]["temp_code"] = current_code
            await update_code_display(callback_query, current_code)
    
    elif action == "del":
        current_code = current_code[:-1]
        state["data"]["temp_code"] = current_code
        await update_code_display(callback_query, current_code)
        
    elif action == "done":
        if len(current_code) != 5:
             await callback_query.answer("Kod mesti 5 digit.", show_alert=True)
             return
        
        # Proceed with login
        await callback_query.message.edit_text("🔄 Memeriksa kod...")
        await process_login_code(client, callback_query.message, user_id, current_code)

async def update_code_display(callback_query, code):
    display = " ".join(list(code)) + " _" * (5 - len(code))
    try:
        await callback_query.message.edit_text(
            "4️⃣ **Last Part**\n"
            "Masukkan kod yang Telegram bagi.\n\n"
            "⚠️ **JANGAN HANTAR KOD SEBAGAI TEKS** (Nanti Error).\n"
            "👇 **Guna butang bawah ni untuk masukkan kod:**\n"
            f"Kod: `{display}`",
            reply_markup=get_login_keyboard()
        )
    except:
        pass # Message not modified

async def process_login_code(client, message, user_id, code):
    state = user_login_states.get(user_id)
    temp_client = temp_clients.get(user_id)
    
    try:
        await temp_client.sign_in(
            phone_number=state["data"]["phone"],
            phone_code_hash=state["data"]["phone_code_hash"],
            phone_code=code
        )
        await finalize_login(user_id, message, temp_client, state)
        
    except SessionPasswordNeeded:
        state["step"] = LoginStep.ASK_PASSWORD
        await message.reply_text(
            "🔐 Two-Step Verification. Masukkan **Password**",
            reply_markup=get_cancel_keyboard()
        )
    
    except PhoneCodeInvalid:
        state["data"]["temp_code"] = "" # Reset
        await message.reply_text(
            "❌ Kod tidak sah. Sila cuba lagi menggunakan butang.", 
            reply_markup=get_login_keyboard()
        )
    except PhoneCodeExpired:
        await message.reply_text(
            "❌ Kod dah expired.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="menu_login")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_back")]
            ])
        )
        await cleanup_login(user_id)
    
    except Exception as e:
        await message.reply_text(
            f"❌ Error: {e}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data="menu_login")],
                [InlineKeyboardButton("� Back to Menu", callback_data="menu_back")]
            ])
        )
        await cleanup_login(user_id)

async def finalize_login(user_id, message, temp_client, state):
    session_string = await temp_client.export_session_string()
    
    await save_user_session(
        user_id, 
        session_string, 
        state["data"]["api_id"], 
        state["data"]["api_hash"]
    )
    
    await cleanup_login(user_id)
    
    # Start profile setup
    await start_profile_setup(user_id, message)

async def cleanup_login(user_id):
    if user_id in temp_clients:
        await temp_clients[user_id].disconnect()
        del temp_clients[user_id]
    if user_id in user_login_states:
        del user_login_states[user_id]

# ========== Profile Setup Functions ==========

async def start_profile_setup(user_id, message):
    """Start the profile setup process (gender, then age)."""
    user_profile_states[user_id] = {
        "step": ProfileStep.ASK_GENDER,
        "data": {}
    }
    
    await message.reply_text(
        "✅ **Login Berjaya!**\n\n"
        "Sebelum guna bot ni, sila set profile anda.\n\n"
        "👇 **Pilih jantina anda:**",
        reply_markup=get_gender_keyboard()
    )

async def handle_profile_callback(client: Client, callback_query: CallbackQuery):
    """Handle profile setup callbacks (gender selection)."""
    user_id = callback_query.from_user.id
    state = user_profile_states.get(user_id)
    
    if not state:
        await callback_query.answer("Sesi tamat. Sila login semula.", show_alert=True)
        return
    
    data = callback_query.data
    
    if data.startswith("profile_gender_"):
        gender = data.replace("profile_gender_", "")
        state["data"]["gender"] = gender
        state["step"] = ProfileStep.ASK_AGE
        
        await callback_query.answer(f"Jantina: {gender.capitalize()}")
        await callback_query.message.edit_text(
            f"✅ **Jantina:** {gender.capitalize()}\n\n"
            "👇 **Sekarang, masukkan umur anda (10-100):**\n"
            "Hantar sebagai mesej teks."
        )

async def handle_profile_age_message(client: Client, message: Message):
    """Handle age input from user during profile setup."""
    user_id = message.from_user.id
    state = user_profile_states.get(user_id)
    
    if not state or state["step"] != ProfileStep.ASK_AGE:
        return False  # Not handling profile age
    
    text = message.text.strip()
    
    # Validate age
    if not text.isdigit():
        await message.reply_text(
            "⚠️ **Umur mesti nombor!**\n\n"
            "Sila masukkan nombor antara 10-100."
        )
        return True
    
    age = int(text)
    if age < 10 or age > 100:
        await message.reply_text(
            "⚠️ **Umur tidak sah!**\n\n"
            "Sila masukkan nombor antara 10-100."
        )
        return True
    
    # Save profile
    gender = state["data"]["gender"]
    await save_user_profile(user_id, gender, age)
    
    # Cleanup state
    del user_profile_states[user_id]
    
    await message.reply_text(
        "Dah boleh start guna bot ni.\n\n"
        "**Cara guna:**\n"
        "Copy dan paste mesej link dari private channel/group. Contohnya:\n"
        "`https://t.me/c/1234567890/123`",
        reply_markup=get_main_menu_keyboard(is_logged_in=True)
    )
    
    return True

async def check_user_profile_complete(user_id):
    """Check if user has completed profile setup (gender and age)."""
    profile = await get_user_profile(user_id)
    return profile is not None
