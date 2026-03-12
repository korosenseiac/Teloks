import sys
import asyncio
from pyrogram import idle

# Force unbuffered stdout so all print() calls appear immediately in server logs
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

from pyrogram.raw.functions.messages import SendMessage
from pyrogram.raw.types import InputPeerChannel
from app.bot.main import app as bot_app, get_backup_group_peer
from app.config import BACKUP_GROUP_ID
from app.torrent import cleanup_orphaned_torrent_dirs

# Clean up any leftover torrent temp dirs from previous crashes
cleanup_orphaned_torrent_dirs()

async def start_services():
    # Start the Bot
    print("Starting Bot...")
    await bot_app.start()
    
    # Validate backup group access
    print(f"Validating Backup Group ({BACKUP_GROUP_ID})...")
    peer = await get_backup_group_peer(bot_app)
    if peer:
        print(f"✅ Backup Group validated successfully!")
        # Send startup message using raw API with the cached peer
        try:
            from app.bot.main import backup_group_actual_id as actual_id
            
            # Use raw API to send message with the cached peer directly
            await bot_app.invoke(
                SendMessage(
                    peer=peer,
                    message=f"🤖 Bot Started/Restarted\n\n✅ Backup Group cached successfully!\n📍 Group ID: {actual_id}",
                    random_id=bot_app.rnd_id()
                )
            )
            print(f"✅ Startup message sent to backup group")
        except Exception as e:
            print(f"⚠️ Could not send startup message: {e}")
            print("   This is not critical - bot will work normally.")
    else:
        print(f"⚠️ WARNING: Could not access Backup Group ({BACKUP_GROUP_ID})")
        print("   The bot hasn't 'seen' the group yet.")
        print("   👉 Please send /checkgroup in the backup group to initialize it.")
        print("   After that, restart the bot and it will work automatically.")
    
    print("Bot is running...")
    await idle()

    # Stop Bot when idle ends
    await bot_app.stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_services())
