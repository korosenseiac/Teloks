import asyncio
import logging
from pyrogram.types import Message
from pyrogram.errors import FloodWait, MessageNotModified

logger = logging.getLogger(__name__)

async def safe_edit(message: Message, text: str, reply_markup=None, **kwargs):
    """
    Safely edit a message, ignoring MessageNotModified and handling FloodWait.
    If FloodWait is short (<= 10s), it sleeps and retries.
    If FloodWait is long (> 10s), it skips the edit to avoid stalling the process.
    """
    if not message:
        return
        
    try:
        return await message.edit_text(text, reply_markup=reply_markup, **kwargs)
    except MessageNotModified:
        # The message content is the same, no need to update
        pass
    except FloodWait as e:
        # If wait is short, we can afford to sleep.
        # If it's long (e.g. 334s), status updates are non-critical, so skip it.
        if e.value <= 10:
            logger.warning(f"FloodWait of {e.value}s encountered. Sleeping and retrying...")
            await asyncio.sleep(e.value + 1)
            try:
                return await message.edit_text(text, reply_markup=reply_markup, **kwargs)
            except Exception:
                pass
        else:
            logger.warning(f"Long FloodWait of {e.value}s encountered. Skipping this status update.")
    except Exception as e:
        logger.error(f"Failed to edit message: {e}")
