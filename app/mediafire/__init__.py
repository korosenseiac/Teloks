"""
mediafire/ — MediaFire download integration for the Telegram forwarder bot.

Exports the handler function and link pattern used by app.bot.main to
register the Pyrogram message handler.
"""
from app.mediafire.handler import mediafire_link_handler, MEDIAFIRE_LINK_PATTERN

__all__ = ["mediafire_link_handler", "MEDIAFIRE_LINK_PATTERN"]
