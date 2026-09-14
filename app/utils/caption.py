"""
utils/caption.py — Video caption utilities and prompt flow management for Direct & Torrent.

Handles:
- Filename-to-caption generation with extension removal & word/sentence exclusion
- Supported Telegram HTML formatting & styling
- Entity parsing for raw MTProto SendMedia
- Keyboards and in-memory state management for user prompt flow
"""
from __future__ import annotations

import html
import inspect
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ---------------------------------------------------------------------------
# In-memory state storage for caption prompt flow
# user_id -> {
#     "type": str,          # "direct", "torrent", "torrent_file"
#     "step": str,          # CaptionStep.ASK_CAPTION or CaptionStep.ASK_EXCLUDE
#     "message": Message,   # original incoming message
#     "status_msg": Message,# prompt message sent to user
#     "created_at": float,  # timestamp for TTL cleanup
#     "data": dict,         # handler-specific params (url, skip_non_videos, etc.)
# }
# ---------------------------------------------------------------------------
user_caption_states: Dict[int, Dict[str, Any]] = {}

STATE_TTL_SECONDS = 900  # 15 minutes TTL for uncompleted prompt interactions


def set_caption_state(
    user_id: int,
    flow_type: str,
    message: Any,
    status_msg: Any,
    step: str = "ASK_CAPTION",
    **data: Any,
) -> None:
    """Store or update pending caption configuration state for a user."""
    _cleanup_expired_states()
    entry = {
        "source_type": flow_type,
        "type": flow_type,
        "step": step,
        "message": message,
        "status_msg": status_msg,
        "created_at": time.time(),
        "data": data,
    }
    entry.update(data)
    user_caption_states[user_id] = entry


def set_caption_thumb(user_id: int, raw: Optional[bytes]) -> bool:
    """Attach a user-provided thumbnail to the pending caption state.

    The image is sent by the user as a separate message while the caption
    prompt is on screen (see the torrent thumbnail interceptor in
    ``app/bot/main.py``). It is stored on the live state entry so the
    download callbacks can forward it to the upload pipeline.

    Returns True when a pending state existed and the thumbnail was stored.
    """
    state = get_caption_state(user_id)
    if not state:
        return False
    state["user_thumb_raw"] = raw
    state.setdefault("data", {})["user_thumb_raw"] = raw
    return True


def get_caption_state(user_id: int) -> Optional[Dict[str, Any]]:
    """Get pending caption state for user if not expired."""
    state = user_caption_states.get(user_id)
    if not state:
        return None
    if time.time() - state.get("created_at", 0) > STATE_TTL_SECONDS:
        user_caption_states.pop(user_id, None)
        return None
    return state


def clear_caption_state(user_id: int) -> Optional[Dict[str, Any]]:
    """Remove and return pending caption state for user."""
    return user_caption_states.pop(user_id, None)


def _cleanup_expired_states() -> None:
    """Prune stale caption states."""
    now = time.time()
    expired = [
        uid for uid, s in user_caption_states.items()
        if now - s.get("created_at", 0) > STATE_TTL_SECONDS
    ]
    for uid in expired:
        user_caption_states.pop(uid, None)


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def get_caption_choice_keyboard() -> InlineKeyboardMarkup:
    """Keyboard asking if user wants to caption video files with their filename."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📝 Ya (Letak Caption)", callback_data="cap_choose_yes"),
            InlineKeyboardButton("❌ Tidak Perlu", callback_data="cap_choose_no"),
        ],
        [
            InlineKeyboardButton("🚫 Batal", callback_data="cap_cancel"),
        ]
    ])


def get_exclude_keyboard() -> InlineKeyboardMarkup:
    """Keyboard asking if user wants to exclude any words or proceed as-is."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⏩ Teruskan Tanpa Buang", callback_data="cap_exclude_skip"),
        ],
        [
            InlineKeyboardButton("🚫 Batal", callback_data="cap_cancel"),
        ]
    ])


# ---------------------------------------------------------------------------
# Caption generation & styling
# ---------------------------------------------------------------------------

def generate_video_caption(file_name: str, exclude_words: Optional[str] = None) -> str:
    """
    Format a video file name into a styled Telegram caption:
    1. Removes file extension (e.g., .mkv, .mp4) whether user excludes words or not.
    2. Excludes user-specified word or sentence (case-insensitive) if provided.
    3. Cleans excess whitespace, leading/trailing hyphens, underscores, dots.
    4. Applies Telegram-supported styling & decoration (HTML bold with 🎬 emoji).
    """
    # 1. Always remove file extension
    base_name, _ = os.path.splitext(file_name)

    # 2. Exclude user-specified word or sentence
    if exclude_words:
        # Support comma-separated phrases or single sentence/phrase
        terms = [t.strip() for t in exclude_words.split(",") if t.strip()]
        if not terms:
            terms = [exclude_words.strip()]
        for term in terms:
            if term:
                base_name = re.sub(re.escape(term), "", base_name, flags=re.IGNORECASE)

    # 3. Clean up formatting
    # Collapse multiple spaces into single space
    cleaned = re.sub(r"\s+", " ", base_name).strip()
    # Strip leading or trailing dashes, underscores, dots, or spaces
    cleaned = re.sub(r"^[\s\-_.]+|[\s\-_.]+$", "", cleaned).strip()

    # Fallback if title was completely wiped out by exclusion
    if not cleaned:
        cleaned, _ = os.path.splitext(file_name)
        cleaned = cleaned.strip()

    # 4. Telegram HTML styling and decoration
    escaped_title = html.escape(cleaned)

    # Ensure length adheres to Telegram caption limit (1024 chars)
    if len(escaped_title) > 950:
        escaped_title = escaped_title[:950] + "..."

    return f"🎬 <b>{escaped_title}</b>"


async def parse_caption(client: Client, caption: Optional[str]) -> Tuple[str, Optional[List]]:
    """
    Parse an HTML caption string using Pyrogram's parser into plain text and raw entities.
    Used for raw MTProto SendMedia calls to ensure formatted styling displays properly.
    """
    if not caption:
        return "", None

    try:
        try:
            from pyrogram.enums import ParseMode
            mode = ParseMode.HTML
        except ImportError:
            mode = "html"

        if hasattr(client, "parser") and hasattr(client.parser, "parse"):
            res = client.parser.parse(caption, mode)
            if inspect.isawaitable(res):
                res = await res
            if isinstance(res, dict):
                return res.get("message", caption), res.get("entities")
    except Exception as e:
        print(f"[Caption] parse_caption error: {e}")

    return caption, None
