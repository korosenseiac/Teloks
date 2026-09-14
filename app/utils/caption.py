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
import uuid
from typing import Any, Dict, List, Optional, Tuple

from pyrogram import Client
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ---------------------------------------------------------------------------
# In-memory state storage for the caption prompt flow
# ---------------------------------------------------------------------------
# A user may have several jobs pending at once (up to
# MAX_CONCURRENT_PROCESSES), so every prompt owns its own entry. The key is the
# process-slot id of the job that created the prompt, which doubles as the id
# embedded in the cap_* callback data — that is what stops two prompts from
# overwriting each other.
#
# sid -> {
#     "sid": str,           # process slot id (also the caption-state key)
#     "user_id": int,       # owner
#     "type": str,          # "direct", "torrent", "torrent_file"
#     "source_type": str,   # same as "type"
#     "step": str,          # CaptionStep.ASK_CAPTION or CaptionStep.ASK_EXCLUDE
#     "message": Message,   # original incoming message
#     "status_msg": Message,# prompt message sent to user
#     "created_at": float,  # timestamp for TTL cleanup
#     "data": dict,         # handler-specific params (url, skip_non_videos, etc.)
# }
# ---------------------------------------------------------------------------
user_caption_states: Dict[str, Dict[str, Any]] = {}

# user_id -> [sid, ...] in creation order (oldest first)
_user_state_ids: Dict[int, List[str]] = {}

STATE_TTL_SECONDS = 900  # 15 minutes TTL for uncompleted prompt interactions


def set_caption_state(
    user_id: int,
    flow_type: str,
    message: Any,
    status_msg: Any,
    step: str = "ASK_CAPTION",
    sid: Optional[str] = None,
    **data: Any,
) -> str:
    """Store a pending caption prompt and return its key (the ``sid``).

    Pass the job's process-slot id as *sid* so the prompt, the reserved
    concurrency slot and the ``cap_*`` / ``job_cancel_*`` buttons all share one
    identity.
    """
    _cleanup_expired_states()
    state_id = sid or uuid.uuid4().hex[:10]
    entry = {
        "sid": state_id,
        "user_id": user_id,
        "source_type": flow_type,
        "type": flow_type,
        "step": step,
        "message": message,
        "status_msg": status_msg,
        "created_at": time.time(),
        "data": data,
    }
    entry.update(data)
    user_caption_states[state_id] = entry
    ids = _user_state_ids.setdefault(user_id, [])
    if state_id not in ids:
        ids.append(state_id)
    return state_id


def list_caption_states(user_id: int) -> List[Dict[str, Any]]:
    """All non-expired caption states of a user, oldest first."""
    _cleanup_expired_states()
    return [
        user_caption_states[sid]
        for sid in list(_user_state_ids.get(user_id, []))
        if sid in user_caption_states
    ]


def has_caption_state(user_id: int) -> bool:
    """True when the user has at least one pending caption prompt."""
    return bool(list_caption_states(user_id))


def _entry_is_expired(state: Dict[str, Any]) -> bool:
    return time.time() - state.get("created_at", 0) > STATE_TTL_SECONDS


def get_caption_state(user_id: int, sid: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Pending state by *sid*, or the user's only pending state when unambiguous.

    With several jobs pending the caller must pass the ``sid`` from the callback
    data — returning the wrong prompt's options would start the wrong job.
    """
    if sid:
        state = user_caption_states.get(sid)
        if not state:
            return None
        if _entry_is_expired(state):
            clear_caption_state(state.get("user_id", user_id), sid)
            return None
        return state

    states = list_caption_states(user_id)
    return states[0] if len(states) == 1 else None


def clear_caption_state(user_id: int, sid: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Remove and return a pending state (the only one when ``sid`` is None)."""
    if sid:
        state = user_caption_states.pop(sid, None)
        if state is not None:
            _forget_state(state.get("user_id", user_id), sid)
        return state

    states = list_caption_states(user_id)
    if len(states) == 1:
        return clear_caption_state(user_id, states[0]["sid"])
    return None


def set_caption_thumb(user_id: int, raw: Optional[bytes], sid: Optional[str] = None) -> bool:
    """Attach a user-provided thumbnail to the pending caption state.

    The image is sent by the user as a separate message while the caption
    prompt is on screen (see the torrent thumbnail interceptor in
    ``app/bot/main.py``). It is stored on the live state entry so the
    download callbacks can forward it to the upload pipeline.

    Returns True when a pending state existed and the thumbnail was stored.
    """
    state = get_caption_state(user_id, sid)
    if not state:
        return False
    state["user_thumb_raw"] = raw
    state.setdefault("data", {})["user_thumb_raw"] = raw
    return True


def parse_caption_callback(data: Optional[str]) -> Tuple[str, Optional[str]]:
    """Split ``"cap_choose_yes:<sid>"`` into ``("cap_choose_yes", "<sid>")``."""
    action, _, sid = (data or "").partition(":")
    return action, (sid or None)


def _forget_state(user_id: int, sid: str) -> None:
    ids = _user_state_ids.get(user_id)
    if not ids:
        return
    if sid in ids:
        ids.remove(sid)
    if not ids:
        _user_state_ids.pop(user_id, None)


def _cleanup_expired_states() -> None:
    """Prune stale caption states and free the job slots they were holding."""
    for sid, state in list(user_caption_states.items()):
        if not _entry_is_expired(state):
            continue
        user_id = state.get("user_id")
        clear_caption_state(user_id, sid)
        # That job never started, so hand its reserved slot back.
        try:
            from app.bot import process_registry

            slot = process_registry.get_slot(sid)
            if slot is not None and not slot.started:
                process_registry.release_process(slot)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def _cap_callback(action: str, sid: Optional[str]) -> str:
    """``cap_choose_yes`` + sid -> ``cap_choose_yes:<sid>`` (plain when no sid)."""
    return f"{action}:{sid}" if sid else action


def get_caption_choice_keyboard(sid: Optional[str] = None) -> InlineKeyboardMarkup:
    """Keyboard asking if user wants to caption video files with their filename.

    *sid* identifies the job so the answer is applied to the right prompt when
    the user has several jobs waiting.
    """
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📝 Ya (Letak Caption)", callback_data=_cap_callback("cap_choose_yes", sid)),
            InlineKeyboardButton("❌ Tidak Perlu", callback_data=_cap_callback("cap_choose_no", sid)),
        ],
        [
            InlineKeyboardButton("🚫 Batal", callback_data=_cap_callback("cap_cancel", sid)),
        ]
    ])


def get_exclude_keyboard(sid: Optional[str] = None) -> InlineKeyboardMarkup:
    """Keyboard asking if user wants to exclude any words or proceed as-is."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⏩ Teruskan Tanpa Buang", callback_data=_cap_callback("cap_exclude_skip", sid)),
        ],
        [
            InlineKeyboardButton("🚫 Batal", callback_data=_cap_callback("cap_cancel", sid)),
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
