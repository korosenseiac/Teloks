# Plan: Custom Video Thumbnail from a User-Sent Image (Torrent, 1 video only)

## Goal
When the user sends an **image while the torrent caption prompt is on screen**,
use that image as the thumbnail of the uploaded video instead of the
auto-generated ffmpeg frame — but **only when the torrent contains exactly one
video**. Torrents with zero or multiple videos keep the current behaviour
(auto thumbnail for every video, image ignored).

## Problem (before)
1. The image was never captured: the only private media handlers were
   `bot_media_interceptor` (requires a pending Option-2 upload), the `.torrent`
   document handler (group 2) and text handlers — a photo sent during the
   caption flow fell through silently.
2. The torrent pipeline had no custom-thumbnail path at all: `_process_torrent`
   always called `_generate_video_thumb(...)` and `_split_and_upload_video`
   always generated `base_thumb`.

## Implementation (DONE)

### 1. `app/utils/caption.py` — `set_caption_thumb(user_id, raw)`
Mutates the **live** caption state entry (`user_thumb_raw` at top level and
inside `data`) so the value survives the `cap_choose_yes` step change to
`ASK_EXCLUDE` and is still readable after `clear_caption_state()` returns the
dict. Returns `False` when the state expired/was consumed.

### 2. `app/torrent/handler.py` — image → thumbnail bytes
- `_resize_thumb_high_quality(raw, max_side=320)` (Pillow, `LANCZOS`, JPEG
  quality 90, ImportError/exception fallback returns the raw bytes) — same
  implementation as the Direct Link feature.
- `extract_user_thumbnail(bot, message)`: image **document** (full quality) →
  `message.download()`, otherwise the largest **photo** →
  `bot.download_media(message.photo.file_id)`; reads the bytes, deletes the
  temp file, resizes to ≤320 px JPEG. Returns `None` when the message has no
  usable image.

### 3. `app/bot/main.py` — interceptor (group 0)
`torrent_thumbnail_interceptor` + `pending_torrent_caption_filter`
(`filters.photo | filters.document` & private & state exists with
`source_type in ("torrent", "torrent_file")`):
- non-image documents return (real `.torrent` uploads keep flowing to
  `torrent_file_upload_handler`),
- an image whose **caption contains a link** (magnet / torrent URL / direct /
  terabox / mediafire) returns, so a real job is never hijacked,
- success → `set_caption_thumb(...)` + “🖼 Gambar disimpan sebagai thumbnail.”
  + `message.stop_propagation()`; failure → error reply, nothing stored.

### 4. Carry-through
- Three torrent call sites in `caption_callback_handler` /
  `handle_caption_exclusion_message` pass
  `custom_thumb_raw=state.get("user_thumb_raw")`.
- `process_torrent_download(..., custom_thumb_raw=None)` forwards it to
  `_process_torrent(..., custom_thumb_raw=None)` for both `torrent` and
  `torrent_file` sources.

### 5. Single-video gate (`_process_torrent`)
```python
video_files = [f for f in files if f["kind"] == "video"]
custom_thumb = custom_thumb_raw if (custom_thumb_raw and len(video_files) == 1) else None
```
- `custom_thumb` set → `thumb_raw = custom_thumb or await _generate_video_thumb(...)`
  (ffmpeg snapshot skipped).
- `custom_thumb` is `None` (0 or ≥2 videos) → behaviour identical to before,
  plus one info message telling the user the image was ignored.
- `_split_and_upload_video(..., custom_thumb_raw=...)`:
  `base_thumb = custom_thumb_raw or await _generate_video_thumb(...)` →
  Part 1 uses the image, parts 2+ keep generated snapshots (Direct Link style).
- Both prompts now mention: “🖼 Hantar gambar sekarang jika mahu ia dijadikan
  thumbnail (hanya untuk torrent dengan 1 video).”

## Behaviour matrix
| Torrent content | Image during prompt | Outcome |
| --- | --- | --- |
| 1 video | yes | user image is the thumbnail |
| 1 video needing split | yes | Part 1 → user image, parts 2+ → snapshots |
| 2+ videos | yes | ignored (unchanged) + info message |
| no videos | yes | ignored (unchanged) + info message |
| any | no | unchanged |

## Edge cases
- Several images sent → the last one wins (state value overwritten).
- State TTL is 15 minutes (`STATE_TTL_SECONDS`); cancel/`cap_cancel` discards
  the image together with the state.
- Pillow missing or unreadable image → `extract_user_thumbnail` returns `None`
  and the user is told the image could not be processed.
- Image sent before the link or as a reply is **not** supported (by design).

## Validation
- `python -m py_compile app/torrent/handler.py app/bot/main.py app/utils/caption.py`
  → OK.
- Not runnable locally (neither `pyrogram` nor `Pillow` is installed in the
  local `.venv`); manual checks on the server:
  1. single-video magnet + image during prompt → video shows the image;
  2. multi-video magnet + image → unchanged;
  3. image with no pending prompt → still ignored (no regression);
  4. `.torrent` upload + image during prompt → image applied.
