# Plan: Auto-Thumbnail Frame Between 20%–70% (Direct & Torrent)

## Goal
Change auto-generated video thumbnails for the **Direct Link** and **Torrent**
features so the extracted frame is a **random position between 20% and 70%** of
the video duration, instead of the current 10% (Direct) or fixed 1s (Torrent).
No blank/black-frame detection is performed. When the user supplies a custom
thumbnail, auto-generation is skipped entirely.

## Implementation (DONE)

### 1. `app/direct/handler.py` — `_generate_video_thumb`
- `_generate_video_thumb(video_path, duration_sec=0)` now picks a random seek
  position inside 20%–70% of the duration:
  ```python
  if duration_sec > 0:
      seek_time = max(1, random.uniform(duration_sec * 0.20, duration_sec * 0.70))
  else:
      seek_time = 1
  ```
- Runs `ffmpeg` once with `-ss <seek_time>`, returns the JPEG bytes directly
  (no size/blank-frame checks), and cleans up the temp `.thumb.jpg` in
  `finally`.
- Custom-thumbnail guard untouched: auto-generation only runs when
  `thumb_raw` is empty (existing `if not thumb_raw` check).

### 2. `app/torrent/handler.py` — `_generate_video_thumb`
- Signature changed to `_generate_video_thumb(video_path, duration_sec=0)`
  with the same random 20%–70% seek logic.
- Call sites updated to fetch duration (via `_get_video_metadata`) first and
  pass it in:
  - Main upload loop: `_get_video_metadata` before thumb, passes
    `video_meta.get("duration", 0)`.
  - `_split_and_upload_video`: metadata fetched before `base_thumb`, passes
    `total_duration`.
  - Split part thumbs: passes `int(dur_sec)`.
- Torrent has no custom-thumbnail path today, so auto-generation always runs.

## Edge cases
- Unknown/zero duration → fallback to `1s`.
- Very short videos (< ~2s) → `max(1, ...)` may push the seek above 70%;
  acceptable and consistent with prior clamping behavior.
- Non-video files → unaffected.

## Validation
- `python -m py_compile` on both files — PASSED.
- Manual: direct-link video → thumbnail is mid-video (not intro).
- Manual: torrent video → same.
- Manual: direct-link with custom thumbnail → custom thumbnail preserved
  (no auto-thumb).