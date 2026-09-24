"""
hls/downloader.py — HLS download + MP4 remux orchestration.

Turns an m3u8/HLS URL into a local, Telegram-ready MP4 with the SAME method the
bot already uses for MKV → MP4 (``app/utils/video_compressor.py``): ffmpeg
**stream copy** (``-c copy``) — no re-encode, no quality loss, ~0 CPU and I/O
bound wall time, plus a cheap audio-only AAC fallback for streams whose audio
codec MP4 cannot hold (AC-3/E-AC-3/FLAC are common on HLS).

Two engines, same output recipe
-------------------------------
* **Engine A (segments)** — fetch the playlists, download every segment (and the
  AES-128 key files) in parallel through the same ``proxy.txt`` SOCKS5 tunnel
  every other download in this bot uses, rewrite a *local* playlist pointing at
  the downloaded files, then let ffmpeg decrypt (AES-128) and remux in one pass.
  With a separate ``#EXT-X-MEDIA`` audio group, both playlists are muxed with
  ``-map 0:v:0 -map 1:a:0`` so the result is never silent.
* **Engine B (ffmpeg)** — hand the remote playlist straight to ffmpeg. One
  process, no Python parsing, but ffmpeg cannot use a SOCKS5 proxy and fetches
  segments sequentially.

Failure/cleanup policy (identical in spirit to ``video_compressor``)
-------------------------------------------------------------------
* ffmpeg always writes a private ``.hls_<rand>.mp4`` scratch file which is only
  published with ``os.replace`` after the output was verified, so a half-written
  MP4 can never be uploaded.
* Any failure, timeout or cancellation removes the scratch file and the whole
  per-job download directory — a cancel never leaves multi-GB junk behind.
* ``cleanup_orphaned_hls_dirs()`` (called once at startup) sweeps leftovers from
  a hard kill (SIGKILL / OOM).

Self-test (needs network + ffmpeg):

    python -m app.hls.downloader
"""
from __future__ import annotations

import asyncio
import glob
import os
import random
import shutil
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from app.config import (
    HLS_ENABLED,
    HLS_ENGINE,
    HLS_MAX_DURATION,
    HLS_MAX_HEIGHT,
    HLS_REENCODE_FALLBACK,
    HLS_SEGMENT_RETRIES,
    HLS_SEGMENT_WORKERS,
    HLS_TEMP_DIR,
    HLS_TIMEOUT,
)
from app.hls.client import (
    AUTO_PROXY,
    HlsClient,
    default_headers,
    ffmpeg_header_args,
    ffmpeg_proxy_args,
)
from app.hls.playlist import (
    MediaPlaylist,
    MediaSegment,
    is_drm,
    is_hls_playlist,
    is_master_playlist,
    parse_media,
    parse_master,
    select_audio_track,
    select_variant,
)
from app.utils.video_compressor import (
    MP4_SAFE_AUDIO,
    MP4_SAFE_VIDEO,
    convert_semaphore,
)

# ---------------------------------------------------------------------------
# Outcome reason codes (logging / debugging only)
# ---------------------------------------------------------------------------

R_DISABLED = "disabled"
R_NO_FFMPEG = "no-ffmpeg"
R_NOT_HLS = "not-hls"
R_DRM = "drm"
R_NO_SEGMENTS = "no-segments"
R_LOW_DISK = "low-disk"
R_DOWNLOAD_FAILED = "download-failed"
R_REMUX_FAILED = "remux-failed"


@dataclass
class HlsResult:
    """Outcome of an HLS download.

    When ``converted`` is False no usable MP4 exists and ``reason`` explains
    why; handlers must report the error instead of uploading anything.
    """
    path: str = ""
    name: str = ""
    size: int = 0
    converted: bool = False
    reason: str = ""
    engine: str = ""
    duration: float = 0.0
    is_live: bool = False
    estimated_size: int = 0


@dataclass
class _Prepared:
    """Everything resolved from the playlists before a single byte is fetched."""
    video_url: str = ""
    video_text: str = ""
    video_pl: Optional[MediaPlaylist] = None
    audio_url: str = ""
    audio_text: str = ""
    audio_pl: Optional[MediaPlaylist] = None
    duration: float = 0.0
    bandwidth: int = 0
    is_live: bool = True


# ---------------------------------------------------------------------------
# Small helpers (mirrors app/utils/video_compressor.py)
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    """``print`` that can never raise (unsafe stdout encodings must not break us)."""
    try:
        print(msg)
    except Exception:
        pass


async def _notify(status_cb: Optional[Callable[[str], object]], text: str) -> None:
    """Best-effort status message; never lets a UI failure break the pipeline."""
    if status_cb is None:
        return
    try:
        await status_cb(text)
    except Exception as e:
        _log(f"[HLS] status_cb failed: {e}")


def _safe_size(path: str) -> int:
    """``os.path.getsize`` that returns 0 instead of raising."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _mp4_name(file_name: str) -> str:
    """Replace *file_name*'s extension with ``.mp4``."""
    base, _ = os.path.splitext(file_name or "")
    base = base or "stream"
    return f"{base}.mp4"


def _has_disk_space(directory: str, size: int) -> bool:
    """True when *directory* can hold roughly twice *size* bytes."""
    try:
        free = shutil.disk_usage(directory).free
    except OSError:
        return True
    return free >= int(size * 2.05) + 300 * 1024 * 1024


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _segment_filename(index: int, url: str) -> str:
    """Local file name for a segment, keeping its original extension."""
    path = url.split("?", 1)[0]
    ext = os.path.splitext(path)[1].lower()
    allowed = {".ts", ".m4s", ".mp4", ".aac", ".mp3", ".cmfv", ".cmfa", ".vtt", ".key"}
    if ext not in allowed:
        ext = ".ts"
    return f"seg_{index:05d}{ext}"


# ---------------------------------------------------------------------------
# ffmpeg plumbing
# ---------------------------------------------------------------------------

def _build_remux_args(
    inputs: List[str],
    dst: str,
    *,
    local: bool,
    mode: str = "copy",
    extra_input_args: Optional[List[str]] = None,
    limit_seconds: int = 0,
) -> List[str]:
    """Build the ffmpeg argv for the HLS → MP4 remux.

    Deliberately mirrors ``app/utils/video_compressor.py::_build_ffmpeg_args``:
    ``-c copy`` (never a video re-encode unless *mode* asks for it), the same
    timestamp normalisation and the same ``+faststart`` so Telegram can stream
    and seek immediately.

    ``mode`` is ``"copy"``, ``"audio-aac"`` (video copied, audio re-encoded —
    the cheap rescue pass for AC-3/E-AC-3/FLAC audio) or ``"reencode"``.
    """
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]

    if local:
        # Reading our own rewritten playlists: only local files, the key files
        # (crypto) and in-playlist data URIs are allowed.
        args += [
            "-allowed_extensions", "ALL",
            "-protocol_whitelist", "file,crypto,data",
        ]
    else:
        # ffmpeg fetches everything itself: network protocols + reconnect logic.
        args += [
            "-allowed_extensions", "ALL",
            "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
            "-reconnect", "1", "-reconnect_streamed", "1",
            "-reconnect_delay_max", "10",
        ]

    if extra_input_args:
        args += list(extra_input_args)

    for src in inputs:
        args += ["-i", src]

    # Only the first video stream + all audio streams: drops attachment and
    # subtitle streams the MP4 muxer could choke on. The "?" makes audio
    # optional. With split audio groups the audio comes from input 2.
    if len(inputs) > 1:
        args += ["-map", "0:v:0", "-map", "1:a:0"]
    else:
        args += ["-map", "0:v:0", "-map", "0:a?"]
    args += ["-sn", "-map_chapters", "-1"]

    if mode == "audio-aac":
        args += ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]
    elif mode == "reencode":
        args += [
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-c:a", "aac", "-b:a", "192k",
        ]
    else:
        args += ["-c", "copy"]

    if limit_seconds and limit_seconds > 0:
        # Output option: stop muxing after N seconds (live cap / long VOD cap).
        args += ["-t", str(int(limit_seconds))]

    args += [
        # HLS timestamps can start negative or be non-monotonic; normalise them
        # so players don't freeze/desync after the container change.
        "-fflags", "+genpts", "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "1024",
        # Move the moov atom to the front so Telegram can stream/seek instantly.
        "-movflags", "+faststart",
        dst,
    ]
    return args



async def _read_progress(proc: asyncio.subprocess.Process, on_chunk) -> None:
    """Consume ffmpeg's ``-progress pipe:1`` output and report byte deltas."""
    if proc.stdout is None:
        return
    total = 0
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode("utf-8", "replace").strip()
            if text.startswith("total_size="):
                try:
                    new_total = int(text.split("=", 1)[1])
                except (ValueError, IndexError):
                    continue
                delta = new_total - total
                if delta > 0:
                    total = new_total
                    try:
                        on_chunk(delta)
                    except Exception as e:
                        _log(f"[HLS] progress callback failed: {e}")
            elif text == "progress=end":
                break
    except asyncio.CancelledError:
        raise
    except Exception as e:
        _log(f"[HLS] progress reader error: {e}")


async def _run_ffmpeg(
    args: List[str],
    dst: str,
    timeout: int,
    on_progress=None,
) -> bool:
    """Run ffmpeg, returning True only when a usable output file was produced."""
    proc = None
    reader = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=(
                asyncio.subprocess.PIPE if on_progress is not None
                else asyncio.subprocess.DEVNULL
            ),
            stderr=asyncio.subprocess.DEVNULL,
        )
        if on_progress is not None:
            # Must run concurrently with wait(), otherwise a full stdout pipe
            # would deadlock ffmpeg.
            reader = asyncio.create_task(_read_progress(proc, on_progress))

        await asyncio.wait_for(proc.wait(), timeout=max(60, timeout))

        if reader is not None:
            try:
                await asyncio.wait_for(reader, timeout=10)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                reader.cancel()

        if proc.returncode != 0:
            _log(f"[HLS] ffmpeg exited with code {proc.returncode}")
            return False
        if not os.path.exists(dst) or os.path.getsize(dst) < 1024:
            _log("[HLS] ffmpeg produced no usable output")
            return False
        return True
    except asyncio.TimeoutError:
        _log(f"[HLS] ffmpeg timed out after {timeout}s - killing process")
        return False
    except asyncio.CancelledError:
        # Kill the child before letting cancellation propagate so we never
        # leave an orphaned ffmpeg writing to disk.
        raise
    except Exception as e:
        _log(f"[HLS] ffmpeg error: {e}")
        return False
    finally:
        if reader is not None and not reader.done():
            reader.cancel()
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except BaseException:
                # Already cancelled / refusing to die — the caller removes the
                # scratch file anyway.
                pass



async def _remux_with_fallbacks(
    inputs: List[str],
    dst: str,
    *,
    local: bool,
    timeout: int,
    extra_input_args: Optional[List[str]] = None,
    limit_seconds: int = 0,
    on_progress=None,
    modes: Optional[List[str]] = None,
) -> bool:
    """Remux with ``-c copy``, then retry with progressively safer modes.

    The default chain is ``copy`` → ``audio-aac`` (video copied, audio re-encoded
    to AAC — cheap, and the only fallback allowed on a small VPS) and, when
    ``HLS_REENCODE_FALLBACK`` is enabled, a final full re-encode. Callers may pass
    a reordered *modes* list derived from a codec probe so the first attempt is
    already the right one.
    """
    if not modes:
        attempts = ["copy", "audio-aac"]
    else:
        attempts = list(modes)
    if HLS_REENCODE_FALLBACK and "reencode" not in attempts:
        attempts.append("reencode")

    for mode in attempts:
        args = _build_remux_args(
            inputs, dst, local=local, mode=mode,
            extra_input_args=extra_input_args, limit_seconds=limit_seconds,
        )
        if await _run_ffmpeg(args, dst, timeout, on_progress=on_progress):
            if mode != "copy":
                _log(f"[HLS] Remux succeeded with mode '{mode}'")
            return True
        _log(f"[HLS] Remux mode '{mode}' failed")
    return False


async def _probe_streams(inputs: List[str], local: bool) -> Dict[str, List[str]]:
    """Best-effort ffprobe of the remux inputs → ``{"video": [...], "audio": [...]}``.

    Local playlists are probeable exactly like a downloaded file, so the same
    "decide before muxing" idea as ``video_compressor._decide_mode`` applies
    here. Any failure simply returns empty lists (the caller then keeps the
    default copy-first chain).
    """
    video: List[str] = []
    audio: List[str] = []
    for src in inputs:
        args = ["ffprobe", "-v", "error"]
        if local:
            args += [
                "-allowed_extensions", "ALL",
                "-protocol_whitelist", "file,crypto,data",
            ]
        args += [
            "-show_entries", "stream=codec_type,codec_name",
            "-of", "json", src,
        ]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
            if proc.returncode != 0 or not stdout:
                continue
            import json as _json
            data = _json.loads(stdout.decode("utf-8", "replace"))
            for stream in data.get("streams") or []:
                name = (stream.get("codec_name") or "").lower()
                kind = (stream.get("codec_type") or "").lower()
                if not name:
                    continue
                if kind == "video":
                    video.append(name)
                elif kind == "audio":
                    audio.append(name)
        except asyncio.CancelledError:
            if proc is not None and proc.returncode is None:
                proc.kill()
            raise
        except Exception as e:
            _log(f"[HLS] probe failed for {src}: {e}")
            if proc is not None and proc.returncode is None:
                proc.kill()
    return {"video": video, "audio": audio}


async def _choose_remux_modes(
    inputs: List[str], local: bool
) -> Optional[List[str]]:
    """Order the remux modes so the first attempt matches the actual codecs."""
    probe = await _probe_streams(inputs, local)
    video, audio = probe["video"], probe["audio"]
    if not video and not audio:
        return None

    default = ["copy", "audio-aac"]

    # Unsupported video codec: only a re-encode can produce a playable MP4, and
    # only when the operator explicitly allowed it.
    if video and video[0] not in MP4_SAFE_VIDEO:
        if HLS_REENCODE_FALLBACK:
            return ["reencode"] + default
        _log(f"[HLS] Video codec {video[0]} may not be MP4-safe; keeping -c copy")

    # Audio MP4 cannot hold (AC-3/E-AC-3/FLAC/...): skip the doomed copy pass and
    # transcode the audio straight away. This is what stops silent/unplayable
    # audio on the many HLS masters whose default track is AC-3.
    if audio and any(codec not in MP4_SAFE_AUDIO for codec in audio):
        _log(f"[HLS] Audio codecs {audio} need an AAC transcode")
        return ["audio-aac"] + [m for m in default if m != "audio-aac"]

    return None



# ---------------------------------------------------------------------------
# Engine A — local segment downloader (proxy aware, parallel)
# ---------------------------------------------------------------------------

def _truncate(pl: MediaPlaylist, limit_seconds: int) -> MediaPlaylist:
    """Return *pl* limited to *limit_seconds* (a no-op when unset/already short).

    Segments are only ever dropped from the END, so the media sequence — and
    therefore ffmpeg's implicit AES-128 IV derivation — stays valid.
    """
    if not limit_seconds or limit_seconds <= 0:
        return pl
    if pl.total_duration and pl.total_duration <= limit_seconds:
        return pl

    kept: List[MediaSegment] = []
    total = 0.0
    for seg in pl.segments:
        if total >= limit_seconds:
            break
        kept.append(seg)
        total += seg.duration
    return MediaPlaylist(
        segments=kept,
        keys=pl.keys,
        init_url=pl.init_url,
        init_byte_range=pl.init_byte_range,
        media_sequence=pl.media_sequence,
        target_duration=pl.target_duration,
        is_live=False,
        is_fmp4=pl.is_fmp4,
        total_duration=total,
    )


async def _download_segment(
    client: HlsClient,
    url: str,
    path: str,
    byte_range=None,
    on_chunk=None,
) -> int:
    """Download one segment with retries (always from a clean file)."""
    attempts = max(1, HLS_SEGMENT_RETRIES)
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await client.download_to(
                url, path, on_chunk=on_chunk, byte_range=byte_range,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_error = e
            if attempt < attempts:
                _log(f"[HLS] Segment retry {attempt}/{attempts - 1}: {e}")
                await asyncio.sleep(min(2 * attempt, 6))
    raise ValueError(f"Failed to download segment {url}: {last_error}")


def _write_local_playlist(
    pl: MediaPlaylist,
    dst_path: str,
    filenames: List[str],
    key_files: Dict[str, str],
    init_name: str = "",
) -> None:
    """Write a self-contained playlist pointing at the local segment files.

    ffmpeg then performs AES-128 decryption AND the remux in the same stream-copy
    pass, which is why this module needs no crypto library at all.
    """
    version = 7 if (pl.is_fmp4 or init_name) else 3
    lines = [
        "#EXTM3U",
        f"#EXT-X-VERSION:{version}",
        f"#EXT-X-TARGETDURATION:{pl.target_duration or 10}",
    ]
    if pl.media_sequence:
        lines.append(f"#EXT-X-MEDIA-SEQUENCE:{pl.media_sequence}")
    lines.append("#EXT-X-PLAYLIST-TYPE:VOD")
    if init_name:
        lines.append(f'#EXT-X-MAP:URI="{init_name}"')

    current_key = None
    for seg, filename in zip(pl.segments, filenames):
        if seg.key_index != current_key:
            key = pl.keys[seg.key_index] if seg.key_index < len(pl.keys) else None
            if key is not None and key.is_aes128:
                local_key = key_files.get(key.url)
                if not local_key:
                    raise ValueError(f"No local key for {key.url}")
                iv = f",IV={key.iv}" if key.iv else ""
                lines.append(f'#EXT-X-KEY:METHOD=AES-128,URI="{local_key}"{iv}')
            else:
                lines.append("#EXT-X-KEY:METHOD=NONE")
            current_key = seg.key_index
        lines.append(f"#EXTINF:{seg.duration:.3f},")
        lines.append(filename)

    lines.append("#EXT-X-ENDLIST")
    with open(dst_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")



def _extension_of(url: str, default: str = ".ts") -> str:
    """Return the (safely recognised) extension of *url*, or *default*."""
    ext = os.path.splitext(url.split("?", 1)[0])[1].lower()
    if 3 <= len(ext) <= 5 and ext[1:].isalnum():
        return ext
    return default


async def _localize_playlist(
    client: HlsClient,
    pl: MediaPlaylist,
    work_dir: str,
    on_chunk=None,
) -> str:
    """Download every segment/key of *pl* into *work_dir*; return the playlist.

    Nothing here talks to Telegram — this only produces a local, self-contained
    playlist that ffmpeg can consume in a single stream-copy pass.
    """
    os.makedirs(work_dir, exist_ok=True)

    # Refuse anything that is not plain AES-128/NONE: an unsupported method
    # would otherwise be written as METHOD=NONE and produce a corrupt MP4.
    for key in pl.keys:
        if key.method and key.method not in ("NONE", "AES-128"):
            raise ValueError(f"Unsupported encryption method: {key.method}")

    # --- fMP4 init segment (#EXT-X-MAP) -------------------------------------
    init_name = ""
    if pl.init_url:
        init_name = "init_0" + _extension_of(pl.init_url, default=".mp4")
        await _download_segment(
            client, pl.init_url, os.path.join(work_dir, init_name),
            byte_range=pl.init_byte_range, on_chunk=on_chunk,
        )

    # --- AES-128 key files --------------------------------------------------
    key_files: Dict[str, str] = {}
    for key in pl.keys:
        if not key.is_aes128 or not key.url or key.url in key_files:
            continue
        name = f"key_{len(key_files)}.bin"
        data = await client.fetch_bytes(key.url)
        if len(data) < 16:
            raise ValueError(f"AES-128 key too short ({len(data)} bytes) for {key.url}")
        with open(os.path.join(work_dir, name), "wb") as fh:
            fh.write(data)
        key_files[key.url] = name

    # --- segments (parallel, bounded by HLS_SEGMENT_WORKERS) ----------------
    filenames: List[str] = [""] * len(pl.segments)
    sem = asyncio.Semaphore(max(1, HLS_SEGMENT_WORKERS))

    async def _worker(idx: int, seg: MediaSegment) -> None:
        async with sem:
            name = _segment_filename(idx, seg.url)
            await _download_segment(
                client, seg.url, os.path.join(work_dir, name),
                byte_range=seg.byte_range, on_chunk=on_chunk,
            )
            filenames[idx] = name

    await asyncio.gather(*(_worker(i, seg) for i, seg in enumerate(pl.segments)))

    playlist_path = os.path.join(work_dir, "index.m3u8")
    _write_local_playlist(pl, playlist_path, filenames, key_files, init_name)
    return playlist_path


async def _engine_segments(
    client: HlsClient,
    prep: _Prepared,
    temp_dir: str,
    limit_seconds: int,
    on_chunk=None,
) -> List[str]:
    """Engine A: localize the playlists and return the ffmpeg input paths."""
    video_pl = _truncate(prep.video_pl, limit_seconds)
    if not video_pl.segments:
        raise ValueError("video playlist has no segments")

    video_playlist = await _localize_playlist(
        client, video_pl, os.path.join(temp_dir, "video"), on_chunk=on_chunk,
    )
    inputs = [video_playlist]

    # Separate audio group (fMP4/CMAF masters): mux it as the second input so
    # the delivered MP4 is never silent.
    if prep.audio_pl and prep.audio_pl.segments:
        audio_pl = _truncate(prep.audio_pl, limit_seconds)
        if audio_pl.segments:
            audio_playlist = await _localize_playlist(
                client, audio_pl, os.path.join(temp_dir, "audio"), on_chunk=on_chunk,
            )
            inputs.append(audio_playlist)

    return inputs



# ---------------------------------------------------------------------------
# Playlist preparation (parsing only — no media downloads yet)
# ---------------------------------------------------------------------------

class _HlsError(Exception):
    """Internal error carrying a public reason code."""

    def __init__(self, reason: str, message: str = "") -> None:
        super().__init__(message or reason)
        self.reason = reason


_REASON_MESSAGES = {
    R_DISABLED: "❌ Ciri muat turun HLS dimatikan pada pelayan ini.",
    R_NO_FFMPEG: "❌ ffmpeg tidak dipasang pada pelayan ini.",
    R_NOT_HLS: "❌ URL ini bukan playlist HLS (.m3u8) yang sah.",
    R_DRM: "❌ Strim ini dilindungi DRM (SAMPLE-AES/Widevine) dan tidak boleh dimuat turun.",
    R_NO_SEGMENTS: "❌ Playlist HLS tidak mengandungi segmen video.",
    R_LOW_DISK: "❌ Ruang cakera tidak mencukupi untuk memuat turun strim ini.",
    R_DOWNLOAD_FAILED: "❌ Gagal memuat turun segmen HLS. Sila cuba lagi.",
    R_REMUX_FAILED: "❌ Gagal menukar strim HLS ke MP4.",
}


def reason_message(reason: str) -> str:
    """User-facing (Malay) explanation for a failed HLS download."""
    return _REASON_MESSAGES.get(reason, f"❌ Ralat HLS ({reason}).")


async def _prepare(
    client: HlsClient, url: str, max_height: int
) -> _Prepared:
    """Resolve a master/media playlist into the playlists we will download."""
    prep = _Prepared(video_url=url)

    text = await client.fetch_text(url)
    if not is_hls_playlist(text):
        raise _HlsError(R_NOT_HLS, "not an m3u8 playlist")
    if is_drm(text):
        raise _HlsError(R_DRM)

    if is_master_playlist(text):
        variants, audios = parse_master(text, url)
        variant = select_variant(variants, max_height)
        if variant is not None and variant.url:
            prep.video_url = variant.url
            prep.bandwidth = variant.rank_bandwidth
            track = select_audio_track(audios, variant.audio_group)
            if track is not None and track.url:
                prep.audio_url = track.url

    if prep.video_url == url:
        media_text = text
    else:
        media_text = await client.fetch_text(prep.video_url)

    if is_drm(media_text):
        raise _HlsError(R_DRM)

    prep.video_text = media_text
    if is_master_playlist(media_text):
        # Nested master (rare): localization would need a second hop, so leave
        # video_pl unset and let Engine B hand the playlist to ffmpeg.
        return prep

    prep.video_pl = parse_media(media_text, prep.video_url)
    prep.duration = prep.video_pl.total_duration
    prep.is_live = prep.video_pl.is_live

    if not prep.audio_url:
        return prep

    try:
        audio_text = await client.fetch_text(prep.audio_url)
        if is_drm(audio_text):
            raise _HlsError(R_DRM)
        prep.audio_text = audio_text
        if not is_master_playlist(audio_text):
            prep.audio_pl = parse_media(audio_text, prep.audio_url)
    except _HlsError:
        raise
    except Exception as e:
        # Missing audio must never fail the whole job: the video still uploads.
        _log(f"[HLS] Could not read audio playlist {prep.audio_url}: {e}")
        prep.audio_url = ""

    return prep



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def download_hls_to_mp4(
    url: str,
    file_name: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    status_cb: Optional[Callable[[str], object]] = None,
    on_chunk: Optional[Callable[[int], None]] = None,
    on_meta: Optional[Callable[[int], None]] = None,
    out_dir: Optional[str] = None,
    max_height: Optional[int] = None,
    max_duration: Optional[int] = None,
    proxy_url: Any = AUTO_PROXY,
) -> HlsResult:
    """Download an HLS stream and remux it into a local MP4.

    Parameters
    ----------
    url : str
        The m3u8/m3u URL (master or media playlist).
    file_name : str
        Display name of the result; only the base name is kept and ``.mp4`` is
        appended (e.g. ``"my_show.m3u8"`` → ``"my_show.mp4"``).
    headers : dict | None
        Request headers. Defaults to a browser UA plus a ``Referer`` derived
        from the URL origin, which many HLS CDNs require.
    status_cb : callable(str) -> awaitable | None
        Async callback used to surface status text (e.g. ``safe_edit``).
    on_chunk : callable(int) | None
        Called with downloaded byte counts to drive a progress bar. Works for
        both engines (segments while downloading, remux output for ffmpeg).
    on_meta : callable(int) | None
        Called once with the estimated total size in bytes (0 when unknown),
        right before the download starts — the point at which a progress bar can
        finally be sized.
    out_dir : str | None
        Where the MP4 is written. Defaults to ``HLS_TEMP_DIR`` or the system
        temp directory.
    max_height, max_duration : int | None
        Per-job overrides of ``HLS_MAX_HEIGHT`` / ``HLS_MAX_DURATION``.
    proxy_url : str | None
        ``AUTO_PROXY`` (default) follows ``proxy.txt``; ``None`` forces a direct
        connection for this job, which is what a page resolved by the headless
        browser needs when that browser could not use the proxy (the token it
        minted is bound to the address that asked for it).

    Returns
    -------
    HlsResult
        ``converted=False`` (with a ``reason``) means nothing usable was
        produced; callers must report the error instead of uploading.
    """
    result = HlsResult(name=_mp4_name(file_name))

    if not HLS_ENABLED:
        result.reason = R_DISABLED
        return result
    if not _ffmpeg_available():
        result.reason = R_NO_FFMPEG
        return result

    base_headers = dict(headers) if headers else default_headers(url)
    client = HlsClient(headers=base_headers, proxy_url=proxy_url)

    height_cap = HLS_MAX_HEIGHT if max_height is None else max_height
    duration_cap = HLS_MAX_DURATION if max_duration is None else max_duration
    engine = HLS_ENGINE if HLS_ENGINE in ("auto", "segments", "ffmpeg") else "auto"

    temp_dir: Optional[str] = None
    scratch: Optional[str] = None
    published = False

    try:
        # ---- 1. Parse the playlists (cheap: text only) --------------------
        prep: Optional[_Prepared] = None
        prep_error: str = ""
        if engine in ("auto", "segments"):
            try:
                await _notify(status_cb, "🔍 Membaca playlist HLS…")
                prep = await _prepare(client, url, height_cap)
            except _HlsError as e:
                # DRM and "not a playlist" cannot be rescued by ffmpeg either,
                # so fail fast with the precise reason.
                if e.reason in (R_DRM, R_NOT_HLS):
                    result.reason = e.reason
                    return result
                prep_error = e.reason or R_DOWNLOAD_FAILED
                _log(f"[HLS] Prepare failed: {e}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Network/proxy/parse problem: still worth letting ffmpeg try.
                prep_error = R_DOWNLOAD_FAILED
                _log(f"[HLS] Prepare failed: {e}")

        if prep is not None:
            result.duration = prep.duration
            result.is_live = prep.is_live
            if prep.bandwidth and prep.duration:
                result.estimated_size = int(prep.bandwidth * prep.duration / 8)

        # ---- 2. Workspace + guards ---------------------------------------
        dest_dir = out_dir or HLS_TEMP_DIR or tempfile.gettempdir()
        try:
            os.makedirs(dest_dir, exist_ok=True)
        except OSError as e:
            _log(f"[HLS] Cannot create output dir {dest_dir}: {e}")
            result.reason = R_REMUX_FAILED
            return result

        temp_dir = tempfile.mkdtemp(prefix="hls_", dir=dest_dir)
        if result.estimated_size and not _has_disk_space(
            dest_dir, result.estimated_size
        ):
            _log(f"[HLS] Not enough free disk for ~{result.estimated_size} bytes")
            result.reason = R_LOW_DISK
            return result

        final_path = os.path.join(dest_dir, result.name)
        scratch = os.path.join(
            dest_dir, f".hls_{random.randint(10 ** 9, 10 ** 10)}.mp4"
        )

        # Live streams never end by themselves; a long VOD can also be capped.
        limit_seconds = 0
        if duration_cap and duration_cap > 0:
            if prep is None or prep.is_live or (
                prep.duration and prep.duration > duration_cap
            ):
                limit_seconds = int(duration_cap)

        # The caller can now size its progress bar (0 = size still unknown).
        if on_meta is not None:
            try:
                on_meta(result.estimated_size or 0)
            except Exception as e:
                _log(f"[HLS] on_meta callback failed: {e}")


        # ---- 3. Engine A: local segment downloader -----------------------
        local_inputs: List[str] = []
        if engine in ("auto", "segments") and prep is not None and (
            prep.video_pl is not None and prep.video_pl.segments
        ):
            try:
                await _notify(status_cb, "⬇️ Memuat turun segmen HLS…")
                local_inputs = await _engine_segments(
                    client, prep, temp_dir, limit_seconds, on_chunk=on_chunk,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log(f"[HLS] Engine A failed, trying ffmpeg: {e}")
                local_inputs = []

        if local_inputs:
            await _notify(status_cb, "🔄 Menukar ke MP4 (stream copy)…")
            # Probe the downloaded segments first so an unplayable audio codec
            # (AC-3/E-AC-3/...) is transcoded on the first pass instead of
            # shipping a silent MP4 — same idea as the MKV→MP4 codec decision.
            modes = await _choose_remux_modes(local_inputs, local=True)
            async with convert_semaphore():
                ok = await _remux_with_fallbacks(
                    local_inputs, scratch, local=True,
                    timeout=HLS_TIMEOUT, limit_seconds=limit_seconds,
                    modes=modes,
                )
            if ok:
                os.replace(scratch, final_path)
                published = True
                result.path = final_path
                result.size = _safe_size(final_path)
                result.converted = True
                result.engine = "segments"
                _log(f"[HLS] OK {result.name} ({result.size} bytes) via segments")
                return result
            _log("[HLS] Engine A remux failed — falling back to ffmpeg")

        # ---- 4. Engine B: let ffmpeg fetch and remux ---------------------
        if engine in ("auto", "ffmpeg"):
            await _notify(status_cb, "⬇️ Memuat turun & menukar ke MP4 (ffmpeg)…")
            inputs: List[str] = [prep.video_url if prep and prep.video_url else url]
            if prep is not None and prep.audio_url:
                inputs.append(prep.audio_url)
            extra = (
                ffmpeg_header_args(base_headers)
                + ffmpeg_proxy_args(client.proxy_url)
            )
            ok = await _remux_with_fallbacks(
                inputs, scratch, local=False, timeout=HLS_TIMEOUT,
                extra_input_args=extra, limit_seconds=limit_seconds,
                on_progress=on_chunk,
            )
            if ok:
                os.replace(scratch, final_path)
                published = True
                result.path = final_path
                result.size = _safe_size(final_path)
                result.converted = True
                result.engine = "ffmpeg"
                _log(f"[HLS] OK {result.name} ({result.size} bytes) via ffmpeg")
                return result

        result.reason = R_NO_SEGMENTS if (
            prep is not None and prep.video_pl is not None
            and not prep.video_pl.segments
        ) else (prep_error or R_DOWNLOAD_FAILED)
        return result
    finally:
        # Always release the HTTP session and the per-job download directory —
        # on success, failure, timeout and cancellation alike.
        try:
            await client.close()
        except Exception:
            pass
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
        if not published and scratch and os.path.exists(scratch):
            try:
                os.remove(scratch)
            except OSError:
                pass



# ---------------------------------------------------------------------------
# Startup housekeeping
# ---------------------------------------------------------------------------

def cleanup_orphaned_hls_dirs() -> None:
    """Remove leftover HLS scratch from a previous crash.

    Called once at bot startup (synchronous, before the event loop is busy). A
    🚫 cancel removes its own temp directory and scratch file (see the job's
    ``finally`` block), but a hard kill (SIGKILL / OOM) cannot: this sweep
    reclaims those leftovers.

    A generous age gate keeps it safe even if a second bot process happens to be
    running (in which case its in-flight downloads are far younger than the
    cutoff).
    """
    removed = 0
    cutoff = time.time() - 6 * 3600

    def _stale(path: str) -> bool:
        try:
            return os.path.getmtime(path) < cutoff
        except OSError:
            return False

    def _sweep(directory: str) -> None:
        nonlocal removed
        for d in glob.glob(os.path.join(directory, "hls_*")):
            if os.path.isdir(d) and _stale(d):
                shutil.rmtree(d, ignore_errors=True)
                removed += 1
        for f in glob.glob(os.path.join(directory, ".hls_*.mp4")):
            if os.path.isfile(f) and _stale(f):
                try:
                    os.remove(f)
                    removed += 1
                except OSError:
                    pass

    _sweep(tempfile.gettempdir())
    if HLS_TEMP_DIR and os.path.isdir(HLS_TEMP_DIR):
        _sweep(HLS_TEMP_DIR)

    if removed:
        _log(f"[HLS] Cleaned up {removed} orphaned temp item(s)")



# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------
# No test framework is used in this project, so this block (run with
# ``python -m app.hls.downloader``) performs a real download + remux of a public
# HLS test stream and asserts the container/cleanup invariants.

if __name__ == "__main__":
    import json
    import subprocess
    import sys

    TEST_URL = "https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8"

    failures: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            failures.append(label)

    def _probe(path: str) -> dict:
        out = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "stream=codec_type,codec_name,width,height",
             "-show_entries", "format=format_name,duration",
             "-of", "json", path],
            capture_output=True, text=True,
        )
        if out.returncode != 0 or not out.stdout:
            return {}
        return json.loads(out.stdout)

    def _faststart_ok(path: str) -> bool:
        """True when the moov atom precedes mdat (what Telegram needs to stream)."""
        with open(path, "rb") as fh:
            head = fh.read(1024 * 1024)
        moov, mdat = head.find(b"moov"), head.find(b"mdat")
        return moov != -1 and (mdat == -1 or moov < mdat)

    async def _run_case() -> None:
        work = tempfile.mkdtemp(prefix="hls_selftest_")
        res = await download_hls_to_mp4(
            TEST_URL, "selftest.m3u8", out_dir=work, max_duration=20,
        )
        _check("converted", res.converted, f"reason={res.reason!r}")
        if not res.converted:
            shutil.rmtree(work, ignore_errors=True)
            return

        _check("engine reported", res.engine in ("segments", "ffmpeg"), res.engine)
        _check("output exists", os.path.exists(res.path), res.path)
        _check("name is .mp4", res.name == "selftest.mp4", res.name)
        _check("size matches file", res.size == _safe_size(res.path), str(res.size))
        _check("size > 100 KB", res.size > 100 * 1024, str(res.size))

        meta = _probe(res.path)
        fmt = (meta.get("format") or {}).get("format_name", "")
        streams = meta.get("streams") or []
        vcodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "video"]
        acodecs = [s.get("codec_name") for s in streams if s.get("codec_type") == "audio"]
        _check("container is mp4", "mp4" in fmt, fmt)
        _check("video stream is h264", "h264" in vcodecs, str(vcodecs))
        _check("audio stream present (not silent)", bool(acodecs), str(acodecs))
        _check("faststart (moov before mdat)", _faststart_ok(res.path))
        dur = float((meta.get("format") or {}).get("duration", 0) or 0)
        _check("duration ~= 20s cap", abs(dur - 20) < 6, str(dur))
        _check("no .hls_* scratch left",
               not [f for f in os.listdir(work) if f.startswith(".hls_")],
               str(os.listdir(work)))
        _check("no hls_* temp dir left",
               not [d for d in os.listdir(work) if d.startswith("hls_")],
               str(os.listdir(work)))
        shutil.rmtree(work, ignore_errors=True)

    async def _cancel_case() -> None:
        """A cancelled job must leave neither a temp dir nor a scratch file."""
        work = tempfile.mkdtemp(prefix="hls_selftest_")
        task = asyncio.create_task(
            download_hls_to_mp4(TEST_URL, "cancel.m3u8", out_dir=work)
        )
        await asyncio.sleep(2.0)
        task.cancel()
        cancelled = False
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        _check("cancellation propagates", cancelled)
        _check("cancel: no hls_* temp dir left",
               not [d for d in os.listdir(work) if d.startswith("hls_")],
               str(os.listdir(work)))
        _check("cancel: no .hls_* scratch left",
               not [f for f in os.listdir(work) if f.startswith(".hls_")],
               str(os.listdir(work)))
        shutil.rmtree(work, ignore_errors=True)

    print("--- HLS download + remux ---")
    asyncio.run(_run_case())
    print("--- cancellation cleanup ---")
    asyncio.run(_cancel_case())

    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for name in failures:
        print(f"    FAILED: {name}")
    sys.exit(1 if failures else 0)

