"""
utils/video_compressor.py — MKV → MP4 remux (stream copy) helpers.

Remuxes Matroska files into the MP4 container WITHOUT re-encoding the video
stream, so quality is preserved bit-for-bit and the operation is purely I/O
bound (safe on a 2 vCPU / 2 GB VPS and under the bot service's MemoryMax
cgroup limit).

Why remux instead of re-encode
------------------------------
* ``-c copy`` never decodes or encodes a frame: ~0 CPU, ~30-80 MB RAM for the
  ffmpeg process regardless of file size; wall time is limited only by disk
  read+write throughput.
* MP4 with H.264 + AAC is the combination Telegram plays inline/streamable,
  which is the entire point of converting MKV uploads.

Failure policy
--------------
Every public helper is *best effort*: it never raises into the calling pipeline
and never returns a broken file. If anything is off (feature disabled, no
ffmpeg, unsupported codec, insufficient disk, ffmpeg error, timeout,
cancellation) the caller gets the ORIGINAL path/name/size back and uploads it
exactly as it did before this feature existed.

Cleanup policy
--------------
* ffmpeg always writes to a private scratch name; the file is atomically moved
  to its final name with ``os.replace`` only after the output was verified, so
  a half-written MP4 can never be seen by the uploader.
* Any failure / timeout / cancellation removes the partial output.
* The original MKV is deleted ONLY after the converted output is verified.
* ``cleanup_orphaned_convert_dirs()`` (called once at startup) sweeps scratch
  left behind by a previous crash.
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import random
import shutil
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Tuple

from app.config import (
    CONVERT_CONCURRENCY,
    CONVERT_EXTENSIONS,
    CONVERT_TEMP_DIR,
    CONVERT_TIMEOUT,
    MKV_TO_MP4,
)
from app.utils.media import ext as _ext

# ---------------------------------------------------------------------------
# Codec compatibility tables
# ---------------------------------------------------------------------------

# Video codecs Telegram clients can play from an MP4 container. Anything else
# (vp8, prores, wmv3, theora, ...) is left as the original MKV — remuxing it
# would produce an MP4 that Telegram cannot stream anyway.
MP4_SAFE_VIDEO = {"h264", "hevc", "av1", "mpeg4", "mpeg2video", "vc1"}

# Audio codecs that can be copied into MP4 without re-encoding. Anything else
# (dts, truehd, flac, vorbis, pcm_*, ac3, eac3, ...) triggers an audio-only
# AAC transcode, which is cheap (no video work at all).
MP4_SAFE_AUDIO = {"aac", "mp3"}

# Outcome reason codes (logging / debugging only).
R_DISABLED = "disabled"
R_NOT_MKV = "not-mkv"
R_NO_FFMPEG = "no-ffmpeg"
R_PROBE_FAILED = "probe-failed"
R_NO_VIDEO = "no-video"
R_UNSUPPORTED_VIDEO = "unsupported-video"
R_LOW_DISK = "low-disk"
R_FFMPEG_FAILED = "ffmpeg-failed"


@dataclass
class ConvertResult:
    """Outcome of a conversion attempt.

    When ``converted`` is False, ``path``/``name``/``size`` are the untouched
    originals — callers can always use the result unconditionally.
    """
    path: str
    name: str
    size: int
    converted: bool
    reason: str = ""


# Conversions are serialised by default: it keeps the bot service comfortably
# inside its 700 MB MemoryMax cgroup and stops two remuxes from fighting over
# disk I/O on a small VPS.
_SEM: Optional[asyncio.Semaphore] = None


def _sem() -> asyncio.Semaphore:
    """Return the module-level conversion semaphore (created lazily)."""
    global _SEM
    if _SEM is None:
        _SEM = asyncio.Semaphore(max(1, CONVERT_CONCURRENCY))
    return _SEM


def convert_semaphore() -> asyncio.Semaphore:
    """Public accessor for the shared remux semaphore.

    Other pipelines (e.g. the HLS → MP4 remux in ``app/hls/downloader.py``) use
    this so the VPS never runs more conversions at once than
    ``CONVERT_CONCURRENCY`` allows, whatever the source format is.
    """
    return _sem()


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def is_convertible(file_name: str) -> bool:
    """True when *file_name* should be remuxed to MP4 (feature on + extension)."""
    if not MKV_TO_MP4 or not file_name:
        return False
    return _ext(file_name) in CONVERT_EXTENSIONS


def _mp4_name(file_name: str) -> str:
    """Return *file_name* with its extension replaced by ``.mp4``."""
    base, _ = os.path.splitext(file_name)
    return f"{base}.mp4" if base else "video.mp4"


def _safe_size(path: str) -> int:
    """``os.path.getsize`` that returns 0 instead of raising."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _has_disk_space(directory: str, size: int) -> bool:
    """True when *directory* can hold a second copy of *size* bytes.

    The source and the converted file coexist while remuxing, so the peak
    requirement is roughly twice the file size. When the free space cannot be
    determined we optimistically allow the attempt.
    """
    try:
        free = shutil.disk_usage(directory).free
    except OSError:
        return True
    return free >= int(size * 1.05) + 200 * 1024 * 1024


def _log(msg: str) -> None:
    """``print`` that can never raise.

    The stdout encoding is not guaranteed (e.g. a Windows cp1252 console), and
    a *logging* failure must never turn an already-successful conversion into a
    reported failure — by then the source file may have been deleted and the
    caller would be handed a path that no longer exists.

    All messages passed here are intentionally ASCII-only for the same reason.
    """
    try:
        print(msg)
    except Exception:
        pass


async def _notify(status_cb: Optional[Callable[[str], Any]], text: str) -> None:
    """Best-effort status message; never lets a UI failure break the pipeline."""
    if status_cb is None:
        return
    try:
        await status_cb(text)
    except Exception as e:
        _log(f"[Convert] status_cb failed: {e}")


# ---------------------------------------------------------------------------
# ffprobe / ffmpeg plumbing
# ---------------------------------------------------------------------------

async def _probe_streams(path: str) -> Optional[dict]:
    """Return ffprobe's JSON description of *path*, or None when it fails."""
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "stream=index,codec_type,codec_name",
            "-show_entries", "format=format_name,duration",
            "-of", "json",
            path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode != 0 or not stdout:
            return None
        return json.loads(stdout.decode("utf-8", "replace"))
    except asyncio.CancelledError:
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
        raise
    except Exception as e:
        _log(f"[Convert] ffprobe failed for {path}: {e}")
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
        return None


def _decide_mode(probe: Optional[dict]) -> Tuple[Optional[str], str]:
    """Decide how (or whether) to remux, based on an ffprobe result.

    Returns ``("copy" | "transcode-audio", "")`` when a conversion should be
    attempted, or ``(None, reason_code)`` when it must be skipped.
    """
    if not probe:
        return None, R_PROBE_FAILED

    streams = probe.get("streams") or []
    video_codecs = [
        (s.get("codec_name") or "").lower()
        for s in streams
        if (s.get("codec_type") or "").lower() == "video"
    ]
    audio_codecs = [
        (s.get("codec_name") or "").lower()
        for s in streams
        if (s.get("codec_type") or "").lower() == "audio"
    ]

    if not video_codecs:
        return None, R_NO_VIDEO

    # Only the first video stream is copied, so it alone must be MP4-safe.
    if video_codecs[0] not in MP4_SAFE_VIDEO:
        return None, R_UNSUPPORTED_VIDEO

    if any(codec not in MP4_SAFE_AUDIO for codec in audio_codecs):
        return "transcode-audio", ""

    return "copy", ""


def _build_ffmpeg_args(src: str, dst: str, mode: str) -> List[str]:
    """Build the ffmpeg argv for a stream-copy (or audio-only) remux.

    ``mode`` is either ``"copy"`` (pure remux) or ``"transcode-audio"``
    (video copied, audio re-encoded to AAC — the only fallback allowed on a
    small VPS; video re-encoding is deliberately never attempted).
    """
    args = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-i", src,
        # Only the first video stream + all audio streams. This drops
        # attachment streams (fonts, cover art) and subtitle streams so the MP4
        # muxer cannot fail on them. The "?" makes audio optional.
        "-map", "0:v:0", "-map", "0:a?",
        "-sn", "-map_chapters", "-1",
    ]

    if mode == "transcode-audio":
        args += ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]
    else:
        args += ["-c", "copy"]

    args += [
        # MKV timestamps can start negative or be non-monotonic; normalise them
        # so players don't freeze/desync after the container change.
        "-fflags", "+genpts", "-avoid_negative_ts", "make_zero",
        "-max_muxing_queue_size", "1024",
        # Move the moov atom to the front so Telegram can stream and seek
        # immediately instead of downloading the whole file first.
        "-movflags", "+faststart",
        dst,
    ]
    return args


async def _run_ffmpeg(args: List[str], dst_tmp: str, timeout: int) -> bool:
    """Run ffmpeg, returning True only when a usable output file was produced."""
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=max(60, timeout))
        if proc.returncode != 0:
            _log(f"[Convert] ffmpeg exited with code {proc.returncode}")
            return False
        if not os.path.exists(dst_tmp) or os.path.getsize(dst_tmp) < 1024:
            _log("[Convert] ffmpeg produced no usable output")
            return False
        return True
    except asyncio.TimeoutError:
        _log(f"[Convert] ffmpeg timed out after {timeout}s - killing process")
        return False
    except asyncio.CancelledError:
        # Kill the child before letting cancellation propagate so we never
        # leave an orphaned ffmpeg writing to disk.
        raise
    except Exception as e:
        _log(f"[Convert] ffmpeg error: {e}")
        return False
    finally:
        if proc is not None and proc.returncode is None:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except BaseException:
                # Already cancelled / process refused to die — nothing more we
                # can safely do here; the scratch file is removed by the caller.
                pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def _convert_locked(
    input_path: str,
    file_name: str,
    original: ConvertResult,
    status_cb: Optional[Callable[[str], Any]],
) -> ConvertResult:
    """Remux work performed while holding the conversion semaphore."""
    probe = await _probe_streams(input_path)
    mode, reason = _decide_mode(probe)
    if mode is None:
        _log(f"[Convert] Skipping {file_name}: {reason}")
        original.reason = reason
        return original

    # By default the MP4 is written next to the source, i.e. inside the
    # pipeline's own temp directory. That makes the existing rmtree()/startup
    # sweeps automatically clean up converted files.
    out_dir = CONVERT_TEMP_DIR or os.path.dirname(os.path.abspath(input_path))
    try:
        os.makedirs(out_dir, exist_ok=True)
    except OSError as e:
        _log(f"[Convert] Cannot create output dir {out_dir}: {e}")
        original.reason = R_FFMPEG_FAILED
        return original

    if not _has_disk_space(out_dir, original.size):
        _log(f"[Convert] Not enough free disk for {file_name} - skipping")
        original.reason = R_LOW_DISK
        await _notify(
            status_cb,
            "⚠️ Ruang cakera tidak cukup untuk tukar ke MP4 — fail asal dimuat naik.",
        )
        return original

    new_name = _mp4_name(file_name)
    final_path = os.path.join(out_dir, new_name)
    # Private scratch name: ffmpeg never writes the final file directly, so a
    # half-written MP4 can never be picked up by the uploader.
    dst_tmp = os.path.join(
        out_dir, f".convert_{random.randint(10 ** 9, 10 ** 10)}.mp4"
    )
    ok = False
    try:
        await _notify(status_cb, f"🔄 Menukar MKV → MP4: `{file_name}`…")

        args = _build_ffmpeg_args(input_path, dst_tmp, mode)
        ok = await _run_ffmpeg(args, dst_tmp, CONVERT_TIMEOUT)
        if not ok:
            # One fallback pass with an AAC audio transcode: it is cheap (no
            # video work) and rescues files whose audio codec MP4 can't hold.
            _log(f"[Convert] Retrying {file_name} with AAC audio transcode")
            args = _build_ffmpeg_args(input_path, dst_tmp, "transcode-audio")
            ok = await _run_ffmpeg(args, dst_tmp, CONVERT_TIMEOUT)
        if not ok:
            original.reason = R_FFMPEG_FAILED
            return original

        # Verified output — publish it atomically, then drop the source.
        os.replace(dst_tmp, final_path)
        # Past this point the MP4 is live, so nothing below may be allowed to
        # turn the result into a "failed" conversion (the source may be gone).
        ok = True
        new_size = _safe_size(final_path)
        try:
            os.remove(input_path)
        except OSError as e:
            _log(f"[Convert] Could not remove source {input_path}: {e}")

        _log(
            f"[Convert] OK {file_name} -> {new_name} ({mode}), "
            f"{original.size} -> {new_size} bytes"
        )
        return ConvertResult(
            path=final_path,
            name=new_name,
            size=new_size,
            converted=True,
        )
    finally:
        # On any failure (return above, timeout, cancellation) remove the
        # scratch file so nothing is left behind.
        if not ok and os.path.exists(dst_tmp):
            try:
                os.remove(dst_tmp)
            except OSError:
                pass


def _recover_result(
    input_path: str, file_name: str, original: ConvertResult
) -> ConvertResult:
    """Last-resort result when an unexpected error escaped ``_convert_locked``.

    The dangerous case is an error *after* the MP4 was published and the source
    deleted: reporting failure would hand the caller a path that no longer
    exists, losing the file entirely. So if the source is gone but the expected
    MP4 is present, report the conversion as successful instead.
    """
    if not os.path.exists(original.path):
        out_dir = CONVERT_TEMP_DIR or os.path.dirname(os.path.abspath(input_path))
        candidate = os.path.join(out_dir, _mp4_name(file_name))
        if os.path.exists(candidate) and _safe_size(candidate) > 0:
            _log(f"[Convert] Recovered published output after error: {candidate}")
            original.path = candidate
            original.name = _mp4_name(file_name)
            original.size = _safe_size(candidate)
            original.converted = True
            original.reason = ""
            return original

    original.converted = False
    original.reason = R_FFMPEG_FAILED
    return original


async def convert_mkv_to_mp4(
    input_path: str,
    file_name: str,
    *,
    status_cb: Optional[Callable[[str], Any]] = None,
) -> ConvertResult:
    """Remux *input_path* into MP4, returning the untouched original on failure.

    Parameters
    ----------
    input_path : str
        Local path of the downloaded/extracted source file.
    file_name : str
        Display name of the source file (used for the extension check and for
        deriving the ``.mp4`` name).
    status_cb : callable(str) -> awaitable | None
        Optional async callback used to surface progress (e.g. ``safe_edit``).

    Returns
    -------
    ConvertResult
        Always safe to consume: when ``converted`` is False the original
        ``path``/``name``/``size`` are returned unchanged, so callers can use
        the result unconditionally.
    """
    original = ConvertResult(
        path=input_path,
        name=file_name,
        size=_safe_size(input_path),
        converted=False,
    )

    if not is_convertible(file_name):
        original.reason = R_DISABLED if not MKV_TO_MP4 else R_NOT_MKV
        return original

    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        _log("[Convert] ffmpeg/ffprobe not installed - skipping conversion")
        original.reason = R_NO_FFMPEG
        return original

    if not os.path.exists(input_path):
        _log(f"[Convert] Source missing: {input_path}")
        original.reason = R_PROBE_FAILED
        return original

    async with _sem():
        try:
            return await _convert_locked(input_path, file_name, original, status_cb)
        except asyncio.CancelledError:
            # User cancelled: _convert_locked's finally block has already
            # removed the scratch file. Re-raise so the caller's own cleanup
            # (which deletes the source temp file) still runs.
            _log(f"[Convert] Cancelled while converting {file_name}")
            raise
        except Exception as e:
            _log(f"[Convert] Unexpected error for {file_name}: {e}")
            return _recover_result(input_path, file_name, original)


# ---------------------------------------------------------------------------
# Startup housekeeping
# ---------------------------------------------------------------------------

def cleanup_orphaned_convert_dirs() -> None:
    """Remove leftover scratch files/dirs from a previous crash.

    Called once at bot startup (synchronous, before the event loop is busy).
    A 🚫 cancel removes its own temp files (see the handler ``finally`` blocks),
    but a hard kill (SIGKILL / OOM) cannot: this sweep reclaims those leftovers.

    With the default config converted MP4s are written next to their source, so
    the pipeline's own temp directory owns that cleanup; this sweep covers an
    explicitly configured ``CONVERT_TEMP_DIR`` plus the per-job temp paths used
    by the download pipelines.

    A generous age gate keeps this safe even if a second bot process happens to
    be running.
    """
    import time

    removed = 0

    if CONVERT_TEMP_DIR:
        for pattern in (".convert_*.mp4", ".convert_*.tmp"):
            for f in glob.glob(os.path.join(CONVERT_TEMP_DIR, pattern)):
                try:
                    os.remove(f)
                    removed += 1
                except OSError:
                    pass

    # Nothing can be running at startup, so a generous age gate is enough to
    # stay safe while still reclaiming multi-GB leftovers from old crashes.
    cutoff = time.time() - 6 * 3600
    tmp = tempfile.gettempdir()

    def _stale(path: str) -> bool:
        try:
            return os.path.getmtime(path) < cutoff
        except OSError:
            return False

    # Streaming downloads that were interrupted mid-write.
    for f in glob.glob(os.path.join(tmp, "direct_link_*")):
        if os.path.isfile(f) and _stale(f):
            try:
                os.remove(f)
                removed += 1
            except OSError:
                pass

    # Per-job temp directories (torrent_* is handled by
    # cleanup_orphaned_torrent_dirs() at startup).
    for pattern in (
        "tg_archive_*", "direct_archive_*", "mf_archive_*",
        "direct_split_*", "torrent_split_*", "thumb_*",
    ):
        for d in glob.glob(os.path.join(tmp, pattern)):
            if os.path.isdir(d) and _stale(d):
                try:
                    shutil.rmtree(d, ignore_errors=True)
                    removed += 1
                except OSError:
                    pass

    if removed:
        _log(f"[Convert] Cleaned up {removed} orphaned temp item(s)")


# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------
# No test framework is used in this project, so this block (run with
# ``python -m app.utils.video_compressor``) builds small MKV fixtures with
# ffmpeg, converts them, and asserts the container/codec/cleanup invariants.

if __name__ == "__main__":
    import sys

    def _probe(path: str) -> dict:
        import subprocess
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries",
             "stream=codec_type,codec_name", "-of", "json", path],
            capture_output=True, text=True,
        )
        if out.returncode != 0 or not out.stdout:
            return {}
        return json.loads(out.stdout)

    def _make(path: str, vcodec: str, acodec: str) -> bool:
        import subprocess
        return subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", "testsrc=size=320x240:rate=30",
            "-f", "lavfi", "-i", "sine=frequency=440",
            "-t", "3", "-c:v", vcodec, "-c:a", acodec, path,
        ]).returncode == 0

    def _faststart_ok(path: str) -> bool:
        """``moov`` must precede ``mdat`` for Telegram to stream/seek."""
        with open(path, "rb") as fh:
            head = fh.read(8192)
        moov, mdat = head.find(b"moov"), head.find(b"mdat")
        return 0 <= moov < mdat

    failures: List[str] = []

    def _check(label: str, cond: bool, detail: str = "") -> None:
        print(f"{'PASS' if cond else 'FAIL'}  {label} {detail}".rstrip())
        if not cond:
            failures.append(label)

    # --- 1. Pure decision logic (independent of local codec support) -------
    def _streams(*spec):
        return {"streams": [{"codec_type": t, "codec_name": c} for t, c in spec]}

    decisions = [
        ("copy: h264+aac", _streams(("video", "h264"), ("audio", "aac")), "copy", ""),
        ("copy: h264+mp3", _streams(("video", "h264"), ("audio", "mp3")), "copy", ""),
        ("copy: h264, no audio", _streams(("video", "h264")), "copy", ""),
        ("copy: hevc+av1-safe audio",
         _streams(("video", "hevc"), ("audio", "aac")), "copy", ""),
        ("audio transcode: h264+dts",
         _streams(("video", "h264"), ("audio", "dts")), "transcode-audio", ""),
        ("audio transcode: h264+truehd",
         _streams(("video", "h264"), ("audio", "truehd")), "transcode-audio", ""),
        ("audio transcode: ac3",
         _streams(("video", "h264"), ("audio", "ac3")), "transcode-audio", ""),
        ("skip: vp8", _streams(("video", "vp8"), ("audio", "vorbis")),
         None, R_UNSUPPORTED_VIDEO),
        ("skip: prores", _streams(("video", "prores")),
         None, R_UNSUPPORTED_VIDEO),
        ("skip: audio-only", _streams(("audio", "aac")), None, R_NO_VIDEO),
        ("skip: no streams", {"streams": []}, None, R_NO_VIDEO),
        ("skip: probe failed", None, None, R_PROBE_FAILED),
    ]
    for label, probe, want_mode, want_reason in decisions:
        got = _decide_mode(probe)
        _check(label, got == (want_mode, want_reason), f"-> {got}")

    _check("is_convertible: movie.mkv", is_convertible("movie.mkv"))
    _check("is_convertible: MOVIE.MKV (case)", is_convertible("MOVIE.MKV"))
    _check("is_convertible: movie.mp4 (false)", not is_convertible("movie.mp4"))
    _check("is_convertible: notes.txt (false)", not is_convertible("notes.txt"))

    work = tempfile.mkdtemp(prefix="convert_selftest_")


    async def _real_cases() -> None:
        for name, vcodec, acodec in (
            ("h264_aac.mkv", "libx264", "aac"),    # pure stream copy
            ("h264_flac.mkv", "libx264", "flac"),  # audio-only transcode → AAC
        ):
            src = os.path.join(work, name)
            if not _make(src, vcodec, acodec):
                print(f"SKIP  {name}: fixture could not be created")
                continue

            res = await convert_mkv_to_mp4(src, name)
            _check(f"{name}: converted", res.converted, f"reason={res.reason!r}")
            if not res.converted:
                continue

            codecs = {s["codec_type"]: s["codec_name"]
                      for s in _probe(res.path).get("streams", [])}
            print(f"      {codecs}  size={res.size}")
            _check(f"{name}: video is MP4-safe",
                   codecs.get("video") in MP4_SAFE_VIDEO, str(codecs))
            _check(f"{name}: audio is MP4-safe",
                   codecs.get("audio") in MP4_SAFE_AUDIO, str(codecs))
            _check(f"{name}: name ends with .mp4", res.name.endswith(".mp4"), res.name)
            _check(f"{name}: faststart (moov before mdat)", _faststart_ok(res.path))
            _check(f"{name}: source MKV removed after verify",
                   not os.path.exists(src))
            _check(f"{name}: returned size matches disk",
                   res.size == os.path.getsize(res.path))

    asyncio.run(_real_cases())

    # --- 2. Cancellation must not leak scratch or lose the source ----------
    async def _cancel_case() -> None:
        global _run_ffmpeg
        name = "cancel_me.mkv"
        src = os.path.join(work, name)
        if not _make(src, "libx264", "aac"):
            print("SKIP  cancellation test: fixture could not be created")
            return

        original_run = _run_ffmpeg

        async def _never_finishes(args, dst_tmp, timeout):
            await asyncio.sleep(60)
            return False

        _run_ffmpeg = _never_finishes
        try:
            task = asyncio.create_task(convert_mkv_to_mp4(src, name))
            await asyncio.sleep(0.3)
            task.cancel()
            cancelled = False
            try:
                await task
            except asyncio.CancelledError:
                cancelled = True
            _check("cancellation propagates to the caller", cancelled)
        finally:
            _run_ffmpeg = original_run

        _check("cancellation: source kept on disk", os.path.exists(src))
        _check("cancellation: no .convert_* scratch left",
               not [f for f in os.listdir(work) if f.startswith(".convert_")])

    asyncio.run(_cancel_case())

    # --- 3. Corrupt input must be skipped, never converted -----------------
    async def _broken_case() -> None:
        bad = os.path.join(work, "broken.mkv")
        with open(bad, "wb") as fh:
            fh.write(b"not a real matroska file" * 20)
        res = await convert_mkv_to_mp4(bad, "broken.mkv")
        _check("corrupt mkv skipped", not res.converted, f"reason={res.reason!r}")
        _check("corrupt input still on disk", os.path.exists(bad))

    asyncio.run(_broken_case())

    # --- 4. A broken/legacy stdout must never break a successful conversion -
    async def _bad_stdout_case() -> None:
        name = "stdout_test.mkv"
        src = os.path.join(work, name)
        if not _make(src, "libx264", "aac"):
            print("SKIP  stdout test: fixture could not be created")
            return

        class _BadStream:
            """Mimics a console that cannot encode the log output (e.g. cp1252)."""

            def write(self, *_a, **_k):
                raise UnicodeEncodeError("charmap", "x", 0, 1, "boom")

            def flush(self):
                pass

        real_stdout = sys.stdout
        sys.stdout = _BadStream()
        try:
            res = await convert_mkv_to_mp4(src, name)
        finally:
            sys.stdout = real_stdout

        _check("bad stdout: conversion still reported OK",
               res.converted, f"reason={res.reason!r}")
        _check("bad stdout: output file exists",
               bool(res.path) and os.path.exists(res.path))
        _check("bad stdout: output size reported", res.size > 0)
        _check("bad stdout: source consumed, not lost",
               not os.path.exists(src) and res.converted)

    asyncio.run(_bad_stdout_case())

    # --- 5. Global cleanup invariant ---------------------------------------
    scratch = [f for f in os.listdir(work) if f.startswith(".convert_")]
    _check("no .convert_* scratch left anywhere", not scratch, str(scratch))

    shutil.rmtree(work, ignore_errors=True)
    print("---", "ALL PASSED" if not failures else f"{len(failures)} FAILURE(S)")
    for f in failures:
        print(f"    FAILED: {f}")
    sys.exit(1 if failures else 0)




