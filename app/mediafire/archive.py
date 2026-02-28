"""
mediafire/archive.py — Extract media files from ZIP / RAR archives.

Runs the blocking extraction in ``asyncio.run_in_executor()`` so the
event loop is never blocked.

Only photos and videos (per ``app.utils.media.MEDIA_EXTS``) are
extracted; everything else is skipped.

Memory-efficient: supports both batch extraction (legacy) and one-at-a-time
yielding to keep disk/memory usage minimal on low-RAM VPS instances.
"""
from __future__ import annotations

import asyncio
import os
import zipfile
from typing import AsyncIterator, Dict, List

from app.utils.media import MEDIA_EXTS, ext, classify

# Try to import rarfile — graceful fallback if not installed
try:
    import rarfile

    _HAS_RAR = True
except ImportError:
    _HAS_RAR = False


# ---------------------------------------------------------------------------
# Public API — batch (legacy, still used for small archives)
# ---------------------------------------------------------------------------


async def extract_media_from_archive(
    archive_path: str,
    dest_dir: str,
) -> List[Dict[str, object]]:
    """
    Extract only media files (photos + videos) from *archive_path* into
    *dest_dir*.  Returns a list of dicts::

        [{"name": "photo.jpg", "size": 123456, "path": "/tmp/.../photo.jpg",
          "kind": "photo"}, ...]

    The extraction is performed in a thread-pool executor.

    Raises
    ------
    ValueError
        If the archive format is unsupported or the archive is corrupted.
    """
    lower = archive_path.lower()

    if lower.endswith(".zip"):
        return await _extract_zip(archive_path, dest_dir)
    elif lower.endswith(".rar"):
        if not _HAS_RAR:
            raise ValueError(
                "Sokongan RAR tidak tersedia — sila pasang `rarfile` dan "
                "binary `unrar` pada pelayan."
            )
        return await _extract_rar(archive_path, dest_dir)
    else:
        raise ValueError(f"Format arkib tidak disokong: {os.path.basename(archive_path)}")


# ---------------------------------------------------------------------------
# Public API — one-at-a-time extraction (memory efficient)
# ---------------------------------------------------------------------------


def count_media_in_archive(archive_path: str) -> int:
    """
    Return the number of media files inside the archive without extracting.
    Quick scan used to show progress info.
    """
    lower = archive_path.lower()
    count = 0
    try:
        if lower.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "r") as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    basename = os.path.basename(info.filename)
                    if basename and ext(basename) in MEDIA_EXTS:
                        count += 1
        elif lower.endswith(".rar") and _HAS_RAR:
            with rarfile.RarFile(archive_path, "r") as rf:
                for info in rf.infolist():
                    if info.is_dir():
                        continue
                    basename = os.path.basename(info.filename)
                    if basename and ext(basename) in MEDIA_EXTS:
                        count += 1
    except Exception:
        pass
    return count


async def iter_extract_media(
    archive_path: str,
    dest_dir: str,
) -> AsyncIterator[Dict[str, object]]:
    """
    Async generator that extracts media files from an archive ONE AT A TIME.

    Yields dicts identical to ``extract_media_from_archive`` entries.
    The caller should delete each file after uploading to keep disk usage low.

    This avoids extracting all files at once, which would consume too much
    memory/disk on a low-RAM VPS.
    """
    lower = archive_path.lower()

    if lower.endswith(".zip"):
        async for item in _iter_extract_zip(archive_path, dest_dir):
            yield item
    elif lower.endswith(".rar"):
        if not _HAS_RAR:
            raise ValueError(
                "Sokongan RAR tidak tersedia — sila pasang `rarfile` dan "
                "binary `unrar` pada pelayan."
            )
        async for item in _iter_extract_rar(archive_path, dest_dir):
            yield item
    else:
        raise ValueError(f"Format arkib tidak disokong: {os.path.basename(archive_path)}")


# ---------------------------------------------------------------------------
# Iterative ZIP extraction (one file at a time)
# ---------------------------------------------------------------------------


async def _iter_extract_zip(
    archive_path: str, dest_dir: str
) -> AsyncIterator[Dict[str, object]]:
    loop = asyncio.get_running_loop()
    # Get the list of media entries first (metadata only, no extraction)
    entries = await loop.run_in_executor(
        None, _list_zip_media_entries, archive_path
    )
    seen_names: Dict[str, int] = {}
    for entry_name in entries:
        basename = os.path.basename(entry_name)
        safe_name = _unique_name(basename, seen_names)
        out_path = os.path.join(dest_dir, safe_name)
        # Extract just this one file
        await loop.run_in_executor(
            None, _extract_single_zip_entry, archive_path, entry_name, out_path
        )
        file_size = os.path.getsize(out_path)
        yield {
            "name": safe_name,
            "size": file_size,
            "path": out_path,
            "kind": classify(safe_name),
        }


def _list_zip_media_entries(archive_path: str) -> List[str]:
    """Return entry names of media files inside a ZIP (no extraction)."""
    entries = []
    with zipfile.ZipFile(archive_path, "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            basename = os.path.basename(info.filename)
            if basename and ext(basename) in MEDIA_EXTS:
                entries.append(info.filename)
    return entries


def _extract_single_zip_entry(
    archive_path: str, entry_name: str, out_path: str
) -> None:
    """Extract a single entry from a ZIP to out_path, streaming in chunks."""
    with zipfile.ZipFile(archive_path, "r") as zf:
        with zf.open(entry_name) as src, open(out_path, "wb") as dst:
            while True:
                chunk = src.read(2 * 1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)


# ---------------------------------------------------------------------------
# Iterative RAR extraction (one file at a time)
# ---------------------------------------------------------------------------


async def _iter_extract_rar(
    archive_path: str, dest_dir: str
) -> AsyncIterator[Dict[str, object]]:
    loop = asyncio.get_running_loop()
    entries = await loop.run_in_executor(
        None, _list_rar_media_entries, archive_path
    )
    seen_names: Dict[str, int] = {}
    for entry_name in entries:
        basename = os.path.basename(entry_name)
        safe_name = _unique_name(basename, seen_names)
        out_path = os.path.join(dest_dir, safe_name)
        await loop.run_in_executor(
            None, _extract_single_rar_entry, archive_path, entry_name, out_path
        )
        file_size = os.path.getsize(out_path)
        yield {
            "name": safe_name,
            "size": file_size,
            "path": out_path,
            "kind": classify(safe_name),
        }


def _list_rar_media_entries(archive_path: str) -> List[str]:
    """Return entry names of media files inside a RAR (no extraction)."""
    entries = []
    with rarfile.RarFile(archive_path, "r") as rf:
        for info in rf.infolist():
            if info.is_dir():
                continue
            basename = os.path.basename(info.filename)
            if basename and ext(basename) in MEDIA_EXTS:
                entries.append(info.filename)
    return entries


def _extract_single_rar_entry(
    archive_path: str, entry_name: str, out_path: str
) -> None:
    """Extract a single entry from a RAR to out_path, streaming in chunks."""
    with rarfile.RarFile(archive_path, "r") as rf:
        with rf.open(entry_name) as src, open(out_path, "wb") as dst:
            while True:
                chunk = src.read(2 * 1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)


# ---------------------------------------------------------------------------
# ZIP batch extraction (legacy — kept for compatibility)
# ---------------------------------------------------------------------------


async def _extract_zip(archive_path: str, dest_dir: str) -> List[Dict[str, object]]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sync_extract_zip, archive_path, dest_dir)


def _sync_extract_zip(archive_path: str, dest_dir: str) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    seen_names: Dict[str, int] = {}

    with zipfile.ZipFile(archive_path, "r") as zf:
        for info in zf.infolist():
            # Skip directories
            if info.is_dir():
                continue

            # Only extract media files
            basename = os.path.basename(info.filename)
            if not basename or ext(basename) not in MEDIA_EXTS:
                continue

            # Handle duplicate names
            safe_name = _unique_name(basename, seen_names)
            out_path = os.path.join(dest_dir, safe_name)

            # Extract the single member
            with zf.open(info) as src, open(out_path, "wb") as dst:
                while True:
                    chunk = src.read(2 * 1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

            file_size = os.path.getsize(out_path)
            results.append({
                "name": safe_name,
                "size": file_size,
                "path": out_path,
                "kind": classify(safe_name),
            })

    return results


# ---------------------------------------------------------------------------
# RAR extraction
# ---------------------------------------------------------------------------


async def _extract_rar(archive_path: str, dest_dir: str) -> List[Dict[str, object]]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _sync_extract_rar, archive_path, dest_dir)


def _sync_extract_rar(archive_path: str, dest_dir: str) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    seen_names: Dict[str, int] = {}

    with rarfile.RarFile(archive_path, "r") as rf:
        for info in rf.infolist():
            if info.is_dir():
                continue

            basename = os.path.basename(info.filename)
            if not basename or ext(basename) not in MEDIA_EXTS:
                continue

            safe_name = _unique_name(basename, seen_names)
            out_path = os.path.join(dest_dir, safe_name)

            with rf.open(info) as src, open(out_path, "wb") as dst:
                while True:
                    chunk = src.read(2 * 1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

            file_size = os.path.getsize(out_path)
            results.append({
                "name": safe_name,
                "size": file_size,
                "path": out_path,
                "kind": classify(safe_name),
            })

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_name(name: str, seen: Dict[str, int]) -> str:
    """
    Return *name* if not seen before, otherwise append a counter
    (e.g. ``photo_2.jpg``).
    """
    if name not in seen:
        seen[name] = 1
        return name

    seen[name] += 1
    base, extension = os.path.splitext(name)
    return f"{base}_{seen[name]}{extension}"
