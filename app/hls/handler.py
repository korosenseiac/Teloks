"""
hls/handler.py — Telegram handlers for m3u8/HLS stream downloads.

Mirrors ``app/direct/handler.py``: the link is guarded, a per-user job slot is
reserved while the caption prompt is on screen, and the finished MP4 is uploaded
through the very same Direct pipeline (backup group → copy to user, custom
thumbnail, captions, automatic splitting for oversized files).

The only HLS-specific part is the download+remux step, which lives in
``app/hls/downloader.py``.
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from pyrogram import Client
from pyrogram.types import Message

from app.config import (
    BACKUP_GROUP_ID,
    HLS_BROWSER_ENABLED,
    HLS_BROWSER_TIMEOUT,
    HLS_ENABLED,
    HLS_PAGE_EXTRACT,
    HLS_PAGE_HINT,
    HLS_PAGE_MAX_BYTES,
    HLS_PAGE_MAX_CANDIDATES,
)
from app.bot.session_manager import manager
from app.database.db import (
    get_user_session,
    get_user_profile,
    log_forward,
)
from app.direct.handler import (
    EXCLUSIVE_DOMAINS,
    _generate_video_thumb,
    _get_backup_group_peer,
    _get_video_metadata,
    _resize_thumb_high_quality,
    _send_to_user,
    _split_video_part,
    _upload_file_to_backup,
)
from app.hls.browser import browser_available, resolve_stream_with_browser
from app.hls.client import AUTO_PROXY, HlsClient, default_headers, read_proxy_url
from app.hls.downloader import download_hls_to_mp4, reason_message
from app.hls.extract import resolve_stream_from_page
from app.hls.playlist import HLS_LINK_PATTERN

from app.mediafire.streamer import FileStreamer
from app.terabox.progress import ProgressTracker
from app.utils.caption import (
    generate_video_caption,
    get_caption_choice_keyboard,
    set_caption_state,
)
from app.utils.media import MAX_FILE_SIZE, MAX_FILE_SIZE_PREMIUM
from app.utils.message import safe_edit, clear_reply_markup
from app.utils.streamer import SessionInvalidError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: A link that *might* be a video page: any http(s) URL no other pipeline owns.
#: The negative lookaheads stop this handler from ever stealing traffic from the
#: HLS (``.m3u8``), torrent/magnet, terabox, mediafire and ``t.me`` handlers, and
#: from wasting a page fetch on an obvious file download — those keep working
#: exactly as before. The patterns of the sibling pipelines are re-checked at
#: run time too (``_owned_by_other_pipeline``), so the list can never drift.
PAGE_LINK_PATTERN = re.compile(
    r"https?://"
    r"(?!(?:" + "|".join(re.escape(d) for d in sorted(EXCLUSIVE_DOMAINS)) + r"))"
    r"(?![^\s<>\"']*\.m3u8?(?:[?#]|$))"
    r"(?![^\s<>\"']*\.(?:torrent|mp4|mkv|avi|mov|webm|flv|wmv|ts|mp3|m4a|aac|"
    r"zip|rar|7z|tar|gz|pdf|docx?|xlsx?|apk|exe|iso|jpg|jpeg|png|gif|webp|"
    r"srt|vtt|ass)(?:[?#]|$))"
    r"[^\s<>\"']+",
    re.IGNORECASE,
)

#: Page slugs that say nothing about the video.
_USELESS_SLUGS = {
    "video", "videos", "watch", "view", "embed", "player", "play", "index",
    "page", "post", "v", "e", "w", "id", "download", "file", "stream",
}


def _owned_by_other_pipeline(text: str) -> bool:
    """True when another link handler must keep this message.

    Imported lazily (all of these modules are already loaded by the bot) so the
    only thing that has to stay in sync with them is this one list.
    """
    from app.mediafire.handler import MEDIAFIRE_LINK_PATTERN
    from app.terabox.handler import TERABOX_LINK_PATTERN
    from app.torrent.handler import MAGNET_LINK_PATTERN, TORRENT_URL_PATTERN

    return any(
        pattern.search(text)
        for pattern in (
            HLS_LINK_PATTERN, MAGNET_LINK_PATTERN, TORRENT_URL_PATTERN,
            TERABOX_LINK_PATTERN, MEDIAFIRE_LINK_PATTERN,
        )
    )


def _name_from_url(url: str) -> str:
    """Derive a display name from an m3u8 URL (falls back to ``stream.m3u8``)."""
    path = url.split("?", 1)[0].rstrip("/")
    base = os.path.basename(path)
    if not base or base in (".", "/"):
        return "stream.m3u8"
    # The extension is replaced by the downloader; keep the rest as-is.
    stem, ext = os.path.splitext(base)
    if not stem:
        return "stream.m3u8"
    if ext.lower() in (".m3u8", ".m3u"):
        return base
    # Paths like /hls/1080p/index → "index" is useless; use the parent segment.
    parent = os.path.basename(os.path.dirname(path))
    if stem.lower() in ("index", "playlist", "master", "manifest", "out") and parent:
        return f"{parent}.m3u8"
    return f"{stem}.m3u8"


def _name_from_page(page_url: Optional[str]) -> Optional[str]:
    """Display name from a page URL (``/video/135123-fc2-ppv-4332967.html``).

    A page-derived job would otherwise be named after the CDN path
    (``upnuod4v9rr3_,n,.urlset``), which tells the user nothing. Returns ``None``
    when the slug carries no information, so the caller falls back to the
    manifest URL.
    """
    if not page_url:
        return None
    try:
        slug = os.path.basename(urlparse(page_url).path.rstrip("/"))
    except ValueError:
        # Malformed URL (e.g. an unmatched '[' in the host): no name from it.
        return None
    stem = os.path.splitext(slug)[0]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-._")
    if len(stem) < 3 or stem.lower() in _USELESS_SLUGS:
        return None
    return stem[:120]


async def _load_custom_thumb(bot: Client, message: Message) -> Optional[bytes]:
    """Load the user's custom thumbnail (document > photo), like Direct does."""
    source = None
    if (message.document and message.document.mime_type
            and message.document.mime_type.startswith("image/")):
        source = "document"
    elif message.photo:
        source = "photo"
    if not source:
        return None

    try:
        if source == "document":
            thumb_path = await message.download()
        else:
            thumb_path = await bot.download_media(message.photo.file_id)
        if not thumb_path:
            return None
        with open(thumb_path, "rb") as fh:
            thumb_raw = fh.read()
        try:
            os.remove(thumb_path)
        except OSError:
            pass
        thumb_raw = _resize_thumb_high_quality(thumb_raw)
        print(f"[HLS] Custom thumbnail loaded ({source}): {len(thumb_raw)} bytes")
        return thumb_raw
    except Exception as e:
        print(f"[HLS] Failed to load custom thumbnail: {e}")
        return None


# ---------------------------------------------------------------------------
# Entry point — called by the bot's message handler
# ---------------------------------------------------------------------------

async def _begin_hls_job(
    bot: Client,
    message: Message,
    url: str,
    *,
    page_url: Optional[str] = None,
    force_direct: bool = False,
) -> bool:
    """Guard, reserve a slot and ask about captions for *url*.

    Shared by both entry points (a raw ``.m3u8`` link, and a page we extracted a
    manifest from). Returns True when the message is handled — a job was started,
    or the user was told why it cannot start — and False when HLS is disabled,
    in which case the caller leaves the message to the Direct handler.
    """
    from app.bot.main import (
        has_free_slot, process_limit_message, reserve_process,
    )

    if not HLS_ENABLED:
        # Nothing to do: the caller leaves the message to the Direct handler.
        return False

    user_id = message.from_user.id

    if not has_free_slot(user_id):
        await message.reply_text(process_limit_message())
        return True

    user_session = await get_user_session(user_id)
    if not user_session:
        await message.reply_text("❌ Belum login. Sila /start untuk login.")
        return True

    user_client = await manager.get_client(user_id)
    if not user_client:
        await message.reply_text("❌ Sesi tidak sah. Sila login semula.")
        return True

    user_profile = await get_user_profile(user_id)
    if not user_profile:
        await message.reply_text(
            "⚠️ **Profile belum lengkap!**\n\n"
            "Sila set profile anda terlebih dahulu.\n\n"
            "👇 **Pilih jantina anda:**"
        )
        return True

    # Never log a signed URL in full: its query string is the credential.
    shown = url.split("?", 1)[0]
    if page_url:
        print(f"[HLS] user={user_id} page={page_url.split('?', 1)[0]} -> {shown[:100]}")
    else:
        print(f"[HLS] user={user_id} url={shown[:100]}")

    # Reserve the slot *before* the caption prompt: the prompt is already a job,
    # so spamming links can never start more jobs than allowed.
    slot = reserve_process(user_id, "hls", prompt_msg=message)
    if slot is None:
        await message.reply_text(process_limit_message())
        return True

    status_msg = await message.reply_text(
        "🎬 **Tetapan Caption Video (HLS/m3u8)**\n\n"
        "Adakah anda mahu meletakkan nama fail sebagai caption pada video?\n\n"
        "💡 *Strim akan dimuat turun dan ditukar ke MP4 secara automatik "
        "(stream copy — tanpa turun kualiti).*\n"
        "💡 *Jika Ya, nama fail akan dijadikan caption (format fail seperti "
        ".m3u8/.mp4 akan dibuang secara automatik).*",
        reply_markup=get_caption_choice_keyboard(slot.sid),
    )
    slot.status_msg = status_msg

    set_caption_state(
        user_id=user_id,
        flow_type="hls",
        message=message,
        status_msg=status_msg,
        sid=slot.sid,
        url=url,
        page_url=page_url or "",
        force_direct=force_direct,
    )
    return True


async def hls_link_handler(bot: Client, message: Message) -> None:
    """Validate an m3u8 link, reserve a job slot and ask about captions.

    The download itself only starts once the caption flow is answered (see
    ``process_hls_download``), exactly like the Direct and Torrent handlers.
    """
    message_text = message.text or message.caption or ""
    match = HLS_LINK_PATTERN.search(message_text)
    if not match:
        await message.reply_text("❌ Tidak dapat mengesan URL m3u8 yang sah.")
        return
    await _begin_hls_job(bot, message, match.group(0).strip())


async def _browser_stage(
    message: Message,
    page_url: str,
    fallback_page: str,
) -> Tuple[str, str, bool]:
    """Resolve the manifest by running the page in a headless browser.

    Last resort for hosts that build the playlist URL inside obfuscated JS behind
    bot/device fingerprinting (no ``.m3u8`` in the HTML, no JSON API to call).
    Returns ``(url, page_url, force_direct)``; ``("", fallback_page, False)`` when
    the engine is not installed, the page yields nothing, or anything goes
    wrong — the caller then behaves exactly as before.

    ``force_direct`` is True when Chromium had to connect directly while
    ``proxy.txt`` is configured: the token it just minted is bound to that direct
    address, so the download must use the same egress or it would 403.
    """
    if not HLS_BROWSER_ENABLED or not browser_available():
        return "", fallback_page, False

    status_msg = None
    try:
        # This stage can take a while, so say something instead of staying quiet.
        status_msg = await message.reply_text("🌐 Membuka halaman di pelayar…")
    except Exception:
        status_msg = None

    try:
        scan = await resolve_stream_with_browser(page_url, timeout=HLS_BROWSER_TIMEOUT)
    except Exception as e:
        print(f"[HLS] Browser stage failed for {page_url.split('?', 1)[0]}: "
              f"{type(e).__name__}: {e}")
        return "", fallback_page, False
    finally:
        if status_msg is not None:
            try:
                await status_msg.delete()
            except Exception:
                pass

    if scan.url:
        force_direct = (not scan.used_proxy) and read_proxy_url() is not None
        if force_direct:
            print("[HLS] Browser had to go direct while proxy.txt is configured - "
                  "this job will download directly too, so the minted URL matches "
                  "the address it was minted for")
        return scan.url, page_url, force_direct
    detail = f" - {scan.detail}" if scan.detail else ""
    print(f"[HLS] Browser stage: no manifest in {page_url.split('?', 1)[0]} "
          f"({scan.reason or 'unknown'}{detail})")
    return "", fallback_page, False


async def page_hls_handler(bot: Client, message: Message) -> bool:
    """Start an HLS job from a *page* link, using the manifest it embeds.

    This is the cure for CDNs that only honour playlist URLs minted for the
    client that loaded the page (the token carries that client's IP/ASN, so a
    URL copied out of a browser gets 403 from this server whatever headers it
    sends). Loading the page ourselves — same proxy, same headers as the
    download — makes the site mint the playlist for *us*, and the child URLs
    inside that master are then signed for us too.

    Returns True when the message is claimed (a job started, or the user was
    told why it cannot start) and False when the link should keep flowing to the
    Direct handler — which is the case for every link that turns out not to be a
    video page.
    """
    from app.bot.main import has_free_slot, process_limit_message

    if not (HLS_ENABLED and HLS_PAGE_EXTRACT):
        return False

    message_text = message.text or message.caption or ""
    match = PAGE_LINK_PATTERN.search(message_text)
    if not match or _owned_by_other_pipeline(message_text):
        return False
    page_url = match.group(0).strip()

    # Never spend a page fetch on a user who could not start a job anyway.
    if not has_free_slot(message.from_user.id):
        await message.reply_text(process_limit_message())
        return True

    client: Optional[HlsClient] = None
    try:
        # A browser sends the whole document URL as Referer, not just the origin
        # — and anti-hotlink CDNs check exactly that, both for the page fetch
        # and for the candidate playlists it validates.
        page_headers = default_headers(page_url)
        page_headers["Referer"] = page_url
        client = HlsClient(page_headers)
        scan = await resolve_stream_from_page(
            client,
            page_url,
            max_bytes=HLS_PAGE_MAX_BYTES,
            limit=HLS_PAGE_MAX_CANDIDATES,
        )
    except Exception as e:
        print(f"[HLS] Page scan failed for {page_url.split('?', 1)[0]}: "
              f"{type(e).__name__}: {e}")
        return False
    finally:
        if client is not None:
            await client.close()

    url = scan.url or ""
    page_used = scan.page_url or page_url
    force_direct = False

    if not url:
        detail = f" - {scan.detail}" if scan.detail else ""
        print(f"[HLS] Page scan: no manifest in {page_url.split('?', 1)[0]} "
              f"({scan.reason}, {scan.candidates} candidate(s){detail})")
        # Last resort: JS-only players. Run the page in Chromium and take the
        # first m3u8 it requests — that URL is minted for OUR egress, which is
        # also what makes client-bound signed CDNs work.
        url, page_used, force_direct = await _browser_stage(message, page_url, page_used)

    if not url:
        if HLS_PAGE_HINT and scan.player_page:
            try:
                await message.reply_text(
                    "🔎 Halaman ini memainkan video tetapi pautan strimnya tidak "
                    "boleh dibaca terus dari halaman.\n\n"
                    "Sila hantar pautan `.m3u8` (buka video di pelayar → F12 → "
                    "Network → tapis `m3u8` → salin pautan)."
                )
            except Exception:
                pass
        return False

    print(f"[HLS] Page scan: manifest found via {page_used.split('?', 1)[0]}")
    return await _begin_hls_job(
        bot, message, url, page_url=page_used, force_direct=force_direct,
    )



# ---------------------------------------------------------------------------
# Job — download the HLS stream, remux to MP4, upload it
# ---------------------------------------------------------------------------

async def process_hls_download(
    bot: Client,
    user_id: int,
    message: Message,
    status_msg: Message,
    *,
    url: Optional[str] = None,
    page_url: Optional[str] = None,
    force_direct: bool = False,
    enable_caption: bool = False,
    exclude_words: Optional[str] = None,
    slot_sid: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Download an HLS stream and deliver the resulting MP4 to the user.

    Reuses the Direct pipeline for everything after the download: backup-group
    upload, custom/generated thumbnail, caption, multi-part splitting, delivery
    to the user and logging.
    """
    from app.bot.main import (
        begin_process, get_process_slot, has_free_slot, is_cancelled,
        process_cancel_keyboard, process_limit_message, release_process,
    )

    user_id = user_id or message.from_user.id
    url = url or kwargs.get("url")
    page_url = page_url or kwargs.get("page_url")
    force_direct = bool(force_direct or kwargs.get("force_direct"))
    if not url:
        await safe_edit(status_msg, "❌ URL tidak sah.")
        return

    is_premium = getattr(message.from_user, "is_premium", False) or False
    size_limit = MAX_FILE_SIZE_PREMIUM if is_premium else MAX_FILE_SIZE

    user_client = await manager.get_client(user_id)
    if not user_client:
        await safe_edit(status_msg, "❌ Sesi tidak sah. Sila login semula.")
        return

    # Adopt the slot reserved when the link was accepted.
    slot = begin_process(user_id, slot_sid, "hls", status_msg)
    if slot is None:
        if slot_sid and get_process_slot(slot_sid) is None:
            print(f"[HLS] job {slot_sid} was cancelled before it started")
            await safe_edit(status_msg, "🚫 **Proses dibatalkan.**")
        elif not has_free_slot(user_id):
            await safe_edit(status_msg, process_limit_message())
        return

    if status_msg is None:
        status_msg = await message.reply_text("🔄 Sedang Diproses..")
    await safe_edit(
        status_msg, "🔄 Sedang Diproses..",
        reply_markup=process_cancel_keyboard(slot.sid),
    )
    slot.status_msg = status_msg

    # A page-derived job knows the page the player lived on: that page URL is
    # what a browser sends as Referer, and anti-hotlink CDNs check exactly that.
    file_name = _name_from_page(page_url) or _name_from_url(url)
    mp4_name = f"{os.path.splitext(file_name)[0]}.mp4"
    headers = default_headers(url)
    if page_url:
        headers["Referer"] = page_url

    tracker: Optional[ProgressTracker] = None
    temp_mp4: Optional[str] = None
    work_dir: Optional[str] = None

    try:
        # ---- Custom thumbnail from the message that carried the link -------
        thumb_raw = await _load_custom_thumb(bot, message)

        # The progress bar can only be sized once the playlist has been parsed,
        # so the downloader tells us the estimate via on_meta and routes every
        # downloaded byte through on_chunk.
        holder: Dict[str, Optional[ProgressTracker]] = {"tracker": None}

        def _on_meta(estimated: int) -> None:
            if holder["tracker"] is not None:
                return
            holder["tracker"] = ProgressTracker(
                status_msg, mp4_name, max(1, int(estimated or 0)),
                reply_markup=process_cancel_keyboard(slot.sid),
            )
            holder["tracker"].start()

        def _on_chunk(n: int) -> None:
            current = holder["tracker"]
            if current is not None:
                current.add_downloaded(n)

        try:
            backup_peer = await _get_backup_group_peer()
        except Exception as e:
            await safe_edit(status_msg, f"❌ Gagal mendapat backup group: {e}")
            return

        work_dir = tempfile.mkdtemp(prefix="hls_job_")
        result = await download_hls_to_mp4(
            url,
            file_name,
            headers=headers,
            status_cb=lambda text: safe_edit(status_msg, text),
            on_chunk=_on_chunk,
            on_meta=_on_meta,
            out_dir=work_dir,
            # A page resolved by a browser that had to go direct must download
            # directly too: its token is bound to that address.
            proxy_url=None if force_direct else AUTO_PROXY,
        )

        tracker = holder["tracker"]
        if tracker is None:
            tracker = ProgressTracker(
                status_msg, mp4_name, 1,
                reply_markup=process_cancel_keyboard(slot.sid),
            )
            tracker.start()

        if not result.converted:
            print(f"[HLS] Download failed for user {user_id}: {result.reason}")
            await tracker.stop(reason_message(result.reason))
            return

        temp_mp4 = result.path
        file_name = result.name
        file_size = result.size

        # The download is done: hand the same bar over to the upload phase,
        # sized with the REAL MP4 byte count.
        tracker.file_name = file_name
        tracker.file_size = max(1, file_size)
        tracker.downloaded = file_size
        await tracker.stop(f"📄 `{file_name}` ({file_size / (1024 * 1024):.1f} MB)")

        video_meta = await _get_video_metadata(temp_mp4)
        print(f"[HLS] Video metadata: {video_meta}")


        # ---- Oversized MP4: reuse the Direct multi-part splitting flow -----
        if file_size > size_limit:
            import math
            target_part_size = int(size_limit * 0.95)
            num_parts = math.ceil(file_size / target_part_size)
            total_duration = video_meta.get("duration", 0)
            part_duration = total_duration / num_parts if total_duration > 0 else 3600

            await safe_edit(
                status_msg,
                f"✂️ Fail video melebihi had ({file_size / (1024 ** 3):.2f} GB > "
                f"{size_limit / (1024 ** 3):.1f} GB).\n"
                f"Memotong video kepada {num_parts} bahagian…"
            )

            custom_thumb_raw = thumb_raw
            split_dir = tempfile.mkdtemp(prefix="hls_split_")
            base_name, ext_str = os.path.splitext(file_name)
            delivered_count = 0

            try:
                for i in range(num_parts):
                    if is_cancelled(user_id):
                        raise asyncio.CancelledError()

                    part_num = i + 1
                    part_filename = f"{base_name} (Part {part_num}){ext_str}"
                    part_path = os.path.join(split_dir, part_filename)

                    start_sec = i * part_duration
                    dur_sec = (
                        part_duration if part_num < num_parts
                        else (total_duration - start_sec)
                    )
                    if dur_sec <= 0:
                        dur_sec = part_duration

                    await safe_edit(
                        status_msg,
                        f"✂️ Memotong Bahagian {part_num}/{num_parts}: "
                        f"`{part_filename}`…"
                    )

                    ok = await _split_video_part(temp_mp4, start_sec, dur_sec, part_path)
                    if not ok:
                        raise Exception(f"Gagal memotong bahagian {part_num}")

                    part_size = os.path.getsize(part_path)

                    # A custom thumbnail is only used for Part 1; later parts get
                    # a snapshot from their own clip.
                    if part_num == 1 and custom_thumb_raw:
                        part_thumb = custom_thumb_raw
                    else:
                        part_thumb = await _generate_video_thumb(part_path, int(dur_sec))

                    part_meta = await _get_video_metadata(part_path)
                    part_tracker = ProgressTracker(
                        status_msg, part_filename, part_size,
                        reply_markup=process_cancel_keyboard(slot.sid),
                    )
                    part_tracker.start()

                    part_streamer = FileStreamer(part_path, part_filename)
                    part_caption = (
                        generate_video_caption(part_filename, exclude_words)
                        if enable_caption else None
                    )

                    msg_id, is_sent_to_bot = await _upload_file_to_backup(
                        bot, user_client, backup_peer, part_streamer,
                        part_filename, part_size,
                        tracker=part_tracker,
                        thumb_raw=part_thumb,
                        video_meta=part_meta,
                        caption=part_caption,
                    )

                    if hasattr(part_streamer, "close"):
                        await part_streamer.close()

                    if not msg_id:
                        await part_tracker.stop(
                            f"❌ Gagal memuat naik bahagian {part_num}."
                        )
                        return

                    await part_tracker.stop(
                        f"⬆️ Menghantar bahagian {part_num}/{num_parts}…"
                    )
                    delivered = await _send_to_user(
                        bot, user_id, msg_id, is_sent_to_bot, caption=part_caption,
                    )

                    if delivered:
                        delivered_count += 1
                        try:
                            channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                            link = (
                                f"https://t.me/c/{channel_id_str}/{msg_id}"
                                if not is_sent_to_bot else None
                            )
                            await log_forward(
                                message.from_user.username or "Unknown",
                                msg_id,
                                part_size,
                                f"HLS/{part_filename}",
                                link,
                            )
                        except Exception as e:
                            print(f"[HLS] Part logging error: {e}")

                    if os.path.exists(part_path):
                        try:
                            os.remove(part_path)
                        except OSError:
                            pass

                if delivered_count == num_parts:
                    try:
                        await status_msg.delete()
                    except Exception:
                        pass
                else:
                    await safe_edit(
                        status_msg,
                        f"⚠️ Selesai! {delivered_count}/{num_parts} bahagian berjaya dihantar.",
                    )
                return
            finally:
                if os.path.exists(split_dir):
                    shutil.rmtree(split_dir, ignore_errors=True)


        # ---- Single-file flow: thumbnail (custom or generated) -------------
        if not thumb_raw:
            thumb_raw = await _generate_video_thumb(
                temp_mp4, video_meta.get("duration", 0)
            )
            if thumb_raw:
                print(f"[HLS] Thumbnail generated: {len(thumb_raw)} bytes")
        else:
            print("[HLS] Using the user-supplied thumbnail")

        # The download is already fully counted, so FileStreamer gets no
        # on_download_chunk callback — only the upload side moves the bar.
        streamer = FileStreamer(temp_mp4, file_name)

        video_caption = (
            generate_video_caption(file_name, exclude_words) if enable_caption else None
        )

        msg_id, is_sent_to_bot = await _upload_file_to_backup(
            bot, user_client, backup_peer, streamer, file_name, file_size,
            tracker=tracker,
            thumb_raw=thumb_raw,
            video_meta=video_meta,
            caption=video_caption,
        )

        if hasattr(streamer, "close"):
            await streamer.close()

        if not msg_id:
            await tracker.stop("❌ Gagal memuat naik ke grup sandaran.")
            return

        await tracker.stop("⬆️ Menghantar ke anda…")
        delivered = await _send_to_user(
            bot, user_id, msg_id, is_sent_to_bot, caption=video_caption,
        )

        if delivered:
            try:
                channel_id_str = str(BACKUP_GROUP_ID).replace("-100", "")
                link = (
                    f"https://t.me/c/{channel_id_str}/{msg_id}"
                    if not is_sent_to_bot else None
                )
                await log_forward(
                    message.from_user.username or "Unknown",
                    msg_id,
                    file_size,
                    f"HLS/{file_name}",
                    link,
                )
            except Exception as e:
                print(f"[HLS] Logging error: {e}")

            try:
                await status_msg.delete()
            except Exception:
                pass
        else:
            await safe_edit(
                status_msg,
                "⚠️ Selesai! Fail telah dimuat naik ke grup sandaran tetapi "
                "gagal dihantar.",
            )

    except asyncio.CancelledError:
        if tracker:
            await tracker.stop("❌ Dibatalkan oleh pengguna.")
        else:
            await safe_edit(status_msg, "❌ Dibatalkan oleh pengguna.")
    except SessionInvalidError as e:
        print(f"[HLS] Session invalid for user {user_id}: {e}")
        if tracker:
            await tracker.stop("❌ Sesi anda telah tamat. Sila /start dan login semula.")
        else:
            await safe_edit(status_msg, "❌ Sesi anda telah tamat. Sila /start dan login semula.")
    except Exception as e:
        print(f"[HLS] Handler error: {e}")
        import traceback
        traceback.print_exc()
        if tracker:
            await tracker.stop(f"❌ Ralat: {e}")
        else:
            try:
                await safe_edit(status_msg, f"❌ Ralat: {e}")
            except Exception:
                pass
    finally:
        if tracker:
            await tracker.stop()
        # Release only this job's slot — a second job of the same user keeps its
        # own slot, cancellation state and HTTP session.
        release_process(slot)
        # 🧹 Always remove the downloaded MP4 and its workspace, on every exit
        # path (success, split return, cancellation, error).
        if temp_mp4 and os.path.exists(temp_mp4):
            try:
                os.remove(temp_mp4)
            except OSError as e:
                print(f"[HLS] Temp MP4 cleanup error: {e}")
        if work_dir and os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)
        await clear_reply_markup(status_msg)

