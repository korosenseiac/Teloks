"""
hls/playlist.py — Minimal, dependency-free M3U8 (HLS) playlist parser.

Turns a raw playlist into exactly the pieces the HLS pipeline needs, without
pulling an extra third-party parser into the bot:

* ``#EXT-X-STREAM-INF`` variants (``BANDWIDTH`` / ``AVERAGE-BANDWIDTH`` /
  ``RESOLUTION`` / ``CODECS`` / ``AUDIO`` group),
* ``#EXT-X-MEDIA`` audio tracks, so a master whose audio lives in a *separate*
  group still yields a video WITH sound (very common on fMP4/CMAF streams),
* ``#EXTINF`` segments plus ``#EXT-X-BYTERANGE`` and ``#EXT-X-MAP`` (fMP4),
* ``#EXT-X-KEY`` — AES-128 only; DRM (SAMPLE-AES / CENC / licence ``data:``
  URIs) is detected up front so the pipeline can fail with a clear message,
* ``#EXT-X-ENDLIST`` (VOD vs. live) and ``#EXT-X-MEDIA-SEQUENCE``, which must be
  preserved when the playlist is rewritten locally so ffmpeg still derives the
  correct implicit AES-128 IV per segment.

Self-test:

    python -m app.hls.playlist
"""
from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote_to_bytes, urljoin

# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

#: Matches an m3u8/m3u URL (optional query string kept) anywhere in a message.
HLS_LINK_PATTERN = re.compile(
    r"https?://[^\s<>\"']+?\.m3u8?(?:\?[^\s<>\"']*)?",
    re.IGNORECASE,
)

#: The only encryption method this pipeline can handle (ffmpeg decrypts it).
AES128 = "AES-128"

#: Key methods that mean DRM: not decryptable without a licence server.
_UNSUPPORTED_KEY_METHODS = {
    "SAMPLE-AES", "SAMPLE-AES-CTR", "SAMPLE-AES-CENC", "AES-CTR",
}

#: Audio codecs an MP4 container (and therefore Telegram) can play as-is.
#: These are the *CODECS attribute* tokens of a master playlist — the ffprobe
#: codec names used by the downloader live in ``app/utils/video_compressor.py``.
_NON_MP4_AUDIO_TOKENS = (
    "ac-3", "ec-3", "dts", "flac", "opus", "vorbis",
)


_VIDEO_CODEC_PREFIXES = (
    "avc", "hev", "hvc", "av01", "vp0", "vp8", "vp9", "mp4v", "dvh", "dva",
)

_AUDIO_CODEC_PREFIXES = (
    "mp4a", "aac", "mp3", "ac-3", "ec-3", "opus", "flac", "vorbis", "dts", "alac",
)


def strip_bom(text: str) -> str:
    """Drop a leading UTF-8 byte-order mark (``\\ufeff``) and surrounding blanks.

    Plenty of CDNs (and Windows-based packagers) prepend a BOM to the playlist;
    ``"\\ufeff#EXTM3U".startswith("#EXTM3U")`` is False, which used to reject a
    perfectly valid playlist as "not HLS".
    """
    return (text or "").lstrip("\ufeff \t\r\n")


def is_hls_playlist(text: str) -> bool:
    """True when *text* looks like an M3U8 playlist at all (BOM/case tolerant)."""
    return strip_bom(text)[:7].upper() == "#EXTM3U"


def is_master_playlist(text: str) -> bool:
    """True when the playlist lists variants (a master) instead of segments."""
    return "#EXT-X-STREAM-INF" in (text or "").upper()


def decode_data_key_uri(uri: str) -> Optional[bytes]:
    """Decode an inline ``data:`` AES-128 key URI if it holds exactly 16 bytes.

    Valid AES-128 playlists sometimes embed the 16-byte key directly as
    ``data:application/octet-stream;base64,...`` or ``data:text/plain;base64,...``.
    DRM PSSH blobs in ``data:`` URIs are not 16-byte raw AES keys, so returning
    ``None`` for anything other than 16 decoded bytes keeps DRM detection strict.
    """
    if not uri:
        return None
    raw = uri.strip()
    if not raw.lower().startswith("data:"):
        return None
    header, sep, payload = raw[5:].partition(",")
    if not sep:
        return None
    meta = header.lower()
    try:
        if ";base64" in meta or meta.endswith("base64"):
            compact = re.sub(r"\s+", "", payload)
            padded = compact + "=" * (-len(compact) % 4)
            decoded = base64.b64decode(padded, validate=True)
        else:
            decoded = unquote_to_bytes(payload)
    except (binascii.Error, ValueError):
        return None
    return decoded if len(decoded) == 16 else None


def _is_key_attrs_drm(attrs: Dict[str, str]) -> bool:
    """True when a single ``#EXT-X-KEY`` / ``#EXT-X-SESSION-KEY`` attribute map is DRM."""
    method = (attrs.get("METHOD") or "").upper()
    if not method or method == "NONE":
        return False
    if method in _UNSUPPORTED_KEY_METHODS or method != AES128:
        return True
    keyformat = (attrs.get("KEYFORMAT") or "").strip().lower()
    if keyformat and keyformat != "identity":
        return True
    uri = (attrs.get("URI") or "").strip()
    if uri.lower().startswith("data:"):
        return decode_data_key_uri(uri) is None
    return False


def is_drm(text: str) -> bool:
    """True when the playlist is DRM protected (cannot be downloaded here).

    Multi-key playlists (RFC 8216) may advertise both an ``AES-128`` (identity)
    key and a Widevine/FairPlay ``SAMPLE-AES`` key for the same segment group.
    A key group is only treated as DRM when *none* of its keys is usable
    (``NONE`` or valid ``AES-128``).
    """
    session_keys: List[Dict[str, str]] = []
    pending_keys: List[Dict[str, str]] = []

    def _group_is_drm(group: List[Dict[str, str]]) -> bool:
        return bool(group) and all(_is_key_attrs_drm(a) for a in group)

    for line in strip_bom(text).splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        upper = stripped.upper()
        if upper.startswith("#EXT-X-SESSION-KEY"):
            attrs = parse_attributes(stripped.split(":", 1)[1] if ":" in stripped else "")
            session_keys.append(attrs)
            continue
        if upper.startswith("#EXT-X-KEY"):
            attrs = parse_attributes(stripped.split(":", 1)[1] if ":" in stripped else "")
            pending_keys.append(attrs)
            continue
        if not stripped.startswith("#"):
            # Segment URI commits the pending key group.
            if _group_is_drm(pending_keys):
                return True
            pending_keys.clear()

    if _group_is_drm(pending_keys):
        return True
    if session_keys and all(_is_key_attrs_drm(a) for a in session_keys):
        return True
    return False


# ---------------------------------------------------------------------------
# Attribute list parsing (HLS spec: KEY=VALUE,KEY="quoted,value")
# ---------------------------------------------------------------------------

def parse_attributes(text: str) -> Dict[str, str]:
    """Parse an HLS attribute list into a ``{KEY: value}`` dict.

    Handles quoted values that contain commas (``CODECS="avc1,mp4a"``), which a
    naive ``split(",")`` would destroy — the reason this helper exists.
    """
    attrs: Dict[str, str] = {}
    i, n = 0, len(text)
    while i < n:
        while i < n and text[i] in ", \t":
            i += 1
        start = i
        while i < n and text[i] not in "=,":
            i += 1
        key = text[start:i].strip().upper()
        if not key:
            i += 1
            continue
        value = ""
        if i < n and text[i] == "=":
            i += 1
            if i < n and text[i] == '"':
                i += 1
                buf: List[str] = []
                while i < n and text[i] != '"':
                    if text[i] == "\\" and i + 1 < n and text[i + 1] in ('"', "\\"):
                        i += 1
                    buf.append(text[i])
                    i += 1
                value = "".join(buf)
                i += 1  # closing quote
            else:
                start = i
                while i < n and text[i] != ",":
                    i += 1
                value = text[start:i].strip()
        attrs[key] = value
    return attrs


def _int(value: Optional[str]) -> int:
    """Best-effort int() that never raises."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _float(value: Optional[str]) -> float:
    """Best-effort float() that never raises."""
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def _parse_resolution(value: str) -> Tuple[int, int]:
    """``"1920x1080"`` -> ``(1920, 1080)`` (0, 0 when absent/unparsable)."""
    if not value or "x" not in value:
        return 0, 0
    w, _, h = value.partition("x")
    return _int(w), _int(h)


def _parse_byte_range(value: str) -> Optional[Tuple[int, int]]:
    """Parse ``length[@offset]`` into ``(offset, length)`` (offset defaults 0)."""
    parsed = _parse_byte_range_raw(value)
    if parsed is None:
        return None
    offset, length = parsed
    return (offset or 0), length


def _parse_byte_range_raw(value: str) -> Optional[Tuple[Optional[int], int]]:
    """Parse ``length[@offset]`` into ``(offset | None, length)``.

    ``None`` as offset means "not given": RFC 8216 §4.3.2.2 then says the
    sub-range starts right after the previous segment's sub-range, which the
    media parser resolves (the old parser silently restarted at byte 0, so every
    such segment re-downloaded the head of the file).
    """
    value = (value or "").strip().strip('"')
    if not value:
        return None
    length_str, sep, offset_str = value.partition("@")
    length = _int(length_str)
    if length <= 0:
        return None
    return (_int(offset_str) if sep else None), length


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Variant:
    """One ``#EXT-X-STREAM-INF`` entry of a master playlist."""
    uri: str = ""
    url: str = ""            # absolute
    bandwidth: int = 0
    avg_bandwidth: int = 0
    width: int = 0
    height: int = 0
    codecs: str = ""
    audio_group: str = ""

    @property
    def rank_bandwidth(self) -> int:
        """Bandwidth used for ranking: AVERAGE-BANDWIDTH wins over BANDWIDTH."""
        return self.avg_bandwidth or self.bandwidth


@dataclass
class AudioTrack:
    """One ``#EXT-X-MEDIA:TYPE=AUDIO`` entry of a master playlist."""
    group_id: str = ""
    name: str = ""
    language: str = ""
    uri: str = ""
    url: str = ""            # absolute
    default: bool = False
    autoselect: bool = False


@dataclass
class EncryptionKey:
    """An ``#EXT-X-KEY`` entry (``METHOD`` defaults to ``NONE``)."""
    method: str = "NONE"
    uri: str = ""
    url: str = ""
    iv: str = ""
    keyformat: str = ""

    @property
    def is_aes128(self) -> bool:
        return self.method.upper() == AES128


@dataclass
class MediaSegment:
    """A single media segment of a media playlist."""
    url: str = ""
    duration: float = 0.0
    byte_range: Optional[Tuple[int, int]] = None   # (offset, length)
    key_index: int = 0                             # index into MediaPlaylist.keys
    #: Media sequence number (drives the implicit AES-128 IV).
    sequence: int = 0
    #: The ``#EXT-X-MAP`` in force for this segment ("" = none / TS).
    init_url: str = ""
    init_byte_range: Optional[Tuple[int, int]] = None
    #: Index into ``MediaPlaylist.keys`` encrypting this segment's ``#EXT-X-MAP``.
    init_key_index: int = 0
    #: An ``#EXT-X-DISCONTINUITY`` precedes this segment.
    discontinuity: bool = False


@dataclass
class MediaPlaylist:
    """Parsed media playlist (the file that actually lists segments)."""
    segments: List[MediaSegment] = field(default_factory=list)
    keys: List[EncryptionKey] = field(default_factory=lambda: [EncryptionKey()])
    init_url: str = ""                              # first #EXT-X-MAP URI (fMP4)
    init_byte_range: Optional[Tuple[int, int]] = None
    init_key_index: int = 0
    media_sequence: int = 0
    target_duration: int = 0
    version: int = 3
    is_live: bool = True
    is_fmp4: bool = False
    total_duration: float = 0.0

    @property
    def is_encrypted(self) -> bool:
        """True when at least one segment or init map is AES-128 encrypted."""
        return any(k.is_aes128 for k in self.keys)

    @property
    def init_sections(self) -> List[Tuple[str, Optional[Tuple[int, int]]]]:
        """Distinct ``(init_url, init_byte_range)`` pairs, in playlist order."""
        seen: List[Tuple[str, Optional[Tuple[int, int]]]] = []
        for seg in self.segments:
            if seg.init_url and (seg.init_url, seg.init_byte_range) not in seen:
                seen.append((seg.init_url, seg.init_byte_range))
        return seen

    @property
    def init_section_keys(self) -> Dict[Tuple[str, Optional[Tuple[int, int]]], int]:
        """Map each distinct ``(init_url, init_byte_range)`` to its ``key_index``."""
        mapping: Dict[Tuple[str, Optional[Tuple[int, int]]], int] = {}
        if self.init_url:
            mapping[(self.init_url, self.init_byte_range)] = self.init_key_index
        for seg in self.segments:
            if seg.init_url:
                spec = (seg.init_url, seg.init_byte_range)
                if spec not in mapping:
                    mapping[spec] = seg.init_key_index
        return mapping



# ---------------------------------------------------------------------------
# Master playlist
# ---------------------------------------------------------------------------

def parse_master(text: str, base_url: str) -> Tuple[List[Variant], List[AudioTrack]]:
    """Return ``(variants, audio_tracks)`` parsed from a master playlist."""
    variants: List[Variant] = []
    audios: List[AudioTrack] = []
    pending: Optional[Dict[str, str]] = None

    for raw in strip_bom(text).splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-STREAM-INF"):
            pending = parse_attributes(line.split(":", 1)[1] if ":" in line else "")
            continue
        if line.startswith("#EXT-X-MEDIA"):
            attrs = parse_attributes(line.split(":", 1)[1] if ":" in line else "")
            if (attrs.get("TYPE") or "").upper() == "AUDIO" and attrs.get("URI"):
                audios.append(AudioTrack(
                    group_id=attrs.get("GROUP-ID", ""),
                    name=attrs.get("NAME", ""),
                    language=attrs.get("LANGUAGE", ""),
                    uri=attrs["URI"],
                    url=urljoin(base_url, attrs["URI"]),
                    default=(attrs.get("DEFAULT", "").upper() == "YES"),
                    autoselect=(attrs.get("AUTOSELECT", "").upper() == "YES"),
                ))
            continue
        if line.startswith("#"):
            continue
        if pending is not None:
            width, height = _parse_resolution(pending.get("RESOLUTION", ""))
            variants.append(Variant(
                uri=line,
                url=urljoin(base_url, line),
                bandwidth=_int(pending.get("BANDWIDTH")),
                avg_bandwidth=_int(pending.get("AVERAGE-BANDWIDTH")),
                width=width,
                height=height,
                codecs=pending.get("CODECS", ""),
                audio_group=pending.get("AUDIO", ""),
            ))
            pending = None

    return variants, audios


def is_audio_only_variant(variant: Variant) -> bool:
    """True when a ``#EXT-X-STREAM-INF`` entry advertises only audio codecs."""
    if variant.width > 0 or variant.height > 0:
        return False
    codecs = (variant.codecs or "").lower()
    if not codecs:
        return False
    tokens = [part.strip() for part in codecs.split(",") if part.strip()]
    if not tokens:
        return False
    has_video = any(t.startswith(_VIDEO_CODEC_PREFIXES) for t in tokens)
    if has_video:
        return False
    return any(t.startswith(_AUDIO_CODEC_PREFIXES) for t in tokens)


def variant_audio_codec(variant: Variant) -> str:
    """Audio codec token from a variant's ``CODECS`` attribute ("" if unknown)."""
    codecs = (variant.codecs or "").lower()
    if not codecs:
        return ""
    for part in codecs.split(","):
        token = part.strip()
        if not token:
            continue
        if token.startswith(_VIDEO_CODEC_PREFIXES):
            continue  # video codec
        return token
    return ""


def is_mp4_audio(codec: str) -> bool:
    """True when *codec* (a CODECS token) plays from MP4, or is unknown."""
    if not codec:
        # No CODECS information: never downgrade a variant on a guess.
        return True
    lowered = codec.lower()
    if any(token in lowered for token in _NON_MP4_AUDIO_TOKENS):
        return False
    return lowered.startswith(("mp4a", "aac", "mp3", "mp4v.20"))


def select_variant(variants: List[Variant], max_height: int = 0) -> Optional[Variant]:
    """Pick the best variant: highest bandwidth, optionally capped by height.

    *max_height* = 0 means "highest available". When a cap is set but no variant
    fits under it, the smallest video variant is used instead of failing.
    Audio-only ``#EXT-X-STREAM-INF`` entries are excluded whenever at least one
    video variant exists.

    A master often lists the *same* resolution several times, once per audio
    group (``mp4a`` / ``ac-3`` / ``ec-3``). MP4 files with AC-3/E-AC-3 audio are
    not playable on most Telegram clients, so when the top variant's audio codec
    is not MP4-friendly and a same-resolution alternative with compatible audio
    exists, that one is preferred instead — same picture quality, audio that
    actually plays.
    """
    if not variants:
        return None

    video_variants = [v for v in variants if not is_audio_only_variant(v)]
    candidates = video_variants if video_variants else variants

    pool = candidates
    if max_height > 0:
        capped = [v for v in candidates if 0 < v.height <= max_height]
        if capped:
            pool = capped
        else:
            return min(candidates, key=lambda v: v.rank_bandwidth or 10 ** 9)

    best = max(pool, key=lambda v: (v.rank_bandwidth, v.height))
    if is_mp4_audio(variant_audio_codec(best)):
        return best

    alternatives = [
        v for v in pool
        if (v.width, v.height) == (best.width, best.height)
        and is_mp4_audio(variant_audio_codec(v))
        and v.rank_bandwidth >= best.rank_bandwidth * 0.9
    ]
    if alternatives:
        return max(alternatives, key=lambda v: v.rank_bandwidth)
    return best


def rank_variants(variants: List[Variant], max_height: int = 0) -> List[Variant]:
    """All variants, best first: :func:`select_variant`'s pick, then fallbacks.

    The downloader walks this list when the preferred variant's playlist cannot
    be fetched (a dead CDN edge, a 403 on one rendition only, ...), instead of
    failing the whole job on the first variant. Video variants within the height
    cap come first, then video variants above it, and audio-only variants last;
    duplicates (same URL) are listed once.
    """
    if not variants:
        return []
    first = select_variant(variants, max_height)
    video_variants = [v for v in variants if not is_audio_only_variant(v)]
    audio_only = [v for v in variants if is_audio_only_variant(v)]
    capped = [
        v for v in video_variants
        if max_height <= 0 or (0 < v.height <= max_height) or v.height == 0
    ]
    rest_in_cap = sorted(capped, key=lambda v: (v.rank_bandwidth, v.height), reverse=True)
    rest_over = sorted(
        (v for v in video_variants if v not in capped),
        key=lambda v: (v.rank_bandwidth, v.height),
    )
    audio_sorted = sorted(audio_only, key=lambda v: v.rank_bandwidth, reverse=True)
    ordered: List[Variant] = []
    seen = set()
    for v in [first] + rest_in_cap + rest_over + audio_sorted:
        if v is None or not v.url or v.url in seen:
            continue
        seen.add(v.url)
        ordered.append(v)
    return ordered


def _sort_audio_tracks(tracks: List[AudioTrack]) -> List[AudioTrack]:
    ordered: List[AudioTrack] = []
    seen = set()
    for predicate in (
        lambda t: t.default,
        lambda t: t.autoselect,
        lambda t: True,
    ):
        for track in tracks:
            if not track.url or track.url in seen:
                continue
            if predicate(track):
                seen.add(track.url)
                ordered.append(track)
    return ordered


def rank_audio_tracks(
    audios: List[AudioTrack], group_id: str
) -> List[AudioTrack]:
    """Return all audio tracks for *group_id*, best first (DEFAULT -> AUTOSELECT -> rest)."""
    if not group_id:
        return []
    group = [a for a in audios if a.group_id == group_id and a.url]
    return _sort_audio_tracks(group)


def fallback_audio_tracks(audios: List[AudioTrack]) -> List[AudioTrack]:
    """Return all standalone audio tracks across groups, best first."""
    return _sort_audio_tracks([a for a in audios if a.url])


def select_audio_track(
    audios: List[AudioTrack], group_id: str
) -> Optional[AudioTrack]:
    """Pick the audio track of *group_id* (the DEFAULT one when present).

    Returns ``None`` when the variant has no audio group — that means the audio
    is muxed inside the video playlist, so no second input is needed.
    """
    ranked = rank_audio_tracks(audios, group_id)
    return ranked[0] if ranked else None


# ---------------------------------------------------------------------------
# Media playlist
# ---------------------------------------------------------------------------

def _build_encryption_key(attrs: Dict[str, str], base_url: str) -> EncryptionKey:
    uri = attrs.get("URI", "")
    if uri and uri.strip().lower().startswith("data:"):
        resolved_url = uri.strip()
    elif uri:
        resolved_url = urljoin(base_url, uri)
    else:
        resolved_url = ""
    return EncryptionKey(
        method=(attrs.get("METHOD") or "NONE").upper(),
        uri=uri,
        url=resolved_url,
        iv=attrs.get("IV", ""),
        keyformat=attrs.get("KEYFORMAT", ""),
    )


def _pick_best_key_attrs(group: List[Dict[str, str]]) -> Dict[str, str]:
    """Pick the best usable key from consecutive ``#EXT-X-KEY`` tags.

    Prefers a non-DRM ``AES-128`` (identity) key or ``NONE`` over DRM keys
    (Widevine/FairPlay/SAMPLE-AES) advertised for the same segment.
    """
    for attrs in group:
        if not _is_key_attrs_drm(attrs):
            method = (attrs.get("METHOD") or "NONE").upper()
            if method == AES128:
                return attrs
    for attrs in group:
        if not _is_key_attrs_drm(attrs):
            return attrs
    return group[-1]


def parse_media(text: str, base_url: str) -> MediaPlaylist:
    """Parse a media playlist into segments, keys and fMP4 init info.

    Besides the basics this tracks, per segment: its media sequence number (the
    implicit AES-128 IV), the ``#EXT-X-MAP`` in force (and any AES-128 key
    protecting that init section), ``#EXT-X-DISCONTINUITY`` markers, multi-key
    selection (preferring ``AES-128``/``NONE`` over DRM alternatives), and the
    RFC 8216 running offset of ``#EXT-X-BYTERANGE`` entries without ``@offset``.
    """
    text = strip_bom(text)
    pl = MediaPlaylist()
    pl.is_live = "#EXT-X-ENDLIST" not in text.upper()
    current_key = 0
    pending_key_attrs: List[Dict[str, str]] = []
    pending_duration = 0.0
    pending_range: Optional[Tuple[Optional[int], int]] = None
    pending_discontinuity = False
    current_init = ""
    current_init_range: Optional[Tuple[int, int]] = None
    current_init_key = 0
    last_range_end: Dict[str, int] = {}
    sequence_base: Optional[int] = None

    def _commit_pending_keys() -> None:
        nonlocal current_key
        if not pending_key_attrs:
            return
        chosen = _pick_best_key_attrs(pending_key_attrs)
        pl.keys.append(_build_encryption_key(chosen, base_url))
        current_key = len(pl.keys) - 1
        pending_key_attrs.clear()

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        upper = line.upper()
        if upper.startswith("#EXTINF:"):
            pending_duration = _float(line.split(":", 1)[1].split(",")[0])
        elif upper.startswith("#EXT-X-BYTERANGE:"):
            pending_range = _parse_byte_range_raw(line.split(":", 1)[1])
        elif upper.startswith("#EXT-X-KEY:"):
            attrs = parse_attributes(line.split(":", 1)[1])
            pending_key_attrs.append(attrs)
        elif upper.startswith("#EXT-X-MAP:"):
            _commit_pending_keys()
            attrs = parse_attributes(line.split(":", 1)[1])
            uri = attrs.get("URI", "")
            if uri:
                current_init = urljoin(base_url, uri)
                current_init_range = (
                    _parse_byte_range(attrs["BYTERANGE"]) if attrs.get("BYTERANGE") else None
                )
                current_init_key = current_key
                if not pl.init_url:
                    pl.init_url = current_init
                    pl.init_byte_range = current_init_range
                    pl.init_key_index = current_init_key
            pl.is_fmp4 = True
        elif upper.startswith("#EXT-X-DISCONTINUITY") and not upper.startswith(
            "#EXT-X-DISCONTINUITY-SEQUENCE"
        ):
            pending_discontinuity = True
        elif upper.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            pl.media_sequence = _int(line.split(":", 1)[1])
        elif upper.startswith("#EXT-X-TARGETDURATION:"):
            pl.target_duration = _int(line.split(":", 1)[1])
        elif upper.startswith("#EXT-X-VERSION:"):
            pl.version = _int(line.split(":", 1)[1]) or 3
        elif upper.startswith("#EXT-X-PLAYLIST-TYPE:"):
            if line.split(":", 1)[1].strip().upper() == "VOD":
                pl.is_live = False
        elif line.startswith("#"):
            continue
        else:
            # A URI line: the segment itself.
            _commit_pending_keys()
            if sequence_base is None:
                sequence_base = pl.media_sequence
            url = urljoin(base_url, line)
            byte_range: Optional[Tuple[int, int]] = None
            if pending_range is not None:
                offset, length = pending_range
                if offset is None:
                    offset = last_range_end.get(url, 0)
                byte_range = (offset, length)
                last_range_end[url] = offset + length
            pl.segments.append(MediaSegment(
                url=url,
                duration=pending_duration,
                byte_range=byte_range,
                key_index=current_key,
                sequence=sequence_base + len(pl.segments),
                init_url=current_init,
                init_byte_range=current_init_range,
                init_key_index=current_init_key,
                discontinuity=pending_discontinuity,
            ))
            pl.total_duration += pending_duration
            path = line.split("?", 1)[0].lower()
            if path.endswith((".m4s", ".mp4", ".cmfv", ".cmfa", ".m4v", ".m4a")):
                pl.is_fmp4 = True
            pending_duration = 0.0
            pending_range = None
            pending_discontinuity = False

    _commit_pending_keys()
    return pl



# ---------------------------------------------------------------------------
# Self-test / manual smoke check
# ---------------------------------------------------------------------------
# No test framework is used in this project, so this block (run with
# ``python -m app.hls.playlist``) parses inline fixtures and asserts the
# invariants the downloader relies on.

if __name__ == "__main__":
    import sys

    _failed: List[str] = []

    def _check(label: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}{'' if ok else f'  -> {detail}'}")
        if not ok:
            _failed.append(label)

    _MASTER_URL = "https://cdn.example.com/hls/master.m3u8"
    _MASTER = """#EXTM3U
#EXT-X-VERSION:6
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=1292926,BANDWIDTH=1296989,CODECS="avc1.64001e,mp4a.40.2",RESOLUTION=768x432,AUDIO="aud1"
v4/prog_index.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=8399417,BANDWIDTH=8178040,CODECS="avc1.64002a,ac-3",RESOLUTION=1920x1080,AUDIO="aud2"
v9/prog_index.m3u8
#EXT-X-STREAM-INF:AVERAGE-BANDWIDTH=8178040,BANDWIDTH=8207417,CODECS="avc1.64002a,mp4a.40.2",RESOLUTION=1920x1080,AUDIO="aud1"
v9/prog_index.m3u8
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud1",LANGUAGE="en",NAME="English",AUTOSELECT=YES,DEFAULT=YES,CHANNELS="2",URI="a1/prog_index.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud2",LANGUAGE="en",NAME="English 6ch",AUTOSELECT=YES,DEFAULT=YES,CHANNELS="6",URI="a2/prog_index.m3u8"
#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=186522,CODECS="avc1.64002a",RESOLUTION=1920x1080,URI="v7/iframe_index.m3u8"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="sub1",NAME="English",URI="s1/en/prog_index.m3u8"
"""

    _MEDIA = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:7
#EXT-X-KEY:METHOD=AES-128,URI="key.php?token=abc",IV=0x1234
#EXTINF:9.009,
seg0.ts
#EXTINF:9.009,
#EXT-X-BYTERANGE:1048576@0
seg1.ts
#EXTINF:3.003,
seg2.ts
#EXT-X-ENDLIST
"""

    _FMP4 = """#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4",BYTERANGE="1024@0"
#EXTINF:6.0,
#EXT-X-BYTERANGE:5000@1024
seg0.m4s
#EXTINF:6.0,
seg1.m4s
#EXT-X-ENDLIST
"""

    print("--- detection ---")
    _check("is_hls_playlist(master)", is_hls_playlist(_MASTER))
    _check("is_master_playlist(master)", is_master_playlist(_MASTER))
    _check("not is_master_playlist(media)", not is_master_playlist(_MEDIA))
    _check("HLS_LINK_PATTERN matches", bool(HLS_LINK_PATTERN.search(
        "here: https://x.io/a/b/play.m3u8?t=1&k=2 done")))
    _check("HLS_LINK_PATTERN .m3u (legacy)", bool(HLS_LINK_PATTERN.search("http://x.io/live.m3u")))
    _check("HLS_LINK_PATTERN ignores .mp4", not HLS_LINK_PATTERN.search("http://x.io/v.mp4"))

    print("--- attributes ---")
    attrs = parse_attributes('AVERAGE-BANDWIDTH=8144656,CODECS="avc1.64002a,ac-3",RESOLUTION=1920x1080')
    _check("quoted comma survived", attrs.get("CODECS") == "avc1.64002a,ac-3", str(attrs))
    _check("bandwidth parsed", attrs.get("AVERAGE-BANDWIDTH") == "8144656", str(attrs))

    print("--- master ---")
    variants, audios = parse_master(_MASTER, _MASTER_URL)
    _check("3 variants", len(variants) == 3, str(len(variants)))
    _check("iframe variant ignored", all("iframe" not in v.uri for v in variants))
    _check("2 audio sub-groups", len(audios) == 2, str(len(audios)))
    best = select_variant(variants)
    _check("best variant = highest resolution", best is not None and best.height == 1080, str(best))
    _check("AC-3 variant swapped for AAC at same resolution",
           best is not None and best.audio_group == "aud1", str(best))
    _check("audio codec helper: mp4a is safe", is_mp4_audio(variant_audio_codec(best)))
    _check("audio codec helper: ac-3 is not safe", not is_mp4_audio("ac-3"))
    _check("audio codec helper: unknown assumed safe", is_mp4_audio(""))
    capped = select_variant(variants, max_height=720)
    _check("max_height=720 caps the variant", capped is not None and capped.height == 432, str(capped))
    _check("relative URIs resolved", capped.url == "https://cdn.example.com/hls/v4/prog_index.m3u8", capped.url)
    _check("variant audio group kept", capped.audio_group == "aud1", capped.audio_group)
    track = select_audio_track(audios, "aud1")
    _check("audio track of aud1 selected", track is not None and track.url.endswith("a1/prog_index.m3u8"), str(track))
    _check("no audio group -> None", select_audio_track(audios, "") is None)

    print("--- media (TS + AES-128 + byterange) ---")
    media = parse_media(_MEDIA, _MASTER_URL)
    _check("3 segments", len(media.segments) == 3, str(len(media.segments)))
    _check("duration summed", abs(media.total_duration - 21.021) < 0.01, str(media.total_duration))
    _check("VOD detected", media.is_live is False)
    _check("media sequence preserved", media.media_sequence == 7, str(media.media_sequence))
    _check("AES-128 detected", media.is_encrypted)
    _check("key URL resolved", media.keys[1].url == "https://cdn.example.com/hls/key.php?token=abc", media.keys[1].url)
    _check("byterange parsed as (offset, length)", media.segments[1].byte_range == (0, 1048576), str(media.segments[1].byte_range))
    _check("all segments use the encrypted key", all(s.key_index == 1 for s in media.segments))
    _check("segment URL resolved", media.segments[0].url == "https://cdn.example.com/hls/seg0.ts", media.segments[0].url)

    print("--- media (fMP4) ---")
    fmp4 = parse_media(_FMP4, _MASTER_URL)
    _check("fMP4 detected", fmp4.is_fmp4)
    _check("init segment found", fmp4.init_url == "https://cdn.example.com/hls/init.mp4", fmp4.init_url)
    _check("init byterange parsed", fmp4.init_byte_range == (0, 1024), str(fmp4.init_byte_range))
    _check("2 m4s segments", len(fmp4.segments) == 2, str(len(fmp4.segments)))

    print("--- DRM & edge cases ---")
    _check("SAMPLE-AES rejected", is_drm(
        '#EXTM3U\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://x"\nseg.ts\n'))
    _check("data: key rejected", is_drm(
        '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="data:text/plain;base64,AAAA"\nseg.ts\n'))
    _check("FairPlay KEYFORMAT rejected", is_drm(
        '#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI="skd://x",KEYFORMAT="com.apple.streamingkeydelivery"\nseg.ts\n'))
    _check("AES-128 key allowed", not is_drm(_MEDIA))
    _check("no key at all allowed", not is_drm("#EXTM3U\n#EXTINF:9,\nseg.ts\n#EXT-X-ENDLIST\n"))
    _check("UTF-8 BOM tolerated", is_hls_playlist("\ufeff#EXTM3U\n#EXTINF:5,\ns.ts\n#EXT-X-ENDLIST\n"))

    _BYTERANGE_CONT = """#EXTM3U
#EXT-X-VERSION:4
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="media.mp4",BYTERANGE="800@0"
#EXTINF:5.0,
#EXT-X-BYTERANGE:1200@800
media.mp4
#EXTINF:5.0,
#EXT-X-BYTERANGE:1500
media.mp4
#EXT-X-DISCONTINUITY
#EXT-X-MAP:URI="init2.mp4"
#EXTINF:4.0,
seg2.m4s
#EXT-X-ENDLIST
"""
    _br = parse_media(_BYTERANGE_CONT, _MASTER_URL)
    _check("running byterange offset computed",
           _br.segments[1].byte_range == (2000, 1500), str(_br.segments[1].byte_range))
    _check("discontinuity flag preserved", _br.segments[2].discontinuity is True)
    _check("per-segment #EXT-X-MAP preserved",
           _br.segments[0].init_url.endswith("media.mp4")
           and _br.segments[2].init_url.endswith("init2.mp4"),
           str([s.init_url for s in _br.segments]))
    _check("rank_variants lists fallback variants",
           len(rank_variants(variants)) == 2)

    print("---", "ALL PASSED" if not _failed else f"{len(_failed)} FAILURE(S)")
    for name in _failed:
        print(f"    FAILED: {name}")
    sys.exit(1 if _failed else 0)

