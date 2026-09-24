import os
from dotenv import load_dotenv

load_dotenv()

# Admin/Bot Credentials
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Database
MONGO_URI = os.getenv("MONGO_URI", "")

# Configuration
BACKUP_GROUP_ID = int(os.getenv("BACKUP_GROUP_ID", "0")) # The private group for storage
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

# TeraBox
TERABOX_NDUS = os.getenv("TERABOX_NDUS", "")

# Torrent (aria2c)
ARIA2_RPC_PORT = int(os.getenv("ARIA2_RPC_PORT", "6800"))
ARIA2_RPC_SECRET = os.getenv("ARIA2_RPC_SECRET", "")
# Hard ceiling for the TOTAL torrent size. Oversized videos are split into
# parts automatically (2 GB regular / 4 GB premium per-file limit), so this
# only guards against absurdly large multi-hundred-GB torrents.
TORRENT_MAX_SIZE = int(os.getenv("TORRENT_MAX_SIZE", str(50 * 1024 * 1024 * 1024)))  # 50 GB default
TORRENT_DOWNLOAD_DIR = os.getenv("TORRENT_DOWNLOAD_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp", "torrent_downloads"))

# Torrent timeouts (seconds)
TORRENT_STALL_TIMEOUT = int(os.getenv("TORRENT_STALL_TIMEOUT", "600"))   # abort after Ns with no progress
TORRENT_TOTAL_TIMEOUT = int(os.getenv("TORRENT_TOTAL_TIMEOUT", "7200"))  # abort after Ns overall wait

# Peer discovery (aria2c)
# aria2 binds ONE fixed port for both incoming peer connections (TCP) and DHT
# (UDP). A fixed, non-default port avoids the ISP-throttled 6881-6999 range
# and lets the VPS firewall be opened reliably (see deploy/install.sh).
TORRENT_LISTEN_PORT = int(os.getenv("TORRENT_LISTEN_PORT", "51413"))
# aria2 retries IPv6 (AAAA) lookups / connections before falling back to IPv4.
# On servers without working IPv6 this shows up as "many seeders but no peers".
# Set to false if your host has working IPv6 and the swarm is IPv6-friendly.
TORRENT_DISABLE_IPV6 = os.getenv("TORRENT_DISABLE_IPV6", "true").lower() in ("1", "true", "yes", "on")
# Optional comma-separated override for the public fallback tracker list.
TORRENT_TRACKERS = os.getenv("TORRENT_TRACKERS", "")

# ---------------------------------------------------------------------------
# MKV → MP4 remux (stream copy — no re-encode, no quality loss)
# ---------------------------------------------------------------------------
# Enabled by default. Conversion is strictly best-effort: any failure (no
# ffmpeg, unsupported codec, low disk, timeout, cancellation) falls back to
# uploading the original MKV, so this can never break an existing flow.
MKV_TO_MP4 = os.getenv("MKV_TO_MP4", "true").lower() in ("1", "true", "yes", "on")

# Extensions that get remuxed. MP4 (H.264 + AAC) is what Telegram streams.
CONVERT_EXTENSIONS = {".mkv"}

# Hard timeout (seconds) for a single ffmpeg remux before it is killed.
CONVERT_TIMEOUT = int(os.getenv("CONVERT_TIMEOUT", "1800"))

# How many conversions may run at once. 1 keeps the bot comfortably inside the
# service's MemoryMax cgroup limit and avoids disk I/O thrash on a small VPS.
CONVERT_CONCURRENCY = int(os.getenv("CONVERT_CONCURRENCY", "1"))

# Where converted MP4s are written. Empty = next to the source file (default),
# which keeps every converted file inside the pipeline's own temp directory so
# the existing cleanup sweeps remove it automatically. Only point this at a
# central directory if you rely on cleanup_orphaned_convert_dirs() too.
CONVERT_TEMP_DIR = os.getenv("CONVERT_TEMP_DIR", "")

# ---------------------------------------------------------------------------
# HLS / m3u8 → MP4 (download + stream-copy remux)
# ---------------------------------------------------------------------------
# Enabled by default. The stream is downloaded (parallel, SOCKS5-proxy aware)
# and remuxed into MP4 with the SAME method as MKV → MP4 above: ffmpeg stream
# copy, no re-encode, no quality loss. Every failure path falls back to the
# next engine, and a complete failure reports an error instead of uploading a
# broken file.
HLS_ENABLED = os.getenv("HLS_ENABLED", "true").lower() in ("1", "true", "yes", "on")

# Which download engine to use:
#   auto     — proxy-aware Python segment downloader first, ffmpeg as fallback
#              (recommended: the segment downloader is parallel AND routes
#               through the same proxy.txt SOCKS5 tunnel as every other
#               download in this bot, which ffmpeg cannot use)
#   segments — Python segment downloader only
#   ffmpeg   — let ffmpeg fetch the playlist itself (single process, sequential,
#              no SOCKS5 support)
HLS_ENGINE = os.getenv("HLS_ENGINE", "auto").strip().lower()

# Highest variant to keep from a master playlist (0 = best available).
# e.g. 720 downloads the best variant up to 720p, which keeps MP4s small.
HLS_MAX_HEIGHT = int(os.getenv("HLS_MAX_HEIGHT", "0"))

# Cap the recording length in seconds. Live streams (no #EXT-X-ENDLIST) never
# end on their own, and a very long VOD would fill the disk before uploading;
# 0 disables the cap (live streams then record until the user taps 🚫 Batal).
HLS_MAX_DURATION = int(os.getenv("HLS_MAX_DURATION", "0"))

# Parallel segment downloads and per-segment retries.
HLS_SEGMENT_WORKERS = int(os.getenv("HLS_SEGMENT_WORKERS", "4"))
HLS_SEGMENT_RETRIES = int(os.getenv("HLS_SEGMENT_RETRIES", "3"))

# Hard timeout (seconds) for one ffmpeg download/remux before it is killed.
HLS_TIMEOUT = int(os.getenv("HLS_TIMEOUT", "1800"))

# Last-resort full re-encode when the stream cannot be stream-copied at all
# (rare: exotic codecs). Costs CPU time — off by default.
HLS_REENCODE_FALLBACK = os.getenv("HLS_REENCODE_FALLBACK", "false").lower() in ("1", "true", "yes", "on")

# Where the finished MP4 is written. Empty = the system temp dir (the job's own
# temp directory owns the cleanup, plus cleanup_orphaned_hls_dirs() at startup).
HLS_TEMP_DIR = os.getenv("HLS_TEMP_DIR", "")

# Ignore proxy.txt for HLS downloads and connect directly instead. Useful when
# the configured SOCKS5 proxy is blocked by (or cannot reach) the HLS CDN.
HLS_DISABLE_PROXY = os.getenv("HLS_DISABLE_PROXY", "false").lower() in ("1", "true", "yes", "on")

# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------
# How many download/upload jobs a SINGLE user may run at the same time.
# Every job gets its own "🚫 Batal" button and a slot is reserved as soon as a
# link is accepted (a job waiting at the caption prompt already counts), so a
# user can never start more jobs than this even by spamming links.
# 2 doubles per-user throughput. Keep it in mind together with the service
# limits in deploy/install.sh (MemoryMax / CPUQuota) and CONVERT_CONCURRENCY.
MAX_CONCURRENT_PROCESSES = int(os.getenv("MAX_CONCURRENT_PROCESSES", "2"))

