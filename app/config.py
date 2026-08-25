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
