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
TORRENT_MAX_SIZE = int(os.getenv("TORRENT_MAX_SIZE", str(4 * 1024 * 1024 * 1024)))  # 4 GB default
TORRENT_DOWNLOAD_DIR = os.getenv("TORRENT_DOWNLOAD_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp", "torrent_downloads"))
