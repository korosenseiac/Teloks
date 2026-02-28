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
