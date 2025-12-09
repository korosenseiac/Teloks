from motor.motor_asyncio import AsyncIOMotorClient
from app.config import MONGO_URI
import datetime

client = AsyncIOMotorClient(MONGO_URI)
db = client.telegram_forwarder

users_collection = db.users
logs_collection = db.logs
sessions_collection = db.sessions
settings_collection = db.settings

async def add_user(user_id, username):
    await users_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "username": username,
            "last_active": datetime.datetime.utcnow()
        }},
        upsert=True
    )

async def save_user_profile(user_id, gender, age):
    """Save user's profile (gender and age)."""
    await users_collection.update_one(
        {"_id": user_id},
        {"$set": {
            "gender": gender,
            "age": age,
            "profile_updated_at": datetime.datetime.utcnow()
        }},
        upsert=True
    )

async def get_user_profile(user_id):
    """Get user's profile (gender and age)."""
    user = await users_collection.find_one({"_id": user_id})
    if user and user.get("gender") and user.get("age"):
        return {"gender": user["gender"], "age": user["age"]}
    return None

async def save_backup_group_cache(group_id, access_hash):
    """Save the backup group cache to database for persistence across restarts."""
    await settings_collection.update_one(
        {"_id": "backup_group_cache"},
        {"$set": {
            "group_id": group_id,
            "access_hash": access_hash,
            "updated_at": datetime.datetime.utcnow()
        }},
        upsert=True
    )

async def get_backup_group_cache():
    """Get the cached backup group info from database."""
    return await settings_collection.find_one({"_id": "backup_group_cache"})

async def save_user_session(user_id, session_string, api_id, api_hash):
    """Save the user's custom session for accessing their private chats."""
    await sessions_collection.update_one(
        {"user_id": user_id},
        {"$set": {
            "session_string": session_string,
            "api_id": api_id,
            "api_hash": api_hash
        }},
        upsert=True
    )

async def get_user_session(user_id):
    return await sessions_collection.find_one({"user_id": user_id})

async def log_forward(username, backup_msg_id, file_size, source_name, backup_message_link):
    # Convert file_size to MB format
    file_size_mb = round(file_size / (1024 * 1024), 2) if file_size else 0
    
    await logs_collection.insert_one({
        "username": username,
        "backup_message_id": backup_msg_id,
        "file_size": f"{file_size_mb} MB",
        "source_name": source_name,
        "backup_message_link": backup_message_link,
        "timestamp": datetime.datetime.utcnow()
    })

async def get_all_logs():
    cursor = logs_collection.find({}).sort("timestamp", -1)
    return await cursor.to_list(length=1000)
