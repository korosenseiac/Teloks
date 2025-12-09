import asyncio
from pyrogram import Client
from app.database.db import get_user_session

class UserClientManager:
    def __init__(self):
        self.clients = {} # Cache active clients: user_id -> Client

    async def get_client(self, user_id: int):
        """
        Retrieves an active client for the user, or starts a new one if it exists in DB.
        """
        if user_id in self.clients:
            client = self.clients[user_id]
            if client.is_connected:
                return client
            else:
                # If disconnected, try to reconnect
                try:
                    await client.start()
                    return client
                except:
                    del self.clients[user_id]

        # Fetch from DB
        session_data = await get_user_session(user_id)
        if not session_data:
            return None

        # Create new client
        # We use MemoryStorage or just the session string which Pyrogram handles
        client = Client(
            name=f"user_{user_id}",
            api_id=session_data['api_id'],
            api_hash=session_data['api_hash'],
            session_string=session_data['session_string'],
            in_memory=True, # Don't create .session files
            no_updates=True # We don't need to receive updates for the user, just make requests
        )

        try:
            await client.start()
            self.clients[user_id] = client
            return client
        except Exception as e:
            print(f"Failed to start client for user {user_id}: {e}")
            return None

    async def stop_client(self, user_id: int):
        if user_id in self.clients:
            await self.clients[user_id].stop()
            del self.clients[user_id]

manager = UserClientManager()
