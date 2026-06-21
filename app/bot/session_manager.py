import asyncio
from pyrogram import Client
from pyrogram.errors.exceptions.unauthorized_401 import (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionRevoked,
    SessionExpired,
    UserDeactivated,
    UserDeactivatedBan,
)
from app.database.db import get_user_session, delete_user_session

# Some Pyrogram versions expose UserDeactivatedSanitized under forbidden_403.
# Import it defensively so we don't crash on older/newer versions.
try:
    from pyrogram.errors.exceptions.forbidden_403 import UserDeactivatedSanitized
    _FORBIDDEN_SESSION_ERRORS = (UserDeactivatedSanitized,)
except ImportError:
    _FORBIDDEN_SESSION_ERRORS = ()


# Errors that indicate the saved session is no longer valid server-side.
# Retrying with the same session is pointless — the user must re-login.
_SESSION_INVALID_ERRORS = (
    AuthKeyUnregistered,
    AuthKeyInvalid,
    SessionRevoked,
    SessionExpired,
    UserDeactivated,
    UserDeactivatedBan,
) + _FORBIDDEN_SESSION_ERRORS


class SessionInvalidError(Exception):
    """Raised when a user's saved session is no longer valid on Telegram's side."""


def is_session_invalid_error(exc: BaseException) -> bool:
    """Return True if *exc* indicates the session is dead (revoked/deactivated/etc)."""
    return isinstance(exc, _SESSION_INVALID_ERRORS)


class UserClientManager:
    def __init__(self):
        self.clients = {} # Cache active clients: user_id -> Client

    async def get_client(self, user_id: int):
        """
        Retrieves an active client for the user, or starts a new one if it exists in DB.

        Performs a pre-flight ``get_me()`` after (re)starting a client so that a
        revoked/dead session is detected immediately and the user is told to
        re-login, rather than failing mid-upload with AUTH_KEY_UNREGISTERED.
        """
        if user_id in self.clients:
            client = self.clients[user_id]
            if client.is_connected:
                # Verify the session is still alive server-side.
                try:
                    await client.get_me()
                    return client
                except Exception as e:
                    if is_session_invalid_error(e):
                        print(f"[SessionManager] Cached session for user {user_id} is invalid: {e}")
                        await self.invalidate(user_id)
                        return None
                    # Transient error — keep the cached client, let caller retry.
                    print(f"[SessionManager] get_me() failed for user {user_id} (transient): {e}")
                    return client
            else:
                # If disconnected, try to reconnect
                try:
                    await client.start()
                except Exception as e:
                    if is_session_invalid_error(e):
                        print(f"[SessionManager] Reconnect failed (session invalid) for user {user_id}: {e}")
                        await self.invalidate(user_id)
                        return None
                    del self.clients[user_id]
                    # Fall through to DB lookup below.

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
        except Exception as e:
            if is_session_invalid_error(e):
                print(f"[SessionManager] Start failed (session invalid) for user {user_id}: {e}")
                await self.invalidate(user_id)
                return None
            print(f"Failed to start client for user {user_id}: {e}")
            return None

        # Pre-flight: confirm the session is actually alive server-side.
        try:
            await client.get_me()
        except Exception as e:
            if is_session_invalid_error(e):
                print(f"[SessionManager] Pre-flight get_me() failed (session invalid) for user {user_id}: {e}")
                await self.invalidate(user_id)
                return None
            # Transient error — return the client anyway; caller will retry on real failure.
            print(f"[SessionManager] Pre-flight get_me() failed (transient) for user {user_id}: {e}")

        self.clients[user_id] = client
        return client

    async def invalidate(self, user_id: int):
        """Stop and evict the cached client, and delete the dead session from DB."""
        client = self.clients.pop(user_id, None)
        if client is not None:
            try:
                await client.stop()
            except Exception as e:
                print(f"[SessionManager] Error stopping client for user {user_id}: {e}")
        try:
            await delete_user_session(user_id)
            print(f"[SessionManager] Deleted dead session for user {user_id}")
        except Exception as e:
            print(f"[SessionManager] Failed to delete session for user {user_id}: {e}")

    async def stop_client(self, user_id: int):
        if user_id in self.clients:
            await self.clients[user_id].stop()
            del self.clients[user_id]

manager = UserClientManager()
