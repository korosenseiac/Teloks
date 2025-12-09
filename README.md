# Telegram Forwarder Bot

This bot forwards restricted content (from private channels/groups) to a user by streaming it through a backup group. It uses a "Userbot" (your account) to access the content and a "Bot" to deliver it.

## Prerequisites

1.  **Python 3.10+**
2.  **MongoDB Atlas** account (for database).
3.  **Telegram API ID & Hash** (from [my.telegram.org](https://my.telegram.org)).
4.  **Bot Token** (from [@BotFather](https://t.me/BotFather)).
5.  **Backup Group**: Create a private group, add your Bot as Admin. Get its ID (e.g., `-100xxxx`).

## Installation

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Configure Environment:
    Create a `.env` file in the root directory:
    ```env
    API_ID=123456
    API_HASH=your_api_hash
    BOT_TOKEN=your_bot_token
    MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
    BACKUP_GROUP_ID=-1001234567890
    OWNER_ID=your_telegram_id
    ```

## Running the Bot

Start the application (Bot + Web Dashboard):

```bash
python main.py
```

## Usage

1.  **Start the Bot**: Send `/start` to your bot.
2.  **Login**: Send `/login` to start the interactive login process.
    *   The bot will ask for your **API ID** and **API HASH** (from my.telegram.org).
    *   It will then ask for your **Phone Number**.
    *   Finally, it will ask for the **Login Code** sent to your Telegram account.
    *   (If you have 2FA, it will ask for your password).
    *   Once done, your session is saved securely in the database.
3.  **Forward**: Send a link to a message in a private channel you (the User) have access to.
    *   Example: `https://t.me/c/123456789/100`
4.  **Dashboard**: Visit `http://localhost:8000` to see the logs.

## Deployment (VPS)

1.  Ensure `ffmpeg` is installed (optional, but good for media).
2.  Use a process manager like `systemd` or `Docker`.
# Teloks
# Teloks
