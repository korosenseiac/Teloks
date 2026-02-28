#!/bin/bash

# =============================================================================
# Telegram Forwarder Bot - Update Script
# =============================================================================
# Pulls the latest version from GitHub and restarts the bot
# Usage: sudo bash update-bot.sh
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BOT_DIR="/opt/telegram-forwarder-bot"
SERVICE_NAME="telegram-forwarder"
REPO_URL="https://github.com/korosenseiac/Teloks.git"
BACKUP_DIR="/opt/telegram-forwarder-bot-backup-$(date +%Y%m%d_%H%M%S)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Telegram Forwarder Bot - Updater${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

# Step 1: Create backup
echo -e "${BLUE}[1/5] Creating backup...${NC}"
mkdir -p $BACKUP_DIR
cp -r $BOT_DIR/* $BACKUP_DIR/ 2>/dev/null || true
echo -e "${GREEN}[✓]${NC} Backup created at $BACKUP_DIR"

# Step 2: Stop bot
echo -e "${BLUE}[2/5] Stopping bot...${NC}"
systemctl stop $SERVICE_NAME
echo -e "${GREEN}[✓]${NC} Bot stopped"

# Step 3: Pull latest code from GitHub
echo -e "${BLUE}[3/5] Pulling latest code from GitHub...${NC}"
cd $BOT_DIR

# Initialize git if not already a repo
if [ ! -d "$BOT_DIR/.git" ]; then
    sudo -u botuser git init
    sudo -u botuser git remote add origin $REPO_URL
fi

# Fetch and reset to latest
chown -R botuser:botuser $BOT_DIR/.git
sudo -u botuser git fetch origin
sudo -u botuser git reset --hard origin/main 2>/dev/null || sudo -u botuser git reset --hard origin/master

# Restore .env and session files from backup
cp $BACKUP_DIR/.env $BOT_DIR/.env 2>/dev/null || true
cp $BACKUP_DIR/*.session $BOT_DIR/ 2>/dev/null || true

chown -R botuser:botuser $BOT_DIR
echo -e "${GREEN}[✓]${NC} Code updated to latest version"

# Step 4: Update dependencies
echo -e "${BLUE}[4/5] Updating dependencies...${NC}"

# Ensure system dependency for RAR extraction
apt-get install -y unrar >/dev/null 2>&1 || true

cd $BOT_DIR
sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
echo -e "${GREEN}[✓]${NC} Dependencies updated"

# Step 5: Start bot
echo -e "${BLUE}[5/5] Starting bot...${NC}"
systemctl start $SERVICE_NAME
echo -e "${GREEN}[✓]${NC} Bot started"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Update Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Backup saved at: $BACKUP_DIR"
echo "Check status: bot status"
echo "View logs: bot logs"
echo ""
