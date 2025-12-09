#!/bin/bash

# =============================================================================
# Telegram Forwarder Bot - Update Script
# =============================================================================
# Use this script to update bot files from a new version
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BOT_DIR="/opt/telegram-forwarder-bot"
SERVICE_NAME="telegram-forwarder"
BACKUP_DIR="/opt/telegram-forwarder-bot-backup-$(date +%Y%m%d_%H%M%S)"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Telegram Forwarder Bot - Updater${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

# Check if new files are provided
if [ ! -f "main.py" ]; then
    echo -e "${RED}Error: No bot files found in current directory${NC}"
    echo "Please run this script from the directory containing the new bot files"
    exit 1
fi

echo -e "${BLUE}Step 1: Creating backup...${NC}"
mkdir -p $BACKUP_DIR
cp -r $BOT_DIR/* $BACKUP_DIR/ 2>/dev/null || true
echo -e "${GREEN}[✓]${NC} Backup created at $BACKUP_DIR"

echo -e "${BLUE}Step 2: Stopping bot...${NC}"
systemctl stop $SERVICE_NAME
echo -e "${GREEN}[✓]${NC} Bot stopped"

echo -e "${BLUE}Step 3: Updating files...${NC}"
# Preserve .env and session files
cp $BOT_DIR/.env /tmp/bot_env_backup 2>/dev/null || true
cp $BOT_DIR/*.session /tmp/ 2>/dev/null || true

# Copy new files (exclude deploy folder)
rsync -av --exclude='deploy' --exclude='.env' --exclude='*.session' --exclude='venv' ./ $BOT_DIR/

# Restore preserved files
cp /tmp/bot_env_backup $BOT_DIR/.env 2>/dev/null || true
cp /tmp/*.session $BOT_DIR/ 2>/dev/null || true

chown -R botuser:botuser $BOT_DIR
echo -e "${GREEN}[✓]${NC} Files updated"

echo -e "${BLUE}Step 4: Updating dependencies...${NC}"
cd $BOT_DIR
sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
echo -e "${GREEN}[✓]${NC} Dependencies updated"

echo -e "${BLUE}Step 5: Starting bot...${NC}"
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
