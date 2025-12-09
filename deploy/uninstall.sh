#!/bin/bash

# =============================================================================
# Telegram Forwarder Bot - Uninstall Script
# =============================================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BOT_NAME="telegram-forwarder-bot"
BOT_USER="botuser"
BOT_DIR="/opt/$BOT_NAME"
SERVICE_NAME="telegram-forwarder"

echo -e "${RED}========================================${NC}"
echo -e "${RED}  Telegram Forwarder Bot Uninstaller${NC}"
echo -e "${RED}========================================${NC}"
echo ""

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

read -p "Are you sure you want to uninstall the bot? (y/N): " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Uninstallation cancelled."
    exit 0
fi

echo ""
echo -e "${BLUE}Stopping service...${NC}"
systemctl stop $SERVICE_NAME 2>/dev/null || true
systemctl disable $SERVICE_NAME 2>/dev/null || true

echo -e "${BLUE}Removing systemd service...${NC}"
rm -f /etc/systemd/system/$SERVICE_NAME.service
systemctl daemon-reload

echo -e "${BLUE}Removing management command...${NC}"
rm -f /usr/local/bin/bot

read -p "Remove bot files from $BOT_DIR? (y/N): " remove_files
if [ "$remove_files" = "y" ] || [ "$remove_files" = "Y" ]; then
    echo -e "${BLUE}Removing bot files...${NC}"
    rm -rf $BOT_DIR
fi

read -p "Remove bot user ($BOT_USER)? (y/N): " remove_user
if [ "$remove_user" = "y" ] || [ "$remove_user" = "Y" ]; then
    echo -e "${BLUE}Removing bot user...${NC}"
    userdel -r $BOT_USER 2>/dev/null || true
fi

echo ""
echo -e "${GREEN}Uninstallation complete!${NC}"
