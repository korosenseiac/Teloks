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

# Ensure system dependencies for RAR extraction and video thumbnails
apt-get install -y unrar ffmpeg aria2 >/dev/null 2>&1 || true

cd $BOT_DIR
sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
echo -e "${GREEN}[✓]${NC} Dependencies updated"

# Step 4.5: Update management script
echo -e "${BLUE}[4.5/5] Updating management script...${NC}"
cat > /usr/local/bin/bot << 'EOF'
#!/bin/bash

SERVICE_NAME="telegram-forwarder"
BOT_DIR="/opt/telegram-forwarder-bot"

case "$1" in
    start)
        sudo systemctl start $SERVICE_NAME
        echo "Bot started"
        ;;
    stop)
        sudo systemctl stop $SERVICE_NAME
        echo "Bot stopped"
        ;;
    restart)
        sudo systemctl restart $SERVICE_NAME
        echo "Bot restarted"
        ;;
    status)
        sudo systemctl status $SERVICE_NAME
        ;;
    logs)
        sudo journalctl -u $SERVICE_NAME -f --no-pager
        ;;
    logs-tail)
        sudo journalctl -u $SERVICE_NAME -n ${2:-100} --no-pager
        ;;
    edit-env)
        sudo nano $BOT_DIR/.env
        ;;
    edit-proxy)
        sudo nano $BOT_DIR/proxy.txt
        ;;
    update)
        echo "========================================="
        echo "  Updating Bot from GitHub..."
        echo "========================================="
        echo ""
        echo "[1/5] Stopping bot..."
        sudo systemctl stop $SERVICE_NAME
        echo "Bot stopped."
        echo ""
        echo "[2/5] Creating backup..."
        BACKUP_DIR="/opt/telegram-forwarder-bot-backup-$(date +%Y%m%d_%H%M%S)"
        sudo mkdir -p $BACKUP_DIR
        sudo cp -r $BOT_DIR/* $BACKUP_DIR/ 2>/dev/null || true
        echo "Backup saved at: $BACKUP_DIR"
        echo ""
        echo "[3/5] Pulling latest code from GitHub..."
        cd $BOT_DIR
        # Initialize git if not already a repo
        if [ ! -d "$BOT_DIR/.git" ]; then
            sudo -u botuser git init
            sudo -u botuser git remote add origin https://github.com/korosenseiac/Teloks.git
        fi
        # Stash any local changes, pull latest, then restore .env and sessions
        sudo chown -R botuser:botuser $BOT_DIR/.git
        sudo -u botuser git fetch origin
        sudo -u botuser git reset --hard origin/main 2>/dev/null || sudo -u botuser git reset --hard origin/master
        # Restore preserved files from backup
        sudo cp $BACKUP_DIR/.env $BOT_DIR/.env 2>/dev/null || true
        sudo cp $BACKUP_DIR/*.session $BOT_DIR/ 2>/dev/null || true
        sudo chown -R botuser:botuser $BOT_DIR
        echo "Code updated to latest version."
        echo ""
        echo "[4/5] Updating dependencies..."
        sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
        echo "Dependencies updated."
        echo ""
        echo "[5/5] Starting bot..."
        sudo systemctl start $SERVICE_NAME
        echo "Bot started."
        echo ""
        echo "========================================="
        echo "  Update Complete!"
        echo "========================================="
        echo "Backup: $BACKUP_DIR"
        echo "Check status: bot status"
        echo "View logs: bot logs"
        ;;
    *)
        echo "Telegram Forwarder Bot Management"
        echo ""
        echo "Usage: bot <command>"
        echo ""
        echo "Commands:"
        echo "  start       - Start the bot"
        echo "  stop        - Stop the bot"
        echo "  restart     - Restart the bot"
        echo "  status      - Show bot status"
        echo "  logs        - Show live logs (Ctrl+C to exit)"
        echo "  logs-tail   - Show last N logs (default 100)"
        echo "  edit-env    - Edit environment variables"
        echo "  edit-proxy  - Edit HTTP proxy configuration"
        echo "  update      - Pull latest version from GitHub and restart"
        ;;
esac
EOF
chmod +x /usr/local/bin/bot
echo -e "${GREEN}[✓]${NC} Management script updated"

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
