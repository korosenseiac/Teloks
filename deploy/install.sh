#!/bin/bash

# =============================================================================
# Telegram Forwarder Bot - Ubuntu 24 VPS Installation Script
# =============================================================================
# This script will:
# 1. Install required system dependencies
# 2. Create a dedicated user for the bot
# 3. Set up Python virtual environment
# 4. Install Python dependencies
# 5. Create systemd service for auto-restart
# 6. Enable and start the bot service
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BOT_NAME="telegram-forwarder-bot"
BOT_USER="botuser"
BOT_DIR="/opt/$BOT_NAME"
SERVICE_NAME="telegram-forwarder"
PYTHON_VERSION="python3.12"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Telegram Forwarder Bot Installer${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

# Function to print status
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

# Step 1: Update system and install dependencies
echo ""
echo -e "${BLUE}Step 1: Installing system dependencies...${NC}"
apt update && apt upgrade -y
apt install -y python3.12 python3.12-venv python3-pip git curl wget unrar ffmpeg

print_status "System dependencies installed"

# Step 2: Create bot user (if not exists)
echo ""
echo -e "${BLUE}Step 2: Creating bot user...${NC}"
if id "$BOT_USER" &>/dev/null; then
    print_warning "User $BOT_USER already exists"
else
    useradd -r -m -s /bin/bash $BOT_USER
    print_status "User $BOT_USER created"
fi

# Step 3: Create bot directory
echo ""
echo -e "${BLUE}Step 3: Setting up bot directory...${NC}"
mkdir -p $BOT_DIR
chown -R $BOT_USER:$BOT_USER $BOT_DIR
print_status "Directory $BOT_DIR created"

# Step 4: Copy bot files (if script is in deploy folder)
echo ""
echo -e "${BLUE}Step 4: Copying bot files...${NC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

# Check if we're in the deploy folder
if [ -f "$PARENT_DIR/main.py" ]; then
    cp -r "$PARENT_DIR"/* $BOT_DIR/
    rm -rf $BOT_DIR/deploy  # Remove deploy folder from installation
    chown -R $BOT_USER:$BOT_USER $BOT_DIR
    print_status "Bot files copied to $BOT_DIR"
else
    print_warning "Bot files not found in parent directory"
    print_info "Please copy your bot files to $BOT_DIR manually"
fi

# Step 5: Create virtual environment and install dependencies
echo ""
echo -e "${BLUE}Step 5: Setting up Python virtual environment...${NC}"
cd $BOT_DIR
sudo -u $BOT_USER $PYTHON_VERSION -m venv venv
sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install --upgrade pip
sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install -r requirements.txt
print_status "Virtual environment created and dependencies installed"

# Step 6: Create .env file template if not exists
echo ""
echo -e "${BLUE}Step 6: Setting up environment configuration...${NC}"
if [ ! -f "$BOT_DIR/.env" ]; then
    cat > $BOT_DIR/.env << 'EOF'
# Telegram API Credentials (get from https://my.telegram.org)
API_ID=your_api_id_here
API_HASH=your_api_hash_here

# Bot Token (get from @BotFather)
BOT_TOKEN=your_bot_token_here

# MongoDB Atlas URI (get from MongoDB Atlas dashboard)
MONGO_URI=mongodb+srv://username:password@cluster.xxxxx.mongodb.net/telegram_forwarder?retryWrites=true&w=majority

# Backup Group ID (the private group for storage)
BACKUP_GROUP_ID=-1001234567890

# Owner Telegram ID
OWNER_ID=your_telegram_id_here
EOF
    chown $BOT_USER:$BOT_USER $BOT_DIR/.env
    chmod 600 $BOT_DIR/.env
    print_warning "Created .env template - Please edit $BOT_DIR/.env with your credentials!"
else
    print_status ".env file already exists"
fi

# Step 7: Create systemd service
echo ""
echo -e "${BLUE}Step 7: Creating systemd service...${NC}"
cat > /etc/systemd/system/$SERVICE_NAME.service << EOF
[Unit]
Description=Telegram Forwarder Bot
After=network.target network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=$BOT_USER
Group=$BOT_USER
WorkingDirectory=$BOT_DIR
Environment=PATH=$BOT_DIR/venv/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=$BOT_DIR/venv/bin/python main.py

# Restart configuration - restart on any failure
Restart=always
RestartSec=10
StartLimitBurst=5

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$BOT_DIR

# Resource limits (optimized for 2 vCPU, 1GB RAM VPS)
MemoryMax=700M
MemoryHigh=600M
CPUQuota=150%

[Install]
WantedBy=multi-user.target
EOF

print_status "Systemd service created"

# Step 8: Reload systemd and enable service
echo ""
echo -e "${BLUE}Step 8: Enabling service...${NC}"
systemctl daemon-reload
systemctl enable $SERVICE_NAME
print_status "Service enabled to start on boot"

# Step 9: Create management script
echo ""
echo -e "${BLUE}Step 9: Creating management commands...${NC}"
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
        echo "  update      - Pull latest version from GitHub and restart"
        ;;
esac
EOF

chmod +x /usr/local/bin/bot
print_status "Management command 'bot' created"

# Done!
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}IMPORTANT: Before starting the bot:${NC}"
echo ""
echo "1. Edit the environment file with your credentials:"
echo -e "   ${BLUE}sudo nano $BOT_DIR/.env${NC}"
echo ""
echo "2. Start the bot:"
echo -e "   ${BLUE}bot start${NC}"
echo ""
echo "3. Check bot status:"
echo -e "   ${BLUE}bot status${NC}"
echo ""
echo "4. View live logs:"
echo -e "   ${BLUE}bot logs${NC}"
echo ""
echo -e "${GREEN}The bot will automatically restart if it crashes!${NC}"
echo ""
