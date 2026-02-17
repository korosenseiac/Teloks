#!/bin/bash

# =============================================================================
# Telegram Forwarder Bot - Quick Deploy Script
# =============================================================================
# One-liner deploy: Upload your bot files to VPS and run this script
# Usage: curl -sSL <url>/quick-deploy.sh | sudo bash
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

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Quick Deploy - Telegram Forwarder Bot${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: Please run as root (use sudo)${NC}"
    exit 1
fi

# Check if bot directory already exists (update mode)
if [ -d "$BOT_DIR" ] && [ -f "$BOT_DIR/main.py" ]; then
    echo -e "${YELLOW}Existing installation detected. Running update...${NC}"
    
    # Stop service
    systemctl stop $SERVICE_NAME 2>/dev/null || true
    
    # Update dependencies
    cd $BOT_DIR
    sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
    
    # Start service
    systemctl start $SERVICE_NAME
    
    echo -e "${GREEN}Update complete! Bot restarted.${NC}"
    exit 0
fi

# Fresh install
echo -e "${BLUE}Running fresh installation...${NC}"

# Install dependencies
apt update
apt install -y python3.12 python3.12-venv python3-pip

# Create user
if ! id "$BOT_USER" &>/dev/null; then
    useradd -r -m -s /bin/bash $BOT_USER
fi

# Setup directory
mkdir -p $BOT_DIR

# Check if we're in a directory with bot files
if [ -f "main.py" ]; then
    cp -r ./* $BOT_DIR/
    rm -rf $BOT_DIR/deploy 2>/dev/null || true
else
    echo -e "${RED}Error: No bot files found in current directory${NC}"
    echo "Please run this script from the directory containing main.py"
    exit 1
fi

chown -R $BOT_USER:$BOT_USER $BOT_DIR

# Setup venv
cd $BOT_DIR
sudo -u $BOT_USER python3.12 -m venv venv
sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install --upgrade pip
sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install -r requirements.txt

# Create .env if not exists
if [ ! -f "$BOT_DIR/.env" ]; then
    cat > $BOT_DIR/.env << 'EOF'
API_ID=your_api_id_here
API_HASH=your_api_hash_here
BOT_TOKEN=your_bot_token_here
MONGO_URI=mongodb+srv://username:password@cluster.xxxxx.mongodb.net/telegram_forwarder?retryWrites=true&w=majority
BACKUP_GROUP_ID=-1001234567890
OWNER_ID=your_telegram_id_here
EOF
    chown $BOT_USER:$BOT_USER $BOT_DIR/.env
    chmod 600 $BOT_DIR/.env
fi

# Create systemd service
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
Restart=always
RestartSec=10
StartLimitBurst=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$BOT_DIR
MemoryMax=700M
MemoryHigh=600M
CPUQuota=150%

[Install]
WantedBy=multi-user.target
EOF

# Create bot command
cat > /usr/local/bin/bot << 'BOTCMD'
#!/bin/bash
SERVICE_NAME="telegram-forwarder"
BOT_DIR="/opt/telegram-forwarder-bot"
case "$1" in
    start) sudo systemctl start $SERVICE_NAME && echo "Bot started" ;;
    stop) sudo systemctl stop $SERVICE_NAME && echo "Bot stopped" ;;
    restart) sudo systemctl restart $SERVICE_NAME && echo "Bot restarted" ;;
    status) sudo systemctl status $SERVICE_NAME ;;
    logs) sudo journalctl -u $SERVICE_NAME -f --no-pager ;;
    logs-tail) sudo journalctl -u $SERVICE_NAME -n ${2:-100} --no-pager ;;
    edit-env) sudo nano $BOT_DIR/.env ;;
    update)
        echo "========================================="
        echo "  Updating Bot from GitHub..."
        echo "========================================="
        echo ""
        echo "[1/5] Stopping bot..."
        sudo systemctl stop $SERVICE_NAME
        echo ""
        echo "[2/5] Creating backup..."
        BACKUP_DIR="/opt/telegram-forwarder-bot-backup-$(date +%Y%m%d_%H%M%S)"
        sudo mkdir -p $BACKUP_DIR
        sudo cp -r $BOT_DIR/* $BACKUP_DIR/ 2>/dev/null || true
        echo "Backup saved at: $BACKUP_DIR"
        echo ""
        echo "[3/5] Pulling latest code from GitHub..."
        cd $BOT_DIR
        if [ ! -d "$BOT_DIR/.git" ]; then
            sudo -u botuser git init
            sudo -u botuser git remote add origin https://github.com/korosenseiac/Teloks.git
        fi
        sudo -u botuser git fetch origin
        sudo -u botuser git reset --hard origin/main 2>/dev/null || sudo -u botuser git reset --hard origin/master
        sudo cp $BACKUP_DIR/.env $BOT_DIR/.env 2>/dev/null || true
        sudo cp $BACKUP_DIR/*.session $BOT_DIR/ 2>/dev/null || true
        sudo chown -R botuser:botuser $BOT_DIR
        echo "Code updated."
        echo ""
        echo "[4/5] Updating dependencies..."
        sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
        echo ""
        echo "[5/5] Starting bot..."
        sudo systemctl start $SERVICE_NAME
        echo ""
        echo "========================================="
        echo "  Update Complete!"
        echo "========================================="
        ;;
    *) echo "Usage: bot {start|stop|restart|status|logs|logs-tail|edit-env|update}" ;;
esac
BOTCMD
chmod +x /usr/local/bin/bot

# Enable service
systemctl daemon-reload
systemctl enable $SERVICE_NAME

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Installation Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Edit credentials: ${BLUE}sudo nano $BOT_DIR/.env${NC}"
echo "2. Start bot: ${BLUE}bot start${NC}"
echo "3. View logs: ${BLUE}bot logs${NC}"
echo ""
