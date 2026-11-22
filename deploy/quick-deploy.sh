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
    if [ -f "$BOT_DIR/deploy/update-bot.sh" ]; then
        systemctl stop $SERVICE_NAME 2>/dev/null || true
        SKIP_BACKUP_AND_PULL=1 SKIP_BROWSER="${SKIP_BROWSER:-0}" bash "$BOT_DIR/deploy/update-bot.sh"
        exit $?
    fi

    # Stop service
    systemctl stop $SERVICE_NAME 2>/dev/null || true

    # Update dependencies
    cd $BOT_DIR
    sudo -u $BOT_USER $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
    if [ "${SKIP_BROWSER:-0}" != "1" ] && $BOT_DIR/venv/bin/python -c "import playwright" 2>/dev/null; then
        BROWSER_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-$BOT_DIR/ms-playwright}"
        mkdir -p "$BROWSER_CACHE"
        chown -R $BOT_USER:$BOT_USER "$BROWSER_CACHE"
        $BOT_DIR/venv/bin/python -m playwright install-deps chromium 2>/dev/null || true
        sudo -H -u $BOT_USER env PLAYWRIGHT_BROWSERS_PATH="$BROWSER_CACHE" \
            $BOT_DIR/venv/bin/python -m playwright install chromium 2>/dev/null || true
    fi

    # Start service
    systemctl start $SERVICE_NAME

    echo -e "${GREEN}Update complete! Bot restarted.${NC}"
    exit 0
fi

# Fresh install
echo -e "${BLUE}Running fresh installation...${NC}"

# Install dependencies
apt update
apt install -y python3.12 python3.12-venv python3-pip unrar ffmpeg aria2

# Open the torrent listen port in the firewall (peer discovery).
# aria2c binds ONE fixed port (default 51413, see TORRENT_LISTEN_PORT in .env)
# for incoming peer connections (TCP) and DHT (UDP). Without this the bot can
# only make outbound connections — a common cause of "many seeders but no peers".
TORRENT_PORT="${TORRENT_LISTEN_PORT:-51413}"
if command -v ufw >/dev/null 2>&1; then
    ufw allow ${TORRENT_PORT}/tcp >/dev/null 2>&1 || true
    ufw allow ${TORRENT_PORT}/udp >/dev/null 2>&1 || true
    echo "ufw: opened ${TORRENT_PORT}/tcp+udp (BitTorrent/DHT)"
else
    echo "WARNING: ufw not found — open TCP+UDP ${TORRENT_PORT} (or your TORRENT_LISTEN_PORT) on your VPS firewall / security group"
fi

# Create user
if ! id "$BOT_USER" &>/dev/null; then
    useradd -r -m -s /bin/bash $BOT_USER
fi

# Setup directory
mkdir -p $BOT_DIR

# Check if we're in a directory with bot files
if [ -f "main.py" ]; then
    cp -r ./* $BOT_DIR/
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
TORRENT_LISTEN_PORT=51413
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
# Headless Chromium (app/hls/browser.py) lives here, NOT in the bot user's home:
# ProtectHome=true below empties /home for the service, so a browser under /home
# is invisible to the bot and looks exactly like "Chromium is not downloaded".
# $BOT_DIR is covered by ReadWritePaths, so the service can read it.
Environment=PLAYWRIGHT_BROWSERS_PATH=$BOT_DIR/ms-playwright
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

browser_ready() {
    local cache_dir="${BROWSER_CACHE:-${PLAYWRIGHT_BROWSERS_PATH:-$BOT_DIR/ms-playwright}}"
    sudo -H -u botuser env PLAYWRIGHT_BROWSERS_PATH="$cache_dir" \
        "$BOT_DIR/venv/bin/python" -c 'from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    p.chromium.launch(headless=True).close()' 2>/dev/null
}

active_chromium_revisions() {
    "$BOT_DIR/venv/bin/python" -c '
import json, pathlib, re
import playwright
revs = set()
bj = pathlib.Path(playwright.__file__).resolve().parent / "driver" / "package" / "browsers.json"
if bj.is_file():
    try:
        for b in json.loads(bj.read_text(encoding="utf-8")).get("browsers", []):
            if b.get("name") in ("chromium", "chromium-headless-shell") and b.get("revision"):
                revs.add(str(b["revision"]))
    except Exception:
        pass
if not revs:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        m = re.search(r"chromium(?:_headless_shell)?-(\d+)", p.chromium.executable_path or "")
        if m:
            revs.add(m.group(1))
print(" ".join(sorted(revs)))
' 2>/dev/null || true
}

prune_stale_chromium_builds() {
    local cache_dir="$1"
    [ -n "$cache_dir" ] && [ -d "$cache_dir" ] || return 0
    local active_revs
    active_revs="$(active_chromium_revisions)"
    [ -n "$active_revs" ] || return 0
    local build base rev keep active
    for build in "$cache_dir"/chromium-* "$cache_dir"/chromium_headless_shell-*; do
        [ -d "$build" ] || continue
        base="$(basename "$build")"
        rev="${base#chromium_headless_shell-}"
        rev="${rev#chromium-}"
        case "$rev" in
            ""|*[!0-9]*) continue ;;
        esac
        keep=0
        for active in $active_revs; do
            if [ "$rev" = "$active" ]; then
                keep=1
                break
            fi
        done
        if [ "$keep" = "0" ]; then
            echo "Pruning stale Chromium build: $base"
            sudo rm -rf "$build" || true
        fi
    done
}

ensure_service_browser_path() {
    local cache_dir="${BROWSER_CACHE:-${PLAYWRIGHT_BROWSERS_PATH:-$BOT_DIR/ms-playwright}}"
    [ -f "/etc/systemd/system/$SERVICE_NAME.service" ] || return 0
    if systemctl show -p Environment "$SERVICE_NAME" 2>/dev/null \
        | grep -qF "PLAYWRIGHT_BROWSERS_PATH=$cache_dir" \
      || grep -qF "PLAYWRIGHT_BROWSERS_PATH=$cache_dir" \
           "/etc/systemd/system/$SERVICE_NAME.service.d/browser-cache.conf" 2>/dev/null; then
        return 0
    fi
    echo "Pointing the service at $cache_dir (it cannot see /home)..."
    sudo mkdir -p "/etc/systemd/system/$SERVICE_NAME.service.d" || return 0
    printf '[Service]\n# Written by the updater: ProtectHome=true hides /home from the service,\n# so the Chromium cache must live where the service can read it.\nEnvironment=PLAYWRIGHT_BROWSERS_PATH=%s\n' \
        "$cache_dir" \
        | sudo tee "/etc/systemd/system/$SERVICE_NAME.service.d/browser-cache.conf" >/dev/null || return 0
    sudo systemctl daemon-reload || true
}

case "$1" in
    start) sudo systemctl start $SERVICE_NAME && echo "Bot started" ;;
    stop) sudo systemctl stop $SERVICE_NAME && echo "Bot stopped" ;;
    restart) sudo systemctl restart $SERVICE_NAME && echo "Bot restarted" ;;
    status) sudo systemctl status $SERVICE_NAME ;;
    logs) sudo journalctl -u $SERVICE_NAME -f --no-pager ;;
    logs-tail) sudo journalctl -u $SERVICE_NAME -n ${2:-100} --no-pager ;;
    edit-env) sudo nano $BOT_DIR/.env ;;
    edit-proxy) sudo nano $BOT_DIR/proxy.txt ;;
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
        sudo chown -R botuser:botuser $BOT_DIR/.git
        sudo -u botuser git fetch origin
        sudo -u botuser git reset --hard origin/main 2>/dev/null || sudo -u botuser git reset --hard origin/master
        sudo cp $BACKUP_DIR/.env $BOT_DIR/.env 2>/dev/null || true
        sudo cp $BACKUP_DIR/*.session $BOT_DIR/ 2>/dev/null || true
        sudo chown -R botuser:botuser $BOT_DIR
        echo "Code updated."
        echo ""
        if [ -f "$BOT_DIR/deploy/update-bot.sh" ]; then
            sudo SKIP_BACKUP_AND_PULL=1 SKIP_BROWSER="${SKIP_BROWSER:-0}" BACKUP_DIR="$BACKUP_DIR" bash "$BOT_DIR/deploy/update-bot.sh"
            exit $?
        fi
        echo "[4/5] Updating dependencies..."
        sudo -u botuser $BOT_DIR/venv/bin/pip install -r requirements.txt --upgrade
        echo ""
        if [ "${SKIP_BROWSER:-0}" != "1" ] && $BOT_DIR/venv/bin/python -c "import playwright" 2>/dev/null; then
            BROWSER_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-$BOT_DIR/ms-playwright}"
            sudo mkdir -p "$BROWSER_CACHE"
            sudo chown -R botuser:botuser "$BROWSER_CACHE"
            ensure_service_browser_path
            if browser_ready; then
                echo "Headless browser already installed."
                prune_stale_chromium_builds "$BROWSER_CACHE"
            else
                echo "Installing headless Chromium (~170 MB) for JS-only player pages..."
                sudo $BOT_DIR/venv/bin/python -m playwright install-deps chromium \
                    || echo "WARN: install-deps failed - continuing."
                sudo -H -u botuser env PLAYWRIGHT_BROWSERS_PATH="$BROWSER_CACHE" \
                    $BOT_DIR/venv/bin/python -m playwright install chromium \
                    || echo "WARN: Chromium install failed (disk/RAM?) - continuing without it. Retry with: bot browser"
                if browser_ready; then
                    prune_stale_chromium_builds "$BROWSER_CACHE"
                fi
            fi
        fi
        echo ""
        echo "[5/5] Starting bot..."
        sudo systemctl start $SERVICE_NAME
        echo ""
        echo "========================================="
        echo "  Update Complete!"
        echo "========================================="
        ;;
    browser)
        echo "========================================="
        echo "  Optional Headless Browser (HLS)"
        echo "========================================="
        echo ""
        sudo bash $BOT_DIR/deploy/install-browser.sh
        ;;
    *) echo "Usage: bot {start|stop|restart|status|logs|logs-tail|edit-env|edit-proxy|browser|update}" ;;
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
