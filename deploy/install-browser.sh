#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Optional component: headless Chromium for HLS pages whose stream URL is built
# in JavaScript (no .m3u8 in the HTML, no JSON API to call — the site mints the
# URL at runtime behind bot/device fingerprinting; see app/hls/browser.py).
#
#   sudo bash deploy/install-browser.sh
#
# Costs roughly: 170 MB download, ~450 MB disk, ~300-500 MB RAM per scan
# (Chromium is closed automatically after HLS_BROWSER_IDLE seconds of no use).
# Uninstall with:
#   venv/bin/pip uninstall -y playwright && rm -rf ~/.cache/ms-playwright
# ---------------------------------------------------------------------------
set -euo pipefail

BOT_DIR="${BOT_DIR:-/opt/telegram-forwarder-bot}"
BOT_USER="${BOT_USER:-botuser}"
VENV_PY="$BOT_DIR/venv/bin/python"

if [ ! -x "$VENV_PY" ]; then
  echo "Bot venv not found at $BOT_DIR/venv" >&2
  echo "Set BOT_DIR (and BOT_USER if it is not 'botuser') and retry." >&2
  exit 1
fi

echo "Disk space before install:"
df -h "$BOT_DIR" | tail -1
echo

echo "1/3  Installing the Playwright package..."
sudo -u "$BOT_USER" "$VENV_PY" -m pip install --upgrade -q playwright

echo "2/3  Downloading Chromium (this can take a few minutes)..."
if [ "$(id -u)" -eq 0 ]; then
  # --with-deps needs root: it installs the shared libraries Chromium needs.
  sudo -u "$BOT_USER" "$VENV_PY" -m playwright install --with-deps chromium
else
  "$VENV_PY" -m playwright install chromium
  echo "NOTE: if Chromium complains about missing libraries, run:"
  echo "      sudo $VENV_PY -m playwright install-deps chromium"
fi

echo
echo "3/3  Verifying with the module self-test..."
sudo -u "$BOT_USER" bash -c "cd '$BOT_DIR' && '$VENV_PY' -m app.hls.browser" || true

echo
echo "Done. HLS_BROWSER_ENABLED defaults to true — restart the bot:"
echo "  sudo systemctl restart telegram-forwarder"
