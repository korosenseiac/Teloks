#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Optional component: headless Chromium for HLS pages whose stream URL is built
# in JavaScript (no .m3u8 in the HTML, no JSON API to call — the site mints the
# URL at runtime behind bot/device fingerprinting; see app/hls/browser.py).
#
#   sudo bash deploy/install-browser.sh          (sudo is required, see below)
#
# Run this from your admin user, NOT from the bot account: on AWS/Lightsail that
# is the default 'ubuntu' user, which has passwordless sudo. 'botuser' is a
# service account with no password and no sudo rights, so running it from there
# only produces an unanswerable password prompt.
#
# Chromium lives in a *per-user* cache, so it must be installed for the account
# that runs the bot. Running this without sudo would install it for whoever
# typed the command and the bot would still say "Chromium is not downloaded".
#
# Costs roughly: 170 MB download, ~450 MB disk, ~300-500 MB RAM per scan
# (Chromium is closed automatically after HLS_BROWSER_IDLE seconds of no use).
# Uninstall with:
#   sudo -u botuser /opt/telegram-forwarder-bot/venv/bin/pip uninstall -y playwright
#   sudo rm -rf /home/botuser/.cache/ms-playwright
# ---------------------------------------------------------------------------
set -euo pipefail

BOT_DIR="${BOT_DIR:-/opt/telegram-forwarder-bot}"
BOT_USER="${BOT_USER:-botuser}"
VENV_PY="$BOT_DIR/venv/bin/python"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

[ -x "$VENV_PY" ] || die "Bot venv not found at $BOT_DIR/venv (set BOT_DIR=... and retry)."

# The bot account is a service account: no password, no sudo rights. Running
# this from inside it can only end in a sudo password prompt that nothing can
# answer, so say so before that happens.
if [ "$(id -un)" = "$BOT_USER" ] && ! sudo -n true 2>/dev/null; then
  die "you are running this AS '$BOT_USER', which is a service account with no
       password and no sudo rights. Leave it first and run this from your admin
       user instead (on AWS/Lightsail that is the default 'ubuntu' user, which
       has passwordless sudo):

         exit                                    # leave the botuser shell
         cd $BOT_DIR && sudo bash deploy/install-browser.sh"
fi

# Root can install for the bot user; a user with passwordless sudo can too.
# Anything else would install Chromium into the WRONG user's cache, which looks
# successful but leaves the bot reporting "Chromium is not downloaded".
if [ "$(id -u)" -eq 0 ]; then
  WITH_DEPS=1
elif sudo -n true 2>/dev/null; then
  WITH_DEPS=0
else
  die "please run this with sudo, so Chromium is installed for '$BOT_USER'
       (the account that runs the bot) and not for '$(id -un)':
         sudo bash $BOT_DIR/deploy/install-browser.sh
       Or install it by hand with the bot's own interpreter:
         sudo -u $BOT_USER $VENV_PY -m playwright install chromium"
fi

BOT_HOME="$(getent passwd "$BOT_USER" | cut -d: -f6 || true)"
BOT_HOME="${BOT_HOME:-/home/$BOT_USER}"
BROWSER_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-$BOT_HOME/.cache/ms-playwright}"

echo "Bot user:       $BOT_USER"
echo "Browser cache:  $BROWSER_CACHE"
echo "Disk space:"
df -h "$BOT_DIR" | tail -1
echo

echo "1/3  Installing the Playwright package..."
sudo -u "$BOT_USER" "$VENV_PY" -m pip install --upgrade -q playwright

echo
echo "2/3  Downloading Chromium (~170 MB download, ~450 MB on disk)..."
if [ "$WITH_DEPS" = "1" ]; then
  # --with-deps needs root: it installs the shared libraries Chromium needs.
  sudo -u "$BOT_USER" "$VENV_PY" -m playwright install --with-deps chromium
else
  sudo -u "$BOT_USER" "$VENV_PY" -m playwright install chromium
  echo "NOTE: if Chromium complains about missing libraries, run:"
  echo "      sudo $VENV_PY -m playwright install-deps chromium"
fi

echo
if ls "$BROWSER_CACHE"/chromium-* >/dev/null 2>&1; then
  echo "3/3  Chromium is in place: $(ls -d "$BROWSER_CACHE"/chromium-* | head -1)"
  echo
  echo "Running the module self-test as $BOT_USER..."
  sudo -u "$BOT_USER" bash -c "cd '$BOT_DIR' && '$VENV_PY' -m app.hls.browser" \
    || echo "WARN: the self-test above did not pass."
else
  echo "3/3  ERROR: no Chromium found in $BROWSER_CACHE" >&2
  echo "     Retry with the bot's OWN interpreter (not the system python):" >&2
  echo "       sudo -u $BOT_USER $VENV_PY -m playwright install chromium" >&2
  exit 1
fi

echo
echo "Done. Restart the bot now:"
echo "  sudo systemctl restart telegram-forwarder"
