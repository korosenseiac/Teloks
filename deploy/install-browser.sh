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
# Chromium is installed into <bot dir>/ms-playwright, NOT into the bot user's
# home: the systemd unit is hardened with ProtectHome=true, which empties /home
# for the service, so a browser under /home is invisible to the bot and looks
# exactly like "Chromium is not downloaded". Step 5/5 points the service at this
# path and verifies it from inside the same sandbox.
#
# Costs roughly: 170 MB download, ~450 MB disk, ~300-500 MB RAM per scan
# (Chromium is closed automatically after HLS_BROWSER_IDLE seconds of no use).
# Uninstall with:
#   sudo -u botuser /opt/telegram-forwarder-bot/venv/bin/pip uninstall -y playwright
#   sudo rm -rf /opt/telegram-forwarder-bot/ms-playwright
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
# Deliberately outside /home: ProtectHome=true (see the unit in install.sh)
# empties /home for the service, so a browser there can never be reached by it.
BROWSER_CACHE="${PLAYWRIGHT_BROWSERS_PATH:-$BOT_DIR/ms-playwright}"
SERVICE_NAME="${SERVICE_NAME:-telegram-forwarder}"
UNIT="/etc/systemd/system/$SERVICE_NAME.service"
DROPIN_DIR="/etc/systemd/system/$SERVICE_NAME.service.d"
DROPIN="$DROPIN_DIR/browser-cache.conf"

# Root when we already are root, sudo otherwise (see WITH_DEPS above).
if [ "$WITH_DEPS" = "1" ]; then
  as_root() { "$@"; }
else
  as_root() { sudo "$@"; }
fi

# Chromium must land in the cache the SERVICE reads. Running the download as the
# bot user is NOT enough on its own: sudo keeps the *invoking* user's HOME unless
# -H is given, so the browser would quietly be installed into /home/ubuntu/.cache
# and the bot would keep reporting "Chromium is not downloaded". So: run as the
# bot user, with -H, and pin PLAYWRIGHT_BROWSERS_PATH as well.
bot_playwright() {
  sudo -H -u "$BOT_USER" env PLAYWRIGHT_BROWSERS_PATH="$BROWSER_CACHE" "$@"
}

# Which cache does the service actually resolve? (unit file plus any drop-ins.)
service_cache_path() {
  systemctl show -p Environment "$SERVICE_NAME" 2>/dev/null \
    | tr ' ' '\n' | sed -n 's/^PLAYWRIGHT_BROWSERS_PATH=//p' | tail -1
}

# Point the service at $BROWSER_CACHE. Playwright falls back to
# $HOME/.cache/ms-playwright, which ProtectHome=true makes unreachable, so
# without this the bot cannot find the browser however often it is installed.
ensure_service_browser_path() {
  [ -f "$UNIT" ] || return 1
  current="$(service_cache_path)"
  if [ "$current" = "$BROWSER_CACHE" ]; then
    echo "     The service already uses $BROWSER_CACHE"
    return 0
  fi
  echo "     The service looks in ${current:-$BOT_HOME/.cache/ms-playwright}; adding a"
  echo "     drop-in so it uses $BROWSER_CACHE instead..."
  as_root mkdir -p "$DROPIN_DIR"
  printf '[Service]\n# Written by deploy/install-browser.sh: ProtectHome=true hides /home from\n# the service, so the Chromium cache must live where the service can read it.\nEnvironment=PLAYWRIGHT_BROWSERS_PATH=%s\n' \
    "$BROWSER_CACHE" | as_root tee "$DROPIN" >/dev/null
  as_root systemctl daemon-reload
  return 0
}

# Recreate the service's sandbox for one launch. This is the check that catches
# the ProtectHome trap - the reason a browser that launches fine from a shell can
# still be invisible to the bot. Returns 2 when systemd-run is unavailable.
sandbox_check() {
  command -v systemd-run >/dev/null 2>&1 || return 2
  as_root systemd-run --quiet --wait --pipe --collect \
    --uid="$BOT_USER" \
    --property=ProtectHome=true --property=ProtectSystem=strict \
    --property=PrivateTmp=true --property=NoNewPrivileges=true \
    --property=ReadWritePaths="$BOT_DIR" \
    --setenv=PLAYWRIGHT_BROWSERS_PATH="$BROWSER_CACHE" \
    "$VENV_PY" -c 'from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    p.chromium.launch(headless=True).close()' >/dev/null 2>&1
}

echo "Bot user:       $BOT_USER"
echo "Browser cache:  $BROWSER_CACHE"
echo "Disk space:"
df -h "$BOT_DIR" | tail -1
echo

# Create it up front so the download (which runs as the bot user) can write into
# /opt and not only into its own home, where the service could never read it.
as_root mkdir -p "$BROWSER_CACHE"
as_root chown "$BOT_USER:$BOT_USER" "$BROWSER_CACHE"

echo "1/5  Installing the Playwright package..."
bot_playwright "$VENV_PY" -m pip install --upgrade -q playwright

echo
echo "2/5  Installing Chromium's system libraries (apt, needs root)..."
# `playwright install --with-deps` must NOT be used here: it would call sudo
# itself, from inside the bot account, and hang on a password that account does
# not have. Dependencies and the download are two separate steps.
if [ "$WITH_DEPS" = "1" ]; then
  "$VENV_PY" -m playwright install-deps chromium \
    || echo "WARN: install-deps failed - retry later with: $VENV_PY -m playwright install-deps chromium"
else
  sudo "$VENV_PY" -m playwright install-deps chromium \
    || echo "WARN: install-deps failed - retry later with: sudo $VENV_PY -m playwright install-deps chromium"
fi

echo
echo "3/5  Downloading Chromium into $BROWSER_CACHE ..."
# Always run this: it is a no-op when the build is already current, and it is the
# only thing that repairs the case where an earlier run downloaded an OLD build
# (afterwards `pip install --upgrade playwright` wants a newer revision, which
# looks exactly like "Chromium is not downloaded").
bot_playwright "$VENV_PY" -m playwright install chromium

echo
echo "4/5  Verifying..."
# Ask Playwright itself which executable it launches. A `chromium-*` directory
# listing proves nothing: a stale build left by an older Playwright passes that
# check while the bot still reports Chromium as missing.
WANTED="$(bot_playwright "$VENV_PY" -c '
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    print(p.chromium.executable_path)
' 2>/dev/null || true)"
if [ -n "$WANTED" ] && [ -x "$WANTED" ]; then
  echo "     wanted: $WANTED"
  echo "     exists: yes"
else
  echo "     wanted: ${WANTED:-<unknown - Playwright could not report it>}"
  echo "     exists: NO - the download above did not produce this build"
fi

# Builds from an older Playwright are unusable (~450 MB each): Playwright only
# launches the revision matching its own version. Name the leftovers so the disk
# can be reclaimed instead of quietly filling up.
for build in "$BROWSER_CACHE"/chromium-*; do
  [ -d "$build" ] || continue
  case "$WANTED" in "$build"/*) continue ;; esac
  echo "     NOTE: $(basename "$build") is not the build this Playwright launches"
  echo "           (unusable, ~450 MB) - free it with: sudo rm -rf $build"
done

# Chromium copies from earlier attempts under other accounts just waste disk.
for stale in /home/*/.cache/ms-playwright /root/.cache/ms-playwright; do
  [ -d "$stale" ] || continue
  [ "$stale" = "$BROWSER_CACHE" ] && continue
  echo "     NOTE: an unused copy from an earlier run is at $stale"
  echo "           (safe to delete, frees ~450 MB): sudo rm -rf $stale"
done

echo
echo "Launching Chromium as $BOT_USER (the real check - the bot does the same)..."
if ! bot_playwright bash -c "cd '$BOT_DIR' && '$VENV_PY' -m app.hls.browser"; then
  echo "ERROR: the browser self-test above did not pass." >&2
  echo "     It prints which build Playwright wants and what the cache holds;" >&2
  echo "     a mismatch there means the download step failed - re-run this script" >&2
  echo "     and read its output, or install by hand with the bot's interpreter:" >&2
  echo "       sudo -H -u $BOT_USER env PLAYWRIGHT_BROWSERS_PATH=$BROWSER_CACHE \\" >&2
  echo "         $VENV_PY -m playwright install chromium" >&2
  exit 1
fi

echo
echo "5/5  Checking the bot service can reach it..."
if [ ! -f "$UNIT" ]; then
  echo "     (no $UNIT yet - install the bot, then re-run this script)"
else
  ensure_service_browser_path || true
  sandbox_check
  rc=$?
  if [ "$rc" = "0" ]; then
    echo "     OK - Chromium launches from inside the service sandbox"
    echo "          (ProtectHome=true hides /home, so this is the check that matters)"
  elif [ "$rc" = "2" ]; then
    echo "     (systemd-run is unavailable - the sandbox could not be simulated)"
  else
    echo "WARN: Chromium works, but not under the service's sandbox settings." >&2
    echo "      The bot would still report 'Chromium is not downloaded'. Check that" >&2
    echo "      $UNIT and $DROPIN" >&2
    echo "      agree on $BROWSER_CACHE, then run: sudo systemctl daemon-reload" >&2
    exit 1
  fi
fi

echo
echo "Done. Restart the bot now:"
echo "  sudo systemctl restart telegram-forwarder"
