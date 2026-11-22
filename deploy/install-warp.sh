#!/usr/bin/env bash
# =============================================================================
# Cloudflare WARP (wireproxy) VPS Installer for Telegram Forwarder Bot
# =============================================================================
# Installs userspace Cloudflare WARP + wireproxy (SOCKS5 + HTTP on 127.0.0.1)
# as a hardened systemd service aligned with deploy/install.sh.
#
# Usage:
#   sudo bash deploy/install-warp.sh            # idempotent install/repair
#   sudo bash deploy/install-warp.sh --force    # re-download & re-register WARP
#
# Note: You can also install WARP directly from Telegram with /warp_install
# (which automatically uses systemd when privileged, or bot-managed user-space
# mode inside deploy/install.sh's hardened systemd sandbox).
# =============================================================================
set -euo pipefail

BOT_DIR="${BOT_DIR:-/opt/telegram-forwarder-bot}"
BOT_USER="${BOT_USER:-botuser}"
WARP_DATA_DIR="${WARP_DATA_DIR:-$BOT_DIR/warp}"
WARP_BIND_HOST="127.0.0.1"
WARP_SOCKS5_PORT="${WARP_SOCKS5_PORT:-40000}"
WARP_HTTP_PORT="${WARP_HTTP_PORT:-40001}"
WARP_SERVICE_NAME="${WARP_SERVICE_NAME:-warp-wireproxy}"
WARP_MTU="${WARP_MTU:-1280}"
WARP_KEEPALIVE="${WARP_KEEPALIVE:-25}"
WARP_ENDPOINT="${WARP_ENDPOINT:-engage.cloudflareclient.com:2408}"
FORCE=0

for arg in "$@"; do
  case "$arg" in
    --force|-f) FORCE=1 ;;
    --help|-h)
      echo "Usage: sudo bash $0 [--force]"
      exit 0
      ;;
    *)
      echo "ERROR: Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

die() {
  echo "ERROR: $*" >&2
  exit 1
}

info() {
  echo "[i] $*"
}

ok() {
  echo "[OK] $*"
}

# Validate configuration inputs strictly before touching the system
[[ "$WARP_SERVICE_NAME" =~ ^[A-Za-z0-9_.@-]{1,64}$ ]] || die "Invalid WARP_SERVICE_NAME: $WARP_SERVICE_NAME"
[[ "$WARP_SOCKS5_PORT" =~ ^[0-9]+$ ]] && [ "$WARP_SOCKS5_PORT" -ge 1 ] && [ "$WARP_SOCKS5_PORT" -le 65535 ] \
  || die "Invalid WARP_SOCKS5_PORT: $WARP_SOCKS5_PORT"
[[ "$WARP_HTTP_PORT" =~ ^[0-9]+$ ]] && [ "$WARP_HTTP_PORT" -ge 1 ] && [ "$WARP_HTTP_PORT" -le 65535 ] \
  || die "Invalid WARP_HTTP_PORT: $WARP_HTTP_PORT"
[ "$WARP_SOCKS5_PORT" -ne "$WARP_HTTP_PORT" ] || die "WARP_SOCKS5_PORT and WARP_HTTP_PORT must differ"
[[ "$WARP_MTU" =~ ^[0-9]+$ ]] && [ "$WARP_MTU" -ge 576 ] && [ "$WARP_MTU" -le 1500 ] \
  || die "Invalid WARP_MTU: $WARP_MTU (expected 576..1500)"
[[ "$WARP_KEEPALIVE" =~ ^[0-9]+$ ]] && [ "$WARP_KEEPALIVE" -ge 0 ] && [ "$WARP_KEEPALIVE" -le 300 ] \
  || die "Invalid WARP_KEEPALIVE: $WARP_KEEPALIVE (expected 0..300)"
[[ "$WARP_ENDPOINT" =~ ^[A-Za-z0-9._:\[\]-]+$ ]] \
  || die "Invalid WARP_ENDPOINT: $WARP_ENDPOINT"
case "$WARP_DATA_DIR" in
  *$'\n'*|*$'\r'*|*$'"'*|"") die "Invalid WARP_DATA_DIR" ;;
esac

# Must run on Linux amd64/arm64
[ "$(uname -s)" = "Linux" ] || die "This installer requires Linux (detected: $(uname -s))."
case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) die "Unsupported CPU architecture: $(uname -m) (supported: amd64, arm64)" ;;
esac

# Privilege check: re-exec as root via passwordless sudo so every step has
# consistent permissions on chmod-700 $WARP_DATA_DIR and /etc/systemd/system.
if [ "$(id -u)" -ne 0 ]; then
  if [ "$(id -un)" = "$BOT_USER" ] && ! sudo -n true 2>/dev/null; then
    die "Running as '$BOT_USER' without sudo rights. Run from your admin user with:
         sudo bash $0
       Or install in user-space directly from Telegram with /warp_install."
  fi
  if sudo -n true 2>/dev/null; then
    exec sudo env \
      BOT_DIR="$BOT_DIR" \
      BOT_USER="$BOT_USER" \
      WARP_DATA_DIR="$WARP_DATA_DIR" \
      WARP_SOCKS5_PORT="$WARP_SOCKS5_PORT" \
      WARP_HTTP_PORT="$WARP_HTTP_PORT" \
      WARP_SERVICE_NAME="$WARP_SERVICE_NAME" \
      WARP_MTU="$WARP_MTU" \
      WARP_KEEPALIVE="$WARP_KEEPALIVE" \
      WARP_ENDPOINT="$WARP_ENDPOINT" \
      bash "$0" "$@"
  fi
  die "Please run as root (sudo bash $0)."
fi

command -v curl >/dev/null 2>&1 || die "curl is required (sudo apt install -y curl)"
command -v tar >/dev/null 2>&1 || die "tar is required"

BIN_DIR="$WARP_DATA_DIR/bin"
TMP_DIR="$WARP_DATA_DIR/tmp"
WIREPROXY_BIN="$BIN_DIR/wireproxy"
WGCF_BIN="$BIN_DIR/wgcf"
WGCF_ACCOUNT="$WARP_DATA_DIR/wgcf-account.toml"
WGCF_PROFILE="$WARP_DATA_DIR/wgcf-profile.conf"
WIREPROXY_CONF="$WARP_DATA_DIR/wireproxy.conf"
UNIT_PATH="/etc/systemd/system/${WARP_SERVICE_NAME}.service"

mkdir -p "$BIN_DIR" "$TMP_DIR"
chmod 700 "$WARP_DATA_DIR"

resolve_latest_tag() {
  local repo="$1"
  local final_url tag
  final_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "https://github.com/${repo}/releases/latest")" || return 1
  if [[ "$final_url" != *"/releases/tag/"* ]]; then
    return 1
  fi
  tag="${final_url##*/releases/tag/}"
  tag="${tag%%[?#]*}"
  [ -n "$tag" ] || return 1
  echo "$tag"
}

download_wireproxy() {
  if [ "$FORCE" -eq 0 ] && [ -x "$WIREPROXY_BIN" ] && "$WIREPROXY_BIN" -v >/dev/null 2>&1; then
    ok "Reusing existing wireproxy binary ($WIREPROXY_BIN)"
    return 0
  fi

  info "Downloading wireproxy (linux/$ARCH)..."
  local tag="" repo=""
  for candidate in windtf/wireproxy whyvl/wireproxy pufferffish/wireproxy; do
    if tag="$(resolve_latest_tag "$candidate" 2>/dev/null)" && [ -n "$tag" ]; then
      repo="$candidate"
      break
    fi
  done
  [ -n "$repo" ] || die "Failed to resolve latest wireproxy release tag"

  local url="https://github.com/${repo}/releases/download/${tag}/wireproxy_linux_${ARCH}.tar.gz"
  local tar_file="$TMP_DIR/wireproxy_linux_${ARCH}.tar.gz"
  local extract_dir="$TMP_DIR/extract_$$"
  rm -rf "$extract_dir"
  mkdir -p "$extract_dir"

  curl -fsSL "$url" -o "$tar_file"
  tar -xzf "$tar_file" -C "$extract_dir" wireproxy 2>/dev/null \
    || tar -xzf "$tar_file" -C "$extract_dir" ./wireproxy
  local found
  found="$(find "$extract_dir" -maxdepth 2 -type f -name wireproxy | head -n 1)"
  [ -n "$found" ] || die "wireproxy binary not found in downloaded tarball"

  install -m 0755 "$found" "$WIREPROXY_BIN"
  rm -rf "$extract_dir" "$tar_file"
  "$WIREPROXY_BIN" -v >/dev/null 2>&1 || die "Downloaded wireproxy binary failed self-check"
  ok "Installed wireproxy ($tag)"
}

download_wgcf() {
  if [ "$FORCE" -eq 0 ] && [ -x "$WGCF_BIN" ] && "$WGCF_BIN" --help >/dev/null 2>&1; then
    ok "Reusing existing wgcf binary ($WGCF_BIN)"
    return 0
  fi

  info "Downloading wgcf (linux/$ARCH)..."
  local tag
  tag="$(resolve_latest_tag "ViRb3/wgcf")" || die "Failed to resolve latest wgcf release"
  local ver="${tag#v}"
  local url="https://github.com/ViRb3/wgcf/releases/download/${tag}/wgcf_${ver}_linux_${ARCH}"
  local dl_file="$TMP_DIR/wgcf_dl_$$"

  curl -fsSL "$url" -o "$dl_file"
  install -m 0755 "$dl_file" "$WGCF_BIN"
  rm -f "$dl_file"
  "$WGCF_BIN" --help >/dev/null 2>&1 || die "Downloaded wgcf binary failed self-check"
  ok "Installed wgcf ($tag)"
}

extract_wg_first() {
  local key="$1" file="$2"
  awk -v k="$key" '
    $0 ~ "^[[:space:]]*" k "[[:space:]]*=" {
      line = $0
      sub(/^[^=]*=[[:space:]]*/, "", line)
      sub(/[[:space:]]*$/, "", line)
      print line
      exit
    }
  ' "$file"
}

extract_wg_csv() {
  local key="$1" file="$2"
  awk -v k="$key" '
    $0 ~ "^[[:space:]]*" k "[[:space:]]*=" {
      line = $0
      sub(/^[^=]*=[[:space:]]*/, "", line)
      sub(/[[:space:]]*$/, "", line)
      if (line != "") {
        printf "%s%s", sep, line
        sep = ", "
      }
    }
  ' "$file"
}

write_configs() {
  local endpoint="$1"
  local private_key public_key addresses dns_vals allowed_ips

  private_key="$(extract_wg_first "PrivateKey" "$WGCF_PROFILE")"
  public_key="$(extract_wg_first "PublicKey" "$WGCF_PROFILE")"
  addresses="$(extract_wg_csv "Address" "$WGCF_PROFILE")"
  dns_vals="$(extract_wg_csv "DNS" "$WGCF_PROFILE")"
  allowed_ips="$(extract_wg_csv "AllowedIPs" "$WGCF_PROFILE")"

  [ -n "$private_key" ] || die "wgcf-profile.conf missing PrivateKey"
  [ -n "$public_key" ] || die "wgcf-profile.conf missing PublicKey"
  [ -n "$addresses" ] || die "wgcf-profile.conf missing Address"
  [ -n "$dns_vals" ] || dns_vals="1.1.1.1"
  [ -n "$allowed_ips" ] || allowed_ips="0.0.0.0/0, ::/0"

  local ka_key test_out=""
  for ka_key in PersistentKeepalive KeepAlive; do
    {
      echo "[Interface]"
      echo "PrivateKey = ${private_key}"
      echo "Address = ${addresses}"
      echo "DNS = ${dns_vals}"
      echo "MTU = ${WARP_MTU}"
      echo ""
      echo "[Peer]"
      echo "PublicKey = ${public_key}"
      echo "AllowedIPs = ${allowed_ips}"
      echo "Endpoint = ${endpoint}"
      if [ "$WARP_KEEPALIVE" -gt 0 ]; then
        echo "${ka_key} = ${WARP_KEEPALIVE}"
      fi
    } > "$WGCF_PROFILE"
    chmod 600 "$WGCF_PROFILE"

    cat > "$WIREPROXY_CONF" <<EOF
# Generated by deploy/install-warp.sh
# Localhost-only Cloudflare WARP wireproxy (SOCKS5 + HTTP)
WGConfig = ${WGCF_PROFILE}

[Socks5]
BindAddress = ${WARP_BIND_HOST}:${WARP_SOCKS5_PORT}

[http]
BindAddress = ${WARP_BIND_HOST}:${WARP_HTTP_PORT}
EOF
    chmod 600 "$WIREPROXY_CONF"

    if test_out="$("$WIREPROXY_BIN" -c "$WIREPROXY_CONF" -n 2>&1)"; then
      return 0
    fi
    if ! echo "$test_out" | grep -qi "keepalive"; then
      break
    fi
  done

  die "wireproxy config validation (-n) failed: ${test_out}"
}

download_wireproxy
download_wgcf

# Register Cloudflare WARP account idempotently (with retry on transient API errors)
if [ "$FORCE" -eq 1 ]; then
  rm -f "$WGCF_ACCOUNT" "$WGCF_PROFILE"
fi

if [ -f "$WGCF_ACCOUNT" ]; then
  ok "Reusing existing WARP account ($WGCF_ACCOUNT)"
else
  info "Registering Cloudflare WARP account..."
  reg_ok=0
  for attempt in 1 2 3; do
    if (cd "$WARP_DATA_DIR" && "$WGCF_BIN" register --accept-tos) && [ -f "$WGCF_ACCOUNT" ]; then
      reg_ok=1
      break
    fi
    rm -f "$WGCF_ACCOUNT"
    if [ "$attempt" -lt 3 ]; then
      info "Registration attempt ${attempt} failed; retrying in $((attempt * 2))s..."
      sleep $((attempt * 2))
    fi
  done
  [ "$reg_ok" -eq 1 ] || die "Cloudflare WARP registration failed after 3 attempts"
  chmod 600 "$WGCF_ACCOUNT"
  ok "Cloudflare WARP account registered"
fi

if [ ! -f "$WGCF_PROFILE" ] || [ "$FORCE" -eq 1 ]; then
  info "Generating WireGuard profile..."
  gen_ok=0
  for attempt in 1 2 3; do
    if (cd "$WARP_DATA_DIR" && "$WGCF_BIN" generate) && [ -f "$WGCF_PROFILE" ]; then
      gen_ok=1
      break
    fi
    if [ "$attempt" -lt 3 ]; then
      info "Profile generation attempt ${attempt} failed; retrying in $((attempt * 2))s..."
      sleep $((attempt * 2))
    fi
  done
  [ "$gen_ok" -eq 1 ] || die "wgcf generate failed after 3 attempts"
  chmod 600 "$WGCF_PROFILE"
fi

info "Writing and validating localhost-only wireproxy.conf..."
write_configs "$WARP_ENDPOINT"
ok "wireproxy.conf validated (SOCKS5 ${WARP_BIND_HOST}:${WARP_SOCKS5_PORT}, HTTP ${WARP_BIND_HOST}:${WARP_HTTP_PORT})"

# Align ownership with bot user if present
USER_DIRECTIVES=""
if id "$BOT_USER" >/dev/null 2>&1; then
  chown -R "${BOT_USER}:${BOT_USER}" "$WARP_DATA_DIR"
  USER_DIRECTIVES="User=${BOT_USER}
Group=${BOT_USER}"
fi

# ProtectHome=true hides /home and /root; downgrade to read-only if data dir lives there
PROTECT_HOME="true"
case "$WARP_DATA_DIR" in
  /home|/home/*|/root|/root/*) PROTECT_HOME="read-only" ;;
esac
case "$WIREPROXY_BIN" in
  /home|/home/*|/root|/root/*) PROTECT_HOME="read-only" ;;
esac

# Install and start systemd service if systemd is active
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  info "Installing systemd service: ${WARP_SERVICE_NAME}.service..."
  cat > "$UNIT_PATH" <<EOF
[Unit]
Description=Cloudflare WARP WireProxy (${WARP_SERVICE_NAME})
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
${USER_DIRECTIVES}
WorkingDirectory=${WARP_DATA_DIR}
ExecStart=${WIREPROXY_BIN} -c ${WIREPROXY_CONF}
Restart=always
RestartSec=5
StartLimitBurst=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${WARP_SERVICE_NAME}
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=${PROTECT_HOME}
ReadOnlyPaths=${WARP_DATA_DIR}
MemoryMax=128M

[Install]
WantedBy=multi-user.target
EOF
  chmod 644 "$UNIT_PATH"
  systemctl daemon-reload
  systemctl enable "$WARP_SERVICE_NAME"
  systemctl restart "$WARP_SERVICE_NAME"
  ok "Systemd service ${WARP_SERVICE_NAME} enabled and started"
else
  die "systemd is not active on this host; run /warp_install from Telegram for managed user-space mode."
fi

# Verify WARP connectivity via Cloudflare trace
verify_trace() {
  local attempt trace_out
  for attempt in 1 2 3; do
    sleep 2
    if trace_out="$(curl -fsS --max-time 12 --socks5-hostname "${WARP_BIND_HOST}:${WARP_SOCKS5_PORT}" https://www.cloudflare.com/cdn-cgi/trace 2>/dev/null)"; then
      if echo "$trace_out" | grep -Eq '^warp=(on|plus)$'; then
        local ip loc warp_val
        ip="$(echo "$trace_out" | awk -F'=' '/^ip=/{print $2}')"
        loc="$(echo "$trace_out" | awk -F'=' '/^loc=/{print $2}')"
        warp_val="$(echo "$trace_out" | awk -F'=' '/^warp=/{print $2}')"
        ok "Cloudflare WARP verified: warp=${warp_val}, ip=${ip}, loc=${loc}"
        return 0
      fi
    fi
  done
  return 1
}

info "Verifying WARP trace via SOCKS5 ${WARP_BIND_HOST}:${WARP_SOCKS5_PORT}..."
if ! verify_trace; then
  if [ "$WARP_ENDPOINT" = "engage.cloudflareclient.com:2408" ]; then
    info "Retrying with IPv4 fallback endpoint (162.159.192.1:2408)..."
    write_configs "162.159.192.1:2408"
    if id "$BOT_USER" >/dev/null 2>&1; then
      chown -R "${BOT_USER}:${BOT_USER}" "$WARP_DATA_DIR"
    fi
    systemctl restart "$WARP_SERVICE_NAME"
    verify_trace || die "WARP trace verification failed. Check outbound UDP 2408 and 'journalctl -u ${WARP_SERVICE_NAME} -n 50'."
  else
    die "WARP trace verification failed. Check outbound UDP 2408 and 'journalctl -u ${WARP_SERVICE_NAME} -n 50'."
  fi
fi

echo ""
echo "========================================="
echo "  WARP WireProxy Ready!"
echo "========================================="
echo "  SOCKS5 : socks5://${WARP_BIND_HOST}:${WARP_SOCKS5_PORT}"
echo "  HTTP   : http://${WARP_BIND_HOST}:${WARP_HTTP_PORT}"
echo "  Service: sudo systemctl status ${WARP_SERVICE_NAME}"
echo "  Enable in Telegram bot with: /warp_on"
echo "========================================="
