"""Configuration, validation, and wireproxy config generation for app.warp."""

from __future__ import annotations

import ipaddress
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

import app.config as app_config

logger = logging.getLogger(__name__)

_SERVICE_NAME_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")
DEFAULT_WARP_ENDPOINT = "engage.cloudflareclient.com:2408"
FALLBACK_WARP_IPV4_ENDPOINT = "162.159.192.1:2408"
DEFAULT_WARP_DNS = "1.1.1.1"


def validate_loopback_host(host: str) -> str:
    """Strictly validate that *host* is a loopback address.

    Raises ``ValueError`` for non-loopback addresses (such as ``0.0.0.0`` or a
    LAN/public IP) so wireproxy can never be bound publicly without auth.
    """
    raw = (host or "").strip().strip("[]")
    if not raw:
        raise ValueError("WARP bind host cannot be empty")
    if raw.lower() == "localhost":
        return "127.0.0.1"
    try:
        addr = ipaddress.ip_address(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid WARP bind host {host!r}: must be a loopback IP") from exc
    if not addr.is_loopback:
        raise ValueError(
            f"Refusing non-loopback WARP bind host {host!r}: wireproxy must bind to localhost only"
        )
    return str(addr)


def ensure_loopback_host(host: str) -> str:
    """Return a valid loopback host, falling back to ``127.0.0.1`` on invalid config."""
    try:
        return validate_loopback_host(host)
    except ValueError as exc:
        logger.warning("%s; clamping to 127.0.0.1", exc)
        return "127.0.0.1"


def validate_port(port: int, name: str = "port") -> int:
    """Validate TCP port range (1..65535)."""
    try:
        value = int(port)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid {name}: {port!r}") from exc
    if not (1 <= value <= 65535):
        raise ValueError(f"Invalid {name}: {value} (must be between 1 and 65535)")
    return value


def validate_service_name(name: str) -> str:
    """Validate systemd unit base name against unsafe characters."""
    cleaned = (name or "").strip()
    if cleaned.endswith(".service"):
        cleaned = cleaned[: -len(".service")]
    if not cleaned or ".." in cleaned or not _SERVICE_NAME_RE.match(cleaned):
        raise ValueError(f"Invalid WARP systemd service name: {name!r}")
    return cleaned


def format_host_port(host: str, port: int) -> str:
    """Format host:port, bracketing IPv6 literals."""
    if ":" in host and not host.startswith("["):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


@dataclass(frozen=True)
class WarpProxyConfig:
    """Active proxy connection settings returned to downloader consumers."""

    enabled: bool
    host: str
    socks5_port: int
    http_port: int
    socks5_url: str
    http_url: str


@dataclass(frozen=True)
class WarpConfig:
    """Resolved runtime configuration and filesystem paths for WARP wireproxy."""

    bind_host: str
    socks5_port: int
    http_port: int
    data_dir: str
    service_name: str
    mtu: int
    keepalive: int
    endpoint: str
    trace_timeout: int

    @property
    def socks5_bind(self) -> str:
        return format_host_port(self.bind_host, self.socks5_port)

    @property
    def http_bind(self) -> str:
        return format_host_port(self.bind_host, self.http_port)

    @property
    def socks5_url(self) -> str:
        return f"socks5://{self.socks5_bind}"

    @property
    def http_url(self) -> str:
        return f"http://{self.http_bind}"

    @property
    def bin_dir(self) -> str:
        return os.path.join(self.data_dir, "bin")

    @property
    def tmp_dir(self) -> str:
        return os.path.join(self.data_dir, "tmp")

    @property
    def wireproxy_bin(self) -> str:
        return os.path.join(self.bin_dir, "wireproxy")

    @property
    def wgcf_bin(self) -> str:
        return os.path.join(self.bin_dir, "wgcf")

    @property
    def wgcf_account_path(self) -> str:
        return os.path.join(self.data_dir, "wgcf-account.toml")

    @property
    def wgcf_profile_path(self) -> str:
        return os.path.join(self.data_dir, "wgcf-profile.conf")

    @property
    def wireproxy_conf_path(self) -> str:
        return os.path.join(self.data_dir, "wireproxy.conf")

    @property
    def pid_file(self) -> str:
        return os.path.join(self.data_dir, "wireproxy.pid")

    @property
    def log_file(self) -> str:
        return os.path.join(self.data_dir, "wireproxy.log")

    @property
    def state_file(self) -> str:
        return os.path.join(self.data_dir, "state.json")

    @property
    def systemd_unit_path(self) -> str:
        return f"/etc/systemd/system/{self.service_name}.service"

    def to_proxy_config(self, enabled: bool) -> WarpProxyConfig:
        return WarpProxyConfig(
            enabled=bool(enabled),
            host=self.bind_host,
            socks5_port=self.socks5_port,
            http_port=self.http_port,
            socks5_url=self.socks5_url,
            http_url=self.http_url,
        )


def get_warp_config() -> WarpConfig:
    """Build the current ``WarpConfig`` from ``app.config`` settings."""
    bind_host = ensure_loopback_host(getattr(app_config, "WARP_BIND_HOST", "127.0.0.1"))

    try:
        socks5_port = validate_port(getattr(app_config, "WARP_SOCKS5_PORT", 40000), "WARP_SOCKS5_PORT")
    except ValueError:
        logger.warning("Invalid WARP_SOCKS5_PORT; falling back to 40000")
        socks5_port = 40000

    try:
        http_port = validate_port(getattr(app_config, "WARP_HTTP_PORT", 40001), "WARP_HTTP_PORT")
    except ValueError:
        logger.warning("Invalid WARP_HTTP_PORT; falling back to 40001")
        http_port = 40001

    if http_port == socks5_port:
        fallback = 40001 if socks5_port != 40001 else 40002
        logger.warning(
            "WARP_HTTP_PORT (%d) matches WARP_SOCKS5_PORT; using %d for HTTP proxy",
            http_port,
            fallback,
        )
        http_port = fallback

    default_data_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "warp",
    )
    data_dir = os.path.abspath(str(getattr(app_config, "WARP_DATA_DIR", default_data_dir) or default_data_dir))

    raw_service = str(getattr(app_config, "WARP_SERVICE_NAME", "warp-wireproxy") or "warp-wireproxy")
    try:
        service_name = validate_service_name(raw_service)
    except ValueError:
        logger.warning("Invalid WARP_SERVICE_NAME %r; falling back to 'warp-wireproxy'", raw_service)
        service_name = "warp-wireproxy"

    try:
        mtu = int(getattr(app_config, "WARP_MTU", 1280))
        if not (576 <= mtu <= 1500):
            mtu = 1280
    except (TypeError, ValueError):
        mtu = 1280

    try:
        keepalive = int(getattr(app_config, "WARP_KEEPALIVE", 25))
        if not (0 <= keepalive <= 300):
            keepalive = 25
    except (TypeError, ValueError):
        keepalive = 25

    endpoint = str(getattr(app_config, "WARP_ENDPOINT", DEFAULT_WARP_ENDPOINT) or DEFAULT_WARP_ENDPOINT).strip()
    if not endpoint or "\n" in endpoint or "\r" in endpoint:
        endpoint = DEFAULT_WARP_ENDPOINT

    try:
        trace_timeout = int(getattr(app_config, "WARP_TRACE_TIMEOUT", 12))
        if not (2 <= trace_timeout <= 60):
            trace_timeout = 12
    except (TypeError, ValueError):
        trace_timeout = 12

    return WarpConfig(
        bind_host=bind_host,
        socks5_port=socks5_port,
        http_port=http_port,
        data_dir=data_dir,
        service_name=service_name,
        mtu=mtu,
        keepalive=keepalive,
        endpoint=endpoint,
        trace_timeout=trace_timeout,
    )


def parse_wgcf_profile(content: str) -> dict[str, dict[str, list[str]]]:
    """Parse a WireGuard / wgcf-profile.conf file preserving repeated keys."""
    sections: dict[str, dict[str, list[str]]] = {}
    current_section: Optional[str] = None

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1].strip()
            sections.setdefault(current_section, {})
            continue
        if current_section is None or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        bucket = sections[current_section].setdefault(key, [])
        for part in value.split(","):
            item = part.strip()
            if item and item not in bucket:
                bucket.append(item)

    return sections


def _get_section_ci(
    sections: dict[str, dict[str, list[str]]],
    target: str,
) -> dict[str, list[str]]:
    lower = target.lower()
    for name, values in sections.items():
        if name.lower() == lower:
            return values
    return {}


def _get_values_ci(section: dict[str, list[str]], key: str) -> list[str]:
    lower = key.lower()
    for k, vals in section.items():
        if k.lower() == lower:
            return list(vals)
    return []


def build_wg_profile(
    parsed: dict[str, dict[str, list[str]]],
    *,
    mtu: int = 1280,
    keepalive: int = 25,
    endpoint: Optional[str] = None,
    keepalive_key: str = "PersistentKeepalive",
) -> str:
    """Render a normalized WireGuard configuration from parsed wgcf profile data."""
    iface = _get_section_ci(parsed, "Interface")
    peer = _get_section_ci(parsed, "Peer")

    private_keys = _get_values_ci(iface, "PrivateKey")
    addresses = _get_values_ci(iface, "Address")
    dns_servers = _get_values_ci(iface, "DNS") or [DEFAULT_WARP_DNS]

    public_keys = _get_values_ci(peer, "PublicKey")
    allowed_ips = _get_values_ci(peer, "AllowedIPs") or ["0.0.0.0/0", "::/0"]
    endpoints = _get_values_ci(peer, "Endpoint")

    if not private_keys:
        raise ValueError("wgcf profile is missing Interface.PrivateKey")
    if not addresses:
        raise ValueError("wgcf profile is missing Interface.Address")
    if not public_keys:
        raise ValueError("wgcf profile is missing Peer.PublicKey")

    final_endpoint = (endpoint or (endpoints[0] if endpoints else DEFAULT_WARP_ENDPOINT)).strip()
    if not final_endpoint or "\n" in final_endpoint or "\r" in final_endpoint:
        raise ValueError(f"Invalid WireGuard endpoint: {final_endpoint!r}")

    lines = [
        "[Interface]",
        f"PrivateKey = {private_keys[0]}",
        f"Address = {', '.join(addresses)}",
        f"DNS = {', '.join(dns_servers)}",
        f"MTU = {int(mtu)}",
        "",
        "[Peer]",
        f"PublicKey = {public_keys[0]}",
        f"AllowedIPs = {', '.join(allowed_ips)}",
        f"Endpoint = {final_endpoint}",
    ]
    if keepalive > 0:
        lines.append(f"{keepalive_key} = {int(keepalive)}")
    lines.append("")
    return "\n".join(lines)


def build_wireproxy_conf(
    cfg: WarpConfig,
    wg_profile_path: Optional[str] = None,
) -> str:
    """Generate a localhost-only ``wireproxy.conf`` exposing SOCKS5 and HTTP proxies."""
    bind_host = validate_loopback_host(cfg.bind_host)
    socks5_port = validate_port(cfg.socks5_port, "WARP_SOCKS5_PORT")
    http_port = validate_port(cfg.http_port, "WARP_HTTP_PORT")
    if socks5_port == http_port:
        raise ValueError("WARP_SOCKS5_PORT and WARP_HTTP_PORT must be distinct")

    profile = wg_profile_path or cfg.wgcf_profile_path
    if not profile or "\n" in profile or "\r" in profile:
        raise ValueError(f"Invalid WGConfig path: {profile!r}")

    socks5_bind = format_host_port(bind_host, socks5_port)
    http_bind = format_host_port(bind_host, http_port)

    return (
        "# Generated by Telegram Forwarder Bot (app/warp)\n"
        "# Localhost-only Cloudflare WARP wireproxy (SOCKS5 + HTTP)\n"
        f"WGConfig = {profile}\n"
        "\n"
        "[Socks5]\n"
        f"BindAddress = {socks5_bind}\n"
        "\n"
        "[http]\n"
        f"BindAddress = {http_bind}\n"
    )


def _assert_safe_unit_value(value: str, label: str) -> str:
    if not value or any(ch in value for ch in ("\n", "\r", "\x00", '"')):
        raise ValueError(f"Unsafe {label} for systemd unit: {value!r}")
    return value


def _systemd_quote(value: str) -> str:
    if any(ch.isspace() for ch in value):
        return f'"{value}"'
    return value


def _path_under_home(path: str) -> bool:
    norm = path.replace("\\", "/").rstrip("/")
    return norm in ("/home", "/root") or norm.startswith("/home/") or norm.startswith("/root/")


def build_systemd_unit(
    cfg: WarpConfig,
    binary_path: str,
    run_user: Optional[str] = None,
) -> str:
    """Generate a hardened systemd unit file for ``warp-wireproxy``."""
    service_name = validate_service_name(cfg.service_name)
    binary = _assert_safe_unit_value(binary_path, "binary_path")
    conf_path = _assert_safe_unit_value(cfg.wireproxy_conf_path, "wireproxy_conf_path")
    data_dir = _assert_safe_unit_value(cfg.data_dir, "data_dir")

    user_lines = ""
    if run_user:
        clean_user = _assert_safe_unit_value(run_user.strip(), "run_user")
        if not re.match(r"^[a-zA-Z0-9_.-]+$", clean_user):
            raise ValueError(f"Unsafe run_user for systemd unit: {clean_user!r}")
        user_lines = f"User={clean_user}\nGroup={clean_user}\n"

    # ProtectHome=true hides /home and /root, which breaks ExecStart/WorkingDirectory
    # with status=200/CHDIR if data_dir or binary lives under /home or /root.
    protect_home = (
        "read-only"
        if any(_path_under_home(p) for p in (data_dir, binary, conf_path))
        else "true"
    )

    return (
        "[Unit]\n"
        f"Description=Cloudflare WARP WireProxy ({service_name})\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "StartLimitIntervalSec=0\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        f"{user_lines}"
        f"WorkingDirectory={_systemd_quote(data_dir)}\n"
        f"ExecStart={_systemd_quote(binary)} -c {_systemd_quote(conf_path)}\n"
        "Restart=always\n"
        "RestartSec=5\n"
        "StartLimitBurst=10\n"
        "StandardOutput=journal\n"
        "StandardError=journal\n"
        f"SyslogIdentifier={service_name}\n"
        "NoNewPrivileges=true\n"
        "PrivateTmp=true\n"
        "ProtectSystem=strict\n"
        f"ProtectHome={protect_home}\n"
        f"ReadOnlyPaths={_systemd_quote(data_dir)}\n"
        "MemoryMax=128M\n"
        "\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )
