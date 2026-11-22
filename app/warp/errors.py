"""WARP wireproxy exception hierarchy and error formatting helpers."""

from __future__ import annotations

from typing import Optional

import aiohttp

from app.warp.config import get_warp_config


class WarpError(Exception):
    """Base exception for all WARP wireproxy errors."""


class WarpProxyError(WarpError):
    """Raised when WARP proxy routing is ON but wireproxy cannot be used."""


class WarpNotInstalledError(WarpError):
    """Raised when wireproxy binary or configuration is not installed."""


class WarpUnsupportedPlatformError(WarpError):
    """Raised when an operation is attempted on an unsupported OS or CPU arch."""


def _collect_proxy_exception_types() -> tuple[type[BaseException], ...]:
    types: list[type[BaseException]] = [WarpProxyError]

    client_proxy_err = getattr(aiohttp, "ClientProxyConnectionError", None)
    if isinstance(client_proxy_err, type) and issubclass(client_proxy_err, BaseException):
        types.append(client_proxy_err)

    for module_name in ("aiohttp_socks", "python_socks"):
        try:
            mod = __import__(module_name, fromlist=["ProxyError", "ProxyConnectionError", "ProxyTimeoutError"])
        except Exception:
            continue
        for attr in ("ProxyError", "ProxyConnectionError", "ProxyTimeoutError"):
            exc_cls = getattr(mod, attr, None)
            if isinstance(exc_cls, type) and issubclass(exc_cls, BaseException) and exc_cls not in types:
                types.append(exc_cls)

    return tuple(types)


_PROXY_EXCEPTION_TYPES = _collect_proxy_exception_types()


def _iter_exception_chain(exc: BaseException):
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def is_warp_proxy_error(exc: BaseException) -> bool:
    """Return True if *exc* (or its cause chain) indicates a local WARP proxy failure."""
    cfg = get_warp_config()
    warp_hosts = {cfg.bind_host.lower(), "127.0.0.1", "localhost", "::1"}
    warp_ports = {cfg.socks5_port, cfg.http_port}
    warp_endpoints = {
        f"{cfg.bind_host}:{cfg.socks5_port}".lower(),
        f"{cfg.bind_host}:{cfg.http_port}".lower(),
        f"127.0.0.1:{cfg.socks5_port}",
        f"127.0.0.1:{cfg.http_port}",
        f"localhost:{cfg.socks5_port}",
        f"localhost:{cfg.http_port}",
    }

    for err in _iter_exception_chain(exc):
        if isinstance(err, _PROXY_EXCEPTION_TYPES):
            return True

        # aiohttp.ClientConnectorError carries conn_key.host / conn_key.port
        if isinstance(err, aiohttp.ClientConnectorError):
            conn_key = getattr(err, "conn_key", None)
            host = str(getattr(conn_key, "host", "") or "").strip("[]").lower()
            port = getattr(conn_key, "port", None)
            if host in warp_hosts and port in warp_ports:
                return True

        if isinstance(err, (ConnectionRefusedError, TimeoutError, OSError)):
            text = str(err).lower()
            if any(ep in text for ep in warp_endpoints):
                return True

    return False


def format_warp_proxy_error(
    exc: Optional[BaseException] = None,
    feature: Optional[str] = None,
) -> str:
    """Return a concise, user-facing error message with actionable recovery commands."""
    prefix = f"WARP proxy error ({feature})" if feature else "WARP proxy error"

    if isinstance(exc, WarpNotInstalledError):
        detail = str(exc).strip() or "WARP wireproxy is not installed on this server."
        return (
            f"{prefix}: {detail}\n"
            "Run /warp_install to install WARP wireproxy, check /warp_status, or /warp_off to disable proxy routing."
        )

    detail = ""
    if exc is not None:
        raw = str(exc).strip()
        if raw:
            # Avoid duplicating guidance if exc is already a formatted WarpProxyError
            if "/warp_status" in raw and "/warp_off" in raw:
                return raw if not feature or feature in raw else f"{prefix}: {raw}"
            detail = raw[:220]

    reason = (
        f"Could not connect through local WARP wireproxy ({detail})."
        if detail
        else "Local WARP wireproxy is unreachable or not running."
    )
    return (
        f"{prefix}: {reason}\n"
        "Check proxy health with /warp_status, or disable WARP routing with /warp_off."
    )
