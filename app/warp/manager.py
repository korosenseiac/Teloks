"""WARP wireproxy runtime state, service management, health checks, and consumer helpers."""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, Optional

import aiohttp
from aiohttp_socks import ProxyConnector

import app.config as app_config
from app.warp.config import WarpConfig, WarpProxyConfig, get_warp_config
from app.warp.errors import (
    WarpNotInstalledError,
    WarpProxyError,
    format_warp_proxy_error,
)

logger = logging.getLogger(__name__)

CLOUDFLARE_TRACE_URLS = (
    "https://www.cloudflare.com/cdn-cgi/trace",
    "https://1.1.1.1/cdn-cgi/trace",
)

AFFECTED_FEATURES: tuple[str, ...] = ("Direct", "Torrent", "MediaFire")

# In-memory cache of the persisted ON/OFF toggle (scoped by state_file path)
_proxy_enabled: Optional[bool] = None
_db_synced: bool = False
_cached_state_file: Optional[str] = None
_managed_proc: Optional[subprocess.Popen] = None
_state_lock: Optional[asyncio.Lock] = None
_state_lock_loop: Optional[asyncio.AbstractEventLoop] = None
_service_lock: Optional[asyncio.Lock] = None
_service_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_state_lock() -> asyncio.Lock:
    global _state_lock, _state_lock_loop
    loop = asyncio.get_running_loop()
    if _state_lock is None or _state_lock_loop is not loop:
        _state_lock = asyncio.Lock()
        _state_lock_loop = loop
    return _state_lock


def _get_service_lock() -> asyncio.Lock:
    global _service_lock, _service_lock_loop
    loop = asyncio.get_running_loop()
    if _service_lock is None or _service_lock_loop is not loop:
        _service_lock = asyncio.Lock()
        _service_lock_loop = loop
    return _service_lock


def reset_runtime_state() -> None:
    """Reset in-memory state cache (primarily for tests)."""
    global _proxy_enabled, _db_synced, _cached_state_file
    _proxy_enabled = None
    _db_synced = False
    _cached_state_file = None


@dataclass(frozen=True)
class WarpTraceResult:
    """Result of querying Cloudflare's ``/cdn-cgi/trace`` via wireproxy."""

    ok: bool
    warp_on: bool
    warp_state: str = "unknown"
    exit_ip: Optional[str] = None
    location: Optional[str] = None
    colo: Optional[str] = None
    error: Optional[str] = None


@dataclass(frozen=True)
class WarpStatus:
    """Comprehensive WARP wireproxy status snapshot."""

    installed: bool
    wireproxy_binary: Optional[str]
    config_exists: bool
    service_active: bool
    service_manager: str  # "systemd" | "managed" | "external" | "none"
    socks5_listening: bool
    http_listening: bool
    ports_listening: bool
    proxy_enabled: bool
    socks5_url: str
    http_url: str
    warp_on: Optional[bool] = None
    warp_state: str = "unknown"
    exit_ip: Optional[str] = None
    location: Optional[str] = None
    colo: Optional[str] = None
    platform: str = ""
    detail: str = ""
    error: Optional[str] = None
    affected_features: tuple[str, ...] = AFFECTED_FEATURES

    @property
    def running(self) -> bool:
        return self.service_active

    @property
    def mode(self) -> str:
        return self.service_manager


@dataclass(frozen=True)
class WarpActionResult:
    """Structured outcome returned by install / enable / disable actions."""

    ok: bool
    enabled: Optional[bool]
    message: str
    error: Optional[str] = None
    socks5_url: str = ""
    http_url: str = ""
    status: Optional[WarpStatus] = None
    trace: Optional[WarpTraceResult] = None


# ---------------------------------------------------------------------------
# Subprocess & OS helpers (strictly exec-based, never shell=True)
# ---------------------------------------------------------------------------

async def run_exec(
    *args: str,
    cwd: Optional[str] = None,
    timeout: float = 30.0,
    stdin_data: Optional[bytes] = None,
) -> tuple[int, str, str]:
    """Run an executable without a shell and return ``(returncode, stdout, stderr)``."""
    proc = await asyncio.create_subprocess_exec(
        *args,
        cwd=cwd,
        stdin=asyncio.subprocess.PIPE if stdin_data is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(input=stdin_data),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.communicate()
        except Exception:
            pass
        return (-1, "", f"Command timed out after {timeout:.0f}s: {args[0]}")

    return (
        int(proc.returncode if proc.returncode is not None else -1),
        (stdout_b or b"").decode("utf-8", errors="replace").strip(),
        (stderr_b or b"").decode("utf-8", errors="replace").strip(),
    )


def _chmod_safe(path: str, mode: int) -> None:
    if os.name == "posix":
        try:
            os.chmod(path, mode)
        except OSError:
            pass


def _write_file_atomic(path: str, content: str, mode: int = 0o600) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".warp_", dir=parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(content)
        _chmod_safe(tmp_path, mode)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Local & MongoDB persistent state
# ---------------------------------------------------------------------------

def _has_mongo_uri() -> bool:
    return bool(str(getattr(app_config, "MONGO_URI", "") or "").strip())


def _read_local_state(cfg: WarpConfig) -> Optional[dict[str, Any]]:
    try:
        with open(cfg.state_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
    except (OSError, ValueError, TypeError):
        return None
    return None


def _write_local_state(
    cfg: WarpConfig,
    enabled: bool,
    meta: Optional[dict[str, Any]] = None,
) -> bool:
    payload: dict[str, Any] = {
        "enabled": bool(enabled),
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if meta:
        for key, value in meta.items():
            if key not in ("enabled", "updated_at"):
                payload[key] = value
    try:
        _write_file_atomic(cfg.state_file, json.dumps(payload, indent=2) + "\n", mode=0o600)
        return True
    except OSError as exc:
        logger.warning("Failed to write local WARP state file %s: %s", cfg.state_file, exc)
        return False


async def load_proxy_state(cfg: Optional[WarpConfig] = None) -> bool:
    """Load persisted WARP ON/OFF state from MongoDB, falling back to local ``state.json``."""
    global _proxy_enabled, _db_synced, _cached_state_file
    cfg = cfg or get_warp_config()

    async with _get_state_lock():
        db_doc: Optional[dict[str, Any]] = None
        if _has_mongo_uri():
            try:
                from app.database.db import get_warp_proxy_state

                db_doc = await asyncio.wait_for(get_warp_proxy_state(), timeout=3.0)
            except Exception as exc:
                logger.warning("WARP DB state load failed (using local fallback): %s", exc)

        if isinstance(db_doc, dict) and "enabled" in db_doc:
            enabled = bool(db_doc["enabled"])
            _proxy_enabled = enabled
            _db_synced = True
            _cached_state_file = cfg.state_file
            _write_local_state(cfg, enabled)
            return enabled

        local = _read_local_state(cfg)
        if isinstance(local, dict) and "enabled" in local:
            enabled = bool(local["enabled"])
        else:
            enabled = False

        _proxy_enabled = enabled
        _db_synced = True
        _cached_state_file = cfg.state_file
        return enabled


async def _persist_proxy_state(
    enabled: bool,
    cfg: WarpConfig,
    meta: Optional[dict[str, Any]] = None,
) -> tuple[bool, Optional[str]]:
    global _proxy_enabled, _db_synced, _cached_state_file
    enabled_bool = bool(enabled)

    async with _get_state_lock():
        _proxy_enabled = enabled_bool
        _db_synced = True
        _cached_state_file = cfg.state_file
        _write_local_state(cfg, enabled_bool, meta=meta)

        if not _has_mongo_uri():
            return (False, None)

        try:
            from app.database.db import save_warp_proxy_state

            await asyncio.wait_for(
                save_warp_proxy_state(enabled_bool, meta=meta),
                timeout=4.0,
            )
            return (True, None)
        except Exception as exc:
            logger.warning("WARP DB state save failed (saved locally only): %s", exc)
            return (False, str(exc))


def is_proxy_enabled(cfg: Optional[WarpConfig] = None) -> bool:
    """Fast synchronous check for whether WARP proxy routing is currently ON.

    Lazily initializes from local ``state.json`` if async DB sync has not yet run.
    """
    global _proxy_enabled, _cached_state_file, _db_synced
    resolved_cfg = cfg or get_warp_config()
    if _proxy_enabled is not None and _cached_state_file == resolved_cfg.state_file:
        return _proxy_enabled

    if _cached_state_file is not None and _cached_state_file != resolved_cfg.state_file:
        _db_synced = False

    local = _read_local_state(resolved_cfg)
    if isinstance(local, dict) and "enabled" in local:
        _proxy_enabled = bool(local["enabled"])
        _cached_state_file = resolved_cfg.state_file
        return _proxy_enabled
    return False


async def is_proxy_enabled_async(cfg: Optional[WarpConfig] = None) -> bool:
    """Return whether WARP proxy routing is ON, syncing persisted DB state once."""
    resolved_cfg = cfg or get_warp_config()
    if not _db_synced or _cached_state_file != resolved_cfg.state_file:
        return await load_proxy_state(resolved_cfg)
    return bool(_proxy_enabled)


# ---------------------------------------------------------------------------
# Installation & service detection / lifecycle
# ---------------------------------------------------------------------------

def _is_executable_file(path: str) -> bool:
    if not path or not os.path.isfile(path):
        return False
    if os.name == "posix":
        return os.access(path, os.X_OK)
    return True


def find_wireproxy_binary(cfg: Optional[WarpConfig] = None) -> Optional[str]:
    """Locate the ``wireproxy`` executable if installed."""
    cfg = cfg or get_warp_config()
    candidates = [cfg.wireproxy_bin]
    if sys.platform == "linux":
        candidates.extend(["/usr/local/bin/wireproxy", "/usr/bin/wireproxy"])

    for path in candidates:
        if _is_executable_file(path):
            return path

    found = shutil.which("wireproxy")
    if found and _is_executable_file(found):
        return found
    return None


def find_wireproxy_config(cfg: Optional[WarpConfig] = None) -> Optional[str]:
    """Locate an existing ``wireproxy.conf`` file."""
    cfg = cfg or get_warp_config()
    candidates = [cfg.wireproxy_conf_path]
    if sys.platform == "linux":
        candidates.append("/etc/wireproxy/wireproxy.conf")

    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def is_wireproxy_installed(cfg: Optional[WarpConfig] = None) -> bool:
    cfg = cfg or get_warp_config()
    return bool(find_wireproxy_binary(cfg) and find_wireproxy_config(cfg))


def is_systemd_available() -> bool:
    return (
        sys.platform == "linux"
        and os.path.isdir("/run/systemd/system")
        and shutil.which("systemctl") is not None
    )


def systemd_unit_exists(cfg: WarpConfig) -> bool:
    return is_systemd_available() and os.path.isfile(cfg.systemd_unit_path)


async def can_manage_systemd(cfg: Optional[WarpConfig] = None) -> tuple[bool, bool]:
    """Return ``(can_manage, needs_sudo)`` for system-level systemd unit control."""
    if not is_systemd_available():
        return (False, False)

    geteuid = getattr(os, "geteuid", None)
    if geteuid is not None and geteuid() == 0:
        if os.path.isdir("/etc/systemd/system") and os.access("/etc/systemd/system", os.W_OK):
            return (True, False)
        return (False, False)

    if shutil.which("sudo") is None:
        return (False, False)

    rc, _, _ = await run_exec("sudo", "-n", "true", timeout=5.0)
    if rc != 0:
        return (False, False)

    rc_w, _, _ = await run_exec("sudo", "-n", "test", "-w", "/etc/systemd/system", timeout=5.0)
    if rc_w != 0:
        return (False, False)

    return (True, True)


async def is_systemd_active(cfg: WarpConfig) -> bool:
    if not is_systemd_available():
        return False
    rc, _, _ = await run_exec("systemctl", "is-active", "--quiet", cfg.service_name, timeout=5.0)
    return rc == 0


def _read_pid(cfg: WarpConfig) -> Optional[int]:
    try:
        with open(cfg.pid_file, "r", encoding="utf-8") as fh:
            raw = fh.read().strip()
        pid = int(raw)
        return pid if pid > 0 else None
    except (OSError, ValueError):
        return None


def _is_managed_wireproxy_pid(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name != "posix":
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    except OSError:
        return False

    if sys.platform == "linux":
        cmdline_path = f"/proc/{pid}/cmdline"
        try:
            with open(cmdline_path, "rb") as fh:
                raw = fh.read()
            if b"wireproxy" not in raw:
                return False
        except OSError:
            return False
    return True


def is_managed_running(cfg: WarpConfig) -> bool:
    pid = _read_pid(cfg)
    if pid is None:
        return False
    return _is_managed_wireproxy_pid(pid)


def _reap_managed_proc() -> None:
    global _managed_proc
    if _managed_proc is not None and _managed_proc.poll() is not None:
        _managed_proc = None


async def stop_managed_wireproxy(cfg: Optional[WarpConfig] = None) -> tuple[bool, str]:
    """Stop the bot-managed user-space wireproxy process if running."""
    global _managed_proc
    cfg = cfg or get_warp_config()
    _reap_managed_proc()
    pid = _read_pid(cfg)
    if pid is None and _managed_proc is not None:
        pid = _managed_proc.pid

    if pid is None:
        return (True, "no managed wireproxy process")

    if not _is_managed_wireproxy_pid(pid):
        _reap_managed_proc()
        try:
            os.unlink(cfg.pid_file)
        except OSError:
            pass
        return (True, "cleaned stale wireproxy pid file")

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError as exc:
        return (False, f"failed to stop managed wireproxy (pid {pid}): {exc}")

    for _ in range(20):
        _reap_managed_proc()
        if not _is_managed_wireproxy_pid(pid):
            break
        await asyncio.sleep(0.25)

    if _is_managed_wireproxy_pid(pid):
        try:
            os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        except OSError:
            pass
        await asyncio.sleep(0.2)
        _reap_managed_proc()

    if _managed_proc is not None and _managed_proc.pid == pid:
        try:
            _managed_proc.wait(timeout=1.0)
        except Exception:
            pass
        _managed_proc = None

    try:
        os.unlink(cfg.pid_file)
    except OSError:
        pass
    return (True, f"stopped managed wireproxy (pid {pid})")


async def start_managed_wireproxy(
    cfg: Optional[WarpConfig] = None,
    binary: Optional[str] = None,
    conf_path: Optional[str] = None,
) -> tuple[bool, str]:
    """Start wireproxy as a detached user-space subprocess (no root required)."""
    global _managed_proc
    cfg = cfg or get_warp_config()
    binary = binary or find_wireproxy_binary(cfg)
    conf_path = conf_path or find_wireproxy_config(cfg)

    if not binary:
        return (False, "wireproxy binary not found")
    if not conf_path:
        return (False, "wireproxy.conf not found")

    await stop_managed_wireproxy(cfg)
    os.makedirs(cfg.data_dir, exist_ok=True)

    log_fh = open(cfg.log_file, "ab")
    try:
        popen_kwargs: dict[str, Any] = {
            "cwd": cfg.data_dir,
            "stdin": subprocess.DEVNULL,
            "stdout": log_fh,
            "stderr": subprocess.STDOUT,
        }
        if os.name == "posix":
            popen_kwargs["start_new_session"] = True
        proc = subprocess.Popen([binary, "-c", conf_path], **popen_kwargs)
    except OSError as exc:
        return (False, f"failed to launch wireproxy: {exc}")
    finally:
        try:
            log_fh.close()
        except OSError:
            pass

    _managed_proc = proc
    _write_file_atomic(cfg.pid_file, f"{proc.pid}\n", mode=0o600)

    # Give the process a brief moment to fail fast on bad config / port bind errors
    await asyncio.sleep(0.4)
    exit_code = proc.poll()
    if exit_code is not None:
        _managed_proc = None
        tail = _read_log_tail(cfg.log_file)
        try:
            os.unlink(cfg.pid_file)
        except OSError:
            pass
        detail = f": {tail}" if tail else ""
        return (False, f"wireproxy exited immediately with code {exit_code}{detail}")

    return (True, f"started managed wireproxy (pid {proc.pid})")


def _read_log_tail(path: str, max_chars: int = 400) -> str:
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 2048))
            text = fh.read().decode("utf-8", errors="replace").strip()
        if len(text) > max_chars:
            return text[-max_chars:]
        return text
    except OSError:
        return ""


# ---------------------------------------------------------------------------
# Port & Cloudflare trace checks
# ---------------------------------------------------------------------------

async def check_tcp_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check whether a local TCP listener accepts connections on ``host:port``."""
    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        return True
    except (OSError, asyncio.TimeoutError):
        return False
    finally:
        if writer is not None:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass


async def check_proxy_reachable(
    require_http: bool = True,
    require_socks5: bool = True,
    timeout: float = 2.0,
    cfg: Optional[WarpConfig] = None,
) -> bool:
    """Fast localhost TCP reachability probe for wireproxy's SOCKS5 and/or HTTP listeners."""
    cfg = cfg or get_warp_config()
    checks = []
    if require_socks5:
        checks.append(check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=timeout))
    if require_http:
        checks.append(check_tcp_port(cfg.bind_host, cfg.http_port, timeout=timeout))
    if not checks:
        return True
    results = await asyncio.gather(*checks)
    return all(results)


async def wait_for_ports(
    cfg: WarpConfig,
    timeout: float = 10.0,
    poll_interval: float = 0.35,
) -> tuple[bool, bool]:
    """Wait until both SOCKS5 and HTTP ports are accepting connections or *timeout* elapses."""
    deadline = asyncio.get_running_loop().time() + timeout
    socks_ok = False
    http_ok = False

    while True:
        socks_ok, http_ok = await asyncio.gather(
            check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=1.5),
            check_tcp_port(cfg.bind_host, cfg.http_port, timeout=1.5),
        )
        if socks_ok and http_ok:
            return (True, True)
        if asyncio.get_running_loop().time() >= deadline:
            return (socks_ok, http_ok)
        await asyncio.sleep(poll_interval)


def parse_cloudflare_trace(text: str) -> dict[str, str]:
    """Parse key=value lines from Cloudflare's ``/cdn-cgi/trace`` response."""
    result: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            result[key] = value
    return result


async def check_cloudflare_trace(
    cfg: Optional[WarpConfig] = None,
    timeout: Optional[float] = None,
) -> WarpTraceResult:
    """Verify live WARP egress by querying Cloudflare trace through the SOCKS5 proxy."""
    cfg = cfg or get_warp_config()
    req_timeout = float(timeout if timeout is not None else cfg.trace_timeout)
    last_error = "Unable to reach Cloudflare trace endpoint"

    for url in CLOUDFLARE_TRACE_URLS:
        connector = ProxyConnector.from_url(cfg.socks5_url, rdns=True)
        client_timeout = aiohttp.ClientTimeout(total=req_timeout)
        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=client_timeout,
                trust_env=False,
            ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        last_error = f"Trace endpoint returned HTTP {resp.status}"
                        continue
                    body = await resp.text()
                    fields = parse_cloudflare_trace(body)
                    warp_state = fields.get("warp", "unknown").lower()
                    warp_on = warp_state in ("on", "plus")
                    return WarpTraceResult(
                        ok=True,
                        warp_on=warp_on,
                        warp_state=warp_state,
                        exit_ip=fields.get("ip"),
                        location=fields.get("loc"),
                        colo=fields.get("colo"),
                        error=None if warp_on else f"Cloudflare trace reported warp={warp_state}",
                    )
        except Exception as exc:
            last_error = str(exc) or type(exc).__name__

    return WarpTraceResult(
        ok=False,
        warp_on=False,
        warp_state="unknown",
        error=last_error,
    )


# ---------------------------------------------------------------------------
# Service start / stop / restart
# ---------------------------------------------------------------------------

async def start_wireproxy(cfg: Optional[WarpConfig] = None) -> tuple[bool, str]:
    """Ensure wireproxy is running via systemd (when available) or managed user-space process."""
    cfg = cfg or get_warp_config()

    async with _get_service_lock():
        if await check_proxy_reachable(require_http=True, require_socks5=True, timeout=1.5, cfg=cfg):
            return (True, "wireproxy is already running")

        if sys.platform != "linux":
            return (
                False,
                f"Cannot start wireproxy service on {platform.system()} ({sys.platform}); "
                "managed WARP wireproxy requires a Linux VPS.",
            )

        if not is_wireproxy_installed(cfg):
            return (False, "WARP wireproxy is not installed. Run /warp_install first.")

        if systemd_unit_exists(cfg):
            can_manage, needs_sudo = await can_manage_systemd(cfg)
            if can_manage:
                cmd = (["sudo", "-n"] if needs_sudo else []) + ["systemctl", "start", cfg.service_name]
                rc, _, stderr = await run_exec(*cmd, timeout=20.0)
                if rc == 0:
                    socks_ok, http_ok = await wait_for_ports(cfg, timeout=10.0)
                    if socks_ok and http_ok:
                        return (True, f"started systemd service {cfg.service_name}")
                    return (
                        False,
                        f"systemd service {cfg.service_name} started, but ports are not listening "
                        f"(socks5={socks_ok}, http={http_ok})",
                    )
                logger.warning("systemctl start %s failed (%s); falling back to managed mode", cfg.service_name, stderr)

        ok, detail = await start_managed_wireproxy(cfg)
        if not ok:
            return (False, detail)

        socks_ok, http_ok = await wait_for_ports(cfg, timeout=10.0)
        if socks_ok and http_ok:
            return (True, detail)
        tail = _read_log_tail(cfg.log_file)
        extra = f" | log: {tail}" if tail else ""
        return (
            False,
            f"wireproxy started, but proxy ports did not become ready "
            f"(socks5={socks_ok}, http={http_ok}){extra}",
        )


async def stop_wireproxy(cfg: Optional[WarpConfig] = None) -> tuple[bool, str]:
    """Stop wireproxy (systemd if privileged, plus any managed subprocess)."""
    cfg = cfg or get_warp_config()
    async with _get_service_lock():
        if systemd_unit_exists(cfg):
            can_manage, needs_sudo = await can_manage_systemd(cfg)
            if can_manage:
                cmd = (["sudo", "-n"] if needs_sudo else []) + ["systemctl", "stop", cfg.service_name]
                await run_exec(*cmd, timeout=20.0)
        return await stop_managed_wireproxy(cfg)


async def restart_wireproxy(cfg: Optional[WarpConfig] = None) -> tuple[bool, str]:
    cfg = cfg or get_warp_config()
    await stop_wireproxy(cfg)
    return await start_wireproxy(cfg)


# ---------------------------------------------------------------------------
# High-level bot / lifecycle API
# ---------------------------------------------------------------------------

async def init_warp() -> bool:
    """Initialize WARP state on bot startup; auto-start wireproxy if routing is ON."""
    cfg = get_warp_config()
    enabled = await load_proxy_state(cfg)
    if enabled:
        reachable = await check_proxy_reachable(require_http=True, require_socks5=True, timeout=1.5, cfg=cfg)
        if not reachable and is_wireproxy_installed(cfg) and sys.platform == "linux":
            ok, detail = await start_wireproxy(cfg)
            if ok:
                logger.info("WARP wireproxy auto-started on startup: %s", detail)
            else:
                logger.warning("WARP routing is ON, but auto-start failed: %s", detail)
    return enabled


async def shutdown_warp() -> None:
    """Cleanly stop any bot-managed user-space wireproxy process on bot shutdown.

    Leaves an independently managed systemd service running.
    """
    cfg = get_warp_config()
    try:
        await stop_managed_wireproxy(cfg)
    except Exception as exc:
        logger.debug("Ignoring error during managed wireproxy shutdown: %s", exc)


async def get_warp_status(
    check_trace: bool = True,
    cfg: Optional[WarpConfig] = None,
) -> WarpStatus:
    """Inspect wireproxy install state, service health, listeners, routing toggle, and trace."""
    cfg = cfg or get_warp_config()
    enabled = await is_proxy_enabled_async(cfg)
    binary = find_wireproxy_binary(cfg)
    conf_path = find_wireproxy_config(cfg)
    installed = bool(binary and conf_path)

    socks_ok, http_ok = await asyncio.gather(
        check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=1.5),
        check_tcp_port(cfg.bind_host, cfg.http_port, timeout=1.5),
    )
    ports_ok = socks_ok and http_ok

    systemd_active = await is_systemd_active(cfg) if systemd_unit_exists(cfg) else False
    managed_active = is_managed_running(cfg)

    if systemd_active:
        service_manager = "systemd"
        service_active = True
    elif managed_active:
        service_manager = "managed"
        service_active = True
    elif ports_ok:
        service_manager = "external"
        service_active = True
    else:
        service_manager = "systemd" if systemd_unit_exists(cfg) else ("managed" if installed else "none")
        service_active = False

    trace: Optional[WarpTraceResult] = None
    if check_trace and socks_ok:
        trace = await check_cloudflare_trace(cfg)

    plat = f"{platform.system()} ({platform.machine()})"
    error: Optional[str] = None
    if not installed and not ports_ok:
        detail = "Not installed (run /warp_install on Linux VPS)."
    elif not ports_ok:
        detail = (
            f"Installed ({service_manager}), but proxy ports are not listening "
            f"(SOCKS5={'up' if socks_ok else 'down'}, HTTP={'up' if http_ok else 'down'})."
        )
        error = detail
    elif trace is not None and not trace.ok:
        detail = f"Proxy ports listening ({service_manager}), but Cloudflare trace failed."
        error = trace.error
    elif trace is not None and not trace.warp_on:
        detail = f"Proxy ports listening ({service_manager}), trace reported warp={trace.warp_state}."
        error = trace.error
    else:
        detail = f"Healthy ({service_manager}); routing is {'ON' if enabled else 'OFF'}."

    return WarpStatus(
        installed=installed,
        wireproxy_binary=binary,
        config_exists=bool(conf_path),
        service_active=service_active,
        service_manager=service_manager,
        socks5_listening=socks_ok,
        http_listening=http_ok,
        ports_listening=ports_ok,
        proxy_enabled=enabled,
        socks5_url=cfg.socks5_url,
        http_url=cfg.http_url,
        warp_on=trace.warp_on if trace is not None else None,
        warp_state=trace.warp_state if trace is not None else "unknown",
        exit_ip=trace.exit_ip if trace is not None else None,
        location=trace.location if trace is not None else None,
        colo=trace.colo if trace is not None else None,
        platform=plat,
        detail=detail,
        error=error,
    )


async def enable_proxy(cfg: Optional[WarpConfig] = None) -> WarpActionResult:
    """Turn WARP proxy routing ON after verifying wireproxy is installed and reachable."""
    cfg = cfg or get_warp_config()
    await is_proxy_enabled_async(cfg)

    socks_ok, http_ok = await asyncio.gather(
        check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=1.5),
        check_tcp_port(cfg.bind_host, cfg.http_port, timeout=1.5),
    )

    if not (socks_ok and http_ok):
        if not is_wireproxy_installed(cfg):
            status = await get_warp_status(check_trace=False, cfg=cfg)
            msg = (
                "WARP wireproxy is not installed on this server. "
                "Run /warp_install first before enabling WARP proxy."
            )
            return WarpActionResult(
                ok=False,
                enabled=is_proxy_enabled(cfg),
                message=msg,
                error=msg,
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
            )

        started, start_detail = await start_wireproxy(cfg)
        if not started:
            status = await get_warp_status(check_trace=False, cfg=cfg)
            msg = f"Could not start WARP wireproxy: {start_detail}"
            return WarpActionResult(
                ok=False,
                enabled=is_proxy_enabled(cfg),
                message=msg,
                error=msg,
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
            )

        socks_ok, http_ok = await asyncio.gather(
            check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=1.5),
            check_tcp_port(cfg.bind_host, cfg.http_port, timeout=1.5),
        )
        if not (socks_ok and http_ok):
            status = await get_warp_status(check_trace=False, cfg=cfg)
            msg = (
                "WARP wireproxy is unreachable on localhost "
                f"(SOCKS5 {cfg.socks5_bind}={'up' if socks_ok else 'down'}, "
                f"HTTP {cfg.http_bind}={'up' if http_ok else 'down'}). "
                "Run /warp_status or reinstall with /warp_install."
            )
            return WarpActionResult(
                ok=False,
                enabled=is_proxy_enabled(cfg),
                message=msg,
                error=msg,
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
            )

    db_saved, db_error = await _persist_proxy_state(
        True,
        cfg,
        meta={"socks5_url": cfg.socks5_url, "http_url": cfg.http_url},
    )
    status = await get_warp_status(check_trace=False, cfg=cfg)
    note = "" if db_saved or not _has_mongo_uri() else f" (DB warning: {db_error}; saved locally)"
    return WarpActionResult(
        ok=True,
        enabled=True,
        message=(
            "WARP proxy routing is now ON for Direct, Torrent, and MediaFire downloads."
            + note
        ),
        socks5_url=cfg.socks5_url,
        http_url=cfg.http_url,
        status=status,
    )


async def disable_proxy(cfg: Optional[WarpConfig] = None) -> WarpActionResult:
    """Turn WARP proxy routing OFF persistently (preserving wireproxy installation)."""
    cfg = cfg or get_warp_config()
    db_saved, db_error = await _persist_proxy_state(False, cfg)
    status = await get_warp_status(check_trace=False, cfg=cfg)
    note = "" if db_saved or not _has_mongo_uri() else f" (DB warning: {db_error}; saved locally)"
    return WarpActionResult(
        ok=True,
        enabled=False,
        message=(
            "WARP proxy routing is now OFF. Direct, Torrent, and MediaFire will use default routing."
            + note
        ),
        socks5_url=cfg.socks5_url,
        http_url=cfg.http_url,
        status=status,
    )


# ---------------------------------------------------------------------------
# Downloader consumer helpers
# ---------------------------------------------------------------------------

def get_active_socks5_url() -> Optional[str]:
    """Return ``socks5://127.0.0.1:<port>`` when WARP proxy is ON, else ``None``."""
    if not is_proxy_enabled():
        return None
    return get_warp_config().socks5_url


def get_active_http_url() -> Optional[str]:
    """Return ``http://127.0.0.1:<port>`` when WARP proxy is ON, else ``None``."""
    if not is_proxy_enabled():
        return None
    return get_warp_config().http_url


def get_active_proxy_config() -> Optional[WarpProxyConfig]:
    """Return active ``WarpProxyConfig`` when WARP proxy is ON, else ``None``."""
    if not is_proxy_enabled():
        return None
    return get_warp_config().to_proxy_config(enabled=True)


def create_aiohttp_connector(**kwargs: Any) -> Optional[ProxyConnector]:
    """Create a ``ProxyConnector`` pointing at WARP SOCKS5 when ON, or ``None`` when OFF.

    Defaults ``rdns=True`` so DNS lookups also traverse Cloudflare WARP.
    Additional ``aiohttp.TCPConnector`` / ``ProxyConnector`` keyword arguments
    (such as ``limit``, ``limit_per_host``, ``ssl``) are forwarded unchanged.
    """
    socks5_url = get_active_socks5_url()
    if not socks5_url:
        return None
    kwargs.setdefault("rdns", True)
    return ProxyConnector.from_url(socks5_url, **kwargs)


async def ensure_proxy_ready(
    require_http: bool = False,
    require_socks5: bool = True,
    auto_start: bool = True,
    feature: Optional[str] = None,
    cfg: Optional[WarpConfig] = None,
) -> Optional[WarpProxyConfig]:
    """Sync state and ensure local wireproxy is reachable when WARP routing is ON.

    Returns ``None`` when WARP proxy routing is OFF, ``WarpProxyConfig`` when ON
    and reachable, or raises ``WarpProxyError`` with clear user guidance if ON
    but wireproxy cannot be reached.
    """
    cfg = cfg or get_warp_config()
    enabled = await is_proxy_enabled_async(cfg)
    if not enabled:
        return None

    reachable = await check_proxy_reachable(
        require_http=require_http,
        require_socks5=require_socks5,
        timeout=2.0,
        cfg=cfg,
    )
    if reachable:
        return cfg.to_proxy_config(enabled=True)

    if auto_start and is_wireproxy_installed(cfg) and sys.platform == "linux":
        started, _ = await start_wireproxy(cfg)
        if started:
            reachable = await check_proxy_reachable(
                require_http=require_http,
                require_socks5=require_socks5,
                timeout=2.0,
                cfg=cfg,
            )
            if reachable:
                return cfg.to_proxy_config(enabled=True)

    if not is_wireproxy_installed(cfg):
        raise WarpProxyError(
            format_warp_proxy_error(
                WarpNotInstalledError("WARP wireproxy is not installed."),
                feature=feature,
            )
        )

    missing = []
    if require_socks5 and not await check_tcp_port(cfg.bind_host, cfg.socks5_port, timeout=1.0):
        missing.append(f"SOCKS5 {cfg.socks5_bind}")
    if require_http and not await check_tcp_port(cfg.bind_host, cfg.http_port, timeout=1.0):
        missing.append(f"HTTP {cfg.http_bind}")
    ports_desc = ", ".join(missing) or f"{cfg.socks5_bind} / {cfg.http_bind}"
    raise WarpProxyError(
        format_warp_proxy_error(
            ConnectionRefusedError(f"wireproxy listener unreachable ({ports_desc})"),
            feature=feature,
        )
    )
