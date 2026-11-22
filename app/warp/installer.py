"""Async Linux VPS installer for Cloudflare WARP + wireproxy."""

from __future__ import annotations

import asyncio
import getpass
import inspect
import logging
import os
import platform
import shutil
import sys
import tarfile
import tempfile
from typing import Awaitable, Callable, Optional
from urllib.parse import urlparse

import aiohttp

from app.warp.config import (
    DEFAULT_WARP_ENDPOINT,
    FALLBACK_WARP_IPV4_ENDPOINT,
    WarpConfig,
    build_systemd_unit,
    build_wg_profile,
    build_wireproxy_conf,
    get_warp_config,
    parse_wgcf_profile,
)
from app.warp.errors import WarpUnsupportedPlatformError
from app.warp.manager import (
    WarpActionResult,
    WarpTraceResult,
    _chmod_safe,
    _write_file_atomic,
    can_manage_systemd,
    check_cloudflare_trace,
    check_proxy_reachable,
    get_warp_status,
    is_managed_running,
    is_proxy_enabled,
    is_systemd_active,
    run_exec,
    start_managed_wireproxy,
    stop_managed_wireproxy,
    systemd_unit_exists,
    wait_for_ports,
)

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str], Optional[Awaitable[None]]]

WIREPROXY_REPOS: tuple[str, ...] = (
    "windtf/wireproxy",
    "whyvl/wireproxy",
    "pufferffish/wireproxy",
)
WGCF_REPOS: tuple[str, ...] = ("ViRb3/wgcf",)

_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024  # 100 MB safety ceiling
_ELF_MAGIC = b"\x7fELF"

_install_lock: Optional[asyncio.Lock] = None
_install_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_install_lock() -> asyncio.Lock:
    global _install_lock, _install_lock_loop
    loop = asyncio.get_running_loop()
    if _install_lock is None or _install_lock_loop is not loop:
        _install_lock = asyncio.Lock()
        _install_lock_loop = loop
    return _install_lock


def detect_platform() -> tuple[str, str]:
    """Return ``("linux", "amd64" | "arm64")`` or raise ``WarpUnsupportedPlatformError``."""
    if sys.platform != "linux":
        raise WarpUnsupportedPlatformError(
            f"WARP wireproxy installation is only supported on Linux VPS "
            f"(detected: {platform.system()} / {sys.platform})."
        )

    machine = (platform.machine() or "").strip().lower()
    if machine in ("x86_64", "amd64"):
        return ("linux", "amd64")
    if machine in ("aarch64", "arm64"):
        return ("linux", "arm64")

    raise WarpUnsupportedPlatformError(
        f"Unsupported Linux CPU architecture: {machine or 'unknown'} (supported: amd64, arm64)."
    )


async def _emit_progress(on_progress: Optional[ProgressCallback], text: str) -> None:
    logger.info("[warp-install] %s", text)
    if on_progress is None:
        return
    try:
        res = on_progress(text)
        if inspect.isawaitable(res):
            await res
    except Exception as exc:
        logger.debug("Progress callback raised (ignored): %s", exc)


# ---------------------------------------------------------------------------
# GitHub release resolution & safe binary downloads
# ---------------------------------------------------------------------------

def _match_wireproxy_asset(name: str, arch: str) -> bool:
    lower = name.lower()
    if any(bad in lower for bad in (".sha256", ".asc", ".sig", ".sbom", "checksum")):
        return False
    return lower == f"wireproxy_linux_{arch}.tar.gz" or (
        "wireproxy" in lower and f"linux_{arch}" in lower and lower.endswith(".tar.gz")
    )


def _match_wgcf_asset(name: str, arch: str) -> bool:
    lower = name.lower()
    if any(bad in lower for bad in (".sha256", ".asc", ".sig", ".tar", ".zip", "checksum")):
        return False
    return lower.startswith("wgcf") and lower.endswith(f"_linux_{arch}")


async def _resolve_release_asset(
    session: aiohttp.ClientSession,
    repos: tuple[str, ...],
    matcher: Callable[[str], bool],
    fallback_filename_for_tag: Callable[[str], str],
) -> tuple[str, str]:
    """Resolve ``(download_url, version_tag)`` via GitHub API with redirect fallback."""
    last_error = "no release found"

    # 1. GitHub Releases API (preferred — gives exact asset browser_download_url)
    for repo in repos:
        api_url = f"https://api.github.com/repos/{repo}/releases/latest"
        try:
            async with session.get(
                api_url,
                headers={"Accept": "application/vnd.github+json"},
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    tag = str(data.get("tag_name") or "").strip()
                    assets = data.get("assets") or []
                    for asset in assets:
                        name = str(asset.get("name") or "")
                        dl_url = str(asset.get("browser_download_url") or "")
                        if name and dl_url and matcher(name):
                            return (dl_url, tag or "latest")
                    last_error = f"{repo}: no matching release asset found for tag {tag!r}"
                else:
                    last_error = f"{repo}: GitHub API HTTP {resp.status}"
        except Exception as exc:
            last_error = f"{repo}: {exc}"

    # 2. Fallback: resolve https://github.com/<repo>/releases/latest redirect (no API rate limit)
    for repo in repos:
        latest_url = f"https://github.com/{repo}/releases/latest"
        try:
            async with session.get(
                latest_url,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                final_url = str(resp.url)
                parsed = urlparse(final_url)
                parts = [p for p in parsed.path.split("/") if p]
                if "tag" in parts:
                    tag_idx = parts.index("tag")
                    if tag_idx + 1 < len(parts):
                        tag = parts[tag_idx + 1]
                        filename = fallback_filename_for_tag(tag)
                        dl_url = f"https://github.com/{repo}/releases/download/{tag}/{filename}"
                        return (dl_url, tag)
                last_error = f"{repo}: unexpected releases/latest redirect URL {final_url}"
        except Exception as exc:
            last_error = f"{repo}: {exc}"

    raise RuntimeError(f"Failed to resolve GitHub release asset ({last_error})")


async def _download_to_path(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: str,
) -> int:
    """Stream *url* safely into *dest_path* via a temp file and return bytes written."""
    parent = os.path.dirname(os.path.abspath(dest_path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".dl_", dir=parent)
    os.close(fd)
    total = 0

    try:
        async with session.get(url, allow_redirects=True) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Download failed with HTTP {resp.status} for {url}")
            with open(tmp_path, "wb") as fh:
                async for chunk in resp.content.iter_chunked(65536):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > _MAX_DOWNLOAD_BYTES:
                        raise RuntimeError("Downloaded archive exceeded safety size limit")
                    fh.write(chunk)

        if total == 0:
            raise RuntimeError(f"Downloaded empty file from {url}")
        os.replace(tmp_path, dest_path)
        return total
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _verify_elf_binary(path: str, label: str) -> None:
    with open(path, "rb") as fh:
        magic = fh.read(4)
    if magic != _ELF_MAGIC:
        raise RuntimeError(
            f"Downloaded {label} binary is invalid (expected ELF header, got {magic!r})"
        )


def _safe_extract_wireproxy_tarball(tar_path: str, dest_bin_path: str) -> None:
    """Safely extract ONLY the ``wireproxy`` regular file from a .tar.gz archive."""
    parent = os.path.dirname(os.path.abspath(dest_bin_path))
    os.makedirs(parent, exist_ok=True)
    fd, tmp_bin = tempfile.mkstemp(prefix=".wireproxy_", dir=parent)
    os.close(fd)

    try:
        with tarfile.open(tar_path, mode="r:gz") as tf:
            target_member = None
            for member in tf.getmembers():
                # Reject absolute paths, path traversal, symlinks, and non-regular files
                norm = member.name.replace("\\", "/")
                if norm.startswith("/") or ".." in norm.split("/"):
                    continue
                if not member.isfile():
                    continue
                if os.path.basename(norm) == "wireproxy":
                    target_member = member
                    break

            if target_member is None:
                raise RuntimeError("wireproxy binary not found inside downloaded tarball")
            if target_member.size <= 0 or target_member.size > _MAX_DOWNLOAD_BYTES:
                raise RuntimeError(f"Unexpected wireproxy binary size: {target_member.size}")

            extracted = tf.extractfile(target_member)
            if extracted is None:
                raise RuntimeError("Could not extract wireproxy binary from tarball")
            with extracted, open(tmp_bin, "wb") as out_fh:
                shutil.copyfileobj(extracted, out_fh, length=65536)

        _verify_elf_binary(tmp_bin, "wireproxy")
        _chmod_safe(tmp_bin, 0o755)
        os.replace(tmp_bin, dest_bin_path)
    except Exception:
        try:
            os.unlink(tmp_bin)
        except OSError:
            pass
        raise


async def _ensure_wireproxy_binary(
    session: aiohttp.ClientSession,
    cfg: WarpConfig,
    arch: str,
    force: bool,
    on_progress: Optional[ProgressCallback],
) -> str:
    if not force and os.path.isfile(cfg.wireproxy_bin) and os.access(cfg.wireproxy_bin, os.X_OK):
        rc, _, _ = await run_exec(cfg.wireproxy_bin, "-v", timeout=10.0)
        if rc == 0:
            await _emit_progress(on_progress, "Reusing existing wireproxy binary.")
            return cfg.wireproxy_bin

    await _emit_progress(on_progress, f"Downloading wireproxy (linux/{arch})...")
    dl_url, tag = await _resolve_release_asset(
        session,
        WIREPROXY_REPOS,
        matcher=lambda name: _match_wireproxy_asset(name, arch),
        fallback_filename_for_tag=lambda _tag: f"wireproxy_linux_{arch}.tar.gz",
    )

    os.makedirs(cfg.tmp_dir, exist_ok=True)
    tar_path = os.path.join(cfg.tmp_dir, f"wireproxy_linux_{arch}.tar.gz")
    try:
        await _download_to_path(session, dl_url, tar_path)
        _safe_extract_wireproxy_tarball(tar_path, cfg.wireproxy_bin)
    finally:
        try:
            os.unlink(tar_path)
        except OSError:
            pass

    rc, stdout, stderr = await run_exec(cfg.wireproxy_bin, "-v", timeout=10.0)
    if rc != 0:
        raise RuntimeError(f"wireproxy self-check failed: {stderr or stdout or f'exit {rc}'}")
    await _emit_progress(on_progress, f"Installed wireproxy ({tag}).")
    return cfg.wireproxy_bin


async def _ensure_wgcf_binary(
    session: aiohttp.ClientSession,
    cfg: WarpConfig,
    arch: str,
    force: bool,
    on_progress: Optional[ProgressCallback],
) -> str:
    wgcf_cwd = cfg.data_dir if os.path.isdir(cfg.data_dir) else None
    if not force and os.path.isfile(cfg.wgcf_bin) and os.access(cfg.wgcf_bin, os.X_OK):
        rc, _, _ = await run_exec(cfg.wgcf_bin, "--help", cwd=wgcf_cwd, timeout=10.0)
        if rc == 0:
            await _emit_progress(on_progress, "Reusing existing wgcf binary.")
            return cfg.wgcf_bin

    await _emit_progress(on_progress, f"Downloading wgcf (linux/{arch})...")
    dl_url, tag = await _resolve_release_asset(
        session,
        WGCF_REPOS,
        matcher=lambda name: _match_wgcf_asset(name, arch),
        fallback_filename_for_tag=lambda t: f"wgcf_{t.lstrip('v')}_linux_{arch}",
    )

    os.makedirs(cfg.bin_dir, exist_ok=True)
    fd, tmp_bin = tempfile.mkstemp(prefix=".wgcf_", dir=cfg.bin_dir)
    os.close(fd)
    try:
        await _download_to_path(session, dl_url, tmp_bin)
        _verify_elf_binary(tmp_bin, "wgcf")
        _chmod_safe(tmp_bin, 0o755)
        os.replace(tmp_bin, cfg.wgcf_bin)
    except Exception:
        try:
            os.unlink(tmp_bin)
        except OSError:
            pass
        raise

    wgcf_cwd = cfg.data_dir if os.path.isdir(cfg.data_dir) else None
    rc, stdout, stderr = await run_exec(cfg.wgcf_bin, "--help", cwd=wgcf_cwd, timeout=10.0)
    if rc != 0:
        raise RuntimeError(f"wgcf self-check failed: {stderr or stdout or f'exit {rc}'}")
    await _emit_progress(on_progress, f"Installed wgcf ({tag}).")
    return cfg.wgcf_bin


# ---------------------------------------------------------------------------
# WARP registration & config validation
# ---------------------------------------------------------------------------

async def _register_warp_account(
    cfg: WarpConfig,
    wgcf_bin: str,
    force: bool,
    on_progress: Optional[ProgressCallback],
) -> None:
    os.makedirs(cfg.data_dir, exist_ok=True)
    _chmod_safe(cfg.data_dir, 0o700)

    if force:
        for stale in (cfg.wgcf_account_path, cfg.wgcf_profile_path):
            try:
                os.unlink(stale)
            except OSError:
                pass

    if os.path.isfile(cfg.wgcf_account_path):
        await _emit_progress(on_progress, "Reusing existing Cloudflare WARP account credentials.")
    else:
        await _emit_progress(on_progress, "Registering new Cloudflare WARP account...")
        last_err = ""
        for attempt in range(1, 4):
            rc, stdout, stderr = await run_exec(
                wgcf_bin,
                "register",
                "--accept-tos",
                cwd=cfg.data_dir,
                timeout=45.0,
            )
            if rc == 0 and os.path.isfile(cfg.wgcf_account_path):
                _chmod_safe(cfg.wgcf_account_path, 0o600)
                break
            last_err = stderr or stdout or f"wgcf register exited with code {rc}"
            # Remove any partial account file left by a mid-register API error so
            # wgcf's EnsureNoExistingAccount() check does not block retries.
            if os.path.isfile(cfg.wgcf_account_path):
                try:
                    os.unlink(cfg.wgcf_account_path)
                except OSError:
                    pass
            if attempt < 3:
                await asyncio.sleep(2.0 * attempt)
        else:
            raise RuntimeError(f"Cloudflare WARP registration failed: {last_err}")

    if os.path.isfile(cfg.wgcf_profile_path) and not force:
        try:
            with open(cfg.wgcf_profile_path, "r", encoding="utf-8") as fh:
                parsed = parse_wgcf_profile(fh.read())
            # Validate it contains required keys before reusing
            build_wg_profile(parsed, mtu=cfg.mtu, keepalive=cfg.keepalive, endpoint=cfg.endpoint)
            _chmod_safe(cfg.wgcf_profile_path, 0o600)
            return
        except Exception:
            pass

    await _emit_progress(on_progress, "Generating Cloudflare WARP WireGuard profile...")
    last_gen_err = ""
    for attempt in range(1, 4):
        rc, stdout, stderr = await run_exec(
            wgcf_bin,
            "generate",
            cwd=cfg.data_dir,
            timeout=45.0,
        )
        if rc == 0 and os.path.isfile(cfg.wgcf_profile_path):
            _chmod_safe(cfg.wgcf_profile_path, 0o600)
            break
        last_gen_err = stderr or stdout or f"exit code {rc}"
        if attempt < 3:
            await asyncio.sleep(2.0 * attempt)
    else:
        raise RuntimeError(f"wgcf generate failed: {last_gen_err}")


def _read_text_if_exists(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


async def _write_and_validate_configs(
    cfg: WarpConfig,
    wireproxy_bin: str,
    *,
    endpoint_override: Optional[str] = None,
    previous_snapshot: Optional[tuple[Optional[str], Optional[str]]] = None,
) -> bool:
    """Write normalized wgcf-profile.conf + wireproxy.conf, validate with ``-n``.

    Returns True if either config file changed relative to *previous_snapshot*.
    """
    prev_wg, prev_wp = (
        previous_snapshot
        if previous_snapshot is not None
        else (_read_text_if_exists(cfg.wgcf_profile_path), _read_text_if_exists(cfg.wireproxy_conf_path))
    )

    with open(cfg.wgcf_profile_path, "r", encoding="utf-8") as fh:
        parsed = parse_wgcf_profile(fh.read())

    endpoint = endpoint_override or cfg.endpoint
    last_error = ""

    for keepalive_key in ("PersistentKeepalive", "KeepAlive"):
        wg_content = build_wg_profile(
            parsed,
            mtu=cfg.mtu,
            keepalive=cfg.keepalive,
            endpoint=endpoint,
            keepalive_key=keepalive_key,
        )
        _write_file_atomic(cfg.wgcf_profile_path, wg_content, mode=0o600)

        wp_content = build_wireproxy_conf(cfg, wg_profile_path=cfg.wgcf_profile_path)
        _write_file_atomic(cfg.wireproxy_conf_path, wp_content, mode=0o600)

        rc, stdout, stderr = await run_exec(
            wireproxy_bin,
            "-c",
            cfg.wireproxy_conf_path,
            "-n",
            timeout=15.0,
        )
        if rc == 0:
            return (wg_content != prev_wg) or (wp_content != prev_wp)
        last_error = stderr or stdout or f"wireproxy -n exited with code {rc}"
        if "keepalive" not in last_error.lower():
            break

    raise RuntimeError(f"wireproxy configuration validation failed: {last_error}")


# ---------------------------------------------------------------------------
# Service deployment (systemd with automatic managed user-space fallback)
# ---------------------------------------------------------------------------

def _resolve_systemd_run_user(cfg: WarpConfig) -> Optional[str]:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        return None
    if geteuid() != 0:
        try:
            return getpass.getuser()
        except Exception:
            return None

    # Running as root: if data_dir is owned by a dedicated non-root user (e.g. botuser),
    # run the service as that user so permissions stay aligned with the bot service.
    try:
        import pwd

        st = os.stat(cfg.data_dir)
        if st.st_uid != 0:
            return pwd.getpwuid(st.st_uid).pw_name
    except Exception:
        pass
    return None


def _chown_tree_for_user(path: str, username: str) -> None:
    """When running as root, align ownership of *path* with *username*."""
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None or geteuid() != 0 or not username:
        return
    try:
        import pwd

        pw = pwd.getpwnam(username)
        uid, gid = pw.pw_uid, pw.pw_gid
        os.chown(path, uid, gid)
        for root_dir, dirs, files in os.walk(path):
            for name in dirs:
                os.chown(os.path.join(root_dir, name), uid, gid)
            for name in files:
                os.chown(os.path.join(root_dir, name), uid, gid)
    except Exception as exc:
        logger.warning("Failed to chown %s to %s: %s", path, username, exc)


async def _deploy_and_start_service(
    cfg: WarpConfig,
    wireproxy_bin: str,
    on_progress: Optional[ProgressCallback],
    *,
    config_changed: bool = False,
) -> str:
    can_manage, needs_sudo = await can_manage_systemd(cfg)
    if can_manage:
        await _emit_progress(on_progress, f"Configuring systemd service ({cfg.service_name})...")
        await stop_managed_wireproxy(cfg)

        run_user = _resolve_systemd_run_user(cfg)
        if run_user:
            _chown_tree_for_user(cfg.data_dir, run_user)
        unit_content = build_systemd_unit(cfg, binary_path=wireproxy_bin, run_user=run_user)

        if needs_sudo:
            rc, _, stderr = await run_exec(
                "sudo",
                "-n",
                "tee",
                cfg.systemd_unit_path,
                stdin_data=unit_content.encode("utf-8"),
                timeout=15.0,
            )
            if rc != 0:
                logger.warning("Failed to write systemd unit via sudo (%s); using managed mode", stderr)
            else:
                prefix = ["sudo", "-n", "systemctl"]
                rc1, _, err1 = await run_exec(*prefix, "daemon-reload", timeout=20.0)
                rc2, _, err2 = await run_exec(*prefix, "enable", cfg.service_name, timeout=20.0)
                rc3, _, err3 = await run_exec(*prefix, "restart", cfg.service_name, timeout=20.0)
                if rc1 == 0 and rc2 == 0 and rc3 == 0:
                    return "systemd"
                logger.warning(
                    "systemctl setup failed (%s / %s / %s); falling back to managed mode",
                    err1,
                    err2,
                    err3,
                )
        else:
            _write_file_atomic(cfg.systemd_unit_path, unit_content, mode=0o644)
            rc1, _, err1 = await run_exec("systemctl", "daemon-reload", timeout=20.0)
            rc2, _, err2 = await run_exec("systemctl", "enable", cfg.service_name, timeout=20.0)
            rc3, _, err3 = await run_exec("systemctl", "restart", cfg.service_name, timeout=20.0)
            if rc1 == 0 and rc2 == 0 and rc3 == 0:
                return "systemd"
            logger.warning(
                "systemctl setup failed (%s / %s / %s); falling back to managed mode",
                err1,
                err2,
                err3,
            )

    # Unprivileged / sandboxed environment (cannot manage systemd units).
    # Avoid port collision if systemd or an external wireproxy already holds the ports.
    systemd_running = systemd_unit_exists(cfg) and await is_systemd_active(cfg)
    ports_up = await check_proxy_reachable(
        require_http=True,
        require_socks5=True,
        timeout=1.5,
        cfg=cfg,
    )
    if systemd_running:
        if config_changed:
            raise RuntimeError(
                f"WARP configuration changed, but systemd service '{cfg.service_name}' is already "
                f"active and cannot be restarted from this unprivileged sandbox. "
                f"Run on your VPS: sudo systemctl restart {cfg.service_name}"
            )
        if ports_up:
            await _emit_progress(
                on_progress,
                f"Systemd service ({cfg.service_name}) is already active and listening; reusing it.",
            )
            return "systemd"
        raise RuntimeError(
            f"Systemd service '{cfg.service_name}' is active, but proxy ports are not listening "
            f"and cannot be restarted without privileges. "
            f"Run on your VPS: sudo systemctl restart {cfg.service_name}"
        )

    if ports_up and not is_managed_running(cfg):
        if config_changed:
            raise RuntimeError(
                f"WARP configuration changed, but ports ({cfg.socks5_bind}, {cfg.http_bind}) are "
                "already held by an external wireproxy process. Restart it to apply the changes."
            )
        await _emit_progress(on_progress, "External wireproxy is already listening; reusing it.")
        return "external"

    await _emit_progress(
        on_progress,
        "Starting wireproxy in managed user-space mode (no root/systemd privileges required)...",
    )
    ok, detail = await start_managed_wireproxy(
        cfg,
        binary=wireproxy_bin,
        conf_path=cfg.wireproxy_conf_path,
    )
    if not ok:
        raise RuntimeError(detail)
    return "managed"


async def _verify_warp_trace_with_retries(
    cfg: WarpConfig,
    attempts: int = 3,
    delay: float = 2.0,
) -> WarpTraceResult:
    last = WarpTraceResult(ok=False, warp_on=False, error="trace did not run")
    for idx in range(attempts):
        last = await check_cloudflare_trace(cfg)
        if last.ok and last.warp_on:
            return last
        if idx + 1 < attempts:
            await asyncio.sleep(delay)
    return last


# ---------------------------------------------------------------------------
# Public installer entry point
# ---------------------------------------------------------------------------

async def install_warp(
    on_progress: Optional[ProgressCallback] = None,
    force: bool = False,
    cfg: Optional[WarpConfig] = None,
) -> WarpActionResult:
    """Install and configure Cloudflare WARP + ``wireproxy`` on a Linux VPS.

    Idempotent by default (reuses existing binaries and WARP credentials unless
    ``force=True``). Supports both systemd service installation (when privileged)
    and bot-managed user-space fallback (inside hardened sandboxes such as
    ``deploy/install.sh``'s ``ProtectSystem=strict`` + ``NoNewPrivileges=true``).
    """
    cfg = cfg or get_warp_config()
    lock = _get_install_lock()
    if lock.locked():
        msg = "A WARP wireproxy installation is already in progress. Please wait for it to finish."
        return WarpActionResult(
            ok=False,
            enabled=is_proxy_enabled(cfg),
            message=msg,
            error=msg,
            socks5_url=cfg.socks5_url,
            http_url=cfg.http_url,
        )

    async with lock:
        try:
            os_name, arch = detect_platform()
        except WarpUnsupportedPlatformError as exc:
            status = await get_warp_status(check_trace=False, cfg=cfg)
            return WarpActionResult(
                ok=False,
                enabled=is_proxy_enabled(cfg),
                message=str(exc),
                error=str(exc),
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
            )

        await _emit_progress(
            on_progress,
            f"Detected platform: {os_name}/{arch}. Preparing WARP directory ({cfg.data_dir})...",
        )

        try:
            os.makedirs(cfg.bin_dir, exist_ok=True)
            _chmod_safe(cfg.data_dir, 0o700)

            previous_snapshot = (
                _read_text_if_exists(cfg.wgcf_profile_path),
                _read_text_if_exists(cfg.wireproxy_conf_path),
            )

            timeout = aiohttp.ClientTimeout(total=120, connect=20)
            headers = {"User-Agent": "telegram-forwarder-bot-warp-installer"}
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                trust_env=False,
            ) as session:
                wireproxy_bin = await _ensure_wireproxy_binary(session, cfg, arch, force, on_progress)
                wgcf_bin = await _ensure_wgcf_binary(session, cfg, arch, force, on_progress)

            await _register_warp_account(cfg, wgcf_bin, force, on_progress)

            await _emit_progress(
                on_progress,
                f"Writing localhost-only wireproxy.conf (SOCKS5 {cfg.socks5_bind}, HTTP {cfg.http_bind})...",
            )
            config_changed = await _write_and_validate_configs(
                cfg,
                wireproxy_bin,
                previous_snapshot=previous_snapshot,
            )

            mode = await _deploy_and_start_service(
                cfg,
                wireproxy_bin,
                on_progress,
                config_changed=config_changed or force,
            )

            await _emit_progress(on_progress, "Waiting for local SOCKS5 and HTTP listeners...")
            socks_ok, http_ok = await wait_for_ports(cfg, timeout=10.0)
            if not (socks_ok and http_ok):
                raise RuntimeError(
                    f"wireproxy started ({mode}), but local ports are not listening "
                    f"(SOCKS5 {cfg.socks5_bind}={'up' if socks_ok else 'down'}, "
                    f"HTTP {cfg.http_bind}={'up' if http_ok else 'down'})."
                )

            await _emit_progress(on_progress, "Verifying Cloudflare WARP connectivity (cdn-cgi/trace)...")
            trace = await _verify_warp_trace_with_retries(cfg, attempts=3, delay=2.0)

            # Fallback to direct IPv4 WARP endpoint if hostname resolution / IPv6 failed on VPS
            if (not trace.ok or not trace.warp_on) and cfg.endpoint == DEFAULT_WARP_ENDPOINT:
                await _emit_progress(
                    on_progress,
                    f"Initial trace failed; retrying with IPv4 WARP endpoint ({FALLBACK_WARP_IPV4_ENDPOINT})...",
                )
                fallback_changed = await _write_and_validate_configs(
                    cfg,
                    wireproxy_bin,
                    endpoint_override=FALLBACK_WARP_IPV4_ENDPOINT,
                )
                mode = await _deploy_and_start_service(
                    cfg,
                    wireproxy_bin,
                    on_progress,
                    config_changed=fallback_changed,
                )
                fb_socks_ok, fb_http_ok = await wait_for_ports(cfg, timeout=10.0)
                if not (fb_socks_ok and fb_http_ok):
                    raise RuntimeError(
                        f"wireproxy restarted with IPv4 endpoint ({mode}), but local ports are not listening "
                        f"(SOCKS5 {cfg.socks5_bind}={'up' if fb_socks_ok else 'down'}, "
                        f"HTTP {cfg.http_bind}={'up' if fb_http_ok else 'down'})."
                    )
                trace = await _verify_warp_trace_with_retries(cfg, attempts=3, delay=2.0)

            status = await get_warp_status(check_trace=False, cfg=cfg)
            # Attach the verified trace to status snapshot
            status = type(status)(
                installed=status.installed,
                wireproxy_binary=status.wireproxy_binary,
                config_exists=status.config_exists,
                service_active=status.service_active,
                service_manager=status.service_manager,
                socks5_listening=status.socks5_listening,
                http_listening=status.http_listening,
                ports_listening=status.ports_listening,
                proxy_enabled=status.proxy_enabled,
                socks5_url=status.socks5_url,
                http_url=status.http_url,
                warp_on=trace.warp_on,
                warp_state=trace.warp_state,
                exit_ip=trace.exit_ip,
                location=trace.location,
                colo=trace.colo,
                platform=status.platform,
                detail=status.detail,
                error=trace.error if not (trace.ok and trace.warp_on) else None,
                affected_features=status.affected_features,
            )

            if not trace.ok or not trace.warp_on:
                err_msg = (
                    f"wireproxy is installed and listening ({mode}), "
                    f"but WARP trace check failed: {trace.error or f'warp={trace.warp_state}'}. "
                    "Ensure outbound UDP port 2408 is allowed on your VPS firewall."
                )
                return WarpActionResult(
                    ok=False,
                    enabled=status.proxy_enabled,
                    message=err_msg,
                    error=err_msg,
                    socks5_url=cfg.socks5_url,
                    http_url=cfg.http_url,
                    status=status,
                    trace=trace,
                )

            loc_part = f" ({trace.location})" if trace.location else ""
            ip_part = f"{trace.exit_ip}{loc_part}" if trace.exit_ip else "verified"
            summary = (
                f"WARP wireproxy installed and verified ({mode} mode).\n"
                f"WARP: {trace.warp_state} | Exit IP: {ip_part}\n"
                f"SOCKS5: {cfg.socks5_url} | HTTP: {cfg.http_url}\n"
                f"Bot proxy routing is currently {'ON' if status.proxy_enabled else 'OFF'} "
                "(use /warp_on to enable routing)."
            )
            await _emit_progress(on_progress, "WARP wireproxy installation complete and verified!")
            return WarpActionResult(
                ok=True,
                enabled=status.proxy_enabled,
                message=summary,
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
                trace=trace,
            )

        except Exception as exc:
            logger.exception("WARP wireproxy installation failed")
            err_msg = str(exc) or type(exc).__name__
            status = await get_warp_status(check_trace=False, cfg=cfg)
            return WarpActionResult(
                ok=False,
                enabled=status.proxy_enabled,
                message=f"WARP wireproxy installation failed: {err_msg}",
                error=err_msg,
                socks5_url=cfg.socks5_url,
                http_url=cfg.http_url,
                status=status,
            )
