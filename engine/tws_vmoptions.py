"""Portable, nonfatal installer for the Start Multi Port TWS heap cap.

The caller supplies the TWS executable discovered on the current machine.
This module derives its sibling ``tws.vmoptions`` for that invocation only; it
does not persist a machine path in project state.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
from pathlib import Path
import stat
import tempfile


VMOPTIONS_NAME = "tws.vmoptions"
BACKUP_SUFFIX = ".ema-backup-"
BACKUP_PREFIX = f"{VMOPTIONS_NAME}{BACKUP_SUFFIX}"
HEAP_CAP_MB = 1024
HEAP_CAP_OPTION = f"-Xmx{HEAP_CAP_MB}m"
MANAGED_BEGIN = "# EMA-FLEET-CAP BEGIN"
MANAGED_OPTION = HEAP_CAP_OPTION
MANAGED_END = "# EMA-FLEET-CAP END"
MAX_VMOPTIONS_BYTES = 2 * 1024 * 1024
MAX_BACKUP_CANDIDATES = 64


@dataclass(frozen=True)
class CapInstallResult:
    """Path-free result safe to carry into a durable fleet transcript."""

    action: str
    verified: bool
    installed: bool = False
    backup_created: bool = False
    backup_name: str = ""
    option: str = MANAGED_OPTION
    detail: str = ""

    @property
    def outcome(self) -> str:
        return self.action

    @property
    def changed(self) -> bool:
        return self.installed

    def evidence(self) -> dict:
        """Return the controlled, path-free transcript payload."""
        return {
            "outcome": self.action,
            "verified": self.verified,
            "changed": self.changed,
            "cap_option": self.option,
            "backup_name": self.backup_name or None,
            "detail": self.detail,
        }

    @property
    def progress_message(self) -> str:
        if self.action == "installed":
            return "TWS fleet memory cap installed and verified."
        if self.action == "verified":
            return "TWS fleet memory cap already installed and verified."
        return (
            "TWS fleet memory cap was not verified; continuing with the "
            f"conservative AUTO policy ({self.detail or 'nonfatal warning'})."
        )


def _warning(detail: str) -> CapInstallResult:
    return CapInstallResult(
        action="warning", verified=False,
        detail=" ".join(str(detail or "nonfatal warning").split())[:160],
    )


def _is_safe_regular_file(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    if not stat.S_ISREG(info.st_mode) or path.is_symlink():
        return False
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0) or 0)
    return not (reparse and attributes & reparse)


def vmoptions_for_executable(executable) -> Path | None:
    """Return a runtime-derived sibling vmoptions path, or ``None``."""
    try:
        tws_exe = Path(executable).expanduser()
    except (TypeError, ValueError, OSError):
        return None
    if (tws_exe.name.casefold() != "tws.exe"
            or not _is_safe_regular_file(tws_exe)):
        return None
    return tws_exe.parent / VMOPTIONS_NAME


def _newline_for(payload: bytes) -> bytes:
    if b"\r\n" in payload:
        return b"\r\n"
    if b"\n" in payload:
        return b"\n"
    if b"\r" in payload:
        return b"\r"
    return b"\n"


def _managed_state(payload: bytes) -> str:
    """Return ``absent``, ``verified``, or ``invalid`` for the fenced block."""
    begin = MANAGED_BEGIN.encode("ascii")
    option = MANAGED_OPTION.encode("ascii")
    end = MANAGED_END.encode("ascii")
    lines = payload.splitlines()
    normalized = [line.strip() for line in lines]
    begins = [index for index, line in enumerate(normalized) if line == begin]
    ends = [index for index, line in enumerate(normalized) if line == end]
    if not begins and not ends:
        return "absent"
    if len(begins) != 1 or len(ends) != 1 or begins[0] >= ends[0]:
        return "invalid"
    exact = (
        lines[begins[0]] == begin
        and lines[ends[0]] == end
        and lines[begins[0] + 1:ends[0]] == [option]
    )
    return "verified" if exact else "invalid"


def _append_managed_block(payload: bytes) -> bytes:
    newline = _newline_for(payload)
    prefix = payload
    if prefix and not prefix.endswith((b"\r\n", b"\n", b"\r")):
        prefix += newline
    block = newline.join((
        MANAGED_BEGIN.encode("ascii"),
        MANAGED_OPTION.encode("ascii"),
        MANAGED_END.encode("ascii"),
        b"",
    ))
    return prefix + block


def _existing_backup(target: Path) -> Path | None:
    try:
        candidates = sorted(
            (
                child for child in target.parent.iterdir()
                if child.name.startswith(BACKUP_PREFIX)
            ),
            key=lambda child: child.name,
        )
    except OSError as exc:
        raise OSError("backup directory could not be inspected") from exc
    if len(candidates) > MAX_BACKUP_CANDIDATES:
        raise OSError("too many managed backup candidates")
    for candidate in candidates:
        suffix = candidate.name[len(BACKUP_PREFIX):]
        if (len(suffix) != 8 or not suffix.isdigit()
                or not _is_safe_regular_file(candidate)):
            raise OSError("managed backup candidate is not a safe regular file")
    return candidates[0] if candidates else None


def _create_backup(target: Path, payload: bytes, stamp: str) -> tuple[Path, bool]:
    existing = _existing_backup(target)
    if existing is not None:
        return existing, False
    backup = target.with_name(f"{BACKUP_PREFIX}{stamp}")
    mode = target.stat().st_mode & 0o777
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(backup, flags, mode or 0o600)
    except FileExistsError:
        if _is_safe_regular_file(backup):
            return backup, False
        raise
    complete = False
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        complete = True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not complete:
            try:
                backup.unlink(missing_ok=True)
            except OSError:
                pass
    return backup, True


def _write_backup_once(
        target: Path, payload: bytes, stamp: str) -> tuple[Path | None, str | None]:
    """Create/reuse the one backup and convert filesystem errors to detail."""
    try:
        backup, _created = _create_backup(target, payload, stamp)
        return backup, None
    except OSError:
        return None, "original vmoptions backup could not be created"


def _atomic_replace(target: Path, payload: bytes) -> None:
    mode = target.stat().st_mode & 0o777
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.ema-cap-", suffix=".tmp",
        dir=str(target.parent),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, target)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def ensure_memory_cap(executable, *, today=None) -> CapInstallResult:
    """Install or verify the managed heap cap without ever blocking startup.

    All expected discovery/filesystem failures become a controlled warning.
    Existing vmoptions bytes are preserved exactly before the appended block.
    """
    target = vmoptions_for_executable(executable)
    if target is None:
        return _warning("runtime TWS executable was not a safe regular file")
    if not _is_safe_regular_file(target):
        return _warning("tws.vmoptions was missing or not a safe regular file")
    try:
        info = target.stat()
        if info.st_size < 0 or info.st_size > MAX_VMOPTIONS_BYTES:
            return _warning("tws.vmoptions exceeded the bounded size limit")
        original = target.read_bytes()
        if len(original) > MAX_VMOPTIONS_BYTES:
            return _warning("tws.vmoptions exceeded the bounded size limit")
        if b"\x00" in original:
            return _warning("tws.vmoptions was not a supported text file")
        state = _managed_state(original)
        if state == "verified":
            return CapInstallResult(
                action="verified", verified=True,
                detail="managed block already exact",
            )
        if state == "invalid":
            return _warning("managed block was partial, duplicated, or mismatched")

        stamp_value = today or date.today()
        stamp = (
            stamp_value.strftime("%Y%m%d")
            if hasattr(stamp_value, "strftime")
            else str(stamp_value)
        )
        if not (len(stamp) == 8 and stamp.isdigit()):
            return _warning("backup date was invalid")
        backup_existed = _existing_backup(target) is not None
        backup, backup_error = _write_backup_once(target, original, stamp)
        if backup is None:
            return _warning(backup_error or "original backup was unavailable")
        created = not backup_existed
        if target.read_bytes() != original:
            return _warning("tws.vmoptions changed during installation")
        updated = _append_managed_block(original)
        try:
            _atomic_replace(target, updated)
        except OSError:
            return CapInstallResult(
                action="warning", verified=False,
                backup_created=created, backup_name=backup.name,
                detail="vmoptions write failed; original backup retained",
            )
        if (_managed_state(target.read_bytes()) != "verified"
                or not target.read_bytes().startswith(original)):
            return _warning("managed block could not be verified after installation")
        return CapInstallResult(
            action="installed", verified=True, installed=True,
            backup_created=created, backup_name=backup.name,
            detail="managed block appended and verified",
        )
    except (OSError, ValueError, TypeError, OverflowError):
        return _warning("backup or vmoptions write failed")
    except Exception:  # noqa: BLE001 - cap optimization must never gate startup
        return _warning("unexpected installer failure")


__all__ = [
    "BACKUP_PREFIX", "BACKUP_SUFFIX", "CapInstallResult", "HEAP_CAP_MB",
    "HEAP_CAP_OPTION", "MANAGED_BEGIN", "MANAGED_END", "MANAGED_OPTION",
    "MAX_VMOPTIONS_BYTES", "VMOPTIONS_NAME",
    "ensure_memory_cap", "vmoptions_for_executable",
]
