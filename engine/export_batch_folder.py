"""Pure destination-folder and bundle-layout handling for exports."""

from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path


MAX_COLLISIONS = 999
MAX_TOKEN = 32
DATA_DIR_NAME = "Data"
HEALTH_REPORT_NAME = "health_report.json"
_UNSAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")
_SPACE_RE = re.compile(r"\s+")


class BatchFolderError(RuntimeError):
    """A batch destination cannot be created or safely cleaned up."""


def _clock(now=None):
    if now is None:
        value = dt.datetime.now().astimezone()
    elif isinstance(now, dt.datetime):
        value = now
    else:
        raise BatchFolderError("injected folder clock must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise BatchFolderError("injected folder clock must include a timezone")
    return value


def _count(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise BatchFolderError("ticker count must be a positive integer")
    return value


def safe_token(value, label="token"):
    """Return one short Windows-safe filename token."""
    text = _SPACE_RE.sub("-", str(value or "").strip())
    text = _UNSAFE_RE.sub("-", text).strip(" .-_")
    text = re.sub(r"-+", "-", text)[:MAX_TOKEN].rstrip(" .-_")
    if not text:
        raise BatchFolderError(f"{label} has no filesystem-safe characters")
    return text


def folder_name(ticker_count, interval, file_format, *, now=None):
    count = _count(ticker_count)
    stamp = _clock(now).strftime("%Y-%m-%d %H%M%S")
    iv = safe_token(interval, "interval")
    fmt = safe_token(file_format, "format").lower()
    noun = "ticker" if count == 1 else "tickers"
    return f"Export {stamp} - {count} {noun} - {iv} - {fmt}"


def create_batch_folder(parent, ticker_count, interval, file_format, *,
                        now=None, max_collisions=MAX_COLLISIONS):
    """Atomically create one never-reused direct child of ``parent``."""
    parent = Path(parent).resolve()
    if not parent.is_dir():
        raise BatchFolderError("export destination is not an existing directory")
    if (isinstance(max_collisions, bool) or not isinstance(max_collisions, int)
            or max_collisions < 1 or max_collisions > MAX_COLLISIONS):
        raise BatchFolderError("invalid folder collision limit")
    base = folder_name(
        ticker_count, interval, file_format, now=now)
    for ordinal in range(1, max_collisions + 1):
        suffix = "" if ordinal == 1 else f" ({ordinal})"
        target = parent / f"{base}{suffix}"
        try:
            target.mkdir(exist_ok=False)
            return target
        except FileExistsError:
            continue
        except OSError as exc:
            raise BatchFolderError(
                f"cannot create export batch folder: {type(exc).__name__}") from exc
    raise BatchFolderError("could not allocate a unique export batch folder")


def prepare_destination(parent, ticker_count, interval, file_format, *,
                        now=None):
    """Create and return one stamped outer export folder for every count."""
    count = _count(ticker_count)
    parent = Path(parent).resolve()
    if not parent.is_dir():
        raise BatchFolderError("export destination is not an existing directory")
    return create_batch_folder(
        parent, count, interval, file_format, now=now), True


def bundle_paths(batch_dir, ticker_count):
    """Return ``(data_dir, report_path)`` for one created outer folder.

    Multi-ticker data lives under ``Data/`` so the delivered health report is
    visible beside that inner folder.  A single-ticker bundle stays flat.
    This function defines paths only; the orchestrator owns creation/writes.
    """
    count = _count(ticker_count)
    batch = Path(batch_dir).resolve()
    if not batch.is_dir():
        raise BatchFolderError("export batch folder is not an existing directory")
    data_dir = batch if count == 1 else batch / DATA_DIR_NAME
    return data_dir, batch / HEALTH_REPORT_NAME


def remove_created_folder_if_empty(path):
    """Remove only an empty helper-shaped directory; never recurse."""
    path = Path(path)
    if not path.name.startswith("Export "):
        raise BatchFolderError("refusing to clean a non-batch folder")
    try:
        if path.is_symlink() or (
                hasattr(os.path, "isjunction") and os.path.isjunction(path)):
            raise BatchFolderError("refusing to clean a linked batch folder")
        if not path.is_dir():
            return False
        next(path.iterdir())
        return False
    except StopIteration:
        try:
            path.rmdir()
            return True
        except OSError:
            return False
    except BatchFolderError:
        raise
    except OSError as exc:
        raise BatchFolderError(
            f"cannot inspect export batch folder: {type(exc).__name__}") from exc


__all__ = [
    "BatchFolderError",
    "DATA_DIR_NAME",
    "HEALTH_REPORT_NAME",
    "bundle_paths",
    "create_batch_folder",
    "folder_name",
    "prepare_destination",
    "remove_created_folder_if_empty",
    "safe_token",
]
