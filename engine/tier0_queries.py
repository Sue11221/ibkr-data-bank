"""Proven-pure Tier 0 queries over the fixed local data bank.

This module exposes an allowlisted read surface only. It has no caller-selected
root, no refresh path, no network operation, no command dispatcher, and no file
write. Historical run artifacts are reachable only through a tracked catalog
whose basename, size, SHA-256, schema, and semantic views are all fixed.
"""

from __future__ import annotations

import codecs
import datetime as dt
import hashlib
import json
import math
import re
import stat
from pathlib import Path

import gap_evidence
import split_audit
import stock_storage as storage


SCHEMA_VERSION = 1
MODULE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_DIR.parent
STORAGE_ROOT = PROJECT_ROOT / storage.STORAGE_DIR_NAME
RUN_LOGS_ROOT = PROJECT_ROOT / "Run Logs"
SCRIPT_ARCHIVE_ROOT = (
    PROJECT_ROOT / "archive" / "_repair_scripts_archive" / "2026-07")
CATALOG_PATH = MODULE_DIR / "tier0_history_catalog.json"

HEALTH_REPORT = "_health_report.json"
COVERAGE_REPORT = "_coverage_audit.json"
GAP_REPORT = "_data_gaps.json"

MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
MAX_CATALOG_BYTES = 512 * 1024
MAX_SIDECAR_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_EVIDENCE_BYTES = 128 * 1024 * 1024
MAX_OUTPUT_BYTES = 256 * 1024
MAX_ROWS = 100
MAX_STRING = 1000
MAX_INTERVALS = 32
MAX_OBJECT_KEYS = 128
MAX_DEPTH = 8
MAX_CATALOG_RECORDS = 100
MAX_RECORD_TICKERS = 100
STREAM_CHUNK_BYTES = 64 * 1024

RUN_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")
KIND_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
BASENAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,159}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
MONTH_RE = re.compile(r"^[12]\d{3}-(?:0[1-9]|1[0-2])$")
ALLOWED_VIEWS = frozenset({
    "summary", "before_after", "verification", "fetch_totals", "seam_rows",
})
ARTIFACT_SCHEMAS = frozenset({
    "ticker_repair", "live_seam_probe", "phantom_fix", "availability_probe",
})
SENSITIVE_KEYS = frozenset({"traceback"})


class Tier0Error(RuntimeError):
    """Structured error returned by the JSON CLI without a traceback."""

    def __init__(self, code, message, *, exit_code=3, details=None):
        self.code = str(code)
        self.message = str(message)[:MAX_STRING]
        self.exit_code = int(exit_code)
        self.details = details if isinstance(details, dict) else None
        super().__init__(self.message)


def _validation(message, details=None):
    raise Tier0Error(
        "invalid_request", message, exit_code=2, details=details)


def _not_found(message):
    raise Tier0Error("not_found", message, exit_code=2)


def _canonical_ticker(value):
    if not isinstance(value, str) or not value or len(value) > 20:
        _validation("ticker must be a canonical 1-10 character symbol")
    if any(ord(ch) < 32 for ch in value):
        _validation("ticker contains a control character")
    try:
        canonical = storage.canonical_ticker(value)
    except Exception as exc:  # noqa: BLE001 - normalize storage error
        _validation(f"invalid ticker: {exc}")
    if value != canonical:
        _validation(
            f"ticker must use canonical storage spelling: {canonical}")
    return canonical


def _run_id(value):
    if not isinstance(value, str) or not RUN_ID_RE.fullmatch(value):
        _validation("run_id is not a canonical catalog identifier")
    return value


def _kind(value):
    if value is None:
        return None
    if not isinstance(value, str) or not KIND_RE.fullmatch(value):
        _validation("kind is not a canonical catalog kind")
    return value


def _interval(value):
    if value is None:
        return None
    if not isinstance(value, str) or not storage.INTERVAL_RE.fullmatch(value):
        _validation("interval is not a canonical storage interval")
    return value


def _pagination(cursor, limit, *, default_limit):
    if cursor is None:
        cursor = 0
    if limit is None:
        limit = default_limit
    if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
        _validation("cursor must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int):
        _validation("limit must be an integer")
    if not 1 <= limit <= MAX_ROWS:
        _validation(f"limit must be between 1 and {MAX_ROWS}")
    return cursor, limit


def _nonnegative_int(value, label, *, default=None):
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise Tier0Error(
            "invalid_evidence", f"{label} is not a non-negative integer")
    return value


def _optional_positive_int(value, label):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise Tier0Error(
            "invalid_evidence", f"{label} is not a positive integer")
    return value


def _page(items, cursor, limit):
    total = len(items)
    page = items[cursor:cursor + limit]
    end = cursor + len(page)
    return page, {
        "cursor": cursor,
        "limit": limit,
        "returned": len(page),
        "total": total,
        "next_cursor": end if end < total else None,
    }


def _is_reparse(info):
    attributes = int(getattr(info, "st_file_attributes", 0) or 0)
    marker = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return stat.S_ISLNK(info.st_mode) or bool(attributes & marker)


def _inside(path, root):
    try:
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
        return True
    except (OSError, ValueError):
        return False


def _path_label(path):
    path = Path(path)
    try:
        return path.resolve(strict=True).relative_to(
            PROJECT_ROOT.resolve(strict=True)).as_posix()
    except (OSError, ValueError):
        return path.name


def _file_state(path, *, allowed_root=None, direct_child=False):
    path = Path(path)
    try:
        info = path.lstat()
    except FileNotFoundError as exc:
        raise Tier0Error(
            "evidence_missing", f"required evidence is missing: {path.name}") from exc
    except OSError as exc:
        raise Tier0Error(
            "evidence_unavailable",
            f"cannot inspect evidence {path.name}: {exc}") from exc
    if _is_reparse(info):
        raise Tier0Error(
            "unsafe_path", f"reparse/symlink evidence is refused: {path.name}")
    if not stat.S_ISREG(info.st_mode):
        raise Tier0Error(
            "unsafe_path", f"evidence is not a regular file: {path.name}")
    if allowed_root is not None:
        root = Path(allowed_root)
        if not _inside(path, root):
            raise Tier0Error(
                "unsafe_path", f"evidence escapes its fixed root: {path.name}")
        if direct_child:
            try:
                if path.resolve(strict=True).parent != root.resolve(strict=True):
                    raise Tier0Error(
                        "unsafe_path",
                        f"evidence is not a direct child of its fixed root: {path.name}")
            except OSError as exc:
                raise Tier0Error(
                    "evidence_unavailable",
                    f"cannot resolve evidence {path.name}: {exc}") from exc
    return {
        "device": int(getattr(info, "st_dev", 0)),
        "inode": int(getattr(info, "st_ino", 0)),
        "size": int(info.st_size),
        "mtime_ns": int(info.st_mtime_ns),
        "ctime_ns": int(info.st_ctime_ns),
    }


def _state_token(state):
    return tuple(state[key] for key in (
        "device", "inode", "size", "mtime_ns", "ctime_ns"))


def _read_stable_bytes(path, *, max_bytes, expected_size=None,
                       expected_sha=None, allowed_root=None,
                       direct_child=False):
    path = Path(path)
    before = _file_state(
        path, allowed_root=allowed_root, direct_child=direct_child)
    if before["size"] > max_bytes:
        raise Tier0Error(
            "evidence_too_large",
            f"evidence exceeds the {max_bytes}-byte input cap: {path.name}")
    if expected_size is not None and before["size"] != int(expected_size):
        raise Tier0Error(
            "artifact_integrity",
            f"evidence size does not match the catalog: {path.name}")
    try:
        with path.open("rb") as handle:
            raw = handle.read(max_bytes + 1)
    except OSError as exc:
        raise Tier0Error(
            "evidence_unavailable",
            f"cannot read evidence {path.name}: {exc}") from exc
    if len(raw) > max_bytes:
        raise Tier0Error(
            "evidence_too_large",
            f"evidence exceeds the {max_bytes}-byte input cap: {path.name}")
    after = _file_state(
        path, allowed_root=allowed_root, direct_child=direct_child)
    if _state_token(before) != _state_token(after) or len(raw) != after["size"]:
        raise Tier0Error(
            "evidence_changed",
            f"evidence changed while it was read: {path.name}")
    digest = hashlib.sha256(raw).hexdigest()
    if expected_sha is not None and digest != str(expected_sha).lower():
        raise Tier0Error(
            "artifact_integrity",
            f"evidence SHA-256 does not match the catalog: {path.name}")
    evidence = {
        "_path": str(path.resolve(strict=True)),
        "_allowed_root": (
            str(Path(allowed_root).resolve(strict=True))
            if allowed_root is not None else None),
        "_direct_child": bool(direct_child),
        "path": _path_label(path),
        "size": after["size"],
        "mtime_ns": after["mtime_ns"],
        "sha256": digest,
        "state": _state_token(after),
    }
    return raw, evidence


def _read_stable_json(path, *, max_bytes, expected_size=None,
                      expected_sha=None, allowed_root=None,
                      direct_child=False):
    raw, evidence = _read_stable_bytes(
        path, max_bytes=max_bytes, expected_size=expected_size,
        expected_sha=expected_sha, allowed_root=allowed_root,
        direct_child=direct_child)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise Tier0Error(
            "invalid_evidence", f"invalid JSON evidence: {Path(path).name}") from exc
    if not isinstance(payload, dict):
        raise Tier0Error(
            "invalid_evidence", f"JSON evidence is not an object: {Path(path).name}")
    return payload, evidence


def _hash_stable_file(path, *, max_bytes, expected_size=None,
                      expected_sha=None, allowed_root=None,
                      direct_child=False):
    """Hash one fixed-root file without retaining its bytes in memory."""
    path = Path(path)
    before = _file_state(
        path, allowed_root=allowed_root, direct_child=direct_child)
    if before["size"] > max_bytes:
        raise Tier0Error(
            "evidence_too_large",
            f"evidence exceeds the {max_bytes}-byte input cap: {path.name}")
    if expected_size is not None and before["size"] != int(expected_size):
        raise Tier0Error(
            "artifact_integrity",
            f"evidence size does not match the catalog: {path.name}")
    digest = hashlib.sha256()
    count = 0
    try:
        with path.open("rb") as handle:
            while True:
                raw = handle.read(STREAM_CHUNK_BYTES)
                if not raw:
                    break
                count += len(raw)
                if count > max_bytes:
                    raise Tier0Error(
                        "evidence_too_large",
                        f"evidence exceeds the {max_bytes}-byte input cap: "
                        f"{path.name}")
                digest.update(raw)
    except Tier0Error:
        raise
    except OSError as exc:
        raise Tier0Error(
            "evidence_unavailable",
            f"cannot read evidence {path.name}: {exc}") from exc
    after = _file_state(
        path, allowed_root=allowed_root, direct_child=direct_child)
    if _state_token(before) != _state_token(after) or count != after["size"]:
        raise Tier0Error(
            "evidence_changed",
            f"evidence changed while it was read: {path.name}")
    sha256 = digest.hexdigest()
    if expected_sha is not None and sha256 != str(expected_sha).lower():
        raise Tier0Error(
            "artifact_integrity",
            f"evidence SHA-256 does not match the catalog: {path.name}")
    return {
        "_path": str(path.resolve(strict=True)),
        "_allowed_root": (
            str(Path(allowed_root).resolve(strict=True))
            if allowed_root is not None else None),
        "_direct_child": bool(direct_child),
        "path": _path_label(path),
        "size": after["size"],
        "mtime_ns": after["mtime_ns"],
        "sha256": sha256,
        "state": _state_token(after),
    }


class _JsonStream:
    """Small incremental JSON reader used for the growing gap sidecar."""

    def __init__(self, handle, *, basename, max_bytes):
        self.handle = handle
        self.basename = basename
        self.max_bytes = int(max_bytes)
        self.decoder = json.JSONDecoder()
        self.utf8 = codecs.getincrementaldecoder("utf-8")()
        self.buffer = ""
        self.position = 0
        self.eof = False
        self.byte_count = 0
        self.digest = hashlib.sha256()

    def _invalid(self):
        raise Tier0Error(
            "invalid_evidence", f"invalid JSON evidence: {self.basename}")

    def _compact(self):
        if self.position:
            self.buffer = self.buffer[self.position:]
            self.position = 0

    def _fill(self):
        if self.eof:
            return False
        self._compact()
        raw = self.handle.read(STREAM_CHUNK_BYTES)
        if raw:
            self.byte_count += len(raw)
            if self.byte_count > self.max_bytes:
                raise Tier0Error(
                    "evidence_too_large",
                    f"evidence exceeds the {self.max_bytes}-byte streamed cap: "
                    f"{self.basename}")
            self.digest.update(raw)
            try:
                self.buffer += self.utf8.decode(raw, final=False)
            except UnicodeDecodeError:
                self._invalid()
            return True
        try:
            self.buffer += self.utf8.decode(b"", final=True)
        except UnicodeDecodeError:
            self._invalid()
        self.eof = True
        return False

    def _skip_space(self):
        while True:
            while (self.position < len(self.buffer)
                   and self.buffer[self.position] in " \t\r\n"):
                self.position += 1
            if self.position < len(self.buffer) or self.eof:
                return
            self._fill()

    def consume(self, token):
        self._skip_space()
        if (self.position < len(self.buffer)
                and self.buffer[self.position] == token):
            self.position += 1
            return True
        return False

    def expect(self, token):
        if not self.consume(token):
            self._invalid()

    def value(self, *, max_chars):
        self._skip_space()
        self._compact()
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer)
            except json.JSONDecodeError:
                if self.eof:
                    self._invalid()
                if len(self.buffer) > max_chars:
                    raise Tier0Error(
                        "evidence_too_large",
                        f"JSON value exceeds the {max_chars}-character cap: "
                        f"{self.basename}")
                self._fill()
                continue
            if end > max_chars:
                raise Tier0Error(
                    "evidence_too_large",
                    f"JSON value exceeds the {max_chars}-character cap: "
                    f"{self.basename}")
            lookahead = end
            while (lookahead < len(self.buffer)
                   and self.buffer[lookahead] in " \t\r\n"):
                lookahead += 1
            if lookahead == len(self.buffer) and not self.eof:
                self._fill()
                continue
            self.position = end
            return value

    def finish(self):
        self._skip_space()
        if self.position != len(self.buffer):
            self._invalid()
        if not self.eof:
            self._fill()
            self._skip_space()
            if self.position != len(self.buffer) or not self.eof:
                self._invalid()
        return self.byte_count, self.digest.hexdigest()


def _stream_gap_sidecar(*, series_keys=None, ticker=None):
    """Stream the gap-report JSON and retain only the requested series."""
    if series_keys is not None and ticker is not None:
        raise AssertionError("gap-sidecar selectors are mutually exclusive")
    wanted = set(series_keys) if series_keys is not None else None
    prefix = f"{ticker} " if ticker is not None else None
    path = _fixed_child(STORAGE_ROOT, GAP_REPORT, suffix=".json")
    before = _file_state(
        path, allowed_root=STORAGE_ROOT, direct_child=True)
    stream_cap = int(gap_evidence.MAX_BYTES)
    if before["size"] > stream_cap:
        raise Tier0Error(
            "evidence_too_large",
            f"evidence exceeds the {stream_cap}-byte streamed cap: {path.name}")
    selected = {}
    root = {}
    root_keys = set()
    series_keys_seen = set()
    try:
        with path.open("rb") as handle:
            stream = _JsonStream(
                handle, basename=path.name, max_bytes=stream_cap)
            stream.expect("{")
            if not stream.consume("}"):
                while True:
                    key = stream.value(max_chars=MAX_STRING)
                    if not isinstance(key, str) or key in root_keys:
                        raise Tier0Error(
                            "invalid_evidence",
                            f"duplicate or invalid JSON object key: {path.name}")
                    root_keys.add(key)
                    if len(root_keys) > MAX_OBJECT_KEYS:
                        raise Tier0Error(
                            "invalid_evidence",
                            f"JSON evidence has too many root keys: {path.name}")
                    stream.expect(":")
                    if key == "series":
                        stream.expect("{")
                        if not stream.consume("}"):
                            while True:
                                series_key = stream.value(max_chars=MAX_STRING)
                                if (not isinstance(series_key, str)
                                        or series_key in series_keys_seen):
                                    raise Tier0Error(
                                        "invalid_evidence",
                                        "duplicate or invalid gap-series key: "
                                        f"{path.name}")
                                series_keys_seen.add(series_key)
                                if len(series_keys_seen) > gap_evidence.MAX_SERIES:
                                    raise Tier0Error(
                                        "invalid_evidence",
                                        "gap evidence series map is too large: "
                                        f"{path.name}")
                                stream.expect(":")
                                value = stream.value(
                                    max_chars=MAX_SIDECAR_BYTES)
                                retain = (
                                    wanted is None and prefix is None
                                    or (wanted is not None
                                        and series_key in wanted)
                                    or (prefix is not None
                                        and series_key.startswith(prefix))
                                )
                                if retain:
                                    selected[series_key] = value
                                if stream.consume("}"):
                                    break
                                stream.expect(",")
                        root["series"] = selected
                    else:
                        root[key] = stream.value(max_chars=MAX_SIDECAR_BYTES)
                    if stream.consume("}"):
                        break
                    stream.expect(",")
            byte_count, sha256 = stream.finish()
    except Tier0Error:
        raise
    except OSError as exc:
        raise Tier0Error(
            "evidence_unavailable",
            f"cannot read evidence {path.name}: {exc}") from exc
    after = _file_state(
        path, allowed_root=STORAGE_ROOT, direct_child=True)
    if (_state_token(before) != _state_token(after)
            or byte_count != after["size"]):
        raise Tier0Error(
            "evidence_changed",
            f"evidence changed while it was read: {path.name}")
    evidence = {
        "_path": str(path.resolve(strict=True)),
        "_allowed_root": str(STORAGE_ROOT.resolve(strict=True)),
        "_direct_child": True,
        "_stream_max_bytes": stream_cap,
        "path": _path_label(path),
        "size": after["size"],
        "mtime_ns": after["mtime_ns"],
        "sha256": sha256,
        "state": _state_token(after),
    }
    return root, evidence


def _verify_evidence(items):
    verified = []
    seen = set()
    for item in items:
        key = item["_path"].casefold()
        if key in seen:
            continue
        seen.add(key)
        if item.get("_stream_max_bytes") is not None:
            current = _hash_stable_file(
                item["_path"], max_bytes=int(item["_stream_max_bytes"]),
                expected_size=item["size"], expected_sha=item["sha256"],
                allowed_root=item.get("_allowed_root"),
                direct_child=item.get("_direct_child", False))
        else:
            _raw, current = _read_stable_bytes(
                item["_path"], max_bytes=min(
                    max(int(item["size"]), 1), MAX_EVIDENCE_BYTES),
                expected_size=item["size"], expected_sha=item["sha256"],
                allowed_root=item.get("_allowed_root"),
                direct_child=item.get("_direct_child", False))
        if current["state"] != item["state"]:
            raise Tier0Error(
                "evidence_changed",
                f"evidence changed before the query completed: {item['path']}")
        verified.append(current)
    return verified


def _evidence_summary(items):
    unique = {}
    for item in items:
        unique[item["path"]] = {
            "path": item["path"],
            "size": int(item["size"]),
            "mtime_ns": int(item["mtime_ns"]),
            "sha256": item["sha256"],
        }
    rows = [unique[key] for key in sorted(unique)]
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode(
        "utf-8")
    return {
        "fingerprint": hashlib.sha256(encoded).hexdigest(),
        "file_count": len(rows),
        "paths": [row["path"] for row in rows],
    }


def _safe_string(value):
    if value is None:
        return None
    text = str(value)
    try:
        root = str(PROJECT_ROOT.resolve(strict=True))
        text = text.replace(root, ".").replace(root.replace("\\", "/"), ".")
    except OSError:
        pass
    if re.match(r"^[A-Za-z]:[\\/]", text):
        text = "<absolute-path>"
    return text[:MAX_STRING]


def _sanitize(value, *, depth=0):
    if depth >= MAX_DEPTH:
        return "<depth-limit>"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return value.isoformat()
    if isinstance(value, Path):
        return _safe_string(value)
    if isinstance(value, str):
        return _safe_string(value)
    if isinstance(value, dict):
        out = {}
        keys = sorted(value.items(), key=lambda item: str(item[0]))
        for original, item_value in keys[:MAX_OBJECT_KEYS]:
            shown = str(original)
            if shown.lower() in SENSITIVE_KEYS:
                continue
            out[shown[:128]] = _sanitize(item_value, depth=depth + 1)
        if len(keys) > MAX_OBJECT_KEYS:
            out["_truncated_keys"] = len(keys) - MAX_OBJECT_KEYS
        return out
    if isinstance(value, (list, tuple)):
        out = [_sanitize(item, depth=depth + 1)
               for item in value[:MAX_ROWS]]
        if len(value) > MAX_ROWS:
            out.append({"_truncated_items": len(value) - MAX_ROWS})
        return out
    return _safe_string(value)


def _bounded(result):
    try:
        encoded = json.dumps(
            result, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise Tier0Error(
            "invalid_output", "query output is not valid bounded JSON") from exc
    if len(encoded) > MAX_OUTPUT_BYTES:
        raise Tier0Error(
            "output_too_large",
            f"query output exceeds the {MAX_OUTPUT_BYTES}-byte cap")
    return result


def _with_evidence(data, evidence):
    out = dict(data)
    out["evidence"] = _evidence_summary(evidence)
    return _bounded(out)


def _validate_sha(value, label):
    if not isinstance(value, str) or not SHA_RE.fullmatch(value):
        raise Tier0Error("invalid_catalog", f"invalid {label} SHA-256")
    return value


def _validate_basename(value, label, suffix):
    if (not isinstance(value, str) or not BASENAME_RE.fullmatch(value)
            or Path(value).name != value or not value.endswith(suffix)):
        raise Tier0Error("invalid_catalog", f"invalid {label} basename")
    return value


def _catalog_token(value, label, run_id):
    if not isinstance(value, str) or not KIND_RE.fullmatch(value):
        raise Tier0Error(
            "invalid_catalog", f"invalid {label} for {run_id}")
    return value


def _catalog_timestamp(value, label, run_id):
    if not isinstance(value, str) or len(value) > 64:
        raise Tier0Error(
            "invalid_catalog", f"invalid {label} for {run_id}")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Tier0Error(
            "invalid_catalog", f"invalid {label} for {run_id}") from exc
    if parsed.tzinfo is None:
        raise Tier0Error(
            "invalid_catalog", f"{label} lacks timezone for {run_id}")
    return value


def _validate_catalog(payload):
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise Tier0Error("invalid_catalog", "unsupported catalog schema version")
    raw_records = payload.get("records")
    if (not isinstance(raw_records, list)
            or not 1 <= len(raw_records) <= MAX_CATALOG_RECORDS):
        raise Tier0Error("invalid_catalog", "catalog records are unavailable or unbounded")
    records = []
    seen = set()
    for raw in raw_records:
        if not isinstance(raw, dict):
            raise Tier0Error("invalid_catalog", "catalog record is not an object")
        run_id = raw.get("run_id")
        if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
            raise Tier0Error("invalid_catalog", "catalog run_id is invalid")
        if run_id in seen:
            raise Tier0Error("invalid_catalog", f"duplicate catalog run_id: {run_id}")
        seen.add(run_id)
        kind = raw.get("kind")
        if not isinstance(kind, str) or not KIND_RE.fullmatch(kind):
            raise Tier0Error("invalid_catalog", f"invalid kind for {run_id}")
        tickers = raw.get("tickers")
        if (not isinstance(tickers, list)
                or not 1 <= len(tickers) <= MAX_RECORD_TICKERS):
            raise Tier0Error("invalid_catalog", f"invalid tickers for {run_id}")
        normalized = []
        for ticker in tickers:
            try:
                canonical = storage.canonical_ticker(ticker)
            except Exception as exc:  # noqa: BLE001
                raise Tier0Error(
                    "invalid_catalog", f"invalid ticker in {run_id}") from exc
            if ticker != canonical or ticker in normalized:
                raise Tier0Error(
                    "invalid_catalog", f"noncanonical/duplicate ticker in {run_id}")
            normalized.append(ticker)
        artifact = raw.get("artifact")
        if not isinstance(artifact, dict):
            raise Tier0Error("invalid_catalog", f"missing artifact for {run_id}")
        basename = _validate_basename(
            artifact.get("basename"), "artifact", ".json")
        if basename != f"{run_id}.json":
            raise Tier0Error(
                "invalid_catalog", f"artifact basename does not match {run_id}")
        size = artifact.get("size")
        if (isinstance(size, bool) or not isinstance(size, int)
                or not 1 <= size <= MAX_ARTIFACT_BYTES):
            raise Tier0Error("invalid_catalog", f"invalid artifact size for {run_id}")
        _validate_sha(artifact.get("sha256"), "artifact")
        if artifact.get("schema") not in ARTIFACT_SCHEMAS:
            raise Tier0Error("invalid_catalog", f"invalid artifact schema for {run_id}")
        script = raw.get("script")
        if script is not None:
            if not isinstance(script, dict):
                raise Tier0Error("invalid_catalog", f"invalid script for {run_id}")
            _validate_basename(script.get("basename"), "script", ".py")
            script_size = script.get("size")
            if (isinstance(script_size, bool) or not isinstance(script_size, int)
                    or not 1 <= script_size <= MAX_ARTIFACT_BYTES):
                raise Tier0Error("invalid_catalog", f"invalid script size for {run_id}")
            _validate_sha(script.get("sha256"), "script")
            archive = script.get("archive")
            if archive is not None:
                if not isinstance(archive, dict):
                    raise Tier0Error(
                        "invalid_catalog", f"invalid script archive for {run_id}")
                if archive.get("state") != "archived_disabled":
                    raise Tier0Error(
                        "invalid_catalog", f"invalid archive state for {run_id}")
                archive_basename = _validate_basename(
                    archive.get("basename"), "script archive", ".py.disabled")
                if archive_basename != f"{script['basename']}.disabled":
                    raise Tier0Error(
                        "invalid_catalog",
                        f"archive basename does not match script for {run_id}")
                _catalog_timestamp(
                    archive.get("archived_at"), "archived_at", run_id)
        views = raw.get("allowed_views")
        if (not isinstance(views, list) or not views
                or len(views) != len(set(views))
                or not set(views).issubset(ALLOWED_VIEWS)):
            raise Tier0Error("invalid_catalog", f"invalid views for {run_id}")
        if raw.get("historical_only") is not True:
            raise Tier0Error("invalid_catalog", f"{run_id} is not historical-only")
        _catalog_timestamp(raw.get("performed_at"), "performed_at", run_id)
        _catalog_timestamp(
            raw.get("disposition_asof"), "disposition_asof", run_id)
        for field in (
                "historical_result", "reviewer_state", "reviewed_disposition"):
            _catalog_token(raw.get(field), field, run_id)
        records.append(dict(raw))
    by_id = {record["run_id"]: record for record in records}
    ids = set(by_id)
    for record in records:
        references = [
            record.get(field) for field in ("superseded_by", "resolved_by")
            if record.get(field) is not None]
        if len(references) > 1:
            raise Tier0Error(
                "invalid_catalog",
                f"{record['run_id']} has multiple terminal references")
        for field in ("superseded_by", "resolved_by"):
            target = record.get(field)
            if target is not None and target not in ids:
                raise Tier0Error(
                    "invalid_catalog",
                    f"{record['run_id']} has dangling {field}: {target}")
            if target == record["run_id"]:
                raise Tier0Error(
                    "invalid_catalog",
                    f"{record['run_id']} has a self-referential {field}")
            if (target is not None
                    and not (set(record["tickers"]) & set(by_id[target]["tickers"]))):
                raise Tier0Error(
                    "invalid_catalog",
                    f"{record['run_id']} {field} has unrelated tickers")
    return records


def _load_catalog():
    payload, evidence = _read_stable_json(
        CATALOG_PATH, max_bytes=MAX_CATALOG_BYTES,
        allowed_root=MODULE_DIR, direct_child=True)
    return _validate_catalog(payload), evidence


def _fixed_child(root, basename, *, suffix):
    _validate_basename(basename, "fixed child", suffix)
    return Path(root) / basename


def _file_integrity_state(path, expected, *, root):
    try:
        _raw, evidence = _read_stable_bytes(
            path, max_bytes=MAX_ARTIFACT_BYTES,
            expected_size=expected["size"], expected_sha=expected["sha256"],
            allowed_root=root, direct_child=True)
        return "verified", evidence
    except Tier0Error as exc:
        states = {
            "evidence_missing": "missing",
            "artifact_integrity": "integrity_error",
            "unsafe_path": "unsafe_path",
            "evidence_changed": "evidence_changed",
            "evidence_too_large": "oversized",
        }
        return states.get(exc.code, "unavailable"), None


def _record_summary(record, *, verify_files):
    artifact = record["artifact"]
    artifact_state, artifact_evidence = ("unchecked", None)
    script_state, script_evidence = (None, None)
    script_root_state = None
    archive_file_state = None
    if verify_files:
        artifact_state, artifact_evidence = _file_integrity_state(
            _fixed_child(RUN_LOGS_ROOT, artifact["basename"], suffix=".json"),
            artifact, root=RUN_LOGS_ROOT)
        if record.get("script") is not None:
            script = record["script"]
            archive = script.get("archive")
            if archive is None:
                script_state, script_evidence = _file_integrity_state(
                    _fixed_child(
                        PROJECT_ROOT, script["basename"], suffix=".py"),
                    script, root=PROJECT_ROOT)
            else:
                script_root_state, root_evidence = _file_integrity_state(
                    _fixed_child(
                        PROJECT_ROOT, script["basename"], suffix=".py"),
                    script, root=PROJECT_ROOT)
                archive_file_state, archive_evidence = _file_integrity_state(
                    _fixed_child(
                        SCRIPT_ARCHIVE_ROOT, archive["basename"],
                        suffix=".py.disabled"),
                    script, root=SCRIPT_ARCHIVE_ROOT)
                if archive_file_state == "verified" and script_root_state == "missing":
                    script_state = "archived"
                elif archive_file_state == "verified":
                    script_state = "archived_root_residue"
                else:
                    script_state = f"archive_{archive_file_state}"
                script_evidence = archive_evidence
                if root_evidence is not None:
                    script_evidence = [
                        item for item in (archive_evidence, root_evidence) if item]
    return {
        "run_id": record["run_id"],
        "kind": record["kind"],
        "tickers": list(record["tickers"]),
        "performed_at": record["performed_at"],
        "historical_result": record["historical_result"],
        "reviewer_state": record["reviewer_state"],
        "reviewed_disposition": record["reviewed_disposition"],
        "disposition_asof": record["disposition_asof"],
        "historical_only": True,
        "superseded_by": record.get("superseded_by"),
        "resolved_by": record.get("resolved_by"),
        "allowed_views": list(record["allowed_views"]),
        "artifact": {
            "basename": artifact["basename"],
            "schema": artifact["schema"],
            "state": artifact_state,
        },
        "script": (None if record.get("script") is None else {
            "basename": record["script"]["basename"],
            "state": script_state,
            **({
                "root_state": (
                    "absent" if script_root_state == "missing"
                    else script_root_state),
                "archive": {
                    "state": record["script"]["archive"]["state"],
                    "basename": record["script"]["archive"]["basename"],
                    "relative_path": (
                        "archive/_repair_scripts_archive/2026-07/"
                        + record["script"]["archive"]["basename"]),
                    "archived_at": record["script"]["archive"]["archived_at"],
                    "integrity_state": archive_file_state,
                },
            } if record["script"].get("archive") is not None else {}),
        }),
    }, [item for item in (
        [artifact_evidence]
        + (script_evidence if isinstance(script_evidence, list)
           else [script_evidence])) if item]


def history_list(*, ticker=None, kind=None, cursor=0, limit=20):
    ticker = _canonical_ticker(ticker) if ticker is not None else None
    kind = _kind(kind)
    cursor, limit = _pagination(cursor, limit, default_limit=20)
    records, catalog_evidence = _load_catalog()
    selected = [
        record for record in records
        if (ticker is None or ticker in record["tickers"])
        and (kind is None or record["kind"] == kind)
    ]
    selected.sort(key=lambda row: (row["performed_at"], row["run_id"]),
                  reverse=True)
    page_records, pagination = _page(selected, cursor, limit)
    rows = []
    evidence = [catalog_evidence]
    for record in page_records:
        summary, item_evidence = _record_summary(record, verify_files=True)
        rows.append(summary)
        evidence.extend(item_evidence)
    evidence = _verify_evidence(evidence)
    return _with_evidence({
        "kind": "tier0_history_list",
        "schema_version": SCHEMA_VERSION,
        "historical": True,
        "filters": {"ticker": ticker, "kind": kind},
        "records": rows,
        "pagination": pagination,
    }, evidence)


def _artifact_payload(record):
    artifact = record["artifact"]
    path = _fixed_child(
        RUN_LOGS_ROOT, artifact["basename"], suffix=".json")
    payload, evidence = _read_stable_json(
        path, max_bytes=MAX_ARTIFACT_BYTES,
        expected_size=artifact["size"], expected_sha=artifact["sha256"],
        allowed_root=RUN_LOGS_ROOT, direct_child=True)
    schema = artifact["schema"]
    required = {
        "ticker_repair": ("ticker", "result", "verification"),
        "live_seam_probe": ("task", "summary", "rows", "no_bank_writes"),
        "phantom_fix": ("ticker", "result", "post_verification"),
        "availability_probe": ("ticker", "result", "probes", "zero_bank_writes"),
    }[schema]
    if any(key not in payload for key in required):
        raise Tier0Error(
            "invalid_artifact_schema",
            f"artifact does not match catalog schema {schema}")
    artifact_run = payload.get("run")
    if artifact_run is not None and artifact_run != record["run_id"]:
        raise Tier0Error(
            "invalid_artifact_schema",
            "artifact run identifier does not match its catalog record")
    artifact_result = payload.get("result")
    if (artifact_result is not None
            and artifact_result != record["historical_result"]):
        raise Tier0Error(
            "invalid_artifact_schema",
            "artifact result does not match its catalog record")
    if schema in {"ticker_repair", "phantom_fix", "availability_probe"}:
        ticker = payload.get("ticker")
        if ticker not in record["tickers"]:
            raise Tier0Error(
                "invalid_artifact_schema",
                "artifact ticker does not match its catalog record")
    if schema == "live_seam_probe":
        if not isinstance(payload.get("rows"), list):
            raise Tier0Error("invalid_artifact_schema", "seam rows are not a list")
        row_tickers = set()
        for row in payload["rows"]:
            if not isinstance(row, dict):
                raise Tier0Error(
                    "invalid_artifact_schema", "seam row is not an object")
            ticker = row.get("ticker")
            try:
                canonical = storage.canonical_ticker(ticker)
            except Exception as exc:  # noqa: BLE001
                raise Tier0Error(
                    "invalid_artifact_schema", "seam row ticker is invalid") from exc
            if ticker != canonical:
                raise Tier0Error(
                    "invalid_artifact_schema", "seam row ticker is noncanonical")
            row_tickers.add(ticker)
        if row_tickers != set(record["tickers"]):
            raise Tier0Error(
                "invalid_artifact_schema",
                "seam row tickers do not match the catalog record")
    if schema == "availability_probe" and not isinstance(payload.get("probes"), list):
        raise Tier0Error("invalid_artifact_schema", "probe rows are not a list")
    return payload, evidence


def _pick(payload, names):
    return {name: payload.get(name) for name in names if name in payload}


def _artifact_view(record, payload, view, cursor, limit):
    schema = record["artifact"]["schema"]
    pagination = None
    if schema == "ticker_repair":
        if view == "summary":
            data = _pick(payload, (
                "run", "ticker", "result", "started", "finished", "intervals",
                "boundary_day", "join_month", "since", "no_basis_action",
                "predecessor_note", "deep_history_note", "error"))
        elif view == "before_after":
            verification = payload.get("verification") or {}
            data = {
                "before": payload.get("before"),
                "boundary_fill": payload.get("boundary_fill"),
                "daily_interior_seal": payload.get("daily_interior_seal"),
                "after": _pick(verification, (
                    "edges", "boundary_after", "join_after",
                    "missing_daily_after_boundary")),
            }
        elif view == "verification":
            data = payload.get("verification") or {}
        elif view == "fetch_totals":
            gap = payload.get("gap_fill") or {}
            series = gap.get("series") if isinstance(gap.get("series"), list) else []
            rows, pagination = _page(series, cursor, limit)
            data = {
                "totals": gap.get("totals"),
                "per_port": gap.get("per_port"),
                "aborted": gap.get("aborted"),
                "cancelled": gap.get("cancelled"),
                "series": rows,
            }
        else:
            _validation(f"view {view!r} is not valid for ticker repair artifacts")
    elif schema == "live_seam_probe":
        if view == "summary":
            data = _pick(payload, (
                "task", "started", "finished", "no_bank_writes",
                "seams_requested", "ports_requested", "ports_listening",
                "port_errors", "summary"))
        elif view == "seam_rows":
            rows, pagination = _page(payload.get("rows") or [], cursor, limit)
            data = {"summary": payload.get("summary"), "rows": rows}
        else:
            _validation(f"view {view!r} is not valid for seam artifacts")
    elif schema == "phantom_fix":
        if view == "summary":
            data = _pick(payload, (
                "ticker", "result", "dry_run", "ex_date", "factor", "intervals",
                "affected_bars", "affected_file_count", "triage"))
        elif view == "verification":
            data = {
                "manifest_note": payload.get("manifest_note"),
                "post_verification": payload.get("post_verification"),
            }
        else:
            _validation(f"view {view!r} is not valid for correction artifacts")
    elif schema == "availability_probe":
        if view == "summary":
            data = _pick(payload, (
                "run", "ticker", "result", "started", "finished", "conid",
                "head_trades", "head_historical_volatility", "blocker",
                "zero_bank_writes"))
        elif view == "verification":
            rows, pagination = _page(payload.get("probes") or [], cursor, limit)
            data = {"blocker": payload.get("blocker"), "probes": rows}
        else:
            _validation(f"view {view!r} is not valid for availability artifacts")
    else:  # catalog validation makes this unreachable
        raise Tier0Error("invalid_catalog", "unsupported artifact schema")
    return _sanitize(data), pagination


def run_result(run_id, *, view="summary", cursor=0, limit=50):
    run_id = _run_id(run_id)
    if view not in ALLOWED_VIEWS:
        _validation("view is not an allowlisted semantic view")
    cursor, limit = _pagination(cursor, limit, default_limit=50)
    records, catalog_evidence = _load_catalog()
    index = {record["run_id"]: record for record in records}
    record = index.get(run_id)
    if record is None:
        _not_found(f"run_id is not cataloged: {run_id}")
    if view not in record["allowed_views"]:
        _validation(f"view {view!r} is not allowed for {run_id}")
    payload, artifact_evidence = _artifact_payload(record)
    selected, pagination = _artifact_view(
        record, payload, view, cursor, limit)
    summary, _unused = _record_summary(record, verify_files=False)
    summary["artifact"]["state"] = "verified"
    evidence = _verify_evidence([catalog_evidence, artifact_evidence])
    out = {
        "kind": "tier0_run_result",
        "schema_version": SCHEMA_VERSION,
        "historical": True,
        "current": None,
        "record": summary,
        "view": view,
        "payload": selected,
    }
    if pagination is not None:
        out["pagination"] = pagination
    return _with_evidence(out, evidence)


def _load_sidecar(basename, *, expected_kind=None):
    path = _fixed_child(STORAGE_ROOT, basename, suffix=".json")
    payload, evidence = _read_stable_json(
        path, max_bytes=MAX_SIDECAR_BYTES,
        allowed_root=STORAGE_ROOT, direct_child=True)
    if expected_kind is not None and payload.get("kind") != expected_kind:
        raise Tier0Error(
            "invalid_evidence",
            f"{basename} is not a {expected_kind} report")
    return payload, evidence


def _health_freshness(report):
    source = report.get("source_state") if isinstance(report, dict) else None
    try:
        if (not isinstance(source, dict)
                or source.get("schema_version")
                != storage.BANK_STATE_FINGERPRINT_VERSION
                or source.get("algorithm") != "sha256"
                or source.get("current") is not True
                or not SHA_RE.fullmatch(str(source.get("after_sha256") or ""))
                or source.get("before_sha256") != source.get("after_sha256")):
            raise ValueError("cached health has no current bank-state provenance")
        current = storage.bank_manifest_state_fingerprint(STORAGE_ROOT)
        if current["sha256"] != source["after_sha256"]:
            raise ValueError("bank manifest state changed after cached health")
        return True, None, current
    except Exception as exc:  # noqa: BLE001 - currentness failure is result data
        return False, _safe_string(f"{type(exc).__name__}: {exc}"), None


def bank_health_summary():
    report, evidence = _load_sidecar(
        HEALTH_REPORT, expected_kind="health_report")
    cached_queue = report.get("queue") if isinstance(report.get("queue"), list) else []
    cached_errors = report.get("errors") if isinstance(report.get("errors"), list) else []
    current, freshness_error, current_state = _health_freshness(report)
    queue = cached_queue if current else []
    errors = list(cached_errors)
    if freshness_error:
        errors.append({"component": "source_state", "error": freshness_error})
    evidence = _verify_evidence([evidence])
    return _with_evidence({
        "kind": "tier0_bank_health",
        "schema_version": SCHEMA_VERSION,
        "asof": _safe_string(report.get("asof")),
        "clean": bool(report.get("clean")) and current,
        "evidence_current": current,
        "source_state": _sanitize(current_state or report.get("source_state") or {}),
        "counts": _sanitize(report.get("counts") or {}),
        "queue_count": len(queue),
        "queue": _sanitize(queue[:MAX_ROWS]),
        "errors": _sanitize(errors[:MAX_ROWS]),
        "source": "cached_health_report",
        "network": False,
        "written": False,
    }, evidence)


def coverage_status(*, ticker=None, cursor=0, limit=50):
    ticker = _canonical_ticker(ticker) if ticker is not None else None
    cursor, limit = _pagination(cursor, limit, default_limit=50)
    report, evidence = _load_sidecar(
        COVERAGE_REPORT, expected_kind="coverage_audit")
    if ticker is not None:
        summary = report.get("summary")
        row = summary.get(ticker) if isinstance(summary, dict) else None
        if not isinstance(row, dict):
            _not_found(f"coverage status is unavailable for {ticker}")
        data = {
            "kind": "tier0_coverage_status",
            "schema_version": SCHEMA_VERSION,
            "asof": _safe_string(report.get("asof")),
            "ticker": ticker,
            "status": _sanitize(row),
            "network": False,
            "written": False,
        }
    else:
        flagged = []
        for label in ("forward_stale", "front_short"):
            rows = report.get(label) if isinstance(report.get(label), list) else []
            for row in rows:
                flagged.append({"category": label, **(row if isinstance(row, dict) else {})})
        for value in report.get("unknown") or []:
            flagged.append({"category": "unknown", "ticker": value})
        flagged.sort(key=lambda row: (
            str(row.get("ticker") or ""), str(row.get("category") or "")))
        rows, pagination = _page(flagged, cursor, limit)
        data = {
            "kind": "tier0_coverage_status",
            "schema_version": SCHEMA_VERSION,
            "asof": _safe_string(report.get("asof")),
            "ticker_count": _nonnegative_int(
                report.get("ticker_count"), "coverage ticker_count", default=0),
            "bank_latest": _safe_string(report.get("bank_latest")),
            "baseline_start": _safe_string(report.get("baseline_start")),
            "counts": {
                "forward_stale": len(report.get("forward_stale") or []),
                "front_short": len(report.get("front_short") or []),
                "unknown": len(report.get("unknown") or []),
            },
            "flagged": _sanitize(rows),
            "pagination": pagination,
            "errors": _sanitize((report.get("errors") or [])[:MAX_ROWS]),
            "network": False,
            "written": False,
        }
    evidence = _verify_evidence([evidence])
    return _with_evidence(data, evidence)


def cached_gap_summary(*, ticker=None, interval=None, cursor=0, limit=50):
    ticker = _canonical_ticker(ticker) if ticker is not None else None
    interval = _interval(interval)
    cursor, limit = _pagination(cursor, limit, default_limit=50)
    if ticker is not None and interval is not None:
        requested = [(ticker, interval)]
        discovery_errors = []
    else:
        requested, discovery_errors = gap_evidence.discover_primary_series(
            STORAGE_ROOT, tickers=[ticker] if ticker is not None else None)
        if interval is not None:
            requested = [row for row in requested if row[1] == interval]
    report, evidence = _stream_gap_sidecar(
        series_keys={f"{row_ticker} {row_interval}"
                     for row_ticker, row_interval in requested})
    evaluated = gap_evidence.evaluate_payload(
        STORAGE_ROOT, report, requested, source={
            "basename": GAP_REPORT,
            "sha256": evidence["sha256"],
            "bytes": evidence["size"],
        }, discovery_errors=discovery_errors)
    rows = []
    for value in evaluated["rows"]:
        rows.append({
            "ticker": value["ticker"],
            "interval": value["interval"],
            "current": True,
            "missing_total": value["missing_total"],
            "gap_events": value["gap_events"],
            "days": value["days"],
            "missing_days": value["missing_days"],
            "missing_day_list": value["missing_day_list"],
            "missing_day_runs": value["missing_day_runs"],
            "largest_missing_day_run": value["largest_missing_day_run"],
            "source_absent": value["source_absent"],
            "source_absent_list": value["source_absent_list"],
            "asof": value["asof"],
            "interval_fingerprint": value["interval_fingerprint"],
        })
    page_rows, pagination = _page(rows, cursor, limit)
    issue_rows = [row for row in rows if any(row[key] for key in (
        "missing_total", "gap_events", "missing_days", "source_absent"))]
    evidence = _verify_evidence([evidence])
    return _with_evidence({
        "kind": "tier0_cached_gap_summary",
        "schema_version": SCHEMA_VERSION,
        "asof": _sanitize(report.get("asof")),
        "filters": {"ticker": ticker, "interval": interval},
        "applicable": not interval or gap_evidence.is_gap_evidence_interval(interval),
        "evidence_current": not evaluated["unavailable"] and not evaluated["errors"],
        "series_count": len(rows),
        "expected_series_count": evaluated["expected_series"],
        "issue_series_count": len(issue_rows),
        "totals": {
            key: sum(row[key] for row in rows)
            for key in ("missing_total", "gap_events", "missing_days",
                        "source_absent")
        },
        "series": page_rows,
        "unavailable": _sanitize(evaluated["unavailable"][:MAX_ROWS]),
        "errors": _sanitize(evaluated["errors"][:MAX_ROWS]),
        "pagination": pagination,
        "source": "cached_gap_report",
        "network": False,
        "written": False,
    }, evidence)


def repair_queue(*, cursor=0, limit=50):
    cursor, limit = _pagination(cursor, limit, default_limit=50)
    report, evidence = _load_sidecar(
        HEALTH_REPORT, expected_kind="health_report")
    cached_queue = report.get("queue") if isinstance(report.get("queue"), list) else []
    current, freshness_error, current_state = _health_freshness(report)
    queue = cached_queue if current else []
    rows, pagination = _page(queue, cursor, limit)
    evidence = _verify_evidence([evidence])
    return _with_evidence({
        "kind": "tier0_repair_queue",
        "schema_version": SCHEMA_VERSION,
        "asof": _safe_string(report.get("asof")),
        "clean": bool(report.get("clean")) and current,
        "evidence_current": current,
        "source_state": _sanitize(current_state or report.get("source_state") or {}),
        "errors": ([{"component": "source_state", "error": freshness_error}]
                   if freshness_error else []),
        "queue": _sanitize(rows),
        "pagination": pagination,
        "source": "cached_health_report",
        "network": False,
        "written": False,
    }, evidence)


def _manifest(ticker):
    path = STORAGE_ROOT / ticker / storage.MANIFEST_NAME
    payload, evidence = _read_stable_json(
        path, max_bytes=MAX_MANIFEST_BYTES, allowed_root=STORAGE_ROOT)
    intervals = payload.get("intervals")
    if not isinstance(intervals, dict):
        raise Tier0Error("invalid_evidence", f"manifest intervals are invalid for {ticker}")
    folder = payload.get("folder") or ticker
    if folder != ticker:
        raise Tier0Error("invalid_evidence", f"manifest folder does not match {ticker}")
    for token, item in intervals.items():
        if (not isinstance(token, str) or not storage.INTERVAL_RE.fullmatch(token)
                or not isinstance(item, dict)
                or not isinstance(item.get("months"), dict)):
            raise Tier0Error("invalid_evidence", f"manifest series shape is invalid for {ticker}")
        if not all(isinstance(entry, dict) for entry in item["months"].values()):
            raise Tier0Error("invalid_evidence", f"manifest month shape is invalid for {ticker}")
    return payload, evidence


def _bar_json(bar):
    if not isinstance(bar, (list, tuple)) or len(bar) != 6:
        raise Tier0Error("invalid_evidence", "stored edge bar has an invalid shape")
    stamp = bar[0]
    if not isinstance(stamp, dt.datetime):
        raise Tier0Error("invalid_evidence", "stored edge bar has an invalid timestamp")
    return {
        "timestamp": stamp.isoformat(timespec="seconds"),
        "open": float(bar[1]),
        "high": float(bar[2]),
        "low": float(bar[3]),
        "close": float(bar[4]),
        "volume": int(bar[5]),
    }


def _present_months(interval_item):
    out = []
    for month, entry in interval_item.get("months", {}).items():
        if not isinstance(month, str) or not MONTH_RE.fullmatch(month):
            continue
        if str(entry.get("status") or "present").upper() == "MISSING":
            continue
        out.append((month, entry))
    out.sort(key=lambda item: item[0])
    return out


def _read_edge_month(ticker, interval, month, entry):
    year, number = map(int, month.split("-"))
    path = storage.find_month_file(
        STORAGE_ROOT, ticker, year, number, interval)
    if path is None:
        raise Tier0Error(
            "evidence_missing", f"stored month is missing for {ticker} {interval} {month}")
    expected_sha = entry.get("sha256")
    expected_size = entry.get("size")
    if (not isinstance(expected_sha, str) or not SHA_RE.fullmatch(expected_sha)
            or isinstance(expected_size, bool) or not isinstance(expected_size, int)
            or expected_size <= 0):
        raise Tier0Error(
            "invalid_evidence",
            f"manifest integrity fields are unavailable for {ticker} {interval} {month}")
    before = _file_state(path, allowed_root=STORAGE_ROOT)
    try:
        bars, stats = storage.read_month_file_fast(path, expected_sha)
    except Exception as exc:  # noqa: BLE001 - normalize storage evidence failure
        raise Tier0Error(
            "invalid_evidence",
            f"cannot strictly read {ticker} {interval} {month}: {exc}") from exc
    after = _file_state(path, allowed_root=STORAGE_ROOT)
    if _state_token(before) != _state_token(after):
        raise Tier0Error(
            "evidence_changed", f"stored month changed during read: {path.name}")
    if (stats.get("sha256") != expected_sha
            or int(stats.get("size") or -1) != expected_size
            or int(stats.get("rows") or -1) != len(bars)):
        raise Tier0Error(
            "evidence_changed",
            f"stored month no longer matches its manifest: {path.name}")
    if not bars:
        raise Tier0Error("invalid_evidence", f"stored month is empty: {path.name}")
    evidence = {
        "_path": str(Path(path).resolve(strict=True)),
        "_allowed_root": str(STORAGE_ROOT.resolve(strict=True)),
        "_direct_child": False,
        "path": _path_label(path),
        "size": after["size"],
        "mtime_ns": after["mtime_ns"],
        "sha256": expected_sha,
        "state": _state_token(after),
    }
    return bars, evidence


def _series_extents(ticker, manifest):
    intervals = manifest["intervals"]
    tokens = sorted(intervals)
    shown = tokens[:MAX_INTERVALS]
    rows = []
    evidence = []
    for token in shown:
        months = _present_months(intervals[token])
        if not months:
            rows.append({
                "interval": token,
                "month_count": 0,
                "first_month": None,
                "last_month": None,
                "rows": 0,
                "first_bar": None,
                "last_bar": None,
            })
            continue
        first_month, first_entry = months[0]
        last_month, last_entry = months[-1]
        first_bars, first_evidence = _read_edge_month(
            ticker, token, first_month, first_entry)
        if first_month == last_month:
            last_bars, last_evidence = first_bars, first_evidence
        else:
            last_bars, last_evidence = _read_edge_month(
                ticker, token, last_month, last_entry)
        evidence.extend([first_evidence, last_evidence])
        total_rows = 0
        for _month, entry in months:
            total_rows += _nonnegative_int(
                entry.get("rows"),
                f"{ticker} {token} {_month} manifest rows")
        rows.append({
            "interval": token,
            "month_count": len(months),
            "first_month": first_month,
            "last_month": last_month,
            "rows": total_rows,
            "first_bar": _bar_json(first_bars[0]),
            "last_bar": _bar_json(last_bars[-1]),
            "verified_absent_count": len(
                intervals[token].get("verified_absent") or []),
        })
    return rows, {
        "total": len(tokens),
        "returned": len(shown),
        "truncated": max(0, len(tokens) - len(shown)),
    }, evidence


def _split_status_data(ticker, *, cursor, limit):
    manifest_path = STORAGE_ROOT / ticker / storage.MANIFEST_NAME
    _raw, manifest_evidence = _read_stable_bytes(
        manifest_path, max_bytes=MAX_MANIFEST_BYTES,
        allowed_root=STORAGE_ROOT)
    try:
        report = split_audit.audit(
            STORAGE_ROOT, [ticker], write=False)
    except Exception as exc:  # noqa: BLE001 - normalize offline audit failure
        raise Tier0Error(
            "evidence_unavailable", f"split status failed for {ticker}: {exc}") from exc
    items = report.get("tickers")
    if not isinstance(items, list) or len(items) != 1:
        raise Tier0Error("invalid_evidence", "split audit returned an invalid ticker result")
    item = items[0]
    provenance = item.get("source_provenance")
    if not isinstance(provenance, dict):
        provenance = {}
    evidence_generated_at = (
        provenance.get("reference_asof") or provenance.get("fetched_at"))
    rows = item.get("rows") if isinstance(item.get("rows"), list) else []
    page_rows, pagination = _page(rows, cursor, limit)
    data = {
        "kind": "tier0_split_status",
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker,
        "generated_at": _safe_string(evidence_generated_at),
        "network": False,
        "written": False,
        "data_clean": bool(item.get("data_clean")),
        "evidence_complete": bool(item.get("evidence_complete")),
        "detection_current": bool(item.get("detection_current")),
        "cache": _sanitize(item.get("cache") or {}),
        "fingerprint": _sanitize(item.get("fingerprint") or {}),
        "verdict_counts": _sanitize(item.get("verdict_counts") or {}),
        "repair_queue": _sanitize(item.get("repair_queue") or []),
        "rows": _sanitize(page_rows),
        "pagination": pagination,
    }
    evidence = _verify_evidence([manifest_evidence])
    return data, evidence


def split_status(ticker, *, cursor=0, limit=50):
    ticker = _canonical_ticker(ticker)
    cursor, limit = _pagination(cursor, limit, default_limit=50)
    data, evidence = _split_status_data(ticker, cursor=cursor, limit=limit)
    return _with_evidence(data, evidence)


def ticker_status(ticker):
    ticker = _canonical_ticker(ticker)
    manifest, manifest_evidence = _manifest(ticker)
    series, series_page, month_evidence = _series_extents(ticker, manifest)
    coverage, coverage_evidence = _load_sidecar(
        COVERAGE_REPORT, expected_kind="coverage_audit")
    gaps, gap_report_evidence = _stream_gap_sidecar(ticker=ticker)
    catalog, catalog_evidence = _load_catalog()
    split_data, split_evidence = _split_status_data(
        ticker, cursor=0, limit=MAX_ROWS)

    coverage_summary = coverage.get("summary")
    coverage_row = (
        coverage_summary.get(ticker)
        if isinstance(coverage_summary, dict) else None)
    gap_series = gaps.get("series") if isinstance(gaps.get("series"), dict) else {}
    ticker_gaps = []
    for key, value in sorted(gap_series.items()):
        if key.startswith(f"{ticker} ") and isinstance(value, dict):
            gap_interval = key[len(ticker) + 1:]
            if not storage.INTERVAL_RE.fullmatch(gap_interval):
                raise Tier0Error(
                    "invalid_evidence", f"cached gap interval is invalid: {key}")
            ticker_gaps.append({
                "interval": gap_interval,
                "missing_total": _nonnegative_int(
                    value.get("missing_total"), f"{key} missing_total", default=0),
                "gap_events": _nonnegative_int(
                    value.get("gap_events"), f"{key} gap_events", default=0),
                "missing_days": _nonnegative_int(
                    value.get("missing_days"), f"{key} missing_days", default=0),
                "source_absent": _nonnegative_int(
                    value.get("source_absent"), f"{key} source_absent", default=0),
            })
    history = []
    for record in sorted(
            (item for item in catalog if ticker in item["tickers"]),
            key=lambda item: (item["performed_at"], item["run_id"]),
            reverse=True)[:MAX_ROWS]:
        summary, _unused = _record_summary(record, verify_files=False)
        history.append(summary)

    evidence = [
        manifest_evidence, coverage_evidence, gap_report_evidence,
        catalog_evidence,
        *month_evidence, *split_evidence,
    ]
    evidence = _verify_evidence(evidence)
    aliases = manifest.get("aliases")
    actions = manifest.get("actions")
    if not isinstance(aliases, list):
        aliases = []
    if not isinstance(actions, list):
        actions = []
    return _with_evidence({
        "kind": "tier0_ticker_status",
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker,
        "current": {
            "manifest": {
                "symbol": _safe_string(manifest.get("symbol") or ticker),
                "folder": ticker,
                "conid": _optional_positive_int(
                    manifest.get("conid"), f"{ticker} manifest conid"),
                "basis": _safe_string(manifest.get("basis") or "unknown"),
                "generation": _nonnegative_int(
                    manifest.get("generation"),
                    f"{ticker} manifest generation", default=0),
                "aliases": _sanitize(aliases[:MAX_ROWS]),
                "actions": _sanitize(actions[:MAX_ROWS]),
                "data_corrections": _sanitize(
                    manifest.get("data_corrections") or []),
            },
            "series": series,
            "series_pagination": series_page,
            "coverage": _sanitize(coverage_row or {"status": "not_present"}),
            "coverage_asof": _safe_string(coverage.get("asof")),
            "cached_gaps": ticker_gaps,
            "gaps_asof": _sanitize(gaps.get("asof")),
            "split": {
                key: value for key, value in split_data.items()
                if key not in {"kind", "schema_version", "ticker"}
            },
        },
        "historical": {
            "asof_labeled": True,
            "records": history,
        },
        "network": False,
        "written": False,
    }, evidence)


__all__ = [
    "Tier0Error",
    "bank_health_summary",
    "cached_gap_summary",
    "coverage_status",
    "history_list",
    "repair_queue",
    "run_result",
    "split_status",
    "ticker_status",
]
