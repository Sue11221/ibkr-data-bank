"""Pure readiness, failure, retry, and evidence primitives for TWS bring-up."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import uuid


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUN_LOG_ROOT = PROJECT_ROOT / "Run Logs"
MAX_EVENTS = 256
MAX_SCREENSHOTS = 16
MAX_STEP_ROWS = 256
MAX_STEP_SCREENSHOTS = 96
_TOKEN_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ACCOUNT_RE = re.compile(r"\bDU\d+\b", re.IGNORECASE)
_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
    re.IGNORECASE)

TRANSIENT_CODES = frozenset({
    "login_window_absent",
    "login_drive_failed",
    "account_window_absent",
    "focus_lost",
    "config_open_failed",
    "api_node_not_found",
    "enable_text_not_matched",
    "socket_port_label_not_found",
    "apply_button_not_found",
    "port_not_listening",
    "handshake_failed",
})
FAILURE_CODES = TRANSIENT_CODES | frozenset({
    "aborted",
    "dependency_unavailable",
    "config_dir_busy",
    "evidence_capture_failed",
    "port_already_listening",
    "unexpected_error",
})
EVENT_CODES = FAILURE_CODES | frozenset({"ok", "already_up", "self_recovered"})
EVENT_PHASES = frozenset({
    "unknown", "preflight", "launch", "login_window", "login_drive",
    "account_window", "config_open", "api_navigation", "controls",
    "enable", "port_readiness", "cleanup", "abort", "restart",
    "readiness", "handshake", "evidence", "resource_warning_sweep",
    "memory_cap_install",
})
EVENT_OUTCOMES = frozenset({"failed", "succeeded", "self_recovered", "aborted"})
OPERATIONS = frozenset({"fleet_bringup", "launch_many", "enable_fleet",
                        "restart_dead"})
REQUIRED_STEPS = (
    "launch",
    "login_account",
    "config_open",
    "api_enable",
    "port_bind",
    "listener_up",
    "handshake",
)
STEP_OUTCOMES = frozenset({"succeeded", "failed", "not_attempted"})
MENU_PATHS = frozenset({"File", "Edit"})


class StepFailure(RuntimeError):
    """A controlled bring-up failure with a stable machine-readable code."""

    def __init__(self, message, *, code="unexpected_error", phase="unknown"):
        super().__init__(str(message))
        self.code = _allowed_token(code, FAILURE_CODES, "unexpected_error")
        self.phase = _allowed_token(phase, EVENT_PHASES, "unknown")

    @property
    def retryable(self):
        return self.code in TRANSIENT_CODES


def _controlled_token(value, fallback):
    value = str(value or "").strip().lower()
    return value if _TOKEN_RE.fullmatch(value) else fallback


def _allowed_token(value, allowed, fallback):
    value = _controlled_token(value, fallback)
    return value if value in allowed else fallback


def redacted_evidence_text(value, limit=240):
    """Bound one diagnostic label while excluding account/email identities."""
    text = " ".join(str(value or "").split())
    text = _ACCOUNT_RE.sub("DU_REDACTED", text)
    text = _EMAIL_RE.sub("EMAIL_REDACTED", text)
    return text[:max(0, int(limit))]


def failure_code(exc):
    """Return a controlled code without parsing arbitrary exception text."""
    seen = set()
    cur = exc
    for _ in range(4):
        if cur is None or id(cur) in seen:
            break
        seen.add(id(cur))
        code = getattr(cur, "code", None)
        if code:
            return _allowed_token(code, FAILURE_CODES, "unexpected_error")
        cur = getattr(cur, "__cause__", None)
    return "unexpected_error"


def failure_phase(exc):
    phase = getattr(exc, "phase", None)
    return _allowed_token(phase, EVENT_PHASES, "unknown") if phase else "unknown"


def is_retryable(exc):
    return failure_code(exc) in TRANSIENT_CODES


@dataclass(frozen=True)
class ReadinessPolicy:
    account_timeout: float = 60.0
    port_timeout: float = 20.0
    initial_interval: float = 0.5
    max_interval: float = 2.0
    backoff: float = 1.5
    consecutive: int = 2

    def __post_init__(self):
        if self.account_timeout <= 0 or self.port_timeout <= 0:
            raise ValueError("readiness timeouts must be positive")
        if self.initial_interval <= 0 or self.max_interval < self.initial_interval:
            raise ValueError("invalid readiness polling interval")
        if self.backoff < 1 or self.consecutive < 1:
            raise ValueError("invalid readiness stability policy")


@dataclass(frozen=True)
class StableResult:
    ok: bool
    value: object
    elapsed_ms: int
    probes: int


def wait_stable(probe, timeout, *, consecutive=2, initial_interval=0.5,
                max_interval=2.0, backoff=1.5, abort_check=None,
                clock=time.monotonic, sleep=time.sleep):
    """Wait for the same truthy probe value on consecutive observations."""
    if timeout <= 0 or consecutive < 1 or initial_interval <= 0:
        raise ValueError("invalid stable-wait policy")
    if max_interval < initial_interval or backoff < 1:
        raise ValueError("invalid stable-wait backoff")
    start = clock()
    deadline = start + float(timeout)
    interval = float(initial_interval)
    stable_value = None
    stable_count = 0
    probes = 0
    while True:
        if abort_check is not None and abort_check():
            raise StepFailure("aborted by user", code="aborted", phase="readiness")
        now = clock()
        if now > deadline:
            break
        value = probe()
        probes += 1
        if clock() > deadline:
            break
        if value and value == stable_value:
            stable_count += 1
        elif value:
            stable_value = value
            stable_count = 1
        else:
            stable_value = None
            stable_count = 0
        if stable_count >= consecutive:
            return StableResult(True, stable_value,
                                max(0, int((clock() - start) * 1000)), probes)
        remaining = deadline - clock()
        if remaining <= 0:
            break
        sleep(min(interval, remaining))
        interval = min(float(max_interval), interval * float(backoff))
    return StableResult(False, None, max(0, int((clock() - start) * 1000)),
                        probes)


class FleetTranscript:
    """Bounded redacted evidence for one caller-owned fleet operation."""

    schema_version = 3

    def __init__(self, operation, *, root=None, run_token=None, clock=None,
                 step_verification=False, expected_ports=None):
        self.operation = _allowed_token(operation, OPERATIONS, "fleet_bringup")
        self.root = Path(root) if root is not None else DEFAULT_RUN_LOG_ROOT
        now = (clock or (lambda: datetime.now(timezone.utc)))()
        stamp = now.strftime("%Y%m%d-%H%M%S")
        token = run_token or uuid.uuid4().hex[:8]
        if not re.fullmatch(r"[a-zA-Z0-9]{4,32}", str(token)):
            raise ValueError("invalid transcript run token")
        self.run_dir = self.root / f"tws-bringup-{stamp}-{token}"
        self.events = []
        self.events_truncated = 0
        self._events_lock = threading.RLock()
        self._screenshots = 0
        self.step_verification = bool(step_verification)
        self.expected_ports = []
        for raw_port in expected_ports or ():
            port = max(0, min(65535, int(raw_port)))
            if port and port not in self.expected_ports:
                self.expected_ports.append(port)
        self.steps = []
        self.steps_truncated = 0
        self._step_screenshots = 0

    def add_event(self, *, port, attempt, phase, code, elapsed_ms=0,
                  retryable=False, outcome="failed"):
        with self._events_lock:
            if len(self.events) >= MAX_EVENTS:
                self.events_truncated += 1
                return False
            event = {
                "operation": self.operation,
                "port": max(0, min(65535, int(port))),
                "attempt": max(0, min(3, int(attempt))),
                "phase": _allowed_token(phase, EVENT_PHASES, "unknown"),
                "code": _allowed_token(code, EVENT_CODES, "unexpected_error"),
                "elapsed_ms": max(0, min(86_400_000, int(elapsed_ms))),
                "retryable": bool(retryable),
                "outcome": _allowed_token(outcome, EVENT_OUTCOMES, "failed"),
            }
            self.events.append(event)
            return True

    def add_resource_warning_event(self, window, *, outcome="succeeded"):
        """Record one bounded, redacted resource-warning dismissal attempt."""
        with self._events_lock:
            succeeded = str(outcome) == "succeeded"
            if not self.add_event(
                    port=0, attempt=0, phase="resource_warning_sweep",
                    code="ok" if succeeded else "unexpected_error",
                    outcome="succeeded" if succeeded else "failed"):
                return False
            try:
                _hwnd, title, _pid, width, height = window
            except (TypeError, ValueError):
                title, width, height = "(unavailable)", 0, 0
            self.events[-1]["window_title"] = redacted_evidence_text(title)
            self.events[-1]["window_geometry"] = {
                "width": max(0, min(100_000, int(width or 0))),
                "height": max(0, min(100_000, int(height or 0))),
            }
            return True

    def add_memory_cap_event(self, result):
        """Record one path-free vmoptions install/verification result."""
        with self._events_lock:
            try:
                payload = (result.evidence() if callable(
                    getattr(result, "evidence", None)) else dict(result))
            except Exception:  # noqa: BLE001 - evidence must stay nonfatal
                payload = {}
            install_outcome = str(payload.get("outcome") or "warning")
            if install_outcome not in {"installed", "verified", "warning"}:
                install_outcome = "warning"
            verified = bool(payload.get("verified"))
            if not self.add_event(
                    port=0, attempt=0, phase="memory_cap_install",
                    code="ok" if verified else "unexpected_error",
                    outcome="succeeded" if verified else "failed"):
                return False
            backup_name = payload.get("backup_name")
            if backup_name:
                try:
                    backup_name = str(backup_name).replace("\\", "/").rsplit(
                        "/", 1)[-1]
                except (TypeError, ValueError, OSError):
                    backup_name = None
            self.events[-1]["memory_cap"] = {
                "outcome": install_outcome,
                "verified": verified,
                "changed": bool(payload.get("changed")),
                "cap_option": redacted_evidence_text(
                    payload.get("cap_option"), limit=40),
                "backup_name": redacted_evidence_text(
                    backup_name, limit=160) if backup_name else None,
                "detail": redacted_evidence_text(
                    payload.get("detail"), limit=240),
            }
            return True

    def screenshot_path(self, *, port, attempt, code):
        if self._screenshots >= MAX_SCREENSHOTS:
            return None
        self._screenshots += 1
        safe_code = _allowed_token(code, EVENT_CODES, "unexpected_error")
        name = (f"{self._screenshots:02d}-{int(port)}-"
                f"a{max(0, min(3, int(attempt)))}-{safe_code}.png")
        return self.run_dir / "screenshots" / name

    def step_screenshot_path(self, *, port, attempt, step):
        """Reserve one controlled per-step screenshot path for proof mode."""
        if not self.step_verification:
            return None
        if self._step_screenshots >= MAX_STEP_SCREENSHOTS:
            return None
        self._step_screenshots += 1
        safe_step = (str(step) if str(step) in REQUIRED_STEPS
                     else "unknown")
        name = (f"step-{self._step_screenshots:02d}-{int(port)}-"
                f"a{max(0, min(3, int(attempt)))}-{safe_step}.png")
        return self.run_dir / "screenshots" / name

    def add_step(self, *, port, attempt, step, outcome, screenshot=None,
                 menu_path=None, account_match=None, failure_code=None,
                 foreground_holder=None):
        """Add one redacted row to the per-instance verification matrix."""
        if not self.step_verification:
            return False
        if len(self.steps) >= MAX_STEP_ROWS:
            self.steps_truncated += 1
            return False
        step = str(step)
        if step not in REQUIRED_STEPS:
            raise ValueError(f"unsupported verification step: {step}")
        outcome = str(outcome)
        if outcome not in STEP_OUTCOMES:
            raise ValueError(f"unsupported verification outcome: {outcome}")
        row = {
            "port": max(0, min(65535, int(port))),
            "attempt": max(0, min(3, int(attempt))),
            "step": step,
            "outcome": outcome,
            "captured_utc": datetime.now(timezone.utc).isoformat(
                timespec="milliseconds").replace("+00:00", "Z"),
        }
        if menu_path is not None:
            row["menu_path"] = (str(menu_path)
                                if str(menu_path) in MENU_PATHS else "UNKNOWN")
        if account_match is not None:
            row["account_match"] = bool(account_match)
        if failure_code is not None:
            row["failure_code"] = _allowed_token(
                failure_code, FAILURE_CODES, "unexpected_error")
        if foreground_holder is not None:
            holder = redacted_evidence_text(foreground_holder)
            if holder:
                row["foreground_holder"] = holder
        if screenshot is not None:
            evidence = file_evidence(screenshot, relative_to=self.run_dir)
            if evidence is None:
                return False
            row["screenshot"] = evidence
        self.steps.append(row)
        return True

    def fill_not_attempted(self):
        """Make a halted proof explicit for every missing port/step cell."""
        if not self.step_verification:
            return
        present = {(row["port"], row["step"]) for row in self.steps}
        for port in self.expected_ports:
            for step in REQUIRED_STEPS:
                if (port, step) not in present:
                    self.add_step(
                        port=port, attempt=0, step=step,
                        outcome="not_attempted")

    def _successful_steps(self):
        successful = {}
        for row in self.steps:
            if row["outcome"] == "succeeded":
                successful[(row["port"], row["step"])] = row
        return successful

    def verification_complete(self):
        if not self.step_verification or not self.expected_ports:
            return False
        successful = self._successful_steps()
        for port in self.expected_ports:
            for step in REQUIRED_STEPS:
                row = successful.get((port, step))
                if row is None or not evidence_matches(
                        self.run_dir, row.get("screenshot")):
                    return False
                if (step == "config_open"
                        and row.get("menu_path") not in MENU_PATHS):
                    return False
                if (step in {"login_account", "handshake"}
                        and row.get("account_match") is not True):
                    return False
        return self.steps_truncated == 0

    def payload(self, summary=None):
        latest = {}
        for event in self.events:
            latest[event["port"]] = event
        derived = {
            "first_pass": len({event["port"] for event in self.events
                               if event["attempt"] <= 1}),
            "retried": len({event["port"] for event in self.events
                            if event["attempt"] >= 2
                            and event["code"] != "self_recovered"}),
            "self_recovered": len({event["port"] for event in self.events
                                    if event["code"] == "self_recovered"}),
            "succeeded": sum(
                event["outcome"] in {"succeeded", "self_recovered"}
                for event in latest.values()),
            "failed": sum(event["outcome"] in {"failed", "aborted"}
                          for event in latest.values()),
        }
        clean_summary = {}
        for key in ("first_pass", "retried", "self_recovered", "succeeded",
                    "failed"):
            try:
                clean_summary[key] = max(
                    0, int((summary or {}).get(key, derived[key])))
            except (TypeError, ValueError):
                clean_summary[key] = 0
        menu_paths = []
        if self.step_verification:
            successful = self._successful_steps()
            for port in self.expected_ports:
                row = successful.get((port, "config_open"))
                menu_paths.append({
                    "port": port,
                    "menu_path": (row or {}).get("menu_path"),
                })
        return {
            "schema_version": self.schema_version,
            "operation": self.operation,
            "events": list(self.events),
            "events_truncated": self.events_truncated,
            "screenshots_reserved": self._screenshots,
            "summary": clean_summary,
            "step_verification": self.step_verification,
            "required_steps": (list(REQUIRED_STEPS)
                               if self.step_verification else []),
            "expected_ports": list(self.expected_ports),
            "steps": list(self.steps),
            "steps_truncated": self.steps_truncated,
            "step_screenshots_reserved": self._step_screenshots,
            "menu_paths": menu_paths,
            "verification_complete": self.verification_complete(),
        }

    def write(self, summary=None, *, on_progress=None):
        log = on_progress or (lambda _message: None)
        tmp = None
        try:
            self.fill_not_attempted()
            self.run_dir.mkdir(parents=True, exist_ok=True)
            target = self.run_dir / "transcript.json"
            tmp = target.with_suffix(".json.tmp")
            data = json.dumps(self.payload(summary), indent=2, sort_keys=True)
            with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
            write_evidence_inventory(self.run_dir)
            return target
        except OSError:
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass
            log("fleet diagnostics unavailable (transcript write failed)")
            return None


def file_evidence(path, *, relative_to=None):
    """Return bounded file evidence or None when the file is unavailable."""
    candidate = Path(path)
    try:
        stat = candidate.stat()
        digest = hashlib.sha256()
        with open(candidate, "rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        shown = candidate
        if relative_to is not None:
            shown = candidate.resolve().relative_to(Path(relative_to).resolve())
        return {
            "path": shown.as_posix(),
            "bytes": int(stat.st_size),
            "sha256": digest.hexdigest(),
        }
    except (OSError, ValueError):
        return None


def evidence_matches(run_dir, expected):
    """Re-hash one recorded artifact; missing, escaped, or changed fails."""
    if not isinstance(expected, dict):
        return False
    rel = expected.get("path")
    if not isinstance(rel, str) or not rel:
        return False
    current = file_evidence(Path(run_dir) / rel, relative_to=run_dir)
    return current == {
        "path": rel,
        "bytes": expected.get("bytes"),
        "sha256": expected.get("sha256"),
    }


def write_evidence_inventory(run_dir):
    """Atomically inventory every durable run artifact except the inventory."""
    root = Path(run_dir)
    root.mkdir(parents=True, exist_ok=True)
    target = root / "evidence_inventory.json"
    rows = []
    for path in sorted(root.rglob("*")):
        if (not path.is_file() or path == target
                or path.name.endswith(".tmp")):
            continue
        evidence = file_evidence(path, relative_to=root)
        if evidence is not None:
            rows.append(evidence)
    payload = {
        "schema_version": 1,
        "run_dir": root.name,
        "files": rows,
    }
    tmp = target.with_suffix(".json.tmp")
    data = json.dumps(payload, indent=2, sort_keys=True)
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)
    return target
