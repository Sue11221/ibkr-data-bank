"""Thread-safe, headless core for the GUI-confirmed TWS restart flow.

Tk owns popup construction on the GUI thread.  This module owns every worker
thread transition after scheduling so timeout races, guard failures, launcher
entry, redaction, and cleanup can be tested without a desktop or TWS process.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import threading
import time



POPUP_DECISION_TIMEOUT_S = 180.0
REASON_CAP = 240
PROGRESS_CAP = 480
TIMESTAMP_CAP = 40

AUTO_CONFIRMED = "auto_confirmed"
USER_CONFIRMED = "user_confirmed"
NOT_NOW_DECLINED = "not_now_declined"
ESCAPE_DECLINED = "escape_declined"
WINDOW_CLOSED_DECLINED = "window_closed_declined"
POPUP_RENDER_ERROR = "popup_render_error"
POPUP_DECISION_TIMEOUT = "popup_decision_timeout"
EVENT_LOOP_STARVED = "event_loop_starved"

_DECISION_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_ACCOUNT_RE = re.compile(r"\b(?:DU|U)\d{3,12}\b", re.IGNORECASE)


def _bounded_text(value, fallback, cap=REASON_CAP):
    try:
        text = " ".join(str(value).split())
    except Exception:  # noqa: BLE001 - diagnostics must not escape
        text = ""
    if not text:
        text = fallback
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = _ACCOUNT_RE.sub("[redacted-account]", text)
    return text[:cap]


def _error_reason(prefix, exc):
    try:
        detail = str(exc)
    except Exception:  # noqa: BLE001 - an unprintable error stays bounded
        detail = "unprintable error"
    return _bounded_text(
        f"{prefix}: {type(exc).__name__}: {detail}", prefix)


def error_reason(prefix, exc):
    """Public bounded/redacted exception formatter for the Tk adapter."""
    return _error_reason(prefix, exc)


def _decision_token(value, fallback="invalid_popup_decision"):
    if isinstance(value, str) and _DECISION_RE.fullmatch(value):
        return value
    return fallback


def _utc_timestamp():
    return (datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"))


def _timestamp(timestamp_fn):
    try:
        value = str(timestamp_fn()).strip()
    except Exception:  # noqa: BLE001 - diagnostics must not alter decisions
        value = ""
    return value[:TIMESTAMP_CAP] or None


def outcome(decision, *, ok=False, attempted=False, reason=None):
    token = _decision_token(decision)
    return {
        "ok": bool(ok),
        "attempted": bool(attempted),
        "decision": token,
        "reason": _bounded_text(reason or token, token),
    }


def failed_outcomes(ports, decision, reason=None):
    return {
        int(port): outcome(decision, reason=reason)
        for port in dict.fromkeys(int(p) for p in ports)
    }


def _emit(say, value):
    message = _bounded_text(value, "restart diagnostic unavailable", PROGRESS_CAP)
    try:
        say(message)
    except Exception:  # noqa: BLE001 - progress display is diagnostic only
        pass


@dataclass(frozen=True)
class RestartDecisionSnapshot:
    scheduled_at: str | None
    callback_entered: bool
    callback_entered_at: str | None
    rendered: bool
    rendered_at: str | None
    countdown_armed_at: str | None
    settled: bool
    settled_at: str | None
    go: bool | None
    decision: str
    reason: str
    expired: bool
    expired_at: str | None


class RestartDecisionState:
    """One atomic terminal decision shared by the Tk and worker threads."""

    def __init__(self, timestamp_fn=None):
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._timestamp_fn = timestamp_fn or _utc_timestamp
        self._scheduled_at = _timestamp(self._timestamp_fn)
        self._callback_entered = False
        self._callback_entered_at = None
        self._rendered = False
        self._rendered_at = None
        self._countdown_armed_at = None
        self._settled = False
        self._settled_at = None
        self._go = None
        self._decision = "pending"
        self._reason = "waiting for restart popup decision"
        self._expired = False
        self._expired_at = None

    def snapshot(self):
        with self._lock:
            return RestartDecisionSnapshot(
                scheduled_at=self._scheduled_at,
                callback_entered=self._callback_entered,
                callback_entered_at=self._callback_entered_at,
                rendered=self._rendered,
                rendered_at=self._rendered_at,
                countdown_armed_at=self._countdown_armed_at,
                settled=self._settled,
                settled_at=self._settled_at,
                go=self._go,
                decision=self._decision,
                reason=self._reason,
                expired=self._expired,
                expired_at=self._expired_at,
            )

    def wait(self, timeout):
        return self._done.wait(timeout=max(0.0, float(timeout)))

    def mark_callback_entered(self):
        """Record that Tk began servicing the scheduled popup callback."""
        with self._lock:
            first = not self._callback_entered
            self._callback_entered = True
            if first:
                self._callback_entered_at = _timestamp(self._timestamp_fn)
            return first

    def mark_rendered(self):
        """Mark a completely laid-out warning visible unless timeout won."""
        with self._lock:
            if self._settled or self._expired:
                return False
            self._rendered = True
            if self._rendered_at is None:
                self._rendered_at = _timestamp(self._timestamp_fn)
            return True

    def mark_countdown_armed(self):
        """Record the first successfully scheduled countdown callback."""
        with self._lock:
            if (self._settled or self._expired or not self._rendered
                    or self._countdown_armed_at is not None):
                return False
            self._countdown_armed_at = _timestamp(self._timestamp_fn)
            return True

    def settle(self, go, decision, reason):
        """Set a GUI decision once. A confirmation requires a rendered popup."""
        decision_valid = (isinstance(decision, str)
                          and _DECISION_RE.fullmatch(decision) is not None)
        token = _decision_token(decision)
        detail = _bounded_text(reason, token)
        requested_go = type(go) is bool and go
        with self._lock:
            if self._settled or self._expired:
                return False
            if requested_go and not self._rendered:
                requested_go = False
                token = "popup_not_rendered"
                detail = "restart confirmation rejected because popup was not rendered"
            elif type(go) is not bool or not decision_valid:
                requested_go = False
                token = "invalid_popup_decision"
                detail = "restart popup returned an invalid decision"
            self._settled = True
            self._settled_at = _timestamp(self._timestamp_fn)
            self._go = requested_go
            self._decision = token
            self._reason = detail
            self._done.set()
            return True

    def expire(self):
        """Let the worker timeout win exactly once and forbid a late restart."""
        with self._lock:
            if self._settled or self._expired:
                return False
            self._settled = True
            stamp = _timestamp(self._timestamp_fn)
            self._settled_at = stamp
            self._go = False
            if self._callback_entered:
                self._decision = POPUP_DECISION_TIMEOUT
                self._reason = "restart popup decision timed out"
            else:
                self._decision = EVENT_LOOP_STARVED
                self._reason = (
                    "GUI event loop did not enter the restart popup callback")
            self._expired = True
            self._expired_at = stamp
            self._done.set()
            return True


def popup_timing(snapshot):
    """Return the bounded stage timeline persisted with each port outcome."""
    return {
        "scheduled_at": snapshot.scheduled_at,
        "callback_entered_at": snapshot.callback_entered_at,
        "rendered_at": snapshot.rendered_at,
        "countdown_armed_at": snapshot.countdown_armed_at,
        "settled_at": snapshot.settled_at,
        "expired_at": snapshot.expired_at,
    }


def _with_popup_timing(results, snapshot):
    timing = popup_timing(snapshot)
    for value in results.values():
        if isinstance(value, dict):
            value["popup_timing"] = dict(timing)
    return results


def _cleanup_call(cleanup, say, label):
    try:
        cleanup()
    except Exception as exc:  # noqa: BLE001 - preserve the settled result
        _emit(say, _error_reason(f"{label} cleanup failed", exc))


def render_popup_safely(state, build, cleanup, say=lambda _message: None):
    """Build a popup and mark it rendered only after `build` fully returns."""
    state.mark_callback_entered()
    if state.snapshot().settled:
        return False
    try:
        build()
    except Exception as exc:  # noqa: BLE001 - always release the worker wait
        _cleanup_call(cleanup, say, "popup")
        state.settle(False, POPUP_RENDER_ERROR,
                     _error_reason("restart popup render failed", exc))
        return False
    if state.mark_rendered():
        return True
    _cleanup_call(cleanup, say, "expired popup")
    return False


def _guard_aborted(guard):
    return bool(guard.aborted())


def _cancel_requested(cancelled):
    """Fail closed when a supplied fetch-cancel predicate is unavailable."""
    if cancelled is None:
        return False
    try:
        return bool(cancelled())
    except Exception:  # noqa: BLE001 - cancellation uncertainty blocks lifecycle work
        return True


def _drive_restarts(ports, emails, confirmation_decision, say, guard_mod,
                    relaunch_one, readiness_policy=None, cancelled=None,
                    port_up=None):
    results = {}
    guard = None
    minimize_entered = False

    try:
        if _cancel_requested(cancelled):
            return failed_outcomes(
                ports, "fetch_cancelled", "fetch cancelled before restart")
        try:
            guard = guard_mod.InputGuard(on_progress=lambda msg: _emit(say, msg),
                                         max_block_s=220)
        except Exception as exc:  # noqa: BLE001
            reason = _error_reason("input guard construction failed", exc)
            _emit(say, reason)
            return failed_outcomes(ports, "guard_init_error", reason)

        try:
            guard.start()
        except Exception as exc:  # noqa: BLE001
            reason = _error_reason("input guard start failed", exc)
            _emit(say, reason)
            return failed_outcomes(ports, "guard_start_error", reason)

        try:
            minimize_entered = True
            guard_mod.minimize_all()
        except Exception as exc:  # noqa: BLE001
            reason = _error_reason("window minimization failed", exc)
            _emit(say, reason)
            return failed_outcomes(ports, "minimize_error", reason)

        for index, port in enumerate(ports):
            remaining = ports[index:]
            if _cancel_requested(cancelled):
                reason = "fetch cancelled before launcher entry"
                results.update(failed_outcomes(
                    remaining, "fetch_cancelled", reason))
                _emit(say, reason)
                break
            try:
                aborted = _guard_aborted(guard)
            except Exception as exc:  # noqa: BLE001
                reason = _error_reason("input guard state check failed", exc)
                results.update(failed_outcomes(
                    remaining, "guard_state_error", reason))
                _emit(say, reason)
                break
            if aborted:
                reason = "restart aborted before launcher entry"
                results.update(failed_outcomes(
                    remaining, "restart_aborted", reason))
                _emit(say, reason)
                break

            try:
                email = emails[port]
            except Exception as exc:  # noqa: BLE001
                reason = _error_reason("fleet account mapping failed", exc)
                results[port] = outcome(
                    "fleet_mapping_error", reason=reason)
                _emit(say, reason)
                continue

            try:
                guard.block()
            except Exception as exc:  # noqa: BLE001
                reason = _error_reason("input guard block failed", exc)
                results.update(failed_outcomes(
                    remaining, "guard_block_error", reason))
                _emit(say, reason)
                break

            unblock_error = None
            stop_after_current = False
            try:
                listening = False
                if port_up is not None:
                    try:
                        listening = bool(port_up(port))
                    except Exception:  # noqa: BLE001 - failed probe is not proof of life
                        listening = False
                if listening:
                    results[port] = outcome(
                        "already_listening", ok=True, attempted=False,
                        reason="port already listening; relaunch skipped")
                    _emit(say, f"port {port} is already listening; "
                          "skipping relaunch")
                elif _cancel_requested(cancelled):
                    reason = "fetch cancelled before launcher entry"
                    results[port] = outcome(
                        "fetch_cancelled", attempted=False, reason=reason)
                    results.update(failed_outcomes(
                        ports[index + 1:], "fetch_cancelled", reason))
                    _emit(say, reason)
                    stop_after_current = True
                else:
                    def abort_check():
                        return (_guard_aborted(guard)
                                or _cancel_requested(cancelled))

                    try:
                        launch_kw = {
                            "on_progress": lambda msg: _emit(say, msg),
                            "abort_check": abort_check,
                        }
                        if readiness_policy is not None:
                            launch_kw["readiness_policy"] = readiness_policy
                        launched = relaunch_one(email, port, **launch_kw)
                        if (isinstance(launched, dict)
                                and type(launched.get("ok")) is bool):
                            ok = launched["ok"]
                            results[port] = outcome(
                                confirmation_decision if ok else "launcher_failed",
                                ok=ok, attempted=True,
                                reason=("launcher reported port listening" if ok else
                                        "launcher returned without a listening port"))
                        else:
                            results[port] = outcome(
                                "invalid_launcher_return", attempted=True,
                                reason="launcher returned an invalid outcome")
                    except Exception as exc:  # noqa: BLE001 - launcher was entered
                        try:
                            was_aborted = _guard_aborted(guard)
                        except Exception:  # noqa: BLE001
                            was_aborted = False
                        decision = ("fetch_cancelled"
                                    if _cancel_requested(cancelled) else
                                    "launcher_aborted" if was_aborted else
                                    "launcher_error")
                        reason = _error_reason("TWS launcher failed", exc)
                        results[port] = outcome(
                            decision, attempted=True, reason=reason)
                        _emit(say, f"port {port}: {reason}")
            finally:
                try:
                    guard.unblock()
                except Exception as exc:  # noqa: BLE001
                    unblock_error = exc

            if unblock_error is not None:
                reason = _error_reason("input guard unblock failed", unblock_error)
                _emit(say, reason)
                # The current port keeps its known result. No later launcher is
                # entered while cleanup of the current input block is uncertain.
                results.update(failed_outcomes(
                    ports[index + 1:], "guard_cleanup_error", reason))
                break
            if stop_after_current:
                break
    finally:
        if guard is not None:
            _cleanup_call(guard.stop, say, "input guard")
        if minimize_entered:
            _cleanup_call(guard_mod.restore_all, say, "window restore")

    succeeded = sum(1 for value in results.values() if value["ok"])
    attempted = sum(1 for value in results.values() if value["attempted"])
    _emit(say, f"restart finished: {succeeded}/{len(ports)} port(s) back up; "
               f"{attempted} launcher attempt(s)")
    return results


def restart_ports_with_confirmation(
        ports, emails, schedule_popup, say, guard_mod, relaunch_one,
        decision_timeout=POPUP_DECISION_TIMEOUT_S, readiness_policy=None,
        cancelled=None, port_up=None):
    """Schedule one popup, wait atomically, then run a confirmed restart batch."""
    plist = list(dict.fromkeys(int(port) for port in ports))
    if not plist:
        return {}

    state = RestartDecisionState()
    try:
        schedule_popup(state)
    except Exception as exc:  # noqa: BLE001
        reason = _error_reason("restart popup scheduling failed", exc)
        _emit(say, reason)
        return _with_popup_timing(
            failed_outcomes(plist, "popup_schedule_error", reason),
            state.snapshot())

    if cancelled is None:
        if not state.wait(decision_timeout):
            state.expire()
    else:
        deadline = time.monotonic() + max(0.0, float(decision_timeout))
        while not state.snapshot().settled:
            if _cancel_requested(cancelled):
                state.settle(
                    False, "fetch_cancelled",
                    "fetch cancelled while awaiting restart confirmation")
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                state.expire()
                break
            state.wait(min(0.1, remaining))
    settled = state.snapshot()
    if not settled.settled:
        reason = "restart popup ended without a terminal decision"
        _emit(say, reason)
        return _with_popup_timing(
            failed_outcomes(plist, "popup_state_error", reason), settled)
    if not settled.go:
        _emit(say, f"restart not started: {settled.decision}: {settled.reason}")
        return _with_popup_timing(
            failed_outcomes(plist, settled.decision, settled.reason), settled)
    if not settled.rendered:
        reason = "restart confirmation rejected because popup was not rendered"
        _emit(say, reason)
        return _with_popup_timing(
            failed_outcomes(plist, "popup_not_rendered", reason), settled)
    if _cancel_requested(cancelled):
        reason = "fetch cancelled before restart"
        _emit(say, reason)
        return _with_popup_timing(
            failed_outcomes(plist, "fetch_cancelled", reason), settled)

    _emit(say, f"restarting {len(plist)} port(s): "
               f"{', '.join(map(str, plist))}; press ESC to abort")
    results = _drive_restarts(
        plist, emails, settled.decision, say, guard_mod, relaunch_one,
        readiness_policy=readiness_policy, cancelled=cancelled,
        port_up=port_up)
    return _with_popup_timing(results, settled)
