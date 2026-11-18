"""Robust auto-launch + demo login for Interactive Brokers Trader Workstation,
single instance or several at once.

The app's data fetch (engine.stock_ibkr) can only talk to TWS *after* TWS is
running and logged in. This module removes that manual step for the **demo**
(free-trial) login, which needs no password: it launches TWS, drives the Java
"Login" window through the demo flow (Return to the demo -> email -> Try Demo),
and clears the stack of startup popups TWS throws up. It can also fan out N
instances serially, each in its own jtsConfigDir (TWS locks a config dir to
one process, so every instance needs a private one).

Why it is built the way it is — the demo login is a Java/Swing GUI, so *some*
automation is unavoidable (no config file or API submits it, and IBC only
drives the real username/password login, not the demo's email flow). But blind
coordinate clicking is fragile, so this hardens it:

  * each "Login" window is found by handle, pinned to a fixed origin and raised
    to the top of the z-order, so nothing overlaps it and its position is known;
  * every control is located by IMAGE-TEMPLATE match (pyautogui) within that
    window's rectangle, NOT by fixed screen coordinates;
  * popups are dismissed by posting WM_CLOSE to each window by handle, keeping
    the largest window *per process* (so multi-instance keeps every main window).

Windows-only. pyautogui is imported lazily so importing this module never
requires it (mirrors stock_ibkr's lazy ib_async import).
"""
from __future__ import annotations

from contextlib import contextmanager
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import tws_bringup as bringup
import tws_discovery

TWS_APP_DISCOVERY = True
# Backward-compatible explicit override/test seam.  Shipped installs are
# derived by tws_discovery.candidate_roots() instead of a project identity.
TWS_EXE = None
CONFIG_DIR_DEFAULT = r"C:\Jts"          # IBKR installer's universal default;
                                        # real lookups go through tws_discovery
# Derived from the RUNNING user's profile, never a stored identity (Row 52 M2a).
# Was hard-coded to one user's TwsMulti folder, which broke on any other account
# or machine. Path.home() re-resolves per machine, so a plain copy-paste of the
# project works wherever it lands.
MULTI_BASE_DEFAULT = str(Path.home() / "TwsMulti")
API_PORT_DEFAULT = 7497

PIN_X, PIN_Y = 80, 80
# cascade so several login windows are visible / driven without overlapping
CASCADE = [(40, 40), (140, 95), (240, 150), (340, 205), (440, 260)]
TEMPLATES = Path(__file__).resolve().parent / "tws_templates"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEBUG_SHOTS_DIR = (PROJECT_ROOT / "archive" / "_code_snapshots"
                   / "debug_screenshots")

# Control positions on the demo form, as offsets from the centre of the matched
# "Try Demo" button. The form layout is fixed once the window size is fixed, so
# these relative offsets are stable regardless of where the window sits.
EMAIL_OFFSET = (0, -109)       # email field sits directly above the button
NO_RADIO_OFFSET = (10, 130)    # "No" (don't contact me) sits below the button
LOGIN_FOCUS_TIMEOUT = 2.0
LOGIN_FOCUS_INTERVAL = 0.10
EMAIL_TYPE_INTERVAL = 0.12
EMAIL_CLEAR_ROUNDS = 2
EMAIL_INPUT_ROUNDS = 3


_NO_CLIPBOARD_TEXT = object()


# Only ONE restart may drive the GUI (foreground / image match) at a time;
# two concurrent relaunches would fight over the foreground and mis-click.
_GUI_LOCK = threading.Lock()
# A warning sweeper must never dismiss/foreground anything after a target has
# been proved and before the corresponding guarded input is sent.  GUI-driving
# code holds this re-entrant lock across that complete assertion+input section;
# the background sweeper only takes it non-blockingly and yields when busy.
_RESOURCE_WARNING_INPUT_LOCK = threading.RLock()


@contextmanager
def resource_warning_input_interlock():
    """Exclude the resource-warning sweeper across one guarded GUI input."""
    with _RESOURCE_WARNING_INPUT_LOCK:
        yield


class TwsLaunchError(bringup.StepFailure):
    """A typed launch/login failure (caller decides what to show)."""


def _existing_file(path):
    try:
        candidate = Path(path).expanduser().resolve(strict=True)
        return str(candidate) if candidate.is_file() else None
    except (TypeError, ValueError, OSError, RuntimeError):
        return None


def _resolved_tws_executable():
    """Resolve TWS at operation time; retain ``TWS_EXE`` as a test/fallback seam."""
    # Existing tests and callers may explicitly supply a live override.  The
    # shipped build leaves this unset and re-derives the app every operation.
    overridden = _existing_file(TWS_EXE)
    if overridden is not None:
        return overridden
    if TWS_APP_DISCOVERY:
        resolved, _source = tws_discovery.resolve_tws()
        if resolved is not None:
            return resolved
    return None


def _require_tws_executable():
    resolved = _resolved_tws_executable()
    if resolved is None:
        raise TwsLaunchError(
            "TWS not found by automatic discovery or a valid remembered "
            "selection; use 'Set IBKR app...' in the Storage tab",
            code="dependency_unavailable", phase="preflight")
    return resolved


DEFAULT_READINESS_POLICY = bringup.ReadinessPolicy()
MAX_LAUNCH_ATTEMPTS = 3


def _record_event(recorder, *, port, attempt, phase, code, elapsed_ms=0,
                  retryable=False, outcome="failed"):
    if recorder is not None:
        recorder.add_event(
            port=port, attempt=attempt, phase=phase, code=code,
            elapsed_ms=elapsed_ms, retryable=retryable, outcome=outcome)


def _screenshot_sink(recorder, port, attempt):
    if recorder is None:
        return None
    return lambda code: recorder.screenshot_path(
        port=port, attempt=attempt, code=code)


def _step_verification(recorder):
    return bool(recorder is not None
                and getattr(recorder, "step_verification", False))


def _main_hwnd_for_account(account):
    if not account:
        return None
    prefix = str(account) + " Interactive Brokers"
    return next(
        (row[0] for row in _all_tws_windows()
         if row[1].startswith(prefix)),
        None)


def _capture_step(recorder, *, port, attempt, step, hwnd=None,
                  menu_path=None, account_match=None):
    """Capture one mandatory proof screenshot and its hashed transcript row."""
    if not _step_verification(recorder):
        return
    if not hwnd:
        recorder.add_step(
            port=port, attempt=attempt, step=step, outcome="failed",
            menu_path=menu_path, account_match=account_match,
            failure_code="evidence_capture_failed")
        failure = TwsLaunchError(
            "required step window was unavailable for evidence",
            code="evidence_capture_failed", phase="evidence")
        failure.verification_step = step
        raise failure
    if (step in {"login_account", "handshake"}
            and account_match is not True):
        recorder.add_step(
            port=port, attempt=attempt, step=step, outcome="failed",
            menu_path=menu_path, account_match=False,
            failure_code=("handshake_failed" if step == "handshake"
                          else "account_window_absent"))
        failure = TwsLaunchError(
            "required account identity did not match",
            code=("handshake_failed" if step == "handshake"
                  else "account_window_absent"),
            phase=("handshake" if step == "handshake"
                   else "account_window"))
        failure.verification_step = step
        raise failure
    path = recorder.step_screenshot_path(
        port=port, attempt=attempt, step=step)
    if path is None:
        recorder.add_step(
            port=port, attempt=attempt, step=step, outcome="failed",
            menu_path=menu_path, account_match=account_match,
            failure_code="evidence_capture_failed")
        failure = TwsLaunchError(
            "required step screenshot limit was reached",
            code="evidence_capture_failed", phase="evidence")
        failure.verification_step = step
        raise failure
    try:
        if hwnd:
            foreground(hwnd)
            time.sleep(0.35)
        import pyautogui
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        pyautogui.screenshot(str(path))
        if not recorder.add_step(
                port=port, attempt=attempt, step=step, outcome="succeeded",
                screenshot=path, menu_path=menu_path,
                account_match=account_match):
            raise OSError("verification transcript step limit was reached")
    except TwsLaunchError:
        raise
    except Exception as exc:  # noqa: BLE001 - proof must fail closed
        recorder.add_step(
            port=port, attempt=attempt, step=step, outcome="failed",
            menu_path=menu_path, account_match=account_match,
            failure_code="evidence_capture_failed")
        failure = TwsLaunchError(
            "required step screenshot could not be captured",
            code="evidence_capture_failed", phase="evidence")
        failure.verification_step = step
        raise failure from exc


_FAILURE_STEP_BY_PHASE = {
    "preflight": "launch",
    "launch": "launch",
    "login_window": "launch",
    "login_drive": "login_account",
    "account_window": "login_account",
    "config_open": "config_open",
    "api_navigation": "api_enable",
    "enable": "api_enable",
    "controls": "port_bind",
    "port_readiness": "listener_up",
    "handshake": "handshake",
    "evidence": "handshake",
    "abort": "launch",
    "unknown": "launch",
}


def _capture_failed_step(recorder, *, port, attempt, exc, step=None,
                         hwnd=None):
    """Best-effort evidence for the exact failed step; preserve the cause."""
    if not _step_verification(recorder):
        return
    code = bringup.failure_code(exc)
    failed_step = step or getattr(exc, "verification_step", None)
    failed_step = failed_step or _FAILURE_STEP_BY_PHASE.get(
        bringup.failure_phase(exc), "launch")
    path = recorder.step_screenshot_path(
        port=port, attempt=attempt, step=failed_step)
    if path is not None:
        try:
            # A focus-loss frame must retain the window that stole focus.
            # Raising the target first would destroy the exact failure proof.
            if hwnd and code != "focus_lost":
                foreground(hwnd)
                time.sleep(0.35)
            import pyautogui
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            pyautogui.screenshot(str(path))
        except Exception:  # noqa: BLE001 - retain the original failure
            path = None
    recorder.add_step(
        port=port, attempt=attempt, step=failed_step, outcome="failed",
        screenshot=path, failure_code=code,
        foreground_holder=getattr(exc, "foreground_holder", None))


# --- API port probe --------------------------------------------------------

def port_open(port=API_PORT_DEFAULT, host="127.0.0.1", timeout=1.0):
    """True if something accepts a TCP connection on host:port (= TWS API up)."""
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        s.close()


# --- Win32 window control (ctypes, stdlib only) ----------------------------

def _win():
    import ctypes
    from ctypes import wintypes
    user32 = ctypes.WinDLL("user32", use_last_error=True)
    user32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    user32.GetWindowTextLengthW.argtypes = [wintypes.HWND]
    user32.GetForegroundWindow.restype = wintypes.HWND
    user32.IsWindowVisible.argtypes = [wintypes.HWND]
    user32.GetWindowThreadProcessId.argtypes = [
        wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    user32.GetWindowRect.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.RECT)]
    user32.SetWindowPos.argtypes = [
        wintypes.HWND, wintypes.HWND, ctypes.c_int, ctypes.c_int,
        ctypes.c_int, ctypes.c_int, ctypes.c_uint]
    user32.PostMessageW.argtypes = [
        wintypes.HWND, ctypes.c_uint, wintypes.WPARAM, wintypes.LPARAM]
    return ctypes, wintypes, user32


def _tws_pids():
    try:
        out = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq tws.exe", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, timeout=10).stdout
    except Exception:  # noqa: BLE001 — tasklist absent/odd
        return set()
    pids = set()
    for line in out.splitlines():
        cells = [c.strip('"') for c in line.split('","')]
        if len(cells) >= 2 and cells[1].isdigit():
            pids.add(int(cells[1]))
    return pids


def _rect(hwnd):
    ctypes, wintypes, user32 = _win()
    r = wintypes.RECT()
    user32.GetWindowRect(hwnd, ctypes.byref(r))
    return r.left, r.top, r.right, r.bottom


def _all_tws_windows():
    """[(hwnd, title, pid, w, h)] for visible top-level windows owned by tws.exe."""
    ctypes, wintypes, user32 = _win()
    pids = _tws_pids()
    out = []

    @ctypes.WINFUNCTYPE(ctypes.c_bool, wintypes.HWND, wintypes.LPARAM)
    def _cb(hwnd, _lp):
        if not user32.IsWindowVisible(hwnd):
            return True
        pid = wintypes.DWORD()
        user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        if not pids or pid.value not in pids:
            return True
        n = user32.GetWindowTextLengthW(hwnd)
        buf = ctypes.create_unicode_buffer(n + 1)
        user32.GetWindowTextW(hwnd, buf, n + 1)
        left, top, right, bottom = _rect(hwnd)
        out.append((hwnd, buf.value, pid.value, right - left, bottom - top))
        return True

    user32.EnumWindows(_cb, 0)
    return out


def find_login_windows():
    """HWNDs of all visible TWS 'Login' windows."""
    return [w[0] for w in _all_tws_windows() if w[1] == "Login"]


def find_login_window():
    ws = find_login_windows()
    return ws[0] if ws else None


def pin_window(hwnd, x=PIN_X, y=PIN_Y, topmost=False):
    """Restore, move to a fixed origin, raise it. With topmost=True it becomes
    always-on-top so a late-appearing window (e.g. the previous instance's main
    desktop) can't cover it mid-drive. Returns the pyautogui search region."""
    ctypes, wintypes, user32 = _win()
    SWP_NOSIZE, SWP_SHOWWINDOW = 0x0001, 0x0040
    insert_after = -1 if topmost else 0       # HWND_TOPMOST vs HWND_TOP
    user32.ShowWindow(hwnd, 9)  # SW_RESTORE
    user32.SetWindowPos(hwnd, insert_after, x, y, 0, 0, SWP_NOSIZE | SWP_SHOWWINDOW)
    try:
        user32.SetForegroundWindow(hwnd)
    except Exception:  # noqa: BLE001
        pass
    left, top, right, bottom = _rect(hwnd)
    return (left, top, right - left, bottom - top)


def foreground(hwnd):
    """Forcibly bring hwnd to the foreground. The AttachThreadInput trick beats
    the Windows foreground-lock so keystrokes/clicks land on it, not on whatever
    else (e.g. a browser) currently owns focus."""
    ctypes, wintypes, user32 = _win()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    user32.ShowWindow(hwnd, 9)  # SW_RESTORE
    fg = user32.GetForegroundWindow()
    cur = kernel32.GetCurrentThreadId()
    t_fg = user32.GetWindowThreadProcessId(fg, None)
    t_tg = user32.GetWindowThreadProcessId(hwnd, None)
    for th in {t_fg, t_tg}:
        user32.AttachThreadInput(cur, th, True)
    user32.BringWindowToTop(hwnd)
    user32.SetForegroundWindow(hwnd)
    for th in {t_fg, t_tg}:
        user32.AttachThreadInput(cur, th, False)


def foreground_window():
    """Return ``(hwnd, title)`` for the actual Win32 foreground window."""
    ctypes, _wintypes, user32 = _win()
    hwnd = int(user32.GetForegroundWindow() or 0)
    if not hwnd:
        return 0, ""
    length = max(0, int(user32.GetWindowTextLengthW(hwnd)))
    buf = ctypes.create_unicode_buffer(length + 1)
    user32.GetWindowTextW(hwnd, buf, length + 1)
    return hwnd, buf.value


def _require_login_foreground(
        hwnd, *, abort_check=None, timeout=LOGIN_FOCUS_TIMEOUT,
        interval=LOGIN_FOCUS_INTERVAL, clock=time.monotonic,
        sleep=time.sleep):
    """Boundedly prove the exact Login window before sending GUI input."""
    target = int(hwnd or 0)
    deadline = clock() + max(float(timeout), 0.05)
    holder = "hwnd=0 title=(foreground unavailable)"
    while target:
        _abort_if(abort_check)
        actual, title = foreground_window()
        actual = int(actual or 0)
        holder = bringup.redacted_evidence_text(
            f"hwnd={actual} title={title or '(untitled)'}")
        if actual == target:
            return target
        if clock() >= deadline:
            break
        foreground(target)
        sleep(max(float(interval), 0.01))

    failure = TwsLaunchError(
        f"login window lost foreground to {holder}",
        code="focus_lost", phase="login_drive")
    failure.foreground_holder = holder
    raise failure


def _minimize_others(keep_hwnd):
    """Minimize every TWS window except keep_hwnd, so the one we're driving is
    alone on screen and an image match can't pick up another instance."""
    _, _, user32 = _win()
    SW_MINIMIZE = 6
    for hwnd, _t, _p, _w, _h in _all_tws_windows():
        if hwnd != keep_hwnd:
            user32.ShowWindow(hwnd, SW_MINIMIZE)


def _restore_all_tws():
    """Un-minimize every TWS window (so the whole fleet is visible at the end)."""
    _, _, user32 = _win()
    SW_RESTORE = 9
    for hwnd, _t, _p, _w, _h in _all_tws_windows():
        user32.ShowWindow(hwnd, SW_RESTORE)


# --- popup dismissal (per process) -----------------------------------------

def dismiss_popups(on_progress=None, rounds=6, settle=1.2, min_w=120, min_h=80):
    """Close TWS startup popups by posting WM_CLOSE, keeping the LARGEST window
    of each process (its main trading window). No coordinates, and per-process
    so a 5-instance fleet keeps all five mains. Loops: closing one popup can
    reveal the next."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    _, _, user32 = _win()
    WM_CLOSE = 0x0010
    total = 0
    for _ in range(rounds):
        wins = [w for w in _all_tws_windows() if w[3] >= min_w and w[4] >= min_h]
        by_pid = {}
        for hwnd, title, pid, w, h in wins:
            by_pid.setdefault(pid, []).append((hwnd, title, w * h))
        to_close = []
        for lst in by_pid.values():
            lst.sort(key=lambda x: x[2], reverse=True)
            to_close.extend(lst[1:])          # everything but each pid's largest
        if not to_close:
            break
        for hwnd, title, _area in to_close:
            user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)
            total += 1
            log(f"closed popup: '{title or '(untitled)'}'")
        time.sleep(settle)
    return total


# IBKR demo nag popups that appear CENTERED after a short delay for each new
# instance and STEAL keyboard focus (so a following Alt+F lands on the popup,
# not the workstation). Matched by substring so a title variation still hits.
NUISANCE_POPUP_TITLES = ("Complete your Application",)

# TWS emits a second delayed warning when a dense demo fleet exhausts its
# preferred memory reserve.  Unlike "Complete your Application", the warning's
# title is just the owning account plus "IBKR Trader Workstation", so title-only
# matching would also catch API terms dialogs.  Its observed Swing frame is
# 642x341 at 100% scaling; compare X/Y scale factors instead of raw pixels so
# common DPI scaling remains safe while differently shaped dialogs are kept.
RESOURCE_WARNING_BASE_SIZE = (642.0, 341.0)
RESOURCE_WARNING_COMMON_SCALES = (0.75, 1.0, 1.25, 1.5, 1.75, 2.0)
RESOURCE_WARNING_SCALE_DELTA = 0.025
RESOURCE_WARNING_SCALE_TOLERANCE = 0.035


def is_resource_warning_window(window):
    """Return True only for the small, scale-normalized resource warning."""
    try:
        _hwnd, title, _pid, width, height = window
        account, separator, suffix = str(title or "").partition(" ")
        if (not separator or not account.upper().startswith("DU")
                or not account[2:].isdigit()
                or suffix.lower()
                != "ibkr trader workstation (demo system)"):
            return False
        scale_x = float(width) / RESOURCE_WARNING_BASE_SIZE[0]
        scale_y = float(height) / RESOURCE_WARNING_BASE_SIZE[1]
    except (TypeError, ValueError, ZeroDivisionError):
        return False
    scale = (scale_x + scale_y) / 2.0
    return (
        abs(scale_x - scale_y) <= RESOURCE_WARNING_SCALE_DELTA
        and min(abs(scale - expected)
                for expected in RESOURCE_WARNING_COMMON_SCALES)
        <= RESOURCE_WARNING_SCALE_TOLERANCE
    )


def close_resource_warnings(settle=0.2, on_result=None):
    """WM_CLOSE only delayed low-resource warnings, never main/config/T&C.

    ``on_result(window, outcome)`` is an optional evidence seam shared by the
    continuous sweeper.  A failed post is reported but remains non-fatal.
    """
    _, _, user32 = _win()
    WM_CLOSE = 0x0010
    closed = 0
    with resource_warning_input_interlock():
        for window in _all_tws_windows():
            if not is_resource_warning_window(window):
                continue
            outcome = "failed"
            try:
                posted = user32.PostMessageW(window[0], WM_CLOSE, 0, 0)
                # ctypes returns a BOOL; simple test doubles commonly return
                # None after recording the call, which still means no error.
                outcome = (
                    "succeeded" if posted is None or bool(posted)
                    else "failed")
                if outcome == "succeeded":
                    closed += 1
            except Exception:  # noqa: BLE001 - best-effort popup cleanup
                outcome = "failed"
            if on_result is not None:
                try:
                    on_result(window, outcome)
                except Exception:  # noqa: BLE001 - evidence is diagnostic
                    pass
    if closed and settle:
        time.sleep(settle)
    return closed


def close_nuisance_popups(titles=NUISANCE_POPUP_TITLES, settle=0.2):
    """FAST, targeted close of known nuisance popups by substring title — a
    single window scan + WM_CLOSE, with none of dismiss_popups' per-process
    largest-window logic or multi-round 1.2s settle (~0.2s vs ~7s). Cheap enough
    to call right before a focus-sensitive step (e.g. Alt+F) so the popup can't
    grab the keystroke. Returns how many were closed."""
    _, _, user32 = _win()
    WM_CLOSE = 0x0010
    lows = tuple(s.lower() for s in titles)
    closed = 0
    with resource_warning_input_interlock():
        for hwnd, title, _pid, _w, _h in _all_tws_windows():
            low = (title or "").lower()
            if any(s in low for s in lows):
                user32.PostMessageW(hwnd, WM_CLOSE, 0, 0)
                closed += 1
    if closed and settle:
        time.sleep(settle)
    return closed


class ResourceWarningSweeper:
    """Bounded bring-up-only watcher for exact resource-warning windows."""

    def __init__(self, *, recorder=None, on_progress=None, interval=0.3):
        self.recorder = recorder
        self.log = on_progress or (lambda _message: None)
        self.interval = max(0.05, float(interval))
        self._stop = threading.Event()
        self._thread = None
        self._count_lock = threading.Lock()
        self.dismissed = 0
        self.errors = 0

    def _safe_log(self, message):
        try:
            self.log(message)
        except Exception:  # noqa: BLE001 - progress is never a control plane
            pass

    def record_result(self, window, outcome):
        """Transcript callback shared by sweeps and guarded inline clears."""
        with self._count_lock:
            if str(outcome) == "succeeded":
                self.dismissed += 1
            else:
                self.errors += 1
        if str(outcome) != "succeeded":
            try:
                _hwnd, title, _pid, width, height = window
                detail = bringup.redacted_evidence_text(
                    f"{title} {width}x{height}")
            except (TypeError, ValueError):
                detail = "unavailable window"
            self._safe_log(
                f"resource-warning dismissal failed: {detail}")
        if self.recorder is not None:
            self.recorder.add_resource_warning_event(
                window, outcome=outcome)

    def tick(self):
        """Run one non-blocking sweep; return ``interlocked`` when input owns it."""
        if not _RESOURCE_WARNING_INPUT_LOCK.acquire(blocking=False):
            return "interlocked"
        try:
            try:
                close_resource_warnings(
                    settle=0, on_result=self.record_result)
                return "swept"
            except Exception as exc:  # noqa: BLE001 - never fail bring-up
                with self._count_lock:
                    self.errors += 1
                detail = bringup.redacted_evidence_text(exc)
                self._safe_log(
                    f"resource-warning sweep skipped after error: {detail}")
                return "error"
        finally:
            _RESOURCE_WARNING_INPUT_LOCK.release()

    def _run(self):
        while not self._stop.is_set():
            self.tick()
            if self._stop.wait(self.interval):
                break

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, daemon=True, name="resource-warning-sweep")
            self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        if not _step_verification(self.recorder):
            self._safe_log(
                "resource-warning sweeper dismissed "
                f"{self.dismissed} warning(s); errors={self.errors}")
        return self.dismissed

    @property
    def alive(self):
        return bool(self._thread is not None and self._thread.is_alive())

    def __enter__(self):
        return self.start()

    def __exit__(self, *_exc):
        self.stop()


def _resource_warning_sweeper(*, recorder=None, on_progress=None):
    """Factory seam keeps offline orchestration tests entirely headless."""
    return ResourceWarningSweeper(
        recorder=recorder, on_progress=on_progress, interval=0.3)


# --- image matching + clicking ---------------------------------------------

def _locate(name, region, timeout=20, interval=0.5, abort_check=None):
    import pyautogui
    pyautogui.FAILSAFE = False
    path = str(TEMPLATES / name)
    end = time.time() + timeout
    while time.time() < end:
        _abort_if(abort_check)          # ESC during a long locate releases input
        try:
            pt = pyautogui.locateCenterOnScreen(path, region=region)
        except Exception:  # noqa: BLE001 — ImageNotFound / transient screencap
            pt = None
        if pt:
            return int(pt.x), int(pt.y)
        time.sleep(interval)
    return None


def _click(x, y):
    import pyautogui
    pyautogui.moveTo(x, y, duration=0.15)
    time.sleep(0.1)
    pyautogui.click()


def _dbg(tag, on):
    if not on:
        return
    try:
        import pyautogui
        DEBUG_SHOTS_DIR.mkdir(parents=True, exist_ok=True)
        pyautogui.screenshot(str(DEBUG_SHOTS_DIR / f"_dbg_{tag}.png"))
    except Exception:  # noqa: BLE001
        pass


# --- launching + driving the login -----------------------------------------

def _launch_proc(config_dir, seed=True, fresh=False, *, tws_exe=None):
    """Start TWS on its own config dir. `fresh` wipes the dir first; `seed`
    copies jts.ini in so it opens on the Paper/demo login we automate (never
    touches the real C:\\Jts)."""
    executable = Path(tws_exe or _require_tws_executable())
    install_dir = executable.parent
    p = Path(config_dir)
    if fresh and p.exists():
        target = p.resolve()
        protected = (Path(CONFIG_DIR_DEFAULT).resolve(),
                     install_dir.resolve(), PROJECT_ROOT.resolve())
        # A newly selected install may live somewhere other than C:\Jts.  Never
        # let the legacy fresh-config cleanup erase that install, this project,
        # or a parent that contains either one.
        if not any(target == item or target in item.parents
                   for item in protected):
            shutil.rmtree(p, ignore_errors=True)
    p.mkdir(parents=True, exist_ok=True)
    if seed:
        src, dst = install_dir / "jts.ini", p / "jts.ini"
        try:
            if src.exists() and src.resolve() != dst.resolve():
                shutil.copyfile(src, dst)
        except Exception:  # noqa: BLE001
            pass
    return subprocess.Popen(
        [str(executable), f"-J-DjtsConfigDir={config_dir}"],
        cwd=str(install_dir))


def _abort_if(abort_check):
    """Raise TwsLaunchError if the cooperative abort (ESC) was requested. The
    long wait loops call this so a mid-restart ESC is honored within ~2s
    instead of after a full appear/login/port timeout."""
    if abort_check is not None and abort_check():
        raise TwsLaunchError("aborted by user (ESC)", code="aborted",
                             phase="abort")


def _sleep_or_abort(seconds, abort_check, step=0.25):
    """Sleep up to `seconds`, but check `abort_check` every `step` so an ESC
    raises promptly (within ~`step`) instead of after the full sleep. While the
    input guard holds BlockInput, only the OWNING worker thread can release it,
    so it must reach an abort point quickly — hence these short polled sleeps in
    the input-held phases (close / config-free / login drive)."""
    end = time.time() + seconds
    _abort_if(abort_check)
    while time.time() < end:
        time.sleep(min(step, max(0.0, end - time.time())))
        _abort_if(abort_check)


class _WindowsClipboard:
    """Small CF_UNICODETEXT clipboard seam with best-effort custody.

    Only text can be restored portably through this seam.  An existing
    non-text clipboard is therefore left untouched and reported unavailable to
    the caller; when it was actually empty, restoration best-effort removes the
    temporary email.  All failures stay private: clipboard content is never
    embedded in an exception or a log.
    """

    CF_UNICODETEXT = 13
    GMEM_MOVEABLE = 0x0002
    TEXT_FORMATS = frozenset({1, 7, 13, 16})  # TEXT, OEMTEXT, UNICODETEXT, LOCALE

    def __init__(self):
        import ctypes
        from ctypes import wintypes

        self.ctypes = ctypes
        self.user32 = ctypes.WinDLL("user32", use_last_error=True)
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

        self.user32.OpenClipboard.argtypes = [wintypes.HWND]
        self.user32.OpenClipboard.restype = wintypes.BOOL
        self.user32.CloseClipboard.restype = wintypes.BOOL
        self.user32.EmptyClipboard.restype = wintypes.BOOL
        self.user32.CountClipboardFormats.restype = ctypes.c_int
        self.user32.EnumClipboardFormats.argtypes = [wintypes.UINT]
        self.user32.EnumClipboardFormats.restype = wintypes.UINT
        self.user32.IsClipboardFormatAvailable.argtypes = [wintypes.UINT]
        self.user32.IsClipboardFormatAvailable.restype = wintypes.BOOL
        self.user32.GetClipboardData.argtypes = [wintypes.UINT]
        self.user32.GetClipboardData.restype = ctypes.c_void_p
        self.user32.SetClipboardData.argtypes = [wintypes.UINT, ctypes.c_void_p]
        self.user32.SetClipboardData.restype = ctypes.c_void_p
        self.kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
        self.kernel32.GlobalAlloc.restype = ctypes.c_void_p
        self.kernel32.GlobalFree.argtypes = [ctypes.c_void_p]
        self.kernel32.GlobalFree.restype = ctypes.c_void_p
        self.kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
        self.kernel32.GlobalLock.restype = ctypes.c_void_p
        self.kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
        self.kernel32.GlobalUnlock.restype = wintypes.BOOL

    def _open(self):
        for _ in range(8):
            if self.user32.OpenClipboard(None):
                return
            time.sleep(0.025)
        raise OSError("clipboard unavailable")

    def _read_open_text(self):
        expected = max(0, int(self.user32.CountClipboardFormats()))
        formats = set()
        current = 0
        while True:
            current = int(self.user32.EnumClipboardFormats(current) or 0)
            if not current:
                break
            formats.add(current)
        if len(formats) != expected or formats - self.TEXT_FORMATS:
            raise OSError("clipboard contains unsupported prior data")
        if not self.user32.IsClipboardFormatAvailable(self.CF_UNICODETEXT):
            if formats:
                raise OSError("clipboard contains unsupported prior data")
            return _NO_CLIPBOARD_TEXT
        handle = self.user32.GetClipboardData(self.CF_UNICODETEXT)
        if not handle:
            return _NO_CLIPBOARD_TEXT
        pointer = self.kernel32.GlobalLock(handle)
        if not pointer:
            return _NO_CLIPBOARD_TEXT
        try:
            return self.ctypes.wstring_at(pointer)
        finally:
            self.kernel32.GlobalUnlock(handle)

    def _write_open_text(self, value):
        if value is _NO_CLIPBOARD_TEXT:
            if not self.user32.EmptyClipboard():
                raise OSError("clipboard clear failed")
            return

        buffer = self.ctypes.create_unicode_buffer(str(value))
        handle = self.kernel32.GlobalAlloc(
            self.GMEM_MOVEABLE, self.ctypes.sizeof(buffer))
        if not handle:
            raise OSError("clipboard allocation failed")
        pointer = self.kernel32.GlobalLock(handle)
        if not pointer:
            self.kernel32.GlobalFree(handle)
            raise OSError("clipboard allocation lock failed")
        try:
            self.ctypes.memmove(
                pointer, self.ctypes.addressof(buffer), self.ctypes.sizeof(buffer))
        finally:
            self.kernel32.GlobalUnlock(handle)

        if not self.user32.EmptyClipboard():
            self.kernel32.GlobalFree(handle)
            raise OSError("clipboard clear failed")
        if not self.user32.SetClipboardData(self.CF_UNICODETEXT, handle):
            self.kernel32.GlobalFree(handle)
            raise OSError("clipboard write failed")
        # SetClipboardData owns handle after success.

    def replace_text(self, value):
        self._open()
        try:
            previous = self._read_open_text()
            try:
                self._write_open_text(value)
            except Exception:
                try:
                    self._write_open_text(previous)
                except Exception:  # noqa: BLE001 - best-effort rollback
                    pass
                raise
            return previous
        finally:
            self.user32.CloseClipboard()

    def restore(self, previous):
        self._open()
        try:
            self._write_open_text(previous)
        finally:
            self.user32.CloseClipboard()


@contextmanager
def _staged_clipboard_text(value, clipboard_factory=None):
    """Yield whether a temporary Unicode clipboard value was staged.

    The injected factory keeps selftests off the real clipboard.  Staging and
    restoration are deliberately non-fatal so login can use its one guarded
    slow-type fallback when Windows denies clipboard access.
    """
    clipboard = None
    previous = None
    try:
        clipboard = (clipboard_factory or _WindowsClipboard)()
        previous = clipboard.replace_text(value)
    except Exception:  # noqa: BLE001 - clipboard is an optional input path
        yield False
        return
    try:
        yield True
    finally:
        try:
            clipboard.restore(previous)
        except Exception:  # noqa: BLE001 - restoration is best-effort
            pass


def _wait_new_login(before, timeout, abort_check=None):
    """Return a 'Login' HWND that appeared since `before` (set), or None."""
    end = time.time() + timeout
    before = set(before)
    while time.time() < end:
        _abort_if(abort_check)
        new = set(find_login_windows()) - before
        if new:
            return sorted(new)[0]
        time.sleep(1)
    return None


def _drive_login(hwnd, email, pin_pos, log, debug, tag="", abort_check=None,
                 *, clipboard_factory=None):
    """Pin the given Login window and drive: Return to the demo -> email -> No
    -> Try Demo. Raises TwsLaunchError if a control can't be found. `abort_check`
    is polled between steps (and inside each _locate) so an ESC while input is
    frozen releases within ~1s instead of after the multi-step drive."""
    _abort_if(abort_check)
    _minimize_others(hwnd)          # other instances out of the way -> clean match
    pin_window(hwnd, *pin_pos, topmost=True)
    time.sleep(1.0)                 # let the form finish rendering
    region = pin_window(hwnd, *pin_pos, topmost=True)
    _dbg(f"{tag}_01_login", debug)

    if not _locate("try_demo.png", region, timeout=3, abort_check=abort_check):
        link = _locate("return_demo.png", region, timeout=25,
                       abort_check=abort_check)
        if not link:
            raise TwsLaunchError(f"[{tag or 'tws'}] no 'Return to the demo' link")
        _click(*link)
        log(f"[{tag}] clicked 'Return to the demo'")
        time.sleep(0.5)
        region = pin_window(hwnd, *pin_pos)
    _dbg(f"{tag}_02_demo_form", debug)

    _abort_if(abort_check)
    tryd = _locate("try_demo.png", region, timeout=25, abort_check=abort_check)
    if not tryd:
        raise TwsLaunchError(f"[{tag or 'tws'}] demo form ('Try Demo') not found")
    import pyautogui
    email_pos = (tryd[0] + EMAIL_OFFSET[0],
                 tryd[1] + EMAIL_OFFSET[1])
    no_pos = (tryd[0] + NO_RADIO_OFFSET[0],
              tryd[1] + NO_RADIO_OFFSET[1])
    with resource_warning_input_interlock():
        for input_round in range(1, EMAIL_INPUT_ROUNDS + 1):
            # Re-focus the Swing combobox on every in-window attempt, then
            # repeat the proven double-clear before delivering new input.
            _require_login_foreground(hwnd, abort_check=abort_check)
            _click(*email_pos)
            _sleep_or_abort(0.20, abort_check)
            for _ in range(EMAIL_CLEAR_ROUNDS):
                _require_login_foreground(hwnd, abort_check=abort_check)
                pyautogui.hotkey("ctrl", "a")
                _sleep_or_abort(0.20, abort_check)
                _require_login_foreground(hwnd, abort_check=abort_check)
                pyautogui.press("backspace")
                _sleep_or_abort(0.20, abort_check)

            # One Ctrl+V is atomic from the Swing field's point of view.  The
            # field is clicked again immediately before it, while the exact
            # Login HWND and resource-warning interlock are still held.
            with _staged_clipboard_text(
                    email, clipboard_factory=clipboard_factory) as can_paste:
                _require_login_foreground(hwnd, abort_check=abort_check)
                _click(*email_pos)
                _require_login_foreground(hwnd, abort_check=abort_check)
                if can_paste:
                    pyautogui.hotkey("ctrl", "v")
                    input_method = "clipboard paste"
                else:
                    pyautogui.typewrite(email, interval=EMAIL_TYPE_INTERVAL)
                    input_method = "slow typing fallback"
                # Let Swing consume Ctrl+V before the prior clipboard is
                # restored; aborts still unwind through the restore finally.
                _sleep_or_abort(0.20 if can_paste else 0.60, abort_check)

            log(f"[{tag}] entered demo email via {input_method} "
                f"(round {input_round})")
            _dbg(f"{tag}_03_email_r{input_round}", debug)

            _require_login_foreground(hwnd, abort_check=abort_check)
            _click(*no_pos)
            _sleep_or_abort(0.50, abort_check)
            # This template is the enabled red button.  A malformed/partial
            # email leaves it disabled; never click stale coordinates.
            submit = _locate(
                "try_demo.png", region, timeout=5,
                abort_check=abort_check)
            if submit:
                _require_login_foreground(hwnd, abort_check=abort_check)
                _click(*submit)
                log(f"[{tag}] clicked 'Try Demo'")
                _dbg(f"{tag}_04_trydemo", debug)
                return

            # Clipboard denial gets exactly one slow fallback round.  A failed
            # paste may retry in-window up to the bounded three rounds.
            if not can_paste:
                break
            if input_round < EMAIL_INPUT_ROUNDS:
                log(f"[{tag}] demo email input round {input_round} was not "
                    "accepted; retrying in the same Login window")

    raise TwsLaunchError(
        f"[{tag or 'tws'}] demo email entry was not accepted",
        code="login_drive_failed", phase="login_drive")


def _close_all_tws(log):
    if not _tws_pids():
        return
    log("closing existing TWS instance(s)…")
    _, _, user32 = _win()
    for hwnd, _t, _p, _w, _h in _all_tws_windows():
        user32.PostMessageW(hwnd, 0x0010, 0, 0)   # WM_CLOSE
    time.sleep(3)
    try:
        subprocess.run(["taskkill", "/IM", "tws.exe", "/F"],
                       capture_output=True, timeout=15)
    except Exception:  # noqa: BLE001
        pass
    time.sleep(2)


def launch_one(email, config_dir, pin_pos, on_progress=None, debug=False,
               tag="", appear_timeout=90, fresh=False, readiness_policy=None,
               abort_check=None, tws_exe=None, recorder=None,
               evidence_port=0, evidence_attempt=1):
    """Launch one TWS instance and log it into the demo with `email`."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    policy = readiness_policy or DEFAULT_READINESS_POLICY
    _abort_if(abort_check)
    before = find_login_windows()
    log(f"[{tag}] launching TWS  (config: {config_dir})")
    _launch_proc(config_dir, fresh=fresh, tws_exe=tws_exe)
    hwnd = _wait_new_login(before, appear_timeout, abort_check=abort_check)
    if not hwnd:
        raise TwsLaunchError(f"[{tag or 'tws'}] login window never appeared",
                             code="login_window_absent", phase="login_window")
    if _step_verification(recorder):
        pin_window(hwnd, *pin_pos, topmost=True)
        _capture_step(
            recorder, port=evidence_port, attempt=evidence_attempt,
            step="launch", hwnd=hwnd)
    last = None
    for attempt in range(3):
        try:
            _drive_login(hwnd, email, pin_pos, log, debug, tag,
                         abort_check=abort_check)
            break
        except TwsLaunchError as exc:
            last = exc
            log(f"[{tag}] attempt {attempt + 1} failed ({exc}); retrying")
            _sleep_or_abort(2, abort_check)
    else:
        if bringup.failure_code(last) == "aborted":
            raise last
        raise TwsLaunchError(str(last), code="login_drive_failed",
                             phase="login_drive") from last
    end = time.time() + 40
    while time.time() < end and hwnd in find_login_windows():
        _sleep_or_abort(1, abort_check)
    stable = _wait_account_for_config_stable(
        config_dir, policy.account_timeout, abort_check=abort_check,
        policy=policy)
    if not stable.ok:
        raise TwsLaunchError(
            f"[{tag or 'tws'}] account window for {config_dir} never appeared",
            code="account_window_absent", phase="account_window")
    account_hwnd = _main_hwnd_for_account(stable.value)
    _capture_step(
        recorder, port=evidence_port, attempt=evidence_attempt,
        step="login_account", hwnd=account_hwnd,
        account_match=bool(
            account_hwnd and _account_for_config_now(config_dir)
            == stable.value))
    log(f"[{tag}] login ready after {stable.elapsed_ms} ms")
    return True


def _launch_many_impl(emails, base_dir=MULTI_BASE_DEFAULT, on_progress=None,
                      debug=False, close_existing=True, *,
                      retry_transient=True, recorder=None, ports=None,
                      readiness_policy=None, abort_check=None,
                      halt_on_failure=None):
    """Launch demo instances serially with bounded post-pass retries.

    A step-verification recorder switches only this run to immediate per-member
    retries and a first-terminal-failure halt; ordinary callers retain the
    reviewed post-pass isolation behavior.
    """
    if os.name != "nt":
        raise TwsLaunchError("TWS auto-launch is Windows-only",
                             code="dependency_unavailable", phase="preflight")
    log = on_progress or (lambda m: print("[tws_launch]", m))
    policy = readiness_policy or DEFAULT_READINESS_POLICY
    tws_exe = _require_tws_executable()
    emails = list(emails)
    port_ids = [0] * len(emails) if ports is None else [int(p) for p in ports]
    if len(port_ids) != len(emails):
        raise ValueError("ports must match emails when provided")
    halt_on_failure = (_step_verification(recorder)
                       if halt_on_failure is None
                       else bool(halt_on_failure))
    if close_existing:
        _close_all_tws(log)

    results = {}
    failed = []

    def attempt_one(i, email, cfg, pin, attempt):
        started = time.monotonic()
        try:
            launch_one(email, cfg, pin, on_progress=log, debug=debug, tag=
                       email.split("@")[0], fresh=True,
                       readiness_policy=policy, abort_check=abort_check,
                       tws_exe=tws_exe, recorder=recorder,
                       evidence_port=port_ids[i],
                       evidence_attempt=attempt)
            results[email] = "launched"
            _record_event(
                recorder, port=port_ids[i], attempt=attempt, phase="launch",
                code="ok", elapsed_ms=(time.monotonic() - started) * 1000,
                outcome="succeeded")
            return None
        except Exception as raw_exc:  # noqa: BLE001 - isolate per instance
            exc = (raw_exc if isinstance(raw_exc, TwsLaunchError) else
                   TwsLaunchError(
                       f"unexpected launch failure: {raw_exc}",
                       code=bringup.failure_code(raw_exc),
                       phase=bringup.failure_phase(raw_exc)))
            results[email] = f"FAILED: {exc}"
            log(str(exc))
            _record_event(
                recorder, port=port_ids[i], attempt=attempt,
                phase=bringup.failure_phase(exc),
                code=bringup.failure_code(exc),
                elapsed_ms=(time.monotonic() - started) * 1000,
                retryable=bringup.is_retryable(exc), outcome="failed")
            _capture_failed_step(
                recorder, port=port_ids[i], attempt=attempt, exc=exc)
            return exc

    if halt_on_failure:
        for i, email in enumerate(emails):
            cfg = config_dir_for(email, base_dir)
            pin = CASCADE[i % len(CASCADE)]
            exc = attempt_one(i, email, cfg, pin, 1)
            attempt = 1
            while (exc is not None and retry_transient
                   and bringup.is_retryable(exc)
                   and attempt < MAX_LAUNCH_ATTEMPTS):
                attempt += 1
                log(f"[{email.split('@')[0]}] retrying verified launch "
                    f"(attempt {attempt}/{MAX_LAUNCH_ATTEMPTS})")
                try:
                    close_one(cfg, on_progress=log)
                    free = _ensure_config_free(cfg, on_progress=log)
                except Exception as cleanup_exc:  # noqa: BLE001
                    free = False
                    log(f"[{email.split('@')[0]}] retry cleanup failed: "
                        f"{cleanup_exc}")
                if not free:
                    exc = TwsLaunchError(
                        "config directory remained busy before retry",
                        code="config_dir_busy", phase="cleanup")
                    results[email] = f"FAILED: {exc}"
                    _record_event(
                        recorder, port=port_ids[i], attempt=attempt,
                        phase="cleanup", code=exc.code, outcome="failed")
                    _capture_failed_step(
                        recorder, port=port_ids[i], attempt=attempt, exc=exc,
                        step="launch")
                    break
                exc = attempt_one(i, email, cfg, pin, attempt)
            if exc is not None:
                try:
                    close_one(cfg, on_progress=log)
                except Exception as cleanup_exc:  # noqa: BLE001
                    log(f"[{email.split('@')[0]}] terminal cleanup failed: "
                        f"{cleanup_exc}")
                for remaining in emails[i + 1:]:
                    results[remaining] = (
                        f"NOT ATTEMPTED: halted after port {port_ids[i]}")
                log(f"verified launch halted at port {port_ids[i]}")
                break
        _restore_all_tws()
        time.sleep(2)
        log(f"dismissed {dismiss_popups(on_progress=log)} popup(s) "
            "across the verified fleet")
        log(f"running tws processes now: {len(_tws_pids())}")
        return results

    for i, email in enumerate(emails):
        cfg = config_dir_for(email, base_dir)
        pin = CASCADE[i % len(CASCADE)]
        exc = attempt_one(i, email, cfg, pin, 1)
        if exc is not None:
            failed.append((i, email, cfg, pin, exc))

    log("letting instances settle…")
    time.sleep(20)
    pending = [item for item in failed if bringup.is_retryable(item[4])]
    retry_aborted = False
    if retry_transient:
        for attempt in range(2, MAX_LAUNCH_ATTEMPTS + 1):
            if not pending:
                break
            next_pending = []
            for pos, (i, email, cfg, pin, exc) in enumerate(pending):
                try:
                    recovered = _wait_account_for_config_stable(
                        cfg, min(5.0, policy.account_timeout), policy=policy,
                        abort_check=abort_check)
                except Exception as recovery_exc:  # noqa: BLE001
                    if bringup.failure_code(recovery_exc) != "aborted":
                        raise
                    for _i, remaining, _cfg, _pin, _exc in pending[pos:]:
                        results[remaining] = "FAILED: aborted by user"
                    retry_aborted = True
                    break
                if recovered.ok:
                    results[email] = "launched"
                    _record_event(
                        recorder, port=port_ids[i], attempt=attempt,
                        phase="account_window", code="self_recovered",
                        elapsed_ms=recovered.elapsed_ms,
                        outcome="self_recovered")
                    continue
                log(f"[{email.split('@')[0]}] retrying transient launch "
                    f"failure (attempt {attempt}/{MAX_LAUNCH_ATTEMPTS})")
                try:
                    close_one(cfg, on_progress=log, abort_check=abort_check)
                    free = _ensure_config_free(
                        cfg, on_progress=log, abort_check=abort_check)
                except Exception as cleanup_exc:  # noqa: BLE001
                    if bringup.failure_code(cleanup_exc) == "aborted":
                        for _i, remaining, _cfg, _pin, _exc in pending[pos:]:
                            results[remaining] = "FAILED: aborted by user"
                        retry_aborted = True
                        break
                    free = False
                    log(f"[{email.split('@')[0]}] retry cleanup failed: "
                        f"{cleanup_exc}")
                if not free:
                    busy = TwsLaunchError(
                        "config directory remained busy before retry",
                        code="config_dir_busy", phase="cleanup")
                    results[email] = f"FAILED: {busy}"
                    _record_event(
                        recorder, port=port_ids[i], attempt=attempt,
                        phase="cleanup", code=busy.code, outcome="failed")
                    continue
                retry_exc = attempt_one(i, email, cfg, pin, attempt)
                if retry_exc is not None and bringup.is_retryable(retry_exc):
                    next_pending.append((i, email, cfg, pin, retry_exc))
            pending = next_pending
            if retry_aborted:
                break

    # A final transient failure may have left a half-launched TWS process
    # holding its private config directory. Clean only those exhausted members;
    # successful and permanently failed members retain their existing behavior.
    if retry_transient and pending and not retry_aborted:
        for _i, email, cfg, _pin, _exc in pending:
            try:
                close_one(cfg, on_progress=log, abort_check=abort_check)
            except Exception as cleanup_exc:  # noqa: BLE001 - diagnostic only
                log(f"[{email.split('@')[0]}] final launch cleanup failed: "
                    f"{cleanup_exc}")
    _restore_all_tws()                       # un-minimize so the user sees them
    time.sleep(2)
    log(f"dismissed {dismiss_popups(on_progress=log)} popup(s) across the fleet")
    _dbg("fleet_final", debug)
    log(f"running tws processes now: {len(_tws_pids())}")
    return results


def launch_many(emails, base_dir=MULTI_BASE_DEFAULT, on_progress=None,
                debug=False, close_existing=True, *, retry_transient=True,
                recorder=None, ports=None, readiness_policy=None,
                abort_check=None, halt_on_failure=None,
                memory_cap_result=None):
    """Run launch/login while continuously clearing exact resource warnings."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    if memory_cap_result is not None:
        try:
            evidence = (memory_cap_result.evidence() if callable(getattr(
                memory_cap_result, "evidence", None)) else
                dict(memory_cap_result))
        except Exception:  # noqa: BLE001 - cap optimization never gates launch
            evidence = {"outcome": "warning", "verified": False,
                        "detail": "memory-cap result was unavailable"}
        if recorder is not None:
            try:
                recorder.add_memory_cap_event(memory_cap_result)
            except Exception as exc:  # noqa: BLE001 - evidence is nonfatal
                log("TWS memory-cap evidence warning: "
                    f"{type(exc).__name__}")
        outcome = str(evidence.get("outcome") or "warning")
        detail = bringup.redacted_evidence_text(evidence.get("detail"))
        if bool(evidence.get("verified")):
            cap_option = (bringup.redacted_evidence_text(
                evidence.get("cap_option"), limit=40) or "managed cap")
            log(f"TWS memory cap {outcome}: {cap_option} verified")
        else:
            log("TWS memory-cap warning: " +
                (detail or "cap not verified; conservative AUTO retained"))
    with _resource_warning_sweeper(
            recorder=recorder, on_progress=log):
        return _launch_many_impl(
            emails, base_dir=base_dir, on_progress=on_progress, debug=debug,
            close_existing=close_existing, retry_transient=retry_transient,
            recorder=recorder, ports=ports,
            readiness_policy=readiness_policy, abort_check=abort_check,
            halt_on_failure=halt_on_failure)


# --- per-instance restart (daily-logout recovery) --------------------------

def config_dir_for(email, base_dir=MULTI_BASE_DEFAULT):
    """The private jtsConfigDir launch_many gives an email (inst_<local part>).
    Deterministic, so a restart targets the SAME instance it launched."""
    return str(Path(base_dir) / f"inst_{email.split('@')[0]}")


def email_from_config_dir(config_dir):
    """Reverse config_dir_for: inst_<local> -> <local>@gmail.com."""
    name = Path(config_dir).name
    local = name[5:] if name.startswith("inst_") else name
    return f"{local}@gmail.com"


# --- the fleet manifest: which instance serves which port ------------------
# The demo assigns DUxxx accounts unpredictably and the bring-up enables ports
# in sorted-ACCOUNT order, so port 2000 is NOT necessarily inst_a. This map
# (written when a port is enabled or restarted) records the truth so a restart
# always relaunches the RIGHT config dir instead of guessing by index.

def fleet_path(base_dir=MULTI_BASE_DEFAULT):
    return Path(base_dir) / "fleet.json"


def load_fleet(base_dir=MULTI_BASE_DEFAULT):
    """{port(int): {'email','config_dir','account'}} or {} if never recorded."""
    import json
    try:
        data = json.loads(fleet_path(base_dir).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    out = {}
    for k, v in (data or {}).items():
        try:
            out[int(k)] = v
        except (TypeError, ValueError):
            continue
    return out


def save_fleet(fleet, base_dir=MULTI_BASE_DEFAULT):
    """Atomically persist the fleet map (best effort — never raises)."""
    import json
    p = fleet_path(base_dir)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({str(k): v for k, v in fleet.items()},
                                  indent=1), encoding="utf-8")
        tmp.replace(p)
    except OSError:
        pass


def record_member(port, email=None, config_dir=None, account=None,
                  base_dir=MULTI_BASE_DEFAULT):
    """Upsert one port's instance into the fleet map. Only non-None fields are
    written, so a later restart can refresh the account without losing the
    email/config dir. Returns the merged member."""
    fleet = load_fleet(base_dir)
    member = dict(fleet.get(int(port), {}))
    if email is not None:
        member["email"] = email
    if config_dir is not None:
        member["config_dir"] = str(config_dir)
    if account is not None:
        member["account"] = account
    fleet[int(port)] = member
    save_fleet(fleet, base_dir)
    return member


def email_for_port(port, base_dir=MULTI_BASE_DEFAULT, fallback_index=None):
    """The recorded email that serves `port`, or (if unrecorded) the
    lettered fallback a@…, b@… at `fallback_index`, or None."""
    member = load_fleet(base_dir).get(int(port))
    if member and member.get("email"):
        return member["email"]
    if fallback_index is not None:
        return f"{chr(ord('a') + int(fallback_index))}@gmail.com"
    return None


def account_pid(account):
    """PID of the instance currently logged in as `account` (its main window
    is 'DUxxx Interactive Brokers …'), or None."""
    for _hwnd, title, pid, _w, _h in _all_tws_windows():
        if title.startswith(account + " Interactive Brokers"):
            return pid
    return None


def config_dir_for_account(account):
    """The jtsConfigDir of the instance logged in as `account` — via its main
    window's PID -> command line. Lets the bring-up record port->config dir
    from a live, healthy instance. None if it can't be resolved."""
    pid = account_pid(account)
    if pid is None:
        return None
    import re
    cmd = _proc_cmdlines().get(pid) or ""
    m = re.search(r'jtsconfigdir=(?:"([^"]+)"|([^"\s]+))', cmd, re.IGNORECASE)
    if m:
        return (m.group(1) or m.group(2)).strip()
    return None


def _parse_cmdlines(stdout):
    """Parse `Get-CimInstance … | ConvertTo-Json` output into {pid: cmdline}.
    Handles the single-object (dict) vs array vs empty shapes, a null
    CommandLine, and a non-numeric ProcessId. Pure + testable."""
    out = (stdout or "").strip()
    if not out:
        return {}
    import json
    try:
        data = json.loads(out)
    except ValueError:
        return {}
    if isinstance(data, dict):
        data = [data]
    res = {}
    for d in data:
        if not isinstance(d, dict):
            continue
        try:
            pid = int(d.get("ProcessId"))
        except (TypeError, ValueError):
            continue
        res[pid] = d.get("CommandLine") or ""
    return res


def _query_cmdlines():
    """One PowerShell/CIM query -> {pid: cmdline} (or {} on any failure)."""
    cmd = ("Get-CimInstance Win32_Process -Filter \"Name='tws.exe'\" | "
           "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress")
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", cmd],
            capture_output=True, text=True, timeout=25).stdout
    except Exception:  # noqa: BLE001 — powershell absent/odd
        return {}
    return _parse_cmdlines(out)


def _proc_cmdlines():
    """{pid: command-line} for every running tws.exe (Windows). Lets us map a
    jtsConfigDir back to its PID so ONE instance can be closed without touching
    the others. WMIC is gone on recent Win11, so this uses PowerShell/CIM.

    Retries ONCE when the query is empty while tws.exe is actually running — a
    transient PowerShell hiccup must NOT read as 'nothing running', or a
    restart could wipe + relaunch over a still-live config dir."""
    data = _query_cmdlines()
    if not data and _tws_pids():
        time.sleep(0.5)
        data = _query_cmdlines()
    return data


def _pid_for_config(config_dir):
    """PID of the tws.exe launched with this jtsConfigDir, or None. Matches the
    dir at a path boundary so 'inst_a' never matches 'inst_ab' (the char after
    the dir must end the path token — quote, space, separator, or end)."""
    target = str(Path(config_dir)).replace("/", "\\").rstrip("\\").lower()
    for pid, cmd in _proc_cmdlines().items():
        low = (cmd or "").replace("/", "\\").lower()
        i = low.find(target)
        while i != -1:
            after = low[i + len(target): i + len(target) + 1]
            if after == "" or not (after.isalnum() or after == "_"):
                return pid
            i = low.find(target, i + 1)
    return None


def account_titles():
    """DUxxx account id of every logged-in instance (its main window is titled
    'DUxxx Interactive Brokers …'). Used to spot the NEW instance after a
    re-login while the rest of the fleet stays up."""
    out = set()
    for _hwnd, title, _pid, _w, _h in _all_tws_windows():
        if "Interactive Brokers" in title and title.split():
            out.add(title.split()[0])
    return out


def _wait_account(baseline, timeout):
    """The first account id that appears beyond `baseline` within timeout."""
    baseline = set(baseline)
    end = time.time() + timeout
    while time.time() < end:
        new = account_titles() - baseline
        if new:
            return sorted(new)[0]
        time.sleep(2)
    return None


def _wait_port(port, timeout, host="127.0.0.1", abort_check=None):
    end = time.time() + timeout
    while time.time() < end:
        _abort_if(abort_check)
        if port_open(port, host):
            return True
        time.sleep(2)
    return False


def _wait_port_stable(port, timeout, host="127.0.0.1", abort_check=None,
                      policy=None):
    policy = policy or DEFAULT_READINESS_POLICY
    return bringup.wait_stable(
        lambda: bool(port_open(port, host)), timeout,
        consecutive=policy.consecutive,
        initial_interval=policy.initial_interval,
        max_interval=policy.max_interval,
        backoff=policy.backoff,
        abort_check=abort_check)


def close_one(config_dir, on_progress=None, abort_check=None):
    """Close ONLY the instance bound to `config_dir` — WM_CLOSE its windows,
    then force-kill its PID. Every other instance keeps running. Returns True
    if an instance was found and closed, False if none was running there.
    `abort_check` is polled during the exit waits so a mid-restart ESC (while
    input is frozen) is honored within ~`step` rather than after ~13s."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    pid = _pid_for_config(config_dir)
    if pid is None:
        log(f"no running instance for {config_dir}")
        return False
    _, _, user32 = _win()
    for hwnd, _t, p, _w, _h in _all_tws_windows():
        if p == pid:
            user32.PostMessageW(hwnd, 0x0010, 0, 0)   # WM_CLOSE
    _sleep_or_abort(3, abort_check)
    if pid in _tws_pids():
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                           capture_output=True, timeout=15)
        except Exception:  # noqa: BLE001
            pass
        for _ in range(10):                  # wait for it to ACTUALLY exit so
            if pid not in _tws_pids():        # the config dir is unlocked before
                break                         # any relaunch wipes/reuses it
            _sleep_or_abort(1, abort_check)
    log(f"closed instance pid {pid} ({config_dir})")
    return True


def _ensure_config_free(config_dir, on_progress=None, tries=3, abort_check=None):
    """True once no tws.exe is running on `config_dir`. The PID lookup can
    transiently return None (a partial/failed CIM read) WHILE the instance is
    still up, so 'free' is only trusted when re-probes agree (or no tws.exe is
    running at all); any straggler PID is force-killed. This stops relaunch
    from wiping/duplicating a config dir that is still locked by a live
    process."""
    log = on_progress or (lambda m: None)

    def _looks_free():
        if not _tws_pids():
            return True                          # nothing of ours is running
        for _ in range(3):                       # a partial CIM read must NOT
            if _pid_for_config(config_dir) is not None:   # pass as 'free'
                return False
            time.sleep(0.4)
        return True

    for i in range(tries):
        _abort_if(abort_check)
        pid = _pid_for_config(config_dir)
        if pid is None:
            if _looks_free():
                return True
            continue                             # re-probe found it — loop kills
        log(f"instance pid {pid} still on {config_dir} — killing (try {i + 1})")
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                           capture_output=True, timeout=15)
        except Exception:  # noqa: BLE001
            pass
        _sleep_or_abort(2, abort_check)
    return _pid_for_config(config_dir) is None


def _account_for_config_now(config_dir):
    """Current account for an exact config-dir/PID match, or None."""
    target = str(Path(config_dir)).replace("/", "\\").rstrip("\\").lower()
    cmds = _proc_cmdlines()
    for _hwnd, title, pid, _w, _h in _all_tws_windows():
        if "Interactive Brokers" not in title or not title.split():
            continue
        low = (cmds.get(pid) or "").replace("/", "\\").lower()
        i = low.find(target)
        while i != -1:
            after = low[i + len(target): i + len(target) + 1]
            if after == "" or not (after.isalnum() or after == "_"):
                return title.split()[0]
            i = low.find(target, i + 1)
    return None


def _wait_account_for_config(config_dir, timeout, abort_check=None):
    """Wait for one exact config-dir/PID account observation."""
    end = time.time() + timeout
    while time.time() < end:
        _abort_if(abort_check)
        account = _account_for_config_now(config_dir)
        if account:
            return account
        time.sleep(2)
    return None


def _wait_account_for_config_stable(config_dir, timeout, abort_check=None,
                                    policy=None):
    """Require the same exact config-bound account on consecutive probes."""
    policy = policy or DEFAULT_READINESS_POLICY
    return bringup.wait_stable(
        lambda: _account_for_config_now(config_dir), timeout,
        consecutive=policy.consecutive,
        initial_interval=policy.initial_interval,
        max_interval=policy.max_interval,
        backoff=policy.backoff,
        abort_check=abort_check)


def relaunch_one(email, port, base_dir=MULTI_BASE_DEFAULT, on_progress=None,
                 do_enable=True, pin_pos=(PIN_X, PIN_Y), appear_timeout=90,
                 login_timeout=150, port_timeout=60, debug=False,
                 abort_check=None, readiness_policy=None, screenshot_sink=None):
    """Restart ONE port. The TWS demo auto-logs out daily, leaving the API
    socket dead until the instance is logged back in; this does exactly that
    for a single account without disturbing the rest of the fleet:

        close just this instance -> relaunch on its own config dir -> drive the
        demo login -> (do_enable) re-tick the API and re-bind `port` -> wait
        for the socket.

    A fresh login always comes up on the 7497 default, so the re-bind is what
    actually brings `port` back. Serialized by the module GUI lock so two
    restarts never fight for the foreground. Returns
    {'email','port','account','ok'}; raises TwsLaunchError on failure."""
    if os.name != "nt":
        raise TwsLaunchError("TWS auto-launch is Windows-only",
                             code="dependency_unavailable", phase="preflight")
    tws_exe = _require_tws_executable()
    log = on_progress or (lambda m: print("[tws_launch]", m))
    policy = readiness_policy or DEFAULT_READINESS_POLICY
    tag = email.split("@")[0]
    cfg = config_dir_for(email, base_dir)

    def _ck():
        # cooperative ESC/abort check (the input guard sets this) — bail out
        # cleanly between major steps so a mid-restart ESC is honored.
        if abort_check is not None and abort_check():
            raise TwsLaunchError(f"[{tag}] aborted by user (ESC)",
                                 code="aborted", phase="abort")

    _ck()
    # Resolve the API-enable module BEFORE anything destructive: a missing
    # pyautogui/opencv must fail fast (as a typed TwsLaunchError) instead of
    # stranding a relaunched instance on the 7497 default with its port unbound
    # — and as TwsLaunchError so restart_dead's handler catches it per-instance.
    api = None
    if do_enable:
        try:
            import tws_api as api
        except Exception as exc:  # noqa: BLE001
            raise TwsLaunchError(f"[{tag}] API-enable unavailable (install "
                                 f"pyautogui+opencv): {exc}",
                                 code="dependency_unavailable",
                                 phase="preflight")
    with _GUI_LOCK:
        log(f"[{tag}] restarting port {port} (config {cfg})")
        close_one(cfg, on_progress=log, abort_check=abort_check)
        if not _ensure_config_free(cfg, on_progress=log,
                                   abort_check=abort_check):
            raise TwsLaunchError(f"[{tag}] an instance is still running on "
                                 f"{cfg}; refusing to relaunch over it",
                                 code="config_dir_busy", phase="cleanup")
        try:
            _ck()
            before = find_login_windows()
            _launch_proc(cfg, fresh=True, tws_exe=tws_exe)
            hwnd = _wait_new_login(before, appear_timeout,
                                   abort_check=abort_check)
            if not hwnd:
                raise TwsLaunchError(f"[{tag}] login window never reappeared",
                                     code="login_window_absent",
                                     phase="login_window")
            last = None
            for attempt in range(3):
                _ck()
                try:
                    _drive_login(hwnd, email, pin_pos, log, debug, tag,
                                 abort_check=abort_check)
                    break
                except TwsLaunchError as exc:
                    last = exc
                    log(f"[{tag}] login attempt {attempt + 1} failed ({exc})")
                    time.sleep(2)
            else:
                if bringup.failure_code(last) == "aborted":
                    raise last
                raise TwsLaunchError(str(last), code="login_drive_failed",
                                     phase="login_drive") from last
            # bind the account to OUR config dir (by PID), not 'first new DUxxx'
            stable_account = _wait_account_for_config_stable(
                cfg, min(float(login_timeout), policy.account_timeout),
                abort_check=abort_check, policy=policy)
            if not stable_account.ok:
                raise TwsLaunchError(f"[{tag}] account window for {cfg} never "
                                     f"appeared after re-login",
                                     code="account_window_absent",
                                     phase="account_window")
            acct = stable_account.value
            log(f"[{tag}] re-logged in as {acct}")
            time.sleep(3)
            dismiss_popups(on_progress=log)
            _ck()
            if do_enable:
                log(f"[{tag}] re-enabling API on {acct} -> port {port}")
                api.configure(acct, port, log, abort_check=abort_check,
                              screenshot_sink=screenshot_sink)
            stable_port = _wait_port_stable(
                port, min(float(port_timeout), policy.port_timeout),
                abort_check=abort_check, policy=policy)
            ok = stable_port.ok
            _restore_all_tws()                # un-minimize the whole fleet
            log(f"[{tag}] port {port} listening={ok}")
            if not ok:
                raise TwsLaunchError(f"[{tag}] port {port} never came back up",
                                     code="port_not_listening",
                                     phase="port_readiness")
            record_member(port, email=email, config_dir=cfg, account=acct,
                          base_dir=base_dir)
            return {"email": email, "port": port, "account": acct, "ok": ok}
        except TwsLaunchError:
            # never orphan a half-logged-in instance holding the config-dir
            # lock — close it before propagating so the next restart is clean.
            try:
                close_one(cfg, on_progress=log)
            except Exception:  # noqa: BLE001
                pass
            raise
        except Exception as exc:  # noqa: BLE001 — tws_api.configure raises a
            # bare RuntimeError on its normal GUI-match failures, and
            # _drive_login can raise ImportError/pyautogui errors. Clean up the
            # half-logged-in instance and NORMALIZE to TwsLaunchError, so
            # restart_dead's per-instance isolation (except TwsLaunchError)
            # still catches it instead of aborting the whole fleet loop.
            try:
                close_one(cfg, on_progress=log)
            except Exception:  # noqa: BLE001
                pass
            raise TwsLaunchError(f"[{tag}] restart failed: {exc}",
                                 code=bringup.failure_code(exc),
                                 phase=bringup.failure_phase(exc)) from exc


def _enable_fleet_impl(
        pairs, base_dir=MULTI_BASE_DEFAULT, on_progress=None,
        account_timeout=120, port_timeout=60, abort_check=None, *,
        retry_transient=True, recorder=None, readiness_policy=None,
        resource_warning_sink=None):
    """Enable the API + bind each instance's intended socket port, for a fleet
    that launch_many has ALREADY brought up and logged in.

    A fresh demo login always comes up on the 7497 default with the API off, so
    THIS is the step that actually binds 2000/3000/… Each instance is matched
    BY ITS CONFIG DIR (inst_<local>, via the window PID) — never 'first new
    DUxxx' — so inst_a takes pairs[0]'s port, inst_b pairs[1]'s, no matter what
    order the demo handed out the DUxxx accounts. Records the fleet.json
    port->instance map as it goes (so a later restart targets the right one).
    Serialized by the module GUI lock — the config dialogs fight for the
    foreground, so they run one at a time. `pairs` is [(email, port), …].

    Per-instance isolation: one failure (or a missing instance) does not stop
    the rest. A step-verification recorder instead halts before the next port.
    Returns {port: {'account','ok'} | 'up' | 'aborted' | 'FAILED: …'}.
    """
    if os.name != "nt":
        raise TwsLaunchError("TWS API-enable is Windows-only",
                             code="dependency_unavailable", phase="preflight")
    log = on_progress or (lambda m: print("[tws_launch]", m))
    policy = readiness_policy or DEFAULT_READINESS_POLICY
    halt_on_failure = _step_verification(recorder)
    try:
        import tws_api as api
    except Exception as exc:  # noqa: BLE001 — no pyautogui/opencv installed
        raise TwsLaunchError(f"API-enable unavailable (install pyautogui+"
                             f"opencv): {exc}",
                             code="dependency_unavailable",
                             phase="preflight")
    res = {}
    aborted = False
    failed = []
    # The IBKR demo throws a delayed "Complete your Application" nag mid-enable
    # that STEALS focus and stalls the automation. A background watcher closes it
    # (by title) the INSTANT it pops — not just before the next focus step. It
    # self-stops after the enable finishes (or a 10-min safety deadline).
    _popup_stop = threading.Event()
    _popup_deadline = time.monotonic() + 600

    def _popup_watch():
        while not _popup_stop.is_set() and time.monotonic() < _popup_deadline:
            try:
                close_nuisance_popups()
            except Exception:  # noqa: BLE001
                pass
            _popup_stop.wait(1.0)

    _popup_thread = threading.Thread(
        target=_popup_watch, daemon=True, name="popup-watch")
    _popup_thread.start()

    def enable_one(email, port, attempt):
        started = time.monotonic()
        if port_open(port):
            if halt_on_failure:
                exc = TwsLaunchError(
                    f"port {port} was already listening before verified bind",
                    code="port_already_listening",
                    phase="port_readiness")
                res[port] = f"FAILED: {exc}"
                _record_event(
                    recorder, port=port, attempt=attempt,
                    phase="port_readiness", code=exc.code,
                    outcome="failed")
                _capture_failed_step(
                    recorder, port=port, attempt=attempt, exc=exc,
                    step="listener_up")
                return exc
            res[port] = "up"
            log(f"port {port} already listening — skipping enable")
            _record_event(
                recorder, port=port, attempt=attempt, phase="port_readiness",
                code="already_up", outcome="succeeded")
            return None
        cfg = config_dir_for(email, base_dir)
        tag = email.split("@")[0]
        acct = None
        try:
            stable_account = _wait_account_for_config_stable(
                cfg, min(float(account_timeout), policy.account_timeout),
                abort_check=abort_check, policy=policy)
            if not stable_account.ok:
                raise TwsLaunchError(
                    f"account window for {cfg} never appeared",
                    code="account_window_absent", phase="account_window")
            acct = stable_account.value
            log(f"[{tag}] enabling API on {acct} -> port {port}")
            dismiss_popups(on_progress=log)
            api.configure(
                acct, port, log, abort_check=abort_check,
                screenshot_sink=_screenshot_sink(recorder, port, attempt),
                resource_warning_sink=resource_warning_sink,
                step_sink=lambda step, **kw: _capture_step(
                    recorder, port=port, attempt=attempt, step=step, **kw))
            stable_port = _wait_port_stable(
                port, min(float(port_timeout), policy.port_timeout),
                abort_check=abort_check, policy=policy)
            if not stable_port.ok:
                raise TwsLaunchError(
                    f"port {port} never came up", code="port_not_listening",
                    phase="port_readiness")
            _capture_step(
                recorder, port=port, attempt=attempt, step="listener_up",
                hwnd=_main_hwnd_for_account(acct))
            record_member(port, email=email, config_dir=cfg, account=acct,
                          base_dir=base_dir)
            res[port] = {"account": acct, "ok": True}
            log(f"[{tag}] port {port} listening")
            _record_event(
                recorder, port=port, attempt=attempt, phase="enable",
                code="ok", elapsed_ms=(time.monotonic() - started) * 1000,
                outcome="succeeded")
            return None
        except Exception as exc:  # noqa: BLE001 — isolate per instance
            if (abort_check is not None and abort_check()
                    or bringup.failure_code(exc) == "aborted"):
                res[port] = "aborted"
                _record_event(
                    recorder, port=port, attempt=attempt, phase="abort",
                    code="aborted",
                    elapsed_ms=(time.monotonic() - started) * 1000,
                    outcome="aborted")
                return TwsLaunchError("aborted by user", code="aborted",
                                      phase="abort")
            res[port] = f"FAILED: {exc}"
            code = bringup.failure_code(exc)
            log(f"[{tag}] enable failed for port {port}: {exc}")
            _record_event(
                recorder, port=port, attempt=attempt,
                phase=bringup.failure_phase(exc), code=code,
                elapsed_ms=(time.monotonic() - started) * 1000,
                retryable=bringup.is_retryable(exc), outcome="failed")
            _capture_failed_step(
                recorder, port=port, attempt=attempt, exc=exc,
                hwnd=_main_hwnd_for_account(acct))
            return exc

    try:
        with _GUI_LOCK:
            if halt_on_failure:
                for pos, (email, port) in enumerate(pairs):
                    port = int(port)
                    if abort_check is not None and abort_check():
                        res[port] = "aborted"
                        aborted = True
                        break
                    exc = enable_one(email, port, 1)
                    if (exc is not None and retry_transient
                            and bringup.is_retryable(exc)
                            and bringup.failure_code(exc) != "aborted"):
                        if bringup.failure_phase(exc) == "port_readiness":
                            recovered = _wait_port_stable(
                                port, min(5.0, policy.port_timeout),
                                policy=policy, abort_check=abort_check)
                            if recovered.ok:
                                account = _account_for_config_now(
                                    config_dir_for(email, base_dir))
                                _capture_step(
                                    recorder, port=port, attempt=2,
                                    step="listener_up",
                                    hwnd=_main_hwnd_for_account(account))
                                record_member(
                                    port, email=email,
                                    config_dir=config_dir_for(email, base_dir),
                                    account=account, base_dir=base_dir)
                                res[port] = {
                                    "account": account, "ok": True}
                                _record_event(
                                    recorder, port=port, attempt=2,
                                    phase="port_readiness",
                                    code="self_recovered",
                                    elapsed_ms=recovered.elapsed_ms,
                                    outcome="self_recovered")
                                exc = None
                        if exc is not None:
                            log(f"port {port} retrying verified enable failure")
                            exc = enable_one(email, port, 2)
                    if exc is not None:
                        if bringup.failure_code(exc) == "aborted":
                            aborted = True
                        for _remaining_email, remaining_port in pairs[pos + 1:]:
                            res[int(remaining_port)] = (
                                f"NOT ATTEMPTED: halted after port {port}")
                        log(f"verified enable halted at port {port}")
                        break
                _restore_all_tws()
                return res
            for email, port in pairs:
                port = int(port)
                if aborted or (abort_check is not None and abort_check()):
                    res[port] = "aborted"
                    aborted = True
                    continue
                exc = enable_one(email, port, 1)
                if exc is not None:
                    if bringup.failure_code(exc) == "aborted":
                        aborted = True
                    else:
                        failed.append((email, port, exc))
            if retry_transient and not aborted:
                for pos, (email, port, exc) in enumerate(failed):
                    if not bringup.is_retryable(exc):
                        continue
                    try:
                        recovered = _wait_port_stable(
                            port, min(5.0, policy.port_timeout), policy=policy,
                            abort_check=abort_check)
                    except Exception as recovery_exc:  # noqa: BLE001
                        if bringup.failure_code(recovery_exc) != "aborted":
                            raise
                        for _email, remaining, _exc in failed[pos:]:
                            res[remaining] = "aborted"
                        break
                    if recovered.ok:
                        acct = _account_for_config_now(
                            config_dir_for(email, base_dir))
                        res[port] = {"account": acct, "ok": True}
                        _record_event(
                            recorder, port=port, attempt=2,
                            phase="port_readiness", code="self_recovered",
                            elapsed_ms=recovered.elapsed_ms,
                            outcome="self_recovered")
                        continue
                    log(f"port {port} retrying transient enable failure")
                    enable_one(email, port, 2)
            _restore_all_tws()
    finally:
        _popup_stop.set()
        _popup_thread.join()
    return res


def enable_fleet(pairs, base_dir=MULTI_BASE_DEFAULT, on_progress=None,
                 account_timeout=120, port_timeout=60, abort_check=None, *,
                 retry_transient=True, recorder=None, readiness_policy=None):
    """Run API enable/bind with a joined exact resource-warning sweeper."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    with _resource_warning_sweeper(
            recorder=recorder, on_progress=log) as sweeper:
        return _enable_fleet_impl(
            pairs, base_dir=base_dir, on_progress=on_progress,
            account_timeout=account_timeout, port_timeout=port_timeout,
            abort_check=abort_check, retry_transient=retry_transient,
            recorder=recorder, readiness_policy=readiness_policy,
            resource_warning_sink=sweeper.record_result)


def verify_handshake(port, expected_account, *, recorder=None, attempt=1,
                     on_progress=None):
    """Prove one listener with a real API handshake and exact account match."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    port = int(port)
    started = time.monotonic()
    adapter = None
    try:
        import stock_ibkr
        adapter = stock_ibkr.LiveIB(
            host=stock_ibkr.HOST_DEFAULT, ports=(port,),
            client_id=stock_ibkr.CLIENT_ID_DOCTOR).connect()
        matched = bool(expected_account
                       and adapter.account() == expected_account)
        if not matched:
            raise TwsLaunchError(
                "API handshake account did not match the bound instance",
                code="handshake_failed", phase="handshake")
        _capture_step(
            recorder, port=port, attempt=attempt, step="handshake",
            hwnd=_main_hwnd_for_account(expected_account),
            account_match=True)
        _record_event(
            recorder, port=port, attempt=attempt, phase="handshake",
            code="ok", elapsed_ms=(time.monotonic() - started) * 1000,
            outcome="succeeded")
        log(f"port {port} API handshake verified")
        return {"port": port, "ok": True, "account_match": True}
    except Exception as raw_exc:  # noqa: BLE001 - normalize proof failure
        exc = (raw_exc if isinstance(raw_exc, TwsLaunchError) else
               TwsLaunchError(
                   "API handshake failed",
                   code="handshake_failed", phase="handshake"))
        _capture_failed_step(
            recorder, port=port, attempt=attempt, exc=exc,
            step="handshake",
            hwnd=_main_hwnd_for_account(expected_account))
        _record_event(
            recorder, port=port, attempt=attempt, phase="handshake",
            code=bringup.failure_code(exc),
            elapsed_ms=(time.monotonic() - started) * 1000,
            retryable=bringup.is_retryable(exc), outcome="failed")
        if exc is raw_exc:
            raise
        raise exc from raw_exc
    finally:
        if adapter is not None:
            adapter.disconnect()


def fleet_health(ports, host="127.0.0.1"):
    """{port: True/False} — is each port's API socket serving right now?"""
    return {int(p): port_open(int(p), host) for p in ports}


def restart_dead(fleet, on_progress=None, host="127.0.0.1", *,
                 retry_transient=True, recorder=None, readiness_policy=None,
                 **kw):
    """Restart only the DOWN ports in `fleet` (a list of (email, port) pairs).
    Up ports are left untouched. Restarts run one at a time (GUI lock), and a
    single failure does not stop the rest. Returns
    {port: 'up' | result-dict | 'FAILED: …'}."""
    log = on_progress or (lambda m: print("[tws_launch]", m))
    policy = readiness_policy or DEFAULT_READINESS_POLICY
    abort_check = kw.get("abort_check")
    res = {}
    failed = []

    def restart_one(email, port, attempt):
        started = time.monotonic()
        try:
            value = relaunch_one(
                email, port, on_progress=log, readiness_policy=policy,
                screenshot_sink=_screenshot_sink(recorder, port, attempt),
                **kw)
            res[port] = value
            _record_event(
                recorder, port=port, attempt=attempt, phase="restart",
                code="ok", elapsed_ms=(time.monotonic() - started) * 1000,
                outcome="succeeded")
            return None
        except Exception as raw_exc:  # noqa: BLE001 — isolate per port
            exc = (raw_exc if isinstance(raw_exc, TwsLaunchError) else
                   TwsLaunchError(
                       f"unexpected restart failure: {raw_exc}",
                       code=bringup.failure_code(raw_exc),
                       phase=bringup.failure_phase(raw_exc)))
            res[port] = f"FAILED: {exc}"
            log(str(exc))
            _record_event(
                recorder, port=port, attempt=attempt,
                phase=bringup.failure_phase(exc),
                code=bringup.failure_code(exc),
                elapsed_ms=(time.monotonic() - started) * 1000,
                retryable=bringup.is_retryable(exc), outcome="failed")
            return exc

    for email, port in fleet:
        port = int(port)
        if port_open(port, host):
            res[port] = "up"
            log(f"port {port} already up — skipping")
            _record_event(
                recorder, port=port, attempt=0, phase="preflight",
                code="already_up", outcome="succeeded")
            continue
        log(f"port {port} is down — restarting {email}…")
        exc = restart_one(email, port, 1)
        if exc is not None:
            failed.append((email, port, exc))
    if retry_transient:
        for pos, (email, port, exc) in enumerate(failed):
            if not bringup.is_retryable(exc):
                continue
            try:
                recovered = _wait_port_stable(
                    port, min(5.0, policy.port_timeout), host=host,
                    policy=policy, abort_check=abort_check)
            except Exception as recovery_exc:  # noqa: BLE001
                if bringup.failure_code(recovery_exc) != "aborted":
                    raise
                for _email, remaining, _exc in failed[pos:]:
                    res[remaining] = "FAILED: aborted by user"
                break
            if recovered.ok:
                member = load_fleet(
                    kw.get("base_dir", MULTI_BASE_DEFAULT)).get(port, {})
                res[port] = {
                    "email": email, "port": port,
                    "account": member.get("account"), "ok": True,
                }
                _record_event(
                    recorder, port=port, attempt=2,
                    phase="port_readiness", code="self_recovered",
                    elapsed_ms=recovered.elapsed_ms,
                    outcome="self_recovered")
                continue
            log(f"port {port} retrying transient restart failure")
            restart_one(email, port, 2)
    return res


# --- single-instance orchestrator ------------------------------------------

def open_demo(email="a@gmail.com", config_dir=CONFIG_DIR_DEFAULT,
              pin_pos=(PIN_X, PIN_Y), port=API_PORT_DEFAULT, on_progress=None,
              wait_port=True, launch_timeout=90, login_timeout=120, debug=False):
    """Launch one TWS, log into the demo, clear popups. If wait_port, also wait
    for the API socket. Returns True on success; raises TwsLaunchError."""
    if os.name != "nt":
        raise TwsLaunchError("TWS auto-launch is Windows-only")
    log = on_progress or (lambda m: print("[tws_launch]", m))

    if wait_port and port_open(port):
        log(f"TWS already serving the API on {port} — clearing any popups")
        log(f"dismissed {dismiss_popups(on_progress=log)} popup(s)")
        return True
    tws_exe = _require_tws_executable()

    before = find_login_windows()
    log("launching TWS…")
    _launch_proc(config_dir, tws_exe=tws_exe)
    hwnd = _wait_new_login(before, launch_timeout)
    if not hwnd:
        raise TwsLaunchError("the TWS Login window never appeared")
    _drive_login(hwnd, email, pin_pos, log, debug, tag="tws")

    if wait_port:
        end = time.time() + login_timeout
        while time.time() < end:
            if port_open(port):
                log(f"API up on {port} — TWS demo ready")
                log(f"dismissed {dismiss_popups(on_progress=log)} popup(s)")
                return True
            time.sleep(2)
        raise TwsLaunchError(f"API port {port} never opened within {login_timeout}s")
    time.sleep(15)
    log(f"dismissed {dismiss_popups(on_progress=log)} popup(s)")
    return True


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="Launch TWS and log into the demo")
    ap.add_argument("--email", default="a@gmail.com")
    ap.add_argument("--port", type=int, default=API_PORT_DEFAULT)
    ap.add_argument("--many", help="comma-separated demo emails to launch serially")
    ap.add_argument("--base-dir", default=MULTI_BASE_DEFAULT)
    ap.add_argument("--keep-existing", action="store_true",
                    help="with --many, don't close already-running TWS first")
    ap.add_argument("--no-wait-port", action="store_true",
                    help="single mode: don't wait for the API socket")
    ap.add_argument("--debug", action="store_true",
                    help="save debug screenshots under archive/_code_snapshots")
    ap.add_argument("--dismiss-only", action="store_true",
                    help="just close popups on already-running TWS")
    ap.add_argument("--restart", nargs=2, metavar=("EMAIL", "PORT"),
                    help="relaunch+re-login+rebind ONE instance (others stay up)")
    ap.add_argument("--restart-dead",
                    help="comma list of email:port; restart only the down ones")
    ap.add_argument("--health",
                    help="comma list of ports to probe; prints up/down and exits")
    ap.add_argument("--no-enable", action="store_true",
                    help="with --restart/--restart-dead, skip the API re-enable step")
    a = ap.parse_args(argv)

    if a.health:
        ports = [int(p) for p in a.health.split(",") if p.strip()]
        h = fleet_health(ports)
        for p in ports:
            print(f"    port {p}: {'UP' if h[p] else 'DOWN'}")
        return 0 if all(h.values()) else 1
    if a.restart:
        try:
            r = relaunch_one(a.restart[0], int(a.restart[1]),
                             base_dir=a.base_dir, do_enable=not a.no_enable,
                             debug=a.debug)
        except TwsLaunchError as exc:
            print("[tws_launch] restart FAILED:", exc)
            return 1
        print("[tws_launch] restart result:", r)
        return 0
    if a.restart_dead:
        fleet = []
        for item in a.restart_dead.split(","):
            item = item.strip()
            if not item:
                continue
            email, _, port = item.partition(":")
            fleet.append((email.strip(), int(port)))
        res = restart_dead(fleet, base_dir=a.base_dir,
                           do_enable=not a.no_enable, debug=a.debug)
        print("[tws_launch] restart-dead result:")
        for p, s in res.items():
            print(f"    port {p}: {s}")
        return 0 if all(v == "up" or isinstance(v, dict) for v in res.values()) \
            else 1
    if a.dismiss_only:
        print(f"[tws_launch] dismissed {dismiss_popups()} popup(s)")
        return 0
    if a.many:
        emails = [e.strip() for e in a.many.split(",") if e.strip()]
        res = launch_many(emails, base_dir=a.base_dir, debug=a.debug,
                          close_existing=not a.keep_existing)
        print("[tws_launch] fleet result:")
        for e, s in res.items():
            print(f"    {e}: {s}")
        return 0 if all(v == "launched" for v in res.values()) else 1
    try:
        ok = open_demo(a.email, port=a.port, wait_port=not a.no_wait_port,
                       debug=a.debug)
    except TwsLaunchError as exc:
        print("[tws_launch] FAILED:", exc)
        return 1
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
