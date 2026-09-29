"""Enable the TWS API on a demo instance and set its socket port, via the GUI
(the settings are encrypted, so file editing isn't possible).

This is the PROVEN flow that bound the 2000-6000 fleet, lifted out of the
_enable_api.py CLI so it can be reused programmatically — notably by
tws_launch.relaunch_one, which re-binds a single instance's port after the
demo's daily auto-logout forces a fresh login (a fresh login always comes up
on the 7497 default until this re-enables the intended port).

Image-matches the fragile menu/dialog bits; fixed coords for the controls once
the config dialog is pinned to (0,0). Windows-only; needs pyautogui+opencv.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import time
from pathlib import Path

import pyautogui

import tws_launch as t
import tws_bringup as bringup

pyautogui.FAILSAFE = False
MODULE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_DIR.parent
DEBUG_SHOTS_DIR = (PROJECT_ROOT / "archive" / "_code_snapshots"
                   / "debug_screenshots")
TPL = str(MODULE_DIR / "tws_templates")


def _noop(_m):
    pass


@dataclass(frozen=True)
class ImageWaitPolicy:
    config_open_timeout: float = 75.0
    navigation_timeout: float = 45.0
    controls_timeout: float = 30.0
    focus_timeout: float = 3.0
    focus_interval: float = 0.15
    initial_interval: float = 0.5
    max_interval: float = 2.0
    backoff: float = 1.5


DEFAULT_IMAGE_WAIT_POLICY = ImageWaitPolicy()
# The target TWS window is pinned at (0, 0) before these are used.  Keyboard
# mnemonics are unsafe in Mosaic: focus can remain inside a chart and Alt+F
# opens that chart's File menu rather than the application File menu.  The
# application menu labels stay at these fixed top-bar points in the two layouts;
# the Global Configuration entry itself is still image-verified before clicking.
CONFIG_MENU_POINTS = (((20, 17), "File"), ((56, 17), "Edit"))
SOCKET_PORT_SCROLL_ATTEMPTS = 3
SOCKET_PORT_SCROLL_NOTCHES = 6
SOCKET_PORT_SCROLL_POINT = (900, 400)


def _abort(abort_check, phase):
    if abort_check is not None and abort_check():
        raise bringup.StepFailure("aborted by user", code="aborted", phase=phase)


def _pause(seconds, *, deadline=None, abort_check=None, phase="unknown",
           clock=time.monotonic, sleep=time.sleep):
    _abort(abort_check, phase)
    delay = float(seconds)
    if deadline is not None:
        delay = min(delay, max(0.0, deadline - clock()))
    if delay > 0:
        sleep(delay)
    _abort(abort_check, phase)


def locate(name, timeout=12, region=None, confidence=0.72, *, deadline=None,
           abort_check=None, phase="unknown", initial_interval=0.5,
           max_interval=2.0, backoff=1.5, clock=time.monotonic,
           sleep=time.sleep):
    end = clock() + float(timeout)
    if deadline is not None:
        end = min(end, deadline)
    interval = float(initial_interval)
    while clock() <= end:
        _abort(abort_check, phase)
        try:
            p = pyautogui.locateCenterOnScreen(f"{TPL}/{name}", region=region,
                                               confidence=confidence)
        except Exception:  # noqa: BLE001 — ImageNotFound / transient screencap
            p = None
        if p:
            return (int(p.x), int(p.y))
        remaining = end - clock()
        if remaining <= 0:
            break
        sleep(min(interval, remaining))
        interval = min(float(max_interval), interval * float(backoff))
    return None


def _screenshot(screenshot_sink, code):
    if screenshot_sink is None:
        return
    try:
        path = screenshot_sink(code)
        if path is not None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            pyautogui.screenshot(str(path))
    except Exception:  # noqa: BLE001 - diagnostics never change the result
        pass


def _foreground_snapshot():
    try:
        hwnd, title = t.foreground_window()
        hwnd = int(hwnd or 0)
        title = str(title or "(untitled)")
    except Exception:  # noqa: BLE001 - absence becomes typed focus evidence
        hwnd, title = 0, "(foreground unavailable)"
    label = bringup.redacted_evidence_text(
        f"hwnd={hwnd} title={title}")
    return hwnd, label


def _require_target_foreground(acct, hwnd, *, deadline, abort_check, policy,
                               screenshot_sink=None, phase="config_open",
                               clock=time.monotonic, sleep=time.sleep):
    """Boundedly prove the target before input; report if recovery was needed."""
    target = main_hwnd(acct) or hwnd
    actual, holder = _foreground_snapshot()
    if target and actual == int(target):
        return target, False

    focus_deadline = min(
        float(deadline),
        clock() + max(float(policy.focus_timeout), 0.05))
    interval = max(float(policy.focus_interval), 0.01)
    while clock() < focus_deadline:
        _abort(abort_check, phase)
        target = main_hwnd(acct) or target
        if target:
            t._minimize_others(target)
            t.pin_window(target, 0, 0)
            t.foreground(target)
        _pause(
            interval, deadline=focus_deadline, abort_check=abort_check,
            phase=phase, clock=clock, sleep=sleep)
        actual, holder = _foreground_snapshot()
        if target and actual == int(target):
            return target, True

    # Capture before any later caller can re-raise the target; the failed-step
    # recorder also preserves the observed holder without foregrounding target.
    _screenshot(screenshot_sink, "focus_lost")
    failure = bringup.StepFailure(
        f"target window lost foreground to {holder}",
        code="focus_lost", phase=phase)
    failure.foreground_holder = holder
    raise failure


def _require_window_foreground(hwnd, *, deadline, abort_check, policy,
                               screenshot_sink=None, phase="api_navigation",
                               clock=time.monotonic, sleep=time.sleep):
    """Boundedly require one already-owned dialog before sending it input."""
    target = int(hwnd or 0)
    actual, holder = _foreground_snapshot()
    if target and actual == target:
        return target, False

    focus_deadline = min(
        float(deadline),
        clock() + max(float(policy.focus_timeout), 0.05))
    interval = max(float(policy.focus_interval), 0.01)
    while target and clock() < focus_deadline:
        _abort(abort_check, phase)
        t.foreground(target)
        _pause(
            interval, deadline=focus_deadline, abort_check=abort_check,
            phase=phase, clock=clock, sleep=sleep)
        actual, holder = _foreground_snapshot()
        if actual == target:
            return target, True

    _screenshot(screenshot_sink, "focus_lost")
    failure = bringup.StepFailure(
        f"configuration window lost foreground to {holder}",
        code="focus_lost", phase=phase)
    failure.foreground_holder = holder
    raise failure


def _clear_resource_warnings(resource_warning_sink=None):
    """Call the shared closer without forcing callback-aware test doubles."""
    if resource_warning_sink is None:
        return t.close_resource_warnings()
    return t.close_resource_warnings(on_result=resource_warning_sink)


@contextmanager
def _target_input_guard(
        acct, hwnd, *, deadline, abort_check, policy,
        screenshot_sink=None, phase="config_open",
        resource_warning_sink=None, log=None,
        clock=time.monotonic, sleep=time.sleep):
    """Keep exact popup clearing + foreground proof atomic with one input."""
    with t.resource_warning_input_interlock():
        t.close_nuisance_popups()
        _clear_resource_warnings(resource_warning_sink)
        _ack_terms_dialogs(
            acct, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase, log=log,
            clock=clock, sleep=sleep)
        yield _require_target_foreground(
            acct, hwnd, deadline=deadline, abort_check=abort_check,
            policy=policy, screenshot_sink=screenshot_sink, phase=phase,
            clock=clock, sleep=sleep)


def _prepare_config_input(cfg, *, deadline, abort_check, policy,
                          screenshot_sink=None, phase="api_navigation",
                          acct=None, log=None, resource_warning_sink=None,
                          clock=time.monotonic, sleep=time.sleep):
    """Clear exact overlays, then prove the owned config dialog is active."""
    _clear_resource_warnings(resource_warning_sink)
    if acct:
        _ack_terms_dialogs(
            acct, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase, log=log,
            clock=clock, sleep=sleep)
    return _require_window_foreground(
        cfg, deadline=deadline, abort_check=abort_check, policy=policy,
        screenshot_sink=screenshot_sink, phase=phase,
        clock=clock, sleep=sleep)


@contextmanager
def _config_input_guard(
        cfg, *, deadline, abort_check, policy,
        screenshot_sink=None, phase="api_navigation",
        acct=None, log=None, resource_warning_sink=None,
        clock=time.monotonic, sleep=time.sleep):
    """Keep overlay clearing + config foreground proof atomic with one input."""
    with t.resource_warning_input_interlock():
        yield _prepare_config_input(
            cfg, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase, acct=acct, log=log,
            resource_warning_sink=resource_warning_sink,
            clock=clock, sleep=sleep)


def main_hwnd(acct):
    w = [x for x in t._all_tws_windows()
         if x[1].startswith(acct + " Interactive Brokers")]
    return w[0][0] if w else None


def cfg_hwnd(owner_hwnd=None):
    """Configuration window owned by the target TWS process, if present."""
    windows = t._all_tws_windows()
    owner_pid = next(
        (row[2] for row in windows if row[0] == owner_hwnd), None)
    configs = [
        row for row in windows
        if "Configuration" in row[1]
        and (owner_pid is None or row[2] == owner_pid)
    ]
    return configs[0][0] if configs else None


def _terms_dialog_windows(acct):
    """Return API terms dialogs without mistaking resource warnings for one."""
    return [
        row for row in t._all_tws_windows()
        if row[1].startswith(acct + " IBKR Trader Workstation")
        and not t.is_resource_warning_window(row)
    ]


def _window_region(hwnd):
    left, top, right, bottom = t._rect(hwnd)
    width = max(0, int(right) - int(left))
    height = max(0, int(bottom) - int(top))
    if width < 1 or height < 1:
        raise ValueError("empty window region")
    return (int(left), int(top), width, height)


def _ack_terms_dialogs(
        acct, *, deadline, abort_check, policy, screenshot_sink=None,
        phase="enable", log=None, clock=time.monotonic, sleep=time.sleep):
    """Acknowledge only title-filtered dialogs containing the exact T&C cue."""
    log = log or _noop
    acknowledged = 0
    for _attempt in range(6):
        _abort(abort_check, phase)
        match = None
        for row in _terms_dialog_windows(acct):
            try:
                region = _window_region(row[0])
                ack = locate(
                    "menu_acknowledge.png", timeout=1.0, region=region,
                    confidence=0.85, deadline=deadline,
                    abort_check=abort_check, phase=phase,
                    initial_interval=policy.initial_interval,
                    max_interval=policy.max_interval, backoff=policy.backoff,
                    clock=clock, sleep=sleep)
            except bringup.StepFailure:
                raise
            except Exception:  # noqa: BLE001 - leave unknown windows untouched
                ack = None
            if ack:
                match = (row, ack)
                break
        if match is None:
            return acknowledged

        row, _stale_ack = match
        hwnd = row[0]
        _require_window_foreground(
            hwnd, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase,
            clock=clock, sleep=sleep)
        # Foreground recovery may move/repaint the dialog. Re-identify its
        # exact acknowledgement control, then assert ownership immediately
        # before the click. A lookalike without the cue receives no input.
        try:
            ack = locate(
                "menu_acknowledge.png", timeout=1.0,
                region=_window_region(hwnd), confidence=0.85,
                deadline=deadline, abort_check=abort_check, phase=phase,
                initial_interval=policy.initial_interval,
                max_interval=policy.max_interval, backoff=policy.backoff,
                clock=clock, sleep=sleep)
        except bringup.StepFailure:
            raise
        except Exception:  # noqa: BLE001 - leave unknown windows untouched
            ack = None
        if not ack:
            return acknowledged
        _require_window_foreground(
            hwnd, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase,
            clock=clock, sleep=sleep)
        pyautogui.click(ack[0], ack[1])
        acknowledged += 1
        log(f"acknowledged API T&C {acknowledged}")
        _pause(
            0.8, deadline=deadline, abort_check=abort_check, phase=phase,
            clock=clock, sleep=sleep)
    return acknowledged


def set_rect(hwnd, x, y, w, h):
    """Move AND resize a window (no SWP_NOSIZE) so its layout is deterministic."""
    _, _, user32 = t._win()
    user32.SetWindowPos(hwnd, 0, x, y, w, h, 0x0040)  # SWP_SHOWWINDOW
    time.sleep(0.5)


def close_configs():
    """Close any open config dialog (Escape to discard, then WM_CLOSE)."""
    _, _, user32 = t._win()
    for _ in range(3):
        wins = [x for x in t._all_tws_windows()
                if "Configuration" in x[1]]
        if not wins:
            return
        for x in wins:
            with t.resource_warning_input_interlock():
                t.foreground(x[0]); time.sleep(0.3)
                pyautogui.press("escape"); time.sleep(0.3)
                user32.PostMessageW(x[0], 0x0010, 0, 0)   # WM_CLOSE
        time.sleep(1.3)


def open_config(acct, log, *, deadline=None, abort_check=None,
                screenshot_sink=None, step_sink=None, policy=None,
                resource_warning_sink=None,
                clock=time.monotonic, sleep=time.sleep):
    policy = policy or DEFAULT_IMAGE_WAIT_POLICY
    deadline = deadline or (clock() + policy.config_open_timeout)
    phase = "config_open"
    h = main_hwnd(acct)
    if not h:
        raise bringup.StepFailure("main window unavailable",
                                  code="config_open_failed", phase=phase)
    close_configs()
    t.close_nuisance_popups()                    # clear the nag up front
    _clear_resource_warnings(
        resource_warning_sink)                    # ignore low-memory warnings
    h = main_hwnd(acct)
    t._minimize_others(h); t.pin_window(h, 0, 0); t.foreground(h)
    _pause(0.9, deadline=deadline, abort_check=abort_check, phase=phase,
           clock=clock, sleep=sleep)
    attempt = 0
    while clock() < deadline:
        restart_menu_cycle = False
        # Mosaic exposes Global Configuration under File, while Classic uses
        # Edit.  A fleet may contain either layout, so each bounded attempt
        # tries both documented menus.  The delayed "Complete your
        # Application" popup can also steal focus; clear it immediately before
        # each menu click and re-pin the exact account window.
        for menu_point, menu_name in CONFIG_MENU_POINTS:
            if clock() >= deadline:
                break
            with _target_input_guard(
                    acct, h, deadline=deadline, abort_check=abort_check,
                    policy=policy, screenshot_sink=screenshot_sink,
                    phase=phase, log=log,
                    resource_warning_sink=resource_warning_sink,
                    clock=clock, sleep=sleep) as (h, _recovered):
                pyautogui.press("escape")
            _pause(
                0.3, deadline=deadline, abort_check=abort_check,
                phase=phase, clock=clock, sleep=sleep)
            with _target_input_guard(
                    acct, h, deadline=deadline, abort_check=abort_check,
                    policy=policy, screenshot_sink=screenshot_sink,
                    phase=phase, log=log,
                    resource_warning_sink=resource_warning_sink,
                    clock=clock, sleep=sleep) as (h, _recovered):
                pyautogui.click(menu_point)
            _pause(
                1.0, deadline=deadline, abort_check=abort_check,
                phase=phase, clock=clock, sleep=sleep)
            loc = locate(
                "menu_global_config.png", timeout=4, deadline=deadline,
                abort_check=abort_check, phase=phase,
                initial_interval=policy.initial_interval,
                max_interval=policy.max_interval, backoff=policy.backoff,
                clock=clock, sleep=sleep)
            if loc:
                with _target_input_guard(
                        acct, h, deadline=deadline,
                        abort_check=abort_check, policy=policy,
                        screenshot_sink=screenshot_sink, phase=phase,
                        resource_warning_sink=resource_warning_sink, log=log,
                        clock=clock, sleep=sleep) as (h, recovered):
                    if recovered:
                        # Raising the target can dismiss the menu. Never trust
                        # a location captured before that focus transition.
                        log(
                            "open_config focus recovered before "
                            f"{menu_name} item; reopening menu")
                        pyautogui.press("escape")
                        restart_menu_cycle = True
                    else:
                        pyautogui.click(loc)
                if restart_menu_cycle:
                    _pause(
                        0.2, deadline=deadline, abort_check=abort_check,
                        phase=phase, clock=clock, sleep=sleep)
                    break
                _pause(
                    2.6, deadline=deadline, abort_check=abort_check,
                    phase=phase, clock=clock, sleep=sleep)
                cfg = cfg_hwnd(h)
                if cfg:
                    log(f"config opened via {menu_name}")
                    if step_sink is not None:
                        step_sink(
                            "config_open", hwnd=cfg, menu_path=menu_name)
                    return cfg
            with _target_input_guard(
                    acct, h, deadline=deadline, abort_check=abort_check,
                    policy=policy, screenshot_sink=screenshot_sink,
                    phase=phase, log=log,
                    resource_warning_sink=resource_warning_sink,
                    clock=clock, sleep=sleep) as (h, _recovered):
                pyautogui.press("escape")
            _pause(
                0.2, deadline=deadline, abort_check=abort_check,
                phase=phase, clock=clock, sleep=sleep)
        if restart_menu_cycle:
            log(f"open_config retry {attempt}")
            attempt += 1
            continue
        log(f"open_config retry {attempt}")
        attempt += 1
    _screenshot(screenshot_sink, "config_open_failed")
    raise bringup.StepFailure("could not open configuration",
                              code="config_open_failed", phase=phase)


def goto_api(cfg, log):
    set_rect(cfg, 0, 0, 1440, 810); t.foreground(cfg); time.sleep(0.7)
    if locate("api_enable_text.png", timeout=2):
        log("already on API settings"); return True
    for attempt in range(3):
        pyautogui.click(12, 235); time.sleep(0.4)          # expand API (handle)
        pyautogui.doubleClick(50, 235); time.sleep(0.8)    # expand API (label)
        pyautogui.click(75, 255); time.sleep(0.9)          # click Settings sub-node
        set_rect(cfg, 0, 0, 1440, 810); t.foreground(cfg); time.sleep(0.5)
        if locate("api_enable_text.png", timeout=3):
            log("navigated to API settings"); return True
        log(f"goto_api retry {attempt}")
    try:
        DEBUG_SHOTS_DIR.mkdir(parents=True, exist_ok=True)
        pyautogui.screenshot(str(DEBUG_SHOTS_DIR / "_fail_nav.png"))
    except Exception:  # noqa: BLE001
        pass
    return False


def _locate_socket_port_label(
        guard, *, deadline, abort_check, policy, log,
        clock=time.monotonic, sleep=time.sleep):
    """Relocate the socket row after at most two bounded upward scrolls."""
    for attempt in range(SOCKET_PORT_SCROLL_ATTEMPTS):
        with guard(deadline, "controls"):
            pass
        found = locate(
            "api_socketport_label.png", timeout=3, confidence=0.9,
            deadline=deadline, abort_check=abort_check, phase="controls",
            initial_interval=policy.initial_interval,
            max_interval=policy.max_interval, backoff=policy.backoff,
            clock=clock, sleep=sleep)
        if found:
            return found
        if attempt + 1 >= SOCKET_PORT_SCROLL_ATTEMPTS:
            break
        with guard(deadline, "controls"):
            pyautogui.moveTo(*SOCKET_PORT_SCROLL_POINT)
            pyautogui.scroll(SOCKET_PORT_SCROLL_NOTCHES)
        log(
            "socket-port row not visible; scrolled API pane upward "
            f"({attempt + 1}/{SOCKET_PORT_SCROLL_ATTEMPTS - 1})")
        _pause(
            0.4, deadline=deadline, abort_check=abort_check,
            phase="controls", clock=clock, sleep=sleep)
    return None


def configure(acct, port, log=None, tick_enable=True, *, policy=None,
              abort_check=None, screenshot_sink=None, step_sink=None,
              resource_warning_sink=None,
              clock=time.monotonic, sleep=time.sleep):
    """Drive Global Config -> API -> Settings for instance `acct`, tick Enable
    ActiveX (acknowledging the T&C dialogs), UNCHECK "Read-Only API" (it defaults
    ON every fresh login and stalls the handshake / strands a port on retry),
    set the socket port, Apply + OK. Returns True iff the port is listening after
    Apply. `log` is an optional progress callback (defaults to silent)."""
    log = log or _noop
    policy = policy or DEFAULT_IMAGE_WAIT_POLICY
    config_deadline = clock() + policy.config_open_timeout
    cfg = open_config(
        acct, log, deadline=config_deadline, abort_check=abort_check,
        screenshot_sink=screenshot_sink, step_sink=step_sink, policy=policy,
        resource_warning_sink=resource_warning_sink,
        clock=clock, sleep=sleep)

    def guard(deadline, phase):
        return _config_input_guard(
            cfg, deadline=deadline, abort_check=abort_check, policy=policy,
            screenshot_sink=screenshot_sink, phase=phase, acct=acct, log=log,
            resource_warning_sink=resource_warning_sink,
            clock=clock, sleep=sleep)

    nav_deadline = clock() + policy.navigation_timeout
    # --- navigate to API -> Settings (image-matched, size independent) ---
    with guard(nav_deadline, "api_navigation"):
        pass
    if not locate("api_enable_text.png", timeout=2, deadline=nav_deadline,
                  abort_check=abort_check, phase="api_navigation",
                  clock=clock, sleep=sleep):
        reached = False
        attempt = 0
        while clock() < nav_deadline:
            with guard(nav_deadline, "api_navigation"):
                pass
            n = locate("api_node.png", timeout=5, deadline=nav_deadline,
                       abort_check=abort_check, phase="api_navigation",
                       clock=clock, sleep=sleep)
            if not n:
                log("API node not visible"); _pause(
                    1, deadline=nav_deadline, abort_check=abort_check,
                    phase="api_navigation", clock=clock, sleep=sleep)
                attempt += 1
                continue
            with guard(nav_deadline, "api_navigation"):
                pyautogui.doubleClick(n[0], n[1])       # expand API
            time.sleep(0.9)
            with guard(nav_deadline, "api_navigation"):
                pyautogui.click(n[0] + 25, n[1] + 18)  # Settings child
            time.sleep(0.9)
            with guard(nav_deadline, "api_navigation"):
                pass
            if locate("api_enable_text.png", timeout=3,
                      deadline=nav_deadline, abort_check=abort_check,
                      phase="api_navigation", clock=clock, sleep=sleep):
                reached = True; break
            log(f"nav retry {attempt}")
            attempt += 1
        if not reached:
            _screenshot(screenshot_sink, "api_node_not_found")
            raise bringup.StepFailure("could not reach API settings",
                                      code="api_node_not_found",
                                      phase="api_navigation")
    log("on API settings")
    controls_deadline = clock() + policy.controls_timeout
    with guard(controls_deadline, "enable"):
        pass
    if tick_enable:
        # tick Enable ActiveX: precise match, checkbox 137px left of text centre
        et = locate("api_enable_text.png", timeout=4, confidence=0.9,
                    deadline=controls_deadline, abort_check=abort_check,
                    phase="enable", clock=clock, sleep=sleep)
        if not et:
            _screenshot(screenshot_sink, "enable_text_not_matched")
            raise bringup.StepFailure("enable text not matched",
                                      code="enable_text_not_matched",
                                      phase="enable")
        with guard(controls_deadline, "enable"):
            pyautogui.click(et[0] - 137, et[1])
        time.sleep(1.6)
        # The next guard clears a T&C that appeared after Enable; every later
        # guard repeats that same narrow check for mid-flow dialogs.
        with guard(controls_deadline, "enable"):
            pass
        # Uncheck "Read-Only API": TWS defaults it ON every fresh demo login
        # (error 321), which stalls the API handshake (~8-12s/connect) and
        # strands the slowest fleet port "on retry" during a fleet build. It
        # sits ONE row below "Enable ActiveX" in the SAME checkbox column.
        # STATE-AWARE: the template carries the checkmark, so locate() only
        # matches when it is CHECKED — so a re-run can never re-enable it.
        try:
            ro = locate("api_readonly_checked.png", timeout=3, confidence=0.85)
            if ro:
                ecol = locate("api_enable_text.png", timeout=2, confidence=0.9)
                cx = (ecol[0] - 137) if ecol else (ro[0] - 35)   # checkbox column
                with guard(controls_deadline, "enable"):
                    pyautogui.click(cx, ro[1])
                time.sleep(0.5)
                log("Read-Only API unchecked")
            else:
                log("Read-Only API already off — skipped")
        except Exception as exc:  # noqa: BLE001 — never block the enable on this
            if isinstance(exc, bringup.StepFailure):
                raise
            log(f"Read-Only toggle skipped: {exc}")
    with guard(controls_deadline, "enable"):
        pass
    if step_sink is not None:
        step_sink("api_enable", hwnd=cfg)
    # set Socket port: field ~99px right of the label centre, then Tab to commit
    sp = _locate_socket_port_label(
        guard, deadline=controls_deadline, abort_check=abort_check,
        policy=policy, log=log, clock=clock, sleep=sleep)
    if not sp:
        _screenshot(screenshot_sink, "socket_port_label_not_found")
        raise bringup.StepFailure("socket port label not found",
                                  code="socket_port_label_not_found",
                                  phase="controls")
    with guard(controls_deadline, "controls"):
        pyautogui.click(sp[0] + 99, sp[1])
    time.sleep(0.3)
    with guard(controls_deadline, "controls"):
        pyautogui.hotkey("ctrl", "a")
    time.sleep(0.15)
    with guard(controls_deadline, "controls"):
        pyautogui.press("delete")
    time.sleep(0.15)
    with guard(controls_deadline, "controls"):
        pyautogui.typewrite(str(port), interval=0.05)
    time.sleep(0.3)
    with guard(controls_deadline, "controls"):
        pyautogui.press("tab")                       # commit the field value
    time.sleep(0.4)
    with guard(controls_deadline, "controls"):
        pass
    if step_sink is not None:
        step_sink("port_bind", hwnd=cfg)
    # Apply, then OK (matched wherever they render)
    with guard(controls_deadline, "controls"):
        pass
    ap = locate("btn_apply.png", timeout=4, confidence=0.9,
                deadline=controls_deadline, abort_check=abort_check,
                phase="controls", clock=clock, sleep=sleep)
    log(f"apply at {ap}")
    if not ap:
        _screenshot(screenshot_sink, "apply_button_not_found")
        raise bringup.StepFailure("apply button not found",
                                  code="apply_button_not_found",
                                  phase="controls")
    with guard(controls_deadline, "controls"):
        pyautogui.click(ap[0], ap[1])
    time.sleep(3.0)
    listening = t.port_open(port)
    with guard(controls_deadline, "controls"):
        pass
    okb = locate("btn_ok.png", timeout=3, confidence=0.9,
                 deadline=controls_deadline, abort_check=abort_check,
                 phase="controls", clock=clock, sleep=sleep)
    log(f"ok at {okb}")
    if not okb:
        _screenshot(screenshot_sink, "apply_button_not_found")
        raise bringup.StepFailure("OK button not found",
                                  code="apply_button_not_found",
                                  phase="controls")
    with guard(controls_deadline, "controls"):
        pyautogui.click(okb[0], okb[1])
    time.sleep(1.5)
    log(f"port {port} listening={listening}")
    return listening
