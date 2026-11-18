"""Offline tests for deadline-aware TWS API image driving."""
import sys
import tempfile
import threading
import threading
from pathlib import Path
from types import SimpleNamespace


class _FakePyAutoGUI:
    FAILSAFE = False

    def __init__(self):
        self.shots = []

    def locateCenterOnScreen(self, *_args, **_kwargs):
        return None

    def screenshot(self, path):
        self.shots.append(str(path))

    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


sys.modules.setdefault("pyautogui", _FakePyAutoGUI())
sys.path.insert(0, str(Path(__file__).resolve().parent))

import tws_api as api
import tws_bringup as bringup


PASS = 0
FAIL = 0


def check(ok, message):
    global PASS, FAIL
    if ok:
        PASS += 1
    else:
        FAIL += 1
        print("FAIL:", message)


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(float(seconds))
        self.now += float(seconds)


class _Saved:
    def __init__(self, obj, **changes):
        self.obj = obj
        self.changes = changes
        self.old = {}

    def __enter__(self):
        for name, value in self.changes.items():
            self.old[name] = getattr(self.obj, name)
            setattr(self.obj, name, value)
        return self

    def __exit__(self, *_args):
        for name, value in self.old.items():
            setattr(self.obj, name, value)


class _MenuPyAutoGUI:
    def __init__(self):
        self.hotkeys = []
        self.presses = []
        self.clicks = []
        self.shots = []

    def hotkey(self, *keys):
        self.hotkeys.append(tuple(keys))

    def press(self, key):
        self.presses.append(key)

    def click(self, *point):
        self.clicks.append(point[0] if len(point) == 1 else tuple(point))

    def screenshot(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"focus evidence")
        self.shots.append(str(path))


def test_locate_deadline_backoff_and_abort():
    clock = _Clock()
    result = api.locate(
        "missing.png", timeout=99, deadline=3.0, clock=clock.monotonic,
        sleep=clock.sleep)
    check(result is None and clock.now == 3.0,
          "locate consumes one absolute deadline")
    check(clock.sleeps == [0.5, 0.75, 1.125, 0.625],
          "locate uses bounded deterministic backoff")
    try:
        api.locate("missing.png", abort_check=lambda: True,
                   clock=clock.monotonic, sleep=clock.sleep)
        failure = None
    except bringup.StepFailure as exc:
        failure = exc
    check(failure is not None and failure.code == "aborted",
          "locate raises a typed abort before image capture")


def test_open_config_typed_and_redacted_screenshot():
    try:
        api.open_config("DU123456", lambda _m: None)
        failure = None
    except bringup.StepFailure as exc:
        failure = exc
    check(failure is not None and failure.code == "config_open_failed",
          "missing main window is a typed config-open failure")
    check("DU123456" not in str(failure),
          "typed config-open failure omits account identity")


def test_config_window_is_bound_to_target_process():
    windows = [
        (11, "DU111 Interactive Brokers", 101, 1200, 800),
        (22, "Global Configuration", 202, 900, 700),
        (33, "Global Configuration", 101, 900, 700),
    ]
    with _Saved(api.t, _all_tws_windows=lambda: windows):
        check(api.cfg_hwnd(11) == 33,
              "configuration success cannot use another TWS process")


def test_open_config_falls_back_from_file_to_edit_menu():
    clock = _Clock()
    gui = _MenuPyAutoGUI()
    menu_matches = iter((None, (40, 50)))
    logs = []
    steps = []
    focus_checks = []
    with _Saved(api, pyautogui=gui,
                main_hwnd=lambda _acct: 11,
                cfg_hwnd=lambda _owner=None: 77,
                close_configs=lambda: None,
                _ack_terms_dialogs=lambda *_a, **_k: 0,
                locate=lambda *_a, **_k: next(menu_matches)), _Saved(
            api.t, close_nuisance_popups=lambda: 0,
            close_resource_warnings=lambda: 0,
            _minimize_others=lambda _h: None,
            pin_window=lambda *_a, **_k: None,
            foreground=lambda _h: None,
            foreground_window=lambda: (
                focus_checks.append(11) or (11, "target"))):
        result = api.open_config(
            "DU123456", logs.append,
            step_sink=lambda step, **kw: steps.append((step, kw)),
            policy=api.ImageWaitPolicy(config_open_timeout=20),
            clock=clock.monotonic, sleep=clock.sleep)
    check(result == 77,
          "Edit-menu fallback returns the verified configuration window")
    check(gui.hotkeys == [],
          "configuration opener never delegates menu choice to nested focus")
    check(gui.clicks == [(20, 17), (56, 17), (40, 50)],
          "configuration opener clicks Mosaic File before Classic Edit, then "
          "the image-verified item")
    check(focus_checks == [11] * 6,
          "configuration opener proves foreground before every Escape, "
          "application-menu click, and image-matched click")
    check(logs == ["config opened via Edit"],
          "configuration opener reports the successful layout path")
    check(steps == [("config_open", {"hwnd": 77, "menu_path": "Edit"})],
          "configuration opener exposes the winning menu path to proof mode")


def test_open_config_focus_loss_is_bounded_typed_and_evidenced():
    clock = _Clock()
    gui = _MenuPyAutoGUI()
    root = Path(tempfile.mkdtemp(prefix="api_focus_"))
    isolation = []
    try:
        with _Saved(
                api, pyautogui=gui, main_hwnd=lambda _acct: 11,
                close_configs=lambda: None,
                _ack_terms_dialogs=lambda *_a, **_k: 0,
                locate=lambda *_a, **_k: None), _Saved(
                api.t, close_nuisance_popups=lambda: 0,
                close_resource_warnings=lambda: 0,
                _minimize_others=lambda h: isolation.append(("min", h)),
                pin_window=lambda h, *_a, **_k:
                isolation.append(("pin", h)),
                foreground=lambda h: isolation.append(("raise", h)),
                foreground_window=lambda: (
                    99, "DU004 Interactive Brokers — Data Viewer")):
            try:
                api.open_config(
                    "DU123456", lambda _m: None,
                    screenshot_sink=lambda code: root / f"{code}.png",
                    policy=api.ImageWaitPolicy(
                        config_open_timeout=5, focus_timeout=0.3,
                        focus_interval=0.1),
                    clock=clock.monotonic, sleep=clock.sleep)
                failure = None
            except bringup.StepFailure as exc:
                failure = exc
        holder = getattr(failure, "foreground_holder", "")
        check(failure is not None and failure.code == "focus_lost"
              and bringup.is_retryable(failure),
              "foreground exhaustion is a typed retryable focus_lost")
        check("hwnd=99" in holder and "DU_REDACTED" in holder
              and "DU004" not in holder,
              "focus_lost names and redacts the observed foreground holder")
        check(gui.shots == [str(root / "focus_lost.png")]
              and (root / "focus_lost.png").is_file(),
              "focus_lost captures the holder before later recovery actions")
        check(gui.presses == [] and gui.clicks == [],
              "focus exhaustion stops before any GUI input reaches a window")
        check(clock.now <= 1.25
              and len([row for row in isolation if row[0] == "raise"]) >= 3,
              "foreground recovery is retried within one bounded deadline")
    finally:
        for path in root.glob("*"):
            path.unlink()
        root.rmdir()


def test_foreground_verifier_recovers_before_input():
    clock = _Clock()
    holders = iter((
        (99, "Data Bank — Data Viewer"),
        (99, "Data Bank — Data Viewer"),
        (11, "DU123456 Interactive Brokers"),
    ))
    isolation = []
    with _Saved(api, main_hwnd=lambda _acct: 11), _Saved(
            api.t,
            foreground_window=lambda: next(holders),
            _minimize_others=lambda h: isolation.append(("min", h)),
            pin_window=lambda h, *_a, **_k:
            isolation.append(("pin", h)),
            foreground=lambda h: isolation.append(("raise", h))):
        result = api._require_target_foreground(
            "DU123456", 11, deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy(
                focus_timeout=1, focus_interval=0.1),
            clock=clock.monotonic, sleep=clock.sleep)
    check(result == (11, True)
          and [row[0] for row in isolation]
          == ["min", "pin", "raise", "min", "pin", "raise"],
          "foreground verifier re-isolates until the exact target wins")
    check(clock.now == 0.2,
          "foreground recovery stops immediately after verified ownership")


def test_owned_config_foreground_recovers_after_resource_warning():
    clock = _Clock()
    holders = iter((
        (99, "DU004 IBKR Trader Workstation (Demo System)"),
        (77, "Global Configuration"),
    ))
    raised = []
    with _Saved(
            api.t, foreground_window=lambda: next(holders),
            foreground=lambda hwnd: raised.append(hwnd)):
        result = api._require_window_foreground(
            77, deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy(
                focus_timeout=1, focus_interval=0.1),
            clock=clock.monotonic, sleep=clock.sleep)
    check(result == (77, True) and raised == [77],
          "owned config foreground recovers after a resource warning steals it")


def test_prepare_config_input_clears_resource_warning_first():
    order = []

    def require(hwnd, **_kwargs):
        order.append(("verify", hwnd))
        return hwnd, False

    with _Saved(api, _require_window_foreground=require), _Saved(
            api.t, close_resource_warnings=lambda: order.append("close") or 1):
        result = api._prepare_config_input(
            77, deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy())
    check(result == (77, False)
          and order == ["close", ("verify", 77)],
          "config input preparation dismisses resource warnings before focus")


def test_terms_dialog_candidates_exclude_resource_warning():
    rows = [
        (1, "DU123 IBKR Trader Workstation (Demo System)", 10, 642, 341),
        (2, "DU123 IBKR Trader Workstation (Demo System)", 10, 642, 430),
        (3, "DU999 IBKR Trader Workstation (Demo System)", 11, 642, 430),
    ]
    with _Saved(api.t, _all_tws_windows=lambda: rows):
        candidates = api._terms_dialog_windows("DU123")
    check(candidates == [rows[1]],
          "API terms selection excludes the resource-warning lookalike")


def test_terms_handler_rechecks_midflow_and_leaves_lookalikes_untouched():
    clock = _Clock()
    gui = _MenuPyAutoGUI()
    candidate = (
        44, "DU123 IBKR Trader Workstation (Demo System)", 10, 642, 430)
    candidate_rounds = iter(([candidate], []))
    foreground_checks = []
    logs = []
    with _Saved(
            api, pyautogui=gui,
            _terms_dialog_windows=lambda _acct: next(candidate_rounds),
            _window_region=lambda _hwnd: (100, 100, 642, 430),
            locate=lambda name, **_kwargs:
            (420, 360) if name == "menu_acknowledge.png" else None,
            _require_window_foreground=lambda hwnd, **_kwargs:
            foreground_checks.append(hwnd) or (hwnd, False)):
        count = api._ack_terms_dialogs(
            "DU123", deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy(), log=logs.append,
            clock=clock.monotonic, sleep=clock.sleep)
    check(count == 1 and gui.clicks == [(420, 360)]
          and foreground_checks == [44, 44],
          "T&C handler re-identifies and foreground-proves the exact dialog")
    check(logs == ["acknowledged API T&C 1"],
          "T&C acknowledgement remains visible in the progress log")

    gui.clicks.clear()
    foreground_checks.clear()
    with _Saved(
            api, pyautogui=gui,
            _terms_dialog_windows=lambda _acct: [candidate],
            _window_region=lambda _hwnd: (100, 100, 642, 430),
            locate=lambda *_args, **_kwargs: None,
            _require_window_foreground=lambda hwnd, **_kwargs:
            foreground_checks.append(hwnd) or (hwnd, False)):
        count = api._ack_terms_dialogs(
            "DU123", deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy(),
            clock=clock.monotonic, sleep=clock.sleep)
    check(count == 0 and not gui.clicks and not foreground_checks,
          "title lookalike without the acknowledgement cue is never touched")

    terms_checks = []
    inputs = []
    with _Saved(
            api, _ack_terms_dialogs=lambda *_args, **_kwargs:
            terms_checks.append("terms") or 0,
            _require_window_foreground=lambda hwnd, **_kwargs:
            (hwnd, False)), _Saved(
            api.t, close_resource_warnings=lambda: 0):
        for label in ("first", "midflow"):
            with api._config_input_guard(
                    77, deadline=5, abort_check=None,
                    policy=api.ImageWaitPolicy(), acct="DU123"):
                inputs.append(label)
    check(terms_checks == ["terms", "terms"]
          and inputs == ["first", "midflow"],
          "every guarded config input rechecks for a mid-flow T&C")

    target_terms = []
    with _Saved(
            api, _ack_terms_dialogs=lambda *_args, **_kwargs:
            target_terms.append("terms") or 0,
            _require_target_foreground=lambda *_args, **_kwargs:
            (11, False)), _Saved(
            api.t, close_nuisance_popups=lambda: 0,
            close_resource_warnings=lambda: 0):
        with api._target_input_guard(
                "DU123", 11, deadline=5, abort_check=None,
                policy=api.ImageWaitPolicy()):
            inputs.append("menu")
    check(target_terms == ["terms"] and inputs[-1] == "menu",
          "main-window menu inputs also clear a leftover mid-flow T&C")


def test_socket_port_row_scrolls_and_relocates_boundedly():
    class _ScrollGUI:
        def __init__(self):
            self.moves = []
            self.scrolls = []

        def moveTo(self, *point):
            self.moves.append(tuple(point))

        def scroll(self, amount):
            self.scrolls.append(amount)

    class _Guard:
        def __init__(self, entries):
            self.entries = entries

        def __enter__(self):
            self.entries.append("enter")

        def __exit__(self, *_exc):
            self.entries.append("exit")

    clock = _Clock()
    gui = _ScrollGUI()
    entries = []
    locate_calls = []
    answers = iter((None, (70, 80)))

    def guard(_deadline, _phase):
        return _Guard(entries)

    def locate_once(name, **_kwargs):
        locate_calls.append(name)
        return next(answers)

    with _Saved(api, pyautogui=gui, locate=locate_once):
        found = api._locate_socket_port_label(
            guard, deadline=5, abort_check=None,
            policy=api.ImageWaitPolicy(), log=lambda _m: None,
            clock=clock.monotonic, sleep=clock.sleep)
    check(found == (70, 80) and len(locate_calls) == 2
          and gui.moves == [api.SOCKET_PORT_SCROLL_POINT]
          and gui.scrolls == [api.SOCKET_PORT_SCROLL_NOTCHES],
          "socket-port label is re-located after one guarded upward scroll")

    gui = _ScrollGUI()
    locate_calls.clear()
    entries.clear()
    with _Saved(
            api, pyautogui=gui,
            locate=lambda name, **_kwargs:
            locate_calls.append(name) or None):
        found = api._locate_socket_port_label(
            guard, deadline=20, abort_check=None,
            policy=api.ImageWaitPolicy(), log=lambda _m: None,
            clock=clock.monotonic, sleep=clock.sleep)
    check(found is None
          and len(locate_calls) == api.SOCKET_PORT_SCROLL_ATTEMPTS
          and len(gui.scrolls) == api.SOCKET_PORT_SCROLL_ATTEMPTS - 1,
          "socket-port relocation exhausts a fixed locate/scroll bound")


def test_guarded_input_interlock_blocks_the_sweeper():
    status = []
    close_calls = []
    sweeper = api.t.ResourceWarningSweeper(interval=0.05)

    def prepared(hwnd, **_kwargs):
        return hwnd, False

    with _Saved(api, _prepare_config_input=prepared), _Saved(
            api.t, close_resource_warnings=lambda *_a, **_k:
            close_calls.append(True) or 0):
        with api._config_input_guard(
                77, deadline=5, abort_check=None,
                policy=api.ImageWaitPolicy(), acct="DU123"):
            worker = threading.Thread(
                target=lambda: status.append(sweeper.tick()))
            worker.start()
            worker.join()
            input_sent = True
    check(input_sent and status == ["interlocked"] and not close_calls,
          "sweeper cannot act between foreground proof and guarded input")


def test_focus_recovery_discards_stale_menu_match():
    clock = _Clock()
    gui = _MenuPyAutoGUI()
    menu_matches = iter(((40, 50), (60, 70)))
    foregrounds = iter((
        (11, "target"),
        (11, "target"),
        (99, "Data Viewer"),
        (11, "target"),
        (11, "target"),
        (11, "target"),
        (11, "target"),
    ))
    logs = []
    with _Saved(
            api, pyautogui=gui, main_hwnd=lambda _acct: 11,
            cfg_hwnd=lambda _owner=None: 77, close_configs=lambda: None,
            _ack_terms_dialogs=lambda *_a, **_k: 0,
            locate=lambda *_a, **_k: next(menu_matches)), _Saved(
            api.t, close_nuisance_popups=lambda: 0,
            close_resource_warnings=lambda: 0,
            _minimize_others=lambda _h: None,
            pin_window=lambda *_a, **_k: None,
            foreground=lambda _h: None,
            foreground_window=lambda: next(foregrounds)):
        result = api.open_config(
            "DU123456", logs.append,
            policy=api.ImageWaitPolicy(
                config_open_timeout=20, focus_timeout=1,
                focus_interval=0.1),
            clock=clock.monotonic, sleep=clock.sleep)
    check(result == 77,
          "configuration opening can recover from a transient focus steal")
    check(gui.clicks == [(20, 17), (20, 17), (60, 70)]
          and (40, 50) not in gui.clicks,
          "focus recovery discards the stale match and reopens the menu")
    check(logs == [
        "open_config focus recovered before File item; reopening menu",
        "open_config retry 0",
        "config opened via File",
    ], "focus-recovery retry remains explicit in the run log")


def test_configure_emits_control_steps_in_order():
    steps = []

    def opened(*_args, **kwargs):
        kwargs["step_sink"](
            "config_open", hwnd=77, menu_path="File")
        return 77

    def matched(name, **_kwargs):
        if name == "api_readonly_checked.png":
            return None
        return (100, 100)

    with _Saved(
            api, open_config=opened, locate=matched,
            _prepare_config_input=lambda *_a, **_k: (77, False)), _Saved(
            api.t, foreground=lambda *_a, **_k: None,
            port_open=lambda *_a, **_k: True), _Saved(
            api.time, sleep=lambda _seconds: None):
        result = api.configure(
            "DU123456", 2000, tick_enable=False,
            step_sink=lambda step, **kw: steps.append((step, kw)))
    check(result is True,
          "configure proof seam preserves the normal success result")
    check([step for step, _kw in steps]
          == ["config_open", "api_enable", "port_bind"],
          "configure emits config, API-enable, and port-bind proof steps")
    check(steps[0][1]["menu_path"] == "File"
          and all(row[1]["hwnd"] == 77 for row in steps),
          "configure proof steps remain bound to the exact config window")


def test_configure_guards_every_owned_dialog_input():
    class _GuardedGUI:
        def __init__(self):
            self.generation = 0
            self.last_input_generation = 0
            self.unguarded = []
            self.events = []

        def arm(self):
            self.generation += 1

        def record(self, name):
            if self.generation <= self.last_input_generation:
                self.unguarded.append(name)
            self.last_input_generation = self.generation
            self.events.append(name)

        def doubleClick(self, *_args, **_kwargs):
            self.record("doubleClick")

        def click(self, *_args, **_kwargs):
            self.record("click")

        def hotkey(self, *_args, **_kwargs):
            self.record("hotkey")

        def press(self, *_args, **_kwargs):
            self.record("press")

        def typewrite(self, *_args, **_kwargs):
            self.record("typewrite")

    gui = _GuardedGUI()
    enable_matches = 0

    def matched(name, **_kwargs):
        nonlocal enable_matches
        if name == "api_enable_text.png":
            enable_matches += 1
            return None if enable_matches == 1 else (20, 20)
        if name == "api_node.png":
            return (10, 10)
        if name in {"api_socketport_label.png", "btn_apply.png", "btn_ok.png"}:
            return (30, 30)
        return None

    def prepare(*_args, **_kwargs):
        gui.arm()
        return 77, False

    with _Saved(
            api, pyautogui=gui,
            open_config=lambda *_a, **_k: 77,
            locate=matched, _prepare_config_input=prepare), _Saved(
            api.t, port_open=lambda _port: True), _Saved(
            api.time, sleep=lambda _seconds: None):
        result = api.configure("DU123456", 2000, tick_enable=False)
    check(result is True and not gui.unguarded,
          "every owned-config GUI input has a fresh foreground guard")
    check(gui.events == [
        "doubleClick", "click", "click", "hotkey", "press",
        "typewrite", "press", "click", "click",
    ], "navigation, port entry, Apply, and OK all use the guarded path")


def _configure_failure(locate_fn, tick_enable=False):
    clock = _Clock()
    policy = api.ImageWaitPolicy(
        config_open_timeout=1, navigation_timeout=2, controls_timeout=2)
    with _Saved(
            api, open_config=lambda *a, **k: 1, locate=locate_fn,
            _prepare_config_input=lambda *_a, **_k: (1, False)), _Saved(
            api.t, foreground=lambda *_a, **_k: None,
            port_open=lambda *_a, **_k: False), _Saved(
            api.time, sleep=lambda _seconds: None):
        try:
            api.configure(
                "DU123456", 2000, tick_enable=tick_enable, policy=policy,
                clock=clock.monotonic, sleep=clock.sleep)
            return None
        except bringup.StepFailure as exc:
            return exc


def test_navigation_and_control_codes():
    def missing_nav(name, **_kw):
        return None

    failure = _configure_failure(missing_nav)
    check(failure is not None and failure.code == "api_node_not_found",
          "navigation exhaustion maps to api_node_not_found")

    def missing_port(name, **_kw):
        if name == "api_enable_text.png":
            return (10, 10)
        return None

    failure = _configure_failure(missing_port)
    check(failure is not None
          and failure.code == "socket_port_label_not_found",
          "missing socket label has its reviewed stable code")

    def missing_apply(name, **_kw):
        if name in {"api_enable_text.png", "api_socketport_label.png"}:
            return (10, 10)
        return None

    failure = _configure_failure(missing_apply)
    check(failure is not None and failure.code == "apply_button_not_found",
          "missing apply control has its reviewed stable code")


def test_screenshot_sink_controls_filename():
    fake = sys.modules["pyautogui"]
    fake.shots.clear()
    root = Path(tempfile.mkdtemp(prefix="api_shot_"))
    api._screenshot(lambda code: root / f"{code}.png",
                    "enable_text_not_matched")
    check(fake.shots == [str(root / "enable_text_not_matched.png")],
          "diagnostic screenshot uses only the caller-controlled sink path")


def main():
    for name, value in sorted(globals().items()):
        if name.startswith("test_") and callable(value):
            value()
    total = PASS + FAIL
    print(f"tws_api_selftest: {PASS}/{total} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
