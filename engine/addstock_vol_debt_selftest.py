"""Headless dynamic tests for Row 48 port-free verification controls.

The relevant ``DataViewerApp`` methods are extracted from ``display_data.py``
with :mod:`ast`.  This module never imports ``display_data`` or tkinter.  All
worker threads and cancellation/pause events are real ``threading`` objects;
``FakeRoot.after`` queues GUI callbacks for explicit main-thread draining.

Run with::

    python -B engine/addstock_vol_debt_selftest.py
"""

from __future__ import annotations

import ast
from collections import deque
from pathlib import Path
import queue
import sys
import threading
import time


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
SOURCE = PROJECT_ROOT / "display_data.py"
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402
from event_probe import Collector  # noqa: E402


KIT = CheckKit()
check = KIT.check
section = KIT.section
TIMEOUT = 2.0
METHODS = {
    "_addstock_consume_portfree_debt",
    "_addstock_reconcile_evidence",
    "_addstock_resume_active",
    "_storage_find_build_state",
    "_storage_find_cancel",
    "_storage_find_pause",
    "_storage_find_close",
    "_storage_find_search_poll",
    "_storage_find_set_recovery_blocked",
}


class FakeTk:
    DISABLED = "disabled"
    NORMAL = "normal"


class FakeRoot:
    """Thread-safe ``after`` queue that only the test's main thread drains."""

    def __init__(self, main_ident):
        self.main_ident = main_ident
        self.after_call_threads = []
        self._callbacks = deque()
        self._lock = threading.Lock()

    def after(self, delay, callback):
        with self._lock:
            self.after_call_threads.append(threading.get_ident())
            self._callbacks.append((delay, callback))
            return len(self._callbacks)

    def drain_once(self):
        if threading.get_ident() != self.main_ident:
            raise AssertionError("FakeRoot callbacks must be drained on GUI thread")
        with self._lock:
            if not self._callbacks:
                return False
            _delay, callback = self._callbacks.popleft()
        callback()
        return True

    def drain_all(self):
        count = 0
        while self.drain_once():
            count += 1
        return count

    def pending(self):
        with self._lock:
            return len(self._callbacks)


class FakeButton:
    def __init__(self, owner, *, state=FakeTk.DISABLED, text=None):
        self.owner = owner
        self.options = {"state": state}
        if text is not None:
            self.options["text"] = text
        self.calls = []

    def config(self, **options):
        ident = threading.get_ident()
        self.calls.append((ident, dict(options)))
        self.options.update(options)
        if ident != self.owner.main_ident:
            self.owner.gui_thread_violations.append(
                ("button", ident, dict(options)))

    configure = config


class FakeWindow:
    def __init__(self):
        self.destroy_calls = 0

    def destroy(self):
        self.destroy_calls += 1


class FakeTree:
    def __init__(self, children=()):
        self.children = tuple(children)

    def get_children(self):
        return self.children


class FakeManifest:
    class ManifestError(Exception):
        pass

    class RunMismatch(ManifestError):
        pass

    def __init__(self, data, *, archive_when_clear=False,
                 disappear_when_clear=False):
        self._lock = threading.RLock()
        self.active = data
        self.archive_when_clear = archive_when_clear
        self.disappear_when_clear = disappear_when_clear
        self.archive_event = threading.Event()
        self.disappear_event = threading.Event()
        self.fail_reads = False
        self.load_calls = 0

    def load_run(self, _root):
        with self._lock:
            self.load_calls += 1
            if self.fail_reads:
                raise self.ManifestError("simulated unreadable recovery")
            return self.active

    @staticmethod
    def pending_selections(_data):
        return []

    def exempt_empty_verification(
            self, _root, run_id, empty_fn, *, now=None, archive_dir=None):
        del now, archive_dir
        with self._lock:
            if self.active is None:
                raise self.ManifestError("active recovery is missing")
            if self.active.get("run_id") != run_id:
                raise self.RunMismatch("stale empty-series heal")
            exempted = []
            for ticker, record in self.active["tickers"].items():
                for interval, series in record["series"].items():
                    if series["xval"] and series["gaps"]:
                        continue
                    try:
                        is_empty = empty_fn(ticker, interval)
                    except Exception:
                        continue
                    if is_empty is not True:
                        continue
                    series["xval"] = True
                    series["gaps"] = True
                    exempted.append((ticker, interval))
            clear = all(
                series["xval"] and series["gaps"]
                for record in self.active["tickers"].values()
                for series in record["series"].values())
            if self.archive_when_clear and exempted and clear:
                self.active = None
                self.archive_event.set()
                return {"exempted": exempted,
                        "archived": "run-log.json"}
            return {"exempted": exempted, "archived": None}

    def credit(self, ticker, stage, intervals):
        with self._lock:
            if self.active is None:
                return {"archived": "already-archived"}
            series = self.active["tickers"][ticker]["series"]
            for interval in intervals or ():
                series[interval][stage] = True
            clear = all(
                    state["xval"] and state["gaps"]
                    for record in self.active["tickers"].values()
                    for state in record["series"].values())
            if self.archive_when_clear and clear:
                self.active = None
                self.archive_event.set()
                return {"archived": "run-log.json"}
            if self.disappear_when_clear and clear:
                # Simulate external loss: absence without a positive archive
                # result must never be interpreted as successful completion.
                self.active = None
                self.disappear_event.set()
                return {"archived": None}
            return {"archived": None}


class SecondReadManifest(FakeManifest):
    """Change recovery state exactly at reconcile's final authority read."""

    def __init__(self, data, terminal):
        super().__init__(data)
        self.terminal = terminal

    def load_run(self, _root):
        with self._lock:
            self.load_calls += 1
            if self.load_calls == 2:
                if self.terminal == "missing":
                    self.active = None
                    return None
                if self.terminal == "unreadable":
                    raise self.ManifestError(
                        "simulated second-read corruption")
                if self.terminal == "replaced":
                    self.active = make_resume_data("replacement-run")
                    return self.active
            return self.active


class FakeStorage:
    @staticmethod
    def session_of(interval):
        if str(interval).endswith("-pre"):
            return "pre"
        if str(interval).endswith("-post"):
            return "post"
        return "rth"


class FakeValidator:
    def __init__(self):
        self._lock = threading.Lock()
        self.series_calls = []
        self.write_calls = []
        self.cross_entries = {}
        self.earliest = {}
        self.gap_rows = []

    def load_cross_validation(self, _root):
        return dict(self.cross_entries)

    def load_ibkr_earliest(self, _root):
        return dict(self.earliest)

    def evaluate_gap_evidence(self, _root, _series):
        return {"rows": list(self.gap_rows)}

    @staticmethod
    def cross_validation_key(ticker, interval):
        return f"{ticker} {interval}"

    def _record(self, stage, ticker, interval):
        with self._lock:
            self.series_calls.append(
                (stage, ticker, interval, threading.get_ident()))

    def cross_validate_ticker(self, _root, ticker, interval):
        self._record("xval", ticker, interval)
        return {"ticker": ticker, "interval": interval}

    def record_cross_validation_many(self, _root, entries):
        with self._lock:
            self.write_calls.append(
                ("xval", tuple(entry["interval"] for entry in entries)))
        return {"written": len(entries)}

    def update_gap_report(self, _root, series):
        ticker, interval = series[0]
        self._record("gaps", ticker, interval)
        with self._lock:
            self.write_calls.append(("gaps", (interval,)))
        return {"error": None, "sidecar": "data_gaps.json"}


def make_data(run_id, ticker_series):
    """Build the worker's bounded manifest view while preserving input order."""
    tickers = {}
    for ticker, specs in ticker_series:
        tickers[ticker] = {
            "series": {
                interval: {"xval": xval, "gaps": gaps}
                for interval, xval, gaps in specs
            }
        }
    return {"run_id": run_id, "tickers": tickers}


def make_resume_data(run_id, *, earliest=True):
    return {
        "run_id": run_id,
        "state": "interrupted",
        "params": {},
        "tickers": {
            "RESUME": {
                "earliest": bool(earliest),
                "series": {
                    "1m-iv": {
                        "state": "complete",
                        "xval": True,
                        "gaps": True,
                    },
                },
            },
        },
    }


def _compile_probe():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
    app = next(node for node in tree.body
               if isinstance(node, ast.ClassDef)
               and node.name == "DataViewerApp")
    found = {node.name: node for node in app.body
             if isinstance(node, ast.FunctionDef) and node.name in METHODS}
    missing = METHODS - set(found)
    if missing:
        raise AssertionError(f"missing production methods: {sorted(missing)}")
    probe_node = ast.ClassDef(
        name="Probe",
        bases=[],
        keywords=[],
        body=[found[name] for name in sorted(METHODS)],
        decorator_list=[],
    )
    module = ast.Module(body=[probe_node], type_ignores=[])
    ast.fix_missing_locations(module)
    return compile(module, "<row48-portfree-debt-probe>", "exec")


PROBE_CODE = _compile_probe()


def extract_probe(manifest, validator):
    namespace = {
        "addstock_run_manifest": manifest,
        "stock_storage": FakeStorage,
        "stock_validate": validator,
        "tk": FakeTk,
    }
    exec(PROBE_CODE, namespace)
    return namespace["Probe"]


def _say(self, text, append=True, warn=False):
    del append, warn
    ident = threading.get_ident()
    self.say_calls.append((ident, str(text)))
    if ident != self.main_ident:
        self.gui_thread_violations.append(("say", ident, str(text)))


def _idle(self):
    if threading.get_ident() != self.main_ident:
        self.gui_thread_violations.append(
            ("idle", threading.get_ident(), "worker called idle"))
    self._storage_find_building = False
    self._storage_find_phase = None
    self._storage_find_ev = None
    self._storage_find_pause_ev = None
    self._storage_find_pause_generation = None
    self._find_pause_state = None
    self._storage_find_build_btn.config(
        state=self._storage_find_build_state())
    self._storage_find_search_btn.config(state=FakeTk.NORMAL)
    self._storage_find_cancel_btn.config(state=FakeTk.DISABLED)
    self._storage_find_pause_btn.config(
        state=FakeTk.DISABLED, text="Pause")
    self.idle_calls += 1


def _refresh(self):
    ident = threading.get_ident()
    self.refresh_calls.append(ident)
    if ident != self.main_ident:
        self.gui_thread_violations.append(("refresh", ident, "refresh"))


def _xval_intervals(self, entries):
    return [entry["interval"] for entry in entries]


def _gap_intervals(self, _ticker, intervals):
    return list(intervals)


def _credit(self, ticker, stage, intervals=None, run_id=None):
    intervals = tuple(intervals or ())
    result = self.manifest.credit(ticker, stage, intervals)
    self.credit_results.append(result)
    self.credit_calls.append((ticker, stage, intervals, run_id,
                              threading.get_ident()))
    self.events({
        "kind": "durable_credit",
        "ticker": ticker,
        "stage": stage,
        "intervals": intervals,
        "run_id": run_id,
        "count": len(self.credit_calls),
    })
    hook = self.credit_hook
    if hook is not None:
        hook(self, len(self.credit_calls))
    return result


def _bomb(name):
    def _called(self, *args, **kwargs):
        self.fetch_only_calls.append((name, args, kwargs))
        raise AssertionError(f"verification control entered fetch-only {name}")
    return _called


def new_probe(data, *, archive_when_clear=False,
              disappear_when_clear=False, manifest=None, validator=None):
    manifest = manifest or FakeManifest(
        data,
        archive_when_clear=archive_when_clear,
        disappear_when_clear=disappear_when_clear)
    validator = validator or FakeValidator()
    Probe = extract_probe(manifest, validator)
    Probe._storage_find_say = _say
    Probe._storage_find_idle = _idle
    Probe._addstock_refresh_debt_indicator = _refresh
    Probe._addstock_current_xval_intervals = _xval_intervals
    Probe._addstock_current_gap_intervals = _gap_intervals
    Probe._addstock_credit = _credit
    Probe._addstock_interval_empty_on_disk = (
        lambda _self, _ticker, _interval: False)
    Probe._addstock_archive_dir = lambda _self: "fake-run-logs"
    for name in (
            "_batch_resume", "_xval_start", "_find_pause_status_detail",
            "_storage_find_finalize", "_storage_find_paused_complete"):
        setattr(Probe, name, _bomb(name))

    probe = Probe()
    probe.main_ident = threading.get_ident()
    probe.root = FakeRoot(probe.main_ident)
    probe.manifest = manifest
    probe.validator = validator
    probe._storage_root = "fake-bank"
    probe.say_calls = []
    probe.gui_thread_violations = []
    probe.credit_calls = []
    probe.credit_results = []
    probe.credit_hook = None
    probe.events = Collector()
    probe.fetch_only_calls = []
    probe.refresh_calls = []
    probe.idle_calls = 0
    probe._storage_find_building = False
    probe._storage_find_phase = None
    probe._storage_find_ev = threading.Event()
    probe._storage_find_pause_ev = threading.Event()
    probe._storage_find_pause_generation = 99
    probe._storage_find_recovery_blocked = False
    probe._find_pause_state = None
    probe._storage_find_busy = False
    probe._storage_find_win = FakeWindow()
    probe._storage_find_tree = FakeTree()
    probe._storage_find_sq = None
    probe._find_req_q = None
    probe._find_reuse_adapter = None
    probe._storage_find_build_btn = FakeButton(probe)
    probe._storage_find_search_btn = FakeButton(probe)
    probe._storage_find_pause_btn = FakeButton(probe, text="Pause")
    probe._storage_find_cancel_btn = FakeButton(probe)
    return probe


def start_debt(probe, data):
    """Start through production code while auditing the real Thread boundary."""
    previous_cancel = probe._storage_find_ev
    previous_pause = probe._storage_find_pause_ev
    real_thread = threading.Thread
    snapshots = []

    def thread_factory(*args, **kwargs):
        snapshots.append({
            "thread": threading.get_ident(),
            "cancel": probe._storage_find_ev,
            "pause": probe._storage_find_pause_ev,
            "phase": probe._storage_find_phase,
            "building": probe._storage_find_building,
            "generation": probe._storage_find_pause_generation,
            "pause_button": dict(probe._storage_find_pause_btn.options),
            "cancel_button": dict(probe._storage_find_cancel_btn.options),
        })
        return real_thread(*args, **kwargs)

    threading.Thread = thread_factory
    try:
        probe._addstock_consume_portfree_debt(data)
    finally:
        threading.Thread = real_thread
    return previous_cancel, previous_pause, snapshots


def wait_until(predicate, *, root=None, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if root is not None:
            root.drain_all()
        if predicate():
            return True
        time.sleep(0.005)
    if root is not None:
        root.drain_all()
    return bool(predicate())


def wait_worker(probe, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        workers = [thread for thread in threading.enumerate()
                   if thread.name == "addstock-verification-debt"
                   and thread.is_alive()]
        if not workers:
            probe.root.drain_all()
            return True
        for thread in workers:
            thread.join(0.01)
        probe.root.drain_all()
    return False


def messages(probe):
    return [text for _ident, text in probe.say_calls]


def finish_search(probe, *, children=("row",)):
    result_queue = queue.Queue()
    result_queue.put(("sdone", None))
    probe._storage_find_sq = result_queue
    probe._storage_find_busy = True
    probe._storage_find_tree = FakeTree(children)
    probe._storage_find_search_poll()


def test_clean_flow():
    data = make_data("run-clean", [
        ("AAA", [("1m-iv", False, False)]),
        ("BBB", [("1d-hvol", False, False)]),
    ])
    probe = new_probe(data)
    old_cancel, old_pause, snapshots = start_debt(probe, data)
    started_cancel = probe._storage_find_ev
    started_pause = probe._storage_find_pause_ev

    snapshot = snapshots[0] if snapshots else {}
    check("fresh real cancel/pause events replace stale handles before Thread",
          len(snapshots) == 1
          and isinstance(snapshot.get("cancel"), threading.Event)
          and isinstance(snapshot.get("pause"), threading.Event)
          and snapshot["cancel"] is not old_cancel
          and snapshot["pause"] is not old_pause
          and snapshot["cancel"] is not snapshot["pause"], repr(snapshot))
    check("verification phase and both controls are enabled before Thread",
          snapshot.get("thread") == probe.main_ident
          and snapshot.get("phase") == "verification"
          and snapshot.get("building") is True
          and snapshot.get("generation") == 0
          and snapshot.get("pause_button") == {
              "state": FakeTk.NORMAL, "text": "Pause"}
          and snapshot.get("cancel_button", {}).get("state")
          == FakeTk.NORMAL, repr(snapshot))
    finished = wait_worker(probe)
    check("clean verification worker terminates within deterministic timeout",
          finished)

    calls = [(stage, ticker, interval)
             for stage, ticker, interval, _ident
             in probe.validator.series_calls]
    expected = [
        ("xval", "AAA", "1m-iv"),
        ("gaps", "AAA", "1m-iv"),
        ("xval", "BBB", "1d-hvol"),
        ("gaps", "BBB", "1d-hvol"),
    ]
    check("clean flow validates and durably credits each series one at a time",
          calls == expected
          and [(ticker, stage, intervals)
               for ticker, stage, intervals, _rid, _ident
               in probe.credit_calls] == [
                   ("AAA", "xval", ("1m-iv",)),
                   ("AAA", "gaps", ("1m-iv",)),
                   ("BBB", "xval", ("1d-hvol",)),
                   ("BBB", "gaps", ("1d-hvol",)),
               ], repr(calls))
    progress = [text for text in messages(probe)
                if text.startswith("Verifying ")]
    check("per-series progress keeps stable per-ticker k/N positions",
          progress == [
              "Verifying AAA (1/2): xval 1m-iv...",
              "Verifying AAA (1/2): gaps 1m-iv...",
              "Verifying BBB (2/2): xval 1d-hvol...",
              "Verifying BBB (2/2): gaps 1d-hvol...",
          ], repr(progress))
    check("all Tk-facing say/button work stays on the queued GUI thread",
          not probe.gui_thread_violations
          and all(ident == probe.main_ident
                  for ident, _text in probe.say_calls)
          and all(ident == probe.main_ident
                  for button in (
                      probe._storage_find_build_btn,
                      probe._storage_find_search_btn,
                      probe._storage_find_pause_btn,
                      probe._storage_find_cancel_btn)
                  for ident, _options in button.calls),
          repr(probe.gui_thread_violations))
    check("unresolved clean pass releases worker but preserves recovery Build gate",
          started_cancel is snapshot.get("cancel")
          and started_pause is snapshot.get("pause")
          and probe.idle_calls == 1
          and probe._storage_find_building is False
          and probe._storage_find_search_btn.options["state"] == FakeTk.NORMAL
          and probe._storage_find_build_btn.options["state"]
          == FakeTk.DISABLED
          and probe._storage_find_recovery_blocked is True)


def test_empty_heal_short_circuits_evidence():
    data = make_data("run-empty", [
        ("EMPTY", [("1d-hvol", False, False)]),
    ])
    probe = new_probe(data, archive_when_clear=True)
    probe._addstock_interval_empty_on_disk = (
        lambda ticker, interval:
        (ticker, interval) == ("EMPTY", "1d-hvol"))
    start_debt(probe, data)
    finished = wait_worker(probe)
    rendered = messages(probe)
    note = (
        "EMPTY 1d-hvol: empty series — no data to verify "
        "(kind not computable with current history); verification excused")
    check("empty heal archives before any xval or gap work",
          finished and probe.manifest.active is None
          and probe.manifest.archive_event.is_set()
          and not probe.validator.series_calls
          and not probe.credit_calls,
          repr(probe.validator.series_calls))
    check("empty heal logs the exact exemption and positive completion",
          note in rendered
          and rendered[-1:] == ["Port-free verification debt finished."],
          repr(rendered))
    check("empty heal keeps every UI mutation on the queued GUI thread",
          not probe.gui_thread_violations,
          repr(probe.gui_thread_violations))


def test_pause_resume():
    data = make_data("run-pause", [
        ("PAUSE", [
            ("1m-iv", False, True),
            ("1d-hvol", False, True),
        ]),
    ])
    probe = new_probe(data)
    first_credit = probe.events.on(
        "first-credit",
        lambda event: event.get("kind") == "durable_credit",
        snapshot=lambda event: {
            "ticker": event.get("ticker"),
            "stage": event.get("stage"),
            "intervals": event.get("intervals"),
            "count": event.get("count"),
        })
    release_credit = threading.Event()

    def hold_after_first_credit(_probe, count):
        if count == 1:
            if not release_credit.wait(TIMEOUT):
                raise TimeoutError("timeout releasing first durable credit")

    probe.credit_hook = hold_after_first_credit
    start_debt(probe, data)
    reached = first_credit.wait(TIMEOUT)
    first_snapshot = (probe.events.snapshot("first-credit")
                      if reached else None)
    if reached:
        probe._storage_find_pause()
    release_credit.set()
    paused = reached and wait_until(
        lambda: probe._find_pause_state == "paused",
        root=probe.root)
    calls_at_pause = list(probe.validator.series_calls)
    check("Pause requested after one durable credit settles at next boundary",
          paused
          and len(probe.credit_calls) == 1
          and [(stage, ticker, interval)
               for stage, ticker, interval, _ident in calls_at_pause]
          == [("xval", "PAUSE", "1m-iv")]
          and data["tickers"]["PAUSE"]["series"]["1m-iv"]["xval"] is True
          and data["tickers"]["PAUSE"]["series"]["1d-hvol"]["xval"] is False
          and first_snapshot == {
              "ticker": "PAUSE", "stage": "xval",
              "intervals": ("1m-iv",), "count": 1,
          },
          repr(calls_at_pause))
    check("actual paused state offers Resume and keeps Cancel enabled",
          probe._storage_find_pause_btn.options == {
              "state": FakeTk.NORMAL, "text": "Resume"}
          and probe._storage_find_cancel_btn.options["state"] == FakeTk.NORMAL)

    if paused:
        probe._storage_find_pause()
    elif (probe._storage_find_phase == "verification"
          and probe._storage_find_ev is not None):
        probe._storage_find_cancel()
    finished = wait_worker(probe)
    later_calls = [(stage, ticker, interval)
                   for stage, ticker, interval, _ident
                   in probe.validator.series_calls]
    check("Resume clears the hold and starts each remaining series exactly once",
          finished
          and later_calls == [
              ("xval", "PAUSE", "1m-iv"),
              ("xval", "PAUSE", "1d-hvol"),
          ]
          and len([text for text in messages(probe)
                   if text.startswith("Verification resumed")]) == 1,
          repr(later_calls))
    check("verification Pause/Resume bypasses fetch finalization, xval, and batch paths",
          not probe.fetch_only_calls, repr(probe.fetch_only_calls))
    check("pause/resume notifications and controls remain GUI-thread-only",
          not probe.gui_thread_violations,
          repr(probe.gui_thread_violations))


def test_cancel_from_paused():
    data = make_data("run-cancel", [
        ("CANCEL", [
            ("1m-iv", False, True),
            ("1d-hvol", False, True),
        ]),
    ])
    probe = new_probe(data)
    first_credit = probe.events.on(
        "first-credit",
        lambda event: event.get("kind") == "durable_credit"
        and event.get("count") == 1)
    release_credit = threading.Event()

    def hold_after_first_credit(_probe, count):
        if count == 1:
            if not release_credit.wait(TIMEOUT):
                raise TimeoutError("timeout releasing cancellable credit")

    probe.credit_hook = hold_after_first_credit
    start_debt(probe, data)
    reached = first_credit.wait(TIMEOUT)
    if reached:
        probe._storage_find_pause()
    release_credit.set()
    paused = reached and wait_until(
        lambda: probe._find_pause_state == "paused",
        root=probe.root)
    cancel_ev = probe._storage_find_ev
    pause_ev = probe._storage_find_pause_ev
    if paused:
        probe._storage_find_cancel()
    elif (probe._storage_find_phase == "verification"
          and probe._storage_find_ev is not None):
        probe._storage_find_cancel()
    cancelled_controls = (cancel_ev.is_set() and not pause_ev.is_set()
                          and probe._find_pause_state == "cancelling")
    finished = wait_worker(probe)
    calls = [(stage, ticker, interval)
             for stage, ticker, interval, _ident
             in probe.validator.series_calls]
    active = probe.manifest.active
    check("Cancel while actually paused wakes the worker and exits promptly",
          paused and cancelled_controls and finished)
    check("paused cancellation never starts the next series",
          calls == [("xval", "CANCEL", "1m-iv")], repr(calls))
    check("completed credit persists while remaining debt stays resumable",
          active is data
          and active["tickers"]["CANCEL"]["series"]["1m-iv"]["xval"]
          and not active["tickers"]["CANCEL"]["series"]["1d-hvol"]["xval"]
          and probe._storage_find_build_btn.options["state"]
          == FakeTk.DISABLED
          and any(text.startswith("Verification cancelled -")
                  for text in messages(probe)))
    finish_search(probe)
    check("a later successful Search cannot reopen Build over recovery debt",
          probe._storage_find_search_btn.options["state"] == FakeTk.NORMAL
          and probe._storage_find_build_btn.options["state"] == FakeTk.DISABLED
          and probe._storage_find_recovery_blocked is True)
    check("verification cancellation bypasses every fetch-only path",
          not probe.fetch_only_calls, repr(probe.fetch_only_calls))
    check("paused cancellation renders only on the GUI thread",
          not probe.gui_thread_violations,
          repr(probe.gui_thread_violations))


def test_rapid_pause_resume_pause_generation():
    data = make_data("run-pause-aba", [
        ("ABA", [
            ("1m-iv", False, True),
            ("1d-hvol", False, True),
        ]),
    ])
    probe = new_probe(data)
    first_credit = probe.events.on(
        "first-credit",
        lambda event: event.get("kind") == "durable_credit"
        and event.get("count") == 1)
    release_credit = threading.Event()

    def hold_after_first_credit(_probe, count):
        if count == 1:
            if not release_credit.wait(TIMEOUT):
                raise TimeoutError("timeout releasing ABA credit")

    probe.credit_hook = hold_after_first_credit
    start_debt(probe, data)
    reached = first_credit.wait(TIMEOUT)
    if reached:
        probe._storage_find_pause()  # generation 1
    queued_before_boundary = probe.root.pending()
    release_credit.set()
    first_announcement_queued = reached and wait_until(
        lambda: probe.root.pending() > queued_before_boundary)

    if first_announcement_queued:
        probe._storage_find_pause()  # generation 2: rapid Resume
        probe._storage_find_pause()  # generation 3: immediate new Pause
    current_announcement_queued = first_announcement_queued and wait_until(
        lambda: probe.root.pending() > queued_before_boundary + 1)
    if current_announcement_queued:
        probe.root.drain_all()

    paused_messages = [text for text in messages(probe)
                       if text.startswith("Verification paused before")]
    current_pause_settled = (
        current_announcement_queued
        and probe._storage_find_pause_generation == 3
        and probe._find_pause_state == "paused")
    check("rapid Pause/Resume/Pause queues both old and current generations",
          first_announcement_queued and current_announcement_queued)
    check("stale pause callback is suppressed and current generation renders once",
          current_pause_settled
          and len(paused_messages) == 1
          and probe._storage_find_pause_btn.options == {
              "state": FakeTk.NORMAL, "text": "Resume"},
          repr(paused_messages))
    check("rapid toggles do not leak through fetch-only control paths",
          not probe.fetch_only_calls, repr(probe.fetch_only_calls))

    if (probe._storage_find_phase == "verification"
            and probe._storage_find_ev is not None):
        probe._storage_find_cancel()
    finished = wait_worker(probe)
    calls = [(stage, ticker, interval)
             for stage, ticker, interval, _ident
             in probe.validator.series_calls]
    check("ABA probe cancels cleanly without starting the held next series",
          finished
          and calls == [("xval", "ABA", "1m-iv")]
          and probe._storage_find_pause_generation is None
          and not probe.gui_thread_violations,
          repr(calls))


def test_late_cancel_after_archive():
    data = make_data("run-archive", [
        ("DONE", [("1m-iv", False, False)]),
    ])
    probe = new_probe(data, archive_when_clear=True)
    start_debt(probe, data)
    archived = probe.manifest.archive_event.wait(TIMEOUT)
    if archived:
        # The archive is durable, but _done is still queued on FakeRoot.
        probe._storage_find_cancel()
    finished = wait_worker(probe)
    rendered = messages(probe)
    check("late Cancel race occurs after the final archive and still settles",
          archived and finished and probe.manifest.active is None
          and probe.credit_results[-1:] == [{"archived": "run-log.json"}]
          and probe._storage_find_build_btn.options["state"]
          == FakeTk.NORMAL
          and probe._storage_find_recovery_blocked is False)
    check("durable archive wins over late Cancel in the terminal report",
          rendered[-1:] == ["Port-free verification debt finished."]
          and not any(text.startswith("Verification cancelled -")
                      for text in rendered), repr(rendered))
    check("late archive/cancel race keeps all rendering on GUI thread",
          not probe.gui_thread_violations,
          repr(probe.gui_thread_violations))


def test_missing_manifest_is_not_completion():
    data = make_data("run-missing", [
        ("MISSING", [("1m-iv", False, True)]),
    ])
    probe = new_probe(data, disappear_when_clear=True)
    start_debt(probe, data)
    disappeared = probe.manifest.disappear_event.wait(TIMEOUT)
    finished = wait_worker(probe)
    rendered = messages(probe)
    check("missing manifest race settles without a false completion",
          disappeared and finished and probe.manifest.active is None
          and probe.credit_results[-1:] == [{"archived": None}])
    check("only positive archive credit can produce the completion message",
          not any(text == "Port-free verification debt finished."
                  for text in rendered)
          and any("disappeared" in text and "completion" in text
                  for text in rendered), repr(rendered))
    check("unproven disappearance blocks a new build instead of going idle-green",
          probe._storage_find_build_btn.options["state"] == FakeTk.DISABLED
          and probe._storage_find_recovery_blocked is True)


def test_completed_old_run_cannot_steal_recovery_gate():
    outcomes = []
    for mode in ("new-run", "unreadable"):
        data = make_data(f"run-old-{mode}", [
            ("OLD", [("1m-iv", False, False)]),
        ])
        probe = new_probe(data, archive_when_clear=True)
        start_debt(probe, data)
        archived = probe.manifest.archive_event.wait(TIMEOUT)
        if archived:
            if mode == "new-run":
                probe.manifest.active = make_data(
                    "run-new-owner", [("NEW", [("1m", False, False)])])
            else:
                probe.manifest.fail_reads = True
        finished = wait_worker(probe)
        rendered = messages(probe)
        outcomes.append({
            "mode": mode,
            "archived": archived,
            "finished": finished,
            "build": probe._storage_find_build_btn.options["state"],
            "positive": probe.credit_results[-1:]
                        == [{"archived": "run-log.json"}],
            "owner_note": any(
                ("different active recovery" in text
                 if mode == "new-run" else "unreadable" in text)
                for text in rendered),
            "gui_safe": not probe.gui_thread_violations,
        })
    check("positive old-run archive cannot unlock over a concurrent new recovery",
          outcomes[0] == {
              "mode": "new-run", "archived": True, "finished": True,
              "build": FakeTk.DISABLED, "positive": True,
              "owner_note": True, "gui_safe": True,
          }, repr(outcomes[0]))
    check("positive old-run archive cannot unlock over unreadable recovery state",
          outcomes[1] == {
              "mode": "unreadable", "archived": True, "finished": True,
              "build": FakeTk.DISABLED, "positive": True,
              "owner_note": True, "gui_safe": True,
          }, repr(outcomes[1]))


def test_resume_reconcile_authority():
    failures = []
    for terminal in ("missing", "unreadable", "replaced"):
        data = make_resume_data(f"resume-{terminal}")
        manifest = SecondReadManifest(data, terminal)
        probe = new_probe(data, manifest=manifest)
        dispatched = []
        probe._addstock_consume_portfree_debt = (
            lambda current, out=dispatched: out.append(("debt", current)))
        probe._storage_find_start_fill = (
            lambda selections, resume_manifest=None, out=dispatched:
            out.append(("fetch", selections, resume_manifest)))
        probe._addstock_resume_active(data)
        rendered = messages(probe)
        failures.append({
            "terminal": terminal,
            "loads": manifest.load_calls,
            "blocked": probe._storage_find_recovery_blocked,
            "build": probe._storage_find_build_btn.options["state"],
            "cannot": any(text.startswith("Cannot resume Add Stocks:")
                          for text in rendered),
            "false_complete": any(
                text.startswith("Saved verification evidence completed")
                for text in rendered),
            "dispatched": dispatched,
        })
    check("Resume reconciliation fails closed on missing/unreadable/replaced authority",
          all(item["loads"] == 2
              and item["blocked"] is True
              and item["build"] == FakeTk.DISABLED
              and item["cannot"] is True
              and item["false_complete"] is False
              and item["dispatched"] == []
              for item in failures),
          repr(failures))

    data = make_resume_data("resume-positive", earliest=False)
    manifest = FakeManifest(data, archive_when_clear=True)
    validator = FakeValidator()
    validator.earliest = {"RESUME": "2026-07-01"}
    probe = new_probe(data, manifest=manifest, validator=validator)
    dispatched = []
    probe._addstock_consume_portfree_debt = (
        lambda current: dispatched.append(("debt", current)))
    probe._storage_find_start_fill = (
        lambda selections, resume_manifest=None:
        dispatched.append(("fetch", selections, resume_manifest)))
    probe._addstock_resume_active(data)
    rendered = messages(probe)
    check("Resume completion requires positive archive credit plus no active recovery",
          manifest.load_calls == 3
          and manifest.active is None
          and probe.credit_calls[-1][:3] == ("RESUME", "earliest", ())
          and probe.credit_results[-1:] == [{"archived": "run-log.json"}]
          and probe._storage_find_recovery_blocked is False
          and probe._storage_find_build_btn.options["state"] == FakeTk.NORMAL
          and rendered[-1:] == [
              "Saved verification evidence completed the interrupted run."]
          and dispatched == [],
          repr(rendered))


def test_close_guidance():
    data = make_data("run-close", [
        ("CLOSE", [("1m-iv", False, True)]),
    ])
    probe = new_probe(data)
    probe._storage_find_building = True
    probe._storage_find_phase = "verification"
    pending_cancel = threading.Event()
    probe._storage_find_ev = pending_cancel
    probe._storage_find_close()
    rendered = messages(probe)
    check("Close refuses an in-flight verification without silently cancelling",
          probe._storage_find_win.destroy_calls == 0
          and not pending_cancel.is_set()
          and probe._storage_find_building is True)
    check("Close gives verification-specific Cancel and resumable-debt guidance",
          rendered
          and "Port-free verification is running" in rendered[-1]
          and "choose Cancel" in rendered[-1]
          and "Completed evidence stays credited" in rendered[-1]
          and "remaining debt stays resumable" in rendered[-1],
          repr(rendered[-1:] if rendered else []))

    pending_cancel.set()
    probe._storage_find_close()
    pending_rendered = messages(probe)
    check("Close reports an already-requested verification cancellation truthfully",
          probe._storage_find_win.destroy_calls == 0
          and "cancellation is still settling" in pending_rendered[-1]
          and "Close after it stops" in pending_rendered[-1]
          and "choose Cancel" not in pending_rendered[-1],
          repr(pending_rendered[-1:] if pending_rendered else []))


def main():
    print("=== Add Stocks port-free verification controls (Row 48) ===\n")
    section("[A] empty exemption + clean per-series discharge")
    test_empty_heal_short_circuits_evidence()
    test_clean_flow()
    section("[B] safe pause boundary + one-shot resume")
    test_pause_resume()
    test_rapid_pause_resume_pause_generation()
    section("[C] cancel from a settled pause")
    test_cancel_from_paused()
    section("[D] terminal archive race + close guidance")
    test_late_cancel_after_archive()
    test_missing_manifest_is_not_completion()
    test_completed_old_run_cannot_steal_recovery_gate()
    section("[E] real Resume reconciliation authority + persistent Build gate")
    test_resume_reconcile_authority()
    test_close_guidance()
    return KIT.finish()


if __name__ == "__main__":
    raise SystemExit(main())
