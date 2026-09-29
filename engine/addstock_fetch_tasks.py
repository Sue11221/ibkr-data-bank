"""Fixed Add Stocks request phases; the coordinator remains an observer.

No callback, coordinator identity or stored adapter attribute grants authority.
Only an explicit operation child enters this dispatch, and each request phase
transfers a fresh narrowed child to its shipped implementation. Fleet/GUI wiring
must supply that child; this module never starts a root or opens a connection.
"""

from datetime import date
from pathlib import Path
import json
import threading
import uuid

import fetch_ibkr_bridge as fib
import fetch_operations as fops
from fetch_authority import AuthorityError
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused
import live_spot_probe
import vol_value_reconcile


def terminal_error(error):
    return (isinstance(error, (AuthorityError, LedgerError, RequestCancelled))
            or not isinstance(error, Exception))


def error_detail(error):
    def describe():
        try:
            return f"{type(error).__name__}: {error}"[:500]
        except BaseException:
            return "terminal request failure; diagnostic unavailable"
    return fib.without_authority(describe)()


class _RunState:
    """Engine-owned stop coordination, with no request rights or callbacks."""
    def __init__(self):
        self.stop = threading.Event()
        self._lock = threading.Lock()
        self.error = None
        self._threads = []
        self._stoppers = []
        self._reports = []

    def fail(self, error):
        with self._lock:
            if self.error is None:
                self.error = error
            self.stop.set()
            for event in self._stoppers:
                event.set()
            return self.error

    def add_stopper(self, event):
        with self._lock:
            self._stoppers.append(event)
            if self.stop.is_set():
                event.set()

    def keep_report(self, report):
        with self._lock:
            self._reports.append(report)

    def reports(self):
        with self._lock:
            return list(self._reports)

    def start(self, thread):
        # Only engine-created threads enter here. Register before start: even
        # a start implementation that launches and then raises remains owned.
        run = thread.run
        def guarded_run():
            try:
                self.check()
                run()
            except BaseException as exc:
                self.fail(exc)
        thread.run = guarded_run
        try:
            with self._lock:
                if self.error is not None:
                    raise self.error
                self._threads.append(thread)
                thread.start()
        except BaseException as exc:
            raise self.fail(exc)

    def join(self):
        # Stop controllers before draining, including when normal fleet cleanup
        # was skipped. A controller can adopt a worker while its join is pending.
        joined = set()
        while True:
            with self._lock:
                for event in self._stoppers:
                    event.set()
                pending = [t for t in self._threads if t not in joined]
            if not pending:
                return
            for thread in pending:
                try:
                    if thread.ident is not None:
                        thread.join()
                except BaseException as exc:
                    # A cleanup interruption must not skip root finalization
                    # or replace an earlier request failure. A still-live
                    # worker remains owned and is retried on the next pass.
                    self.fail(exc)
                    if thread.is_alive():
                        continue
                joined.add(thread)

    def check(self):
        with self._lock:
            error = self.error
        if error is not None:
            raise error


class _StopCancel:
    """A terminal stop never sets the user's pause/cancel event."""
    def __init__(self, original, state):
        self.original, self.state = original, state
        self.local = threading.Event()

    def is_set(self):
        return (self.state.stop.is_set() or self.local.is_set()
                or (self.original is not None and self.original.is_set()))

    def set(self):
        self.local.set()
        if self.original is not None:
            setter = getattr(self.original, "set", None)
            if setter is not None:
                setter()


def checked_state(state, completion):
    if state is not None and type(state) is not _RunState:
        raise RequestRefused("invalid engine Add Stocks stop state")
    if completion is not None and (type(completion) is not dict or completion):
        raise RequestRefused("fetch completion receipt must be a plain empty object")
    if state is None and completion is not None:
        raise RequestRefused("fetch completion receipt requires engine state")
    if state is not None and completion is None:
        raise RequestRefused("engine state requires fetch completion receipt")
    if state is not None:
        state.check()
    return state


def _task_data(root, task):
    """Detach all caller-selected data while inside the observer boundary."""
    if not isinstance(task, (list, tuple)) or len(task) not in (2, 3):
        raise RequestRefused("invalid Add Stocks task shape")
    phase, ticker = task[:2]
    if type(phase) is not str or phase not in {"check", "reconcile-plan", "probe", "reconcile"}:
        raise RequestRefused("invalid Add Stocks task phase")
    if type(ticker) is not str or not ticker or ticker != ticker.strip().upper():
        raise RequestRefused("invalid Add Stocks task ticker")
    if phase in {"check", "reconcile-plan"}:
        if len(task) != 2:
            raise RequestRefused("observer task does not accept a request payload")
        return Path(root).resolve(), (phase, ticker)
    if len(task) != 3 or not isinstance(task[2], dict):
        raise RequestRefused("request task requires an object payload")
    if phase == "probe":
        day = task[2].get("day")
        try:
            valid_day = type(day) is str and date.fromisoformat(day).isoformat() == day
        except ValueError:
            valid_day = False
        if not valid_day:
            raise RequestRefused("Add Stocks probe requires an ISO day")
        payload = {"day": day}
    else:
        payload = live_spot_probe._normalize_addstock_plan_row(ticker, task[2])
    # Normalizers accept some subclasses for legacy compatibility. No such
    # caller object may survive into an authority-bearing request argument.
    payload = json.loads(json.dumps(payload, allow_nan=False))
    return Path(root).resolve(), (phase, ticker, payload)


@fib.worker_scope
def _run_addstock_task(root, post_pipeline, task, adapter, pacer=None,
                       cancel=None, progress=None, on_request_error=None):
    """Call fixed request functions, never an injected privileged executor."""
    root, task = fib.without_authority(_task_data)(root, task)
    phase, ticker = task[:2]
    if phase in {"check", "reconcile-plan"}:
        # The facade isolates both descriptor lookup and callback execution.
        return post_pipeline.execute(task, adapter, pacer)
    error = None
    outcome = None
    try:
        options = fib.without_authority(lambda: dict(post_pipeline.request_options(task)))()
        with fib.request_error_observer(on_request_error):
            if phase == "probe":
                outcome = live_spot_probe.run_embedded_probe(
                    root, ticker=ticker, day=task[2]["day"], interval="1m",
                    adapter=adapter, pacer=pacer, cancel=cancel,
                    _fetch_child=fops.narrow_worker_child(fib.current_worker(),
                        "addstock-probe-" + uuid.uuid4().hex,
                        rights={"ibkr.live_spot_probe.minute"}))
            else:
                run_id = fib.without_authority(lambda: str(options["run_id"]))()
                outcome = vol_value_reconcile.reconcile_one(
                    adapter, pacer, root, task[2], run_id, cancel=cancel,
                    manifest_lock=options.get("manifest_lock"), progress=progress,
                    _fetch_child=fops.narrow_worker_child(fib.current_worker(),
                        "addstock-reconcile-" + uuid.uuid4().hex,
                        rights={"ibkr.vol_value_bank.ratio_day"}))
    except BaseException as exc:
        error = exc
    terminal = error is not None and (
        isinstance(error, (AuthorityError, LedgerError, RequestCancelled))
        or not isinstance(error, Exception))
    try:
        post_pipeline.complete_request(task, outcome, error=error)
    except BaseException:
        if terminal:
            # An observer cannot replace or suppress the original stop signal.
            raise error
        raise
    if terminal:
        raise error
