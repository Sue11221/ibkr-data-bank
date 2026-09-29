"""Port-free A1-3a pacing contracts, real threads and inverse guard probes."""

from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import inspect
import math
from pathlib import Path
import sys
import textwrap
import threading
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_kit
import stock_ibkr as sk

KIT = check_kit.CheckKit()
check = KIT.check


class Clock:
    def __init__(self):
        self.now = 0.0
        self.lock = threading.Lock()
        self.sleeps = []

    def time(self):
        with self.lock:
            return self.now

    def sleep(self, seconds, cancel=None):
        if cancel is not None and cancel.is_set():
            raise sk.Cancelled()
        with self.lock:
            self.sleeps.append(seconds)
            self.now += seconds


def pacer(clock, **kwargs):
    return sk.Pacer(time_fn=clock.time, sleep_fn=clock.sleep, **kwargs)


class TrackedLock:
    """Real mutex with an ownership oracle, not a replacement policy."""
    def __init__(self):
        self.lock = threading.Lock()
        self.owner = None
        self.entries = 0
        self.attempted = threading.Event()

    def acquire(self, *args, **kwargs):
        self.attempted.set()
        acquired = self.lock.acquire(*args, **kwargs)
        if acquired:
            self.owner = threading.get_ident()
            self.entries += 1
        return acquired

    def release(self):
        self.owner = None
        self.lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()

    def held_here(self):
        return self.owner == threading.get_ident()


class GuardedDeque(deque):
    def __init__(self, lock, history):
        super().__init__()
        self.lock = lock
        self.history = history

    def append(self, stamp):
        if not self.lock.held_here():
            raise AssertionError("reservation mutation without the state lock")
        self.history.append(stamp)
        super().append(stamp)

    def popleft(self):
        if not self.lock.held_here():
            raise AssertionError("expiry mutation without the state lock")
        return super().popleft()


def concurrent_reservations():
    clock = Clock()
    p = pacer(clock, max_requests=5, window_s=10, min_gap_s=0.2,
              burst_max=3, burst_window_s=2)
    lock = TrackedLock()
    p._lock = lock
    all_stamps, metered_stamps = [], []
    p._burst = GuardedDeque(lock, all_stamps)
    p._stamps = GuardedDeque(lock, metered_stamps)
    start = threading.Barrier(8)
    def worker():
        start.wait(timeout=3)
        return [p.wait_turn() for _ in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker) for _ in range(8)]
        outcomes = [future.result(timeout=10) for future in futures]
    return all_stamps, metered_stamps, outcomes, lock


KIT.section("shipped pacing floors, single-run math and elapsed evidence")
p = sk.Pacer()
check("shipped floors unchanged", (p.max_requests, p.window_s, p.min_gap_s,
      p.burst_max, p.burst_window_s) == (58, 600, 0.15, 6, 2.0))
clock = Clock()
p = pacer(clock, max_requests=5, window_s=600, min_gap_s=0, burst_max=0)
waits = [p.wait_turn() for _ in range(6)]
check("metered window admits five then delays sixth", waits[:5] == [0.0] * 5 and waits[5] > 600)
check("elapsed evidence is finite numeric seconds", all(type(w) is float and math.isfinite(w) and w >= 0 for w in waits))
clock = Clock()
p = pacer(clock, max_requests=1, min_gap_s=0.6, burst_max=0)
waits = [p.wait_turn(metered=False) for _ in range(12)]
check("nonmetered skips HMDS but retains min-gap", all(w >= 0.599999 for w in waits[1:])
      and clock.time() < 7 and not p._stamps)
clock = Clock()
p = pacer(clock, max_requests=1000, min_gap_s=0, burst_max=6, burst_window_s=2)
stamps = []
for _ in range(30):
    p.wait_turn(metered=False)
    stamps.append(clock.time())
check("burst limit holds in every inclusive two-second window",
      max(sum(t - 2 <= s <= t for s in stamps) for t in stamps) <= 6)
check("burst expiry strict equality delays next turn", stamps[6] > 2 and stamps[-1] > 8)
clock = Clock()
p = pacer(clock, max_requests=4, min_gap_s=0, burst_max=0)
p.saturate()
wait = p.wait_turn(metered=False)
check("saturation preserves approximately one-slot backoff", 150 < wait < 151 and p._forced)

KIT.section("shared reservations, expiry and minimum gaps are atomic")
stamps, metered, outcomes, lock = concurrent_reservations()
check("eight simultaneous workers reserve 64 turns, none lost", len(stamps) == len(metered) == 64)
check("every append and expiry owns the real state mutex", lock.entries >= 64)
check("same governor serializes all reservation stamps", stamps == sorted(stamps) and stamps == metered)
check("concurrent minimum gaps hold", all(b - a >= 0.2 - 1e-9 for a, b in zip(stamps, stamps[1:])))
check("concurrent bursts hold", max(sum(t - 2 <= s <= t for s in stamps) for t in stamps) <= 3)
check("concurrent rolling window holds", max(sum(t - 10 <= s <= t for s in metered) for t in metered) <= 5)
check("all workers receive elapsed wait evidence", len(outcomes) == 8 and all(
      len(waits) == 8 and all(type(w) is float and w >= 0 for w in waits) for waits in outcomes))


def saturation_during_wait():
    clock = Clock()
    p = pacer(clock, max_requests=3, min_gap_s=1, burst_max=0)
    p.wait_turn(metered=False)
    trace = []
    def sleep(seconds, cancel):
        if not trace:
            # A peer must be able to publish a violation while we sleep.
            peer = threading.Thread(target=p.saturate, daemon=True)
            peer.start()
            peer.join(timeout=1)
            trace.append(not peer.is_alive())
        clock.sleep(seconds, cancel)
    p._sleep = sleep
    waited = p.wait_turn(metered=False)
    return waited, trace, p


KIT.section("saturation during a wait is immediately re-evaluated")
waited, trace, p = saturation_during_wait()
check("sleep holds no state mutex; peer saturation completes", trace == [True])
check("already-waiting nonmetered turn sees forced window", 200 < waited < 201)
check("forced reservation enters metered history", len(p._stamps) == 3 and p._stamps[-1] > 200)
clock = Clock()
p = pacer(clock)
lock = TrackedLock()
p._lock = lock
calls = []
p._time = lambda: calls.append(lock.held_here()) or 0.0
p.saturate()
check("saturation publishes forced flag and queue under the same lock", calls == [True]
      and p._forced and len(p._stamps) == p.max_requests)

KIT.section("cancel responsiveness, observer isolation and month-boundary pause")
def cancellation_at_acquisition():
    """Set cancel at the exact gap after precheck and before lock recheck."""
    cancel = threading.Event()
    class CancellingLock(TrackedLock):
        def acquire(self, *args, **kwargs):
            acquired = super().acquire(*args, **kwargs)
            if acquired:
                cancel.set()
            return acquired
    p = pacer(Clock())
    p._lock = CancellingLock()
    done = []
    try:
        p.wait_turn(cancel)
        done.append("reserved")
    except sk.Cancelled:
        done.append("cancelled")
    return done == ["cancelled"] and not p._burst and not p._stamps

check("cancel between precheck and acquisition reserves nothing", cancellation_at_acquisition())

clock = Clock()
p = pacer(clock)
cancel = threading.Event()
cancel.set()
try:
    p.wait_turn(cancel=cancel)
except sk.Cancelled:
    check("pre-cancel raises without reserving or sleeping", not p._burst and not p._stamps and not clock.sleeps)
else:
    check("pre-cancel raises without reserving or sleeping", False)


def cancelled_wait(p, cancel, done):
    try:
        p.wait_turn(cancel=cancel)
        done.append("reserved")
    except sk.Cancelled:
        done.append("cancelled")
    except BaseException as exc:
        done.append(type(exc).__name__)


p = sk.Pacer(max_requests=1, min_gap_s=0, burst_max=0)
p.wait_turn()
sleeping = threading.Event()
notifications = []
def observer(waiting, info):
    notifications.append((waiting, info))
    if waiting:
        sleeping.set()
p._on_wait = observer
cancel = threading.Event()
done = []
thread = threading.Thread(target=cancelled_wait, args=(p, cancel, done), daemon=True)
thread.start()
entered = sleeping.wait(timeout=2)
cancel.set()
thread.join(timeout=2)
check("real default sleep cancels promptly", entered and done == ["cancelled"] and not thread.is_alive())
check("cancel leaves prior reservation only", len(p._burst) == len(p._stamps) == 1)
check("waiting observer is paired on cancellation", [v for v, _ in notifications] == [True, False])

# Contention itself must not trap a cancelled peer behind a state owner.
p = sk.Pacer()
p._lock = TrackedLock()
cancel = threading.Event()
done = []
p._lock.acquire()
p._lock.attempted.clear()
try:
    thread = threading.Thread(target=cancelled_wait, args=(p, cancel, done), daemon=True)
    thread.start()
    contending = p._lock.attempted.wait(timeout=2)
    cancel.set()
    thread.join(timeout=1)
    check("cancelled peer exits even while state mutex is held", contending and done == ["cancelled"] and not thread.is_alive())
finally:
    p._lock.release()
    thread.join(timeout=2)

clock = Clock()
p = pacer(clock, min_gap_s=0.5, burst_max=0)
p.wait_turn(metered=False)
observed = []
def callback(waiting, info):
    acquired = p._lock.acquire(blocking=False)
    if acquired:
        p._lock.release()
    observed.append((waiting, acquired))
    raise ValueError("cosmetic observer must not break pacing")
waited = p.wait_turn(metered=False, on_wait=callback)
check("callback outside mutex and failure cosmetic", observed == [(True, True), (False, True)] and waited == 0.5)
p._pause = threading.Event()
p._pause.set()
check("pause remains a month-boundary policy, not a request pause", p.wait_turn(metered=False) == 0.5)

KIT.section("process-wide default survives operations and first-use races")
clock = Clock()
real_pacer = sk.Pacer
created = []
def factory():
    # Release the GIL during construction so an unlocked singleton races.
    time.sleep(0.02)
    created.append(1)
    return real_pacer(max_requests=1, min_gap_s=0, burst_max=0,
                      time_fn=clock.time, sleep_fn=clock.sleep)
with patch.object(sk, "_DEFAULT_PACER", None), patch.object(sk, "Pacer", factory):
    start = threading.Barrier(8)
    def get_default():
        start.wait(timeout=3)
        return sk._default_pacer()
    with ThreadPoolExecutor(max_workers=8) as pool:
        defaults = list(pool.map(lambda _: get_default(), range(8)))
    check("concurrent first-use creates exactly one governor", len(created) == 1 and len({id(p) for p in defaults}) == 1)
    defaults[0].wait_turn()
    waited = sk._default_pacer().wait_turn()
    check("later operation retains prior rolling-window debt", waited > 600)

KIT.section("inverse mutations run shipped method bodies, never edit production")
source = inspect.getsource(sk.Pacer.wait_turn)
needle = "                    if cancel is not None and cancel.is_set():\n                        raise Cancelled()\n"
check("in-lock cancel recheck inverse anchored", source.count(needle) == 1)
namespace = dict(vars(sk))
exec(compile(textwrap.dedent(source.replace(needle, "")), "<pacer-no-lock-cancel>", "exec"), namespace)
with patch.object(sk.Pacer, "wait_turn", namespace["wait_turn"]):
    check("missing in-lock cancel recheck makes acquisition oracle RED", not cancellation_at_acquisition())

needle = "self._lock.acquire(timeout=0.05)"
check("reservation lock inverse anchored", source.count(needle) == 1)
mutant = source.replace(needle, "True").replace("self._lock.release()", "pass")
namespace = dict(vars(sk))
exec(compile(textwrap.dedent(mutant), "<pacer-unlocked>", "exec"), namespace)
with patch.object(sk.Pacer, "wait_turn", namespace["wait_turn"]):
    try:
        concurrent_reservations()
    except AssertionError as exc:
        check("unlocked reservation makes custody oracle RED", "without the state lock" in str(exc))
    else:
        check("unlocked reservation makes custody oracle RED", False)

needle = "effective_metered = bool(metered or self._forced)"
check("forced-metering inverse anchored", source.count(needle) == 1)
namespace = dict(vars(sk))
exec(compile(textwrap.dedent(source.replace(needle, "effective_metered = bool(metered)")),
             "<pacer-stale-forced>", "exec"), namespace)
with patch.object(sk.Pacer, "wait_turn", namespace["wait_turn"]):
    waited, _, _ = saturation_during_wait()
    check("ignoring concurrent saturation makes window oracle RED", waited < 200)

source = inspect.getsource(sk.Pacer.saturate)
check("saturation lock inverse anchored", source.count("with self._lock:") == 1)
namespace = dict(vars(sk))
exec(compile(textwrap.dedent(source.replace("with self._lock:", "if True:")),
             "<saturate-unlocked>", "exec"), namespace)
p = sk.Pacer()
p._lock = TrackedLock()
calls = []
p._time = lambda: calls.append(p._lock.held_here()) or 0.0
namespace["saturate"](p)
check("unlocked saturation makes custody oracle RED", calls == [False])

source = inspect.getsource(sk._default_pacer)
check("singleton lock inverse anchored", source.count("with _DEFAULT_PACER_LOCK:") == 1)
namespace = dict(vars(sk), _DEFAULT_PACER=None, Pacer=factory)
exec(compile(source.replace("with _DEFAULT_PACER_LOCK:", "if True:"),
             "<singleton-unlocked>", "exec"), namespace)
start = threading.Barrier(8)
def unlocked_default(_):
    start.wait(timeout=3)
    return namespace["_default_pacer"]()
with ThreadPoolExecutor(max_workers=8) as pool:
    defaults = list(pool.map(unlocked_default, range(8)))
check("unlocked singleton makes first-use identity oracle RED", len({id(p) for p in defaults}) > 1)

raise SystemExit(KIT.finish())
