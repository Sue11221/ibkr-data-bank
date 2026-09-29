"""Row 81 acceptance harness — provider-backoff pacing label (F-CLAUDE-6-2).

Claude-owned reference gate, authored BEFORE the engine change (harness-first
rule).  Offline and deterministic: no network, no ports, no GUI, no tkinter,
no bank access.  It drives the REAL production seams:

  * the real ``_fetch_request`` loop, with a fake adapter that raises the real
    ``PacingViolation``, a real ``Pacer`` with injected clock/sleep, and
    ``_interruptible_sleep`` stubbed so the 60 s backoff costs no wall clock;
  * the real ``_pacing_status_msg`` / ``_parse_pacing_status_msg`` pair and the
    real ``_PACING_STATUS_RE``;
  * the SHIPPED ``_batch_visible_detail`` composer, AST-lifted out of
    ``display_data.py`` so the gate cannot drift from the code the user sees
    and tkinter is never imported.

THE DEFECT (F-CLAUDE-6-2).  The pacing event a user actually experiences — the
provider's code-162 limit, 60 s each and up to 12 back-offs — is emitted by
``say()`` to the scrolling log only.  That branch calls ``pacer.saturate()``
and ``_interruptible_sleep`` directly and never ``pacer.wait_turn``, so no
``PACING_STATUS`` is produced and the progress label keeps showing a stale
counter while the run is stalled.  Row 81 routes that branch through the same
structured pair the internal pacer already emits.

EXIT CONTRACT (``startup_port_selection_reference`` precedent):
    0  every check passed — the Row 81 feature is present and correct
    3  BASELINE PINNED: the feature is absent, and every invariant that must
       survive the change still holds (this is the expected result until Codex
       implements Row 81; it is NOT a failure)
    1  a baseline invariant BROKE — a real regression, investigate

Run:  python engine/provider_backoff_label_reference.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
ENGINE = HERE.parent
ROOT = ENGINE.parent
if str(ENGINE) not in sys.path:
    sys.path.insert(0, str(ENGINE))

import stock_ibkr as si  # noqa: E402

NEW_REASON = "provider-backoff"
LEGACY_REASONS = ("min-gap", "burst", "window")
SEPARATOR = "  |  "

_PASS: list[str] = []
_BASE_FAIL: list[str] = []      # invariants that must hold today  -> exit 1
_FEATURE_FAIL: list[str] = []   # Row 81 behaviour not yet present -> exit 3


def check(cond, name, *, feature=False):
    if cond:
        _PASS.append(name)
    elif feature:
        _FEATURE_FAIL.append(name)
    else:
        _BASE_FAIL.append(name)


# --------------------------------------------------------------------------
# the shipped composer, lifted by AST so tkinter is never imported
# --------------------------------------------------------------------------
def load_visible_detail():
    src = (ROOT / "display_data.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if (isinstance(node, ast.FunctionDef)
                and node.name == "_batch_visible_detail"):
            node.decorator_list = []          # drop @staticmethod
            mod = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(mod)
            ns: dict = {}
            exec(compile(mod, "<display_data:_batch_visible_detail>", "exec"), ns)
            return ns["_batch_visible_detail"]
    return None


# --------------------------------------------------------------------------
# fakes for the real _fetch_request loop
# --------------------------------------------------------------------------
class FakeAdapter:
    """Raises the real PacingViolation `violations` times, then returns bars."""

    def __init__(self, violations):
        self.violations = violations
        self.calls = 0
        self.use_rth = True
        self.port = 4002

    def fetch(self, *_args, **_kwargs):
        self.calls += 1
        if self.calls <= self.violations:
            raise si.PacingViolation(
                "Historical Market Data Service error message:"
                "Historical data request pacing violation")
        return [("bar",)]


class Clock:
    """Virtual clock: sleeping ADVANCES it, so a real Pacer's wait loop
    terminates instantly instead of spinning against a frozen time source."""

    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, seconds, _cancel=None):
        self.t += max(0.0, float(seconds))


class Recorder:
    """Captures say() output and splits structured events from prose."""

    def __init__(self):
        self.all: list[str] = []

    def say(self, msg):
        self.all.append(str(msg))

    @property
    def status(self):
        return [m for m in self.all if m.startswith("PACING_STATUS")]

    @property
    def prose(self):
        return [m for m in self.all if not m.startswith("PACING_STATUS")]

    def parsed(self):
        return [si._parse_pacing_status_msg(m) for m in self.status]


def drive_fetch(violations, *, metered=True):
    """Run the REAL _fetch_request against the fakes; never sleeps for real."""
    rec = Recorder()
    sleeps: list[float] = []
    saturated: list[int] = []
    real_sleep = si._interruptible_sleep
    clock = Clock()
    pacer = si.Pacer(time_fn=clock, sleep_fn=clock.sleep)
    real_saturate = pacer.saturate

    def spy_saturate():
        saturated.append(1)
        return real_saturate()

    pacer.saturate = spy_saturate
    def fake_backoff(secs, cancel):
        sleeps.append(secs)
        clock.sleep(secs)          # the provider backoff really does pass time

    si._interruptible_sleep = fake_backoff
    try:
        res = {"requests": 0}
        outcome = None
        try:
            si._fetch_request(
                FakeAdapter(violations), object(), "20260724 09:30:00",
                "1800 S", "1 secs", pacer, None, rec.say, lambda: None,
                "NFLX", res, "2026-07-24", metered=metered)
            outcome = "returned"
        except si.SeriesHalt as exc:
            outcome = f"halt: {exc}"
        return rec, sleeps, saturated, res, outcome
    finally:
        si._interruptible_sleep = real_sleep


def main():
    # ---- A: the structured message/regex contract -------------------------
    built = si._pacing_status_msg(True, {"seconds": 60.0, "reason": NEW_REASON})
    parsed = si._parse_pacing_status_msg(built)
    check(parsed is not None and parsed.get("reason") == NEW_REASON
          and parsed.get("waiting") is True and parsed.get("seconds") == 60.0
          and parsed.get("label"),
          f"A1 PACING_STATUS round-trips the new '{NEW_REASON}' reason",
          feature=True)

    for reason in LEGACY_REASONS:
        msg = si._pacing_status_msg(True, {"seconds": 1.5, "reason": reason})
        got = si._parse_pacing_status_msg(msg)
        check(got is not None and got["reason"] == reason
              and got["waiting"] is True and got["label"],
              f"A2 legacy reason '{reason}' still parses unchanged")

    ready = si._parse_pacing_status_msg(si._pacing_status_msg(False))
    check(ready is not None and ready["waiting"] is False
          and ready["label"] == "",
          "A3 the paired ready event still clears the label")
    check(si._parse_pacing_status_msg("NFLX: IBKR pacing limit reached") is None,
          "A4 an ordinary log line is never mistaken for a status event")

    # ---- B: the shipped composer ------------------------------------------
    compose = load_visible_detail()
    check(compose is not None,
          "B0 _batch_visible_detail lifted from the shipped display_data.py")
    if compose is not None:
        counter = "NFLX 1s - 5/6 days | 117,000 bars"
        backoff = (parsed or {}).get("label") or \
            si._pacing_wait_label(60.0)
        joined = compose(counter, backoff)
        check(joined == f"{counter}{SEPARATOR}{backoff}",
              "B1 counter and backoff text join with exactly two-space pipe "
              "two-space")
        check(compose(counter, "") == counter
              and compose(counter, None) == counter,
              "B2 a cleared pacing detail leaves the counter alone, no "
              "dangling separator")
        check(compose("", backoff) == backoff,
              "B3 a pacing wait before any counter shows the pacing text alone")

    # ---- C: the real 162 branch -------------------------------------------
    rec, sleeps, saturated, res, outcome = drive_fetch(2)
    prose_hits = [m for m in rec.prose if "pacing limit reached" in m]
    check(outcome == "returned" and res["requests"] == 3,
          "C0 the branch retries the same request and eventually succeeds")
    check(len(prose_hits) == 2
          and "waiting 60s" in prose_hits[0]
          and "back-off 1/12" in prose_hits[0],
          "C1 the scrolling-log line is unchanged (text, 60s, back-off n/12)")
    check(len(sleeps) == 2 and all(s == si.PACE_VIOLATION_BACKOFF_S
                                   for s in sleeps),
          "C2 each violation still sleeps the full provider backoff")
    check(len(saturated) == 2,
          "C3 pacer.saturate() is still called on every violation")

    backoff_waits = [p for p in rec.parsed()
                     if p and p["waiting"] and p["reason"] == NEW_REASON]
    readys = [p for p in rec.parsed() if p and not p["waiting"]]
    check(len(backoff_waits) == 2,
          f"C4 each provider backoff emits PACING_STATUS waiting "
          f"({NEW_REASON})", feature=True)
    check(len(backoff_waits) == 2 and len(readys) >= 2,
          "C5 every backoff waiting event is paired with a ready event",
          feature=True)
    check(all(p["seconds"] == si.PACE_VIOLATION_BACKOFF_S
              for p in backoff_waits) if backoff_waits else False,
          "C6 the emitted wait carries the true 60 s backoff, not a guess",
          feature=True)

    # unpaired-event safety: whatever is emitted must never end on a waiting
    seq = [p for p in rec.parsed() if p]
    check(not seq or not seq[-1]["waiting"],
          "C7 the event stream never ends on an unpaired waiting event")

    # ---- D: the terminal halt path emits nothing dangling ------------------
    hrec, _hs, _hsat, _hres, houtcome = drive_fetch(
        si.PACE_VIOLATION_MAX_WAITS + 1)
    hseq = [p for p in hrec.parsed() if p]
    check(str(houtcome).startswith("halt:")
          and "did not clear" in str(houtcome),
          "D1 exceeding the backoff budget still raises SeriesHalt")
    check(not hseq or not hseq[-1]["waiting"],
          "D2 the terminal halt leaves no dangling waiting event on the label")

    # ---- D3: a CANCEL mid-backoff must not strand the waiting label --------
    # Pacer.wait_turn pairs its events in a finally; the 162 branch must obey
    # the same contract when _interruptible_sleep raises Cancelled
    # (F-CLAUDE-81-1, found in review: the first cut emitted ready only on the
    # success path).
    crec = Recorder()
    cclock = Clock()
    cpacer = si.Pacer(time_fn=cclock, sleep_fn=cclock.sleep)
    real_sleep = si._interruptible_sleep

    def cancelling_backoff(_secs, _cancel):
        raise si.Cancelled()

    si._interruptible_sleep = cancelling_backoff
    try:
        coutcome = None
        try:
            si._fetch_request(
                FakeAdapter(1), object(), "20260724 09:30:00", "1800 S",
                "1 secs", cpacer, None, crec.say, lambda: None,
                "NFLX", {"requests": 0}, "2026-07-24", metered=True)
            coutcome = "returned"
        except si.Cancelled:
            coutcome = "cancelled"
        except si.SeriesHalt as exc:
            coutcome = f"halt: {exc}"
    finally:
        si._interruptible_sleep = real_sleep
    cseq = [p for p in crec.parsed() if p]
    check(coutcome == "cancelled",
          "D3a a cancel during the provider backoff still propagates Cancelled")
    check(bool(cseq) and not cseq[-1]["waiting"],
          "D3b the cancelled backoff still emits its paired ready event "
          "(no stranded 'pacing: waiting…' after Cancel)", feature=True)

    # ---- E: the internal pacer path is untouched --------------------------
    erec = Recorder()
    eclock = Clock()
    epacer = si.Pacer(max_requests=2, window_s=600.0,
                      time_fn=eclock, sleep_fn=eclock.sleep)
    for _ in range(3):
        si._wait_turn_with_status(epacer, None, metered=True, say=erec.say)
    ereasons = {p["reason"] for p in erec.parsed() if p and p["waiting"]}
    check(ereasons <= set(LEGACY_REASONS),
          f"E1 the internal pacer still emits only its own reasons "
          f"({sorted(ereasons)})")
    check(NEW_REASON not in ereasons,
          "E2 the internal pacer never borrows the provider-backoff reason")

    # ---- report -----------------------------------------------------------
    total = len(_PASS) + len(_BASE_FAIL) + len(_FEATURE_FAIL)
    for name in _BASE_FAIL:
        print("  BASELINE FAIL:", name)
    for name in _FEATURE_FAIL:
        print("  feature absent:", name)
    print(f"provider_backoff_label_reference: {len(_PASS)}/{total} passed, "
          f"{len(_BASE_FAIL)} baseline failures, "
          f"{len(_FEATURE_FAIL)} feature-absent")
    if _BASE_FAIL:
        print("EXIT 1 — a baseline invariant broke; this is a regression.")
        return 1
    if _FEATURE_FAIL:
        print("EXIT 3 — BASELINE PINNED: Row 81 is not implemented yet. "
              "Every invariant that must survive the change holds.")
        return 3
    print("EXIT 0 — Row 81 present and correct.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
