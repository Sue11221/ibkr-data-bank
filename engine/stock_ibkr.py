"""Stock Data Storage — Tier 2: IBKR gap-fill (GUI-free).

Extends existing (ticker, interval) series forward by fetching the
missing sessions from a locally running TWS / IB Gateway through
ib_async, then committing them through the SAME strict merge machinery
Tier 1 uses. Nothing here weakens the tree's guarantees:

  * conId is PINNED in the manifest on first contact and asserted every
    session — a symbol that silently changed meaning halts the series.
  * The OVERLAP GATE runs before the first commit of every series: the
    first fetched session is the LAST STORED session (refetched on
    purpose), so IBKR's prices are compared bar-for-bar against the
    archive. >10% price disagreement (beyond 0.5% tolerance) on >=30
    overlapping bars = different basis/vendor -> series halted, nothing
    written (volume disagreement is EXPECTED — IBKR volume is
    NBBO-filtered — and never gates).
  * The JOIN GATE runs at every session boundary of the fetched stream:
    a close->open ratio outside [0.55, 1.9] looks like a split or other
    corporate action -> the series stops BEFORE the cliff; everything
    already committed is contiguous and clean. Classifying a boundary
    is the basis doctor's job (stock_basis), never an auto-fix.
  * RECORDED ACTIONS (manifest "actions", stock_basis M1) are consulted
    by both gates: a boundary the user classified and recorded is
    CROSSED instead of halted on — whether the feed serves raw bars (a
    real cliff, explained by the factor) or adjusted history (overlap
    shifted by the factor, smooth joins). Unrecorded boundaries still
    halt; stored bytes are never rewritten — recorded factors are
    consumed at read time (M3).
  * Volume units are CALIBRATED on the refetched overlap session: if
    vendor volume / IBKR volume lands near 100x, IBKR is reporting lots
    -> multiplied to shares with a loud note; near 1x -> shares. No
    overlap -> "unverified" note, no coercion.
  * Identical duplicate bars drop silently; same-timestamp conflicts
    keep the EXISTING archive value and are logged (the vendor archive
    is golden; IBKR only extends it).

Pacing: a sliding-window governor keeps the session under IBKR's
historical-data limit (58 requests / 10 min, safety margin under 60)
with a minimum spacing between requests; cancel is honored mid-wait.
Disconnects are retried with reconnect+resume at request granularity.

The module is importable and fully testable WITHOUT ib_async or a
running TWS: every network touch goes through an adapter object; tests
inject a fake. The live adapter (made by `live_adapter_factory`)
imports ib_async lazily and works on Python 3.14's plain sync API
(proven by milestone 0 on this machine: TWS paper port 7497).
"""

import os
import json
import math
import queue
import random
import re
import shutil
import socket
import statistics
import threading
import time as _time
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import wraps
from pathlib import Path
import uuid
import inspect

import stock_storage as ss
import stock_ingest as ingest
import stock_basis as sb
import market_calendar as mc
import split_join_enrichment as split_enrichment
import addstock_watchdog
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
from fetch_run_context import FetchRunContext, RequestRefused, RequestCancelled
from fetch_ledger import LedgerError, inspect_ledger
from fetch_authority import AuthorityError, CalendarUnsupported

HOST_DEFAULT = "127.0.0.1"
PORTS_DEFAULT = (7497, 4002, 7496, 4001)   # TWS paper first (this machine)
CLIENT_ID_FETCH = 7311
CLIENT_ID_DOCTOR = 7312
CLIENT_ID_SEARCH = 7313        # the Add-stock dialog's persistent name-search
CLIENT_ID_SEAL = 7314          # pause-finalize gap-seal — distinct from FETCH(7311,
#                                held by paused workers), DOCTOR(7312) and SEARCH(7313)
#                                connection — a SEPARATE id so it can't clash
#                                with a fetch worker's CLIENT_ID_FETCH on the
#                                same port (that clash aborted port 2000).
CLIENT_ID_SPOT_PROBE = 7315    # WS8 report-only one-day minute probes
EMPTY_MONTH_ABSENCE_PROOF = True
EMPTY_MONTH_CONTROL_PROBE_CAP = 2
# When a connection drops, its socket can sit HALF-OPEN on the TWS side for a
# while; a same-id reconnect to that port is then rejected with error 326
# ("client id already in use"), so a single port (e.g. 3000) flaps online↔dead
# until TWS finally reaps the zombie. LiveIB.connect() defeats this by ROTATING
# the client id on a 326 — base, base+10, base+20, … — so a stale/in-use id on
# one TWS instance never blocks that port's reconnect. Stride 10 keeps each
# family's residue (FETCH≡1, DOCTOR≡2, SEARCH≡3, SEAL≡4 mod 10) disjoint, so a
# rotated id can never collide with another operation's id.
CLIENT_ID_STRIDE = 10
CLIENT_ID_RETRIES = 8          # ids to try past the base on a 326 (base..base+80)
# A contended id never completes the handshake, so ib_async waits the FULL
# connect timeout before failing — give the base id a generous 8 s (a slow/busy
# TWS is legit) but probe the rotation ids briefly so a held id fails fast
# (worst case ≈ CONNECT_TIMEOUT_S + RETRIES*ROTATE_CONNECT_TIMEOUT_S per port).
ROTATE_CONNECT_TIMEOUT_S = 3
# ib_async's connect() BLOCKS for the whole timeout waiting for an `apiStart`
# handshake the IBKR demo never sends — yet the socket is fully usable the entire
# time (qualify/head/historical all work). So the connect time == this timeout.
# We pass fetchFields=NONE (skip the positions/orders/account-update startup pull
# the engine never needs — those also stall under "Read-Only API", error 321),
# and use a SHORT base timeout: a healthy TWS returns sub-second; the demo
# returns a working connection here instead of burning 8 s. Measured: a usable
# connection comes back in ~2 s. 4 s leaves margin without the old 8 s stall.
CONNECT_TIMEOUT_S = 4
# Hard per-request timeouts so a frozen / unresponsive TWS can NEVER hang an
# unattended run forever. reqHistoricalData already passes timeout=60 per call;
# the contract-lookup and head-timestamp requests had NO timeout (a single
# unanswered reqContractDetails would block the whole run indefinitely).
QUALIFY_TIMEOUT_S = 20.0
HEAD_TIMEOUT_S = 20.0
HEAD_PROBE_DURATION = "30 Y"   # one bounded daily request ending today finds
#                                actual served reach and reconciles unreliable
#                                head metadata (false-deep ODFL, false-late PLTR).
#                                It also avoids pre-listing window hangs for a
#                                recent IPO when reqHeadTimeStamp fails.
HEAD_PROBE_FLOOR_TOLERANCE_DAYS = 14
BACKFILL_SEAL_TOLERANCE_DAYS = 62
# A listing's first intraday bar can trail the first served daily bar by days or
# weeks. Treat starts within roughly two calendar months as the same frontier;
# anything deeper remains visibly incomplete instead of being sealed short.
FETCH_TIMEOUT_S = 60.0    # a frozen TWS must raise (retryable), not silently
                         # return [] that looks like a holiday

# --- connection diagnostics -------------------------------------------------
# Always-on (once a run points it at a file), thread-safe, append-only TIMELINE
# of the signals that separate a healthy run from a stranded port: connect
# timings + client-id, every NON-benign server error (321/420/326/110x…),
# pacing back-offs and reconnects, and the halt that finally strands a series.
# A run calls set_diag_log(<root>) at start; every line is timestamped so the
# latest run is easy to find. Best-effort: a logging failure NEVER touches a run.
_DIAG_LOCK = threading.Lock()
_DIAG_PATH = [None]
_DIAG_LAST_BANNER = [0.0]
# pure status noise (data-farm up/down "OK" notices) — never logged as an error
_DIAG_SKIP_CODES = frozenset({2100, 2103, 2104, 2105, 2106, 2107, 2108,
                              2119, 2150, 2157, 2158})


def set_diag_log(path):
    """Point the connection-diagnostics log at `path` (a run calls this with its
    output root). Idempotent + safe to call from every worker: a START banner is
    written at most once per run (3 s throttle). None/'' disables. Never raises."""
    _DIAG_PATH[0] = str(path) if path else None
    if not path:
        return
    now = _time.monotonic()
    if now - _DIAG_LAST_BANNER[0] < 3.0:      # workers re-calling within one run
        return
    _DIAG_LAST_BANNER[0] = now
    try:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with _DIAG_LOCK:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(f"\n===== RUN START {stamp}  pid={os.getpid()}  "
                         f"connect_timeout={CONNECT_TIMEOUT_S}s skip_fetch=on "
                         f"=====\n")
    except Exception:  # noqa: BLE001
        pass


def _connection_diag_path(root):
    """Return the project-level diagnostics path for a data-bank root."""
    return Path(root).parent / "Run Logs" / "connection_diagnostics.log"


def _diag(event, **fields):
    """Append one timestamped diagnostic line. Thread-safe; never raises."""
    p = _DIAG_PATH[0]
    if not p:
        return
    try:
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        rest = " ".join(f"{k}={v}" for k, v in fields.items()
                        if v is not None and v != "")
        with _DIAG_LOCK:
            with open(p, "a", encoding="utf-8") as fh:
                fh.write(f"{ts} {event:<12} {rest}\n")
    except Exception:  # noqa: BLE001 — diagnostics must never break a run
        pass


def _diag_codes(errors):
    """Compact 'code×n' summary of an error list for a CONNECT line."""
    seen = {}
    for code, _m in (errors or []):
        if code not in _DIAG_SKIP_CODES:
            seen[code] = seen.get(code, 0) + 1
    return ",".join(f"{c}x{n}" for c, n in seen.items()) or "clean"


def _adapter_port(adapter):
    """Best-effort port of a fetch adapter, read from CACHED state only — it runs
    inside error/diagnostic paths, so it must NEVER trigger ReusableAdapter._live()
    (which would RECONNECT a dead link) and must never raise."""
    try:
        cached = getattr(adapter, "_a", None)     # ReusableAdapter's live conn,
        target = cached if cached is not None else adapter   # or a bare LiveIB
        return target.__dict__.get("port")        # plain read — no _live(), no I/O
    except Exception:  # noqa: BLE001
        return None


PACE_MAX_REQUESTS = 58                     # under IBKR's 60 / 10 min
PACE_WINDOW_S = 600.0
PACE_MIN_GAP_S = 0.15                      # small anti-hammer floor; the real
                                          # same-contract throttle is the burst
                                          # window below. Was 0.6 — measured
                                          # 2026-06-23 to dominate at the fast
                                          # "1 W" 1m span (~302 of 406 s/ticker
                                          # was pure gap); the burst window
                                          # captures IBKR's ACTUAL rule with no
                                          # arbitrary slack.
PACE_BURST_MAX = 6                         # IBKR: no more than 6 historical
PACE_BURST_WINDOW_S = 2.0                  # requests for the same Contract/
                                          # Exchange/TickType within 2 seconds.
                                          # Sliding-window enforced -> allows a
                                          # 6-request burst then throttles to a
                                          # sustained ~3 req/s (vs the old fixed
                                          # 0.6s gap = ~1.7 req/s).
RECONNECT_ATTEMPTS = 20            # ride out TWS's daily auto-restart (down
RECONNECT_BACKOFF_S = 5.0          # ~2-5 min): 20 tries x up-to-30s backoff
RECONNECT_BACKOFF_CAP_S = 30.0     # ≈ 9 min before a series gives up
# A REQUEST timeout (the link is up, other requests on it work — the demo HMDS
# just won't answer THIS contract, e.g. a ticker it can't serve) must NOT trigger
# the 20-reconnect ride-out: reconnecting can't make an unservable contract
# answer, and 20 x (60 s timeout + reconnect) strands the whole port for ~20 min
# on one bad ticker. Instead retry the WARM link a couple times, then halt JUST
# that series so the port moves on (committed months stay; re-run resumes).
TIMEOUT_RETRIES = 2                # warm-link re-issues before halting a series
PACE_VIOLATION_BACKOFF_S = 60.0            # wait for the 10-min window to
PACE_VIOLATION_MAX_WAITS = 12              # free slots; ~12 min then halt
MANIFEST_CHECKPOINT_MONTHS = 12            # A3: persist the manifest every K
                                          # committed months (+ once at series
                                          # end), not after every single one
PIPELINE_QUEUE_MAX = 8                     # bounds fetched-but-unpacked sessions
PROGRESS_HEARTBEAT_MIN_S = 0.75
_POST_STATUS_POLL_S = 0.05
_POST_STATUS_INITIAL_S = 0.05
_PROGRESS_RE = re.compile(
    r"^(?P<ticker>\S+)\s+(?P<interval>\S+):\s+PROGRESS"
    r"(?:\s+(?P<phase>backfill))?\s+"
    r"(?P<done>\d+)/(?P<total>\d+)\s+days\s+"
    r"(?P<bars>\d+)\s+bars$"
)
_PACING_STATUS_RE = re.compile(
    r"^PACING_STATUS\s+(?P<state>waiting|ready)"
    r"(?:\s+(?P<seconds>\d+(?:\.\d+)?)"
    # min-gap/burst/window come from OUR pacer; provider-backoff is the
    # provider's own code-162 stall, which the pacer never schedules.
    r"\s+(?P<reason>min-gap|burst|window|provider-backoff))?$"
)


def _probe_wait_heartbeat_interval():
    """Read Row 45's shared heartbeat knob at use time.

    The import is deliberately lazy: stock_ibkr remains independently
    importable, while Fix Data and Add Stocks still honor one runtime-tunable
    interval in production and in deterministic tests.
    """
    try:
        import fix_data_pipeline as _fix_data_pipeline
        value = _fix_data_pipeline.PROBE_WAIT_HEARTBEAT_S
        return max(0.02, float(value))
    except (AttributeError, ImportError, TypeError, ValueError):
        return 2.0


def _progress_msg(ticker, interval, done, total, bars, phase=""):
    phase_part = f" {phase}" if phase else ""
    return (f"{ticker} {interval}: PROGRESS{phase_part} "
            f"{int(done)}/{int(total)} days {int(bars or 0)} bars")


def _parse_progress_msg(msg):
    m = _PROGRESS_RE.match(str(msg or ""))
    if not m:
        return None
    return {"ticker": m.group("ticker"), "interval": m.group("interval"),
            "phase": m.group("phase") or "",
            "done": int(m.group("done")),
            "total": int(m.group("total")),
            "bars": int(m.group("bars"))}


def _progress_detail(info):
    if not info:
        return ""
    prefix = "backfill " if info.get("phase") == "backfill" else ""
    return f"{prefix}{info['done']}/{info['total']} days"


def _pacing_wait_label(seconds):
    """User-facing wait derived from the pacer's exact sleep arithmetic."""
    shown = max(1, int(math.floor(max(0.0, float(seconds)) + 0.5)))
    return f"pacing: waiting for request budget (~{shown}s)"


def _pacing_status_msg(waiting, info=None):
    """Structured cosmetic progress event; never changes pacing behavior."""
    if not waiting:
        return "PACING_STATUS ready"
    info = dict(info or {})
    return (f"PACING_STATUS waiting {float(info.get('seconds') or 0.0):.6f} "
            f"{str(info.get('reason') or 'window')}")


def _parse_pacing_status_msg(msg):
    m = _PACING_STATUS_RE.match(str(msg or ""))
    if not m:
        return None
    waiting = m.group("state") == "waiting"
    seconds = float(m.group("seconds") or 0.0)
    return {
        "waiting": waiting,
        "seconds": seconds,
        "reason": m.group("reason") or "",
        "label": _pacing_wait_label(seconds) if waiting else "",
    }


def _pacing_wait_observer(say):
    """Bind one request path to its own progress sink (no global callback)."""
    if say is None:
        return None
    return lambda waiting, info=None: say(_pacing_status_msg(waiting, info))


def _wait_turn_with_status(pacer, cancel=None, metered=True, say=None):
    """Use the D4 observer on real Pacers while preserving old fake seams."""
    # Engine-owned requests reserve exactly once at BoundRequest's live choke.
    # Legacy head/planner callers must not also reserve the private Pacer here.
    if fib.current_worker() is not None:
        return 0.0
    if hasattr(pacer, "_on_wait"):
        return pacer.wait_turn(
            cancel, metered=metered, on_wait=_pacing_wait_observer(say))
    return pacer.wait_turn(cancel, metered=metered)


def _maybe_emit_progress(say, ticker, interval, done, total, bars,
                         throttle, phase="", now_fn=None, force=False):
    """Throttled monitor heartbeat; consumed by GUI/port rows, not the text log."""
    if say is None or total <= 0:
        return False
    now_fn = now_fn or _time.monotonic
    now = now_fn()
    last = throttle.get("last", float("-inf"))
    if not force and now - last < PROGRESS_HEARTBEAT_MIN_S:
        return False
    throttle["last"] = now
    say(_progress_msg(ticker, interval, done, total, bars, phase=phase))
    return True
                                          # held in memory when pipeline=True

# ---- Adaptive concurrency (opt-in) -------------------------------------
# On the IBKR DEMO all instances share ONE HMDS backend; per-account pacing
# budgets are independent, so the real bottleneck is CONCURRENT data volume =
# (# ports pulling at once). When that backend chokes it drops sockets ->
# "connection lost — reconnect" on several ports at once. AIMD congestion
# control: when drops SPIKE, PARK an active port (multiplicative-ish, fast);
# after a CLEAN stretch, un-park one (additive, slow). Converges on the demo's
# sweet spot (~2-3 heavy backfill) without a fixed guess. All time in seconds.
ADAPT_WINDOW_S = 60.0          # sliding window for the reconnect-rate mark
ADAPT_TICK_S = 12.0            # controller cadence (one reconnect cycle ≈ 5-30s)
ADAPT_DROP_RETRY_FRAC = 0.5    # >= ceil(active*frac) ports retrying NOW -> drop
ADAPT_DROP_RATE = 2            # >= this many DISTINCT ports reconnected in the
                               # window -> drop (distinct ports, NOT raw retry
                               # attempts: one stuck port flaps up to 20x per
                               # episode, which must NOT look like a choke)
ADAPT_SEVERE_RATE = 3          # >= this many distinct ports (with the frac) -> 2
ADAPT_ADD_CLEAN_S = 120.0      # clean stretch (no retry, no drops) before adding
ADAPT_COOLDOWN_S = 60.0        # settle after ANY change before the next
ADAPT_MIN_ACTIVE = 1           # floor: never park the last working port
ADAPT_SWAP_STUCK_S = 60.0      # an ACTIVE port stuck reconnecting this long,
                               # while a parked ("dropped") port is free, is
                               # swapped out for it (concurrency-neutral): the
                               # stuck instance idles & recovers, the healthy
                               # idle instance takes over real work

# ---- Confirmed-dead mid-run fleet revival -------------------------------
# The hard-death watchdog remains the authority for DEAD/HEALTHY state and
# worker re-adoption.  This controller only supplies lifecycle power after a
# long, maintenance-aware grace period. Partial deaths and an all-dead HOLD
# share the same callback/evidence path; the HOLD path gets one offer before
# the existing degraded-finalize boundary.
MIDRUN_RESTART = True
MIDRUN_FLEETDOWN_REVIVAL = True
PICKUP_FAST_START = True
PICKUP_SHARED_EVIDENCE = True
KIND_EARLIEST_PROBE = True
IDENTITY_FLOOR_CLAMP = True
PACING_WAIT_STATUS = True
PROGRESS_UNIQUE_COUNT = True
VOL_VALUE_GATE = True
VOL_HARD_CEILING = 10.0
UNIT_FLIP_RATIO = 50.0
PICKUP_PLAN_WAIT_S = 30.0
REVIVE_AFTER_S = 150.0
MIDRUN_RESTART_MAX_PER_PORT = 2
BATCH_WINDOW_S = 60.0
DECLINE_COOLDOWN_S = 900.0


def _midrun_controller_gate(snapshot, *, work_remains, paused=False,
                            cancelled=False, draining=False):
    """Pure partial-fleet policy gate shared with the offline tests."""
    return bool(
        MIDRUN_RESTART and work_remains and not paused and not cancelled
        and not draining and isinstance(snapshot, dict)
        and not snapshot.get("holding")
        and not snapshot.get("maintenance_active"))


def _midrun_fleetdown_gate(snapshot, *, work_remains, paused=False,
                           cancelled=False, draining=False, offered=False):
    """Pure one-shot HOLD revival policy; finalize is checked by the caller.

    A snapshot may already say ``finalize`` when this gate opens because the
    production revival and fleet-grace thresholds are both 150 seconds.  The
    watchdog monitor therefore reserves the offer before committing the
    existing degraded-finalize decision.
    """
    return bool(
        MIDRUN_RESTART and MIDRUN_FLEETDOWN_REVIVAL and work_remains
        and not paused and not cancelled and not draining and not offered
        and isinstance(snapshot, dict) and snapshot.get("holding")
        and not snapshot.get("maintenance_active"))


def _midrun_port_candidate(row, *, listening, worker_active, offers, now,
                           cooldown_until, require_grace=True):
    """Pure confirmed-death/cap/socket eligibility predicate."""
    if not isinstance(row, dict) or row.get("state") != "DEAD":
        return False
    if (require_grace and float(row.get("grace_seconds") or 0.0)
            < max(0.0, float(REVIVE_AFTER_S))):
        return False
    return bool(
        not listening and not worker_active
        and int(offers) < max(0, int(MIDRUN_RESTART_MAX_PER_PORT))
        and float(now) >= float(cooldown_until))

GATE_MIN_OVERLAP = 30
GATE_PRICE_TOL = 0.005                     # 0.5% per-bar tolerance
GATE_DISAGREE_FRAC = 0.10
JOIN_RATIO_LOW = 0.55                      # outside => suspected split
JOIN_RATIO_HIGH = 1.9
VOLUME_LOTS_RATIO = (60.0, 160.0)          # vendor/IBKR ~100 => lots

ONE_SECOND_MAX_AGE_DAYS = 178              # IBKR serves ~6 months of 1s

# interval token -> (IBKR barSizeSetting, seconds per request window;
# None = one whole-session request of duration "1 D")
_BAR_SIZES = {
    "1s": ("1 secs", 1800), "5s": ("5 secs", 7200),
    "10s": ("10 secs", 14400), "30s": ("30 secs", 23400),
    "1m": ("1 min", None), "2m": ("2 mins", None),
    "5m": ("5 mins", None), "15m": ("15 mins", None),
    "30m": ("30 mins", None), "1h": ("1 hour", None),
    "1d": ("1 day", None),                         # daily (IV/HVOL) — whole-session
}

# Coarse-span fetch for the whole-session intervals (win=None above):
# one request pulls MANY sessions of the SAME bar size instead of one
# "1 D" per session — the pacing budget is by request COUNT, so this is
# the only real speed lever. (duration string, max CALENDAR days the
# chunk may span — kept under the duration so the request fully covers
# it). "1 M" for 1m proven live (probe_max_span: KO 1m served 8,190
# bars at "1 M", 16,770 at "2 M", ceiling at "3 M"); ~20x vs day-by-day.
_FETCH_SPAN = {
    # 1m set to "1 W" (2026-06-23, MEASURED). The demo HMDS serves large 1m
    # responses SUPER-LINEARLY slowly — raw adapter.fetch() on AAPL:
    #   1 W (~1950 bars) 0.21s | 2 W (~3900) 5.98s | 1 M (~8190) 24s | 2 M 60s.
    # So per-1k-bar cost EXPLODES (0.11 -> 1.53 -> 2.94 -> 3.57 s/1k). Modelling
    # total 10yr time = n_req*(min_gap + fetch): "1 W" wins at ~6.8 min/ticker,
    # ~7x faster than "1 M" and ~9x faster than "2 M" (bigger spans are WORSE,
    # not better — the earlier 2 M widening was reverted after this measurement).
    # "1 W" balances the fast small-response regime against the per-request
    # min-gap. max_cal 6 (< the 7 calendar days a "1 W" request spans). Re-run
    # scratch span_knee.py if IBKR's behavior changes. Coarser intervals keep
    # "1 M" (their responses are far smaller, so the blow-up doesn't bite).
    "1m": ("1 W", 6), "2m": ("1 M", 27),
    "5m": ("1 M", 27), "15m": ("1 M", 27),
    "30m": ("1 M", 27), "1h": ("1 M", 27),
    # daily (IV/HVOL/daily-TRADES): DIRT CHEAP — LIVE-CONFIRMED 2026-06-24 the demo
    # serves a FULL 10 years (2511 bars, 2016->2026) in ONE request in 0.32s, no
    # truncation. So one "10 Y" request covers a decade; max_cal=3600 < the ~3652-day
    # window keeps every chunk's oldest day inside it. (10yr backfill: 11 reqs -> ~2.)
    "1d": ("10 Y", 3600),
    # 1m EXTENDED sessions (useRTH=False) return ~960 bars/session vs 390 RTH,
    # so the same per-request bar-count blow-up hits at a smaller span -> "2 D"
    # (~1920 bars) keeps them in the fast regime. Used by a standalone -pre/
    # -post fetch AND by the post-session prefetch chunks (ending at 20:00).
    "1m-pre": ("2 D", 1), "1m-post": ("2 D", 1),   # max_cal = duration_days-1
}                                                  # (a "2 D" req covers 2 cal
#   days, so a chunk may span at most a 1-day difference, else its oldest day
#   falls outside the request window and never gets fetched)

RTH_OPEN = time(9, 30, 0)
RTH_CLOSE = time(16, 0, 0)                 # session end label (exclusive)

# Session specs keyed by token suffix (see stock_storage.session_of): a
# "1m-pre" / "1m-post" series is fetched with useRTH=False (RTH-only would
# return NOTHING extended) and the engine then KEEPS ONLY that session's bars.
# session_seconds feeds the expected-bars / completeness checks so the shorter
# extended sessions aren't false-flagged as truncated.
#   token -> (use_rth, session_open, session_close, last_label, session_seconds)
_SESSION_SPECS = {
    "rth":  (True,  time(9, 30), time(16, 0), time(15, 59, 59), 23400),
    "pre":  (False, time(4, 0),  time(9, 30), time(9, 29, 59),  19800),
    "post": (False, time(16, 0), time(20, 0), time(19, 59, 59), 14400),
}

# whatToShow per data KIND (see stock_storage.kind_of). Default TRADES so any bare
# (kindless) token keeps fetching trades exactly as before. iv/hvol are RATIO kinds
# (OPTION_IMPLIED_VOLATILITY/HISTORICAL_VOLATILITY); bidask is a price kind.
KIND_WHATTOSHOW = {
    "": "TRADES", "iv": "OPTION_IMPLIED_VOLATILITY",
    "hvol": "HISTORICAL_VOLATILITY", "bidask": "BID_ASK",
}


def _what_to_show(interval):
    """IBKR whatToShow for an interval token's kind: '1m'->'TRADES',
    '1m-iv'->'OPTION_IMPLIED_VOLATILITY', '1d-hvol'->'HISTORICAL_VOLATILITY'."""
    return KIND_WHATTOSHOW.get(ss.kind_of(interval), "TRADES")


def _session_spec(interval):
    return _SESSION_SPECS[ss.session_of(interval)]


class SeriesHalt(Exception):
    """Stops ONE series with a user-facing reason; the run continues."""

    def __init__(self, reason, *, metadata=None):
        super().__init__(reason)
        self.metadata = dict(metadata or {})


@dataclass(frozen=True)
class _ConIdRepinIntent:
    """One narrowly authorized dead-contract identity transition.

    The intent is created only after the pinned contract is rejected before
    any data-bearing work.  ``evidence_day`` remains empty until the ordinary
    non-ratio entry gate accepts nonempty bars from ``accepted_new``; merely
    having a divergent live conId is never publication authority.
    """

    expected_old: int
    accepted_new: int
    evidence_day: str = ""

    def __post_init__(self):
        for label, value in (("expected_old", self.expected_old),
                             ("accepted_new", self.accepted_new)):
            if isinstance(value, bool) or not isinstance(value, int) \
                    or value <= 0:
                raise ValueError(f"{label} must be a positive integer conId")
        if self.expected_old == self.accepted_new:
            raise ValueError("a conId repin must change the durable identity")
        if self.evidence_day:
            try:
                date.fromisoformat(self.evidence_day)
            except (TypeError, ValueError) as exc:
                raise ValueError("evidence_day must be an ISO date") from exc

    @property
    def activated(self):
        return bool(self.evidence_day)

    def activate(self, evidence_day):
        if self.activated:
            return self
        if isinstance(evidence_day, datetime):
            evidence_day = evidence_day.date()
        if isinstance(evidence_day, date):
            evidence_day = evidence_day.isoformat()
        return _ConIdRepinIntent(
            self.expected_old, self.accepted_new, str(evidence_day))


class _ConIdRepinConflict(ss.StorageError):
    """A bounded fail-closed durable-identity CAS conflict."""


class Cancelled(RequestCancelled):
    """User cancel — committed months stay, the report says where."""


class PacingViolation(ConnectionError):
    """IBKR rejected a historical request for exceeding the 60/10-min
    pacing limit. A SUBCLASS of ConnectionError so every existing
    ConnectionError handler still degrades gracefully — but the fetch
    retry loop catches it FIRST and BACKS OFF (waits for the rolling
    window to clear) instead of reconnecting, which never helps a
    pacing limit. NOT a network drop."""


class ConnectionLost(ConnectionError):
    """A GENUINE lost link mid-request: the socket reset, or
    is_connected()==False after the call. Reconnect helps. Distinct from
    RequestTimeout so a 'connection lost' message means the link actually
    dropped — not that a busy TWS merely answered slowly."""


class RequestTimeout(ConnectionError, TimeoutError):
    """TWS did not answer within the per-call timeout — busy / frozen /
    overloaded. Retryable like a drop (a ConnectionError subclass, so every
    existing handler still catches it), but it is NOT evidence the connection
    was lost: the link is often still up and just slow. Tagging it separately
    is what lets us tell a real disconnect from backend/TWS slowness."""


def _identity_floor_for_series(manifest, ticker, interval, result):
    """Resolve one strict listing floor or halt this series observably.

    Quarantine membership alone is deliberately not a floor: it carries no
    cutover date. Recovery must restore a valid correction to the active
    manifest; this fetch path never guesses a boundary from quarantine state.
    """
    if manifest is None:
        return None
    try:
        # The containing manifest is the fetch authority. Historical notes may
        # retain an earlier display ticker across a rename/reuse campaign, so
        # do not make that advisory field override the pinned manifest.
        return ss.identity_floor(manifest, interval)
    except ss.StorageError as exc:
        message = (
            f"identity listing-floor correction is malformed for "
            f"{ticker} {interval}: {exc}; refusing backfill")
        result.setdefault("notes", []).append(message)
        raise SeriesHalt(message) from exc


HALTED_SERIES_FILE = "_halted_series.json"
_HALTED_SERIES_LOCK = threading.Lock()


def _ibkr_symbol(symbol):
    """Bank/user ticker -> IBKR SMART spelling. US class shares are written
    'BRK B' / 'BF B' (root + SPACE + single class letter) at IBKR, but the bank
    canonicalizes them to the folder-safe 'BRK-B' and users type 'BRK.B'. A
    hyphen/dot/space class separator followed by ONE class letter is rewritten
    to the space form so qualify resolves; everything else passes through
    unchanged (a no-op for ordinary tickers like AAPL, and for multi-letter
    preferred suffixes like 'BAC-PL' which IBKR spells differently)."""
    s = str(symbol).strip().upper()
    if (len(s) >= 3 and s[-2] in ".- " and s[-1].isalpha()
            and s[:-2].isalpha()):
        return f"{s[:-2]} {s[-1]}"
    return s


def _series_key(ticker, interval):
    return f"{ss.canonical_ticker(ticker)} {interval}"


def _split_series_key(key):
    try:
        ticker, interval = str(key).rsplit(" ", 1)
    except ValueError:
        return "", ""
    return ticker.strip().upper(), interval.strip()


def load_halted_series(root):
    """Retryable zero-commit SeriesHalt records from the bank sidecar."""
    try:
        data = json.loads(
            (Path(root) / HALTED_SERIES_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    out = {}
    for key, rec in data.items():
        ticker, interval = _split_series_key(key)
        if not ticker or not interval or not isinstance(rec, dict):
            continue
        reason = str(rec.get("reason") or "").strip()
        if not reason:
            continue
        out[f"{ticker} {interval}"] = {
            "reason": reason,
            "asof": str(rec.get("asof") or "").strip(),
        }
    return out


def halted_series_for_interval(root, interval, known=None):
    """Sorted retryable halted records for one interval.

    `known` is an optional {(ticker, interval)} set already visible in the
    update table; those entries are skipped to avoid duplicate rows.
    """
    known = known or set()
    out = []
    for key, rec in load_halted_series(root).items():
        ticker, iv = _split_series_key(key)
        if iv != interval or (ticker, iv) in known:
            continue
        out.append((ticker, iv, rec))
    out.sort(key=lambda x: x[0])
    return out


def record_halted_series(root, ticker, interval, reason, asof=None):
    """Persist a retryable zero-commit halt. Best-effort -> path or None."""
    key = _series_key(ticker, interval)
    why = str(reason or "").strip()
    if not why:
        return None
    target = Path(root) / HALTED_SERIES_FILE
    with _HALTED_SERIES_LOCK:
        data = load_halted_series(root)
        rec = {"reason": why,
               "asof": asof or datetime.now().isoformat(timespec="seconds")}
        if data.get(key) == rec:
            return str(target)
        data[key] = rec
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 - advisory sidecar only
            return None


def clear_halted_series(root, ticker, interval):
    """Clear a retryable halt after the series successfully commits."""
    key = _series_key(ticker, interval)
    target = Path(root) / HALTED_SERIES_FILE
    with _HALTED_SERIES_LOCK:
        data = load_halted_series(root)
        if key not in data:
            return str(target) if target.exists() else None
        data.pop(key, None)
        try:
            Path(root).mkdir(parents=True, exist_ok=True)
            ss._atomic_write_bytes(
                target,
                json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))
            return str(target)
        except Exception:  # noqa: BLE001 - advisory sidecar only
            return None


def _is_contract_rejected_halt(reason):
    text = str(reason or "").lower()
    return ("contract rejected" in text
            or "no security definition" in text)


def _ny():
    from zoneinfo import ZoneInfo
    return ZoneInfo("America/New_York")


def now_ny():
    return datetime.now(timezone.utc).astimezone(_ny()).replace(tzinfo=None)


# --- pacing -------------------------------------------------------------------

class Pacer:
    """INTERVAL-AWARE request governor (measured live 2026-06-15):

      * MINUTE+ bars are served from IBKR's cache and BYPASS the
        60/10-min historical-data-farm (HMDS) limit (sustained ~26/min,
        latency-bound). Pace them only by a min-gap (the 6-per-2-sec
        same-contract BURST rule) — `metered=False`.
      * SUB-MINUTE bars (1s..30s) are genuine HMDS hits, capped at
        EXACTLY 60/10 min. Pace them with the sliding window AND the
        min-gap — `metered=True`.
      * A pacing VIOLATION (saturate()) flips the pacer to force-metered
        for the rest of the run: proof that these requests DO count
        (deep/illiquid/first-time), so meter everything from then on.

    Injectable clock/sleep so the selftest proves the math instantly."""

    def __init__(self, max_requests=PACE_MAX_REQUESTS,
                 window_s=PACE_WINDOW_S, min_gap_s=PACE_MIN_GAP_S,
                 time_fn=_time.monotonic, sleep_fn=None,
                 force_metered=False, burst_max=PACE_BURST_MAX,
                 burst_window_s=PACE_BURST_WINDOW_S):
        self.max_requests = max_requests
        self.window_s = window_s
        self.min_gap_s = min_gap_s
        self.burst_max = burst_max
        self.burst_window_s = burst_window_s
        self._time = time_fn
        self._sleep = sleep_fn or self._default_sleep
        self._lock = threading.Lock()  # reservations and saturation share custody
        self._stamps = deque()       # METERED (HMDS) request times
        self._burst = deque()        # ALL request times -> 6-per-2s burst rule
        self._last = None            # ANY request time -> burst min-gap
        self._last_search = None     # account-shared symbol-search sub-budget
        self._not_before = 0.0       # provider Retry-After/backoff, monotonic
        self._forced = bool(force_metered)
        self._pause = None           # a SET threading.Event() pauses the run
                                     # (gap_fill sets it; wait_turn blocks)
        self._on_wait = None         # optional cosmetic observer; paired
                                     # True/False around genuine pacer sleeps

    def _default_sleep(self, seconds, cancel):
        end = self._time() + seconds
        while self._time() < end:
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            _time.sleep(min(0.25, max(0.0, end - self._time())))

    def wait_turn(self, cancel=None, metered=True, on_wait=None, *, symbol_search=False):
        """Block until a request may be sent; record it. `metered`
        counts it against the 60/10-min HMDS window (sub-minute); a
        non-metered (cache-served minute+) request is only burst-gapped.
        A prior violation (saturate) forces metering regardless. Return elapsed
        wait seconds for the request ledger. Sleeps and observer callbacks run
        OUTSIDE the state lock; every wake rechecks all constraints atomically."""
        # Pause is honored at MONTH-COMMIT boundaries (in the session processor)
        # and between series — NOT mid-request — so a pause never leaves a
        # partial month. See _make_session_processor / _fill_series_inner.
        started = self._time()
        effective_metered = bool(metered)
        waited = False
        last_wait = None

        def notify(waiting, info):
            # A per-port observer owns parallel pacers. Otherwise use the
            # request-local sink supplied by the serial caller; never mutate a
            # process-wide default pacer's callback to route one run's GUI.
            callback = self._on_wait or on_wait
            if callback is None:
                return
            try:
                callback(waiting, dict(info or {}))
            except Exception:  # noqa: BLE001 - cosmetic observer only
                pass

        def sleep_for(seconds, reason):
            nonlocal waited, last_wait
            seconds = max(0.0, float(seconds))
            if seconds <= 0.0:
                return
            last_wait = {"seconds": seconds, "reason": reason,
                         "metered": effective_metered}
            waited = True
            notify(True, last_wait)
            self._sleep(seconds, cancel)

        try:
            while True:
                if cancel is not None and cancel.is_set():
                    raise Cancelled()
                # Even contention on the short critical section is cancellable.
                if not self._lock.acquire(timeout=0.05):
                    continue
                try:
                    if cancel is not None and cancel.is_set():
                        raise Cancelled()
                    now = self._time()
                    effective_metered = bool(metered or self._forced)
                    while (self._burst
                           and now - self._burst[0] > self.burst_window_s):
                        self._burst.popleft()
                    while (self._stamps
                           and now - self._stamps[0] > self.window_s):
                        self._stamps.popleft()
                    constraints = [(0.0, "ready"),
                                   (self._not_before - now, "provider-backoff")]
                    if self._last is not None:
                        constraints.append((self._last + self.min_gap_s - now,
                                            "min-gap"))
                    if symbol_search and self._last_search is not None:
                        constraints.append((self._last_search + 1.0 - now,
                                            "symbol-search"))
                    if self.burst_max and len(self._burst) >= self.burst_max:
                        constraints.append((self._burst[0] + self.burst_window_s
                                            - now + 0.005, "burst"))
                    if effective_metered and len(self._stamps) >= self.max_requests:
                        constraints.append((self._stamps[0] + self.window_s
                                            - now + 0.005, "window"))
                    seconds, reason = max(constraints, key=lambda item: item[0])
                    if seconds <= 0.0:
                        if effective_metered:
                            self._stamps.append(now)
                        self._burst.append(now)
                        self._last = now
                        if symbol_search:
                            self._last_search = now
                        return max(0.0, now - started)
                finally:
                    self._lock.release()
                sleep_for(seconds, reason)
        finally:
            if waited:
                notify(False, last_wait)

    def saturate(self):
        """Assume the window is FULL — used after IBKR rejects a request
        for pacing. Also forces metering ON for every subsequent request
        (the rejection PROVES they count). Fills the window with stamps
        spaced one slot apart ending now, so requests proceed at the
        sustained safe rate (~window/max per request) without a stall."""
        with self._lock:
            self._forced = True
            now = self._time()
            step = self.window_s / max(1, self.max_requests)
            self._stamps = deque(
                now - self.window_s + step * (i + 1)
                for i in range(self.max_requests))

    def defer(self, seconds):
        """Publish a provider deadline under the reservation/saturation lock."""
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("invalid provider backoff")
        with self._lock:
            self._not_before = max(self._not_before, self._time() + seconds)


_DEFAULT_PACER = None
_DEFAULT_PACER_LOCK = threading.Lock()


def _default_pacer():
    """A process-wide default Pacer so BACK-TO-BACK runs share the
    sliding 60/10-min window and self-throttle. A fresh per-run pacer
    let consecutive runs trip IBKR's server-side limit (proven in the
    live 1s demo). Tests inject their own pacer, so this is untouched
    by the suite."""
    global _DEFAULT_PACER
    with _DEFAULT_PACER_LOCK:
        if _DEFAULT_PACER is None:
            _DEFAULT_PACER = Pacer()
        return _DEFAULT_PACER


# --- gap planning (pure, offline) ----------------------------------------------

def series_last_dt(manifest, interval):
    """Last stored bar's datetime for (manifest, interval), or None."""
    months = (manifest or {}).get("intervals", {}).get(interval,
                                                       {}).get("months", {})
    best = None
    for entry in months.values():
        if entry.get("status") != "present":
            continue
        last = entry.get("last", "")
        parts = last.split(" ", 1)
        if len(parts) == 2:
            dt = ss.parse_timestamp(parts[0], parts[1])
            if dt and (best is None or dt > best):
                best = dt
    return best


def series_first_dt(manifest, interval):
    """EARLIEST stored bar's datetime for (manifest, interval), or None — the
    mirror of series_last_dt. Used to cap an extended-hours (-pre/-post)
    backfill at where its regular series already starts."""
    months = (manifest or {}).get("intervals", {}).get(interval,
                                                       {}).get("months", {})
    best = None
    for entry in months.values():
        if entry.get("status") != "present":
            continue
        first = entry.get("first", "")
        parts = first.split(" ", 1)
        if len(parts) == 2:
            dt = ss.parse_timestamp(parts[0], parts[1])
            if dt and (best is None or dt < best):
                best = dt
    return best


NO_HEAD_FALLBACK_DAYS = 365 * 5      # backfill depth when the head-timestamp
#                          probe fails AND no `since` was set.


def _probe_earliest_daily(adapter, contract, today, what_to_show="TRADES",
                          cancel=None):
    """Discover a contract's REAL earliest available bar date with ONE
    big-duration DAILY request ending today. The demo CLAMPS the duration to what
    exists and answers instantly (HOOD -> 2021-07-29 in 0.2 s), so unlike a deep
    `since` this never requests pre-existence windows (which the demo HANGS on for
    a recent IPO). Returns a date, or None if even this probe fails (the caller
    then keeps the old `since` fallback). Provider absence is best-effort;
    cancellation, authority and ledger failures remain visible and propagate."""
    worker = fib.current_worker()
    covered_today = (worker is not None and
        worker.context.authority.first_date <= today <= worker.context.authority.valid_through)
    if covered_today:
        today = _operation_today(today)
    end = datetime.combine(today, time(23, 59))
    kind = next(key for key, value in fib.KINDS.items() if value == what_to_show)
    interval = "1d" + (f"-{kind}" if kind else "")
    if covered_today:
        context = worker.context
        horizon = context.horizons[interval]
        if horizon is None:
            raise CalendarUnsupported("no settled daily probe is covered")
        from fetch_envelopes import carrier_start
        floor = max(_head_probe_floor(today), context.authority.first_date)
        effective_end = min(fib.ny_bound(end), horizon)
        start = datetime.combine(floor, time.min, fib.NY)
        if carrier_start(effective_end, HEAD_PROBE_DURATION).date() < context.authority.first_date:
            return _probe_covered_daily_prefix(adapter, contract, interval, start,
                                               effective_end, cancel)
    request = fib.bar_request("ibkr.gap_fill.head_daily_probe", contract,
                              interval, end, HEAD_PROBE_DURATION)
    for _try in range(TIMEOUT_RETRIES + 1):
        try:
            with fib.adapter_session(adapter, True), fib.send_scope(
                    request, lambda: fib.acquire_turn(cancel), cancel=cancel):
                if what_to_show == "TRADES":
                    bars = adapter.fetch(contract, end, HEAD_PROBE_DURATION, "1 day")
                else:
                    bars = adapter.fetch(contract, end, HEAD_PROBE_DURATION, "1 day",
                                         what_to_show)
            if not bars:
                return None
            d0 = bars[0].date
            if isinstance(d0, datetime):
                return d0.date()
            if isinstance(d0, date):
                return d0
            return None
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except PacingViolation:
            fib.pacer().saturate()
            if _try >= TIMEOUT_RETRIES:
                raise
        except RequestTimeout:
            # a transient busy backend must NOT make the probe fail CLOSED to the
            # deep-`since` hang it exists to prevent — warm-retry the (normally
            # instant) daily probe a couple of times before giving up.
            if _try >= TIMEOUT_RETRIES:
                _diag("PROBE_FAIL", sym=getattr(contract, "symbol", None),
                      why="RequestTimeout")
                return None
            _interruptible_sleep(min(RECONNECT_BACKOFF_S, 5.0), cancel)
        except Exception as exc:  # noqa: BLE001 — best-effort, falls back to `since`
            _diag("PROBE_FAIL", sym=getattr(contract, "symbol", None),
                  why=type(exc).__name__)
            return None
    return None


def _probe_covered_daily_prefix(adapter, contract, interval, start, end, cancel):
    """Coverage-edge fallback: no carrier may round into the unproven era.

    Walk bounded daily carriers from the first covered session. The first
    session is an exact intraday carrier; later chunks are at most 365 days.
    Stop at the first demonstrated bar. Absence is returned only after every
    covered chunk returned empty; a refusal/fault never masquerades as absence.
    """
    from fetch_envelopes import carrier_start, encode_carrier
    context = fib.current_worker().context
    day = start.date()
    while day <= end.date():
        window = context.authority.window(interval, day)
        if window is not None:
            break
        day += timedelta(days=1)
    else:
        raise RequestRefused("no settled session in covered daily probe range")
    first_open, first_close = window
    # The sole caller bounds end by a non-None daily horizon: its first
    # covered session is already settled. Before the first settlement that
    # caller refuses without entering here. Send-time decide remains the
    # authority for any actual request, including a wholly unsettled range.
    pieces = [(first_open, first_close, encode_carrier(first_open, first_close))]
    cursor = datetime.combine(day + timedelta(days=1), time.min, fib.NY)
    while cursor < end:
        stop = min(cursor + timedelta(days=365), end)
        duration = encode_carrier(cursor, stop)
        if carrier_start(stop, duration).date() < context.authority.first_date:
            raise CalendarUnsupported("daily probe carrier predates coverage")
        pieces.append((cursor, stop, duration))
        cursor = stop
    kind = fib.parse_token(interval)[1]
    earliest = None
    for first, last, duration in pieces:
        # Empty closed-only tails do not represent a provider attempt.
        if not any(context.authority.window(interval, first.date() + timedelta(days=i))
                   for i in range((last.date() - first.date()).days + 1)):
            continue
        request = fib.bar_request("ibkr.gap_fill.head_daily_probe", contract, interval,
                                  last, duration, start=first)
        for attempt in range(TIMEOUT_RETRIES + 1):
            try:
                with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn, cancel=cancel):
                    bars = adapter.fetch(contract, last.replace(tzinfo=None), duration,
                                          "1 day", what_to_show=fib.KINDS[kind])
                break
            except PacingViolation:
                fib.pacer().saturate()
                if attempt == TIMEOUT_RETRIES:
                    raise
            except RequestTimeout:
                if attempt == TIMEOUT_RETRIES:
                    raise
        for bar in bars:
            stamp = bar.date.date() if isinstance(bar.date, datetime) else bar.date
            earliest = stamp if earliest is None else min(earliest, stamp)
        if earliest is not None:
            return earliest
    return None


def _head_probe_floor(today):
    """Oldest calendar date covered by HEAD_PROBE_DURATION's 30Y request."""
    years = int(HEAD_PROBE_DURATION.split()[0])
    try:
        return today.replace(year=today.year - years)
    except ValueError:  # February 29 -> February 28
        return today.replace(year=today.year - years, day=28)


def _reconcile_head_and_served(head_date, served_date, today):
    """Prefer observed daily reach when it disproves head metadata.

    A 30Y request cannot disprove a valid head older than its request window.
    When its first bar sits at that window floor, retain the older head. A first
    bar materially inside the window is authoritative: this handles both ODFL's
    false-deep head and PLTR's false-late head.
    """
    if served_date is None:
        return head_date
    if head_date is None:
        return served_date
    floor = _head_probe_floor(today)
    if (head_date < floor
            and served_date <= floor + timedelta(
                days=HEAD_PROBE_FLOOR_TOLERANCE_DAYS)):
        return head_date
    return served_date


def _head_start_evidence(adapter, contract, since, today,
                         what_to_show="TRADES"):
    """Resolve an empty series' backfill START from IBKR's head timestamp
    (earliest available bar), with a GRACEFUL fallback when that probe FAILS.

    The demo's reqHeadTimeStamp returns 'Query failed' for some contracts
    (AMD, live-confirmed 2026-06-23) even though their bars fetch perfectly — so
    a head failure must NOT halt the whole series. Fall back to the requested
    `since`, or a default lookback when no depth was set, and let the fetch
    return whatever history actually exists. Caller sets adapter.use_rth first.
    Returns ``(start_date, note_or_None, served_earliest_or_None)``. The third
    value is only the frontier demonstrated by the bounded daily request; head
    metadata alone is deliberately not seal evidence."""
    head_date = None
    head_error = None
    try:
        request = fib.head_request("ibkr.gap_fill.head_timestamp", contract,
                                   what_to_show, getattr(adapter, "use_rth", True))
        with fib.send_scope(request, fib.acquire_turn):
            head = (adapter.head_timestamp(contract) if what_to_show == "TRADES"
                    else adapter.head_timestamp(contract, what_to_show))
        head_date = (head.astimezone(_ny()).date() if head.tzinfo
                     else head.date())
        if fib.current_worker() is not None:
            fib.current_worker().context.authority.row(head_date)
    except SeriesHalt as exc:
        head_error = exc

    # Head metadata can be wrong in EITHER direction: ODFL advertises 1991 but
    # serves only 2024+, while PLTR advertises 2024 but serves from its 2020
    # listing. One bounded daily request establishes actual served reach.
    probed = _probe_earliest_daily(adapter, contract, today, what_to_show)
    resolved = _reconcile_head_and_served(head_date, probed, today)
    if head_date is not None:
        if resolved != head_date:
            start = max(resolved, since) if since is not None else resolved
            return start, (f"head timestamp {head_date} disagreed with the "
                           f"served daily first bar {probed}; starting at "
                           f"{start}"), probed
        return head_date, None, probed

    if head_error is not None:
        # The head-timestamp probe failed (demo 'Query failed'). Before falling
        # back to a deep `since` — which for a RECENT IPO (HOOD, listed 2021-07)
        # would request pre-existence windows the demo HANGS on (25 s each,
        # stranding the port for ~20 reconnects) — use the bounded daily probe.
        if probed is not None:
            if since is not None:
                start = max(probed, since)
            else:
                # no user-set depth: keep the SAME recent floor the old since=None
                # fallback used, so a DEEP daily first-bar (an old stock whose head
                # also fails) can't drive a deep INTRADAY sweep into pre-intraday-
                # reach windows that HANG. A recent IPO's probe date is later than
                # this floor, so it still wins (HOOD 2021 > today-5y).
                start = max(probed, today - timedelta(days=NO_HEAD_FALLBACK_DAYS))
            return start, (f"head timestamp unavailable ({head_error}) — probed real "
                           f"first bar {probed} from daily history; starting "
                           f"at {start}"), probed
        if since is not None:
            return since, (f"head timestamp unavailable ({head_error}) — backfilling "
                            f"from the requested {since} (the data itself "
                            f"fetches fine)"), None
        return (today - timedelta(days=NO_HEAD_FALLBACK_DAYS),
                f"head timestamp unavailable ({head_error}) and no depth set — "
                f"backfilling ~{NO_HEAD_FALLBACK_DAYS // 365}y; set a lookback "
                f"to bound it", None)

    raise SeriesHalt("no usable head timestamp or served-history probe")


def _head_start(adapter, contract, since, today, what_to_show="TRADES"):
    """Compatibility wrapper returning the established two-value contract."""
    start, note, _served_earliest = _head_start_evidence(
        adapter, contract, since, today, what_to_show)
    return start, note


@fib.symbol_scope
def earliest_available(adapter, ticker, today=None, what_to_show="TRADES",
                       cancel=None):
    """The earliest date IBKR can demonstrably SERVE for `ticker`.

    A bounded daily request reconciles head metadata because the demo can return
    false-deep (ODFL) and false-late (PLTR) timestamps. Powers the main table's
    'Earliest on IBKR' column. SAFE BY CONSTRUCTION: the probe ends TODAY and
    clamps to available data, so it never requests a pre-listing window.
    Returns a datetime.date, or None on provider absence. Safety/ledger failures
    and cancellation propagate instead of masquerading as absent history."""
    today = today or now_ny().date()
    try:
        with fib.qualification_scope("qualify", [ticker], lambda: fib.acquire_turn(cancel), cancel=cancel):
            _cid, contract = adapter.qualify(ticker)
    except (AuthorityError, LedgerError, Cancelled):
        raise
    except Exception:  # noqa: BLE001 — unknown/delisted symbol -> no earliest
        return None
    prev_rth = getattr(adapter, "use_rth", True)
    adapter.use_rth = True             # daily/head under RTH for a STABLE earliest
    try:
        head_date = None
        try:
            request = fib.head_request("ibkr.earliest_available.head_timestamp", contract,
                                       what_to_show, True)
            with fib.send_scope(request, lambda: fib.acquire_turn(cancel), cancel=cancel):
                head = (adapter.head_timestamp(contract) if what_to_show == "TRADES"
                        else adapter.head_timestamp(contract, what_to_show))
            if isinstance(head, datetime):
                head_date = (head.astimezone(_ny()).date() if head.tzinfo
                             else head.date())
                if fib.current_worker() is not None:
                    fib.current_worker().context.authority.row(head_date)
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception:  # noqa: BLE001 — head fails for some contracts -> probe
            pass
        served = _probe_earliest_daily(
            adapter, contract, today, what_to_show, cancel)
        return _reconcile_head_and_served(head_date, served, today)
    finally:
        adapter.use_rth = prev_rth      # don't leak RTH into the next series fetch


def _identity_earliest_evidence(adapter, contract, today,
                                what_to_show="TRADES", cancel=None):
    """Return a reconciled earliest date only with demonstrated daily bars.

    Head metadata is useful for reconciling a 30-year probe, but it is not
    sufficient identity evidence by itself.  A transient empty probe must
    leave the add-stock identity check unverified instead of false-blocking a
    legitimate ticker.
    """
    prev_rth = getattr(adapter, "use_rth", True)
    adapter.use_rth = True
    try:
        head_date = None
        try:
            request = fib.head_request("ibkr.identity.head_timestamp", contract, what_to_show, True)
            with fib.send_scope(request, fib.acquire_turn, cancel=cancel):
                head = (adapter.head_timestamp(contract) if what_to_show == "TRADES"
                        else adapter.head_timestamp(contract, what_to_show))
            if isinstance(head, datetime):
                head_date = (head.astimezone(_ny()).date() if head.tzinfo
                             else head.date())
                if fib.current_worker() is not None:
                    fib.current_worker().context.authority.row(head_date)
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception:  # noqa: BLE001 - demonstrated bars can still decide
            pass
        served = _probe_earliest_daily(
            adapter, contract, today, what_to_show, cancel)
        if served is None:
            return None
        return _reconcile_head_and_served(head_date, served, today)
    finally:
        adapter.use_rth = prev_rth


def trading_days(first, last):
    """Mon-Fri dates from `first` through `last` inclusive (holidays are
    handled downstream: they simply return zero bars)."""
    out, d = [], first
    while d <= last:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def day_requests(interval, day, *, session_window=None):
    """The (end_dt_naive_NY, duration_str) request list covering ONE
    session of `day` at `interval`. Sub-minute bars need windowing. A
    -pre/-post token windows that extended session instead of RTH. Execution
    may supply a proven authority window; static legacy estimates do not."""
    bar_size, win = _BAR_SIZES[ss.base_interval(interval)]
    _use, s_open, s_close, _last, _secs = _session_spec(interval)
    if session_window is not None:
        s_open, s_close = (stamp.time() for stamp in session_window)
    if win is None:
        return [(datetime.combine(day, s_close), "1 D")]
    out = []
    t = datetime.combine(day, s_open)
    end = datetime.combine(day, s_close)
    while t < end:
        t2 = min(t + timedelta(seconds=win), end)
        out.append((t2, f"{int((t2 - t).total_seconds())} S"))
        t = t2
    return out


def _covered_session_requests(interval, day, context):
    """C1 day planners: proven opening/close, with carriers inside coverage.

    Keep ordinary one-day carriers and the existing sub-minute slices. At the
    first covered date only, replace an outward-rounded carrier with exact
    session seconds. Intended starts are aware; ends retain day_requests' naive
    New York adapter convention. This planner creates no operation authority.
    """
    from fetch_envelopes import carrier_start, encode_carrier
    window = context.authority.window(interval, day)
    if window is None:
        raise RequestRefused("closed session has no request")
    first, _ = window
    pieces = []
    for end, duration in day_requests(interval, day, session_window=window):
        last = fib.ny_bound(end)
        if carrier_start(last, duration).date() < context.authority.first_date:
            duration = encode_carrier(first, last)
        pieces.append((first, end, duration))
        first = last
    return pieces


def _fetch_span(interval):
    """Resolve a full kind/session token to its measured base fetch span."""
    return _FETCH_SPAN.get(interval, _FETCH_SPAN[ss.base_interval(interval)])


def span_chunks(interval, days):
    """Group `days` (sorted trading dates) into coarse fetch chunks for a
    whole-session interval -> [(end_dt_naive_NY, duration_str,
    [covered dates])]. Each chunk's first->last stays within the
    interval's max calendar span so one request fully covers it. The chunk
    ends at the session's close (20:00 for -post) so useRTH=False captures
    the extended bars."""
    # full-token entry (e.g. "1m-pre"/"1m-post") wins over the base ("1m") so
    # extended series can use a SMALLER span: useRTH=False returns ~960 bars/
    # session (vs 390 RTH), so the same per-request bar-count blow-up hits at a
    # smaller calendar span -> they need their own, tighter span to stay fast.
    duration, max_cal = _fetch_span(interval)
    _use, _open, s_close, _last, _secs = _session_spec(interval)
    out = []
    i = 0
    while i < len(days):
        first = days[i]
        j = i
        while j + 1 < len(days) and (days[j + 1] - first).days <= max_cal:
            j += 1
        covered = days[i:j + 1]
        out.append((datetime.combine(covered[-1], s_close), duration,
                    covered))
        i = j + 1
    return out


def request_count(interval, days):
    """How many IBKR requests `days` will cost at `interval` — spans for
    whole-session intervals, intra-day windows for sub-minute. Keeps the
    estimate and the fetcher in lockstep (one source of truth)."""
    if not days:
        return 0
    if _BAR_SIZES[ss.base_interval(interval)][1] is None:
        return len(span_chunks(interval, days))
    return len(days) * len(day_requests(interval, days[0]))


DATE_SPLIT_MIN_MONTHS = 6


def _ym(d):
    return (d.year, d.month)


def partition_series_by_months(days, k, min_months=DATE_SPLIT_MIN_MONTHS):
    """Split trading days into contiguous, month-aligned chunks.

    Returns chunks shaped like {"lo": (y,m), "hi": (y,m), "months": [...],
    "days": [...]}. If the requested split would create a sub-minimum tail, it
    falls back to fewer chunks, possibly one.
    """
    days = sorted(days or [])
    months = sorted({_ym(d) for d in days})
    if not months:
        return []
    by_month = {}
    for d in days:
        by_month.setdefault(_ym(d), []).append(d)

    def _chunk(run):
        return {"lo": run[0], "hi": run[-1], "months": list(run),
                "days": [d for mo in run for d in by_month.get(mo, [])]}

    max_k = max(1, len(months) // max(1, int(min_months)))
    kk = max(1, min(int(k), max_k))
    if kk <= 1:
        return [_chunk(months)]
    base, extra = divmod(len(months), kk)
    out, i = [], 0
    for c in range(kk):
        n = base + (1 if c < extra else 0)
        out.append(_chunk(months[i:i + n]))
        i += n
    return out


def _normalize_month_range(month_range):
    if not month_range:
        return None
    lo, hi = month_range
    lo = (int(lo[0]), int(lo[1]))
    hi = (int(hi[0]), int(hi[1]))
    if hi < lo:
        raise ValueError(f"bad month range {month_range!r}")
    return lo, hi


def _days_in_month_range(days, month_range):
    rng = _normalize_month_range(month_range)
    if rng is None:
        return list(days or [])
    lo, hi = rng
    return [d for d in (days or []) if lo <= (d.year, d.month) <= hi]


def _month_keys_for_range(month_range):
    rng = _normalize_month_range(month_range)
    if rng is None:
        return []
    (y, m), (hy, hm) = rng
    out = []
    while (y, m) <= (hy, hm):
        out.append(ss.month_key(y, m))
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def _month_range_spec(month_ranges, ticker, interval):
    if not month_ranges:
        return None, True
    spec = (month_ranges.get((ticker, interval))
            or month_ranges.get((str(ticker), str(interval))))
    if not spec:
        return None, True
    if isinstance(spec, dict):
        return spec.get("range"), bool(spec.get("owns_earliest", True))
    return spec, True


def plan_gap(root, ticker, interval, today=None, manifest=None):
    """Offline gap plan for one series (no network): which sessions are
    missing, starting with a REFETCH of the last stored session. Empty
    series have start=None (resolved live via head timestamp). `manifest`
    (optional) reuses an already-loaded manifest dict so a bulk caller (the
    update dialog's per-ticker refresh) parses each ticker's JSON once
    instead of re-loading it here."""
    if ss.base_interval(interval) not in _BAR_SIZES:
        return {"ticker": ticker, "interval": interval,
                "error": f"interval {interval} not fetchable from IBKR"}
    if ss.kind_of(interval) == "hvol" and ss.base_interval(interval) != "1d":
        return {"ticker": ticker, "interval": interval,
                "error": "historical volatility is daily-only — use '1d-hvol', "
                         f"not {interval!r}"}
    today = today or now_ny().date()
    if manifest is None:
        manifest = ss.load_manifest(Path(root) / ticker)
    last = series_last_dt(manifest, interval)
    plan = {"ticker": ticker, "interval": interval,
            "last_stored": last, "clipped_1s": False, "days": None,
            "empty_series": last is None, "error": None}
    start = None if last is None else last.date()
    if start is not None and ss.base_interval(interval).endswith("s"):
        floor = today - timedelta(days=ONE_SECOND_MAX_AGE_DAYS)
        if start < floor:
            plan["clipped_1s"] = True
            start = floor
    if start is not None:
        plan["days"] = trading_days(start, today)
        plan["est_requests"] = request_count(interval, plan["days"])
    return plan


class _PickupPlanAhead:
    """One-run cache for the next series' entirely offline preparation.

    ``prepare`` is called at the current series' final historical-request
    boundary.  The local planning time therefore consumes time that would
    otherwise be spent waiting for the pacer slot, rather than sitting between
    two series.  Parallel workers may prepare the same queued job; a small
    event-backed slot lets its eventual owner reuse the first complete result
    without reserving that job or weakening dynamic load balancing.

    Failed speculative preparation is deliberately forgotten.  The normal
    foreground path then repeats the operation and preserves its original
    exception/reporting behavior.
    """

    def __init__(self, root, today):
        self.root = Path(root)
        self.today = today
        self._lock = threading.Lock()
        self._slots = {}

    @staticmethod
    def _key(ticker, interval):
        return str(ticker).upper(), str(interval)

    def reserve(self, ticker, interval):
        key = self._key(ticker, interval)
        with self._lock:
            if key in self._slots:
                return None
            slot = {"ready": threading.Event(), "payload": None}
            self._slots[key] = slot
        return key, slot, ticker, interval

    def prepare_reserved(self, reservation):
        if reservation is None:
            return
        key, slot, ticker, interval = reservation
        try:
            plan = plan_gap(self.root, ticker, interval, today=self.today)
            actions = (None if plan.get("error")
                       else sb.load_actions(self.root, ticker))
            slot["payload"] = (plan, actions)
        except Exception:  # noqa: BLE001 - speculation must not break current work
            with self._lock:
                if self._slots.get(key) is slot:
                    self._slots.pop(key, None)
        finally:
            slot["ready"].set()

    def prepare(self, ticker, interval):
        self.prepare_reserved(self.reserve(ticker, interval))

    def take(self, ticker, interval, cancel=None):
        return self._read(ticker, interval, consume=True, cancel=cancel)

    def peek(self, ticker, interval):
        return self._read(ticker, interval, consume=False, cancel=None)

    def _read(self, ticker, interval, consume, cancel):
        key = self._key(ticker, interval)
        with self._lock:
            slot = self._slots.get(key)
        if slot is None:
            return None
        deadline = _time.monotonic() + PICKUP_PLAN_WAIT_S
        while not slot["ready"].wait(timeout=0.05):
            if cancel is not None and cancel.is_set():
                return None
            if _time.monotonic() >= deadline:
                with self._lock:
                    if self._slots.get(key) is slot:
                        self._slots.pop(key, None)
                return None
        payload = slot.get("payload")
        if payload is not None:
            try:
                plan = payload[0]
                manifest = ss.load_manifest(self.root / str(ticker))
                current_last = series_last_dt(manifest, str(interval))
                fresh = plan.get("last_stored") == current_last
            except Exception:  # noqa: BLE001 - foreground planning is fallback
                fresh = False
            if not fresh:
                payload = None
        with self._lock:
            if self._slots.get(key) is slot and (consume or payload is None):
                self._slots.pop(key, None)
        return payload


_PICKUP_MISSING = object()


class _PickupSingleFlight:
    """Run-local cancel-aware single-flight for immutable pickup evidence.

    Only slot/value publication holds the lock. Producers perform broker work
    outside it, so unrelated keys remain independent. A failed producer leaves
    no cached exception or value; waiters wake and arbitrate again.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}
        self._slots = {}

    def peek(self, key):
        with self._lock:
            return self._values.get(key, _PICKUP_MISSING)

    def get_or_compute(self, key, producer, cancel=None, cache_when=None):
        while True:
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            with self._lock:
                cached = self._values.get(key, _PICKUP_MISSING)
                if cached is _PICKUP_MISSING:
                    slot = self._slots.get(key)
                    owner = slot is None
                    if owner:
                        slot = {"ready": threading.Event()}
                        self._slots[key] = slot
            if cached is not _PICKUP_MISSING:
                if cancel is not None and cancel.is_set():
                    raise Cancelled()
                return cached, True
            if owner:
                try:
                    value = producer()
                    publish = (cache_when is None or cache_when(value))
                except BaseException:
                    with self._lock:
                        if self._slots.get(key) is slot:
                            self._slots.pop(key, None)
                    raise
                else:
                    with self._lock:
                        if publish:
                            self._values[key] = value
                        if self._slots.get(key) is slot:
                            self._slots.pop(key, None)
                    return value, False
                finally:
                    slot["ready"].set()
            while not slot["ready"].wait(timeout=0.05):
                if cancel is not None and cancel.is_set():
                    raise Cancelled()


class _PickupResolvedMap(_PickupSingleFlight):
    """One parallel run's canonical ticker -> positive conId/None map."""

    @staticmethod
    def key(ticker):
        return str(ticker).strip().upper()

    @classmethod
    def mapping_value(cls, mapping, ticker):
        if not isinstance(mapping, dict):
            return None
        key = cls.key(ticker)
        matches = [value for raw_key, value in mapping.items()
                   if isinstance(raw_key, str)
                   and cls.key(raw_key) == key]
        return matches[0] if len(matches) == 1 else None

    def resolve(self, ticker, producer, cancel=None):
        def validated():
            value = producer()
            try:
                if isinstance(value, bool) or int(value) <= 0:
                    return None
                return int(value)
            except (TypeError, ValueError, OverflowError):
                return None

        return self.get_or_compute(
            self.key(ticker), validated, cancel=cancel)


class _PickupHeadCache(_PickupSingleFlight):
    """One parallel run's immutable live head/daily evidence cache."""

    def __init__(self, shared=False):
        super().__init__()
        self._complete_only = bool(shared)

    @staticmethod
    def key(conid, what_to_show, today, since):
        if isinstance(conid, bool) or int(conid) <= 0:
            raise ValueError("pickup head evidence requires a positive conId")
        return int(conid), str(what_to_show), today, since

    @staticmethod
    def _complete(evidence):
        try:
            _start, _note, served_earliest = evidence
            return served_earliest is not None
        except (TypeError, ValueError):
            return False

    def get_or_compute(self, key, producer, cancel=None):
        # _probe_earliest_daily deliberately converts link failures into None
        # so a serial producer can retain its established head-only fallback.
        # Do not amplify that incomplete result across adapters: publish only
        # evidence carrying a demonstrated daily served frontier. Waiters wake
        # and retry arbitration on their own healthy connection.
        return super().get_or_compute(
            key, producer, cancel=cancel,
            cache_when=(self._complete if self._complete_only else None))


def _strict_pickup_cache_date(raw, ticker, today, expected_conid):
    """Parse only conId-bound evidence for the expected live contract."""
    try:
        key = str(ticker).strip().upper()
        # A hit suppresses the only live head check. Legacy ticker-only dates,
        # coerced ids, extra fields, and ambiguously cased keys are therefore
        # not strong enough identity evidence.
        if not isinstance(raw, dict) or key not in raw:
            return None
        aliases = [k for k in raw if isinstance(k, str)
                   and k.strip().upper() == key]
        if aliases != [key]:
            return None
        value = raw[key]
        if not isinstance(value, dict) or set(value) != {"earliest", "conid"}:
            return None
        cached_text = value.get("earliest")
        bound = value.get("conid")
        if (not isinstance(cached_text, str) or len(cached_text) != 10
                or cached_text != cached_text.strip()
                or isinstance(bound, bool) or not isinstance(bound, int)
                or bound <= 0 or expected_conid is None
                or isinstance(expected_conid, bool)
                or bound != int(expected_conid)):
            return None
        cached = date.fromisoformat(cached_text)
        if cached.isoformat() != cached_text:
            return None
        return cached if cached <= today else None
    except (AttributeError, OSError, TypeError, ValueError, OverflowError):
        return None


def _cached_pickup_start(root, ticker, today, expected_conid):
    """Return a trustworthy ticker-wide earliest sidecar date, or ``None``.

    The caller separately establishes that flat TRADES evidence is applicable
    to this kind and ticker lifecycle before invoking this strict parser.
    The cache is advisory: missing, corrupt, malformed, and future-dated
    entries, legacy strings, and entries bound to another conId all retain the
    live head-probe path. Reloading for each empty series also sees evidence
    persisted earlier in the same run.
    """
    try:
        import stock_validate as _sv
        return _strict_pickup_cache_date(
            _sv.load_ibkr_earliest(root, include_identity=True),
            ticker, today, expected_conid)
    except Exception:  # noqa: BLE001 - advisory read, live probe is fallback
        return None


def _has_stored_trades_month(root, ticker, manifest):
    """Whether this ticker really holds a manifest-backed TRADES month.

    The ticker-wide earliest sidecar is TRADES evidence.  It may retain the
    same conId across a fresh add, quarantine, or identity truncation, so
    identity binding alone cannot give it lifecycle authority.  Require both
    an exact ``present`` TRADES-family manifest entry and its month file.  This
    is read-only and deliberately fails closed to a fresh live probe.
    """
    try:
        folder = ss.canonical_ticker(ticker)
        intervals = (manifest or {}).get("intervals")
        if not isinstance(intervals, dict):
            return False
        for interval, section in intervals.items():
            if (not isinstance(interval, str)
                    or not ss.INTERVAL_RE.fullmatch(interval)
                    or ss.kind_of(interval) != ""
                    or not isinstance(section, dict)):
                continue
            months = section.get("months")
            if not isinstance(months, dict):
                continue
            for key, record in months.items():
                if (not isinstance(key, str) or not isinstance(record, dict)
                        or record.get("status") != "present"):
                    continue
                if re.fullmatch(r"[12]\d{3}-(?:0[1-9]|1[0-2])", key) is None:
                    return False
                year_text, month_text = key.split("-", 1)
                year, month = int(year_text), int(month_text)
                if ss.month_key(year, month) != key:
                    continue
                path = ss.find_month_file(
                    root, folder, year, month, interval)
                if path is not None and path.is_file():
                    return True
    except (AttributeError, IndexError, OSError, TypeError, ValueError,
            OverflowError,
            ss.StorageError):
        return False
    return False


def _pickup_sidecar_authoritative(root, ticker, manifest, what_to_show):
    """Whether the flat durable TRADES cache may suppress this live probe."""
    if not KIND_EARLIEST_PROBE:
        return True
    return (what_to_show == "TRADES"
            and _has_stored_trades_month(root, ticker, manifest))


def _pickup_identity_conid(manifest, resolved, ticker):
    """Return the agreed pinned/live conId required for cache authority."""
    pinned = (manifest or {}).get("conid")
    live = (resolved or {}).get(ticker)
    try:
        if (pinned is None or live is None or isinstance(pinned, bool)
                or isinstance(live, bool) or int(pinned) <= 0
                or int(pinned) != int(live)):
            return None
        return int(pinned)
    except (TypeError, ValueError, OverflowError):
        return None


def _record_pickup_evidence(root, ticker, served_earliest, conid,
                            cache_start=None):
    """Best-effort durable binding of a safe TRADES start to ``conid``.

    A bounded daily probe can land at its 30-year floor while a trustworthy
    head timestamp is older.  The older head must remain the fetch start, but
    the later demonstrated bar is not a full-history cache frontier: persisting
    it would silently omit the older decades on the next empty kind/session.
    Leave that conservative case uncached so the next series probes again.
    """
    if served_earliest is None:
        return None
    if cache_start is not None and cache_start < served_earliest:
        return None
    try:
        import stock_validate as _sv
        return _sv.record_ibkr_earliest(
            root, ticker, served_earliest.isoformat(), conid=conid)
    except Exception:  # noqa: BLE001 - advisory persistence never breaks fetch
        return None


# --- bar conversion -------------------------------------------------------------

def split_session_bars(raw_bars, valid_days, counters, interval=None):
    """IBKR bars -> {session date: [contract-clean tuples]} for every
    date in `valid_days` (a set). One cleaning implementation shared by
    the per-day and the coarse-span fetch paths. Raw bars carry
    aware-UTC datetimes (formatDate=2); volume may be a float and is
    only accepted when integral and >=0. `interval` selects the session
    window to KEEP (its bars) — a -post token keeps only 16:00-19:59 and
    drops the RTH/pre bars the useRTH=False request also returned; None ->
    RTH. The non_rth counter tallies bars dropped as out-of-session."""
    ny = _ny()
    window = (ss.session_window(interval) if interval is not None
              else (ss.RTH_FIRST, ss.RTH_LAST))
    lo_t, hi_t = window
    kind = ss.kind_of(interval) if interval is not None else ""
    # Volume is MEANINGLESS for COMPUTED whatToShow kinds (iv/hvol/bidask): IBKR
    # returns an env-dependent placeholder (the demo serves 1.0; some report -1 —
    # confirmed live 2026-06-24). Normalize ALL of it to a consistent 0 so the bar
    # always stores and a re-fetch never differs.
    sentinel_vol = kind in ss.RATIO_KINDS or kind == "bidask"
    out = {}
    for b in raw_bars:
        dt = b.date
        if isinstance(dt, datetime):
            if dt.tzinfo is not None:
                dt = dt.astimezone(ny).replace(tzinfo=None)
            # else: a naive intraday datetime — keep as-is
        elif isinstance(dt, date):
            # ib_async returns a date-ONLY object for DAILY bars (formatDate=2
            # epoch-encodes only INTRADAY). Pin to NAIVE MIDNIGHT on that calendar
            # day — never astimezone it (the IBKR daily date is tz-naive; a
            # UTC->NY shift would move it to the PRIOR day). Inside DAILY_WINDOW.
            dt = datetime.combine(dt, time(0, 0, 0))
        else:
            counters["invalid"] += 1          # genuinely unparseable
            continue
        if dt.date() not in valid_days:
            counters["outside_day"] += 1
            continue
        if dt.weekday() > 4 or not (lo_t <= dt.time() <= hi_t):
            counters["non_rth"] += 1
            continue
        try:
            o, h, lo, c = (float(b.open), float(b.high),
                           float(b.low), float(b.close))
            v = float(b.volume)
        except (TypeError, ValueError):
            counters["invalid"] += 1
            continue
        if (VOL_VALUE_GATE and kind in ss.RATIO_KINDS
                and any(value > VOL_HARD_CEILING
                        for value in (o, h, lo, c))):
            # Volatility is stored as a decimal ratio.  A value above 10.0
            # (1000%) is not a price-scale outlier; it is malformed ratio data.
            # Use the existing loud invalid-bar path so clean bars in the same
            # month still commit and the run's invalid counter stays truthful.
            counters["invalid"] += 1
            continue
        if sentinel_vol:
            v = 0.0                  # meaningless for computed kinds -> consistent 0
        if v < 0 or v != v:          # negative or NaN -> genuinely invalid, drop
            counters["invalid"] += 1
            continue
        # The demo serves FRACTIONAL (split/dividend-ADJUSTED) volume for old bars
        # (e.g. 60982.56) — legitimate data, not corruption. ROUND to whole shares
        # (<1-share error) rather than dropping it, or all deep history is lost
        # engine-wide. Live IBKR returns integer share volume, so this only touches
        # adjusted demo history; recent integer volumes round to themselves.
        bar = (dt, o, h, lo, c, int(round(v)))
        if ss.validate_bar(
                *bar, window=window,
                allow_zero_prices=kind in ss.RATIO_KINDS):
            counters["invalid"] += 1
            continue
        out.setdefault(dt.date(), []).append(bar)
    for d in out:
        out[d].sort(key=lambda x: x[0])
    return out


def convert_bars(raw_bars, day, counters, interval=None):
    """IBKR bars -> contract-clean tuples for ONE session date (thin
    wrapper over split_session_bars; the per-day fetch path). MUST forward
    `interval` so a sub-minute -pre/-post series keeps its OWN session's bars
    (the sub-minute fetch path and the F7 spot-check both rely on this)."""
    return split_session_bars(raw_bars, frozenset((day,)),
                              counters, interval).get(day, [])


# --- the live adapter ------------------------------------------------------------

class LiveIB:
    """Thin wrapper over ib_async's sync API. Lazy import; creates an
    event loop for worker threads (3.14 raises without one). All errors
    surface as ConnectionError (retryable) or SeriesHalt (not)."""

    def __init__(self, host=HOST_DEFAULT, ports=PORTS_DEFAULT,
                 client_id=CLIENT_ID_FETCH, selected_account=None):
        self.host, self.ports, self.client_id = host, ports, client_id
        self.selected_account = selected_account
        self._base_client_id = client_id     # rotation always restarts from here
        self._operation_lease = None
        self.ib = None
        self.port = None
        self.errors = []                     # (code, msg) of last request
        self.use_rth = True                  # set per-series by _fetch_request:
                                             # True for RTH, False for -pre/-post

    def _loop(self):
        import asyncio
        try:
            asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:                 # worker thread, no loop yet
            asyncio.set_event_loop(asyncio.new_event_loop())

    def _await(self, coro, timeout, what):
        """Run one ib_async async request with a HARD timeout so a frozen or
        unresponsive TWS can never block forever. A timeout surfaces as a
        (retryable) ConnectionError, exactly like a dropped socket."""
        import asyncio
        try:
            return self.ib.run(asyncio.wait_for(coro, timeout))
        except asyncio.TimeoutError:
            raise RequestTimeout(
                f"IBKR did not answer {what} within {timeout:.0f}s "
                f"(TWS busy or unresponsive)")

    def connect(self):
        acquired_here = False
        if self._operation_lease is None:
            from operation_gate import acquire
            self._operation_lease = acquire(
                "fetch", owner=f"LiveIB client {self._base_client_id}")
            acquired_here = True
        try:
            return self._connect_under_gate()
        except BaseException:
            if acquired_here:
                self._release_operation_gate()
            raise

    def _connect_under_gate(self):
        from ib_async import IB
        try:                                    # skip the startup positions /
            from ib_async import StartupFetch   # orders / account-update pull: the
            no_fetch = StartupFetch(0)          # engine only fetches historical
        except Exception:  # noqa: BLE001       # bars, and on the demo those acct
            no_fetch = None                     # reqs STALL under Read-Only (321)
        self._loop()
        last = None
        for port in self.ports:
            # Try the base id, then base+10, base+20, … on a 326 ("client id
            # already in use"): a half-open zombie or a live peer holding the id
            # on THIS TWS instance no longer wedges the port — we grab the next
            # free id in the family instead (fixes the recurring port flap).
            for k in range(CLIENT_ID_RETRIES + 1):
                cid = self._base_client_id + k * CLIENT_ID_STRIDE
                ib = IB()
                ib.errorEvent += self._on_error
                self.errors = []
                # TWS signals a held id TWO ways: a numbered error 326 (->
                # errorEvent) OR, for a half-open zombie, by closing the socket
                # mid-handshake, which ib_async surfaces as a "clientId N already
                # in use?" note on client.apiError (NOT errorEvent). Capture both
                # so rotation fires for either.
                api_errs = []
                try:
                    ib.client.apiError += (
                        lambda *m, _a=api_errs: _a.append(" ".join(map(str, m))))
                except Exception:  # noqa: BLE001 — ib_async without apiError
                    pass
                # the connect blocks for the whole timeout (the demo never sends
                # apiStart) but the socket is usable throughout — so keep it short.
                to = CONNECT_TIMEOUT_S if k == 0 else ROTATE_CONNECT_TIMEOUT_S
                _ct0 = _time.monotonic()
                try:
                    kw = dict(clientId=cid, timeout=to, readonly=True)
                    if no_fetch is not None:
                        kw["fetchFields"] = no_fetch
                    ib.connect(self.host, port, **kw)
                    ib.reqMarketDataType(3)      # delayed is fine for bars
                    # backstop: bound ANY blocking ib call (managedAccounts,
                    # search, …) so a frozen/half-open TWS can't hang forever.
                    # The hot calls (qualify/head/fetch) use explicit _await
                    # timeouts; this catches the rest.
                    ib.RequestTimeout = QUALIFY_TIMEOUT_S
                    self.ib, self.port, self.client_id = ib, port, cid
                    _diag("CONNECT", port=port, cid=cid,
                          secs=round(_time.monotonic() - _ct0, 1),
                          errs=_diag_codes(self.errors))
                    return self
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    _diag("CONNECT_FAIL", port=port, cid=cid,
                          secs=round(_time.monotonic() - _ct0, 1),
                          why=type(exc).__name__, errs=_diag_codes(self.errors))
                    try:
                        ib.disconnect()         # free the half-built socket
                    except Exception:  # noqa: BLE001
                        pass
                    busy = (any(code == 326 for code, _ in self.errors)
                            or any("already in use" in str(m).lower()
                                   for _, m in self.errors)
                            or any("already in use" in m.lower()
                                   for m in api_errs))
                    if busy:
                        continue                 # id held on this port — next id
                    break                        # other failure — next port
        kind = addstock_watchdog.hard_signal_kind(last)
        failure_type = (ConnectionRefusedError
                        if kind == "connection_refused" else
                        ConnectionLost
                        if kind in ("connection_lost", "socket_reset") else
                        ConnectionError)
        raise failure_type(
            f"no TWS/Gateway answered on {self.host} ports "
            f"{list(self.ports)} ({last}) — is it running with the API "
            f"enabled?")

    def _on_error(self, reqId, code, msg, *a):
        code = int(code)
        self.errors.append((code, str(msg)))
        if code not in _DIAG_SKIP_CODES:      # 321/420/326/110x… worth a line
            _diag("ERROR", port=getattr(self, "port", None),
                  cid=getattr(self, "client_id", None), code=code,
                  msg=str(msg)[:90].replace("\n", " "))

    def is_connected(self):
        return self.ib is not None and self.ib.isConnected()

    def reconnect(self):
        try:
            if self.ib is not None:
                self.ib.disconnect()
        except Exception:  # noqa: BLE001
            pass
        self.ib = None
        self.connect()

    def account(self):
        if self.ib is None:           # dead connection during a give-up
            return "?"                # flush crashed the whole run here
        from fetch_governors import verified_account
        accts = self.ib.managedAccounts()
        return verified_account(accts, self.selected_account)

    def request_governor(self, context):
        """Revalidate connected account membership for each physical attempt."""
        if self.ib is None or not self.is_connected():
            raise AuthorityError("no connected account evidence")
        # account() validates the actual managed set, including explicit selection.
        return context.governors.ibkr([self.account()])

    def qualify(self, symbol):
        """-> (conId, contract) or SeriesHalt."""
        requests, acquire_turn = fib.take_qualification("qualify", [symbol])
        from ib_async import Stock, Contract
        c = Stock(_ibkr_symbol(symbol), "SMART", "USD")
        try:
            got = fib.bind_request(self, requests[0]).execute(
                lambda effective: self._await(self.ib.qualifyContractsAsync(c),
                    QUALIFY_TIMEOUT_S, f"the {symbol} contract lookup"),
                acquire_turn=acquire_turn, normalizer=fib.normalize_qualification)
        except PacingViolation:
            fib.pacer().saturate()
            raise
        if not got:
            raise SeriesHalt(f"IBKR has no SMART/USD stock {symbol!r} "
                             f"(delisted? wrong symbol?)")
        # Publish only a fresh object reconstructed from the durable snapshot,
        # never the provider-owned mutable object used during qualification.
        item = got[0]
        c = Contract(conId=item["con_id"], symbol=item["symbol"], secType=item["sec_type"],
                     exchange=item["exchange"], currency=item["currency"],
                     primaryExchange=item["primary_exchange"], localSymbol=item["local_symbol"],
                     tradingClass=item["trading_class"])
        return c.conId, c

    def company_name(self, symbol, *, contract=None):
        """Child-only, ledgered details of an explicitly qualified contract."""
        attempt = fib.take_metadata("company_name", symbol,
                                    getattr(contract, "conId", None))
        contract = deepcopy(contract)
        def transport(effective):
            try:
                return self._await(self.ib.reqContractDetailsAsync(contract),
                    QUALIFY_TIMEOUT_S, f"the {symbol} contract details")
            except (PacingViolation, AuthorityError, LedgerError, Cancelled):
                raise
            except Exception:  # noqa: BLE001 — only provider unavailability is best effort
                return []
        try:
            result = fib.bind_request(self, attempt.request).execute(
                transport, acquire_turn=attempt.acquire_turn,
                normalizer=lambda response: fib.normalize_company(response, attempt.request.envelope.con_id))
        except PacingViolation:
            fib.pacer().saturate()
            raise
        return result["name"]

    def contract_for(self, conid):
        """Build a historical-data contract straight from a PINNED conId
        — NO network round-trip (reqHistoricalData resolves a conId).
        Lets the engine SKIP qualify for already-known series, and pins
        us to the exact contract the series was built from (immune to
        ticker reuse). A stale/delisted conId just fails the fetch."""
        from ib_async import Contract
        return Contract(conId=int(conid), secType="STK",
                        exchange="SMART", currency="USD")

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        """Resolve MANY symbols as individually ledgered qualification calls ->
        {symbol: conId or None}. None = no live SMART/USD contract
        (delisted / wrong symbol). reqContractDetails (what this uses)
        is not on the metered historical window, but every physical request
        obtains the shared burst/min-gap turn. `chunk` controls progress groups,
        not hidden library fan-out. Cancellation is checked by each turn."""
        syms = list(symbols)
        requests, acquire_turn = fib.take_qualification("qualify_many", syms)
        from ib_async import Stock
        out = {}
        for i in range(0, len(syms), chunk):
            if cancel is not None and cancel.is_set():
                raise Cancelled("symbol validation cancelled")
            part = syms[i:i + chunk]
            if progress is not None:
                try:
                    progress(f"Checking {i + len(part)}/{len(syms)} "
                             f"symbols at IBKR…")
                except Exception:  # noqa: BLE001
                    pass
            cs = [Stock(_ibkr_symbol(s), "SMART", "USD") for s in part]
            # ib_async expands a batch into one physical contract-details
            # request per input. Expand here so every send has its own pair.
            for offset, c in enumerate(cs):
                request = fib.bind_request(self, requests[i + offset])
                got = None
                try:
                    got = request.execute(
                        lambda effective: self._await(self.ib.qualifyContractsAsync(c),
                            QUALIFY_TIMEOUT_S, "a contract lookup"),
                        acquire_turn=acquire_turn, normalizer=fib.normalize_qualification)
                except PacingViolation:
                    fib.pacer().saturate()
                    raise
                except (ConnectionError, AuthorityError, LedgerError, Cancelled):
                    raise
                except Exception:  # noqa: BLE001 — one bounded fallback attempt
                    c = Stock(_ibkr_symbol(part[offset]), "SMART", "USD")
                    try:
                        got = request.execute(
                            lambda effective: self._await(self.ib.qualifyContractsAsync(c),
                                QUALIFY_TIMEOUT_S, "a contract lookup retry"),
                            acquire_turn=acquire_turn, normalizer=fib.normalize_qualification)
                    except PacingViolation:
                        fib.pacer().saturate()
                        raise
                    except (ConnectionError, AuthorityError, LedgerError, Cancelled):
                        raise
                    except Exception:  # noqa: BLE001
                        pass
                out[part[offset]] = got[0]["con_id"] if got else None
        return out

    def search(self, text):
        """Single-use exact-text search; account-shared throttle and durable output."""
        attempt = fib.take_metadata("symbol_search", text)
        if text != fib.canonical_search(text):
            raise RequestRefused("symbol search text is not canonical")
        try:
            return fib.bind_request(self, attempt.request).execute(
                lambda effective: self.ib.reqMatchingSymbols(effective.symbol),
                acquire_turn=attempt.acquire_turn, normalizer=fib.normalize_search)
        except PacingViolation:
            fib.pacer().saturate()
            raise
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception as exc:  # noqa: BLE001
            raise ConnectionError(f"symbol search failed: {exc}") from exc

    def head_timestamp(self, contract, what_to_show="TRADES"):
        attempt = fib.take("ibkr-head", contract, what_to_show=what_to_show,
                           use_rth=fib.without_authority(getattr)(self, "use_rth"))
        contract = deepcopy(contract)  # Observers cannot retarget an authorized request.
        try:
            metadata = fib.bind_request(self, attempt.request).execute(
                lambda effective: self._await(
                    self.ib.reqHeadTimeStampAsync(contract, whatToShow=effective.what_to_show,
                                                  useRTH=effective.use_rth, formatDate=2),
                    HEAD_TIMEOUT_S, f"the {contract.symbol} head timestamp"),
                acquire_turn=attempt.acquire_turn, normalizer=fib.normalize_head)
        except PacingViolation:
            fib.pacer().saturate()
            raise
        return datetime.fromisoformat(metadata["timestamp"])

    def fetch(self, contract, end_dt_ny_naive, duration, bar_size,
              what_to_show="TRADES"):
        """One reqHistoricalData call. [] is a legitimate answer
        (holiday); permission errors halt; disconnects raise
        ConnectionError for the retry layer. `what_to_show` selects the data
        KIND (TRADES / OPTION_IMPLIED_VOLATILITY / HISTORICAL_VOLATILITY / BID_ASK)."""
        attempt = fib.take_bars(contract, raw_end=fib.ny_bound(end_dt_ny_naive),
                           duration=duration, bar_size=bar_size,
                           what_to_show=what_to_show,
                           use_rth=fib.without_authority(getattr)(self, "use_rth"))
        contract = deepcopy(contract)
        def transport(effective):
            self.errors.clear()
            try:
                bars = self._await(
                    self.ib.reqHistoricalDataAsync(
                        contract, endDateTime=effective.raw_end, durationStr=effective.duration,
                        barSizeSetting=effective.bar_size, whatToShow=effective.what_to_show,
                        useRTH=effective.use_rth, formatDate=2),
                    FETCH_TIMEOUT_S, f"{contract.symbol} {effective.duration} bars")
            except (AuthorityError, LedgerError, Cancelled, RequestTimeout, PacingViolation):
                raise
            except ConnectionError as exc:
                raise ConnectionLost(f"link error mid-request: {fib.without_authority(format)(exc)}")
            except Exception as exc:  # noqa: BLE001
                raise ConnectionError(f"request failed: {fib.without_authority(format)(exc)}")
            if not self.is_connected():
                raise ConnectionLost("TWS connection dropped mid-request")
            no_data = False
            for code, msg in self.errors:
                # IB's errorEvent also carries system/farm notifications.
                # They are not historical-request failures; the connection
                # check above and the bounded transport still govern delivery.
                if 1100 <= code <= 1102 or 2100 <= code <= 2199:
                    continue
                if code in (354, 10089, 10168):
                    raise SeriesHalt(f"no market data permission for {contract.symbol}: {msg}")
                if code == 200:
                    raise SeriesHalt(f"contract rejected: {msg}")
                if code == 420 or "pacing" in msg.lower():
                    raise PacingViolation(f"pacing violation: {msg}")
                if code == 162 and not bars and re.search(r"\bHMDS query returned no data\b", msg, re.IGNORECASE):
                    no_data = True
                    continue
                # An error is not evidence that this session has no bars.
                # Diagnostic suppression does not authorize success either.
                raise SeriesHalt(f"IB historical request error {code}: {msg}")
            return [] if no_data else bars
        @fib.without_authority
        def normalize_response(response):
            token = attempt.request.envelope.token
            if attempt.request.producer_id == "ibkr.vol_value_bank.ratio_day":
                # Preserve the ratio consumer's tight one-session bound BEFORE
                # normalization/materialization, including an endless provider.
                from itertools import islice
                from vol_value_bank import _raw_response_limit, RatioDayError
                limit = _raw_response_limit(token)
                response = list(islice(iter(response or ()), limit + 1))
                if len(response) > limit:
                    raise RatioDayError("ratio-day source response is unreasonably large", request_count=1)
            return fib.normalize_bars(response, token)
        rows = fib.bind_request(self, attempt.request).execute(transport, acquire_turn=attempt.acquire_turn,
            normalizer=normalize_response)
        return fib.publish_bars(rows, attempt.request.envelope.token)

    def disconnect(self):
        try:
            if self.ib is not None:
                self.ib.disconnect()
        except Exception:  # noqa: BLE001
            pass
        self.ib = None
        self._release_operation_gate()

    def _release_operation_gate(self):
        lease, self._operation_lease = self._operation_lease, None
        if lease is not None:
            lease.release()


def live_adapter_factory(host=HOST_DEFAULT, ports=PORTS_DEFAULT,
                         client_id=CLIENT_ID_FETCH, selected_account=None):
    def make():
        return LiveIB(host, ports, client_id=client_id, selected_account=selected_account).connect()
    return make


class ReusableAdapter:
    """Keep ONE live TWS connection alive across a SEQUENCE of GUI lookups
    (find_symbol / estimate_backfill) instead of reconnecting on every call —
    the connect handshake costs ~5-10s, so a dialog doing several searches pays
    it once instead of once-per-search.

    Pass `ReusableAdapter(make)` (where `make` is a live_adapter_factory result)
    as the `adapter_factory` — but note the engine's per-call `finally:
    disconnect()` would normally tear the link down, so this wrapper's
    `disconnect()` is a NO-OP; the shared connection survives. The owner calls
    `close()` to really disconnect (e.g. when the dialog closes). A dropped link
    is transparently re-established on the next use.

    Use it as the factory itself: `find_symbol(text, adapter_factory=reuser)`
    — calling the instance returns the instance (it is its own factory)."""

    def __init__(self, make):
        self._make = make
        self._a = None

    def __call__(self):
        return self                      # so it works as the adapter_factory

    def _live(self):
        a = self._a
        try:
            if a is not None and fib.without_authority(lambda: a.is_connected())():
                return a
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception:  # noqa: BLE001
            pass
        if a is not None:                # drop the dead/stale link FIRST — a
            try:                         # same-clientId reconnect while the old
                fib.without_authority(lambda: a.disconnect())()  # release old clientId
            except (AuthorityError, LedgerError, Cancelled):
                raise
            except Exception:  # noqa: BLE001 (err 326), and it'd leak otherwise
                pass
        self._a = fib.without_authority(self._make)()  # lazy factory is still an observer
        return self._a

    def disconnect(self):
        pass                             # NO-OP — keep the shared connection

    def __setattr__(self, name, value):
        # writes to real adapter attrs (e.g. use_rth) must reach the LIVE
        # connection, not land on the wrapper; the wrapper's own _make/_a stay
        # local. (__getattr__ already delegates reads.)
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._live(), name, value)

    def close(self):
        """Really disconnect the shared connection (call on dialog close)."""
        a, self._a = self._a, None
        if a is not None:
            try:
                a.disconnect()
            except Exception:  # noqa: BLE001
                pass

    def __getattr__(self, name):
        # delegate every real adapter method (search / qualify_many /
        # head_timestamp / …) to the live connection; never recurse on the
        # wrapper's own private attrs.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._live(), name)


# --- gates ------------------------------------------------------------------------

def _iso_day(d):
    """date/datetime/'YYYY-MM-DD' -> 'YYYY-MM-DD'. ISO day strings
    sort exactly like the dates they name — comparisons stay explicit
    whatever mix of types the callers hold."""
    if isinstance(d, datetime):              # datetime IS a date: first
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def _actions_between(actions, after_date, upto_date, applies):
    """Recorded actions with after_date < date <= upto_date. An action
    date is the FIRST session on the NEW basis (stock_basis schema),
    so a boundary at day D sits between D-1's close and D's open:
    lower bound exclusive, upper inclusive."""
    lo, hi = _iso_day(after_date), _iso_day(upto_date)
    return [a for a in (actions or [])
            if a.get("applies") in applies
            and lo < str(a.get("date", "")) <= hi]


def boundary_factor(actions, after_date, upto_date,
                    applies=("price", "both")):
    """PRODUCT of recorded factors crossing (after_date, upto_date]
    (NEW = OLD * factor at each boundary). 1.0 when nothing matches —
    the gates then behave exactly as they always did."""
    f = 1.0
    for a in _actions_between(actions, after_date, upto_date, applies):
        try:
            f *= float(a.get("factor", 1.0))
        except (TypeError, ValueError):
            pass        # hand-mangled manifest entry: treat as absent
    return f            # — the gates stay protective, never looser


def join_gate(prev_close, next_open, where, expected_factor=1.0):
    """expected_factor is the recorded action product across this
    boundary: the measured ratio is checked AGAINST it (1.0 = none)."""
    ratio = next_open / prev_close / expected_factor
    if not (JOIN_RATIO_LOW <= ratio <= JOIN_RATIO_HIGH):
        return (f"JOIN GATE at {where}: close->open ratio {ratio:.3f} "
                f"outside [{JOIN_RATIO_LOW}, {JOIN_RATIO_HIGH}] — "
                f"suspected split/corporate action. The series was "
                f"committed only UP TO the session before the jump; "
                f"run the basis doctor (stock_basis) to classify and "
                f"record this boundary, then re-run.")
    return None


def _interval_seconds(tok):
    return int(tok[:-1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[tok[-1]]


def _expected_session_bars(interval):
    secs = _session_spec(interval)[4]          # session length (rth/pre/post)
    return max(1, secs // _interval_seconds(ss.base_interval(interval)))


def entry_gate(existing_bars, fetched_bars, interval, stored_last_close,
               price_factor=1.0):
    """POSITIVE verification before the FIRST commit of a non-empty
    series. The adversarial review proved two silent bypasses of the
    old overlap gate: a thin last session (<30 shared bars — which also
    made the gate structurally DEAD for 15m/30m/1h, whose full sessions
    are under 30 bars) and an empty refetch day (the next, zero-overlap
    day was mistaken for the overlap session). The rule now: enough
    shared bars to compare prices (scaled per interval), or an explicit
    close->open check against the ARCHIVE's own last close — absence of
    overlap is never a pass. Returns (halt_reason|None, note|None).
    price_factor: recorded post-action basis — fetched bars are
    expected at existing * price_factor (1.0 = the archive's basis)."""
    required = max(5, min(GATE_MIN_OVERLAP,
                          _expected_session_bars(interval) // 2))
    emap = {b[0]: b for b in existing_bars}
    pairs = [(emap[b[0]], b) for b in fetched_bars if b[0] in emap]
    if len(pairs) >= required:
        bad = 0
        for e, b in pairs:
            for i in (1, 2, 3, 4):
                exp = e[i] * price_factor
                if abs(b[i] - exp) / exp > GATE_PRICE_TOL:
                    bad += 1
                    break
        frac = bad / len(pairs)
        if frac > GATE_DISAGREE_FRAC:
            return (f"OVERLAP GATE: {bad:,} of {len(pairs):,} "
                    f"overlapping bars ({frac:.0%}) disagree with the "
                    f"stored series on PRICE (>{GATE_PRICE_TOL:.1%}) — "
                    f"different adjustment basis or corporate action "
                    f"since the archive was written. Nothing written; "
                    f"run the basis doctor (stock_basis) to classify "
                    f"and record this boundary, then re-run."), None
        return None, None
    reason = join_gate(stored_last_close * price_factor,
                       fetched_bars[0][1],
                       where=f"the archive boundary "
                             f"({fetched_bars[0][0].date()})")
    if reason:
        return (f"ENTRY GATE (overlap too thin: {len(pairs)} shared "
                f"bar(s), {required} required): {reason}"), None
    return None, (f"overlap gate THIN ({len(pairs)} shared bar(s), "
                  f"{required} required) — basis checked only by the "
                  f"close->open ratio at the archive boundary; treat "
                  f"with care until task #23 settles the basis")


def calibrate_volume(existing_bars, fetched_bars):
    """Vendor-vs-IBKR volume unit check on the overlap session.
    -> (multiplier, note). Lots show up as a ~100x vendor/IBKR ratio;
    NBBO filtering only explains ~1-2x."""
    emap = {b[0]: b[5] for b in existing_bars}
    pairs = [(emap[b[0]], b[5]) for b in fetched_bars
             if b[0] in emap and emap[b[0]] > 0 and b[5] > 0]
    if len(pairs) < 10:
        return 1, "volume units unverified (no usable overlap)"
    vendor = sum(p[0] for p in pairs)
    ibkr = sum(p[1] for p in pairs)
    if ibkr <= 0:
        return 1, "volume units unverified (zero IBKR volume)"
    ratio = vendor / ibkr
    if VOLUME_LOTS_RATIO[0] <= ratio <= VOLUME_LOTS_RATIO[1]:
        return 100, (f"IBKR volume arrived in LOTS (vendor/IBKR ratio "
                     f"{ratio:.1f}) — multiplied by 100 to shares")
    return 1, (f"volume units: shares (vendor/IBKR ratio {ratio:.2f}; "
               f"IBKR is NBBO-filtered, expect stored vendor volume "
               f"to win conflicts on the overlap session)")


# --- per-series fill ---------------------------------------------------------------

_DATE_SPLIT_OWNER_STATE_KEYS = frozenset({
    "backfill_incomplete",
    "backfill_incomplete_reason",
    "under_backfilled",
    "backfill_seal",
    "backfill_served_earliest",
})

_VOL_MONTH_KEY_RE = re.compile(r"^[12]\d{3}-(?:0[1-9]|1[0-2])$")
_MANIFEST_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _manifest_entry_sha(entry):
    if not isinstance(entry, dict):
        return None
    value = entry.get("sha256")
    if isinstance(value, str) and _MANIFEST_SHA256_RE.fullmatch(value):
        return value
    return None


def _active_manifest_month_sha(tdir, interval, key):
    """Hash the exact active month named by one manifest entry."""
    if not isinstance(key, str) or not _VOL_MONTH_KEY_RE.fullmatch(key):
        raise ss.StorageError(
            f"{Path(tdir).name} {interval}: malformed manifest month {key!r}")
    year, month = int(key[:4]), int(key[5:7])
    path = ss.find_month_file(
        Path(tdir).parent, Path(tdir).name, year, month, interval)
    return ss._sha256_of_file(path) if path is not None else None


def _resolve_manifest_month_for_save(tdir, interval, key, fresh, mine):
    """Resolve two records for one month without guessing across byte sets."""
    if not isinstance(fresh, dict) or not isinstance(mine, dict):
        raise ss.StorageError(
            f"{Path(tdir).name} {interval} {key}: malformed month metadata")
    if fresh == mine:
        # Exact equality needs no general file read, which keeps long backfill
        # checkpoints cheap.  Correction-bearing months are the exception:
        # never republish their ledger when its recorded bytes are no longer
        # active, even if both in-memory records share the same stale view.
        if "value_corrections" in fresh:
            shared_sha = _manifest_entry_sha(fresh)
            if (shared_sha is None
                    or _active_manifest_month_sha(tdir, interval, key)
                    != shared_sha):
                raise ss.StorageError(
                    f"{Path(tdir).name} {interval} {key}: correction "
                    "metadata does not match the active month")
        return deepcopy(mine)
    fresh_sha = _manifest_entry_sha(fresh)
    mine_sha = _manifest_entry_sha(mine)
    if fresh_sha is not None and fresh_sha == mine_sha:
        active_sha = _active_manifest_month_sha(tdir, interval, key)
        if active_sha != fresh_sha:
            raise ss.StorageError(
                f"{Path(tdir).name} {interval} {key}: shared manifest "
                "SHA-256 does not match the active month")
        merged = deepcopy(mine)
        # The published ledger is the current durable record.  It may be
        # copied only when both month entries identify the same bytes.
        if "value_corrections" in fresh:
            merged["value_corrections"] = deepcopy(
                fresh["value_corrections"])
        return merged
    if fresh_sha is None and mine_sha is None:
        raise ss.StorageError(
            f"{Path(tdir).name} {interval} {key}: differing month metadata "
            "has no comparable SHA-256")
    active_sha = _active_manifest_month_sha(tdir, interval, key)
    if active_sha is not None and active_sha == fresh_sha:
        return deepcopy(fresh)
    if active_sha is not None and active_sha == mine_sha:
        return deepcopy(mine)
    raise ss.StorageError(
        f"{Path(tdir).name} {interval} {key}: active month SHA-256 matches "
        "neither manifest record")


def _merge_manifest_months_for_save(tdir, interval, fresh, mine):
    if not isinstance(fresh, dict) or not isinstance(mine, dict):
        raise ss.StorageError(
            f"{Path(tdir).name} {interval}: malformed interval metadata")
    fresh_months = fresh.get("months")
    mine_months = mine.get("months")
    if not isinstance(fresh_months, dict) or not isinstance(mine_months, dict):
        raise ss.StorageError(
            f"{Path(tdir).name} {interval}: malformed manifest months")
    merged = deepcopy(fresh_months)
    for key, mine_entry in mine_months.items():
        if key in fresh_months:
            merged[key] = _resolve_manifest_month_for_save(
                tdir, interval, key, fresh_months[key], mine_entry)
        else:
            if not isinstance(mine_entry, dict):
                raise ss.StorageError(
                    f"{Path(tdir).name} {interval} {key}: malformed month "
                    "metadata")
            merged[key] = deepcopy(mine_entry)
    return merged


def _merge_manifest_interval_for_save(tdir, interval, fresh, mine, *,
                                      date_split=False,
                                      interval_state_owner=False):
    """Apply existing interval/date-split ownership plus SHA-safe months."""
    months = _merge_manifest_months_for_save(
        tdir, interval, fresh, mine)
    if not date_split:
        merged = deepcopy(mine)
        merged["months"] = months
        return merged

    merged = deepcopy(fresh)
    merged["months"] = months
    va = sorted(set(merged.get("verified_absent", []))
                | set(mine.get("verified_absent", [])))
    if va:
        merged["verified_absent"] = va
    fresh_evidence = merged.get("verified_absent_evidence", {})
    mine_evidence = mine.get("verified_absent_evidence", {})
    evidence = (deepcopy(fresh_evidence)
                if isinstance(fresh_evidence, dict) else {})
    if isinstance(mine_evidence, dict):
        evidence.update(deepcopy(mine_evidence))
    evidence = {day: value for day, value in evidence.items() if day in va}
    if evidence:
        merged["verified_absent_evidence"] = evidence
    else:
        merged.pop("verified_absent_evidence", None)
    if interval_state_owner:
        for key in tuple(merged):
            if key not in ("months", "verified_absent",
                           "verified_absent_evidence") and key not in mine:
                del merged[key]
    for key, value in mine.items():
        if key in ("months", "verified_absent",
                   "verified_absent_evidence"):
            continue
        if not interval_state_owner and key in _DATE_SPLIT_OWNER_STATE_KEYS:
            continue
        if interval_state_owner or key not in merged:
            merged[key] = deepcopy(value)
    return merged


def _conid_repin_cas(fresh, intent):
    """Validate an activated repin against the freshly loaded manifest."""
    if intent is None:
        return None
    if not isinstance(intent, _ConIdRepinIntent):
        raise _ConIdRepinConflict(
            "conId repin identity conflict: invalid internal intent")
    if not intent.activated:
        return None
    if not isinstance(fresh, dict):
        raise _ConIdRepinConflict(
            "conId repin identity conflict: durable manifest is missing")
    marker = object()
    durable = fresh.get("conid", marker)
    if durable is marker:
        raise _ConIdRepinConflict(
            "conId repin identity conflict: durable pin is missing")
    if isinstance(durable, bool) or not isinstance(durable, int) \
            or durable <= 0:
        raise _ConIdRepinConflict(
            "conId repin identity conflict: durable pin is malformed")
    if durable not in (intent.expected_old, intent.accepted_new):
        raise _ConIdRepinConflict(
            f"conId repin identity conflict: durable pin {durable} is neither "
            f"expected {intent.expected_old} nor accepted "
            f"{intent.accepted_new}")
    return intent.accepted_new


def _manifest_publication_candidate(tdir, manifest, interval=None, *,
                                    date_split=False,
                                    interval_state_owner=False,
                                    repin_intent=None):
    if not isinstance(manifest, dict):
        raise ss.StorageError("manifest is not an object")
    mine_intervals = manifest.get("intervals")
    if not isinstance(mine_intervals, dict):
        raise ss.StorageError("manifest intervals is not an object")
    fresh = ss.load_manifest(tdir)
    if fresh is None:
        _conid_repin_cas(fresh, repin_intent)
        return deepcopy(manifest)

    if interval is not None:
        candidate = deepcopy(fresh)
        candidate.setdefault("intervals", {})
        mine = mine_intervals.get(interval)
        if mine is not None:
            fresh_section = candidate["intervals"].get(interval)
            if fresh_section is None:
                candidate["intervals"][interval] = deepcopy(mine)
            else:
                candidate["intervals"][interval] = (
                    _merge_manifest_interval_for_save(
                        tdir, interval, fresh_section, mine,
                        date_split=date_split,
                        interval_state_owner=interval_state_owner))
        for key in ("conid", "name"):
            if manifest.get(key) is not None and not candidate.get(key):
                candidate[key] = manifest[key]
        accepted_conid = _conid_repin_cas(fresh, repin_intent)
        if accepted_conid is not None:
            candidate["conid"] = accepted_conid
        return candidate

    # Keep the unscoped path safe for future/internal reuse: fresh-only
    # sections survive and overlapping months receive the same SHA CAS.
    candidate = deepcopy(manifest)
    candidate_intervals = candidate.setdefault("intervals", {})
    for token, fresh_section in (fresh.get("intervals") or {}).items():
        mine = candidate_intervals.get(token)
        if mine is None:
            candidate_intervals[token] = deepcopy(fresh_section)
        else:
            candidate_intervals[token] = _merge_manifest_interval_for_save(
                tdir, token, fresh_section, mine)
    for key in ("conid", "name"):
        if fresh.get(key) is not None and not candidate.get(key):
            candidate[key] = fresh[key]
    accepted_conid = _conid_repin_cas(fresh, repin_intent)
    if accepted_conid is not None:
        candidate["conid"] = accepted_conid
        if "name" in fresh:
            candidate["name"] = deepcopy(fresh["name"])
    return candidate


def _save_manifest_safely(tdir, manifest, res, interval=None, lock=None,
                          date_split=False, interval_state_owner=False,
                          _manifest_lock_held=False, repin_intent=None):
    """Persist the manifest; a failure is non-fatal (the next scan rebuilds it
    from the tree, the source of truth).

    Every publication takes the cross-process ticker transaction.  When the
    caller supplies the in-process manifest lock, that lock is taken first to
    match the correction writer's global lock order and prevent deadlock.

    INTERVAL-SPLIT mode (lock + interval given): sibling series of the SAME
    ticker may run on OTHER ports/threads, each owning a DIFFERENT interval
    section ('1m' vs '1m-pre' vs '1m-post'). They share one manifest file, so
    the save is serialised on the per-ticker `lock` and MERGES only THIS
    interval's section into the freshest on-disk manifest — never clobbering a
    sibling worker's concurrently-written section.

    DATE-SPLIT mode (date_split=True) is narrower: sibling workers own
    DISJOINT months of the SAME interval. Replacing the interval section would
    lose the other chunk's months, so this mode unions month entries under the
    same per-ticker lock. Only the earliest chunk owns mutable interval-level
    state such as the backward-seal marker; non-owner chunks preserve the
    freshest state already on disk.  In every mode overlapping month records
    are resolved against the active file SHA; same-SHA correction evidence is
    never erased by a stale in-memory manifest."""
    tdir = Path(tdir)
    stage = tdir / ss.VOL_VALUE_RECONCILE_STAGE_DIR

    def _save_locked():
        if stage.exists() or stage.is_symlink():
            raise ss.StorageError(
                "volatility correction recovery is pending; manifest save "
                "refused")
        candidate = _manifest_publication_candidate(
            tdir, manifest, interval,
            date_split=date_split,
            interval_state_owner=interval_state_owner,
            repin_intent=repin_intent)
        # An out-of-contract/manual marker creation must not slip between the
        # fresh read and publication even though the correction writer itself
        # uses this same ticker transaction.
        if stage.exists() or stage.is_symlink():
            raise ss.StorageError(
                "volatility correction recovery became active; manifest "
                "save refused")
        ss.save_manifest(tdir, candidate)

    try:
        def _transaction():
            with ss.ticker_transaction(tdir):
                _save_locked()

        # vol_value_bank.replace_ratio_day uses this same manifest lock before
        # ticker_transaction.  Keep one global order so a fill publication and
        # correction can never wait on each other's opposite lock.
        if (lock is not None and interval is not None
                and not _manifest_lock_held):
            with lock:
                _transaction()
        else:
            _transaction()
        return True
    except (ss.StorageError, OSError) as exc:
        res.setdefault("notes", []).append(
            f"manifest not saved ({exc}) — the next scan rebuilds it")
        if isinstance(exc, _ConIdRepinConflict):
            res["_conid_repin_conflict"] = str(exc)
        return False


def _manifest_present_months_readonly(manifest, interval, before):
    """Return exact present month keys without materializing manifest state."""
    if manifest is None:
        return set()
    if not isinstance(manifest, dict):
        raise ss.StorageError("manifest is not an object")
    intervals = manifest.get("intervals", {})
    if not isinstance(intervals, dict):
        raise ss.StorageError("manifest intervals is not an object")
    section = intervals.get(interval, {})
    if not isinstance(section, dict):
        raise ss.StorageError(f"manifest interval {interval!r} is malformed")
    months = section.get("months", {})
    if not isinstance(months, dict):
        raise ss.StorageError(f"manifest months for {interval!r} is malformed")
    out = set()
    for key, record in months.items():
        if (not isinstance(key, str) or not _VOL_MONTH_KEY_RE.match(key)
                or not isinstance(record, dict)
                or record.get("status") != "present"):
            continue
        candidate = (int(key[:4]), int(key[5:7]))
        if candidate < before:
            out.add(candidate)
    return out


def _date_split_series_safe(series):
    """Whether month chunks may commit independently for these series.

    Row 51's volatility unit gate is intentionally chronological: month N is
    compared with the greatest committed month before N.  Splitting the same
    ratio series across workers would make that baseline depend on scheduling,
    so ratio-bearing ticker jobs stay single-owner.  Price-only jobs retain the
    existing date-split acceleration unchanged.
    """
    return not any(ss.kind_of(interval) in ss.RATIO_KINDS
                   for _ticker, interval in series)


def _latest_tree_month(root, ticker, interval, before, after=None):
    """Greatest existing exact-series month in ``(after, before)``.

    The manifest is normally current and makes this scan tiny.  The bounded
    year/month fallback matters after a crash because month files are the
    authority and the manifest may legitimately lag them.
    """
    tdir = Path(root) / ticker
    if not tdir.is_dir():
        return None
    try:
        years = sorted(
            (int(p.name) for p in tdir.iterdir()
             if p.is_dir() and re.fullmatch(r"[12]\d{3}", p.name)),
            reverse=True)
    except OSError as exc:
        raise ss.StorageError(
            f"cannot inspect prior volatility months ({exc})") from exc
    for year in years:
        if year > before[0]:
            continue
        for month in range(12, 0, -1):
            candidate = (year, month)
            if candidate >= before or (after is not None and candidate <= after):
                continue
            if ss.find_month_file(root, ticker, year, month, interval) is not None:
                return candidate
    return None


def _ratio_ohlc_median(bars):
    values = [float(value) for bar in bars for value in bar[1:5]]
    return float(statistics.median(values)) if values else None


def _guard_ratio_month_commit(root, ticker, interval, ym, added, res, mstate):
    """Fail closed on a percent/decimal unit flip before any target write."""
    if (not VOL_VALUE_GATE or ss.kind_of(interval) not in ss.RATIO_KINDS
            or not added):
        return
    key = ss.month_key(*ym)

    def halt(reason, details=None):
        record = {"status": "HALTED", "reason": reason}
        if details:
            record.update(details)
        res.setdefault("months", {})[key] = record
        raise SeriesHalt(reason, metadata={"vol_value_gate": dict(record)})

    manifest = (mstate.get("manifest")
                if isinstance(mstate, dict) else None)
    if manifest is None:
        try:
            manifest = ss.load_manifest(Path(root) / ticker)
        except (ss.StorageError, OSError) as exc:
            halt(f"volatility unit gate could not read the manifest before "
                 f"{key} ({exc}) — month left untouched")
    try:
        claimed = _manifest_present_months_readonly(manifest, interval, ym)
        manifest_prior = max(claimed) if claimed else None
        tree_prior = _latest_tree_month(
            root, ticker, interval, ym, after=manifest_prior)
    except (ss.StorageError, OSError) as exc:
        halt(f"volatility unit gate could not establish the prior committed "
             f"month before {key} ({exc}) — month left untouched")
    prior = tree_prior or manifest_prior
    if prior is None:
        return                              # fresh series: ceiling only
    prior_key = ss.month_key(*prior)
    prior_path = ss.find_month_file(root, ticker, *prior, interval)
    if prior_path is None:
        halt(f"volatility unit gate found committed prior month {prior_key} "
             f"but its file is missing — {key} left untouched",
             {"prior_month": prior_key})
    try:
        prior_bars, _ = ss.read_month_file(prior_path)
        prior_median = _ratio_ohlc_median(prior_bars)
    except (ss.StorageError, OSError, TypeError, ValueError) as exc:
        halt(f"volatility unit gate could not strictly read prior month "
             f"{prior_key} ({exc}) — {key} left untouched",
             {"prior_month": prior_key})
    if prior_median is None or prior_median <= 0.0:
        halt(f"volatility unit gate cannot compare {key}: prior committed "
             f"month {prior_key} has a non-positive OHLC median; month left "
             f"untouched",
             {"prior_month": prior_key,
              "prior_median": prior_median})
    incoming_median = _ratio_ohlc_median(added)
    if incoming_median is None:
        return
    threshold = UNIT_FLIP_RATIO * prior_median
    at_or_above_threshold = (
        incoming_median > threshold
        or math.isclose(incoming_median, threshold,
                        rel_tol=1e-12, abs_tol=1e-15)
    )
    if at_or_above_threshold:
        details = {
            "prior_month": prior_key,
            "prior_median": prior_median,
            "incoming_median": incoming_median,
            "ratio": UNIT_FLIP_RATIO,
        }
        halt(
            f"volatility unit-flip gate HALTED {key}: incoming OHLC median "
            f"{incoming_median:.12g} is at least {UNIT_FLIP_RATIO:g}x prior "
            f"committed month {prior_key} median {prior_median:.12g}; "
            f"month left untouched",
            details)


def _commit_month(root, ticker, interval, ym, bars, run_id, src_name,
                  res, conflict_sink, conid=None, mstate=None):
    """Lock-ordered wrapper for one ordinary month commit."""
    canon = ss.canonical_ticker(ticker)
    ticker_dir = Path(root) / canon
    manifest_lock = (mstate.get("lock")
                     if isinstance(mstate, dict) else None)

    def _transaction():
        with ss.ticker_transaction(ticker_dir):
            return _commit_month_locked(
                root, canon, interval, ym, bars, run_id, src_name,
                res, conflict_sink, conid=conid, mstate=mstate)

    if manifest_lock is None:
        return _transaction()
    with manifest_lock:
        return _transaction()


def _commit_month_locked(root, ticker, interval, ym, bars, run_id, src_name,
                         res, conflict_sink, conid=None, mstate=None):
    """Merge ONE month of fetched bars through the strict machinery —
    same rules as Tier 1: identical dups drop, conflicts keep existing,
    unreadable existing month blocks, manifest follows the file."""
    y, m = ym
    correction_stage = (Path(root) / ticker /
                        ss.VOL_VALUE_RECONCILE_STAGE_DIR)
    if correction_stage.exists() or correction_stage.is_symlink():
        res["blocked_months"].append({
            "month": ss.month_key(y, m),
            "reason": "volatility correction recovery is pending; month "
                      "left untouched",
        })
        return
    repin_intent = (mstate.get("repin_intent")
                    if isinstance(mstate, dict) else None)
    if repin_intent is not None:
        if not isinstance(repin_intent, _ConIdRepinIntent):
            raise SeriesHalt(
                "conId repin identity conflict: invalid internal intent")
        if not repin_intent.activated:
            raise SeriesHalt(
                "conId repin refused: positive continuity evidence was not "
                "established before the month merge")
        try:
            _conid_repin_cas(ss.load_manifest(Path(root) / ticker),
                             repin_intent)
        except _ConIdRepinConflict as exc:
            raise SeriesHalt(str(exc)) from exc
    path = ss.month_file_path(root, ticker, y, m, interval)   # parquet target
    read_path = ss.find_month_file(root, ticker, y, m, interval)   # any format
    existing = []
    existing_stats = None
    if read_path is not None:
        try:
            existing, existing_stats = ss.read_month_file(read_path)
        except (ss.StorageError, OSError) as exc:
            res["blocked_months"].append(
                {"month": ss.month_key(y, m),
                 "reason": f"existing file failed the strict read "
                           f"({exc}) — month left untouched"})
            return
    emap = {b[0]: b for b in existing}
    defer_conflicts = (VOL_VALUE_GATE
                       and ss.kind_of(interval) in ss.RATIO_KINDS)
    added, dups, confs = [], 0, 0
    conflicts = [] if defer_conflicts else None
    for b in bars:
        e = emap.get(b[0])
        if e is None:
            added.append(b)
        elif e == b:
            dups += 1
        else:
            confs += 1
            if defer_conflicts:
                conflicts.append((e, b))
            else:
                conflict_sink(ticker, interval, ss.month_key(y, m), e, b)
    key = ss.month_key(y, m)
    _guard_ratio_month_commit(
        root, ticker, interval, ym, added, res, mstate)
    if conflicts is not None:
        for existing_bar, incoming_bar in conflicts:
            conflict_sink(ticker, interval, key, existing_bar, incoming_bar)
    res["dup_existing"] += dups
    res["conflicts"] += confs
    if not added:
        res["months"][key] = {"status": "unchanged", "dups": dups,
                              "conflicts": confs}
        return
    tdir = Path(root) / ticker
    # A3: keep the manifest IN MEMORY across the series (mstate) and persist it
    # only every MANIFEST_CHECKPOINT_MONTHS commits — the caller saves it once
    # more at series end. A deep mass backfill thus does ~hundreds of manifest
    # writes, not ~60k (each re-serializing the growing manifest). The data
    # file is already on disk, so a crash before the next save just leaves the
    # manifest LAGGING the tree; the next scan heals the month index from the
    # files (the tree is source of truth) and the conId re-derives on requalify.
    if mstate is None:                      # standalone call -> save each time
        mstate = {"manifest": None, "since_save": MANIFEST_CHECKPOINT_MONTHS}
    manifest = mstate.get("manifest")
    if manifest is None:
        manifest = ss.load_manifest(tdir) or ss.new_manifest(ticker, ticker)
        mstate["manifest"] = manifest
    old_conid = manifest.get("conid")
    if conid is not None and old_conid is None:
        # A2: pin the conId on a brand-new IBKR-only series (new_manifest seeds
        # conid=None). A dead-contract replacement is published only by the
        # typed, evidence-activated CAS; it never uses this first-contact path.
        manifest["conid"] = int(conid)
    months = ss.manifest_months(manifest, interval)
    old = months.get(key, {})
    published = ss.load_manifest(tdir) or {}
    published_old = ((((published.get("intervals") or {}).get(interval) or {})
                      .get("months") or {}).get(key) or {})
    prior = old
    if (isinstance(existing_stats, dict)
            and isinstance(published_old, dict)
            and published_old.get("sha256") == existing_stats.get("sha256")):
        # ``mstate`` can span many checkpointed commits.  Re-read the current
        # published pre-write entry under the ticker transaction so a newer
        # same-byte correction ledger is inherited by this ordinary append.
        prior = published_old
    source = ingest._provenance(
        prior.get("source"), run_id, src_name, len(added), dups, confs)

    # A correction-bearing month is not an ordinary data-first/cache-later
    # write: if the process died between those two writes, a scanner could no
    # longer prove the ledger belonged to the new bytes and would correctly
    # drop it.  Force this rare case through the shared month+manifest WAL.
    published_has_ledger = (
        isinstance(published_old, dict)
        and "value_corrections" in published_old)
    if published_has_ledger:
        if (not isinstance(existing_stats, dict)
                or published_old.get("sha256") != existing_stats.get("sha256")):
            res["blocked_months"].append({
                "month": key,
                "reason": "correction metadata does not match the active "
                          "prewrite month; ordinary rewrite refused",
            })
            return
        try:
            candidate = _manifest_publication_candidate(
                tdir, manifest, interval,
                date_split=bool(mstate.get("date_split")),
                interval_state_owner=bool(
                    mstate.get("interval_state_owner")),
                repin_intent=repin_intent)
            stats, published_manifest = (
                ss.write_month_preserving_correction_ledger(
                    root, ticker, interval, y, m, existing + added,
                    candidate, {"status": "present", "source": source}))
        except (ss.StorageError, OSError) as exc:
            res["months"][key] = {
                "status": f"WRITE FAILED: {exc}; recovery may be required"}
            return
        mstate["manifest"] = published_manifest
        mstate["since_save"] = 0
        res["added"] += len(added)
        res["written"] += 1
        res["months"][key] = {
            "status": "written", "added": len(added),
            "dups": dups, "conflicts": confs}
        return

    try:
        stats = ss.write_month_file(path, existing + added)
    except (ss.StorageError, OSError) as exc:
        res["months"][key] = {"status": f"WRITE FAILED: {exc}"}
        return
    csv_twin = ss.month_file_path(root, ticker, y, m, interval, fmt="csv")
    if csv_twin != path and csv_twin.exists():
        try:                                   # migrate: drop the legacy CSV twin
            csv_twin.unlink()
        except OSError:
            pass
    res["added"] += len(added)
    res["written"] += 1
    res["months"][key] = {"status": "written", "added": len(added),
                          "dups": dups, "conflicts": confs}
    new_entry = dict(stats, status="present", source=source)
    months[key] = new_entry
    mstate["since_save"] = mstate.get("since_save", 0) + 1
    if mstate["since_save"] >= MANIFEST_CHECKPOINT_MONTHS:
        _save_manifest_safely(tdir, manifest, res, interval,
                              mstate.get("lock"),
                              date_split=bool(mstate.get("date_split")),
                              interval_state_owner=bool(
                                  mstate.get("interval_state_owner")),
                              _manifest_lock_held=bool(
                                  mstate.get("lock")),
                              repin_intent=repin_intent)
        mstate["since_save"] = 0


def _heal_manifest_months_from_tree(root, ticker, intervals, month_ranges,
                                    lock=None, res=None):
    """Lock-ordered wrapper for targeted date-split manifest healing."""
    root = Path(root)
    canon = ss.canonical_ticker(ticker)
    ticker_dir = root / canon

    def _transaction():
        with ss.ticker_transaction(ticker_dir):
            return _heal_manifest_months_from_tree_locked(
                root, canon, intervals, month_ranges, res=res)

    try:
        if lock is None:
            return _transaction()
        with lock:
            return _transaction()
    except (ss.StorageError, OSError) as exc:
        if res is not None:
            res.setdefault("notes", []).append(
                f"{canon}: date-split manifest heal skipped ({exc})")
        return 0


def _heal_manifest_months_from_tree_locked(
        root, ticker, intervals, month_ranges, res=None):
    """Targeted P5 heal for date-split: adopt committed month files into the
    manifest month index without scanning the whole bank."""
    root = Path(root)
    canon = ss.canonical_ticker(ticker)
    tdir = root / canon
    if not intervals or not tdir.exists():
        return 0
    correction_stage = tdir / ss.VOL_VALUE_RECONCILE_STAGE_DIR
    if correction_stage.exists() or correction_stage.is_symlink():
        if isinstance(res, dict):
            res.setdefault("blocked_months", []).append({
                "month": "manifest-heal",
                "reason": "volatility correction recovery is pending; "
                          "manifest heal refused",
            })
        return 0

    def _do():
        manifest = ss.load_manifest(tdir) or ss.new_manifest(canon, canon)
        changed = 0
        for iv in sorted(set(intervals)):
            keys = []
            for rng in month_ranges:
                keys.extend(_month_keys_for_range(rng))
            for key in sorted(set(keys)):
                try:
                    y, m = int(key[:4]), int(key[5:7])
                except (ValueError, IndexError):
                    continue
                fp = ss.find_month_file(root, canon, y, m, iv)
                if fp is None:
                    continue
                months = ss.manifest_months(manifest, iv)
                entry = months.get(key)
                try:
                    bars, stats = ss.read_month_file(fp)
                except (ss.StorageError, OSError) as exc:
                    if res is not None:
                        res.setdefault("notes", []).append(
                            f"{canon} {iv} {key}: date-split heal skipped "
                            f"unreadable month ({exc})")
                    continue
                new = {
                    "rows": stats["rows"],
                    "first": f"{ss.format_date(bars[0][0])} "
                             f"{ss.format_time(bars[0][0].time())}",
                    "last": f"{ss.format_date(bars[-1][0])} "
                            f"{ss.format_time(bars[-1][0].time())}",
                    "size": stats["size"],
                    "mtime_ns": stats["mtime_ns"],
                    "sha256": stats["sha256"],
                    "source": (entry or {}).get("source", "found-by-scan"),
                    "status": "present",
                }
                if (isinstance(entry, dict)
                        and entry.get("sha256") == new.get("sha256")
                        and "value_corrections" in entry):
                    new["value_corrections"] = deepcopy(
                        entry["value_corrections"])
                if entry != new:
                    months[key] = new
                    changed += 1
        if changed:
            if (correction_stage.exists()
                    or correction_stage.is_symlink()):
                raise ss.StorageError(
                    "volatility correction recovery became active; "
                    "manifest heal refused")
            ss.save_manifest(tdir, manifest)
        return changed

    try:
        return _do()
    except (ss.StorageError, OSError) as exc:
        if res is not None:
            res.setdefault("notes", []).append(
                f"{canon}: date-split manifest heal skipped ({exc})")
    return 0


class _CallableCancel:
    def __init__(self, callback):
        self._callback = callback

    def is_set(self):
        return bool(self._callback())


def _covered_month_requests(interval, first, last, duration, context):
    """Preserve ordinary spans; split unprovable carriers/windows before send."""
    if context is None:
        return [(first, last, duration)]
    from fetch_envelopes import carrier_start, encode_carrier
    authority = context.authority
    first, last = fib.ny_bound(first), fib.ny_bound(last)
    split = carrier_start(last, duration).date() < authority.first_date
    has_window = False
    day = first.date()
    while day <= last.date():
        try:
            if authority.window(interval, day) is not None:
                has_window = True
        except CalendarUnsupported:
            split = True
        day += timedelta(days=1)
    if not split:
        # A backwards weekly walk can end on a weekend-only fragment at
        # month start. It has no request to make, not a failed settled span.
        if not has_window:
            return []
        return [(first.replace(tzinfo=None), last.replace(tzinfo=None), duration)]
    planned = []
    horizon = context.horizons[interval]
    day = first.date()
    while day <= last.date():
        try:
            window = authority.window(interval, day)
        except CalendarUnsupported:
            window = None
        if window is not None and horizon is not None:
            opening, closing = max(first, window[0]), min(last, window[1], horizon)
            base = ss.base_interval(interval)
            if opening < closing and (base != "1d" or closing == window[1]):
                # Daily labels remain date-scoped in the result filter. An
                # exact first-session carrier need not reach before coverage.
                if _BAR_SIZES[base][1] is None:
                    pieces = [(closing.replace(tzinfo=None), encode_carrier(opening, closing))]
                else:
                    pieces = day_requests(interval, day, session_window=(opening, closing))
                cursor = opening
                for stop, _legacy_duration in pieces:
                    stop = fib.ny_bound(stop)
                    planned.append((cursor.replace(tzinfo=None), stop.replace(tzinfo=None),
                                    encode_carrier(cursor, stop)))
                    cursor = stop
        day += timedelta(days=1)
    return planned


def _fetch_month_bars(adapter, contract, year, month, interval, cancel=None):
    """Fetch + contract-clean ONE month of `interval` bars for the connected-pattern
    gap fill. Each span uses its canonical token's RTH flag; daily uses a span
    covering the month. Returns (bars, skipped_days): clean
    (dt,o,h,l,c,v) tuples for days IN (year, month) only, plus the set of dates whose
    span was skipped or extends beyond the captured horizon — those days are UNKNOWN, not
    absent, so the caller must not treat them as source-absent."""
    import calendar as _cal
    from types import SimpleNamespace
    last = _cal.monthrange(year, month)[1]
    base = ss.base_interval(interval)
    bar_size = _BAR_SIZES[base][0]
    wts = _what_to_show(interval)
    month_days = frozenset(date(year, month, d) for d in range(1, last + 1))
    counters = {"invalid": 0, "outside_day": 0, "non_rth": 0}
    prev_rth = fib.without_authority(getattr)(adapter, "use_rth", True)
    raw = []
    skipped = set()           # dates whose 1-W span was transiently skipped
    worker = fib.current_worker()
    context = worker.context if worker is not None else None
    try:
        if base == "1d":
            # A borrowed adapter may still carry useRTH=False from an earlier
            # extended-hours request. Daily gap fills are interval-scoped too;
            # set the requested session explicitly, then restore it below.
            fib.without_authority(setattr)(adapter, "use_rth", _session_spec(interval)[0])
            end = datetime(year, month, last, 23, 59, 0)
            for first, end, duration in _covered_month_requests(
                    interval, datetime(year, month, 1), end, "2 M", context):
                if cancel is not None and cancel():
                    raise Cancelled("targeted daily month cancelled before request")
                request = fib.bar_request("ibkr.gap_fill.month_daily", contract, interval,
                                          end, duration, start=first)
                with fib.send_scope(request, lambda: fib.acquire_turn(
                        _CallableCancel(cancel) if cancel is not None else None),
                        cancel=_CallableCancel(cancel) if cancel is not None else None):
                    sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
                    raw.extend(sender.fetch(contract, end, duration, bar_size, what_to_show=wts))
        else:
            # The demo serves a "1 M" intraday request TOO SLOWLY (~8k bars -> >60s
            # timeout). Walk the month in the engine's standard "1 W" spans instead
            # (fast + reliable), de-duping the overlap by bar timestamp.
            fib.without_authority(setattr)(adapter, "use_rth", _session_spec(interval)[0])
            cur = datetime(year, month, last, 20, 0, 0)
            start = datetime(year, month, 1, 0, 0, 0)
            horizon = context.horizons[interval] if context is not None else None
            if horizon is not None and horizon.replace(tzinfo=None) > start:
                # A current-month walk starts at its captured horizon, not a
                # wholly future last week that would abort the settled prefix.
                cur = min(cur, datetime.combine(horizon.date(), time(20)))
            planned = []
            while cur >= start:
                first = max(start, datetime.combine((cur - timedelta(days=6)).date(), time.min))
                planned.extend((first, stop, duration, cur.date())
                    for first, stop, duration in _covered_month_requests(
                        interval, first, cur, "1 W", context))
                cur -= timedelta(days=7)
            seen = set()
            for first, cur, duration, unvisited_through in planned:
                if cancel is not None and cancel():    # budget spent / Resume:
                    skipped |= {d for d in month_days if d <= unvisited_through}
                    break                              # abort the in-month walk
                chunk = []
                errored = False
                request = fib.bar_request("ibkr.gap_fill.month_intraday", contract,
                    interval, cur, duration, start=first)
                for attempt in range(3):
                    if cancel is not None and cancel():   # don't start another
                        errored = True                    # ~60s fetch after a stop
                        break
                    try:
                        with fib.send_scope(request, lambda: fib.acquire_turn(
                                _CallableCancel(cancel) if cancel is not None else None,
                                metered=_BAR_SIZES[base][1] is not None),
                                cancel=_CallableCancel(cancel) if cancel is not None else None):
                            fib.without_authority(setattr)(adapter, "use_rth", _session_spec(interval)[0])
                            sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
                            chunk = sender.fetch(contract, cur, duration, bar_size,
                                                  what_to_show=wts)
                        break
                    except PacingViolation:  # rolling-window limit -> BACK OFF + retry
                        fib.pacer().saturate()
                        if attempt == 2:
                            chunk = []
                            errored = True
                    except ConnectionLost:   # a GENUINE drop: reconnect; if it never
                        if attempt == 2:     # recovers, RAISE so fill_missing_days logs
                            raise            # a blocked month (no false "sealed/lossless")
                        try:
                            fib.without_authority(lambda: adapter.reconnect())()
                        except ConnectionError:
                            pass
                    except RequestTimeout:   # slow/busy TWS, link up -> skip THIS week
                        if attempt == 2:     # only (caller re-scans), don't poison month
                            chunk = []
                            errored = True
                    except ConnectionError:  # any other link error -> skip the week
                        if attempt == 2:
                            chunk = []
                            errored = True
                if errored:                  # this span never reached the source: its
                    span_lo = first.date()                      # days are UNKNOWN, not
                    span_hi = cur.date()                         # absent -> record them
                    skipped |= {d for d in month_days            # so they can't poison
                                if span_lo <= d <= span_hi}       # source_absent
                for b in chunk:
                    if b.date not in seen:
                        seen.add(b.date)
                        raw.append(b)
    finally:
        fib.without_authority(setattr)(adapter, "use_rth", prev_rth)
    by_day = split_session_bars(raw, month_days, counters, interval)
    bars = [b for d in sorted(by_day) for b in by_day[d]]
    covered_days = set()
    for day in month_days:
        try:
            if context is not None:
                context.authority.window(interval, day)
            covered_days.add(day)
        except CalendarUnsupported:
            # A whole-month repair may surround an unrequested unsupported
            # date. It remains UNKNOWN, never evidence of source absence.
            skipped.add(day)
    skipped |= fib.unsettled_days(interval, covered_days)
    return bars, skipped


def _contract_conid_value(contract):
    if isinstance(contract, dict):
        value = contract.get("conId", contract.get("conid"))
    else:
        value = getattr(contract, "conId", None)
    if isinstance(value, bool):
        return None
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _empty_month_control_candidates(manifest, interval, empty_months,
                                    identity_floor=None):
    """At most two same-series present months, preferring both target sides."""
    if not isinstance(manifest, dict) or not empty_months:
        return []
    section = ((manifest.get("intervals") or {}).get(interval) or {})
    months = section.get("months") if isinstance(section, dict) else None
    if not isinstance(months, dict):
        return []
    targets = {int(year) * 12 + int(month) - 1
               for year, month in empty_months}
    candidates = []
    for key, entry in months.items():
        if (not isinstance(key, str) or not _VOL_MONTH_KEY_RE.match(key)
                or not isinstance(entry, dict)):
            continue
        rows = entry.get("rows")
        if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
            continue
        year, month = int(key[:4]), int(key[5:7])
        if (year, month) in empty_months:
            continue
        if (identity_floor is not None
                and (year, month) < (identity_floor.year,
                                     identity_floor.month)):
            continue
        ordinal = year * 12 + month - 1
        distance = min(abs(ordinal - target) for target in targets)
        candidates.append((distance, ordinal, year, month))
    if not candidates:
        return []

    candidates.sort()
    lower = [row for row in candidates if row[1] < min(targets)]
    upper = [row for row in candidates if row[1] > max(targets)]
    selected = []
    if lower:
        selected.append(min(lower))
    if upper:
        selected.append(min(upper))
    for row in candidates:
        if row not in selected:
            selected.append(row)
        if len(selected) >= EMPTY_MONTH_CONTROL_PROBE_CAP:
            break
    return [(year, month) for _distance, _ordinal, year, month
            in selected[:EMPTY_MONTH_CONTROL_PROBE_CAP]]


def _positive_empty_month_control(adapter, contract, manifest, interval,
                                  empty_months, *, identity_floor=None,
                                  cancel=None):
    """Return the first positively served control key, or ``None``.

    The request uses the exact contract already selected for the target series.
    A malformed/mismatched durable pin, an empty control, a skipped-only control,
    or any control exception is not evidence and therefore cannot promote.
    """
    pinned = (manifest or {}).get("conid") if isinstance(manifest, dict) else None
    if (isinstance(pinned, bool) or not isinstance(pinned, int) or pinned <= 0
            or _contract_conid_value(contract) != pinned):
        return None
    positive = None
    for year, month in _empty_month_control_candidates(
            manifest, interval, empty_months, identity_floor=identity_floor):
        if cancel is not None and cancel():
            raise Cancelled("targeted fill cancelled before control request")
        try:
            bars, _skipped = _fetch_month_bars(
                adapter, contract, year, month, interval, cancel=cancel)
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception:  # noqa: BLE001 - only positive evidence may promote
            continue
        if identity_floor is not None:
            bars = [bar for bar in bars if bar[0].date() >= identity_floor]
        if bars and positive is None:
            positive = ss.month_key(year, month)
    return positive


class TargetedNoRequest(RuntimeError):
    """Named zero-send calendar outcome, not an authority or integrity failure.

    The requested debt remains unresolved. Per-series callers may continue;
    this is never evidence of source absence or successful repair.
    """


def fill_missing_days(adapter, root, ticker, interval, missing_days,
                      contract=None, run_id="gap-fill", cancel=None,
                      progress=None, manifest_lock=None, *,
                      evidence_dir=None, _test_capability=None, _fetch_child=None,
                      _completion=None):
    options = {"contract": contract, "run_id": run_id, "cancel": cancel,
               "progress": progress, "manifest_lock": manifest_lock}
    args = (adapter, root, ticker, interval, missing_days)
    if _fetch_child is not None:
        if evidence_dir is not None or _test_capability is not None:
            return fib.refuse_transferred_child(
                _fetch_child, "root-only options cannot accompany a transferred child")
        if _completion is not None and (type(_completion) is not dict or _completion):
            return fib.refuse_transferred_child(
                _fetch_child, "targeted completion requires an empty plain dictionary")
        # Fixed engine dispatchers supply their own receipt. Never recover
        # trusted partial progress from a provider exception's attributes.
        completion = {} if _completion is None else _completion
        completion["state"] = "not_started"
        try:
            return _fill_missing_days_body(*args, **options,
                _completion=completion, _fetch_child=_fetch_child)
        except BaseException as exc:
            # Child cleanup has already run. Keep the original terminal type;
            # an injected exception's attribute hook is still an observer.
            try:
                fib.without_authority(setattr)(exc, "targeted_fill", deepcopy(completion))
            except Exception:
                pass
            raise
    if _completion is not None:
        raise RequestRefused("targeted completion requires a transferred child")
    operation = fops.begin_operation("repair", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "repair", args, options, evidence_dir)


@fib.worker_scope
@fib.symbol_scope
def _fill_missing_days_body(adapter, root, ticker, interval, missing_days,
                           contract=None, run_id="gap-fill", cancel=None,
                           progress=None, manifest_lock=None, *, _completion=None):
    """Targeted CONNECTED-PATTERN fill: re-fetch the months that contain
    `missing_days` (iso strings) for ONE series and MERGE via _commit_month — adds
    the missing bars, KEEPS existing on conflict; the ticker's 1d and other series
    are untouched. Does NOT re-judge: the caller re-scans to see which days remain
    (the unfetchable ones to flag). Returns {months_refetched, added, dup_existing,
    conflicts, written, blocked, source_absent, unfilled} — `source_absent` are days
    the source confirmed it LACKS (recorded in the manifest's verified_absent so they
    flag as 'data does not exist'); `unfilled` are days whose month didn't fetch (retry)."""
    canon = ss.canonical_ticker(ticker)
    days = sorted({date.fromisoformat(str(d)) for d in (missing_days or [])})
    completion = {} if _completion is None else _completion
    if type(completion) is not dict:
        raise RequestRefused("targeted completion requires engine-owned plain data")
    completion.update(state="started", ticker=canon, interval=interval,
        requested_days=[day.isoformat() for day in days],
        unresolved_days=[day.isoformat() for day in days],
        unresolved_semantics="not confirmed by this invocation; re-scan before retry",
        months={}, published_months=[], source_absent=[])
    if not days:
        completion["state"] = "returned"
        return {"months_refetched": 0, "added": 0, "dup_existing": 0,
                "conflicts": 0, "written": 0, "blocked": [],
                "source_absent": [], "unfilled": []}
    context = fib.current_worker().context
    base, _kind, _session = fib.parse_token(interval)
    horizon = context.horizons[interval]
    eligible, unsupported, closed, unsettled, partial = [], [], [], [], set()
    for day in days:
        try:
            window = context.authority.window(interval, day)
        except CalendarUnsupported:
            unsupported.append(day.isoformat())
            continue
        if window is None:
            closed.append(day.isoformat())
        elif horizon is None or (window[1] > horizon if base == "1d" else window[0] >= horizon):
            unsettled.append(day.isoformat())
        else:
            eligible.append(day)
            if window[1] > horizon:
                partial.add(day)
    completion.update(calendar_unsupported_days=unsupported, closed_days=closed,
                      unsettled_days=unsettled)
    if not eligible:
        completion["state"] = "no_request"
        if unsupported:
            raise TargetedNoRequest("targeted fill has no supported settled request day")
        if closed and not unsettled:
            raise TargetedNoRequest("targeted fill has only closed request days")
        raise TargetedNoRequest("targeted fill has no settled request day")
    days = eligible
    tdir = Path(root) / canon
    bman = ss.load_manifest(tdir)
    res = {"blocked_months": [], "dup_existing": 0, "conflicts": 0,
           "months": {}, "added": 0, "written": 0}
    identity_floor = _identity_floor_for_series(
        bman, canon, interval, res)
    before_floor = [day for day in days
                    if identity_floor is not None and day < identity_floor]
    if before_floor:
        message = (
            f"{canon} {interval}: targeted gap request starts before identity "
            f"listing floor {identity_floor} ({before_floor[0]}); refusing fill")
        res.setdefault("notes", []).append(message)
        raise SeriesHalt(message)
    if contract is None:
        with fib.qualification_scope("qualify", [ticker], fib.acquire_turn):
            _cid, contract = adapter.qualify(ticker)
    months = sorted({(d.year, d.month) for d in days})
    days_by_month = {}
    for d in days:
        days_by_month.setdefault((d.year, d.month), []).append(d)
    day_total = len(days)
    day_done = 0
    conid = (bman or {}).get("conid")
    mstate = {"manifest": bman, "lock": manifest_lock}
    fetched_days = set()                  # dates the source actually returned
    fetched_months = set()                # months that reached the source (>=1 bar)
    skipped_days = set()                  # days in a transiently-skipped week (UNKNOWN)
    clean_empty_months = set()            # zero bars with no skip/error ambiguity
    for (y, m) in months:
        if cancel is not None and cancel():
            raise Cancelled("targeted fill cancelled before month request")
        for d in days_by_month.get((y, m), []):
            day_done += 1
            if progress is not None:
                try:
                    progress(day_done, day_total, d.isoformat())
                except (AuthorityError, LedgerError, Cancelled):
                    raise
                except Exception:  # noqa: BLE001
                    pass
        key = ss.month_key(y, m)
        month_completion = {"stage": "request_started", "file_written": False,
                            "manifest_published": False, "added": 0}
        completion["months"][key] = month_completion
        try:
            bars, skipped = _fetch_month_bars(adapter, contract, y, m, interval,
                                              cancel=cancel)
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception as exc:  # noqa: BLE001 — a failed month is just left missing
            month_completion["stage"] = "request_failed"
            res["blocked_months"].append({"month": ss.month_key(y, m),
                                          "reason": f"{type(exc).__name__}: {exc}"})
            continue
        month_completion["stage"] = "response_returned"
        skipped_days |= skipped
        if identity_floor is not None:
            pre_floor_count = sum(
                1 for bar in bars if bar[0].date() < identity_floor)
            if pre_floor_count:
                res.setdefault("notes", []).append(
                    f"{canon} {interval}: discarded {pre_floor_count} "
                    f"month-response bar(s) before identity listing floor "
                    f"{identity_floor}")
                bars = [
                    bar for bar in bars
                    if bar[0].date() >= identity_floor
                ]
        if bars:
            written_before = res["written"]
            added_before = res["added"]
            month_completion["stage"] = "committing"
            _commit_month(root, canon, interval, (y, m), bars, run_id, "IBKR:fill",
                          res, (lambda *a: None), conid=conid, mstate=mstate)
            month_result = res["months"].get(key, {})
            month_completion["storage_result"] = deepcopy(month_result)
            month_completion["added"] = res["added"] - added_before
            if month_result.get("status") not in {"written", "unchanged"}:
                month_completion["stage"] = "storage_failed"
                if not any(item.get("month") == key for item in res["blocked_months"]):
                    res["blocked_months"].append({"month": key,
                        "reason": month_result.get("status", "month publication refused")})
                # Source receipt does not prove storage. In particular it
                # cannot promote another missing day to source-absent.
                continue
            if res["written"] > written_before:
                month_completion.update(stage="manifest_pending", file_written=True)
                # Targeted repairs can terminate at the next month boundary.
                # Publish each completed month now: the general backfill's
                # batched manifest save would leave preserved bytes invisible
                # to the normal SHA-gated reader after a terminal cancellation.
                if not _save_manifest_safely(tdir, mstate["manifest"], res,
                                              interval, mstate.get("lock")):
                    raise ss.StorageError("targeted fill month manifest publication failed")
                mstate["since_save"] = 0
                month_completion.update(stage="published", manifest_published=True)
                completion["published_months"].append(key)
                confirmed = {row[0].date().isoformat() for row in bars
                             if row[0].date() not in skipped}
                completion["unresolved_days"] = sorted(
                    set(completion["unresolved_days"]) - confirmed)
            else:
                month_completion["stage"] = "unchanged"
            fetched_months.add((y, m))
            fetched_days |= {b[0].date() for b in bars} - skipped
        elif not skipped:
            clean_empty_months.add((y, m))
    if cancel is not None and cancel():
        raise Cancelled("targeted fill cancelled after month boundary")
    control_month = _positive_empty_month_control(
        adapter, contract, bman, interval, clean_empty_months,
        identity_floor=identity_floor, cancel=cancel)
    # SOURCE-ABSENT: a requested day whose MONTH reached the source (returned bars) yet
    # the day never came back AND its week was NOT transiently skipped -> the source
    # genuinely LACKS it. A completely empty month gets the same classification ONLY
    # after a separate present month of this exact pinned series serves positive bars.
    # A day we DID fetch is removed (self-heal). CRITICAL: a timed-out week's days are
    # UNKNOWN -> 'unfilled' (retryable), so one slow week can NEVER poison a real
    # trading day into a fake source-absent flag.
    served_absent = {
        d for d in days
        if (d.year, d.month) in fetched_months
        and d not in fetched_days and d not in skipped_days and d not in partial
    }
    controlled_absent = ({
        d for d in days
        if (d.year, d.month) in clean_empty_months
        and d not in fetched_days and d not in skipped_days and d not in partial
    } if control_month is not None else set())
    source_absent_dates = served_absent | controlled_absent
    source_absent = sorted(d.isoformat() for d in source_absent_dates)
    unfilled = sorted(d.isoformat() for d in days
                      if d not in fetched_days and d not in source_absent_dates)
    man = mstate.get("manifest")
    if man is not None:
        if source_absent or fetched_days or skipped_days:
            sec = man.setdefault("intervals", {}).setdefault(interval, {})
            va = set(sec.get("verified_absent", []))
            evidence = sec.get("verified_absent_evidence", {})
            evidence = dict(evidence) if isinstance(evidence, dict) else {}
            va |= set(source_absent)                       # newly CONFIRMED absent
            va -= {d.isoformat() for d in fetched_days}    # now present -> heal to stored
            va -= {d.isoformat() for d in skipped_days}    # inconclusive -> retryable, never absent
            at = datetime.now().astimezone().isoformat(timespec="seconds")
            for day in served_absent:
                key = day.isoformat()
                evidence[key] = {
                    "method": "day_in_served_month",
                    "control": ss.month_key(day.year, day.month),
                    "at": at,
                }
            for day in controlled_absent:
                evidence[day.isoformat()] = {
                    "method": "empty_month_control",
                    "control": control_month,
                    "at": at,
                }
            for day in fetched_days:
                evidence.pop(day.isoformat(), None)
            for day in skipped_days:
                evidence.pop(day.isoformat(), None)
            evidence = {day: value for day, value in evidence.items()
                        if day in va}
            sec["verified_absent"] = sorted(va)
            if evidence:
                sec["verified_absent_evidence"] = evidence
            else:
                sec.pop("verified_absent_evidence", None)
        if not _save_manifest_safely(tdir, man, res, interval, mstate.get("lock")):
            raise ss.StorageError("targeted fill manifest publication failed")
    completion.update(state="returned", source_absent=source_absent,
        unresolved_days=sorted(set(unfilled + unsupported + unsettled)),
        calendar_unsupported_days=unsupported, closed_days=closed,
        unsettled_days=unsettled)
    return {"months_refetched": len(months), "added": res["added"],
            "dup_existing": res["dup_existing"], "conflicts": res["conflicts"],
            "written": res["written"], "blocked": res["blocked_months"],
            "source_absent": source_absent, "unfilled": unfilled,
            "calendar_unsupported_days": unsupported, "closed_days": closed,
            "unsettled_days": unsettled,
            "settled_prefix_days": sorted(day.isoformat() for day in partial)}


def _attach_split_enrichment(res, root, ticker, interval, run_id, exc):
    gate_context = getattr(exc, "metadata", {}).get("split_gate")
    if not isinstance(gate_context, dict):
        return False
    try:
        proposal = split_enrichment.enrich_halt(
            root, ticker, interval, original_halt=str(exc),
            run_id=run_id, **gate_context)
    except Exception:  # noqa: BLE001 - enrichment may never alter halt behavior
        proposal = None
    if proposal is None:
        return False
    res["split_enrichment"] = proposal
    return True


@fib.symbol_scope
def _fill_series(root, adapter, pacer, ticker, interval, run_id,
                 progress, cancel, conflict_sink, today,
                 since=None, resolved=None, pipeline=False,
                 manifest_lock=None, session_cache=None, month_range=None,
                 date_split_owns_earliest=True, pickup_plan_ahead=None,
                 before_last_request=None, pickup_head_cache=None,
                 _fetch_state=None, _fetch_completion=None):
    """Wrapper around the actual fill: a halt (or cancel) must NOT
    discard the partial counters — a join-gate halt used to report
    '+0 rows' after physically committing months (adversarial review,
    proven). Cancelled propagates (the run stops) but carries `res`.

    `session_cache` (extended-hours combined path) lets this series serve its
    days from a shared pre-split useRTH=False fetch instead of its own IBKR
    requests — see _fetch_sessions / _fill_combined."""
    import addstock_fetch_tasks as post_tasks
    task_state = post_tasks.checked_state(_fetch_state, _fetch_completion)
    res = {"ticker": ticker, "interval": interval, "days_planned": 0,
           "requests": 0, "bars_fetched": 0, "added": 0,
           "dup_existing": 0, "conflicts": 0, "written": 0,
           "months": {}, "blocked_months": [], "halt": None,
           "stored_through": None, "committed_through": None,
           "counters": {"non_rth": 0, "invalid": 0, "outside_day": 0},
           "notes": [], "_empty_days": []}    # pre-created: the pipeline
    #   consumer only ever APPENDS to _empty_days, never INSERTS the key —
    #   so the producer thread can mutate res['requests']/['counters']
    #   concurrently with no top-level dict-structure change (race-free even
    #   under a free-threaded / no-GIL Python build)
    if _fetch_completion is not None:
        _fetch_completion["result"] = res
    preplanned = (pickup_plan_ahead.take(ticker, interval, cancel=cancel)
                  if pickup_plan_ahead is not None else None)
    try:
        _fill_series_inner(res, root, adapter, pacer, ticker, interval,
                           run_id, progress, cancel, conflict_sink,
                           today, since, resolved, pipeline, manifest_lock,
                           session_cache, month_range,
                           date_split_owns_earliest, preplanned,
                           before_last_request, pickup_head_cache)
    except SeriesHalt as exc:
        res["halt"] = str(exc)
        _attach_split_enrichment(
            res, root, ticker, interval, run_id, exc)
        if res.get("written", 0):
            clear_halted_series(root, ticker, interval)
        else:
            record_halted_series(root, ticker, interval, res["halt"])
    except RequestRefused as exc:
        if task_state is not None:
            res["halt"] = post_tasks.error_detail(exc)
            res["halt_kind"] = "request_refused"
            raise task_state.fail(exc)
        res["halt"] = f"request refused: {exc}"
        res["halt_kind"] = "request_refused"
        record_halted_series(root, ticker, interval, res["halt"])
    except Cancelled as exc:
        res["halt"] = ("cancelled — committed months stay; re-run to "
                       "resume")
        if res.get("written", 0):
            clear_halted_series(root, ticker, interval)
        if task_state is None:
            exc.res = res
        elif cancel is None or not cancel.is_set():
            task_state.fail(exc)
        raise
    except Exception as exc:  # Preserve committed evidence through batch isolation.
        if task_state is not None:
            res["halt"] = post_tasks.error_detail(exc)
            if post_tasks.terminal_error(exc):
                task_state.fail(exc)
            raise
        res["halt"] = f"series failed: {exc}"
        exc.res = res
        raise
    else:
        if res.get("written", 0):
            clear_halted_series(root, ticker, interval)
    return res


def _backfill_marker(manifest, interval, set_to=None):
    """Per-interval 'a backward backfill is INCOMPLETE' flag in the manifest. Set True
    when Call B starts, False when it lands; a cancel/halt leaves it True (saved by the
    series-end manifest write), so the NEXT run knows to run the interior seal — and a
    plain in-range update where it's clear pays ZERO read cost (the #6 read is gated)."""
    sec = manifest.setdefault("intervals", {}).setdefault(interval, {})
    if set_to is not None:
        sec["backfill_incomplete"] = bool(set_to)
    return bool(sec.get("backfill_incomplete"))


def _backfill_date(value):
    """Normalize persisted/backfill timestamps to a calendar date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value:
        try:
            return date.fromisoformat(str(value)[:10])
        except (TypeError, ValueError):
            pass
    return None


def _backfill_begin(manifest, interval, served_earliest):
    """Persist enough evidence to make a later resume seal deterministic."""
    sec = manifest.setdefault("intervals", {}).setdefault(interval, {})
    _backfill_marker(manifest, interval, set_to=True)
    sec["backfill_incomplete_reason"] = "interrupted"
    source_first = _backfill_date(served_earliest)
    if source_first is None:
        sec.pop("backfill_served_earliest", None)
    else:
        sec["backfill_served_earliest"] = source_first.isoformat()
    sec.pop("under_backfilled", None)
    sec.pop("backfill_seal", None)


def _backfill_try_seal(res, manifest, interval, stored_first,
                       served_earliest=None):
    """Close a backward extension only when served source reach is covered.

    The bounded daily probe is the evidence source; head metadata alone is not.
    Unknown source reach retains the historical permissive behavior, but leaves
    a durable unverified seal record. Returns True when the marker was cleared.
    """
    sec = manifest.setdefault("intervals", {}).setdefault(interval, {})
    stored_date = _backfill_date(stored_first)
    source_date = (_backfill_date(served_earliest)
                   or _backfill_date(sec.get("backfill_served_earliest")))
    notes = res.setdefault("notes", [])

    if stored_date is None:
        _backfill_marker(manifest, interval, set_to=True)
        sec["backfill_incomplete_reason"] = "no_stored_frontier"
        sec["under_backfilled"] = True
        sec["backfill_seal"] = {
            "sealed": False, "verified": bool(source_date),
            "stored_first": None,
            "served_earliest": (source_date.isoformat()
                                if source_date else None),
            "tolerance_days": BACKFILL_SEAL_TOLERANCE_DAYS,
        }
        notes.append("backward extension NOT SEALED - no stored first bar was "
                     "available after the fetch")
        return False

    if source_date is None:
        _backfill_marker(manifest, interval, set_to=False)
        sec.pop("backfill_incomplete_reason", None)
        sec.pop("under_backfilled", None)
        sec["backfill_seal"] = {
            "sealed": True, "verified": False,
            "stored_first": stored_date.isoformat(),
            "served_earliest": None,
            "tolerance_days": BACKFILL_SEAL_TOLERANCE_DAYS,
        }
        notes.append("backward extension sealed UNVERIFIED - the bounded daily "
                     "probe did not establish IBKR's served earliest bar")
        return True

    gap_days = (stored_date - source_date).days
    sealed = gap_days <= BACKFILL_SEAL_TOLERANCE_DAYS
    sec["backfill_served_earliest"] = source_date.isoformat()
    sec["backfill_seal"] = {
        "sealed": sealed, "verified": True,
        "stored_first": stored_date.isoformat(),
        "served_earliest": source_date.isoformat(),
        "gap_days": gap_days,
        "tolerance_days": BACKFILL_SEAL_TOLERANCE_DAYS,
    }
    if sealed:
        _backfill_marker(manifest, interval, set_to=False)
        sec.pop("backfill_incomplete_reason", None)
        sec.pop("under_backfilled", None)
        return True

    _backfill_marker(manifest, interval, set_to=True)
    sec["backfill_incomplete_reason"] = "source_history_remaining"
    sec["under_backfilled"] = True
    detail = {
        "stored_first": stored_date.isoformat(),
        "served_earliest": source_date.isoformat(),
        "gap_days": gap_days,
        "tolerance_days": BACKFILL_SEAL_TOLERANCE_DAYS,
    }
    res["under_backfilled"] = detail
    notes.append(
        f"backward extension NOT SEALED - stored history starts {gap_days} "
        f"day(s) after IBKR's demonstrated served frontier {source_date}; "
        "request the remaining earlier history")
    return False


def _series_stored_days(root, ticker, interval, manifest):
    """The set of calendar dates present in a stored series (reads each month
    file's bars). Used by the backward-extension RESUME safety net."""
    days = set()
    for key in list(ss.manifest_months(manifest, interval)):
        try:
            y, m = int(str(key)[:4]), int(str(key)[5:7])
        except (ValueError, IndexError):
            continue
        fp = ss.find_month_file(root, ticker, y, m, interval)
        if fp is None:
            continue
        try:
            bars, _ = ss.read_month_file(fp)
        except (ss.StorageError, OSError):
            continue
        days.update((b[0].date() if hasattr(b[0], "date") else b[0]) for b in bars)
    return days


def _backfill_seal_interior(res, root, adapter, ticker, interval, contract,
                            first, last, manifest):
    """RESUME safety net: fill any INTERIOR missing trading day in the EXISTING
    [first, last] range. A prior CANCELLED backward backfill commits older months
    OLDEST-FIRST, which can leave a hole between the partial backfill and the original
    boundary that the 'since >= first' short-circuit would skip FOREVER — silent,
    permanent loss on a bare re-run. Diff the stored days against the weekday calendar
    and MERGE-fill the holes (the boundary basis is already validated on disk).
    Calendar-only no-request outcomes and ordinary provider failures are noted
    without blocking the forward update. Authority, ledger and cancellation
    failures remain terminal and must stop further requests."""
    if first is None or last is None:
        return
    try:
        # HOLIDAY-AWARE calendar: the ticker's OWN stored 1d. A market holiday is not
        # in the 1d at all, so it never looks like a gap — meaning even a LONE missing
        # day is sealed (no fragile >=2 run-filter) and holidays are never re-fetched.
        # No stored 1d -> can't tell a real gap from a holiday, so skip (the GUI
        # connected-pattern heal, consensus-calendar-based, covers that case).
        cal = _series_stored_days(root, ss.canonical_ticker(ticker), "1d", manifest)
        if not cal:
            return
        lo, hi = first.date(), last.date()
        cal = {d for d in cal if lo <= d <= hi}
        stored = _series_stored_days(root, ticker, interval, manifest)
        holes = sorted(d.isoformat() for d in (cal - stored))
        if not holes:
            return
        r = fill_missing_days(adapter, root, ticker, interval, holes,
                              contract=contract,
                              _fetch_child=fops.narrow_worker_child(
                                  fib.current_worker(), "interior-" + uuid.uuid4().hex))
        # fill_missing_days wrote its OWN manifest copy; merge the sealed months back
        # into the IN-MEMORY manifest `_fill_series_inner` will persist at series end —
        # else its save CLOBBERS those entries and a whole-month hole re-fetches FOREVER.
        try:
            fresh = ss.load_manifest(Path(root) / ss.canonical_ticker(ticker))
            if fresh is not None:
                mine = ss.manifest_months(manifest, interval)
                for k, v in ss.manifest_months(fresh, interval).items():
                    mine.setdefault(k, v)         # adopt sealed months, never overwrite
        except Exception:  # noqa: BLE001
            pass
        res["bars_fetched"] = res.get("bars_fetched", 0) + r.get("added", 0)
        res["added"] = res.get("added", 0) + r.get("added", 0)      # consistent run
        res["written"] = res.get("written", 0) + r.get("written", 0)  # accounting
        res.setdefault("notes", []).append(
            f"backward-extension RESUME: sealed {r.get('added', 0):,} interior-hole "
            f"bar(s) across {len(holes)} day(s) ({holes[0]}..{holes[-1]}) — a prior "
            f"cancelled backfill left them; a bare re-run is now lossless")
    except (AuthorityError, LedgerError, Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001 — ordinary provider failure is non-fatal
        res.setdefault("notes", []).append(
            f"interior-hole resume-check skipped ({type(exc).__name__}: {exc})")


def _backfill_earlier(res, root, adapter, pacer, ticker, interval, contract,
                      manifest, identity_floor, since, today, flush, buffer,
                      actions, say, cancel, pipeline, session_cache,
                      run_id=None):
    """Deeper-lookback BACKWARD extension for an EXISTING series.

    If the requested depth (`since`) predates the series' EARLIEST stored bar,
    fill the missing earlier history too (an existing series otherwise only
    updates FORWARD). Two passes that reuse the normal gate machinery:

      A) VALIDATE the basis at the stored boundary — re-fetch the earliest stored
         session and run the entry gate against what is on disk (full overlap at
         `first_stored`). A mismatch (undetected split / re-adjustment) means the
         older bars would be on a different scale, so we STOP rather than merge
         mis-scaled data.
      B) BACKFILL [bw_start, first_stored]; the per-session join gate catches a
         split INSIDE the older range exactly as on the forward path.

    NON-FATAL: any halt here notes + stops the backfill but does NOT block the
    forward update (the forward boundary is independent). Mutates `res`."""
    first = series_first_dt(manifest, interval)
    if since is None or first is None:
        return
    if since >= first.date():
        # 'since >= first' normally means the stored history already reaches the
        # requested depth. An interior HOLE only exists when a PRIOR backfill was
        # actually INTERRUPTED (the marker) — seal it then (a bare re-run is lossless).
        # A clean in-range update skips the read+seal entirely (no wasted decode).
        if _backfill_marker(manifest, interval):
            sec = manifest.setdefault("intervals", {}).setdefault(interval, {})
            if sec.get("backfill_incomplete_reason") \
                    != "source_history_remaining":
                _backfill_seal_interior(
                    res, root, adapter, ticker, interval, contract,
                    first, series_last_dt(manifest, interval), manifest)
            _backfill_try_seal(
                res, manifest, interval, series_first_dt(manifest, interval),
                sec.get("backfill_served_earliest"))
        return
    adapter.use_rth = _session_spec(interval)[0]
    _wait_turn_with_status(
        pacer, cancel, metered=False, say=say)  # head: off HMDS budget
    res["requests"] += 1
    bw, note, served_earliest = _head_start_evidence(
        adapter, contract, since, today, _what_to_show(interval))
    if note:
        res["notes"].append(note)
    if ss.base_interval(interval).endswith("s"):
        bw = max(bw, today - timedelta(days=ONE_SECOND_MAX_AGE_DAYS))
    bw = max(bw, since)
    if identity_floor is not None and bw < identity_floor:
        res["notes"].append(
            f"identity listing floor clamps backward extension from "
            f"{bw} to cutover {identity_floor}")
        bw = identity_floor
    if bw >= first.date():
        if _backfill_marker(manifest, interval):
            _backfill_try_seal(
                res, manifest, interval, first, served_earliest)
        res["notes"].append(
            f"requested depth ({since}) is within this stock's IBKR reach "
            f"({bw}) — nothing earlier to backfill")
        return
    runner = _run_days_pipelined if pipeline else _run_days
    try:
        # A) basis check at the existing boundary (entry gate, full overlap)
        fp = (ss.find_month_file(root, ticker, first.year, first.month, interval)
              or ss.month_file_path(root, ticker, first.year, first.month,
                                    interval))
        exist_first, _ = ss.read_month_file(fp)
        stored_first_close = exist_first[-1][4] if exist_first else None
        _bars_before = res["bars_fetched"]
        runner(res, [first.date()], root, adapter, pacer, ticker, interval,
               contract, exist_first, stored_first_close, False, say, cancel,
               flush, buffer, actions, session_cache=session_cache,
               progress_phase="backfill")
        flush()
        # The entry gate only RUNS if Call A actually received the boundary
        # session — `process` returns on empty bars BEFORE the gate. If IBKR
        # served NO bars for first_stored, the basis at the older->existing
        # boundary is UNVERIFIED, so refuse to merge older history blindly
        # (an unrecorded split would land mis-scaled with no halt).
        if res["bars_fetched"] == _bars_before:
            res["notes"].append(
                f"backward extension SKIPPED — the basis-check session "
                f"({first.date()}) returned NO bars from IBKR, so the older "
                f"history could not be verified against the stored boundary; "
                f"the forward update still ran. Re-add once that session serves.")
            return
        # B) backfill the EARLIER range (gate already verified at the boundary;
        #    the join gate still guards against a split INSIDE this older range)
        _backfill_begin(manifest, interval, served_earliest)
        res["notes"].append(
            f"backward extension: backfilling {bw}..{first.date()} "
            f"(deeper than the stored start {first.date()})")
        runner(res, trading_days(bw, first.date()), root, adapter, pacer,
               ticker, interval, contract, [], None, True, say, cancel,
               flush, buffer, actions, session_cache=session_cache,
               progress_phase="backfill")
        flush()
        actual_first = series_first_dt(manifest, interval)
        sealed = _backfill_try_seal(
            res, manifest, interval, actual_first, served_earliest)
        res["backfilled_to"] = (_backfill_date(actual_first) or bw)
        if sealed:
            res["notes"].append(
                f"backward extension complete — series now reaches "
                f"{res['backfilled_to']}")
    except SeriesHalt as exc:
        flush()                                   # keep months committed pre-cliff
        _attach_split_enrichment(
            res, root, ticker, interval, run_id, exc)
        res["notes"].append(
            f"backward extension stopped ({exc}) — the forward update still ran; "
            f"run the basis doctor for the older boundary, then re-add")
    except (ss.StorageError, OSError) as exc:
        res["notes"].append(
            f"backward extension skipped — first stored month unreadable ({exc})")


def _fill_series_inner(res, root, adapter, pacer, ticker, interval,
                       run_id, progress, cancel, conflict_sink,
                       today, since=None, resolved=None, pipeline=False,
                       manifest_lock=None, session_cache=None,
                       month_range=None, date_split_owns_earliest=True,
                       preplanned=None, before_last_request=None,
                       pickup_head_cache=None):
    def say(msg):
        if progress is not None:
            try:
                progress(msg)
            except Exception:  # noqa: BLE001
                pass

    # FINISH-THE-MONTH pause also applies BETWEEN series: if a pause is already
    # in effect, idle BEFORE starting this series (a clean boundary — nothing
    # fetched yet) rather than starting its first month.
    _wait_while_paused(pacer._pause, cancel, say=say,
                       on_pause=getattr(pacer, "_on_pause", None),
                       note=f"before {ticker} {interval}",
                       pause_info={"last_month": ""})

    # Resolve the contract with the FEWEST requests:
    #  · A1 — if the manifest PINS a conId, fetch by it with NO qualify
    #    round-trip. The conId is immutable, so this is immune to ticker
    #    reuse (we always fetch the exact contract the series was built
    #    from). `resolved` is gap_fill's batch pre-pass map; when it
    #    re-resolves a PINNED ticker to a DIFFERENT conId the symbol was
    #    reused/relisted — the data stays correct (we keep fetching the
    #    pinned contract), so we WARN and continue rather than halt.
    #  · A2 — for an UNPINNED ticker, take the conId the batch pre-pass
    #    already resolved (so NO per-series qualify runs). Only when the
    #    pre-pass has nothing (single-series call, or a symbol it could
    #    not resolve) do we qualify here — that path also raises the
    #    standard not-found halt for a delisted/bad symbol.
    # qualify/head are off the HMDS budget either way (metered=False).
    manifest = ss.load_manifest(Path(root) / ticker)
    identity_floor = _identity_floor_for_series(
        manifest, ticker, interval, res)
    pinned = (manifest or {}).get("conid")
    live = (resolved or {}).get(ticker)
    dead_conid_retry = None
    divergence_note = ""
    if pinned:
        contract = adapter.contract_for(pinned)
        conid = pinned
        if live is not None and int(live) != int(pinned):
            res["notes"].append(
                f"conId DIVERGENCE: {ticker} now resolves to {live} at "
                f"IBKR but this series is pinned to {pinned} — the symbol "
                f"was reused or the listing changed. Still fetching the "
                f"ORIGINAL contract ({pinned}); verify before trusting "
                f"the {ticker} label.")
            divergence_note = res["notes"][-1]
            dead_conid_retry = int(live)
        elif live is None and resolved is not None and ticker in resolved:
            # the pre-pass RAN and could not resolve the symbol at all —
            # possibly delisted/renamed. The pinned conId still fetches,
            # so we surface a breadcrumb and continue.
            res["notes"].append(
                f"{ticker} no longer resolves as a symbol at IBKR "
                f"(possibly delisted or renamed) — still fetching by the "
                f"pinned contract ({pinned}).")
    elif live is not None:
        conid = int(live)                          # A2: resolved by the
        contract = adapter.contract_for(conid)     # batch pre-pass. Held
        #                                          # IN MEMORY only — NOT
        #   persisted here. _commit_month pins it on the first SUCCESSFUL
        #   commit (after the gate accepts), so a series whose gate HALTS
        #   never leaves a wrong conId pinned to its manifest (F6).
    else:
        if fib.current_worker() is None:
            _wait_turn_with_status(pacer, cancel, metered=False, say=say)
        res["requests"] += 1
        with fib.qualification_scope("qualify", [ticker], lambda:
                _wait_turn_with_status(pacer, cancel, metered=False, say=say),
                cancel=cancel, on_wait=_pacing_wait_observer(say)):
            conid, contract = adapter.qualify(ticker)
        #   deal — pinned at commit time, not before the gate (F6).
    # captured ONCE while the connection is alive: the give-up flush
    # used to call account() on a dead adapter and crash the whole run
    src_name = f"IBKR:{adapter.account()}"
    if preplanned is None:
        plan = plan_gap(root, ticker, interval, today=today)
        actions = None
    else:
        plan, actions = preplanned
    if plan.get("error"):
        raise SeriesHalt(plan["error"])
    if actions is None:
        actions = sb.load_actions(root, ticker)   # recorded boundaries (M1)
    if manifest and manifest.get("basis", "unknown") == "unknown" \
            and not plan["empty_series"]:
        res["notes"].append("stored basis is UNKNOWN (task #23 pending) "
                            "— the overlap gate is the only protection")
    rng = _normalize_month_range(month_range)
    owns_earliest = bool(date_split_owns_earliest)
    if plan["empty_series"] and rng is not None:
        # Unknown live head means unknown month set. Date-split must be built
        # from a deterministic offline plan, so a fresh series falls back to the
        # normal whole-series path instead of guessing split boundaries.
        rng = None
        res["notes"].append("date-split skipped for empty series "
                            "(IBKR head date required)")

    if plan["empty_series"]:
        what_to_show = _what_to_show(interval)
        head_cache = (pickup_head_cache if pickup_head_cache is not None
                      else _PickupHeadCache())
        head_key = _PickupHeadCache.key(
            conid, what_to_show, today, since)
        evidence = head_cache.peek(head_key)
        evidence_found = evidence is not _PICKUP_MISSING
        head_reused = evidence_found
        cached_start = None
        if evidence_found:
            start, _head_note, served_earliest = evidence
            res["notes"].append(
                "live head/daily evidence reused for this contract")
        else:
            if _pickup_sidecar_authoritative(
                    root, ticker, manifest, what_to_show):
                expected_conid = _pickup_identity_conid(
                    manifest, resolved, ticker)
                cached_start = _cached_pickup_start(
                    root, ticker, today, expected_conid)
            if cached_start is not None:
                start = cached_start
                res["notes"].append(
                    f"earliest-on-IBKR cache hit ({start}) - live head probe "
                    f"skipped")
            else:
                def _probe_head():
                    _wait_turn_with_status(
                        pacer, cancel, metered=False, say=say)
                    res["requests"] += 1
                    adapter.use_rth = _session_spec(interval)[0]
                    return _head_start_evidence(
                        adapter, contract, since, today, what_to_show)

                evidence, head_reused = head_cache.get_or_compute(
                    head_key, _probe_head, cancel=cancel)
                start, _head_note, served_earliest = evidence
                if head_reused:
                    res["notes"].append(
                        "live head/daily evidence reused for this contract")
        if what_to_show == "TRADES":
            # Whether the empty-series start came from live/shared evidence or
            # from the strict bound sidecar, this worker has completed the
            # pickup-head decision. In a parallel publication race its local
            # earliest_seen snapshot may predate the new sidecar; suppress the
            # otherwise redundant post-series earliest_available fallback.
            res["_pickup_head_attempted"] = True
        if cached_start is None:
            if _head_note:
                res["notes"].append(_head_note)
            if what_to_show == "TRADES":
                if served_earliest is not None:
                    # Identity/display metadata must not claim the bounded
                    # daily floor is later than a reconciled older head: the
                    # Add Stocks identity gate would then reject the valid
                    # pre-floor archive.  This remains an unbound legacy value
                    # when cache_start < served_earliest; the strict D1 cache
                    # therefore probes again rather than trusting head alone.
                    res["ibkr_earliest"] = min(
                        start, served_earliest).isoformat()
                    if not head_reused:
                        _record_pickup_evidence(
                            root, ticker, served_earliest, int(conid),
                            cache_start=start)
        if ss.base_interval(interval).endswith("s"):
            floor = today - timedelta(days=ONE_SECOND_MAX_AGE_DAYS)
            if start < floor:
                start = floor
                res["notes"].append(
                    f"1-second history only reaches ~{floor} at IBKR")
        if since is not None and start < since:
            res["notes"].append(
                f"backfill limited to {since} by request — IBKR history "
                f"reaches back to {start}")
            start = since
        elif since is not None and start > since:
            res["notes"].append(
                f"requested depth reaches before this stock's history "
                f"— fetching from the earliest available ({start})")
        if identity_floor is not None and start < identity_floor:
            res["notes"].append(
                f"identity listing floor clamps empty-series backfill from "
                f"{start} to cutover {identity_floor}")
            start = identity_floor
        # An extended-hours (-pre/-post) series mirrors its REGULAR base
        # interval's bars, so a NEW one must not reach back further than the
        # regular series already does — otherwise an UPDATE with extended-hours
        # on would re-pull YEARS of pre/post from IBKR's earliest. Cap it at the
        # regular series' first stored bar (#10: an update's furthest-back is the
        # data's own start). Brand-new tickers have no base data → no cap.
        base_only = ss.base_interval(interval)
        if base_only != interval:                # this IS a -pre/-post series
            base_first = series_first_dt(manifest, base_only)
            if base_first is not None and start < base_first.date():
                res["notes"].append(
                    f"extended-hours backfill capped at the regular series' "
                    f"start ({base_first.date()})")
                start = base_first.date()
        days = trading_days(start, today)
        res["notes"].append(f"empty series — full backfill from {start}")
    else:
        days = plan["days"]
        if plan["clipped_1s"]:
            res["notes"].append(
                "gap predates IBKR's ~6-month 1-second window — the "
                "older part is UNFILLABLE and was skipped")
    days = _settled_plan_days(interval, days, res)
    full_days = list(days)
    probe_day = None
    if rng is not None:
        chunk_days = _days_in_month_range(full_days, rng)
        if not chunk_days:
            res["date_split"] = {"month_range": [list(rng[0]), list(rng[1])],
                                 "owns_earliest": owns_earliest,
                                 "skipped": "no planned days in range"}
            res["days_planned"] = 0
            return
        if not owns_earliest:
            try:
                pos = full_days.index(chunk_days[0])
            except ValueError:
                pos = -1
            if pos > 0:
                probe_day = full_days[pos - 1]
                days = [probe_day] + chunk_days
            else:
                days = chunk_days
        else:
            days = chunk_days
        res["date_split"] = {"month_range": [list(rng[0]), list(rng[1])],
                             "owns_earliest": owns_earliest,
                             "probe_day": (probe_day.isoformat()
                                           if probe_day else None)}
    res["days_planned"] = len(days)

    last_stored = plan["last_stored"]
    res["stored_through"] = last_stored      # where the series ended
    existing_last_month = []                 # BEFORE this run (None=new)
    interior_date_split = rng is not None and not owns_earliest
    if last_stored is not None and not interior_date_split:
        p = (ss.find_month_file(root, ticker, last_stored.year,
                                last_stored.month, interval)
             or ss.month_file_path(root, ticker, last_stored.year,
                                   last_stored.month, interval))
        try:
            existing_last_month, _ = ss.read_month_file(p)
        except (ss.StorageError, OSError) as exc:
            raise SeriesHalt(f"the last stored month fails its strict "
                             f"read ({exc}) — repair it before fetching")
    stored_last_close = (existing_last_month[-1][4]
                         if existing_last_month else None)

    gate_passed = last_stored is None or interior_date_split
    # empty series: nothing to
    buffer = {}                              # compare against; (y,m)->[bars]
    # A3: ONE in-memory manifest for the whole series, persisted every K
    # commits (in _commit_month) and ONCE more at series end (the finally).
    mstate = {
        "manifest": manifest, "since_save": 0, "lock": manifest_lock,
        "date_split": rng is not None,
        "interval_state_owner": rng is None or owns_earliest,
    }

    def flush(upto_month=None):
        for ym in sorted(list(buffer)):
            if upto_month is not None and ym >= upto_month:
                continue
            _commit_month(root, ticker, interval, ym, buffer.pop(ym),
                          run_id, src_name, res, conflict_sink, conid, mstate)

    repin_evidence_eligible = (
        last_stored is not None
        and not gate_passed
        and bool(existing_last_month)
        and stored_last_close is not None
        and ss.kind_of(interval) not in ss.RATIO_KINDS)

    def activate_repin(evidence_day):
        intent = mstate.get("repin_intent")
        if intent is None or intent.activated:
            return
        mstate["repin_intent"] = intent.activate(evidence_day)
        # The first data-bearing month from the accepted contract publishes the
        # identity CAS in the same ticker transaction. Duplicate-only evidence
        # still converges through the final series save below.
        mstate["since_save"] = MANIFEST_CHECKPOINT_MONTHS
        res["notes"].append(
            f"conId repin evidence accepted on {evidence_day}: entry gate "
            f"proved continuity from {intent.expected_old} to "
            f"{intent.accepted_new}")

    try:
        while True:
            try:
                # DEEPER LOOKBACK: if the requested depth predates the earliest stored
                # bar, backfill the missing earlier history BEFORE the forward update
                # (no-op for new/empty series and when nothing earlier is requested).
                intent = mstate.get("repin_intent")
                pending_repin = intent is not None and not intent.activated
                if (not pending_repin
                        and (rng is None or owns_earliest)):
                    _backfill_earlier(
                        res, root, adapter, pacer, ticker, interval, contract,
                        manifest, identity_floor, since, today, flush, buffer,
                        actions, say, cancel, pipeline, session_cache,
                        run_id=run_id)
                try:
                    runner = _run_days_pipelined if pipeline else _run_days
                    runner(res, days, root, adapter, pacer, ticker, interval,
                           contract, existing_last_month, stored_last_close,
                           gate_passed, say, cancel, flush, buffer, actions,
                           session_cache=session_cache,
                           commit_month_range=rng,
                           before_last_request=before_last_request,
                           on_identity_evidence=activate_repin)
                except Cancelled:
                    flush()       # a pacer-raised cancel mid-day used to DROP the
                    raise         # buffered complete days (adversarial review)
                intent = mstate.get("repin_intent")
                if intent is not None and not intent.activated:
                    raise SeriesHalt(
                        f"conId repin refused: current conId "
                        f"{intent.accepted_new} supplied no nonempty bars that "
                        "the entry gate could accept; no fresh-contract bytes "
                        "were merged")
                flush()
                if (pending_repin and intent is not None and intent.activated
                        and (rng is None or owns_earliest)):
                    # The usual path validates deeper history first. A repin
                    # instead proves the forward archive boundary before any
                    # fresh-contract backward merge, then performs the same
                    # independently gated extension.
                    _backfill_earlier(
                        res, root, adapter, pacer, ticker, interval, contract,
                        manifest, identity_floor, since, today, flush, buffer,
                        actions, say, cancel, pipeline, session_cache,
                        run_id=run_id)
                break
            except SeriesHalt as exc:
                reason = str(exc)
                can_retry = (
                    dead_conid_retry is not None
                    and mstate.get("repin_intent") is None
                    and _is_contract_rejected_halt(reason)
                    and not buffer
                    and not res.get("_empty_days")
                    and res.get("written", 0) == 0
                    and res.get("added", 0) == 0
                    and res.get("bars_fetched", 0) == 0)
                if not can_retry:
                    intent = mstate.get("repin_intent")
                    if (intent is not None
                            and _is_contract_rejected_halt(reason)):
                        raise SeriesHalt(
                            f"{reason} after retrying current conId "
                            f"{intent.accepted_new}; {divergence_note}") from exc
                    raise
                old_conid = conid
                intent = _ConIdRepinIntent(
                    int(old_conid), int(dead_conid_retry))
                mstate["repin_intent"] = intent
                if not repin_evidence_eligible:
                    raise SeriesHalt(
                        f"{reason}; current conId {intent.accepted_new} was not "
                        "fetched because this empty, ratio, interior "
                        "date-split, or otherwise unproved series cannot "
                        "supply the required positive continuity evidence") from exc
                conid = int(dead_conid_retry)
                contract = adapter.contract_for(conid)
                msg = (f"{ticker}: pinned conId {old_conid} was rejected by "
                       f"IBKR; retrying once with current conId {conid} from "
                       f"the divergence pre-pass")
                res["notes"].append(msg)
                say(msg)
        # F5 reception-completeness: a planned TRADING day (weekday, non-
        # holiday) that returned NO bars from INSIDE the fetched range is a
        # suspected dropped/truncated transfer — flag + record (never HALT:
        # a gap can also be IBKR's own truth). Edge gaps (before the first
        # received day, or today's not-yet-formed session) are IPO/boundary.
        empty = set(res.pop("_empty_days", []))
        if empty and days:
            received = [d for d in days if d not in empty]
            if received:
                lo, hi = received[0], received[-1]
                missing = sorted(d for d in days if d in empty
                                 and lo < d < hi and mc.is_trading_day(d))
                if missing and ss.session_of(interval) == "rth":
                    res["completeness"] = {
                        "trading_days_expected":
                            sum(1 for d in days if mc.is_trading_day(d)),
                        "received": len(received),
                        "missing_interior":
                            [d.isoformat() for d in missing]}
                    res["notes"].append(
                        f"RECEPTION GAP: {len(missing)} trading day(s) "
                        f"returned NO data inside the fetched range "
                        f"({missing[0]}..{missing[-1]}) — suspected dropped/"
                        f"truncated transfer; re-fetch while still available")
                elif missing:
                    # extended sessions legitimately have empty days (thin
                    # liquidity) — note it neutrally, not as a transfer gap.
                    res["notes"].append(
                        f"{len(missing)} interior trading day(s) had no "
                        f"{ss.session_of(interval)}-market bars "
                        f"({missing[0]}..{missing[-1]}) — normal for thin "
                        f"extended-hours sessions, not a transfer gap")
        if res.get("committed_through") is not None:
            res["notes"].append(f"fetched through "
                                f"{res['committed_through']}")
    finally:
        # A3: persist the manifest ONCE for the series — covers success, a
        # SeriesHalt, and cancel (the buffered complete months were already
        # flushed to disk by then, so the manifest just catches up).
        if mstate.get("manifest") is not None:
            _save_manifest_safely(
                Path(root) / ticker, mstate["manifest"], res,
                interval, mstate.get("lock"),
                date_split=bool(mstate.get("date_split")),
                interval_state_owner=bool(mstate.get("interval_state_owner")),
                repin_intent=mstate.get("repin_intent"))
            if rng is not None and not res.get("_conid_repin_conflict"):
                healed = _heal_manifest_months_from_tree(
                    root, ticker, [interval], [rng],
                    lock=mstate.get("lock"), res=res)
                if healed:
                    res["date_split_healed_months"] = healed
    repin_conflict = res.pop("_conid_repin_conflict", None)
    if repin_conflict:
        raise SeriesHalt(repin_conflict)


class _OrEvent:
    """Duck-typed Event whose .is_set() is the OR of several events. Used so a
    worker pauses when EITHER the global Pause button is set OR the adaptive
    controller has PARKED that worker's port. _wait_while_paused / the Pacer
    only ever call .is_set(), so this stands in for a real threading.Event."""
    __slots__ = ("_events",)

    def __init__(self, *events):
        self._events = [e for e in events if e is not None]

    def is_set(self):
        return any(e.is_set() for e in self._events)


class _WorkerCancel(_OrEvent):
    """Read caller/watchdog stops; propagate writes only to the caller."""
    __slots__ = ("_caller", "_stop")

    def __init__(self, caller, interrupt=None):
        self._stop = threading.Event()
        super().__init__(caller, interrupt, self._stop)
        self._caller = caller

    def set(self):
        # Latch locally first: a broken caller setter must not leave siblings
        # or the watchdog waiting for more work after the worker aborts.
        self._stop.set()
        if self._caller is not None:
            self._caller.set()


def _wait_while_paused(pause, cancel, say=None, note=None, on_pause=None,
                       pause_info=None):
    """Block while the run is PAUSED (a SET threading.Event), at a CLEAN
    boundary (a fully-committed month, an empty session with no partial month
    pending, or between series), still honoring cancel. No-op when `pause` is
    None or clear. When `say`+`note` are given, emit ONE line stating exactly
    what is safely completed. The TWS connection merely idles while paused; a
    very long pause may drop it, which the next request's reconnect
    transparently restores — no data is lost.

    `on_pause(bool, info)` (optional) fires True the moment THIS worker reaches
    its clean boundary and starts idling, and False when it resumes (or cancels)
    — so the parallel monitor can show each port as PAUSED and the button can say
    'Paused' only once EVERY port has actually stopped. Older one-arg callbacks
    are still accepted."""
    if pause is None or not pause.is_set():
        return
    if say is not None and note:
        say(f"⏸ Paused — {note}. No new IBKR requests until Resume.")
    def _fire_on_pause(paused):
        if on_pause is None:
            return
        info = dict(pause_info or {}) if paused else {}
        try:
            on_pause(paused, info)
        except TypeError:
            try:
                on_pause(paused)
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass

    _fire_on_pause(True)
    try:
        while pause.is_set():
            if cancel is not None and cancel.is_set():
                raise Cancelled()
            _time.sleep(0.2)
    finally:
        _fire_on_pause(False)


def _interruptible_sleep(seconds, cancel):
    """Wall-clock sleep that honors cancel (raises Cancelled)."""
    end = _time.monotonic() + seconds
    while True:
        if cancel is not None and cancel.is_set():
            raise Cancelled()
        remaining = end - _time.monotonic()
        if remaining <= 0:
            return
        _time.sleep(min(0.5, remaining))


def _fetch_request(adapter, contract, end_dt, duration, bar_size, pacer,
                   cancel, say, flush, ticker, res, where, metered=True,
                   use_rth=True, what_to_show="TRADES", *, interval=None,
                   intended_start=None, intended_end=None):
    """One paced request. `metered` gates it against the 60/10-min HMDS
    window (sub-minute); minute+ bars are cache-served and pass
    non-metered (burst-gapped only). A real disconnect RECONNECTS (up to
    RECONNECT_ATTEMPTS); a PACING VIOLATION instead BACKS OFF and
    retries the same request without reconnecting (reconnecting never
    clears a pacing limit — the rolling 10-min window does). Either
    give-up flushes the buffered months (they stay) and halts. `use_rth`
    is False for a -pre/-post series so the request returns extended bars."""
    adapter.use_rth = use_rth          # the live adapter reads this in fetch()
    attempt = 0
    paced = 0
    timed_out = 0
    request = fib.bar_request("ibkr.gap_fill.session_fetch", contract,
                              interval, end_dt, duration, symbol=ticker,
                              start=intended_start, intended_end=intended_end)
    acquire = lambda: _wait_turn_with_status(pacer, cancel, metered=metered, say=say)
    while True:
        if request is None:
            acquire()
        res["requests"] += 1
        try:
            # keep the 4-arg call for TRADES so existing adapters/tests are
            # untouched; pass whatToShow only for the new kinds.
            with fib.send_scope(request, acquire, cancel=cancel,
                                on_wait=_pacing_wait_observer(say)):
                adapter.use_rth = use_rth  # Re-apply after a reusable adapter reconnects.
                if what_to_show == "TRADES":
                    return adapter.fetch(contract, end_dt, duration, bar_size)
                return adapter.fetch(contract, end_dt, duration, bar_size,
                                     what_to_show)
        except PacingViolation as exc:
            paced += 1
            if paced > PACE_VIOLATION_MAX_WAITS:
                flush()
                _diag("PACE_HALT", port=_adapter_port(adapter), ticker=ticker,
                      where=where, backoffs=paced)
                raise SeriesHalt(
                    f"IBKR pacing limit did not clear after {paced} "
                    f"back-offs at {where} ({exc}); committed months "
                    f"stay — re-run later (the 60-requests/10-min limit "
                    f"is per ACCOUNT, shared with any other client)")
            say(f"{ticker}: IBKR pacing limit reached — waiting "
                f"{PACE_VIOLATION_BACKOFF_S:.0f}s for the 10-min window "
                f"to clear (back-off {paced}/{PACE_VIOLATION_MAX_WAITS})…")
            _diag("PACING", port=_adapter_port(adapter), ticker=ticker,
                  backoff=f"{paced}/{PACE_VIOLATION_MAX_WAITS}",
                  wait=PACE_VIOLATION_BACKOFF_S, where=where)
            if request is None:
                pacer.saturate()      # legacy/test adapters without a context
            else:
                fib.pacer().saturate()  # the verified account that actually sent
            # This branch never calls wait_turn, so it used to emit NOTHING
            # structured and the progress label sat frozen on a stale counter
            # for the whole stall — the pacing a user actually experiences was
            # the only one they could not see (F-CLAUDE-6-2). Emit the same
            # waiting/ready pair the pacer emits so the label reports the real
            # back-off; the scrolling-log line above is unchanged. Cosmetic
            # only — no pacing decision reads these events.
            say(_pacing_status_msg(True, {
                "seconds": PACE_VIOLATION_BACKOFF_S,
                "reason": "provider-backoff"}))
            try:
                _interruptible_sleep(PACE_VIOLATION_BACKOFF_S, cancel)
            finally:
                # paired even when the sleep raises Cancelled — same contract
                # as Pacer.wait_turn's finally, so a mid-backoff Cancel never
                # strands "pacing: waiting…" on the label (F-CLAUDE-81-1).
                say(_pacing_status_msg(False))
            # retry the SAME request — no reconnect, attempt unchanged
        except RequestTimeout as exc:
            # the LINK is up (other requests on it work) — the demo HMDS just is
            # not answering THIS contract (a ticker it can't serve). Reconnecting
            # can't fix that, so retry the WARM link a couple times, then halt
            # JUST this series so the port moves on, instead of grinding the
            # 20-reconnect ride-out (~20 min) on one poison ticker.
            timed_out += 1
            if timed_out > TIMEOUT_RETRIES:
                flush()
                _diag("TIMEOUT_HALT", port=_adapter_port(adapter),
                      ticker=ticker, where=where, tries=timed_out)
                raise SeriesHalt(
                    f"IBKR HMDS did not answer {ticker} at {where} after "
                    f"{timed_out} timed-out tries on a healthy link — the demo "
                    f"can't serve this contract; committed months stay, skipping "
                    f"this series (re-run later to retry it)")
            say(f"{ticker}: IBKR didn't answer in {FETCH_TIMEOUT_S:.0f}s "
                f"(link is up) — warm retry {timed_out}/{TIMEOUT_RETRIES}…")
            _diag("TIMEOUT", port=_adapter_port(adapter), ticker=ticker,
                  retry=f"{timed_out}/{TIMEOUT_RETRIES}", where=where)
            _interruptible_sleep(min(RECONNECT_BACKOFF_S, 5.0), cancel)
            # NO reconnect — the link is healthy; just re-issue the request.
            # (The timed-out request is left orphaned on the live socket — ib_async
            # has no cancelHistoricalData here — so a late reply could land in a
            # SUBSEQUENT fetch's error list. fetch() clears errors per request and
            # only acts on a small code set; a stray code is at worst a benign
            # pacing back-off, so the warm-link orphan is a bounded, accepted risk.)
        except ConnectionError as exc:
            attempt += 1
            if attempt > RECONNECT_ATTEMPTS:
                flush()
                _diag("RECON_HALT", port=_adapter_port(adapter), ticker=ticker,
                      where=where, attempts=attempt)
                raise SeriesHalt(
                    f"gave up after {RECONNECT_ATTEMPTS} reconnect "
                    f"attempts at {where} ({exc}); committed months "
                    f"stay — re-run to resume")
            # only a REAL disconnect reaches here now (RequestTimeout is handled
            # above) — ride out a possible TWS restart.
            cause = "connection lost"
            say(f"{ticker}: {cause} ({exc}) — reconnect "
                f"{attempt}/{RECONNECT_ATTEMPTS} (riding out a possible "
                f"TWS restart)…")
            _diag("RECONNECT", port=_adapter_port(adapter), ticker=ticker,
                  attempt=f"{attempt}/{RECONNECT_ATTEMPTS}",
                  why=type(exc).__name__)
            # growing but CAPPED backoff, and CANCEL-AWARE (was a plain
            # blocking sleep that ignored Cancel for tens of seconds)
            _interruptible_sleep(
                min(RECONNECT_BACKOFF_S * attempt, RECONNECT_BACKOFF_CAP_S),
                cancel)
            try:
                adapter.reconnect()
            except ConnectionError:
                continue


def _fetch_sessions(days, interval, adapter, contract, pacer, cancel,
                    say, flush, ticker, res, session_cache=None,
                    before_last_request=None):
    """Yield (session date, clean bars) in order. Whole-session
    intervals fetch COARSE SPANS (one request per ~week/month) and split
    the response by session; sub-minute intervals keep their intra-day
    windowing. Same per-session stream either way -> the gate/commit
    logic downstream is unchanged.

    `session_cache` (the extended-hours prefetch path) keeps separately
    authorized token responses, so this series serves its
    days from `session_cache[interval]` with NO IBKR request — but only when
    EVERY planned day is cached (else it falls through to a normal fetch, so
    correctness never depends on the cache being complete)."""
    days = list(days)
    def publication(day, bars):
        if fib.unsettled_days(interval, [day]):
            deferred = res.setdefault("horizon_deferred_days", [])
            if day.isoformat() not in deferred:
                deferred.append(day.isoformat())
                res.setdefault("notes", []).append(
                    f"{day}: only settled bars through the captured horizon are available; "
                    "the remaining session is deferred, not source-absent")
            if not bars:
                raise RequestRefused(f"{day} is beyond the captured horizon; not source-absent")
        return day, bars

    if session_cache is not None and interval in session_cache:
        sc = session_cache[interval]
        if all(day in sc for day in days):
            if before_last_request is not None:
                before_last_request()
            for day in days:
                yield publication(day, sc[day])
            return
    base = ss.base_interval(interval)
    bar_size = _BAR_SIZES[base][0]
    # sub-minute = genuine HMDS hits (metered); minute+ = cache-served
    # (non-metered, bypasses the 60/10-min limit — measured live)
    metered = _BAR_SIZES[base][1] is not None
    use_rth = _session_spec(interval)[0]           # False for -pre/-post
    wts = _what_to_show(interval)                  # TRADES, or the kind's whatToShow
    if _BAR_SIZES[base][1] is not None:            # sub-minute: per-day
        for day_i, day in enumerate(days):
            raw = []
            worker = fib.current_worker()
            window = worker.context.authority.window(interval, day) if worker else None
            requests = list(day_requests(interval, day, session_window=window))
            for req_i, (end_dt, duration) in enumerate(requests):
                if (before_last_request is not None
                        and day_i == len(days) - 1
                        and req_i == len(requests) - 1):
                    before_last_request()
                raw.extend(_fetch_request(
                    adapter, contract, end_dt, duration, bar_size,
                    pacer, cancel, say, flush, ticker, res, day,
                    metered=metered, use_rth=use_rth, what_to_show=wts,
                    interval=interval))
            yield publication(day, convert_bars(raw, day, res["counters"], interval))
    else:                                          # coarse spans
        chunks = list(span_chunks(interval, days))
        for chunk_i, (end_dt, duration, covered) in enumerate(chunks):
            if (before_last_request is not None
                    and chunk_i == len(chunks) - 1):
                before_last_request()
            label = (f"{covered[0]}" if len(covered) == 1
                     else f"{covered[0]}..{covered[-1]}")
            raw = _fetch_request(adapter, contract, end_dt, duration,
                                 bar_size, pacer, cancel, say, flush,
                                 ticker, res, label, metered=metered,
                                 use_rth=use_rth, what_to_show=wts,
                                 interval=interval,
                                 intended_start=datetime.combine(covered[0], time.min))
            by_day = split_session_bars(raw, frozenset(covered),
                                        res["counters"], interval)
            for day in covered:
                yield publication(day, by_day.get(day, []))


def _make_session_processor(res, ticker, interval, existing_last_month,
                            stored_last_close, gate_passed, say, flush,
                            buffer, actions, days, pause=None, cancel=None,
                            on_pause=None, commit_month_range=None,
                            on_identity_evidence=None):
    """The per-session gate+buffer+commit body, shared VERBATIM by the
    serial (`_run_days`) and pipelined (`_run_days_pipelined`) fetch loops
    so both produce a byte-identical tree. Returns a `process(day, bars,
    di, nd)` closure that holds the cross-session gate state (gate_passed,
    vol_mult, prev_close/day). A gate halt raises SeriesHalt (after a flush
    on a join cliff, exactly as before)."""
    # IV/HVOL are 0-1 RATIOS, not dollar prices — the entry/join/volume gates
    # (price-ratio band [0.55,1.9], share-volume calibration) would FALSE-HALT a
    # normal vol move, so a ratio series skips them (mirrors the cross-val skip).
    _ratio = ss.kind_of(interval) in ss.RATIO_KINDS
    st = {"vol_mult": 1, "prev_close": None, "prev_day": None,
          "gate_passed": gate_passed or _ratio, "last_month": None}
    # recorded-action window for the ENTRY comparison: the archive's last
    # session vs a stream possibly on today's basis — actions strictly
    # after that session, up to the last plannable day
    last_day = (existing_last_month[-1][0].date()
                if existing_last_month else None)
    win_end = days[-1] if days else None
    commit_month_range = _normalize_month_range(commit_month_range)

    def _in_commit_range(day):
        return (commit_month_range is None
                or commit_month_range[0] <= (day.year, day.month)
                <= commit_month_range[1])

    def process(day, bars, di, nd):
        say(f"{ticker} {interval}: {day} ({di + 1}/{nd})…")
        res["bars_fetched"] += len(bars)
        if not bars:
            # F5 reception-completeness: remember which planned days came
            # back empty — classified after the run (a TRADING day empty
            # INSIDE the fetched range is a suspected dropped transfer; a
            # holiday or today's not-yet-formed session is not).
            if _in_commit_range(day):
                res["_empty_days"].append(day)    # key pre-created in _fill_series
            # An empty session is a clean pause boundary only when no partial
            # data-bearing month is still in memory. A fully empty series has
            # no such month. If the empty day has crossed into a later month,
            # the ordered session walk proves the prior month complete, so
            # commit it before idling. An empty day inside the SAME month must
            # keep walking to preserve the finish-the-month contract.
            lm = st["last_month"]
            day_month = (day.year, day.month)
            clean_empty_boundary = lm is None or day_month != lm
            if (clean_empty_boundary and pause is not None
                    and pause.is_set()):
                # F-PAUSE-1: honor a pending Pause on a NO-DATA session too.
                # An empty-source stretch (HMDS "no data" or empty responses)
                # contains no month transition, which let a fully empty series
                # ignore Pause from start to finish (NVR, 2026-07-20).
                # Crossing into an empty later month also makes the preceding
                # data month safe to commit before the hold.
                if lm is not None:
                    flush(upto_month=day_month)
                _wait_while_paused(
                    pause, cancel, say=say, on_pause=on_pause,
                    note=(f"{ticker} {interval}: {day} returned no data — "
                          f"holding on a clean no-data session"),
                    pause_info={"last_month": (f"{lm[0]}-{lm[1]:02d}"
                                               if lm else "")})
            return                            # holiday / no data yet
        if not st["gate_passed"]:
            # positive-evidence entry check: enough overlap pairs, or
            # an explicit join against the archive's own last close —
            # never a pass by absence of overlap (two proven bypasses)
            reason, note = entry_gate(existing_last_month, bars,
                                      interval, stored_last_close)
            # the archive may be PRE-action while IBKR serves ADJUSTED
            # history: a RECORDED boundary after the archive end makes
            # existing*factor the equally-explained expectation — only
            # then is the second comparison allowed at all
            f_entry = boundary_factor(actions, last_day, win_end)
            if reason and f_entry != 1.0:
                r2, note = entry_gate(existing_last_month, bars,
                                      interval, stored_last_close,
                                      price_factor=f_entry)
                if r2 is None:
                    reason = None
                    res["notes"].append(
                        f"overlap matches the recorded post-action "
                        f"basis (factor {f_entry:g}) — accepted")
                else:
                    reason += (f" (recorded factor {f_entry:g} does "
                               f"not explain the difference either)")
            if reason:
                stats = sb.measure_overlap(existing_last_month, bars)
                verdict = sb.classify(stats)
                if stats.get("pairs", 0) >= sb.MIN_PAIRS:
                    gate_name = "entry_overlap"
                    observed = verdict.get("factor") or \
                        stats.get("price_ratio_median")
                else:
                    gate_name = "entry_thin"
                    observed = (bars[0][1] / stored_last_close
                                if stored_last_close else None)
                raise SeriesHalt(reason, metadata={"split_gate": {
                    "gate": gate_name,
                    "prior_date": last_day,
                    "current_date": day,
                    "observed_factor": observed,
                    "overlap_verdict": verdict,
                }})
            if note:
                res["notes"].append(note)
            st["vol_mult"], vnote = calibrate_volume(existing_last_month,
                                                     bars)
            res["notes"].append(vnote)
            vf = boundary_factor(actions, last_day, win_end,
                                 applies=("volume", "both"))
            if vf != 1.0:
                # vol_mult above is about UNITS (lots vs shares) only;
                # the recorded factor is NOT applied to any bar — it
                # is consumed at read time (M3), nothing is rewritten
                res["notes"].append(
                    f"volume basis change recorded (factor {vf:g}) — "
                    f"expected; volumes stored as served")
            st["gate_passed"] = True
            if on_identity_evidence is not None:
                on_identity_evidence(day)
        if st["vol_mult"] != 1:
            bars = [(b[0], b[1], b[2], b[3], b[4], b[5] * st["vol_mult"])
                    for b in bars]
        if st["prev_close"] is not None and not _ratio:
            f = boundary_factor(actions, st["prev_day"], day)
            reason = join_gate(st["prev_close"], bars[0][1],
                               where=str(day), expected_factor=f)
            if reason and f != 1.0:
                # an adjusted feed pre-applies the recorded action, so
                # the boundary join is SMOOTH — either explained basis
                # passes; anything else still halts
                reason = join_gate(st["prev_close"], bars[0][1],
                                   where=str(day))
                if reason:
                    reason += (f" (recorded factor {f:g} does not "
                               f"explain the jump either)")
            if reason:
                flush()                       # everything BEFORE the cliff
                raise SeriesHalt(reason, metadata={"split_gate": {
                    "gate": "join",
                    "prior_date": st["prev_day"],
                    "current_date": day,
                    "observed_factor": bars[0][1] / st["prev_close"],
                }})
            if f != 1.0:
                hits = _actions_between(actions, st["prev_day"], day,
                                        ("price", "both"))
                dates = ", ".join(a.get("date", "?") for a in hits)
                res["notes"].append(
                    f"crossed recorded split/basis boundary {dates} "
                    f"(factor {f:g}) — accepted")
        st["prev_close"] = bars[-1][4]
        st["prev_day"] = day
        if not _in_commit_range(day):
            return
        for b in bars:
            buffer.setdefault((b[0].year, b[0].month), []).append(b)
        flush(upto_month=(day.year, day.month))   # commit completed months
        res["committed_through"] = day            # last day committed so far
        cur_month = (day.year, day.month)
        if (st["last_month"] is not None and cur_month != st["last_month"]
                and pause is not None and pause.is_set()):
            # FINISH-THE-MONTH pause: stepping into a NEW month means the PRIOR
            # month is now fully committed (flush above) -> idle here on a CLEAN
            # boundary, never mid-month (pause is not honored at the request level).
            ly, lm = st["last_month"]
            _wait_while_paused(
                pause, cancel, say=say, on_pause=on_pause,
                note=(f"{ticker} {interval}: {ly}-{lm:02d} fully written — "
                      f"resuming at {cur_month[0]}-{cur_month[1]:02d}"),
                pause_info={"last_month": f"{ly}-{lm:02d}"})
        st["last_month"] = cur_month

    return process


def _run_days(res, days, root, adapter, pacer, ticker, interval,
              contract, existing_last_month, stored_last_close,
              gate_passed, say, cancel, flush, buffer, actions,
              session_cache=None, progress_phase="",
              commit_month_range=None, before_last_request=None,
              on_identity_evidence=None):
    """Serial fetch loop (the DEFAULT, proven path): fetch a session, then
    gate+buffer+commit it, one thread, in order."""
    process = _make_session_processor(
        res, ticker, interval, existing_last_month, stored_last_close,
        gate_passed, say, flush, buffer, actions, days,
        pause=pacer._pause, cancel=cancel,
        on_pause=getattr(pacer, "_on_pause", None),
        commit_month_range=commit_month_range,
        on_identity_evidence=on_identity_evidence)
    nd = len(days)
    progress_throttle = {"last": float("-inf")}
    for di, (day, bars) in enumerate(_fetch_sessions(
            days, interval, adapter, contract, pacer, cancel, say,
            flush, ticker, res, session_cache=session_cache,
            before_last_request=before_last_request)):
        if cancel is not None and cancel.is_set():
            flush()
            raise Cancelled()
        process(day, bars, di, nd)
        done_days = di + 1
        _maybe_emit_progress(
            say, ticker, interval, done_days, nd, res.get("bars_fetched", 0),
            progress_throttle, phase=progress_phase, force=(done_days >= nd))


def _run_days_pipelined(res, days, root, adapter, pacer, ticker, interval,
                        contract, existing_last_month, stored_last_close,
                        gate_passed, say, cancel, flush, buffer, actions,
                        session_cache=None, progress_phase="",
                        commit_month_range=None, before_last_request=None,
                        on_identity_evidence=None):
    """OPT-IN overlap of fetch with pack. THIS thread stays the producer —
    it owns the adapter (ib_async's event loop is thread-bound, so every
    network call must stay here) and only fetches, pushing each (day, bars)
    onto a bounded FIFO. A single CONSUMER thread drains the FIFO and does
    the gate+buffer+commit via the SAME `process` closure the serial path
    uses. Single producer -> FIFO -> single consumer keeps the session order
    identical, so the tree is byte-for-byte the serial tree; the only win is
    the commit (~0.19s/span) hiding under the next fetch.

    Safety contract:
      * res keys are PARTITIONED — the producer writes only res['requests']
        (in _fetch_request) and res['counters'] (in convert/split); the
        consumer writes everything else and owns `buffer`+`flush`. No field
        is written by both threads, so there is no data race.
      * The producer gets a NO-OP flush, so a fetch give-up never commits
        from this thread; it routes a terminal marker to the consumer, which
        flushes (committed months stay) and re-raises — same as serial.
      * A `stop` Event + put/get timeouts + a guaranteed terminal marker
        make it deadlock-free in every exit (done / halt / cancel / error,
        producer-side or consumer-side).
      * Any consumer or producer exception surfaces on THIS thread, so
        _fill_series_inner's existing try/except/finally is unchanged.
      * CALLER CONTRACT: `progress` may be invoked CONCURRENTLY from both
        threads (producer say() during fetch, consumer say() during pack), so
        it must be thread-safe. Every real caller satisfies this — the GUI
        passes a queue.Queue.put and the selftests a list.append (both
        thread-safe); a custom progress callback must not assume single-thread
        access. Progress text is cosmetic and never touches the tree."""
    process = _make_session_processor(
        res, ticker, interval, existing_last_month, stored_last_close,
        gate_passed, say, flush, buffer, actions, days,
        pause=pacer._pause, cancel=cancel,
        on_pause=getattr(pacer, "_on_pause", None),
        commit_month_range=commit_month_range,
        on_identity_evidence=on_identity_evidence)
    nd = len(days)
    progress_throttle = {"last": float("-inf")}
    q = queue.Queue(maxsize=PIPELINE_QUEUE_MAX)
    stop = threading.Event()
    consumer_exc = [None]

    def consumer():
        di = 0
        try:
            while True:
                try:
                    item = q.get(timeout=0.1)
                except queue.Empty:
                    if stop.is_set():         # producer died w/o a terminal
                        return                # (defensive; shouldn't happen)
                    continue
                kind = item[0]
                if kind == "DATA":
                    if cancel is not None and cancel.is_set():
                        flush()
                        raise Cancelled()
                    process(item[1], item[2], di, nd)
                    di += 1
                    _maybe_emit_progress(
                        say, ticker, interval, di, nd,
                        res.get("bars_fetched", 0), progress_throttle,
                        phase=progress_phase, force=(di >= nd))
                elif kind == "DONE":
                    flush()                   # final partial month
                    return
                elif kind == "HALT":
                    flush()                   # committed months stay
                    raise SeriesHalt(item[1])
                elif kind == "CANCEL":
                    flush()
                    raise Cancelled()
                else:                         # ("ERROR", exc): match serial —
                    raise item[1]             # an unexpected error does NOT flush
        except Exception as exc:              # noqa: BLE001 — carry to producer
            consumer_exc[0] = exc
        finally:
            stop.set()                        # let the producer unblock

    t = threading.Thread(target=consumer,
                         name=f"pack-{ticker}-{interval}", daemon=True)
    t.start()

    def safe_put(item):
        # block for a free slot but bail the instant the consumer stops, so
        # a dead consumer can never deadlock the producer on a full queue
        while not stop.is_set():
            try:
                q.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    terminal = None
    try:
        try:
            for day, bars in _fetch_sessions(
                    days, interval, adapter, contract, pacer, cancel, say,
                    lambda *a, **k: None, ticker, res,    # NO-OP flush
                    session_cache=session_cache,
                    before_last_request=before_last_request):
                if stop.is_set():
                    break                     # consumer already stopped — do
                    #   NOT pull/fetch another session. Bounds the wasted
                    #   work after a mid-stream consumer halt to the in-flight
                    #   fetch (matters for sub-minute: each session is its own
                    #   METERED request, so over-fetching burns pacing budget).
                if not safe_put(("DATA", day, bars)):
                    break                     # consumer stopped; exc is set
            else:
                terminal = ("DONE",)          # for-loop finished cleanly
        except Cancelled:
            terminal = ("CANCEL",)
        except SeriesHalt as exc:
            terminal = ("HALT", str(exc))
        except Exception as exc:              # noqa: BLE001
            terminal = ("ERROR", exc)
        if terminal is not None:
            safe_put(terminal)                # no-op if the consumer stopped
    finally:
        # ALWAYS unblock the consumer before joining, even on a non-Exception
        # BaseException (KeyboardInterrupt/SystemExit/GeneratorExit) that skips
        # the except clauses above and leaves `terminal` undelivered — without
        # this, the consumer would spin on q.get and t.join() would hang. On
        # every normal path the consumer has already returned (via its terminal
        # marker) and set stop itself, so this is a harmless idempotent set.
        stop.set()
        t.join()
    if consumer_exc[0] is not None:
        raise consumer_exc[0]


@fib.worker_scope
def _retired_spot_check_day(adapter, contract, interval, day, stored, pacer, cancel):
    """Re-fetch ONE settled session from IBKR and compare it to what was
    stored — the F7 end-to-end fidelity audit (fetch->convert->write->place).
    Compares timestamp + OHLC EXACTLY; volume allows the known lots->shares
    x100 calibration. The session's LAST bar is skipped (the one most likely
    to be late-revised). Ordinary provider failure returns 'skipped'; authority,
    durability and cancellation failures are terminal. Retired, child-only."""
    context = fib.current_worker().context
    interval = fib.canonical_token(interval)
    window = context.authority.window(interval, day)
    horizon = context.horizons[interval]
    if window is None:
        raise RequestRefused("retired spot check selected a closed session")
    if horizon is None or window[1] > horizon:
        raise RequestRefused("retired spot check requires a fully settled session")
    bar_size, win = _BAR_SIZES[ss.base_interval(interval)]
    metered = win is not None
    what_to_show = _what_to_show(interval)
    previous_rth = fib.without_authority(getattr)(adapter, "use_rth", True)
    from types import SimpleNamespace
    sender = SimpleNamespace(fetch=fib.without_authority(getattr)(adapter, "fetch"))
    raw = []
    try:
        fib.without_authority(setattr)(adapter, "use_rth", _session_spec(interval)[0])
        for first, end_dt, duration in _covered_session_requests(interval, day, context):
            request = fib.bar_request("ibkr.stock_ibkr.retired_spot_check", contract,
                                      interval, end_dt, duration, start=first)
            with fib.send_scope(request, lambda: fib.acquire_turn(cancel, metered=metered),
                                cancel=cancel):
                raw.extend(sender.fetch(contract, end_dt, duration, bar_size, what_to_show=what_to_show))
    except (AuthorityError, LedgerError, Cancelled):
        raise
    except (ConnectionError, SeriesHalt) as exc:
        if isinstance(exc, PacingViolation):
            fib.pacer().saturate()
        return {"day": day.isoformat(), "result": "skipped",
                "detail": f"re-fetch failed: {fib.without_authority(format)(exc)}"}
    finally:
        fib.without_authority(setattr)(adapter, "use_rth", previous_rth)
    refetched = convert_bars(raw, day, {"non_rth": 0, "invalid": 0,
                                        "outside_day": 0}, interval)
    if not stored:
        return {"day": day.isoformat(), "result": "skipped",
                "detail": "no stored bars"}
    last_ts = max(b[0] for b in stored)        # skip the late-revisable close
    smap = {b[0]: b for b in stored if b[0] != last_ts}
    rmap = {b[0]: b for b in refetched if b[0] != last_ts}
    missing = set(smap) - set(rmap)
    extra = set(rmap) - set(smap)
    diffs = 0
    for ts in set(smap) & set(rmap):
        a, b = smap[ts], rmap[ts]
        if (a[1], a[2], a[3], a[4]) != (b[1], b[2], b[3], b[4]):
            diffs += 1
        elif a[5] != b[5] and a[5] != b[5] * 100:   # allow lots->shares x100
            diffs += 1
    if missing or extra or diffs:
        return {"day": day.isoformat(), "result": "MISMATCH",
                "compared": len(smap),
                "detail": f"{len(missing)} missing, {len(extra)} extra, "
                          f"{diffs} differing bar(s)"}
    return {"day": day.isoformat(), "result": "ok", "compared": len(smap)}


@fib.worker_scope
def _retired_run_spot_checks(report, root, adapter, pacer, cancel, today, rng):
    """F7 — after the run, re-fetch a random SETTLED session from a random
    committed month of up to SPOTCHECK_MAX_PER_RUN series and compare it to
    disk. Advisory: a mismatch is recorded loudly (per-series note +
    spot_check dict), never halts. One paced request per check."""
    budget, checked = SPOTCHECK_MAX_PER_RUN, 0
    for s in report.get("series", []):
        if budget <= 0:
            break
        if s.get("halt"):
            continue
        written = [ym for ym, info in (s.get("months") or {}).items()
                   if isinstance(info, dict)
                   and info.get("status") == "written"]
        ticker, interval = s.get("ticker"), s.get("interval")
        if interval and ss.kind_of(interval) in ss.RATIO_KINDS:
            continue          # IV/HVOL are ratios; a TRADES re-fetch always "mismatches"
        if not written or not ticker:
            continue
        conid = (ss.load_manifest(Path(root) / ticker) or {}).get("conid")
        if conid is None:
            continue
        ym = rng.choice(written)
        try:
            y, m = int(ym[:4]), int(ym[5:7])
            bars, _ = ss.read_month_file(
                ss.find_month_file(root, ticker, y, m, interval)
                or ss.month_file_path(root, ticker, y, m, interval))
        except (ss.StorageError, OSError, ValueError):
            continue
        by_day = {}
        for b in bars:
            by_day.setdefault(b[0].date(), []).append(b)
        days = sorted(d for d in by_day if d < today)   # SETTLED sessions only
        if not days:
            continue
        day = rng.choice(days)
        result = _retired_spot_check_day(adapter, adapter.contract_for(conid),
                                 interval, day, by_day[day], pacer, cancel,
                                 _fetch_child=fops.narrow_worker_child(
                                     fib.current_worker(), "retired-" + uuid.uuid4().hex))
        s["spot_check"] = result
        if result.get("result") == "MISMATCH":
            s.setdefault("notes", []).append(
                f"SPOT-CHECK MISMATCH on {result['day']}: a fresh IBKR "
                f"re-fetch disagrees with the stored bars "
                f"({result['detail']}) — investigate the fetch/convert/"
                f"write path before trusting this series")
        budget -= 1
        checked += 1
    report["spot_checks_run"] = checked


def _prevent_sleep():
    """Ask Windows to keep the SYSTEM awake for the duration of a run (the
    screen may still sleep). Thread-scoped — call it on the worker thread that
    runs the fetch and clear it with _allow_sleep() in a finally. No-op off
    Windows or if the call fails (a run must never break over power policy)."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ES_CONTINUOUS = 0x80000000
        ES_SYSTEM_REQUIRED = 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    except Exception:  # noqa: BLE001
        pass


def _allow_sleep():
    """Release the keep-awake request — back to the normal power policy."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
    except Exception:  # noqa: BLE001
        pass


def _settled_plan_days(interval, days, res):
    """Nightly plans fetch complete sessions only, using the frozen token horizon.

    Keep this at execution, not only in plan_gap: speculative plans can be
    prepared outside a worker, and empty-series plans are resolved later.
    Lower-level requests still reject future-empty publication independently.
    """
    worker = fib.current_worker()
    closed, unsupported = set(), set()
    if worker is not None:
        for day in days:
            # Unknown coverage is still an error, not a silently omitted date.
            worker.context.authority.row(day)
            try:
                window = worker.context.authority.window(interval, day)
            except CalendarUnsupported:
                unsupported.add(day)
            else:
                if window is None:
                    closed.add(day)
    supported = [day for day in days if day not in closed and day not in unsupported]
    deferred = fib.unsettled_days(interval, supported)
    if unsupported:
        res["calendar_unsupported_days"] = sorted(day.isoformat() for day in unsupported)
        res.setdefault("notes", []).append(
            "calendar window unsupported (unknown, not requested or source-absent): "
            + ", ".join(res["calendar_unsupported_days"]))
    if closed:
        res["calendar_closed_days"] = sorted(day.isoformat() for day in closed)
        res.setdefault("notes", []).append(
            "calendar closed (not requested, not source-absent): "
            + ", ".join(res["calendar_closed_days"]))
    if deferred:
        res["horizon_deferred_days"] = sorted(day.isoformat() for day in deferred)
        res.setdefault("notes", []).append(
            "horizon deferred (unknown, not source-absent): "
            + ", ".join(res["horizon_deferred_days"]))
    return [day for day in days if day not in deferred and day not in closed and day not in unsupported]


def _prefetch_unfiltered(adapter, pacer, contract, ticker, base, session_ivs,
                         days, cancel, say, pre_res,
                         before_last_request=None, days_by_token=None):
    """Build the existing per-token cache using independently authorized sends.

    A single-token envelope cannot authorize an unfiltered three-session
    response. Each token now pays its own actual request count; only its
    durably ledgered, accepted rows enter that token's cache.
    """
    cache = {iv: {} for iv in session_ivs}
    if not days:
        return cache
    bar_size = _BAR_SIZES[base][0]
    chunks = [(iv, end, duration, covered) for iv in session_ivs
              for end, duration, covered in span_chunks(
                  iv, days if days_by_token is None else days_by_token[iv])]
    nspan = len(chunks)
    for i, (iv, end_dt, duration, covered) in enumerate(chunks, 1):
        label = (f"{covered[0]}" if len(covered) == 1
                 else f"{covered[0]}..{covered[-1]}")
        # per-span heartbeat: WITHOUT this the port looks "offline/quiet" in the
        # monitor for the whole (silent) prefetch, even though it is fetching.
        say(f"{ticker} {iv}: {label} ({i}/{nspan} token-scoped requests)…")
        if before_last_request is not None and i == nspan:
            before_last_request()
        raw = _fetch_request(adapter, contract, end_dt, duration, bar_size,
                             pacer, cancel, say, lambda *a, **k: None,
                             ticker, pre_res, label, metered=False,
                             use_rth=_session_spec(iv)[0], interval=iv,
                             intended_start=datetime.combine(covered[0], time.min))
        by_day = split_session_bars(raw, frozenset(covered),
                                    pre_res["counters"], iv)
        for d in covered:
            if fib.unsettled_days(iv, [d]) and not by_day.get(d):
                raise RequestRefused(f"{d}: future empty session cannot enter the prefetch cache")
            cache[iv].setdefault(d, by_day.get(d, []))
    return cache


@fib.symbol_scope
def _prefetch_combined(root, adapter, pacer, ticker, base, session_ivs,
                       resolved, today, since, cancel, say, report,
                       month_ranges=None, pickup_plan_ahead=None,
                       pickup_head_cache=None, before_last_request=None):
    """Build the per-token session caches for one ticker's full
    extended set. Resolves the contract from the pinned/pre-pass conId (no
    extra qualify), computes the UNION of the three series' planned days
    (offline plan_gap; one head_timestamp if any session is empty), and
    pre-fetches each token's spans once. Returns the cache, or None to fall back to per-series
    fetches (correctness never depends on the cache being built)."""
    manifest = ss.load_manifest(Path(root) / ticker)
    floors = {
        iv: _identity_floor_for_series(manifest, ticker, iv, report)
        for iv in session_ivs
    }
    distinct_floors = set(floors.values())
    if len(distinct_floors) > 1:
        report.setdefault("notes", []).append(
            f"{ticker}: combined extended-hours prefetch skipped because "
            f"identity floors differ by session ({floors}); per-series "
            f"fetch preserves each scope")
        return None
    identity_floor = next(iter(distinct_floors), None)
    conid = (manifest or {}).get("conid") or (resolved or {}).get(ticker)
    if conid is None:
        return None                                # let per-series qualify run
    contract = adapter.contract_for(int(conid))
    pre_res = {"requests": 0,
               "counters": {"non_rth": 0, "invalid": 0, "outside_day": 0}}
    union, need_head = set(), False
    for iv in session_ivs:
        mr, _owns = _month_range_spec(month_ranges, ticker, iv)
        plan = None
        if pickup_plan_ahead is not None:
            pickup_plan_ahead.prepare(ticker, iv)
            payload = pickup_plan_ahead.peek(ticker, iv)
            if payload is not None:
                plan = payload[0]
        if plan is None:
            plan = plan_gap(root, ticker, iv, today=today)
        if plan.get("error"):
            continue
        if plan.get("empty_series"):
            need_head = True
        else:
            union |= set(_days_in_month_range(plan.get("days", []), mr))
    if need_head:
        if month_ranges:
            return None
        head_cache = (pickup_head_cache if pickup_head_cache is not None
                      else _PickupHeadCache())
        head_key = _PickupHeadCache.key(
            conid, "TRADES", today, since)
        evidence = head_cache.peek(head_key)
        evidence_found = evidence is not _PICKUP_MISSING
        head_reused = evidence_found
        cached_start = None
        if evidence_found:
            start, _head_note, served_earliest = evidence
            report.setdefault("notes", []).append(
                f"{ticker}: live head/daily evidence reused for combined fetch")
        else:
            if _pickup_sidecar_authoritative(
                    root, ticker, manifest, "TRADES"):
                expected_conid = _pickup_identity_conid(
                    manifest, resolved, ticker)
                cached_start = _cached_pickup_start(
                    root, ticker, today, expected_conid)
        if cached_start is not None:
            start = cached_start
            report.setdefault("notes", []).append(
                f"{ticker}: earliest-on-IBKR cache hit ({start}) - "
                f"combined live head probe skipped")
        elif not evidence_found:
            def _probe_head():
                adapter.use_rth = False
                _wait_turn_with_status(
                    pacer, cancel, metered=False, say=say)
                pre_res["requests"] += 1
                return _head_start_evidence(
                    adapter, contract, since, today, "TRADES")

            evidence, head_reused = head_cache.get_or_compute(
                head_key, _probe_head, cancel=cancel)
            start, _head_note, served_earliest = evidence
            if head_reused:
                report.setdefault("notes", []).append(
                    f"{ticker}: live head/daily evidence reused for "
                    f"combined fetch")
            if served_earliest is not None and not head_reused:
                _record_pickup_evidence(
                    root, ticker, served_earliest, int(conid),
                    cache_start=start)
        if cached_start is None:
            if _head_note:
                report.setdefault("notes", []).append(f"{ticker}: {_head_note}")
        if since is not None and start < since:
            start = since
        if identity_floor is not None and start < identity_floor:
            report.setdefault("notes", []).append(
                f"{ticker}: identity listing floor clamps combined prefetch "
                f"from {start} to cutover {identity_floor}")
            start = identity_floor
        union |= set(trading_days(start, today))   # superset; per-series caps
        #   (pre/post capped at the regular start) are applied in each fill
    days = sorted(union)
    if not days:
        return None
    say(f"{ticker}: extended-hours prefetch — separate token-scoped requests "
        f"for rth+pre+post over {len(days)} sessions…")
    cache = _prefetch_unfiltered(adapter, pacer, contract, ticker, base,
                                 session_ivs, days, cancel, say, pre_res,
                                 before_last_request=before_last_request,
                                 days_by_token={iv: _settled_plan_days(iv, days, {})
                                                for iv in session_ivs})
    report["_prefetch_requests"] = (report.get("_prefetch_requests", 0)
                                    + pre_res["requests"])
    return cache


# --- the public entry ---------------------------------------------------------------

# Disk preflight for fetch runs. Unlike ingest (which weighs concrete input
# files), a fetch run's output size isn't known up front — each series' span is
# its own forward gap. So it's two-tier on purpose: a HARD floor that refuses to
# start a doomed run before any port connects, and a SOFT per-series estimate
# that only WARNS (the run proceeds) so a legitimate top-up on a small disk is
# never falsely blocked. Pairs with the run-level WRITE FAILED detector in
# summarize_report so a disk-full that BEGINS mid-run still ends loud, not silent.
FETCH_PREFLIGHT_FLOOR = 1 * 1024 ** 3            # < 1 GB free -> refuse to start
FETCH_EST_BYTES_PER_SERIES = 25 * 1024 ** 2      # conservative deep-1m extended


def _safe_say(say, msg):
    if say:
        try:
            say(msg)
        except Exception:  # noqa: BLE001
            pass


def _disk_preflight(root, n_series, say, report):
    """Free-space gate run BEFORE any port connects. Returns False (and sets
    report['aborted']) only when free space is below the hard floor — a run that
    cannot possibly write. A soft shortfall vs the rough estimate is a loud
    WARNING (note + say), never an abort, because the per-series span isn't
    known up front."""
    base = Path(root)
    anchor = base if base.exists() else base.parent
    try:
        free = shutil.disk_usage(anchor).free
    except OSError:
        return True                              # can't measure -> don't block
    est = int(n_series) * FETCH_EST_BYTES_PER_SERIES
    report["preflight"] = {"free_bytes": free, "estimated_bytes": est,
                           "series": int(n_series)}
    if free < FETCH_PREFLIGHT_FLOOR:
        report["aborted"] = (
            f"disk preflight: only {free / 1e9:.2f} GB free at {anchor} "
            f"(floor {FETCH_PREFLIGHT_FLOOR / 1e9:.0f} GB) — refusing to start "
            f"so a mid-run disk-full can't silently drop writes")
        _safe_say(say, f"ABORTED — {report['aborted']}")
        return False
    if free < est:
        msg = (f"LOW DISK: ~{est / 1e9:.1f} GB may be needed for "
               f"{int(n_series)} series but only {free / 1e9:.1f} GB is free — "
               f"proceeding, but free space now or writes may fail mid-run")
        report.setdefault("notes", []).append(msg)
        _safe_say(say, f"WARNING: {msg}")
    return True


class _WorkerPacer:
    """Worker-local UI/pause state, shared process-wide reservation history."""
    def __init__(self):
        self._governor = _default_pacer()
        self._pause = self._on_pause = self._on_wait = None

    def wait_turn(self, cancel=None, metered=True, on_wait=None):
        observer = on_wait or self._on_wait
        return self._governor.wait_turn(cancel, metered=metered,
            on_wait=fib.without_authority(observer) if observer is not None else None)

    def saturate(self):
        return self._governor.saturate()


def _run_fetch_operation(operation, root, mode, args, kwargs, evidence_dir=None):
    """Dispatch only shipped bodies; never mint/adopt an ambient operation."""
    context, purpose = operation.context, operation.purpose
    report, error, propagate = None, None, False
    outcome = "returned"
    completion, task_state, coordinator = {}, None, None
    try:
        fops.require_root_admission(operation)  # Before factories, ports or preflight.
        target = {"serial": gap_fill, "parallel": gap_fill_parallel,
                  "resilient": gap_fill_parallel_resilient, "update": _nightly_update_body}.get(mode)
        if target is None:
            raise RequestRefused("unknown engine-owned operation dispatch")
        prefix = (operation, root) if mode == "update" else (root,)
        bound = inspect.signature(target).bind(*prefix, *args, **kwargs)
        options = bound.arguments.get("kwargs", bound.arguments)
        if any(name in options for name in ("_fetch_state", "_fetch_completion")):
            raise RequestRefused("private fetch lifecycle arguments are engine-owned")
        engine_post_tasks = options.get("engine_post_tasks", False)
        if type(engine_post_tasks) is not bool:
            raise RequestRefused("engine_post_tasks must be a boolean")
        if engine_post_tasks:
            if purpose != "add_stocks" or options.get("post_pipeline") is None:
                raise RequestRefused("fixed post tasks require the Add Stocks root and coordinator")
            from addstock_fetch_tasks import _RunState, error_detail
            task_state = _RunState()
            options["_fetch_state"] = task_state
            options["_fetch_completion"] = completion
        fib.guard_callbacks(bound.arguments)
        if task_state is not None:
            coordinator = options.get("post_pipeline")
        if mode == "serial":
            report = gap_fill(*bound.args, **bound.kwargs,
                              _fetch_child=operation.child("serial-root"))
        elif mode == "parallel":
            report = gap_fill_parallel(*bound.args, **bound.kwargs, _fetch_parent=operation)
        elif mode == "resilient":
            report = gap_fill_parallel_resilient(*bound.args, **bound.kwargs, _fetch_parent=operation)
        elif mode == "update":
            report = _nightly_update_body(*bound.args, **bound.kwargs)
        if isinstance(report, dict):
            if report.get("cancelled"):
                outcome = "cancelled"
            elif report.get("aborted") or any(
                    row.get("aborted") for row in (report.get("per_port") or {}).values()):
                outcome = "failed"
    except BaseException as exc:
        error, propagate, outcome = exc, True, "failed"
        if task_state is not None:
            error = task_state.fail(exc)
            report = completion.get("report", report)
    if task_state is not None:
        task_state.join()  # Also drains threads after startup/parent failures.
        if task_state.error is not None:
            error, propagate, outcome = task_state.error, True, "failed"
            # Receipts are engine-owned, not attributes on a supplied error.
            # Rebuild across all passes if a failure bypassed a fleet merge.
            partials = task_state.reports()
            report = dict(report if isinstance(report, dict)
                          else (partials[0] if partials else {}))
            report.update(root=str(root), series=[], totals={}, cancelled=False,
                          per_port={}, aborted=error_detail(error))
            for partial in partials:
                port = partial.get("port")
                report = _fold_worker_report(report, partial, port)
                report["per_port"][str(port)] = _fold_worker_report(
                    report["per_port"].get(str(port)), partial, port)
        if isinstance(report, dict) and coordinator is not None:
            for key, method in (("spot_probe", "result"),
                                ("vol_value_reconcile", "reconcile_result")):
                try:
                    def snapshot():
                        import json
                        value = getattr(coordinator, method)()
                        return json.loads(json.dumps(value, allow_nan=False))
                    report[key] = fib.without_authority(snapshot)()
                except BaseException as exc:
                    report[key + "_error"] = error_detail(exc)
                    error, propagate, outcome = task_state.fail(exc), True, "failed"
                    report["aborted"] = error_detail(error)
    if error is None:
        try:
            operation.seal()  # All explicitly tracked children must have joined.
        except LedgerError as exc:
            error, outcome = exc, "failed"
            if task_state is not None:
                # Fixed-task completion must not return success after its
                # evidence failed. Keep this primary if report writing fails.
                error, propagate = task_state.fail(exc), True
                if isinstance(report, dict):
                    report["aborted"] = error_detail(error)
        except BaseException as exc:
            error, propagate, outcome = exc, True, "failed"
    try:
        operation.close()
    except BaseException as exc:
        error, propagate, outcome = error if error is not None else exc, True, "failed"
    evidence = freport.evidence(context, purpose, outcome=outcome, error=error)
    try:
        path = freport.write_report(evidence, evidence_dir)
        evidence["report_path"] = path
        if isinstance(report, dict):
            report["fetch_ledger"] = evidence
            _write_parallel_report(root, report, stage="operation close")
    except BaseException as exc:
        if task_state is None:
            raise
        error = task_state.fail(exc)
        propagate = True
        evidence.update(verified=False, state="UNVERIFIED", outcome="failed",
                        report_failure=error_detail(exc))
        if isinstance(report, dict):
            report["fetch_ledger"] = evidence
    if propagate:
        def attach_failure():
            for name, value in (("fetch_ledger", evidence), ("fetch_report", report)):
                try:
                    setattr(error, name, value)
                except BaseException:
                    pass
        fib.without_authority(attach_failure)()
        raise error
    return report


def _fetch_run_gate(func):
    """Hold the fetch operation gate for one complete logical batch run."""
    @wraps(func)
    def guarded(*args, **kwargs):
        from operation_gate import acquire
        with acquire("fetch", owner=f"{func.__name__} run"):
            return func(*args, **kwargs)

    guarded._holds_fetch_run_gate = True
    return guarded


def _operation_directory(root, evidence_dir):
    return Path(evidence_dir) if evidence_dir is not None else Path(root) / "_ingest_reports" / "fetch-ledgers"


@_fetch_run_gate
def nightly_gap_fill(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("nightly", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "serial", args, kwargs, evidence_dir)


@_fetch_run_gate
def nightly_gap_fill_parallel(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("nightly", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "parallel", args, kwargs, evidence_dir)


@_fetch_run_gate
def nightly_gap_fill_parallel_resilient(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("nightly", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "resilient", args, kwargs, evidence_dir)


@_fetch_run_gate
def nightly_update(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("nightly", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "update", args, kwargs, evidence_dir)


@_fetch_run_gate
def add_stocks_gap_fill(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("add_stocks", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "serial", args, kwargs, evidence_dir)


@_fetch_run_gate
def add_stocks_gap_fill_parallel_resilient(root, *args, evidence_dir=None, _test_capability=None, **kwargs):
    operation = fops.begin_operation("add_stocks", _operation_directory(root, evidence_dir),
                                     test_capability=_test_capability)
    return _run_fetch_operation(operation, root, "resilient", args, kwargs, evidence_dir)


def _nightly_update_body(operation, root, selections, ports, *, preflight_factory=None, **kwargs):
    """GUI-independent nightly preflight and fill under the same parent."""
    progress, cancel = kwargs.get("progress"), kwargs.get("cancel")
    checked = validate_symbols(sorted({ticker for ticker, _ in selections}),
        adapter_factory=preflight_factory, progress=progress, cancel=cancel,
        _fetch_child=operation.child("nightly-preflight", rights={"ibkr.choke.qualify_many"}))
    if checked.get("cancelled"):
        return {"series": [], "totals": {}, "cancelled": True,
                "aborted": "nightly preflight cancelled", "identity_preflight": checked}
    # A logged-out port must still reach the existing restart orchestration.
    # Only validate_symbols' handled operational errors are best-effort;
    # authority/ledger failures propagate before any fill can start.
    if checked.get("error") and progress is not None:
        fib.without_authority(progress)(
            f"Identity preflight unavailable: {checked['error']}; continuing recovery")
    identities = check_security_ids(root, checked.get("resolved") or {})
    mismatched = {value["symbol"] for value in identities.values() if value.get("status") == "mismatch"}
    for value in identities.values():
        if value.get("status") == "mismatch" and progress is not None:
            fib.without_authority(progress)(f"SECURITY ID MISMATCH: {value['symbol']} stored="
                f"{value['stored']} != IBKR now={value['current']} - SKIPPED")
    selected = [(ticker, interval) for ticker, interval in selections if ticker not in mismatched]
    if not selected:
        return {"series": [], "totals": {}, "identity_preflight": checked,
                "notes": ["all series excluded: security-id mismatch"]}
    report = gap_fill_parallel_resilient(root, selected, ports, _fetch_parent=operation, **kwargs)
    report["identity_preflight"] = checked
    return report


@_fetch_run_gate
@fib.worker_scope
def gap_fill(root, selections, progress=None, cancel=None,
             adapter_factory=None, pacer=None, today=None,
             since=None, spot_check=False, spot_rng=None, pause=None,
             resolved=None, pipeline=False, run_id=None, on_series=None,
             manifest_locks=None, month_ranges=None, on_series_start=None,
             post_pipeline=None, engine_post_tasks=False,
             _pickup_plan_ahead=None, _pickup_after_last=None,
             _pickup_resolved=None, _pickup_head_cache=None,
             _fetch_state=None, _fetch_completion=None):
    """Fill the forward gap for each (ticker, interval) in `selections`.

    ``spot_check`` and ``spot_rng`` are retained as no-op compatibility
    arguments. WS8 now owns all post-repair spot probes.
    Returns a run report dict (saved to _ingest_reports/<run>/ like
    Tier 1). Per-series problems halt THAT series with a reason; the
    run always completes and reports. `run_id` is auto-stamped unless a
    caller (e.g. gap_fill_parallel's per-account workers) pins a unique one."""
    import addstock_fetch_tasks as post_tasks
    task_state = post_tasks.checked_state(_fetch_state, _fetch_completion)
    if engine_post_tasks and task_state is None:
        raise RequestRefused("fixed post tasks require engine-owned lifecycle state")
    if task_state is not None:
        cancel = post_tasks._StopCancel(cancel, task_state)
    root = Path(root)
    # F-DIAG-1: the diagnostics log lives in the PROJECT-level Run Logs (next
    # to the per-run logs), never inside the data bank that `root` points at.
    set_diag_log(_connection_diag_path(root))
    run_id = run_id or f"ibkr-{datetime.now():%Y%m%d-%H%M%S}"
    dirs = ingest._RunDirs(root, run_id)
    pacer = pacer or (_WorkerPacer() if fib.current_worker() else _default_pacer())
    today = today or now_ny().date()     # throttle, don't trip IBKR
    if _pickup_plan_ahead is False:       # date-split siblings keep the
        pickup_plan_ahead = None          # pre-M1 canonical planning path
    else:
        pickup_plan_ahead = (_pickup_plan_ahead
                             if _pickup_plan_ahead is not None
                             else _PickupPlanAhead(root, today))
    report = {"run": run_id, "root": str(root), "series": [],
              "totals": {}, "cancelled": False, "spot_checks_run": 0,
              "started": datetime.now().isoformat(timespec="seconds")}
    if _fetch_completion is not None:
        _fetch_completion["report"] = report
    if task_state is not None:
        task_state.keep_report(report)
    terminal = None

    def say(msg):
        if progress is not None:
            try:
                progress(msg)
            except Exception:  # noqa: BLE001
                pass

    conflict_records = []
    conflict_counts = {}

    def conflict_sink(ticker, interval, month, e, b):
        key = (ticker, interval, month)
        conflict_counts[key] = conflict_counts.get(key, 0) + 1
        if conflict_counts[key] <= ingest.CONFLICT_LOG_CAP_PER_MONTH:
            conflict_records.append((key, e, b))

    if not _disk_preflight(root, len(selections), say, report):
        return _finalize(report, dirs, say)
    say("Connecting to TWS/Gateway…")
    _prevent_sleep()                  # keep the machine awake for the whole run
    # Pause is consumed at the between-series, month-commit, and clean
    # no-data-session checkpoints, not at the request level.
    pacer._pause = pause
    try:
        adapter = (adapter_factory or live_adapter_factory())()
    except (ConnectionError, OSError, ImportError) as exc:
        _allow_sleep()
        pacer._pause = None
        report["aborted"] = str(exc)
        return _finalize(report, dirs, say)
    except BaseException as exc:
        if task_state is not None:
            terminal = task_state.fail(exc)
            report["aborted"] = post_tasks.error_detail(terminal)
            try:
                _allow_sleep()
            finally:
                pacer._pause = None
            raise terminal
        raise

    try:
        report["account"] = fib.without_authority(lambda: adapter.account())()
        report["port"] = fib.without_authority(getattr)(adapter, "port", None)
        # A2 + detection pre-pass: resolve EVERY symbol in one batched
        # qualifyContracts sweep (reqContractDetails is OFF the 60/10-min
        # historical budget). This pins fresh tickers up front so no
        # per-series qualify runs, AND re-checks already-pinned tickers
        # for a conId divergence (symbol reuse / relisting), surfaced as a
        # per-series warning note. A batch hiccup is non-fatal — we fall
        # back to per-series qualify.
        tickers = sorted({t for t, _ in selections})
        if resolved is None:
            # no map from the caller — resolve here. (The GUI's add-stock
            # flow already validated these symbols and passes that map in,
            # so we SKIP this and avoid a second "Checking … at IBKR" pass.)
            resolved = {}
            if tickers and hasattr(adapter, "qualify_many"):
                say(f"Resolving {len(tickers)} contract(s) at IBKR…")
                try:
                    if _pickup_resolved is None:
                        with fib.qualification_scope("qualify_many", tickers, lambda:
                                _wait_turn_with_status(pacer, cancel, metered=False, say=say),
                                cancel=cancel, on_wait=_pacing_wait_observer(say)):
                            resolved = adapter.qualify_many(tickers, progress=progress)
                    else:
                        for ticker in tickers:
                            def _resolve_one(_ticker=ticker):
                                with fib.qualification_scope("qualify_many", [_ticker], lambda:
                                        _wait_turn_with_status(pacer, cancel, metered=False, say=say),
                                        cancel=cancel, on_wait=_pacing_wait_observer(say)):
                                    found = adapter.qualify_many([_ticker], progress=progress)
                                return _PickupResolvedMap.mapping_value(
                                    found, _ticker)

                            conid, _reused = _pickup_resolved.resolve(
                                ticker, _resolve_one, cancel=cancel)
                            resolved[ticker] = conid
                except Cancelled:
                    report["cancelled"] = True
                except (ConnectionError, SeriesHalt) as exc:
                    report.setdefault("notes", []).append(
                        f"batch contract pre-pass failed ({exc}) — falling "
                        f"back to per-series qualify")
                    resolved = {}
        report["prepass_resolved"] = sum(
            1 for t in tickers if resolved.get(t) is not None)
        prepass_unresolved = sorted(
            t for t in tickers if resolved.get(t) is None)
        if prepass_unresolved:
            report["prepass_unresolved"] = prepass_unresolved

        # Lazy extended-hours cache: the full {base, base-pre, base-post} group
        # prefetches each token under its OWN envelope. Keep only one ticker's
        # cache, discard it after the last member, and fall back to independent
        # per-series requests on failure. No cross-token raw response is shared.
        _g = {}
        for t, iv in selections:
            if ss.kind_of(iv):                 # IV/HVOL/BID_ASK never combine —
                continue                       # the combined extended fetch is
            _g.setdefault((t, ss.base_interval(iv)), []).append(iv)   # TRADES-only
        combine_groups = {}                    # (ticker, base) -> [the 3 ivs]
        for (t, b), ivs in _g.items():
            full = [b, ss.with_session(b, "pre"), ss.with_session(b, "post")]
            if (all(x in ivs for x in full)
                    and b in _BAR_SIZES and _BAR_SIZES[b][1] is None):
                combine_groups[(t, b)] = full
        def _combined_member(ticker, interval):
            group = (ticker, ss.base_interval(interval))
            return (group in combine_groups
                    and interval in combine_groups[group])

        group_last_idx = {}                    # last 1-based member index/group
        for j, (t, iv) in enumerate(selections, 1):
            if _combined_member(t, iv):
                group_last_idx[(t, ss.base_interval(iv))] = j
        combined_cache = {}                    # (ticker, base) -> cache | None
        combined_boundaries = {}               # group -> one-shot successor plan
        pickup_head_cache = (_pickup_head_cache
                             if _pickup_head_cache is not None
                             else _PickupHeadCache())
        earliest_seen = set()                  # tickers captured this run
        earliest_cached = {}                   # strict dates still feed GUI hooks
        try:                                   # pre-seed with already-known ones so
            import stock_validate as _sv       # the (sometimes slow) head/probe runs
            _earliest_raw = _sv.load_ibkr_earliest(
                root, include_identity=True)
            for _ticker in tickers:
                _manifest = ss.load_manifest(Path(root) / _ticker)
                if not _pickup_sidecar_authoritative(
                        root, _ticker, _manifest, "TRADES"):
                    continue
                _expected = _pickup_identity_conid(
                    _manifest, resolved, _ticker)
                _cached_earliest = _strict_pickup_cache_date(
                    _earliest_raw, _ticker, today, _expected)
                if _cached_earliest is not None:
                    _ticker_upper = str(_ticker).upper()
                    earliest_seen.add(_ticker_upper)
                    earliest_cached[_ticker_upper] = (
                        _cached_earliest.isoformat())
        except Exception:  # noqa: BLE001 — cache read is best-effort
            pass

        n_series = len(selections)

        def _one_shot_prepare(callback):
            if callback is None:
                return None
            fired = [False]

            def run_once():
                if fired[0]:
                    return
                fired[0] = True
                try:
                    callback()
                except Exception as exc:  # noqa: BLE001
                    report.setdefault("notes", []).append(
                        f"pickup pre-plan skipped ({exc})")

            return run_once

        def _prepare_target_after(position, current=None, skip_group=None):
            """Return an identity token and callback for the next useful plan.

            ``position`` is the current one-based selection index.  A combined
            prefetch has already prepared every member of its own three-session
            group, so its network-overlap callback skips those members and aims
            at the first still-unprepared logical successor.  Per-series
            callbacks do not skip: they retain correct pickup if the combined
            optimization falls back or valid group members are interleaved.
            """
            if pickup_plan_ahead is not None:
                for next_pos in range(position, n_series):
                    next_ticker, next_interval = selections[next_pos]
                    next_group = (next_ticker,
                                  ss.base_interval(next_interval))
                    if (skip_group is not None
                            and next_group == skip_group
                            and _combined_member(next_ticker, next_interval)):
                        continue
                    target = ("series", str(next_ticker).upper(),
                              str(next_interval))
                    if (current is not None
                            and target[1:] == (str(current[0]).upper(),
                                              str(current[1]))):
                        continue
                    return target, (lambda t=next_ticker, iv=next_interval:
                                    pickup_plan_ahead.prepare(t, iv))
            if ((pickup_plan_ahead is not None or position >= n_series)
                    and _pickup_after_last is not None):
                return ("after_last",), _pickup_after_last
            return None, None

        for idx, (ticker, interval) in enumerate(selections, 1):
            if task_state is not None:
                task_state.check()
            # batch-progress marker the GUI parses to drive a progress bar:
            # "[i/N] TICKER interval" at the start of every series.
            say(f"[{idx}/{n_series}] {ticker} {interval}")
            if on_series_start is not None:
                try:
                    on_series_start(ticker, interval)
                except Exception:  # noqa: BLE001 - observer never breaks a run
                    pass
            grp = (ticker, ss.base_interval(interval))
            is_combined_member = _combined_member(ticker, interval)
            if is_combined_member:
                if grp not in combined_boundaries:
                    group_target, group_prepare = _prepare_target_after(
                        idx, (ticker, interval), skip_group=grp)
                    combined_boundaries[grp] = {
                        "target": group_target,
                        "callback": _one_shot_prepare(group_prepare),
                    }
                group_info = combined_boundaries[grp]
                group_boundary = group_info["callback"]
                member_target, member_prepare = _prepare_target_after(
                    idx, (ticker, interval))
                before_last_request = (
                    group_boundary
                    if member_target == group_info["target"]
                    else _one_shot_prepare(member_prepare))
            else:
                group_boundary = None
                _target, prepare_after = _prepare_target_after(
                    idx, (ticker, interval))
                before_last_request = _one_shot_prepare(
                    prepare_after)
            if is_combined_member and grp not in combined_cache:
                # build the per-token caches for this ticker's full
                # extended set, lazily, at its first series.
                try:
                    combined_cache[grp] = _prefetch_combined(
                        root, adapter, pacer, ticker, grp[1],
                        combine_groups[grp], resolved, today, since,
                        cancel, say, report, month_ranges=month_ranges,
                        pickup_plan_ahead=pickup_plan_ahead,
                        pickup_head_cache=pickup_head_cache,
                        before_last_request=group_boundary)
                except Cancelled:
                    report["cancelled"] = True
                    report["series"].append(
                        {"ticker": ticker, "interval": interval,
                         "halt": "cancelled — committed months stay; re-run "
                                 "to resume"})
                    break
                except Exception as exc:       # noqa: BLE001 — per-series fallback
                    if task_state is not None and post_tasks.terminal_error(exc):
                        raise task_state.fail(exc)
                    report.setdefault("notes", []).append(
                        f"{ticker}: combined extended-hours prefetch failed "
                        f"({exc}) — fetching the three sessions independently")
                    combined_cache[grp] = None
            series_completion = {} if task_state is not None else None
            try:
                mr, owns_earliest = _month_range_spec(month_ranges, ticker,
                                                      interval)
                res = _fill_series(root, adapter, pacer, ticker,
                                   interval, run_id, progress, cancel,
                                   conflict_sink, today, since, resolved,
                                   pipeline,
                                   manifest_lock=(manifest_locks.get(ticker)
                                                  if manifest_locks else None),
                                   session_cache=(combined_cache.get(grp)
                                                  if is_combined_member else None),
                                   month_range=mr,
                                   date_split_owns_earliest=owns_earliest,
                                   pickup_plan_ahead=pickup_plan_ahead,
                                   before_last_request=before_last_request,
                                   pickup_head_cache=pickup_head_cache,
                                   **({"_fetch_state": task_state,
                                       "_fetch_completion": series_completion}
                                      if task_state is not None else {}))
            except SeriesHalt as exc:
                res = {"ticker": ticker, "interval": interval,
                       "halt": str(exc)}
            except Cancelled as exc:
                report["cancelled"] = True
                # exc.res carries the partial counters — a fresh dict
                # here reported '+0 rows' for months that were
                # physically committed before the cancel
                partial = (series_completion.get("result") if series_completion is not None
                           else getattr(exc, "res", None))
                report["series"].append(partial or
                                        {"ticker": ticker,
                                         "interval": interval,
                                         "halt": "cancelled — committed "
                                                 "months stay; re-run "
                                                 "to resume"})
                break
            except Exception as exc:  # noqa: BLE001 — a single bad or
                # UNFETCHABLE series (delisted, not found, a quirky
                # qualify error) must NEVER abort the rest of the batch:
                # halt THAT one and move on to the next stock
                partial = (series_completion.get("result") if series_completion is not None
                           else getattr(exc, "res", None))
                if task_state is not None and post_tasks.terminal_error(exc):
                    if partial is not None:
                        report["series"].append(partial)
                    raise task_state.fail(exc)
                res = partial or {
                    "ticker": ticker, "interval": interval,
                    "halt": f"series failed: {exc}"}
            except BaseException as exc:
                if task_state is not None:
                    partial = series_completion.get("result")
                    if partial is not None:
                        report["series"].append(partial)
                    raise task_state.fail(exc)
                raise
            if before_last_request is not None:
                before_last_request()             # zero-day/early-halt fallback
            report["series"].append(res)
            # Capture the earliest IBKR-servable date ONCE per ticker (auto, for
            # the main-table 'Earliest on IBKR' column) and stash it on `res` so
            # the on_series hook below can persist it. head/probe only — never a
            # pre-listing request, so it can't reintroduce the HOOD-style hang.
            # Gated on on_series (only the GUI consumes it), so the offline tests
            # pay no extra request; try/except-guarded so it NEVER breaks a run.
            _tkU = str(ticker).upper()
            is_date_split_chunk = bool((res or {}).get("date_split"))
            pickup_head_attempted = bool(
                (res or {}).pop("_pickup_head_attempted", False))
            # A strict conId-bound cache hit intentionally suppresses another
            # live head probe, but the Add Stocks callback still needs that
            # already-proven date to settle its durable ``earliest`` credit.
            # This matters after a hard-death re-adoption: the interrupted
            # worker may have persisted the cache before its callback ran.
            if not res.get("halt") and _tkU in earliest_cached:
                res.setdefault("ibkr_earliest", earliest_cached[_tkU])
            if on_series is not None and not res.get("halt") \
                    and not is_date_split_chunk \
                    and _tkU not in earliest_seen:
                earliest_seen.add(_tkU)
                if not pickup_head_attempted:
                    try:
                        _wait_turn_with_status(
                            pacer, cancel, metered=False,
                            say=say)  # gap it like the
                        # normal head path (head/daily bars are off HMDS budget)
                        _ed = earliest_available(
                            adapter, ticker, today, cancel=cancel)
                        if _ed is not None:
                            res["ibkr_earliest"] = _ed.isoformat()
                            _bound = _pickup_identity_conid(
                                ss.load_manifest(Path(root) / ticker),
                                resolved, ticker)
                            if _bound is not None:
                                _record_pickup_evidence(
                                    root, ticker, _ed, _bound,
                                    cache_start=_ed)
                    except Exception as exc:  # noqa: BLE001 — never break the run
                        if task_state is not None and post_tasks.terminal_error(exc):
                            raise task_state.fail(exc)
                        pass
            # INGEST-TIME cross-check hook: after a series is committed, let the
            # caller validate it against the online reference and decide whether
            # to keep fetching. Returning "stop" halts the run (sets cancel so a
            # parallel sibling worker stops too). Never lets a hook error abort
            # the run. Skipped for a halted/failed series (no good data to check).
            if on_series is not None and not res.get("halt") \
                    and not is_date_split_chunk \
                    and not report.get("cancelled"):
                try:
                    stop_requested = on_series(ticker, interval, res) == "stop"
                except Exception:  # noqa: BLE001 — cross-check never breaks a run
                    stop_requested = False
                # Hook errors are best-effort; a failure to propagate a requested
                # stop is not. Never swallow it and continue to another series.
                if stop_requested:
                    report["validation_stopped"] = True
                    if cancel is not None:
                        cancel.set()
                    break
            # free the group's shared cache after its LAST series so only ONE
            # ticker's payload is held in memory at a time (not all of them).
            if (is_combined_member
                    and idx >= group_last_idx.get(grp, 0)):
                combined_cache.pop(grp, None)
                combined_boundaries.pop(grp, None)
        if post_pipeline is not None:
            # Publish every terminal fill row even when a safe Pause->Cancel
            # ends the run here. The coordinator then reports exact pending
            # audit/reconcile/check debt instead of a false zero-work success.
            post_pipeline.record_series(report.get("series") or [])
        if (post_pipeline is not None and not report.get("cancelled")
                and not (cancel is not None and cancel.is_set())):
            # Serial Add Stocks uses the same post-ticker coordinator as the
            # parallel fleet. Keep it inside this decorated fetch run so its
            # audit/refetch owns the same operation lease and the same adapter.
            # Post tasks are atomic units. Pause is honored only between them;
            # clearing the request-level hook prevents a newly-set Pause from
            # stranding a row between source evidence and durable finalization.
            pacer._pause = None
            while not (cancel is not None and cancel.is_set()):
                try:
                    _wait_while_paused(
                        pause, cancel, say,
                        note="the current ticker task is durably complete",
                        on_pause=getattr(pacer, "_on_pause", None),
                        pause_info={"detail": "between ticker tasks"})
                except Cancelled:
                    break
                task = post_pipeline.claim()
                if task is None:
                    if post_pipeline.has_pending():
                        _time.sleep(0.02)
                        continue
                    break
                try:
                    if engine_post_tasks:
                        task_state.check()
                        post_tasks._run_addstock_task(root, post_pipeline, task, adapter, pacer,
                            cancel=cancel, progress=progress,
                            _fetch_child=fops.narrow_worker_child(fib.current_worker(),
                                "addstock-post-" + uuid.uuid4().hex))
                    else:
                        fib.without_authority(post_pipeline.execute)(task, adapter, pacer)
                except Exception as exc:  # noqa: BLE001 - parent fill stays durable
                    if task_state is not None and post_tasks.terminal_error(exc):
                        raise task_state.fail(exc)
                    say(f"post-ticker task failed ({exc})")

        # A validator 'stop' (or a Cancel pressed in-flight) sets the cancel
        # event but NOT report['cancelled']; the spot-check phase must not run
        # with cancel already tripped — pacer.wait_turn would raise Cancelled,
        # which escapes gap_fill (no except here, only finally) and skips the
        # totals roll-up / conflict log / _finalize, DISCARDING the whole
        # report. Gate on the stop/cancel state, and guard the call so a Cancel
        # during the audit still falls through to finalize.
    except BaseException as exc:
        if task_state is None:
            raise
        terminal = task_state.fail(exc) if post_tasks.terminal_error(exc) else exc
        report["aborted"] = post_tasks.error_detail(terminal)
    finally:
        try:
            fib.without_authority(lambda: adapter.disconnect())()
        except BaseException as exc:
            if task_state is not None and post_tasks.terminal_error(exc):
                terminal = task_state.fail(exc)
                report["aborted"] = post_tasks.error_detail(terminal)
            elif not isinstance(exc, Exception):
                raise
        finally:
            _allow_sleep()
            pacer._pause = None

    t = report["totals"]
    for k in ("added", "dup_existing", "conflicts", "written",
              "requests", "bars_fetched"):
        t[k] = sum(r.get(k, 0) for r in report["series"])
    # the combined extended-hours prefetch's IBKR requests aren't attributed to
    # any single series (it serves all three) — count them in the run total.
    t["requests"] += report.pop("_prefetch_requests", 0)
    t["halted_series"] = sum(1 for r in report["series"] if r.get("halt"))
    # WRITE FAILED months: _commit_month swallows a StorageError/OSError (disk
    # full, file locked) into a per-month "WRITE FAILED: …" status. Tally them so
    # a systemic mid-run write failure shows at run level, not just buried in
    # report.json. summarize_report turns a nonzero count into a loud line.
    t["write_failed"] = sum(
        1 for r in report["series"]
        for mst in (r.get("months") or {}).values()
        if str((mst or {}).get("status", "")).startswith("WRITE FAILED"))
    if conflict_records:
        try:
            import json
            dest = dirs.reports() / "conflicts.jsonl"
            with open(dest, "w", encoding="utf-8") as fh:
                for (tic, iv, month), e, b in conflict_records:
                    fh.write(json.dumps(
                        {"ticker": tic, "interval": iv, "month": month,
                         "ts": f"{ss.format_date(e[0])} "
                               f"{ss.format_time(e[0].time())}",
                         "existing": list(e[1:]),
                         "incoming": list(b[1:])}) + "\n")
            report["conflict_log"] = str(dest)
        except OSError as exc:
            report["conflict_log_note"] = f"not written: {exc}"
    try:
        result = _finalize(report, dirs, say)
    except BaseException:
        if terminal is not None and post_tasks.terminal_error(terminal):
            raise terminal
        raise
    if task_state is not None:
        task_state.check()
    if terminal is not None:
        raise terminal
    return result


def _finalize(report, dirs, say):
    import json
    try:
        payload = json.dumps(report, indent=1, sort_keys=True,
                             default=str).encode("utf-8")
        dest = dirs.reports() / "report.json"
        ss._atomic_write_bytes(dest, payload)
        report["report_path"] = str(dest)
    except (OSError, ss.StorageError, TypeError) as exc:
        report["report_path"] = None
        report.setdefault("notes", []).append(f"report not saved: {exc}")
    say("IBKR update finished.")
    return report


def partition_tickers(selections, ports, weights=None, allow_split=False):
    """Assign tickers to ports. Returns {port: [(ticker, interval), …]}.

    Default: WHOLE tickers (all a ticker's intervals on one port, so one worker
    owns its files + manifest) — at most #distinct-tickers ports work. With
    `weights` (ticker -> estimated work) it BALANCES BY WORK via greedy longest-
    processing-time bin-packing (heaviest ticker -> least-loaded port), so one
    port doesn't grind a big backfill while another idles; deterministic given
    the same weights so recovery gets the identical partition. Without weights,
    round-robin over sorted symbols.

    INTERVAL-SPLIT (`allow_split=True` AND fewer distinct tickers than ports AND
    some ticker has >1 series): a ticker's individual SERIES are spread across
    ports (LPT by per-series weight) so idle ports get work — e.g. 2 tickers ×
    {1m, 1m-pre, 1m-post} fills 5 ports instead of 2. Same-ticker series on
    different ports write DIFFERENT interval files but share one manifest, so
    gap_fill_parallel hands each worker a per-ticker lock (merge-on-save) to
    keep that manifest consistent. Ports are de-duplicated (order preserved)."""
    ports = list(dict.fromkeys(int(p) for p in ports))
    by_ticker = {}
    for t, iv in selections:
        by_ticker.setdefault(t, []).append((t, iv))
    chunks = {p: [] for p in ports}
    if not ports:
        return chunks
    if (allow_split and len(by_ticker) < len(ports)
            and len(selections) > len(by_ticker)):
        # split INDIVIDUAL series across ports (LPT). A ticker's weight is
        # shared evenly across its series so heavy tickers still spread out.
        load = {p: 0 for p in ports}
        units = []
        for t, series in by_ticker.items():
            w = (weights.get(t) if weights else None) or len(series)
            per = max(1, (w / len(series)))
            for u in series:
                units.append((u, per))
        for u, w in sorted(units, key=lambda x: (-x[1], x[0])):
            p = min(ports, key=lambda p: (load[p], p))
            chunks[p].append(u)
            load[p] += w
        return chunks
    if weights:
        load = {p: 0 for p in ports}
        # heaviest first; ties broken by symbol so the result is deterministic.
        for t in sorted(by_ticker, key=lambda t: (-(weights.get(t) or 1), t)):
            p = min(ports, key=lambda p: (load[p], p))    # least-loaded port
            chunks[p].extend(by_ticker[t])
            load[p] += max(1, weights.get(t) or 1)
    else:
        for i, t in enumerate(sorted(by_ticker)):
            chunks[ports[i % len(ports)]].extend(by_ticker[t])
    return chunks


def _ticker_weights(root, selections, today=None):
    """Offline per-ticker work estimate (sum of gap requests across its
    intervals) for the work-balanced partition. A brand-new full backfill has
    no stored data to size from, so it's weighted HEAVY (so several new builds
    spread across ports instead of piling on one). Best-effort — a bad probe
    just weighs 1."""
    weights = {}
    for t in {tk for tk, _iv in selections}:
        w = 0
        for tk, iv in selections:
            if tk != t:
                continue
            try:
                pl = plan_gap(root, tk, iv, today=today)
                w += 5000 if pl.get("empty_series") else (pl.get("est_requests")
                                                          or 1)
            except Exception:  # noqa: BLE001
                w += 1
        weights[t] = max(1, w)
    return weights


def _fold_worker_report(acc, r, port):
    """Accumulate one job's gap_fill report `r` into a dynamic-queue worker's
    RUNNING report `acc` (same port, several jobs on one connection): sum totals,
    concatenate series, carry account + the cancelled/validation/abort flags.
    Returns the (mutated) acc — its shape matches what gap_fill_parallel's merge
    expects from a single-call worker, so the merge stays unchanged. Pure aside
    from mutating acc; unit-testable without threads."""
    if acc is None:
        acc = {"series": [], "totals": {}, "cancelled": False, "port": port,
               "spot_checks_run": 0, "account": None}
    acc["series"].extend(r.get("series", []) or [])
    for k, v in (r.get("totals") or {}).items():
        acc["totals"][k] = acc["totals"].get(k, 0) + (v or 0)
    acc["spot_checks_run"] = (acc.get("spot_checks_run", 0)
                              + (r.get("spot_checks_run") or 0))
    if r.get("account") and not acc.get("account"):
        acc["account"] = r.get("account")
    if r.get("cancelled"):
        acc["cancelled"] = True
    if r.get("validation_stopped"):
        acc["validation_stopped"] = True
    if r.get("notes"):
        acc.setdefault("notes", []).extend(r["notes"])
    if r.get("aborted"):
        acc["aborted"] = r["aborted"]      # a dead connection on this port
    return acc


def _adaptive_decide(n_active, retrying_now, recon_in_window, clean_for,
                     cooling, parked, active_working):
    """Pure AIMD decision for the adaptive-concurrency controller. Kept
    side-effect-free so it unit-tests without threads. Returns one of:

        'release' | 'drop2' | 'drop1' | 'add' | 'none'

    n_active        active (un-parked, un-finished) ports right now
    retrying_now    of those, how many are in 'retrying' this instant
    recon_in_window reconnect events across active ports in ADAPT_WINDOW_S
    clean_for       seconds since the last retry/drop (0 while dirty)
    cooling         True while inside the post-change cooldown
    parked          # ports currently parked but not yet finished
    active_working  # active ports still doing work (not done/error)

    'release' overrides cooldown and the floor: it means nothing active is
    still working but parked work remains, so we MUST resume to avoid a
    join() deadlock at the tail. Drop is multiplicative-ish & fast; add is
    additive & slow (asymmetric on purpose — slow to re-load the backend)."""
    import math
    if active_working == 0 and parked > 0:
        return "release"                       # forced progress — never deadlock
    if cooling:
        return "none"
    # a single hiccup never trips a drop alone: need >=2 ports struggling at
    # once (the backend-choke signature) OR the steadier reconnect-rate mark.
    drop_mark = max(2, math.ceil(n_active * ADAPT_DROP_RETRY_FRAC))
    severe = retrying_now >= drop_mark and recon_in_window >= ADAPT_SEVERE_RATE
    choke = retrying_now >= drop_mark or recon_in_window >= ADAPT_DROP_RATE
    if choke and n_active > ADAPT_MIN_ACTIVE:
        if severe and n_active - 2 >= ADAPT_MIN_ACTIVE:
            return "drop2"
        return "drop1"
    if parked > 0 and clean_for >= ADAPT_ADD_CLEAN_S:
        return "add"
    return "none"


@_fetch_run_gate
def gap_fill_parallel(root, selections, ports, progress=None, cancel=None,
                      pause=None, today=None, since=None, pipeline=False,
                      resolved=None, port_status=None, spot_check=False,
                      spot_rng=None, host=HOST_DEFAULT, on_series=None,
                      adaptive=False, adapter_factory=None, port_up=None,
                      reprobe_interval=45.0, allow_date_split=False,
                      date_split_min_months=DATE_SPLIT_MIN_MONTHS,
                      post_pipeline=None, on_series_start=None, engine_post_tasks=False,
                      hard_death_watchdog=False, maintenance_path=None,
                      watchdog_factory=None, restart_ports=None, _fetch_parent=None,
                      _fetch_state=None, _fetch_completion=None):
    """N-way gap_fill: one worker per port, sharing the process-wide A1 pacing
    governor, over a TICKER-disjoint partition of `selections` (all of a
    ticker's intervals go to one worker, so no two workers ever touch the same
    file/manifest). Each worker reuses the serial gap_fill with a pinned
    single-port adapter and a unique run_id. Falls back to serial gap_fill for
    fewer than 2 ports unless `post_pipeline` needs that worker lifecycle.
    A post pipeline may claim lower-priority ticker check/probe tasks only when
    no fill job is waiting; it borrows the worker's adapter and pacer. Returns
    one merged report (gap_fill report shape)."""
    import re
    import threading
    import addstock_fetch_tasks as post_tasks
    task_state = post_tasks.checked_state(_fetch_state, _fetch_completion)
    if engine_post_tasks and task_state is None:
        raise RequestRefused("fixed post tasks require engine-owned lifecycle state")
    caller_cancel = cancel
    # Shared engine stop, including when the caller supplied no event or its
    # setter fails. Keep caller_cancel raw for final user-cancel classification.
    cancel = _WorkerCancel(cancel)
    if task_state is not None:
        cancel = post_tasks._StopCancel(cancel, task_state)
    root = Path(root)
    fetch_context = fops.parent_context(_fetch_parent)
    fetch_pass = uuid.uuid4().hex
    today = today or fetch_context.captured_now.date()
    # F-DIAG-1: project-level Run Logs, never inside the data bank (see gap_fill).
    set_diag_log(_connection_diag_path(root))
    ports = list(dict.fromkeys(int(p) for p in ports))     # de-dup, keep order
    # adapter_factory mirrors live_adapter_factory's (host, ports)->make()
    # contract; injectable so the adaptive controller (which otherwise only ever
    # builds a LIVE adapter) can be exercised against a fault-injecting fake.
    _mk = adapter_factory or live_adapter_factory
    watchdog = None
    if hard_death_watchdog and ports:
        provider = None
        if maintenance_path is not None:
            provider = lambda: addstock_watchdog.maintenance_status(
                maintenance_path)
        if watchdog_factory is None:
            watchdog = addstock_watchdog.HardDeathWatchdog(
                ports, maintenance_provider=provider)
        else:
            watchdog = fib.observer_object(watchdog_factory(
                ports, maintenance_provider=provider))
    if post_pipeline is not None and allow_date_split:
        raise ValueError("post-ticker pipeline does not support date splitting")
    if len(ports) < 2 and post_pipeline is None:
        af = fib.without_authority(_mk(host, tuple(ports) or PORTS_DEFAULT))
        return gap_fill(root, selections, progress, cancel, af, None,
                        today, since, spot_check, spot_rng, pause, resolved,
                        pipeline, on_series=on_series,
                        on_series_start=on_series_start,
                        **({"_fetch_state": task_state, "_fetch_completion": _fetch_completion}
                           if task_state is not None else {}),
                        _fetch_child=_fetch_parent.child(
                            f"pass-{fetch_pass}.serial-{ports[0] if ports else 'default'}"))

    # DYNAMIC WORK QUEUE — instead of a STATIC per-port partition (where a port
    # that finishes its share sits idle while another grinds the rest), every
    # port pulls whole-ticker JOBS from ONE shared queue until it's empty, so all
    # ports keep working to the very tail. A job is one TICKER (all its intervals
    # together) -> the combined extended fetch still sees rth+pre+post in one
    # gap_fill, and a ticker is never on two ports at once (manifests never
    # collide, no lock needed). When there are FEWER tickers than ports, jobs are
    # single SERIES instead so the spare ports still get work (same-ticker series
    # may then run concurrently -> a per-ticker manifest lock keeps merge-saves
    # safe). Heaviest jobs first (LPT list-scheduling) so a big backfill starts
    # early and isn't the lone tail. This DYNAMIC scheme strictly beats the old
    # static LPT partition: it balances by ACTUAL completion, not an offline
    # weight estimate, so a port is never idle while any job remains.
    import collections
    n_tickers = len({t for t, _i in selections})
    split = n_tickers < len(ports) and len(selections) > n_tickers
    weights = _ticker_weights(root, selections, today=today)
    pickup_plan_ahead = _PickupPlanAhead(root, today)
    by_ticker = {}
    for t, iv in selections:
        by_ticker.setdefault(t, []).append((t, iv))
    date_split_active = False
    # Durable for this run and preserved when the SAME job dict is re-queued.
    # Do not use id(job): addresses can be reused; distinct date-split jobs
    # intentionally get distinct identities even when their series repeat.
    next_job_seq = [0]

    def _job(series, weight, month_ranges=None, date_split=False):
        next_job_seq[0] += 1
        return {"seq": next_job_seq[0],
                "series": list(series), "weight": max(1, weight or 1),
                "month_ranges": month_ranges or {},
                "date_split": bool(date_split)}

    if allow_date_split:
        spare = len(ports) - len(by_ticker)
        if spare > 0:
            jobs = []
            for t in sorted(by_ticker, key=lambda x: (-(weights.get(x) or 1), x)):
                series = list(by_ticker[t])
                split_done = False
                if spare > 0:
                    union_days = set()
                    eligible = _date_split_series_safe(series)
                    for _tk, iv in series:
                        if not eligible:
                            break
                        try:
                            pl = plan_gap(root, _tk, iv, today=today)
                        except Exception:  # noqa: BLE001
                            eligible = False
                            break
                        if pl.get("error") or pl.get("empty_series"):
                            eligible = False
                            break
                        union_days |= set(pl.get("days") or [])
                    if eligible and union_days:
                        chunks = partition_series_by_months(
                            union_days, spare + 1, date_split_min_months)
                        if len(chunks) > 1:
                            total_days = sum(len(c["days"]) for c in chunks) or 1
                            for ci, c in enumerate(chunks):
                                mr = {(tk, iv): {"range": (c["lo"], c["hi"]),
                                                 "owns_earliest": ci == 0}
                                      for tk, iv in series}
                                w = (weights.get(t) or 1) * len(c["days"]) / total_days
                                jobs.append(_job(series, w, mr, date_split=True))
                            spare -= len(chunks) - 1
                            date_split_active = True
                            split_done = True
                if not split_done:
                    jobs.append(_job(series, weights.get(t) or len(series)))
            if date_split_active:
                manifest_locks = {t: threading.Lock() for t in by_ticker}

    if not date_split_active and split:
        # one job per SERIES so the SPARE ports (more ports than tickers) still
        # get work — DELIBERATELY chosen over keeping a ticker's extended triple
        # together: filling the fleet matters more here than the combined
        # token-scoped prefetch (which only fires when one gap_fill sees all of
        # {base,pre,post}). The combined fetch is preserved for the common
        # whole-ticker path below; in split mode the three sessions fetch
        # independently (a few extra requests on a handful of stocks, parallelised
        # across the otherwise-idle ports). Per-ticker lock keeps merge-saves safe.
        jobs = [_job([s], 1) for s in selections]
        manifest_locks = {t: threading.Lock() for t in by_ticker}

        def _job_weight(job):                        # share a ticker's weight
            t = job["series"][0][0]
            return (weights.get(t) or 1) / max(1, len(by_ticker[t]))
    else:
        if not date_split_active:
            jobs = [_job(by_ticker[t], weights.get(t) or 1)
                    for t in by_ticker]   # one job per TICKER

        def _job_weight(job):
            return job.get("weight") or weights.get(job["series"][0][0]) or 1
        if not date_split_active:
            manifest_locks = None
    series_split_active = bool(split and not date_split_active)
    if series_split_active or date_split_active:
        pickup_resolved = _PickupResolvedMap()
        pickup_head_cache = _PickupHeadCache(shared=True)
    else:
        pickup_resolved = None
        pickup_head_cache = None
    # heaviest first; ties broken by symbol+interval so it's deterministic.
    jobs.sort(key=lambda j: (-_job_weight(j), j["series"][0][0],
                             j["series"][0][1],
                             str(j.get("month_ranges") or "")))
    job_q = collections.deque(jobs)
    job_qlock = threading.Lock()
    active_fill = [0]
    port_jobs = {p: [] for p in ports}        # what each port ACTUALLY pulled
    watchdog_rerouted = set()

    def _take_job():
        with job_qlock:
            if not job_q:
                return None
            active_fill[0] += 1
            return job_q.popleft()

    def _preplan_queued_job():
        # Peek only: reserving the successor here would undo the dynamic
        # queue's load balancing and could strand work on a cancelled/dead
        # worker.  The eventual owner consumes this shared immutable payload.
        with job_qlock:
            candidate = job_q[0] if job_q else None
            if candidate is not None:
                candidate = dict(candidate)
                candidate["series"] = list(candidate.get("series") or [])
            if (candidate is None or candidate.get("date_split")
                    or not candidate["series"]):
                reservation = None
            else:
                next_ticker, next_interval = candidate["series"][0]
                # Publish only the tiny waitable slot under the queue lock.
                # The filesystem work happens after release.
                reservation = pickup_plan_ahead.reserve(
                    next_ticker, next_interval)
        pickup_plan_ahead.prepare_reserved(reservation)

    def _finish_job(job=None, requeue=False):
        with job_qlock:
            if requeue and job is not None:
                job_q.appendleft(job)
            active_fill[0] -= 1

    def _fill_work_remains():
        with job_qlock:
            return bool(job_q or active_fill[0])

    total = sum(len(j["series"]) for j in jobs)
    done = [0]
    # Bounded by `total`; guarded by `lock` together with the numerator.
    announced = set()
    lock = threading.Lock()
    series_re = re.compile(r"^\[\d+/\d+\]\s+(.*)$")

    def say(msg):
        if progress is not None:
            try:
                progress(msg)
            except Exception:  # noqa: BLE001
                pass

    # disk preflight — refuse a doomed run before any port connects (mirrors the
    # serial gap_fill / ingest gate). A soft shortfall only warns; on the hard
    # floor return an aborted report in gap_fill shape so summarize_report reads
    # it the same as a serial abort.
    pf = {"run": f"ibkr-{datetime.now():%Y%m%d-%H%M%S}", "root": str(root),
          "series": [], "totals": {}, "cancelled": False,
          "started": datetime.now().isoformat(timespec="seconds")}
    if not _disk_preflight(root, len(selections), say, pf):
        return _finalize(pf, ingest._RunDirs(root, pf["run"]), say)

    # Bounded per-port status (one entry per port, overwritten) for a live
    # monitor — pushed via port_status(port, info) on every heartbeat. ALL keys
    # are pre-seeded so later writes only REASSIGN values (never grow the dict),
    # so a concurrent dict() snapshot in emit can't hit "dict changed size".
    pinfo = {p: {"ticker": None, "count": 0, "state": "queued",
                 "account": None, "added": 0, "error": None,
                 "days_done": 0, "days_total": 0, "bars": 0,
                 "detail": "", "last_month": "",
                 "pacing_detail": "", "pacing_seconds": 0.0,
                 "pause_detail": "",
                 "watchdog_reason": None}
             for p in ports}
    emit_locks = {p: threading.Lock() for p in ports}
    post_meta = {p: {"key": None, "base": "", "since": 0.0,
                     "next": float("inf"), "revision": 0}
                 for p in ports}

    def emit(port):
        if port_status is not None:
            try:
                # Recapture only after this port's earlier publication has
                # finished.  A heartbeat delayed in the callback can never
                # deliver an old working snapshot after a terminal update:
                # the terminal emitter waits, then recaptures the latest row.
                with emit_locks[port]:
                    with lock:                     # consistent vs writers
                        snap = dict(pinfo[port])
                    port_status(port, snap)
            except Exception:  # noqa: BLE001
                pass

    # ---- adaptive concurrency state -----------------------------------
    # park[p] SET => the controller has "dropped" port p: its worker blocks at
    # the next request boundary (its effective pause OR's park[p]). recon_at[p]
    # is the monotonic time of p's LAST reconnect (distinct-port rate, NOT a raw
    # attempt count — one stuck port flaps up to 20x/episode). retry_since[p] is
    # when p ENTERED 'retrying' (for the >60s stuck-port swap), or None. With the
    # dynamic queue EVERY port gets a worker that pulls from the shared queue, so
    # all ports are live; one that pulls nothing (queue already drained) finishes
    # immediately as 'done' and the controller simply sees it stop working.
    done_states = ("done", "error")
    live_ports = list(ports)
    use_adaptive = bool(adaptive) and len(live_ports) >= 2
    park = {p: threading.Event() for p in ports}
    recon_at = {p: float("-inf") for p in ports}
    retry_since = {p: None for p in ports}

    def _set_state(port, st, parked_guard=False, force=False):
        """The ONE funnel for pinfo[*]['state'] writes — held under `lock` so
        the worker and controller can't race. Never resurrects a terminal
        done/error port, and with parked_guard never lets a worker overwrite a
        controller 'parked'. `force` (the worker's own terminal done/error) is
        authoritative: it writes unconditionally, but STILL under `lock`, so the
        controller's guarded read-check-write can't straddle it and clobber a
        just-finished port back to 'parked'/'working'."""
        with lock:
            if not force:
                if (watchdog is not None
                        and watchdog.interrupt_event(port).is_set()):
                    return
                cur = pinfo[port]["state"]
                if cur in done_states:
                    return
                if parked_guard and park[port].is_set():
                    return
            pinfo[port]["state"] = st

    def _post_snapshot():
        getter = (getattr(post_pipeline, "status_snapshot", None)
                  if post_pipeline is not None else None)
        if not callable(getter):
            return {}
        try:
            value = getter()
        except Exception:  # noqa: BLE001 - status is observational only
            return {}
        return dict(value) if isinstance(value, dict) else {}

    def _post_count(snapshot, key):
        try:
            return max(0, int(snapshot.get(key) or 0))
        except (TypeError, ValueError):
            return 0

    def _post_base_label(key, snapshot):
        mode, ticker = key
        if mode == "reconcile-plan":
            return f"volatility audit {ticker}"
        if mode == "reconcile":
            done = _post_count(snapshot, "reconcile_completed")
            total = _post_count(snapshot, "reconcile_planned")
            return f"volatility reconcile {ticker} ({done}/{total})"
        if mode == "check":
            return f"cross-check {ticker}"
        probe_done = _post_count(snapshot, "probe_done")
        probe_total = _post_count(snapshot, "probe_total")
        if mode == "probe":
            return f"spot-probe {ticker} ({probe_done}/{probe_total})"
        parts = []
        active_checks = _post_count(snapshot, "active_checks")
        active_probes = _post_count(snapshot, "active_probes")
        active_reconcile_plans = _post_count(
            snapshot, "active_reconcile_plans")
        active_reconciles = _post_count(snapshot, "active_reconciles")
        reconcile_plan_ready = _post_count(
            snapshot, "reconcile_plan_ready")
        reconcile_ready = _post_count(snapshot, "reconcile_ready")
        check_ready = _post_count(snapshot, "check_ready")
        probe_ready = _post_count(snapshot, "probe_ready")
        fill_waiting = _post_count(snapshot, "fill_waiting")
        if active_reconcile_plans:
            parts.append(f"{active_reconcile_plans} volatility audit"
                         f"{'s' if active_reconcile_plans != 1 else ''} running")
        if active_reconciles:
            parts.append(f"{active_reconciles} volatility reconcile"
                         f"{'s' if active_reconciles != 1 else ''} running")
        if active_checks:
            parts.append(f"{active_checks} check"
                         f"{'s' if active_checks != 1 else ''} running")
        if active_probes:
            parts.append(f"{active_probes} probe"
                         f"{'s' if active_probes != 1 else ''} running "
                         "(cap 2)")
        if check_ready:
            parts.append(f"{check_ready} check"
                         f"{'s' if check_ready != 1 else ''} queued")
        if probe_ready:
            parts.append(f"{probe_ready} probe"
                         f"{'s' if probe_ready != 1 else ''} queued")
        if reconcile_plan_ready:
            parts.append(f"{reconcile_plan_ready} volatility audit"
                         f"{'s' if reconcile_plan_ready != 1 else ''} queued")
        if reconcile_ready:
            parts.append(f"{reconcile_ready} volatility reconcile row"
                         f"{'s' if reconcile_ready != 1 else ''} queued")
        if fill_waiting:
            parts.append(f"{fill_waiting} ticker fill"
                         f"{'s' if fill_waiting != 1 else ''} left")
        if probe_total:
            parts.append(f"probes ({probe_done}/{probe_total})")
        if not parts:
            parts.append("post-check work in flight")
        return "waiting: " + " · ".join(parts)

    def _set_post_status(port, key):
        """Publish one semantic WS8 state without changing worker scheduling."""
        _set_state(port, "working", parked_guard=True)
        while True:
            with lock:
                if pinfo[port]["state"] != "working":
                    return
                expected_revision = post_meta[port]["revision"]
            snapshot = _post_snapshot()
            now = _time.monotonic()
            base = _post_base_label(key, snapshot)
            with lock:
                if pinfo[port]["state"] != "working":
                    return
                meta = post_meta[port]
                # Snapshotting the coordinator is deliberately outside the
                # worker-state lock.  If a heartbeat or another semantic
                # update published fresher evidence meanwhile, retry instead
                # of letting this older observation overwrite it.
                if meta["revision"] != expected_revision:
                    continue
                key_changed = meta["key"] != key
                base_changed = meta["base"] != base
                if key_changed:
                    meta["key"] = key
                    meta["since"] = now
                    meta["next"] = now + min(
                        _POST_STATUS_INITIAL_S,
                        _probe_wait_heartbeat_interval())
                if not key_changed and not base_changed:
                    return
                meta["revision"] += 1
                meta["base"] = base
                pinfo[port]["ticker"] = (
                    str(key[1]) if key[0] != "waiting" else "WS8")
                pinfo[port]["detail"] = (
                    f"{base} · {max(0, int(now - meta['since']))}s")
                pinfo[port]["pause_detail"] = ""
                break
        emit(port)

    def _clear_post_status(port, *, publish=True):
        with lock:
            meta = post_meta[port]
            changed_status = meta["key"] is not None
            meta["key"] = None
            meta["base"] = ""
            meta["since"] = 0.0
            meta["next"] = float("inf")
            if changed_status:
                meta["revision"] += 1
                pinfo[port]["detail"] = ""
        if changed_status and publish:
            emit(port)

    def _emit_due_post_heartbeats():
        now = _time.monotonic()
        interval = _probe_wait_heartbeat_interval()
        with lock:
            due = [(port, meta["key"], meta["since"], meta["revision"])
                   for port, meta in post_meta.items()
                   if (meta["key"] is not None
                       and pinfo[port]["state"] not in done_states
                       and now + 1e-9 >= meta["next"])]
        if not due:
            return
        snapshot = _post_snapshot()
        for port, key, since, revision in due:
            with lock:
                meta = post_meta[port]
                if (meta["key"] != key
                        or meta["revision"] != revision
                        or pinfo[port]["state"] in done_states
                        or now + 1e-9 < meta["next"]):
                    continue
                meta["next"] = now + interval
                base = _post_base_label(key, snapshot)
                if meta["base"] != base:
                    meta["revision"] += 1
                meta["base"] = base
                pinfo[port]["detail"] = (
                    f"{base} · {max(0, int(now - since))}s")
            emit(port)

    def _pause_emit(port, paused, info=None):
        """on_pause callback for THIS port: the finish-month pause has it idling on a
        CLEAN boundary (paused=True) or resuming (False) -> reflect it in the monitor
        so the button shows 'Paused' only once EVERY port has reported in."""
        info = info or {}
        with lock:
            if paused:
                pinfo[port]["last_month"] = str(info.get("last_month") or "")
                pinfo[port]["pause_detail"] = str(info.get("detail") or "")
            else:
                pinfo[port]["pause_detail"] = ""
        _set_state(port, "paused" if paused else "working", parked_guard=True)
        emit(port)

    def _pacing_emit(port, waiting, info=None):
        """Publish one port's exact pacer wait as independent status state.

        A dedicated field preserves any day-progress detail produced by the
        consumer thread. emit() stays outside the lock because it takes the
        same lock to snapshot the bounded row.
        """
        label = (_pacing_wait_label((info or {}).get("seconds", 0.0))
                 if waiting else "")
        with lock:
            pinfo[port]["pacing_detail"] = label
            pinfo[port]["pacing_seconds"] = (
                max(0.0, float((info or {}).get("seconds") or 0.0))
                if waiting else 0.0)
        emit(port)

    base = f"ibkr-{datetime.now():%Y%m%d-%H%M%S}"
    reports = [None] * len(ports)
    report_ports = list(ports)            # port for each reports slot; GROWS when a
    #                                       revived port is re-adopted mid-run
    extra_lock = threading.Lock()
    extra_threads = []
    worker_active = set(ports)

    def _watchdog_interrupted_since(port, sequence):
        return bool(
            watchdog is not None
            and sequence is not None
            and watchdog.interrupt_sequence(port) != sequence
            and not (cancel is not None and cancel.is_set()))

    def _watchdog_signal(port, kind, exc):
        if watchdog is None or not watchdog.hard_signal(port, kind):
            return
        snapshot = watchdog.snapshot()
        state = snapshot["ports"][port]["state"].lower()
        reason = f"{kind}: {str(exc)[:120]}"
        with lock:
            pinfo[port]["error"] = reason
            pinfo[port]["watchdog_reason"] = kind
        _set_state(port, state, force=True)
        emit(port)
        say(f"port {port} hard connection failure ({kind}) - "
            "re-queueing its in-flight ticker")
        if snapshot["holding"]:
            say("FLEET DOWN - HOLDING: no new requests will dispatch while "
                "the watchdog probes for a recovered port")

    def worker(i, port):
        _set_state(port, "queued")
        emit(port)

        def wprog(msg):
            prog = _parse_progress_msg(msg)
            if prog:
                with lock:
                    retry_since[port] = None
                    pinfo[port]["ticker"] = prog["ticker"]
                    pinfo[port]["days_done"] = prog["done"]
                    pinfo[port]["days_total"] = prog["total"]
                    pinfo[port]["bars"] = prog["bars"]
                    pinfo[port]["detail"] = _progress_detail(prog)
                    pinfo[port]["pause_detail"] = ""
                    cur = pinfo[port]["state"]
                    if (cur not in done_states
                            and not (use_adaptive and park[port].is_set())):
                        pinfo[port]["state"] = "working"
                emit(port)
                return
            m = series_re.match(msg)
            if m:                                  # one global [done/total] bar
                series_parts = m.group(1).split()
                ticker = series_parts[0]
                interval = series_parts[1]
                with lock:
                    series_key = (job["seq"], ticker, interval)
                    resumed = series_key in announced
                    if not resumed:
                        announced.add(series_key)
                        done[0] += 1
                    n = done[0]
                    retry_since[port] = None       # progressed => not stuck
                    pinfo[port]["ticker"] = ticker
                    pinfo[port]["count"] += 1
                    pinfo[port]["days_done"] = 0
                    pinfo[port]["days_total"] = 0
                    pinfo[port]["bars"] = 0
                    pinfo[port]["detail"] = ""
                    pinfo[port]["last_month"] = ""
                    pinfo[port]["pause_detail"] = ""
                suffix = f"port {port}, resumed" if resumed else f"port {port}"
                say(f"[{n}/{total}] {m.group(1)}  ({suffix})")
                _set_state(port, "working", parked_guard=True)
            else:
                say(f"{msg}  (port {port})")
                low = msg.lower()                  # 'connection lost — reconnect'
                if "reconnect" in low or "connection lost" in low:
                    # a PARKED port that's still grinding its reconnect loop
                    # stays SILENT to the controller (no recon record, no
                    # display change) — it is being idled on purpose.
                    if not (use_adaptive and park[port].is_set()):
                        _set_state(port, "retrying", parked_guard=True)
                        with lock:
                            if retry_since[port] is None:
                                retry_since[port] = _time.monotonic()
                            recon_at[port] = _time.monotonic()
                else:
                    with lock:
                        retry_since[port] = None
                    _set_state(port, "working", parked_guard=True)
            emit(port)                             # heartbeat (ticker preserved)
        # effective pause = global Pause button OR this port being parked
        wpause = _OrEvent(pause, park[port]) if use_adaptive else pause
        wcancel = (_WorkerCancel(cancel, watchdog.interrupt_event(port))
                   if watchdog is not None else cancel)
        # ONE connection per port, reused across every job this port pulls
        # (ReusableAdapter's disconnect() is a no-op so gap_fill's per-call
        # teardown doesn't drop the link; close() at the end really disconnects).
        reuser = ReusableAdapter(fib.without_authority(_mk(host, (port,))))
        observed = (addstock_watchdog.HardSignalAdapter(
            reuser, lambda kind, exc, p=port:
            _watchdog_signal(p, kind, exc))
                    if watchdog is not None else reuser)
        pacer = _WorkerPacer()  # Local UI callbacks; process-wide A1 governor.
        pacer._on_pause = (
            lambda paused, info=None, p=port: _pause_emit(p, paused, info))
        pacer._on_wait = (
            lambda waiting, info=None, p=port: _pacing_emit(p, waiting, info))

        def post_pause_boundary():
            """Do not claim fill/post work while the user pause is settled."""
            if wpause is None or not wpause.is_set():
                return True
            try:
                _wait_while_paused(
                    wpause, wcancel, say,
                    note="the current ticker task is durably complete",
                    on_pause=getattr(pacer, "_on_pause", None),
                    pause_info={"detail": "between ticker tasks"})
            except Cancelled:
                return False
            return not (wcancel is not None and wcancel.is_set())

        acc = None
        job = None
        job_interrupt_sequence = None
        try:
            while not (cancel is not None and cancel.is_set()):
                interrupt_sequence = (
                    watchdog.interrupt_sequence(port)
                    if watchdog is not None else None)
                if (watchdog is not None
                        and watchdog.interrupt_event(port).is_set()):
                    break
                # adaptive: a PARKED port idles BETWEEN jobs too, so it never
                # holds a fresh job hostage while the controller wants it quiet;
                # the held job stays in the queue for the ports still pulling.
                while (use_adaptive and park[port].is_set()
                       and not (cancel is not None and cancel.is_set())
                       and not stop_ctl.is_set()):
                    _set_state(port, "parked")
                    emit(port)
                    _time.sleep(0.2)
                if cancel is not None and cancel.is_set():
                    break
                if not post_pause_boundary():
                    break
                job = _take_job()
                if job is None:
                    # Pause can race the empty-queue observation.  Recheck at
                    # the exact post-task claim boundary so a completed
                    # reconcile remains atomic and no successor starts.
                    if not post_pause_boundary():
                        break
                    task = (post_pipeline.claim()
                            if post_pipeline is not None else None)
                    if task is not None:
                        phase = str(task[0])
                        ticker = str(task[1])
                        _set_post_status(port, (phase, ticker))
                        try:
                            try:
                                if engine_post_tasks:
                                    task_state.check()
                                    # The fixed probe resolves the shipped reusable
                                    # transport; no observer facade receives a child.
                                    post_tasks._run_addstock_task(root, post_pipeline, task, reuser, pacer,
                                        cancel=wcancel, progress=wprog,
                                        on_request_error=(lambda exc, p=port:
                                            _watchdog_signal(p, addstock_watchdog.hard_signal_kind(exc), exc))
                                            if watchdog is not None else None,
                                        _fetch_child=_fetch_parent.child(
                                            "addstock-post-" + uuid.uuid4().hex))
                                else:
                                    fib.without_authority(post_pipeline.execute)(task, observed, pacer)
                            except Exception as exc:  # noqa: BLE001
                                if task_state is not None and post_tasks.terminal_error(exc):
                                    raise task_state.fail(exc)
                                say(f"post-ticker task failed ({exc})")
                        finally:
                            _clear_post_status(port)
                        if (watchdog is not None
                                and watchdog.interrupt_event(port).is_set()):
                            break
                        continue
                    if (_fill_work_remains()
                            or (post_pipeline is not None
                                and post_pipeline.has_pending())):
                        if post_pipeline is not None:
                            _set_post_status(port, ("waiting", None))
                        else:
                            # A queue waiter owns no job. Counting it as queued
                            # or working prevents forced release when every
                            # actual remaining fill is parked at a safe boundary.
                            _set_state(port, "idle", parked_guard=True)
                        _time.sleep(0.02)
                        continue
                    _clear_post_status(port)
                    break                          # every work phase drained
                _clear_post_status(port)
                job_interrupt_sequence = interrupt_sequence
                job_series = list(job["series"])
                with lock:
                    port_jobs[port].extend(job_series)
                if acc is None:
                    _set_state(port, "connecting")
                    emit(port)
                job_completion = {} if task_state is not None else None
                try:
                    job_plan_ahead = (False if job.get("date_split")
                                      else pickup_plan_ahead)
                    job_after_last = (None if job.get("date_split")
                                      else _preplan_queued_job)
                    job_shared_pickup = bool(
                        series_split_active or job.get("date_split"))
                    r = gap_fill(root, job_series, wprog, wcancel, observed, pacer,
                                 today, since, spot_check,
                                 (random.Random(port) if spot_check else None),
                                 wpause, resolved, pipeline,
                                 run_id=f"{base}-p{port}", on_series=on_series,
                                 **({"_fetch_state": task_state, "_fetch_completion": job_completion}
                                    if task_state is not None else {}),
                                 _fetch_child=_fetch_parent.child(
                                     f"pass-{fetch_pass}.port-{port}.worker-{i}"),
                                 manifest_locks=manifest_locks,
                                 month_ranges=(job.get("month_ranges") or None),
                                 on_series_start=on_series_start,
                                 _pickup_plan_ahead=job_plan_ahead,
                                 _pickup_after_last=job_after_last,
                                 _pickup_resolved=(pickup_resolved
                                                   if job_shared_pickup
                                                   else None),
                                 _pickup_head_cache=(pickup_head_cache
                                                     if job_shared_pickup
                                                     else None))
                except Exception as exc:  # noqa: BLE001 — link died for this port
                    if job_completion is not None and job_completion.get("report") is not None:
                        acc = _fold_worker_report(acc, job_completion["report"], port)
                    if task_state is not None and post_tasks.terminal_error(exc):
                        raise task_state.fail(exc)
                    kind = addstock_watchdog.hard_signal_kind(exc)
                    if kind is not None and watchdog is not None:
                        _watchdog_signal(port, kind, exc)
                    hard_interrupted = _watchdog_interrupted_since(
                        port, job_interrupt_sequence)
                    acc = _fold_worker_report(
                        acc, {"aborted": f"worker crashed: {exc}"}, port)
                    pinfo[port]["error"] = str(exc)
                    _finish_job(job, requeue=hard_interrupted)
                    job = None
                    job_interrupt_sequence = None
                    if hard_interrupted:
                        with job_qlock:
                            watchdog_rerouted.add(port)
                    break                          # stop pulling; recovery refetches
                hard_interrupted = _watchdog_interrupted_since(
                    port, job_interrupt_sequence)
                if hard_interrupted:
                    r = dict(r)
                    r["cancelled"] = False
                    r["aborted"] = (
                        f"watchdog interrupted port {port} after a hard "
                        "connection failure")
                    _finish_job(job, requeue=True)
                    job = None
                    job_interrupt_sequence = None
                    with job_qlock:
                        watchdog_rerouted.add(port)
                else:
                    # Publish a ticker to the post pipeline only after the
                    # watchdog sequence proves this fill will not be rerouted.
                    # Keeping active_fill set during publication prevents an
                    # idle sibling from racing a reconcile against recovery.
                    if post_pipeline is not None:
                        post_pipeline.record_series(r.get("series") or [])
                    _finish_job()
                    job = None
                    job_interrupt_sequence = None
                acc = _fold_worker_report(acc, r, port)
                if hard_interrupted:
                    break
                if r.get("aborted"):
                    break        # initial connect failed (port down) -> stop pulling
                if r.get("cancelled"):
                    break
        except BaseException as exc:  # Every engine worker still publishes its accumulated report.
            # record this port's accumulated report (committed work is reported)
            # and flag it aborted so recovery refetches its pulled jobs — never
            # leave reports[i] None, which the merge would silently skip.
            if task_state is None and not isinstance(exc, Exception):
                raise
            if task_state is not None and post_tasks.terminal_error(exc):
                exc = task_state.fail(exc)
            detail = post_tasks.error_detail(exc) if task_state is not None else str(exc)
            acc = _fold_worker_report(acc, {"aborted": f"worker error: {detail}"},
                                      port)
            if job is not None:
                hard_interrupted = _watchdog_interrupted_since(
                    port, job_interrupt_sequence)
                _finish_job(job, requeue=hard_interrupted)
                if hard_interrupted:
                    with job_qlock:
                        watchdog_rerouted.add(port)
                job = None
            with lock:
                pinfo[port]["error"] = detail
        finally:
            try:
                reuser.close()                     # really drop the shared link
            except Exception:  # noqa: BLE001
                pass
            with extra_lock:
                worker_active.discard(port)
        reports[i] = acc
        with lock:                                 # value-reassign only (no grow)
            if acc is None:                        # pulled nothing (queue raced empty)
                pinfo[port]["ticker"] = "—"
            elif not acc.get("aborted"):
                pinfo[port]["account"] = acc.get("account")
                pinfo[port]["added"] = acc.get("totals", {}).get("added", 0)
            pinfo[port]["days_done"] = 0
            pinfo[port]["days_total"] = 0
            pinfo[port]["bars"] = 0
            pinfo[port]["detail"] = ""
            pinfo[port]["pacing_detail"] = ""
            pinfo[port]["pacing_seconds"] = 0.0
            pinfo[port]["last_month"] = ""
            pinfo[port]["pause_detail"] = ""
            post_meta[port]["key"] = None
            post_meta[port]["base"] = ""
            post_meta[port]["since"] = 0.0
            post_meta[port]["next"] = float("inf")
            post_meta[port]["revision"] += 1
        if (watchdog is not None
                and watchdog.interrupt_event(port).is_set()):
            _set_state(port, watchdog.state(port).lower(), force=True)
        elif acc is None:
            _set_state(port, "done", force=True)
        elif acc.get("aborted"):
            _set_state(port, "error", force=True)
        else:
            _set_state(port, "done", force=True)   # authoritative, under lock
        emit(port)

    # ---- mid-run re-adoption monitor ----------------------------------
    # The resilient wrapper's recovery only runs AFTER this whole fetch returns,
    # so a port that dies mid-run (the daily logout) would otherwise sit idle for
    # the rest of a multi-hour drain. This monitor re-probes each ABORTED port;
    # once it is LISTENING again (restarted by recovery, by hand, or self-healed)
    # AND work remains in the shared queue, it spawns a FRESH worker that pulls the
    # rest — regaining parallelism without waiting for the run to end. Bounded per
    # port so a flapping port can't spawn endlessly. No-op unless a port_up probe
    # is supplied and there are >= 2 ports.
    _REPROBE_MAX_PER_PORT = 6
    stop_reprobe = threading.Event()
    if task_state is not None:
        task_state.add_stopper(stop_reprobe)

    def _start_thread(thread):
        if task_state is not None:
            task_state.start(thread)
        else:
            thread.start()

    reprobe_n = {p: 0 for p in ports}

    def _reprobe():
        while not stop_reprobe.is_set():
            slept = 0.0
            while slept < reprobe_interval and not stop_reprobe.is_set():
                _time.sleep(0.5)
                slept += 0.5
            if stop_reprobe.is_set() or (cancel is not None and cancel.is_set()):
                break
            with job_qlock:
                work_left = bool(job_q)
            if not work_left:
                continue                       # nothing left to hand a revived port
            for p in ports:
                if stop_reprobe.is_set():
                    break
                with lock:
                    st = pinfo[p]["state"]
                if st != "error" or reprobe_n[p] >= _REPROBE_MAX_PER_PORT:
                    continue                   # only revive a TRULY-aborted port
                try:
                    up = bool(port_up(p))
                except Exception:  # noqa: BLE001 — a flaky probe reads as still down
                    up = False
                if not up:
                    continue
                with extra_lock:               # ONE critical section with the join
                    if stop_reprobe.is_set():  # drain's snapshot -> never spawn a
                        break                  # worker PAST that snapshot
                    reprobe_n[p] += 1          # p is back + work remains -> adopt it
                    _set_state(p, "queued", force=True)   # clear 'error' BEFORE spawn
                    if use_adaptive:           # else the worker's own _set_state is
                        park[p].clear()        # blocked by the terminal guard; also
                        with lock:             # never inherit a controller-set park
                            retry_since[p] = None   # an 'error' port can't shed
                    ni = len(reports)
                    reports.append(None)
                    report_ports.append(p)
                    t = threading.Thread(target=worker, args=(ni, p),
                                         name=f"ibkr-readopt-{p}", daemon=True)
                    try:
                        _start_thread(t)       # start FIRST so the join drain never
                        extra_threads.append(t)   # sees an UNSTARTED thread
                    except Exception as exc:   # noqa: BLE001 — thread exhaustion:
                        if task_state is not None:
                            raise task_state.fail(exc)
                        reports.pop()          # roll the slot back + restore 'error'
                        report_ports.pop()     # so a transient spawn failure stays
                        reprobe_n[p] -= 1      # non-fatal (no dead monitor, no
                        _set_state(p, "error", force=True)   # unstarted thread for
                        say(f"port {p} re-adoption spawn failed ({exc}); "  # the join)
                            f"recovery will refetch it after the run")
                        continue
                say(f"port {p} is back up — re-adopting it for the remaining "
                    f"queue (regaining parallelism mid-run)")

    degraded_finalize = [False]
    last_hold_line = [float("-inf")]
    probe_port = port_up or (
        lambda p: _port_open(host, int(p), timeout=CONNECT_TIMEOUT_S))
    midrun_restart_events = []
    midrun_restart_events_truncated = [0]
    midrun_restart_offers = {p: 0 for p in ports}
    midrun_decline_until = [0.0]
    midrun_lifecycle_lock = threading.Lock()
    midrun_restart_inflight = threading.Event()
    midrun_probe_lock = threading.Lock()
    midrun_force_probes = set()
    fleetdown_restart_pending = []
    fleetdown_restart_pending_event = threading.Event()
    fleetdown_restart_offered = [False]

    def _record_midrun_restart_event(event):
        if len(midrun_restart_events) < _RESTART_EVENT_CAP:
            midrun_restart_events.append(event)
        else:
            midrun_restart_events_truncated[0] += 1

    def _midrun_work_remains():
        if _fill_work_remains():
            return True
        return (post_pipeline is not None and post_pipeline.has_pending())

    def _midrun_candidates(snapshot, now, candidates=None, require_grace=True):
        """Return still-closed confirmed-dead candidates in fleet order."""
        allowed = set(ports if candidates is None else candidates)
        result = []
        with extra_lock:
            active_workers = set(worker_active)
        for p in ports:
            if p not in allowed or p in active_workers:
                continue
            row = snapshot["ports"].get(p) or {}
            try:
                listening = bool(probe_port(p))
            except Exception:  # noqa: BLE001 - a failed probe is still not proof of life
                listening = False
            if _midrun_port_candidate(
                    row, listening=listening, worker_active=False,
                    offers=midrun_restart_offers[p], now=now,
                    cooldown_until=midrun_decline_until[0],
                    require_grace=require_grace):
                result.append(p)
        return result

    def _midrun_listening_ports(snapshot):
        """Return inactive failed ports whose listener recovered ahead of state."""
        result = []
        with extra_lock:
            active_workers = set(worker_active)
        for p in ports:
            row = snapshot["ports"].get(p) or {}
            if p in active_workers or row.get("state") == "HEALTHY":
                continue
            try:
                listening = bool(probe_port(p))
            except Exception:  # noqa: BLE001 - failed probe is not proof of life
                listening = False
            if listening:
                result.append(p)
        return result

    def _execute_midrun_restart_batch(batch, snapshot, *, phase,
                                      inflight_reserved=False):
        """Run one normalized lifecycle batch and queue watchdog validation.

        The caller holds ``midrun_lifecycle_lock``.  A fleet-down reservation
        sets the in-flight fence before handing work to this controller; the
        partial-fleet path sets it here.  Either way, the watchdog monitor is
        still the sole owner of HEALTHY state and worker re-adoption.
        """
        batch = [int(p) for p in batch]
        if not batch:
            if inflight_reserved:
                midrun_restart_inflight.clear()
            return set()
        for p in batch:
            midrun_restart_offers[p] += 1
        if not inflight_reserved:
            midrun_restart_inflight.set()
        fleet_down = phase == "fleet_down"
        if fleet_down:
            say("MID-RUN fleet-down restart: every confirmed-dead port "
                f"({', '.join(map(str, batch))}); the run is holding at a "
                "recoverable boundary and will resume after recovery")
            _diag("MIDRUN_FLEETDOWN_RESTART",
                  ports=",".join(map(str, batch)), survivors=0)
        else:
            say("MID-RUN restart: confirmed-dead port(s) "
                f"{', '.join(map(str, batch))}; surviving ports continue "
                "while restart input is blocked briefly")
            _diag("MIDRUN_RESTART", ports=",".join(map(str, batch)),
                  survivors=len(snapshot["healthy_ports"]))
        final_ok_ports = set()
        try:
            outcomes = _call_restart_ports_batch(
                restart_ports, batch, phase=phase)
            decline_like = False
            for p in batch:
                outcome = outcomes[p]
                callback_ok = outcome["ok"]
                final_ok = callback_ok
                final_probe = "not_run"
                recovered_by_probe = False
                reason = outcome["reason"]
                try:
                    if bool(probe_port(p)):
                        final_probe = "up"
                        final_ok = True
                        recovered_by_probe = not callback_ok
                    else:
                        final_probe = "down"
                        final_ok = False
                except Exception as exc:  # noqa: BLE001
                    final_probe = "error"
                    final_ok = False
                    probe_reason = _restart_error_reason(
                        "final port probe error", exc)
                    reason = _bounded_restart_reason(
                        f"{reason}; {probe_reason}", reason)
                event = {
                    "phase": phase,
                    "round": midrun_restart_offers[p],
                    "port": p,
                    "decision": outcome["decision"],
                    "attempted": outcome["attempted"],
                    "callback_ok": callback_ok,
                    "final_probe": final_probe,
                    "ok": final_ok,
                    "recovered_by_probe": recovered_by_probe,
                    "reason": reason,
                }
                if outcome.get("popup_timing") is not None:
                    event["popup_timing"] = dict(outcome["popup_timing"])
                _record_midrun_restart_event(event)
                if final_ok:
                    final_ok_ports.add(p)
                if not outcome["attempted"] and not final_ok:
                    decline_like = True
            if decline_like:
                midrun_decline_until[0] = (
                    _time.monotonic()
                    + max(0.0, float(DECLINE_COOLDOWN_S)))
            # Force the existing monitor to validate every offered port
            # promptly; it alone clears watchdog state or starts workers.
            with midrun_probe_lock:
                midrun_force_probes.update(batch)
            return final_ok_ports
        finally:
            midrun_restart_inflight.clear()

    def _watchdog_reprobe():
        """Probe hard-failed ports and re-adopt them without lifecycle power."""
        while not stop_reprobe.is_set():
            if cancel is not None and cancel.is_set():
                break
            now = _time.monotonic()
            snapshot = watchdog.snapshot(now=now)
            if (not snapshot["holding"]
                    and not midrun_restart_inflight.is_set()):
                # A later, distinct all-dead episode gets its own one-shot.
                fleetdown_restart_offered[0] = False
            if snapshot["holding"] and now - last_hold_line[0] >= 5.0:
                last_hold_line[0] = now
                suffix = ("; restart maintenance is active"
                          if snapshot["maintenance_active"] else "")
                say("FLEET DOWN - HOLDING "
                    f"({snapshot['fleet_grace_seconds']:.0f}s grace){suffix}")

            # M2: reserve the complete all-dead batch BEFORE honoring finalize.
            # REVIVE_AFTER_S and the production fleet grace are both 150s, so
            # checking finalize first would make this path unreachable.  The
            # callback itself runs on the controller thread so this monitor can
            # keep advancing maintenance-aware grace and process adoption probes.
            work_remains = _midrun_work_remains()
            paused = pause is not None and pause.is_set()
            cancelled = cancel is not None and cancel.is_set()
            waiting_for_lifecycle = False
            waiting_for_worker_teardown = False
            fleetdown_gate = (
                restart_ports is not None and len(ports) >= 2
                and not midrun_restart_inflight.is_set()
                and _midrun_fleetdown_gate(
                    snapshot, work_remains=work_remains, paused=paused,
                    cancelled=cancelled, draining=degraded_finalize[0],
                    offered=fleetdown_restart_offered[0]))
            if fleetdown_gate:
                # A hard-dead worker unregisters only after its adapter
                # disconnect returns. Fence finalize until every old worker is
                # gone, regardless of whether its listener self-heals during
                # teardown. Never relaunch an active worker; once unregistering
                # finishes, the normal paths either adopt that listener, reserve
                # the complete closed fleet, or honor a genuine ineligible
                # finalize. The main drain already joins these same workers.
                with extra_lock:
                    waiting_for_worker_teardown = bool(worker_active)
                if not waiting_for_worker_teardown:
                    batch = _midrun_candidates(snapshot, now)
                    if set(batch) == set(ports):
                        if midrun_lifecycle_lock.acquire(blocking=False):
                            try:
                                now = _time.monotonic()
                                refreshed = watchdog.snapshot(now=now)
                                snapshot = refreshed
                                refreshed_work = _midrun_work_remains()
                                refreshed_batch = _midrun_candidates(
                                    refreshed, now)
                                if (not midrun_restart_inflight.is_set()
                                        and not fleetdown_restart_pending_event.is_set()
                                        and _midrun_fleetdown_gate(
                                            refreshed,
                                            work_remains=refreshed_work,
                                            paused=(pause is not None
                                                    and pause.is_set()),
                                            cancelled=(cancel is not None
                                                       and cancel.is_set()),
                                            draining=degraded_finalize[0],
                                            offered=fleetdown_restart_offered[0])
                                        and set(refreshed_batch) == set(ports)):
                                    fleetdown_restart_pending.clear()
                                    fleetdown_restart_pending.extend(
                                        refreshed_batch)
                                    # Fence the main drain and watchdog finalize
                                    # before exposing the reservation to worker.
                                    midrun_restart_inflight.set()
                                    fleetdown_restart_pending_event.set()
                            finally:
                                midrun_lifecycle_lock.release()
                        else:
                            # The main drain or another lifecycle callback owns
                            # the boundary briefly. Give reservation another
                            # monitor turn instead of finalizing through it.
                            waiting_for_lifecycle = True

            # A listener that self-healed just ahead of watchdog state must be
            # adopted, never relaunched or discarded by a stale finalize view.
            if (snapshot["finalize"] and restart_ports is not None
                    and len(ports) >= 2 and not fleetdown_restart_offered[0]
                    and not midrun_restart_inflight.is_set()
                    and MIDRUN_RESTART and MIDRUN_FLEETDOWN_REVIVAL):
                listening = _midrun_listening_ports(snapshot)
                if listening:
                    with midrun_probe_lock:
                        midrun_force_probes.update(listening)

            if snapshot["finalize"]:
                with midrun_probe_lock:
                    handoff_pending = bool(midrun_force_probes)
                if fleetdown_gate:
                    with extra_lock:
                        waiting_for_worker_teardown = (
                            waiting_for_worker_teardown or bool(worker_active))
                if (not waiting_for_lifecycle
                        and not waiting_for_worker_teardown
                        and not midrun_restart_inflight.is_set()
                        and not handoff_pending):
                    degraded_finalize[0] = True
                    say("FLEET DOWN grace exhausted - ending the fetch at a "
                        "recoverable boundary; port-free checks will drain and "
                        "gap seals remain pending")
                    break
            due = set(watchdog.due_probes(now=now))
            with midrun_probe_lock:
                due.update(midrun_force_probes)
                midrun_force_probes.clear()
            for p in (p for p in ports if p in due):
                _set_state(p, "probing", force=True)
                emit(p)
                try:
                    up = bool(probe_port(p))
                except Exception:  # noqa: BLE001 - failed probe means still down
                    up = False
                if up:
                    with extra_lock:
                        worker_tearing_down = p in worker_active
                    if worker_tearing_down:
                        # A listening socket does not make the old worker gone.
                        # Keep its interrupt evidence and schedule another probe;
                        # the next success may adopt only after teardown unregisters.
                        state = watchdog.probe_result(
                            p, False, now=_time.monotonic()).lower()
                        _set_state(p, state, force=True)
                        emit(p)
                        continue
                state = watchdog.probe_result(
                    p, up, now=_time.monotonic()).lower()
                if not up:
                    _set_state(p, state, force=True)
                    emit(p)
                    continue
                say(f"port {p} recovered - resuming queued Add Stocks work")
                with job_qlock:
                    fill_left = bool(job_q or active_fill[0])
                post_left = (post_pipeline is not None
                             and post_pipeline.has_pending())
                if not (fill_left or post_left):
                    _set_state(p, "done", force=True)
                    emit(p)
                    continue
                with extra_lock:
                    if stop_reprobe.is_set() or p in worker_active:
                        continue
                    if use_adaptive:
                        park[p].clear()
                        with lock:
                            retry_since[p] = None
                    ni = len(reports)
                    reports.append(None)
                    report_ports.append(p)
                    worker_active.add(p)
                    thread = threading.Thread(
                        target=worker, args=(ni, p),
                        name=f"ibkr-watchdog-readopt-{p}", daemon=True)
                    try:
                        _start_thread(thread)
                        extra_threads.append(thread)
                    except Exception as exc:  # noqa: BLE001
                        if task_state is not None:
                            raise task_state.fail(exc)
                        worker_active.discard(p)
                        reports.pop()
                        report_ports.pop()
                        watchdog.hard_signal(p, "connection_lost")
                        _set_state(p, watchdog.state(p).lower(), force=True)
                        say(f"port {p} watchdog worker spawn failed ({exc})")
                        emit(p)
            stop_reprobe.wait(0.2)

    def _midrun_restart_controller():
        """Offer lifecycle power for watchdog-confirmed mid-run deaths.

        The watchdog monitor above remains the sole owner of HEALTHY state and
        worker adoption. This thread executes both coalesced partial-death
        offers and watchdog-reserved one-shot fleet-down offers, then asks the
        monitor for an immediate follow-up probe.
        """
        pending = set()
        deadline = None
        while not stop_reprobe.wait(0.1):
            if fleetdown_restart_pending_event.is_set():
                # The watchdog reserved this before its finalize boundary. Run
                # the potentially long popup/launcher callback here so the
                # monitor remains free to account for maintenance and probes.
                with midrun_lifecycle_lock:
                    batch = list(fleetdown_restart_pending)
                    fleetdown_restart_pending.clear()
                    fleetdown_restart_pending_event.clear()
                    now = _time.monotonic()
                    snapshot = watchdog.snapshot(now=now)
                    work_remains = _midrun_work_remains()
                    paused = pause is not None and pause.is_set()
                    cancelled = cancel is not None and cancel.is_set()
                    eligible = (_midrun_candidates(snapshot, now)
                                if work_remains and not cancelled else [])
                    valid = bool(
                        batch and set(batch) == set(ports)
                        and set(eligible) == set(ports)
                        and _midrun_fleetdown_gate(
                            snapshot, work_remains=work_remains,
                            paused=paused, cancelled=cancelled,
                            draining=stop_reprobe.is_set(),
                            offered=fleetdown_restart_offered[0]))
                    if valid:
                        # Consume the episode's one shot at callback entry, not
                        # at reservation, so Pause/maintenance can still re-arm.
                        fleetdown_restart_offered[0] = True
                        _execute_midrun_restart_batch(
                            batch, snapshot, phase="fleet_down",
                            inflight_reserved=True)
                    else:
                        # A listener may have self-healed between reservation
                        # and callback entry. Validate through the existing
                        # monitor; never launch it and never strand the fence.
                        if (batch and work_remains and not paused and not cancelled
                                and snapshot.get("holding")
                                and not snapshot.get("maintenance_active")):
                            with midrun_probe_lock:
                                midrun_force_probes.update(batch)
                        midrun_restart_inflight.clear()
                pending.clear()
                deadline = None
                continue
            if cancel is not None and cancel.is_set():
                break
            if pause is not None and pause.is_set():
                pending.clear()
                deadline = None
                continue
            if not _midrun_work_remains():
                pending.clear()
                deadline = None
                continue

            now = _time.monotonic()
            snapshot = watchdog.snapshot(now=now)
            # The ordinary coalescer stays partial-fleet only. Fleet-down work
            # arrives solely through the watchdog reservation above.
            if not _midrun_controller_gate(
                    snapshot, work_remains=True):
                pending.clear()
                deadline = None
                continue
            # Begin coalescing as soon as the watchdog says DEAD, but never
            # fire before BOTH the batch window and REVIVE_AFTER_S have elapsed.
            # This overlaps the two safety waits instead of needlessly adding
            # them end-to-end.
            eligible = _midrun_candidates(
                snapshot, now, require_grace=False)
            eligible_set = set(eligible)
            pending.intersection_update(eligible_set)
            pending.update(eligible)
            if not pending:
                deadline = None
                continue
            if deadline is None:
                first_grace = min(
                    float(snapshot["ports"][p].get("grace_seconds") or 0.0)
                    for p in pending)
                deadline = now + max(
                    max(0.0, float(BATCH_WINDOW_S)),
                    max(0.0, float(REVIVE_AFTER_S) - first_grace))
            if now < deadline:
                continue

            # Serialize callback entry with the main-thread drain decision.  A
            # callback that has entered is allowed to finish while the existing
            # watchdog monitor keeps probing and remains able to adopt it.
            with midrun_lifecycle_lock:
                if (stop_reprobe.is_set()
                        or (cancel is not None and cancel.is_set())
                        or (pause is not None and pause.is_set())
                        or not _midrun_work_remains()):
                    pending.clear()
                    deadline = None
                    continue
                now = _time.monotonic()
                snapshot = watchdog.snapshot(now=now)
                if not _midrun_controller_gate(
                        snapshot, work_remains=True):
                    pending.clear()
                    deadline = None
                    continue
                batch = _midrun_candidates(snapshot, now, pending)
                pending.clear()
                deadline = None
                if not batch:
                    continue
                _execute_midrun_restart_batch(
                    batch, snapshot, phase="midrun")

    # ---- adaptive controller (AIMD + stuck-port swap) -----------------
    stop_ctl = threading.Event()
    if task_state is not None:
        task_state.add_stopper(stop_ctl)

    def _most_work(cands):
        """Pick a parked port to resume first. With the shared queue any parked
        port can take the remaining jobs, so prefer the one that has done the
        LEAST so far (most spare capacity); ties broken by port for determinism."""
        if not cands:
            return None
        return min(cands, key=lambda p: (pinfo[p].get("count", 0), p))

    def _controller():
        last_dirty = _time.monotonic()
        cooldown_until = 0.0
        while not stop_ctl.is_set():
            slept = 0.0
            while slept < ADAPT_TICK_S and not stop_ctl.is_set():
                _time.sleep(0.25)
                slept += 0.25
            if stop_ctl.is_set():
                break
            if cancel is not None and cancel.is_set():
                break
            now = _time.monotonic()
            with lock:
                # only ports with a real worker (live_ports) ever count — a
                # phantom empty-chunk port has no thread and must not be seen
                # as 'working' (else active_working never hits 0 and the
                # forced-release anti-deadlock guarantee can't fire).
                active = [p for p in live_ports if not park[p].is_set()
                          and pinfo[p]["state"] not in done_states]
                parked = [p for p in live_ports if park[p].is_set()
                          and pinfo[p]["state"] not in done_states]
                retrying = [p for p in active
                            if pinfo[p]["state"] == "retrying"]
                working = [p for p in active
                           if pinfo[p]["state"] in
                           ("connecting", "working", "retrying", "queued")]
                stuck = sorted(
                    (p for p in active
                     if pinfo[p]["state"] == "retrying"
                     and retry_since[p] is not None
                     and now - retry_since[p] >= ADAPT_SWAP_STUCK_S),
                    key=lambda p: retry_since[p])
                # DISTINCT active ports that reconnected within the window —
                # the multi-port backend-choke signature (one stuck port that
                # flaps 20x is ONE struggling port, not a choke).
                nstruggle = sum(
                    1 for p in live_ports if not park[p].is_set()
                    and recon_at[p] >= now - ADAPT_WINDOW_S)
            n_active = len(active)
            # 'dirty' (which blocks the additive add and resets the clean timer)
            # means GENUINE saturation: >=2 ports struggling. A single flapping
            # port must NOT pin clean_for at 0 forever and starve add.
            dirty = len(retrying) >= 2 or nstruggle >= 2
            if dirty:
                last_dirty = now
            clean_for = now - last_dirty
            cooling = now < cooldown_until

            # 1) stuck-port SWAP (concurrency-neutral) takes priority: a port
            #    that has lost its connection for >=ADAPT_SWAP_STUCK_S, when a
            #    parked port is free, is swapped out for that idle healthy port.
            if stuck and parked:
                out = stuck[0]
                intake = _most_work(parked)
                if intake is not None:
                    park[out].set()
                    park[intake].clear()
                    with lock:
                        retry_since[out] = None
                    _set_state(out, "parked")
                    _set_state(intake, "working")
                    emit(out)
                    emit(intake)
                    say(f"port {out} lost connection "
                        f">{int(ADAPT_SWAP_STUCK_S)}s — swapping in idle "
                        f"port {intake} to take over")
                    cooldown_until = now + ADAPT_COOLDOWN_S
                    last_dirty = now
                    continue

            action = _adaptive_decide(n_active, len(retrying), nstruggle,
                                      clean_for, cooling, len(parked),
                                      len(working))
            if action == "release":
                # forced tail progress: nothing active is still working but
                # parked work remains -> resume ALL parked (backend is idle).
                rel = [p for p in parked]
                for p in rel:
                    park[p].clear()
                    _set_state(p, "working")
                    emit(p)
                if rel:
                    say(f"resuming parked port(s) {', '.join(map(str, rel))} "
                        f"— no other port left to work")
                    cooldown_until = now + ADAPT_COOLDOWN_S
                    last_dirty = now
            elif action in ("drop1", "drop2"):
                k = 2 if action == "drop2" else 1
                victims = retrying + [p for p in active if p not in retrying]
                parked_n = 0
                for p in victims:
                    if k <= 0 or n_active <= ADAPT_MIN_ACTIVE:
                        break
                    park[p].set()
                    _set_state(p, "parked")
                    emit(p)
                    with lock:
                        retry_since[p] = None
                    k -= 1
                    n_active -= 1
                    parked_n += 1
                if parked_n:
                    say(f"backend busy — parking {parked_n} port(s); "
                        f"{n_active} now pulling")
                    cooldown_until = now + ADAPT_COOLDOWN_S
                    last_dirty = now
            elif action == "add":
                p = _most_work(parked)
                if p is not None:
                    park[p].clear()
                    _set_state(p, "working")
                    emit(p)
                    say(f"backend clear {int(ADAPT_ADD_CLEAN_S)}s — resuming "
                        f"port {p} ({n_active + 1} now pulling)")
                    cooldown_until = now + ADAPT_COOLDOWN_S
                    last_dirty = now

    say(f"Parallel update: {len(ports)} accounts, {total} series "
        f"(ports {', '.join(map(str, ports))})"
        f"{' [adaptive]' if use_adaptive else ''}…")
    threads = [threading.Thread(target=worker, args=(i, p),
                                name=f"ibkr-fetch-{p}", daemon=True)
               for i, p in enumerate(ports)]
    for th in threads:
        _start_thread(th)
    ctl = None
    if use_adaptive:
        ctl = threading.Thread(target=_controller, name="ibkr-adaptive",
                               daemon=True)
        _start_thread(ctl)
    rp = None
    if watchdog is not None:
        rp = threading.Thread(target=_watchdog_reprobe,
                              name="ibkr-hard-death-watchdog", daemon=True)
        _start_thread(rp)
    elif port_up is not None and len(ports) >= 2:
        rp = threading.Thread(target=_reprobe, name="ibkr-reprobe", daemon=True)
        _start_thread(rp)
    midrun_ctl = None
    if (watchdog is not None and MIDRUN_RESTART
            and restart_ports is not None and len(ports) >= 2):
        midrun_ctl = threading.Thread(
            target=_midrun_restart_controller,
            name="ibkr-midrun-restart", daemon=True)
        _start_thread(midrun_ctl)
    alive = list(threads)
    while alive:
        poll = min(_POST_STATUS_POLL_S,
                   max(0.01, _probe_wait_heartbeat_interval() / 2.0))
        alive[0].join(timeout=poll)
        _emit_due_post_heartbeats()
        alive = [th for th in alive if th.is_alive()]
    if watchdog is not None:
        while True:
            # The same lock is held across restart callback entry.  Rechecking
            # every drain condition inside it makes "no callback after drain"
            # an atomic boundary rather than a timing convention.
            with midrun_lifecycle_lock:
                cancelled = cancel is not None and cancel.is_set()
                with job_qlock:
                    fill_left = bool(job_q or active_fill[0])
                post_left = (post_pipeline is not None
                             and post_pipeline.has_pending())
                with extra_lock:
                    active = bool(worker_active)
                with midrun_probe_lock:
                    handoff_pending = bool(midrun_force_probes)
                if (degraded_finalize[0] or cancelled
                        or (not fill_left and not post_left and not active
                            and not midrun_restart_inflight.is_set()
                            and not handoff_pending)):
                    stop_reprobe.set()
                    break
            _emit_due_post_heartbeats()
            _time.sleep(0.1)
    else:
        stop_reprobe.set()                  # stop spawning legacy re-adoptions
    if midrun_ctl is not None:
        midrun_ctl.join()                   # stop-checked 0.1s wait; callback entry
        #                                    is serialized with the drain above
    if rp is not None:
        rp.join()                           # lossless: the monitor only blocks in a
        #                                     0.5s stop-checked sleep + a bounded
        #                                     port_up probe, so this returns promptly
        #                                     AND no spawn can slip past the snapshot
    with extra_lock:                        # drain re-adopted workers (each started
        extra = list(extra_threads)         # under the lock, so join() is valid)
    alive = list(extra)
    while alive:
        poll = min(_POST_STATUS_POLL_S,
                   max(0.01, _probe_wait_heartbeat_interval() / 2.0))
        alive[0].join(timeout=poll)
        _emit_due_post_heartbeats()
        alive = [th for th in alive if th.is_alive()]
    if ctl is not None:                     # all workers done -> stop the controller
        stop_ctl.set()
        ctl.join(timeout=5.0)

    # recovery needs the ACTUAL per-port assignment (which tickers each port
    # pulled), plus any jobs left UNPULLED (only non-empty if everything aborted
    # or the user cancelled mid-run) so the resilient wrapper re-fetches both.
    merged = {"run": base, "root": str(root), "parallel_ports": ports,
              "series": [], "totals": {}, "cancelled": False, "per_port": {},
              "report_path": None,
              "_partition": {p: list(port_jobs[p]) for p in ports},
              "_unpulled": [s for job in list(job_q) for s in job["series"]],
              "started": datetime.now().isoformat(timespec="seconds")}
    if midrun_restart_events:
        merged["midrun_restart_events"] = list(midrun_restart_events)
    if midrun_restart_events_truncated[0]:
        merged["midrun_restart_events_truncated"] = (
            midrun_restart_events_truncated[0])
    if date_split_active:
        merged["date_split"] = {
            "enabled": True,
            "jobs": sum(1 for j in jobs if j.get("date_split")),
            "min_months": int(date_split_min_months),
        }
    _abort_seen = set()                # ports with an aborted slot ...
    _ok_seen = set()                   # ... and with a committed (non-aborted) slot
    for port, rep in zip(report_ports, reports):   # report_ports may repeat a port
        if not rep:                                # (its original slot + a re-adopt)
            continue
        merged["series"].extend(rep.get("series", []))
        merged["cancelled"] = merged["cancelled"] or rep.get("cancelled", False)
        # a validator 'stop' on ANY port is a distinct signal from a user
        # cancel — carry it through the merge (it lives only on the stopping
        # worker's report, whose 'cancelled' is False).
        if rep.get("validation_stopped"):
            merged["validation_stopped"] = True
        if rep.get("notes"):                   # diagnostics (qualify fallback,
            merged.setdefault("notes", []).extend(rep["notes"])   # etc.) — keep
        pp = merged["per_port"].get(port)
        if pp is None:
            merged["per_port"][port] = {
                "account": rep.get("account"),
                "series": len(rep.get("series", [])),
                "added": rep.get("totals", {}).get("added", 0),
                "aborted": rep.get("aborted")}
        else:                                  # FOLD a re-adopted port's 2nd slot
            pp["series"] += len(rep.get("series", []))
            pp["added"] += rep.get("totals", {}).get("added", 0)
            pp["account"] = pp.get("account") or rep.get("account")
            pp["aborted"] = pp.get("aborted") or rep.get("aborted")
        # keep aborted_ports set if the ORIGINAL run aborted (its lost in-flight job
        # still needs the recovery refetch) even when the re-adopt then succeeded.
        if rep.get("aborted"):
            _abort_seen.add(port)
            merged.setdefault("aborted_ports", {})[port] = rep["aborted"]
        else:
            _ok_seen.add(port)
    # Cancellation may arrive after every worker published its final report,
    # including after a watchdog rewrite cleared a worker-local cancel flag.
    # Consult the caller event, not the engine terminal-stop wrapper. A
    # validator stop deliberately sets the shared event without being a user
    # cancellation; preserve that distinction and existing worker flags.
    if (caller_cancel is not None and caller_cancel.is_set()
            and not merged.get("validation_stopped")):
        merged["cancelled"] = True
    # a port with BOTH an aborted slot AND a committed slot was RE-ADOPTED mid-run:
    # its queue work is already done, so the resilient wrapper must NOT relaunch it
    # (kills a healthy instance) or report it 'NOT fetched' (a false readout).
    readopted_ok = sorted(_abort_seen & _ok_seen)
    if readopted_ok:
        merged["readopted_ok"] = readopted_ok
    if watchdog is not None:
        snapshot = watchdog.snapshot()
        rerouted = sorted(watchdog_rerouted)
        merged["watchdog"] = {
            "enabled": True,
            "states": {str(port): row["state"]
                       for port, row in snapshot["ports"].items()},
            "rerouted_ports": rerouted,
            "fleet_grace_seconds": snapshot["fleet_grace_seconds"],
            "maintenance_active": snapshot["maintenance_active"],
        }
        if (rerouted and not degraded_finalize[0]
                and not merged["cancelled"] and not merged["_unpulled"]):
            for port in rerouted:
                merged.get("aborted_ports", {}).pop(port, None)
            if not merged.get("aborted_ports"):
                merged.pop("aborted_ports", None)
            # A successful reroute supersedes the interrupted copy of the same
            # series in the human report. The month commits remain untouched.
            folded = {}
            order = []
            for row in merged["series"]:
                key = (row.get("ticker"), row.get("interval"))
                if key not in folded:
                    order.append(key)
                    folded[key] = row
                elif folded[key].get("halt") and not row.get("halt"):
                    folded[key] = row
            merged["series"] = [folded[key] for key in order]
            # A re-adopted port's committed slot supersedes its interrupted
            # slot in the human-facing per-port status just as it does above
            # in the series list. Keep the cumulative added-work accounting.
            completed_reroutes = (set(rerouted) if task_state is not None
                                  else set(readopted_ok) & set(rerouted))
            for port in completed_reroutes:
                pp = merged.get("per_port", {}).get(port)
                if pp is not None:
                    if task_state is not None and pp.get("aborted"):
                        # A peer may finish the interrupted job without the dead
                        # port itself being re-adopted. Keep its failure history,
                        # but do not label drained work an unresolved root abort.
                        pp["rerouted_abort"] = pp["aborted"]
                    pp["aborted"] = None
        if degraded_finalize[0]:
            merged["fleet_down_finalize"] = True
            merged["interrupted"] = True
            merged["seal_pending"] = sorted({
                f"{ticker} {interval}" for ticker, interval in selections
                if ss.session_of(str(interval)) == "rth"
            })
    for k in ("added", "dup_existing", "conflicts", "written", "requests",
              "bars_fetched", "halted_series", "write_failed"):
        merged["totals"][k] = sum((r or {}).get("totals", {}).get(k, 0)
                                  for r in reports)
    # ``halted_series`` describes the final human-facing series list, not every
    # discarded interrupted copy that contributed legitimate cumulative work.
    merged["totals"]["halted_series"] = sum(
        1 for row in merged["series"] if row.get("halt"))
    merged["spot_checks_run"] = sum((r or {}).get("spot_checks_run", 0)
                                    for r in reports)
    # account/port strings so summarize_report's header line reads naturally.
    accts = [merged["per_port"][p].get("account") for p in merged["per_port"]
             if merged["per_port"][p].get("account")]
    merged["account"] = " + ".join(str(a) for a in accts) or "parallel"
    merged["port"] = "/".join(map(str, ports))
    if (not merged["series"] and merged.get("aborted_ports")
            and not merged.get("fleet_down_finalize")):
        merged["aborted"] = "all accounts failed: " + "; ".join(
            f"{p}: {e}" for p, e in merged["aborted_ports"].items())
    # Persist the fetch-phase shape now; the resilient wrapper atomically
    # refreshes the same artifact after any restart/recovery rounds complete.
    if _fetch_completion is not None:
        _fetch_completion["report"] = merged
    if task_state is not None and task_state.error is not None:
        merged["aborted"] = post_tasks.error_detail(task_state.error)
    _write_parallel_report(root, merged, stage="fetch")
    _aborted_n = len(merged.get("aborted_ports") or {})
    if merged.get("fleet_down_finalize"):
        say("Parallel update interrupted after the fleet-down grace window; "
            f"{len(merged.get('seal_pending') or [])} series remain marked "
            "for seal-on-resume.")
    elif _aborted_n:
        _need = sum(len(merged.get("_partition", {}).get(p, []))
                    for p in (merged.get("aborted_ports") or {}))
        _need += len(merged.get("_unpulled") or [])
        say(f"Parallel update fetch phase finished - "
            f"{merged['totals'].get('added', 0)} bars added; "
            f"{_aborted_n} port(s) ABORTED; {_need} series need "
            f"recovery or a re-run.")
    else:
        say(f"Parallel update finished - {merged['totals'].get('added', 0)} "
            f"bars added across {len(ports)} accounts.")
    if task_state is not None:
        task_state.check()
    return merged


def _merge_recovery(base, extra, recovered_ports):
    """Fold a recovery refetch (`extra`, run on `recovered_ports`) back into
    the original `base` report: drop the recovered ports from aborted_ports,
    add any NEW aborts the refetch hit, append its series, sum its totals,
    and override its per-port entries. Pure + testable — no I/O."""
    out = dict(base)
    ab = dict(base.get("aborted_ports") or {})
    for p in recovered_ports:
        ab.pop(int(p), None)
    for p, e in (extra.get("aborted_ports") or {}).items():
        ab[int(p)] = e
    out["series"] = list(base.get("series", [])) + list(extra.get("series", []))
    bt = dict(base.get("totals", {}))
    for k, v in (extra.get("totals", {}) or {}).items():
        bt[k] = bt.get(k, 0) + v
    out["totals"] = bt
    out["spot_checks_run"] = (base.get("spot_checks_run", 0)
                              + extra.get("spot_checks_run", 0))
    pp = dict(base.get("per_port", {}))
    pp.update(extra.get("per_port", {}))
    out["per_port"] = pp
    out["cancelled"] = base.get("cancelled", False) or extra.get("cancelled",
                                                                 False)
    notes = list(base.get("notes", []) or []) + list(extra.get("notes", []) or [])
    if notes:
        out["notes"] = notes
    if base.get("validation_stopped") or extra.get("validation_stopped"):
        out["validation_stopped"] = True
    if ab:
        out["aborted_ports"] = ab
    else:
        out.pop("aborted_ports", None)
    # any series at all means it was NOT a total wipe-out — clear the
    # top-level abort that gap_fill_parallel sets only when nothing came back.
    if out["series"]:
        out.pop("aborted", None)
    return out


def _as_parallel_shape(rep, ports):
    """Coerce a gap_fill report into the PARALLEL merged shape (per_port +
    aborted_ports) so the recovery merge is safe. gap_fill_parallel already
    returns parallel shape for >=2 ports, but for the 1-port recovery refetch
    (the common single-instance-logout case) it falls back to the raw SERIAL
    gap_fill report — which has no per_port/aborted_ports. Without this, a
    refetch that still can't connect (serial sets only the top-level
    'aborted') would be merged as 'fully recovered' and its tickers would
    vanish silently. Here a serial result for the recovered port is turned
    into an explicit per_port entry and, if it produced nothing / aborted, an
    aborted_ports entry so the port stays visibly unrecovered."""
    ports = list(dict.fromkeys(int(p) for p in ports))
    if rep.get("per_port") is not None or len(ports) != 1:
        return rep                              # already parallel-shaped
    port = ports[0]
    out = dict(rep)
    series = rep.get("series", [])
    aborted = rep.get("aborted")
    out["per_port"] = {port: {"account": rep.get("account"),
                              "series": len(series),
                              "added": rep.get("totals", {}).get("added", 0),
                              "aborted": aborted}}
    if aborted or not series:
        out.setdefault("aborted_ports", {})[port] = (
            aborted or "refetch produced no series")
    return out


_RESTART_EVENT_CAP = 256
_RESTART_REASON_CAP = 240
_RESTART_DECISION_RE = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_RESTART_EMAIL_RE = re.compile(
    r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_RESTART_ACCOUNT_RE = re.compile(r"\b(?:DU|U)\d{3,12}\b", re.IGNORECASE)
_RESTART_OUTCOME_FIELDS = frozenset({"ok", "attempted", "decision", "reason"})
_RESTART_POPUP_TIMING_FIELDS = (
    "scheduled_at", "callback_entered_at", "rendered_at",
    "countdown_armed_at", "settled_at", "expired_at")
_RESTART_TIMESTAMP_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z\Z")


def _bounded_restart_reason(value, fallback="restart outcome unavailable"):
    """One-line, redacted callback detail safe for progress/report artifacts."""
    try:
        text = " ".join(str(value).split())
    except Exception:  # noqa: BLE001 - hostile callback values stay diagnostic
        text = ""
    if not text:
        text = str(fallback)
    text = _RESTART_EMAIL_RE.sub("[redacted-email]", text)
    text = _RESTART_ACCOUNT_RE.sub("[redacted-account]", text)
    return text[:_RESTART_REASON_CAP]


def _restart_error_reason(prefix, exc):
    try:
        detail = str(exc)
    except Exception:  # noqa: BLE001 - exception formatting must not escape
        detail = "unprintable error"
    return _bounded_restart_reason(
        f"{prefix}: {type(exc).__name__}: {detail}", prefix)


def _normalize_popup_timing(value):
    if not isinstance(value, dict):
        return None
    timing = {}
    for field in _RESTART_POPUP_TIMING_FIELDS:
        stamp = value.get(field)
        timing[field] = (
            stamp if isinstance(stamp, str)
            and _RESTART_TIMESTAMP_RE.fullmatch(stamp) else None)
    return timing


def _restart_outcome(decision, *, ok=False, attempted=False, reason=None,
                     popup_timing=None):
    result = {"ok": bool(ok), "attempted": bool(attempted),
              "decision": decision,
              "reason": _bounded_restart_reason(reason or decision, decision)}
    timing = _normalize_popup_timing(popup_timing)
    if timing is not None:
        result["popup_timing"] = timing
    return result


def _normalize_restart_outcome(value):
    """Normalize the backward-compatible bool or strict structured result."""
    if type(value) is bool:
        return _restart_outcome(
            "legacy_callback", ok=value, attempted=value,
            reason=("legacy callback returned true" if value else
                    "legacy callback returned false; attempt state unavailable"))
    if not isinstance(value, dict):
        return _restart_outcome(
            "invalid_callback_return", reason="invalid_callback_return")
    if not _RESTART_OUTCOME_FIELDS.issubset(value):
        return _restart_outcome(
            "invalid_callback_return", reason="invalid_callback_return")
    ok = value.get("ok")
    attempted = value.get("attempted")
    decision = value.get("decision")
    reason = value.get("reason")
    if (type(ok) is not bool or type(attempted) is not bool
            or not isinstance(decision, str)
            or _RESTART_DECISION_RE.fullmatch(decision) is None
            or not isinstance(reason, str)):
        return _restart_outcome(
            "invalid_callback_return", reason="invalid_callback_return")
    return _restart_outcome(
        decision, ok=ok, attempted=attempted, reason=reason,
        popup_timing=value.get("popup_timing"))


def _call_restart_ports_batch(callback, ports, *, phase=None):
    """Call one batch callback and normalize every requested-port outcome.

    Production callbacks may explicitly opt into the optional phase keyword;
    legacy callbacks (including the offline reference harness) keep their
    original one-argument contract and are never invoked twice.
    """
    ports = [int(p) for p in ports]
    if not ports:
        return {}
    try:
        if bool(getattr(callback, "_accepts_restart_phase", False)):
            raw = callback(list(ports), phase=phase)
        else:
            raw = callback(list(ports))
    except Exception as exc:  # noqa: BLE001 - restart failure cannot abort fetch
        reason = _restart_error_reason("batch restart callback error", exc)
        return {p: _restart_outcome(
            "callback_error", attempted=False, reason=reason) for p in ports}

    port_keys = set(ports) | {str(p) for p in ports}
    try:
        wrong_shape = (not isinstance(raw, dict)
                       or (bool(raw) and not (set(raw) & port_keys)))
    except Exception:  # noqa: BLE001 - hostile mapping stays a failed outcome
        wrong_shape = True
    if wrong_shape:
        return {p: _restart_outcome(
            "invalid_callback_return", reason="invalid_callback_return")
                for p in ports}

    outcomes = {}
    for p in ports:
        try:
            if p in raw:
                value = raw[p]
            elif str(p) in raw:
                value = raw[str(p)]
            else:
                outcomes[p] = _restart_outcome(
                    "missing_batch_result", reason="missing_batch_result")
                continue
        except Exception:  # noqa: BLE001 - malformed mapping stays diagnostic
            outcomes[p] = _restart_outcome(
                "invalid_callback_return", reason="invalid_callback_return")
            continue
        outcomes[p] = _normalize_restart_outcome(value)
    return outcomes


_REPORT_RUN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


def _append_report_note(report, value):
    note = _bounded_restart_reason(value, "report not saved")
    existing = report.get("notes")
    if isinstance(existing, list):
        notes = existing
    else:
        notes = []
        if existing:
            notes.append(_bounded_restart_reason(existing))
        report["notes"] = notes
    if note not in notes:
        notes.append(note)


def _write_parallel_report(root, report, stage="final"):
    """Atomically persist one bounded-path parallel report, best effort."""
    if not isinstance(report, dict):
        return None
    run = report.get("run")
    if not isinstance(run, str) or _REPORT_RUN_RE.fullmatch(run) is None:
        report["report_path"] = None
        _append_report_note(report, f"{stage} report not saved: invalid run id")
        return None
    try:
        import json
        dest = ingest._RunDirs(Path(root), run).reports() / "report.json"
        persisted = dict(report)
        persisted["report_path"] = str(dest)
        payload = json.dumps(
            persisted, indent=1, sort_keys=True, default=str).encode("utf-8")
        ss._atomic_write_bytes(dest, payload)
    except Exception as exc:  # noqa: BLE001 - report I/O cannot abort recovery
        report["report_path"] = None
        _append_report_note(
            report, _restart_error_reason(f"{stage} report not saved", exc))
        return None
    report["report_path"] = str(dest)
    return str(dest)


def _gap_fill_parallel_resilient_core(root, selections, ports,
                                      restart_port=None, restart_ports=None,
                                      port_up=None, max_recover_rounds=2,
                                      on_recover=None, **kwargs):
    """gap_fill_parallel hardened against the TWS demo's daily auto-logout.

    The engine's per-request reconnect already rides out a brief TWS
    *restart* (RECONNECT_ATTEMPTS), but a demo *logout* leaves the API port
    dead until someone logs the instance back in — which never happens on its
    own. This wrapper closes that gap with two INJECTED callbacks (so this
    module stays GUI-free and unit-testable):

        port_up(port)      -> bool   # is the socket serving? (cheap probe)
        restart_port(port) -> bool | {ok, attempted, decision, reason}
        restart_ports(ps)  -> {port: bool | structured outcome}

    Flow:
      1. PRE-FLIGHT — probe every port; restart any that are already down
         (the overnight logout case) before fetching.
      2. FETCH — gap_fill_parallel as usual.
      3. RECOVER — for each port that ABORTED (its account dropped and never
         reconnected), restart just that port and re-fetch ONLY its tickers,
         merging the result. Repeats up to max_recover_rounds; a port that
         can't be restarted is left aborted (its tickers re-run next time).

    on_recover(phase, port, ok) is an optional notification hook
    (phase ∈ {"preflight", "recover"}). Returns the merged report with an
    extra "recovery_rounds" count and bounded "restart_events" evidence."""
    completion = kwargs.pop("_fetch_completion", None)
    def emit(m):
        p = kwargs.get("progress")
        if p is not None:
            try:
                p(m)
            except Exception:  # noqa: BLE001
                pass

    restart_events = []
    restart_events_truncated = 0

    def _record_restart_event(event):
        nonlocal restart_events_truncated
        if len(restart_events) < _RESTART_EVENT_CAP:
            restart_events.append(event)
        else:
            restart_events_truncated += 1
        emit(
            "restart "
            f"phase={event['phase']} round={event['round']} "
            f"port={event['port']} attempted={event['attempted']} "
            f"decision={event['decision']} "
            f"callback_ok={event['callback_ok']} "
            f"final_probe={event['final_probe']} ok={event['ok']} "
            f"recovered_by_probe={event['recovered_by_probe']} "
            f"reason={event['reason']}")

    def _attach_restart_events(target):
        if restart_events:
            target["restart_events"] = list(restart_events)
        if restart_events_truncated:
            target["restart_events_truncated"] = restart_events_truncated
        return target

    pstat = kwargs.get("port_status")

    def _pset(port, **info):
        # drive the GUI per-port monitor during restart/recovery so a recovered
        # port shows 'restarting…/re-fetching…' (not a stale 'done') and its
        # final counts land. No-op when there's no monitor (port_status=None).
        if pstat is not None:
            try:
                pstat(int(port), dict(info))
            except Exception:  # noqa: BLE001
                pass

    ports = list(dict.fromkeys(int(p) for p in ports))
    cancel = kwargs.get("cancel")

    def _cancelled():
        return cancel is not None and cancel.is_set()

    has_restart = restart_port is not None or restart_ports is not None

    def _restart_one(port):
        try:
            raw = restart_port(port)
        except Exception as exc:  # noqa: BLE001 — a failed restart is not fatal
            return _restart_outcome(
                "callback_error", attempted=False,
                reason=_restart_error_reason("restart callback error", exc))
        return _normalize_restart_outcome(raw)

    def _restart_batch(ports_list, phase, round_no=0):
        """Restart a BATCH of ports -> {port: ok}. Prefers a single
        restart_ports(list) call so the GUI can show ONE confirmation / one
        input-block session for the whole batch (e.g. all ports logged out);
        falls back to per-port restart_port. Records + notifies on_recover."""
        ports_list = [int(p) for p in ports_list]
        if not ports_list:
            return {}
        if restart_ports is not None:
            outcomes = _call_restart_ports_batch(
                restart_ports, ports_list, phase=phase)
        else:
            outcomes = {p: _restart_one(p) for p in ports_list}
        res = {}
        for p in ports_list:
            outcome = outcomes[p]
            callback_ok = outcome["ok"]
            final_ok = callback_ok
            final_probe = "not_run"
            recovered_by_probe = False
            reason = outcome["reason"]
            if not callback_ok and port_up is not None:
                try:
                    if bool(port_up(p)):
                        final_probe = "up"
                        final_ok = True
                        recovered_by_probe = True
                    else:
                        final_probe = "down"
                except Exception as exc:  # noqa: BLE001
                    final_probe = "error"
                    probe_reason = _restart_error_reason(
                        "final port probe error", exc)
                    reason = _bounded_restart_reason(
                        f"{reason}; {probe_reason}", reason)
            event = {
                "phase": phase,
                "round": int(round_no),
                "port": p,
                "decision": outcome["decision"],
                "attempted": outcome["attempted"],
                "callback_ok": callback_ok,
                "final_probe": final_probe,
                "ok": final_ok,
                "recovered_by_probe": recovered_by_probe,
                "reason": reason,
            }
            if outcome.get("popup_timing") is not None:
                event["popup_timing"] = dict(outcome["popup_timing"])
            _record_restart_event(event)
            res[p] = final_ok
            if on_recover is not None:
                try:
                    on_recover(phase, p, final_ok)
                except Exception:  # noqa: BLE001
                    pass
        return res

    # 1. pre-flight: ports down before we even start = the daily logout. Bring
    #    them back as ONE batch (one prompt / one input-block) so their tickers
    #    aren't skipped wholesale.
    if has_restart and port_up is not None and not _cancelled():
        down = []
        for p in ports:
            if _cancelled():
                break
            try:
                up = bool(port_up(p))
            except Exception:  # noqa: BLE001
                up = False
            if not up:
                down.append(p)
        if down:
            emit(f"{len(down)} port(s) down (logged out?) — restarting before "
                 f"the fetch: {', '.join(map(str, down))}")
            _restart_batch(down, "preflight")

    pass_completion = {} if completion is not None else None
    try:
        rep = gap_fill_parallel(
            root, selections, ports, port_up=port_up,
            restart_ports=restart_ports,
            **({"_fetch_completion": pass_completion} if completion is not None else {}), **kwargs)
    except BaseException:
        if completion is not None and pass_completion.get("report") is not None:
            completion["report"] = pass_completion["report"]
        raise
    if completion is not None:
        completion["report"] = rep
    _attach_restart_events(rep)
    if rep.get("fleet_down_finalize"):
        rep["recovery_rounds"] = 0
        _write_parallel_report(root, rep, stage="final")
        return rep

    # A port RE-ADOPTED mid-run (its queue work already drained by a fresh worker)
    # that is STILL listening must not be relaunched (that kills a healthy instance
    # behind an input-blocking popup) nor reported 'NOT fetched'. Drop it from
    # aborted_ports; its single lost in-flight job (if any) is filled by the gap
    # system on the next run. Scoped to readopted_ok, so a normal aborted port is
    # unaffected (the recovery + its selftests behave exactly as before).
    _radopt = set(rep.get("readopted_ok") or [])
    if _radopt and port_up is not None and rep.get("aborted_ports"):
        for p in list(rep["aborted_ports"]):
            if int(p) not in _radopt:
                continue
            try:
                still_up = bool(port_up(int(p)))
            except Exception:  # noqa: BLE001
                still_up = False
            if still_up:
                rep["aborted_ports"].pop(p, None)
                emit(f"port {p} was re-adopted mid-run and is still up — "
                     f"skipping its recovery restart")
        if not rep["aborted_ports"]:
            rep.pop("aborted_ports", None)

    def _snapshot_report(src):
        try:
            import copy
            return copy.deepcopy(src)
        except Exception:  # noqa: BLE001
            return dict(src)

    def _dedupe_series(seq):
        out, seen = [], set()
        for s in seq or []:
            try:
                key = (s[0], s[1])
            except Exception:  # noqa: BLE001
                continue
            if key in seen:
                continue
            seen.add(key)
            out.append(s)
        return out

    # 2. post-run recovery for ports that aborted mid-fetch — but never drive a
    #    relaunch after the user cancelled (a cancel can co-occur with a
    #    sibling abort, leaving aborted_ports set AND cancelled=True).
    if (has_restart and rep.get("aborted_ports") and not _cancelled()
            and not rep.get("cancelled")):
        emit(("report", _snapshot_report(rep)))
    rounds = 0
    while (has_restart and rep.get("aborted_ports")
           and rounds < max_recover_rounds and not _cancelled()
           and not rep.get("cancelled")):
        rounds += 1
        dead = [int(p) for p in rep["aborted_ports"]]
        part = rep.get("_partition") or partition_tickers(selections, ports)
        pending_refetch = _dedupe_series(
            [s for p in dead for s in part.get(p, [])]
            + list(rep.get("_unpulled", []) or []))
        emit(("recovering", {"round": rounds, "ports": list(dead),
                             "series": len(pending_refetch)}))
        for p in dead:                          # monitor: these are NOT done
            _pset(p, state="retrying", ticker="restarting…", count=0)
        emit(f"recovery round {rounds}: {len(dead)} port(s) could not "
             f"reconnect ({', '.join(map(str, dead))}) — restarting…")
        res = _restart_batch(dead, "recover", rounds)
        if _cancelled():
            rep["cancelled"] = True
            emit("recovery cancelled before re-fetch; committed months stay "
                 "and a normal re-run resumes.")
            break
        recovered = [p for p in dead if res.get(p)]
        if not recovered:
            for p in dead:
                _pset(p, state="error", error="restart failed")
            emit("no aborted port could be restarted — leaving them for the "
                 "next run")
            break
        # re-fetch what the dead ports actually pulled (the RECORDED per-port
        # assignment — the dynamic queue's real history, not a recomputed guess),
        # PLUS any jobs that were never pulled (the all-aborted case). Pop the
        # unpulled set so a second round can't re-fetch it; committed series are
        # skipped by the manifest resume either way.
        refetch = _dedupe_series(
            [s for p in recovered for s in part.get(p, [])]
            + list(rep.pop("_unpulled", []) or []))
        if not refetch:
            break
        for p in recovered:                     # monitor: now re-fetching
            _pset(p, state="working", ticker="re-fetching…", count=0)
        emit(f"re-fetching {len(refetch)} series on the recovered "
             f"port(s) {', '.join(map(str, recovered))}…")
        pass_completion = {} if completion is not None else None
        try:
            extra = gap_fill_parallel(root, refetch, recovered,
                port_up=port_up,
                **({"_fetch_completion": pass_completion} if completion is not None else {}), **kwargs)
        except BaseException:
            if completion is not None:
                partial = pass_completion.get("report")
                completion["report"] = (_merge_recovery(rep, partial, recovered)
                    if partial is not None else rep)
            raise
        extra = _as_parallel_shape(extra, recovered)   # serial 1-port -> shape
        rep = _merge_recovery(rep, extra, recovered)
        if completion is not None:
            completion["report"] = rep
        _attach_restart_events(rep)
        for p in recovered:                     # monitor: reflect recovered count
            pp = (rep.get("per_port") or {}).get(p, {})
            _pset(p, state="done", account=pp.get("account"),
                  added=pp.get("added", 0), count=pp.get("series", 0),
                  ticker="—")

    rep["recovery_rounds"] = rounds
    _attach_restart_events(rep)
    _write_parallel_report(root, rep, stage="final")
    return rep


@_fetch_run_gate
def gap_fill_parallel_resilient(root, selections, ports, restart_port=None,
                                restart_ports=None, port_up=None,
                                max_recover_rounds=2, on_recover=None, _fetch_parent=None,
                                **kwargs):
    """Run resilient fill/recovery while holding the production fetch gate."""
    fops.parent_context(_fetch_parent)
    return _gap_fill_parallel_resilient_core(
        root, selections, ports, restart_port=restart_port,
        restart_ports=restart_ports, port_up=port_up,
        max_recover_rounds=max_recover_rounds, on_recover=on_recover,
        _fetch_parent=_fetch_parent, **kwargs)


def lost_connection_series(report):
    """The series in `report` that halted because the connection gave up
    after RECONNECT_ATTEMPTS — the set the GUI raises a popup for."""
    return [s for s in report.get("series", [])
            if "reconnect attempts" in (s.get("halt") or "")]


def progress_summary(s):
    """How far a (possibly halted) series got: original stored end ->
    last committed day, with an APPROXIMATE trading-day count = added
    bars / one full session. GUI-free + testable. Used by the
    connection-lost popup."""
    tic, iv = s.get("ticker", "?"), s.get("interval", "?")
    added = s.get("added", 0)
    frm, to = s.get("stored_through"), s.get("committed_through")
    exp = (_expected_session_bars(iv)
           if ss.base_interval(iv) in _BAR_SIZES else None)
    frm_s = (frm.date().isoformat() if hasattr(frm, "date")
             else "new series (was empty)")
    if to is None:
        return (f"{tic} {iv}: from {frm_s} — nothing new committed yet "
                f"(0 trading days)")
    to_s = to.isoformat() if hasattr(to, "isoformat") else str(to)
    days = f"~{added / exp:.1f}" if exp else "?"
    return (f"{tic} {iv}: {frm_s} -> {to_s} "
            f"({days} trading days, {added:,} bars saved)")


def _split_proposal_summary(series):
    enrichment = series.get("split_enrichment")
    if not isinstance(enrichment, dict):
        return None
    if (enrichment.get("status") != "confirmed_split_proposal"
            or enrichment.get("requires_user_approval") is not True
            or enrichment.get("applied") is not False):
        return None
    action = enrichment.get("action")
    if not isinstance(action, dict):
        return None
    factor = action.get("factor")
    if isinstance(factor, bool) or not isinstance(factor, (int, float)):
        return None
    boundary = str(enrichment.get("boundary_date") or "?")[:16]
    return (f"SPLIT PROPOSAL: cached evidence matched {boundary}; price "
            f"factor {factor:.8g} requires approval and is NOT APPLIED")


def summarize_report(report):
    """Terse human lines for the issues pane (GUI-free, testable)."""
    out = []
    ledger = report.get("fetch_ledger")
    if ledger is not None:
        out.append(f"Fetch ledger {ledger['state']}: {ledger['path']}")
        if ledger.get("failure"):
            out.append(f"  ledger failure: {ledger['failure']}")
    if report.get("fleet_down_finalize"):
        out.append(
            "ADD STOCKS INTERRUPTED: every TWS port remained unavailable "
            "past the fleet grace window; committed months and port-free "
            "checks were kept, and volatility reconciles, live probes, and "
            "gap seals remain pending for Resume.")
    if report.get("aborted"):
        out.append(f"IBKR UPDATE ABORTED: {report['aborted']}")
        return out
    t = report.get("totals", {})
    # Aggregate the silent-drop reject counters across every series so the
    # run line can surface them (each series carries res["counters"] =
    # {"non_rth", "invalid", "outside_day"}; B1 — make them visible with
    # NO GUI edit since the GUI renders these lines verbatim).
    agg = {"non_rth": 0, "invalid": 0, "outside_day": 0}
    fetched_total = 0
    for r in report.get("series", []):
        c = r.get("counters") or {}
        for k in agg:
            agg[k] += c.get(k, 0)
        fetched_total += r.get("bars_fetched", 0)
    out.append(f"IBKR {report['run']} (acct {report.get('account', '?')}"
               f", port {report.get('port', '?')}): "
               f"+{t.get('added', 0):,} rows in "
               f"{t.get('written', 0):,} month file(s), "
               f"{t.get('dup_existing', 0):,} already present, "
               f"{t.get('conflicts', 0):,} conflict(s) (existing kept), "
               f"{t.get('requests', 0):,} request(s)"
               + (f", {t['halted_series']} series HALTED"
                  if t.get("halted_series") else "")
               + ((f", dropped {agg['invalid']:,} invalid"
                   f"/{agg['non_rth']:,} non-RTH"
                   f"/{agg['outside_day']:,} outside-day bar(s)")
                  if any(agg.values()) else "")
               + (", CANCELLED" if report.get("cancelled") else ""))
    reconcile = report.get("vol_value_reconcile")
    if isinstance(reconcile, dict):
        planned = int(reconcile.get("planned_count") or 0)
        completed = int(reconcile.get("completed_count") or 0)
        requests = int(reconcile.get("request_count") or 0)
        request_unknown = int(reconcile.get("request_count_unknown") or 0)
        settled = int(reconcile.get("settled_count") or 0)
        corrected = int(reconcile.get("corrected_count") or 0)
        unresolved = int(reconcile.get("unresolved_count") or 0)
        plan_errors = int(reconcile.get("plan_error_count") or 0)
        queue_pending = int(reconcile.get("queue_pending_count") or 0)
        pending = (len(reconcile.get("pending_rows") or [])
                   + int(reconcile.get("pending_rows_truncated") or 0)
                   + len(reconcile.get("pending_plan_tickers") or [])
                   + int(reconcile.get(
                       "pending_plan_tickers_truncated") or 0)
                   + len(reconcile.get("pending_fill_tickers") or [])
                   + int(reconcile.get(
                       "pending_fill_tickers_truncated") or 0))
        halted_fill = (len(reconcile.get("halted_fill_tickers") or [])
                       + int(reconcile.get(
                           "halted_fill_tickers_truncated") or 0))
        status = str(reconcile.get("status") or "unknown")
        out.append(
            f"  Volatility refetch: {completed:,}/{planned:,} day(s), "
            f"{requests:,} request(s), {settled:,} settled, "
            f"{corrected:,} corrected, {unresolved:,} unresolved, "
            f"{queue_pending:,} queue row(s) pending"
            + (f", {plan_errors:,} planning error(s)" if plan_errors else "")
            + (f", {request_unknown:,} request state(s) unknown"
               if request_unknown else "")
            + (f", {pending:,} unstarted" if pending else "")
            + (f", {halted_fill:,} fill-blocked ticker(s)"
               if halted_fill else "")
            + f" - {status}")
        if reconcile.get("report"):
            out.append(f"  Volatility report: {reconcile['report']}")
        elif reconcile.get("report_error"):
            out.append(
                "  WARNING: volatility report not saved: "
                f"{reconcile['report_error']}")
    probe = report.get("spot_probe")
    if isinstance(probe, dict):
        issues = int(probe.get("issue_count") or 0)
        completed = int(probe.get("completed_count") or 0)
        requests = int(probe.get("request_count") or 0)
        out.append(
            f"  WS8 spot probes: {completed:,} ticker(s), "
            f"{requests:,} request(s), {issues:,} issue(s)"
            + (" - completed with probe issues" if issues else " - clean"))
        if probe.get("report"):
            out.append(f"  WS8 report: {probe['report']}")
        elif probe.get("report_error"):
            out.append(f"  WARNING: WS8 report not saved: "
                       f"{probe['report_error']}")
    # Surface dead accounts in a PARTIAL parallel failure (some ports OK, some
    # offline). Without this, an offline account's unfetched tickers would
    # vanish silently because only a FULL abort sets the top-level 'aborted'.
    for _port, _err in (report.get("aborted_ports") or {}).items():
        out.append(f"  ⚠ ACCOUNT OFFLINE  port {_port}: {_err} — its tickers "
                   f"were NOT fetched (re-run to fill them).")
    # LOUDER: a high invalid:fetched ratio across the run usually means a
    # bar-size / feed-leak misconfiguration (daily bars leaking through),
    # not normal noise — flag it the same way other warnings read.
    if fetched_total and agg["invalid"] / fetched_total > 0.01:
        out.append(f"  WARNING: {agg['invalid']:,} of {fetched_total:,} "
                   f"fetched bar(s) invalid "
                   f"({100.0 * agg['invalid'] / fetched_total:.1f}%) — "
                   f"likely a bar-size / feed-leak misconfiguration")
    # WRITE FAILED months were FETCHED but could NOT be saved (disk full / the
    # data folder is locked). Without a run-level line a systemic failure ends
    # "normally" while writing nothing — make it impossible to miss.
    wf = []
    for r in report.get("series", []):
        fails = [str((mst or {}).get("status", ""))
                 for mst in (r.get("months") or {}).values()
                 if str((mst or {}).get("status", "")).startswith("WRITE FAILED")]
        if fails:
            wf.append((f"{r['ticker']} {r['interval']}", len(fails), fails[0]))
    if wf:
        nfiles = sum(n for _nm, n, _ex in wf)
        out.append(f"  ‼ WRITE FAILED: {nfiles:,} month file(s) across "
                   f"{len(wf)} series were fetched but COULD NOT be saved (disk "
                   f"full or the data folder is locked). Those bars are NOT on "
                   f"disk — free space / unlock the folder and re-run to save "
                   f"them.")
        for nm, n, ex in wf[:8]:
            out.append(f"      {nm}: {n} unsaved — {ex}")
        if len(wf) > 8:
            out.append(f"      …and {len(wf) - 8} more series")
    for r in report.get("series", []):
        name = f"{r['ticker']} {r['interval']}"
        if r.get("halt"):
            out.append(f"  HALTED    {name}: {r['halt']}")
            proposal = _split_proposal_summary(r)
            if proposal:
                out.append(f"            {name}: {proposal}")
        else:
            bits = [f"+{r['added']:,} rows",
                    f"{r['written']} month(s) written",
                    f"{r['requests']} req"]
            if r.get("conflicts"):
                bits.append(f"{r['conflicts']:,} conflict(s), existing "
                            f"kept")
            if r.get("blocked_months"):
                bits.append(f"{len(r['blocked_months'])} month(s) "
                            f"BLOCKED")
            wfm = sum(1 for mst in (r.get("months") or {}).values()
                      if str((mst or {}).get("status", "")).startswith(
                          "WRITE FAILED"))
            if wfm:
                bits.append(f"{wfm} month(s) WRITE-FAILED (not saved)")
                out.append(f"  WRITE-FAIL {name}: " + ", ".join(bits))
            else:
                out.append(f"  ok        {name}: " + ", ".join(bits))
        # terse per-series reject line when the counters aren't trivial.
        c = r.get("counters") or {}
        if any(c.get(k) for k in ("non_rth", "invalid", "outside_day")):
            fetched = r.get("bars_fetched", 0)
            out.append(
                f"            {name}: dropped {c.get('invalid', 0):,} "
                f"invalid, {c.get('non_rth', 0):,} non-RTH, "
                f"{c.get('outside_day', 0):,} outside-day "
                f"(of {fetched:,} fetched)")
            # a HIGH per-series invalid ratio is the tell-tale of a bar-size
            # / feed-leak misconfiguration on THAT series — flag it even when
            # the run-wide ratio is diluted by other clean series.
            if fetched and c.get("invalid", 0) / fetched > 0.01:
                pct = 100.0 * c.get("invalid", 0) / fetched
                out.append(
                    f"            WARNING: {name} — {pct:.1f}% of fetched "
                    f"bars invalid ({c.get('invalid', 0):,}/{fetched:,}), "
                    f"likely a bar-size / feed-leak misconfiguration")
        for n in r.get("notes", []):
            out.append(f"            {name}: {n}")
    if report.get("conflict_log"):
        out.append(f"  conflict examples: {report['conflict_log']}")
    if report.get("report_path"):
        out.append(f"  full report: {report['report_path']}")
    return out


# --- Connection Doctor ----------------------------------------------------------------

def _import_ib_async():
    import ib_async
    return ib_async.__version__


def _port_open(host, port, timeout=0.6):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        return s.connect_ex((host, port)) == 0
    finally:
        s.close()


def _latest_completed_session_end(now=None):
    """Return the local-ET end of the latest fully completed NYSE session.

    The connection doctor only needs a known-complete historical slice to
    prove that HMDS returns bars.  Pointing a one-day request at today's RTH
    close before that close exists yields a legitimate empty response and
    falsely classifies every healthy premarket account as permissionless.
    Use the prior trading session until today's regular/early close has
    actually passed, and skip full-closure days and weekends.
    """
    current = now or now_ny()
    day = current.date()
    if (mc.is_trading_day(day)
            and current.time() >= mc.expected_close(day)):
        return datetime.combine(day, mc.expected_close(day))
    day -= timedelta(days=1)
    while not mc.is_trading_day(day):
        day -= timedelta(days=1)
    return datetime.combine(day, mc.expected_close(day))


def doctor(host=HOST_DEFAULT, ports=PORTS_DEFAULT, adapter_factory=None,
           probe_fn=None, import_fn=None, cancel=None, *, evidence_dir=None,
           _test_capability=None):
    operation = fops.begin_operation("diagnostic", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "connection", (),
        {"host": host, "ports": ports, "adapter_factory": adapter_factory,
         "probe_fn": probe_fn, "import_fn": import_fn, "cancel": cancel}, evidence_dir)


@fib.worker_scope
def _doctor_body(host=HOST_DEFAULT, ports=PORTS_DEFAULT, adapter_factory=None,
                 probe_fn=None, import_fn=None, cancel=None):
    """Step-by-step connection diagnosis -> [(status, line)] with
    plain-English advice. Safety/durability errors propagate. probe_fn/import_fn are test
    seams: every step must be exercisable without ib_async installed
    or a listening TWS (the module contract). `cancel` is honoured
    BETWEEN steps: the remaining (possibly slow: connect ~4s, qualify
    ~20s, sample fetch ~60s) steps are skipped and a FAIL 'cancelled'
    row ends the report — callers gate on the event before acting."""
    rows = []

    def add(ok, line):
        rows.append(("ok" if ok else "FAIL", line))
        return ok

    def _cancelled():
        if cancel is not None and cancel.is_set():
            add(False, "cancelled by user")
            return True
        return False

    tz = ss.tzdata_problem()
    if not add(tz is None, tz or "timezone database present"):
        return rows
    try:
        ver = fib.without_authority(import_fn or _import_ib_async)()
        add(True, f"ib_async {ver} importable")
    except ImportError:
        add(False, "ib_async not installed — run: pip install ib_async")
        return rows
    if _cancelled():
        return rows
    probe = fib.without_authority(probe_fn or _port_open)
    open_ports = [p for p in ports if probe(host, p)]
    if not add(bool(open_ports),
               (f"listening: {open_ports}" if open_ports else
                f"nothing listening on {list(ports)} — start TWS or IB "
                f"Gateway and log in")):
        return rows
    if _cancelled():
        return rows
    try:
        adapter = _auxiliary_adapter(adapter_factory, host, tuple(open_ports))
    except (AuthorityError, LedgerError, Cancelled):
        raise
    except Exception as exc:  # noqa: BLE001
        add(False, f"socket open but the API handshake failed ({exc}) — "
                   f"in TWS: File > Global Configuration > API > "
                   f"Settings > check 'Enable ActiveX and Socket "
                   f"Clients'; accept the connection popup; add "
                   f"127.0.0.1 to trusted IPs")
        return rows
    try:
        acct = adapter.account()
        paper = str(acct).startswith("D")
        add(True, f"connected on port {getattr(adapter, 'port', '?')} "
                  f"as {acct}"
                  + (" (paper — good)" if paper else
                     " — LIVE account! keep Read-Only API ON"))
        if _cancelled():
            return rows
        try:
            with fib.qualification_scope("qualify", ["AAPL"], fib.acquire_turn, cancel=cancel):
                conid, contract = adapter.qualify("AAPL")
            add(True, f"contract lookup works (AAPL conId {conid})")
        except SeriesHalt as exc:
            add(False, f"contract lookup failed: {exc}")
            return rows
        if _cancelled():
            return rows
        context = fib.current_worker().context
        horizon = context.horizons["1m"]
        if horizon is None:
            raise CalendarUnsupported("no settled diagnostic minute is covered")
        end = horizon.replace(tzinfo=None)
        opening, _ = context.authority.window("1m", horizon.date())
        from fetch_envelopes import encode_carrier
        duration = encode_carrier(opening, horizon)
        try:
            request = fib.bar_request("ibkr.stock_ibkr.doctor", contract, "1m", end,
                                      duration, start=opening)
            with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn, cancel=cancel):
                raw = adapter.fetch(contract, end, duration, "1 min")
            add(bool(raw), f"historical data flows ({len(raw)} bars for "
                           f"the latest session)"
                if raw else "connected but ZERO bars came back — "
                            "market data permissions? (trial logins "
                            "and some unfunded accounts are refused)")
        except (SeriesHalt, ConnectionError) as exc:
            add(False, f"historical data request failed: {exc}")
    finally:
        _auxiliary_disconnect(adapter)
    return rows


def preflight(host=HOST_DEFAULT, ports=PORTS_DEFAULT,
              adapter_factory=None, probe_fn=None, import_fn=None,
              cancel=None, *, evidence_dir=None, _test_capability=None):
    """Go/no-go gate for the moment BEFORE a fill run touches the
    archive: ok only when every doctor step passes INCLUDING the
    sample data fetch — a connection that returns zero bars is a dud
    and must never be mistaken for 'nothing to do'. Returns
    (ok, failures) where failures are the plain-English FAIL lines.
    `cancel` passes through to doctor (checked between steps)."""
    rows = doctor(host=host, ports=ports,
                  adapter_factory=adapter_factory,
                  probe_fn=probe_fn, import_fn=import_fn, cancel=cancel,
                  evidence_dir=evidence_dir, _test_capability=_test_capability)
    failures = [line for status, line in rows if status != "ok"]
    return (bool(rows) and not failures), failures


def tws_listening(host=HOST_DEFAULT, ports=PORTS_DEFAULT,
                  probe_fn=None):
    """True when something answers on a TWS/Gateway port. A cheap
    pre-dialog gate (the full preflight still runs before any write);
    on localhost a closed port refuses instantly, so this is fast."""
    probe = probe_fn or _port_open
    return any(probe(host, p) for p in ports)


def _auxiliary_directory(evidence_dir):
    return (Path(evidence_dir) if evidence_dir is not None else
            Path(__file__).resolve().parents[1] / "_ingest_reports" / "fetch-ledgers")


def _auxiliary_adapter(factory=None, *args):
    if factory is None:
        factory = fib.without_authority(live_adapter_factory)(*args)
    return fib.without_authority(factory)()


def _auxiliary_disconnect(adapter):
    try:
        # Retrieve even a descriptor-backed callback inside observer isolation.
        fib.without_authority(lambda: adapter.disconnect())()
    except (AuthorityError, LedgerError, Cancelled):
        raise
    except Exception:  # noqa: BLE001 — cleanup cannot invent successful evidence
        pass


def _run_auxiliary_operation(operation, mode, args, kwargs, evidence_dir):
    """Named non-bank roots preserve return shapes and always leave a companion."""
    context, purpose = operation.context, operation.purpose
    output, failure, outcome = None, None, "returned"
    completion = {"state": "not_started"} if mode == "repair" else None
    try:
        fops.require_root_admission(operation)
        target = {"search": _find_symbol_body, "company": _company_lookup_body,
                  "identity": _validate_symbols_body, "connection": _doctor_body,
                  "estimate": _estimate_backfill_body, "span": _probe_max_span_body,
                  "repair": _fill_missing_days_body}[mode]
        target_options = dict(kwargs)
        if completion is not None:
            target_options["_completion"] = completion
        child_rights = (fops.PURPOSE_PRODUCERS[purpose] - {"http.stockanalysis.validation"}
                        if mode == "repair" else None)
        output = target(*args, **target_options,
                        _fetch_child=operation.child(mode + "-root", rights=child_rights))
        if isinstance(output, dict) and (output.get("error") or output.get("cancelled")):
            outcome = "cancelled" if output.get("cancelled") else "failed"
        elif mode == "repair" and any(output.get(key) for key in
                ("blocked", "unfilled", "unsettled_days", "calendar_unsupported_days")):
            outcome = "partial"
        elif mode == "search" and output[1] is not None:
            outcome = "failed"
        elif mode == "connection" and (not output or any(status != "ok" for status, _ in output)):
            outcome = "failed"
        operation.seal()
    except BaseException as exc:
        failure, outcome = exc, "failed"
    try:
        operation.close()
    except BaseException as exc:
        failure, outcome = failure or exc, "failed"
    # Formatting an observer-supplied exception can invoke its __str__ hook.
    evidence = fib.without_authority(freport.evidence)(
        context, purpose, outcome=outcome, error=failure)
    if completion is not None:
        # This object belongs to the fixed engine dispatcher, not to a
        # provider/observer exception that could supply fabricated progress.
        evidence["targeted_fill"] = deepcopy(completion)
    try:
        evidence["report_path"] = freport.write_report(evidence, evidence_dir)
    except freport.OperationReportError as report_error:
        if completion is not None:
            # Keep a truthful in-memory diagnostic when durable reporting
            # itself fails. This is explicitly NOT a persisted companion.
            evidence.update(verified=False, state="UNVERIFIED",
                operation_outcome=outcome, outcome="report_failed",
                report_persisted=False, report_failure=str(report_error))
            fib.without_authority(setattr)(report_error, "fetch_ledger", evidence)
        raise
    if failure is not None:
        try:
            fib.without_authority(setattr)(failure, "fetch_ledger", evidence)
        except Exception:
            pass
        raise failure
    if isinstance(output, dict):
        output["fetch_ledger"] = evidence
    return output


def find_symbol(text, adapter_factory=None, *, cancel=None, evidence_dir=None,
                _test_capability=None):
    operation = fops.begin_operation("identity", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "search", (text,),
        {"adapter_factory": adapter_factory, "cancel": cancel}, evidence_dir)


def company_lookup(symbol, adapter_factory=None, *, cancel=None, evidence_dir=None,
                   _test_capability=None):
    operation = fops.begin_operation("identity", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "company", (symbol,),
        {"adapter_factory": adapter_factory, "cancel": cancel}, evidence_dir)


@fib.worker_scope
def _company_lookup_body(symbol, adapter_factory=None, cancel=None):
    adapter = _auxiliary_adapter(adapter_factory)
    try:
        with fib.qualification_scope("qualify", [symbol], fib.acquire_turn, cancel=cancel):
            con_id, pinned = adapter.qualify(symbol)
        if (type(con_id) is not int or con_id <= 0 or pinned.conId != con_id
                or getattr(pinned, "secType", None) != "STK"):
            raise RequestRefused("company lookup requires a qualified stock identity")
        request = fib.metadata_request("company_name", symbol, con_id)
        with fib.send_scope(request, fib.acquire_turn, cancel=cancel):
            return adapter.company_name(symbol, contract=pinned)
    finally:
        _auxiliary_disconnect(adapter)


@fib.worker_scope
def _find_symbol_body(text, adapter_factory=None, cancel=None):
    """GUI-free name/ticker lookup through a (possibly injected)
    adapter. -> (matches, error): matches as LiveIB.search dicts;
    error is plain English when TWS is unreachable (callers fall back
    to matching the local tree only)."""
    if isinstance(text, str) and not text.strip():
        return [], "type a company name or ticker first"
    text = fib.canonical_search(text)
    try:
        adapter = _auxiliary_adapter(adapter_factory)
    except (ConnectionError, OSError, ImportError) as exc:
        return [], (f"TWS/Gateway unreachable ({exc}) — the name "
                    f"search needs it; only stored tickers can be "
                    f"matched offline")
    try:
        request = fib.metadata_request("symbol_search", text)
        with fib.send_scope(request, fib.acquire_turn, cancel=cancel):
            return adapter.search(text), None
    except (ConnectionError, SeriesHalt) as exc:
        return [], str(exc)
    finally:
        _auxiliary_disconnect(adapter)


def validate_symbols(symbols, adapter_factory=None, progress=None,
                     cancel=None, identity_root=None, identity_today=None, *,
                     evidence_dir=None, _test_capability=None, _fetch_child=None):
    options = {"adapter_factory": adapter_factory, "progress": progress,
               "cancel": cancel, "identity_root": identity_root,
               "identity_today": identity_today}
    if _fetch_child is not None:
        if _test_capability is not None or evidence_dir is not None:
            return fib.refuse_transferred_child(
                _fetch_child, "root-only options cannot accompany a transferred child")
        return _validate_symbols_body(symbols, **options, _fetch_child=_fetch_child)
    operation = fops.begin_operation("identity", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "identity", (symbols,), options, evidence_dir)


@fib.worker_scope
def _validate_symbols_body(symbols, adapter_factory=None, progress=None,
                           cancel=None, identity_root=None, identity_today=None):
    """Pre-flight a BATCH: which symbols resolve to a live SMART/USD
    contract and which do NOT — WITHOUT fetching bars on the default path.
    ``identity_root`` enables the Add Stocks pre-write identity gate; only an
    existing history with no conId-bound cached frontier gets one bounded daily
    probe on this same connection. Returns
    {'found': [...], 'not_found': [...], 'error': str|None}, both lists
    in the input order. Lets the GUI ask 'stop or skip?' before the writable
    fill starts. Order-preserving and de-duplicated. `cancel` is
    honoured between qualify chunks -> {'cancelled': True, 'error':
    'cancelled'} (nothing was fetched; nothing can have been written)."""
    syms = list(dict.fromkeys(s for s in symbols if s))
    if not syms:
        return {"found": [], "not_found": [], "error": "no symbols"}
    if cancel is not None and cancel.is_set():
        return {"found": [], "not_found": [], "error": "cancelled",
                "cancelled": True}
    secid = None
    try:
        adapter = _auxiliary_adapter(adapter_factory)
    except (ConnectionError, OSError, ImportError) as exc:
        return {"found": [], "not_found": [], "error": str(exc)}
    try:
        with fib.qualification_scope("qualify_many", syms,
                lambda: fib.acquire_turn(cancel=cancel), cancel=cancel):
            resolved = adapter.qualify_many(syms, progress=progress,
                                            cancel=cancel)
        if identity_root is not None:
            secid = check_add_identities(
                identity_root, resolved, adapter,
                progress=progress, cancel=cancel, today=identity_today)
    except Cancelled:
        return {"found": [], "not_found": [], "error": "cancelled",
                "cancelled": True}
    except (ConnectionError, SeriesHalt) as exc:
        return {"found": [], "not_found": [], "error": str(exc)}
    finally:
        _auxiliary_disconnect(adapter)
    found = [s for s in syms if resolved.get(s)]
    not_found = [s for s in syms if not resolved.get(s)]
    # return the conId map so gap_fill can REUSE it instead of re-resolving
    # every symbol a second time (no double "Checking … at IBKR" pass).
    result = {"found": found, "not_found": not_found, "error": None,
              "resolved": resolved}
    if identity_root is not None:
        result["secid"] = secid or {}
    return result


def _identity_date(value):
    if isinstance(value, dict):
        value = value.get("earliest") or value.get("date")
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "").strip()[:10])
    except (TypeError, ValueError):
        return None


def _identity_stored_first(manifest):
    """Earliest stored TRADES bar across the ticker's sessions/intervals."""
    best = None
    for interval, section in ((manifest or {}).get("intervals") or {}).items():
        try:
            if ss.kind_of(interval):
                continue
        except Exception:  # noqa: BLE001 - malformed tokens are not evidence
            continue
        if not isinstance(section, dict):
            continue
        first = series_first_dt(manifest, interval)
        if first is None:
            # Old manifests can have present month records without exact first
            # timestamps.  The month floor is conservative identity evidence;
            # treating such a series as empty would bypass the reuse gate.
            for ym, entry in ((section or {}).get("months") or {}).items():
                if (not isinstance(entry, dict)
                        or str(entry.get("status") or "present").lower()
                        != "present"):
                    continue
                try:
                    year, month = int(str(ym)[:4]), int(str(ym)[5:7])
                    candidate = datetime(year, month, 1)
                except (TypeError, ValueError):
                    continue
                if first is None or candidate < first:
                    first = candidate
        if first is not None and (best is None or first < best):
            best = first
    return best


def _identity_earliest_cache(root, supplied=None):
    if supplied is not None:
        raw = supplied
    else:
        try:
            import stock_validate as _sv
            raw = _sv.load_ibkr_earliest(root)
        except Exception:  # noqa: BLE001 - missing cache becomes unverified
            raw = {}
    return {str(k).strip().upper(): v for k, v in (raw or {}).items()}


def check_security_ids(root, resolved, served_earliest=None,
                       check_history=False):
    """Validate the security_id (conId) of each to-be-built symbol against what
    the bank ALREADY stores for that ticker — the guard that would have caught
    GENZ (a reused ticker whose live conId is a DIFFERENT company than the
    stored history) and XOM (a dead stored conId). `resolved` = {symbol: conId}
    from validate_symbols. Returns
    {canonical_ticker: detail}. The baseline statuses remain ``new``, ``ok``,
    and ``mismatch``. With ``check_history=True``, existing TRADES history also
    yields ``ok_unpinned``, ``history_mismatch``, or ``history_unverified``.
    The latter two are fail-closed Add Stocks verdicts, preventing a top-up
    from silently splicing predecessor history onto the current security.
    Read-only; a missing/unreadable manifest is treated as a new ticker."""
    out = {}
    earliest = (_identity_earliest_cache(root, served_earliest)
                if check_history else {})
    for sym, cid in (resolved or {}).items():
        if not cid:
            continue
        try:
            canon = ss.canonical_ticker(sym)
        except Exception:  # noqa: BLE001 — odd spelling: compare on the raw form
            canon = str(sym).strip().upper()
        man = ss.load_manifest(Path(root) / canon)
        stored = (man or {}).get("conid")
        stored_first_dt = _identity_stored_first(man)
        stored_first = (stored_first_dt.date()
                        if stored_first_dt is not None else None)
        current_first = _identity_date(earliest.get(canon))
        conid_mismatch = False
        if man and stored is not None:
            try:
                conid_mismatch = int(stored) != int(cid)
            except (TypeError, ValueError):
                conid_mismatch = True

        gap_days = None
        reason = None
        if not man or (stored is None and stored_first is None):
            status = "new"
        elif conid_mismatch:
            status = "mismatch"
            reason = "stored conId differs from the current IBKR contract"
        elif not check_history or stored_first is None:
            status = "ok" if stored is not None else "new"
        elif current_first is None:
            status = "history_unverified"
            reason = "current listing-history frontier is unavailable"
        else:
            gap_days = (current_first - stored_first).days
            if gap_days > BACKFILL_SEAL_TOLERANCE_DAYS:
                status = "history_mismatch"
                reason = ("stored TRADES history materially predates the "
                          "current IBKR contract")
            else:
                status = "ok" if stored is not None else "ok_unpinned"
        out[canon] = {
            "symbol": sym,
            "stored": stored,
            "current": cid,
            "status": status,
            "stored_first": (stored_first.isoformat()
                             if stored_first is not None else None),
            "current_earliest": (current_first.isoformat()
                                 if current_first is not None else None),
            "gap_days": gap_days,
            "tolerance_days": BACKFILL_SEAL_TOLERANCE_DAYS,
            "reason": reason,
        }
    return out


def check_add_identities(root, resolved, adapter, progress=None, cancel=None,
                         today=None, served_earliest=None):
    """Run the Add Stocks identity gate before any fetch or bank write.

    The normal path joins the freshly resolved conIds to stored manifests and
    the cached demonstrated-served frontier.  Only an existing TRADES history
    missing that cache entry is probed, on the already-open validation
    connection. Unpinned stored history is also refreshed because its cached
    value is not conId-bound. New tickers incur no history probe.
    """
    earliest = _identity_earliest_cache(root, served_earliest)
    checks = check_security_ids(
        root, resolved, served_earliest=earliest, check_history=True)
    pending = [
        (canon, detail) for canon, detail in checks.items()
        if detail.get("status") in {"history_unverified", "ok_unpinned"}
    ]
    if cancel is not None and cancel.is_set():
        raise Cancelled("identity validation cancelled")
    if not pending:
        return checks  # Pure cached/local inspection needs no clock or transport authority.
    today = _operation_today(today)
    for idx, (canon, detail) in enumerate(pending, 1):
        # An unpinned archive cannot prove that a ticker-keyed cache value came
        # from today's conId.  Discard it and require fresh evidence.
        if detail.get("status") == "ok_unpinned":
            earliest.pop(canon, None)
        if cancel is not None and cancel.is_set():
            raise Cancelled("identity validation cancelled")
        if progress is not None:
            try:
                progress(f"Checking identity history {idx}/{len(pending)}: "
                         f"{detail['symbol']}...")
            except Exception:  # noqa: BLE001 - progress never breaks the gate
                pass
        try:
            cid = detail["current"]
            if type(cid) is not int or cid <= 0:
                raise RequestRefused("identity requires a positive qualified conId")
            if hasattr(adapter, "contract_for"):
                contract = adapter.contract_for(cid)
            else:
                with fib.qualification_scope("qualify", [detail["symbol"]], fib.acquire_turn, cancel=cancel):
                    _cid, contract = adapter.qualify(detail["symbol"])
                if _cid != cid:
                    raise RequestRefused("identity contract changed after qualification")
            if contract.conId != cid:
                raise RequestRefused("identity adapter returned a different pinned contract")
            contract = deepcopy(contract)
            contract.symbol = detail["symbol"]  # contract_for(conId) has no symbol on real IB contracts.
            probed = _identity_earliest_evidence(
                adapter, contract, today, "TRADES", cancel)
        except (AuthorityError, LedgerError, Cancelled):
            raise
        except Exception:  # noqa: BLE001 - this ticker remains fail-closed
            probed = None
        if probed is not None:
            earliest[canon] = probed.isoformat()
    if cancel is not None and cancel.is_set():
        raise Cancelled("identity validation cancelled")
    return check_security_ids(
        root, resolved, served_earliest=earliest, check_history=True)


ADD_IDENTITY_BLOCK_STATUSES = frozenset({
    "mismatch", "history_mismatch", "history_unverified",
})


def blocked_add_identities(checks):
    """Return ``{symbol: detail}`` for fail-closed Add Stocks verdicts."""
    return {
        detail["symbol"]: detail
        for detail in (checks or {}).values()
        if detail.get("status") in ADD_IDENTITY_BLOCK_STATUSES
    }


_SPAN_LADDER = ["1 D", "2 D", "1 W", "2 W", "1 M", "2 M", "3 M"]


def probe_max_span(interval, ticker="AAPL", adapter_factory=None,
                   today=None, *, cancel=None, evidence_dir=None, _test_capability=None):
    operation = fops.begin_operation("diagnostic", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "span", (interval,),
        {"ticker": ticker, "adapter_factory": adapter_factory, "today": today,
         "cancel": cancel}, evidence_dir)


def _operation_today(today=None):
    worker = fib.current_worker()
    if worker is None:
        raise RequestRefused("planner requires an explicit operation child")
    frozen = worker.context.captured_now.date()
    if today is not None and type(today) is not date:
        raise RequestRefused("planner today must be a calendar date")
    day = frozen if today is None else min(today, frozen)
    worker.context.authority.row(day)
    return day


@fib.worker_scope
def _probe_max_span_body(interval, ticker="AAPL", adapter_factory=None,
                         today=None, cancel=None):
    """Empirically find the largest duration IBKR will serve for this
    bar size, climbing _SPAN_LADDER until a request errors or returns
    nothing, then backing off one step. Read-only, paced. Returns
    {'interval', 'safe_duration', 'tried': [...]} or {'error': ...}.
    IBKR's published limits are fuzzy/version-dependent — measure, then
    widen `_FETCH_SPAN[interval]` if you want more than the safe
    default. Sub-minute intervals are already windowed; skip them."""
    base, kind, session = fib.parse_token(interval)
    if base not in _BAR_SIZES or _BAR_SIZES[base][1] is not None:
        return {"error": f"{interval} is windowed/sub-minute — span "
                         f"tuning does not apply"}
    today = _operation_today(today)
    bar_size = _BAR_SIZES[base][0]
    context = fib.current_worker().context
    horizon = context.horizons[interval]
    if horizon is None:
        raise CalendarUnsupported("no settled span is covered")
    end = min(datetime.combine(today, time(23, 59), fib.NY), horizon)
    try:
        adapter = _auxiliary_adapter(adapter_factory)
    except (ConnectionError, OSError, ImportError) as exc:
        return {"error": str(exc)}
    tried, safe, calendar = [], None, {}
    try:
        with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
            _conid, contract = adapter.qualify(ticker)
        for dur in _SPAN_LADDER:
            from fetch_envelopes import carrier_start
            first = carrier_start(end, dur).date()
            unsupported = []
            for offset in range((end.date() - first).days + 1):
                day = first + timedelta(days=offset)
                try:
                    context.authority.window(interval, day)
                except CalendarUnsupported:
                    unsupported.append(day.isoformat())
            if unsupported:
                calendar = {"error": "span ladder reached unproven calendar windows",
                            "calendar_unsupported_days": unsupported}
                tried.append((dur, "calendar unsupported; not sent"))
                break
            try:
                request = fib.bar_request("ibkr.stock_ibkr.span_probe", contract, interval, end, dur)
                with fib.adapter_session(adapter, session == "rth"), fib.send_scope(request, fib.acquire_turn, cancel=cancel):
                    raw = adapter.fetch(contract, end.replace(tzinfo=None), dur, bar_size,
                                        what_to_show=fib.KINDS[kind])
            except (SeriesHalt, ConnectionError) as exc:
                tried.append((dur, f"failed: {exc}"))
                break
            tried.append((dur, f"{len(raw)} bars"))
            if not raw:
                break
            safe = dur
    finally:
        _auxiliary_disconnect(adapter)
    return {"interval": interval, "safe_duration": safe, "tried": tried, **calendar}


def _recent_session(today):
    d = today
    while d.weekday() > 4:
        d -= timedelta(days=1)
    return d


def estimate_backfill(ticker, interval, adapter_factory=None,
                      today=None, since=None, cancel=None, *, evidence_dir=None,
                      _test_capability=None):
    operation = fops.begin_operation("diagnostic", _auxiliary_directory(evidence_dir),
                                     test_capability=_test_capability)
    return _run_auxiliary_operation(operation, "estimate", (ticker, interval),
        {"adapter_factory": adapter_factory, "today": today, "since": since,
         "cancel": cancel}, evidence_dir)


@fib.worker_scope
def _estimate_backfill_body(ticker, interval, adapter_factory=None,
                            today=None, since=None, cancel=None):
    """Size a FULL backfill for a ticker that is NOT in the tree yet:
    connect, qualify, head-timestamp -> {'start', 'sessions',
    'est_requests', 'est_minutes', 'clipped_1s'} or {'error': ...}.
    Read-only; lets the GUI confirm before committing to a long paced
    run. `cancel` is honoured between the connect/qualify/head steps
    (the head probe alone can hang ~20s) -> {'error': 'cancelled',
    'cancelled': True}."""
    try:
        base, _kind, session = fib.parse_token(interval)
    except AuthorityError:
        return {"error": f"unfetchable interval {interval!r}"}
    if base == "1d" and session != "rth":
        raise CalendarUnsupported("daily extended-session estimates are unsupported")
    if cancel is not None and cancel.is_set():
        return {"error": "cancelled", "cancelled": True}
    today = _operation_today(today)
    try:
        adapter = _auxiliary_adapter(adapter_factory)
    except (ConnectionError, OSError, ImportError) as exc:
        return {"error": str(exc)}
    head_note = None
    prior_rth = getattr(adapter, "use_rth", True)
    try:
        if cancel is not None and cancel.is_set():
            return {"error": "cancelled", "cancelled": True}
        with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
            _conid, contract = adapter.qualify(ticker)
        adapter.use_rth = _session_spec(interval)[0]   # extended head for -pre/-post
        if cancel is not None and cancel.is_set():
            return {"error": "cancelled", "cancelled": True}
        # head failure falls back to `since` (AMD-style 'Query failed') instead
        # of erroring the whole estimate; qualify/connection errors still error.
        start, head_note = _head_start(adapter, contract, since, today,
                                       _what_to_show(interval))
    except (SeriesHalt, ConnectionError) as exc:
        return {"error": str(exc)}
    finally:
        adapter.use_rth = prior_rth
        _auxiliary_disconnect(adapter)
    earliest = None if head_note else start
    if since is not None and start < since:
        start = since
    clipped = False
    if ss.base_interval(interval).endswith("s"):
        floor = today - timedelta(days=ONE_SECOND_MAX_AGE_DAYS)
        if start < floor:
            start, clipped = floor, True
    calendar = {}
    context = fib.current_worker().context
    context.authority.row(start)  # A trusted pre-coverage head remains a blocker.
    days = _settled_plan_days(interval,
        [start + timedelta(days=i) for i in range(max(0, (today - start).days + 1))], calendar)
    if ss.base_interval(interval).endswith("s"):
        reqs = sum(len(day_requests(interval, day,
            session_window=context.authority.window(interval, day))) for day in days)
    else:
        reqs = len(_estimate_covered_spans(interval, days, context))
    # interval-aware req->min: minute+ now fetch a small "1 W" span (~1.5 s/req
    # incl. pack+gap); sub-minute stay HMDS-metered (~12 s/req). (was a flat
    # 5.8 req/min = ~10 s/req, which over-counted minute backfills ~7x after the
    # 2026-06-23 span/burst speedups.)
    _secs = 12.0 if ss.base_interval(interval).endswith("s") else 1.5
    return {"start": start, "sessions": len(days),
            "est_requests": reqs, "est_minutes": round(reqs * _secs / 60),
            "clipped_1s": clipped, "earliest": earliest, **calendar}


def _estimate_covered_spans(interval, days, context):
    """Count representable spans, splitting at unsupported windows and coverage.

    Existing coarse grouping remains where it is provable. A first-month
    carrier that rounds before coverage becomes exact session carriers rather
    than being counted as one request the guard would subsequently refuse.
    """
    from fetch_envelopes import carrier_start, encode_carrier
    groups, current = [], []
    for day in days:
        if current:
            try:
                for offset in range(1, (day - current[-1]).days):
                    context.authority.window(interval, current[-1] + timedelta(days=offset))
            except CalendarUnsupported:
                groups.append(current)
                current = []
        current.append(day)
    if current:
        groups.append(current)
    planned = []
    for group in groups:
        for _end, duration, covered in span_chunks(interval, group):
            first = context.authority.window(interval, covered[0])[0]
            last = context.authority.window(interval, covered[-1])[1]
            if carrier_start(last, duration).date() >= context.authority.first_date:
                planned.append((first, last, duration))
            else:
                for day in covered:
                    opening, closing = context.authority.window(interval, day)
                    planned.append((opening, closing, encode_carrier(opening, closing)))
    return planned
