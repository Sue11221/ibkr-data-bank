"""Acceptance harness: whole-empty-month source-absence, re-provability, Deep scan (Row 63).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (STORAGE_OPTIMIZATION_LOG.md, Row 63).

Offline and headless. M1/M1b execute the real `stock_ibkr.fill_missing_days` and
`stock_validate.scan_series_gaps` against a temporary bank with the month FETCH
seam (`stock_ibkr._fetch_month_bars`) scripted - no adapter, no network, no port,
no production bank. M2 is checked at SOURCE level against `display_data.py` with
tkinter never imported (modelled on `vol_extended_tickbox_reference.py`).

Exit contract (Row 41 check_kit): 1 = check failed; 3 = every check green but the
M1 flag (`stock_ibkr.EMPTY_MONTH_ABSENCE_PROOF = True`) is absent, i.e. the
pre-fix baseline; 0 = acceptance.

    python engine/empty_month_absence_reference.py

Row 63 defect, found by the authorized Row 30 live proof: `fill_missing_days`
promotes a requested day to `source_absent` only when its month is in
`fetched_months`, and a month enters that set only if it returned >= 1 bar. A
month the source returns COMPLETELY EMPTY therefore yields `unfilled` for every
day forever. The fix must promote on a CORROBORATING POSITIVE CONTROL only, and
nothing it writes may latch permanently.
"""

from __future__ import annotations

import ast
import json
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

# C1-ENTRY-1: Claude's entry-only approval, 2026-09-14, Row79 review.
# Import the existing confined owner before any subject module. The canonical
# import below still defines the unchanged reference; its main runs exactly once.
if __name__ == "__main__":
    from fetch_a2_ibkr_workflows_selftest import Operations
    Operations.setUpClass()
    _owner = Operations("test_unchanged_empty_month_reference_under_confined_owner")
    try:
        _owner.setUp()
        _reference_exit = _owner.run_empty_month_reference()
    finally:
        if not _owner.doCleanups():
            raise RuntimeError("confined reference cleanup failed")
    raise SystemExit(_reference_exit)

import stock_ibkr as sk               # noqa: E402
import stock_storage as ss            # noqa: E402
import stock_validate as sv           # noqa: E402
from check_kit import CheckKit        # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

DISPLAY_SOURCE = PROJECT_ROOT / "display_data.py"

# The M1 feature flag. Absent => pre-fix baseline => exit 3.
FEATURE = getattr(sk, "EMPTY_MONTH_ABSENCE_PROOF", False) is True

TICKER = "VRTX"                       # the ticker the live proof actually used
CONID = 275850
VOL = "1m-iv"                         # the live-evidence series
PRICE = "1d"                          # the user asked explicitly about OHLC too

# Bank-root sidecar mirroring the existing `_gap_ignores.json` convention:
# user-owned, hot-reloaded, moves with the bank (portability rule).
EXPIRY_FILE = "_absence_expiry.json"

_TMPDIRS = []


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

def fresh_root():
    tmp = Path(tempfile.mkdtemp(prefix="empty_month_absence_"))
    _TMPDIRS.append(tmp)
    root = tmp / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def cleanup():
    for tmp in _TMPDIRS:
        shutil.rmtree(tmp, ignore_errors=True)


def bars_for(year, month, days, *, hour=9, minute=30):
    """Clean (dt, o, h, l, c, v) tuples, one bar per named day."""
    out = []
    for day in days:
        stamp = datetime(year, month, day, hour, minute)
        out.append((stamp, 10.0, 10.5, 9.5, 10.25, 1000))
    return out


def month_entry(rows, year, month):
    """A manifest month record shaped as the production writer leaves it.

    Deliberately NO `status` field here: stamping "present" bank-wide makes
    the Row 51 volatility unit gate treat every seeded prior month as a
    committed file it must find on disk, and sections A/B run `fill` against
    file-less fixtures. Section F stamps status only where the strict
    interval fingerprint (the identity a known_deliberate pin embeds)
    requires it, via `stamp_month_status`.
    """
    return {"first": f"{month}/1/{year} 9:30:00",
            "last": f"{month}/28/{year} 15:59:00",
            "rows": int(rows), "size": int(rows) * 24,
            "sha256": "0" * 64, "mtime_ns": 0,
            "source": {"kind": "ingest", "contributions": []}}


def stamp_month_status(root, ticker=TICKER, interval=VOL):
    """Mark every seeded month `status: present` for fingerprint-strict tests."""
    folder = Path(root) / ticker
    manifest = ss.load_manifest(folder) or {}
    sec = (manifest.get("intervals") or {}).get(interval) or {}
    for entry in (sec.get("months") or {}).values():
        entry["status"] = "present"
    ss.save_manifest(folder, manifest)


def seed(root, ticker=TICKER, *, interval=VOL, present_months=(),
         verified_absent=(), evidence=None, conid=CONID, extra_intervals=None):
    """A ticker whose manifest records `present_months` as stored with rows > 0."""
    manifest = ss.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    sec = manifest.setdefault("intervals", {}).setdefault(interval, {})
    months = sec.setdefault("months", {})
    for (year, month, rows) in present_months:
        months[ss.month_key(year, month)] = month_entry(rows, year, month)
    if verified_absent:
        sec["verified_absent"] = sorted(verified_absent)
    if evidence:
        sec["verified_absent_evidence"] = dict(evidence)
    for token, spec in (extra_intervals or {}).items():
        other = manifest["intervals"].setdefault(token, {})
        other.setdefault("months", {})
        for (year, month, rows) in spec:
            other["months"][ss.month_key(year, month)] = month_entry(
                rows, year, month)
    ss.save_manifest(Path(root) / ticker, manifest)
    return manifest


def read_manifest(root, ticker=TICKER):
    return ss.load_manifest(Path(root) / ticker) or {}


def absent_list(root, interval=VOL, ticker=TICKER):
    sec = ((read_manifest(root, ticker).get("intervals") or {})
           .get(interval) or {})
    return list(sec.get("verified_absent", []))


def evidence_map(root, interval=VOL, ticker=TICKER):
    sec = ((read_manifest(root, ticker).get("intervals") or {})
           .get(interval) or {})
    return dict(sec.get("verified_absent_evidence", {}) or {})


def write_expiry_config(root, **values):
    payload = {"weak_days": 30, "strong_days": 365,
               "reprobe_budget_per_run": 25}
    payload.update(values)
    (Path(root) / EXPIRY_FILE).write_text(
        json.dumps(payload, indent=1), encoding="utf-8")
    return payload


def stamp(days_ago):
    return (datetime.now() - timedelta(days=days_ago)).isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# the scripted month-fetch seam
# --------------------------------------------------------------------------

class Fetches:
    """Scripts `stock_ibkr._fetch_month_bars` and records every month it asks for.

    A script entry is (bars, skipped_days) keyed by (year, month), or an
    Exception instance to raise. Unlisted months answer EMPTY - which is exactly
    the Row 63 condition - so a test only names what it cares about.
    """

    def __init__(self, script=None, default=None):
        self.script = dict(script or {})
        self.default = default            # None => (no bars, nothing skipped)
        self.calls = []                   # every (year, month) asked, in order
        self._saved = None

    def __enter__(self):
        self._saved = sk._fetch_month_bars

        def fake(adapter, contract, year, month, interval, cancel=None):
            self.calls.append((year, month))
            answer = self.script.get((year, month), self.default)
            if isinstance(answer, BaseException):
                raise answer
            if answer is None:
                return [], set()
            bars, skipped = answer
            return list(bars), set(skipped or ())

        sk._fetch_month_bars = fake
        return self

    def __exit__(self, *exc):
        sk._fetch_month_bars = self._saved
        return False

    def count(self, year, month):
        return sum(1 for c in self.calls if c == (year, month))


class Adapter:
    """Minimal stand-in. `fill_missing_days` only needs `qualify` when no
    contract is passed; every real request goes through the scripted seam."""

    use_rth = True

    def qualify(self, ticker):
        return CONID, {"symbol": ticker, "conId": CONID}

    def contract_for(self, conid):
        return {"symbol": TICKER, "conId": int(conid)}

    def reconnect(self):
        return True


def run_fill(root, days, script=None, *, interval=VOL, default=None,
             ticker=TICKER):
    """Execute the production fill over a scripted month seam."""
    with Fetches(script, default=default) as seam:
        res = sk.fill_missing_days(
            Adapter(), root, ticker, interval, list(days),
            contract={"symbol": ticker, "conId": CONID},
            run_id="empty_month_absence_reference",
            progress=lambda *a: None)
    return res, seam


# --------------------------------------------------------------------------
# A. the defect and its fix
# --------------------------------------------------------------------------

def section_a():
    section("A. whole-empty month -> source-absent only on a positive control")

    # 2014-03 is requested and answers EMPTY. 2014-01 is recorded present in the
    # manifest with rows > 0, so it is an eligible control. This is the exact
    # VRTX 1m-iv shape the live proof hit.
    empty_days = [f"2014-03-{d:02d}" for d in (3, 4, 5)]

    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580), (2014, 2, 8190)])
    res, seam = run_fill(root, empty_days,
                         {(2014, 1): (bars_for(2014, 1, [6, 7, 8]), set())})

    if not FEATURE:
        check("A1 baseline: empty month promotes nothing (the defect)",
              res["source_absent"] == [] and sorted(res["unfilled"]) ==
              sorted(empty_days),
              f"source_absent={res['source_absent']} unfilled={res['unfilled']}")
        check("A2 baseline: nothing is written to verified_absent",
              absent_list(root) == [], f"verified_absent={absent_list(root)}")
        return

    check("A1 positive control promotes the empty month's days",
          sorted(res["source_absent"]) == sorted(empty_days),
          f"source_absent={res['source_absent']}")
    check("A2 promoted days leave `unfilled`",
          res["unfilled"] == [], f"unfilled={res['unfilled']}")
    check("A3 promotion is durable in verified_absent",
          sorted(absent_list(root)) == sorted(empty_days),
          f"verified_absent={absent_list(root)}")
    check("A4 the control probe actually ran against a present month",
          any(c != (2014, 3) for c in seam.calls), f"calls={seam.calls}")
    check("A5 verified_absent stays a plain list of day strings",
          all(isinstance(d, str) for d in absent_list(root)),
          f"types={sorted({type(d).__name__ for d in absent_list(root)})}")

    # --- negatives: only a POSITIVE control may promote ---
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, _ = run_fill(root, empty_days, {})              # control answers empty too
    check("A6 an EMPTY control promotes nothing",
          res["source_absent"] == [] and sorted(res["unfilled"]) ==
          sorted(empty_days), f"source_absent={res['source_absent']}")
    check("A7 an empty control writes no verified_absent",
          absent_list(root) == [], f"verified_absent={absent_list(root)}")

    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, _ = run_fill(root, empty_days,
                      {(2014, 1): RuntimeError("control probe timed out")})
    check("A8 an ERRORING control promotes nothing",
          res["source_absent"] == [] and sorted(res["unfilled"]) ==
          sorted(empty_days), f"source_absent={res['source_absent']}")

    root = fresh_root()
    seed(root, present_months=[])                        # no present month at all
    res, _ = run_fill(root, empty_days, {})
    check("A9 no eligible control month promotes nothing",
          res["source_absent"] == [] and sorted(res["unfilled"]) ==
          sorted(empty_days), f"source_absent={res['source_absent']}")

    # --- the control must be the SAME series, not a sibling that happens to exist ---
    root = fresh_root()
    seed(root, present_months=[], extra_intervals={PRICE: [(2014, 1, 21)]})
    res, _ = run_fill(root, empty_days,
                      {(2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("A10 a sibling interval's presence is NOT an eligible control",
          res["source_absent"] == [],
          f"source_absent={res['source_absent']} (1d present, 1m-iv is not)")

    # --- OHLC gets the same treatment (user question 2026-07-27) ---
    price_days = ["2018-04-05"]
    root = fresh_root()
    seed(root, interval=PRICE, present_months=[(2018, 3, 21)])
    res, _ = run_fill(root, price_days,
                      {(2018, 3): (bars_for(2018, 3, [1, 2]), set())},
                      interval=PRICE)
    check("A11 the same control rule governs OHLC, not just volatility",
          res["source_absent"] == price_days,
          f"source_absent={res['source_absent']}")

    # --- a control must be a month recorded PRESENT WITH ROWS, not merely listed ---
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 0)])          # recorded, but rows == 0
    res, seam = run_fill(root, empty_days,
                         {(2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("A14 a zero-row month entry is NOT an eligible control",
          res["source_absent"] == [] and seam.count(2014, 1) == 0,
          f"source_absent={res['source_absent']} probes={seam.count(2014, 1)}")

    # --- an EXCEPTION is not evidence of absence ---
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, _ = run_fill(root, empty_days,
                      {(2014, 3): ConnectionError("month request died"),
                       (2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("A15 a month whose fetch RAISED is blocked, never source-absent",
          res["source_absent"] == [] and sorted(res["unfilled"]) ==
          sorted(empty_days),
          f"source_absent={res['source_absent']} blocked={res['blocked']}")

    # --- control budget: N empty months must not cost 2N probes ---
    many = [f"2014-{m:02d}-03" for m in (3, 6, 9, 11)]
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, seam = run_fill(root, many,
                         {(2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    control_calls = sum(1 for c in seam.calls if c == (2014, 1))
    check("A12 the control result is cached per (ticker, interval) per run",
          control_calls == 1,
          f"{control_calls} control probes for 4 empty months (want 1)")
    check("A13 four empty months still all promote off the one control",
          sorted(res["source_absent"]) == sorted(many),
          f"source_absent={res['source_absent']}")


# --------------------------------------------------------------------------
# B. invariants that must survive the fix
# --------------------------------------------------------------------------

def section_b():
    section("B. preserved invariants (must hold before AND after the fix)")

    # A month that RETURNS BARS keeps today's exact behaviour: the day that did
    # not come back is source-absent with no control probe needed.
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    served = ["2014-05-05", "2014-05-06"]
    res, seam = run_fill(root, served,
                         {(2014, 5): (bars_for(2014, 5, [5]), set())})
    check("B1 a served month still promotes its genuinely-absent day",
          res["source_absent"] == ["2014-05-06"],
          f"source_absent={res['source_absent']}")
    check("B2 a served month needs no control probe",
          seam.count(2014, 1) == 0,
          f"{seam.count(2014, 1)} control probes on a served month (want 0)")

    # A transiently-skipped week is UNKNOWN and must stay retryable even when the
    # control is positive. This is the conservatism the row exists to preserve.
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    skipped = {date(2014, 7, 8), date(2014, 7, 9)}
    ask = ["2014-07-08", "2014-07-09"]
    res, _ = run_fill(root, ask,
                      {(2014, 7): ([], skipped),
                       (2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("B3 a transiently-skipped week stays `unfilled` despite a live control",
          res["source_absent"] == [] and sorted(res["unfilled"]) == sorted(ask),
          f"source_absent={res['source_absent']} unfilled={res['unfilled']}")

    # The ORIGINAL conservatism, and the one this row must not disturb: a month
    # that DID return bars but whose week timed out. Those days are UNKNOWN, so
    # one slow week can never poison a real trading day into a permanent flag.
    # (B3 alone does not cover this - it exercises the empty-month filter, not
    # the skipped-day guard on a served month.)
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, _ = run_fill(
        root, ["2014-07-07", "2014-07-08", "2014-07-09"],
        {(2014, 7): (bars_for(2014, 7, [7]),
                     {date(2014, 7, 8), date(2014, 7, 9)}),
         (2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    # A skipped week disqualifies the month from the control path ENTIRELY: the
    # fix contract probes only when the month was empty AND nothing was skipped,
    # so a doubtful month must not even spend a request.
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    _, seam = run_fill(root, ["2014-07-08"],
                       {(2014, 7): ([], {date(2014, 7, 8)}),
                        (2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("B3a a month with a skipped week issues NO control probe",
          seam.count(2014, 1) == 0,
          f"{seam.count(2014, 1)} control probes on a doubtful month (want 0)")

    check("B3b a skipped week inside a SERVED month still stays `unfilled`",
          res["source_absent"] == []
          and sorted(res["unfilled"]) == ["2014-07-08", "2014-07-09"],
          f"source_absent={res['source_absent']} unfilled={res['unfilled']}")
    check("B3c a skipped day is never written to verified_absent",
          absent_list(root) == [], f"verified_absent={absent_list(root)}")

    # Self-heal: a day that really fetches leaves verified_absent.
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)],
         verified_absent=["2014-05-06"])
    res, _ = run_fill(root, ["2014-05-06"],
                      {(2014, 5): (bars_for(2014, 5, [6]), set())})
    check("B4 self-heal drops a day from verified_absent once it truly fetches",
          "2014-05-06" not in absent_list(root),
          f"verified_absent={absent_list(root)}")

    # Row 47: the identity floor refuses before any source contact.
    root = fresh_root()
    manifest = ss.new_manifest(TICKER, TICKER)
    manifest["conid"] = CONID
    manifest["data_corrections"] = [{
        "type": "identity_listing_truncation", "cutover": "2014-04-01",
        "ticker": TICKER, "run": "empty_month_absence_reference",
        "intervals": [VOL]}]
    manifest.setdefault("intervals", {}).setdefault(VOL, {})["months"] = {
        ss.month_key(2014, 5): month_entry(8580, 2014, 5)}
    ss.save_manifest(Path(root) / TICKER, manifest)
    halted = False
    probed = []
    try:
        with Fetches({}) as seam:
            probed = seam.calls
            sk.fill_missing_days(
                Adapter(), root, TICKER, VOL, ["2014-03-03"],
                contract={"symbol": TICKER, "conId": CONID},
                run_id="empty_month_absence_reference",
                progress=lambda *a: None)
    except sk.SeriesHalt:
        halted = True
    except Exception as exc:  # noqa: BLE001
        check("B5 identity floor raises SeriesHalt", False,
              f"raised {type(exc).__name__}: {exc}")
    check("B5 the Row 47 identity floor still refuses a pre-floor request",
          halted, "no SeriesHalt raised")
    check("B6 the identity-floor refusal happens BEFORE any source contact",
          probed == [], f"probed {probed} before halting")

    # An empty request set is a no-op in both worlds.
    root = fresh_root()
    seed(root, present_months=[(2014, 1, 8580)])
    res, seam = run_fill(root, [])
    check("B7 an empty request set probes nothing and promotes nothing",
          res["source_absent"] == [] and seam.calls == [],
          f"calls={seam.calls} source_absent={res['source_absent']}")


# --------------------------------------------------------------------------
# C. nothing may latch permanently (M1b)
# --------------------------------------------------------------------------

def days_reader(days):
    """An injected reader presenting exactly `days` as stored trading days."""
    def read_fn(root, ticker, interval):
        return [(datetime.fromisoformat(f"{d}T09:30:00"),
                 10.0, 10.5, 9.5, 10.25, 1000) for d in sorted(days)]
    return read_fn


def scan(root, *, stored, calendar, interval=VOL, ticker=TICKER):
    """`calendar_days` is a set of date objects, and only days INTERIOR to the
    stored span can be missing at all - both are production contracts."""
    return sv.scan_series_gaps(root, ticker, interval,
                               read_fn=days_reader(stored),
                               calendar_days=[date.fromisoformat(d)
                                              for d in calendar])


def section_c():
    section("C. re-provability: no source-absence latches forever")

    # The absent day must sit INTERIOR to the stored span; a day past the last
    # stored bar is not a gap at all, in production or here.
    stored = ["2014-05-01", "2014-05-02", "2014-05-07"]
    calendar = stored + ["2014-05-06"]
    absent_day = "2014-05-06"

    # Today's behaviour, which must not change for FRESH evidence: a
    # source-absent day is reported separately and is NOT a fillable gap.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "day_in_served_month",
                                "control": "2014-05", "at": stamp(1)}})
    write_expiry_config(root)
    res = scan(root, stored=stored, calendar=calendar)
    check("C1 fresh evidence keeps the day OUT of missing_days",
          absent_day not in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")
    check("C2 fresh evidence still reports the day as source_absent",
          absent_day in (res.get("source_absent") or []),
          f"source_absent={res.get('source_absent')}")

    if not FEATURE:
        return

    # WEAK evidence (this row's whole-empty-month inference) past its short
    # window becomes eligible to re-ask again.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "empty_month_control",
                                "control": "2014-01", "at": stamp(120)}})
    write_expiry_config(root, weak_days=30, strong_days=365)
    res = scan(root, stored=stored, calendar=calendar)
    check("C3 EXPIRED weak evidence re-enters missing_days",
          absent_day in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")
    check("C4 an expired day stays VISIBLE as source_absent",
          absent_day in (res.get("source_absent") or []),
          "expiry governs eligibility to re-ask, never what the UI shows")
    check("C5 missing_day_count agrees with missing_days after expiry",
          res.get("missing_day_count") == len(res.get("missing_days") or []),
          f"count={res.get('missing_day_count')} "
          f"len={len(res.get('missing_days') or [])}")

    # The SAME age against STRONG evidence is not yet expired - the two windows
    # must be genuinely different, not one window wearing two names.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "day_in_served_month",
                                "control": "2014-05", "at": stamp(120)}})
    write_expiry_config(root, weak_days=30, strong_days=365)
    res = scan(root, stored=stored, calendar=calendar)
    check("C6 strong evidence at the same age is NOT expired",
          absent_day not in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")

    # The windows are user-owned: widening the weak window re-silences the day.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "empty_month_control",
                                "control": "2014-01", "at": stamp(120)}})
    write_expiry_config(root, weak_days=400, strong_days=800)
    res = scan(root, stored=stored, calendar=calendar)
    check("C7 the expiry windows are read from the user-owned sidecar",
          absent_day not in (res.get("missing_days") or []),
          f"weak_days=400 should re-silence a 120-day-old stamp; "
          f"missing_days={res.get('missing_days')}")

    # Pre-existing evidence-less days (the 147 already in the bank) must be
    # treated as STRONG and stamped on observation, never mass re-probed.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day])
    write_expiry_config(root, weak_days=30, strong_days=365)
    res = scan(root, stored=stored, calendar=calendar)
    check("C8 a legacy evidence-less day is not re-probed on sight",
          absent_day not in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")
    check("C9 a legacy evidence-less day gets stamped as strong evidence",
          (evidence_map(root).get(absent_day) or {}).get("method")
          in {"legacy_backfill", "day_in_served_month"},
          f"evidence={evidence_map(root).get(absent_day)}")


def section_d():
    section("D. the bounded re-probe closes the loop")

    if not FEATURE:
        return

    absent_day = "2014-05-06"

    # A successful re-probe clears the flag through the EXISTING self-heal.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "empty_month_control",
                                "control": "2014-01", "at": stamp(120)}})
    write_expiry_config(root, weak_days=30)
    res, _ = run_fill(root, [absent_day],
                      {(2014, 5): (bars_for(2014, 5, [6]), set())})
    check("D1 a successful re-probe clears the source-absent flag",
          absent_day not in absent_list(root),
          f"verified_absent={absent_list(root)}")
    check("D2 clearing the flag also drops its evidence record",
          absent_day not in evidence_map(root),
          f"evidence={evidence_map(root)}")

    # A still-absent re-probe RESTAMPS rather than duplicating.
    root = fresh_root()
    old = stamp(120)
    seed(root, present_months=[(2014, 1, 8580)], verified_absent=[absent_day],
         evidence={absent_day: {"method": "empty_month_control",
                                "control": "2014-01", "at": old}})
    write_expiry_config(root, weak_days=30)
    res, _ = run_fill(root, [absent_day],
                      {(2014, 1): (bars_for(2014, 1, [6, 7]), set())})
    check("D3 a still-absent re-probe keeps exactly one verified_absent entry",
          absent_list(root).count(absent_day) == 1
          and len(absent_list(root)) == 1,
          f"verified_absent={absent_list(root)}")
    restamped = (evidence_map(root).get(absent_day) or {}).get("at")
    check("D4 a still-absent re-probe RESTAMPS its evidence",
          bool(restamped) and restamped != old,
          f"at was {old!r}, now {restamped!r}")

    # The per-run re-probe budget is enforced, so the first run after this ships
    # cannot stampede the ports with the whole legacy backlog.
    backlog = [f"2014-05-{d:02d}" for d in range(6, 26)]
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=backlog,
         evidence={d: {"method": "empty_month_control", "control": "2014-01",
                       "at": stamp(120)} for d in backlog})
    cfg = write_expiry_config(root, weak_days=30, reprobe_budget_per_run=5)
    stored = ["2014-05-01", "2014-05-02", "2014-05-28"]   # backlog is interior
    res = scan(root, stored=stored, calendar=stored + backlog)
    due = [d for d in (res.get("missing_days") or []) if d in backlog]
    check("D5 the per-run re-probe budget bounds how many expired days re-enter",
          len(due) <= cfg["reprobe_budget_per_run"],
          f"{len(due)} expired days offered, budget "
          f"{cfg['reprobe_budget_per_run']}")
    check("D6 the budget releases work rather than starving it",
          len(due) > 0, "an expired backlog must still make progress")


# --------------------------------------------------------------------------
# E. M2 - the Deep scan tickbox (source level, tkinter never imported)
# --------------------------------------------------------------------------

def parse_display():
    return ast.parse(DISPLAY_SOURCE.read_text(encoding="utf-8"),
                     filename=str(DISPLAY_SOURCE))


def _is_deep_scan_name(text):
    low = str(text).lower()
    return "deep" in low and "scan" in low


def deep_scan_targets(tree):
    """Attributes/names assigned a tkinter BooleanVar and named for deep scan.

    Deliberately binds the NAME to the CONSTRUCTOR: `display_data.py` already
    holds 25 `BooleanVar(value=False)` assignments (e.g. the Fix Data
    `_fixdata_include_ignored` precedent), so a bare constructor search would
    pass before M2 was written.
    """
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        call = ast.unparse(node.value.func)
        if "BooleanVar" not in call:
            continue
        for target in node.targets:
            text = ast.unparse(target)
            if _is_deep_scan_name(text):
                default = None
                for kw in node.value.keywords:
                    if kw.arg == "value" and isinstance(kw.value, ast.Constant):
                        default = kw.value.value
                if not node.value.keywords and node.value.args:
                    arg = node.value.args[0]
                    if isinstance(arg, ast.Constant):
                        default = arg.value
                found[text] = default
    return found


def functions_mentioning(tree, needle):
    out = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if needle in ast.unparse(node):
                out.append(node)
    return out


def section_e():
    section("E. M2 Fix Data 'Deep scan' tickbox")

    check("E0 display_data.py is present and parses",
          DISPLAY_SOURCE.exists(), f"{DISPLAY_SOURCE}")
    tree = parse_display()

    src = DISPLAY_SOURCE.read_text(encoding="utf-8")
    has_widget = "Deep scan" in src

    if not FEATURE:
        check("E1 baseline: no Deep scan control exists yet",
              not has_widget, "M2 is unimplemented at the pre-fix baseline")
        return

    check("E1 a 'Deep scan' control exists in the Fix Data window",
          has_widget, "no 'Deep scan' label found in display_data.py")

    targets = deep_scan_targets(tree)
    check("E2 a deep-scan-NAMED BooleanVar backs the tickbox",
          bool(targets),
          "no `<...deep_scan...> = tk.BooleanVar(...)` assignment; the Fix Data "
          "`_fixdata_include_ignored` assignment is the shape precedent")
    check("E3 that variable defaults OFF",
          bool(targets) and all(v is False for v in targets.values()),
          f"defaults={targets}")

    # The tick must be READ, and read where the scan set is decided - a declared
    # but unconsumed variable is a tickbox that does nothing.
    reads = [node for node in ast.walk(tree)
             if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute)
             and node.func.attr == "get"
             and _is_deep_scan_name(ast.unparse(node.func.value))]
    check("E4 the tickbox state is actually read",
          bool(reads), "no .get() on a deep-scan variable")

    deciding = [fn for fn in functions_mentioning(tree, "source_absent")
                if _is_deep_scan_name(ast.unparse(fn))]
    check("E5 the tick reaches the code that decides the scanned set",
          bool(deciding),
          "no function references BOTH a deep-scan variable and source_absent; "
          "OFF must reproduce today's exclusion and ON must add those days")

    # Per-run, not sticky: the value must never reach a settings/prefs writer.
    persisted = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = ast.unparse(node.func).lower()
        if not any(w in func for w in ("save_settings", "save_prefs",
                                       "write_settings", "store_settings")):
            continue
        if _is_deep_scan_name(ast.unparse(node)):
            persisted.append(func)
    check("E6 the tick is per-run, never persisted across runs",
          not persisted, f"reached settings writer(s): {persisted}")


# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# F. explicit user policy: known_deliberate pins (Row 75 Phase 3, 2026-07-31)
# --------------------------------------------------------------------------
#
# Row 63's rule stands: AUTOMATIC evidence never latches forever. The pins are
# the one reviewed exception - a USER ORDER (TMUS do-not-refetch) encoded in
# the user-owned sidecar - and they must subtract exact dates from re-probe
# eligibility while everything else keeps expiring. Errors halt; they never
# fetch.

def make_pin(root, *, ticker=TICKER, interval=VOL, start, end, **overrides):
    """A schema-exact pin built from the REAL fingerprint of the seeded bank."""
    pin = {
        "ticker": ticker,
        "interval": interval,
        "start": start,
        "end": end,
        "disposition": "do_not_refetch",
        "interval_fingerprint": ss.interval_state_fingerprint(
            root, ticker, interval),
        "review": {
            "checkpoint": "4bc2692",
            "approval_commit": "143af70",
            "approved_by": "CLAUDE",
            "approved_at": datetime.now().astimezone().isoformat(
                timespec="seconds"),
        },
    }
    pin.update(overrides)
    return pin


def pinned_bank(*, pin_overrides=None, extra_absent=(), budget=25):
    """One seeded bank with an expired-weak absent day inside a pin window."""
    root = fresh_root()
    absent = ["2014-05-06", *extra_absent]
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=absent,
         evidence={d: {"method": "empty_month_control", "control": "2014-01",
                       "at": stamp(120)} for d in absent})
    stamp_month_status(root)
    spec = {"start": "2014-05-06", "end": "2014-05-06"}
    spec.update(pin_overrides or {})
    pin = make_pin(root, **spec)
    write_expiry_config(root, weak_days=30, reprobe_budget_per_run=budget,
                        version=1, known_deliberate=[pin])
    return root


def section_f():
    section("F. explicit user policy: known_deliberate pins subtract, "
            "halt, never fetch")

    if not FEATURE:
        return

    stored = ["2014-05-01", "2014-05-02", "2014-05-28"]
    day = "2014-05-06"
    calendar = stored + [day]

    # F1: a valid pin keeps an EXPIRED weak day out of the re-probe while the
    # day still reports source-absent - the exact TMUS shape.
    root = pinned_bank()
    res = scan(root, stored=stored, calendar=calendar)
    policy = res.get("source_absence_policy") or {}
    check("F1 a valid pin subtracts the expired day from re-probe",
          day not in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")
    check("F1b the pinned day still reports as source-absent",
          day in (res.get("source_absent") or []),
          f"source_absent={res.get('source_absent')}")
    check("F1c the scan names the applied policy without leaking the day set",
          policy.get("status") == "applied"
          and policy.get("disposition") == "do_not_refetch"
          and policy.get("pinned_count") == 1
          and "pinned" not in policy, f"policy={policy}")

    # F2: Row 63 is untouched OUTSIDE the pin - an expired day past the window
    # still re-enters missing_days for its bounded re-probe.
    other = "2014-05-13"
    root = pinned_bank(extra_absent=[other])
    res = scan(root, stored=stored, calendar=calendar + [other])
    check("F2 an expired day OUTSIDE the pin window still re-probes",
          other in (res.get("missing_days") or [])
          and day not in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")

    # F3: a plain fillable gap (never verified absent) is not silenced.
    root = pinned_bank()
    res = scan(root, stored=stored, calendar=calendar + ["2014-05-07"])
    check("F3 a fillable gap outside the pin stays fillable",
          "2014-05-07" in (res.get("missing_days") or []),
          f"missing_days={res.get('missing_days')}")

    # F4: malformed pin -> that series HALTS: no repair candidates at all,
    # a loud error, absence still reported, and the manifest untouched.
    root = pinned_bank(pin_overrides={"disposition": "never_fetch"})
    before = (Path(root) / TICKER / "manifest.json").read_bytes()
    res = scan(root, stored=stored, calendar=calendar)
    check("F4 a malformed pin halts that series' repair, loudly",
          res.get("missing_days") == []
          and "halted" in str(res.get("error", ""))
          and day in (res.get("source_absent") or []),
          f"error={res.get('error')!r} missing={res.get('missing_days')}")
    check("F4b the halt writes nothing to the manifest",
          (Path(root) / TICKER / "manifest.json").read_bytes() == before)

    # F5: a stale fingerprint (manifest changed after the pin was cut) halts.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[day],
         evidence={day: {"method": "empty_month_control",
                         "control": "2014-01", "at": stamp(120)}})
    stamp_month_status(root)
    pin = make_pin(root, start=day, end=day)
    seed(root, present_months=[(2014, 5, 8580), (2014, 6, 700)],
         verified_absent=[day],
         evidence={day: {"method": "empty_month_control",
                         "control": "2014-01", "at": stamp(120)}})
    stamp_month_status(root)
    write_expiry_config(root, weak_days=30, version=1,
                        known_deliberate=[pin])
    res = scan(root, stored=stored, calendar=calendar)
    check("F5 a stale pin fingerprint halts instead of authorizing",
          res.get("missing_days") == []
          and "halted" in str(res.get("error", "")),
          f"error={res.get('error')!r}")

    # F6: duplicate pins halt THAT series; an unpinned series in the same bank
    # keeps its ordinary expiry behaviour - no unrelated silencing.
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[day],
         evidence={day: {"method": "empty_month_control",
                         "control": "2014-01", "at": stamp(120)}})
    stamp_month_status(root)
    seed(root, ticker="OTHR", conid=CONID + 1,
         present_months=[(2014, 5, 8580)], verified_absent=[day],
         evidence={day: {"method": "empty_month_control",
                         "control": "2014-01", "at": stamp(120)}})
    stamp_month_status(root, ticker="OTHR")
    dup = make_pin(root, start=day, end=day)
    write_expiry_config(root, weak_days=30, version=1,
                        known_deliberate=[dup, dict(dup)])
    res = scan(root, stored=stored, calendar=calendar)
    other_res = scan(root, stored=stored, calendar=calendar, ticker="OTHR")
    check("F6 duplicate pins halt exactly that series",
          res.get("missing_days") == []
          and "halted" in str(res.get("error", "")),
          f"error={res.get('error')!r}")
    check("F6b an unpinned series in the same bank still re-probes",
          day in (other_res.get("missing_days") or [])
          and not other_res.get("error"),
          f"other={other_res.get('missing_days')} "
          f"error={other_res.get('error')!r}")

    # F7: no policy -> unchanged defaults (the section-D world).
    root = fresh_root()
    seed(root, present_months=[(2014, 5, 8580)], verified_absent=[day],
         evidence={day: {"method": "empty_month_control",
                         "control": "2014-01", "at": stamp(120)}})
    write_expiry_config(root, weak_days=30)
    res = scan(root, stored=stored, calendar=calendar)
    check("F7 without a policy the generic expiry contract is untouched",
          day in (res.get("missing_days") or [])
          and "source_absence_policy" not in res,
          f"missing_days={res.get('missing_days')}")

    # F8: a pin whose window no longer intersects the absence state halts -
    # a contradiction is surfaced, not smoothed over.
    root = pinned_bank(pin_overrides={"start": "2015-01-01",
                                      "end": "2015-01-31"})
    res = scan(root, stored=stored, calendar=calendar)
    check("F8 a pin contradicting current absence halts",
          res.get("missing_days") == []
          and "halted" in str(res.get("error", "")),
          f"error={res.get('error')!r}")


def main():
    try:
        section_a()
        section_b()
        section_c()
        section_d()
        section_e()
        section_f()
        if not FEATURE:
            KIT.pending(
                "M1/M1b/M2",
                "stock_ibkr.EMPTY_MONTH_ABSENCE_PROOF is not set: this is the "
                "pre-fix baseline.",
                "M1  a whole-empty month promotes to source_absent ONLY on a "
                "positive control probe of the same ticker/interval/conId.",
                "M1b nothing latches: verified_absent_evidence {method,control,"
                "at} plus two USER-OWNED expiry windows in the bank-root "
                f"{EXPIRY_FILE} return expired days to missing_days for one "
                "bounded re-probe.",
                "M2  a Fix Data 'Deep scan' tickbox, default OFF and per-run, "
                "includes source-absent days in the scan when ticked.")
        return KIT.finish(feature_absent=not FEATURE)
    finally:
        cleanup()


if __name__ == "__main__":
    sys.exit(main())
