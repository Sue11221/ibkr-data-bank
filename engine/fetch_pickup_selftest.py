"""Codex-owned offline acceptance tests for Row 43 fetch pickup M1.

No GUI, socket, network, or production-bank access.  Every fetch adapter is a
scripted in-memory fake, storage lives under TemporaryDirectory, and the
operation gate is redirected through testbank.isolated_gates().
"""

from __future__ import annotations

import ast
import json
import math
import queue
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, time as day_time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ENGINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE_DIR))
sys.path.insert(1, str(ENGINE_DIR.parent))

from check_kit import CheckKit  # noqa: E402
import stock_ibkr as sk  # noqa: E402
import stock_storage as ss  # noqa: E402
import stock_validate as sv  # noqa: E402
from testbank import isolated_gates  # noqa: E402


TEST_ONLY = True
NY = ZoneInfo("America/New_York")
MON = date(2024, 6, 10)  # Three real sessions; June 19 is a market holiday.
TUE = MON + timedelta(days=1)
WED = MON + timedelta(days=2)


class Ledger:
    def __init__(self):
        self.lock = threading.Lock()
        self.heads = []
        self.daily = []
        self.fetches = []
        self.qualifies = []
        self.qualify_many_calls = []
        self.contracts = []

    def add(self, name, value):
        with self.lock:
            getattr(self, name).append(value)

    def snapshot(self, name):
        with self.lock:
            return list(getattr(self, name))


def _bar(day, at, price):
    local = datetime.combine(day, at, tzinfo=NY)
    return SimpleNamespace(
        date=local.astimezone(timezone.utc), open=price,
        high=price + 0.01, low=price - 0.01, close=price + 0.005,
        volume=1000.0)


ALL_BARS = [
    _bar(day, at, price)
    for day, price in ((MON, 10.0), (TUE, 10.1), (WED, 10.2))
    for at in (day_time(8, 0), day_time(9, 30), day_time(16, 0))
]


class FakeAdapter:
    def __init__(self, port, ledger, conids, *, slow=0.0,
                 historical_bars=None):
        self.port = int(port)
        self.ledger = ledger
        self.conids = dict(conids)
        self.reverse = {int(v): str(k).upper() for k, v in conids.items()}
        self.slow = float(slow)
        self.historical_bars = (list(ALL_BARS) if historical_bars is None
                                else list(historical_bars))
        self.use_rth = True

    def account(self):
        return f"DU{self.port}"

    def qualify(self, symbol):
        key = str(symbol).upper()
        conid = int(self.conids[key])
        self.ledger.add("qualifies", (self.port, key))
        return conid, SimpleNamespace(symbol=key, conId=conid)

    def qualify_many(self, symbols, chunk=50, progress=None, cancel=None):
        self.ledger.add(
            "qualify_many_calls",
            (self.port, tuple(str(s).upper() for s in symbols)))
        return {str(s).upper(): int(self.conids[str(s).upper()])
                for s in symbols}

    def contract_for(self, conid):
        conid = int(conid)
        self.ledger.add("contracts", (self.port, conid))
        return SimpleNamespace(symbol=self.reverse[conid], conId=conid)

    def head_timestamp(self, contract, what_to_show="TRADES"):
        self.ledger.add("heads", (time.monotonic(), contract.symbol,
                                  what_to_show))
        return datetime.combine(MON, day_time.min, tzinfo=timezone.utc)

    def fetch(self, contract, end_dt, duration, bar_size,
              what_to_show="TRADES"):
        now = time.monotonic()
        row = (now, contract.symbol, duration, bar_size, what_to_show,
               bool(self.use_rth))
        ratio_kind = what_to_show in {
            "OPTION_IMPLIED_VOLATILITY", "HISTORICAL_VOLATILITY",
        }
        if duration == sk.HEAD_PROBE_DURATION or bar_size == "1 day":
            self.ledger.add("daily", row)
            scale = 0.01 if ratio_kind else 1.0
            return [SimpleNamespace(date=MON, open=10.0 * scale,
                                    high=10.1 * scale, low=9.9 * scale,
                                    close=10.0 * scale, volume=1000.0)]
        self.ledger.add("fetches", row)
        if self.slow:
            time.sleep(self.slow)
        if not ratio_kind:
            return list(self.historical_bars)
        # Pickup tests historically reused stock-like prices (~10) for IV.
        # Row 51 correctly rejects ratio OHLC above 10.0, so keep this fake
        # semantically valid without changing any dates/order/pickup evidence.
        return [SimpleNamespace(
            date=b.date,
            open=float(b.open) * 0.01,
            high=float(b.high) * 0.01,
            low=float(b.low) * 0.01,
            close=float(b.close) * 0.01,
            volume=b.volume,
        ) for b in self.historical_bars]

    def is_connected(self):
        return True

    def reconnect(self):
        return None

    def disconnect(self):
        return None


class EmptyPipeline:
    def __init__(self):
        self.series = []

    def claim(self):
        return None

    def execute(self, task, adapter, pacer):  # pragma: no cover - no tasks
        raise AssertionError("no post-pipeline task should exist")

    def has_pending(self):
        return False

    def record_series(self, rows):
        self.series.extend(rows)


def _root(temp_dir, suffix):
    root = Path(temp_dir) / suffix / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def _pin_identity(root, ticker, conid):
    ticker_dir = Path(root) / ticker
    ticker_dir.mkdir(parents=True, exist_ok=True)
    ss.save_manifest(ticker_dir, {
        "ticker": ticker, "conid": int(conid), "basis": "unknown",
        "intervals": {},
    })


def _direct_run(root, selections, ledger, conids, *, on_series=None):
    adapter = FakeAdapter(2000, ledger, conids)
    report = sk.nightly_gap_fill(
        root, selections, progress=None, cancel=None,
        adapter_factory=lambda: adapter,
        pacer=sk.Pacer(min_gap_s=0.0, burst_max=0), today=WED,
        resolved={str(k).upper(): int(v) for k, v in conids.items()},
        on_series=on_series)
    return report, adapter


def _series(report, ticker, interval):
    for row in report.get("series") or []:
        if row.get("ticker") == ticker and row.get("interval") == interval:
            return row
    return {}


def contract_checks(kit, temp_dir):
    kit.section("feature, pacing, and production span contract")
    kit.check("pickup and kind-earliest feature flags are enabled",
              sk.PICKUP_FAST_START is True
              and sk.KIND_EARLIEST_PROBE is True)
    kit.check("historical pacing constants remain 58 requests / 600 seconds",
              (sk.PACE_MAX_REQUESTS, sk.PACE_WINDOW_S) == (58, 600.0))
    kit.check("burst pacing constants remain 6 requests / 2 seconds",
              (sk.PACE_BURST_MAX, sk.PACE_BURST_WINDOW_S) == (6, 2.0))
    kit.check("production resolver gives 1m-iv the literal measured span",
              sk._fetch_span("1m-iv") == ("1 W", 6))
    chunks = sk.span_chunks("1m-iv", [MON, TUE, WED])
    kit.check("span_chunks consumes the production resolver",
              len(chunks) == 1 and chunks[0][1] == "1 W"
              and chunks[0][2] == [MON, TUE, WED], repr(chunks))

    valid = {"AAA": {"earliest": MON.isoformat(), "conid": 51000}}
    kit.check("strict cache parser accepts the writer's canonical schema",
              sk._strict_pickup_cache_date(
                  valid, "AAA", WED, 51000) == MON)
    invalid = (
        {"AAA": MON.isoformat()},
        {"AAA": {"earliest": MON.isoformat()}},
        {"AAA": {"earliest": MON.isoformat(), "conid": 51000,
                 "extra": True}},
        {"AAA": {"earliest": MON.isoformat() + "garbage",
                 "conid": 51000}},
        {"aaa": {"earliest": MON.isoformat(), "conid": 51000}},
        {"AAA": {"earliest": MON.isoformat(), "conid": 51000},
         "aaa": {"earliest": MON.isoformat(), "conid": 51000}},
        {"AAA": {"earliest": (WED + timedelta(days=1)).isoformat(),
                 "conid": 51000}},
        {"AAA": {"earliest": MON.isoformat(), "conid": 51001}},
        {"AAA": {"earliest": MON.isoformat(), "conid": "51000"}},
        {"AAA": {"earliest": MON.isoformat(), "conid": True}},
    )
    kit.check("legacy/malformed/ambiguous/future/wrong-id forms fail closed",
              all(sk._strict_pickup_cache_date(
                      raw, "AAA", WED, 51000) is None
                  for raw in invalid))

    loader_root = _root(temp_dir, "loader-contract")
    sv.record_ibkr_earliest(
        loader_root, "AAA", MON.isoformat(), conid=51000)
    sv.record_ibkr_earliest(loader_root, "LEG", TUE.isoformat())
    projected = sv.load_ibkr_earliest(loader_root)
    identity = sv.load_ibkr_earliest(
        loader_root, include_identity=True)
    kit.check("public earliest loader preserves its legacy string projection",
              projected == {"AAA": MON.isoformat(), "LEG": TUE.isoformat()},
              repr(projected))
    kit.check("identity-aware loader exposes bound evidence without coercion",
              identity == {
                  "AAA": {"earliest": MON.isoformat(), "conid": 51000},
                  "LEG": TUE.isoformat(),
              }, repr(identity))


def malformed_manifest_month_fallback(kit, temp_dir):
    kit.section("malformed stored-month evidence fails closed")
    root = _root(temp_dir, "malformed-month")
    ticker = "BADM"
    conid = 51009
    _pin_identity(root, ticker, conid)
    manifest = ss.load_manifest(root / ticker)
    manifest["intervals"]["1h"] = {
        "months": {"2024-13": {"status": "present"}},
        "verified_absent": [],
    }
    ss.save_manifest(root / ticker, manifest)
    sv.record_ibkr_earliest(
        root, ticker, MON.isoformat(), conid=conid)

    ledger = Ledger()
    report, _adapter = _direct_run(
        root, [(ticker, "1m")], ledger, {ticker: conid})
    row = _series(report, ticker, "1m")
    kit.check("invalid calendar month cannot halt pickup or suppress probing",
              bool(row) and not row.get("halt")
              and len(ledger.snapshot("heads")) == 1
              and len(ledger.snapshot("daily")) == 1,
              f"row={row} heads={ledger.snapshot('heads')} "
              f"daily={ledger.snapshot('daily')}")
    kit.check("invalid calendar month never claims cache authority",
              not any("cache" in str(note).lower()
                      for note in row.get("notes") or []),
              repr(row.get("notes")))


def cached_kind_and_overlap(kit, temp_dir):
    kit.section("kind-specific start and overlap preservation")
    root = _root(temp_dir, "kind")
    _pin_identity(root, "IVX", 51001)
    sv.record_ibkr_earliest(
        root, "IVX", MON.isoformat(), conid=51001)
    ledger = Ledger()
    report, _adapter = _direct_run(
        root, [("IVX", "1m-iv")], ledger, {"IVX": 51001})
    row = _series(report, "IVX", "1m-iv")
    manifest = ss.load_manifest(root / "IVX")
    first = sk.series_first_dt(manifest, "1m-iv")
    kit.check("cached 1m-iv run completes without a halt",
              bool(row) and not row.get("halt"), repr(row))
    kit.check("TRADES sidecar cannot suppress the kind-specific live probe",
              len(ledger.snapshot("heads")) == 1
              and ledger.snapshot("heads")[0][2]
              == "OPTION_IMPLIED_VOLATILITY"
              and len(ledger.snapshot("daily")) == 1
              and ledger.snapshot("daily")[0][4]
              == "OPTION_IMPLIED_VOLATILITY")
    kit.check("the kind-served date drives planning and stored history",
              row.get("days_planned") == 3
              and first is not None and first.date() == MON,
              f"days={row.get('days_planned')} first={first}")
    kit.check("the kind run log does not claim TRADES-cache authority",
              not any("cache" in str(note).lower()
                      for note in row.get("notes") or []),
              repr(row.get("notes")))

    before = len(ledger.snapshot("fetches"))
    probes_before_resume = (len(ledger.snapshot("heads")),
                            len(ledger.snapshot("daily")))
    resumed, _adapter = _direct_run(
        root, [("IVX", "1m-iv")], ledger, {"IVX": 51001})
    resumed_row = _series(resumed, "IVX", "1m-iv")
    kit.check("resumed series still performs a historical overlap fetch",
              len(ledger.snapshot("fetches")) > before
              and resumed_row.get("dup_existing", 0) > 0,
              repr(resumed_row))
    kit.check("resumed series still never uses an empty-series head path",
              (len(ledger.snapshot("heads")),
               len(ledger.snapshot("daily"))) == probes_before_resume)


def malformed_repair(kit, temp_dir):
    kit.section("GUI-shaped cache miss probes once and repairs evidence")
    root = _root(temp_dir, "malformed")
    path = root / "_ibkr_earliest.json"
    path.write_text(json.dumps({"BAD": {"earliest": MON.isoformat()}}),
                    encoding="utf-8")
    ledger = Ledger()
    observed = []

    def persist(_ticker, _interval, row):
        observed.append(dict(row))
        if row.get("ibkr_earliest"):
            sv.record_ibkr_earliest(root, "BAD", row["ibkr_earliest"])

    report, _adapter = _direct_run(
        root, [("BAD", "1m")], ledger, {"BAD": 51002},
        on_series=persist)
    row = _series(report, "BAD", "1m")
    repaired = json.loads(path.read_text(encoding="utf-8"))
    kit.check("GUI-shaped malformed miss issues exactly one head and daily",
              len(ledger.snapshot("heads")) == 1
              and len(ledger.snapshot("daily")) == 1,
              f"heads={ledger.snapshot('heads')} "
              f"daily={ledger.snapshot('daily')}")
    kit.check("malformed cache does not halt the series",
              bool(row) and not row.get("halt"), repr(row))
    kit.check("strict earliest_seen filtering allows post-series repair",
              observed and observed[-1].get("ibkr_earliest") == MON.isoformat()
              and repaired.get("BAD") == {
                  "earliest": MON.isoformat(), "conid": 51002,
              },
              f"observed={observed} repaired={repaired}")


def identity_divergence_fallback(kit, temp_dir):
    kit.section("ticker-cache binding, repair, and divergence fences")
    unbound_root = _root(temp_dir, "unbound")
    sv.record_ibkr_earliest(unbound_root, "NEW", TUE.isoformat())
    unbound_ledger = Ledger()
    first_report, _adapter = _direct_run(
        unbound_root, [("NEW", "1m")], unbound_ledger, {"NEW": 51989})
    first_stored = sk.series_first_dt(
        ss.load_manifest(unbound_root / "NEW"), "1m")
    repaired = sv.load_ibkr_earliest(
        unbound_root, include_identity=True).get("NEW")
    kit.check("stale unbound cache cannot suppress first-run live evidence",
              len(unbound_ledger.snapshot("heads")) == 1
              and len(unbound_ledger.snapshot("daily")) == 1)
    kit.check("first run repairs stale Tuesday to bound Monday evidence",
              not _series(first_report, "NEW", "1m").get("halt")
              and first_stored is not None and first_stored.date() == MON
              and repaired == {
                  "earliest": MON.isoformat(), "conid": 51989,
              },
              f"first={first_stored} repaired={repaired} "
              f"report={first_report}")

    probes_after_first = (
        len(unbound_ledger.snapshot("heads")),
        len(unbound_ledger.snapshot("daily")),
    )
    second_report, _adapter = _direct_run(
        unbound_root, [("NEW", "1m-iv")], unbound_ledger,
        {"NEW": 51989})
    second_stored = sk.series_first_dt(
        ss.load_manifest(unbound_root / "NEW"), "1m-iv")
    kit.check("new kind probes its own earliest despite repaired TRADES evidence",
              probes_after_first == (1, 1)
              and len(unbound_ledger.snapshot("heads")) == 2
              and unbound_ledger.snapshot("heads")[-1][2]
              == "OPTION_IMPLIED_VOLATILITY"
              and len(unbound_ledger.snapshot("daily")) == 2
              and unbound_ledger.snapshot("daily")[-1][4]
              == "OPTION_IMPLIED_VOLATILITY"
              and not _series(second_report, "NEW", "1m-iv").get("halt")
              and second_stored is not None
              and second_stored.date() == MON,
              f"heads={unbound_ledger.snapshot('heads')} "
              f"daily={unbound_ledger.snapshot('daily')} "
              f"first={second_stored} report={second_report}")

    mismatch_root = _root(temp_dir, "cache-conid-mismatch")
    _pin_identity(mismatch_root, "CID", 51991)
    sv.record_ibkr_earliest(
        mismatch_root, "CID", TUE.isoformat(), conid=51990)
    mismatch_ledger = Ledger()
    mismatch_report, _adapter = _direct_run(
        mismatch_root, [("CID", "1m")], mismatch_ledger, {"CID": 51991})
    mismatch_first = sk.series_first_dt(
        ss.load_manifest(mismatch_root / "CID"), "1m")
    mismatch_bound = sv.load_ibkr_earliest(
        mismatch_root, include_identity=True).get("CID")
    kit.check("sidecar conId mismatch probes and rebinds live evidence",
              len(mismatch_ledger.snapshot("heads")) == 1
              and len(mismatch_ledger.snapshot("daily")) == 1
              and not _series(mismatch_report, "CID", "1m").get("halt")
              and mismatch_first is not None
              and mismatch_first.date() == MON
              and mismatch_bound == {
                  "earliest": MON.isoformat(), "conid": 51991,
              },
              f"heads={mismatch_ledger.snapshot('heads')} "
              f"daily={mismatch_ledger.snapshot('daily')} "
              f"first={mismatch_first} bound={mismatch_bound}")

    root = _root(temp_dir, "divergence")
    ticker_dir = root / "DIV"
    ticker_dir.mkdir(parents=True, exist_ok=True)
    old_conid, live_conid = 51990, 51991
    ss.save_manifest(ticker_dir, {
        "ticker": "DIV", "conid": old_conid, "basis": "unknown",
        "intervals": {},
    })
    sv.record_ibkr_earliest(
        root, "DIV", MON.isoformat(), conid=old_conid)
    ledger = Ledger()
    adapter = FakeAdapter(2000, ledger, {"DIV": live_conid})
    adapter.reverse[old_conid] = "DIV"
    report = sk.nightly_gap_fill(
        root, [("DIV", "1m")], adapter_factory=lambda: adapter,
        pacer=sk.Pacer(min_gap_s=0.0, burst_max=0), today=WED,
        resolved={"DIV": live_conid})
    row = _series(report, "DIV", "1m")
    kit.check("pinned/live conId divergence forces live head evidence",
              bool(ledger.snapshot("heads"))
              and bool(ledger.snapshot("daily")))
    kit.check("identity divergence remains loud and the run stays intact",
              bool(row) and not row.get("halt")
              and any("divergence" in str(note).lower()
                      for note in row.get("notes") or []), repr(row))


def combined_cache(kit, temp_dir):
    kit.section("combined cache and final-shared-request successor boundary")
    fresh_root = _root(temp_dir, "combined-empty-lifecycle")
    fresh_conids = {"FEXT": 51005}
    _pin_identity(fresh_root, "FEXT", fresh_conids["FEXT"])
    sv.record_ibkr_earliest(
        fresh_root, "FEXT", TUE.isoformat(), conid=fresh_conids["FEXT"])
    fresh_ledger = Ledger()
    fresh_report, _adapter = _direct_run(
        fresh_root,
        [("FEXT", "1m"), ("FEXT", "1m-pre"), ("FEXT", "1m-post")],
        fresh_ledger, fresh_conids)
    fresh_rows = fresh_report.get("series") or []
    fresh_entry = sv.load_ibkr_earliest(
        fresh_root, include_identity=True).get("FEXT")
    kit.check("combined empty-price lifecycle ignores stale sidecar, probes "
              "once, and re-records fresh TRADES evidence",
              len(fresh_rows) == 3
              and all(not row.get("halt") for row in fresh_rows)
              and not any("cache hit" in str(note).lower()
                          for row in fresh_rows
                          for note in row.get("notes") or [])
              and len(fresh_ledger.snapshot("heads")) == 1
              and fresh_ledger.snapshot("heads")[0][2] == "TRADES"
              and len(fresh_ledger.snapshot("daily")) == 1
              and fresh_ledger.snapshot("daily")[0][4] == "TRADES"
              and fresh_entry == {
                  "earliest": MON.isoformat(), "conid": fresh_conids["FEXT"],
              },
              f"rows={fresh_rows} heads={fresh_ledger.snapshot('heads')} "
              f"daily={fresh_ledger.snapshot('daily')} entry={fresh_entry}")

    root = _root(temp_dir, "combined")
    conids = {"EXT": 51003, "NXT": 51004}
    for ticker, conid in conids.items():
        _pin_identity(root, ticker, conid)
        sv.record_ibkr_earliest(
            root, ticker, MON.isoformat(), conid=conid)
    # Row 50 cache authority requires a real stored TRADES-family month for
    # the ticker, while every selected 1m session below remains empty.
    _direct_run(root, [("EXT", "1h"), ("NXT", "1h")], Ledger(), conids)
    ledger = Ledger()
    selections = [("EXT", "1m"), ("EXT", "1m-pre"),
                  ("EXT", "1m-post"), ("EXT", "1m-iv"),
                  ("NXT", "1m")]
    plan_done = defaultdict(list)
    action_done = defaultdict(list)
    action_context = {}
    real_plan = sk.plan_gap
    real_actions = sk.sb.load_actions

    def tapped_plan(plan_root, ticker, interval, **kwargs):
        result = real_plan(plan_root, ticker, interval, **kwargs)
        plan_done[(ticker, interval)].append(time.monotonic())
        action_context[ticker] = interval
        return result

    def tapped_actions(action_root, ticker):
        result = real_actions(action_root, ticker)
        action_done[(ticker, action_context.pop(ticker, None))].append(
            time.monotonic())
        return result

    sk.plan_gap = tapped_plan
    sk.sb.load_actions = tapped_actions
    try:
        report, _adapter = _direct_run(root, selections, ledger, conids)
    finally:
        sk.plan_gap = real_plan
        sk.sb.load_actions = real_actions
    ext_selections = selections[:3]
    rows = [_series(report, *selection) for selection in ext_selections]
    kit.check("combined cache-hit run completes all three sessions",
              all(row and not row.get("halt") for row in rows), repr(rows))
    kit.check("combined TRADES prefetch uses valid lifecycle cache while IV "
              "probes its own kind earliest",
              len(ledger.snapshot("heads")) == 1
              and ledger.snapshot("heads")[0][2]
              == "OPTION_IMPLIED_VOLATILITY"
              and len(ledger.snapshot("daily")) == 1
              and ledger.snapshot("daily")[0][4]
              == "OPTION_IMPLIED_VOLATILITY",
              f"heads={ledger.snapshot('heads')} "
              f"daily={ledger.snapshot('daily')}")
    expected = {token: len(sk.span_chunks(token, [MON, TUE, WED]))
                for token in ("1m", "1m-pre", "1m-post")}
    ext_fetches = [row for row in ledger.snapshot("fetches")
                   if row[1] == "EXT" and row[4] == "TRADES"]
    kit.check("owned combined path preserves separate token request envelopes",
              len(ext_fetches) == sum(expected.values())
              and sum(row[-1] is True for row in ext_fetches) == expected["1m"]
              and sum(row[-1] is False for row in ext_fetches)
              == expected["1m-pre"] + expected["1m-post"],
              repr(ext_fetches))
    kit.check("each empty session records the cache-based start",
              all(any("cache" in str(note).lower()
                      for note in row.get("notes") or []) for row in rows),
              repr([row.get("notes") for row in rows]))
    ext_final_fetch = (max(row[0] for row in ext_fetches)
                       if ext_fetches else float("inf"))
    iv_plan = plan_done.get(("EXT", "1m-iv"), [])
    iv_actions = action_done.get(("EXT", "1m-iv"), [])
    iv_fetches = [row for row in ledger.snapshot("fetches")
                  if row[1] == "EXT"
                  and row[4] == "OPTION_IMPLIED_VOLATILITY"]
    iv_final_fetch = (max(row[0] for row in iv_fetches)
                      if iv_fetches else float("inf"))
    nxt_plan = plan_done.get(("NXT", "1m"), [])
    nxt_actions = action_done.get(("NXT", "1m"), [])
    kit.check("immediate non-combined IV payload precedes EXT shared fetch",
              bool(ext_fetches) and math.isfinite(ext_final_fetch)
              and len(iv_plan) == 1 and len(iv_actions) == 1
              and iv_plan[0] <= ext_final_fetch
              and iv_actions[0] <= ext_final_fetch,
              f"ext_fetches={ext_fetches} plans={dict(plan_done)} "
              f"actions={dict(action_done)}")
    kit.check("IV final request prepares the following NXT payload",
              bool(iv_fetches) and math.isfinite(iv_final_fetch)
              and len(nxt_plan) == 1 and len(nxt_actions) == 1
              and nxt_plan[0] <= iv_final_fetch
              and nxt_actions[0] <= iv_final_fetch,
              f"iv_fetches={iv_fetches} plans={dict(plan_done)} "
              f"actions={dict(action_done)}")
    kit.check("both non-combined successors consume payloads and complete",
              not _series(report, "EXT", "1m-iv").get("halt")
              and not _series(report, "NXT", "1m").get("halt"),
              f"plans={dict(plan_done)} actions={dict(action_done)} "
              f"iv={_series(report, 'EXT', '1m-iv')} "
              f"nxt={_series(report, 'NXT', '1m')}")


def gui_interleaving_boundaries(kit, temp_dir):
    kit.section("GUI interleaving preserves both logical pickup boundaries")
    root = _root(temp_dir, "combined-gui-order")
    conids = {"EXT": 51103, "NXT": 51104}
    for ticker, conid in conids.items():
        _pin_identity(root, ticker, conid)
        sv.record_ibkr_earliest(
            root, ticker, MON.isoformat(), conid=conid)
    selections = [
        ("EXT", "1m"), ("EXT", "1d"), ("EXT", "1m-pre"),
        ("EXT", "1m-post"), ("NXT", "1m"),
    ]
    ledger = Ledger()
    plan_done = defaultdict(list)
    action_done = defaultdict(list)
    action_context = {}
    series_done = {}
    real_plan = sk.plan_gap
    real_actions = sk.sb.load_actions

    def tapped_plan(plan_root, ticker, interval, **kwargs):
        result = real_plan(plan_root, ticker, interval, **kwargs)
        plan_done[(ticker, interval)].append(time.monotonic())
        action_context[ticker] = interval
        return result

    def tapped_actions(action_root, ticker):
        result = real_actions(action_root, ticker)
        action_done[(ticker, action_context.pop(ticker, None))].append(
            time.monotonic())
        return result

    def on_series(ticker, interval, _row):
        series_done[(ticker, interval)] = time.monotonic()

    sk.plan_gap = tapped_plan
    sk.sb.load_actions = tapped_actions
    try:
        report, _adapter = _direct_run(
            root, selections, ledger, conids, on_series=on_series)
    finally:
        sk.plan_gap = real_plan
        sk.sb.load_actions = real_actions

    shared_fetches = [
        row for row in ledger.snapshot("fetches")
        if row[1] == "EXT" and row[4] == "TRADES" and row[-1] is False
    ]
    final_shared = (max(row[0] for row in shared_fetches)
                    if shared_fetches else float("inf"))
    daily_plan = plan_done.get(("EXT", "1d"), [])
    daily_actions = action_done.get(("EXT", "1d"), [])
    kit.check("interleaved 1d payload completes before final shared fetch",
              bool(shared_fetches) and math.isfinite(final_shared)
              and len(daily_plan) == 1 and len(daily_actions) == 1
              and daily_plan[0] <= final_shared
              and daily_actions[0] <= final_shared,
              f"shared={shared_fetches} plans={dict(plan_done)} "
              f"actions={dict(action_done)}")

    nxt_plan = plan_done.get(("NXT", "1m"), [])
    nxt_actions = action_done.get(("NXT", "1m"), [])
    pre_done = series_done.get(("EXT", "1m-pre"), float("inf"))
    post_done = series_done.get(("EXT", "1m-post"), float("-inf"))
    kit.check("NXT preparation occurs at the post logical boundary",
              len(nxt_plan) == 1 and len(nxt_actions) == 1
              and math.isfinite(pre_done) and math.isfinite(post_done)
              and pre_done < nxt_plan[0] <= post_done
              and pre_done < nxt_actions[0] <= post_done,
              f"series_done={series_done} plans={dict(plan_done)} "
              f"actions={dict(action_done)}")
    kit.check("exact GUI interleaving completes and consumes each payload",
              len(report.get("series") or []) == len(selections)
              and all(not _series(report, *selection).get("halt")
                      for selection in selections), repr(report.get("series")))


def deep_head_floor_is_not_cache_authority(kit, temp_dir):
    kit.section("bounded daily floor cannot truncate a true deep head")
    root = _root(temp_dir, "deep-head-floor")
    ticker, conid = "DEEP", 51201
    true_head = date(1984, 6, 18)
    daily_floor = sk._head_probe_floor(WED)
    old_bars = [
        _bar(true_head, day_time(8, 0), 10.0),
        _bar(true_head, day_time(9, 30), 10.1),
    ]

    class DeepHeadAdapter(FakeAdapter):
        def head_timestamp(self, contract, what_to_show="TRADES"):
            self.ledger.add(
                "heads", (time.monotonic(), contract.symbol, what_to_show))
            return datetime.combine(
                true_head, day_time(12), tzinfo=timezone.utc)

        def fetch(self, contract, end_dt, duration, bar_size,
                  what_to_show="TRADES"):
            if duration == sk.HEAD_PROBE_DURATION or bar_size == "1 day":
                row = (time.monotonic(), contract.symbol, duration, bar_size,
                       what_to_show, bool(self.use_rth))
                self.ledger.add("daily", row)
                return [SimpleNamespace(
                    date=daily_floor, open=10.0, high=10.1, low=9.9,
                    close=10.0, volume=1000.0)]
            return super().fetch(
                contract, end_dt, duration, bar_size, what_to_show)

    ledger = Ledger()
    observed_ranges = []
    persisted = []
    real_trading_days = sk.trading_days

    def bounded_trading_days(start, end):
        observed_ranges.append((start, end))
        return [start]

    def gui_persist(ticker_arg, interval, row):
        earliest = (row or {}).get("ibkr_earliest")
        persisted.append((ticker_arg, interval, earliest))
        if earliest:
            sv.record_ibkr_earliest(root, ticker_arg, earliest)

    def run(interval):
        adapter = DeepHeadAdapter(
            2000, ledger, {ticker: conid}, historical_bars=old_bars)
        return sk.nightly_gap_fill(
            root, [(ticker, interval)], adapter_factory=lambda: adapter,
            pacer=sk.Pacer(min_gap_s=0.0, burst_max=0), today=WED,
            resolved={ticker: conid}, on_series=gui_persist)

    sk.trading_days = bounded_trading_days
    try:
        first_report = run("1m")
        first_bound = sv.load_ibkr_earliest(
            root, include_identity=True).get(ticker)
        first_probe_counts = (
            len(ledger.snapshot("heads")), len(ledger.snapshot("daily")))
        second_report = run("1m-pre")
    finally:
        sk.trading_days = real_trading_days

    first_dt = sk.series_first_dt(ss.load_manifest(root / ticker), "1m")
    second_dt = sk.series_first_dt(
        ss.load_manifest(root / ticker), "1m-pre")
    final_raw = sv.load_ibkr_earliest(root, include_identity=True)
    final_bound = final_raw.get(ticker)
    identity_ledger = Ledger()
    identity_adapter = DeepHeadAdapter(
        2000, identity_ledger, {ticker: conid}, historical_bars=old_bars)
    identity_checks = sk.check_add_identities(
        root, {ticker: conid}, identity_adapter, today=WED)
    blocked = sk.blocked_add_identities(identity_checks)
    identity = identity_checks.get(ticker, {})
    kit.check("first run chooses the true pre-floor head start",
              observed_ranges[:1] == [(true_head, WED)]
              and first_dt is not None and first_dt.date() == true_head
              and not _series(first_report, ticker, "1m").get("halt"),
              f"ranges={observed_ranges} first={first_dt} "
              f"row={_series(first_report, ticker, '1m')}")
    kit.check("GUI hook persists the conservative head only as legacy metadata",
              daily_floor > true_head
              and first_bound == true_head.isoformat()
              and final_bound == true_head.isoformat()
              and persisted == [
                  (ticker, "1m", true_head.isoformat()),
                  (ticker, "1m-pre", true_head.isoformat()),
              ]
              and sk._strict_pickup_cache_date(
                  final_raw, ticker, WED, conid) is None,
              f"head={true_head} floor={daily_floor} "
              f"first_bound={first_bound} final_bound={final_bound} "
              f"persisted={persisted}")
    kit.check("new session probes again and also starts at the deep head",
              first_probe_counts == (1, 1)
              and len(ledger.snapshot("heads")) == 2
              and len(ledger.snapshot("daily")) == 2
              and observed_ranges == [(true_head, WED), (true_head, WED)]
              and second_dt is not None and second_dt.date() == true_head
              and not _series(second_report, ticker, "1m-pre").get("halt"),
              f"counts={first_probe_counts}->"
              f"({len(ledger.snapshot('heads'))},"
              f"{len(ledger.snapshot('daily'))}) ranges={observed_ranges} "
              f"second={second_dt} "
              f"row={_series(second_report, ticker, '1m-pre')}")
    kit.check("Add Stocks identity verdict accepts the older stored archive",
              identity.get("status") == "ok"
              and identity.get("stored_first") == true_head.isoformat()
              and identity.get("current_earliest") == true_head.isoformat()
              and identity.get("gap_days") == 0
              and ticker not in blocked
              and not identity_ledger.snapshot("heads")
              and not identity_ledger.snapshot("daily"),
              f"identity={identity} blocked={blocked} "
              f"heads={identity_ledger.snapshot('heads')} "
              f"daily={identity_ledger.snapshot('daily')}")


def shared_evidence_primitives(kit):
    kit.section("D5 run-scoped single-flight safety")
    kit.check("D5 shared-evidence feature flag is enabled",
              sk.PICKUP_SHARED_EVIDENCE is True)

    # Eight simultaneous readers of one key must execute exactly one producer.
    cache = sk._PickupHeadCache(shared=True)
    key = cache.key(55001, "TRADES", WED, None)
    start = threading.Barrier(8)
    producer_entered = threading.Event()
    release = threading.Event()
    call_lock = threading.Lock()
    calls = [0]
    results = []
    errors = []

    def producer():
        with call_lock:
            calls[0] += 1
        producer_entered.set()
        if not release.wait(timeout=2.0):
            raise AssertionError("same-key producer was not released")
        return MON, "shared", MON

    def reader():
        try:
            start.wait(timeout=2.0)
            value = cache.get_or_compute(key, producer)
            with call_lock:
                results.append(value)
        except BaseException as exc:  # noqa: BLE001 - evidence for the check
            with call_lock:
                errors.append(exc)

    readers = [threading.Thread(target=reader, daemon=True) for _ in range(8)]
    for thread in readers:
        thread.start()
    producer_entered.wait(timeout=2.0)
    time.sleep(0.05)
    release.set()
    for thread in readers:
        thread.join(timeout=2.0)
    kit.check("same-key contention computes once and wakes every waiter",
              calls[0] == 1 and not errors and len(results) == 8
              and all(value == (MON, "shared", MON)
                      for value, _reused in results)
              and sum(not reused for _value, reused in results) == 1
              and not any(thread.is_alive() for thread in readers),
              f"calls={calls} results={results} errors={errors}")

    # If producer work accidentally held the cache lock, this two-key barrier
    # would deadlock/break: both producers must be able to enter together.
    concurrent = sk._PickupHeadCache(shared=True)
    producer_barrier = threading.Barrier(2)
    concurrent_results = []
    concurrent_errors = []

    def distinct_reader(conid):
        try:
            distinct_key = concurrent.key(conid, "TRADES", WED, None)

            def distinct_producer():
                producer_barrier.wait(timeout=1.0)
                return MON, str(conid), MON

            row = concurrent.get_or_compute(
                distinct_key, distinct_producer)
            with call_lock:
                concurrent_results.append(row)
        except BaseException as exc:  # noqa: BLE001
            with call_lock:
                concurrent_errors.append(exc)

    distinct_threads = [
        threading.Thread(target=distinct_reader, args=(conid,), daemon=True)
        for conid in (55002, 55003)
    ]
    for thread in distinct_threads:
        thread.start()
    for thread in distinct_threads:
        thread.join(timeout=2.0)
    kit.check("different keys compute concurrently outside the global lock",
              not concurrent_errors and len(concurrent_results) == 2
              and all(not reused for _value, reused in concurrent_results)
              and not any(thread.is_alive() for thread in distinct_threads),
              f"results={concurrent_results} errors={concurrent_errors}")

    # A failed owner publishes nothing. A waiter wakes, becomes the next owner,
    # and succeeds instead of inheriting the first thread's exception.
    retry_cache = sk._PickupHeadCache(shared=True)
    retry_key = retry_cache.key(55004, "TRADES", WED, None)
    retry_entered = threading.Event()
    retry_release = threading.Event()
    retry_calls = [0]
    retry_results = []
    retry_errors = []

    def flaky_producer():
        with call_lock:
            retry_calls[0] += 1
            attempt = retry_calls[0]
        if attempt == 1:
            retry_entered.set()
            retry_release.wait(timeout=2.0)
            raise RuntimeError("synthetic first producer failure")
        return TUE, "retry", TUE

    def retry_reader():
        try:
            row = retry_cache.get_or_compute(retry_key, flaky_producer)
            with call_lock:
                retry_results.append(row)
        except BaseException as exc:  # noqa: BLE001
            with call_lock:
                retry_errors.append(exc)

    first = threading.Thread(target=retry_reader, daemon=True)
    second = threading.Thread(target=retry_reader, daemon=True)
    first.start()
    retry_entered.wait(timeout=2.0)
    second.start()
    time.sleep(0.05)
    retry_release.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)
    retry_cached = retry_cache.get_or_compute(
        retry_key, lambda: (_ for _ in ()).throw(
            AssertionError("successful retry was not cached")))
    kit.check("failed producers are evicted and a waiter retries cleanly",
              retry_calls[0] == 2 and len(retry_errors) == 1
              and isinstance(retry_errors[0], RuntimeError)
              and retry_results == [((TUE, "retry", TUE), False)]
              and retry_cached == ((TUE, "retry", TUE), True)
              and not first.is_alive() and not second.is_alive(),
              f"calls={retry_calls} results={retry_results} "
              f"errors={retry_errors} cached={retry_cached}")

    predicate_cache = sk._PickupSingleFlight()
    predicate_raised = None
    try:
        predicate_cache.get_or_compute(
            "predicate", lambda: "first",
            cache_when=lambda _value: (_ for _ in ()).throw(
                RuntimeError("synthetic publication predicate failure")))
    except BaseException as exc:  # noqa: BLE001
        predicate_raised = exc
    predicate_results = []
    predicate_errors = []
    predicate_cancel = threading.Event()

    def predicate_retry():
        try:
            predicate_results.append(
                predicate_cache.get_or_compute(
                    "predicate", lambda: "healthy",
                    cancel=predicate_cancel, cache_when=lambda _value: True))
        except BaseException as exc:  # noqa: BLE001
            predicate_errors.append(exc)

    predicate_thread = threading.Thread(
        target=predicate_retry, daemon=True)
    predicate_thread.start()
    predicate_thread.join(timeout=1.0)
    predicate_poisoned = predicate_thread.is_alive()
    if predicate_poisoned:
        predicate_cancel.set()
        predicate_thread.join(timeout=1.0)
    predicate_cached = (None if predicate_poisoned else
                        predicate_cache.get_or_compute(
                            "predicate", lambda: (_ for _ in ()).throw(
                                AssertionError("predicate retry not cached"))))
    kit.check("publication-predicate failure evicts its ready slot",
              isinstance(predicate_raised, RuntimeError)
              and not predicate_poisoned and not predicate_thread.is_alive()
              and not predicate_errors
              and predicate_results == [("healthy", False)]
              and predicate_cached == ("healthy", True),
              f"raised={predicate_raised!r} poisoned={predicate_poisoned} "
              f"results={predicate_results} errors={predicate_errors} "
              f"cached={predicate_cached}")

    # A cancelled waiter owns only its wait. It cannot remove or cancel the
    # producer's slot, and the completed producer remains reusable afterward.
    cancel_cache = sk._PickupHeadCache(shared=True)
    cancel_key = cancel_cache.key(55005, "TRADES", WED, None)
    cancel_entered = threading.Event()
    cancel_release = threading.Event()
    waiter_cancel = threading.Event()
    cancel_results = []
    cancel_errors = []

    def slow_producer():
        cancel_entered.set()
        if not cancel_release.wait(timeout=2.0):
            raise AssertionError("cancel test producer was not released")
        return WED, "healthy", WED

    def producer_reader():
        try:
            cancel_results.append(
                cancel_cache.get_or_compute(cancel_key, slow_producer))
        except BaseException as exc:  # noqa: BLE001
            cancel_errors.append(exc)

    waiter_elapsed = []

    def cancelled_reader():
        began = time.monotonic()
        try:
            cancel_cache.get_or_compute(
                cancel_key,
                lambda: (_ for _ in ()).throw(
                    AssertionError("waiter became producer unexpectedly")),
                cancel=waiter_cancel)
        except BaseException as exc:  # noqa: BLE001
            waiter_elapsed.append(time.monotonic() - began)
            cancel_errors.append(exc)

    healthy_thread = threading.Thread(target=producer_reader, daemon=True)
    healthy_thread.start()
    cancel_entered.wait(timeout=2.0)
    waiter_cancel.set()
    cancelled_thread = threading.Thread(target=cancelled_reader, daemon=True)
    cancelled_thread.start()
    time.sleep(0.005)
    producer_survived = healthy_thread.is_alive()
    cancel_release.set()
    cancelled_thread.join(timeout=1.0)
    healthy_thread.join(timeout=2.0)
    post_cancel = cancel_cache.get_or_compute(
        cancel_key, lambda: (_ for _ in ()).throw(
            AssertionError("cancelled waiter poisoned the producer")))
    cancelled_only = [exc for exc in cancel_errors
                      if isinstance(exc, sk.Cancelled)]
    kit.check("a waiter cancels promptly without poisoning its producer",
              producer_survived and not cancelled_thread.is_alive()
              and len(cancelled_only) == 1 and len(cancel_errors) == 1
              and waiter_elapsed and waiter_elapsed[0] < 0.5
              and cancel_results == [((WED, "healthy", WED), False)]
              and post_cancel == ((WED, "healthy", WED), True)
              and not healthy_thread.is_alive(),
              f"elapsed={waiter_elapsed} results={cancel_results} "
              f"errors={cancel_errors} post={post_cancel}")

    poll_cache = sk._PickupHeadCache(shared=True)
    poll_key = poll_cache.key(55010, "TRADES", WED, None)
    poll_entered = threading.Event()
    poll_release = threading.Event()
    poll_results = []
    poll_errors = []

    def poll_producer():
        poll_entered.set()
        if not poll_release.wait(timeout=2.0):
            raise AssertionError("poll-loop producer was not released")
        return WED, "poll healthy", WED

    class PollCancel:
        def __init__(self):
            self.calls = 0
            self.polled = threading.Event()

        def is_set(self):
            self.calls += 1
            if self.calls >= 2:
                self.polled.set()
                return True
            return False

    poll_cancel = PollCancel()

    def poll_owner():
        try:
            poll_results.append(
                poll_cache.get_or_compute(poll_key, poll_producer))
        except BaseException as exc:  # noqa: BLE001
            poll_errors.append(exc)

    poll_elapsed = []

    def poll_waiter():
        began = time.monotonic()
        try:
            poll_cache.get_or_compute(
                poll_key,
                lambda: (_ for _ in ()).throw(
                    AssertionError("poll waiter became producer")),
                cancel=poll_cancel)
        except BaseException as exc:  # noqa: BLE001
            poll_elapsed.append(time.monotonic() - began)
            poll_errors.append(exc)

    poll_owner_thread = threading.Thread(target=poll_owner, daemon=True)
    poll_owner_thread.start()
    poll_entered.wait(timeout=2.0)
    poll_waiter_thread = threading.Thread(target=poll_waiter, daemon=True)
    poll_waiter_thread.start()
    poll_waiter_thread.join(timeout=1.0)
    poll_owner_survived = poll_owner_thread.is_alive()
    poll_release.set()
    poll_owner_thread.join(timeout=2.0)
    poll_cached = poll_cache.get_or_compute(
        poll_key, lambda: (_ for _ in ()).throw(
            AssertionError("poll producer result was not cached")))
    poll_cancelled = [exc for exc in poll_errors
                      if isinstance(exc, sk.Cancelled)]
    kit.check("an in-flight waiter cancels from the timed poll loop",
              poll_cancel.calls >= 2 and poll_cancel.polled.is_set()
              and poll_owner_survived and not poll_waiter_thread.is_alive()
              and len(poll_cancelled) == 1 and len(poll_errors) == 1
              and poll_elapsed and poll_elapsed[0] < 0.5
              and poll_results == [((WED, "poll healthy", WED), False)]
              and poll_cached == ((WED, "poll healthy", WED), True)
              and not poll_owner_thread.is_alive(),
              f"calls={poll_cancel.calls} elapsed={poll_elapsed} "
              f"results={poll_results} errors={poll_errors} "
              f"cached={poll_cached}")

    # The key itself is the identity fence. Every dimension must occupy a
    # separate producer slot, while an exact repeat reuses its original value.
    isolated = sk._PickupHeadCache(shared=True)
    isolation_keys = [
        isolated.key(55006, "TRADES", WED, None),
        isolated.key(55007, "TRADES", WED, None),
        isolated.key(55006, "OPTION_IMPLIED_VOLATILITY", WED, None),
        isolated.key(55006, "TRADES", WED + timedelta(days=1), None),
        isolated.key(55006, "TRADES", WED, MON),
    ]
    isolation_calls = []
    for i, isolation_key in enumerate(isolation_keys):
        isolated.get_or_compute(
            isolation_key,
            lambda i=i: (isolation_calls.append(i) or
                         (MON, f"isolated-{i}", MON)))
    repeated = isolated.get_or_compute(
        isolation_keys[0],
        lambda: (_ for _ in ()).throw(
            AssertionError("exact head key failed to reuse")))
    kit.check("conId, kind, today, and since all fence head evidence",
              isolation_calls == list(range(len(isolation_keys)))
              and repeated == ((MON, "isolated-0", MON), True),
              f"calls={isolation_calls} repeated={repeated}")

    class DroppedDailyAdapter:
        def head_timestamp(self, _contract):
            return datetime.combine(TUE, day_time(12), tzinfo=timezone.utc)

        def fetch(self, *_args, **_kwargs):
            raise sk.ConnectionLost("synthetic daily-probe link loss")

    head_only_evidence = sk._head_start_evidence(
        DroppedDailyAdapter(), SimpleNamespace(symbol="DROP"),
        None, WED, "TRADES")
    incomplete_cache = sk._PickupHeadCache(shared=True)
    incomplete_key = incomplete_cache.key(55008, "TRADES", WED, None)
    incomplete_first = incomplete_cache.get_or_compute(
        incomplete_key, lambda: head_only_evidence)
    healthy_calls = [0]

    def healthy_evidence():
        healthy_calls[0] += 1
        return MON, "healthy retry", MON

    incomplete_retry = incomplete_cache.get_or_compute(
        incomplete_key, healthy_evidence)
    incomplete_reuse = incomplete_cache.get_or_compute(
        incomplete_key, lambda: (_ for _ in ()).throw(
            AssertionError("complete retry was not cached")))
    kit.check("head-only evidence after a daily link loss is never shared",
              head_only_evidence[2] is None
              and incomplete_first == (head_only_evidence, False)
              and healthy_calls[0] == 1
              and incomplete_retry == ((MON, "healthy retry", MON), False)
              and incomplete_reuse == ((MON, "healthy retry", MON), True),
              f"head_only={head_only_evidence} first={incomplete_first} "
              f"retry={incomplete_retry} reuse={incomplete_reuse}")

    local_cache = sk._PickupHeadCache()
    local_key = local_cache.key(55009, "TRADES", WED, None)
    local_first = local_cache.get_or_compute(
        local_key, lambda: head_only_evidence)
    local_reuse = local_cache.get_or_compute(
        local_key, lambda: (_ for _ in ()).throw(
            AssertionError("serial head-only behavior changed")))
    kit.check("local serial cache preserves legacy head-only reuse",
              local_first == (head_only_evidence, False)
              and local_reuse == (head_only_evidence, True),
              f"first={local_first} reuse={local_reuse}")

    resolved_cache = sk._PickupResolvedMap()
    unresolved_calls = [0]

    def unresolved():
        unresolved_calls[0] += 1
        return None

    missing_first = resolved_cache.resolve(" mix ", unresolved)
    missing_second = resolved_cache.resolve(
        "MIX", lambda: (_ for _ in ()).throw(
            AssertionError("canonical unresolved value was not cached")))
    kit.check("canonical resolver distinguishes cached None from a miss",
              unresolved_calls[0] == 1
              and missing_first == (None, False)
              and missing_second == (None, True),
              f"calls={unresolved_calls} values="
              f"{missing_first}/{missing_second}")
    kit.check("canonical batch lookup accepts one normalized key only",
              sk._PickupResolvedMap.mapping_value(
                  {"RACE": 55010}, " race ") == 55010
              and sk._PickupResolvedMap.mapping_value(
                  {"RACE": 55010, " race ": 55011}, "race") is None)


def parallel_shared_pickup(kit, temp_dir):
    kit.section("D5 real two-port same-ticker pickup reuse")
    root = _root(temp_dir, "shared-pickup")
    ticker = "RACE"
    conids = {ticker: 56001}
    ledger = Ledger()
    head_get_lock = threading.Lock()
    head_get_calls = [0]
    second_head_get = threading.Event()
    real_head_get = sk._PickupHeadCache.get_or_compute

    def traced_head_get(self, key, producer, cancel=None):
        with head_get_lock:
            head_get_calls[0] += 1
            if head_get_calls[0] >= 2:
                second_head_get.set()
        return real_head_get(self, key, producer, cancel=cancel)

    class RacingAdapter(FakeAdapter):
        def head_timestamp(self, contract, what_to_show="TRADES"):
            # The producer cannot return until the sibling has called the real
            # shared-cache method. This proves an in-flight waiter rather than
            # a merely sequential memoization hit.
            if not second_head_get.wait(timeout=2.0):
                raise AssertionError(
                    "second split worker never entered the shared head slot")
            return super().head_timestamp(contract, what_to_show)

    def adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: RacingAdapter(port, ledger, conids)

    observed = []
    observed_lock = threading.Lock()

    def on_series(ticker_name, interval, row):
        with observed_lock:
            observed.append((ticker_name, interval, dict(row)))

    sk._PickupHeadCache.get_or_compute = traced_head_get
    try:
        report = sk.nightly_gap_fill_parallel(
            root, [(ticker, "1m"), (ticker, "5m")], [2000, 3000],
            today=WED, resolved=None, adapter_factory=adapter_factory,
            on_series=on_series)
    finally:
        sk._PickupHeadCache.get_or_compute = real_head_get
    rows = report.get("series") or []
    partitions = report.get("_partition") or {}
    notes = [str(note).lower() for row in rows
             for note in row.get("notes") or []]
    historical = ledger.snapshot("fetches")
    row_requests = sum(int(row.get("requests") or 0) for row in rows)
    kit.check("real series split completes on two distinct worker ports",
              len(rows) == 2 and all(not row.get("halt") for row in rows)
              and {row.get("interval") for row in rows} == {"1m", "5m"}
              and {port for port, jobs in partitions.items() if jobs}
              == {2000, 3000}
              and head_get_calls[0] == 2 and second_head_get.is_set()
              and len(observed) == 2,
              f"rows={rows} partition={partitions} "
              f"head_get_calls={head_get_calls} observed={observed}")
    kit.check("parallel miss performs one shared qualification and no fallback",
              len(ledger.snapshot("qualify_many_calls")) == 1
              and not ledger.snapshot("qualifies"),
              f"many={ledger.snapshot('qualify_many_calls')} "
              f"single={ledger.snapshot('qualifies')}")
    kit.check("each worker constructs its own contract from the shared conId",
              len(ledger.snapshot("contracts")) == 2
              and {port for port, conid in ledger.snapshot("contracts")
                   if conid == conids[ticker]} == {2000, 3000},
              repr(ledger.snapshot("contracts")))
    kit.check("parallel miss performs one head and one bounded daily probe",
              len(ledger.snapshot("heads")) == 1
              and len(ledger.snapshot("daily")) == 1,
              f"heads={ledger.snapshot('heads')} "
              f"daily={ledger.snapshot('daily')}")
    kit.check("only the live-head producer owns request accounting",
              row_requests == len(historical) + 1
              and report.get("totals", {}).get("requests") == row_requests,
              f"row_requests={row_requests} historical={historical} "
              f"totals={report.get('totals')}")
    kit.check("the waiting worker reports reuse and skips GUI fallback probing",
              sum("live head/daily evidence reused" in note
                  for note in notes) == 1
              and len(observed) == 2
              and len(ledger.snapshot("heads")) == 1
              and len(ledger.snapshot("daily")) == 1
              and not ledger.snapshot("qualifies"),
              f"notes={notes} observed={observed}")

    lower_root = _root(temp_dir, "shared-pickup-canonical-resolver")
    lower_ledger = Ledger()
    lower_conids = {ticker: 56003}

    def lower_adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: FakeAdapter(port, lower_ledger, lower_conids)

    lower_report = sk.nightly_gap_fill_parallel(
        lower_root, [(ticker.lower(), "1m"), (ticker.lower(), "5m")],
        [2000, 3000], today=WED, resolved=None,
        adapter_factory=lower_adapter_factory)
    lower_rows = lower_report.get("series") or []
    kit.check("lowercase selections reuse a canonical batch resolution",
              len(lower_rows) == 2
              and all(not row.get("halt") for row in lower_rows)
              and len(lower_ledger.snapshot("qualify_many_calls")) == 1
              and lower_ledger.snapshot("qualify_many_calls")[0][1]
              == (ticker,)
              and not lower_ledger.snapshot("qualifies")
              and len(lower_ledger.snapshot("contracts")) == 2,
              f"rows={lower_rows} "
              f"many={lower_ledger.snapshot('qualify_many_calls')} "
              f"single={lower_ledger.snapshot('qualifies')} "
              f"contracts={lower_ledger.snapshot('contracts')}")

    ordinary_root = _root(temp_dir, "ordinary-parallel-local-cache")
    ordinary_conids = {"LOCAL": 56004, "OTHER": 56005}
    for ordinary_ticker, ordinary_conid in ordinary_conids.items():
        _pin_identity(ordinary_root, ordinary_ticker, ordinary_conid)
    sv.record_ibkr_earliest(
        ordinary_root, "OTHER", MON.isoformat(),
        conid=ordinary_conids["OTHER"])
    _direct_run(ordinary_root, [("OTHER", "1h")], Ledger(),
                ordinary_conids)
    ordinary_ledger = Ledger()

    class HeadOnlyParallelAdapter(FakeAdapter):
        def fetch(self, contract, end_dt, duration, bar_size,
                  what_to_show="TRADES"):
            if (contract.symbol == "LOCAL"
                    and (duration == sk.HEAD_PROBE_DURATION
                         or bar_size == "1 day")):
                self.ledger.add(
                    "daily",
                    (time.monotonic(), contract.symbol, duration, bar_size,
                     what_to_show, bool(self.use_rth)))
                raise sk.ConnectionLost("synthetic ordinary daily drop")
            return super().fetch(
                contract, end_dt, duration, bar_size, what_to_show)

    def ordinary_adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: HeadOnlyParallelAdapter(
            port, ordinary_ledger, ordinary_conids)

    ordinary_report = sk.nightly_gap_fill_parallel(
        ordinary_root,
        [("LOCAL", "1m"), ("LOCAL", "5m"), ("OTHER", "1m")],
        [2000, 3000], today=WED, resolved=ordinary_conids,
        adapter_factory=ordinary_adapter_factory)
    ordinary_rows = ordinary_report.get("series") or []
    local_notes = [str(note).lower() for row in ordinary_rows
                   if row.get("ticker") == "LOCAL"
                   for note in row.get("notes") or []]
    kit.check("ordinary whole-ticker parallel jobs retain local head-only reuse",
              len(ordinary_rows) == 3
              and all(not row.get("halt") for row in ordinary_rows)
              and [row[1] for row in ordinary_ledger.snapshot("heads")]
              == ["LOCAL"]
              and [row[1] for row in ordinary_ledger.snapshot("daily")]
              == ["LOCAL"]
              and sum("live head/daily evidence reused" in note
                      for note in local_notes) == 1,
              f"rows={ordinary_rows} notes={local_notes} "
              f"heads={ordinary_ledger.snapshot('heads')} "
              f"daily={ordinary_ledger.snapshot('daily')}")

    # Force the narrower publication interleaving: worker B has already missed
    # the in-memory cache when worker A publishes evidence and writes the bound
    # sidecar. B then consumes that new sidecar. Its gap_fill-local
    # earliest_seen snapshot predates the write, so only the per-series
    # _pickup_head_attempted marker can suppress a redundant GUI fallback.
    sidecar_root = _root(temp_dir, "shared-pickup-sidecar-race")
    sidecar_conids = {ticker: 56002}
    _pin_identity(sidecar_root, ticker, sidecar_conids[ticker])
    # Keep this a valid continuity-lifecycle race under Row 50: a real 1h
    # TRADES month exists, but no sidecar exists until worker A publishes it.
    _direct_run(sidecar_root, [(ticker, "1h")], Ledger(), sidecar_conids)
    sidecar_path = sidecar_root / "_ibkr_earliest.json"
    if sidecar_path.exists():
        sidecar_path.unlink()
    sidecar_ledger = Ledger()
    cache_lock = threading.Lock()
    cache_calls = [0]
    second_cache_entered = threading.Event()
    sidecar_written = threading.Event()
    real_cached_start = sk._cached_pickup_start
    real_record_evidence = sk._record_pickup_evidence

    def staged_cached_start(*args, **kwargs):
        with cache_lock:
            cache_calls[0] += 1
            call = cache_calls[0]
        if call == 1:
            return None
        second_cache_entered.set()
        if not sidecar_written.wait(timeout=2.0):
            raise AssertionError("producer never published its bound sidecar")
        return real_cached_start(*args, **kwargs)

    def staged_record_evidence(*args, **kwargs):
        result = real_record_evidence(*args, **kwargs)
        sidecar_written.set()
        return result

    class SidecarRaceAdapter(FakeAdapter):
        def head_timestamp(self, contract, what_to_show="TRADES"):
            if not second_cache_entered.wait(timeout=2.0):
                raise AssertionError(
                    "second worker never reached its post-peek sidecar read")
            return super().head_timestamp(contract, what_to_show)

    def sidecar_adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: SidecarRaceAdapter(
            port, sidecar_ledger, sidecar_conids)

    sk._cached_pickup_start = staged_cached_start
    sk._record_pickup_evidence = staged_record_evidence
    try:
        sidecar_report = sk.nightly_gap_fill_parallel(
            sidecar_root, [(ticker, "1m"), (ticker, "5m")],
            [2000, 3000], today=WED, resolved=sidecar_conids,
            adapter_factory=sidecar_adapter_factory,
            on_series=lambda *_args: None)
    finally:
        sk._cached_pickup_start = real_cached_start
        sk._record_pickup_evidence = real_record_evidence
        sidecar_written.set()
    sidecar_rows = sidecar_report.get("series") or []
    sidecar_notes = [str(note).lower() for row in sidecar_rows
                     for note in row.get("notes") or []]
    kit.check("newly published sidecar cannot trigger a second GUI head probe",
              len(sidecar_rows) == 2
              and all(not row.get("halt") for row in sidecar_rows)
              and cache_calls[0] == 2 and second_cache_entered.is_set()
              and any("cache hit" in note for note in sidecar_notes)
              and len(sidecar_ledger.snapshot("heads")) == 1
              and len(sidecar_ledger.snapshot("daily")) == 1
              and not sidecar_ledger.snapshot("qualifies"),
              f"calls={cache_calls} notes={sidecar_notes} "
              f"heads={sidecar_ledger.snapshot('heads')} "
              f"daily={sidecar_ledger.snapshot('daily')} "
              f"qualifies={sidecar_ledger.snapshot('qualifies')} "
              f"rows={sidecar_rows}")

    supplied_root = _root(temp_dir, "shared-pickup-supplied")
    supplied_ledger = Ledger()
    supplied_report, _adapter = _direct_run(
        supplied_root, [(ticker, "1m")], supplied_ledger, conids)
    kit.check("caller-supplied resolved map still performs zero qualification",
              not _series(supplied_report, ticker, "1m").get("halt")
              and not supplied_ledger.snapshot("qualify_many_calls")
              and not supplied_ledger.snapshot("qualifies")
              and supplied_ledger.snapshot("contracts")
              == [(2000, conids[ticker])],
              f"many={supplied_ledger.snapshot('qualify_many_calls')} "
              f"single={supplied_ledger.snapshot('qualifies')} "
              f"contracts={supplied_ledger.snapshot('contracts')}")


def dynamic_plan_consumption(kit, temp_dir):
    kit.section("real dynamic queue consumes completed plan/action payloads")
    root = _root(temp_dir, "dynamic")
    tickers = ("AAA", "BBB", "CCC")
    conids = {ticker: 52000 + i for i, ticker in enumerate(tickers)}
    for ticker in tickers:
        _pin_identity(root, ticker, conids[ticker])
        sv.record_ibkr_earliest(
            root, ticker, MON.isoformat(), conid=conids[ticker])
    _direct_run(root, [(ticker, "1h") for ticker in tickers], Ledger(), conids)
    ledger = Ledger()
    plan_calls = Counter()
    action_calls = Counter()
    plan_done = defaultdict(list)
    action_done = defaultdict(list)
    callback_earliest = []
    evidence_lock = threading.Lock()
    real_plan = sk.plan_gap
    real_actions = sk.sb.load_actions

    def tapped_plan(plan_root, ticker, interval, **kwargs):
        result = real_plan(plan_root, ticker, interval, **kwargs)
        with evidence_lock:
            plan_calls[(ticker, interval)] += 1
            plan_done[(ticker, interval)].append(time.monotonic())
        return result

    def tapped_actions(action_root, ticker):
        result = real_actions(action_root, ticker)
        with evidence_lock:
            action_calls[ticker] += 1
            action_done[ticker].append(time.monotonic())
        return result

    def adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: FakeAdapter(port, ledger, conids, slow=0.01)

    sk.plan_gap = tapped_plan
    sk.sb.load_actions = tapped_actions
    try:
        with isolated_gates():
            report = sk.nightly_gap_fill_parallel(
                root, [(ticker, "1m") for ticker in tickers], [2000],
                today=WED, resolved=conids, adapter_factory=adapter_factory,
                post_pipeline=EmptyPipeline(),
                on_series=lambda ticker, interval, row:
                    callback_earliest.append(
                        (ticker, interval, row.get("ibkr_earliest"))))
    finally:
        sk.plan_gap = real_plan
        sk.sb.load_actions = real_actions

    rows = report.get("series") or []
    fetches = ledger.snapshot("fetches")
    fetch_at = {}
    for fetch in fetches:
        fetch_at.setdefault(fetch[1], fetch[0])
    expected_keys = {(ticker, "1m") for ticker in tickers}
    kit.check("one-port post-pipeline path really used the dynamic queue",
              len(rows) == 3 and all(not row.get("halt") for row in rows),
              repr(rows))
    kit.check("dynamic plans have one weight pass plus one consumed execution plan",
              set(plan_calls) == expected_keys
              and all(plan_calls[key] == 2 for key in expected_keys),
              repr(plan_calls))
    kit.check("every dynamic series action load is completed exactly once",
              set(action_calls) == set(tickers)
              and all(action_calls[ticker] == 1 for ticker in tickers),
              repr(action_calls))
    kit.check("dynamic timing ledger is nonempty, complete, and finite",
              bool(fetches) and set(fetch_at) == set(tickers)
              and all(math.isfinite(float(row[0])) for row in fetches),
              f"fetches={fetches} by_ticker={fetch_at}")
    kit.check("queued successor payloads finish at predecessor final boundaries",
              bool(plan_done[("BBB", "1m")])
              and bool(action_done["BBB"])
              and bool(plan_done[("CCC", "1m")])
              and bool(action_done["CCC"])
              and plan_done[("BBB", "1m")][-1]
              <= fetch_at.get("AAA", float("-inf"))
              and action_done["BBB"][0]
              <= fetch_at.get("AAA", float("-inf"))
              and plan_done[("CCC", "1m")][-1]
              <= fetch_at.get("BBB", float("-inf"))
              and action_done["CCC"][0]
              <= fetch_at.get("BBB", float("-inf")),
              f"fetches={fetch_at} plans={dict(plan_done)} "
              f"actions={dict(action_done)}")
    kit.check("dynamic cache-hit jobs use no live head evidence",
              not ledger.snapshot("heads") and not ledger.snapshot("daily"))
    kit.check("dynamic cache-hit jobs still publish trusted earliest dates to "
              "the completion callback",
              set(callback_earliest)
              == {(ticker, "1m", MON.isoformat()) for ticker in tickers},
              repr(callback_earliest))


def serial_plan_consumption(kit, temp_dir):
    kit.section("serial final-boundary plan/action consumption")
    root = _root(temp_dir, "serial-plan")
    tickers = ("SAA", "SBB")
    conids = {"SAA": 53001, "SBB": 53002}
    for ticker in tickers:
        _pin_identity(root, ticker, conids[ticker])
        sv.record_ibkr_earliest(
            root, ticker, MON.isoformat(), conid=conids[ticker])
    ledger = Ledger()
    plan_calls = Counter()
    action_calls = Counter()
    plan_done = {}
    action_done = {}
    real_plan = sk.plan_gap
    real_actions = sk.sb.load_actions

    def tapped_plan(plan_root, ticker, interval, **kwargs):
        result = real_plan(plan_root, ticker, interval, **kwargs)
        plan_calls[(ticker, interval)] += 1
        plan_done[(ticker, interval)] = time.monotonic()
        return result

    def tapped_actions(action_root, ticker):
        result = real_actions(action_root, ticker)
        action_calls[ticker] += 1
        action_done[ticker] = time.monotonic()
        return result

    sk.plan_gap = tapped_plan
    sk.sb.load_actions = tapped_actions
    try:
        report, _adapter = _direct_run(
            root, [(ticker, "1m") for ticker in tickers], ledger, conids)
    finally:
        sk.plan_gap = real_plan
        sk.sb.load_actions = real_actions
    fetches = ledger.snapshot("fetches")
    first_fetch = fetches[0][0] if fetches else float("inf")
    kit.check("serial preplans are consumed without foreground recomputation",
              all(plan_calls[(ticker, "1m")] == 1 for ticker in tickers)
              and all(action_calls[ticker] == 1 for ticker in tickers),
              f"plans={plan_calls} actions={action_calls}")
    kit.check("successor plan and actions complete before predecessor fetch",
              plan_done[("SBB", "1m")] <= first_fetch
              and action_done["SBB"] <= first_fetch,
              f"first_fetch={first_fetch} plan={plan_done} actions={action_done}")
    kit.check("serial evidence run completes both series",
              len(report.get("series") or []) == 2
              and all(not row.get("halt")
                      for row in report.get("series") or []), repr(report))


def overlap_mismatch_halts(kit, temp_dir):
    kit.section("cached pickup preserves the real overlap safety gate")
    root = _root(temp_dir, "overlap-mismatch")
    ticker, conid, interval = "OVR", 53501, "30m"
    start = datetime.combine(MON, day_time(9, 30))
    stored = []
    mismatched = []
    for index in range(13):
        stamp = start + timedelta(minutes=30 * index)
        price = 10.0 + index * 0.01
        stored.append((stamp, price, price + 0.05, price - 0.05,
                       price + 0.01, 1000 + index))
        wrong = price * 10.0
        mismatched.append(SimpleNamespace(
            date=stamp.replace(tzinfo=NY).astimezone(timezone.utc),
            open=wrong, high=wrong + 0.5, low=wrong - 0.5,
            close=wrong + 0.1, volume=float(1000 + index)))

    month_path = ss.month_file_path(
        root, ticker, MON.year, MON.month, interval)
    stats = ss.write_month_file(month_path, stored)
    manifest = ss.new_manifest(ticker, ticker)
    manifest["conid"] = conid
    ss.manifest_months(manifest, interval)[
        ss.month_key(MON.year, MON.month)] = dict(
            stats, status="present", source="selftest-seed")
    ss.save_manifest(root / ticker, manifest)

    ledger = Ledger()
    adapter = FakeAdapter(
        2000, ledger, {ticker: conid}, historical_bars=mismatched)
    report = sk.nightly_gap_fill(
        root, [(ticker, interval)], adapter_factory=lambda: adapter,
        pacer=sk.Pacer(min_gap_s=0.0, burst_max=0), today=WED,
        resolved={ticker: conid})
    row = _series(report, ticker, interval)
    after, _stats = ss.read_month_file(month_path)
    kit.check("grossly mismatched overlapping bars still halt at the gate",
              "OVERLAP GATE" in (row.get("halt") or "")
              and "13 of 13" in (row.get("halt") or ""),
              repr(row))
    kit.check("overlap halt commits nothing over the seeded archive",
              after == stored and row.get("added", 0) == 0,
              f"rows_before={len(stored)} rows_after={len(after)} row={row}")


def pacing_wait_status(kit, temp_dir):
    kit.section("D4 exact pacing waits and truthful GUI lifecycle")
    kit.check("D4 feature flag is enabled", sk.PACING_WAIT_STATUS is True)
    kit.check("D4 leaves every measured pacing constant unchanged",
              (sk.PACE_MAX_REQUESTS, sk.PACE_WINDOW_S,
               sk.PACE_MIN_GAP_S, sk.PACE_BURST_MAX,
               sk.PACE_BURST_WINDOW_S) == (58, 600.0, 0.15, 6, 2.0))

    class Clock:
        def __init__(self):
            self.t = 100.0
            self.sleeps = []
            self.active = None

        def time(self):
            return self.t

        def sleep(self, seconds, cancel=None):
            if self.active is not None and not self.active():
                raise AssertionError("pacing observer was not active in sleep")
            self.sleeps.append(float(seconds))
            self.t += float(seconds)

    def observer_trace(clock):
        events = []
        active = {"value": False}

        def observe(waiting, info):
            active["value"] = bool(waiting)
            events.append((bool(waiting), dict(info or {}), clock.time()))

        clock.active = lambda: active["value"]
        return events, active, observe

    # Each sleep reason is isolated so the reported value can be compared to
    # the exact argument consumed by the unchanged pacing arithmetic.
    min_clock = Clock()
    min_events, min_active, min_observer = observer_trace(min_clock)
    min_pacer = sk.Pacer(
        max_requests=100, window_s=10.0, min_gap_s=0.75,
        burst_max=0, time_fn=min_clock.time, sleep_fn=min_clock.sleep)
    min_pacer.wait_turn(metered=False, on_wait=min_observer)
    no_wait_events = list(min_events)
    min_pacer.wait_turn(metered=False, on_wait=min_observer)
    kit.check("no status is emitted when wait_turn does not sleep",
              no_wait_events == [])
    kit.check("minimum-gap status equals the exact pacer sleep and clears",
              len(min_events) == 2
              and min_events[0][0] is True
              and min_events[0][1].get("reason") == "min-gap"
              and math.isclose(min_events[0][1].get("seconds"), 0.75)
              and math.isclose(min_clock.sleeps[0], 0.75)
              and min_events[1][0] is False and not min_active["value"],
              f"events={min_events} sleeps={min_clock.sleeps}")

    burst_clock = Clock()
    burst_events, burst_active, burst_observer = observer_trace(burst_clock)
    burst_pacer = sk.Pacer(
        max_requests=100, window_s=10.0, min_gap_s=0.0,
        burst_max=1, burst_window_s=2.0,
        time_fn=burst_clock.time, sleep_fn=burst_clock.sleep)
    burst_pacer.wait_turn(metered=False, on_wait=burst_observer)
    burst_pacer.wait_turn(metered=False, on_wait=burst_observer)
    burst_expected = 2.005
    kit.check("unmetered burst status includes the strict-window slack exactly",
              len(burst_events) == 2
              and burst_events[0][1].get("reason") == "burst"
              and math.isclose(
                  burst_events[0][1].get("seconds"), burst_expected,
                  abs_tol=1e-9)
              and math.isclose(burst_clock.sleeps[0], burst_expected,
                               abs_tol=1e-9)
              and burst_events[-1][0] is False
              and not burst_active["value"],
              f"events={burst_events} sleeps={burst_clock.sleeps}")

    window_clock = Clock()
    window_events, window_active, window_observer = observer_trace(window_clock)
    window_pacer = sk.Pacer(
        max_requests=1, window_s=10.0, min_gap_s=0.0, burst_max=0,
        time_fn=window_clock.time, sleep_fn=window_clock.sleep)
    window_pacer.wait_turn(metered=True, on_wait=window_observer)
    window_pacer.wait_turn(metered=True, on_wait=window_observer)
    window_expected = 10.005
    kit.check("metered-window status equals the exact sliding-window sleep",
              len(window_events) == 2
              and window_events[0][1].get("reason") == "window"
              and math.isclose(
                  window_events[0][1].get("seconds"), window_expected,
                  abs_tol=1e-9)
              and math.isclose(window_clock.sleeps[0], window_expected,
                               abs_tol=1e-9)
              and window_events[-1][0] is False
              and not window_active["value"],
              f"events={window_events} sleeps={window_clock.sleeps}")

    bypass_clock = Clock()
    bypass_pacer = sk.Pacer(
        max_requests=1, window_s=10.0, min_gap_s=0.0, burst_max=0,
        time_fn=bypass_clock.time, sleep_fn=bypass_clock.sleep)
    for _ in range(8):
        bypass_pacer.wait_turn(metered=False)
    kit.check("minute-plus remains outside 58/600 when burst is isolated",
              not bypass_clock.sleeps and not bypass_pacer._stamps)
    bypass_pacer.saturate()
    bypass_pacer.wait_turn(metered=False)
    kit.check("saturate still force-meters later minute-plus requests",
              bool(bypass_clock.sleeps)
              and bypass_clock.sleeps[-1] >= 10.0
              and len(bypass_pacer._stamps) == 1,
              f"sleeps={bypass_clock.sleeps} stamps={list(bypass_pacer._stamps)}")

    cancel_clock = Clock()
    cancel_events, cancel_active, cancel_observer = observer_trace(cancel_clock)
    cancel_pacer = sk.Pacer(
        max_requests=100, window_s=10.0, min_gap_s=0.5, burst_max=0,
        time_fn=cancel_clock.time, sleep_fn=cancel_clock.sleep)
    cancel_pacer.wait_turn(metered=False)
    first_stamp = cancel_pacer._last

    def cancelled_sleep(seconds, cancel=None):
        if not cancel_active["value"]:
            raise AssertionError("cancelled sleep lacked active status")
        raise sk.Cancelled()

    cancel_pacer._sleep = cancelled_sleep
    cancelled = False
    try:
        cancel_pacer.wait_turn(metered=False, on_wait=cancel_observer)
    except sk.Cancelled:
        cancelled = True
    kit.check("cancelled pacing always emits its paired clear and no request",
              cancelled and [event[0] for event in cancel_events] == [True, False]
              and not cancel_active["value"] and cancel_pacer._last == first_stamp,
              f"events={cancel_events} last={cancel_pacer._last}/{first_stamp}")

    observer_clock = Clock()
    observer_pacer = sk.Pacer(
        max_requests=100, window_s=10.0, min_gap_s=0.5, burst_max=0,
        time_fn=observer_clock.time, sleep_fn=observer_clock.sleep)
    observer_pacer.wait_turn(metered=False)

    def broken_observer(_waiting, _info):
        raise RuntimeError("cosmetic observer failure")

    observer_pacer.wait_turn(metered=False, on_wait=broken_observer)
    kit.check("a broken status observer cannot alter pacing or request recording",
              observer_clock.sleeps == [0.5]
              and math.isclose(observer_pacer._last, 100.5))

    # The status clear is inside wait_turn's finally, so it must precede the
    # broker call. This distinguishes pacing from ordinary response latency.
    boundary_clock = Clock()
    boundary_pacer = sk.Pacer(
        max_requests=1, window_s=10.0, min_gap_s=0.0, burst_max=0,
        time_fn=boundary_clock.time, sleep_fn=boundary_clock.sleep)
    boundary_pacer.wait_turn(metered=True)
    pacing_active = {"value": False}
    pacing_messages = []

    def boundary_say(message):
        parsed = sk._parse_pacing_status_msg(message)
        if parsed is not None:
            pacing_active["value"] = bool(parsed["waiting"])
            pacing_messages.append(parsed)

    class BoundaryAdapter:
        use_rth = True

        def fetch(self, contract, end_dt, duration, bar_size):
            if pacing_active["value"]:
                raise AssertionError("pacing leaked into broker latency")
            return []

    boundary_result = {"requests": 0}
    sk._fetch_request(
        BoundaryAdapter(), SimpleNamespace(symbol="BOUND"),
        datetime(2024, 6, 17, 16, 0), "1 D", "1 min",
        boundary_pacer, None, boundary_say, lambda: None,
        "BOUND", boundary_result, "D4 selftest", metered=True)
    kit.check("paired clear lands before adapter.fetch response latency begins",
              [row["waiting"] for row in pacing_messages] == [True, False]
              and not pacing_active["value"]
              and boundary_result["requests"] == 1,
              repr(pacing_messages))

    # Drive the real parallel worker status seam with two instant fake clocks.
    # Every port must publish its own active snapshot and a clear before done.
    root = _root(temp_dir, "pacing-parallel")
    conids = {"PAA": 54501, "PBB": 54502}
    for ticker, conid in conids.items():
        _pin_identity(root, ticker, conid)
    ledger = Ledger()
    port_events = defaultdict(list)
    port_lock = threading.Lock()
    real_pacer = sk.Pacer
    real_default_pacer = sk._default_pacer

    class InstantStatusPacer(real_pacer):
        def __init__(self, *args, **kwargs):
            self.test_clock = Clock()
            super().__init__(
                max_requests=100, window_s=10.0, min_gap_s=0.5,
                burst_max=0, time_fn=self.test_clock.time,
                sleep_fn=self.test_clock.sleep)
            self.wait_turn(metered=False)  # Seed the next exact half-second wait.

    class StatusAdapter(FakeAdapter):
        def fetch(self, contract, end_dt, duration, bar_size, what_to_show="TRADES"):
            # This is a worker-status unit, not LiveIB acceptance. A plain fake
            # never reaches the real choke that now owns turn acquisition, so
            # consume the supplied turn here to drive the unchanged UI seam.
            attempt = sk.fib.take("ibkr-bars", contract)
            attempt.acquire_turn()
            return super().fetch(contract, end_dt, duration, bar_size, what_to_show)

    def adapter_factory(_host, ports):
        port = int(ports[0])
        return lambda: StatusAdapter(port, ledger, conids)

    def capture_status(port, info):
        with port_lock:
            port_events[int(port)].append(dict(info or {}))

    sk._default_pacer = InstantStatusPacer
    try:
        parallel_report = sk.nightly_gap_fill_parallel(
            root, [("PAA", "1m"), ("PBB", "1m")], [2000, 3000],
            today=WED, resolved=conids, adapter_factory=adapter_factory,
            port_status=capture_status)
    finally:
        sk._default_pacer = real_default_pacer
    port_sequences = {port: list(rows) for port, rows in port_events.items()}
    active_by_port = {
        port: [row for row in rows if row.get("pacing_detail")]
        for port, rows in port_sequences.items()
    }
    kit.check("parallel workers publish independent exact pacing snapshots",
              set(active_by_port) == {2000, 3000}
              and all(active_by_port[port] for port in active_by_port)
              and all(any(float(row.get("pacing_seconds") or 0.0) == 0.5
                          for row in active_by_port[port])
                      for port in active_by_port), repr(active_by_port))
    kit.check("each port clears pacing before its terminal snapshot",
              all(rows and not rows[-1].get("pacing_detail")
                  and float(rows[-1].get("pacing_seconds") or 0.0) == 0.0
                  and rows[-1].get("state") in ("done", "error")
                  for rows in port_sequences.values())
              and len(parallel_report.get("series") or []) == 2,
              repr({port: rows[-3:] for port, rows in port_sequences.items()}))

    # Headless GUI contract: control events are swallowed from the text log,
    # empty detail clears are preserved, and stale timestamps cannot override
    # an active, engine-owned pacing row.
    # Compile only the exact UI methods under test. Importing display_data runs
    # dependency bootstrap and can launch an installer/exit before our summary.
    # No module-level statements, GUI construction, or bootstrap are executed.
    wanted = {
        "_fmt_dur", "_batch_elapsed", "_batch_shown", "_batch_label",
        "_batch_with_detail", "_batch_visible_detail", "_batch_tick",
        "_fetch_pacing_detail", "_fetch_progress_detail", "_batch_set_detail",
        "_batch_set_pacing_detail", "_drain_progress_batched",
        "_drain_ibkr_progress", "_batch_say", "_pacing_detail_from_states",
        "_ibkr_render_ports", "_find_render_ports",
    }
    source_path = ENGINE_DIR.parent / "display_data.py"
    source = ast.parse(source_path.read_text(encoding="utf-8-sig"))
    owner = next(node for node in source.body
                 if isinstance(node, ast.ClassDef) and node.name == "DataViewerApp")
    methods = [node for node in owner.body
               if isinstance(node, ast.FunctionDef) and node.name in wanted]
    assert {node.name for node in methods} == wanted
    assert all(isinstance(decorator, ast.Name) and decorator.id == "staticmethod"
               for node in methods for decorator in node.decorator_list)
    isolated = ast.Module(body=[ast.ClassDef(name="DataViewerApp", bases=[],
        keywords=[], body=methods, decorator_list=[])], type_ignores=[])
    namespace = {"stock_ibkr": sk}
    exec(compile(ast.fix_missing_locations(isolated), str(source_path), "exec"), namespace)
    dd = SimpleNamespace(DataViewerApp=namespace["DataViewerApp"])

    app = object.__new__(dd.DataViewerApp)
    waiting_message = sk._pacing_status_msg(
        True, {"seconds": 120.0, "reason": "window"})
    ready_message = sk._pacing_status_msg(False)
    drain_q = queue.Queue()
    drain_q.put(("progress", waiting_message))
    waiting_drain = app._drain_progress_batched(drain_q)
    drain_q.put(("progress", ready_message))
    ready_drain = app._drain_progress_batched(drain_q)
    update_q = queue.Queue()
    update_q.put(("progress", waiting_message))
    update_waiting_drain = app._drain_ibkr_progress(update_q)
    update_q.put(("progress", ready_message))
    update_ready_drain = app._drain_ibkr_progress(update_q)
    wait_label = sk._pacing_wait_label(120.0)
    kit.check("fetch-window control records stay out of the scrolling log",
              waiting_drain[0] == []
              and waiting_drain[-1] == {
                  "detail": None, "pacing": wait_label}
              and ready_drain[0] == []
              and ready_drain[-1] == {"detail": None, "pacing": ""}
              and update_waiting_drain[0] == []
              and update_waiting_drain[-1] == {
                  "detail": None, "pacing": wait_label}
              and update_ready_drain[0] == []
              and update_ready_drain[-1] == {
                  "detail": None, "pacing": ""},
              f"add={waiting_drain}/{ready_drain} "
              f"update={update_waiting_drain}/{update_ready_drain}")

    class DummyLabel:
        def __init__(self):
            self.text = ""

        def config(self, **kwargs):
            if "text" in kwargs:
                self.text = str(kwargs["text"])

    class DummyVar:
        def __init__(self):
            self.value = ""

        def set(self, value):
            self.value = str(value)

    class DummyDot:
        def itemconfig(self, _item, **_kwargs):
            return None

    class DummyRoot:
        def after(self, _delay_ms, _callback):
            return None

    batch_label = DummyLabel()
    now = time.time()
    app._batch_live = {
        "lbl": batch_label, "start": now, "i": 1, "n": 3,
        "samples": [], "eta0": None, "eta_at": 0.0,
        "paused_total": 0.0, "paused_at": None,
        "detail": "", "pacing_detail": "",
    }
    app.root = DummyRoot()
    app._test_progress_label = batch_label
    app._batch_set_detail(batch_label, "PAA 1m - 1/3 days")
    app._batch_say(
        None, "_unused_bar", "_test_progress_label", "_unused_start",
        [], None, waiting_drain[-1])
    serial_during_wait = batch_label.text
    app._batch_say(
        None, "_unused_bar", "_test_progress_label", "_unused_start",
        [], None, ready_drain[-1])
    serial_after_wait = batch_label.text
    kit.check("serial pacing keeps ordinary progress visible, then clears",
              wait_label in serial_during_wait
              and "PAA 1m - 1/3 days" in serial_during_wait
              and serial_during_wait.index("PAA 1m - 1/3 days")
              < serial_during_wait.index(wait_label)
              and wait_label not in serial_after_wait
              and "PAA 1m - 1/3 days" in serial_after_wait,
              f"during={serial_during_wait!r} after={serial_after_wait!r}")

    app._batch_set_detail(batch_label, "PAA 1m - 1/3 days")
    app._batch_set_pacing_detail(batch_label, wait_label)
    app._batch_tick()
    during_wait = batch_label.text
    app._batch_set_pacing_detail(batch_label, "")
    app._batch_tick()
    after_wait = batch_label.text
    kit.check("parallel pacing keeps ordinary progress visible, then clears",
              wait_label in during_wait
              and "PAA 1m - 1/3 days" in during_wait
              and during_wait.index("PAA 1m - 1/3 days")
              < during_wait.index(wait_label)
              and wait_label not in after_wait
              and "PAA 1m - 1/3 days" in after_wait,
              f"during={during_wait!r} after={after_wait!r}")

    two_states = {
        2000: {"state": "working", "pacing_detail":
               sk._pacing_wait_label(5.0), "pacing_seconds": 5.0},
        3000: {"state": "working", "pacing_detail":
               sk._pacing_wait_label(8.0), "pacing_seconds": 8.0},
    }
    both = app._pacing_detail_from_states(two_states)
    two_states[2000]["pacing_detail"] = ""
    one_left = app._pacing_detail_from_states(two_states)
    two_states[3000]["pacing_detail"] = ""
    none_left = app._pacing_detail_from_states(two_states)
    kit.check("one port clear cannot erase another active pacing overlay",
              both == sk._pacing_wait_label(8.0)
              and one_left == sk._pacing_wait_label(8.0)
              and none_left == "")

    stale = {
        "state": "working", "ticker": "PAA", "count": 2,
        "detail": "1/3 days", "pacing_detail": wait_label,
        "pacing_seconds": 120.0, "ts": time.time() - 120.0,
    }
    update_var = DummyVar()
    update_app = object.__new__(dd.DataViewerApp)
    update_app._ibkr_port_rows = {
        2000: (DummyDot(), 1, update_var, DummyLabel())}
    update_app._ibkr_port_lock = threading.Lock()
    update_app._ibkr_port_states = {2000: dict(stale)}
    update_app._ibkr_progress_lbl = None
    update_app._ibkr_pause_state = "running"
    update_app._ibkr_render_ports()

    find_var = DummyVar()
    find_app = object.__new__(dd.DataViewerApp)
    find_app._find_port_rows = {
        2000: (DummyDot(), 1, find_var, DummyLabel())}
    find_app._find_port_lock = threading.Lock()
    find_app._find_port_states = {2000: dict(stale)}
    find_app._storage_find_progress_lbl = None
    find_app._storage_find_pause_ev = None
    find_app._find_pause_state = "running"
    find_app._find_render_ports()
    kit.check("120-second pacing stays truthful in both port renderers",
              wait_label in update_var.value and "OFFLINE?" not in update_var.value
              and wait_label in find_var.value
              and "waiting 120" not in find_var.value,
              f"update={update_var.value!r} find={find_var.value!r}")


def cancellation_boundary(kit, temp_dir):
    kit.section("cancelled final boundary remains bounded")
    root = _root(temp_dir, "cancel")
    conids = {"CAA": 54001, "CBB": 54002}
    for ticker in conids:
        _pin_identity(root, ticker, conids[ticker])
        sv.record_ibkr_earliest(
            root, ticker, MON.isoformat(), conid=conids[ticker])
    ledger = Ledger()
    adapter = FakeAdapter(2000, ledger, conids)
    cancel = threading.Event()
    cancel.set()
    started = time.monotonic()
    report = sk.nightly_gap_fill(
        root, [("CAA", "1m"), ("CBB", "1m")], cancel=cancel,
        adapter_factory=lambda: adapter,
        pacer=sk.Pacer(min_gap_s=0.0, burst_max=0), today=WED,
        resolved=conids)
    elapsed = time.monotonic() - started
    kit.check("pre-set cancel returns promptly without a historical request",
              elapsed < 1.0 and not ledger.snapshot("fetches"),
              f"elapsed={elapsed:.3f} fetches={ledger.snapshot('fetches')}")
    kit.check("cancel remains a cancelled report, not a worker abort",
              report.get("cancelled") is True and not report.get("aborted"),
              repr(report))


def speculative_fallbacks(kit, temp_dir):
    kit.section("stale and failed speculation fall back foreground")
    root = _root(temp_dir, "fallbacks")
    ahead = sk._PickupPlanAhead(root, WED)
    ahead.prepare("STALE", "1m")
    real_last = sk.series_last_dt
    sk.series_last_dt = lambda manifest, interval: datetime(2024, 6, 18)
    try:
        stale_result = ahead.take("STALE", "1m")
    finally:
        sk.series_last_dt = real_last
    kit.check("changed frontier invalidates a requeued ordinary preplan",
              stale_result is None)

    failing = sk._PickupPlanAhead(root, WED)
    real_plan = sk.plan_gap
    sk.plan_gap = lambda *args, **kwargs: (_ for _ in ()).throw(
        OSError("synthetic plan failure"))
    try:
        failing.prepare("FAIL", "1m")
    finally:
        sk.plan_gap = real_plan
    kit.check("failed speculation leaves no consumable payload",
              failing.take("FAIL", "1m") is None)


def main():
    kit = CheckKit()
    with tempfile.TemporaryDirectory(prefix="fetch-pickup-selftest-") as temp:
        with isolated_gates():
            contract_checks(kit, temp)
            malformed_manifest_month_fallback(kit, temp)
            cached_kind_and_overlap(kit, temp)
            malformed_repair(kit, temp)
            identity_divergence_fallback(kit, temp)
            combined_cache(kit, temp)
            gui_interleaving_boundaries(kit, temp)
            deep_head_floor_is_not_cache_authority(kit, temp)
            serial_plan_consumption(kit, temp)
            overlap_mismatch_halts(kit, temp)
            shared_evidence_primitives(kit)
            parallel_shared_pickup(kit, temp)
            pacing_wait_status(kit, temp)
            cancellation_boundary(kit, temp)
            speculative_fallbacks(kit, temp)
        # dynamic_plan_consumption owns its isolation before starting workers.
        dynamic_plan_consumption(kit, temp)
    return kit.finish()


if __name__ == "__main__":
    raise SystemExit(main())
