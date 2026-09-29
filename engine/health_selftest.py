"""Self-tests for health.py.

Standalone, no network. Run:
    python engine/health_selftest.py
"""

import datetime as dt
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import health  # noqa: E402
import stock_storage as ss  # noqa: E402


FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def month_iter(first, last):
    y, m = int(first[:4]), int(first[5:7])
    ey, em = int(last[:4]), int(last[5:7])
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            y += 1
            m = 1


def write_csv_month(root, ticker, interval, year, month, days=None):
    if days is None:
        day = 1
        while dt.date(year, month, day).weekday() >= 5:
            day += 1
        days = [day]
    bars = []
    for day in days:
        ts = dt.datetime(year, month, day, 0, 0, 0)
        if not ss.base_interval(interval).endswith("d"):
            ts = dt.datetime(year, month, day, 9, 30, 0)
        bars.append((ts, 1.0, 1.1, 0.9, 1.0, 100))
    path = ss.month_file_path(root, ticker, year, month, interval, fmt="csv")
    return ss.write_month_file(path, bars)


def seed(root, ticker, first, last, earliest=None, interval="1m",
         conid=None, aliases=None, last_days=None):
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.new_manifest(ticker, ticker)
    man["conid"] = conid
    man["aliases"] = list(aliases or [])
    months = ss.manifest_months(man, interval)
    all_months = list(month_iter(first, last))
    for ym in all_months:
        months[ym] = {"status": "present", "rows": 1}
    y, m = int(last[:4]), int(last[5:7])
    stats = write_csv_month(root, ticker, interval, y, m, days=last_days)
    months[last].update(stats)
    ss.save_manifest(tdir, man)
    if earliest is not None:
        ep = Path(root) / "_ibkr_earliest.json"
        try:
            data = json.loads(ep.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        data[ticker] = earliest
        ep.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")


def add_kind(root, ticker, interval, first, last):
    tdir = Path(root) / ticker
    man = ss.load_manifest(tdir)
    months = ss.manifest_months(man, interval)
    for ym in month_iter(first, last):
        months[ym] = {"status": "present", "rows": 1}
    year, month = int(last[:4]), int(last[5:7])
    months[last].update(
        write_csv_month(root, ticker, interval, year, month))
    ss.save_manifest(tdir, man)


project = Path(tempfile.mkdtemp(prefix="health_st_")) / "proj"
root = project / ss.STORAGE_DIR_NAME
root.mkdir(parents=True)

# Coverage: baseline-depth tickers + one stale tail + one front-short.
for idx, t in enumerate(("AAA", "BBB", "CCC", "DDD")):
    seed(root, t, "2011-06", "2026-07", earliest="1980-01-02",
         conid=1000 + idx)
seed(root, "STALE", "2011-06", "2020-01", earliest="2010-01-01",
     conid=2001)
seed(root, "ODFL", "2024-05", "2026-07", earliest="1991-10-01",
     conid=2002)

# Report-only per-kind stale row. It must be visible without queueing AAA or
# making an otherwise clean report non-clean.
add_kind(root, "AAA", "1m-iv", "2026-05", "2026-05")

# Identity: duplicate conId + quarantined-conId reuse + rename exemption.
seed(root, "DUPA", "2020-01", "2026-07", earliest="2020-01-01", conid=777)
seed(root, "DUPB", "2020-01", "2026-07", earliest="2020-01-01", conid=777)
seed(root, "QACT", "2020-01", "2026-07", earliest="2020-01-01", conid=999)
seed(project / "_quarantine", "QOLD", "2020-01", "2020-01", conid=999)
seed(root, "OLD", "2020-01", "2026-07", earliest="2020-01-01", conid=333,
     aliases=["NEW"])
seed(root, "NEW", "2020-01", "2026-07", earliest="2020-01-01", conid=333,
     aliases=["OLD"])

# Frozen frontier: corrupt the last stored month of one active series.
seed(root, "BADF", "2020-01", "2020-02", earliest="2020-01-01", conid=444)
bad_path = ss.month_file_path(root, "BADF", 2020, 2, "1m", fmt="csv")
bad_path.write_text("bad header\n", encoding="ascii")

# Calendar drift: two daily series trade Monday/Wednesday but not Tuesday.
seed(root, "CALA", "2020-01", "2020-01", interval="1d", conid=501,
     last_days=[6, 8])
seed(root, "CALB", "2020-01", "2020-01", interval="1d", conid=502,
     last_days=[6, 8])

report = health.audit(root, project_root=project, write=True, asof="TEST")
counts = health.summary_counts(report)
queue = set(health.format_queue(report))
loaded = health.load_report(root)

check("health: coverage forward-stale is surfaced",
      counts["coverage_forward_stale"] >= 1,
      str(report["coverage"].get("forward_stale")))
check("health: coverage front-short is surfaced",
      any(r.get("ticker") == "ODFL"
          for r in report["coverage"].get("front_short") or []),
      str(report["coverage"].get("front_short")))
check("health: per-kind staleness is visible and remains off the queue",
      counts["coverage_kind_forward_stale"] == 1
      and counts["coverage_kind_unevaluated"] == 2
      and "AAA" not in queue,
      str((counts, sorted(queue))))
check("health: duplicate identity group is surfaced",
      report["identity"]["dup_conid"] == {777: ["DUPA", "DUPB"]},
      str(report["identity"]["dup_conid"]))
check("health: quarantined identity reuse is surfaced",
      [tuple(x) for x in report["identity"]["quarantine_reuse"]]
      == [("QACT", 999)],
      str(report["identity"]["quarantine_reuse"]))
check("health: alias rename remains exempt",
      333 not in report["identity"]["dup_conid"],
      str(report["identity"]["dup_conid"]))
check("health: frozen frontier strict-read failure is surfaced",
      any(r.get("ticker") == "BADF"
          for r in report["frontier"]["frozen_frontiers"]),
      str(report["frontier"]["frozen_frontiers"]))
check("health: calendar drift is auto-pinned",
      "2020-01-07" in report["calendar"].get("auto_added", [])
      and "2020-01-07" not in report["calendar"].get("missing", []),
      str(report["calendar"]))
check("health: queue is the union of ticker repair signals",
      {"ODFL", "STALE", "DUPA", "DUPB", "QACT", "BADF"}.issubset(queue),
      str(sorted(queue)))
check("health: report sidecar is written and loadable",
      Path(report["report_path"]).exists()
      and loaded.get("kind") == "health_report")
health_raw = Path(report["report_path"]).read_bytes()
health_canonical = json.dumps(
    loaded, sort_keys=True, separators=(",", ":")
).encode("utf-8")
health_pretty = json.dumps(loaded, sort_keys=True, indent=2).encode("utf-8")
check("health: sidecar uses bounded canonical compact JSON",
      health_raw == health_canonical
      and len(health_raw) < len(health_pretty),
      str((len(health_raw), len(health_pretty))))
check("health: summary reports non-clean state",
      not report["clean"] and "Health report:" in health.summarize_report(report)[0],
      str(health.summarize_report(report)))
check("health: summary includes bounded per-kind rows",
      any(line.startswith("Per-kind staleness: 1 forward-stale")
          and "AAA 1m-iv" in line
          for line in health.summarize_report(report)),
      str(health.summarize_report(report)))
legacy_report = {
    **report,
    "counts": {
        key: value for key, value in report["counts"].items()
        if not key.startswith("coverage_kind_")
    },
}
check("health: cached pre-WS1b reports remain renderable",
      any(line.startswith("Per-kind staleness: 0 forward-stale")
          for line in health.summarize_report(legacy_report)))


def write_daily_ratio_series(root, ticker, start, end, boundary, ratio_fn):
    root = Path(root)
    tdir = root / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.new_manifest(ticker, ticker)
    months = ss.manifest_months(man, "1d")
    by_month = {}
    day = start
    one = dt.timedelta(days=1)
    while day <= end:
        if day.weekday() < 5:
            ratio = ratio_fn(day, boundary)
            close = 100.0 * ratio
            ym = f"{day.year:04d}-{day.month:02d}"
            by_month.setdefault((day.year, day.month), []).append(
                (dt.datetime(day.year, day.month, day.day),
                 close, close, close, close, 100))
        day += one
    for (year, month), bars in sorted(by_month.items()):
        path = ss.month_file_path(
            root, ticker, year, month, "1d", fmt="csv")
        months[f"{year:04d}-{month:02d}"] = ss.write_month_file(path, bars)
    ss.save_manifest(tdir, man)


triage_project = Path(tempfile.mkdtemp(prefix="health_triage_st_")) / "proj"
triage_root = triage_project / ss.STORAGE_DIR_NAME
triage_root.mkdir(parents=True)
triage_boundary = dt.date(2020, 4, 1)
triage_ref = {}
write_daily_ratio_series(
    triage_root, "PHAN", dt.date(2020, 1, 1), dt.date(2020, 6, 30),
    triage_boundary, lambda day, b: 0.25 if day < b else 1.0)
day = dt.date(2020, 1, 1)
while day <= dt.date(2020, 6, 30):
    if day.weekday() < 5:
        triage_ref[day.isoformat()] = (100.0, 100.0, 100.0, 100.0, 1000)
    day += dt.timedelta(days=1)


def triage_ref_fn(ticker, rng="Max"):
    if ticker != "PHAN":
        raise KeyError(ticker)
    return triage_ref


triage_health = health.audit(
    triage_root, project_root=triage_project, asof="TRIAGE",
    triage_flags=[{"ticker": "PHAN", "boundary": "2020-04-01"}],
    triage_ref_fn=triage_ref_fn)
triage_counts = health.summary_counts(triage_health)
check("health: triage component classifies supplied boundary flags",
      triage_counts["triage_phantom"] == 1
      and triage_counts["triage_needs_human"] == 1,
      str(triage_health.get("triage")))
check("health: triage needs-human feeds the repair queue and clean bit",
      "PHAN" in health.format_queue(triage_health)
      and not triage_health["clean"],
      str((health.format_queue(triage_health), triage_health["clean"])))
check("health: summary includes non-empty triage component",
      any(line.startswith("Triage:") for line in health.summarize_report(
          triage_health)),
      str(health.summarize_report(triage_health)))


def split_row(ticker, verdict, *, repair=False, confirm=False,
              severity="info", action="no action"):
    return {
        "ticker": ticker,
        "date": "2020-04-01",
        "verdict": verdict,
        "scope": "current",
        "row_type": "candidate",
        "confidence": "high" if severity != "operational" else "low",
        "severity": severity,
        "repair_queue": repair,
        "needs_confirmation": confirm,
        "reason": f"fixture {verdict}",
        "recommended_action": action,
    }


split_rows = [
    split_row("PHAN", "PHANTOM", repair=True, severity="severe",
              action="confirm guarded correction"),
    split_row("MISS", "MISSING", repair=True, severity="severe",
              action="review unadjusted split"),
    split_row("REGR", "REGRESSION", repair=True, severity="severe",
              action="halt and review recurrence"),
    split_row("BASIS", "IDENTITY_BASIS", repair=True, severity="review",
              action="review identity lineage"),
    split_row("REAL", "REAL"),
    split_row("DONE", "RESOLVED"),
    split_row("OLD", "STALE", severity="operational",
              action="refresh split references"),
    split_row("ASK", "UNVERIFIABLE", confirm=True,
              severity="operational",
              action="obtain authoritative evidence"),
]
split_counts_fixture = {
    verdict: sum(row["verdict"] == verdict for row in split_rows)
    for verdict in (
        "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
        "REAL", "RESOLVED", "STALE", "UNVERIFIABLE", "CLEAN")
}
split_report_fixture = {
    "kind": "split_audit",
    "network": False,
    "report_only": True,
    "output_path": str(triage_root / "_split_audit.json"),
    "summary": {"ticker_count": 8, "data_clean": False},
    "verdict_counts": split_counts_fixture,
    "repair_queue": [row for row in split_rows if row["repair_queue"]] + [
        split_row("BAD", "UNVERIFIABLE", repair=True,
                  severity="operational",
                  action="malformed component queue row")],
    "needs_confirmation": [
        row for row in split_rows if row["needs_confirmation"]],
    "stale_unverifiable_rows": [
        row for row in split_rows
        if row["verdict"] in {"STALE", "UNVERIFIABLE"}],
    "tickers": [{"ticker": "FIXTURE", "rows": split_rows}],
}
split_write_calls = []


def split_audit_fixture(_root, write=False):
    split_write_calls.append(bool(write))
    return split_report_fixture


split_health = health.audit(
    triage_root, project_root=triage_project, write=True, asof="SPLITS",
    triage_flags=[{"ticker": "PHAN", "boundary": "2020-04-01"},
                  {"ticker": "ASK", "boundary": "2020-04-01"}],
    split_audit_fn=split_audit_fixture)
split_health_counts = health.summary_counts(split_health)
split_health_queue = health.format_queue(split_health)
check("health splits: all required verdict counts are stable",
      [split_health_counts[f"split_{name.lower()}"] for name in (
          "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
          "REAL", "RESOLVED", "STALE", "UNVERIFIABLE")]
      == [1] * 8,
      str(split_health_counts))
check("health splits: only four repair verdicts enter queue exactly once",
      all(split_health_queue.count(ticker) == 1
          for ticker in ("PHAN", "MISS", "REGR", "BASIS"))
      and "ASK" not in split_health_queue and "OLD" not in split_health_queue
      and "BAD" not in split_health_queue
      and split_health_counts["split_repair_queue"] == 4,
      str(split_health_queue))
check("health splits: confirmation and operational states stay separate",
      split_health_counts["split_needs_confirmation"] == 1
      and split_health_counts["split_operational_warnings"] == 2
      and split_health_counts["triage_needs_confirmation"] == 1,
      str(split_health_counts))
check("health splits: context reaches triage before legacy reference",
      {row["ticker"]: row["bucket"]
       for row in split_health["triage"]["classified"]}
      == {"PHAN": "needs-human", "ASK": "needs-confirmation"},
      str(split_health["triage"]))
split_summary = health.summarize_report(split_health)
check("health splits: Verify/Fix summary includes bounded actions",
      any(line.startswith("Splits: PHANTOM=1") for line in split_summary)
      and any("confirm guarded correction" in line
              and "obtain authoritative evidence" in line
              for line in split_summary)
      and split_write_calls == [True],
      str(split_summary))

review_only = {
    "coverage": {
        "kind_series_count": 1,
        "kind_current_count": 0,
        "kind_forward_stale": [{
            "ticker": "AAA", "kind": "1m-iv",
            "kind_last": "2026-05", "primary_last": "2026-07",
            "lag_months": 2, "verdict": "KIND_FORWARD_STALE",
        }],
        "kind_unevaluated": [],
    },
    "identity": {}, "frontier": {}, "calendar": {},
    "triage": {}, "errors": [],
    "gaps": {
        "kind": "gap_evidence_evaluation", "expected_series": 0,
        "current_series": 0, "rows": [], "fillable": [],
        "source_absent": [], "interior": [], "ignored": [],
        "unavailable": [], "errors": [],
    },
    "source_state": {
        "schema_version": ss.BANK_STATE_FINGERPRINT_VERSION,
        "algorithm": "sha256", "before_sha256": "a" * 64,
        "after_sha256": "a" * 64, "manifest_count": 0,
        "current": True,
    },
    "splits": {
        **split_report_fixture,
        "repair_queue": [],
        "verdict_counts": {
            **{key: 0 for key in split_counts_fixture},
            "STALE": 1, "UNVERIFIABLE": 1,
        },
    },
}
review_only["counts"] = health.summary_counts(review_only)
check("health splits: operational/confirmation-only state remains data-clean",
      health._is_clean(review_only)
      and review_only["counts"]["split_repair_queue"] == 0
      and review_only["counts"]["coverage_kind_forward_stale"] == 1
      and health.format_queue(review_only) == [],
      str(review_only["counts"]))

resolved_identity = {
    "ticker_count": 1,
    "dup_conid": {},
    "quarantine_reuse": [["GOOD", 555]],
    "quarantine_reuse_resolved": [{
        "ticker": "GOOD", "conid": 555, "resolution": "resolved",
        "active_manifest": {"sha256": "a" * 64},
        "quarantine_manifests": [{"sha256": "b" * 64}],
        "quarantine_reasons": [{"sha256": "c" * 64}],
        "corrections": [{
            "type": "identity_listing_truncation",
            "run": "fixture", "cutover": "2020-01-02",
            "snapshot": "fixture-snapshot",
        }],
        "intervals": [{
            "interval": "1m", "first_day": "2020-01-02",
            "identity_floor": "2020-01-02", "on_or_after_floor": True,
        }],
        "errors": [],
    }],
    "quarantine_reuse_unresolved": [],
    "quarantined_conids": {555: ["OLD"]},
    "errors": [],
}
resolved_health = {**review_only, "identity": resolved_identity}
resolved_health["counts"] = health.summary_counts(resolved_health)
check("health identity: raw resolved reuse stays visible but clean and off queue",
      resolved_health["counts"]["identity_quarantine_reuse"] == 1
      and resolved_health["counts"]["identity_quarantine_reuse_resolved"] == 1
      and resolved_health["counts"]["identity_quarantine_reuse_unresolved"] == 0
      and health._is_clean(resolved_health)
      and health.format_queue(resolved_health) == []
      and "1 raw quarantined-conId reuse (1 resolved, 0 unresolved)" in
      "\n".join(health.summarize_report(resolved_health)),
      str(resolved_health["counts"]))

legacy_identity_health = {
    **review_only,
    "identity": {
        "ticker_count": 1, "dup_conid": {},
        "quarantine_reuse": [["LEGACY", 999]],
        "quarantined_conids": {999: ["OLD"]}, "errors": [],
    },
}
legacy_identity_health["counts"] = health.summary_counts(
    legacy_identity_health)
check("health identity: legacy raw reuse remains unresolved and dirty",
      legacy_identity_health["counts"][
          "identity_quarantine_reuse_unresolved"] == 1
      and not health._is_clean(legacy_identity_health)
      and health.format_queue(legacy_identity_health) == ["LEGACY"],
      str(legacy_identity_health["counts"]))

legacy_cached_health = dict(legacy_identity_health)
legacy_cached_health["counts"] = dict(review_only["counts"])
legacy_cached_health["counts"]["identity_quarantine_reuse"] = 1
legacy_cached_health["counts"].pop(
    "identity_quarantine_reuse_resolved", None)
legacy_cached_health["counts"].pop(
    "identity_quarantine_reuse_unresolved", None)
check("health identity: cached pre-v2 counts fail closed and remain renderable",
      not health._is_clean(legacy_cached_health)
      and "0 resolved, 1 unresolved" in
      "\n".join(health.summarize_report(legacy_cached_health)),
      str(health._report_counts(legacy_cached_health)))

gap_row = {
    "ticker": "TMUS", "interval": "1m", "asof": "GAPS",
    "current": True, "missing_total": 0, "gap_events": 0, "days": 0,
    "missing_days": 610,
    "missing_day_list": [f"2024-01-{day:02d}" for day in range(1, 11)],
    "missing_day_runs": [{
        "count": 610, "start": "2013-05-01", "end": "2015-09-30"}],
    "largest_missing_day_run": {
        "count": 610, "start": "2013-05-01", "end": "2015-09-30"},
    "source_absent": 18, "source_absent_list": [],
    "ignored": 0, "ignored_list": [],
    "interval_fingerprint": {"current": True},
}
gap_health = {
    **review_only,
    "gaps": {
        "kind": "gap_evidence_evaluation", "expected_series": 1,
        "current_series": 1, "rows": [gap_row], "fillable": [gap_row],
        "source_absent": [gap_row], "interior": [], "ignored": [],
        "unavailable": [], "errors": [],
    },
}
gap_health["counts"] = health.summary_counts(gap_health)
check("health gaps: current fillable missing days queue ticker and dirty health",
      "TMUS" in health.format_queue(gap_health)
      and not health._is_clean(gap_health)
      and gap_health["counts"]["gap_fillable_days"] == 610,
      str(gap_health["counts"]))
check("health gaps: summary prints per-series count and largest session run",
      any("TMUS 1m (610d, largest=610)" in line
          for line in health.summarize_report(gap_health)),
      str(health.summarize_report(gap_health)))

absent_row = {**gap_row, "missing_days": 0, "missing_day_list": [],
              "missing_day_runs": [], "largest_missing_day_run": {
                  "count": 0, "start": None, "end": None}}
absent_only = {
    **review_only,
    "gaps": {
        "kind": "gap_evidence_evaluation", "expected_series": 1,
        "current_series": 1, "rows": [absent_row], "fillable": [],
        "source_absent": [absent_row], "interior": [], "ignored": [],
        "unavailable": [], "errors": [],
    },
}
absent_only["counts"] = health.summary_counts(absent_only)
check("health gaps: source-absent days stay visible but do not queue or dirty",
      health._is_clean(absent_only) and health.format_queue(absent_only) == []
      and absent_only["counts"]["gap_source_absent_days"] == 18,
      str(absent_only["counts"]))

unavailable_only = {
    **review_only,
    "gaps": {
        "kind": "gap_evidence_evaluation", "expected_series": 1,
        "current_series": 0, "rows": [], "fillable": [],
        "source_absent": [], "interior": [], "ignored": [],
        "unavailable": [{"ticker": "TMUS", "interval": "1m",
                         "state": "stale", "reason": "fixture repair"}],
        "errors": [],
    },
}
unavailable_only["counts"] = health.summary_counts(unavailable_only)
check("health gaps: stale evidence blocks clean without auto-queueing ticker",
      not health._is_clean(unavailable_only)
      and health.format_queue(unavailable_only) == [],
      str(unavailable_only["counts"]))
source_race = health._source_state(
    {"sha256": "a" * 64, "manifest_count": 1}, None,
    {"sha256": "b" * 64, "manifest_count": 1}, None)
check("health freshness: manifest-state race is explicit and non-current",
      source_race["current"] is False
      and "changed during health audit" in source_race["error"],
      str(source_race))
raced_queue = {**gap_health, "source_state": source_race}
raced_queue["counts"] = health.summary_counts(raced_queue)
check("health freshness: non-current v2 source suppresses direct repair queue",
      health.format_queue(raced_queue) == [] and not health._is_clean(raced_queue),
      str((health.format_queue(raced_queue), raced_queue["counts"])))
invalid_state, invalid_state_error = health._capture_bank_state(
    root, state_fn=lambda _root: {
        "schema_version": health.ss.BANK_STATE_FINGERPRINT_VERSION,
        "algorithm": "sha256", "sha256": "Z" * 64, "manifest_count": 1,
    })
check("health freshness: malformed bank-state digest fails closed",
      invalid_state is None and "invalid contract" in invalid_state_error,
      str((invalid_state, invalid_state_error)))

network_calls = []
original_fetch = health.triage_classifier.sv.fetch_daily_reference


def forbidden_fetch(*_args, **_kwargs):
    network_calls.append(1)
    raise AssertionError("offline health called external reference")


health.triage_classifier.sv.fetch_daily_reference = forbidden_fetch
try:
    offline_fallback = health.audit(
        triage_root, project_root=triage_project, asof="OFFLINE",
        triage_flags=[{"ticker": "NONE", "boundary": "2020-04-01"}],
        split_audit_fn=split_audit_fixture)
finally:
    health.triage_classifier.sv.fetch_daily_reference = original_fetch
check("health splits: unmatched triage context fails closed without network",
      not network_calls
      and offline_fallback["triage"]["classified"][0]["verdict"]
      == "UNVERIFIABLE",
      str(offline_fallback["triage"]))

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
