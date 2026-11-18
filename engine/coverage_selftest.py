"""Self-tests for coverage_audit.py.

Standalone, no network. Run:
    python engine/coverage_selftest.py
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import coverage_audit as cov  # noqa: E402
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


def seed(root, ticker, first, last, earliest=None, interval="1m",
         identity_cutover=None, identity_type="identity_truncation",
         identity_intervals=None):
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.load_manifest(tdir) or ss.new_manifest(ticker, ticker)
    months = ss.manifest_months(man, interval)
    for ym in month_iter(first, last):
        months[ym] = {"status": "present", "rows": 1}
    if identity_cutover is not None:
        correction = {
            "type": identity_type,
            "ticker": ticker,
            "cutover": identity_cutover,
        }
        if identity_intervals is not None:
            correction["intervals"] = list(identity_intervals)
        man["data_corrections"] = [correction]
    ss.save_manifest(tdir, man)
    if earliest is not None:
        ep = Path(root) / "_ibkr_earliest.json"
        try:
            data = json.loads(ep.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        data[ticker] = earliest
        ep.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")


root = Path(tempfile.mkdtemp(prefix="coverage_st_")) / ss.STORAGE_DIR_NAME
root.mkdir(parents=True)

# Five baseline-depth tickers make the upper-median baseline 2011-06. They may
# have much earlier IBKR history, but that is a policy-depth info item, not a
# front-short flag.
for t in ("AAA", "BBB", "CCC", "DDD", "EEE"):
    seed(root, t, "2011-06", "2026-07", earliest="1980-01-02")

# ODFL signature: stored far later than the bank baseline and IBKR has much
# earlier bars.
seed(root, "ODFL", "2024-05", "2026-07", earliest="1991-10-01")

# Recent IPO: short stored range, but earliest is close to stored_first.
seed(root, "IPO", "2025-01", "2026-07", earliest="2025-01-15")

# Forward-stale: old tail, but not front-short.
seed(root, "HLT", "2011-06", "2016-12", earliest="2010-10-21")

# Missing earliest metadata: unknown, not front-short.
seed(root, "MISS", "2011-06", "2026-07", earliest=None)

# Fallback to 1d when 1m is absent.
seed(root, "DAILY", "2024-01", "2026-07", earliest="2024-01-01",
     interval="1d")

# Intentional identity truncation: source can serve predecessor history, but
# coverage begins at the replacement identity's durable cutover by design.
seed(root, "WBD", "2022-04", "2026-07", earliest="2011-07-01",
     identity_cutover="2022-04-11", identity_intervals=["1m"])

# Row 47 listing-wide vocabulary plus the historical plain truncation alias.
# Both are intentional identity floors, not missing predecessor history.
seed(root, "TKO", "2023-09", "2026-07", earliest="1999-10-19",
     identity_cutover="2023-09-12",
     identity_type="identity_listing_truncation")
seed(root, "PLAIN", "2020-05", "2026-07", earliest="1990-01-02",
     identity_cutover="2020-05-18", identity_type="truncation",
     identity_intervals=["1m"])

# A malformed recognized identity record must fail closed as an audit error,
# never evaporate into a front-short recommendation.
seed(root, "BADFLOOR", "2023-01", "2026-07", earliest="1990-01-02",
     identity_cutover="not-a-date",
     identity_type="identity_listing_truncation")

# Per-kind coverage uses exact 1m as its comparison frontier. One daily kind
# is exactly at the stale boundary; its sibling is within tolerance.
seed(root, "KINDS", "2011-06", "2026-07", earliest="1980-01-02")
seed(root, "KINDS", "2026-05", "2026-05", interval="1d")
seed(root, "KINDS", "2026-06", "2026-06", interval="1d-hvol")

# An established non-primary kind without exact 1m is visible but unevaluated.
seed(root, "NOPRIM", "2026-01", "2026-07", interval="1m-iv")

report = cov.audit(root, write=True, asof="TEST")
fwd = {r["ticker"] for r in report["forward_stale"]}
front = {r["ticker"] for r in report["front_short"]}
unknown = set(report["unknown"])
info = {r["ticker"] for r in report["info"]}
kind_stale = {
    (r["ticker"], r["kind"]): r
    for r in report["kind_forward_stale"]
}
kind_unevaluated = {
    (r["ticker"], r["kind"])
    for r in report["kind_unevaluated"]
}

check("coverage: synthetic baseline is computed from stored starts",
      report["baseline_start"] == "2011-06", str(report["baseline_start"]))
check("coverage: bank_latest is max stored_last",
      report["bank_latest"] == "2026-07", str(report["bank_latest"]))
check("coverage: stale tail flags forward-stale",
      fwd == {"HLT"}, str(fwd))
check("coverage: ODFL-like short front flags front-short",
      front == {"ODFL"}, str(front))
check("coverage: baseline-depth earlier availability is info only",
      {"AAA", "BBB", "CCC", "DDD", "EEE"}.issubset(info)
      and not {"AAA", "BBB", "CCC", "DDD", "EEE"} & front)
check("coverage: recent IPO is not front-short",
      "IPO" not in front and "IPO" not in fwd, str(report["summary"]["IPO"]))
check("coverage: missing earliest is unknown, not front-short",
      unknown == {"MISS"} and "MISS" not in front, str(unknown))
check("coverage: falls back to daily coverage when 1m is absent",
      report["summary"]["DAILY"]["interval"] == "1d",
      str(report["summary"]["DAILY"]))
check("coverage: identity truncation is not a front-short backfill defect",
      "WBD" not in front
      and report["summary"]["WBD"]["coverage_floor"] == "2022-04"
      and report["summary"]["WBD"]["effective_earliest"] == "2022-04",
      str(report["summary"]["WBD"]))
check("coverage: all identity correction vocabulary suppresses predecessor refetch",
      not {"TKO", "PLAIN"} & front
      and report["summary"]["TKO"]["coverage_floor"] == "2023-09"
      and report["summary"]["TKO"]["effective_earliest"] == "2023-09"
      and report["summary"]["PLAIN"]["coverage_floor"] == "2020-05"
      and report["summary"]["PLAIN"]["effective_earliest"] == "2020-05",
      repr({
          ticker: report["summary"].get(ticker)
          for ticker in ("TKO", "PLAIN")
      }))
check("coverage: malformed identity correction fails closed as an audit error",
      "BADFLOOR" not in report["summary"]
      and any(
          row.get("ticker") == "BADFLOOR"
          and "identity floor invalid" in str(row.get("error") or "")
          for row in report["errors"]),
      repr(report["errors"]))

scope_manifest = {
    "data_corrections": [{
        "type": "identity_truncation",
        "ticker": "SCOPE",
        "cutover": "2021-02-03",
        "intervals": ["1d"],
    }],
}
check("coverage: scoped identity floor matches exact and base-family tokens only",
      cov._identity_coverage_floor(
          scope_manifest, "1d", ticker="SCOPE") == "2021-02"
      and cov._identity_coverage_floor(
          scope_manifest, "1d-hvol", ticker="SCOPE") == "2021-02"
      and cov._identity_coverage_floor(
          scope_manifest, "1m", ticker="SCOPE") is None)
check("coverage: v2 report declares offline report-only contract",
      report["version"] == cov.REPORT_VERSION == 2
      and report["report_only"] is True
      and report["network"] is False)
check("coverage: exact two-month kind lag is surfaced",
      kind_stale[("KINDS", "1d")]["lag_months"] == 2
      and ("KINDS", "1d-hvol") not in kind_stale,
      str(report["kind_forward_stale"]))
check("coverage: no-primary kinds are unevaluated, not stale",
      {("DAILY", "1d"), ("NOPRIM", "1m-iv")}.issubset(
          kind_unevaluated)
      and ("NOPRIM", "1m-iv") not in kind_stale,
      str(report["kind_unevaluated"]))
check("coverage: report sidecar is written and loadable",
      Path(report["report_path"]).exists()
      and cov.load_report(root).get("kind") == "coverage_audit")
check("coverage: re-fetch queue is forward plus front",
      cov.format_queue(report) == ["HLT", "ODFL"], str(cov.format_queue(report)))
check("coverage: summary includes bounded per-kind visibility",
      any(line.startswith("Per-kind staleness: 1 forward-stale")
          and "KINDS 1d" in line
          for line in cov.summarize_report(report)),
      str(cov.summarize_report(report)))
try:
    cov.month_ord("2026-13")
except ValueError:
    invalid_month_rejected = True
else:
    invalid_month_rejected = False
check("coverage: invalid calendar months are rejected",
      invalid_month_rejected)

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
