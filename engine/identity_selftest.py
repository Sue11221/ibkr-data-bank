"""Self-tests for identity.py.

Standalone, no network. Run:
    python engine/identity_selftest.py
"""

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import identity as ident  # noqa: E402
import stock_storage as ss  # noqa: E402


FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def seed(root, ticker, conid, aliases=None, symbol=None, *, first=None,
         corrections=None, interval="1m"):
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    man = ss.new_manifest(symbol or ticker, ticker)
    man["conid"] = conid
    man["aliases"] = list(aliases or [])
    entry = {
        "status": "present",
        "rows": 1,
    }
    if first is not None:
        entry.update({
            "first": first,
            "last": first,
            "sha256": "a" * 64,
        })
    ss.manifest_months(man, interval)["2020-01"] = entry
    if corrections is not None:
        man["data_corrections"] = list(corrections)
    ss.save_manifest(tdir, man)


def listing_correction(ticker, cutover="2020-01-02"):
    return {
        "type": "identity_listing_truncation",
        "ticker": ticker,
        "cutover": cutover,
        "run": f"fixture-{ticker.lower()}-listing-floor",
        "snapshot": f"_quarantine/{ticker}-fixture/snapshot",
    }


project = Path(tempfile.mkdtemp(prefix="identity_st_")) / "proj"
root = project / ss.STORAGE_DIR_NAME
root.mkdir(parents=True)

seed(root, "AAA", 111)
seed(root, "BBB", 111)                 # duplicate conId with AAA
seed(root, "CCC", 999)                 # active on a quarantined conId
seed(root, "GOOD", 555, first="01/02/2020 09:30:00",
     corrections=[listing_correction("GOOD")])
seed(root, "DDD", 222)                 # clean
seed(root, "OLD", 333, aliases=["NEW"])
seed(root, "NEW", 333, aliases=["OLD"])  # legitimate rename, not duplicate
seed(project / "_quarantine", "ZZZ", 999)
seed(project / "_quarantine", "GOOD-OLD", 555)
(project / "_quarantine" / "GOOD-OLD" /
 ident.QUARANTINE_REASON_NAME).write_text(
    "historical predecessor retained for identity proof\n", encoding="utf-8")
(root / "Run Logs").mkdir()

# Invalid quarantine manifests must not poison the audit.
bad = project / "_quarantine" / "BROKEN"
bad.mkdir(parents=True)
(bad / ss.MANIFEST_NAME).write_text("{not json", encoding="utf-8")

report = ident.audit(root, project_root=project, write=True, asof="TEST")
dup = report["dup_conid"]
reuse = [tuple(item) for item in report["quarantine_reuse"]]
queue = ident.format_queue(report)
loaded = ident.load_report(root)

check("identity: duplicate conId flags non-aliased active folders",
      dup == {111: ["AAA", "BBB"]}, str(dup))
check("identity: legitimate cross-aliased rename is exempt",
      333 not in dup, str(dup))
check("identity: quarantined conId reuse flags active ticker",
      reuse == [("GOOD", 555), ("CCC", 999)], str(reuse))
check("identity: quarantine blocklist auto-derived from _quarantine",
      report["quarantined_conids"] == {555: ["GOOD-OLD"], 999: ["ZZZ"]},
      str(report["quarantined_conids"]))
check("identity: summary flags duplicate tickers",
      set(report["summary"]["AAA"]["flags"]) == {"duplicate-conid"}
      and set(report["summary"]["BBB"]["flags"]) == {"duplicate-conid"},
      str(report["summary"]))
check("identity: summary flags quarantined reuse ticker",
      report["summary"]["CCC"]["flags"] == [
          "quarantined-conid-reuse",
          "quarantined-conid-reuse-unresolved"],
      str(report["summary"]["CCC"]))
check("identity: correction-backed reuse resolves but raw signal remains",
      [(item["ticker"], item["conid"])
       for item in report["quarantine_reuse_resolved"]] == [("GOOD", 555)]
      and [(item["ticker"], item["conid"])
           for item in report["quarantine_reuse_unresolved"]]
      == [("CCC", 999)]
      and report["summary"]["GOOD"]["flags"] == [
          "quarantined-conid-reuse", "quarantined-conid-reuse-resolved"],
      str(report["quarantine_reuse_detail"]))
resolved = report["quarantine_reuse_resolved"][0]
check("identity: resolved record retains current provenance and interval proof",
      len(resolved["active_manifest"]["sha256"]) == 64
      and len(resolved["quarantine_manifests"]) == 1
      and len(resolved["quarantine_reasons"]) == 1
      and resolved["corrections"] == [{
          "type": "identity_listing_truncation",
          "run": "fixture-good-listing-floor",
          "cutover": "2020-01-02",
          "snapshot": "_quarantine/GOOD-fixture/snapshot",
      }]
      and resolved["intervals"][0]["first_day"] == "2020-01-02"
      and resolved["intervals"][0]["on_or_after_floor"] is True,
      json.dumps(resolved, sort_keys=True))
check("identity: clean ticker has no flags",
      report["summary"]["DDD"]["flags"] == [],
      str(report["summary"]["DDD"]))
check("identity: review queue is stable and unique",
      queue == ["AAA", "BBB", "CCC"], str(queue))
check("identity: non-canonical storage folders are ignored",
      report["ticker_count"] == 7, str(report["ticker_count"]))
check("identity: report sidecar is written and loadable",
      Path(report["report_path"]).exists()
      and loaded.get("kind") == "identity_audit")
check("identity: JSON sidecar preserves violation counts",
      ident.summary_counts(loaded)["duplicate_conids"] == 1
      and ident.summary_counts(loaded)["quarantine_reuse"] == 2
      and ident.summary_counts(loaded)["quarantine_reuse_resolved"] == 1
      and ident.summary_counts(loaded)["quarantine_reuse_unresolved"] == 1,
      json.dumps(loaded.get("dup_conid"), sort_keys=True))

# The verdict is always recomputed from current manifest/provenance state.
drift_project = Path(tempfile.mkdtemp(prefix="identity_drift_st_")) / "proj"
drift_root = drift_project / ss.STORAGE_DIR_NAME
drift_root.mkdir(parents=True)
seed(drift_root, "DRIFT", 777, first="01/03/2020 09:30:00",
     corrections=[listing_correction("DRIFT")])
seed(drift_project / "_quarantine", "DRIFT-OLD", 777)
reason_path = (drift_project / "_quarantine" / "DRIFT-OLD" /
               ident.QUARANTINE_REASON_NAME)
reason_path.write_text("reviewed predecessor evidence\n", encoding="utf-8")
drift_first = ident.audit(drift_root, project_root=drift_project, asof="ONE")
check("identity drift: complete current proof resolves",
      len(drift_first["quarantine_reuse_resolved"]) == 1
      and not drift_first["quarantine_reuse_unresolved"],
      str(drift_first["quarantine_reuse_unresolved"]))

drift_manifest_path = drift_root / "DRIFT" / ss.MANIFEST_NAME
drift_manifest = ss.load_manifest(drift_root / "DRIFT")
drift_manifest["intervals"]["1m"]["months"]["2020-01"]["first"] = (
    "01/01/2020 09:30:00")
ss.save_manifest(drift_root / "DRIFT", drift_manifest)
drift_second = ident.audit(drift_root, project_root=drift_project, asof="TWO")
check("identity drift: pre-floor mutation revokes resolution immediately",
      not drift_second["quarantine_reuse_resolved"]
      and len(drift_second["quarantine_reuse_unresolved"]) == 1
      and "precedes identity floor" in " ".join(
          drift_second["quarantine_reuse_unresolved"][0]["errors"]),
      str(drift_second["quarantine_reuse_unresolved"]))

drift_manifest["intervals"]["1m"]["months"]["2020-01"]["first"] = (
    "01/03/2020 09:30:00")
drift_manifest["data_corrections"] = []
ss.save_manifest(drift_root / "DRIFT", drift_manifest)
missing_correction = ident.audit(
    drift_root, project_root=drift_project, asof="THREE")
check("identity drift: missing correction fails unresolved",
      len(missing_correction["quarantine_reuse_unresolved"]) == 1
      and "no applicable identity_listing_truncation" in " ".join(
          missing_correction["quarantine_reuse_unresolved"][0]["errors"]),
      str(missing_correction["quarantine_reuse_unresolved"]))

drift_manifest["data_corrections"] = [listing_correction("DRIFT")]
ss.save_manifest(drift_root / "DRIFT", drift_manifest)
reason_path.unlink()
missing_reason = ident.audit(
    drift_root, project_root=drift_project, asof="FOUR")
check("identity drift: missing quarantine reason fails unresolved",
      len(missing_reason["quarantine_reuse_unresolved"]) == 1
      and "reason is missing" in " ".join(
          missing_reason["quarantine_reuse_unresolved"][0]["errors"]),
      str(missing_reason["quarantine_reuse_unresolved"]))

reason_path.write_text("reviewed predecessor evidence\n", encoding="utf-8")
drift_manifest["data_corrections"][0]["cutover"] = "not-a-date"
ss.save_manifest(drift_root / "DRIFT", drift_manifest)
malformed = ident.audit(drift_root, project_root=drift_project, asof="FIVE")
check("identity drift: malformed correction fails unresolved",
      len(malformed["quarantine_reuse_unresolved"]) == 1
      and "cutover is invalid" in " ".join(
          malformed["quarantine_reuse_unresolved"][0]["errors"]),
      str(malformed["quarantine_reuse_unresolved"]))

legacy = {"quarantine_reuse": [["LEGACY", 1]], "dup_conid": {}}
check("identity: legacy report stays conservatively queued",
      ident.format_queue(legacy) == ["LEGACY"]
      and ident.summary_counts(legacy)["quarantine_reuse_unresolved"] == 1,
      str(ident.summary_counts(legacy)))

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
