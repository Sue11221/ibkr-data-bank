"""Offline self-tests for wbd_truncate.py."""

import datetime as dt
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss  # noqa: E402
import wbd_truncate as wt  # noqa: E402


FAILS = []
N = [0]


def check(name, condition, detail=""):
    N[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILS.append(name)


def _bar(day, interval, close=10.0):
    stamp = dt.datetime.combine(
        day, dt.time(9, 30) if interval == "1m" else dt.time())
    return (stamp, close, close + 1, close - 1, close, 100)


def seed(root):
    ticker = "WBD"
    manifest = ss.new_manifest(ticker, ticker)
    for interval in wt.DEFAULT_INTERVALS:
        months = ss.manifest_months(manifest, interval)
        for month, bars in (
                ("2022-03", [_bar(dt.date(2022, 3, 31), interval)]),
                ("2022-04", [
                    _bar(dt.date(2022, 4, 1), interval),
                    _bar(dt.date(2022, 4, 11), interval, 11.0),
                    _bar(dt.date(2022, 4, 12), interval, 12.0),
                ])):
            year, mon = int(month[:4]), int(month[5:7])
            path = ss.month_file_path(root, ticker, year, mon, interval)
            path.parent.mkdir(parents=True, exist_ok=True)
            stats = ss.write_month_file(path, bars, verify_after_write=True)
            months[month] = {
                **stats,
                "status": "present",
                "source": "selftest",
            }
        manifest["intervals"][interval]["verified_absent"] = [
            "2022-03-15", "2022-04-04", "2022-04-12"]
    ss.save_manifest(Path(root) / ticker, manifest)


def identity_gate(_root, ticker, boundary, ref_fn=None):
    return {
        "ticker": ticker,
        "boundary": str(boundary),
        "verdict": "IDENTITY_BASIS",
        "evidence": {"fixture": True},
    }


def applied_fixture(label):
    fixture_base = Path(tempfile.mkdtemp(prefix=f"wbd_{label}_st_"))
    fixture_root = fixture_base / ss.STORAGE_DIR_NAME
    fixture_root.mkdir()
    seed(fixture_root)
    wt.apply_truncation(
        fixture_root,
        dry_run=False,
        triage_fn=identity_gate,
        snapshot_root=fixture_base / "original-snapshot",
        asof="TEST")
    return fixture_base, fixture_root


def inject_pre_cutover(root, intervals):
    manifest = ss.load_manifest(Path(root) / "WBD")
    for interval in intervals:
        path = (ss.find_month_file(root, "WBD", 2022, 4, interval)
                or ss.month_file_path(root, "WBD", 2022, 4, interval))
        bars, _meta = ss.read_month_file(path)
        bars = sorted(
            [_bar(dt.date(2022, 4, 1), interval, 9.0), *bars],
            key=lambda bar: bar[0])
        stats = ss.write_month_file(path, bars, verify_after_write=True)
        entry = ss.manifest_months(manifest, interval)["2022-04"]
        source = entry.get("source")
        corrections = list(entry.get("corrections") or [])
        entry.update(stats)
        entry["status"] = "present"
        if source is not None:
            entry["source"] = source
        if corrections:
            entry["corrections"] = corrections
    ss.save_manifest(Path(root) / "WBD", manifest)


def tree_bytes(root):
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(Path(root).rglob("*"))
        if path.is_file()
    }


base = Path(tempfile.mkdtemp(prefix="wbd_truncate_st_"))
root = base / ss.STORAGE_DIR_NAME
root.mkdir()
seed(root)

before_manifest = (root / "WBD" / ss.MANIFEST_NAME).read_bytes()
dry = wt.apply_truncation(
    root, dry_run=True, triage_fn=identity_gate, asof="TEST")
check("truncate: dry-run finds all pre-cutover rows",
      dry["affected_file_count"] == 6 and dry["affected_bars"] == 6,
      str((dry["affected_file_count"], dry["affected_bars"])))
check("truncate: dry-run performs no writes",
      (root / "WBD" / ss.MANIFEST_NAME).read_bytes() == before_manifest
      and not (base / "snapshot").exists())

try:
    wt.apply_truncation(
        root,
        dry_run=False,
        triage_fn=lambda *_args, **_kwargs: {"verdict": "REAL"},
        snapshot_root=base / "rejected")
except RuntimeError as exc:
    rejected = "not IDENTITY_BASIS" in str(exc)
else:
    rejected = False
check("truncate: non-identity triage verdict refuses the write", rejected)
check("truncate: refused write leaves manifest unchanged",
      (root / "WBD" / ss.MANIFEST_NAME).read_bytes() == before_manifest)

result = wt.apply_truncation(
    root,
    dry_run=False,
    triage_fn=identity_gate,
    snapshot_root=base / "snapshot",
    asof="TEST")
check("truncate: snapshot includes six months plus manifest",
      result["snapshot_file_count"] == 7,
      str(result["snapshot_file_count"]))
check("truncate: whole months removed and boundary months rewritten",
      sum(bool(row.get("removed")) for row in result["applied_files"]) == 3
      and sum("after_rows" in row for row in result["applied_files"]) == 3)
check("truncate: empty source-month directories are pruned",
      any(path.endswith("03-Mar") for path in result["pruned_empty_dirs"]),
      str(result["pruned_empty_dirs"]))
check("truncate: retained series begin at the cutover",
      result["verification"]["ok"]
      and set(result["verification"]["first_dates"].values())
      == {wt.DEFAULT_CUTOVER.isoformat()},
      str(result["verification"]))

manifest = ss.load_manifest(root / "WBD")
absent_ok = all(
    set(manifest["intervals"][interval]["verified_absent"])
    == {"2022-04-12"}
    for interval in wt.DEFAULT_INTERVALS)
check("truncate: pre-cutover verified-absent dates are trimmed", absent_ok)
check("truncate: durable correction note is recorded",
      (manifest.get("data_corrections") or [{}])[0].get("type")
      == wt.CORRECTION_TYPE)

already = wt.apply_truncation(
    root,
    dry_run=False,
    triage_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("triage must not run after a verified apply")))
check("truncate: repeated apply is a verified no-op",
      already["already_applied"] and already["verification"]["ok"],
      str(already))


# A mechanically valid correction note with stale bars in every interval must
# fail before triage, snapshot creation, or mutation.
partial_base, partial_root = applied_fixture("partial_all")
inject_pre_cutover(partial_root, wt.DEFAULT_INTERVALS)
partial_plan = wt.plan(partial_root)
check("truncate: partial-state plan sees one rewrite per interval",
      sorted((row["interval"], row["action"], row["dropped_bars"])
             for row in partial_plan)
      == [("1d", "rewrite", 1), ("1d-hvol", "rewrite", 1),
          ("1m", "rewrite", 1)],
      str(partial_plan))
partial_verify = wt.verify_state(partial_root)
check("truncate: partial-state verification names every stale interval",
      not partial_verify["ok"]
      and all(any(error.startswith(f"{interval}:")
                  for error in partial_verify["errors"])
              for interval in wt.DEFAULT_INTERVALS),
      str(partial_verify))
check("truncate: bar-granular daily predicate rejects false RESOLVED",
      wt.triage_classifier._resolved_identity_truncation(
          partial_root, "WBD", wt.DEFAULT_BOUNDARY) is None)

partial_before = tree_bytes(partial_root)
partial_snapshot = partial_base / "guard-snapshot"
partial_triage_calls = []


def partial_triage(*_args, **_kwargs):
    partial_triage_calls.append(True)
    return {"verdict": "REAL"}


try:
    wt.apply_truncation(
        partial_root,
        dry_run=False,
        triage_fn=partial_triage,
        snapshot_root=partial_snapshot)
except RuntimeError as exc:
    partial_guarded = wt.INCONSISTENT_CORRECTION_ERROR in str(exc)
else:
    partial_guarded = False
check("truncate: existing note plus stale bars raises specific guard",
      partial_guarded)
check("truncate: all-interval guard runs before triage and snapshot",
      not partial_triage_calls and not partial_snapshot.exists())
check("truncate: refused partial-state recovery is byte-for-byte read-only",
      tree_bytes(partial_root) == partial_before)


# Triage intentionally inspects daily bars only. A 1m-only inconsistency may
# retain a RESOLVED label, but apply_truncation's all-interval plan still guards.
minute_base, minute_root = applied_fixture("partial_1m")
inject_pre_cutover(minute_root, ("1m",))
minute_label = wt.triage_classifier.classify_flag(
    minute_root, "WBD", wt.DEFAULT_BOUNDARY,
    ref_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("clean daily correction label must not fetch")))
check("truncate: 1m-only inconsistency keeps documented daily RESOLVED label",
      minute_label["verdict"] == "RESOLVED", str(minute_label))
minute_plan = wt.plan(minute_root)
check("truncate: all-interval plan still sees the 1m-only stale bar",
      len(minute_plan) == 1
      and minute_plan[0]["interval"] == "1m"
      and minute_plan[0]["action"] == "rewrite", str(minute_plan))

minute_before = tree_bytes(minute_root)
minute_snapshot = minute_base / "guard-snapshot"
minute_triage_calls = []


def minute_triage(*_args, **_kwargs):
    minute_triage_calls.append(True)
    return {"verdict": "REAL"}


try:
    wt.apply_truncation(
        minute_root,
        dry_run=False,
        triage_fn=minute_triage,
        snapshot_root=minute_snapshot)
except RuntimeError as exc:
    minute_guarded = wt.INCONSISTENT_CORRECTION_ERROR in str(exc)
else:
    minute_guarded = False
check("truncate: 1m-only inconsistency is blocked by all-interval guard",
      minute_guarded and not minute_triage_calls, str(minute_triage_calls))
check("truncate: 1m-only refusal creates no snapshot and changes no bytes",
      not minute_snapshot.exists() and tree_bytes(minute_root) == minute_before)

print(f"\n{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    print("FAILED:", ", ".join(FAILS))
    sys.exit(1)
print("ALL PASS")
