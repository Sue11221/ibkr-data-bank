"""Offline regression checks for live_repair_gaps' bank merge seam."""

import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import live_repair_gaps as repair  # noqa: E402
import operation_gate  # noqa: E402
import stock_storage as ss  # noqa: E402
import vol_value_audit as audit  # noqa: E402


FAILS = []
CHECKS = [0]


def check(name, condition, detail=""):
    CHECKS[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}")
    if not condition:
        FAILS.append(f"{name}: {detail}")


def price_bar(year, month, day, value):
    ts = datetime(year, month, day, 9, 30)
    return (ts, value, value + 0.1, value - 0.1, value + 0.05, 10)


def ratio_bar(year, month, day, value):
    ts = datetime(year, month, day, 16, 0)
    return (ts, value, value + 0.01, value - 0.01, value + 0.005, 0)


def main():
    base = Path(tempfile.mkdtemp(prefix="repair_merge_st_"))
    try:
        bank = base / "bank"
        temp = base / "fresh"
        ticker = "RPR"

        iv_path = ss.month_file_path(bank, ticker, 2024, 5, "1d-iv")
        iv_stats = ss.write_month_file(
            iv_path, [ratio_bar(2024, 5, 6, 0.25)])
        manifest = ss.new_manifest(ticker, ticker)
        manifest["conid"] = 12345
        ss.manifest_months(manifest, "1d-iv")["2024-05"] = dict(
            iv_stats, status="present", source="selftest")
        ss.save_manifest(bank / ticker, manifest)

        ss.write_month_file(
            ss.month_file_path(temp, ticker, 2024, 6, "1m"),
            [price_bar(2024, 6, 3, 10.0)])
        ss.write_month_file(
            ss.month_file_path(temp, ticker, 2024, 7, "1m"),
            [price_bar(2024, 7, 1, 11.0)])

        lock_path = repair._market_operation_path(bank)
        before_busy = {
            "manifest": (bank / ticker / ss.MANIFEST_NAME).read_bytes(),
            "ratio": iv_path.read_bytes(),
        }
        blocker = operation_gate.acquire(
            "vol_value_audit", owner="repair merge overlap selftest",
            path=lock_path)
        busy_error = None
        try:
            try:
                repair._merge_temp_months(
                    bank, temp, ticker, conid=12345)
            except operation_gate.OperationBusy as exc:
                busy_error = exc
        finally:
            blocker.release()
        check("busy audit lease refuses merge before production writes",
              busy_error is not None
              and (bank / ticker / ss.MANIFEST_NAME).read_bytes()
              == before_busy["manifest"]
              and iv_path.read_bytes() == before_busy["ratio"]
              and ss.find_month_file(
                  bank, ticker, 2024, 6, "1m") is None,
              repr(busy_error))

        real_commit = repair.sk._commit_month
        passed_mstates = []
        audit_refusals = []
        correction = [{
            "day": "2024-05-06", "request_id": "interleaved-correction",
            "stored": 0.25, "served": 0.25,
        }]

        def interleaved_commit(*args, **kwargs):
            passed_mstates.append(kwargs.get("mstate"))
            try:
                audit.audit(bank, write_queue=True)
            except audit.VolValueAuditError as exc:
                audit_refusals.append(("before", str(exc)))
            else:
                audit_refusals.append(("before", "NOT REFUSED"))
            if len(passed_mstates) == 2:
                # Model a correction that lands after the first repair month.
                # The second repair commit must reload this fresh manifest,
                # not publish a manifest object retained from month one.
                fresh = ss.load_manifest(bank / ticker)
                fresh["intervals"]["1d-iv"]["months"]["2024-05"][
                    "value_corrections"] = correction
                ss.save_manifest(bank / ticker, fresh)
            result = real_commit(*args, **kwargs)
            try:
                audit.audit(bank, write_queue=True)
            except audit.VolValueAuditError as exc:
                audit_refusals.append(("after", str(exc)))
            else:
                audit_refusals.append(("after", "NOT REFUSED"))
            return result

        repair.sk._commit_month = interleaved_commit
        try:
            n, result = repair._merge_temp_months(
                bank, temp, ticker, conid=12345)
        finally:
            repair.sk._commit_month = real_commit

        final = ss.load_manifest(bank / ticker)
        final_iv = final["intervals"]["1d-iv"]["months"]["2024-05"]
        final_1m = final["intervals"]["1m"]["months"]
        check("each repair month disables retained manifest state",
              passed_mstates == [None, None], str(passed_mstates))
        check("writing audit is excluded for the complete merge phase",
              [phase for phase, _error in audit_refusals]
              == ["before", "after", "before", "after"]
              and all("refused" in error
                      for _phase, error in audit_refusals),
              repr(audit_refusals))
        check("interleaved correction evidence survives later repair save",
              final_iv.get("value_corrections") == correction,
              str(final_iv))
        check("both temp months commit through the ordinary writer",
              n == 2 and result["written"] == 2
              and set(final_1m) == {"2024-06", "2024-07"},
              f"n={n} result={result} months={sorted(final_1m)}")
        after = operation_gate.acquire(
            "vol_value_audit", owner="post-repair merge selftest",
            path=lock_path)
        after.release()
        check("successful merge releases the root-local operation lease",
              after.released
              and lock_path == audit._market_operation_path(bank),
              str(lock_path))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print(f"{CHECKS[0]} checks, {len(FAILS)} failed")
    if FAILS:
        for failure in FAILS:
            print(f"FAILED: {failure}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
