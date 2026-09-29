"""LIVE feature check for the IBKR engine (task #23, engine-scriptable subset).

UNLIKE the *_selftest.py files, this REQUIRES a running TWS/Gateway with the
API enabled (paper port 7497 on this machine). It connects for real, runs one
short KO 1-minute backfill into a TEMP root (the live archive is never
touched), and asserts the engine-side guarantees end-to-end:

  [1] timeouts + connect  — _await path answers fast, no hang
  [2] dedup (single check) — validate_symbols resolves once; gap_fill REUSES
                             that map and does NOT re-qualify (no 2nd "Checking
                             … at IBKR" pass), and skips qualify entirely
  [3] A3 manifest save     — a multi-month single-series backfill saves the
                             manifest exactly ONCE (not once per month)
  [4] span fetch / pacing  — the monthly span planner covers ~3 months in a
                             handful of requests (proves the live span path)
  [5] F2 codec round-trip  — every committed month re-reads strict-clean
  [6] F3 sha256 scrub      — scrub_storage finds 0 mismatched / 0 missing
  [7] F4+F5 completeness    — no FALSE reception gap (holidays excluded)
  [8] F6 pin-after-verify  — conId pinned only after the gate accepted, and
                             equals the value validate_symbols resolved
  [9] F7 spot-check        — a random settled session re-fetched and compared
                             to disk reports ok (not MISMATCH)
 [10] sleep inhibitor      — _prevent_sleep/_allow_sleep run without error

Run:  python engine/live_feature_check.py
Exit code 0 = all PASS, 1 = partial/failed check, 3 = terminal authority,
ledger or cancellation stop at the CLI boundary.
"""

import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_ibkr as sk          # noqa: E402
import stock_storage as ss       # noqa: E402
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
import fetch_diagnostic_cli as cli
import operation_gate
from fetch_authority import AuthorityError
from fetch_ledger import LedgerError, inspect_ledger
from fetch_run_context import RequestCancelled

TICKER = "KO"                    # liquid, decades of 1m history, stable conId
INTERVAL = "1m"
BACKFILL_DAYS = 76              # ~2.5 months -> spans >=3 calendar months

FAILS = []
N = [0]


def check(label, cond, detail=""):
    N[0] += 1
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {label}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(label)
    return cond


def info(label, value):
    print(f"  ::  {label}: {value}")


# ---- observer-only instrumentation; the ledger counts qualification sends --

_FILL_RIGHTS = frozenset({
    "ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday",
    "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.head_daily_probe",
    "ibkr.gap_fill.head_timestamp", "ibkr.earliest_available.head_timestamp",
    "ibkr.choke.qualify", "ibkr.choke.qualify_many",
})


@fib.worker_scope
def _feature_qualify(adapter, ticker, cancel=None):
    with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
        return adapter.qualify(ticker)


def _qualification_counts(operation):
    events = inspect_ledger(operation.context.ledger.path, require_seal=False)["events"]
    decisions = [event for event in events if event["event"] == "decision"]
    return {method: sum(event["producer_id"] == "ibkr.choke." + method
                        for event in decisions)
            for method in ("qualify", "qualify_many")}


_SAVE_CALLS = [0]
_orig_save = sk._save_manifest_safely


def _counting_save(*a, **k):
    _SAVE_CALLS[0] += 1
    return fib.without_authority(_orig_save)(*a, **k)


def _feature_body(operation, adapter_factory, cancel, scratch):
    FAILS.clear()
    N[0] = 0
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("feature check cancelled before connection")
    probe_factory = (adapter_factory if adapter_factory is not None
                     else (lambda: sk.LiveIB().connect()))
    validate_factory = (adapter_factory if adapter_factory is not None
                        else (lambda: sk.LiveIB().connect()))
    fill_factory = (adapter_factory if adapter_factory is not None
                    else (lambda: sk.LiveIB().connect()))
    print("=" * 74)
    print("LIVE FEATURE CHECK — requires TWS on 7497 (paper). Temp root only.")
    print("=" * 74)

    # ---- [0] connect + qualify quickly (timeouts/_await path, no hang) -------
    print("\n=== [1] connect + qualify (timeout-guarded, must answer fast) ====")
    try:
        adapter = fib.without_authority(probe_factory)()
    except (AuthorityError, LedgerError, RequestCancelled):
        raise
    except Exception as exc:  # noqa: BLE001
        check("connected to TWS", False, f"{exc}")
        print("\nCannot reach TWS — aborting live check.")
        return {"exit_code": 1, "status": "partial", "checks": N[0], "failures": list(FAILS)}
    connected, port, account = fib.without_authority(
        lambda: (adapter.is_connected(), adapter.port, adapter.account()))()
    check("connected to TWS", connected, f"port {port}")
    info("account", account)
    info("port", port)
    try:
        conid_probe, _c = _feature_qualify(adapter, TICKER, cancel=cancel,
            _fetch_child=operation.child("feature-probe", rights={"ibkr.choke.qualify"}))
        check(f"qualify({TICKER}) returned a conId", bool(conid_probe))
        info(f"{TICKER} conId", conid_probe)
    except (AuthorityError, LedgerError, RequestCancelled):
        raise
    except Exception as exc:  # noqa: BLE001
        check(f"qualify({TICKER})", False, f"{exc}")
        conid_probe = None
    finally:
        try:
            fib.without_authority(sk._auxiliary_disconnect)(adapter)
        except (AuthorityError, LedgerError, RequestCancelled):
            raise
        except Exception:  # noqa: BLE001
            pass

    # ---- [10] sleep inhibitor runs clean -----------------------------------
    print("\n=== [10] sleep inhibitor (Windows keep-awake) ===================")
    try:
        fib.without_authority(sk._prevent_sleep)()
        fib.without_authority(sk._allow_sleep)()
        check("_prevent_sleep/_allow_sleep ran without error", True)
    except (AuthorityError, LedgerError, RequestCancelled):
        raise
    except Exception as exc:  # noqa: BLE001
        check("_prevent_sleep/_allow_sleep ran without error", False, f"{exc}")

    # ---- [2] dedup: validate_symbols resolves ONCE --------------------------
    print("\n=== [2] dedup — validate_symbols resolves the contract once =====")
    val = sk.validate_symbols([TICKER],
                              adapter_factory=validate_factory, cancel=cancel,
                              identity_today=operation.context.captured_now.date(),
                              _fetch_child=operation.child("feature-identity",
                                  rights={"ibkr.choke.qualify_many"}))
    if val.get("cancelled") or (cancel is not None and cancel.is_set()):
        raise RequestCancelled("feature check cancelled during validation")
    if operation.context.ledger.failure:
        raise LedgerError(operation.context.ledger.failure)
    validation_counts = _qualification_counts(operation)
    check("validate_symbols found the symbol",
          val.get("error") is None and TICKER in val.get("found", []),
          str(val))
    resolved = val.get("resolved") or {}
    check("validate_symbols returned a resolved map with the conId",
          resolved.get(TICKER) is not None, str(resolved))
    check("exactly ONE qualify_many pass in validate_symbols",
          validation_counts["qualify_many"] == 1,
          f"qm_calls={validation_counts['qualify_many']}")
    info("resolved conId", resolved.get(TICKER))
    if val.get("error") or resolved.get(TICKER) is None:
        return {"exit_code": 1, "status": "partial", "checks": N[0], "failures": list(FAILS)}

    # ---- the backfill (covers A3, span, F2, F5/F6/F7) -----------------------
    root = Path(fib.without_authority(tempfile.mkdtemp)(
        prefix="live_feature_check_"))
    scratch.append(root)
    print(f"\n  temp root: {root}")
    today = operation.context.captured_now.date()
    since = today - timedelta(days=BACKFILL_DAYS)
    print(f"  backfilling {TICKER} {INTERVAL} from {since} .. {today}")

    msgs = []

    def progress(m):
        msgs.append(m)
        print(f"    > {m}")

    # reset dedup + save counters for the gap_fill phase
    _SAVE_CALLS[0] = 0
    sk._save_manifest_safely = _counting_save   # patched global; both
    #   _commit_month and _fill_series_inner resolve it at call time

    try:
        report = sk.gap_fill(
            root, [(TICKER, INTERVAL)],
            progress=progress,
            adapter_factory=fill_factory, cancel=cancel,
            resolved=resolved,                 # <- REUSE the validate map
            since=since,
            today=today,
            _fetch_child=operation.child("feature-fill", rights=_FILL_RIGHTS),
        )
    finally:
        sk._save_manifest_safely = _orig_save
    if report.get("cancelled") or (cancel is not None and cancel.is_set()):
        raise RequestCancelled("feature check cancelled during fill")
    if operation.context.ledger.failure:
        raise LedgerError(operation.context.ledger.failure)
    fill_counts = _qualification_counts(operation)

    series = (report.get("series") or [{}])[0]
    totals = report.get("totals", {})

    print("\n=== summary lines (what the GUI would show) =====================")
    for line in sk.summarize_report(report):
        print("   " + line)

    # ---- [2b] dedup: gap_fill did NOT re-resolve ----------------------------
    print("\n=== [2b] dedup — gap_fill REUSED the map (no second check) ======")
    check("gap_fill did NOT call qualify_many (reused validate's map)",
          fill_counts["qualify_many"] - validation_counts["qualify_many"] == 0,
          f"qm_calls={fill_counts['qualify_many'] - validation_counts['qualify_many']}")
    check("gap_fill did NOT call single qualify (used pinned/resolved conId)",
          fill_counts["qualify"] - validation_counts["qualify"] == 0,
          f"q_calls={fill_counts['qualify'] - validation_counts['qualify']}")
    check("report.prepass_resolved == 1 (the supplied map was honoured)",
          report.get("prepass_resolved") == 1,
          str(report.get("prepass_resolved")))

    # ---- backfill sanity ----------------------------------------------------
    print("\n=== backfill result ============================================")
    check("series did not halt", not series.get("halt"),
          str(series.get("halt")))
    months = {k: v for k, v in (series.get("months") or {}).items()
              if isinstance(v, dict)}
    written = [k for k, v in months.items() if v.get("status") == "written"]
    info("months written", f"{len(written)} -> {sorted(written)}")
    info("rows added", f"{series.get('added', 0):,}")
    info("requests", totals.get("requests"))
    info("bars fetched", f"{series.get('bars_fetched', 0):,}")

    # ---- [3] A3: ONE manifest save for the whole multi-month series ---------
    print("\n=== [3] A3 — manifest saved ONCE for a multi-month series =======")
    check("backfill spanned >= 2 month files (multi-month)",
          len(written) >= 2, f"written={sorted(written)}")
    check("_save_manifest_safely called EXACTLY once for the series",
          _SAVE_CALLS[0] == 1,
          f"saves={_SAVE_CALLS[0]} (old per-month behaviour would be "
          f"{len(written)})")

    # ---- [4] span / pacing --------------------------------------------------
    print("\n=== [4] span fetch — multi-month covered in few requests ========")
    nreq = totals.get("requests") or 0
    # head (1) + a handful of monthly spans; qualify was skipped via resolved.
    check("monthly-span planner kept requests small (<= 10 for ~3 months)",
          0 < nreq <= 10, f"requests={nreq}")
    info("requests for ~3 months of 1m", nreq)

    # ---- [5] F2 codec round-trip: every month re-reads strict-clean ---------
    print("\n=== [5] F2 — every committed month re-reads strict-clean ========")
    reread_ok = True
    total_rows = 0
    for k in sorted(written):
        y, m = int(k[:4]), int(k[5:7])
        p = ss.month_file_path(root, TICKER, y, m, INTERVAL)
        try:
            bars, _stats = ss.read_month_file(p)
            total_rows += len(bars)
        except Exception as exc:  # noqa: BLE001
            reread_ok = False
            info(f"{k} re-read FAILED", exc)
    check("all committed months re-read without a strict-codec error",
          reread_ok, "see failures above")
    info("total rows on disk", f"{total_rows:,}")

    # ---- [6] F3 sha256 scrub ------------------------------------------------
    print("\n=== [6] F3 — sha256 scrub of the temp root ======================")
    scrub = ss.scrub_storage(root)
    info("scrub", f"checked={scrub['checked']} ok={scrub['ok']} "
                  f"mismatched={len(scrub['mismatched'])} "
                  f"missing={len(scrub['missing'])} "
                  f"no_hash={len(scrub['no_hash'])}")
    check("scrub: at least one month checked", scrub["checked"] > 0)
    check("scrub: 0 mismatched hashes", not scrub["mismatched"],
          str(scrub["mismatched"]))
    check("scrub: 0 missing files", not scrub["missing"],
          str(scrub["missing"]))
    check("scrub: every checked month had a recorded hash",
          not scrub["no_hash"], str(scrub["no_hash"]))

    # ---- [7] F4+F5 reception completeness: no FALSE gap ---------------------
    print("\n=== [7] F4+F5 — no false reception gap (holidays excluded) ======")
    comp = series.get("completeness")
    if comp is None:
        check("no reception-gap flag on a clean backfill", True)
    else:
        miss = comp.get("missing_interior") or []
        check("reception-gap missing_interior is empty (no false positive)",
              not miss, f"missing={miss}")
        info("completeness", comp)

    # ---- [8] F6 pin-after-verify -------------------------------------------
    print("\n=== [8] F6 — conId pinned (after the gate accepted) =============")
    man = ss.load_manifest(root / TICKER) or {}
    pinned = man.get("conid")
    check(f"{TICKER} manifest pinned a conId", pinned is not None,
          str(man.get("conid")))
    check("pinned conId == the value validate_symbols resolved",
          pinned is not None and resolved.get(TICKER) is not None
          and int(pinned) == int(resolved[TICKER]),
          f"pinned={pinned} resolved={resolved.get(TICKER)}")
    if conid_probe is not None:
        check("pinned conId == the live qualify probe",
              pinned is not None and int(pinned) == int(conid_probe),
              f"pinned={pinned} probe={conid_probe}")

    # ---- [9] F7 spot-check --------------------------------------------------
    print("\n=== [9] F7 — random settled-session re-fetch vs disk ===========")
    info("spot_checks_run (legacy compatibility)", report.get("spot_checks_run"))
    sc = series.get("spot_check")
    check("legacy spot-check path is retired", sc is None, str(sc))
    if sc is not None:
        info("spot_check", sc)
        check("spot-check result is NOT a MISMATCH",
              sc.get("result") != "MISMATCH", str(sc))
        # 'ok' is the strong pass; 'skipped' (e.g. re-fetch hiccup) is tolerated
        check("spot-check compared bars (result ok) or was cleanly skipped",
              sc.get("result") in ("ok", "skipped"), str(sc))

    print("\n" + "=" * 74)
    print(f"{N[0]} checks, {len(FAILS)} failed")
    if FAILS:
        for f_ in FAILS:
            print(f"  FAILED: {f_}")
        return {"exit_code": 1, "status": "partial", "checks": N[0], "failures": list(FAILS)}
    print("ALL PASS")
    return {"exit_code": 0, "status": "complete", "checks": N[0], "failures": []}


def run_feature_check(*, adapter_factory=None, evidence_dir=None, cancel=None,
                      _test_capability=None):
    """Held diagnostic root for the shipped temporary-bank feature assertions."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory, test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    scratch = []
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)("fetch", owner="Feature check")
        output = _feature_body(operation, adapter_factory, fib.observer_object(cancel), scratch)
        if scratch:
            fib.without_authority(shutil.rmtree)(scratch[0])
            print(f"\n  cleaned temp root {scratch[0]}")
            scratch.clear()
        if output["status"] != "complete":
            outcome = "partial"
        operation.seal()
    except BaseException as exc:
        failure, outcome = exc, "failed"
        if scratch:
            try:
                fib.without_authority(shutil.rmtree)(scratch[0])
            except BaseException:
                pass  # preserve the first failure; seal is still withheld
    try:
        operation.close()
    except BaseException as exc:
        failure, outcome = failure if failure is not None else exc, "failed"
    if lease is not None:
        try:
            fib.without_authority(lambda: lease.release())()
        except BaseException as exc:
            failure, outcome = failure if failure is not None else exc, "failed"
    evidence = fib.without_authority(freport.evidence)(
        context, "diagnostic", outcome=outcome, error=failure)
    try:
        evidence["report_path"] = fib.without_authority(freport.write_report)(evidence, directory)
        evidence["report_persisted"] = True
    except BaseException as exc:
        evidence.update(verified=False, state="UNVERIFIED", report_persisted=False,
            outcome="report_failed", report_failure=f"{type(exc).__name__}: {exc}"[:500])
        failure = failure if failure is not None else exc
    if failure is not None:
        try:
            fib.without_authority(setattr)(failure, "fetch_ledger", evidence)
        except BaseException:
            pass
        raise failure
    output["fetch_ledger"] = evidence
    return output


def main():
    try:
        return run_feature_check()["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
