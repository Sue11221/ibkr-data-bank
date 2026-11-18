"""LIVE proof that pipeline=True == serial against REAL TWS (not FakeAdapter).

The selftests prove the pipeline with a scripted adapter; this proves the
THREADING model against the real ib_async event loop — the consumer thread
commits to disk while the MAIN thread owns the live connection. Backfills KO
1m over a SETTLED date range twice (serial, then pipeline) into two TEMP roots
and asserts the trees are byte-for-byte identical and both scrub clean.

Requires TWS on 7497. Run:  python engine/live_pipeline_check.py
Exit 0 = identical + clean, 1 = mismatch/partial, 3 = terminal authority,
ledger or cancellation stop at the CLI boundary.
"""

import shutil
import sys
import tempfile
from datetime import date
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
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # noqa: BLE001
    pass

TICKER = "KO"
INTERVAL = "1m"
TODAY = date(2026, 6, 12)        # a settled Friday -> both runs see identical
SINCE = date(2026, 5, 1)         # bars (no live partial last session)

FAILS = []
_RIGHTS = frozenset({
    "ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday",
    "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.head_daily_probe",
    "ibkr.gap_fill.head_timestamp", "ibkr.earliest_available.head_timestamp",
    "ibkr.choke.qualify", "ibkr.choke.qualify_many",
})


def check(label, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(label)


def read_tree(root):
    out = {}
    files = (list((root / TICKER).rglob(f"*_{INTERVAL}.parquet"))
             + list((root / TICKER).rglob(f"*_{INTERVAL}.csv")))
    for p in sorted(files):
        bars, _ = ss.read_month_file(p)
        out[p.stem] = bars          # stem: extension-agnostic (parquet|csv)
    return out


@fib.worker_scope
def _pipeline_fill(pipeline, resolved, root, adapter_factory=None, progress=None,
                   cancel=None):
    """One explicit fill child; never opens another diagnostic root."""
    msgs = []
    rep = sk.gap_fill(
        root, [(TICKER, INTERVAL)], cancel=cancel,
        progress=lambda m: (msgs.append(m), fib.without_authority(progress)(m)
                            if progress is not None else None),
        adapter_factory=(adapter_factory if adapter_factory is not None else
                         fib.without_authority(sk.live_adapter_factory)()),
        resolved=resolved, since=SINCE, today=TODAY, pipeline=pipeline,
        _fetch_child=fops.narrow_worker_child(
            fib.current_worker(), "pipeline-fill-" + ("pipe" if pipeline else "serial"),
            rights=_RIGHTS))
    return root, rep


def _pipeline_body(operation, adapter_factory=None, progress=None, cancel=None):
    """Shipped comparison and scrub under one admitted parent."""
    failures = []
    def check_local(label, cond, detail=""):
        print(f"[{'PASS' if cond else 'FAIL'}] {label}"
              + (f"  -- {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(label)

    print("=" * 74)
    print(f"LIVE pipeline==serial — {TICKER} {INTERVAL}  {SINCE}..{TODAY}")
    print("=" * 74)
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("pipeline check cancelled before validation")
    try:
        val = sk.validate_symbols([TICKER], adapter_factory=adapter_factory,
            cancel=cancel,
            identity_today=operation.context.captured_now.date(),
            _fetch_child=operation.child("pipeline-identity", rights={"ibkr.choke.qualify_many"}))
    except (AuthorityError, LedgerError, RequestCancelled):
        raise
    except Exception as exc:  # noqa: BLE001
        check_local("connect + resolve KO", False, str(exc))
        return {"exit_code": 1, "status": "partial", "failures": failures}
    if val.get("cancelled") or (cancel is not None and cancel.is_set()):
        raise RequestCancelled("pipeline check cancelled during validation")
    if val.get("error"):
        check_local("connect + resolve KO", False, val["error"])
        return {"exit_code": 1, "status": "partial", "failures": failures}
    resolved = val.get("resolved") or {}
    check_local("resolved KO conId", resolved.get(TICKER) is not None,
          str(resolved))
    if resolved.get(TICKER) is None:
        return {"exit_code": 1, "status": "partial", "failures": failures}
    print(f"  KO conId = {resolved.get(TICKER)}")

    scratch = Path(tempfile.mkdtemp(prefix="live_pipe_pair_"))
    failure = None
    try:
        print("\n  [1/2] SERIAL backfill…")
        rs, sr = _pipeline_fill(False, resolved, scratch / "serial",
            adapter_factory=adapter_factory, progress=progress, cancel=cancel,
            _fetch_child=operation.child("pipeline-serial", rights=_RIGHTS))
        if sr.get("cancelled") or (cancel is not None and cancel.is_set()):
            raise RequestCancelled("pipeline check cancelled during serial fill")
        if operation.context.ledger.failure:
            raise LedgerError(operation.context.ledger.failure)
        serial = (sr.get("series") or [{}])[0]
        if sr.get("aborted") or serial.get("halt"):
            raise RequestRefused(f"pipeline serial fill halted: {serial.get('halt')}")
        print("  [2/2] PIPELINE backfill…")
        rp, pr = _pipeline_fill(True, resolved, scratch / "pipeline",
            adapter_factory=adapter_factory, progress=progress, cancel=cancel,
            _fetch_child=operation.child("pipeline-parallel", rights=_RIGHTS))
        if pr.get("cancelled") or (cancel is not None and cancel.is_set()):
            raise RequestCancelled("pipeline check cancelled during parallel fill")
        if operation.context.ledger.failure:
            raise LedgerError(operation.context.ledger.failure)
        parallel = (pr.get("series") or [{}])[0]
        if pr.get("aborted") or parallel.get("halt"):
            raise RequestRefused(f"pipeline parallel fill halted: {parallel.get('halt')}")

        ssr = sr["series"][0]
        psr = pr["series"][0]
        print(f"\n  serial  : halt={ssr.get('halt')} "
              f"+{ssr.get('added', 0):,} rows / {ssr.get('written')} months / "
              f"{ssr.get('requests')} req")
        print(f"  pipeline: halt={psr.get('halt')} "
              f"+{psr.get('added', 0):,} rows / {psr.get('written')} months / "
              f"{psr.get('requests')} req")

        check_local("serial run did not halt", not ssr.get("halt"), str(ssr.get("halt")))
        check_local("pipeline run did not halt (no deadlock/error)",
                    not psr.get("halt"), str(psr.get("halt")))
        check_local("both fill reports completed",
                    not any(rep.get("cancelled") or rep.get("aborted")
                            or (rep.get("totals") or {}).get("write_failed")
                            for rep in (sr, pr)))

        ser_tree, pip_tree = read_tree(rs), read_tree(rp)
        check_local("both runs wrote >= 1 month file",
                    len(ser_tree) >= 1 and len(pip_tree) >= 1,
                    f"ser={sorted(ser_tree)} pip={sorted(pip_tree)}")
        check_local("pipeline tree is BYTE-IDENTICAL to serial (real TWS)",
                    ser_tree == pip_tree,
                    f"ser_months={sorted(ser_tree)} pip_months={sorted(pip_tree)} "
                    + ("same filenames" if set(ser_tree) == set(pip_tree)
                       else "DIFFERENT filenames"))
        total = sum(len(v) for v in pip_tree.values())
        print(f"  months={sorted(pip_tree)}  total bars={total:,}")
        check_local("report counters match (added/written/requests/bars_fetched)",
                    (ssr.get("added"), ssr.get("written"), ssr.get("requests"),
                     ssr.get("bars_fetched"))
                    == (psr.get("added"), psr.get("written"), psr.get("requests"),
                        psr.get("bars_fetched")),
                    f"ser=({ssr.get('added')},{ssr.get('written')},{ssr.get('requests')})"
                    f" pip=({psr.get('added')},{psr.get('written')},"
                    f"{psr.get('requests')})")

        for tag, root in (("serial", rs), ("pipeline", rp)):
            scrub = ss.scrub_storage(root)
            check_local(f"{tag} scrub clean (0 mismatched/missing)",
                        not scrub["mismatched"] and not scrub["missing"]
                        and scrub["checked"] >= 1,
                        f"checked={scrub['checked']} mism={scrub['mismatched']} "
                        f"miss={scrub['missing']}")

        print("\n" + "=" * 74)
        print(f"{4 + 6} checks-ish, {len(failures)} failed")
        if failures:
            for f_ in failures:
                print(f"  FAILED: {f_}")
        else:
            print("ALL PASS — pipeline is byte-identical to serial on real TWS")
        return {"exit_code": 1 if failures else 0,
                "status": "partial" if failures else "complete", "failures": failures}
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            shutil.rmtree(scratch)
        except BaseException:
            if failure is None:
                raise


def run_pipeline_check(*, adapter_factory=None, progress=None, evidence_dir=None,
                       cancel=None,
                       _test_capability=None):
    """Held diagnostic root; exact operation owns identity and both fills."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory, test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)("fetch", owner="Pipeline check")
        output = _pipeline_body(operation, adapter_factory=adapter_factory,
            progress=progress, cancel=cancel)
        if output["status"] != "complete":
            outcome = "partial"
        operation.seal()
    except BaseException as exc:
        failure, outcome = exc, "failed"
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
        return run_pipeline_check()["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
