"""Comprehensive VALIDATION catch-up. For every ticker missing the INTERNAL and/or
COMBINED 3-source verdict: (1) ensure the 1d TRADES series is stored (the internal +
combined checks read it), (2) run the INTERNAL daily check (stored 1m vs stored 1d),
(3) run the COMBINED 3-source check (external stockanalysis ∩ internal, month-refetch
confirm). Parallel across free fleet ports.

    python live_validate_catchup.py            # only the tickers missing a verdict
    python live_validate_catchup.py --all      # re-do every ticker
"""
import argparse
import sys
import threading
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk              # noqa: E402
import stock_validate as sv          # noqa: E402
import live_combined_flags as combined # noqa: E402
import fetch_ibkr_bridge as fib       # noqa: E402
import fetch_operations as fops       # noqa: E402
import fetch_operation_report as freport # noqa: E402
import fetch_diagnostic_cli as cli  # noqa: E402
import operation_gate                 # noqa: E402
from live_internal_revalidate import _bank_root, _free_ports   # noqa: E402
from fetch_authority import AuthorityError  # noqa: E402
from fetch_ledger import LedgerError         # noqa: E402
from fetch_run_context import RequestCancelled, RequestRefused  # noqa: E402

_RIGHTS = fops._SHARED | combined._RIGHTS
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


@fib.worker_scope
def _catchup(adapter, root, ticker, today, *, cancel=None, reference_fn=None,
             _reference_opener=None):
    worker = fib.current_worker()
    if today != worker.context.captured_now.date():
        raise RequestRefused("catch-up day must be the parent's captured day")
    out = {"ticker": ticker, "store1d": "-", "internal": "-", "combined": "-"}
    daily = fib.without_authority(sv.read_series)(root, ticker, "1d")
    if not daily:                                  # (1) ensure the 1d reference
        try:
            fill = sk.gap_fill(root, [(ticker, "1d")], progress=lambda m: None,
                        cancel=cancel,
                        adapter_factory=sk.ReusableAdapter(lambda: adapter),
                        pacer=sk.Pacer(), today=today,
                        _fetch_child=fops.narrow_worker_child(
                            worker, worker.worker_id + ":daily-fill",
                            rights=fops._SHARED))
            if fill.get("cancelled"):
                raise RequestCancelled("validation catch-up daily fill cancelled")
            if fill.get("aborted"):
                raise RuntimeError("validation catch-up daily fill aborted")
            daily = fib.without_authority(sv.read_series)(root, ticker, "1d")
            out["store1d"] = "stored" if daily else "empty"
        except _TERMINAL:
            raise
        except Exception as exc:  # noqa: BLE001
            out["store1d"] = f"FAIL:{type(exc).__name__}"
    else:
        out["store1d"] = "had"
    if daily:                                      # (2) internal daily check
        try:
            ref = fib.without_authority(sv.parse_ibkr_daily_reference)(daily)
            v = fib.without_authority(sv.internal_daily_audit)(
                root, ticker, "1m", daily_ref=ref)
            v["asof"] = worker.context.captured_now.isoformat(timespec="seconds")
            if fib.without_authority(sv.record_internal_validation)(root, v) is None:
                raise LedgerError("catch-up internal verdict was not saved")
            out["internal"] = v.get("status", "?")
        except _TERMINAL:
            raise
        except Exception as exc:  # noqa: BLE001
            out["internal"] = f"FAIL:{type(exc).__name__}"
    try:                                           # (3) combined 3-source check
        v = combined._audit_ticker(adapter, root, ticker, "1m",
            require_persisted=True, reference_fn=reference_fn, cancel=cancel,
            _reference_opener=_reference_opener,
            _fetch_child=fops.narrow_worker_child(
                worker, worker.worker_id + ":combined-audit",
                rights=combined._RIGHTS))
        out["combined"] = v.get("status", "?")
    except _TERMINAL:
        raise
    except Exception as exc:  # noqa: BLE001
        out["combined"] = f"FAIL:{type(exc).__name__}"
    return out


@fib.worker_scope
def _catchup_port(port, items, root, today, stop, results, failures, lock,
                  *, adapter_factory=None, progress=None, reference_fn=None,
                  cancel=None, _reference_opener=None):
    adapter, failure = None, None
    emit = fib.without_authority(progress if progress is not None else print)
    try:
        if stop.is_set():
            return
        factory = (adapter_factory if adapter_factory is not None else
                   lambda p: sk.LiveIB(host=sk.HOST_DEFAULT, ports=(p,),
                                       client_id=sk.CLIENT_ID_FETCH).connect())
        adapter = fib.without_authority(factory)(port)
        for number, ticker in enumerate(items):
            if stop.is_set():
                break
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("validation catch-up cancelled")
            try:
                value = _catchup(adapter, root, ticker, today,
                    cancel=cancel, reference_fn=reference_fn,
                    _reference_opener=_reference_opener,
                    _fetch_child=fops.narrow_worker_child(
                        fib.current_worker(),
                        fib.current_worker().worker_id + ":" + str(number),
                        rights=_RIGHTS))
            except _TERMINAL:
                raise
            except Exception as exc:
                value = {"ticker": ticker, "store1d": "-", "internal": "-",
                         "combined": f"ERR:{type(exc).__name__}"}
            with lock:
                results.append(value)
            emit(f"  {ticker:6} 1d={value['store1d']:7} "
                 f"internal={value['internal']:12} combined={value['combined']}")
    except BaseException as exc:
        failure = exc
        stop.set()
        with lock:
            failures.append(exc)
    finally:
        if adapter is not None:
            try:
                sk._auxiliary_disconnect(adapter)
            except BaseException as exc:
                stop.set()
                if failure is None:
                    with lock:
                        failures.append(exc)


def _catchup_body(operation, root, all_tickers, ports, adapter_factory,
                  progress, reference_fn, cancel, _reference_opener=None):
    if type(all_tickers) is not bool:
        raise RequestRefused("catch-up all-tickers flag must be boolean")
    if ports is not None and (type(ports) not in (list, tuple)
            or any(type(port) is not int or not 0 < port < 65536 for port in ports)
            or len(set(ports)) != len(ports)):
        raise RequestRefused("catch-up ports must be distinct valid integers")
    if (adapter_factory is not None or reference_fn is not None
            or _reference_opener is not None) and (
            operation.context.test_capability is None):
        raise RequestRefused("injected catch-up dependencies are offline-only")
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("validation catch-up cancelled before bank scan")
    emit = fib.without_authority(progress if progress is not None else print)
    if not root.exists():
        emit(f"bank not found: {root}")
        return {"exit_code": 2, "status": "missing_bank", "results": []}
    tickers = sorted({ticker for ticker, _iv in
                      fib.without_authority(sv.discover_series)(root, rth_only=True)})
    iv_map = fib.without_authority(sv.load_internal_validation)(root)
    cf_map = fib.without_authority(sv.load_combined_flags)(root)
    todo = (tickers if all_tickers else
            [ticker for ticker in tickers
             if (ticker.upper() not in iv_map
                 or not fib.without_authority(sv.combined_entries_for_ticker)(
                     cf_map, ticker,
                     schema_version=sv.COMBINED_FLAGS_SCHEMA_VERSION))])
    if not todo:
        emit("nothing to catch up — every ticker has internal + combined verdicts.")
        return {"exit_code": 0, "status": "no_work", "results": []}
    ports = tuple(fib.without_authority(_free_ports)() if ports is None else ports)
    if not ports:
        emit("NO free fleet ports.")
        return {"exit_code": 2, "status": "no_ports", "results": []}
    if len(set(ports)) != len(ports) or any(
            type(port) is not int or not 0 < port < 65536 for port in ports):
        raise RequestRefused("catch-up ports must be distinct valid integers")
    today = operation.context.captured_now.date()
    emit(f"bank tickers={len(tickers)} | catch-up todo={len(todo)} | ports={ports}")
    by_port = {port: [] for port in ports}
    for number, ticker in enumerate(todo):
        by_port[ports[number % len(ports)]].append(ticker)
    results, failures, lock, stop = [], [], threading.Lock(), threading.Event()
    cancellation = sk._OrEvent(stop, cancel)
    started = []
    t0 = time.monotonic()

    def worker(port, child):
        try:
            _catchup_port(port, by_port[port], root, today, stop, results,
                failures, lock, adapter_factory=adapter_factory, progress=progress,
                reference_fn=reference_fn, cancel=cancellation,
                _reference_opener=_reference_opener,
                _fetch_child=child)
        except BaseException as exc:
            stop.set()
            with lock:
                failures.append(exc)

    try:
        for port in ports:
            if not by_port[port] or stop.is_set():
                continue
            child = operation.child("catchup-port-" + str(port), rights=_RIGHTS)
            try:
                thread = threading.Thread(target=worker, args=(port, child),
                    name="validate-catchup-" + str(port), daemon=True)
            except BaseException:
                child.close()
                raise
            started.append((thread, child))
            thread.start()
    except BaseException as exc:
        stop.set()
        failures.append(exc)
    finally:
        for thread, child in started:
            if thread.ident is None:
                child.close()
            else:
                thread.join()
    if failures:
        raise failures[0]
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("validation catch-up cancelled after joined ports")
    wall = time.monotonic() - t0
    tset = {ticker.upper() for ticker in tickers}
    iv2 = fib.without_authority(sv.load_internal_validation)(root)
    cf2 = fib.without_authority(sv.load_combined_flags)(root)
    combined_count = sum(bool(fib.without_authority(
        sv.combined_entries_for_ticker)(cf2, ticker,
            schema_version=sv.COMBINED_FLAGS_SCHEMA_VERSION)) for ticker in tset)
    emit(f"\nDONE {len(results)} ticker(s) in {wall:.0f}s")
    emit(f"  INTERNAL now: {sum(1 for key in iv2 if key.upper() in tset)}/{len(tickers)}")
    emit(f"  COMBINED now: {combined_count}/{len(tickers)}")
    complete = len(results) == len(todo) and all(
        row.get("store1d") in {"stored", "had"}
        and row.get("internal") == "ok"
        and row.get("combined") == "ok" for row in results)
    return {"exit_code": 0 if complete else 1,
            "status": "complete" if complete else "partial", "results": results}


def run_catchup(root=None, *, all_tickers=False, ports=None,
                adapter_factory=None, progress=None, reference_fn=None,
                cancel=None, evidence_dir=None, _test_capability=None,
                _reference_opener=None):
    """One held diagnostic parent across every catch-up port and ticker."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory,
                                     test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        if (adapter_factory is not None or reference_fn is not None
                or _reference_opener is not None) and (
                operation.context.test_capability is None):
            raise RequestRefused("injected catch-up dependencies are offline-only")
        bank = Path(root).resolve() if root is not None else _bank_root().resolve()
        if directory == bank or bank in directory.parents:
            raise RequestRefused("diagnostic ledger must stay outside the stock bank")
        lease = fib.without_authority(operation_gate.acquire)(
            "fetch", owner="Validation catch-up diagnostic")
        output = _catchup_body(operation, bank, all_tickers, ports,
            adapter_factory, progress, reference_fn, fib.observer_object(cancel),
            _reference_opener)
        if output["status"] not in {"complete", "no_work"}:
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
        evidence["report_path"] = fib.without_authority(freport.write_report)(
            evidence, directory)
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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args(argv)
    try:
        return run_catchup(all_tickers=args.all)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
