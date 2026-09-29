"""LIVE — run ALL kind/daily port tests, ACCURACY FIRST, then as fast as possible.

Speed: the 4 INDEPENDENT test groups (raw shapes / end-to-end store / daily update
/ span probe) run in PARALLEL, each on its own free fleet port.

Accuracy first: parallel load on the shared demo backend could in principle cause a
transient connection/timeout failure (a false negative). So ANY group that fails under
the parallel pass is automatically RE-RUN ISOLATED (alone, no contention) and that
isolated result is authoritative. False PASSES are not a risk — the assertions are
deterministic (a wrong date / uncoerced -1 volume / non-ratio value cannot look correct
under load). With ALL groups passing (the expected case) there is no re-run and it is
fast (~one group's wall-clock).

Build-only at import; everything runs under __main__. DO NOT run during a fetch run.

Usage (only AFTER the main run is paused and ports are free):
    python live_run_all.py                      # auto-detect free fleet ports
    python live_run_all.py --ports 2000,3000,4000,5000
    python live_run_all.py --ticker MSFT        # also confirm on a 2nd symbol
    python live_run_all.py --serial             # force one-port sequential (max accuracy)
"""
import argparse
import sys
import threading
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk          # noqa: E402
import live_kind_smoke as L      # noqa: E402
import fetch_ibkr_bridge as fib  # noqa: E402
import fetch_operations as fops  # noqa: E402
import fetch_operation_report as freport  # noqa: E402
import fetch_diagnostic_cli as cli  # noqa: E402
import operation_gate             # noqa: E402
from fetch_authority import AuthorityError  # noqa: E402
from fetch_ledger import LedgerError  # noqa: E402
from fetch_run_context import RequestCancelled, RequestRefused  # noqa: E402

_FLEET = (2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000)

# Fixed group names only: a caller-supplied callable can never become a
# request-owning dispatch. Each shipped group consumes its own narrow child.
GROUPS = ("RAW-shapes(1-4)", "STORE(5)", "UPDATE(6)", "SPAN(7)")
_RIGHTS = {
    GROUPS[0]: ("raw", L._RAW_RIGHTS),
    GROUPS[1]: ("store", L._FILL_RIGHTS),
    GROUPS[2]: ("update", L._FILL_RIGHTS),
    GROUPS[3]: ("span", L._SPAN_RIGHTS),
}
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


def _free_ports():
    return [p for p in _FLEET if sk._port_open(sk.HOST_DEFAULT, p, 0.5)]


def _run_group(operation, name, port, ticker, attempt, adapter_factory, cancel):
    """Dispatch one fixed group under one narrowed child; terminal faults escape."""
    if name not in _RIGHTS:
        raise RequestRefused("unknown kind/daily group")
    tly = fib.observer_object(L._Tally())
    t0 = time.monotonic()
    key, rights = _RIGHTS[name]
    child = operation.child(f"all-{key}-{attempt}", rights=rights)
    factory = (None if adapter_factory is None else
               lambda: fib.without_authority(adapter_factory)(port))
    try:
        if name == GROUPS[0]:
            L.run_raw_group(port, ticker, tly, adapter_factory=factory,
                            cancel=cancel, _fetch_child=child)
        elif name == GROUPS[1]:
            L.test_end_to_end_store(port, ticker, tly, adapter_factory=factory,
                                    cancel=cancel, _fetch_child=child)
        elif name == GROUPS[2]:
            L.test_daily_update_twice(port, ticker, tly, adapter_factory=factory,
                                      cancel=cancel, _fetch_child=child)
        else:
            L.run_span_group(port, ticker, tly, adapter_factory=factory,
                             cancel=cancel, _fetch_child=child)
    except _TERMINAL:
        raise
    except Exception as exc:  # ordinary provider failure retains isolated retry
        tly.check(False, f"{name} CRASHED: {type(exc).__name__}: {exc}")
    return tly.passed, tly.failed, time.monotonic() - t0


def _all_body(operation, ports, ticker, serial, adapter_factory, cancel):
    if (ports is not None and (type(ports) not in (list, tuple)
            or any(type(port) is not int or not 0 < port < 65536 for port in ports)
            or len(set(ports)) != len(ports))):
        raise RequestRefused("kind/daily runner ports must be distinct valid integers")
    if type(ticker) is not str or not ticker.strip() or ticker != ticker.strip():
        raise RequestRefused("kind/daily runner ticker must be canonical")
    if type(serial) is not bool:
        raise RequestRefused("kind/daily runner serial flag must be boolean")
    if adapter_factory is not None and operation.context.test_capability is None:
        raise RequestRefused("injected kind/daily adapter factory is offline-only")
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("kind/daily runner cancelled before port discovery")
    ports = list(ports) if ports else fib.without_authority(_free_ports)()
    if not ports:
        print("NO free demo ports on the fleet (2000..9000). Is TWS up and the main "
              "run paused? Pass --ports to be explicit.")
        return {"exit_code": 2, "status": "no_ports", "results": {}}
    if serial:
        ports = ports[:1]
    print(f"PORTS={ports}  TICKER={ticker}  GROUPS={len(GROUPS)}  "
          f"mode={'SERIAL' if serial else 'PARALLEL+isolate-on-fail'}")

    results = {}
    lock = threading.Lock()
    stop = threading.Event()
    failure = []

    class CancelView:
        def is_set(self):
            return stop.is_set() or (cancel is not None and cancel.is_set())

    shared_cancel = fib.observer_object(CancelView())

    # --- PARALLEL pass: distribute groups round-robin across ports ---
    assign = {}
    for i, name in enumerate(GROUPS):
        assign.setdefault(ports[i % len(ports)], []).append(name)

    def worker(port, glist):
        for name in glist:
            if shared_cancel.is_set():
                return
            try:
                p, f, dt = _run_group(operation, name, port, ticker,
                                      "parallel", adapter_factory, shared_cancel)
            except BaseException as exc:
                with lock:
                    if not failure:
                        failure.append(exc)
                stop.set()
                return
            with lock:
                results[name] = {"port": port, "passed": p, "failed": f,
                                 "sec": dt, "iso": False}

    threads = [threading.Thread(target=worker, args=(p, g), daemon=True)
               for p, g in assign.items()]
    wall0 = time.monotonic()
    started = []
    try:
        for th in threads:
            th.start()
            started.append(th)
    finally:
        for th in started:
            th.join()
    wall = time.monotonic() - wall0
    if failure:
        raise failure[0]
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("kind/daily runner cancelled after joined groups")

    # --- ACCURACY-FIRST: re-run any FAILED group ISOLATED (no contention) ---
    failed = [n for n, r in results.items() if r["failed"]]
    if failed and not serial:
        print(f"\n{len(failed)} group(s) failed under parallel load — re-running "
              f"each ISOLATED to rule out backend contention (accuracy first)…")
        for name in failed:
            if shared_cancel.is_set():
                raise RequestCancelled("kind/daily runner cancelled before isolated retry")
            p, f, dt = _run_group(operation, name, ports[0], ticker,
                                  "isolated", adapter_factory, shared_cancel)
            results[name].update({"passed": p, "failed": f, "sec": dt, "iso": True})

    # --- summary ---
    print("\n" + "=" * 64)
    print("SUMMARY  (a failed group shows its ISOLATED re-run = authoritative)")
    print("=" * 64)
    tot_f = 0
    for name in GROUPS:
        r = results.get(name, {"port": "?", "passed": 0, "failed": 1,
                               "sec": 0.0, "iso": False})
        tot_f += r["failed"]
        tag = "PASS" if r["failed"] == 0 else "FAIL"
        flag = " [isolated]" if r["iso"] else ""
        print(f"  {tag}  {name:18} port {r['port']}  "
              f"{r['passed']} ok / {r['failed']} fail  {r['sec']:.1f}s{flag}")
    print(f"\nPARALLEL wall = {wall:.1f}s    TOTAL FAIL = {tot_f}")
    print("\nALL PASS — IV/HVOL/daily confirmed live. Safe to resume the run."
          if tot_f == 0 else
          "\nFAILURES CONFIRMED (isolated) — do NOT trust the feature; see above.")
    return {"exit_code": 0 if tot_f == 0 else 1,
            "status": "complete" if tot_f == 0 else "partial",
            "results": results, "failed": tot_f}


def run_all(*, ports=None, ticker="AAPL", serial=False, adapter_factory=None,
            cancel=None, evidence_dir=None, _test_capability=None):
    """Held diagnostic parent across groups, threads and isolated retries."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory,
                                     test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        if adapter_factory is not None and operation.context.test_capability is None:
            raise RequestRefused("injected kind/daily adapter factory is offline-only")
        lease = fib.without_authority(operation_gate.acquire)(
            "fetch", owner="Kind/daily all-groups diagnostic")
        output = _all_body(operation, ports, ticker, serial, adapter_factory,
                           fib.observer_object(cancel))
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
    ap = argparse.ArgumentParser(
        description="Run ALL live kind/daily tests (accuracy first, then fast).")
    ap.add_argument("--ports", default="",
                    help="comma list of demo ports; default = auto-detect free fleet ports")
    ap.add_argument("--ticker", default="AAPL")
    ap.add_argument("--serial", action="store_true",
                    help="force one-port sequential (no parallel load at all)")
    args = ap.parse_args(argv)
    ports = [int(x) for x in args.ports.split(",") if x.strip()] or None
    try:
        return run_all(ports=ports, ticker=args.ticker, serial=args.serial)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
