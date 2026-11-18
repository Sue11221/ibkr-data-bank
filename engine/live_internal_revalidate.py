"""LIVE — BACK-APPLY the internal daily validation to the EXISTING bank.

For every stored REGULAR sub-daily TRADES series: fetch IBKR's OWN daily TRADES,
audit the stored intraday aggregated-to-daily against it (price >3%, volume
>0.5%, matching stock_validate's calibrated classifier), REFETCH
any discrepant date to rule out a fetch glitch, and write the verdict to
`_internal_validation.json` in the bank. Parallel across free fleet ports.

Held until reviewed diagnostic activation. Admission precedes the exclusive
fetch lease, bank scan and port setup. Offline fixtures never call this CLI.

    python live_internal_revalidate.py                       # whole bank, auto ports
    python live_internal_revalidate.py --tickers AAPL,MSFT   # a subset
    python live_internal_revalidate.py --days 3650           # deeper history
    python live_internal_revalidate.py --ports 2000,3000     # explicit ports
"""
import argparse
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk          # noqa: E402
import stock_storage as ss       # noqa: E402
import stock_validate as sv      # noqa: E402
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
import fetch_diagnostic_cli as cli
import operation_gate
from fetch_authority import AuthorityError
from fetch_envelopes import decide
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused

_DAILY = "ibkr.live_internal_revalidate.daily"
_MINUTE = "ibkr.live_internal_revalidate.minute_refetch"
_RIGHTS = frozenset({"ibkr.choke.qualify", _DAILY, _MINUTE})
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)

_FLEET = (2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000)


def _free_ports():
    return [p for p in _FLEET if sk._port_open(sk.HOST_DEFAULT, p, 0.5)]


def _bank_root():
    return Path(_HERE).parent / ss.STORAGE_DIR_NAME


def _bank_series(root, only=None):
    """{(ticker, interval)} for every stored REGULAR sub-daily TRADES series
    (skips -pre/-post, kinds, and daily)."""
    out = set()
    for f in Path(root).rglob("*"):
        if not f.is_file():
            continue
        m = ss.FILENAME_RE.match(f.name)
        if not m:
            continue
        ticker, interval = m.group(1), m.group(4)
        if interval != ss.base_interval(interval):      # -pre/-post or a kind
            continue
        if ss.base_interval(interval).endswith("d"):     # daily
            continue
        if only and ticker.upper() not in only:
            continue
        out.add((ticker, interval))
    return sorted(out)


def _agg_rth_daily(raw_bars, ny):
    """raw IBKR 1m bars -> {date: (o,h,l,c,v)} for RTH (same as derive_daily)."""
    by_day = {}
    for b in raw_bars:
        dt = b.date
        if getattr(dt, "tzinfo", None) is not None:
            dt = dt.astimezone(ny).replace(tzinfo=None)
        elif not isinstance(dt, datetime):
            continue
        if not (ss.RTH_FIRST <= dt.time() <= ss.RTH_LAST):
            continue
        by_day.setdefault(dt.date(), []).append(
            (dt, float(b.open), float(b.high), float(b.low),
             float(b.close), float(b.volume)))
    out = {}
    for d, rows in by_day.items():
        rows.sort(key=lambda r: r[0])
        out[d] = (rows[0][1], max(r[2] for r in rows), min(r[3] for r in rows),
                  rows[-1][4], sum(r[5] for r in rows))
    return out


def _child(label, rights):
    worker = fib.current_worker()
    return fops.narrow_worker_child(worker, worker.worker_id + ":" + label,
                                    rights=rights)


@fib.worker_scope
def _internal_qualify(adapter, ticker, cancel=None):
    with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
        return adapter.qualify(ticker)[1]


def _coverage(request):
    context = fib.current_worker().context
    decision = decide(request.envelope, context.authority, context.horizons,
                      captured_now=context.captured_now)
    return {"requested": decision.requested.wire(),
            "effective": decision.effective.wire() if decision.effective else None,
            "decision": decision.state, "reason": decision.reason}


@fib.worker_scope
def _internal_daily(adapter, contract, end_dt, days, cancel=None):
    # IBKR caps the 'D' (days) duration ~1yr for daily bars; use the 'Y' (years)
    # carrier for wider windows, without widening the intended comparison.
    dur = f"{days} D" if days <= 365 else f"{-(-days // 365)} Y"
    first = end_dt - timedelta(days=days)
    request = fib.bar_request(_DAILY, contract, "1d", end_dt, dur, start=first)
    coverage = _coverage(request)
    with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn, cancel=cancel):
        daily_raw = adapter.fetch(contract, end_dt.replace(tzinfo=None), dur, "1 day",
                                  what_to_show="TRADES")
    return sv.parse_ibkr_daily_reference(daily_raw), coverage


@fib.worker_scope
def _internal_minute(adapter, contract, day, ny, cancel=None):
    end = datetime(day.year, day.month, day.day, 20, tzinfo=fib.NY)
    first = datetime(day.year, day.month, day.day, 9, 30, tzinfo=fib.NY)
    request = fib.bar_request(_MINUTE, contract, "1m", end, "1 D", start=first)
    coverage = _coverage(request)
    with fib.adapter_session(adapter, True), fib.send_scope(request, fib.acquire_turn, cancel=cancel):
        raw = adapter.fetch(contract, end.replace(tzinfo=None), "1 D", "1 min", what_to_show="TRADES")
    derived = _agg_rth_daily(raw, ny)
    if day not in derived:
        raise RuntimeError("minute refetch has no accepted rows for the discrepant date")
    return derived, coverage


@fib.worker_scope
def _audit_series(adapter, root, ticker, interval, days, ny, cancel=None):
    if type(days) is not int or days < 1:
        raise RequestRefused("diagnostic days must be a positive integer")
    context = fib.current_worker().context
    before = ss.interval_state_fingerprint(root, ticker, interval)
    bars = sv.read_series(root, ticker, interval)
    contract = _internal_qualify(adapter, ticker, cancel=cancel,
        _fetch_child=_child("qualify", {"ibkr.choke.qualify"}))
    y = context.captured_now.date() - timedelta(days=1)
    end_dt = datetime(y.year, y.month, y.day, 16, tzinfo=fib.NY)
    daily_ref, daily_coverage = _internal_daily(adapter, contract, end_dt, days,
        cancel=cancel, _fetch_child=_child("daily", {_DAILY}))
    refetch_coverage = []

    def _refetch(d):
        derived, coverage = _internal_minute(adapter, contract, d, ny, cancel=cancel,
            _fetch_child=_child("minute-" + d.isoformat(), {_MINUTE}))
        refetch_coverage.append(coverage)
        return derived, {str(d): daily_ref.get(str(d))}

    verdict = sv.internal_daily_audit(root, ticker, interval,
        daily_ref=daily_ref, refetch=_refetch, read_fn=lambda *_: bars)
    derived = sv.derive_daily(bars)
    missing = sorted(str(d) for d in derived if str(d) not in daily_ref)
    verdict.update(asof=context.captured_now.isoformat(timespec="seconds"),
        operation_id=context.operation_id, interval_fingerprint=before,
        requested_effective=daily_coverage, refetch_coverage=refetch_coverage,
        missing_reference_days=missing, coverage_complete=bool(derived) and not missing)
    if missing:
        verdict.update(status="inconclusive", note="daily reference does not cover all stored dates")
    if before != ss.interval_state_fingerprint(root, ticker, interval):
        raise LedgerError("internal audit source changed before verdict publication")
    if sv.record_internal_validation(root, verdict) is None:
        raise LedgerError("internal audit result was not saved")
    return verdict


def _connect(port):
    return sk.LiveIB(host=sk.HOST_DEFAULT, ports=(port,),
                     client_id=sk.CLIENT_ID_FETCH).connect()


def _describe(exc, terminal=True):
    try:
        return f"{type(exc).__name__}: {exc}"[:500]
    except BaseException as formatting:
        if terminal and (isinstance(formatting, _TERMINAL) or not isinstance(formatting, Exception)):
            raise
        return "failure; diagnostic unavailable"


@fib.worker_scope
def _internal_port(port, items, root, days, stop, results, failures, lock,
                   adapter_factory=None, progress=None, cancel=None):
    adapter, failure = None, None
    try:
        if stop.is_set():
            return
        adapter = fib.without_authority(adapter_factory if adapter_factory is not None else _connect)(port)
        for number, (ticker, interval) in enumerate(items):
            if stop.is_set():
                break
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("internal audit cancelled")
            try:
                v = _audit_series(adapter, root, ticker, interval, days, fib.NY,
                    cancel=cancel, _fetch_child=_child("series-" + str(number), _RIGHTS))
            except _TERMINAL:
                raise
            except Exception as exc:
                v = {"ticker": ticker, "interval": interval,
                     "status": "error", "note": fib.without_authority(_describe)(exc)}
            with lock:
                results.append(v)
            tag = {"ok": "OK ", "disagree": "X  ", "inconclusive": "-- ",
                   "error": "ERR"}.get(v.get("status"), "?  ")
            fib.without_authority(progress if progress is not None else print)(
                f"  [{tag}] {ticker:6} {interval:4} compared={v.get('compared')} "
                f"confirmed={len(v.get('confirmed') or {})} "
                f"transient={len(v.get('transient') or {})}"
                + (f"  NOTE {v.get('note')}" if v.get("note") else ""))
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


def _internal_body(operation, root, only, ports, days, adapter_factory, progress, cancel):
    if type(days) is not int or days < 1:
        raise RequestRefused("diagnostic days must be a positive integer")
    root = Path(root) if root is not None else _bank_root()
    emit = fib.without_authority(progress if progress is not None else print)
    if not root.exists():
        emit(f"bank not found: {root}")
        return {"exit_code": 2, "results": [], "status": "missing_bank"}
    series = _bank_series(root, only)
    if not series:
        emit("no sub-daily TRADES series found in the bank.")
        return {"exit_code": 0, "results": [], "status": "no_series"}
    ports = tuple(ports or fib.without_authority(_free_ports)())
    if not ports:
        emit("NO free demo ports (2000..9000). Pause the run / pass --ports.")
        return {"exit_code": 2, "results": [], "status": "no_ports"}
    if len(set(ports)) != len(ports) or any(type(p) is not int or not 0 < p < 65536 for p in ports):
        raise RequestRefused("diagnostic ports must be distinct valid integers")
    emit(f"bank={root}\nseries={len(series)}  ports={ports}  window={days}d")
    by_port = {p: [] for p in ports}
    for i, s in enumerate(series):
        by_port[ports[i % len(ports)]].append(s)
    results, failures, lock, stop = [], [], threading.Lock(), threading.Event()
    cancellation = sk._OrEvent(stop, cancel)
    t0 = time.monotonic()
    started = []
    def worker(port, child):
        try:
            _internal_port(port, by_port[port], root, days, stop, results, failures, lock,
                adapter_factory=adapter_factory, progress=progress,
                cancel=cancellation, _fetch_child=child)
        except BaseException as exc:
            stop.set()
            with lock:
                failures.append(exc)
    try:
        for p in ports:
            if not by_port[p] or stop.is_set():
                continue
            child = operation.child("internal-port-" + str(p), rights=_RIGHTS)
            try:
                th = threading.Thread(target=worker, args=(p, child),
                    name="internal-audit-" + str(p), daemon=True)
            except BaseException:
                child.close()
                raise
            # Register before start: a start hook can launch and then raise.
            started.append((th, child))
            th.start()
    except BaseException as exc:
        stop.set()
        failures.append(exc)
    finally:
        for th, child in started:
            if th.ident is None:
                child.close()
            else:
                th.join()
    if failures:
        raise failures[0]
    wall = time.monotonic() - t0
    disagree = [r for r in results if r.get("status") == "disagree"]
    emit("\n" + "=" * 60)
    emit(f"DONE {len(results)} series in {wall:.1f}s  -> _internal_validation.json")
    emit(f"  OK={sum(1 for r in results if r.get('status')=='ok')}  "
          f"DISAGREE(X)={len(disagree)}  "
          f"inconclusive={sum(1 for r in results if r.get('status')=='inconclusive')}  "
          f"error={sum(1 for r in results if r.get('status')=='error')}")
    for r in disagree:
        dates = sorted((r.get("confirmed") or {}).keys())
        emit(f"  X {r['ticker']} {r['interval']}: {len(dates)} confirmed "
              f"disagreement date(s): {dates[:8]}")
    complete = len(results) == len(series) and all(r.get("status") == "ok" for r in results)
    return {"exit_code": 0 if complete else 1, "results": results,
            "status": "complete" if complete else "partial"}


def run_internal_revalidate(root=None, *, only=None, ports=None, days=365,
                             adapter_factory=None, progress=None, cancel=None,
                             evidence_dir=None, _test_capability=None):
    """Held diagnostic root; all port children retire before seal and report."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    bank = Path(root).resolve() if root is not None else _bank_root().resolve()
    if directory == bank or bank in directory.parents:
        raise RequestRefused("diagnostic ledger must stay outside the stock bank")
    operation = fops.begin_operation("diagnostic", directory, test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)("fetch", owner="Internal revalidation")
        output = _internal_body(operation, bank, only, ports, days,
                                adapter_factory, progress, fib.observer_object(cancel))
        if output["status"] not in {"complete", "no_series"}:
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
    evidence.update(report_path=str(directory / (context.operation_id + ".operation.json")),
                    report_persisted=True)
    try:
        fib.without_authority(freport.write_report)(evidence, directory)
    except BaseException as exc:
        evidence.update(verified=False, state="UNVERIFIED", report_persisted=False,
            outcome="report_failed", report_failure=fib.without_authority(_describe)(exc, False))
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
    ap.add_argument("--tickers", default="", help="comma subset (default: whole bank)")
    ap.add_argument("--ports", default="", help="comma ports (default: auto free fleet)")
    ap.add_argument("--days", type=int, default=365, help="history window to audit")
    args = ap.parse_args(argv)
    only = {t.strip().upper() for t in args.tickers.split(",") if t.strip()} or None
    ports = [int(x) for x in args.ports.split(",") if x.strip()]
    try:
        return run_internal_revalidate(only=only, ports=ports, days=args.days)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
