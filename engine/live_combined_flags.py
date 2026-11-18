"""LIVE — 3-SOURCE double-flag check + month refetch over the whole bank.

For each ticker: a date is flagged only when the EXTERNAL (stockanalysis daily)
AND the INTERNAL (the stored 1d / IBKR's own daily) BOTH disagree with the stored
1m. For each such date's MONTH, RE-FETCH all three fresh (1m + IBKR daily +
stockanalysis) ONCE and re-test; a date that STILL double-flags is PERSISTENT and
is written to `_combined_flags.json` (clickable in the storage table). Parallel
across free fleet ports.

Works on OLD data — reads the stored 1m + stored 1d and fetches stockanalysis;
no new fields, no migration. OFF-PEAK only; needs ports.

    python live_combined_flags.py                       # whole bank, auto ports
    python live_combined_flags.py --tickers AAPL,ACN    # a subset
"""
import argparse
import calendar
import sys
import threading
import time
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk              # noqa: E402
import stock_storage as ss          # noqa: E402
import stock_validate as sv         # noqa: E402
import fetch_ibkr_bridge as fib
import fetch_operations as fops
import fetch_operation_report as freport
import fetch_diagnostic_cli as cli
import operation_gate
from fetch_authority import AuthorityError
from fetch_envelopes import decide
from fetch_ledger import LedgerError
from fetch_run_context import RequestCancelled, RequestRefused
from live_internal_revalidate import (_bank_root, _bank_series,    # noqa: E402
                                      _free_ports, _agg_rth_daily)

NY = ZoneInfo("America/New_York")
_MINUTE = "ibkr.live_combined_flags.minute_month"
_DAILY = "ibkr.live_combined_flags.daily_refetch"
_HTTP = "http.stockanalysis.validation"
_RIGHTS = frozenset({"ibkr.choke.qualify", _MINUTE, _DAILY, _HTTP})
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


def _child(label, rights):
    worker = fib.current_worker()
    return fops.narrow_worker_child(worker, worker.worker_id + ":" + label,
                                    rights=rights)


def _coverage(request):
    context = fib.current_worker().context
    decision = decide(request.envelope, context.authority, context.horizons,
                      captured_now=context.captured_now)
    return {"requested": decision.requested.wire(),
            "effective": decision.effective.wire() if decision.effective else None,
            "decision": decision.state, "reason": decision.reason}


def _month_window(token, y, m):
    context = fib.current_worker().context
    last = calendar.monthrange(y, m)[1]
    first_day, last_day = date(y, m, 1), date(y, m, last)
    if first_day < context.authority.first_date or last_day > context.authority.valid_through:
        raise RequestRefused("combined audit month outside proven calendar")
    windows = []
    for day_number in range(1, last + 1):
        window = context.authority.window(token, date(y, m, day_number))
        if window is not None:
            windows.append(window)
    if not windows:
        raise RequestRefused("combined audit month has no covered sessions")
    horizon = context.horizons[token]
    if horizon is None or horizon < windows[0][1]:
        raise RequestRefused("combined audit month has no settled covered session")
    return windows[0][0], min(windows[-1][1], horizon), horizon >= windows[-1][1]


@fib.worker_scope
def _combined_qualify(adapter, ticker, cancel=None):
    with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
        return adapter.qualify(ticker)[1]


@fib.worker_scope
def _fetch_month_1m(adapter, contract, y, m, *, cancel=None):
    """Guarded, settled 1m RTH month; never confuse a partial span with a cure."""
    first, end, full = _month_window("1m", y, m)
    request = fib.bar_request(_MINUTE, contract, "1m", end, "1 M", start=first)
    coverage = _coverage(request)
    with fib.adapter_session(adapter, True), fib.send_scope(
            request, fib.acquire_turn, cancel=cancel):
        raw = adapter.fetch(contract, end.replace(tzinfo=None), "1 M", "1 min",
                            what_to_show="TRADES")
    raw = list({b.date: b for b in raw}.values())
    aggregate = _agg_rth_daily(raw, NY)
    rows = {day: value for day, value in aggregate.items()
            if day.year == y and day.month == m}
    return rows, coverage, full


@fib.worker_scope
def _fetch_month_1d(adapter, contract, y, m, *, cancel=None):
    first, end, full = _month_window("1d", y, m)
    request = fib.bar_request(_DAILY, contract, "1d", end, "2 M", start=first)
    coverage = _coverage(request)
    with fib.adapter_session(adapter, True), fib.send_scope(
            request, fib.acquire_turn, cancel=cancel):
        raw = adapter.fetch(contract, end.replace(tzinfo=None), "2 M", "1 day",
                            what_to_show="TRADES")
    prefix = f"{y:04d}-{m:02d}"
    rows = {key: value for key, value in sv.parse_ibkr_daily_reference(raw).items()
            if str(key).startswith(prefix)}
    return rows, coverage, full


@fib.worker_scope
def _audit_ticker(adapter, root, ticker, interval,
                  rng=sv.FULL_HISTORY_RANGE, require_persisted=False,
                  *, reference_fn=None, cancel=None, _reference_opener=None):
    """One child-owned three-source audit, including every conditional refetch."""
    context = fib.current_worker().context
    if (reference_fn is not None or _reference_opener is not None) and context.test_capability is None:
        raise RequestRefused("injected external reference is offline-only")
    if reference_fn is not None and _reference_opener is not None:
        raise RequestRefused("parsed reference and raw opener are mutually exclusive")
    fatal, incomplete, refetch_coverage = [], [], []
    external = reference_fn if reference_fn is not None else sv.fetch_daily_reference
    reference_number = 0

    def _external():
        nonlocal reference_number
        try:
            if reference_fn is None:
                reference_number += 1
                child = _child("http-reference-" + str(reference_number), {_HTTP})
                with fops.scoped_worker(child, close=True) as worker:
                    # Request-local raw transport, never a parsed callback with
                    # authority. Fresh comparisons do not publish shared cache.
                    return sv.fetch_daily_reference(ticker, rng,
                        _fetch_worker=worker,
                        _opener=(fib.without_authority(_reference_opener)
                                 if _reference_opener is not None else None),
                        _force_fresh=True, _cancel=cancel)
            return fib.without_authority(external)(ticker, rng)
        except RequestRefused as exc:
            if reference_fn is None:
                fatal.append(exc)
                raise
            incomplete.append("StockAnalysis sender remains held by A2-3")
            return {}
        except _TERMINAL as exc:
            fatal.append(exc)
            raise
        except Exception as exc:  # unavailable external comparison is not clean
            incomplete.append("StockAnalysis reference unavailable: " + type(exc).__name__)
            return {}

    def _run():
        contract = _combined_qualify(adapter, ticker, cancel=cancel,
            _fetch_child=_child("qualify", {"ibkr.choke.qualify"}))
        daily = sv.read_series(root, ticker, "1d")
        internal = sv.parse_ibkr_daily_reference(daily) if daily else {}
        external_ref = _external()

        def _refetch_month(y, m):
            try:
                minute, minute_coverage, minute_full = _fetch_month_1m(
                    adapter, contract, y, m, cancel=cancel,
                    _fetch_child=_child(f"minute-{y:04d}-{m:02d}", {_MINUTE}))
                daily_ref, daily_coverage, daily_full = _fetch_month_1d(
                    adapter, contract, y, m, cancel=cancel,
                    _fetch_child=_child(f"daily-{y:04d}-{m:02d}", {_DAILY}))
                refetch_coverage.append({"month": f"{y:04d}-{m:02d}",
                    "minute": minute_coverage, "daily": daily_coverage,
                    "full_month": minute_full and daily_full})
                if (not minute_full or not daily_full
                        or minute_coverage["decision"] != "allowed"
                        or daily_coverage["decision"] != "allowed"
                        or not minute or not daily_ref):
                    incomplete.append(f"{y:04d}-{m:02d} refetch has incomplete coverage")
                    raise RequestRefused("combined refetch is incomplete")
                external_month = _external()
                prefix = f"{y:04d}-{m:02d}"
                comparison = {key: value for key, value in external_month.items()
                              if str(key).startswith(prefix)}
                if not comparison:
                    incomplete.append(prefix + " external refetch unavailable")
                    raise RequestRefused("combined external refetch is incomplete")
                return minute, comparison, daily_ref
            except RequestRefused as exc:
                incomplete.append(f"{y:04d}-{m:02d} refetch refused: "
                                  + fib.without_authority(_describe)(exc))
                raise
            except _TERMINAL as exc:
                fatal.append(exc)
                raise

        try:
            verdict = sv.combined_crosscheck(
                root, ticker, interval, ext_ref=external_ref, int_ref=internal,
                refetch_month=_refetch_month if (internal and external_ref) else None,
                requested_range=rng)
        except _TERMINAL as exc:
            fatal.append(exc)
            raise
        if not internal or not external_ref:
            verdict["status"] = "inconclusive"
            note = ("missing " + ("stored-1d " if not internal else "")
                    + ("stockanalysis" if not external_ref else "")).strip()
            if incomplete:
                note += "; " + "; ".join(incomplete)
            verdict["note"] = note[:500]
        return verdict

    verdict = sv.combined_crosscheck_with_provenance(
        root, ticker, interval, _run,
        asof=context.captured_now.isoformat(timespec="seconds"))
    if fatal:
        raise fatal[0]
    coverage = (verdict.get("reference_coverage") or {}).get("external") or {}
    if verdict["status"] in {"ok", "flagged"}:
        try:
            first = date.fromisoformat(str(coverage["derived_first_date"])[:10])
            last = date.fromisoformat(str(coverage["derived_last_date"])[:10])
            window = context.authority.window(interval, last)
            horizon = context.horizons[interval]
            if (first < context.authority.first_date or window is None
                    or horizon is None or horizon < window[1]):
                incomplete.append("stored comparison includes unproven or unsettled days")
        except (KeyError, TypeError, ValueError, AuthorityError):
            incomplete.append("stored comparison coverage is not proven")
    verdict.update(operation_id=context.operation_id,
                   refetch_coverage=refetch_coverage,
                   coverage_complete=not incomplete and verdict["status"] in {"ok", "flagged"})
    if incomplete and verdict["status"] in {"ok", "flagged"}:
        verdict["status"] = "inconclusive"
        verdict["note"] = "; ".join(incomplete)[:500]
    verdict["_verification_current"] = (
        verdict["status"] not in {"error", "inconclusive", "blocked", "pending"}
        and verdict.get("interval_fingerprint", {}).get("current") is True
        and verdict.get("internal_interval_fingerprint", {}).get("current") is True
        and verdict["coverage_complete"])
    written = sv.record_combined_flags(root, verdict)
    if require_persisted and written is None:
        raise LedgerError("combined cross-check result was not saved")
    return verdict


def _connect(port):
    return sk.LiveIB(host=sk.HOST_DEFAULT, ports=(port,),
                     client_id=sk.CLIENT_ID_FETCH).connect()


def _describe(exc):
    try:
        return f"{type(exc).__name__}: {exc}"[:500]
    except BaseException as formatting:
        if isinstance(formatting, _TERMINAL) or not isinstance(formatting, Exception):
            raise
        return "failure; diagnostic unavailable"


@fib.worker_scope
def _combined_port(port, items, root, stop, results, failures, lock,
                   *, adapter_factory=None, progress=None, reference_fn=None,
                   cancel=None, _reference_opener=None):
    adapter, failure = None, None
    emit = fib.without_authority(progress if progress is not None else print)
    try:
        if stop.is_set():
            return
        adapter = fib.without_authority(
            adapter_factory if adapter_factory is not None else _connect)(port)
        for number, (ticker, interval) in enumerate(items):
            if stop.is_set():
                break
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("combined audit cancelled")
            try:
                verdict = _audit_ticker(adapter, root, ticker, interval,
                    require_persisted=True, reference_fn=reference_fn,
                    cancel=cancel, _reference_opener=_reference_opener,
                    _fetch_child=_child("series-" + str(number), _RIGHTS))
            except _TERMINAL:
                raise
            except Exception as exc:
                verdict = {"ticker": ticker, "interval": interval,
                           "status": "error", "note": fib.without_authority(_describe)(exc)}
            with lock:
                results.append(verdict)
            persistent = len(verdict.get("persistent") or [])
            candidates = len(verdict.get("candidates") or [])
            tag = ("FLAG" if verdict.get("status") == "flagged" else
                   "err " if verdict.get("status") == "error" else "ok  ")
            emit(f"  [{tag}] {ticker:6} {interval:4} "
                 f"double-flag candidates={candidates} persistent={persistent}"
                 + (f"  NOTE {verdict.get('note')}" if verdict.get("note") else ""))
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


def _require_test_dependencies(operation, adapter_factory, reference_fn,
                               reference_opener):
    if (adapter_factory is not None or reference_fn is not None
            or reference_opener is not None) and (
            operation.context.test_capability is None):
        raise RequestRefused("injected combined dependencies are offline-only")


def _combined_body(operation, root, only, ports, adapter_factory,
                   progress, reference_fn, cancel, _reference_opener=None):
    _require_test_dependencies(operation, adapter_factory, reference_fn,
                               _reference_opener)
    emit = fib.without_authority(progress if progress is not None else print)
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("combined audit cancelled before bank scan")
    if not root.exists():
        emit(f"bank not found: {root}")
        return {"exit_code": 2, "results": [], "status": "missing_bank"}
    series = _bank_series(root, only)
    if not series:
        emit("no sub-daily series found.")
        return {"exit_code": 0, "results": [], "status": "no_series"}
    ports = tuple(ports or fib.without_authority(_free_ports)())
    if not ports:
        emit("NO free demo ports. Pause the run / pass --ports.")
        return {"exit_code": 2, "results": [], "status": "no_ports"}
    if len(set(ports)) != len(ports) or any(
            type(port) is not int or not 0 < port < 65536 for port in ports):
        raise RequestRefused("combined audit ports must be distinct valid integers")
    emit(f"bank={root}\nseries={len(series)}  ports={ports}  "
         f"rng={sv.FULL_HISTORY_RANGE}")
    by_port = {port: [] for port in ports}
    for index, item in enumerate(series):
        by_port[ports[index % len(ports)]].append(item)
    results, failures, lock, stop = [], [], threading.Lock(), threading.Event()
    cancellation = sk._OrEvent(stop, cancel)
    started = []
    start_time = time.monotonic()

    def worker(port, child):
        try:
            _combined_port(port, by_port[port], root, stop, results, failures, lock,
                adapter_factory=adapter_factory, progress=progress,
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
            child = operation.child("combined-port-" + str(port), rights=_RIGHTS)
            try:
                thread = threading.Thread(target=worker, args=(port, child),
                    name="combined-audit-" + str(port), daemon=True)
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
    wall = time.monotonic() - start_time
    flagged = [row for row in results if row.get("status") == "flagged"]
    emit("\n" + "=" * 60)
    emit(f"DONE {len(results)} series in {wall:.1f}s -> _combined_flags.json")
    emit(f"  ok={sum(row.get('status') == 'ok' for row in results)}  "
         f"FLAGGED={len(flagged)}  "
         f"error={sum(row.get('status') == 'error' for row in results)}")
    for row in flagged:
        emit(f"  ⚑ {row['ticker']} {row['interval']}: PERSISTENT double-flag dates "
             f"{(row.get('persistent') or [])[:8]}")
    complete = len(results) == len(series) and all(
        row.get("_verification_current") is True for row in results)
    return {"exit_code": 0 if complete else 1, "results": results,
            "status": "complete" if complete else "partial"}


def run_combined_flags(root=None, *, only=None, ports=None, adapter_factory=None,
                       progress=None, reference_fn=None, cancel=None,
                       evidence_dir=None, _test_capability=None,
                       _reference_opener=None):
    """Held diagnostic root; admission precedes bank, port and connection work."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory,
                                     test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        _require_test_dependencies(operation, adapter_factory, reference_fn,
                                   _reference_opener)
        bank = Path(root).resolve() if root is not None else _bank_root().resolve()
        if directory == bank or bank in directory.parents:
            raise RequestRefused("diagnostic ledger must stay outside the stock bank")
        lease = fib.without_authority(operation_gate.acquire)(
            "fetch", owner="Combined flags diagnostic")
        output = _combined_body(operation, bank, only, ports, adapter_factory,
            progress, reference_fn, fib.observer_object(cancel), _reference_opener)
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
            outcome="report_failed", report_failure=fib.without_authority(_describe)(exc))
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
    ap.add_argument("--tickers", default="")
    ap.add_argument("--ports", default="")
    args = ap.parse_args(argv)
    only = {ticker.strip().upper() for ticker in args.tickers.split(",")
            if ticker.strip()} or None
    ports = [int(port) for port in args.ports.split(",") if port.strip()]
    try:
        return run_combined_flags(only=only, ports=ports)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
