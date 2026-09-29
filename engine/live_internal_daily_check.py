"""LIVE — verify the INTERNAL daily cross-check PREMISE: does IBKR's own
1-minute TRADES data, aggregated to a daily bar, match IBKR's own DAILY TRADES
bar EXACTLY (or how close)? This decides the whole internal-check design (what
counts as a real disagreement vs rounding/coverage noise).

OFF-PEAK only; needs ports. DO NOT run during a fetch run.
    python live_internal_daily_check.py --port 2000 --ticker AAPL --days 10
"""
import argparse
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk          # noqa: E402
import stock_storage as ss       # noqa: E402
import stock_basis as sb         # noqa: E402
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

_DAILY = "ibkr.live_internal_daily_check.daily"
_FILL_RIGHTS = frozenset({
    "ibkr.gap_fill.month_daily", "ibkr.gap_fill.month_intraday",
    "ibkr.gap_fill.session_fetch", "ibkr.gap_fill.head_daily_probe",
    "ibkr.gap_fill.head_timestamp", "ibkr.earliest_available.head_timestamp",
    "ibkr.choke.qualify", "ibkr.choke.qualify_many",
})


def _agg_daily_from_1m(bars):
    """OHLCV daily aggregation of stored 1m RTH bars -> {date: (o,h,l,c,v)}.
    open=first, high=max, low=min, close=last, volume=sum (the natural daily)."""
    by_day = {}
    for dt, o, h, lo, c, v in bars:
        by_day.setdefault(dt.date(), []).append((dt, o, h, lo, c, v))
    out = {}
    for d, rows in by_day.items():
        rows.sort(key=lambda r: r[0])
        o = rows[0][1]
        c = rows[-1][4]
        hi = max(r[2] for r in rows)
        lo = min(r[3] for r in rows)
        vol = sum(r[5] for r in rows)
        out[d] = (o, hi, lo, c, vol)
    return out


@fib.worker_scope
def _premise_qualify(adapter, ticker, cancel=None):
    with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
        return adapter.qualify(ticker)


@fib.worker_scope
def _premise_request(adapter, contract, end_dt, days, cancel=None):
    window_days = days + 3
    duration = (f"{window_days} D" if window_days <= 365
                else f"{-(-window_days // 365)} Y")
    first = end_dt - timedelta(days=window_days)
    request = fib.bar_request(_DAILY, contract, "1d", end_dt, duration,
                              start=first)
    context = fib.current_worker().context
    decision = decide(request.envelope, context.authority, context.horizons,
                      captured_now=context.captured_now)
    coverage = {"requested": decision.requested.wire(),
                "effective": decision.effective.wire() if decision.effective else None,
                "decision": decision.state, "reason": decision.reason}
    with fib.adapter_session(adapter, True), fib.send_scope(
            request, fib.acquire_turn, cancel=cancel):
        raw = adapter.fetch(contract, end_dt.replace(tzinfo=None), duration,
                            "1 day", what_to_show="TRADES")
    return sv.parse_ibkr_daily_reference(raw), coverage


@fib.worker_scope
def _premise_daily(factory, ticker, resolved, days, cancel=None):
    worker = fib.current_worker()
    context = worker.context
    horizon = context.horizons["1d"]
    if horizon is None:
        raise RequestRefused("daily premise has no settled covered session")
    adapter = fib.without_authority(factory)()
    failure = None
    try:
        conid, contract = _premise_qualify(adapter, ticker, cancel=cancel,
            _fetch_child=fops.narrow_worker_child(worker, "premise-qualify",
                rights={"ibkr.choke.qualify"}))
        if conid != resolved or contract.conId != conid:
            raise RequestRefused("daily premise identity changed after validation")
        return _premise_request(adapter, contract, horizon, days, cancel=cancel,
            _fetch_child=fops.narrow_worker_child(worker, "premise-daily", rights={_DAILY}))
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            sk._auxiliary_disconnect(adapter)
        except BaseException:
            if failure is None:
                raise


def _daily_body(operation, port, ticker, days, adapter_factory, cancel):
    if type(port) is not int or not 0 < port < 65536:
        raise RequestRefused("daily premise port must be a valid integer")
    if type(ticker) is not str or not ticker.strip() or ticker != ticker.strip():
        raise RequestRefused("daily premise ticker must be a canonical symbol")
    if type(days) is not int or days < 1:
        raise RequestRefused("daily premise days must be positive")
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("daily premise cancelled before validation")
    factory = (adapter_factory if adapter_factory is not None else
        fib.without_authority(sk.live_adapter_factory)(host=sk.HOST_DEFAULT,
            ports=(port,), client_id=sk.CLIENT_ID_FETCH))
    root = Path(tempfile.mkdtemp(prefix="live_intl_")) / ss.STORAGE_DIR_NAME
    today = operation.context.captured_now.date()
    since = today - timedelta(days=days)
    failure = None
    try:
        # 1) fetch 1m RTH TRADES into a temp bank
        val = sk.validate_symbols([ticker], adapter_factory=factory, cancel=cancel,
            identity_today=today,
            _fetch_child=operation.child("premise-identity", rights={"ibkr.choke.qualify_many"}))
        if val.get("cancelled") or (cancel is not None and cancel.is_set()):
            raise RequestCancelled("daily premise cancelled during validation")
        if val.get("error"):
            return {"exit_code": 1, "status": "partial", "reason": val["error"]}
        resolved = val.get("resolved") or {}
        if resolved.get(ticker) is None:
            return {"exit_code": 1, "status": "partial", "reason": "ticker not resolved"}
        rep = sk.gap_fill(root, [(ticker, "1m")], progress=lambda m: None, cancel=cancel,
                          adapter_factory=factory, today=today, since=since,
                          resolved=resolved,
                          _fetch_child=operation.child("premise-fill", rights=_FILL_RIGHTS))
        s = (rep.get("series") or [{}])[0]
        print(f"1m TRADES fetched: added={s.get('added')} halt={s.get('halt')}")
        if rep.get("cancelled") or (cancel is not None and cancel.is_set()):
            raise RequestCancelled("daily premise cancelled during minute fill")
        if operation.context.ledger.failure:
            raise LedgerError(operation.context.ledger.failure)
        if rep.get("aborted") or s.get("halt"):
            raise RequestRefused(f"daily premise minute fill halted: {s.get('halt')}")
        if not s.get("added"):
            return {"exit_code": 1, "status": "partial", "reason": "minute fill incomplete"}

        bars, _ = sb.read_series(root, ticker, "1m")
        derived = _agg_daily_from_1m(bars)

        # 2) fetch IBKR's OWN daily TRADES for the same window
        daily_ref, coverage = _premise_daily(factory, ticker, resolved[ticker], days,
            cancel=cancel,
            _fetch_child=operation.child("premise-reference",
                rights={"ibkr.choke.qualify", _DAILY}))

        # 3) EXACT per-day comparison (derived 1m->daily  vs  IBKR daily)
        print(f"\n{'date':12} {'field':6} {'derived(1m)':>14} "
              f"{'IBKR daily':>14} {'abs diff':>12} {'rel':>9}")
        print("-" * 72)
        # daily_ref keys may be iso-strings; normalize to date for the join
        def _norm(refmap):
            out = {}
            for k, v in refmap.items():
                d = k if hasattr(k, "year") else datetime.fromisoformat(
                    str(k)[:10]).date()
                out[d] = v
            return out
        ref = _norm(daily_ref)
        common = sorted(set(derived) & set(ref))
        worst = {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "vol": 0.0}
        n_exact = 0
        for d in common:
            do, dh, dl, dc, dv = derived[d]
            ro, rh, rl, rc, rv = ref[d]
            row = [("open", do, ro), ("high", dh, rh), ("low", dl, rl),
                   ("close", dc, rc), ("vol", dv, rv)]
            day_exact = True
            for name, dval, rval in row:
                ad = abs(float(dval) - float(rval))
                rel = ad / abs(float(rval)) if rval else (0.0 if ad == 0 else 1.0)
                worst[name] = max(worst[name], rel)
                if ad > 1e-9:
                    day_exact = False
                    print(f"{str(d):12} {name:6} {float(dval):14.4f} "
                          f"{float(rval):14.4f} {ad:12.4f} {rel:8.3%}")
            if day_exact:
                n_exact += 1
        print("-" * 72)
        print(f"days compared: {len(common)}   EXACT-match days: {n_exact}")
        print("worst relative diff per field:")
        for k, v in worst.items():
            print(f"  {k:6}: {v:.4%}")
        print("\nINTERPRETATION: OHLC should match to the penny if 1m RTH covers "
              "the official session; VOLUME often differs (daily TRADES volume "
              "may include odd-lots / different consolidation than summed 1m).")

        # 4) the ACTUAL internal_crosscheck verdict (the machinery that will fire)
        try:
            verdict = sv.internal_crosscheck(root, ticker, "1m",
                                             daily_ref=daily_ref)
            print(f"\ninternal_crosscheck verdict: "
                  f"{ {k: verdict.get(k) for k in ('status', 'score', 'flagged', 'note')} }")
        except (AuthorityError, LedgerError, RequestCancelled):
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"\ninternal_crosscheck raised: {type(exc).__name__}: {exc}")
            return {"exit_code": 1, "status": "partial", "reason": "internal classifier failed",
                    "coverage": coverage}
        complete = (bool(common) and len(common) == len(derived)
                    and len(common) == len(ref)
                    and coverage["decision"] == "allowed"
                    and not verdict.get("error") and verdict.get("checked", 0) > 0)
        return {"exit_code": 0 if complete else 1,
                "status": "complete" if complete else "partial",
                "days_compared": len(common), "exact_days": n_exact,
                "coverage": coverage, "verdict": verdict}
    except BaseException as exc:
        failure = exc
        raise
    finally:
        try:
            shutil.rmtree(root.parent)
        except BaseException:
            if failure is None:
                raise


def run_internal_daily_check(*, port=2000, ticker="AAPL", days=10,
                             adapter_factory=None, evidence_dir=None, cancel=None,
                             _test_capability=None):
    """Held one-parent premise proof with detached operation evidence."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory, test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)("fetch", owner="Daily premise")
        output = _daily_body(operation, port, ticker, days, adapter_factory, cancel)
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


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--ticker", default="AAPL")
    ap.add_argument("--days", type=int, default=10)
    args = ap.parse_args(argv)
    try:
        return run_internal_daily_check(port=args.port, ticker=args.ticker,
                                        days=args.days)["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
