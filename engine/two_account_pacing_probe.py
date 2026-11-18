"""Held two-account historical-data diagnostic; no server-ceiling claim.

The 1-second sends are ledgered and subject to the canonical process-lifetime
account governor. A governed run records factual provider outcomes but cannot
establish the old ungoverned 60/10-minute server-ceiling hypothesis. Production
admission remains disabled until the separately reviewed A2 activation.
"""

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_ibkr as sk          # noqa: E402
import fetch_ibkr_bridge as fib  # noqa: E402
import fetch_operations as fops  # noqa: E402
import fetch_operation_report as freport  # noqa: E402
import fetch_diagnostic_cli as cli  # noqa: E402
import operation_gate  # noqa: E402
from fetch_authority import AuthorityError  # noqa: E402
from fetch_ledger import LedgerError  # noqa: E402
from fetch_run_context import RequestCancelled, RequestRefused  # noqa: E402

TICKER = "KO"
BAR_SIZE = "1 secs"
DURATION = "1800 S"        # one 30-min window of 1s bars = one HMDS request
MAX_FIRE = 80              # safety cap per account
_HAMMER = "ibkr.two_account_pacing_probe.hammer"
_RIGHTS = frozenset({"ibkr.choke.qualify", _HAMMER})
_TERMINAL = (AuthorityError, LedgerError, RequestCancelled)


def settled_session_ends(context, n_sessions=14):
    """Covered, settled RTH half-hours from the parent's frozen authority."""
    horizon = context.horizons["1s"]
    if horizon is None:
        raise RequestRefused("pacing diagnostic has no settled 1s horizon")
    out, day, sessions = [], min(context.captured_now.date(), horizon.date()), 0
    while sessions < n_sessions and day >= context.authority.first_date:
        window = context.authority.window("1s", day)
        if window is not None:
            sessions += 1
            opening, closing = window
            end = closing
            while end - timedelta(minutes=30) >= opening:
                if end <= horizon:
                    out.append(end)
                end -= timedelta(minutes=30)
        day -= timedelta(days=1)
    if not out:
        raise RequestRefused("pacing diagnostic has no covered settled half-hour")
    return out


@fib.worker_scope
def hammer(label, adapter, ticker, ends, cancel=None):
    """One verified account's qualification and exact governed 1s attempts."""
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("pacing diagnostic cancelled before qualification")
    with fib.qualification_scope("qualify", [ticker], fib.acquire_turn, cancel=cancel):
        _con_id, contract = adapter.qualify(ticker)
    fired = ok = walled = perm_halt = 0
    errs = []
    with fib.adapter_session(adapter, True):
        for end in ends[:MAX_FIRE]:
            if cancel is not None and cancel.is_set():
                raise RequestCancelled("pacing diagnostic cancelled before send")
            request = fib.bar_request(_HAMMER, contract, "1s", end, DURATION,
                start=end - timedelta(minutes=30), intended_end=end)
            fired += 1
            try:
                with fib.send_scope(request, fib.acquire_turn, cancel=cancel):
                    bars = adapter.fetch(contract, end.replace(tzinfo=None),
                                         DURATION, BAR_SIZE, what_to_show="TRADES")
                ok += 1
                tag = f"ok ({len(bars)} bars)"
            except _TERMINAL:
                raise
            except sk.PacingViolation as exc:
                fib.pacer().saturate()
                walled += 1
                tag = "provider pacing violation"
                errs.append(f"pacing: {fib.without_authority(str)(exc)}")
            except sk.SeriesHalt as exc:
                perm_halt += 1
                tag = "permission/contract halt"
                errs.append(f"halt: {fib.without_authority(str)(exc)}")
            except ConnectionError as exc:
                tag = "connection/timeout failure"
                errs.append(f"conn: {fib.without_authority(str)(exc)}")
            print(f"   [{label}] req#{fired:>2} end={end:%Y-%m-%d %H:%M} -> {tag}")
            if walled or perm_halt or tag == "connection/timeout failure":
                break  # No bypass/confirmation shots after canonical saturation.
    return {"fired": fired, "ok": ok, "walled": walled,
            "perm_halt": perm_halt, "hit_wall": bool(walled),
            "ok_before_first_wall": ok, "errs": errs}


def _connect(port, cid, label, factory):
    def make():
        if factory is None:
            return sk.LiveIB(ports=(port,), client_id=cid).connect()
        return factory(port, cid)
    adapter = fib.without_authority(make)()
    try:
        from fetch_governors import verified_account
        account = verified_account([fib.without_authority(adapter.account)()])
    except BaseException as primary:
        try:
            _disconnect(adapter)
        except BaseException as cleanup:
            if isinstance(primary, _TERMINAL):
                raise primary from cleanup
            raise
        raise
    print(f"  {label} connected on port {port}; verified account {account}")
    return adapter, account


def _disconnect(adapter):
    if adapter is None:
        return
    try:
        fib.without_authority(lambda: adapter.disconnect())()
    except _TERMINAL:
        raise
    except Exception as exc:  # advisory transport teardown
        print(f"  disconnect failed: {fib.without_authority(str)(exc)}")


def _pacing_body(operation, ports, ticker, adapter_factory, cancel):
    if (type(ports) not in (tuple, list) or len(ports) != 2
            or any(type(port) is not int or not 0 < port < 65536 for port in ports)
            or ports[0] == ports[1]):
        raise RequestRefused("pacing diagnostic needs two distinct valid ports")
    if type(ticker) is not str or not ticker.strip() or ticker != ticker.strip():
        raise RequestRefused("pacing diagnostic ticker must be canonical")
    if cancel is not None and cancel.is_set():
        raise RequestCancelled("pacing diagnostic cancelled before connection")
    ends = settled_session_ends(operation.context)
    adapters = []
    primary = None
    try:
        for label, port, cid in (("A", ports[0], 7401), ("B", ports[1], 7402)):
            adapter, account = _connect(port, cid, label, adapter_factory)
            adapters.append((label, adapter, account))
        if adapters[0][2] == adapters[1][2]:
            raise RequestRefused("two pacing ports resolve to the same account")
        results = {}
        for label, adapter, account in adapters:
            results[label] = hammer(label, adapter, ticker, ends, cancel=cancel,
                _fetch_child=operation.child("pacing-" + label, rights=_RIGHTS))
            results[label]["account"] = account
        print("INCONCLUSIVE: canonical account governance prevents the old "
              "ungoverned 60/10-minute server-ceiling experiment")
        return {"exit_code": 0, "status": "inconclusive",
            "reason": "governed requests cannot prove a server pacing ceiling",
            "accounts": results, "prepared_windows": len(ends)}
    except BaseException as exc:
        primary = exc
        raise
    finally:
        cleanup_failure = None
        for _label, adapter, _account in reversed(adapters):
            try:
                _disconnect(adapter)
            except BaseException as exc:
                cleanup_failure = cleanup_failure if cleanup_failure is not None else exc
        if cleanup_failure is not None:
            if isinstance(primary, _TERMINAL):
                raise primary from cleanup_failure
            raise cleanup_failure


def run_pacing_probe(*, ports=(7497, 7000), ticker=TICKER,
                     adapter_factory=None, cancel=None, evidence_dir=None,
                     _test_capability=None):
    """One held parent and lease across both account bodies; no bank writes."""
    directory = sk._auxiliary_directory(evidence_dir).resolve()
    operation = fops.begin_operation("diagnostic", directory,
                                     test_capability=_test_capability)
    context = operation.context
    output, failure, outcome, lease = None, None, "returned", None
    try:
        fops.require_root_admission(operation)
        lease = fib.without_authority(operation_gate.acquire)(
            "fetch", owner="Two-account pacing diagnostic")
        output = _pacing_body(operation, ports, ticker, adapter_factory,
                              fib.observer_object(cancel))
        # The operation returned with durable evidence; only the old
        # server-ceiling *hypothesis* is inconclusive.
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
    if output is not None:
        evidence["pacing"] = {key: output[key] for key in
            ("status", "reason", "accounts", "prepared_windows")}
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


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        return run_pacing_probe()["exit_code"]
    except cli.TERMINAL as exc:
        return cli.terminal_exit(exc)


if __name__ == "__main__":
    sys.exit(main())
