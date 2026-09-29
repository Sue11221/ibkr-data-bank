"""LIVE — repair interior 1m gaps for a ticker by re-fetching fresh and MERGING
into the bank. Non-destructive: re-fetches into a temp via gap_fill (gated,
validated), then _commit_month merges each month into the bank — ADDING the
missing minutes and KEEPING existing bars on any conflict. The ticker's 1d (and
any other) series are untouched. OFF-PEAK; needs ports.

    python live_repair_gaps.py --ticker ADP --since 2011-01-01
"""
import argparse
import shutil
import sys
import tempfile
from datetime import date
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import stock_ibkr as sk              # noqa: E402
import stock_storage as ss           # noqa: E402
import operation_gate                # noqa: E402
from live_internal_revalidate import _bank_root, _free_ports   # noqa: E402


def _market_operation_path(bank):
    """The root-local market-data fence shared with writing audits."""
    return (Path(bank).resolve(strict=False).parent / "Run Logs"
            / ".market_data_operation.lock")


def _merge_temp_months(bank, temp, ticker, conid=None):
    """Merge temp months without retaining a stale manifest object.

    ``stock_ibkr._commit_month`` owns the shared ticker transaction and, with
    ``mstate=None``, reloads/publishes the current manifest inside that same
    critical section for every month. Keeping one manifest across separately
    locked calls would let the final save erase correction evidence committed
    by another writer between months.
    """
    bank = Path(bank)
    lease = operation_gate.acquire(
        "live_repair_merge", owner=f"live gap repair merge {ticker}",
        path=_market_operation_path(bank))
    with lease:
        res = {"blocked_months": [], "dup_existing": 0, "conflicts": 0,
               "months": {}, "added": 0, "written": 0}
        n = 0
        for tf in sorted((Path(temp) / ticker).rglob("*_1m.parquet")):
            mm = ss.FILENAME_RE.match(tf.name)
            if not mm or mm.group(4) != "1m":
                continue
            y, mo = int(mm.group(2)), int(mm.group(3))
            try:
                fresh, _ = ss.read_month_file(tf)
            except (ss.StorageError, OSError):
                continue
            sk._commit_month(
                bank, ticker, "1m", (y, mo), fresh, "gap-repair",
                "IBKR:repair", res, (lambda *a: None), conid=conid,
                mstate=None)
            n += 1
        return n, res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ticker", required=True)
    ap.add_argument("--since", default="2011-01-01")
    ap.add_argument("--port", type=int, default=0)
    args = ap.parse_args(argv)

    bank = _bank_root()
    tk = args.ticker.strip().upper()
    if not (bank / tk).exists():
        print(f"{tk} not in the bank.")
        return 2
    port = args.port or (_free_ports() or [2000])[0]
    since = date.fromisoformat(args.since)
    today = sk.now_ny().date()

    # 1) re-fetch fresh into a TEMP bank
    temp = Path(tempfile.mkdtemp(prefix="repair_")) / ss.STORAGE_DIR_NAME
    factory = sk.live_adapter_factory(host=sk.HOST_DEFAULT, ports=(port,),
                                      client_id=sk.CLIENT_ID_FETCH)
    print(f"[1] re-fetching {tk} 1m {since}..{today} on port {port} (temp)…",
          flush=True)
    rep = sk.gap_fill(temp, [(tk, "1m")], progress=lambda m: None,
                      adapter_factory=factory, pacer=sk.Pacer(),
                      today=today, since=since)
    s = (rep.get("series") or [{}])[0]
    print(f"    fetched: added={s.get('added')} halt={s.get('halt')}", flush=True)

    # 2) merge each fresh month into the BANK (engine merge + manifest)
    print("[2] merging fresh months into the bank…", flush=True)
    bman = ss.load_manifest(bank / tk)
    conid = (bman or {}).get("conid")
    try:
        n, res = _merge_temp_months(bank, temp, tk, conid=conid)
    except operation_gate.OperationGateError as exc:
        print(f"    merge refused before production writes: {exc}", flush=True)
        print(f"    fetched temp data retained for retry: {temp}", flush=True)
        return 3
    print(f"    merged {n} fresh months -> +{res['added']:,} minutes ADDED, "
          f"{res['dup_existing']:,} already-present, {res['conflicts']} conflicts, "
          f"{res['written']} month files rewritten", flush=True)
    shutil.rmtree(temp.parent, ignore_errors=True)
    print("DONE. Re-run the gap scan to confirm the holes are filled.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
