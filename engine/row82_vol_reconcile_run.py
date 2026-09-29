"""Row 82 — bounded exact-day volatility reconcile retry (CLAUDE-EXECUTED).

User order 2026-07-31: "approve and you should run it youself and not codex."

Drives the PRODUCTION seam only (R1 same entry point, R2 same gate):

    operation_gate.acquire("fetch")            # production boundary, whole run
    vol_value_audit.audit(..., write_queue=True, operation_mode="fetch")
    vol_value_reconcile.plan(root, tickers=[t], kinds=["1m-iv"])
    vol_value_reconcile.reconcile_one(adapter, pacer, root, row, run_id)
    vol_value_reconcile.write_artifact(...)

This module adds NO correction path of its own.  Every decision — settle,
replace, or leave untouched — belongs to vol_value_reconcile's own guards:
hard-invalid never settles, the served grid/count must match the stored day,
the stored close must be unchanged since queue publication, and any failure
leaves the durable queue row and every bank byte untouched.

Self-locating: resolves the project root from this file, never a stored path.
Read-only except through the production reconcile seam.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parent.parent
BANK = ROOT / "Stock Data Storage"
RUN_LOGS = ROOT / "Run Logs"
for p in (str(ROOT), str(ROOT / "engine")):
    if p not in sys.path:
        sys.path.insert(0, p)

import operation_gate                      # noqa: E402
import stock_ibkr                          # noqa: E402
import vol_value_audit                     # noqa: E402
import vol_value_reconcile                 # noqa: E402

# stock_ibkr.PORTS_DEFAULT is (7497, 4002, 7496, 4001) — this machine's fleet
# listens on 2000/3000/4000 instead, so the ports are DISCOVERED from the live
# listeners rather than assumed (portability rule: machine facts discovered,
# never stored).  A first run against the defaults connected to nothing and
# every row failed "refetch could not start" with request_count=0.
FLEET_CANDIDATES = (2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000,
                    7497, 4002, 7496, 4001)


def live_ports():
    import socket
    found = []
    for port in FLEET_CANDIDATES:
        s = socket.socket()
        s.settimeout(0.25)
        try:
            s.connect(("127.0.0.1", port))
            found.append(port)
        except OSError:
            pass
        finally:
            try:
                s.close()
            except OSError:
                pass
    return tuple(found)


TARGETS = [
    ("GEN",  "2016-02-24", 0.93754843),
    ("MTB",  "2016-08-10", 0.18792242),
    ("PNC",  "2016-08-10", 89.14690244),
    ("PPG",  "2015-06-15", 0.21028961),
    ("TSLA", "2020-03-17", 0.67904795),
]
KIND = "1m-iv"


def _utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bank_inventory():
    """Over-inclusive custody: every json/txt beneath the production bank."""
    files = sorted((p for p in BANK.rglob("*")
                    if p.is_file() and p.suffix.lower() in (".json", ".txt")),
                   key=lambda p: str(p).casefold())
    h = hashlib.sha256()
    total = 0
    for p in files:
        raw = p.read_bytes()
        total += len(raw)
        h.update(str(p.relative_to(BANK)).replace("\\", "/").casefold().encode())
        h.update(hashlib.sha256(raw).digest())
    return {"count": len(files), "bytes": total, "inventory_sha256": h.hexdigest()}


def queue_state():
    p = BANK / "_vol_value_queue.json"
    if not p.exists():
        return {"present": False}
    raw = p.read_bytes()
    doc = json.loads(raw.decode("utf-8"))
    return {"present": True, "bytes": len(raw), "sha256": _sha(raw),
            "row_count": len(doc.get("rows") or [])}


def month_state(ticker, day):
    """Exact stored month file for the targeted IV day."""
    y, m = day[:4], day[5:7]
    import calendar as _cal
    mdir = f"{m}-{_cal.month_abbr[int(m)]}"
    f = BANK / ticker / y / mdir / f"{ticker}_{y}-{m}_{KIND}.parquet"
    if not f.exists():
        return {"path": str(f.relative_to(BANK)).replace("\\", "/"),
                "present": False}
    raw = f.read_bytes()
    return {"path": str(f.relative_to(BANK)).replace("\\", "/"),
            "present": True, "bytes": len(raw), "sha256": _sha(raw)}


def main(dry_run=False):
    run_id = f"row82-vol-reconcile-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
    record = {
        "kind": "row82_vol_value_reconcile_retry",
        "version": 1,
        "run_id": run_id,
        "started_at": _utc(),
        "authorization": {
            "user_order": "approve and you should run it youself and not codex",
            "order_date": "2026-07-31",
            "executed_by": "CLAUDE",
            "log_checkpoint": "eed9a59",
        },
        "scope": ("bounded exact-day vol_value_reconcile retry on five 1m-iv "
                  "hard-invalid rows; production seam only; no Claude "
                  "correction path"),
        "targets": [{"ticker": t, "day": d, "queued_value": v}
                    for t, d, v in TARGETS],
        "dry_run": bool(dry_run),
    }

    record["custody_before"] = bank_inventory()
    record["queue_before"] = queue_state()
    record["months_before"] = {f"{t} {d}": month_state(t, d)
                              for t, d, _v in TARGETS}
    record["gate_status_before"] = operation_gate.status()

    if dry_run:
        record["result"] = "dry_run"
        record["finished_at"] = _utc()
        return record

    rows_out = []
    requested = 0
    lease = None
    try:
        lease = operation_gate.acquire("fetch", owner="Claude Row 82 reconcile")
        record["gate"] = {"acquired": True, "mode": "fetch",
                          "owner": "Claude Row 82 reconcile"}
        ports = live_ports()
        record["fleet_ports"] = list(ports)
        if not ports:
            raise RuntimeError("no TWS listener found on any candidate port")
        make = stock_ibkr.live_adapter_factory(ports=ports)
        adapter = stock_ibkr.ReusableAdapter(make)
        pacer = stock_ibkr.Pacer()          # ONE budget per account
        try:
            for ticker, day, _value in TARGETS:
                entry = {"ticker": ticker, "day": day}
                try:
                    report = vol_value_audit.audit(
                        BANK, tickers=[ticker], write_queue=True,
                        operation_mode="fetch")
                    entry["reaudit_complete"] = report.get("complete")
                    if report.get("complete") is not True:
                        entry["outcome"] = "skipped"
                        entry["error"] = "focused re-audit incomplete"
                        rows_out.append(entry)
                        continue
                    planned = [
                        r for r in vol_value_reconcile.plan(
                            BANK, tickers=[ticker], kinds=[KIND])
                        if str(r.get("day")) == day
                    ]
                    entry["planned_rows"] = len(planned)
                    if not planned:
                        entry["outcome"] = "absent_from_queue"
                        rows_out.append(entry)
                        continue
                    result = vol_value_reconcile.reconcile_one(
                        adapter, pacer, BANK, planned[0], run_id)
                    entry["result"] = result
                    entry["outcome"] = result.get("status")
                    entry["request_count"] = result.get("request_count")
                    entry["error"] = result.get("error")
                    requested += int(result.get("request_count") or 0)
                except Exception as exc:  # noqa: BLE001 - record and continue
                    entry["outcome"] = "exception"
                    entry["error"] = f"{type(exc).__name__}: {exc}"
                rows_out.append(entry)
        finally:
            try:
                adapter.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        if lease is not None:
            try:
                lease.release()
                record.setdefault("gate", {})["released"] = True
            except Exception:  # noqa: BLE001
                record.setdefault("gate", {})["released"] = False

    record["rows"] = rows_out
    record["requested_count"] = requested
    record["custody_after"] = bank_inventory()
    record["queue_after"] = queue_state()
    record["months_after"] = {f"{t} {d}": month_state(t, d)
                             for t, d, _v in TARGETS}
    record["gate_status_after"] = operation_gate.status()
    record["bank_unchanged"] = (
        record["custody_before"]["inventory_sha256"]
        == record["custody_after"]["inventory_sha256"])
    record["months_unchanged"] = all(
        record["months_before"][k] == record["months_after"][k]
        for k in record["months_before"])
    record["settled_count"] = sum(
        1 for r in rows_out if r.get("outcome") == "settled")
    record["finished_at"] = _utc()
    return record


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    rec = main(dry_run=dry)
    out = RUN_LOGS / f"{rec['run_id']}.json"
    payload = json.dumps(rec, indent=2, sort_keys=True, default=str)
    out.write_text(payload, encoding="utf-8")
    print(json.dumps({
        "run_id": rec["run_id"],
        "dry_run": rec.get("dry_run"),
        "outcomes": [{"ticker": r.get("ticker"), "day": r.get("day"),
                      "outcome": r.get("outcome"),
                      "requests": r.get("request_count"),
                      "error": (str(r.get("error"))[:120]
                                if r.get("error") else None)}
                     for r in rec.get("rows", [])],
        "requested_count": rec.get("requested_count"),
        "settled_count": rec.get("settled_count"),
        "bank_unchanged": rec.get("bank_unchanged"),
        "months_unchanged": rec.get("months_unchanged"),
        "artifact": str(out),
        "artifact_sha256": _sha(payload.encode("utf-8")),
    }, indent=2))
