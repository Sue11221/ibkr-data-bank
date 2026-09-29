"""Acceptance harness for the export progress uniformity plan (M0, owner: CLAUDE).

Contract under test (EXPORT_PROGRESS_UNIFORMITY_PLAN.md):
  export_batch.run_batch must emit, alongside the existing item_* events, a
  monotonic batch-level progress stream:

      {"kind": "aggregate", "units_done": int, "units_total": int,
       "files_done": int, "files_total": int, "active": [tickers...]}

  - one work unit = one calendar month-slice of one PRESENT ticker;
  - units_total is FIXED for the life of the batch (pre-scanned denominator);
  - units_done never decreases and equals units_total when the batch state is
    COMPLETE; a cancelled batch ends truthfully BELOW units_total;
  - emission is coalesced by export_batch.AGGREGATE_EMIT_MIN_S seconds
    (0.0 -> emit on every ledger update; this harness pins that override);
  - progress reporting stays OBSERVE-ONLY: exported bytes are identical with
    and without a progress callback;
  - no event of any kind is emitted after run_batch returns.

Exit codes:
  0 = M1 accepted (baseline + full aggregate contract green)
  3 = feature absent (baseline green; aggregate surface not implemented yet)
  1 = failure (baseline regression, or aggregate contract violation)

Offline by construction: synthetic bank in a temp dir, exports to temp dirs,
no GUI, no network, no ports, no real-bank access. Safe beside a live run.

Codex MUST NOT weaken or remove assertions here; harness changes require
Claude sign-off (EXPORT_PROGRESS_UNIFORMITY_PLAN.md section 4).
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE), str(_HERE.parent)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import export_batch as batch            # noqa: E402
import export_designer                  # noqa: E402
import stock_storage as storage         # noqa: E402


FAILURES = []
COUNT = [0]
NOW = dt.datetime(2026, 7, 20, 15, 0, 0,
                  tzinfo=dt.timezone(dt.timedelta(hours=-4)))
# months per ticker (consecutive from 2023-01) -> units_total = 63
GEOMETRY = {"AAA": 1, "BBB": 2, "CCC": 4, "DDD": 8, "EEE": 16, "FFF": 32}
UNITS_TOTAL = sum(GEOMETRY.values())
SELECTED_PRESENT = list(GEOMETRY)
SELECTED = list(GEOMETRY) + ["ZZZ"]          # ZZZ is not in the bank
SPEC = {"file_type": "csv", "base_interval": "1m", "sessions": ["rth"],
        "range_preset": "furthest"}


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def section(title):
    print(f"=== {title} ".ljust(68, "="))


# --- synthetic bank ----------------------------------------------------------

def _first_weekday(y, m):
    d = dt.date(y, m, 1)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d


def seed_bank(root):
    """Write GEOMETRY as real month files + manifests (selftest pattern)."""
    for offset, (ticker, n_months) in enumerate(sorted(GEOMETRY.items())):
        tdir = Path(root) / ticker
        tdir.mkdir(parents=True)
        man = storage.new_manifest(ticker, ticker)
        man["conid"] = 20_000 + sum(ord(c) for c in ticker)
        months = storage.manifest_months(man, "1m")
        for i in range(n_months):
            y, m = 2023 + i // 12, 1 + i % 12
            day = _first_weekday(y, m)
            base = 10.0 + offset + i * 0.25
            bars = [
                (dt.datetime(y, m, day.day, 9, 30), base, base + 1.0,
                 base - 0.5, base + 0.5, 100 + i),
                (dt.datetime(y, m, day.day, 9, 31), base + 0.5, base + 1.5,
                 base, base + 1.0, 200 + i),
            ]
            path = storage.month_file_path(root, ticker, y, m, "1m",
                                           fmt="csv")
            months[storage.month_key(y, m)] = \
                storage.write_month_file(path, bars)
        storage.save_manifest(tdir, man)


# --- event collection --------------------------------------------------------

class Collector:
    """Thread-safe progress recorder with an optional per-event trigger."""

    def __init__(self, on_event=None):
        self.events = []
        self._lock = threading.Lock()
        self._on_event = on_event

    def __call__(self, event):
        record = dict(event)
        if isinstance(record.get("detail"), dict):
            record["detail"] = dict(record["detail"])
        with self._lock:
            self.events.append(record)
        if self._on_event is not None:
            self._on_event(record)

    def kind(self, name):
        with self._lock:
            return [e for e in self.events if e.get("kind") == name]

    def __len__(self):
        with self._lock:
            return len(self.events)


@contextlib.contextmanager
def _patched(**over):
    """Override module constants when they exist (no-op pre-M1)."""
    saved = {}
    for key, value in over.items():
        if hasattr(batch, key):
            saved[key] = getattr(batch, key)
            setattr(batch, key, value)
    try:
        yield
    finally:
        for key, value in saved.items():
            setattr(batch, key, value)


def run_designer(bank, parent, collector, *, workers, cancel=None,
                 selected=None):
    Path(parent).mkdir(parents=True, exist_ok=True)
    with _patched(AGGREGATE_EMIT_MIN_S=0.0):
        return batch.run_designer_batch(
            bank, list(SELECTED if selected is None else selected),
            list(GEOMETRY), parent, dict(SPEC),
            workers=workers, progress=collector, cancel=cancel, now=NOW)


def silence_after_return(collector, label):
    n = len(collector)
    time.sleep(0.30)
    check(f"{label}: no event of any kind after run_batch returned",
          len(collector) == n,
          f"{len(collector) - n} late event(s)")


def data_file_shas(folder):
    out = {}
    for path in sorted(Path(folder).glob("*_1m.csv")):
        out[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def monotone(values):
    return all(b >= a for a, b in zip(values, values[1:]))


# --- scenarios ---------------------------------------------------------------

def main():
    print("=== export_progress_reference ===")
    project = Path(tempfile.mkdtemp(prefix="export-progress-ref-"))
    try:
        bank = project / "bank"
        bank.mkdir()
        seed_bank(bank)

        # [B] baseline: today's stream — these stay green forever -------------
        section("[B1] sequential run: raw per-ticker stream is complete")
        seq = Collector()
        r1 = run_designer(bank, project / "seq", seq, workers=1)
        starts, dones = seq.kind("item_start"), seq.kind("item_done")
        months = [e for e in seq.kind("item_progress")
                  if isinstance(e.get("detail"), dict)]
        check("B1: one item_start and one item_done per present ticker",
              sorted(e.get("ticker") for e in starts) == sorted(GEOMETRY)
              and sorted(e.get("ticker") for e in dones) == sorted(GEOMETRY))
        check("B1: item_done 'done' counter reaches every present ticker",
              max((e.get("done", 0) for e in dones), default=0)
              == len(GEOMETRY))
        check("B1: exactly one month event per seeded month-slice "
              f"({UNITS_TOTAL})",
              len(months) == UNITS_TOTAL,
              f"saw {len(months)}")
        per_ticker_totals = {
            e.get("ticker"): e["detail"].get("month_total")
            for e in months
        }
        check("B1: per-ticker month_total equals the seeded geometry",
              per_ticker_totals == GEOMETRY, repr(per_ticker_totals))
        check("B1: an absent selected ticker yields PARTIAL + missing list "
              "(not_in_bank counts as a failure by design)",
              r1.get("state") == "PARTIAL"
              and r1.get("missing") == ["ZZZ"])
        silence_after_return(seq, "B1")

        section("[B2] the defect on record: raw stream cannot drive a bar")
        fractions = [e["detail"]["month_index"] / e["detail"]["month_total"]
                     for e in months]
        check("B2: naive per-event month fraction is NON-monotonic even "
              "sequentially (per-ticker counters reset)",
              not monotone(fractions))

        section("[B3] observe-only: bytes identical with and without progress")
        par = Collector()
        r_obs = run_designer(bank, project / "with-progress", par,
                             workers=4, selected=SELECTED_PRESENT)
        (project / "no-progress").mkdir(parents=True, exist_ok=True)
        with _patched(AGGREGATE_EMIT_MIN_S=0.0):
            r_none = batch.run_designer_batch(
                bank, SELECTED_PRESENT, list(GEOMETRY),
                project / "no-progress", dict(SPEC), workers=4,
                progress=None, cancel=None, now=NOW)
        shas_obs = data_file_shas(r_obs["folder"])
        shas_none = data_file_shas(r_none["folder"])
        check("B3: both runs export one data file per present ticker",
              len(shas_obs) == len(GEOMETRY)
              and sorted(shas_obs) == sorted(shas_none))
        check("B3: exported data bytes are IDENTICAL with progress attached",
              shas_obs == shas_none)
        check("B3: result summaries agree (rows, files_written)",
              r_obs["summary"]["rows"] == r_none["summary"]["rows"]
              and r_obs["summary"]["files_written"]
              == r_none["summary"]["files_written"]
              and r_obs["state"] == r_none["state"] == "COMPLETE")
        silence_after_return(par, "B3")

        section("[B4] cancel mid-batch stays clean (sequential, deterministic)")
        cancel_ev = threading.Event()
        stop = Collector(on_event=lambda e: (
            cancel_ev.set() if e.get("kind") == "item_done" else None))
        r_cancel = run_designer(bank, project / "cancelled", stop,
                                workers=1, cancel=cancel_ev,
                                selected=SELECTED_PRESENT)
        check("B4: batch reports CANCELLED with exactly one exported ticker",
              r_cancel.get("state") == "CANCELLED"
              and r_cancel["summary"]["files_written"] == 1
              and r_cancel["summary"]["cancelled"] == len(GEOMETRY) - 1)
        silence_after_return(stop, "B4")

        if FAILURES:
            return 1

        # [probe] does the M1 aggregate surface exist? ------------------------
        section("[probe] M1 aggregate surface")
        aggregates = par.kind("aggregate")
        if not aggregates:
            print("[M1 PENDING] no {'kind': 'aggregate'} events in the "
                  "stream and/or export_batch.AGGREGATE_EMIT_MIN_S absent.")
            print("  Required event: {'kind': 'aggregate', 'units_done': int,"
                  " 'units_total': int, 'files_done': int,"
                  " 'files_total': int, 'active': [tickers]}")
            print("  See EXPORT_PROGRESS_UNIFORMITY_PLAN.md section 3 (M1).")
            return 3

        # [A] aggregate contract ----------------------------------------------
        section("[A] aggregate contract")
        check("A1: every aggregate carries the full typed contract",
              all(isinstance(e.get("units_done"), int)
                  and isinstance(e.get("units_total"), int)
                  and isinstance(e.get("files_done"), int)
                  and isinstance(e.get("files_total"), int)
                  and isinstance(e.get("active"), list)
                  for e in aggregates))
        check("A2: units_total is the FIXED pre-scanned denominator "
              f"({UNITS_TOTAL})",
              {e.get("units_total") for e in aggregates} == {UNITS_TOTAL}
              and {e.get("files_total") for e in aggregates}
              == {len(GEOMETRY)})
        done_series = [e["units_done"] for e in aggregates]
        check("A3: units_done NEVER decreases (parallel workers, one ledger)",
              monotone(done_series))
        check("A4: terminal exactness on COMPLETE "
              f"(units {UNITS_TOTAL}/{UNITS_TOTAL}, files "
              f"{len(GEOMETRY)}/{len(GEOMETRY)})",
              done_series and done_series[-1] == UNITS_TOTAL
              and aggregates[-1].get("files_done") == len(GEOMETRY))
        max_step = max((b - a for a, b in zip(done_series, done_series[1:])),
                       default=0)
        check("A5: uniform motion — no aggregate step exceeds 8 units "
              "(no per-ticker reset or cross-ticker collapse)",
              max_step <= 8, f"max step {max_step}")
        check("A6: the bar visibly moves — at least "
              f"{UNITS_TOTAL // 2} distinct units_done values",
              len(set(done_series)) >= UNITS_TOTAL // 2,
              f"saw {len(set(done_series))}")
        seq_aggregates = seq.kind("aggregate")
        check("A7a: the sequential legacy path emits the same aggregate "
              "stream (monotone, fixed denominator, terminal-exact)",
              bool(seq_aggregates)
              and monotone([e["units_done"] for e in seq_aggregates])
              and {e.get("units_total") for e in seq_aggregates}
              == {UNITS_TOTAL}
              and seq_aggregates[-1]["units_done"] == UNITS_TOTAL)
        stop_aggregates = stop.kind("aggregate")
        check("A7b: a CANCELLED batch ends truthfully BELOW units_total "
              "(no snap-to-100 on cancel)",
              bool(stop_aggregates)
              and monotone([e["units_done"] for e in stop_aggregates])
              and stop_aggregates[-1]["units_done"] < UNITS_TOTAL)
        check("A8: raw item_* events still flow unchanged beside aggregates",
              len([e for e in par.kind("item_progress")
                   if isinstance(e.get("detail"), dict)]) == UNITS_TOTAL
              and len(par.kind("item_done")) == len(GEOMETRY))
        rendered = batch.progress_text(aggregates[-1])
        check("A9: progress_text renders an aggregate as stable overall "
              "progress (mentions both unit and file counters)",
              str(UNITS_TOTAL) in rendered and str(len(GEOMETRY)) in rendered)
        return 1 if FAILURES else 0
    finally:
        shutil.rmtree(project, ignore_errors=True)


if __name__ == "__main__":
    code = main()
    print(f"\n{COUNT[0]} checks, {len(FAILURES)} failed")
    if FAILURES:
        print("FAILURES: " + ", ".join(FAILURES))
    else:
        print("ALL PASS" + (" (feature pending)" if code == 3 else ""))
    print(f"harness exit={code}")
    sys.exit(code)
