"""Acceptance harness: multi-port progress meter accuracy (Row 49).

CLAUDE-OWNED REFERENCE HARNESS - Codex MUST NOT edit; assertion changes require
Claude sign-off on the board (PROGRESS_METER_ACCURACY_PLAN.md).

Offline and deterministic: drives the REAL gap_fill_parallel (unwrapped, fake
adapters, real HardDeathWatchdog with tightened timings) with a scripted inner
gap_fill that mirrors the production announce-BEFORE-fetch ordering, so a hard
failure after the announcement reproduces the user's overshooting meter
(501/493). No network, no GUI, no bank writes outside temp roots. Reports via
the Row 41 check_kit. Exit contract: 1 = check failed; 3 = green but the M1
flag is absent (expected pre-M1); 0 = acceptance.
"""

from __future__ import annotations

import datetime as dt
import re
import sys
import tempfile
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

ENGINE_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE_ROOT))

from check_kit import CheckKit  # noqa: E402
import addstock_watchdog as aw  # noqa: E402
import stock_ibkr as sk  # noqa: E402
from testbank import build_bank, isolated_gates  # noqa: E402

KIT = CheckKit()
check = KIT.check
section = KIT.section

MARKER = re.compile(r"^\[(\d+)/(\d+)\]\s+(\S+)\s+(\S+)\s+\((port \d+[^)]*)\)$")
FOUR = [("AAA", "1m"), ("BBB", "1m"), ("CCC", "1m"), ("DDD", "1m")]


def drive(temp_root, selections, ports, fail_once, port_up, *, today=None,
          allow_date_split=False, date_split_min_months=None):
    """Run the real parallel runner with scripted failures; return
    (report, parsed_markers) where each marker is
    (n, total, ticker, interval, suffix)."""
    lines = []
    attempts = {port: 0 for port in ports}

    class Adapter:
        def __init__(self, port):
            self.port = port

        def is_connected(self):
            return True

        def disconnect(self):
            return None

        def account(self):
            return f"DU{self.port}"

        def series_gate(self):
            attempts[self.port] += 1
            if fail_once(self.port, attempts[self.port]):
                raise sk.ConnectionLost(f"injected drop on {self.port}")

    def factory(_host, factory_ports):
        return lambda: Adapter(int(factory_ports[0]))

    def fake_gap(_root, series, progress, _cancel, adapter_factory, _pacer,
                 *_args, **kwargs):
        adapter = adapter_factory()
        rows = []
        for index, (ticker, interval) in enumerate(series, 1):
            if progress is not None:
                progress(f"[{index}/{len(series)}] {ticker} {interval}")
            adapter.series_gate()          # announce-first, exactly like :4162
            row = {"ticker": ticker, "interval": interval, "added": 1}
            rows.append(row)
            callback = kwargs.get("on_series")
            if callback is not None:
                callback(ticker, interval, row)
        return {
            "run": f"fake-{adapter.port}", "root": str(_root),
            "account": adapter.account(), "port": adapter.port,
            "series": rows, "cancelled": False,
            "totals": {"added": len(rows), "dup_existing": 0, "conflicts": 0,
                       "written": len(rows), "requests": len(rows),
                       "bars_fetched": len(rows), "halted_series": 0,
                       "write_failed": 0},
            "spot_checks_run": 0,
        }

    def watchdog_factory(wd_ports, maintenance_provider=None):
        return aw.HardDeathWatchdog(
            wd_ports, maintenance_provider=maintenance_provider,
            port_grace_s=0.05, fleet_grace_s=2.0,
            probe_backoff_s=(0.01, 0.02))

    kwargs = {}
    if today is not None:
        kwargs["today"] = today
    if allow_date_split:
        kwargs["allow_date_split"] = True
        kwargs["date_split_min_months"] = date_split_min_months
    original = sk.gap_fill
    sk.gap_fill = fake_gap
    try:
        report = sk.gap_fill_parallel.__wrapped__(
            temp_root, selections, ports, progress=lines.append,
            adapter_factory=factory, port_up=port_up,
            hard_death_watchdog=True, watchdog_factory=watchdog_factory,
            **kwargs)
    finally:
        sk.gap_fill = original

    markers = []
    for raw in lines:
        m = MARKER.match(str(raw))
        if m:
            markers.append((int(m.group(1)), int(m.group(2)),
                            m.group(3), m.group(4), m.group(5)))
    return report, markers


def series_counts(markers):
    seen = {}
    for _n, _total, ticker, interval, _suffix in markers:
        seen[(ticker, interval)] = seen.get((ticker, interval), 0) + 1
    return seen


def main():
    print("=== Multi-port progress meter accuracy (Row 49 reference) ===\n")
    flag = getattr(sk, "PROGRESS_UNIQUE_COUNT", None) is True

    with tempfile.TemporaryDirectory(prefix="fetch_progress_ref_") as temp:
        base = Path(temp)

        section("[B] engine truth: what the global [n/total] meter counts")
        clean_root = base / "clean"
        clean_root.mkdir()
        report, markers = drive(
            clean_root, FOUR, [2000, 3000],
            lambda _port, _attempt: False, lambda _port: True)
        ns = [n for n, *_rest in markers]
        totals = {total for _n, total, *_rest in markers}
        check("B3 clean run: meter is exact - markers are 1..4 of 4, no "
              "resumed text, every planned series completes exactly once",
              sorted(ns) == [1, 2, 3, 4] and max(ns) == 4
              and totals == {4}
              and all("resumed" not in suffix
                      for *_head, suffix in markers)
              and {(row["ticker"], row["interval"])
                   for row in report["series"]} == set(FOUR)
              and set(series_counts(markers)) == set(FOUR),
              repr(markers))

        reroute_root = base / "reroute"
        reroute_root.mkdir()
        report, markers = drive(
            reroute_root, FOUR, [2000, 3000],
            lambda port, attempt: port == 2000 and attempt == 1,
            lambda port: port == 3000)
        ns = [n for n, *_rest in markers]
        counts = series_counts(markers)
        doubled = {key for key, count in counts.items() if count == 2}
        completed = {(row["ticker"], row["interval"])
                     for row in report["series"]}
        if not flag:
            check("B1 defect doc: one hard reroute makes the meter OVERSHOOT "
                  "its total (final 5/4 - the user's 501/493) while the run "
                  "itself completes every series exactly once",
                  max(ns) == 5 and totals == {4}
                  and completed == set(FOUR)
                  and report["watchdog"]["rerouted_ports"] == [2000],
                  f"ns={ns} rerouted="
                  f"{(report.get('watchdog') or {}).get('rerouted_ports')}")
            check("B2 defect doc: the overshoot is the SAME series announced "
                  "on both ports - announcement-counting, not extra work",
                  len(doubled) == 1
                  and all(count == 1 for key, count in counts.items()
                          if key not in doubled),
                  repr(counts))

        if flag:
            section("[F] feature: deduped meter, resumed lines, date-split")
            check("F1 M1 flag present (stock_ibkr.PROGRESS_UNIQUE_COUNT)",
                  flag)
            resumed = [m for m in markers if "resumed" in m[4]]
            plain = [m for m in markers if "resumed" not in m[4]]
            check("F2 the SAME reroute now caps at total: n never exceeds 4, "
                  "the re-pickup is still EMITTED as a parseable resumed "
                  "line, the reroute still happens, all series complete",
                  max(ns) == 4 and totals == {4}
                  and sorted(n for n, *_r in plain) == [1, 2, 3, 4]
                  and len(resumed) == 1
                  and resumed[0][2:4] in {tuple(k) for k in doubled or
                                          {resumed[0][2:4]}}
                  and completed == set(FOUR)
                  and report["watchdog"]["rerouted_ports"] == [2000],
                  repr(markers))

            split_root = base / "datasplit"
            bank = split_root / "bank"
            build_bank(bank, {"DSP": {"1m": ["2026-03", "2026-04"]}})
            report, markers = drive(
                bank, [("DSP", "1m")], [2000, 3000, 4000],
                lambda _port, _attempt: False, lambda _port: True,
                today=dt.date(2026, 7, 20), allow_date_split=True,
                date_split_min_months=1)
            ns = [n for n, *_rest in markers]
            totals = {total for _n, total, *_rest in markers}
            split_total = max(totals) if totals else 0
            check("F3 date-split fence: the SAME series in multiple jobs "
                  "still reaches exactly total/total with no resumed lines - "
                  "a name-only dedupe key fails here",
                  split_total >= 2 and sorted(ns) == list(
                      range(1, split_total + 1))
                  and all("resumed" not in suffix
                          for *_head, suffix in markers),
                  f"markers={markers!r} report_series="
                  f"{len(report.get('series') or [])}")

    if not flag:
        KIT.pending(
            "M1",
            "stock_ibkr.PROGRESS_UNIQUE_COUNT absent - not implemented",
            "D1: dedupe the global [n/total] counter by per-job series key "
            "(job seq + ticker + interval; never id(job), never name-only)",
            "re-pickups emit a parseable resumed line without advancing n",
            "acceptance: this reference exit 0 x3",
        )
        return KIT.finish(feature_absent=True)
    return KIT.finish()


if __name__ == "__main__":
    with isolated_gates():
        raise SystemExit(main())
