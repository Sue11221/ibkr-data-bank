"""Claude-owned acceptance gate for the Export Designer source line.

Offline only: drives the real DataViewerApp._exd_render_source with stub
labels (never imports tkinter) and the real export_designer.source_report
against the bank in this project folder, then asserts the exact user-visible
text of each state. Exit 0 = the source line still tells the truth.

Self-locating: resolves the bank from this file's own location, so the project
folder can be copied anywhere.

Changes to this gate must retain mutation proof against the shipped artifacts.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
import datetime as dt

ROOT = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "engine"))

import export_designer as ed  # noqa: E402

BANK = Path(os.environ.get("EXPORT_SOURCE_LINE_BANK",
                           str(ROOT / "Stock Data Storage")))
FAILED = []


def check(name, ok, detail=""):
    # The rendered text carries ·, ←, → and ■; a cp1252 console would raise on
    # them, turning a readable failure into a traceback.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001 - already utf-8, or not reconfigurable
        pass
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + ("" if ok else f" :: {detail}"))
    if not ok:
        FAILED.append(name)


class StubLabel:
    """Stands in for ttk.Label: records config() and pack visibility."""

    def __init__(self):
        self.text = None
        self.fg = None
        self.visible = False

    def config(self, text=None, foreground=None, **_kw):
        if text is not None:
            self.text = text
        if foreground is not None:
            self.fg = foreground

    def pack(self, **_kw):
        self.visible = True

    def pack_forget(self):
        self.visible = False


class Harness:
    """Minimal object carrying only what _exd_render_source touches."""

    def __init__(self, render, fg_map, months=None):
        if months is not None:
            self._exd_months = months
        self._exd_source_frame = object()
        self._exd_source_spine = StubLabel()
        self._exd_source_stored = StubLabel()
        self._exd_source_derived = StubLabel()
        self._exd_source_kinds = [StubLabel(), StubLabel()]
        self._exd_source_alert = StubLabel()
        self._EXD_SOURCE_FG = fg_map
        self._render = render

    def run(self, report):
        self._render(self, report)
        return self

    def lines(self):
        """Every line the user actually sees, in render order."""
        shown = [self._exd_source_spine, self._exd_source_stored,
                 self._exd_source_derived]
        shown += self._exd_source_kinds + [self._exd_source_alert]
        return [lbl.text for lbl in shown if lbl.visible]


def load_render():
    """Extract the GUI pieces from display_data.py without importing tk.

    The renderer, its severity->colour map, and the pluralisation helper it
    calls are lifted straight out of the class body, so the harness always
    exercises the shipped text rather than a copy that can drift.
    """
    import ast
    src = (ROOT / "display_data.py").read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(src)
    cls = next(n for n in ast.walk(tree)
               if isinstance(n, ast.ClassDef) and n.name == "DataViewerApp")

    def grab(name):
        for n in cls.body:
            if isinstance(n, ast.FunctionDef) and n.name == name:
                n.decorator_list = []   # staticmethod would not exec standalone
                return n
            if (isinstance(n, ast.Assign)
                    and getattr(n.targets[0], "id", "") == name):
                return n
        raise AssertionError(f"DataViewerApp.{name} not found in display_data.py")

    ns = {"tk": type("tk", (), {"TOP": "top", "X": "x", "LEFT": "left"})}
    for name in ("_EXD_SOURCE_FG", "_exd_months", "_exd_render_source"):
        exec(compile(ast.Module([grab(name)], []),                # noqa: S102
                     f"<{name}>", "exec"), ns)
    return ns["_exd_render_source"], ns["_EXD_SOURCE_FG"], ns["_exd_months"]


def report_from_manifest(manifest, ticker, base_iv, sessions, columns,
                         start_date, end_date):
    """Drive shipped source_report against a deterministic manifest fixture."""
    original = ed.ss.load_manifest
    ed.ss.load_manifest = lambda _path: manifest
    try:
        return ed.source_report(BANK, ticker, base_iv, sessions, columns,
                                start_date, end_date)
    finally:
        ed.ss.load_manifest = original


def main():
    render, fg_map, months = load_render()
    jul = (dt.date(2026, 7, 1), dt.date(2026, 7, 30))

    # --- A. price only: spine shows, no kind lines, no alert ---------------
    rep = ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close"], *jul)
    h = Harness(render, fg_map, months).run(rep)
    check("A1 spine line names ticker and series",
          h._exd_source_spine.visible
          and "AAPL \\ 1m" in h._exd_source_spine.text,
          h._exd_source_spine.text)
    check("A2 no volatility columns -> no kind lines, no alert",
          not any(l.visible for l in h._exd_source_kinds)
          and not h._exd_source_alert.visible)

    # --- B. TMUS mismatch: the state that is silent today ------------------
    rep = ed.source_report(BANK, "TMUS", "1d", ["rth"], ["hvol"],
                           dt.date(2013, 1, 1), dt.date(2026, 7, 30))
    hv = next(k for k in rep["kinds"] if k["kind"] == "hvol")
    check("B1 TMUS hvol holds months the price spine cannot emit",
          len(hv["unemittable"]) == 29 and hv["severity"] == "mismatch",
          f"{len(hv['unemittable'])} unemittable, severity {hv['severity']}")
    check("B2 the hole is the known 2013-05..2015-09 window",
          bool(hv["unemittable"])
          and hv["unemittable"][0] == "2013-05"
          and hv["unemittable"][-1] == "2015-09",
          f"{hv['unemittable'][:1]}..{hv['unemittable'][-1:]}")
    h = Harness(render, fg_map, months).run(rep)
    check("B3 kind line is critical-coloured and states the count",
          h._exd_source_kinds[0].visible
          and h._exd_source_kinds[0].fg == "#b00000"
          and "29 months unemittable" in h._exd_source_kinds[0].text,
          h._exd_source_kinds[0].text)
    check("B4 alert names the exact missing span and why",
          h._exd_source_alert.visible
          and "2013-05 → 2015-09 missing" in h._exd_source_alert.text
          and "rows come from price" in h._exd_source_alert.text,
          h._exd_source_alert.text)

    # --- C. absent source: column exports empty ----------------------------
    rep = ed.source_report(BANK, "TMUS", "1d", ["rth"], ["iv"],
                           dt.date(2013, 1, 1), dt.date(2026, 7, 30))
    iv = next(k for k in rep["kinds"] if k["kind"] == "iv")
    h = Harness(render, fg_map, months).run(rep)
    check("C1 no daily IV stored -> flagged, not silently blank",
          iv["source"] is None and iv["severity"] == "absent"
          and "not stored" in h._exd_source_kinds[0].text
          and "exports empty" in h._exd_source_kinds[0].text,
          h._exd_source_kinds[0].text)

    # --- D. HVOL on minute bars is canonical, not a substitution -----------
    rep = ed.source_report(BANK, "AAPL", "1m", ["rth"], ["hvol"], *jul)
    hv = rep["kinds"][0]
    check("D1 daily HVOL under a minute spine is not cried as substituted",
          hv["substituted"] is False and hv["severity"] == "ok"
          and hv["join"] == "step fill",
          f"{hv['severity']} / sub={hv['substituted']}")

    # --- E. join mode follows the resolved series --------------------------
    rep = ed.source_report(BANK, "AAPL", "1m", ["rth"], ["iv"], *jul)
    iv = rep["kinds"][0]
    check("E1 stored 1m-iv under a minute spine joins on exact timestamp",
          iv["source"] == "1m-iv" and iv["join"] == "exact timestamp"
          and iv["severity"] == "ok",
          f"{iv['source']} / {iv['join']}")

    # --- P. inverse coverage: price rows with no volatility values ----------
    fixture = {"intervals": {
        "1m": {"months": {m: {} for m in
                           ("2026-05", "2026-06", "2026-07")}},
        "1m-iv": {"months": {m: {} for m in ("2026-06", "2026-07")}},
    }}
    zero = report_from_manifest(
        fixture, "AAPL", "1m", ["rth"], ["iv"],
        dt.date(2026, 5, 1), dt.date(2026, 5, 31))
    iv = zero["kinds"][0]
    h = Harness(render, fg_map, months).run(zero)
    check("P1 zero coverage has its own non-critical severity",
          iv["severity"] == "coverage"
          and iv["coverage_missing"] == ["2026-05"]
          and iv["months_covered"] == 0
          and iv["spine_months_in_range"] == 1,
          str(iv))
    check("P2 zero coverage is measured against the spine",
          "0 of 1 month" in h._exd_source_kinds[0].text
          and h._exd_source_kinds[0].fg == "#9a6700",
          h._exd_source_kinds[0].text)
    check("P3 zero coverage earns an explicit empty-cells alert",
          h._exd_source_alert.visible
          and h._exd_source_alert.fg == "#9a6700"
          and "no implied_volatility coverage" in h._exd_source_alert.text
          and "exported cells are empty" in h._exd_source_alert.text,
          h._exd_source_alert.text)

    partial = report_from_manifest(
        fixture, "AAPL", "1m", ["rth"], ["iv"],
        dt.date(2026, 5, 1), dt.date(2026, 7, 31))
    iv = partial["kinds"][0]
    h = Harness(render, fg_map, months).run(partial)
    check("P4 partial coverage is legible without a critical alert",
          iv["severity"] == "coverage"
          and iv["coverage_missing"] == ["2026-05"]
          and "2 of 3 months" in h._exd_source_kinds[0].text
          and h._exd_source_kinds[0].fg == "#9a6700"
          and not h._exd_source_alert.visible,
          f"{iv} / {h.lines()}")

    # --- F. requested sessions are not mistaken for stored series ----------
    base = ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close"], *jul)
    ext = ed.source_report(BANK, "AAPL", "1m", ["rth", "pre", "post"],
                           ["close"], *jul)
    h = Harness(render, fg_map, months).run(ext)
    check("F1 spine names only the sessions actually stored",
          "AAPL \\ 1m   ·" in h._exd_source_spine.text
          and "AAPL \\ 1m +pre" not in h._exd_source_spine.text,
          h._exd_source_spine.text)
    check("F2 unavailable requested sessions are disclosed",
          "requested pre+post not stored" in h._exd_source_spine.text,
          h._exd_source_spine.text)
    check("F3 manifest payload remains the authority for stored sessions",
          ext["spine"]["series"] == ["1m"]
          and ext["spine"]["months_in_range"] >=
          base["spine"]["months_in_range"],
          str(ext["spine"]))
    one_extended = dict(ext)
    one_extended["spine"] = dict(ext["spine"], series=["1m", "1m-pre"])
    h = Harness(render, fg_map, months).run(one_extended)
    check("F4 a genuinely stored extended series is named",
          "AAPL \\ 1m +pre   ·" in h._exd_source_spine.text
          and "requested post not stored" in h._exd_source_spine.text,
          h._exd_source_spine.text)

    # --- G. purity: nothing but the manifest is read -----------------------
    import time
    t0 = time.perf_counter()
    for _ in range(40):
        ed.source_report(BANK, "TMUS", "1d", ["rth"], ["iv", "hvol"],
                         dt.date(2013, 1, 1), dt.date(2026, 7, 30))
    per_call_ms = (time.perf_counter() - t0) / 40 * 1000
    check("G1 fast enough for a 160 ms keystroke re-render (<20 ms/call)",
          per_call_ms < 20, f"{per_call_ms:.1f} ms per call")

    # --- H. degrades safely -------------------------------------------------
    check("H1 unknown ticker returns {} rather than raising",
          ed.source_report(BANK, "ZZZZNOTREAL", "1m", ["rth"], ["iv"],
                           *jul) == {})
    h = Harness(render, fg_map, months).run({})
    check("H2 empty report hides every part of the line",
          not h._exd_source_spine.visible
          and not h._exd_source_stored.visible
          and not h._exd_source_derived.visible
          and not any(l.visible for l in h._exd_source_kinds)
          and not h._exd_source_alert.visible)
    check("H3 unknown session name never raises",
          isinstance(ed.source_report(BANK, "AAPL", "1m", ["nonsense"],
                                      ["iv"], *jul), dict))

    # --- J. derived columns: stated, because nothing is fetched for them ----
    rep = ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close", "vwap"], *jul)
    h = Harness(render, fg_map, months).run(rep)
    check("J1 a computed column is named and declared computed",
          h._exd_source_derived.visible
          and "volume_weighted_average_price" in h._exd_source_derived.text
          and "computed from this spine" in h._exd_source_derived.text,
          h._exd_source_derived.text)
    check("J2 the derived line states the reset rule, not just 'computed'",
          "resets each session" in h._exd_source_derived.text)
    h = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close"], *jul))
    check("J3 no derived line when no derived column is selected",
          not h._exd_source_derived.visible)
    # --- N. the spine's OWN columns are attributed too ----------------------
    # Naming the SERIES ("1d") is not the same statement as naming the columns
    # that series fills. Until 2026-07-31 OHLCV was the one group in an export
    # with no stated origin, resolvable only by already knowing the
    # architecture. (This replaces an earlier check that asserted the opposite:
    # that volume needed no line because it "IS" the spine.)
    ohlcv = ["date", "time", "open", "high", "low", "close", "volume"]
    h = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ohlcv + ["vwap", "hvol"],
                         *jul))
    # `or ""` throughout: a hidden label's text is None, and one failing check
    # must not crash the run and hide every check after it.
    stored_line = h._exd_source_stored
    stored_text = stored_line.text or ""
    check("N1 the columns the spine itself supplies get their own line",
          stored_line.visible and all(c in stored_text for c in ohlcv),
          stored_text)
    check("N2 spine-supplied columns do not overclaim byte-exact storage",
          "supplied by this spine" in stored_text
          and "exactly as stored" not in stored_text,
          stored_text)
    # Every selected column appears exactly once across the whole block: none
    # silently unattributed, none claimed by two different origins.
    # Count WHOLE labels, not substrings: plain "volume" also occurs inside
    # "volume_weighted_average_price", which would read as a double claim.
    picked = ohlcv + ["vwap", "hvol"]
    body = " ".join(h.lines())
    seen = {c: len(re.findall(rf"(?<!\w){re.escape(ed.COLUMN_LABEL.get(c, c))}"
                              r"(?!\w)", body))
            for c in picked}
    check("N3 every selected column is attributed exactly once",
          all(n == 1 for n in seen.values()),
          ", ".join(f"{c}={n}" for c, n in seen.items() if n != 1))
    order = [stored_text.find(c) for c in ("open", "close", "volume")]
    check("N4 the stored line follows the export's own column order",
          all(i >= 0 for i in order) and order == sorted(order))
    narrow_text = (Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close"], *jul)
    )._exd_source_stored.text or "")
    check("N5 only the SELECTED spine columns are listed",
          "close" in narrow_text
          and not any(c in narrow_text
                      for c in ("open", "high", "low", "volume")),
          narrow_text)
    kinds_only = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ["iv"], *jul))
    check("N6 no spine column selected -> no stored line, spine still shown",
          not kinds_only._exd_source_stored.visible
          and kinds_only._exd_source_spine.visible)
    # The three groups must PARTITION the column universe, so a column added to
    # ALL_COLUMNS later cannot quietly become invisible in the source line.
    #
    # Coverage alone would be VACUOUS here and was proved so by mutation:
    # SPINE_COLUMNS subtracts from ALL_COLUMNS, so a newly added column joins
    # it automatically and `classified == set(ALL_COLUMNS)` can never fail.
    # The load-bearing half is the DEFINITION — swap the comprehension for a
    # hand-written tuple and the guarantee is gone while coverage still reads
    # true on the day of the change.
    ed_src = (ROOT / "engine" / "export_designer.py").read_text(
        encoding="utf-8", errors="replace")
    classified = set(ed.SPINE_COLUMNS) | set(ed.DERIVED_COLUMNS) | {"iv", "hvol"}
    check("N7 the spine group is DERIVED from ALL_COLUMNS, never hand-listed",
          "SPINE_COLUMNS = tuple(c for c in ALL_COLUMNS" in ed_src
          and classified == set(ed.ALL_COLUMNS)
          and not (set(ed.SPINE_COLUMNS) & set(ed.DERIVED_COLUMNS)),
          f"unclassified={set(ed.ALL_COLUMNS) - classified}")

    # --- K. space budget: annotate the grid, never displace it -------------
    # Worst case selects EVERY column, not a subset, so the ceiling below is a
    # real bound on the block rather than a bound on one convenient example.
    worst = ed.source_report(BANK, "TMUS", "1d", ["rth", "pre", "post"],
                             list(ed.ALL_COLUMNS),
                             dt.date(2013, 1, 1), dt.date(2026, 7, 30))
    lines = Harness(render, fg_map, months).run(worst).lines()
    check("K1 the busiest possible report fits six lines (five + the alert)",
          len(lines) <= 6, f"{len(lines)} lines")
    check("K2 every line stays short enough to avoid wrapping (<=132 chars)",
          all(len(t) <= 132 for t in lines),
          f"longest {max((len(t) for t in lines), default=0)}")
    check("K3 no line carries a newline of its own",
          all("\n" not in t for t in lines))
    quiet = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close"], *jul)).lines()
    check("K4 a healthy price-only export costs two lines: spine + its columns",
          len(quiet) == 2, f"{len(quiet)} lines")
    # The sixth line is not free real estate: it appears only when a genuine
    # mismatch earns it, so an ordinary export never pays for the alert.
    healthy = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], list(ed.ALL_COLUMNS),
                         *jul)).lines()
    check("K6 every column, no mismatch -> five lines, no alert",
          len(healthy) <= 5, f"{len(healthy)} lines")
    src_widgets = (ROOT / "display_data.py").read_text(
        encoding="utf-8", errors="replace")
    check("K5 the block uses a subordinate font and claims no padding",
          '_EXD_SOURCE_FONT = ("Segoe UI", 8)' in src_widgets
          and "self._exd_source_frame.pack(side=tk.TOP, fill=tk.X)"
          in src_widgets)

    # --- L. counts read as English, not as a template ----------------------
    # A one-month window is the common case for a quick export; "1 months" in
    # a line whose whole job is to be trusted undermines the rest of it.
    one = Harness(render, fg_map, months).run(
        ed.source_report(BANK, "AAPL", "1m", ["rth"], ["close", "hvol"],
                         *jul)).lines()
    check("L1 a one-month span is singular everywhere it is counted",
          sum(t.count("1 month") for t in one) >= 2
          and all("1 months" not in t for t in one),
          " | ".join(one))
    check("L2 plural counts keep their s",
          months(0) == "0 months" and months(1) == "1 month"
          and months(29) == "29 months")

    # --- I. placement: the line lives with the rendered export --------------
    # The raw "Bank source" tab was removed 2026-07-31 (user order); the source
    # line moved to the Export tab, where it describes the output beside it.
    src = (ROOT / "display_data.py").read_text(encoding="utf-8",
                                               errors="replace")
    check("I1 the raw Bank source tab is gone, not merely hidden",
          '("bank", "Bank source")' not in src
          and "_exd_set_bank_grid" not in src
          and "_exd_bank_sample" not in src
          and "bank_rows" not in src)
    check("I2 preview tabs are exactly Export and Issues",
          'for key in ("export", "issues"):' in src
          and 'if tab not in ("export", "issues"):' in src)
    export_block = src.split('expf = self._exd_sample_frames["export"]')[1]
    export_block = export_block.split("issuesf =")[0]
    check("I3 the source line is built inside the Export tab frame",
          "self._exd_source_frame = ttk.Frame(expf)" in export_block
          and "_exd_source_spine" in export_block
          and "_exd_source_alert" in export_block)
    check("I4 Bank data remains a preview SOURCE (the dropdown), untouched",
          '_EXD_SAMPLE_SOURCES = ("Sample fake", "Bank data")' in src)

    # --- M. the synthetic source cannot be renamed INTO the bank ------------
    # The branch test is `!= "Bank data"`, so the synthetic label is free to
    # change (it became "Sample fake" 2026-07-31) while "Bank data" is the one
    # string that must stay exact. Pin that asymmetry, not the display name.
    check("M1 preview routing keys off Bank data, never the synthetic label",
          'if source != "Bank data":' in src)
    check("M2 the old label is gone from every user-facing string",
          "All variations" not in src,
          "stale 'All variations' still rendered somewhere")
    check("M3 the synthetic label still says plainly that it is not real",
          '"label": "Sample fake"' in src
          and "invented rows, not your bank" in src)

    print(f"\n{len(FAILED)} failed")
    return 1 if FAILED else 0


def mutation_main():
    """Kill one shipped-source mutation per Row 78-FIX behavioral fence."""
    script = Path(__file__).resolve()
    clean_env = dict(os.environ)
    clean_env["EXPORT_SOURCE_LINE_BANK"] = str(BANK.resolve())

    def run(path):
        return subprocess.run(
            [sys.executable, str(path)], cwd=str(path.parent.parent),
            env=clean_env, text=True, capture_output=True, timeout=120,
            encoding="utf-8", errors="replace")

    def green(label):
        result = run(script)
        ok = result.returncode == 0 and "0 failed" in result.stdout
        print(f"[{'PASS' if ok else 'FAIL'}] {label}")
        if not ok:
            print(result.stdout)
            print(result.stderr)
        return ok

    cases = [
        {
            "name": "F4 empty-unemittable completes red",
            "file": "engine/export_designer.py",
            "anchor": (
                "            unemittable = [m for m in in_range "
                "if m not in spine_months]"),
            "mutant": "            unemittable = []",
            "failures": [
                "B1 TMUS hvol holds months the price spine cannot emit",
                "B2 the hole is the known 2013-05..2015-09 window",
            ],
        },
        {
            "name": "F2 renderer uses stored sessions",
            "file": "display_data.py",
            "anchor": (
                "        stored_sessions = [\n"
                "            \"pre\" if key.endswith(\"-pre\") else\n"
                "            \"post\" if key.endswith(\"-post\") else \"rth\"\n"
                "            for key in (spine.get(\"series\") or [])\n"
                "        ]"),
            "mutant": "        stored_sessions = list(requested_sessions)",
            "failures": [
                "F1 spine names only the sessions actually stored",
                "F2 unavailable requested sessions are disclosed",
                "F4 a genuinely stored extended series is named",
            ],
        },
        {
            "name": "F2 payload preserves manifest series authority",
            "file": "engine/export_designer.py",
            "anchor": '        "spine": {"interval": base_iv, "series": spine_keys,',
            "mutant": (
                '        "spine": {"interval": base_iv, "series": '
                '[ss.with_session(base_iv, s) for s in sessions],'),
            "failures": [
                "F3 manifest payload remains the authority for stored sessions",
            ],
        },
        {
            "name": "F1 inverse coverage is load-bearing",
            "file": "engine/export_designer.py",
            "anchor": (
                "            coverage_missing = [m for m in spine_in_range\n"
                "                                if m not in in_range]"),
            "mutant": "            coverage_missing = []",
            "failures": [
                "P1 zero coverage has its own non-critical severity",
                "P2 zero coverage is measured against the spine",
                "P3 zero coverage earns an explicit empty-cells alert",
                "P4 partial coverage is legible without a critical alert",
            ],
        },
        {
            "name": "F3 wording cannot overclaim exact storage",
            "file": "display_data.py",
            "anchor": "supplied by this spine",
            "mutant": "this spine, exactly as stored",
            "failures": [
                "N2 spine-supplied columns do not overclaim byte-exact storage",
            ],
        },
    ]

    passed = green("shipped baseline green before mutations")
    for case in cases:
        source_path = ROOT / case["file"]
        source = source_path.read_text(encoding="utf-8", errors="strict")
        anchor_count = source.count(case["anchor"])
        anchor_ok = anchor_count == 1
        print(f"[{'PASS' if anchor_ok else 'FAIL'}] {case['name']} anchor "
              f"exists exactly once ({anchor_count})")
        passed = passed and anchor_ok
        if not anchor_ok:
            continue

        with tempfile.TemporaryDirectory(prefix="row78_fix_mutation_") as td:
            mutant_root = Path(td)
            shutil.copytree(
                ROOT / "engine", mutant_root / "engine",
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            shutil.copy2(ROOT / "display_data.py", mutant_root / "display_data.py")
            mutant_path = mutant_root / case["file"]
            mutant_source = mutant_path.read_text(encoding="utf-8")
            mutant_path.write_text(
                mutant_source.replace(case["anchor"], case["mutant"], 1),
                encoding="utf-8", newline="")
            result = run(mutant_root / "engine" /
                         "export_source_line_reference.py")

        output = result.stdout + result.stderr
        expected_red = all(f"[FAIL] {name}" in output
                           for name in case["failures"])
        completed = (result.returncode != 0
                     and re.search(r"\n[1-9][0-9]* failed\s*$", output) is not None
                     and "[PASS] M3 the synthetic label still says plainly" in output
                     and "Traceback" not in output)
        killed = expected_red and completed
        print(f"[{'PASS' if killed else 'FAIL'}] {case['name']} killed; "
              "harness completed red")
        if not killed:
            print(output)
        passed = passed and killed

    passed = green("shipped baseline green after isolated mutations") and passed
    print(f"\n{len(cases)} mutations killed" if passed else
          "\nmutation proof failed")
    return 0 if passed else 1


if __name__ == "__main__":
    if sys.argv[1:] == ["--mutation-proof"]:
        raise SystemExit(mutation_main())
    raise SystemExit(main())
