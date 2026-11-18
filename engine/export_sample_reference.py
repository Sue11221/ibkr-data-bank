"""Row 53 M0 acceptance harness — Export Designer live-sample synthetic data
(the "Sample fake" source, labelled "All variations" until 2026-07-31; the
SAMPLE_VARIATIONS/REQUIRED_VARIATIONS engine names are unchanged) + honest
grid toggles + derived date-column widths.

REALISM REVISION (user feedback 2026-07-22): the fake data must LOOK real
(one ticker, one tight price neighbourhood — huge_price/tiny_price/
quote_trigger dropped) and must ALWAYS show empties: empty_row (date kept,
whole bar absent) + empty_value (complete bar, one hole that lands on a
SHOWN column even with default OHLCV columns).

CLAUDE-OWNED reference harness (Codex MUST NOT edit this file).
Plan: EXPORT_SAMPLE_VARIATIONS_PLAN.md. Offline, headless, deterministic:
display_data.py (tkinter GUI) is NEVER imported — its grid builder is checked
at SOURCE level via AST, and the D4 width helper is exec'd standalone (it is
required to be pure). Engine checks run the real export_designer pipeline.

Exit codes:
  3 = feature absent (no SAMPLE_VARIATIONS flag) and every baseline pin holds
      — the expected pre-M1 state.
  0 = feature present and every feature check passes.
  1 = any check failed, or the harness itself broke.
"""

import ast
import csv
import io
import shutil
import sys
import tempfile
import textwrap
from datetime import date as _date
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import export_designer as xd  # noqa: E402

_DISPLAY_DATA = _HERE.parent / "display_data.py"

# The 10 canonical variation names (plan §2 D1, realism revision 2026-07-22:
# huge_price/tiny_price/quote_trigger dropped as cartoonish; empty_aux split
# into empty_row + empty_value, both always visible). REQUIRED_VARIATIONS in
# the implementation must cover AT LEAST these; extras are welcome.
CANON10 = (
    "normal", "day_gap", "minute_gap", "zero_volume", "fractional_volume",
    "empty_row", "empty_value", "dst_boundary", "month_boundary",
    "kind_scale",
)

ALL_COLS = ["date", "time", "timestamp", "open", "high", "low", "close",
            "volume", "ticker", "vwap", "iv", "hvol"]

GRID_FN = "_exd_set_export_grid"
WIDTH_FN = "_exd_grid_widths"
HARD_156 = '156 if c == "timestamp"'


def _spec(**over):
    s = xd.default_spec()
    s["columns"] = list(ALL_COLS)
    s.update(over)
    return s


def _fn_source(src, name):
    """Source segment of the FIRST def named `name` anywhere in `src`."""
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == name:
            return ast.get_source_segment(src, node)
    return None


def _display_src():
    return _DISPLAY_DATA.read_text(encoding="utf-8", errors="replace")


def _norm_tags(tags):
    out = []
    for t in tags:
        if isinstance(t, (set, frozenset, list, tuple)):
            out.append({str(x) for x in t})
        else:
            out.append({str(t)})
    return out


def _price_vals(r):
    return [r[c] for c in ("open", "high", "low", "close")]


# --- baseline pins (feature absent) -------------------------------------------

def b1_absence():
    present = [n for n in ("synthetic_sample_rows", "SAMPLE_VARIATIONS",
                           "REQUIRED_VARIATIONS") if hasattr(xd, n)]
    if present:
        return False, f"feature names already exist: {present}"
    return True, "no synthetic_sample_rows / SAMPLE_VARIATIONS / REQUIRED_VARIATIONS"


def b2_grid_defect():
    seg = _fn_source(_display_src(), GRID_FN)
    if seg is None:
        return False, f"{GRID_FN} not found in display_data.py"
    if "tv.heading(" not in seg:
        return False, f"{GRID_FN} no longer calls tv.heading( — pin stale"
    if "header" in seg:
        return False, f"{GRID_FN} already consults 'header' — defect pin stale"
    if HARD_156 not in seg:
        return False, f"hardcoded width '{HARD_156}' gone — defect pin stale"
    return True, ("grid draws headings unconditionally (no 'header' consult) "
                  "+ hardcoded 156px timestamp width")


def b3_bank_bound():
    tmp = tempfile.mkdtemp(prefix="exd_ref_empty_")
    try:
        spec = _spec(start_date=_date(2024, 1, 2), end_date=_date(2024, 1, 31))
        rows = xd.sample_rows(tmp, "FAKE", spec)
        if rows:
            return False, f"empty root produced {len(rows)} sample rows"
        return True, "sample_rows over an EMPTY root -> 0 rows (no synthetic fallback)"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --- feature checks (flag present) ---------------------------------------------

def _rows_price():
    return xd.synthetic_sample_rows(_spec())


def _rows_kind():
    return xd.synthetic_sample_rows(_spec(base_interval="1d-hvol"))


def f1_contract():
    req = getattr(xd, "REQUIRED_VARIATIONS", ())
    missing_req = [n for n in CANON10 if n not in req]
    if missing_req:
        return False, f"REQUIRED_VARIATIONS lacks canonical names: {missing_req}"
    rows_p, tags_p = _rows_price()
    rows_k, tags_k = _rows_kind()
    if len(rows_p) != len(tags_p) or len(rows_k) != len(tags_k):
        return False, "rows/tags not parallel"
    if len(rows_p) < 10:
        return False, f"price-spec sample too small: {len(rows_p)} rows"
    for rows in (rows_p, rows_k):
        dts = [r["dt"] for r in rows]
        if any(not (a < b) for a, b in zip(dts, dts[1:])):
            return False, "row dts not strictly increasing"
    union = set().union(*_norm_tags(tags_p)) | set().union(*_norm_tags(tags_k))
    uncovered = [n for n in req if n not in union]
    if uncovered:
        return False, f"variations declared but never tagged: {uncovered}"
    return True, (f"{len(rows_p)}+{len(rows_k)} rows, tags cover all "
                  f"{len(req)} declared variations (>=10 canonical)")


def f2_render_sweep():
    rows_p, _ = _rows_price()
    spec = _spec()
    n_cells = 0
    for em in xd.EMPTY_MODES:
        for df in xd.DATE_FORMATS:
            sv = dict(spec, empty=em, date_format=df)
            for r in rows_p:
                for c in spec["columns"]:
                    val = xd._cell(c, r, sv)
                    if not isinstance(val, str):
                        return False, f"_cell({c},{em},{df}) -> {type(val)}"
                    n_cells += 1
    fts = []
    for ft in ("csv", "tsv", "json", "parquet"):
        try:
            blob = xd.render_rows(rows_p, dict(spec, file_type=ft))
        except ImportError:
            fts.append(f"{ft}:skipped-no-lib")
            continue
        if not blob:
            return False, f"render_rows({ft}) returned empty output"
        fts.append(f"{ft}:{len(blob)}B")
    return True, (f"{n_cells} cells across 4 empty x 4 date formats; "
                  f"render {' '.join(fts)}")


def _tagged(rows, tags, name):
    return [i for i, t in enumerate(_norm_tags(tags)) if name in t]


def f3_variation_semantics():
    rows, tags = _rows_price()
    spec = _spec()
    probs = []

    # empty_value: a COMPLETE bar missing exactly one visible cell. With aux
    # columns selected the hole is the first aux column ...
    idx = _tagged(rows, tags, "empty_value")
    if not idx:
        probs.append("empty_value: no tagged row")
    else:
        r = rows[idx[0]]
        if any(v is None for v in _price_vals(r)):
            probs.append("empty_value: bar must stay complete (OHLC present)")
        col = next((c for c in ("iv", "hvol", "vwap")
                    if c in spec["columns"] and r.get(c) is None), None)
        if col is None:
            probs.append("empty_value: tagged row has no None aux cell")
        else:
            seen = {em: xd._cell(col, r, dict(spec, empty=em))
                    for em in xd.EMPTY_MODES}
            if sorted(seen.values()) != sorted(
                    xd._EMPTY_TOKEN[em] for em in xd.EMPTY_MODES):
                probs.append(f"empty_value: empty-mode tokens wrong: {seen}")
            if not any(o.get(col) is not None for j, o in enumerate(rows)
                       if j != idx[0]):
                probs.append(f"empty_value: no neighbour carries a {col} value")

    # ... and with the DEFAULT OHLCV columns (no aux selected) the hole MUST
    # land on a SHOWN column (volume) — the user-visible bug this revision
    # fixes: default previews used to show no empty at all.
    d_spec = xd.default_spec()
    d_cols = list(d_spec.get("columns") or xd.DEFAULT_COLUMNS)
    if not any(c in d_cols for c in ("iv", "hvol", "vwap")):
        d_rows, d_tags = xd.synthetic_sample_rows(d_spec)
        di = _tagged(d_rows, d_tags, "empty_value")
        if not di:
            probs.append("empty_value/default: no tagged row")
        elif d_rows[di[0]].get("volume") is not None:
            probs.append("empty_value/default: volume not the hole — empty "
                         "invisible with default columns")
        else:
            seen = {em: xd._cell("volume", d_rows[di[0]], dict(d_spec, empty=em))
                    for em in xd.EMPTY_MODES}
            if sorted(seen.values()) != sorted(
                    xd._EMPTY_TOKEN[em] for em in xd.EMPTY_MODES):
                probs.append(f"empty_value/default: volume tokens wrong: {seen}")
    else:
        probs.append("empty_value/default: DEFAULT_COLUMNS now includes aux — "
                     "default-visibility pin needs re-derivation")

    # empty_row: timestamp kept, NO bar at all — every numeric cell renders
    # the empty mode while date/time render normally, in csv AND the grid
    # path (same _cell seam).
    idx = _tagged(rows, tags, "empty_row")
    if not idx:
        probs.append("empty_row: no tagged row")
    else:
        r = rows[idx[0]]
        numeric = [c for c in spec["columns"]
                   if c in ("open", "high", "low", "close", "volume",
                            "vwap", "iv", "hvol")]
        if any(r.get(c) is not None for c in numeric):
            probs.append("empty_row: some numeric cell still has a value")
        if r.get("dt") is None:
            probs.append("empty_row: dt missing — date-only row needs its date")
        for em in xd.EMPTY_MODES:
            sv = dict(spec, empty=em)
            tok = xd._EMPTY_TOKEN[em]
            bad = [c for c in numeric if xd._cell(c, r, sv) != tok]
            if bad:
                probs.append(f"empty_row: cells {bad} != {tok!r} under {em}")
                break
        if not xd._cell("date", r, spec) or not xd._cell("time", r, spec):
            probs.append("empty_row: date/time cells must render normally")
        blob = xd.render_rows(rows, dict(spec, file_type="csv", header=True))
        got = list(csv.reader(io.StringIO(blob.decode("utf-8"))))
        if len(got[idx[0] + 1]) != len(spec["columns"]):
            probs.append("empty_row: CSV row lost cells")

    # realism: one ticker, one price neighbourhood, plausible volumes — the
    # sample must look like a real series, not a stress cartoon.
    present = [v for r in rows for v in _price_vals(r) if v is not None]
    lo, hi = min(present), max(present)
    if not (1.0 <= lo and hi <= 10000.0):
        probs.append(f"realism: price band [{lo}, {hi}] outside sane range")
    if hi / lo > 1.15:
        probs.append(f"realism: price spread {hi / lo:.3f}x > 1.15x — looks fake")
    bad_tick = {r.get("ticker") for r in rows} - {"SAMPLE"}
    if bad_tick:
        probs.append(f"realism: non-SAMPLE ticker cells {bad_tick}")
    vols = [r["volume"] for r in rows if r.get("volume") is not None]
    if any(not (0 <= v < 10_000_000) for v in vols):
        probs.append("realism: implausible volume magnitude")

    idx = _tagged(rows, tags, "dst_boundary")
    if len(idx) < 2:
        probs.append("dst_boundary: needs >=2 tagged rows")
    else:
        sv = dict(spec, date_format="iso", timezone="America/New_York")
        offs = {xd.format_timestamp(rows[i]["dt"], sv)[-6:] for i in idx}
        if not {"-05:00", "-04:00"} <= offs:
            probs.append(f"dst_boundary: offsets seen {offs}, need -05:00 AND -04:00")

    idx = _tagged(rows, tags, "day_gap")
    if not any(i > 0 and (rows[i]["dt"].date() - rows[i - 1]["dt"].date()).days >= 2
               for i in idx):
        probs.append("day_gap: no tagged row >=2 calendar days after its neighbour")

    idx = _tagged(rows, tags, "minute_gap")
    if not any(i > 0 and rows[i]["dt"].date() == rows[i - 1]["dt"].date()
               and (rows[i]["dt"] - rows[i - 1]["dt"]).total_seconds() > 60
               for i in idx):
        probs.append("minute_gap: no same-day tagged pair jumping >1 minute")

    idx = _tagged(rows, tags, "month_boundary")
    if not any(i > 0 and (rows[i]["dt"].year, rows[i]["dt"].month)
               != (rows[i - 1]["dt"].year, rows[i - 1]["dt"].month)
               for i in idx):
        probs.append("month_boundary: no tagged pair crossing a month edge")

    idx = _tagged(rows, tags, "zero_volume")
    if not any(rows[i]["volume"] == 0 for i in idx):
        probs.append("zero_volume: no tagged row with volume == 0")
    elif xd._cell("volume", rows[idx[0]], spec) != "0":
        probs.append("zero_volume: cell does not render '0'")

    idx = _tagged(rows, tags, "fractional_volume")
    frac = [i for i in idx if rows[i].get("volume") is not None
            and rows[i]["volume"] != int(rows[i]["volume"])]
    if not frac:
        probs.append("fractional_volume: no tagged row with non-integer volume")
    else:
        r = rows[frac[0]]
        if xd._cell("volume", r, spec) != str(int(r["volume"])):
            probs.append("fractional_volume: cell != the file's truncated int")

    # kind_scale: under a ratio-kind spec the WHOLE series is vol-scale — a
    # single rescaled row next to $188 bars would itself look fake.
    rows_k, tags_k = _rows_kind()
    idx = _tagged(rows_k, tags_k, "kind_scale")
    if not idx:
        probs.append("kind_scale: no tagged row under a kind-token spec")
    kind_present = [v for r in rows_k for v in _price_vals(r) if v is not None]
    if not kind_present or not all(0 < v <= 10.0 for v in kind_present):
        probs.append("kind_scale: not ALL kind-spec prices are vol-scale (<=10)")
    k_empty = _tagged(rows_k, tags_k, "empty_row")
    if not k_empty or any(v is not None
                          for v in _price_vals(rows_k[k_empty[0]])):
        probs.append("kind_scale: empty_row must stay absent under kind specs")

    if probs:
        return False, "; ".join(probs)
    return True, ("all 10 canonical variation semantics verified, incl. "
                  "default-columns empty visibility + realism band")


def f4_grid_source():
    src = _display_src()
    seg = _fn_source(src, GRID_FN)
    if seg is None:
        return False, f"{GRID_FN} not found in display_data.py"
    probs = []
    if "header" not in seg:
        probs.append(f"{GRID_FN} still ignores the header spec")
    if HARD_156 in seg:
        probs.append(f"hardcoded '{HARD_156}' still present")
    if WIDTH_FN not in seg:
        probs.append(f"{GRID_FN} does not call {WIDTH_FN}")
    wseg = _fn_source(src, WIDTH_FN)
    if wseg is None:
        probs.append(f"pure helper {WIDTH_FN} not found")
    else:
        ns = {}
        try:
            exec(compile(textwrap.dedent(wseg), "<widths>", "exec"), ns)
            fn = ns[WIDTH_FN]
            cols = ["timestamp", "date", "time", "open"]
            long_ts = "2024-06-17T09:30:00-04:00"
            rendered = [[long_ts, "2024-06-17", "09:30:00", "123.45"]] * 3
            w = fn(cols, rendered, _spec())
            if not w["timestamp"] > 156:
                probs.append(f"ISO+tz timestamp width {w['timestamp']} !> 156")
            if not w["date"] > 58:
                probs.append(f"date width {w['date']} !> today's 58 floor")
            if w["open"] != max(58, 8 * len("open") + 26):
                probs.append(f"non-date col width changed: open={w['open']}")
            short = [["1718629800", "2024-06-17", "09:30:00", "1.2"]] * 3
            w2 = fn(cols, short, _spec(date_format="epoch"))
            if w2["timestamp"] != 156:
                probs.append(f"epoch floor broken: timestamp={w2['timestamp']}")
        except Exception as exc:  # noqa: BLE001
            probs.append(f"{WIDTH_FN} not pure/executable standalone: {exc!r}")
    if probs:
        return False, "; ".join(probs)
    return True, (f"{GRID_FN} consults header + calls {WIDTH_FN}; helper widens "
                  "ISO+tz, floors epoch at 156, leaves non-date cols untouched")


def f5_flag():
    if getattr(xd, "SAMPLE_VARIATIONS", None) is not True:
        return False, f"SAMPLE_VARIATIONS = {getattr(xd, 'SAMPLE_VARIATIONS', None)!r}"
    return True, "SAMPLE_VARIATIONS is True"


BASELINE = [("B1 feature-absence pin", b1_absence),
            ("B2 grid header/width defect pin", b2_grid_defect),
            ("B3 bank-bound sample pin", b3_bank_bound)]

FEATURE = [("F1 synthetic contract + coverage", f1_contract),
           ("F2 render sweep (4 empty x 4 date, 4 file types)", f2_render_sweep),
           ("F3 variation semantics", f3_variation_semantics),
           ("F4 grid consults header + derived widths", f4_grid_source),
           ("F5 flag", f5_flag),
           ("B3 bank mode unchanged (empty root -> 0 rows)", b3_bank_bound)]


def main():
    feature = hasattr(xd, "SAMPLE_VARIATIONS")
    checks = FEATURE if feature else BASELINE
    mode = "FEATURE" if feature else "BASELINE (feature absent)"
    print(f"export_sample_reference — mode: {mode}")
    fails = 0
    for name, fn in checks:
        try:
            ok, detail = fn()
        except Exception as exc:  # noqa: BLE001
            ok, detail = False, f"harness exception: {exc!r}"
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} — {detail}")
        fails += 0 if ok else 1
    if fails:
        print(f"RESULT: {fails} check(s) failed -> exit 1")
        return 1
    if not feature:
        print("RESULT: baseline pins hold, feature not built -> exit 3")
        return 3
    print("RESULT: all feature checks pass -> exit 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
