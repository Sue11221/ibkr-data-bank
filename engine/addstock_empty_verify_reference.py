"""Row 56 M0 acceptance harness — Add Stocks empty/uncomputable series never
create unclearable verification debt (the HONA 1d-hvol stuck-debt class).

CLAUDE-OWNED reference harness (Codex MUST NOT edit this file).
Plan: ADDSTOCK_EMPTY_VERIFICATION_PLAN.md. Offline, headless, deterministic:
the manifest checks drive the REAL `addstock_run_manifest` API on temp roots;
the display checks are AST/source extractions of the production methods; the
disk predicate is exercised against a REAL seeded bank; the emptiness predicate
handed to the heal is INJECTED, so no fetch, port, network, or GUI is touched.

Exit codes:
  3 = feature absent (no `empty=` param on mark_series_complete AND no
      exempt_empty_verification) and the STUCK baseline reproduces — the
      expected pre-M1 state.
  0 = feature present and every feature check passes.
  1 = any check failed, or the harness itself broke.
"""

import ast
import datetime as dt
import inspect
import shutil
import sys
import tempfile
import textwrap
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

import addstock_run_manifest as arm  # noqa: E402
import stock_storage as ss  # noqa: E402

_DISPLAY = _HERE.parent / "display_data.py"

PORTFREE_FN = "_addstock_consume_portfree_debt"
FRESH_FILL_FN = "_storage_find_start_fill"
COMPLETE_FN = "_addstock_series_complete"
PREDICATE_FN = "_addstock_interval_empty_on_disk"


# --- helpers ------------------------------------------------------------------

def _display_src():
    return _DISPLAY.read_text(encoding="utf-8", errors="replace")


def _method_src(src, name):
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
                and node.name == name:
            return ast.get_source_segment(src, node)
    return None


def _has_empty_param():
    try:
        return "empty" in inspect.signature(
            arm.mark_series_complete).parameters
    except (ValueError, TypeError):
        return False


def _feature_present():
    return hasattr(arm, "exempt_empty_verification") and _has_empty_param()


def _bank(tmp):
    root = Path(tmp) / ss.STORAGE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def _put_present_month(root, manifest, ticker, interval, bars):
    """Write a real month file and record a canonical PRESENT month entry — the
    shape production writes (a bare write_month_file record has no `status`,
    which the storage fingerprint would reject and the predicate treats as
    absent)."""
    path = ss.month_file_path(root, ticker, 2026, 1, interval, fmt="csv")
    record = dict(ss.write_month_file(path, bars))
    record["status"] = "present"
    ss.manifest_months(manifest, interval)["2026-01"] = record


def _seed_absent(root, ticker, present_intervals=()):
    """A ticker whose manifest loads but does NOT have `1d-hvol` present.

    `present_intervals` may seed OTHER real intervals (e.g. HONA's 1d/1m) so the
    predicate must key on the specific absent interval, not the whole ticker."""
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    manifest = ss.new_manifest(ticker, ticker)
    bars = [(dt.datetime(2026, 1, 2, 9, 30), 10.0, 11.0, 9.5, 10.5, 100)]
    for interval in present_intervals:
        _put_present_month(root, manifest, ticker, interval, bars)
    ss.save_manifest(tdir, manifest)


def _seed_present(root, ticker, interval="1d-hvol"):
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    manifest = ss.new_manifest(ticker, ticker)
    bars = [(dt.datetime(2026, 1, 2, 9, 30), 0.30, 0.31, 0.29, 0.30, 0),
            (dt.datetime(2026, 1, 5, 9, 30), 0.31, 0.32, 0.30, 0.31, 0)]
    _put_present_month(root, manifest, ticker, interval, bars)
    ss.save_manifest(tdir, manifest)


def _new_run(root, selections, earliest):
    arm.create_run(root, selections, earliest_tickers=list(earliest))
    return arm.load_run(root)["run_id"]


def _series(root, ticker, interval="1d-hvol"):
    return arm.load_run(root)["tickers"][ticker]["series"][interval]


def _tstate(root, ticker):
    return arm.load_run(root)["tickers"][ticker]["state"]


def _load_predicate():
    """Exec the extracted display predicate standalone bound to a fake self."""
    src = _method_src(_display_src(), PREDICATE_FN)
    if src is None:
        return None
    ns = {"stock_storage": ss, "Path": Path}
    exec(compile(textwrap.dedent(src), "<pred>", "exec"), ns)
    return ns[PREDICATE_FN]


# --- baseline pins (feature absent) -------------------------------------------

def b1_absence():
    problems = []
    if _has_empty_param():
        problems.append("mark_series_complete already has an `empty` param")
    if hasattr(arm, "exempt_empty_verification"):
        problems.append("exempt_empty_verification already exists")
    if problems:
        return False, "; ".join(problems)
    return True, ("no empty= param and no exempt_empty_verification "
                  "(pre-M1 as expected)")


def b2_stuck_reproduces():
    tmp = tempfile.mkdtemp(prefix="addstk_empty_b2_")
    try:
        root = _bank(tmp)
        _seed_absent(root, "EMPTYX", present_intervals=("1d",))
        rid = _new_run(root, [("EMPTYX", "1d-hvol")], ["EMPTYX"])
        arm.mark_series_complete(root, rid, "EMPTYX", "1d-hvol")
        arm.mark_fetch_finished(root, rid)
        s = _series(root, "EMPTYX")
        rec = arm.load_run(root)["tickers"]["EMPTYX"]
        if s["state"] != "complete":
            return False, f"series did not complete: {s['state']}"
        if s["xval"] or s["gaps"]:
            return False, "empty series unexpectedly owes no checks pre-M1"
        if _tstate(root, "EMPTYX") != "built" or set(rec["missing"]) != {
                "xval", "gaps"}:
            return False, (f"ticker not stuck-as-built: {rec['state']} "
                           f"missing={rec['missing']}")
        if hasattr(arm, "exempt_empty_verification"):
            return False, "an exempt API exists (feature leaked into baseline)"
        return True, ("a complete empty 1d-hvol series is stuck owing xval+gaps "
                      "with NO exempt API — the reported bug")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def b3_display_unwired():
    src = _display_src()
    sc = _method_src(src, COMPLETE_FN)
    if sc is None:
        return False, f"{COMPLETE_FN} not found — pin stale"
    if "empty=" in sc or "empty =" in sc:
        return False, f"{COMPLETE_FN} already passes an empty signal"
    if _method_src(src, PREDICATE_FN) is not None:
        return False, f"{PREDICATE_FN} already exists"
    return True, (f"{COMPLETE_FN} does not classify emptiness and "
                  f"{PREDICATE_FN} is absent")


# --- feature checks (present) -------------------------------------------------

def f1_source_prevention():
    tmp = tempfile.mkdtemp(prefix="addstk_empty_f1_")
    try:
        root = _bank(tmp)
        _seed_absent(root, "T", present_intervals=("1d",))
        rid = _new_run(root, [("T", "1d-hvol")], ["T"])
        arm.mark_series_complete(root, rid, "T", "1d-hvol", empty=True)
        s = _series(root, "T")
        if not (s["state"] == "complete" and s["xval"] is True
                and s["gaps"] is True):
            return False, f"empty=True did not exempt the series: {s}"
        if _tstate(root, "T") != "verified":
            return False, f"exempted ticker not verified: {_tstate(root, 'T')}"

        root2 = _bank(tempfile.mkdtemp(prefix="addstk_empty_f1b_"))
        _seed_present(root2, "U", "1d-hvol")
        rid2 = _new_run(root2, [("U", "1d-hvol")], ["U"])
        arm.mark_series_complete(root2, rid2, "U", "1d-hvol", empty=False)
        s2 = _series(root2, "U")
        if s2["xval"] or s2["gaps"] or _tstate(root2, "U") != "built":
            return False, "empty=False wrongly exempted a real series"
        return True, ("empty=True exempts (verified); empty=False preserves the "
                      "owed checks (built)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f2_heal_and_no_over_exemption():
    tmp = tempfile.mkdtemp(prefix="addstk_empty_f2_")
    try:
        root = _bank(tmp)
        _seed_absent(root, "EMPTYX", present_intervals=("1d",))
        _seed_present(root, "HASX", "1d-hvol")
        rid = _new_run(
            root, [("EMPTYX", "1d-hvol"), ("HASX", "1d-hvol")],
            ["EMPTYX", "HASX"])
        for tkr in ("EMPTYX", "HASX"):
            arm.mark_series_complete(root, rid, tkr, "1d-hvol")  # legacy: owes
        arm.mark_fetch_finished(root, rid)

        empty_fn = lambda t, iv: t == "EMPTYX"  # noqa: E731 - only EMPTYX empty
        res = arm.exempt_empty_verification(root, rid, empty_fn)
        exempted = {tuple(x) for x in (res.get("exempted") or ())}
        ex = _series(root, "EMPTYX")
        hd = _series(root, "HASX")
        if not (ex["xval"] and ex["gaps"]
                and _tstate(root, "EMPTYX") == "verified"):
            return False, f"empty series not healed: {ex} / {_tstate(root,'EMPTYX')}"
        if hd["xval"] or hd["gaps"] or _tstate(root, "HASX") != "built":
            return False, "OVER-EXEMPTION: a series WITH data was excused"
        if ("EMPTYX", "1d-hvol") not in exempted or (
                "HASX", "1d-hvol") in exempted:
            return False, f"exempted set wrong: {sorted(exempted)}"
        res2 = arm.exempt_empty_verification(root, rid, empty_fn)
        if res2.get("exempted"):
            return False, f"not idempotent: {res2['exempted']}"
        return True, ("empty ticker exempted+verified, present ticker "
                      "untouched, idempotent on re-run")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f3_heal_completes_and_archives():
    tmp = tempfile.mkdtemp(prefix="addstk_empty_f3_")
    try:
        root = _bank(tmp)
        _seed_absent(root, "EMPTYX", present_intervals=("1d",))
        rid = _new_run(root, [("EMPTYX", "1d-hvol")], ["EMPTYX"])
        arm.mark_series_complete(root, rid, "EMPTYX", "1d-hvol")
        arm.mark_fetch_finished(root, rid)
        archive_dir = Path(tmp) / "archive"
        res = arm.exempt_empty_verification(
            root, rid, lambda t, iv: True, archive_dir=archive_dir)
        if not res.get("archived"):
            return False, f"run did not archive when only empties remained: {res}"
        if arm.load_run(root) is not None:
            return False, "active manifest still present after archival"
        if not Path(res["archived"]).exists():
            return False, "archive artifact was not written"
        return True, "last-empty-series heal drives the run to archived 503/503"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f4_completion_wiring_and_predicate():
    src = _display_src()
    sc = _method_src(src, COMPLETE_FN)
    if sc is None or ("empty=" not in sc and "empty =" not in sc):
        return False, f"{COMPLETE_FN} does not pass an empty= signal to the ledger"
    fn = _load_predicate()
    if fn is None:
        return False, f"{PREDICATE_FN} not found / not extractable"

    tmp = tempfile.mkdtemp(prefix="addstk_empty_f4_")
    try:
        root = _bank(tmp)
        _seed_absent(root, "MIXED", present_intervals=("1d",))  # 1d yes, hvol no
        _seed_present(root, "HASX", "1d-hvol")

        class _Fake:
            pass
        fake = _Fake()
        fake._storage_root = root
        probs = []
        if fn(fake, "MIXED", "1d-hvol") is not True:
            probs.append("absent 1d-hvol not reported empty")
        if fn(fake, "MIXED", "1d") is not False:
            probs.append("present 1d wrongly reported empty")
        if fn(fake, "HASX", "1d-hvol") is not False:
            probs.append("present 1d-hvol wrongly reported empty")
        if fn(fake, "NOSUCH", "1d-hvol") is not False:
            probs.append("absent ticker/manifest must fail SAFE to False")
        if probs:
            return False, "; ".join(probs)
        return True, (f"{COMPLETE_FN} passes empty=; {PREDICATE_FN} True only on "
                      "a confirmed-empty interval, safe-False otherwise")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def f5_portfree_heals():
    src = _display_src()
    pf = _method_src(src, PORTFREE_FN)
    if pf is None:
        return False, f"{PORTFREE_FN} not found"
    if "exempt_empty_verification" not in pf:
        return False, (f"{PORTFREE_FN} does not call exempt_empty_verification "
                       "— legacy stuck series (HONA) never heal")
    if PREDICATE_FN not in pf:
        return False, (f"{PORTFREE_FN} does not wire {PREDICATE_FN} as the disk "
                       "predicate")
    return True, (f"{PORTFREE_FN} heals via exempt_empty_verification wired to "
                  f"{PREDICATE_FN}")


def f6_safety_invariants():
    tmp = tempfile.mkdtemp(prefix="addstk_empty_f6_")
    try:
        root = _bank(tmp)
        # A present series that a buggy predicate might call empty must never be
        # exempted, and a NON-complete series is never touched.
        _seed_present(root, "HASX", "1d-hvol")
        _seed_absent(root, "PENDINGX", present_intervals=("1d",))
        rid = _new_run(
            root, [("HASX", "1d-hvol"), ("PENDINGX", "1d-hvol")],
            ["HASX", "PENDINGX"])
        arm.mark_series_complete(root, rid, "HASX", "1d-hvol")
        # PENDINGX left NOT complete (state pending)
        arm.mark_fetch_finished(root, rid)
        res = arm.exempt_empty_verification(
            root, rid, lambda t, iv: True)  # predicate says "empty" for ALL
        exempted = {tuple(x) for x in (res.get("exempted") or ())}
        # HASX is complete + predicate=True -> exempted is allowed;
        # PENDINGX is NOT complete -> must NOT be exempted regardless.
        pend = _series(root, "PENDINGX")
        if pend["xval"] or pend["gaps"]:
            return False, "a NON-complete series was exempted"
        if ("PENDINGX", "1d-hvol") in exempted:
            return False, "non-complete series appeared in exempted set"
        return True, ("only complete series can be exempted; a pending series is "
                      "never touched by the heal")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


BASELINE = [
    ("B1 feature-absence pin", b1_absence),
    ("B2 stuck empty-series reproduction", b2_stuck_reproduces),
    ("B3 display prevention unwired", b3_display_unwired),
]

FEATURE = [
    ("F1 source prevention (empty= at completion)", f1_source_prevention),
    ("F2 retroactive heal + no over-exemption", f2_heal_and_no_over_exemption),
    ("F3 heal completes + archives the run", f3_heal_completes_and_archives),
    ("F4 completion wiring + disk predicate", f4_completion_wiring_and_predicate),
    ("F5 port-free verifier heals legacy debt", f5_portfree_heals),
    ("F6 safety invariants (complete-only, no over-exempt)", f6_safety_invariants),
]


def main():
    feature = _feature_present()
    checks = FEATURE if feature else BASELINE
    mode = "FEATURE" if feature else "BASELINE (feature absent)"
    print(f"addstock_empty_verify_reference — mode: {mode}")
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
