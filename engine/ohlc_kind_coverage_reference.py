"""Claude-owned M0 acceptance harness for the OHLC-vs-kind coverage audit
(Fix 2a, Row 57): a READ-ONLY detector that flags any ticker where a derived
kind (iv/hvol) has present months its base OHLC lacks in an INTERIOR window
(bracketed by base data on both sides) -- the TMUS-class signature.

Offline / headless / deterministic: synthetic fixture manifests under a temp
root; no fetch, network, GUI, or production-bank access. Exit contract:
  3 = feature absent AND the baseline pins hold (expected before Codex builds M1)
  0 = every feature check passes (feature present)
  1 = harness failure (a check the harness itself could not run / a real defect)

Codex MUST NOT edit this file (owner split, ADDSTOCK/vol-value pattern). The
feature module Codex implements is `engine/ohlc_kind_coverage_audit.py` with:
  coverage_rows(manifest) -> [ {kind_token, base, interior_count,
                                first_month, last_month, interior_months}, ... ]
  audit(root, tickers=None, *, write_queue=True) -> {kind, version, complete,
                                scanned, flagged:[{ticker, rows}], queue_path}
"""
import json
import os
import shutil
import sys
import tempfile

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stock_storage as ss  # noqa: E402

MODULE = "ohlc_kind_coverage_audit"
try:
    audit_mod = __import__(MODULE)
    FEATURE = all(hasattr(audit_mod, name) for name in ("coverage_rows", "audit"))
except Exception:  # noqa: BLE001 - absent module is the expected baseline
    audit_mod = None
    FEATURE = False


# --- fixture construction --------------------------------------------------

def _months(start, end):
    """Inclusive YYYY-MM list from start to end."""
    y, m = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _write_manifest(root, ticker, intervals):
    """intervals: {token: [present YYYY-MM, ...]}. Returns the manifest path."""
    tdir = os.path.join(root, ss.canonical_ticker(ticker))
    os.makedirs(tdir, exist_ok=True)
    manifest = {
        "conid": 100000 + len(ticker),
        "intervals": {
            tok: {"months": {mo: {"status": "present"} for mo in mos}}
            for tok, mos in intervals.items()},
    }
    path = os.path.join(tdir, ss.MANIFEST_NAME)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, sort_keys=True)
    return path


def _fixtures(root):
    """The four canonical shapes; returns {name: manifest_path}."""
    base_present = _months("2011-07", "2013-04") + _months("2015-10", "2016-03")
    full = _months("2011-07", "2016-03")
    paths = {}
    # TMUS-shaped: base OHLC has an interior hole; both derived kinds do NOT.
    paths["TMUSLIKE"] = _write_manifest(root, "TMUSLIKE", {
        "1d": base_present, "1d-hvol": full,
        "1m": base_present, "1m-iv": full})
    # CLEAN: kind and base aligned.
    paths["CLEAN"] = _write_manifest(root, "CLEAN", {
        "1d": full, "1d-hvol": full})
    # FRONTIER: kind extends one month before AND after base (edge orphans only).
    paths["FRONTIER"] = _write_manifest(root, "FRONTIER", {
        "1d": _months("2020-02", "2020-11"),
        "1d-hvol": _months("2020-01", "2020-12")})
    # SHARED_HOLE: base and kind BOTH miss the same interior months.
    holed = _months("2018-01", "2018-06") + _months("2019-01", "2019-06")
    paths["SHAREDHOLE"] = _write_manifest(root, "SHAREDHOLE", {
        "1d": holed, "1d-hvol": holed})
    return paths


def _load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# Feature-free reference computation of the detector algorithm, so the baseline
# can assert the divergence is REAL and the feature can be checked against an
# oracle the feature module did not produce.
def _present(manifest, token):
    months = (manifest.get("intervals", {}).get(token, {}) or {}).get("months", {})
    return {mo for mo, v in months.items()
            if isinstance(v, dict) and v.get("status") == "present"}


def _interior_oracle(manifest, token):
    base = ss.base_interval(token)
    pk, pb = _present(manifest, token), _present(manifest, base)
    if not pb:
        return []
    orphans = pk - pb
    return sorted(o for o in orphans
                  if any(b < o for b in pb) and any(b > o for b in pb))


# --- check plumbing --------------------------------------------------------
_failures = []


def check(name, ok, detail=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        _failures.append(name)


def main():
    root = tempfile.mkdtemp(prefix="ohlc_kind_cov_")
    try:
        paths = _fixtures(root)
        tmus = _load(paths["TMUSLIKE"])
        oracle_hvol = _interior_oracle(tmus, "1d-hvol")
        oracle_iv = _interior_oracle(tmus, "1m-iv")

        if not FEATURE:
            print("ohlc_kind_coverage_reference -- mode: BASELINE (feature absent)\n")
            check("B1 audit module/API absent (expected pre-M1)", audit_mod is None)
            check("B2 TMUS-shaped interior divergence is real yet unflagged today",
                  len(oracle_hvol) == 29 and oracle_hvol[0] == "2013-05"
                  and oracle_hvol[-1] == "2015-09",
                  f"oracle interior={len(oracle_hvol)} "
                  f"{oracle_hvol[:1]}..{oracle_hvol[-1:]}")
            check("B3 aligned (CLEAN) fixture has zero interior orphans",
                  _interior_oracle(_load(paths["CLEAN"]), "1d-hvol") == [])
            if _failures:
                print(f"\nRESULT: {len(_failures)} baseline checks FAILED -> exit 1")
                return 1
            print("\nRESULT: feature absent, baseline pins hold -> exit 3")
            return 3

        print("ohlc_kind_coverage_reference -- mode: FEATURE (present)\n")
        by_kind = {r["kind_token"]: r for r in audit_mod.coverage_rows(tmus)}

        def matches(token, oracle):
            r = by_kind.get(token)
            return bool(r and r["base"] == ss.base_interval(token)
                        and r["interior_count"] == len(oracle)
                        and r["first_month"] == oracle[0]
                        and r["last_month"] == oracle[-1])

        check("F1 TMUS-shaped 1d-hvol flagged with the exact interior span",
              matches("1d-hvol", oracle_hvol), repr(by_kind.get("1d-hvol")))
        check("F5 multi-kind: 1m-iv flagged independently with its own span",
              matches("1m-iv", oracle_iv), repr(by_kind.get("1m-iv")))
        check("F2 frontier/warm-up skew (leading+trailing edges) NOT flagged",
              audit_mod.coverage_rows(_load(paths["FRONTIER"])) == [])
        check("F3 aligned coverage NOT flagged",
              audit_mod.coverage_rows(_load(paths["CLEAN"])) == [])
        check("F4 shared hole (base+kind miss same months) NOT flagged",
              audit_mod.coverage_rows(_load(paths["SHAREDHOLE"])) == [])

        before = {name: _load(p) for name, p in paths.items()}
        report = audit_mod.audit(root, write_queue=True)
        flagged = {row["ticker"] for row in (report.get("flagged") or [])}
        after = {name: _load(p) for name, p in paths.items()}
        qpath = report.get("queue_path")
        check("F6a audit() flags exactly the TMUS-shaped ticker",
              flagged == {"TMUSLIKE"}, f"flagged={flagged}")
        check("F6b audit() left every fixture manifest byte-identical (read-only)",
              before == after)
        check("F6c audit(write_queue=True) emitted the queue sidecar",
              bool(qpath) and os.path.exists(qpath), repr(qpath))
        check("F6d audit(write_queue=False) writes nothing",
              audit_mod.audit(root, write_queue=False).get("queue_path") is None)

        if _failures:
            print(f"\nRESULT: {len(_failures)} feature checks FAILED -> exit 1")
            return 1
        print("\nRESULT: all feature checks pass -> exit 0")
        return 0
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
