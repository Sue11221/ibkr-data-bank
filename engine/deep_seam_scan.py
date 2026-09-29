"""Bank-wide DEEP-SEAM scan — one-time audit for phantom / mis-applied split
adjustments like FTNT's phantom 4:1 (ex-date 2014-01-13; the deep segment was
stored at 1/4 of the truth). READ-ONLY — no writes, no basis actions. Network:
one stockanalysis reference fetch per ticker.

For each ticker it compares the stored 1d close series against the external
reference across the FULL range and finds sharp, PERSISTENT steps in the stored /
external ratio. The legacy ``scan_ticker`` API returns the largest step;
``scan_ticker_steps`` returns every clustered event candidate.
  * CLEAN = ratio flat across the whole history (both sources agree; a REAL split
            is handled by both, so it leaves no step).
  * SEAM  = a sharp ratio step -> needs triage:
      - PHANTOM (stored WRONG, like FTNT): a clean integer factor (x2..x20) that
        the external source does NOT show; the stored side fails to match reality
        at the stepped dates.  -> the real defect we are hunting.
      - BENIGN (stored RIGHT): a real spinoff / split that the RAW external simply
        doesn't adjust (e.g. ADP's 2014 CDK spinoff, EXC's 2022 CEG spinoff); the
        stored current-date anchor is correct.  -> false positive, not a defect.

Usage:
  python engine/deep_seam_scan.py            # PROOF sample — must exit 0
  from deep_seam_scan import scan_bank, scan_ticker_steps
"""
import hashlib
import math
import os
import statistics as st
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss
import stock_validate as sv

WIN = 20                                   # trading days each side of a boundary
TOL = 0.10                                 # |step-1| beyond this = a seam to triage
NOW_WIN = 20                               # recent ratio anchor, shared with triage
TOL_NOW = 0.05                             # current stored/reference anchor tolerance
CLUSTER_GAP = WIN                          # one rolling event spans about one window
SPLIT_SET = [2, 3, 4, 5, 6, 8, 10, 12, 15, 20]


class SeamScanError(RuntimeError):
    """Stored/reference evidence cannot support candidate detection."""


def manifest_fingerprint(months):
    """SHA-256 over sorted 1d manifest month state, matching validation."""
    h = hashlib.sha256()
    for month in sorted(months):
        entry = months.get(month) if isinstance(months.get(month), dict) else {}
        h.update(
            f"{month}:{entry.get('sha256')}:{entry.get('mtime_ns')}:"
            f"{entry.get('rows')}\n".encode("utf-8"))
    return h.hexdigest()


def _stored_1d_snapshot(root, ticker):
    """Strictly read one daily series and prove its manifest did not change."""
    out = {}
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    manifest = ss.load_manifest(root / ticker)
    if not manifest:
        raise SeamScanError(f"manifest unavailable for {ticker}")
    months = ss.manifest_months(manifest, "1d")
    fingerprint = manifest_fingerprint(months)
    for ym in sorted(months):
        try:
            y, m = int(str(ym)[:4]), int(str(ym)[5:7])
        except (TypeError, ValueError) as exc:
            raise SeamScanError(f"invalid 1d manifest month {ym!r}") from exc
        p = (ss.find_month_file(root, ticker, y, m, "1d")
             or ss.month_file_path(root, ticker, y, m, "1d"))
        try:
            bars, _meta = ss.read_month_file(p)
        except Exception as exc:  # noqa: BLE001 - strict evidence boundary
            raise SeamScanError(f"unreadable 1d month {ym}: {exc}") from exc
        for bar in bars:
            out[str(bar[0])[:10]] = float(bar[4])

    current = ss.load_manifest(root / ticker)
    if not current:
        raise SeamScanError(f"manifest disappeared while reading {ticker}")
    current_fingerprint = manifest_fingerprint(
        ss.manifest_months(current, "1d"))
    if current_fingerprint != fingerprint:
        raise SeamScanError(
            f"1d manifest changed while reading {ticker}; refresh again")
    return out, fingerprint


def _stored_1d(root, ticker):
    return _stored_1d_snapshot(root, ticker)[0]


def _close(value, index):
    if isinstance(value, dict):
        value = value.get("close", value.get("c"))
    elif isinstance(value, (list, tuple)):
        value = value[index]
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("close is not positive and finite")
    return number


def _cv(values):
    values = [float(value) for value in values]
    if len(values) < 2:
        return 0.0
    mean = st.mean(values)
    return st.pstdev(values) / abs(mean) if mean else 0.0


def _cluster_detections(detections):
    clusters = []
    for detection in detections:
        if (not clusters
                or detection["index"] - clusters[-1][-1]["index"]
                > CLUSTER_GAP):
            clusters.append([detection])
        else:
            clusters[-1].append(detection)
    return clusters


def _strongest(cluster):
    peak = max(row["strength"] for row in cluster)
    center = (cluster[0]["index"] + cluster[-1]["index"]) / 2.0
    tied = [
        row for row in cluster
        if math.isclose(row["strength"], peak, rel_tol=1e-12, abs_tol=1e-12)
    ]
    return min(tied, key=lambda row: (abs(row["index"] - center), row["index"]))


def detect_steps(ticker, stored, ref):
    """Pure all-candidate detector over stored/reference daily close mappings."""
    ticker = str(ticker).strip().upper()
    days, ratios = [], []
    for day in sorted(set(stored or {}) & set(ref or {})):
        try:
            stored_close = _close(stored[day], 4)
            reference_close = _close(ref[day], 3)
        except (IndexError, KeyError, TypeError, ValueError):
            continue
        days.append(str(day)[:10])
        ratios.append(stored_close / reference_close)

    if len(ratios) < 3 * WIN:
        return {
            "ticker": ticker,
            "verdict": "SHORT",
            "n": len(ratios),
            "candidates": [],
        }

    raw = []
    best_strength, best_step = 0.0, 1.0
    threshold = math.log(1 + TOL)
    for index in range(WIN, len(ratios) - WIN):
        pre = st.median(ratios[index - WIN:index])
        post = st.median(ratios[index:index + WIN])
        if pre <= 0 or post <= 0:
            continue
        step = post / pre
        strength = abs(math.log(step))
        if strength > best_strength:
            best_strength, best_step = strength, step
        if strength > threshold:
            raw.append({
                "index": index,
                "date": days[index],
                "pre": pre,
                "post": post,
                "step": step,
                "strength": strength,
            })

    if not raw:
        return {
            "ticker": ticker,
            "verdict": "CLEAN",
            "max_step": round(best_step, 4),
            "candidates": [],
        }

    current_ratio = st.median(ratios[-NOW_WIN:])
    candidates = []
    for cluster in _cluster_detections(raw):
        winner = _strongest(cluster)
        index = winner["index"]
        by_year = {}
        for day, ratio in zip(days[:index], ratios[:index]):
            by_year.setdefault(day[:4], []).append(ratio)
        deep_cv = _cv([st.median(values) for values in by_year.values()])
        step = winner["step"]
        abs_factor = max(step, 1.0 / step)
        near = min(SPLIT_SET, key=lambda value: abs(value - abs_factor))
        split_like = abs(near - abs_factor) / near < 0.04
        candidates.append({
            "ticker": ticker,
            "date": winner["date"],
            "boundary": winner["date"],
            "factor": round(step, 8),
            "abs_factor": round(abs_factor, 8),
            "deep_cv": round(deep_cv, 8),
            "current_anchor_sound": abs(current_ratio - 1.0) <= TOL_NOW,
            "split_like": split_like,
            "near_split": near if split_like else None,
            "pre_ratio": round(winner["pre"], 8),
            "post_ratio": round(winner["post"], 8),
            "current_ratio": round(current_ratio, 8),
            "strength": round(winner["strength"], 8),
            "deep_years": len(by_year),
            "cluster_size": len(cluster),
        })
    candidates.sort(key=lambda row: (row["date"], row["factor"]))
    return {
        "ticker": ticker,
        "verdict": "SEAM",
        "n": len(ratios),
        "candidates": candidates,
    }


def scan_ticker_steps(root, ticker, ref=None):
    """Return a structured result containing every clustered seam candidate."""
    try:
        if ref is None:
            ref = sv.fetch_daily_reference(ticker, rng="Max")
    except Exception as exc:  # noqa: BLE001 - structured scan failure
        return {
            "ticker": str(ticker).strip().upper(),
            "verdict": "UNVERIFIABLE",
            "why": str(exc)[:160],
            "candidates": [],
        }
    try:
        stored, fingerprint = _stored_1d_snapshot(root, ticker)
    except Exception as exc:  # noqa: BLE001 - structured scan failure
        return {
            "ticker": str(ticker).strip().upper(),
            "verdict": "UNVERIFIABLE",
            "why": str(exc)[:160],
            "candidates": [],
        }
    result = detect_steps(ticker, stored, ref)
    result["series_fingerprint"] = fingerprint
    return result


def scan_ticker(root, ticker, ref=None):
    result = scan_ticker_steps(root, ticker, ref=ref)
    if result["verdict"] != "SEAM":
        return {
            key: value for key, value in result.items()
            if key not in {"candidates", "series_fingerprint"}
        }
    largest = min(
        result["candidates"],
        key=lambda row: (-row["strength"], row["date"]),
    )
    return {
        "ticker": result["ticker"],
        "verdict": "SEAM",
        "boundary": largest["date"],
        "factor": round(largest["factor"], 4),
        "abs_factor": round(largest["abs_factor"], 3),
        "split_like": largest["split_like"],
        "near_split": largest["near_split"],
    }


def scan_bank(root, tickers=None):
    root = Path(root)
    if tickers is None:
        tickers = sorted(d for d in os.listdir(root)
                         if (root / d).is_dir() and not d.startswith("_"))
    seams, clean, other = [], 0, []
    for t in tickers:
        r = scan_ticker(root, t)
        if r["verdict"] == "SEAM":
            seams.append(r)
        elif r["verdict"] == "CLEAN":
            clean += 1
        else:
            other.append(r)
    # phantom-split suspects first (integer factor), then the rest
    seams.sort(key=lambda s: (not s.get("split_like"), -s.get("abs_factor", 0)))
    return {"n": len(tickers), "clean": clean, "seams": seams, "other": other}


if __name__ == "__main__":
    root = ss.storage_root(".")
    proof = ["FTNT", "NVDA", "AAPL", "MSFT", "ADP"]
    print("PROOF sample:")
    res = {}
    for t in proof:
        r = scan_ticker(root, t)
        res[t] = r
        tag = (f"SEAM x{r.get('factor')} @ {r.get('boundary')} "
               f"split_like={r.get('split_like')}") if r["verdict"] == "SEAM" else r["verdict"]
        print(f"  {t:6} {tag}")
    ok = (res["FTNT"]["verdict"] == "CLEAN"
          and res["NVDA"]["verdict"] == "CLEAN"
          and res["AAPL"]["verdict"] == "CLEAN"
          and res["MSFT"]["verdict"] == "CLEAN"
          and res["ADP"]["verdict"] == "SEAM"
          and not res["ADP"].get("split_like"))
    print("\nGATE:", "PASS (FTNT/NVDA/AAPL/MSFT clean; ADP benign seam control)"
          if ok else "FAIL")
    sys.exit(0 if ok else 1)
