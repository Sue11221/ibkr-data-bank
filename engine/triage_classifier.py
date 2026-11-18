"""Classify storage anomaly flags into deterministic repair verdicts.

This is the production counterpart to triage_classifier_reference.py. It turns
boundary/seam flags into self-explaining verdicts that can be ranked in the
unified health report. A supplied current split-audit row takes precedence over
the legacy ratio heuristic. Network access is limited to classify_flag(), where
the external daily reference is fetched for a supplied flag; health injects an
offline fail-closed reference when it has no exact split-context match.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics as st
import sys
from pathlib import Path

import stock_storage as ss
import stock_validate as sv


TRIAGE_REPORT_FILE = "_triage_report.json"

TOL_NOW = 0.05
TOL_FLAT = 0.05
INT_TOL = 0.04
CV_UNIFORM = 0.08
WIN = 20
NOW_WIN = 20  # current-anchor window: short so a RECENT corporate action can't drag the
              # tail median into the pre-action basis (FDX FedEx-Freight spinoff 2026-06-01)
SPLIT_SET = [2, 3, 4, 5, 6, 8, 10, 12, 15, 20]

NEEDS_HUMAN = {"PHANTOM", "IDENTITY_BASIS", "LOCAL_SUSPECT",
               "UNVERIFIABLE", "SHORT"}
SPLIT_CONTEXT_VERDICTS = {
    "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
    "UNVERIFIABLE", "STALE", "REAL", "RESOLVED", "CLEAN",
}
SPLIT_REPAIR_VERDICTS = {
    "PHANTOM", "MISSING", "REGRESSION", "IDENTITY_BASIS",
}
VERDICT_RANK = {
    "PHANTOM": 0,
    "REGRESSION": 0,
    "MISSING": 0,
    "LOCAL_SUSPECT": 1,
    "IDENTITY_BASIS": 2,
    "UNVERIFIABLE": 3,
    "SHORT": 4,
    "BENIGN_ACTION": 5,
    "REAL": 6,
    "RESOLVED": 7,
}
BOUNDARY_KEYS = ("ex_date", "boundary", "date", "seam_date")
FLAG_CONTAINER_KEYS = (
    "triage_flags", "boundary_flags", "seam_flags", "deep_seams",
    "seams", "join_flags", "split_flags", "rows",
    "confirmed_phantom_split_tickers", "split_like_review_tickers",
    "benign_or_reference_basis", "benign_or_reference_basis_tickers",
    "benign_or_reference_basis_steps",
)


def _parse_date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value or "").strip()
    if len(text) < 10:
        raise ValueError(f"bad boundary date: {value!r}")
    return dt.date.fromisoformat(text[:10])


def _stored_close(value):
    if isinstance(value, dict):
        value = value.get("close") or value.get("c")
    elif isinstance(value, (list, tuple)) and len(value) >= 5:
        value = value[4]
    return float(value)


def _ref_close(value):
    if isinstance(value, dict):
        value = value.get("close") or value.get("c")
    elif isinstance(value, (list, tuple)) and len(value) >= 4:
        value = value[3]
    return float(value)


def _stored_1d(root, ticker):
    out = {}
    root = Path(root)
    ticker = ss.canonical_ticker(ticker)
    man = ss.load_manifest(root / ticker) or {}
    for ym in ss.manifest_months(man, "1d"):
        try:
            y, m = int(str(ym)[:4]), int(str(ym)[5:7])
        except ValueError:
            continue
        path = (ss.find_month_file(root, ticker, y, m, "1d")
                or ss.month_file_path(root, ticker, y, m, "1d"))
        try:
            bars, _meta = ss.read_month_file(path)
        except Exception:  # noqa: BLE001 - unreadable months yield SHORT/low info
            continue
        for bar in bars:
            out[str(bar[0])[:10]] = float(bar[4])
    return out


def _cv(vals):
    vals = [float(v) for v in vals if v is not None]
    if len(vals) < 2:
        return 0.0
    mean = st.mean(vals)
    return (st.pstdev(vals) / mean) if mean else 0.0


def _base(ticker, boundary):
    return {"ticker": str(ticker).strip().upper(),
            "boundary": _parse_date(boundary).isoformat()}


def _round(value, digits=4):
    return round(float(value), digits) if value is not None else None


def _resolved_identity_truncation(root, ticker, boundary):
    """Return the correction note when an old boundary was intentionally removed."""
    ticker = ss.canonical_ticker(ticker)
    boundary = _parse_date(boundary)
    manifest = ss.load_manifest(Path(root) / ticker) or {}
    raw_months = (((manifest.get("intervals") or {}).get("1d") or {})
                  .get("months") or {})
    present_months = sorted(
        str(month)[:7] for month, entry in raw_months.items()
        if isinstance(entry, dict)
        and str(entry.get("status") or "present").lower() == "present")
    try:
        corrections = ss.identity_correction_records(
            manifest, "1d", ticker=ticker)
    except ss.StorageError:
        return None
    for note, cutover in corrections:
        if (boundary < cutover and present_months
                and present_months[0] >= cutover.isoformat()[:7]):
            first_month = present_months[0]
            try:
                year, month = int(first_month[:4]), int(first_month[5:7])
                path = (ss.find_month_file(
                    root, ticker, year, month, "1d")
                    or ss.month_file_path(
                        root, ticker, year, month, "1d"))
                bars, _meta = ss.read_month_file(path)
                first_bar = min(bar[0].date() for bar in bars)
            except Exception:  # noqa: BLE001 - unreadable is never resolved
                continue
            if first_bar >= cutover:
                return note
    return None


def classify_series(ticker, boundary, stored, ref):
    """Classify one boundary using already-loaded stored/reference daily closes.

    ``stored`` is {YYYY-MM-DD: close}. ``ref`` accepts either
    {YYYY-MM-DD: (o,h,l,c,v)} or {YYYY-MM-DD: close}. This pure function is the
    decision tree; classify_flag() is only the storage/reference loading wrapper.
    """
    boundary_date = _parse_date(boundary)
    bi = boundary_date.isoformat()
    base = _base(ticker, boundary_date)

    days = []
    ratios = {}
    for day in sorted(set(stored or {}) & set(ref or {})):
        try:
            sc = _stored_close(stored[day])
            rc = _ref_close(ref[day])
        except (TypeError, ValueError):
            continue
        if sc and rc:
            days.append(day)
            ratios[day] = sc / rc

    if len(days) < 3 * WIN:
        return {
            **base,
            "verdict": "SHORT",
            "confidence": "low",
            "bucket": "needs-human",
            "recommended_action": "insufficient overlap",
            "evidence": {"overlap_days": len(days)},
        }

    r_now = st.median([ratios[d] for d in days[-NOW_WIN:]])
    pre_days = [d for d in days if d < bi][-WIN:]
    post_days = [d for d in days if d >= bi][:WIN]
    pre = st.median([ratios[d] for d in pre_days]) if pre_days else None
    post = st.median([ratios[d] for d in post_days]) if post_days else None
    step = (post / pre) if (pre and post) else 1.0

    by_year = {}
    for day in days:
        if day < bi:
            by_year.setdefault(day[:4], []).append(ratios[day])
    deep_cv = _cv([st.median(v) for v in by_year.values()])

    abs_factor = step if step >= 1 else (1.0 / step if step else 1.0)
    near = min(SPLIT_SET, key=lambda value: abs(value - abs_factor))
    is_split_mag = abs(near - abs_factor) / near < INT_TOL
    evidence = {
        "r_now": _round(r_now),
        "pre_ratio": _round(pre),
        "post_ratio": _round(post),
        "step": _round(step),
        "abs_factor": _round(abs_factor, 3),
        "near_split": near if is_split_mag else None,
        "deep_cv": _round(deep_cv),
        "deep_years": len(by_year),
    }

    if abs(r_now - 1.0) > TOL_NOW:
        verdict = "LOCAL_SUSPECT"
        confidence = "high"
        action = (
            "stored disagrees with external at current dates; check the live "
            "data, not just history")
    elif abs(step - 1.0) <= TOL_FLAT:
        verdict = "REAL"
        confidence = "high"
        action = (
            "both sources adjust across the date; real corporate action, no "
            "defect")
    elif is_split_mag and deep_cv < CV_UNIFORM:
        verdict = "PHANTOM"
        confidence = "high"
        action = (
            f"uniform x{near} split the external lacks; external-verify, then "
            "phantom-split correction (join gate: never auto-apply)")
    elif is_split_mag:
        verdict = "IDENTITY_BASIS"
        confidence = "high"
        action = (
            "split-magnitude step but non-uniform deep ratio; predecessor "
            "splice or identity/basis lineage review, not an xN correction")
    else:
        verdict = "BENIGN_ACTION"
        confidence = "high"
        action = (
            "non-split step; spinoff/special-dividend convention difference; "
            "stored current anchor is correct")

    bucket = "needs-human" if verdict in NEEDS_HUMAN else "auto-benign"
    return {
        **base,
        "verdict": verdict,
        "confidence": confidence,
        "bucket": bucket,
        "recommended_action": action,
        "evidence": evidence,
    }


def classify_flag(root, ticker, boundary, ref=None, ref_fn=None):
    """Classify one storage boundary flag.

    ``ref_fn`` is injectable for tests and must accept ``(ticker, rng)``.
    """
    base = _base(ticker, boundary)
    resolved = _resolved_identity_truncation(
        root, base["ticker"], base["boundary"])
    if resolved is not None:
        return {
            **base,
            "verdict": "RESOLVED",
            "confidence": "high",
            "bucket": "auto-benign",
            "recommended_action": (
                "boundary removed by verified identity truncation; no action"),
            "evidence": {
                "correction_type": resolved.get("type"),
                "cutover": resolved.get("cutover"),
                "applied": resolved.get("applied"),
                "snapshot": resolved.get("snapshot"),
            },
        }
    try:
        ref = ref if ref is not None else (ref_fn or sv.fetch_daily_reference)(
            base["ticker"], rng="Max")
    except Exception as exc:  # noqa: BLE001 - report, do not abort batch
        return {
            **base,
            "verdict": "UNVERIFIABLE",
            "confidence": "low",
            "bucket": "needs-human",
            "recommended_action": "no external ref",
            "evidence": {"error": str(exc)[:120]},
        }
    return classify_series(base["ticker"], base["boundary"],
                           _stored_1d(root, base["ticker"]), ref)


def _boundary_from_flag(flag):
    if isinstance(flag, dict):
        for key in BOUNDARY_KEYS:
            if flag.get(key):
                return flag[key]
    elif isinstance(flag, (list, tuple)) and len(flag) >= 2:
        return flag[1]
    return None


def _ticker_from_flag(flag):
    if isinstance(flag, dict):
        return flag.get("ticker") or flag.get("symbol")
    if isinstance(flag, (list, tuple)) and flag:
        return flag[0]
    return None


def normalize_flag(flag):
    ticker = _ticker_from_flag(flag)
    boundary = _boundary_from_flag(flag)
    if not ticker or not boundary:
        return None
    out = {
        "ticker": str(ticker).strip().upper(),
        "boundary": _parse_date(boundary).isoformat(),
    }
    if isinstance(flag, dict):
        for key in ("source", "verdict", "factor", "abs_factor",
                    "near_split", "split_like", "classification"):
            if key in flag:
                out[key] = flag[key]
    return out


def _dedupe_flags(flags):
    seen = set()
    out = []
    for flag in flags or []:
        norm = normalize_flag(flag)
        if not norm:
            continue
        key = (norm["ticker"], norm["boundary"])
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _rank_key(row):
    verdict = str((row or {}).get("verdict") or "")
    ticker = str((row or {}).get("ticker") or "")
    boundary = str((row or {}).get("boundary") or "")
    return (VERDICT_RANK.get(verdict, 99), ticker, boundary)


def _split_context_rows(context):
    """Return current, dated rows from a split-audit report or ticker result."""
    if not isinstance(context, dict) or context.get("error"):
        return []
    raw = []
    if isinstance(context.get("rows"), list):
        raw.extend(context["rows"])
    for ticker_result in context.get("tickers") or []:
        if isinstance(ticker_result, dict):
            raw.extend(ticker_result.get("rows") or [])
    rows = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        if str(row.get("scope") or "current").strip().lower() != "current":
            continue
        verdict = str(row.get("verdict") or "").strip().upper()
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            day = _parse_date(row.get("date")).isoformat()
        except (TypeError, ValueError):
            continue
        if ticker and verdict in SPLIT_CONTEXT_VERDICTS:
            rows.append({**row, "ticker": ticker, "date": day,
                         "verdict": verdict})
    return rows


def _classify_from_split_context(flag, context):
    """Use one unambiguous exact current split row; never infer near matches."""
    matches = [
        row for row in _split_context_rows(context)
        if row["ticker"] == flag["ticker"] and row["date"] == flag["boundary"]
    ]
    verdicts = {row["verdict"] for row in matches}
    if len(verdicts) != 1:
        return None
    verdict = next(iter(verdicts))
    # Candidate rows carry the review-state distinction when duplicate event
    # and candidate rows agree on the same verdict/date.
    matches.sort(key=lambda row: (
        0 if row.get("row_type") == "candidate" else 1,
        str(row.get("row_type") or "")))
    row = matches[0]
    if verdict in SPLIT_REPAIR_VERDICTS:
        bucket = "needs-human"
    elif verdict == "UNVERIFIABLE" and row.get("needs_confirmation"):
        bucket = "needs-confirmation"
    elif verdict in {"STALE", "UNVERIFIABLE"}:
        bucket = "operational"
    else:
        bucket = "auto-benign"
    return {
        "ticker": flag["ticker"],
        "boundary": flag["boundary"],
        "verdict": verdict,
        "confidence": str(row.get("confidence") or "low"),
        "bucket": bucket,
        "recommended_action": str(
            row.get("recommended_action") or "review split evidence"),
        "evidence": {
            "source": "split_audit",
            "split_date": row["date"],
            "split_row_type": row.get("row_type"),
            "split_scope": "current",
            "split_reason": row.get("reason"),
        },
    }


def classify_flags(root, flags, ref_fn=None, split_context=None):
    rows = []
    errors = []
    for flag in _dedupe_flags(flags):
        try:
            row = _classify_from_split_context(flag, split_context)
            if row is None:
                row = classify_flag(root, flag["ticker"], flag["boundary"],
                                    ref_fn=ref_fn)
            row["source_flag"] = flag
        except Exception as exc:  # noqa: BLE001 - one bad flag cannot stop all
            row = {
                "ticker": flag.get("ticker"),
                "boundary": flag.get("boundary"),
                "verdict": "UNVERIFIABLE",
                "confidence": "low",
                "bucket": "needs-human",
                "recommended_action": "classifier error",
                "evidence": {"error": f"{type(exc).__name__}: {exc}"[:120]},
                "source_flag": flag,
            }
            errors.append(row)
        rows.append(row)
    return sorted(rows, key=_rank_key), errors


def flags_from_report(report):
    """Extract seam/boundary flags from a scan or future health component.

    The current WS5 report has no seam flags by default; this helper is for
    callers that attach or load a deep-seam/boundary component.
    """
    found = []

    def add_from(value):
        if isinstance(value, list):
            for item in value:
                add_from(item)
        elif isinstance(value, dict):
            norm = normalize_flag(value)
            if norm:
                found.append(norm)
            for key in FLAG_CONTAINER_KEYS:
                if key in value:
                    add_from(value[key])

    if isinstance(report, dict) and (
            isinstance(report.get("analysis"), dict)
            or isinstance(report.get("triage"), dict)):
        curated = []
        if isinstance(report.get("analysis"), dict):
            curated.append(report["analysis"])
        if isinstance(report.get("triage"), dict):
            curated.append(report["triage"])
        for section in curated:
            for key in FLAG_CONTAINER_KEYS:
                if key in section:
                    add_from(section[key])
    else:
        add_from(report)
    return _dedupe_flags(found)


def audit(root, flags=None, ref_fn=None, split_context=None, write=False,
          asof=None):
    root = Path(root)
    asof = asof or dt.datetime.now().isoformat(timespec="seconds")
    rows, errors = classify_flags(
        root, flags or [], ref_fn=ref_fn, split_context=split_context)
    by_verdict = {}
    by_bucket = {}
    for row in rows:
        by_verdict[row["verdict"]] = by_verdict.get(row["verdict"], 0) + 1
        by_bucket[row["bucket"]] = by_bucket.get(row["bucket"], 0) + 1
    report = {
        "kind": "triage_classifier",
        "version": 1,
        "asof": asof,
        "root": str(root),
        "input_count": len(_dedupe_flags(flags or [])),
        "classified": rows,
        "counts": {
            "by_verdict": dict(sorted(by_verdict.items())),
            "by_bucket": dict(sorted(by_bucket.items())),
            "needs_human": by_bucket.get("needs-human", 0),
            "needs_confirmation": by_bucket.get("needs-confirmation", 0),
            "operational": by_bucket.get("operational", 0),
            "auto_benign": by_bucket.get("auto-benign", 0),
        },
        "queue": format_queue({"classified": rows}),
        "errors": errors,
        "clean": not by_bucket.get("needs-human") and not errors,
    }
    if write:
        report["report_path"] = str(root / TRIAGE_REPORT_FILE)
        write_report(root, report)
    return report


def write_report(root, report):
    target = Path(root) / TRIAGE_REPORT_FILE
    out = dict(report or {})
    out["report_path"] = str(target)
    payload = json.dumps(out, indent=2, sort_keys=True).encode("utf-8")
    ss._atomic_write_bytes(target, payload)
    return str(target)


def load_report(root):
    try:
        data = json.loads(
            (Path(root) / TRIAGE_REPORT_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def summary_counts(report):
    rows = list((report or {}).get("classified") or [])
    counts = (report or {}).get("counts") or {}
    by_verdict = counts.get("by_verdict") or {}
    return {
        "input": int((report or {}).get("input_count") or len(rows)),
        "classified": len(rows),
        "needs_human": int(counts.get("needs_human") or 0),
        "needs_confirmation": int(counts.get("needs_confirmation") or 0),
        "operational": int(counts.get("operational") or 0),
        "auto_benign": int(counts.get("auto_benign") or 0),
        "phantom": int(by_verdict.get("PHANTOM") or 0),
        "identity_basis": int(by_verdict.get("IDENTITY_BASIS") or 0),
        "local_suspect": int(by_verdict.get("LOCAL_SUSPECT") or 0),
        "short": int(by_verdict.get("SHORT") or 0),
        "unverifiable": int(by_verdict.get("UNVERIFIABLE") or 0),
        "errors": len((report or {}).get("errors") or []),
    }


def format_queue(report):
    out = []
    for row in (report or {}).get("classified") or []:
        if row.get("bucket") == "needs-human" and row.get("ticker"):
            out.append(str(row["ticker"]).upper())
    return sorted(set(out))


def _names(vals, max_names=12):
    vals = [str(v) for v in vals if v]
    if not vals:
        return "none"
    if len(vals) <= max_names:
        return ", ".join(vals)
    return ", ".join(vals[:max_names]) + f", ... +{len(vals) - max_names}"


def summarize_report(report, max_names=12):
    c = summary_counts(report)
    lines = [
        ("Triage: "
         f"{c['classified']} classified, {c['needs_human']} needs-human, "
         f"{c['needs_confirmation']} needs-confirmation, "
         f"{c['operational']} operational, {c['auto_benign']} auto-benign.")
    ]
    needs = [
        f"{row.get('ticker')}:{row.get('verdict')}"
        for row in report.get("classified") or []
        if row.get("bucket") == "needs-human"
    ]
    benign = [
        f"{row.get('ticker')}:{row.get('verdict')}"
        for row in report.get("classified") or []
        if row.get("bucket") == "auto-benign"
    ]
    confirmation = [
        f"{row.get('ticker')}:{row.get('verdict')}"
        for row in report.get("classified") or []
        if row.get("bucket") == "needs-confirmation"
    ]
    operational = [
        f"{row.get('ticker')}:{row.get('verdict')}"
        for row in report.get("classified") or []
        if row.get("bucket") == "operational"
    ]
    if needs:
        lines.append("Triage needs-human: " + _names(needs, max_names))
    if benign:
        lines.append("Triage auto-benign: " + _names(benign, max_names))
    if confirmation:
        lines.append(
            "Triage needs-confirmation: "
            + _names(confirmation, max_names))
    if operational:
        lines.append("Triage operational: " + _names(operational, max_names))
    if report.get("errors"):
        lines.append(f"Triage errors: {len(report.get('errors') or [])}")
    rp = report.get("report_path")
    if rp:
        lines.append(f"Triage report saved: {rp}")
    return lines


KNOWN_CASES = [
    ("FTNT", "2014-01-13", "REAL"),
    ("WBD", "2021-03-30", "IDENTITY_BASIS"),
    ("ADP", "2014-09-25", "BENIGN_ACTION"),
    ("EXC", "2022-02-01", "BENIGN_ACTION"),
    ("NVDA", "2024-06-10", "REAL"),
    ("FDX", "2026-05-18", "BENIGN_ACTION"),  # recent FedEx Freight spinoff — NOW_WIN=60 regression
]


def _load_flags_from_json(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return flags_from_report(data)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=str(ss.storage_root(".")))
    parser.add_argument("--flag", action="append", default=[],
                        help="TICKER:YYYY-MM-DD boundary flag")
    parser.add_argument("--from-json", action="append", default=[],
                        help="load flags from a scan/report JSON")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    flags = []
    expected = {}
    for spec in args.flag:
        ticker, sep, boundary = spec.partition(":")
        if not sep:
            raise SystemExit("--flag requires TICKER:YYYY-MM-DD")
        flags.append({"ticker": ticker, "boundary": boundary})
    for path in args.from_json:
        flags.extend(_load_flags_from_json(path))
    if not flags:
        for ticker, boundary, verdict in KNOWN_CASES:
            flags.append({"ticker": ticker, "boundary": boundary})
            if (verdict == "IDENTITY_BASIS"
                    and _resolved_identity_truncation(
                        args.root, ticker, boundary) is not None):
                verdict = "RESOLVED"
            expected[(ticker, boundary)] = verdict

    report = audit(args.root, flags=flags, write=args.write)
    for line in summarize_report(report):
        print(line)
    ok = True
    if expected:
        print("\nKnown-case acceptance gate:")
        for row in report["classified"]:
            key = (row["ticker"], row["boundary"])
            want = expected.get(key)
            if want is None:
                continue
            got = row["verdict"]
            mark = "OK" if got == want else f"MISCLASSIFIED (want {want})"
            if got != want:
                ok = False
            print(f"  {row['ticker']:5} @ {row['boundary']}: {got:14} {mark}")
        print("\nGATE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
