"""Minimum acceptance gate for the pure split detector.

This harness imports the production classifier. It proves that unmatched seams
only become PHANTOM with typed complete negative history and FTNT-like uniform
evidence. Run:

    python engine/split_detector_reference.py
"""

import sys
from pathlib import Path

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import split_detector as detector  # noqa: E402


def complete_coverage():
    return {
        "from": "1980-01-01",
        "through": "2026-12-31",
        "evidence_level": "issuer_explicit_history",
        "complete": True,
        "complete_basis": {
            "kind": "issuer_explicit_history",
            "source_url": "https://issuer.example/split-history",
            "payload_sha256": "b" * 64,
            "captured_at": "2026-07-09T12:00:00Z",
            "statement_excerpt": "Complete split history fixture.",
            "reason": "issuer explicitly enumerates every split",
            "source_ids": ["issuer-history"],
        },
        "source_ids": ["issuer-history"],
    }


def provisional_coverage():
    return {
        "from": "1980-01-01",
        "through": "2026-12-31",
        "evidence_level": "provisional",
        "complete": False,
        "source_ids": ["discovery-feed"],
    }


def event(ex_date, ratio, source_id):
    return {
        "ex_date": ex_date,
        "ratio": ratio,
        "ratio_convention": "new_shares_per_old_share",
        "confidence": "confirmed",
        "source_ids": [source_id],
    }


def history(events, coverage=None):
    return {
        "events": events,
        "coverage": coverage if coverage is not None else provisional_coverage(),
        "source_status": "ok",
    }


def candidate(ticker, date, factor, deep_cv=0.001, volume=None):
    return {
        "ticker": ticker,
        "date": date,
        "factor": factor,
        "deep_cv": deep_cv,
        "current_anchor_sound": True,
        "volume": volume,
    }


FTNT_HISTORY = history([
    event("2011-06-02", 2.0, "ftnt-2011"),
    event("2022-06-23", 5.0, "ftnt-2022"),
], complete_coverage())


def run_gate():
    failures = []
    total = [0]

    def expect(name, row, verdict, extra=True):
        total[0] += 1
        passed = row.get("verdict") == verdict and bool(extra)
        print(f"[{'PASS' if passed else 'FAIL'}] {name}: "
              f"{row.get('verdict')} ({row.get('reason')})")
        if not passed:
            failures.append({"name": name, "expected": verdict, "row": row})

    expect(
        "FTNT archived 2014 x4",
        detector.classify_candidate(
            candidate("FTNT", "2014-01-13", 4.0, deep_cv=0.0009),
            FTNT_HISTORY),
        "PHANTOM")
    expect(
        "FTNT confirmed 2022 x5",
        detector.classify_candidate(
            candidate("FTNT", "2022-06-23", 5.0), FTNT_HISTORY),
        "REAL")
    expect(
        "NVDA confirmed 2024 x10",
        detector.classify_candidate(
            candidate("NVDA", "2024-06-10", 10.0),
            history([event("2024-06-10", 10.0, "nvda-2024")])),
        "REAL")
    expect(
        "WBD drifting x2-like seam",
        detector.classify_candidate(
            candidate("WBD", "2021-03-30", 2.0, deep_cv=0.1289),
            history([], complete_coverage())),
        "IDENTITY_BASIS")
    expect(
        "incomplete negative history",
        detector.classify_candidate(
            candidate("UNKNOWN", "2014-01-13", 4.0),
            history([], provisional_coverage())),
        "UNVERIFIABLE")
    expect(
        "pre-XBRL CompanyFacts absence",
        detector.classify_candidate(
            candidate("LEGACY", "2004-02-02", 3.0),
            history([], {
                "from": "2009-01-01",
                "through": "2026-12-31",
                "evidence_level": "unknown",
                "complete": False,
                "source_ids": ["companyfacts-no-pre-xbrl-row"],
            })),
        "UNVERIFIABLE")
    expect(
        "confirmed reverse split",
        detector.classify_candidate(
            candidate("GE", "2021-08-02", 0.125),
            history([event("2021-08-02", 0.125, "ge-2021")])),
        "REAL")

    forward_missing = detector.classify_event_adjustment(
        "FTNT", event("2022-06-23", 5.0, "ftnt-2022"),
        {"pre_close": 100.0, "post_open": 20.0, "post_close": 20.5})
    expect("forward raw split jump", forward_missing, "MISSING")
    reverse_missing = detector.classify_event_adjustment(
        "GE", event("2021-08-02", 0.125, "ge-2021"),
        {"pre_close": 10.0, "post_open": 80.0, "post_close": 79.0})
    expect("reverse raw split jump", reverse_missing, "MISSING")
    expect(
        "small forward overlap",
        detector.classify_event_adjustment(
            "SMALLF", event("2020-01-02", 1.10, "small-forward"),
            {"observed_ratio": 1.0 / 1.10}),
        "UNVERIFIABLE")
    expect(
        "small reverse overlap",
        detector.classify_event_adjustment(
            "SMALLR", event("2020-02-03", 0.90, "small-reverse"),
            {"observed_ratio": 1.0 / 0.90}),
        "UNVERIFIABLE")
    expect(
        "distinguishable 1.20 raw jump",
        detector.classify_event_adjustment(
            "RAW12", event("2020-03-02", 1.20, "raw-1.2"),
            {"observed_ratio": 1.0 / 1.20}),
        "MISSING")

    correction = {
        "type": "phantom_split_correction",
        "ticker": "FTNT",
        "ex_date": "2014-01-13",
        "factor": 4.0,
        "applied": "2026-07-09T12:00:00Z",
    }
    resolved = detector.reconcile_corrections(
        "FTNT", [], FTNT_HISTORY, [correction], detection_current=True)
    expect("corrected FTNT clean refresh", resolved[0], "RESOLVED",
           len(resolved) == 1)
    recurrence = detector.reconcile_corrections(
        "FTNT", [candidate("FTNT", "2014-01-13", 4.0)],
        FTNT_HISTORY, [correction], detection_current=True)
    expect("corrected FTNT recurrence", recurrence[0], "REGRESSION",
           len(recurrence) == 1)

    inverse_volume = {
        "comparable": True,
        "pairs_pre": 20,
        "pairs_post": 20,
        "spread": 0.05,
        "step": 0.25,
    }
    volume_yes = detector.classify_candidate(
        candidate("VOLY", "2014-01-13", 4.0, volume=inverse_volume),
        history([], complete_coverage()))
    expect("comparable inverse volume", volume_yes, "PHANTOM",
           volume_yes["confidence"] == "high")

    noisy_volume = dict(inverse_volume, pairs_pre=3, spread=0.9)
    volume_noisy = detector.classify_candidate(
        candidate("VOLN", "2014-01-13", 4.0, volume=noisy_volume),
        history([], complete_coverage()))
    expect("noisy volume is neutral", volume_noisy, "PHANTOM",
           volume_noisy["confidence"] == "medium")

    volume_missing = detector.classify_candidate(
        candidate("VOLM", "2014-01-13", 4.0),
        history([], complete_coverage()))
    expect("missing volume is neutral", volume_missing, "PHANTOM",
           volume_missing["confidence"] == "medium")

    contradictory_volume = dict(inverse_volume, step=4.0)
    volume_no = detector.classify_candidate(
        candidate("VOLX", "2014-01-13", 4.0,
                  volume=contradictory_volume),
        history([], complete_coverage()))
    expect("contradictory comparable volume", volume_no, "UNVERIFIABLE")

    print(f"\nGATE: {'PASS' if not failures else 'FAIL'} "
          f"({total[0] - len(failures)}/{total[0]})")
    return not failures


if __name__ == "__main__":
    sys.exit(0 if run_gate() else 1)
