"""Row 80 M1 acceptance harness — cross-validation lineage-boundary pins.

Claude-authored independent reference gate (CLAUDE.md acceptance bar).
Offline, deterministic, self-locating; builds a throwaway fixture bank in a
temp dir and exercises the REAL seams:

  * the manifest is written with ss.new_manifest/save_manifest (real writer),
  * the pin's interval fingerprint is captured with the PRODUCTION
    ss.interval_state_fingerprint — and cross_validate_ticker runs with its
    DEFAULT fingerprint path (fingerprint_fn is never injected anywhere here),
    so this gate proves a published pin will actually APPLY, not merely that
    fixture fingerprints round-trip (the one seam the Codex selftest injects);
  * reference/read data are injected (ref_fn/read_fn) — no network, no bank.

Checks (each numbered on failure):
  1  captured production fingerprint shape == pin schema field set exactly
  2  control (no pins file): pre+post corruption both flag
  3  applied pin: pre-boundary day bucketed, post-boundary day still active
  4  bucket evidence byte-faithful vs the control run's pre-boundary rows
  5  active score/status/severity == a genuine post-only base run (equivalence)
  6  boundary day exactly == boundary_date stays ACTIVE (strict <)
  7  mutation: deleting the pins file restores full flagging (pin load-bearing)
  8  known-only era -> validated, score 1.0, bucket carries the evidence
  9  fail-open battery (10 corruptions) -> control-identical scoring + loud
     LINEAGE PIN IGNORED + lineage_boundary_error, no bucket
 10  unpinned ticker (NVDA): result deep-equal with/without pins file
 11  vol kind (1d-hvol): no lineage fields under valid AND malformed policy,
     deep-equal to its no-file baseline (error path must not leak either)
 12  fixture bank custody: no run mutated the pins file or any manifest

F-CLAUDE-80-1 fix (pre-boundary-scoped pin identity) adds:
 13  scoped identity excludes later months and never collides with the
     unscoped one (distinct sha even when every month is in scope)
 14  APPEND-IMMUNITY: appending a later month leaves the pin applied
     (the old whole-interval binding staled here — this is the regression gate)
 15  ERA TAMPER still stales the pin: editing an in-era month, adding an
     earlier month, or a verified-absent change inside the era all fail open
 16  an old-style UNSCOPED pin fingerprint is rejected fail-open
 17  wrong-scope / cross-interval / cross-ticker scoped pins are rejected
 18  F-FABLE-80-2: a malformed HIGH-sorting month key / absence date fails
     closed in BOTH scoped and unscoped reads (scope filters run after
     validation), and the policy lands on the loud unavailable-era arm

Exit 0 iff every check passes.  Run three times for the approval bar:
    python engine/lineage_pin_reference.py
"""
from __future__ import annotations

import copy
import hashlib
import json
import shutil
import sys
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))

import stock_validate as sv  # noqa: E402
import stock_storage as ss   # noqa: E402

TICKER = "APO"
BOUNDARY = "2022-01-03"
PRE_DAY = "2021-12-22"       # weekday strictly before the boundary
POST_DAY = "2022-01-05"      # weekday strictly after the boundary
ASOF = "2026-07-31T00:00:00-04:00"
VOLATILE_KEYS = {"started_at", "finished_at"}

_PASS = [0]
_FAIL = [0]


def check(cond, name):
    if cond:
        _PASS[0] += 1
    else:
        _FAIL[0] += 1
        print("  FAIL:", name)


def weekday_bars(first, last, per_day=60, base=100.0):
    """1m TRADES bars for every weekday in [first, last]."""
    out = []
    d = first
    while d <= last:
        if d.weekday() < 5:
            t = datetime.combine(d, time(9, 30))
            p0 = base + (d.toordinal() % 17)
            for i in range(per_day):
                p = round(p0 + i * 0.01, 2)
                out.append((t, p, round(p + 0.05, 2), round(p - 0.05, 2),
                            round(p + 0.02, 2), 100 + i))
                t += timedelta(minutes=1)
        d += timedelta(days=1)
    return out


def reference_from(bars, corrupt_days=()):
    derived = sv.derive_daily(bars, min_bars=50)
    ref = {d.isoformat(): (o, h, lo, c, v)
           for d, (o, h, lo, c, v) in derived.items()}
    for day in corrupt_days:
        if day not in ref:
            raise SystemExit(f"fixture bug: {day} not a derived day")
        o, h, lo, c, v = ref[day]
        ref[day] = (o * 10, h * 10, lo * 10, c * 10, v)
    return ref


def month_entry(bars):
    return {"status": "present", "sha256": "c" * 64, "rows": len(bars),
            "first": "12/15/2021 9:30:00", "last": "1/14/2022 16:00:00"}


def write_manifest(root, ticker, intervals_months):
    tdir = root / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    manifest = ss.new_manifest(ticker, ticker)
    for interval, months in intervals_months.items():
        bucket = ss.manifest_months(manifest, interval)
        for month, entry in months.items():
            bucket[month] = dict(entry)
    ss.save_manifest(tdir, manifest)


def detail_of(result):
    return result.get("detail") or {}


def active_days(result):
    return {r.get("date") for r in detail_of(result).get("rows") or []}


def row_payload(rows):
    """Order-free data identity of evidence rows: (date, field, derived, ref)."""
    return sorted((r.get("date"), r.get("field"), r.get("derived"),
                   r.get("reference")) for r in rows or [])


def strip_volatile(result):
    if isinstance(result, dict):
        return {k: strip_volatile(v) for k, v in result.items()
                if k not in VOLATILE_KEYS}
    if isinstance(result, list):
        return [strip_volatile(v) for v in result]
    return result


def digest_tree(root):
    h = hashlib.sha256()
    for p in sorted(root.rglob("*.json"), key=lambda p: str(p).casefold()):
        h.update(str(p.relative_to(root)).casefold().encode())
        h.update(hashlib.sha256(p.read_bytes()).digest())
    return h.hexdigest()


def main():
    base = Path(tempfile.mkdtemp(prefix="lineage_pin_ref_"))
    root = base / "bank"
    pins_path = root / sv._LINEAGE_BOUNDARY_FILE
    try:
        bars = weekday_bars(date(2021, 12, 15), date(2022, 1, 14))
        vol_bars = [(datetime(2022, 1, 3 + i, 9, 30), 0.2, 0.21, 0.19,
                     0.2, 100) for i in range(3)]
        entry = month_entry(bars)
        write_manifest(root, TICKER, {
            "1m": {"2021-12": entry, "2022-01": entry},
            "1d-hvol": {"2022-01": entry},
        })
        write_manifest(root, "NVDA", {"1m": {"2021-12": entry,
                                             "2022-01": entry}})

        # -- 1: the production scoped fingerprint IS the pin schema ---------
        boundary_month = BOUNDARY[:7]
        captured = ss.interval_state_fingerprint_through_month(
            root, TICKER, "1m", boundary_month)
        unscoped = ss.interval_state_fingerprint(root, TICKER, "1m")
        pin_fields = {"schema_version", "algorithm", "sha256", "ticker",
                      "interval", "present", "backfill_incomplete",
                      "month_count", "verified_absent_count",
                      "scope_through_month"}
        check(set(captured) == pin_fields
              and captured["scope_through_month"] == boundary_month
              and sv._valid_lineage_fingerprint(captured, TICKER,
                                                boundary_month)
              and not sv._valid_lineage_fingerprint(unscoped, TICKER,
                                                    boundary_month),
              f"1: production scoped fingerprint shape == pin schema; the "
              f"unscoped one is rejected ({sorted(captured)})")

        pin = {
            "ticker": TICKER,
            "boundary_date": BOUNDARY,
            "verdict": "lineage_boundary",
            "scope": "price",
            "interval_fingerprints": [captured],
            "review": {
                "checkpoint": "ffe8fcb",
                "approval_commit": "9648b08",
                "approved_by": "USER",
                "approved_at": "2026-07-31T12:00:00-04:00",
                "user_verdict_date": "2026-07-31",
            },
        }

        def write_pins(entries, pad=0, **root_extra):
            payload = {"version": 1, "lineage_boundaries": entries}
            payload.update(root_extra)
            text = json.dumps(payload)
            if pad:
                text += " " * pad
            pins_path.write_text(text, encoding="utf-8")

        def run(ticker=TICKER, interval="1m", corrupt=()):
            # fingerprint_fn deliberately NOT passed: default production path.
            return sv._cross_validate_ticker_body(
                root, ticker, interval, asof=ASOF,
                read_fn=lambda *_a: (vol_bars if interval == "1d-hvol"
                                     else bars),
                ref_fn=lambda *_a, **_k: reference_from(bars, corrupt))

        # -- 2: control, no pins file ---------------------------------------
        if pins_path.exists():
            pins_path.unlink()
        control = run(corrupt=(PRE_DAY, POST_DAY))
        cdetail = detail_of(control)
        check(control.get("status") == "discrepancy"
              and cdetail.get("flagged_count") == 2
              and active_days(control) == {PRE_DAY, POST_DAY}
              and "lineage_boundary" not in control
              and "lineage_boundary_error" not in control
              and "known_lineage_era" not in cdetail,
              f"2: control flags both eras with no lineage fields "
              f"({control.get('status')}, {active_days(control)})")
        control_pre_rows = [r for r in cdetail.get("rows") or []
                            if r.get("date") == PRE_DAY]

        # -- 3/4: applied pin buckets pre, keeps post active -----------------
        write_pins([pin])
        pins_digest = hashlib.sha256(pins_path.read_bytes()).hexdigest()
        tree_digest = digest_tree(root)
        applied = run(corrupt=(PRE_DAY, POST_DAY))
        adetail = detail_of(applied)
        known = adetail.get("known_lineage_era") or {}
        check(applied.get("status") == "discrepancy"
              and adetail.get("flagged_count") == 1
              and active_days(applied) == {POST_DAY}
              and known.get("flagged_count") == 1
              and known.get("distinct_dates") == 1
              and known.get("boundary_date") == BOUNDARY
              and applied.get("lineage_boundary", {}).get("ticker") == TICKER
              and (applied.get("evidence_counts") or {}).get("row_count") == 4
              and "lineage_boundary_error" not in applied
              and "known lineage-era" in (applied.get("note") or ""),
              f"3: applied pin buckets {PRE_DAY}, keeps {POST_DAY} active "
              f"({applied.get('status')}, {active_days(applied)}, "
              f"{known.get('flagged_count')})")
        check(bool(control_pre_rows)
              and row_payload(known.get("examples"))
              == row_payload(control_pre_rows)
              and all(r.get("date") == PRE_DAY
                      for r in known.get("examples") or [{}]),
              f"4: bucket evidence data-identical to control pre-boundary rows "
              f"({len(control_pre_rows)} vs {len(known.get('examples') or ())})")

        # -- 5: active scoring == genuine post-only base run -----------------
        pins_path.unlink()
        post_only = run(corrupt=(POST_DAY,))
        check(applied.get("score") == post_only.get("score")
              and applied.get("status") == post_only.get("status")
              and applied.get("severity") == post_only.get("severity")
              and active_days(applied) == active_days(post_only)
              and adetail.get("flagged_count")
              == detail_of(post_only).get("flagged_count")
              and row_payload(adetail.get("rows"))
              == row_payload(detail_of(post_only).get("rows")),
              f"5: bucketed rescore equals base scoring of the active era "
              f"(applied {applied.get('score')}/{applied.get('severity')} vs "
              f"base {post_only.get('score')}/{post_only.get('severity')})")

        # -- 6: the boundary day itself stays active (strict <) --------------
        write_pins([pin])
        edge = run(corrupt=(BOUNDARY,))
        eknown = detail_of(edge).get("known_lineage_era") or {}
        check(edge.get("status") == "discrepancy"
              and active_days(edge) == {BOUNDARY}
              and detail_of(edge).get("flagged_count") == 1
              and eknown.get("flagged_count") == 0
              and "lineage_boundary_error" not in edge,
              f"6: boundary-day evidence stays fully active ({active_days(edge)})")

        # -- 7: mutation — delete the pin, full flagging returns -------------
        pins_path.unlink()
        unpinned = run(corrupt=(PRE_DAY, POST_DAY))
        check(active_days(unpinned) == {PRE_DAY, POST_DAY}
              and detail_of(unpinned).get("flagged_count") == 2
              and "known_lineage_era" not in detail_of(unpinned)
              and "lineage_boundary" not in unpinned,
              f"7: removing the pin restores full flagging ({active_days(unpinned)})")

        # -- 8: known-only era scores clean, evidence retained ----------------
        write_pins([pin])
        cleared = run(corrupt=(PRE_DAY,))
        kdetail = detail_of(cleared)
        kknown = kdetail.get("known_lineage_era") or {}
        check(cleared.get("status") == "validated"
              and cleared.get("score") == 1.0
              and kdetail.get("flagged_count") == 0
              and kknown.get("flagged_count") == 1
              and (kknown.get("examples") or [{}])[0].get("date") == PRE_DAY
              and "No active discrepancies" in (cleared.get("note") or ""),
              f"8: known-only era -> validated 1.0 with retained evidence "
              f"({cleared.get('status')}, {cleared.get('score')})")

        # -- 9: fail-open battery --------------------------------------------
        def corrupted_pins():
            dup = copy.deepcopy(pin)
            unknown = copy.deepcopy(pin)
            unknown["surprise"] = True
            bad_date = copy.deepcopy(pin)
            bad_date["boundary_date"] = "2022-1-3"
            stale = copy.deepcopy(pin)
            stale["interval_fingerprints"] = [dict(captured, sha256="b" * 64)]
            unbound = copy.deepcopy(pin)
            unbound["interval_fingerprints"] = [
                dict(captured, interval="1d")]
            contradict = copy.deepcopy(pin)
            contradict["review"] = dict(pin["review"],
                                        user_verdict_date="2021-12-31")
            return [
                ("malformed json", lambda: pins_path.write_text(
                    '{"version": 1,', encoding="utf-8")),
                ("wrong version", lambda: write_pins([pin], version=2)),
                ("extra root key", lambda: write_pins([pin], surplus=1)),
                ("duplicate pins", lambda: write_pins([pin, dup])),
                ("unknown pin field", lambda: write_pins([unknown])),
                ("noncanonical boundary", lambda: write_pins([bad_date])),
                ("stale fingerprint", lambda: write_pins([stale])),
                ("unbound interval", lambda: write_pins([unbound])),
                ("provenance contradiction", lambda: write_pins([contradict])),
                ("oversize policy", lambda: write_pins(
                    [pin], pad=sv._LINEAGE_BOUNDARY_MAX_BYTES)),
            ]

        for label, arm in corrupted_pins():
            arm()
            res = run(corrupt=(PRE_DAY, POST_DAY))
            check(res.get("status") == control.get("status")
                  and res.get("score") == control.get("score")
                  and active_days(res) == {PRE_DAY, POST_DAY}
                  and detail_of(res).get("flagged_count") == 2
                  and "known_lineage_era" not in detail_of(res)
                  and res.get("lineage_boundary_error")
                  and "LINEAGE PIN IGNORED" in (res.get("note") or ""),
                  f"9: fail-open [{label}] keeps control scoring + loud error "
                  f"({res.get('status')}, {res.get('lineage_boundary_error')!r})")

        # -- 10: unpinned ticker deep-equal with/without policy ---------------
        write_pins([pin])
        nvda_with = run(ticker="NVDA", corrupt=(PRE_DAY, POST_DAY))
        pins_path.unlink()
        nvda_without = run(ticker="NVDA", corrupt=(PRE_DAY, POST_DAY))
        check(strip_volatile(nvda_with) == strip_volatile(nvda_without)
              and "lineage_boundary" not in nvda_with
              and "lineage_boundary_error" not in nvda_with,
              "10: unpinned NVDA result deep-equal with/without the policy file")

        # -- 11: vol kinds never read the policy, even a malformed one --------
        vol_base = run(interval="1d-hvol")
        write_pins([pin])
        vol_valid = run(interval="1d-hvol")
        pins_path.write_text('{"version": 1,', encoding="utf-8")
        vol_broken = run(interval="1d-hvol")
        lineage_free = all(
            "lineage_boundary" not in r and "lineage_boundary_error" not in r
            and "known_lineage_era" not in (r.get("detail") or {})
            for r in (vol_base, vol_valid, vol_broken))
        check(lineage_free
              and strip_volatile(vol_valid) == strip_volatile(vol_base)
              and strip_volatile(vol_broken) == strip_volatile(vol_base)
              and vol_base.get("provider") == "internal-structural",
              "11: 1d-hvol identical under no/valid/malformed policy, no lineage fields")

        # -- 12: no run mutated the fixture bank ------------------------------
        write_pins([pin])
        check(hashlib.sha256(pins_path.read_bytes()).hexdigest() == pins_digest
              and digest_tree(root) == tree_digest,
              "12: pins file and manifests byte-identical after every run")

        # ==== F-CLAUDE-80-1: pre-boundary-scoped pin identity ===============
        # These checks deliberately MUTATE the fixture manifest, so they run
        # after the custody check above.
        base_months = {"2021-12": dict(entry), "2022-01": dict(entry)}

        def set_interval(months, verified_absent=None):
            tdir = root / TICKER
            manifest = ss.load_manifest(tdir)
            bucket = ss.manifest_months(manifest, "1m")
            bucket.clear()
            for month, value in months.items():
                bucket[month] = dict(value)
            manifest["intervals"]["1m"]["verified_absent"] = list(
                verified_absent or [])
            ss.save_manifest(tdir, manifest)

        def scoped(month=BOUNDARY[:7], ticker=TICKER, interval="1m"):
            return ss.interval_state_fingerprint_through_month(
                root, ticker, interval, month)

        # -- 13: scope excludes later months and never collides --------------
        set_interval(base_months)
        through_dec = scoped("2021-12")
        through_jan = scoped()
        wide = scoped("2099-12")
        full = ss.interval_state_fingerprint(root, TICKER, "1m")
        check(through_dec["month_count"] == 1
              and through_jan["month_count"] == 2
              and through_dec["sha256"] != through_jan["sha256"]
              and wide["month_count"] == full["month_count"]
              and wide["sha256"] != full["sha256"]
              and "scope_through_month" not in full,
              f"13: scoped identity honors its bound and cannot collide with "
              f"the unscoped one ({through_dec['month_count']}/"
              f"{through_jan['month_count']}/{wide['month_count']})")

        # -- 14: APPEND-IMMUNITY (the actual F-CLAUDE-80-1 regression gate) ---
        write_pins([pin])
        before_append = run(corrupt=(PRE_DAY, POST_DAY))
        set_interval({**base_months,
                      "2026-07": dict(entry, sha256="f" * 64, rows=999)},
                     verified_absent=["2026-07-04"])
        after_append = run(corrupt=(PRE_DAY, POST_DAY))
        appended_full = ss.interval_state_fingerprint(root, TICKER, "1m")
        appended_scoped = scoped()
        check(active_days(before_append) == {POST_DAY}
              and active_days(after_append) == {POST_DAY}
              and (detail_of(after_append).get("known_lineage_era") or {}
                   ).get("flagged_count") == 1
              and "lineage_boundary_error" not in after_append
              and appended_scoped == captured
              and appended_full["sha256"] != unscoped["sha256"],
              f"14: appending a later month leaves the pin APPLIED while the "
              f"whole-interval identity changed (old binding would have staled)"
              f" ({after_append.get('lineage_boundary_error')!r})")

        # -- 15: era tamper still stales the pin (three ways) -----------------
        tampers = (
            ("in-era month edited",
             lambda: set_interval({**base_months,
                                   "2021-12": dict(entry, sha256="9" * 64)})),
            ("earlier month added",
             lambda: set_interval({"2021-11": dict(entry), **base_months})),
            ("in-era absence added",
             lambda: set_interval(base_months,
                                  verified_absent=["2021-12-24"])),
        )
        for label, arm in tampers:
            arm()
            tampered = run(corrupt=(PRE_DAY, POST_DAY))
            check(active_days(tampered) == {PRE_DAY, POST_DAY}
                  and detail_of(tampered).get("flagged_count") == 2
                  and "known_lineage_era" not in detail_of(tampered)
                  and "fingerprint is stale" in (
                      tampered.get("lineage_boundary_error") or "")
                  and "LINEAGE PIN IGNORED" in (tampered.get("note") or ""),
                  f"15: era tamper [{label}] stales the pin fail-open "
                  f"({tampered.get('lineage_boundary_error')!r})")
        set_interval(base_months)

        # -- 16: an old-style UNSCOPED pin fingerprint is rejected ------------
        legacy = copy.deepcopy(pin)
        legacy["interval_fingerprints"] = [copy.deepcopy(unscoped)]
        write_pins([legacy])
        legacy_run = run(corrupt=(PRE_DAY, POST_DAY))
        check(active_days(legacy_run) == {PRE_DAY, POST_DAY}
              and "known_lineage_era" not in detail_of(legacy_run)
              and "malformed" in (legacy_run.get("lineage_boundary_error") or "")
              and "LINEAGE PIN IGNORED" in (legacy_run.get("note") or ""),
              f"16: a legacy whole-interval pin fingerprint fails open "
              f"({legacy_run.get('lineage_boundary_error')!r})")

        # -- 17: wrong-scope / cross-interval / cross-ticker pins rejected ----
        wrong_scope = copy.deepcopy(pin)
        wrong_scope["interval_fingerprints"] = [scoped("2021-12")]
        cross_ticker = copy.deepcopy(pin)
        cross_ticker["interval_fingerprints"] = [scoped(ticker="NVDA")]
        for label, bad in (("scope != boundary month", wrong_scope),
                           ("fingerprint of another ticker", cross_ticker)):
            write_pins([bad])
            res = run(corrupt=(PRE_DAY, POST_DAY))
            check(active_days(res) == {PRE_DAY, POST_DAY}
                  and "known_lineage_era" not in detail_of(res)
                  and res.get("lineage_boundary_error")
                  and "LINEAGE PIN IGNORED" in (res.get("note") or ""),
                  f"17: rejected [{label}] "
                  f"({res.get('lineage_boundary_error')!r})")

        # sanity: the valid pin still applies after all of the above
        write_pins([pin])
        restored = run(corrupt=(PRE_DAY, POST_DAY))
        check(active_days(restored) == {POST_DAY}
              and "lineage_boundary_error" not in restored,
              "17b: the valid scoped pin still applies after the battery")

        # -- 18: F-FABLE-80-2 — malformed high-sorting state fails closed -----
        # Fresh reviewer finding: the original pre-validation scope filter
        # silently DROPPED malformed keys sorting above the scope, so the
        # scoped read succeeded where the unscoped read raised. Both must
        # raise identically, and the policy must land on the loud
        # unavailable-era arm, never on a clean fingerprint.
        for label, mutate in (
            ("month key 9999-99", lambda man: ss.manifest_months(
                man, "1m").__setitem__("9999-99", dict(entry))),
            ("absence date 9999-99-99", lambda man: man["intervals"]["1m"]
             .__setitem__("verified_absent", ["9999-99-99"])),
        ):
            tdir = root / TICKER
            manifest = ss.load_manifest(tdir)
            mutate(manifest)
            ss.save_manifest(tdir, manifest)
            outcomes = {}
            for mode, fn in (
                ("unscoped", lambda: ss.interval_state_fingerprint(
                    root, TICKER, "1m")),
                ("scoped", lambda: ss.interval_state_fingerprint_through_month(
                    root, TICKER, "1m", BOUNDARY[:7])),
            ):
                try:
                    fn()
                    outcomes[mode] = "NO ERROR"
                except ss.StorageError:
                    outcomes[mode] = "StorageError"
            corrupt_run = run(corrupt=(PRE_DAY, POST_DAY))
            check(outcomes == {"unscoped": "StorageError",
                               "scoped": "StorageError"}
                  and "lineage-era fingerprint unavailable"
                  in (corrupt_run.get("lineage_boundary_error") or "")
                  and "known_lineage_era" not in detail_of(corrupt_run)
                  and active_days(corrupt_run) == {PRE_DAY, POST_DAY},
                  f"18: corrupt manifest [{label}] fails closed in both reads "
                  f"and the pin is loudly unavailable ({outcomes}, "
                  f"{corrupt_run.get('lineage_boundary_error')!r})")
            set_interval(base_months)
    finally:
        shutil.rmtree(base, ignore_errors=True)

    total = _PASS[0] + _FAIL[0]
    print(f"lineage_pin_reference: {_PASS[0]}/{total} checks passed"
          + ("" if not _FAIL[0] else f"  ({_FAIL[0]} FAILED)"))
    return 1 if _FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
