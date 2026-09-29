"""Stock Data Storage — Tier 3 (M1): basis verification (GUI-free).

The gates (Tier 1 basis gate, Tier 2 overlap/entry/join gates) HALT a
series when stored and incoming bars disagree on price or volume scale.
This module turns a halted overlap into an explained, recordable
DECISION instead of a dead end:

    stats   = measure_overlap(stored_bars, incoming_bars)
    verdict = classify(stats)
    action, why = propose_action(ticker, interval, verdict, boundary)
    # show `why` to the user; on approval only:
    apply_action(root, ticker, action)

NOTHING is auto-applied and NO bar bytes are ever rewritten (decision
D1, tier3_plan.md): an approved action is appended to the ticker
manifest's "actions" list, where the gates (M2) and read-time
adjustment (M3) consume it.

Action schema (manifest["actions"], kept sorted by date):
    {"date": "YYYY-MM-DD",      # first session on the NEW basis
     "kind": "split" | "price-basis" | "volume-scale",
     "factor": float,           # NEW = OLD * factor at the boundary
     "applies": "price" | "volume" | "both",
     "source": "measured" | "user",
     "evidence": str, "run": str | None}
"""

import math
import statistics
from datetime import date, datetime
from pathlib import Path

import stock_storage as ss

MIN_PAIRS = 30                 # below this, refuse to classify
PRICE_TOL = 0.005              # the same tolerance the gates use
TIGHT_SPREAD = 0.004           # p90/p10 - 1 under this = one clean factor
VOL_PLAIN_BAND = (0.5, 2.0)    # NBBO-vs-consolidated noise lives in here
SPLIT_MAX_TERM = 20            # splits are simple fractions p:q
SPLIT_FRAC_TOL = 0.002

ACTION_KINDS = ("split", "price-basis", "volume-scale")
ACTION_APPLIES = ("price", "volume", "both")


# --- measurement (pure) -------------------------------------------------------------

def measure_overlap(existing_bars, incoming_bars):
    """Per-bar ratio statistics on shared timestamps. Pure, no tree
    access. All ratios are INCOMING / EXISTING (new = old * factor)."""
    emap = {b[0]: b for b in existing_bars}
    pairs = [(emap[b[0]], b) for b in incoming_bars if b[0] in emap]
    stats = {"pairs": len(pairs)}
    if not pairs:
        return stats
    pr = [b[4] / e[4] for e, b in pairs if e[4] > 0]
    vr = [b[5] / e[5] for e, b in pairs if e[5] > 0 and b[5] > 0]
    agree = sum(1 for e, b in pairs
                if all(abs(b[i] - e[i]) / e[i] <= PRICE_TOL
                       for i in (1, 2, 3, 4) if e[i] > 0))
    per_day = {}
    for e, b in pairs:
        if e[4] > 0:
            per_day.setdefault(e[0].date(), []).append(b[4] / e[4])
    stats.update(
        price_agree_frac=agree / len(pairs),
        price_ratio_median=statistics.median(pr) if pr else None,
        price_ratio_spread=_spread(pr),
        volume_ratio_median=statistics.median(vr) if vr else None,
        volume_ratio_spread=_spread(vr),
        first_shared=pairs[0][0][0], last_shared=pairs[-1][0][0],
        per_day_price={d.isoformat(): round(statistics.median(v), 6)
                       for d, v in sorted(per_day.items())})
    return stats


def _spread(vals):
    """p90/p10 - 1 (0 = perfectly constant ratio)."""
    if not vals:
        return None
    s = sorted(vals)
    if s[0] <= 0:
        return float("inf")
    if len(s) < 5:
        return s[-1] / s[0] - 1.0
    lo = s[int(0.10 * (len(s) - 1))]
    hi = s[int(0.90 * (len(s) - 1))]
    return hi / lo - 1.0


def _simple_fraction(factor):
    """factor ~= p/q with small terms -> reduced (p, q), else None."""
    best = None
    for q in range(1, SPLIT_MAX_TERM + 1):
        p = round(factor * q)
        if not 1 <= p <= SPLIT_MAX_TERM:
            continue
        err = abs(factor - p / q) / (p / q)
        if err <= SPLIT_FRAC_TOL and (best is None or err < best[2]):
            best = (p, q, err)
    if best:
        g = math.gcd(best[0], best[1])
        p, q = best[0] // g, best[1] // g
        if (p, q) != (1, 1):
            return p, q
    return None


# --- classification (pure) ----------------------------------------------------------

def classify(stats):
    """-> {kind, factor, applies, confidence, explanation}. Refuses to
    guess: thin overlap and non-constant ratios are dead ends ON
    PURPOSE — only clean, single-factor differences become recordable."""
    n = stats.get("pairs", 0)
    if n < MIN_PAIRS:
        return _verdict("insufficient-overlap", None, None, "low",
                        f"only {n} shared bar(s) (need {MIN_PAIRS}) — "
                        f"load a longer overlap before deciding")
    pf = stats.get("price_ratio_median")
    vf = stats.get("volume_ratio_median")
    psp = stats.get("price_ratio_spread")
    vsp = stats.get("volume_ratio_spread")
    if stats.get("price_agree_frac", 0) >= 0.90:
        if vf is not None and not (VOL_PLAIN_BAND[0] <= vf
                                   <= VOL_PLAIN_BAND[1]):
            tight = vsp is not None and vsp <= 0.5  # volume is noisy
            return _verdict(
                "volume-scale", round(vf, 6), "volume",
                "high" if tight else "low",
                f"prices agree bar-for-bar but volume runs {vf:.2f}x — "
                f"different volume basis (NBBO-filtered vs consolidated,"
                f" or lots vs shares)")
        return _verdict("consistent", 1.0, None, "high",
                        "prices agree within tolerance and volume is in "
                        "the plain band — nothing to record")
    if psp is not None and psp <= TIGHT_SPREAD and pf:
        frac = _simple_fraction(pf)
        if frac:
            p, q = frac
            return _verdict(
                "split", round(pf, 6), "price", "high",
                f"every shared bar differs by the SAME factor {pf:.6g} "
                f"~= {p}:{q} — looks like a {q}-for-{p} corporate "
                f"action (split/reverse split)")
        return _verdict(
            "price-basis", round(pf, 6), "price", "medium",
            f"every shared bar differs by the same factor {pf:.6g}, not "
            f"a simple fraction — different adjustment basis (dividend-"
            f"adjusted vs raw, or a vendor adjustment)")
    return _verdict(
        "incoherent", None, None, "low",
        f"price ratios are NOT a single factor (spread "
        f"{'?' if psp is None else round(psp, 4)}) — mixed or garbled "
        f"sources; do NOT record an action, keep the series gated")


def _verdict(kind, factor, applies, confidence, explanation):
    return {"kind": kind, "factor": factor, "applies": applies,
            "confidence": confidence, "explanation": explanation}


# --- proposal + manifest recording --------------------------------------------------

def propose_action(ticker, interval, verdict, boundary_date,
                   run_id=None):
    """-> (action dict | None, human explanation). boundary_date is the
    FIRST session on the NEW basis (the join-gate date, or the day
    after the overlap for whole-series differences)."""
    why = f"{ticker} {interval}: {verdict['explanation']}"
    if verdict["kind"] not in ACTION_KINDS:
        return None, why
    d = boundary_date.date() if isinstance(boundary_date, datetime) \
        else boundary_date
    action = {"date": d.isoformat() if isinstance(d, date) else str(d),
              "kind": verdict["kind"],
              "factor": float(verdict["factor"]),
              "applies": verdict["applies"] or "price",
              "source": "measured", "evidence": verdict["explanation"],
              "run": run_id}
    return action, why + " — approve to record (no data is rewritten)"


def validate_action(action):
    """ss.StorageError unless the action dict is schema-clean."""
    if not isinstance(action, dict):
        raise ss.StorageError("action must be a dict")
    try:
        date.fromisoformat(str(action.get("date")))
    except ValueError:
        raise ss.StorageError(f"action date {action.get('date')!r} is "
                              f"not YYYY-MM-DD")
    if action.get("kind") not in ACTION_KINDS:
        raise ss.StorageError(f"action kind {action.get('kind')!r} not "
                              f"one of {ACTION_KINDS}")
    if action.get("applies") not in ACTION_APPLIES:
        raise ss.StorageError(f"action applies {action.get('applies')!r}"
                              f" not one of {ACTION_APPLIES}")
    f = action.get("factor")
    if (isinstance(f, bool) or not isinstance(f, (int, float))
            or not math.isfinite(f) or f <= 0):
        raise ss.StorageError(f"action factor {f!r} must be a positive "
                              f"finite number")
    if action.get("source") not in ("measured", "user"):
        raise ss.StorageError(f"action source {action.get('source')!r} "
                              f"must be 'measured' or 'user'")


def apply_action(root, ticker, action):
    """Append an APPROVED action to the ticker manifest. Manifest-only:
    no bar bytes are ever touched (decision D1). Identical duplicates
    (ignoring run id) are refused. Returns the sorted actions list."""
    validate_action(action)
    tdir = Path(root) / ticker
    manifest = ss.load_manifest(tdir) or ss.new_manifest(ticker, ticker)
    actions = manifest.setdefault("actions", [])
    probe = {k: v for k, v in action.items() if k != "run"}
    for a in actions:
        if {k: v for k, v in a.items() if k != "run"} == probe:
            raise ss.StorageError(f"identical {action['kind']} action "
                                  f"already recorded for "
                                  f"{action['date']}")
    actions.append(dict(action))
    actions.sort(key=lambda a: (a.get("date", ""), a.get("kind", "")))
    ss.save_manifest(tdir, manifest)
    return list(actions)


def load_actions(root, ticker, kind=None):
    """The ticker's recorded actions, sorted by (date, kind)."""
    man = ss.load_manifest(Path(root) / ticker) or {}
    acts = [a for a in man.get("actions", [])
            if kind is None or a.get("kind") == kind]
    return sorted(acts, key=lambda a: (a.get("date", ""),
                                       a.get("kind", "")))


# --- read-time adjustment (M3) -------------------------------------------------------

def _iso(d):
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return str(d)


def adjustment_factor(actions, when, applies=("price", "both")):
    """Cumulative factor expressing a bar dated `when` in the NEWEST
    recorded basis: the product of matching factors strictly AFTER
    `when` (an action converts old->new at its boundary; bars before
    it need the multiplication, bars on/after it are already there)."""
    w = _iso(when)
    f = 1.0
    for a in actions:
        if a.get("applies") in applies and _iso(a.get("date")) > w:
            try:
                f *= float(a.get("factor"))
            except (TypeError, ValueError):
                continue       # malformed hand-edit: ignore, stay raw
    return f


def read_series(root, ticker, interval, start=None, end=None,
                basis="raw"):
    """Concatenated bars for a series across its stored months, in
    stored order. basis="raw" returns the bytes' values untouched;
    basis="current" multiplies price fields (and volume, for volume
    actions) by the cumulative recorded factors so the WHOLE series is
    expressed in the newest basis — ANALYSIS ONLY, nothing on disk
    changes (decisions D2/D3). Returns (bars, notes); adjusted volumes
    may be non-integer floats."""
    if basis not in ("raw", "current"):
        raise ss.StorageError(f"basis {basis!r} must be 'raw' or "
                              f"'current'")
    man = ss.load_manifest(Path(root) / ticker) or {}
    months = sorted(ss.manifest_months(man, interval))
    lo = _iso(start) if start is not None else None
    hi = _iso(end) if end is not None else None
    bars, notes = [], []
    for mk in months:
        if (lo and mk < lo[:7]) or (hi and mk > hi[:7]):
            continue
        try:
            mb, _meta = ss.read_month_file(
                ss.find_month_file(root, ticker, int(mk[:4]), int(mk[5:7]),
                                   interval)
                or ss.month_file_path(root, ticker, int(mk[:4]), int(mk[5:7]),
                                      interval))
        except (ss.StorageError, OSError) as exc:
            notes.append(f"{mk}: unreadable, skipped ({exc})")
            continue
        bars.extend(mb)
    if lo or hi:
        bars = [b for b in bars
                if (not lo or b[0].date().isoformat() >= lo)
                and (not hi or b[0].date().isoformat() <= hi)]
    acts = sorted(man.get("actions", []),
                  key=lambda a: _iso(a.get("date", "")))
    if basis == "raw" or not acts or not bars:
        return bars, notes
    out = []
    for b in bars:
        pf = adjustment_factor(acts, b[0])
        vf = adjustment_factor(acts, b[0], applies=("volume", "both"))
        if pf == 1.0 and vf == 1.0:
            out.append(b)
        else:
            out.append((b[0], b[1] * pf, b[2] * pf, b[3] * pf,
                        b[4] * pf, b[5] * vf if vf != 1.0 else b[5]))
    notes.append(f"basis=current: {len(acts)} recorded action(s) "
                 f"applied at read time — stored data unchanged")
    return out, notes
