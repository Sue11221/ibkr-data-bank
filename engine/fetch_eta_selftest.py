"""Offline selftest for fetch_eta.EtaProjector (no GUI, no network, no bank).

The headline regression: a queue of 40 deep builds (1800 s modeled) mixed with
360 cheap companions (6 s modeled). The OLD count-based projection
(remaining_count x window seconds-per-series) swings by orders of magnitude
depending on which series finished recently; the work-weighted projector must
stay within a bounded factor of ground truth the whole way.
"""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_eta import EtaProjector

PASS = [0]
FAIL = [0]


def check(condition, label):
    if condition:
        PASS[0] += 1
    else:
        FAIL[0] += 1
        print(f"  FAIL: {label}")


def old_formula(samples, i, n, est, elapsed,
                warmup_start=5, warmup_full=20):
    """The retired count-based projection, reproduced for comparison."""
    remaining = max(0, n - i)
    est_rem = max(0.0, est - elapsed) if est else None
    if len(samples) < 2:
        return est_rem
    (oi, ot), (ni, nt) = samples[0], samples[-1]
    if ni <= oi or nt <= ot:
        return est_rem
    sps = (nt - ot) / (ni - oi)
    meas = remaining * sps
    if est_rem is None:
        return meas
    w = max(0.0, min(1.0, (i - warmup_start) / float(warmup_full
                                                     - warmup_start)))
    return w * meas + (1.0 - w) * est_rem


def build_queue():
    """400-series queue: every 10th a deep build, the rest cheap companions."""
    pred = {}
    order = []
    for t in range(40):
        deep = f"DEEP{t:02d} 1m"
        pred[deep] = 1800.0
        order.append(deep)
        for c in range(9):
            key = f"CHEAP{t:02d}_{c} 1m-pre"
            pred[key] = 6.0
            order.append(key)
    return pred, order


def scenario_heterogeneous():
    pred, order = build_queue()
    total_truth = sum(pred.values())
    proj = EtaProjector(pred, est_total_s=total_truth)
    n = len(order)
    eff = 0.0
    old_samples = []
    worst_new = 0.0
    old_ever_bad = False
    new_after_warmup = 0
    for i, key in enumerate(order, start=1):
        proj.on_marker(key, eff, port=None)   # start marker retires previous
        old_samples.append((i, eff))
        cutoff = eff - EtaProjector.WINDOW_S
        while len(old_samples) > 5 and old_samples[0][1] < cutoff:
            old_samples.pop(0)
        truth_left = total_truth - (eff)      # model == reality in this run
        got = proj.projection(eff)
        old = old_formula(old_samples, i, n, total_truth, eff)
        if truth_left > 600 and got is not None:
            ratio = got / truth_left
            if proj.stats()["completions"] >= EtaProjector.WARMUP_FULL:
                new_after_warmup += 1
                worst_new = max(worst_new, ratio, 1.0 / ratio)
            if old is not None and truth_left > 0:
                oratio = max(old / truth_left, truth_left / max(old, 1e-9))
                old_ever_bad = old_ever_bad or oratio > 5.0
        eff += pred[key]                      # reality matches the model
    check(new_after_warmup > 300,
          "heterogeneous: projector produced post-warmup readings throughout")
    check(worst_new <= 1.35,
          f"heterogeneous: work-weighted projection stays within 1.35x of "
          f"truth (worst {worst_new:.2f}x)")
    check(old_ever_bad,
          "heterogeneous: the retired count-based formula demonstrably "
          "swings >5x on the same run (the bug being fixed)")


def scenario_slowdown():
    pred = {f"S{i} 1m": 60.0 for i in range(100)}
    proj = EtaProjector(pred, est_total_s=6000.0)
    eff = 0.0
    ratios = []
    for i, key in enumerate(sorted(pred), start=1):
        proj.on_marker(key, eff, port=None)
        truth_left = (100 - (i - 1)) * 120.0   # reality is 2x the model
        got = proj.projection(eff)
        if proj.stats()["completions"] >= EtaProjector.WARMUP_FULL \
                and truth_left > 600 and got:
            ratios.append(got / truth_left)
        eff += 120.0
    check(ratios and all(0.8 <= r <= 1.25 for r in ratios),
          f"slowdown: kappa tracks a 2x-slower reality "
          f"({min(ratios):.2f}..{max(ratios):.2f} of truth)" if ratios
          else "slowdown: no post-warmup readings")


def scenario_warmup_counts_down():
    pred = {f"W{i} 1m": 100.0 for i in range(50)}
    proj = EtaProjector(pred, est_total_s=5000.0)
    eff = 0.0
    ok = True
    for i, key in enumerate(sorted(pred), start=1):
        proj.on_marker(key, eff, port=None)
        got = proj.projection(eff)
        if proj.stats()["completions"] < EtaProjector.WARMUP_START:
            ok = ok and abs(got - max(0.0, 5000.0 - eff)) < 1e-6
        eff += 100.0
    check(ok, "warmup: below WARMUP_START the readout is the pure estimate "
              "countdown (never jumps)")


def scenario_lanes_and_requeue():
    pred = {"AAA 1m": 300.0, "BBB 1m": 300.0, "CCC 1m": 300.0}
    proj = EtaProjector(pred)
    proj.on_marker("AAA 1m", 0.0, port=2000)
    proj.on_marker("BBB 1m", 0.0, port=3000)
    check(proj.stats()["completions"] == 0,
          "lanes: two lanes in flight, nothing retired yet")
    # port 2000 hard-dies; AAA requeues onto port 3000 later
    proj.on_marker("CCC 1m", 300.0, port=3000)   # BBB retires on lane 3000
    check(proj.stats()["completions"] == 1
          and abs(proj.stats()["completed_pred"] - 300.0) < 1e-9,
          "lanes: a lane's next start retires exactly its previous series")
    proj.on_marker("AAA 1m", 600.0, port=3000)   # CCC retires; AAA re-starts
    proj.on_marker(None, 900.0, port=3000)       # AAA retires (once)
    s = proj.stats()
    check(abs(s["completed_pred"] - 900.0) < 1e-9,
          "requeue: a re-started series never double-charges its work")


def scenario_unknown_keys():
    pred = {f"U{i} 1m": 50.0 for i in range(10)}
    proj = EtaProjector(pred, est_total_s=500.0)
    eff = 0.0
    got = None
    for i in range(10):
        proj.on_marker(f"NOSUCH{i} 5m", eff, port=None)
        got = proj.projection(eff)
        check(got is None or got >= 0, f"unknown keys: projection {i} sane")
        eff += 50.0
    check(proj.stats()["unmatched"] == 10
          and proj.stats()["completed_pred"] > 0,
          "unknown keys: median charging keeps the accounting moving")


def scenario_no_model():
    proj = EtaProjector({}, est_total_s=1000.0)
    proj.on_marker("X 1m", 10.0, port=None)
    proj.on_marker("Y 1m", 20.0, port=None)
    check(abs(proj.projection(100.0) - 900.0) < 1e-6,
          "no model: falls back to the pure estimate countdown")
    bare = EtaProjector(None, est_total_s=None)
    check(bare.projection(50.0) is None,
          "no model, no estimate: projection is honestly None")


def scenario_thin_window_gate():
    # 5 near-zero-work series complete after a long idle stretch: the recent
    # window spans almost no modeled work, so the pace must stay the global one
    # instead of whipsawing on the thin slice.
    pred = {"BIG 1m": 3600.0}
    pred.update({f"T{i} 1m-pre": 2.0 for i in range(5)})
    proj = EtaProjector(pred)
    proj.on_marker("BIG 1m", 0.0, port=None)
    proj.on_marker("T0 1m-pre", 3600.0, port=None)     # BIG retires at model pace
    eff = 3600.0
    for i in range(1, 5):
        eff += 500.0                                   # cheap ones crawl (slow!)
        proj.on_marker(f"T{i} 1m-pre", eff, port=None)
    k = proj.kappa(eff)
    check(k is not None and k < 2.0,
          f"thin window: near-zero-work completions cannot set the pace "
          f"(kappa {k:.2f} stays anchored to the whole run)")


def main():
    scenario_heterogeneous()
    scenario_slowdown()
    scenario_warmup_counts_down()
    scenario_lanes_and_requeue()
    scenario_unknown_keys()
    scenario_no_model()
    scenario_thin_window_gate()
    total = PASS[0] + FAIL[0]
    print(f"fetch_eta_selftest: {PASS[0]}/{total} passed, {FAIL[0]} failed")
    return 1 if FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
