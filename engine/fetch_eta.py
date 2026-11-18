"""Work-weighted live ETA projection for batch IBKR fetches (GUI-free).

WHY: the previous live readout projected `remaining_series_count x recent
seconds-per-series`. An Add Stocks queue mixes second-scale series (extended
-pre/-post companions served from the combined fetch cache, already-current
top-ups) with hour-scale deep 1m builds, so that count-based rate swung the
displayed remaining time by orders of magnitude (observed 9 min <-> 16 h inside
one 4-hour run, 2026-07-17) depending on WHICH series happened to finish inside
the trailing window.

HOW: every series carries the pre-run model's per-series seconds (its WORK,
from `_estimate_worktime`'s `per_series_seconds`). Completions are detected per
port lane: a new `[i/N] TICKER interval  (port P)` start marker on a lane
retires that lane's previous series. The projector measures
kappa = realized seconds per modeled second over the RETIRED work (a recent
window blended 50/50 with the whole run, and the window only counts once it
spans enough modeled work), then projects

    remaining = kappa x modeled-seconds-still-unretired

blended with the pre-run countdown during warm-up. A burst of tiny series now
moves the accounting by ~zero modeled work (no collapse), and one deep build
inside the window cannot multiply the whole queue's remainder (no explosion).

Deterministic and clock-free: the caller supplies the pause-adjusted elapsed
seconds for every call. In-flight partial progress is deliberately ignored
(a running series' full cost stays in `remaining` until it retires), so near
the end of a deep build the readout errs HIGH by up to that one build's
modeled cost and then steps down - a bounded, one-directional bias instead of
an unbounded whipsaw.
"""

from __future__ import annotations


class EtaProjector:
    """Seconds-left projection for one batch run.

    Feed `on_marker(key, eff, port=...)` for EVERY `[i/N]` start marker (even
    ones the display drops as out-of-order - lanes deliver them interleaved),
    then read `projection(eff)` whenever a fresh number is wanted. `key` is
    "TICKER interval" exactly as the marker prints it; `eff` is the caller's
    pause-adjusted elapsed seconds.
    """

    WINDOW_S = 900.0          # recent-pace window (matches the old readout)
    WARMUP_START = 5          # completions before measured pace gets weight
    WARMUP_FULL = 20          # completions at full measured trust
    WORK_RAMP_LO = 0.02       # ...and the retired-work fraction must also
    WORK_RAMP_HI = 0.10       # ramp 2% -> 10% before full measured trust
    MIN_KAPPA_WORK_S = 60.0   # a recent window must span this much modeled
    #                           work, else only the whole-run pace is used - a
    #                           burst of near-zero-work series cannot set the
    #                           pace on its own

    def __init__(self, pred_map=None, est_total_s=None):
        self._pred = {}
        for k, v in (pred_map or {}).items():
            try:
                sec = float(v)
            except (TypeError, ValueError):
                continue
            if sec >= 0:
                self._pred[str(k)] = sec
        self._pred_total = float(sum(self._pred.values()))
        try:
            est = float(est_total_s)
        except (TypeError, ValueError):
            est = 0.0
        self._est_total = est if est > 0 else None
        self._consumed = set()        # keys whose START charged their work
        self._completed = set()       # keys retired at full modeled cost
        self._completed_pred = 0.0    # modeled seconds actually retired
        self._completions = 0
        self._in_flight = {}          # lane(port|None) -> (key|None, charge)
        self._unmatched = 0           # markers whose key had no model entry
        self._samples = []            # (completed_pred, eff) per completion

    # ---- accounting -------------------------------------------------------

    def _median_pending(self):
        pending = sorted(v for k, v in self._pred.items()
                         if k not in self._consumed)
        if not pending:
            pending = sorted(self._pred.values())
        if not pending:
            return 0.0
        return pending[len(pending) // 2]

    def _retire(self, entry, eff):
        key, charge = entry
        if key is not None and key in self._pred:
            if key not in self._completed:
                self._completed.add(key)
                self._completed_pred += self._pred[key]
            # a requeued duplicate completion never double-counts
        else:
            self._completed_pred += charge
        self._completions += 1
        self._samples.append((self._completed_pred, float(eff)))
        cutoff = float(eff) - self.WINDOW_S
        while (len(self._samples) > 2 and self._samples[0][1] < cutoff
               and (self._completed_pred - self._samples[1][0])
               >= self.MIN_KAPPA_WORK_S):
            self._samples.pop(0)

    def on_marker(self, key, eff, port=None):
        """A series START on lane `port` - retires that lane's previous one."""
        prev = self._in_flight.get(port)
        if prev is not None:
            self._retire(prev, eff)
        charge = 0.0
        if key is not None and key in self._pred:
            if key not in self._consumed:
                self._consumed.add(key)
            # a requeued re-start re-runs work the model already priced once;
            # kappa absorbs the extra realized time instead of double-charging
        else:
            charge = self._median_pending()
            self._unmatched += 1
        self._in_flight[port] = (key, charge)

    # ---- projection -------------------------------------------------------

    def kappa(self, eff):
        """Realized/modeled pace over retired work; >1 means slower than the
        model. None until any work has retired."""
        if self._completed_pred <= 0:
            return None
        eff = max(0.0, float(eff))
        k_glob = eff / self._completed_pred
        if self._samples:
            base_w, base_t = self._samples[0]
            dw = self._completed_pred - base_w
            dt = eff - base_t
            if dw >= self.MIN_KAPPA_WORK_S and dt >= 0:
                return 0.5 * (dt / dw) + 0.5 * k_glob
        return k_glob

    def projection(self, eff):
        """Seconds left, or None when neither an estimate nor a pace exists."""
        eff = max(0.0, float(eff))
        est_rem = (max(0.0, self._est_total - eff)
                   if self._est_total is not None else None)
        if self._pred_total <= 0:
            return est_rem
        remaining = max(0.0, self._pred_total - self._completed_pred)
        k = self.kappa(eff)
        if k is None:
            return est_rem
        measured = k * remaining
        span = float(self.WARMUP_FULL - self.WARMUP_START)
        by_count = (self._completions - self.WARMUP_START) / span
        frac = (self._completed_pred / self._pred_total
                if self._pred_total > 0 else 0.0)
        by_work = ((frac - self.WORK_RAMP_LO)
                   / (self.WORK_RAMP_HI - self.WORK_RAMP_LO))
        w = max(0.0, min(1.0, min(by_count, by_work)))
        if est_rem is None:
            return measured
        return w * measured + (1.0 - w) * est_rem

    def stats(self):
        """Counters for tests and debugging (no GUI reads these)."""
        return {
            "pred_total": self._pred_total,
            "completed_pred": self._completed_pred,
            "completions": self._completions,
            "in_flight": len(self._in_flight),
            "unmatched": self._unmatched,
        }
