"""Pure memory-sizing policy for the TWS demo fleet.

The GUI discovers free RAM and process working sets at dialog-open time and
injects those measurements here.  This module deliberately performs no GUI,
process, filesystem, or platform probing, so its decisions stay deterministic
and portable.
"""
from __future__ import annotations

from statistics import median
from typing import Iterable

GIB = 1024 ** 3

# Conservative fallback used only when no running TWS working-set sample is
# available.  It is a policy constant, not a stored fact about one machine.
DEFAULT_PER_INSTANCE_BYTES = 8 * GIB // 5  # 1.6 GiB

# The verified 1 GiB Java heap cap still needs bounded native/runtime overhead
# in the startup plan. Uncapped starts retain the old 25% reserve.
CAPPED_PER_INSTANCE_BYTES = 5 * GIB // 4  # 1.25 GiB
DEFAULT_RESERVE_FRAC = 0.15
UNCAPPED_RESERVE_FRAC = 0.25

OVER_AUTO_WARNING = (
    "Going beyond the recommended count may cause issues with port start up "
    "(slow instances, popups, failed configuration)."
)


def _nonnegative_int(value) -> int:
    """Return a whole, non-negative byte/count value."""
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _is_tws_name(name) -> bool:
    text = str(name or "").strip().replace("\\", "/").rsplit("/", 1)[-1]
    stem = text[:-4] if text.casefold().endswith(".exe") else text
    return stem.casefold() == "tws"


def tws_working_set_bytes(rows: Iterable[tuple[object, object]]) -> list[int]:
    """Return positive TWS working sets from one injected process snapshot."""
    samples: list[int] = []
    try:
        iterator = iter(rows)
    except TypeError:
        return samples
    for row in iterator:
        try:
            name, working_set = row
        except (TypeError, ValueError):
            continue
        if isinstance(working_set, bool):
            continue
        value = _nonnegative_int(working_set)
        if _is_tws_name(name) and value > 0:
            samples.append(value)
    return samples


def sample_per_instance_bytes(rows: Iterable[tuple[object, object]]) -> int | None:
    """Return the integer median WorkingSet for positive TWS samples.

    ``rows`` contains injected ``(process_name, working_set_bytes)`` pairs.
    Unrelated processes and malformed/non-positive measurements are ignored.
    ``None`` tells the caller to use :data:`DEFAULT_PER_INSTANCE_BYTES`.
    """
    samples = tws_working_set_bytes(rows)
    return int(median(samples)) if samples else None


def estimate_bytes(n, per_instance) -> int:
    """Return the non-negative linear working-set estimate for ``n`` ports."""
    return _nonnegative_int(n) * _nonnegative_int(per_instance)


def effective_free_bytes(measured_free, closable_working_sets) -> int:
    """Add reclaimable positive TWS working sets to measured free RAM."""
    total = _nonnegative_int(measured_free)
    try:
        iterator = iter(closable_working_sets)
    except TypeError:
        return total
    for working_set in iterator:
        if isinstance(working_set, bool):
            continue
        value = _nonnegative_int(working_set)
        if value > 0:
            total += value
    return total


def auto_select(free_bytes, per_instance, cap=8,
                reserve_frac=DEFAULT_RESERVE_FRAC,
                floor=0) -> int:
    """Choose the largest fleet that fits after retaining a RAM reserve.

    ``floor`` is an explicit caller override.  With the default floor of zero,
    this returns zero when one instance cannot fit.  A non-zero floor may
    intentionally exceed the reserve but is still bounded by ``cap``.
    """
    cap_i = _nonnegative_int(cap)
    floor_i = min(cap_i, _nonnegative_int(floor))
    free_i = _nonnegative_int(free_bytes)
    per_i = _nonnegative_int(per_instance)
    try:
        reserve = float(reserve_frac)
    except (TypeError, ValueError, OverflowError):
        reserve = 1.0
    if not 0.0 <= reserve < 1.0:
        usable = 0
    else:
        usable = max(0, int(free_i * (1.0 - reserve)))
    if cap_i == 0:
        return 0
    fitting = cap_i if per_i == 0 else min(cap_i, usable // per_i)
    return min(cap_i, max(floor_i, int(fitting)))


def format_estimate(n, per_instance, free_bytes, *, effective=False) -> str:
    """Render the single authoritative human-readable memory estimate."""
    count = _nonnegative_int(n)
    estimate_gib = estimate_bytes(count, per_instance) / GIB
    if free_bytes is None:
        return (f"{count} port(s) ≈ {estimate_gib:.1f} GiB; "
                "free RAM unavailable")
    free_gib = _nonnegative_int(free_bytes) / GIB
    free_label = "effective free" if effective else "free"
    return (f"{count} port(s) ≈ {estimate_gib:.1f} GiB of "
            f"~{free_gib:.1f} GiB {free_label}")
