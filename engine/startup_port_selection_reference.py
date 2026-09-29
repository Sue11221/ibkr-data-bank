"""Row 74 M0 reference harness: fleet-sizing policy for Start up multi-port.

Claude-owned acceptance gate (STARTUP_PORT_SELECTION_PLAN.md). Offline only:
pure-function contract checks over injected measurements — no GUI, no process
list, no real memory API is ever touched. Exit 0 = M1 policy module satisfies
the contract. Exit 3 = baseline pinned (feature absent; today's rigid 8-port
behavior verified still in place). Exit 1 = failure in either mode.

Codex must not edit this file.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GIB = 1024 ** 3

CHECKS = []


def check(name, ok, detail=""):
    CHECKS.append((name, bool(ok)))
    tag = "PASS" if ok else "FAIL"
    line = f"[{tag}] {name}"
    if detail and not ok:
        line += f" :: {detail}"
    print(line)


def finish(feature):
    failed = [n for n, ok in CHECKS if not ok]
    print(f"{len(CHECKS)} checks, {len(failed)} failed")
    if failed:
        return 1
    if not feature:
        print("FEATURE ABSENT: rigid 8-port baseline pinned; M1 not implemented")
        return 3
    print("GATE PASS")
    return 0


def main():
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "engine"))
    try:
        import fleet_sizing as fs
    except ImportError:
        # --- Baseline: pin today's rigid behavior so drift is visible -----
        src = (ROOT / "display_data.py").read_text(encoding="utf-8",
                                                   errors="replace")
        check("B1 baseline still hardcodes the full 8-port fleet",
              "always the full 8-port fleet" in src)
        check("B2 no fleet_sizing module exists yet",
              not (ROOT / "engine" / "fleet_sizing.py").is_file())
        return finish(feature=False)

    # --- F1-F4: sampling ---------------------------------------------------
    rows = [("tws", 2 * GIB), ("python", 9 * GIB), ("tws", 1 * GIB),
            ("tws", 4 * GIB), ("chrome", 3 * GIB)]
    check("F1 sample = median WorkingSet of tws rows only",
          fs.sample_per_instance_bytes(rows) == 2 * GIB)
    check("F2 no tws rows -> None (fallback is the caller's decision)",
          fs.sample_per_instance_bytes([("python", GIB)]) is None)
    check("F3 documented fallback constant is sane (1-4 GiB)",
          GIB <= fs.DEFAULT_PER_INSTANCE_BYTES <= 4 * GIB)
    check("F4 even count medians deterministically (int, within the pair)",
          fs.sample_per_instance_bytes([("tws", GIB), ("tws", 3 * GIB)])
          in (GIB, 2 * GIB, 3 * GIB))

    # --- F5-F7: estimate ---------------------------------------------------
    check("F5 estimate is linear in n", fs.estimate_bytes(3, GIB) == 3 * GIB)
    check("F6 estimate of zero ports is zero", fs.estimate_bytes(0, GIB) == 0)
    check("F7 estimate never negative",
          fs.estimate_bytes(2, 0) == 0 and fs.estimate_bytes(0, 0) == 0)

    # --- F8-F13: auto_select ----------------------------------------------
    # 16 GiB free, 25% reserve -> 12 GiB usable -> 6 instances at 2 GiB.
    check("F8 auto_select honors the reserve fraction",
          fs.auto_select(16 * GIB, 2 * GIB) == 6)
    check("F9 auto_select caps at the fleet size",
          fs.auto_select(64 * GIB, 2 * GIB, cap=8) == 8)
    check("F10 auto_select returns 0 when one instance cannot fit",
          fs.auto_select(1 * GIB, 2 * GIB) == 0)
    check("F11 auto_select is monotonic in free memory",
          all(fs.auto_select(g * GIB, 2 * GIB)
              <= fs.auto_select((g + 1) * GIB, 2 * GIB) for g in range(0, 24)))
    check("F12 zero/negative free memory -> 0",
          fs.auto_select(0, 2 * GIB) == 0 and fs.auto_select(-GIB, 2 * GIB) == 0)
    check("F13 custom cap respected",
          fs.auto_select(64 * GIB, 2 * GIB, cap=3) == 3)

    # --- F14-F15: the one authoritative label ------------------------------
    label = fs.format_estimate(6, 2 * GIB, 16 * GIB)
    check("F14 label carries count, estimate and free in GiB",
          "6" in label and "12.0" in label and "16.0" in label, label)
    check("F15 label is a single line", "\n" not in label)

    # --- F16-F19: A-package — memory cap + tuned AUTO + over-AUTO warning --
    # (user 2026-07-29: "make A and increase the recommendation of ports …
    # 5 can work … add a warning say if go beyond recommended may have
    # issues with port start up"). Guarded: pending until fleet_sizing
    # ships the cap-aware constants; then mandatory.
    if hasattr(fs, "CAPPED_PER_INSTANCE_BYTES"):
        check("F16 capped planning basis is sane (1-2 GiB)",
              GIB <= fs.CAPPED_PER_INSTANCE_BYTES <= 2 * GIB)
        check("F17 one authoritative over-AUTO warning names port start-up "
              "risk, single line",
              "port start" in fs.OVER_AUTO_WARNING.lower()
              and "recommend" in fs.OVER_AUTO_WARNING.lower()
              and "\n" not in fs.OVER_AUTO_WARNING)
        check("F18 default reserve loosened with the cap (0.10-0.20) and "
              "exposed as a constant",
              0.10 <= fs.DEFAULT_RESERVE_FRAC <= 0.20)
        check("F19 user calibration point: ~8 GiB free at the capped basis "
              "recommends at least 5",
              fs.auto_select(8 * GIB, fs.CAPPED_PER_INSTANCE_BYTES) >= 5)
        check("F20 effective free counts RAM reclaimed from instances the "
              "launch will close (pure, non-negative)",
              fs.effective_free_bytes(3 * GIB, [GIB, GIB // 2]) == 4 * GIB + GIB // 2
              and fs.effective_free_bytes(-GIB, []) == 0
              and fs.effective_free_bytes(GIB, [-5, "x"]) == GIB)
        check("F21 the user scenario end-to-end: 2.7 GiB measured + ~5 GiB "
              "reclaimable at the capped basis recommends exactly 5",
              fs.auto_select(
                  fs.effective_free_bytes(
                      int(2.7 * GIB), [640 * 1024 * 1024] * 8),
                  fs.CAPPED_PER_INSTANCE_BYTES) == 5)
        # (user 2026-07-29: "I want 5 ports if there is nothing else that
        # consumes too much memory") — the conditional must be structural:
        # an external ~2.7 GiB consumer shrinks effective free to ~5 GiB
        # and AUTO must back off below 5 on its own, never pin to 5.
        check("F22 an external memory consumer drops AUTO below 5 "
              "(the nothing-else-consuming condition is structural)",
              1 <= fs.auto_select(
                  5 * GIB, fs.CAPPED_PER_INSTANCE_BYTES) <= 4)
    else:
        print("[NOTE] A-package constants absent — F16-F22 pending "
              "(pre-cap tree)")

    return finish(feature=True)


if __name__ == "__main__":
    raise SystemExit(main())
