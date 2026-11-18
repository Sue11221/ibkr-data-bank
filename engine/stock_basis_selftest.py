"""Self-tests for stock_basis.py (Tier 3 M1: basis verification).

Standalone, no framework, no network: python engine/stock_basis_selftest.py
"""

import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss   # noqa: E402
import stock_basis as sb     # noqa: E402

FAILS = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


def mk_bars(n, base=100.0, day=15, vol=1000):
    out = []
    t = datetime(2024, 1, day, 9, 30)
    for i in range(n):
        p = base + i * 0.01
        out.append((t, p, p + 0.05, p - 0.05, p + 0.01, vol + i))
        t += timedelta(minutes=1)
    return out


def scaled(bars, price=1.0, volume=1.0):
    return [(t, o * price, h * price, lo * price, c * price,
             max(1, int(v * volume))) for t, o, h, lo, c, v in bars]


base = mk_bars(390)

print("=== [1] measure + classify =========================================")
v = sb.classify(sb.measure_overlap(base, list(base)))
check("identical overlap -> consistent", v["kind"] == "consistent", str(v))

v = sb.classify(sb.measure_overlap(base, scaled(base, volume=6.0)))
check("volume 6x, prices equal -> volume-scale, high confidence",
      v["kind"] == "volume-scale" and v["confidence"] == "high"
      and abs(v["factor"] - 6.0) < 0.05 and v["applies"] == "volume",
      str(v))

v = sb.classify(sb.measure_overlap(base, scaled(base, price=10.0)))
check("price 10x -> split 10:1 (reverse split wording)",
      v["kind"] == "split" and abs(v["factor"] - 10.0) < 1e-6
      and "1-for-10" in v["explanation"], str(v))

v = sb.classify(sb.measure_overlap(base, scaled(base, price=0.25)))
check("price 0.25x -> split 1:4 (4-for-1 wording)",
      v["kind"] == "split" and abs(v["factor"] - 0.25) < 1e-6
      and "4-for-1" in v["explanation"], str(v))

v = sb.classify(sb.measure_overlap(base, scaled(base, price=0.9626)))
check("uniform non-simple factor -> price-basis, medium",
      v["kind"] == "price-basis" and v["confidence"] == "medium"
      and abs(v["factor"] - 0.9626) < 1e-4, str(v))

noisy = [(t, o * (0.9 if i % 2 else 1.1), h, lo,
          c * (0.9 if i % 2 else 1.1), vol)
         for i, (t, o, h, lo, c, vol) in enumerate(base)]
v = sb.classify(sb.measure_overlap(base, noisy))
check("non-constant ratios -> incoherent (refuses to guess)",
      v["kind"] == "incoherent" and v["factor"] is None, str(v))

v = sb.classify(sb.measure_overlap(base[:10], base[:10]))
check("thin overlap -> insufficient, never a verdict",
      v["kind"] == "insufficient-overlap", str(v))

check("simple fractions: 2.0, 0.5, 1.5 found; 0.9626 not",
      sb._simple_fraction(2.0) == (2, 1)
      and sb._simple_fraction(0.5) == (1, 2)
      and sb._simple_fraction(1.5) == (3, 2)
      and sb._simple_fraction(0.9626) is None)

print("=== [2] propose ====================================================")
v = sb.classify(sb.measure_overlap(base, scaled(base, price=10.0)))
action, why = sb.propose_action("KO", "1m", v, datetime(2024, 3, 4),
                                run_id="ver-test")
check("split proposal carries schema fields",
      action and action["kind"] == "split" and action["date"]
      == "2024-03-04" and action["applies"] == "price"
      and action["source"] == "measured" and "approve" in why, str(action))
v = sb.classify(sb.measure_overlap(base, list(base)))
action, why = sb.propose_action("KO", "1m", v, datetime(2024, 3, 4))
check("consistent -> no action proposed, explanation returned",
      action is None and "nothing to record" in why, why)

print("=== [3] apply to manifest ==========================================")
root = Path(tempfile.mkdtemp(prefix="basis_st_")) / ss.STORAGE_DIR_NAME
(root / "KO").mkdir(parents=True)
good = {"date": "2026-06-01", "kind": "volume-scale", "factor": 5.9,
        "applies": "volume", "source": "measured",
        "evidence": "KO smoke finding", "run": "ver-x"}
acts = sb.apply_action(root, "KO", good)
check("apply: recorded and reloadable",
      len(acts) == 1 and sb.load_actions(root, "KO")[0]["factor"] == 5.9)
try:
    sb.apply_action(root, "KO", dict(good, run="ver-y"))
    check("apply: identical duplicate refused (run id ignored)", False)
except ss.StorageError:
    check("apply: identical duplicate refused (run id ignored)", True)
earlier = {"date": "2020-08-31", "kind": "split", "factor": 0.25,
           "applies": "price", "source": "user",
           "evidence": "AAPL 4-for-1", "run": None}
acts = sb.apply_action(root, "KO", earlier)
check("apply: second action accepted, sorted by date",
      [a["date"] for a in acts] == ["2020-08-31", "2026-06-01"])
check("load_actions: kind filter",
      len(sb.load_actions(root, "KO", kind="split")) == 1)
for bad, label in (
        (dict(good, kind="merger"), "unknown kind"),
        (dict(good, date="06/01/2026"), "bad date"),
        (dict(good, factor=-2), "negative factor"),
        (dict(good, factor=float("inf")), "non-finite factor"),
        (dict(good, applies="everything"), "bad applies"),
        (dict(good, source="guessed"), "bad source")):
    try:
        sb.apply_action(root, "KO", bad)
        check(f"validation rejects {label}", False)
    except ss.StorageError:
        check(f"validation rejects {label}", True)
man = ss.load_manifest(root / "KO")
check("manifest survives strict reload with actions present",
      man and len(man.get("actions", [])) == 2)

print("=== [4] read-time adjustment (M3) ==================================")
root4 = Path(tempfile.mkdtemp(prefix="basis_st_")) / ss.STORAGE_DIR_NAME
(root4 / "KO").mkdir(parents=True)
man = ss.new_manifest("KO", "KO")
jan = mk_bars(60, base=100.0, vol=1000)
mar = [(t.replace(month=3), o, h, lo_, c, 6000 + i) for i, (t, o, h, lo_, c, v)
       in enumerate(mk_bars(60, base=25.0))]
for mk_, bars_ in (("2024-01", jan), ("2024-03", mar)):
    y, m = int(mk_[:4]), int(mk_[5:])
    stats = ss.write_month_file(
        ss.month_file_path(root4, "KO", y, m, "1m"), bars_)
    ss.manifest_months(man, "1m")[mk_] = dict(stats, status="present")
ss.manifest_months(man, "1m")["2024-02"] = {"status": "present"}  # no file
ss.save_manifest(root4 / "KO", man)
sb.apply_action(root4, "KO", {"date": "2024-03-01", "kind": "split",
                              "factor": 0.25, "applies": "price",
                              "source": "user", "evidence": "4-for-1",
                              "run": None})
sb.apply_action(root4, "KO", {"date": "2024-03-01",
                              "kind": "volume-scale", "factor": 6.0,
                              "applies": "volume", "source": "user",
                              "evidence": "feed change", "run": None})

raw, notes = sb.read_series(root4, "KO", "1m")
check("raw read: stored values untouched, both months, missing noted",
      len(raw) == 120 and raw[0][1] == 100.0 and raw[0][5] == 1000
      and any("2024-02" in n for n in notes), f"{len(raw)} {notes}")

adj, notes = sb.read_series(root4, "KO", "1m", basis="current")
check("current basis: pre-boundary prices x0.25, volume x6",
      abs(adj[0][1] - 25.0) < 1e-9 and abs(adj[0][5] - 6000.0) < 1e-9
      and any("applied at read time" in n for n in notes),
      str(adj[0]))
check("current basis: post-boundary bars unchanged",
      adj[60][1] == 25.0 and adj[60][5] == 6000, str(adj[60]))
check("current basis: series is continuous across the boundary",
      abs(adj[59][4] * 1.0 - adj[60][1]) / adj[60][1] < 0.03,
      f"{adj[59][4]} vs {adj[60][1]}")

sub, _ = sb.read_series(root4, "KO", "1m", start="2024-03-01")
check("start filter trims to the post-boundary month", len(sub) == 60)

check("adjustment_factor: price vs volume scoping",
      sb.adjustment_factor(sb.load_actions(root4, "KO"),
                           "2024-01-15") == 0.25
      and sb.adjustment_factor(sb.load_actions(root4, "KO"),
                               "2024-01-15",
                               applies=("volume", "both")) == 6.0
      and sb.adjustment_factor(sb.load_actions(root4, "KO"),
                               "2024-03-15") == 1.0)
try:
    sb.read_series(root4, "KO", "1m", basis="adjusted")
    check("bad basis argument raises", False)
except ss.StorageError:
    check("bad basis argument raises", True)

print()
print(f"{N[0]} checks, {len(FAILS)} failed")
if FAILS:
    for f_ in FAILS:
        print(f"  FAILED: {f_}")
    sys.exit(1)
print("ALL PASS")
