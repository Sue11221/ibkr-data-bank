"""Date-range (month-aligned) split — REFERENCE prototype + PASS/FAIL gate.

This is the X0-style gate for archive/_completed_work_archive/DATE_SPLIT_PLAN.md: it (1) provides a proven
reference `partition_series_by_months` (P1) that Codex's production chunker must
match on the invariants, and (2) proves the safety core (P4) — that committing
DISJOINT month ranges of ONE series concurrently is only safe with a MONTH-LEVEL
manifest merge, not the current interval-section replace. It DEMONSTRATES the
month-loss bug in the existing merge and proves the fix + the tree-heal net (P5).

Run:  python engine/date_split_reference.py   (exit 0 = gate PASS)

Codex: `_save_manifest_safely` (stock_ibkr.py:1501) in DATE-SPLIT mode must do
what `_save(mode="month_merge")` does here — re-read fresh UNDER the per-ticker
lock and UNION the months dict; never `fresh["intervals"][iv] = mine`.
"""
import sys, threading, time, tempfile
from pathlib import Path
from datetime import date, datetime, timedelta

try:                                  # harness hygiene (Row 61): never die on a glyph
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:                     # noqa: BLE001 - older/redirected streams
    pass

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stock_storage as ss


# ---------------------------------------------------------------- P1: chunker
def _ym(d):
    return (d.year, d.month)


def partition_series_by_months(days, k, min_months=6):
    """Split missing trading `days` into <=k CONTIGUOUS, month-aligned, disjoint
    chunks. Each chunk = {"lo","hi","months":[(y,m)...],"days":[...]}.

    INVARIANTS (the T1 gate — the production chunker may balance by day/request
    weight instead of month-count, but MUST still satisfy every one of these):
      * months are a contiguous run of the ascending distinct-month list
      * chunks are pairwise disjoint (no month in two chunks)
      * union(chunk months) == all months; union(chunk days) == all days
      * if >1 chunk, EVERY chunk has >= min_months months (no tiny tail)
      * <2 clean chunks possible  -> ONE chunk (no split); deterministic
    """
    days = sorted(days)
    months = sorted({_ym(d) for d in days})
    n = len(months)
    if n == 0:
        return []
    bym = {}
    for d in days:
        bym.setdefault(_ym(d), []).append(d)

    def _chunk(run):
        return {"lo": run[0], "hi": run[-1], "months": list(run),
                "days": [d for mo in run for d in bym.get(mo, [])]}

    max_k = max(1, n // max(1, int(min_months)))     # keep every chunk >= min
    kk = max(1, min(int(k), max_k))
    if kk <= 1:
        return [_chunk(months)]
    base, extra = divmod(n, kk)                      # even, remainder up front
    out, i = [], 0
    for c in range(kk):
        size = base + (1 if c < extra else 0)
        out.append(_chunk(months[i:i + size]))
        i += size
    return out


# ------------------------------------------------ P4/P5: commit + save modes
def _save(tdir, my_manifest, interval, mode, lock):
    """Persist my in-memory (per-chunk, STALE) manifest by `mode`.

      sequential       : single writer, plain save (the control).
      interval_replace : the CURRENT _save_manifest_safely behavior — replaces
                         the whole interval section => LOSES sibling months.
      month_merge      : the FIX — re-read fresh UNDER lock, UNION months.
    """
    tdir = Path(tdir)
    if mode == "sequential":
        ss.save_manifest(tdir, my_manifest)
        return
    with lock:                                       # per-ticker lock (shared)
        fresh = ss.load_manifest(tdir)
        if not fresh:
            ss.save_manifest(tdir, my_manifest)      # first writer creates it
            return
        if mode == "interval_replace":
            fresh.setdefault("intervals", {})
            fresh["intervals"][interval] = my_manifest["intervals"][interval]
        elif mode == "month_merge":
            ss.manifest_months(fresh, interval).update(
                ss.manifest_months(my_manifest, interval))
        else:
            raise ValueError(mode)
        ss.save_manifest(tdir, fresh)


def commit_months(root, ticker, interval, month_bars, mode, lock,
                  checkpoint=4, stagger=0.0):
    """Mirror _commit_month's record+checkpointed-save for a set of months
    (dict {(y,m): [bars]}) using a PRIVATE in-memory manifest (a per-chunk
    worker). Writes each month file once (disjoint across chunks by design)."""
    tdir = Path(root) / ticker
    tdir.mkdir(parents=True, exist_ok=True)
    manifest = ss.new_manifest(ticker, ticker)       # stale per-chunk snapshot
    since = 0
    for (y, m) in sorted(month_bars):
        path = ss.month_file_path(root, ticker, y, m, interval)
        path.parent.mkdir(parents=True, exist_ok=True)
        stats = ss.write_month_file(path, month_bars[(y, m)])
        ss.manifest_months(manifest, interval)[ss.month_key(y, m)] = dict(
            stats, status="present")
        since += 1
        if since >= checkpoint:
            if stagger:
                time.sleep(stagger)                  # widen the race window
            _save(tdir, manifest, interval, mode, lock)
            since = 0
    _save(tdir, manifest, interval, mode, lock)      # final save


def heal_from_tree(root, ticker, interval, candidate_months):
    """P5 net: rebuild the month index from the DISK TREE (source of truth).
    Returns the set of month_keys whose file is actually present."""
    present = set()
    for (y, m) in candidate_months:
        if ss.find_month_file(root, ticker, y, m, interval) is not None:
            present.add(ss.month_key(y, m))
    return present


# --------------------------------------------------------------- test fixtures
def _weekdays(start, end):
    d, out = start, []
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _first_weekdays(y, m, want=2):
    out, d = [], date(y, m, 1)
    while len(out) < want:
        if d.weekday() < 5:
            out.append(d.day)
        d += timedelta(days=1)
    return out


def _month_bars(y, m):
    """A few distinct RTH bars on 2 weekdays of month (y,m)."""
    base = 100.0 + y % 100 + m
    bars = []
    for dd in _first_weekdays(y, m):
        for hh, mm in ((9, 30), (12, 0), (15, 59)):
            px = base + hh / 100.0 + mm / 10000.0
            bars.append((datetime(y, m, dd, hh, mm),
                         px, px + 0.5, px - 0.5, px + 0.1, 1000 + hh))
    return sorted(bars)


# --------------------------------------------------------------------- T1 gate
def run_t1():
    fails = []

    def check(days, k, min_months, expect_split):
        chunks = partition_series_by_months(days, k, min_months)
        all_m = sorted({_ym(d) for d in days})
        # contiguity + disjoint + coverage
        flat = [mo for c in chunks for mo in c["months"]]
        if flat != all_m:
            fails.append(f"coverage/order k={k}: {flat} != {all_m}")
        if len(flat) != len(set(flat)):
            fails.append(f"OVERLAP k={k}: a month is in two chunks")
        du = sorted(d for c in chunks for d in c["days"])
        if du != sorted(days):
            fails.append(f"day coverage k={k}")
        for c in chunks:                              # contiguous run
            idx = [all_m.index(mo) for mo in c["months"]]
            if idx != list(range(idx[0], idx[0] + len(idx))):
                fails.append(f"NON-CONTIGUOUS chunk k={k}: {c['months']}")
        if len(chunks) > 1 and any(len(c["months"]) < min_months for c in chunks):
            fails.append(f"tiny chunk < min_months={min_months} k={k}")
        if expect_split and len(chunks) < 2:
            fails.append(f"expected split, got {len(chunks)} k={k}")
        if not expect_split and len(chunks) != 1:
            fails.append(f"expected NO split, got {len(chunks)} k={k}")
        if partition_series_by_months(days, k, min_months) != chunks:
            fails.append(f"NON-DETERMINISTIC k={k}")

    d36 = _weekdays(date(2011, 1, 1), date(2013, 12, 31))   # 36 months
    check(d36, 5, 6, True)      # 36 mo / 5 -> 5 chunks >=6
    check(d36, 3, 6, True)      # even 12/12/12
    check(d36, 100, 6, True)    # clamp to floor(36/6)=6 chunks
    d8 = _weekdays(date(2020, 1, 1), date(2020, 8, 31))     # 8 months
    check(d8, 5, 6, False)      # 8/6 -> only 1 clean chunk -> no split
    d12 = _weekdays(date(2020, 1, 1), date(2020, 12, 31))
    check(d12, 2, 6, True)      # exactly 6+6
    return fails


# ------------------------------------------------------------ T2/T4/P5 gate
def _run_split(root, ticker, interval, full, mode, nchunks, trials=1, stagger=0.0):
    """Fetch `full` (dict {(y,m):bars}) split into nchunks via threads; return
    the FINAL manifest's tracked month_keys for `interval`."""
    months = sorted(full)
    days = [date(y, m, 15) for (y, m) in months]
    chunks = partition_series_by_months(days, nchunks, min_months=1)
    lock = threading.Lock()
    last = None
    for _ in range(max(1, trials)):
        tdir = Path(root) / ticker
        if tdir.exists():
            import shutil
            shutil.rmtree(tdir)
        threads = []
        for c in chunks:
            mb = {mo: full[mo] for mo in c["months"]}
            threads.append(threading.Thread(
                target=commit_months,
                args=(root, ticker, interval, mb, mode, lock),
                kwargs={"checkpoint": 2, "stagger": stagger}))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        man = ss.load_manifest(tdir) or {}
        last = set((man.get("intervals", {}).get(interval, {})
                    .get("months", {})).keys())
    return last, chunks


def run_t4():
    fails = []
    interval = "1m"
    full = {(y, m): _month_bars(y, m)
            for y in (2011, 2012, 2013) for m in range(1, 13)}   # 36 months
    want = {ss.month_key(y, m) for (y, m) in full}

    with tempfile.TemporaryDirectory() as root:
        # control: sequential single writer
        seq, _ = _run_split(root, "SEQ", interval, full, "sequential", 1)
        if seq != want:
            fails.append(f"sequential control lost months: {want - seq}")

        # BUG demo: interval_replace MUST lose months (proves the hazard is real)
        bug, _ = _run_split(root, "BUG", interval, full, "interval_replace", 5)
        if bug == want:
            fails.append("interval_replace did NOT lose months — bug demo "
                         "invalid (the gate can't prove the fix matters)")
        bug_lost = want - bug

        # FIX: month_merge under lock, hammered over trials + staggered saves
        fix, chunks = _run_split(root, "FIX", interval, full, "month_merge", 5,
                                 trials=8, stagger=0.001)
        if fix != want:
            fails.append(f"month_merge LOST months: {want - fix}")

        # T4 equivalence: fixed month index == sequential control
        if fix != seq:
            fails.append(f"split != sequential (T4): {fix ^ seq}")

        # bars intact + strict-readable for every month (files never corrupted)
        for (y, m) in full:
            p = ss.find_month_file(root, "FIX", y, m, interval)
            if p is None:
                fails.append(f"missing file {y}-{m}")
                continue
            got, _st = ss.read_month_file(p)
            if sorted(got) != sorted(full[(y, m)]):
                fails.append(f"bars differ {y}-{m}")

        # P5 heal net: even the BUG tree has every month file on disk
        healed = heal_from_tree(root, "BUG", interval, list(full))
        if healed != want:
            fails.append(f"tree-heal net failed: {want - healed}")

    return fails, len(bug_lost), len(chunks)


if __name__ == "__main__":
    t1 = run_t1()
    print("T1 chunker invariants:", "PASS" if not t1 else "FAIL")
    for f in t1:
        print("   -", f)
    t4, n_bug_lost, nchunks = run_t4()
    print(f"T4 concurrency/equivalence/heal (split into {nchunks} chunks):",
          "PASS" if not t4 else "FAIL")
    print(f"   demo: interval_replace lost {n_bug_lost} months; "
          f"month_merge lost 0 (over 8 trials)")
    for f in t4:
        print("   -", f)
    ok = not t1 and not t4
    print("\nGATE:", "PASS — reference proven; Codex must reproduce"
          if ok else "FAIL")
    sys.exit(0 if ok else 1)
