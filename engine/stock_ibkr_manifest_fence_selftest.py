"""Offline regressions for stock_ibkr manifest publication fencing.

Every file and lock belongs to a TemporaryDirectory.  This imports the IBKR
engine only to exercise pure storage helpers; it never creates an adapter,
socket, listener, GUI, or production-bank path.

Run: python -B engine/stock_ibkr_manifest_fence_selftest.py
"""

from __future__ import annotations

import copy
import sys
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))

import stock_ibkr as ibkr  # noqa: E402
import stock_storage as storage  # noqa: E402


CHECKS = 0
FAILURES: list[str] = []


def check(name, condition, detail=""):
    global CHECKS
    CHECKS += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def fresh_root(base, name):
    root = Path(base) / name / storage.STORAGE_DIR_NAME
    root.mkdir(parents=True)
    return root


def bar(year, month, day, minute, value):
    stamp = datetime(year, month, day, 9 + minute // 60,
                     30 + minute % 60)
    number = float(value)
    return (stamp, number, number, number, number, 0)


def write_month(root, ticker, interval, year, month, values):
    rows = [bar(year, month, day, offset, value)
            for day, offset, value in values]
    path = storage.month_file_path(
        root, ticker, year, month, interval)
    stats = storage.write_month_file(path, rows)
    return path, stats


def month_entry(stats, *, ledger=None, source="fixture"):
    entry = dict(stats, status="present", source=source)
    if ledger is not None:
        entry["value_corrections"] = copy.deepcopy(ledger)
    return entry


def manifest_for(ticker, interval, key, entry):
    manifest = storage.new_manifest(ticker, ticker)
    storage.manifest_months(manifest, interval)[key] = copy.deepcopy(entry)
    return manifest


def save_stale(tdir, manifest, interval, *, date_split=False, lock=None,
               repin_intent=None):
    result = {"notes": []}
    ibkr._save_manifest_safely(
        tdir, manifest, result, interval, lock or threading.Lock(),
        date_split=date_split, repin_intent=repin_intent)
    return result


LEDGER = [{"version": 1, "day": "2025-01-02", "proof": "fixture"}]


with tempfile.TemporaryDirectory(prefix="ibkr-manifest-fence-") as tmp:
    base = Path(tmp)

    root = fresh_root(base, "same-sha")
    tdir = root / "AAA"
    _, stats = write_month(
        root, "AAA", "1m-iv", 2025, 1, [(2, 0, 0.42)])
    storage.save_manifest(
        tdir, manifest_for(
            "AAA", "1m-iv", "2025-01",
            month_entry(stats, ledger=LEDGER, source="fresh")))
    stale = storage.load_manifest(tdir)
    stale_entry = storage.manifest_months(stale, "1m-iv")["2025-01"]
    stale_entry.pop("value_corrections")
    stale_entry["source"] = "stale-worker"
    result = save_stale(tdir, stale, "1m-iv")
    saved = storage.load_manifest(tdir)
    saved_entry = storage.manifest_months(saved, "1m-iv")["2025-01"]
    check("same-SHA stale save preserves published correction ledger",
          saved_entry.get("value_corrections") == LEDGER, str(saved_entry))
    check("same-SHA merge publishes without a failure note",
          result["notes"] == [], str(result))

    root = fresh_root(base, "same-sha-active-drift")
    tdir = root / "DRIFT"
    _, stats = write_month(
        root, "DRIFT", "1m-iv", 2025, 1, [(2, 0, 0.45)])
    storage.save_manifest(
        tdir, manifest_for(
            "DRIFT", "1m-iv", "2025-01",
            month_entry(stats, ledger=LEDGER)))
    stale = storage.load_manifest(tdir)
    write_month(root, "DRIFT", "1m-iv", 2025, 1, [(2, 0, 0.46)])
    manifest_before = (tdir / storage.MANIFEST_NAME).read_bytes()
    result = save_stale(tdir, stale, "1m-iv")
    check("exact-equal correction metadata verifies active bytes before save",
          (tdir / storage.MANIFEST_NAME).read_bytes() == manifest_before,
          str(result))
    check("exact-equal correction drift fails closed with exact diagnostic",
          len(result["notes"]) == 1
          and "correction metadata does not match" in result["notes"][0],
          str(result))

    # Deterministic two-thread lock-order proof.  The correction-shaped thread
    # deliberately owns manifest_lock first.  A safe save must wait on that
    # lock before entering ticker_transaction.  The historical inverse order
    # would set ``save_ticker_entered`` and force the correction's bounded
    # ticker acquisition to fail instead of hanging this harness.
    root = fresh_root(base, "lock-order")
    tdir = root / "LCK"
    _, stats = write_month(
        root, "LCK", "1m-iv", 2025, 1, [(2, 0, 0.44)])
    manifest = manifest_for(
        "LCK", "1m-iv", "2025-01", month_entry(stats))
    storage.save_manifest(tdir, manifest)
    correction_holds_manifest = threading.Event()
    save_waits_manifest = threading.Event()
    allow_correction_ticker = threading.Event()
    save_ticker_entered = threading.Event()
    raw_manifest_lock = threading.Lock()
    fake_ticker_lock = threading.Lock()
    thread_errors = []

    class ManifestProbeLock:
        def __enter__(self):
            if raw_manifest_lock.locked():
                save_waits_manifest.set()
            raw_manifest_lock.acquire()
            return self

        def __exit__(self, *_exc):
            raw_manifest_lock.release()
            return False

    manifest_lock = ManifestProbeLock()
    real_transaction = storage.ticker_transaction

    @contextmanager
    def bounded_fake_transaction(_path):
        if not fake_ticker_lock.acquire(timeout=1.0):
            raise RuntimeError("bounded ticker lock acquisition failed")
        try:
            if threading.current_thread().name == "manifest-save":
                save_ticker_entered.set()
            yield
        finally:
            fake_ticker_lock.release()

    def correction_shape():
        try:
            with manifest_lock:
                correction_holds_manifest.set()
                if not allow_correction_ticker.wait(1.0):
                    raise RuntimeError("correction release signal timed out")
                with bounded_fake_transaction(tdir):
                    pass
        except Exception as exc:  # noqa: BLE001 - captured harness evidence
            thread_errors.append(f"correction: {exc}")

    save_result = {}

    def ordinary_save():
        try:
            save_result.update(save_stale(
                tdir, manifest, "1m-iv", lock=manifest_lock))
        except Exception as exc:  # noqa: BLE001 - captured harness evidence
            thread_errors.append(f"save: {exc}")

    storage.ticker_transaction = bounded_fake_transaction
    correction_thread = threading.Thread(
        target=correction_shape, name="correction-shape")
    save_thread = threading.Thread(target=ordinary_save, name="manifest-save")
    try:
        correction_thread.start()
        correction_holds_manifest.wait(1.0)
        save_thread.start()
        waited = save_waits_manifest.wait(1.0)
        inverted = save_ticker_entered.is_set()
        allow_correction_ticker.set()
        correction_thread.join(2.0)
        save_thread.join(2.0)
    finally:
        allow_correction_ticker.set()
        storage.ticker_transaction = real_transaction
    check("manifest-save waits before ticker lock when correction owns manifest",
          waited and not inverted, f"waited={waited} inverted={inverted}")
    check("manifest-lock-first concurrency proof completes without deadlock",
          not correction_thread.is_alive() and not save_thread.is_alive()
          and not thread_errors and save_result.get("notes") == [],
          f"errors={thread_errors} result={save_result}")

    root = fresh_root(base, "fresh-wins")
    tdir = root / "BBB"
    _, old_stats = write_month(
        root, "BBB", "1m-iv", 2025, 1, [(2, 0, 0.41)])
    stale = manifest_for(
        "BBB", "1m-iv", "2025-01", month_entry(old_stats))
    _, new_stats = write_month(
        root, "BBB", "1m-iv", 2025, 1, [(2, 0, 0.51)])
    storage.save_manifest(
        tdir, manifest_for(
            "BBB", "1m-iv", "2025-01",
            month_entry(new_stats, ledger=LEDGER, source="corrected")))
    result = save_stale(tdir, stale, "1m-iv")
    saved_entry = storage.manifest_months(
        storage.load_manifest(tdir), "1m-iv")["2025-01"]
    check("active fresh SHA defeats stale worker SHA",
          saved_entry.get("sha256") == new_stats["sha256"],
          str(saved_entry))
    check("fresh-SHA resolution retains fresh correction evidence",
          saved_entry.get("value_corrections") == LEDGER, str(saved_entry))
    check("fresh-SHA resolution is a clean publication",
          result["notes"] == [], str(result))

    root = fresh_root(base, "mine-wins")
    tdir = root / "CCC"
    _, old_stats = write_month(
        root, "CCC", "1m-iv", 2025, 1, [(2, 0, 0.31)])
    storage.save_manifest(
        tdir, manifest_for(
            "CCC", "1m-iv", "2025-01",
            month_entry(old_stats, ledger=LEDGER)))
    _, new_stats = write_month(
        root, "CCC", "1m-iv", 2025, 1, [(2, 0, 0.61)])
    mine = manifest_for(
        "CCC", "1m-iv", "2025-01", month_entry(new_stats))
    result = save_stale(tdir, mine, "1m-iv")
    saved_entry = storage.manifest_months(
        storage.load_manifest(tdir), "1m-iv")["2025-01"]
    check("active worker SHA defeats older fresh manifest SHA",
          saved_entry.get("sha256") == new_stats["sha256"],
          str(saved_entry))
    check("evidence for different bytes is not transplanted",
          "value_corrections" not in saved_entry, str(saved_entry))
    check("worker-SHA resolution is a clean publication",
          result["notes"] == [], str(result))

    root = fresh_root(base, "neither-wins")
    tdir = root / "DDD"
    _, stats_a = write_month(
        root, "DDD", "1m-iv", 2025, 1, [(2, 0, 0.21)])
    storage.save_manifest(
        tdir, manifest_for(
            "DDD", "1m-iv", "2025-01",
            month_entry(stats_a, ledger=LEDGER)))
    _, stats_b = write_month(
        root, "DDD", "1m-iv", 2025, 1, [(2, 0, 0.22)])
    mine = manifest_for(
        "DDD", "1m-iv", "2025-01", month_entry(stats_b))
    write_month(root, "DDD", "1m-iv", 2025, 1, [(2, 0, 0.23)])
    manifest_before = (tdir / storage.MANIFEST_NAME).read_bytes()
    result = save_stale(tdir, mine, "1m-iv")
    check("third active SHA makes publication fail closed",
          (tdir / storage.MANIFEST_NAME).read_bytes() == manifest_before,
          str(result))
    check("failed SHA CAS leaves a bounded diagnostic",
          len(result["notes"]) == 1
          and "matches neither manifest record" in result["notes"][0],
          str(result))

    root = fresh_root(base, "stage-fence")
    tdir = root / "EEE"
    _, stats = write_month(
        root, "EEE", "1m-iv", 2025, 1, [(2, 0, 0.52)])
    manifest = manifest_for(
        "EEE", "1m-iv", "2025-01", month_entry(stats, ledger=LEDGER))
    manifest["conid"] = 555
    storage.save_manifest(tdir, manifest)
    manifest_before = (tdir / storage.MANIFEST_NAME).read_bytes()
    stage = tdir / storage.VOL_VALUE_RECONCILE_STAGE_DIR
    stage.mkdir()
    # Empty markerless stages are safely removable unpublished cleanup.  A
    # non-empty reconcile-owned marker remains an absolute ordinary-writer
    # fence even when its owner will later reject/recover its contents.
    (stage / "transaction.json").write_text("{}", encoding="utf-8")
    result = save_stale(
        tdir, manifest, "1m-iv",
        repin_intent=ibkr._ConIdRepinIntent(555, 999).activate(
            "2025-01-02"))
    check("pending correction stage fences ordinary manifest save",
          (tdir / storage.MANIFEST_NAME).read_bytes() == manifest_before,
          str(result))
    check("stage fence reports recovery instead of publishing",
          len(result["notes"]) == 1
          and "correction recovery is pending" in result["notes"][0],
          str(result))
    check("correction stage also prevents activated conId publication",
          storage.load_manifest(tdir).get("conid") == 555,
          str(storage.load_manifest(tdir)))
    active = storage.find_month_file(root, "EEE", 2025, 1, "1m-iv")
    month_before = storage._sha256_of_file(active)
    commit_result = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }
    ibkr._commit_month(
        root, "EEE", "1m-iv", (2025, 1),
        [bar(2025, 1, 2, 1, 0.53)], "fixture-run", "fixture",
        commit_result, lambda *_args: None)
    check("pending correction stage fences ordinary month commit",
          storage._sha256_of_file(active) == month_before
          and commit_result["written"] == 0
          and len(commit_result["blocked_months"]) == 1,
          str(commit_result))
    heal_result = {"blocked_months": [], "notes": []}
    healed = ibkr._heal_manifest_months_from_tree(
        root, "EEE", ["1m-iv"], [((2025, 1), (2025, 1))],
        res=heal_result)
    check("pending correction stage fences targeted manifest heal",
          healed == 0 and len(heal_result["blocked_months"]) == 1,
          str(heal_result))

    root = fresh_root(base, "date-split")
    tdir = root / "FFF"
    _, jan_stats = write_month(
        root, "FFF", "1m", 2025, 1, [(2, 0, 101.0)])
    _, feb_stats = write_month(
        root, "FFF", "1m", 2025, 2, [(3, 0, 102.0)])
    storage.save_manifest(
        tdir, manifest_for(
            "FFF", "1m", "2025-01", month_entry(jan_stats)))
    feb_manifest = manifest_for(
        "FFF", "1m", "2025-02", month_entry(feb_stats))
    result = save_stale(
        tdir, feb_manifest, "1m", date_split=True)
    saved_months = storage.manifest_months(
        storage.load_manifest(tdir), "1m")
    check("date-split save retains disjoint fresh and worker months",
          set(saved_months) == {"2025-01", "2025-02"},
          str(saved_months))
    check("date-split SHA-safe merge stays clean",
          result["notes"] == [], str(result))

    # Row 58: a dead-contract replacement is a typed, evidence-activated CAS,
    # not a generic stale-manifest overwrite.
    activated_repin = ibkr._ConIdRepinIntent(555, 999).activate(
        "2025-01-02")
    root = fresh_root(base, "repin-pending")
    tdir = root / "RPN"
    durable = storage.new_manifest("RPN", "RPN")
    durable["conid"] = 555
    storage.save_manifest(tdir, durable)
    mine = copy.deepcopy(durable)
    mine["conid"] = 999
    result = save_stale(
        tdir, mine, "1m",
        repin_intent=ibkr._ConIdRepinIntent(555, 999))
    check("unactivated repin intent cannot publish accepted conId",
          storage.load_manifest(tdir).get("conid") == 555
          and result["notes"] == [], str(result))

    # F-CLAUDE-58-1: publication itself ignores an unactivated intent, so the
    # commit-side guard must stop replacement-contract bytes before the first
    # month write.  Drive the locked implementation directly to keep this
    # second defense layer independently pinned.
    root = fresh_root(base, "repin-unactivated-before-merge")
    tdir = root / "RUM"
    durable = storage.new_manifest("RUM", "RUM")
    durable["conid"] = 555
    storage.save_manifest(tdir, durable)
    commit_result = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }
    unactivated_halt = ""
    try:
        ibkr._commit_month_locked(
            root, "RUM", "1m", (2025, 1),
            [bar(2025, 1, 2, 1, 102.0)], "repin-run", "fixture",
            commit_result, lambda *_args: None, conid=999,
            mstate={
                "manifest": storage.load_manifest(tdir),
                "since_save": 0,
                "repin_intent": ibkr._ConIdRepinIntent(555, 999),
            })
    except ibkr.SeriesHalt as exc:
        unactivated_halt = str(exc)
    check("unactivated repin halt is enforced before locked month merge",
          "positive continuity evidence was not established"
          in unactivated_halt, repr(unactivated_halt))
    check("unactivated repin halt writes zero month bytes",
          storage.find_month_file(root, "RUM", 2025, 1, "1m") is None
          and commit_result["written"] == 0
          and commit_result["added"] == 0
          and storage.load_manifest(tdir).get("conid") == 555,
          str(commit_result))

    root = fresh_root(base, "repin-old")
    tdir = root / "RID"
    durable = storage.new_manifest("RID", "Durable Identity")
    durable["conid"] = 555
    durable["name"] = "Durable Identity"
    durable["intervals"] = {
        "1m": {"months": {}, "worker": "durable"},
        "1m-pre": {"months": {}, "sibling": "preserve"},
    }
    storage.save_manifest(tdir, durable)
    stale = copy.deepcopy(durable)
    stale["name"] = "Stale Display Name"
    stale["intervals"]["1m"]["worker"] = "mine"
    result = save_stale(
        tdir, stale, "1m", repin_intent=activated_repin)
    saved = storage.load_manifest(tdir)
    check("repin CAS publishes expected durable 555->accepted 999",
          saved.get("conid") == 999 and result["notes"] == [],
          f"saved={saved} result={result}")
    check("repin interval publication preserves durable name and sibling",
          saved.get("name") == "Durable Identity"
          and saved["intervals"]["1m-pre"].get("sibling") == "preserve"
          and saved["intervals"]["1m"].get("worker") == "mine",
          str(saved))

    # Replaying the same worker after the first publication is an idempotent
    # success, even though its in-memory manifest still says expected_old.
    result = save_stale(
        tdir, stale, "1m", repin_intent=activated_repin)
    check("repin CAS accepts durable already at accepted 999",
          storage.load_manifest(tdir).get("conid") == 999
          and result["notes"] == [], str(result))

    for label, durable_pin, expected_text in (
            ("missing", None, "durable pin is missing"),
            ("malformed", "555", "durable pin is malformed"),
            ("third-party", 777, "neither expected 555 nor accepted 999")):
        root = fresh_root(base, f"repin-{label}")
        tdir = root / "CAS"
        durable = storage.new_manifest("CAS", "CAS")
        if label == "missing":
            durable.pop("conid", None)
        else:
            durable["conid"] = durable_pin
        storage.save_manifest(tdir, durable)
        before = (tdir / storage.MANIFEST_NAME).read_bytes()
        mine = storage.new_manifest("CAS", "CAS")
        mine["conid"] = 555
        result = save_stale(
            tdir, mine, "1m", repin_intent=activated_repin)
        check(f"repin CAS {label} durable pin fails closed",
              (tdir / storage.MANIFEST_NAME).read_bytes() == before
              and len(result["notes"]) == 1
              and expected_text in result["notes"][0], str(result))

    root = fresh_root(base, "repin-date-split")
    tdir = root / "RDS"
    _, jan_stats = write_month(
        root, "RDS", "1m", 2025, 1, [(2, 0, 101.0)])
    _, feb_stats = write_month(
        root, "RDS", "1m", 2025, 2, [(3, 0, 102.0)])
    durable = manifest_for(
        "RDS", "1m", "2025-01", month_entry(jan_stats))
    durable["conid"] = 555
    storage.save_manifest(tdir, durable)
    mine = manifest_for(
        "RDS", "1m", "2025-02", month_entry(feb_stats))
    mine["conid"] = 555
    result = save_stale(
        tdir, mine, "1m", date_split=True,
        repin_intent=activated_repin)
    saved = storage.load_manifest(tdir)
    saved_months = storage.manifest_months(saved, "1m")
    check("repin date-split publication converges identity and both chunks",
          saved.get("conid") == 999
          and set(saved_months) == {"2025-01", "2025-02"}
          and result["notes"] == [],
          f"saved={saved} result={result}")

    # A third pin arriving before a month transaction is observed by the fresh
    # read and blocks before any accepted-contract bytes are merged.
    root = fresh_root(base, "repin-third-before-merge")
    tdir = root / "RTM"
    path, old_stats = write_month(
        root, "RTM", "1m", 2025, 1, [(2, 0, 101.0)])
    stale = manifest_for(
        "RTM", "1m", "2025-01", month_entry(old_stats))
    stale["conid"] = 555
    storage.save_manifest(tdir, stale)
    third = copy.deepcopy(stale)
    third["conid"] = 777
    storage.save_manifest(tdir, third)
    month_before = storage._sha256_of_file(path)
    conflict = ""
    try:
        ibkr._commit_month(
            root, "RTM", "1m", (2025, 1),
            [bar(2025, 1, 2, 1, 102.0)], "repin-run", "fixture",
            {"blocked_months": [], "dup_existing": 0, "conflicts": 0,
             "months": {}, "added": 0, "written": 0, "notes": []},
            lambda *_args: None, conid=999,
            mstate={"manifest": stale, "since_save": 0,
                    "lock": threading.Lock(),
                    "repin_intent": activated_repin})
    except ibkr.SeriesHalt as exc:
        conflict = str(exc)
    check("repin third pin blocks before month-byte merge",
          "durable pin 777" in conflict
          and storage._sha256_of_file(path) == month_before
          and storage.load_manifest(tdir).get("conid") == 777,
          f"conflict={conflict!r}")

    root = fresh_root(base, "new-ticker")
    ticker = "NEW"
    tdir = root / ticker
    transaction_paths = []
    real_transaction = storage.ticker_transaction

    @contextmanager
    def observed_transaction(path):
        transaction_paths.append(
            Path(path).resolve(strict=False))
        with real_transaction(path):
            yield

    storage.ticker_transaction = observed_transaction
    result = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }
    try:
        ibkr._commit_month(
            root, ticker, "1m", (2025, 1),
            [bar(2025, 1, 2, 0, 100.0)], "fixture-run", "fixture",
            result, lambda *_args: None, conid=12345)
    finally:
        storage.ticker_transaction = real_transaction
    saved = storage.load_manifest(tdir)
    saved_entry = storage.manifest_months(saved, "1m")["2025-01"]
    active = storage.find_month_file(root, ticker, 2025, 1, "1m")
    check("ticker transaction permits first commit before ticker dir exists",
          saved is not None and active is not None, str(result))
    check("first commit manifest matches its active month bytes",
          saved_entry.get("sha256") == storage._sha256_of_file(active),
          str(saved_entry))
    check("commit and nested safe save both acquire canonical ticker lock",
          len(transaction_paths) >= 2
          and set(transaction_paths) == {tdir.resolve(strict=False)},
          str(transaction_paths))

    root = fresh_root(base, "append-ledger")
    tdir = root / "GGG"
    _, old_stats = write_month(
        root, "GGG", "1m-iv", 2025, 1, [(2, 0, 0.40)])
    published = manifest_for(
        "GGG", "1m-iv", "2025-01",
        month_entry(old_stats, ledger=LEDGER, source="corrected"))
    storage.save_manifest(tdir, published)
    stale_state = storage.load_manifest(tdir)
    storage.manifest_months(
        stale_state, "1m-iv")["2025-01"].pop("value_corrections")
    result = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }
    ibkr._commit_month(
        root, "GGG", "1m-iv", (2025, 1),
        [bar(2025, 1, 2, 1, 0.41)], "append-run", "fixture",
        result, lambda *_args: None,
        mstate={"manifest": stale_state,
                "since_save": ibkr.MANIFEST_CHECKPOINT_MONTHS,
                "lock": threading.Lock()})
    saved_entry = storage.manifest_months(
        storage.load_manifest(tdir), "1m-iv")["2025-01"]
    check("ordinary append inherits published pre-write correction ledger",
          saved_entry.get("value_corrections") == LEDGER,
          str(saved_entry))
    check("ordinary append ledger matches newly active month entry",
          saved_entry.get("sha256") == storage._sha256_of_file(
              storage.find_month_file(root, "GGG", 2025, 1, "1m-iv")),
          str(saved_entry))

    root = fresh_root(base, "repin-append-ledger")
    tdir = root / "RLG"
    _, old_stats = write_month(
        root, "RLG", "1m-iv", 2025, 1, [(2, 0, 0.40)])
    published = manifest_for(
        "RLG", "1m-iv", "2025-01",
        month_entry(old_stats, ledger=LEDGER, source="corrected"))
    published["conid"] = 555
    storage.save_manifest(tdir, published)
    result = {
        "blocked_months": [], "dup_existing": 0, "conflicts": 0,
        "months": {}, "added": 0, "written": 0, "notes": [],
    }
    ibkr._commit_month(
        root, "RLG", "1m-iv", (2025, 1),
        [bar(2025, 1, 2, 1, 0.41)], "repin-ledger", "fixture",
        result, lambda *_args: None, conid=999,
        mstate={
            "manifest": storage.load_manifest(tdir),
            "since_save": 0,
            "lock": threading.Lock(),
            "repin_intent": ibkr._ConIdRepinIntent(555, 999).activate(
                "2025-01-02"),
        })
    saved = storage.load_manifest(tdir)
    saved_entry = storage.manifest_months(saved, "1m-iv")["2025-01"]
    check("correction-ledger month publication carries activated repin",
          result["written"] == 1 and saved.get("conid") == 999,
          f"saved={saved} result={result}")
    check("correction-ledger repin preserves ledger on new active bytes",
          saved_entry.get("value_corrections") == LEDGER
          and saved_entry.get("sha256") == storage._sha256_of_file(
              storage.find_month_file(root, "RLG", 2025, 1, "1m-iv")),
          str(saved_entry))


print(f"\n{CHECKS - len(FAILURES)}/{CHECKS} checks passed")
if FAILURES:
    print("FAILURES: " + ", ".join(FAILURES))
    raise SystemExit(1)
