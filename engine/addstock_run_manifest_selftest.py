"""Headless tests for the Add Stocks durable run manifest."""

from __future__ import annotations

import ast
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

import addstock_run_manifest as arm


PASS = [0]
FAIL = [0]
NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def check(condition, label):
    if condition:
        PASS[0] += 1
    else:
        FAIL[0] += 1
        print(f"  FAIL: {label}")


def raises(exc_type, fn):
    try:
        fn()
    except exc_type:
        return True
    return False


def fresh(base, name):
    root = base / name / "Stock Data Storage"
    root.mkdir(parents=True)
    return root


def _call_name(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def test_display_integration():
    source = (Path(__file__).resolve().parents[1] / "display_data.py")
    source_text = source.read_text(encoding="utf-8")
    tree = ast.parse(source_text)
    functions = {node.name: node for node in ast.walk(tree)
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    start = functions["_storage_find_start_fill"]
    calls = [node for node in ast.walk(start) if isinstance(node, ast.Call)]
    create_lines = [node.lineno for node in calls if _call_name(node.func) in {
        "addstock_run_manifest.create_run",
        "addstock_run_manifest.resume_run"}]
    clear_lines = [node.lineno for node in ast.walk(start)
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Attribute)
                           and target.attr == "_find_queue"
                           for target in node.targets)]
    thread_lines = [node.lineno for node in calls
                    if isinstance(node.func, ast.Attribute)
                    and node.func.attr == "start"
                    and isinstance(node.func.value, ast.Call)
                    and _call_name(node.func.value.func) == "threading.Thread"]
    check(create_lines and clear_lines and thread_lines
          and max(create_lines) < min(clear_lines) < min(thread_lines),
          "GUI persists intent before clearing its queue or starting a worker")

    engine_calls = [node for node in calls if _call_name(node.func) in {
        "stock_ibkr.gap_fill", "stock_ibkr.gap_fill_parallel_resilient"}]
    check(len(engine_calls) == 2 and all(
        {keyword.arg for keyword in node.keywords}.issuperset(
            {"on_series_start", "on_series"}) for node in engine_calls),
        "serial and parallel Add Stocks paths report lifecycle boundaries")

    recovery = functions["_addstock_offer_recovery"]
    recovery_calls = {_call_name(node.func) for node in ast.walk(recovery)
                      if isinstance(node, ast.Call)}
    check({"messagebox.askyesnocancel",
           "addstock_run_manifest.discard_active",
           "addstock_run_manifest.archive_complete"}.issubset(recovery_calls),
          "GUI exposes Resume/Discard and retries interrupted completion")

    resume = functions["_addstock_resume_active"]
    resume_calls = {_call_name(node.func) for node in ast.walk(resume)
                    if isinstance(node, ast.Call)}
    check("self._addstock_reconcile_evidence" in resume_calls,
          "resume reconciles saved current evidence before redoing work")

    complete = functions["_addstock_series_complete"]
    complete_calls = [node for node in ast.walk(complete)
                      if isinstance(node, ast.Call)]
    completion = next(
        node for node in complete_calls
        if _call_name(node.func)
        == "addstock_run_manifest.mark_series_complete")
    empty_arg = next((keyword.value for keyword in completion.keywords
                      if keyword.arg == "empty"), None)
    empty_assign = [
        node for node in ast.walk(complete)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "empty"
                for target in node.targets)
        and isinstance(node.value, ast.Call)
        and _call_name(node.value.func)
        == "self._addstock_interval_empty_on_disk"]
    check(len(empty_assign) == 1
          and empty_assign[0].lineno < completion.lineno
          and isinstance(empty_arg, ast.Name) and empty_arg.id == "empty",
          "every GUI completion classifies empty storage before ledger credit")

    portfree = functions["_addstock_consume_portfree_debt"]
    portfree_heals = [
        node.lineno for node in ast.walk(portfree)
        if isinstance(node, ast.Call)
        and _call_name(node.func)
        == "addstock_run_manifest.exempt_empty_verification"]
    debt_lines = [
        node.lineno for node in ast.walk(portfree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "debt"
                for target in node.targets)]
    check(portfree_heals and debt_lines
          and min(portfree_heals) < min(debt_lines),
          "port-free verification heals empty debt before snapshotting work")

    probe = next(node for node in start.body
                 if isinstance(node, ast.FunctionDef)
                 and node.name == "_probe_check")
    probe_calls = [node for node in ast.walk(probe)
                   if isinstance(node, ast.Call)]
    probe_heal = [node.lineno for node in probe_calls
                  if _call_name(node.func)
                  == "addstock_run_manifest.exempt_empty_verification"]
    probe_xval = [node.lineno for node in probe_calls
                  if _call_name(node.func)
                  == "stock_validate.cross_validate_ticker"]
    probe_source = ast.get_source_segment(
        source_text, probe) or ""
    check(probe_heal and probe_xval and min(probe_heal) < min(probe_xval)
          and "if interval in confirmed_empty:" in probe_source,
          "fresh WS8 verification heals then skips confirmed-empty cross-validation")

    incremental = functions["_gap_incremental_queue"]
    incremental_calls = [node for node in ast.walk(incremental)
                         if isinstance(node, ast.Call)]
    empty_returns = [
        node for node in ast.walk(incremental)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Call)
        and _call_name(node.test.func)
        == "self._addstock_interval_empty_on_disk"
        and any(isinstance(child, ast.Return) for child in node.body)]
    queue_writes = [node.lineno for node in incremental_calls
                    if _call_name(node.func) == "q.put"]
    check(empty_returns and queue_writes
          and min(node.lineno for node in empty_returns) < min(queue_writes),
          "incremental Add Stocks gap scans exclude confirmed-empty series")

    post_scan = functions["_gap_scan_after_fetch"]
    post_scan_calls = [node for node in ast.walk(post_scan)
                       if isinstance(node, ast.Call)]
    kwonly = {arg.arg: default for arg, default in zip(
        post_scan.args.kwonlyargs, post_scan.args.kw_defaults)}
    skip_default = kwonly.get("skip_empty")
    negative_empty_filters = [
        condition for node in ast.walk(post_scan)
        if isinstance(node, ast.comprehension)
        for condition in node.ifs
        if isinstance(condition, ast.UnaryOp)
        and isinstance(condition.op, ast.Not)
        and isinstance(condition.operand, ast.Call)
        and _call_name(condition.operand.func)
        == "self._addstock_interval_empty_on_disk"]
    check(isinstance(skip_default, ast.Constant)
          and skip_default.value is False
          and negative_empty_filters,
          "post-fetch gap scan keeps opt-in empty-series filtering")

    done = functions["_storage_find_done"]
    done_gap_calls = [node for node in ast.walk(done)
                      if isinstance(node, ast.Call)
                      and _call_name(node.func)
                      == "self._gap_scan_after_fetch"]
    check(any(any(keyword.arg == "skip_empty"
                      and isinstance(keyword.value, ast.Constant)
                      and keyword.value.value is True
                      for keyword in node.keywords)
              for node in done_gap_calls),
          "Add Stocks completion opts into empty-series gap filtering")

    fixdata = functions["_fixdata_start"]
    fix_calls = [node for node in ast.walk(fixdata) if isinstance(node, ast.Call)]
    pipeline_call = next(node for node in fix_calls
                         if _call_name(node.func) == "fix_data_pipeline.run")
    keywords = {keyword.arg: keyword.value for keyword in pipeline_call.keywords}
    required_persistence = [node for node in fix_calls
                            if _call_name(node.func).endswith("_audit_ticker")
                            and any(keyword.arg == "require_persisted"
                                    and isinstance(keyword.value, ast.Constant)
                                    and keyword.value.value is True
                                    for keyword in node.keywords)]
    check("verification_fn" in keywords
          and isinstance(keywords.get("scan_fn"), ast.Name)
          and keywords["scan_fn"].id == "_scan"
          and required_persistence,
          "Fix Data consumes debt only from persisted current evidence")

    fix_open = functions["_storage_fixdata_open"]
    restart_defaults = []
    for node in ast.walk(fix_open):
        if (isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Attribute)
                        and target.attr == "_fixdata_restart_dead"
                        for target in node.targets)
                and isinstance(node.value, ast.Call)
                and _call_name(node.value.func) == "tk.BooleanVar"):
            restart_defaults.extend(
                keyword.value for keyword in node.value.keywords
                if keyword.arg == "value")
    fix_open_source = ast.get_source_segment(source_text, fix_open) or ""
    check(any(isinstance(value, ast.Constant) and value.value is False
              for value in restart_defaults)
          and "takes over the screen" in fix_open_source,
          "Fix Data port recovery is an explicit default-off screen gate")

    fix_call_names = {_call_name(node.func) for node in fix_calls}
    fix_attrs = {node.attr for node in ast.walk(fixdata)
                 if isinstance(node, ast.Attribute)}
    restart_guards = [
        node for node in ast.walk(fixdata)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "restart_dead"
        and any(_call_name(call.func) == "self._fleet_callbacks"
                for branch in node.body
                for call in ast.walk(branch)
                if isinstance(call, ast.Call))]
    recovery_keyword = keywords.get("port_recover_fn")
    check(isinstance(recovery_keyword, ast.Name)
          and recovery_keyword.id == "port_recover_fn"
          and "_fixdata_restart_dead" in fix_attrs
          and "self._fleet_callbacks" in fix_call_names
          and restart_guards,
          "Fix Data wires safe TWS recovery only through the opted-in seam")


def test_lifecycle_and_archive(base):
    root = fresh(base, "lifecycle")
    archive = root.parent / "Run Logs"
    run = arm.create_run(
        root,
        [("msft", "1m-post"), ("AAPL", "1m"), ("AAPL", "1d")],
        params={"mode": "add", "since": "2020-01-01",
                "extended": True, "store_daily": True,
                "ports_at_start": [2000, "3000", 2000]},
        run_id="addstock-test-lifecycle", now=NOW,
        earliest_tickers=["MSFT"])
    check(arm.active_path(root).is_file(), "create writes the active sidecar")
    check(run["params"]["intervals"] == ["1d", "1m", "1m-post"]
          and run["params"]["kinds"] == ["trades"]
          and run["params"]["ports_at_start"] == [2000, 3000],
          "create normalizes bounded run parameters")
    check(run["tickers"]["MSFT"]["series"]["1m-post"]["xval"]
          and run["tickers"]["MSFT"]["series"]["1m-post"]["gaps"],
          "extended sessions never acquire RTH verification debt")
    check(arm.summary(run) == {
        "run_id": "addstock-test-lifecycle", "state": "active",
        "pending": 2, "built_unverified": 0, "verified": 0,
        "total": 2, "remaining_series": 3},
        "summary reports durable pending intent")
    before = arm.active_path(root).read_bytes()
    check(raises(arm.ActiveRunExists, lambda: arm.create_run(
        root, [("IBM", "1m")], run_id="addstock-collision", now=NOW)),
        "create refuses to overwrite an active run")
    check(arm.active_path(root).read_bytes() == before,
          "collision refusal preserves the active bytes")

    arm.mark_series_started(root, run["run_id"], "AAPL", "1m", now=NOW)
    started = arm.load_run(root)
    check(started["tickers"]["AAPL"]["state"] == "building"
          and started["tickers"]["AAPL"]["series"]["1m"]["state"]
          == "building", "first dispatch persists ticker and series building")
    check(raises(arm.RunMismatch, lambda: arm.mark_series_complete(
        root, "stale-run", "AAPL", "1m", now=NOW)),
        "stale callback cannot mutate a newer run")

    for ticker, interval in [("AAPL", "1m"), ("AAPL", "1d"),
                             ("MSFT", "1m-post")]:
        arm.mark_series_complete(root, run["run_id"], ticker, interval, now=NOW)
    built = arm.load_run(root)
    check(built["tickers"]["AAPL"]["state"] == "built"
          and built["tickers"]["AAPL"]["missing"]
          == ["xval", "gaps", "earliest"],
          "all committed RTH series become built with exact debt")
    check(built["tickers"]["MSFT"]["state"] == "verified",
          "known earliest plus extended-only series can verify immediately")

    arm.mark_verification(root, run["run_id"], "AAPL", "xval",
                          ["1m"], now=NOW, archive_dir=archive)
    partial = arm.load_run(root)
    check(partial["tickers"]["AAPL"]["missing"]
          == ["xval", "gaps", "earliest"],
          "one interval does not clear ticker-wide xval debt")
    arm.mark_verification(root, run["run_id"], "AAPL", "xval",
                          ["1d"], now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "AAPL", "gaps",
                          ["1m", "1d"], now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "AAPL", "earliest",
                          now=NOW, archive_dir=archive)
    verified = arm.load_run(root)
    check(verified["tickers"]["AAPL"]["state"] == "verified"
          and arm.active_path(root).exists(),
          "verification alone cannot archive before fetch-finished")
    result = arm.mark_fetch_finished(
        root, run["run_id"], now=NOW, archive_dir=archive)
    archived = Path(result["archived"])
    check(not arm.active_path(root).exists() and archived.is_file(),
          "fetch-finished plus all verified archives then removes active state")
    archived_data = arm.validate_manifest(json.loads(
        archived.read_text(encoding="utf-8")))
    check(archived_data["state"] == "complete"
          and archived_data["finalize"]["reason"] == "complete",
          "archive preserves a valid complete manifest")


def test_resume_and_seal_order(base):
    root = fresh(base, "resume")
    run = arm.create_run(
        root, [("AAA", "1m"), ("AAA", "1m-pre"), ("BBB", "1m")],
        run_id="addstock-test-resume", now=NOW)
    arm.mark_series_started(root, run["run_id"], "AAA", "1m", now=NOW)
    arm.mark_series_complete(root, run["run_id"], "AAA", "1m-pre", now=NOW)
    arm.mark_fetch_finished(
        root, run["run_id"], reason="user_pause",
        seal_pending=["BBB 1m"], now=NOW, archive_dir=root.parent / "logs")
    interrupted = arm.load_run(root)
    check(interrupted["state"] == "interrupted"
          and interrupted["fetch_finished"],
          "unfinished run persists an interrupted finalize boundary")
    ordered = arm.pending_selections(interrupted)
    check(ordered[0] == ("BBB", "1m")
          and ("AAA", "1m") in ordered
          and ("AAA", "1m-pre") not in ordered,
          "resume orders seal debt first and skips completed series")
    resumed = arm.resume_run(root, run["run_id"], now=NOW)
    check(resumed["state"] == "active" and not resumed["fetch_finished"]
          and resumed["tickers"]["AAA"]["series"]["1m"]["state"]
          == "pending", "resume resets in-flight work without losing completion")


def test_verification_debt(base):
    root = fresh(base, "debt")
    archive = root.parent / "logs"
    run = arm.create_run(root, [("DEBT", "1m")],
                         run_id="addstock-test-debt", now=NOW)
    arm.mark_series_complete(root, run["run_id"], "DEBT", "1m", now=NOW)
    arm.mark_fetch_finished(root, run["run_id"], reason="verification_debt",
                            now=NOW, archive_dir=archive)
    debt = arm.load_run(root)
    check(arm.summary(debt)["built_unverified"] == 1
          and debt["tickers"]["DEBT"]["missing"]
          == ["xval", "gaps", "earliest"],
          "built-not-verified remains durably visible")
    arm.mark_verification(root, run["run_id"], "DEBT", "xval", ["1m"],
                          now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "DEBT", "gaps", ["1m"],
                          now=NOW, archive_dir=archive)
    port_free = arm.load_run(root)
    check(port_free["tickers"]["DEBT"]["missing"] == ["earliest"]
          and arm.active_path(root).exists(),
          "port-free stages consume debt but earliest waits for a port")
    arm.mark_series_started(root, run["run_id"], "DEBT", "1m", now=NOW)
    redispatched = arm.load_run(root)["tickers"]["DEBT"]["series"]["1m"]
    check(not redispatched["xval"] and not redispatched["gaps"],
          "redispatch invalidates prior checks before a possible data change")
    arm.mark_series_complete(root, run["run_id"], "DEBT", "1m", now=NOW)
    arm.mark_verification(root, run["run_id"], "DEBT", "xval", ["1m"],
                          now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "DEBT", "gaps", ["1m"],
                          now=NOW, archive_dir=archive)
    result = arm.mark_verification(
        root, run["run_id"], "DEBT", "earliest", now=NOW,
        archive_dir=archive)
    check(result["archived"] is not None and not arm.active_path(root).exists(),
          "last port-bearing evidence completes and archives the run")

    selected_sha = "a" * 64
    reference_sha = "b" * 64
    entry = {
        "ticker": "DEBT", "interval": "1m",
        "interval_fingerprint": {
            "current": True, "after_sha256": selected_sha},
        "reference_interval": "1d-iv",
        "reference_interval_fingerprint": {
            "current": True, "after_sha256": reference_sha},
    }
    current = arm.current_xval_intervals(
        root, [entry, entry],
        fingerprint_fn=lambda *_args: {"sha256": selected_sha},
        optional_fingerprint_fn=lambda *_args: {"sha256": reference_sha})
    check(current == ["1m"],
          "saved xval evidence is reusable only once while inputs still match")
    stale = arm.current_xval_intervals(
        root, [entry],
        fingerprint_fn=lambda *_args: {"sha256": "c" * 64},
        optional_fingerprint_fn=lambda *_args: {"sha256": reference_sha})
    check(stale == [], "changed selected input leaves saved xval as debt")
    stale_reference = arm.current_xval_intervals(
        root, [entry],
        fingerprint_fn=lambda *_args: {"sha256": selected_sha},
        optional_fingerprint_fn=lambda *_args: {"sha256": "c" * 64})
    check(stale_reference == [],
          "changed reference input leaves saved xval as debt")
    malformed = dict(entry, interval_fingerprint={
        "current": True, "after_sha256": "not-a-sha"})
    check(arm.current_xval_intervals(
        root, [malformed],
        fingerprint_fn=lambda *_args: {"sha256": "not-a-sha"}) == [],
        "malformed xval fingerprints fail closed")


def test_empty_verification_exemption(base):
    default_root = fresh(base, "empty-default")
    explicit_root = fresh(base, "empty-explicit")
    for root in (default_root, explicit_root):
        arm.create_run(
            root, [("SAME", "1d-hvol")],
            run_id="addstock-empty-byte-equivalent", now=NOW,
            earliest_tickers=["SAME"])
    arm.mark_series_complete(
        default_root, "addstock-empty-byte-equivalent", "SAME", "1d-hvol",
        now=NOW)
    arm.mark_series_complete(
        explicit_root, "addstock-empty-byte-equivalent", "SAME", "1d-hvol",
        empty=False, now=NOW)
    check(arm.active_path(default_root).read_bytes()
          == arm.active_path(explicit_root).read_bytes(),
          "omitted empty and explicit empty=False remain byte-equivalent")
    before_invalid = arm.active_path(default_root).read_bytes()
    check(raises(arm.ManifestError, lambda: arm.mark_series_complete(
              default_root, "addstock-empty-byte-equivalent", "SAME",
              "1d-hvol", empty="yes", now=NOW))
          and arm.active_path(default_root).read_bytes() == before_invalid,
          "truthy non-boolean empty input cannot waive debt or change bytes")

    source_root = fresh(base, "empty-source")
    source = arm.create_run(
        source_root,
        [("MIXED", "1m"), ("MIXED", "1d-hvol"),
         ("MIXED", "1m-pre")],
        run_id="addstock-empty-source", now=NOW,
        earliest_tickers=["MIXED"])
    arm.mark_series_complete(
        source_root, source["run_id"], "MIXED", "1m",
        empty=True, now=NOW)
    arm.mark_series_complete(
        source_root, source["run_id"], "MIXED", "1d-hvol", now=NOW)
    arm.mark_series_complete(
        source_root, source["run_id"], "MIXED", "1m-pre", now=NOW)
    record = arm.load_run(source_root)["tickers"]["MIXED"]
    check(record["series"]["1m"] == {
              "state": "complete", "xval": True, "gaps": True}
          and record["series"]["1d-hvol"] == {
              "state": "complete", "xval": False, "gaps": False}
          and record["series"]["1m-pre"] == {
              "state": "complete", "xval": True, "gaps": True}
          and record["state"] == "built",
          "empty=True waives only its selected RTH series; real and non-RTH semantics hold")

    heal_root = fresh(base, "empty-heal")
    selections = [
        ("EMPTY", "1d-hvol"),
        ("PRESENT", "1d-hvol"),
        ("RAISE", "1m-iv"),
        ("TRUTHY", "1m"),
        ("PENDING", "1d-hvol"),
        ("EXT", "1m-pre"),
        ("CLEARED", "1d"),
    ]
    healed_run = arm.create_run(
        heal_root, selections, run_id="addstock-empty-heal", now=NOW,
        earliest_tickers=[ticker for ticker, _interval in selections])
    for ticker, interval in selections:
        if ticker != "PENDING":
            arm.mark_series_complete(
                heal_root, healed_run["run_id"], ticker, interval,
                empty=ticker == "CLEARED", now=NOW)
    arm.mark_fetch_finished(
        heal_root, healed_run["run_id"], reason="verification_debt",
        now=NOW, archive_dir=heal_root.parent / "logs")
    predicate_calls = []

    def classify_empty(ticker, interval):
        predicate_calls.append((ticker, interval))
        if ticker == "RAISE":
            raise OSError("injected unreadable bank manifest")
        if ticker == "TRUTHY":
            return "yes"
        return ticker == "EMPTY"

    healed = arm.exempt_empty_verification(
        heal_root, healed_run["run_id"], classify_empty,
        now=NOW, archive_dir=heal_root.parent / "logs")
    state = arm.load_run(heal_root)
    check(healed == {"exempted": [("EMPTY", "1d-hvol")],
                     "archived": None}
          and state["tickers"]["EMPTY"]["state"] == "verified"
          and state["tickers"]["PRESENT"]["state"] == "built"
          and state["tickers"]["RAISE"]["state"] == "built"
          and state["tickers"]["TRUTHY"]["state"] == "built"
          and state["tickers"]["PENDING"]["state"] == "pending",
          "heal is per-series, literal-True, complete-only, and predicate-error safe")
    check(predicate_calls == [
              ("EMPTY", "1d-hvol"),
              ("PRESENT", "1d-hvol"),
              ("RAISE", "1m-iv"),
              ("TRUTHY", "1m"),
          ],
          "heal skips pending, non-RTH, and already-cleared series before predicate I/O")
    before_noop = arm.active_path(heal_root).read_bytes()
    second = arm.exempt_empty_verification(
        heal_root, healed_run["run_id"], classify_empty,
        now=lambda: (_ for _ in ()).throw(
            AssertionError("no-op heal evaluated the clock")),
        archive_dir=heal_root.parent / "logs")
    check(second == {"exempted": [], "archived": None}
          and arm.active_path(heal_root).read_bytes() == before_noop,
          "idempotent no-op heal neither timestamps nor rewrites the manifest")

    archive_root = fresh(base, "empty-heal-archive")
    archive_dir = archive_root.parent / "logs"
    archived_run = arm.create_run(
        archive_root, [("LAST", "1d-hvol")],
        run_id="addstock-empty-heal-archive", now=NOW,
        earliest_tickers=["LAST"])
    arm.mark_series_complete(
        archive_root, archived_run["run_id"], "LAST", "1d-hvol", now=NOW)
    arm.mark_fetch_finished(
        archive_root, archived_run["run_id"],
        reason="verification_debt", now=NOW, archive_dir=archive_dir)
    archived = arm.exempt_empty_verification(
        archive_root, archived_run["run_id"], lambda *_args: True,
        now=NOW, archive_dir=archive_dir)
    check(archived["exempted"] == [("LAST", "1d-hvol")]
          and archived["archived"] is not None
          and Path(archived["archived"]).is_file()
          and arm.load_run(archive_root) is None,
          "last empty verification debt completes and archives the run")

    earliest_root = fresh(base, "empty-heal-earliest")
    earliest_run = arm.create_run(
        earliest_root, [("UNKNOWN", "1d-hvol")],
        run_id="addstock-empty-heal-earliest", now=NOW)
    arm.mark_series_complete(
        earliest_root, earliest_run["run_id"], "UNKNOWN", "1d-hvol",
        now=NOW)
    arm.mark_fetch_finished(
        earliest_root, earliest_run["run_id"],
        reason="verification_debt", now=NOW,
        archive_dir=earliest_root.parent / "logs")
    earliest = arm.exempt_empty_verification(
        earliest_root, earliest_run["run_id"], lambda *_args: True,
        now=NOW, archive_dir=earliest_root.parent / "logs")
    earliest_state = arm.load_run(earliest_root)
    check(earliest["archived"] is None
          and earliest_state["tickers"]["UNKNOWN"]["missing"]
          == ["earliest"],
          "empty exemption cannot bypass independent earliest evidence debt")

    retry_root = fresh(base, "empty-heal-archive-retry")
    retry_archive = retry_root.parent / "logs"
    retry_run = arm.create_run(
        retry_root, [("RETRY", "1d-hvol")],
        run_id="addstock-empty-heal-retry", now=NOW,
        earliest_tickers=["RETRY"])
    arm.mark_series_complete(
        retry_root, retry_run["run_id"], "RETRY", "1d-hvol", now=NOW)
    arm.mark_fetch_finished(
        retry_root, retry_run["run_id"],
        reason="verification_debt", now=NOW, archive_dir=retry_archive)
    original_archive = arm._archive_complete
    arm._archive_complete = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OSError("injected empty-heal archive interruption"))
    try:
        interrupted = raises(OSError, lambda: arm.exempt_empty_verification(
            retry_root, retry_run["run_id"], lambda *_args: True,
            now=NOW, archive_dir=retry_archive))
    finally:
        arm._archive_complete = original_archive
    durable = arm.load_run(retry_root)
    recovered = Path(arm.archive_complete(
        retry_root, retry_run["run_id"], retry_archive))
    check(interrupted and durable["state"] == "complete"
          and durable["tickers"]["RETRY"]["state"] == "verified"
          and recovered.is_file() and arm.load_run(retry_root) is None,
          "empty-heal archive interruption stays durable and retryable")


def test_display_empty_predicate(base):
    source = (Path(__file__).resolve().parents[1] / "display_data.py")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    node = copy.deepcopy(next(
        item for item in ast.walk(tree)
        if isinstance(item, ast.FunctionDef)
        and item.name == "_addstock_interval_empty_on_disk"))
    node.decorator_list = []
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    namespace = {"stock_storage": arm.ss, "Path": Path}
    exec(compile(module, "<empty-disk-predicate>", "exec"), namespace)
    predicate = namespace[node.name]

    root = fresh(base, "empty-predicate")

    class Fake:
        _storage_root = root

    fake = Fake()

    def save(ticker, manifest):
        ticker_dir = root / ticker
        ticker_dir.mkdir(parents=True, exist_ok=True)
        arm.ss.save_manifest(ticker_dir, manifest)

    mixed = arm.ss.new_manifest("MIXED", "MIXED")
    arm.ss.manifest_months(mixed, "1d")
    arm.ss.manifest_months(mixed, "1m")["2026-01"] = {
        "status": "present", "sha256": "a" * 64, "rows": 1,
        "first": "1/2/2026 9:30:00", "last": "1/2/2026 9:30:00"}
    arm.ss.manifest_months(mixed, "1d-hvol")["2026-01"] = {
        "status": "MISSING"}
    arm.ss.manifest_months(mixed, "1m-iv")["2026-01"] = {
        "status": "format-error"}
    save("MIXED", mixed)
    wrong = arm.ss.new_manifest("WRONG", "OTHER")
    save("WRONG", wrong)
    malformed = root / "BROKEN"
    malformed.mkdir(parents=True)
    (malformed / arm.ss.MANIFEST_NAME).mkdir()
    badkey = arm.ss.new_manifest("BADKEY", "BADKEY")
    arm.ss.manifest_months(badkey, "1m")["not-a-month"] = {
        "status": "MISSING"}
    save("BADKEY", badkey)
    badabs = arm.ss.new_manifest("BADABS", "BADABS")
    badabs["intervals"]["1m"] = {
        "months": {}, "verified_absent": ["2026-01-02", "2026-01-02"],
        "backfill_incomplete": False}
    save("BADABS", badabs)
    incomplete = arm.ss.new_manifest("INCOMPLETE", "INCOMPLETE")
    incomplete["intervals"]["1m"] = {
        "months": {}, "verified_absent": [], "backfill_incomplete": True}
    save("INCOMPLETE", incomplete)
    duplicate = root / "DUP"
    duplicate.mkdir(parents=True)
    (duplicate / arm.ss.MANIFEST_NAME).write_text(
        '{"folder":"DUP","folder":"DUP","intervals":{}}',
        encoding="utf-8")

    check(predicate(fake, "MIXED", "not-an-interval") is False
          and predicate(fake, "NOSUCH", "1d-hvol") is False
          and predicate(fake, "WRONG", "1d-hvol") is False
          and predicate(fake, "BROKEN", "1d-hvol") is False
          and predicate(fake, "BADKEY", "1m") is False
          and predicate(fake, "BADABS", "1m") is False
          and predicate(fake, "DUP", "1m") is False
          and predicate(fake, "INCOMPLETE", "1m") is False,
          "disk predicate fails safe on invalid input and unreadable manifests")
    check(predicate(fake, "MIXED", "1h") is True
          and predicate(fake, "MIXED", "1d") is True
          and predicate(fake, "MIXED", "1d-hvol") is True
          and predicate(fake, "MIXED", "1m") is False
          and predicate(fake, "MIXED", "1m-iv") is False,
          "disk predicate distinguishes proven empty, present, and corrupt storage")


def test_seal_debt_blocks_completion(base):
    root = fresh(base, "seal-debt")
    archive = root.parent / "logs"
    run = arm.create_run(root, [("SEAL", "1m")],
                         run_id="addstock-test-seal-debt", now=NOW,
                         earliest_tickers=["SEAL"])
    arm.mark_series_complete(root, run["run_id"], "SEAL", "1m", now=NOW)
    arm.mark_verification(root, run["run_id"], "SEAL", "xval", ["1m"],
                          now=NOW, archive_dir=archive)
    arm.mark_verification(root, run["run_id"], "SEAL", "gaps", ["1m"],
                          now=NOW, archive_dir=archive)
    held = arm.mark_fetch_finished(
        root, run["run_id"], reason="fleet_down",
        seal_pending=["SEAL 1m"], now=NOW, archive_dir=archive)
    check(held["archived"] is None and arm.active_path(root).exists()
          and arm.load_run(root)["state"] == "interrupted",
          "seal debt blocks archival even when every verification is current")
    cleared = arm.replace_seal_pending(
        root, run["run_id"], [], now=NOW, archive_dir=archive)
    check(cleared["archived"] is not None and not arm.active_path(root).exists(),
          "freshly confirmed seal completion clears debt and permits archive")


def test_fail_closed_and_discard(base):
    root = fresh(base, "invalid")
    archive = root.parent / "logs"
    path = arm.active_path(root)
    torn = b'{"schema":1,"run_id":'
    path.write_bytes(torn)
    check(raises(arm.ManifestError, lambda: arm.load_run(root)),
          "torn manifest fails closed")
    check(raises(arm.ActiveRunExists, lambda: arm.create_run(
        root, [("SAFE", "1m")], run_id="addstock-no-overwrite", now=NOW)),
        "invalid existing state blocks a new run")
    check(path.read_bytes() == torn, "invalid collision never rewrites old bytes")
    discarded = Path(arm.discard_active(root, archive, now=NOW))
    check(not path.exists() and discarded.read_bytes() == torn,
          "explicit discard preserves invalid bytes outside the active path")

    path.write_text("[" * 1200 + "0" + "]" * 1200, encoding="ascii")
    check(raises(arm.ManifestError, lambda: arm.load_run(root)),
          "deeply nested JSON fails closed instead of escaping the loader")
    arm.discard_active(root, archive, now=NOW)

    path.write_bytes(b"x" * (arm.MAX_BYTES + 1))
    check(raises(arm.ManifestError, lambda: arm.load_run(root)),
          "oversized manifest fails before JSON parsing")
    arm.discard_active(root, archive, now=NOW)

    def fail_replace(_source, _target):
        raise OSError("injected replace failure")

    check(raises(OSError, lambda: arm.create_run(
        root, [("ATOMIC", "1m")], run_id="addstock-atomic", now=NOW,
        replace_fn=fail_replace)),
        "injected replace failure is surfaced")
    check(not path.exists()
          and not list(root.glob(f".{arm.ACTIVE_NAME}.*.tmp")),
          "failed atomic create leaves no target or temporary file")


def test_archive_retry(base):
    root = fresh(base, "archive-retry")
    archive = root.parent / "logs"
    run = arm.create_run(root, [("RETRY", "1m-pre")],
                         run_id="addstock-archive-retry", now=NOW,
                         earliest_tickers=["RETRY"])
    arm.mark_series_complete(root, run["run_id"], "RETRY", "1m-pre",
                             now=NOW)
    original_archive = arm._archive_complete
    arm._archive_complete = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        OSError("injected archive interruption"))
    try:
        interrupted = raises(OSError, lambda: arm.mark_fetch_finished(
            root, run["run_id"], now=NOW, archive_dir=archive))
    finally:
        arm._archive_complete = original_archive
    complete = arm.load_run(root)
    check(interrupted and complete["state"] == "complete",
          "archive interruption leaves a valid complete active manifest")
    target = archive / f"addstock-run-{run['run_id']}.json"
    archive.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"different")
    check(raises(arm.ManifestError, lambda: arm.archive_complete(
        root, run["run_id"], archive)) and arm.active_path(root).exists(),
        "retry refuses a conflicting archive and preserves active state")
    target.write_bytes(arm._encode(complete))
    result = Path(arm.archive_complete(root, run["run_id"], archive))
    check(result == target and result.is_file()
          and not arm.active_path(root).exists(),
          "retry accepts an identical archive and finishes active removal")


def test_validation_bounds(base):
    root = fresh(base, "bounds")
    check(raises(arm.ManifestError, lambda: arm.create_run(
        root, [], run_id="addstock-empty", now=NOW)),
        "empty selection is rejected")
    check(raises(arm.ManifestError, lambda: arm.create_run(
        root, [("", "1m")], run_id="addstock-bad-ticker", now=NOW)),
        "invalid ticker is rejected")
    check(raises(arm.ManifestError, lambda: arm.create_run(
        root, [("GOOD", "not-an-interval")], run_id="addstock-bad-iv", now=NOW)),
        "invalid interval is rejected")
    check(raises(arm.ManifestError, lambda: arm.create_run(
        root, [("GOOD", "1m")], params={"unknown": True},
        run_id="addstock-bad-param", now=NOW)),
        "unknown parameter is rejected")
    check(raises(arm.ManifestError, lambda: arm.create_run(
        root, [("GOOD", "1m")], params={"ports_at_start": [70000]},
        run_id="addstock-bad-port", now=NOW)),
        "invalid port is rejected")


def main():
    base = Path(tempfile.mkdtemp(prefix="addstock_manifest_selftest_"))
    try:
        test_display_integration()
        test_lifecycle_and_archive(base)
        test_resume_and_seal_order(base)
        test_verification_debt(base)
        test_empty_verification_exemption(base)
        test_display_empty_predicate(base)
        test_seal_debt_blocks_completion(base)
        test_fail_closed_and_discard(base)
        test_archive_retry(base)
        test_validation_bounds(base)
    finally:
        shutil.rmtree(base, ignore_errors=True)
    total = PASS[0] + FAIL[0]
    print(f"addstock_run_manifest_selftest: {PASS[0]}/{total} passed, "
          f"{FAIL[0]} failed")
    return 1 if FAIL[0] else 0


if __name__ == "__main__":
    raise SystemExit(main())
