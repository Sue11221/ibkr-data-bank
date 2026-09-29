"""Run deterministic, test-only gate batteries.

The command line accepts only the named batteries declared in this module.  It
never discovers scripts, invokes a shell, or accepts an arbitrary command/path.
Every child runs under this interpreter with a runner-owned scratch cwd and
TEMP/TMP/TMPDIR, containing accidental relative writes and harness temp leaks.
Child output is continuously drained into a fixed-size tail so a noisy failure
cannot consume unbounded memory or disk; the runner emits exactly one bounded
summary line per suite.

Exit convention:

* 0 -- every suite passed;
* 3 -- no suite failed, but at least one reference-style suite reported that a
  feature is absent/pending;
* 1 -- a suite failed, timed out, contradicted its exit with failed checks, or
  produced inconsistent check counts across repeats;
* 2 -- command/configuration error (the child suite was never launched).

``--repeat N`` repeats the suites explicitly marked thread-heavy; ordinary
suites still run once.  The summary exposes every raw child exit and verifies
that internal check counts stay identical between repetitions.
Timeouts are failure guards, never sequencing.
Suite metadata may raise one suite's default guard; an explicit numeric API
timeout overrides that policy for every suite. No CLI timeout option is exposed.

The curated ``all`` registry remains the fast, port-free default.  The explicit
``corpus`` battery is the complete selftest evidence surface plus every
reference suite already trusted by ``all``; therefore ``all`` is always a
subset of ``corpus``.  It includes ``stock_ibkr_selftest.py``, whose fake-adapter
suite briefly binds a real loopback listener, so callers choose ``corpus`` (or
the focused ``stock-ibkr`` battery) deliberately.  Static inventories register
every ``engine/*_selftest.py`` and ``engine/*_reference.py`` file.  A reference
must be reachable from a battery or carry an explicit exclusion reason (and any
required subcommand); discovery validates those contracts but never selects or
executes an unallowlisted script.
"""

from __future__ import annotations

import argparse
import contextlib
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from types import MappingProxyType
from typing import Callable, Mapping, Sequence, TextIO


TEST_ONLY = True

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TIMEOUT_SECONDS = 300.0
MAX_REPEAT = 20
MAX_TIMEOUT_SECONDS = 86_400.0
TAIL_BYTES = 65_536
DETAIL_CHARS = 800

PASS = 0
FAIL = 1
USAGE = 2
PENDING = 3
TIMEOUT_EXIT = 124
LAUNCH_EXIT = 127

_SAFE_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


class RunnerError(ValueError):
    """A fail-closed configuration error detected before child execution."""


@dataclass(frozen=True)
class SuiteSpec:
    """One statically allowlisted Python suite."""

    name: str
    script: str
    thread_heavy: bool = False
    timeout: float | None = None


@dataclass(frozen=True)
class CheckCounts:
    """A recognized internal check summary from a child suite."""

    passed: int
    total: int
    failed: int

    def token(self) -> str:
        return f"{self.passed}/{self.total}"


@dataclass(frozen=True)
class InvocationResult:
    """One actual child-process attempt."""

    exit_code: int
    counts: CheckCounts | None
    tail: str
    elapsed_ms: int
    problem: str = ""


@dataclass(frozen=True)
class SuiteResult:
    """All repetitions of one suite."""

    spec: SuiteSpec
    invocations: tuple[InvocationResult, ...]
    planned_runs: int
    status: str
    elapsed_ms: int
    problem: str = ""


@dataclass(frozen=True)
class BatteryResult:
    """Aggregate result for a named battery."""

    name: str
    suites: tuple[SuiteResult, ...]
    elapsed_ms: int

    @property
    def exit_code(self) -> int:
        if any(suite.status == "FAIL" for suite in self.suites):
            return FAIL
        if any(suite.status == "PENDING" for suite in self.suites):
            return PENDING
        return PASS


def _suite(stem: str, *, thread_heavy: bool = False,
           timeout: float | None = None) -> SuiteSpec:
    return SuiteSpec(stem, f"engine/{stem}.py", thread_heavy, timeout)


_EXPORT = (
    _suite("export_batch_selftest", thread_heavy=True),
    _suite("export_batch_folder_selftest", thread_heavy=True),
    _suite("export_csv_selftest"),
    _suite("export_cli_selftest"),
    _suite("export_designer_selftest"),
    _suite("export_quality_selftest"),
    _suite("vol_value_audit_selftest"),
    _suite("vol_value_reconcile_selftest"),
    _suite("export_progress_reference", thread_heavy=True),
)
_IBKR = (
    # The core stock_ibkr selftest is deliberately absent because it binds a
    # real loopback port.  These port-free support seams retain the plan's
    # named IBKR battery without weakening the offline surface fence.
    _suite("coverage_selftest"),
    _suite("extended_hours_selftest"),
    _suite("fetch_eta_selftest"),
    _suite("live_repair_gaps_selftest"),
    _suite("ordinary_correction_write_selftest", thread_heavy=True),
    _suite("run_log_selftest"),
    _suite("stock_basis_selftest"),
    _suite("stock_ibkr_manifest_fence_selftest"),
    _suite("stock_ingest_selftest", thread_heavy=True),
    _suite("update_data_fence_reference", thread_heavy=True),
)
_FIXDATA = (
    _suite("fix_data_pipeline_selftest", thread_heavy=True),
    _suite("fixdata_injected_fence_reference", thread_heavy=True),
    _suite("fixdata_kind_selftest"),
    _suite("fixdata_pause_reference", thread_heavy=True),
    _suite("fixdata_validator_fence_reference"),
)
_FLEET = (
    _suite("addstock_lifecycle_fence_reference"),
    _suite("addstock_manifest_fence_reference"),
    _suite("addstock_vol_reconcile_selftest", thread_heavy=True),
    _suite("addstock_watchdog_lease_fence_reference"),
    _suite("addstock_watchdog_signal_fence_reference"),
    _suite("addstock_watchdog_selftest", thread_heavy=True),
    _suite("tws_restart_selftest", thread_heavy=True),
    _suite("tws_inputguard_selftest", thread_heavy=True),
    _suite("midrun_restart_reference", thread_heavy=True),
)
_PORTABILITY = (
    _suite("dependency_manifest_selftest"),
    _suite("portability_reference"),
    _suite("portability_selftest"),
    _suite("midnight_task_installer_selftest"),
)
_VALIDATE = (
    _suite("stock_storage_selftest", thread_heavy=True),
    _suite("stock_validate_selftest", thread_heavy=True),
)
_SESSION = (
    _suite("session_schedule_generator_selftest"),
)
_FETCH_HORIZON = (
    _suite("fetch_send_inventory_selftest"),
    _suite("fetch_horizon_selftest", thread_heavy=True),
    _suite("fetch_pacer_selftest", thread_heavy=True),
    _suite("fetch_ibkr_integration_selftest", thread_heavy=True),
    _suite("fetch_a2_infrastructure_selftest", thread_heavy=True),
    _suite("fetch_a2_http_workflows_selftest", thread_heavy=True),
    _suite("fetch_a2_ibkr_workflows_selftest", thread_heavy=True, timeout=1200.0),
)


def _ordered_union(groups: Sequence[Sequence[SuiteSpec]]) -> tuple[SuiteSpec, ...]:
    seen: set[str] = set()
    result: list[SuiteSpec] = []
    for group in groups:
        for suite in group:
            if suite.name not in seen:
                seen.add(suite.name)
                result.append(suite)
    return tuple(result)


_GROUPS = (
    _IBKR, _EXPORT, _FIXDATA, _FLEET, _FETCH_HORIZON, _PORTABILITY,
    _VALIDATE, _SESSION)

# Static selftest manifest. Keep this alphabetical so review diffs expose
# additions/removals plainly. The combined inventory validator compares it and
# the reference manifest with disk before any built-in battery launches.
CORPUS_SELFTEST_NAMES = (
    "addstock_run_manifest_selftest",
    "addstock_vol_debt_selftest",
    "addstock_vol_reconcile_selftest",
    "addstock_watchdog_selftest",
    "calendar_reconciler_selftest",
    "check_kit_selftest",
    "combined_coverage_selftest",
    "coverage_selftest",
    "deep_seam_scan_selftest",
    "dependency_manifest_selftest",
    "derived_daily_cache_selftest",
    "event_probe_selftest",
    "export_batch_folder_selftest",
    "export_batch_selftest",
    "export_cli_selftest",
    "export_csv_selftest",
    "export_designer_selftest",
    "export_quality_selftest",
    "extended_hours_selftest",
    "external_sweep_selftest",
    "fetch_a2_http_workflows_selftest",
    "fetch_a2_ibkr_workflows_selftest",
    "fetch_a2_infrastructure_selftest",
    "fetch_eta_selftest",
    "fetch_horizon_selftest",
    "fetch_ibkr_integration_selftest",
    "fetch_pacer_selftest",
    "fetch_pickup_selftest",
    "fetch_progress_selftest",
    "fetch_send_inventory_selftest",
    "fix_data_pipeline_selftest",
    "fixdata_kind_selftest",
    "fleet_sizing_selftest",
    "health_selftest",
    "identity_selftest",
    "kind_staleness_selftest",
    "live_combined_flags_selftest",
    "live_repair_gaps_selftest",
    "live_spot_probe_selftest",
    "market_calendar_selftest",
    "midnight_task_installer_selftest",
    "ordinary_correction_write_selftest",
    "phantom_fix_selftest",
    "portability_selftest",
    "run_gates_selftest",
    "run_log_selftest",
    "session_schedule_generator_selftest",
    "sp500_selftest",
    "split_audit_selftest",
    "split_cache_selftest",
    "split_detector_selftest",
    "split_join_enrichment_selftest",
    "stock_basis_selftest",
    "stock_ibkr_manifest_fence_selftest",
    "stock_ibkr_selftest",
    "stock_ingest_selftest",
    "stock_storage_selftest",
    "stock_validate_selftest",
    "testbank_selftest",
    "tier0_mcp_selftest",
    "tier0_queries_selftest",
    "triage_classifier_selftest",
    "tws_api_selftest",
    "tws_inputguard_selftest",
    "tws_restart_selftest",
    "tws_step_verification_selftest",
    "tws_vmoptions_selftest",
    "vol_extended_tickbox_selftest",
    "vol_value_audit_selftest",
    "vol_value_gate_selftest",
    "vol_value_reconcile_selftest",
    "wbd_truncate_selftest",
)

# Reference harnesses are acceptance surfaces, not incidental scripts. Keep
# this complete manifest alphabetical so a new harness cannot land unnoticed.
REFERENCE_SUITE_NAMES = (
    "addstock_empty_verify_reference",
    "addstock_lifecycle_fence_reference",
    "addstock_manifest_fence_reference",
    "addstock_run_protection_reference",
    "addstock_vol_debt_reference",
    "addstock_watchdog_lease_fence_reference",
    "addstock_watchdog_signal_fence_reference",
    "calendar_reconciler_reference",
    "coverage_audit_reference",
    "date_split_reference",
    "empty_month_absence_reference",
    "export_health_bundle_reference",
    "export_progress_reference",
    "export_sample_reference",
    "export_source_line_reference",
    "external_sweep_reference",
    "fetch_pickup_reference",
    "fetch_progress_reference",
    "fixdata_health_relocation_reference",
    "fixdata_injected_fence_reference",
    "fixdata_pause_reference",
    "fixdata_port_death_reference",
    "fixdata_port_recovery_reference",
    "fixdata_validator_fence_reference",
    "ftnt_seam_reference",
    "gap_ignore_removal_reference",
    "headless_launcher_reference",
    "identity_reference",
    "kind_earliest_reference",
    "kind_gap_reference",
    "kind_staleness_reference",
    "lineage_pin_reference",
    "listing_floor_reference",
    "live_spot_daylevel_reference",
    "live_spot_probe_reference",
    "midrun_restart_reference",
    "ohlc_kind_coverage_reference",
    "phantom_fix_reference",
    "portability_reference",
    "probe_wait_status_reference",
    "provider_backoff_label_reference",
    "run_log_retention_reference",
    "served_earliest_reference",
    "split_detector_reference",
    "split_provider_reference",
    "split_verify_reference",
    "startup_port_selection_reference",
    "triage_classifier_reference",
    "tws_discovery_reference",
    "update_data_fence_reference",
    "vol_extended_tickbox_reference",
    "vol_value_reference",
    "wbd_truncate_reference",
    "xval_x0_reference",
)

# No-argument, offline references whose implemented contracts belong in the
# complete corpus. Domain batteries may also carry the same SuiteSpec.
CORPUS_REFERENCE_NAMES = (
    "addstock_lifecycle_fence_reference",
    "addstock_manifest_fence_reference",
    "addstock_watchdog_lease_fence_reference",
    "addstock_watchdog_signal_fence_reference",
    "empty_month_absence_reference",
    "export_health_bundle_reference",
    "export_progress_reference",
    "fixdata_health_relocation_reference",
    "fixdata_injected_fence_reference",
    "fixdata_pause_reference",
    "fixdata_port_death_reference",
    "fixdata_port_recovery_reference",
    "fixdata_validator_fence_reference",
    "gap_ignore_removal_reference",
    "headless_launcher_reference",
    "midrun_restart_reference",
    "portability_reference",
    "run_log_retention_reference",
    "update_data_fence_reference",
)

# These are invocation contracts, not extra argv accepted from a caller. They
# explain why the two harnesses cannot join the fixed no-argument corpus until
# SuiteSpec grows a separately reviewed static-argv model.
REFERENCE_ARGUMENT_REQUIREMENTS: Mapping[str, tuple[str, ...]] = MappingProxyType({
    "identity_reference": ("synthetic", "probe"),
    "kind_gap_reference": ("synthetic", "probe"),
})

_STANDALONE_REFERENCE = (
    "registered standalone acceptance harness; not in the curated "
    "no-argument corpus"
)
EXCLUDED_REFERENCE_SUITES: Mapping[str, str] = MappingProxyType({
    "addstock_empty_verify_reference": _STANDALONE_REFERENCE,
    "addstock_run_protection_reference": _STANDALONE_REFERENCE,
    "addstock_vol_debt_reference": _STANDALONE_REFERENCE,
    "calendar_reconciler_reference": _STANDALONE_REFERENCE,
    "coverage_audit_reference": _STANDALONE_REFERENCE,
    "date_split_reference": _STANDALONE_REFERENCE,
    "export_sample_reference": _STANDALONE_REFERENCE,
    "export_source_line_reference": _STANDALONE_REFERENCE,
    "external_sweep_reference": _STANDALONE_REFERENCE,
    "fetch_pickup_reference": _STANDALONE_REFERENCE,
    "fetch_progress_reference": _STANDALONE_REFERENCE,
    "ftnt_seam_reference": _STANDALONE_REFERENCE,
    "identity_reference": (
        "requires one explicit subcommand: synthetic or probe"),
    "kind_earliest_reference": _STANDALONE_REFERENCE,
    "kind_gap_reference": (
        "requires one explicit subcommand: synthetic or probe"),
    "kind_staleness_reference": _STANDALONE_REFERENCE,
    "lineage_pin_reference": _STANDALONE_REFERENCE,
    "listing_floor_reference": _STANDALONE_REFERENCE,
    "live_spot_daylevel_reference": _STANDALONE_REFERENCE,
    "live_spot_probe_reference": _STANDALONE_REFERENCE,
    "ohlc_kind_coverage_reference": _STANDALONE_REFERENCE,
    "phantom_fix_reference": _STANDALONE_REFERENCE,
    "probe_wait_status_reference": _STANDALONE_REFERENCE,
    "provider_backoff_label_reference": (
        "registered standalone acceptance harness for Row 81; like "
        "startup_port_selection_reference its contract defines a non-zero "
        "baseline exit (3 = feature absent, invariants intact) that no "
        "battery may read as failure"),
    "served_earliest_reference": _STANDALONE_REFERENCE,
    "split_detector_reference": _STANDALONE_REFERENCE,
    "split_provider_reference": _STANDALONE_REFERENCE,
    "split_verify_reference": (
        "known inverted expectation; Row 61 must repair or retire it, never "
        "silence it"),
    "startup_port_selection_reference": (
        "registered standalone acceptance harness; its contract also defines a "
        "non-zero baseline exit (3 = policy feature absent), which no battery "
        "may read as failure"),
    "triage_classifier_reference": _STANDALONE_REFERENCE,
    "tws_discovery_reference": _STANDALONE_REFERENCE,
    "vol_extended_tickbox_reference": _STANDALONE_REFERENCE,
    "vol_value_reference": _STANDALONE_REFERENCE,
    "wbd_truncate_reference": _STANDALONE_REFERENCE,
    "xval_x0_reference": _STANDALONE_REFERENCE,
})

_CORPUS_THREAD_HEAVY = frozenset(
    suite.name
    for suite in _ordered_union(_GROUPS)
    if suite.thread_heavy and suite.name in CORPUS_SELFTEST_NAMES
) | {"stock_ibkr_selftest"}
_GROUP_SELFTEST_SPECS = {
    suite.name: suite
    for suite in _ordered_union(_GROUPS)
    if suite.name in CORPUS_SELFTEST_NAMES
}
_CORPUS_SELFTESTS = tuple(
    _GROUP_SELFTEST_SPECS.get(name,
        _suite(name, thread_heavy=name in _CORPUS_THREAD_HEAVY))
    for name in CORPUS_SELFTEST_NAMES
)
_GROUP_REFERENCE_SPECS = {
    suite.name: suite
    for suite in _ordered_union(_GROUPS)
    if suite.name in REFERENCE_SUITE_NAMES
}
_CURATED_REFERENCES = tuple(
    _GROUP_REFERENCE_SPECS.get(name, _suite(name))
    for name in CORPUS_REFERENCE_NAMES
)
_CORPUS = _ordered_union((_CORPUS_SELFTESTS, _CURATED_REFERENCES))
_STOCK_IBKR_CORE = tuple(
    suite for suite in _CORPUS_SELFTESTS
    if suite.name == "stock_ibkr_selftest")

BATTERIES: Mapping[str, tuple[SuiteSpec, ...]] = MappingProxyType({
    "ibkr": _IBKR,
    "stock-ibkr": _STOCK_IBKR_CORE,
    "export": _EXPORT,
    "fixdata": _FIXDATA,
    "fleet": _FLEET,
    "fetch-horizon": _FETCH_HORIZON,
    "portability": _PORTABILITY,
    "session-schedule": _SESSION,
    "validate": _VALIDATE,
    "all": _ordered_union(_GROUPS),
    "corpus": _CORPUS,
})

EXCLUDED_DEFAULT_SUITES: Mapping[str, str] = MappingProxyType({
    "stock_ibkr_selftest": (
        "explicit stock-ibkr/corpus only: binds a real loopback TCP listener"),
})


def _validate_builtin_suite_inventory(project_root: Path) -> None:
    """Fail before launch when either suite inventory or classification drifts."""

    expected_selftests = set(CORPUS_SELFTEST_NAMES)
    expected_references = set(REFERENCE_SUITE_NAMES)
    if len(expected_selftests) != len(CORPUS_SELFTEST_NAMES):
        raise RunnerError("static corpus repeats a selftest name")
    if len(expected_references) != len(REFERENCE_SUITE_NAMES):
        raise RunnerError("static inventory repeats a reference name")
    try:
        engine_root = (Path(project_root).resolve(strict=True) / "engine")
        engine_root = engine_root.resolve(strict=True)
        actual_selftests = {
            path.stem
            for path in engine_root.iterdir()
            if path.name.endswith("_selftest.py")
        }
        actual_references = {
            path.stem
            for path in engine_root.iterdir()
            if path.name.endswith("_reference.py")
        }
    except (OSError, RuntimeError) as exc:
        raise RunnerError(f"suite inventory is unavailable: {exc}") from exc

    unallowlisted_selftests = sorted(actual_selftests - expected_selftests)
    absent_selftests = sorted(expected_selftests - actual_selftests)
    unallowlisted_references = sorted(actual_references - expected_references)
    absent_references = sorted(expected_references - actual_references)
    if (unallowlisted_selftests or absent_selftests
            or unallowlisted_references or absent_references):
        details = []
        if unallowlisted_selftests:
            details.append(
                "unallowlisted-selftest="
                + ",".join(unallowlisted_selftests))
        if absent_selftests:
            details.append(
                "declared-selftest-absent=" + ",".join(absent_selftests))
        if unallowlisted_references:
            details.append(
                "unallowlisted-reference="
                + ",".join(unallowlisted_references))
        if absent_references:
            details.append(
                "declared-reference-absent=" + ",".join(absent_references))
        raise RunnerError("suite inventory drift: " + "; ".join(details))

    reachable_references = {
        suite.name
        for battery in BATTERIES.values()
        for suite in battery
        if suite.name.endswith("_reference")
    }
    excluded_references = set(EXCLUDED_REFERENCE_SUITES)
    unclassified = sorted(
        expected_references - reachable_references - excluded_references)
    stale_exclusions = sorted(excluded_references - expected_references)
    contradictory = sorted(reachable_references & excluded_references)
    empty_reasons = sorted(
        name for name, reason in EXCLUDED_REFERENCE_SUITES.items()
        if not isinstance(reason, str) or not reason.strip())
    if unclassified or stale_exclusions or contradictory or empty_reasons:
        details = []
        if unclassified:
            details.append("unclassified=" + ",".join(unclassified))
        if stale_exclusions:
            details.append("stale-exclusion=" + ",".join(stale_exclusions))
        if contradictory:
            details.append("reachable-and-excluded=" + ",".join(contradictory))
        if empty_reasons:
            details.append("empty-reason=" + ",".join(empty_reasons))
        raise RunnerError(
            "reference classification drift: " + "; ".join(details))

    argument_references = set(REFERENCE_ARGUMENT_REQUIREMENTS)
    malformed_arguments = sorted(
        name for name, choices in REFERENCE_ARGUMENT_REQUIREMENTS.items()
        if (not isinstance(choices, tuple) or not choices
            or any(not isinstance(choice, str) or not choice
                   for choice in choices)))
    if (not argument_references <= expected_references
            or not argument_references <= excluded_references
            or malformed_arguments):
        raise RunnerError("reference argument contract drift")


_COUNT_PATTERNS = (
    re.compile(
        r"(?P<passed>\d+)\s*/\s*(?P<total>\d+)\s+"
        r"(?:checks?\s+)?passed(?:\s*,\s*(?P<failed>\d+)\s+failed)?",
        re.IGNORECASE,
    ),
    re.compile(
        r"ALL\s+PASS\s*\(\s*(?P<passed>\d+)\s*/\s*(?P<total>\d+)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<total>\d+)\s+checks?\s*,\s*(?P<failed>\d+)\s+failed",
        re.IGNORECASE,
    ),
    re.compile(
        r"FAILED\s*:\s*(?P<failed>\d+)\s*/\s*(?P<total>\d+)\s+checks?",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?<![\d/])(?P<passed>\d+)\s+passed\s*,\s*"
        r"(?P<failed>\d+)\s+failed",
        re.IGNORECASE,
    ),
)


def parse_check_counts(output: str) -> CheckCounts | None:
    """Return the last recognized, internally consistent child count line."""

    if not isinstance(output, str):
        raise TypeError("output must be a string")
    for line in reversed(output.splitlines()):
        for pattern in _COUNT_PATTERNS:
            match = pattern.search(line)
            if match is None:
                continue
            values = match.groupdict()
            total = int(values["total"]) if values.get("total") else None
            passed = int(values["passed"]) if values.get("passed") else None
            failed = int(values["failed"]) if values.get("failed") else None
            if total is None:
                if passed is None or failed is None:
                    continue
                total = passed + failed
            elif passed is None:
                if failed is None or failed > total:
                    continue
                passed = total - failed
            elif failed is None:
                if passed > total:
                    continue
                failed = total - passed
            if passed < 0 or failed < 0 or passed + failed != total:
                continue
            return CheckCounts(passed, total, failed)
    ok = sum(line.startswith("  ok    ") for line in output.splitlines())
    failed = sum(line.startswith("  FAIL  ") for line in output.splitlines())
    if ok or failed:
        return CheckCounts(ok, ok + failed, failed)
    return None


def _validate_repeat(repeat: int) -> int:
    if type(repeat) is not int or not 1 <= repeat <= MAX_REPEAT:
        raise RunnerError(f"repeat must be an integer from 1 through {MAX_REPEAT}")
    return repeat


def _validate_timeout(timeout: float) -> float:
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise RunnerError("timeout must be a number")
    value = float(timeout)
    if not 0 < value <= MAX_TIMEOUT_SECONDS:
        raise RunnerError(
            f"timeout must be greater than 0 and at most {MAX_TIMEOUT_SECONDS:g}")
    return value


def _validate_name(value: str, label: str) -> str:
    if not isinstance(value, str) or _SAFE_NAME.fullmatch(value) is None:
        raise RunnerError(f"invalid {label}: {value!r}")
    return value


def _resolve_suite(project_root: Path, spec: SuiteSpec) -> Path:
    if not isinstance(spec, SuiteSpec):
        raise RunnerError("battery entries must be SuiteSpec instances")
    _validate_name(spec.name, "suite name")
    if type(spec.thread_heavy) is not bool:
        raise RunnerError(f"suite {spec.name} thread_heavy must be boolean")
    if spec.timeout is not None:
        _validate_timeout(spec.timeout)
    if not isinstance(spec.script, str):
        raise RunnerError(f"suite {spec.name} script must be a string")
    raw = Path(spec.script)
    allowed_suffix = raw.stem.endswith(("_selftest", "_reference"))
    if (raw.is_absolute() or raw.parts[:1] != ("engine",)
            or len(raw.parts) != 2 or raw.suffix != ".py"
            or not allowed_suffix):
        raise RunnerError(
            f"suite {spec.name} must name one non-recursive engine selftest/reference")
    try:
        root = Path(project_root).resolve(strict=True)
        engine_root = (root / "engine").resolve(strict=True)
        lexical = root / raw
        if lexical.is_symlink():
            raise RunnerError(f"suite {spec.name} script must not be a symlink")
        script = lexical.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RunnerError(f"suite {spec.name} script is unavailable: {exc}") from exc
    if not script.is_file() or script.parent != engine_root:
        raise RunnerError(f"suite {spec.name} resolves outside the engine directory")
    return script


def _preflight(
    battery_name: str,
    batteries: Mapping[str, Sequence[SuiteSpec]],
    project_root: Path,
) -> tuple[tuple[SuiteSpec, Path], ...]:
    _validate_name(battery_name, "battery name")
    if not isinstance(batteries, Mapping):
        raise RunnerError("batteries must be a mapping")
    if batteries is BATTERIES:
        _validate_builtin_suite_inventory(project_root)
    if battery_name not in batteries:
        available = ", ".join(sorted(str(name) for name in batteries))
        raise RunnerError(
            f"unknown battery {battery_name!r}; choose one of: {available}")
    specs = batteries[battery_name]
    if (isinstance(specs, (str, bytes))
            or not isinstance(specs, Sequence) or not specs):
        raise RunnerError(f"battery {battery_name} must contain at least one suite")
    prepared: list[tuple[SuiteSpec, Path]] = []
    names: set[str] = set()
    paths: set[Path] = set()
    for spec in specs:
        path = _resolve_suite(project_root, spec)
        if spec.name in names:
            raise RunnerError(f"battery {battery_name} repeats suite {spec.name}")
        if path in paths:
            raise RunnerError(
                f"battery {battery_name} aliases one script under multiple names")
        names.add(spec.name)
        paths.add(path)
        prepared.append((spec, path))
    return tuple(prepared)


def _decode_tail(data: bytes, *, truncated: bool) -> str:
    if truncated:
        preceding, data = data[:1], data[1:]
        if preceding != b"\n":
            newline = data.find(b"\n")
            data = b"" if newline < 0 else data[newline + 1:]
    return data.decode("utf-8", errors="replace")


def _terminate_process_tree(process) -> str:
    """Best-effort bounded termination of a suite and its descendants."""

    errors: list[str] = []
    pid = getattr(process, "pid", None)
    if pid is not None:
        if os.name == "nt":
            try:
                ended = subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=10,
                )
                if ended.returncode != 0 and process.poll() is None:
                    errors.append(f"taskkill returned {ended.returncode}")
            except (OSError, subprocess.TimeoutExpired) as exc:
                errors.append(
                    f"tree kill failed: {type(exc).__name__}: {exc}")
        else:
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError as exc:
                errors.append(
                    f"process-group kill failed: {type(exc).__name__}: {exc}")
    try:
        running = process.poll() is None
    except OSError as exc:
        running = True
        errors.append(f"process poll failed: {type(exc).__name__}: {exc}")
    if running:
        try:
            process.kill()
        except OSError as exc:
            errors.append(f"direct kill failed: {type(exc).__name__}: {exc}")
    try:
        process.wait(timeout=10)
    except (OSError, subprocess.TimeoutExpired) as exc:
        errors.append(f"child wait failed: {type(exc).__name__}: {exc}")
    return "; ".join(errors)


def _execute_suite(
    script: Path,
    project_root: Path,
    timeout: float,
) -> InvocationResult:
    started = time.monotonic()
    problem = ""
    exit_code = LAUNCH_EXIT
    tail = ""
    try:
        with tempfile.TemporaryDirectory(
                prefix=f"run-gates-{script.stem}-") as scratch_name:
            scratch = Path(scratch_name)
            environment = os.environ.copy()
            environment.update({
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONIOENCODING": "utf-8",
                "TEMP": str(scratch),
                "TMP": str(scratch),
                "TMPDIR": str(scratch),
            })
            process = None
            reader = None
            force_tree_cleanup = False
            output_tail = bytearray()
            output_size = [0]
            reader_errors: list[BaseException] = []
            try:
                popen_options = (
                    {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
                    if os.name == "nt" else {"start_new_session": True})
                process = subprocess.Popen(
                    [sys.executable, "-B", str(script)],
                    cwd=scratch,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    bufsize=0,
                    **popen_options,
                )
                if process.stdout is None:  # defensive: PIPE must create it
                    raise OSError("child stdout pipe was not created")

                def drain_output():
                    try:
                        while True:
                            chunk = process.stdout.read(8192)
                            if not chunk:
                                break
                            output_size[0] += len(chunk)
                            output_tail.extend(chunk)
                            # Keep one byte before the public tail.  It tells
                            # _decode_tail whether the retained bytes begin at
                            # a real line boundary or inside a forged fragment.
                            excess = len(output_tail) - (TAIL_BYTES + 1)
                            if excess > 0:
                                del output_tail[:excess]
                    except BaseException as exc:  # surfaced as gate failure
                        reader_errors.append(exc)

                reader = threading.Thread(
                    target=drain_output,
                    name=f"run-gates-output-{script.stem}",
                    daemon=True,
                )
                reader.start()
                try:
                    exit_code = int(process.wait(timeout=timeout))
                except subprocess.TimeoutExpired:
                    exit_code = TIMEOUT_EXIT
                    problem = f"timed out after {timeout:g}s"
                    force_tree_cleanup = True
            except OSError as exc:
                exit_code = LAUNCH_EXIT
                problem = f"launch failed: {type(exc).__name__}: {exc}"
                force_tree_cleanup = process is not None
            finally:
                if process is not None:
                    cleanup_error = ""
                    try:
                        running = process.poll() is None
                    except OSError as exc:
                        running = True
                        cleanup_error = (
                            f"process poll failed: {type(exc).__name__}: {exc}")
                    if force_tree_cleanup or running:
                        extra = _terminate_process_tree(process)
                        if extra:
                            cleanup_error = (
                                f"{cleanup_error}; {extra}"
                                if cleanup_error else extra)
                    if cleanup_error:
                        exit_code = LAUNCH_EXIT
                        problem = (
                            f"{problem}; {cleanup_error}"
                            if problem else cleanup_error)
                if reader is not None:
                    try:
                        reader.join(timeout=10)
                    except RuntimeError as exc:
                        exit_code = LAUNCH_EXIT
                        problem = (f"{problem}; " if problem else "") + (
                            f"output reader join failed: {exc}")
                    if reader.is_alive():
                        exit_code = LAUNCH_EXIT
                        problem = (f"{problem}; " if problem else "") + (
                            "output reader did not terminate")
                if process is not None and process.stdout is not None:
                    with contextlib.suppress(OSError):
                        process.stdout.close()
            if reader_errors:
                exit_code = LAUNCH_EXIT
                exc = reader_errors[0]
                reader_problem = (
                    f"output reader failed: {type(exc).__name__}: {exc}")
                problem = f"{problem}; {reader_problem}" if problem else reader_problem
            tail = _decode_tail(
                bytes(output_tail), truncated=output_size[0] > TAIL_BYTES)
    except OSError as exc:
        exit_code = LAUNCH_EXIT
        cleanup_problem = f"scratch setup/cleanup failed: {type(exc).__name__}: {exc}"
        problem = f"{problem}; {cleanup_problem}" if problem else cleanup_problem
    elapsed_ms = max(0, round((time.monotonic() - started) * 1000))
    return InvocationResult(
        exit_code=exit_code,
        counts=parse_check_counts(tail),
        tail=tail,
        elapsed_ms=elapsed_ms,
        problem=problem,
    )


def _single_line(value: str, limit: int = DETAIL_CHARS) -> str:
    text = " | ".join(str(value).splitlines()).strip()
    text = "".join(char if char >= " " else "?" for char in text)
    if len(text) > limit:
        text = text[:limit - 3] + "..."
    return text


def _classify_suite(
    spec: SuiteSpec,
    invocations: Sequence[InvocationResult],
    planned_runs: int,
) -> tuple[str, str]:
    problems: list[str] = []
    if len(invocations) < planned_runs:
        problems.append(
            f"aborted {planned_runs - len(invocations)} remaining repeats "
            "after terminal timeout/launch failure")
    for index, result in enumerate(invocations, 1):
        if result.exit_code not in (PASS, PENDING):
            problems.append(f"run {index} exited {result.exit_code}")
        if result.counts is not None and result.counts.failed:
            problems.append(
                f"run {index} reported {result.counts.failed} failed checks")
        if result.counts is not None and result.counts.total == 0:
            problems.append(f"run {index} reported zero checks")
        if result.counts is None:
            problems.append(f"run {index} emitted no recognizable check counts")
        if result.problem:
            problems.append(f"run {index} {result.problem}")

    count_values = [result.counts for result in invocations]
    known = [value for value in count_values if value is not None]
    if known and len(known) != len(count_values):
        problems.append("recognized check counts disappeared between repeats")
    elif len(set(known)) > 1:
        problems.append("recognized check counts changed between repeats")

    if problems:
        failing_tail = next(
            (result.tail for result in invocations
             if result.exit_code not in (PASS, PENDING)
             or result.counts is None
             or (result.counts is not None and result.counts.failed)),
            "",
        )
        detail = "; ".join(problems)
        if failing_tail.strip():
            detail += "; tail: " + failing_tail
        return "FAIL", _single_line(detail)
    if any(result.exit_code == PENDING for result in invocations):
        return "PENDING", ""
    return "PASS", ""


def _checks_token(invocations: Sequence[InvocationResult]) -> str:
    tokens = [
        result.counts.token() if result.counts is not None else "unknown"
        for result in invocations
    ]
    if len(set(tokens)) == 1 and len(tokens) > 1:
        return f"{tokens[0]}x{len(tokens)}"
    return ",".join(tokens)


def _suite_line(result: SuiteResult) -> str:
    exits = ",".join(str(item.exit_code) for item in result.invocations)
    passed = sum(
        item.exit_code == PASS
        and item.counts is not None and item.counts.total > 0
        and item.counts.failed == 0
        for item in result.invocations)
    pending = sum(
        item.exit_code == PENDING
        and item.counts is not None and item.counts.total > 0
        and item.counts.failed == 0
        for item in result.invocations)
    failed = len(result.invocations) - passed - pending
    line = (
        f"GATE name={result.spec.name} status={result.status} "
        f"runs={len(result.invocations)}/{result.planned_runs} "
        f"passed={passed} pending={pending} "
        f"failed={failed} checks={_checks_token(result.invocations)} "
        f"exit={exits} elapsed_ms={result.elapsed_ms}"
    )
    if result.problem:
        line += " detail=" + json.dumps(result.problem, ensure_ascii=True)
    return line


def run_battery(
    battery_name: str,
    *,
    repeat: int = 1,
    batteries: Mapping[str, Sequence[SuiteSpec]] = BATTERIES,
    project_root: Path = PROJECT_ROOT,
    timeout: float | None = None,
    stream: TextIO | None = None,
    executor: Callable[[Path, Path, float], InvocationResult] = _execute_suite,
) -> BatteryResult:
    """Run a battery; None selects suite policy, a number overrides every guard."""

    repeat = _validate_repeat(repeat)
    if timeout is not None:
        timeout = _validate_timeout(timeout)
    prepared = _preflight(battery_name, batteries, Path(project_root))
    if stream is None:
        stream = sys.stdout
    started = time.monotonic()
    results: list[SuiteResult] = []
    for spec, script in prepared:
        suite_timeout = (timeout if timeout is not None else
                         spec.timeout if spec.timeout is not None else
                         DEFAULT_TIMEOUT_SECONDS)
        planned_runs = repeat if spec.thread_heavy else 1
        attempts: list[InvocationResult] = []
        for _index in range(planned_runs):
            attempt_started = time.monotonic()
            try:
                item = executor(
                    script, Path(project_root).resolve(), suite_timeout)
                if not isinstance(item, InvocationResult):
                    raise TypeError("executor returned a non-InvocationResult")
            except Exception as exc:  # fail this attempt; preserve later gates
                item = InvocationResult(
                    exit_code=LAUNCH_EXIT,
                    counts=None,
                    tail="",
                    elapsed_ms=max(
                        0, round((time.monotonic() - attempt_started) * 1000)),
                    problem=(
                        f"executor failed: {type(exc).__name__}: {exc}"),
                )
            attempts.append(item)
            if item.exit_code in {TIMEOUT_EXIT, LAUNCH_EXIT}:
                break
        invocations = tuple(attempts)
        status, problem = _classify_suite(spec, invocations, planned_runs)
        suite = SuiteResult(
            spec=spec,
            invocations=invocations,
            planned_runs=planned_runs,
            status=status,
            elapsed_ms=sum(item.elapsed_ms for item in invocations),
            problem=problem,
        )
        results.append(suite)
        stream.write(_suite_line(suite) + "\n")
        stream.flush()
    return BatteryResult(
        name=battery_name,
        suites=tuple(results),
        elapsed_ms=max(0, round((time.monotonic() - started) * 1000)),
    )


def _repeat_argument(value: str) -> int:
    try:
        parsed = int(value, 10)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("repeat must be an integer") from exc
    try:
        return _validate_repeat(parsed)
    except RunnerError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("battery", nargs="?", help="named offline battery")
    parser.add_argument(
        "--repeat", type=_repeat_argument, default=1,
        help=f"repeat thread-heavy suites (1-{MAX_REPEAT}; default: 1)",
    )
    parser.add_argument(
        "--list", action="store_true", help="list allowlisted batteries and exit",
    )
    return parser


def _error(stream: TextIO, message: str) -> int:
    stream.write("RUNNER ERROR " + _single_line(message) + "\n")
    stream.flush()
    return USAGE


def main(
    argv: Sequence[str] | None = None,
    *,
    batteries: Mapping[str, Sequence[SuiteSpec]] = BATTERIES,
    project_root: Path = PROJECT_ROOT,
    timeout: float | None = None,
    stream: TextIO | None = None,
    error_stream: TextIO | None = None,
    executor: Callable[[Path, Path, float], InvocationResult] = _execute_suite,
) -> int:
    """CLI entry point.

    Completed validation/execution returns a code for deterministic selftests;
    argparse syntax/type errors retain its standard ``SystemExit(2)`` behavior.
    """

    if stream is None:
        stream = sys.stdout
    if error_stream is None:
        error_stream = sys.stderr
    args = _parser().parse_args(argv)
    if args.list:
        if args.battery is not None:
            return _error(error_stream, "--list does not accept a battery")
        if args.repeat != 1:
            return _error(error_stream, "--list does not accept --repeat")
        try:
            for name in sorted(batteries):
                prepared = _preflight(name, batteries, Path(project_root))
                suites = ",".join(
                    spec.name + ("*" if spec.thread_heavy else "")
                    for spec, _path in prepared)
                scope = (
                    " scope=all-selftests+curated-references"
                    if name == "corpus" and batteries is BATTERIES else "")
                stream.write(
                    f"BATTERY name={name}{scope} suites={suites}\n")
            stream.flush()
            return PASS
        except RunnerError as exc:
            return _error(error_stream, str(exc))
    if args.battery is None:
        return _error(error_stream, "a battery name is required (or use --list)")
    try:
        result = run_battery(
            args.battery,
            repeat=args.repeat,
            batteries=batteries,
            project_root=Path(project_root),
            timeout=timeout,
            stream=stream,
            executor=executor,
        )
    except RunnerError as exc:
        return _error(error_stream, str(exc))
    passed = sum(suite.status == "PASS" for suite in result.suites)
    pending = sum(suite.status == "PENDING" for suite in result.suites)
    failed = sum(suite.status == "FAIL" for suite in result.suites)
    stream.write(
        f"BATTERY name={result.name} suites={len(result.suites)} "
        f"passed={passed} pending={pending} failed={failed} "
        f"exit={result.exit_code} elapsed_ms={result.elapsed_ms}\n")
    stream.flush()
    return result.exit_code


__all__ = [
    "BATTERIES",
    "BatteryResult",
    "CheckCounts",
    "CORPUS_REFERENCE_NAMES",
    "CORPUS_SELFTEST_NAMES",
    "EXCLUDED_DEFAULT_SUITES",
    "EXCLUDED_REFERENCE_SUITES",
    "InvocationResult",
    "REFERENCE_ARGUMENT_REQUIREMENTS",
    "REFERENCE_SUITE_NAMES",
    "RunnerError",
    "SuiteResult",
    "SuiteSpec",
    "TEST_ONLY",
    "main",
    "parse_check_counts",
    "run_battery",
]


if __name__ == "__main__":
    raise SystemExit(main())
