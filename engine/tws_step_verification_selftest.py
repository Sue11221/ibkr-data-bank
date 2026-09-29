"""Offline contract tests for the Start Multi Port step-verification mode."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent))

import tws_bringup as tb
import tws_launch as tl


PASS = 0
FAIL = 0


def check(ok, message):
    global PASS, FAIL
    if ok:
        PASS += 1
    else:
        FAIL += 1
        print("FAIL:", message)


class _Saved:
    def __init__(self, obj, **changes):
        self.obj = obj
        self.changes = changes
        self.old = {}

    def __enter__(self):
        for name, value in self.changes.items():
            self.old[name] = getattr(self.obj, name)
            setattr(self.obj, name, value)
        return self

    def __exit__(self, *_args):
        for name, value in self.old.items():
            setattr(self.obj, name, value)


class _NoopResourceWarningSweeper:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def record_result(self, *_args, **_kwargs):
        return None


class _Clock:
    now = 0.0

    @classmethod
    def monotonic(cls):
        cls.now += 0.01
        return cls.now

    @staticmethod
    def sleep(_seconds):
        return None


def _fixed_clock():
    return datetime(2026, 7, 29, 12, 0, 0, tzinfo=timezone.utc)


def _add_success(recorder, port, attempt, step, *, menu_path=None,
                 account_match=None):
    path = recorder.step_screenshot_path(
        port=port, attempt=attempt, step=step)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(f"{port}:{step}".encode("ascii"))
    recorder.add_step(
        port=port, attempt=attempt, step=step, outcome="succeeded",
        screenshot=path, menu_path=menu_path, account_match=account_match)


def test_complete_matrix_and_inventory():
    root = Path(tempfile.mkdtemp(prefix="tws_step_matrix_"))
    try:
        rec = tb.FleetTranscript(
            "fleet_bringup", root=root, run_token="proof123",
            clock=_fixed_clock, step_verification=True,
            expected_ports=[2000, 3000])
        for port, menu in ((2000, "File"), (3000, "Edit")):
            for step in tb.REQUIRED_STEPS:
                _add_success(
                    rec, port, 1, step,
                    menu_path=menu if step == "config_open" else None,
                    account_match=(
                        True if step in {"login_account", "handshake"}
                        else None))
        transcript = rec.write({"succeeded": 2, "failed": 0})
        payload = json.loads(transcript.read_text(encoding="utf-8"))
        inventory = json.loads(
            (transcript.parent / "evidence_inventory.json").read_text(
                encoding="utf-8"))
        check(payload["schema_version"] == 3
              and payload["verification_complete"] is True,
              "complete 2x7 proof matrix is accepted")
        check(len(payload["steps"]) == 14
              and all(len(row["screenshot"]["sha256"]) == 64
                      for row in payload["steps"]),
              "every successful step carries bytes and SHA-256 evidence")
        check(payload["menu_paths"] == [
                  {"port": 2000, "menu_path": "File"},
                  {"port": 3000, "menu_path": "Edit"}],
              "menu-path table preserves the winning path per instance")
        check(len(inventory["files"]) == 15
              and any(row["path"] == "transcript.json"
                      for row in inventory["files"]),
              "evidence inventory covers the transcript and all screenshots")
        first_shot = transcript.parent / payload["steps"][0]["screenshot"]["path"]
        first_shot.write_bytes(b"changed after capture")
        check(rec.verification_complete() is False,
              "tampered step evidence revokes completion")
        first_shot.unlink()
        check(rec.verification_complete() is False,
              "missing step evidence revokes completion")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_halt_fills_every_unattempted_cell():
    root = Path(tempfile.mkdtemp(prefix="tws_step_halt_"))
    try:
        rec = tb.FleetTranscript(
            "fleet_bringup", root=root, run_token="halt1234",
            clock=_fixed_clock, step_verification=True,
            expected_ports=[2000, 3000])
        rec.add_step(
            port=2000, attempt=1, step="launch", outcome="failed")
        payload = json.loads(rec.write().read_text(encoding="utf-8"))
        check(len(payload["steps"]) == 14
              and sum(row["outcome"] == "failed"
                      for row in payload["steps"]) == 1
              and sum(row["outcome"] == "not_attempted"
                      for row in payload["steps"]) == 13,
              "a halted proof still publishes one row for every port/step")
        check(payload["verification_complete"] is False,
              "failed or unattempted cells fail the proof closed")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_focus_failure_keeps_holder_in_step_evidence():
    root = Path(tempfile.mkdtemp(prefix="tws_focus_evidence_"))
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=root, run_token="focus123",
        clock=_fixed_clock, step_verification=True, expected_ports=[2000])
    foreground_calls = []

    def screenshot(path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"actual foreground holder")

    failure = tl.TwsLaunchError(
        "target lost foreground", code="focus_lost", phase="config_open")
    failure.foreground_holder = (
        "hwnd=99 title=DU004 Interactive Brokers — Data Viewer")
    previous = sys.modules.get("pyautogui")
    sys.modules["pyautogui"] = SimpleNamespace(screenshot=screenshot)
    try:
        with _Saved(
                tl, foreground=lambda hwnd: foreground_calls.append(hwnd),
                time=SimpleNamespace(sleep=lambda _seconds: None)):
            tl._capture_failed_step(
                recorder, port=2000, attempt=1, exc=failure, hwnd=11)
        row = recorder.steps[0]
        check(foreground_calls == [],
              "focus-loss evidence does not raise the target before capture")
        check(row["outcome"] == "failed"
              and row["failure_code"] == "focus_lost"
              and row["foreground_holder"]
              == "hwnd=99 title=DU_REDACTED Interactive Brokers — Data Viewer",
              "failed step preserves a typed redacted foreground holder")
        check(tb.evidence_matches(recorder.run_dir, row["screenshot"]),
              "focus-loss failed-step screenshot is hashed and re-verifiable")
    finally:
        if previous is None:
            del sys.modules["pyautogui"]
        else:
            sys.modules["pyautogui"] = previous
        shutil.rmtree(root, ignore_errors=True)


def test_verified_launch_halts_before_later_instances():
    calls = []
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=tempfile.mkdtemp(prefix="tws_launch_halt_"),
        run_token="launch12", clock=_fixed_clock, step_verification=True,
        expected_ports=[2000, 3000, 4000])

    def launch(email, *_args, **_kwargs):
        calls.append(email)
        if email == "b@gmail.com":
            raise tl.TwsLaunchError(
                "missing", code="dependency_unavailable", phase="preflight")
        return True

    try:
        with _Saved(
                tl, TWS_EXE=sys.executable, launch_one=launch, time=_Clock,
                _close_all_tws=lambda _log: None,
                close_one=lambda *_a, **_k: True,
                _ensure_config_free=lambda *_a, **_k: True,
                _restore_all_tws=lambda: None,
                dismiss_popups=lambda *_a, **_k: 0,
                _tws_pids=lambda: set(),
                _capture_failed_step=lambda *_a, **_k: None):
            result = tl.launch_many(
                ["a@gmail.com", "b@gmail.com", "c@gmail.com"],
                close_existing=False, ports=[2000, 3000, 4000],
                recorder=recorder)
        check(calls == ["a@gmail.com", "b@gmail.com"],
              "verified launch stops before the next instance")
        check(result["c@gmail.com"].startswith("NOT ATTEMPTED:"),
              "verified launch makes the skipped instance explicit")
    finally:
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_verified_enable_halts_before_later_ports():
    calls = []
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=tempfile.mkdtemp(prefix="tws_enable_halt_"),
        run_token="enable12", clock=_fixed_clock, step_verification=True,
        expected_ports=[2000, 3000, 4000])

    class _Api:
        def configure(self, _account, port, _log=None, **_kwargs):
            calls.append(int(port))
            if int(port) == 3000:
                raise tb.StepFailure(
                    "missing", code="dependency_unavailable",
                    phase="preflight")
            return True

    previous = sys.modules.get("tws_api")
    sys.modules["tws_api"] = _Api()
    try:
        with _Saved(
                tl, port_open=lambda *_a, **_k: False,
                _wait_account_for_config_stable=lambda *_a, **_k:
                tb.StableResult(True, "DU_TEST", 1, 2),
                _wait_port_stable=lambda *_a, **_k:
                tb.StableResult(True, True, 1, 2),
                dismiss_popups=lambda *_a, **_k: 0,
                record_member=lambda *_a, **_k: None,
                _restore_all_tws=lambda: None,
                _main_hwnd_for_account=lambda *_a, **_k: None,
                _capture_step=lambda *_a, **_k: None,
                _capture_failed_step=lambda *_a, **_k: None):
            result = tl.enable_fleet(
                [("a@gmail.com", 2000), ("b@gmail.com", 3000),
                 ("c@gmail.com", 4000)],
                recorder=recorder)
        check(calls == [2000, 3000],
              "verified enable stops before the next port")
        check(result[4000].startswith("NOT ATTEMPTED:"),
              "verified enable makes the skipped port explicit")
    finally:
        if previous is None:
            del sys.modules["tws_api"]
        else:
            sys.modules["tws_api"] = previous
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_verified_enable_accepts_late_listener_without_retoggle():
    calls = []
    stable = iter((
        tb.StableResult(False, None, 1, 2),
        tb.StableResult(True, True, 2, 2),
    ))
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=tempfile.mkdtemp(prefix="tws_enable_late_"),
        run_token="enable34", clock=_fixed_clock, step_verification=True,
        expected_ports=[2000])

    class _Api:
        def configure(self, _account, port, _log=None, **_kwargs):
            calls.append(int(port))
            return True

    previous = sys.modules.get("tws_api")
    sys.modules["tws_api"] = _Api()
    try:
        with _Saved(
                tl, port_open=lambda *_a, **_k: False,
                _wait_account_for_config_stable=lambda *_a, **_k:
                tb.StableResult(True, "DU_TEST", 1, 2),
                _wait_port_stable=lambda *_a, **_k: next(stable),
                _account_for_config_now=lambda *_a, **_k: "DU_TEST",
                dismiss_popups=lambda *_a, **_k: 0,
                close_nuisance_popups=lambda *_a, **_k: 0,
                record_member=lambda *_a, **_k: None,
                _restore_all_tws=lambda: None,
                _main_hwnd_for_account=lambda *_a, **_k: 1,
                _capture_step=lambda *_a, **_k: None,
                _capture_failed_step=lambda *_a, **_k: None):
            result = tl.enable_fleet(
                [("a@gmail.com", 2000)], recorder=recorder)
        check(calls == [2000] and result[2000]["ok"],
              "late listener recovery is accepted without toggling API again")
    finally:
        if previous is None:
            del sys.modules["tws_api"]
        else:
            sys.modules["tws_api"] = previous
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_verified_enable_rejects_preexisting_listener():
    calls = []
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=tempfile.mkdtemp(prefix="tws_enable_stale_"),
        run_token="enable56", clock=_fixed_clock, step_verification=True,
        expected_ports=[2000, 3000])

    class _Api:
        def configure(self, *_args, **_kwargs):
            calls.append(True)

    previous = sys.modules.get("tws_api")
    sys.modules["tws_api"] = _Api()
    try:
        with _Saved(
                tl, port_open=lambda *_a, **_k: True,
                _restore_all_tws=lambda: None,
                close_nuisance_popups=lambda *_a, **_k: 0,
                _capture_failed_step=lambda *_a, **_k: None):
            result = tl.enable_fleet(
                [("a@gmail.com", 2000), ("b@gmail.com", 3000)],
                recorder=recorder)
        check(not calls
              and str(result[2000]).startswith("FAILED:")
              and str(result[3000]).startswith("NOT ATTEMPTED:"),
              "proof mode halts on an ambiguous preexisting listener")
    finally:
        if previous is None:
            del sys.modules["tws_api"]
        else:
            sys.modules["tws_api"] = previous
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_handshake_requires_exact_account_and_disconnects():
    accounts = iter(("DU_OK", "DU_WRONG"))
    disconnected = []
    captures = []

    class _Live:
        def __init__(self, **_kwargs):
            self.value = next(accounts)

        def connect(self):
            return self

        def account(self):
            return self.value

        def disconnect(self):
            disconnected.append(self.value)

    fake = SimpleNamespace(
        LiveIB=_Live, HOST_DEFAULT="127.0.0.1", CLIENT_ID_DOCTOR=7312)
    previous = sys.modules.get("stock_ibkr")
    sys.modules["stock_ibkr"] = fake
    try:
        with _Saved(
                tl, _capture_step=lambda *_a, **kw:
                captures.append((kw["step"], kw["account_match"])),
                _capture_failed_step=lambda *_a, **_k: None,
                _main_hwnd_for_account=lambda *_a: None):
            result = tl.verify_handshake(2000, "DU_OK")
            try:
                tl.verify_handshake(3000, "DU_OK")
                mismatch = None
            except tl.TwsLaunchError as exc:
                mismatch = exc
        check(result["ok"] and captures == [("handshake", True)],
              "successful handshake records an exact account match")
        check(mismatch is not None
              and mismatch.code == "handshake_failed",
              "account mismatch is a typed handshake failure")
        check(disconnected == ["DU_OK", "DU_WRONG"],
              "success and failure both release the API connection")
    finally:
        if previous is None:
            del sys.modules["stock_ibkr"]
        else:
            sys.modules["stock_ibkr"] = previous


def test_memory_cap_result_is_recorded_once_and_path_free():
    recorder = tb.FleetTranscript(
        "fleet_bringup", root=tempfile.mkdtemp(prefix="tws_cap_event_"),
        run_token="cap12345", clock=_fixed_clock)
    progress = []
    result = SimpleNamespace(evidence=lambda: {
        "outcome": "installed", "verified": True, "changed": True,
        "cap_option": "-Xmx1024m",
        "backup_name": r"C:\\Jts\\tws.vmoptions.ema-backup-20260729",
        "detail": "installed for owner@example.com",
    })
    try:
        with _Saved(
                tl, _launch_many_impl=lambda *_a, **_k: {},
                _resource_warning_sweeper=lambda **_kwargs:
                _NoopResourceWarningSweeper()):
            tl.launch_many(
                [], recorder=recorder, on_progress=progress.append,
                memory_cap_result=result)
        cap_events = [event for event in recorder.events
                      if event["phase"] == "memory_cap_install"]
        serialized = json.dumps(cap_events, sort_keys=True)
        check(len(cap_events) == 1
              and cap_events[0]["outcome"] == "succeeded"
              and cap_events[0]["memory_cap"]["outcome"] == "installed",
              "launch_many records exactly one successful memory-cap event")
        check("C:\\" not in serialized
              and "owner@example.com" not in serialized
              and cap_events[0]["memory_cap"]["backup_name"]
              == "tws.vmoptions.ema-backup-20260729"
              and any("-Xmx1024m verified" in line for line in progress),
              "memory-cap evidence is path-free, redacted, and summarized")
    finally:
        shutil.rmtree(recorder.root, ignore_errors=True)


def test_gui_wiring_is_proof_only():
    source = (Path(__file__).resolve().parents[1] / "display_data.py").read_text(
        encoding="utf-8")
    check('os.environ.get("EMA_MULTIPORT_STEP_VERIFY") == "1"' in source
          and "step_verification=step_verify" in source
          and "verify_handshake(" in source,
          "GUI enables strict step proof only through the explicit environment")
    check("transcript is not None" in source
          and "and recorder.verification_complete()" in source,
          "GUI completion requires durable transcript and inventory publication")


def main():
    with _Saved(
            tl, _resource_warning_sweeper=lambda **_kwargs:
            _NoopResourceWarningSweeper()):
        for name, value in sorted(globals().items()):
            if name.startswith("test_") and callable(value):
                value()
    total = PASS + FAIL
    print(f"tws_step_verification_selftest: "
          f"{PASS}/{total} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
