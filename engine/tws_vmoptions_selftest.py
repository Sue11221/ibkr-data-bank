"""Offline temp-directory selftests for the portable TWS heap-cap installer."""
from __future__ import annotations

from datetime import date
from contextlib import nullcontext
import json
from pathlib import Path
import sys
import tempfile
from unittest import mock

sys.stdout.reconfigure(encoding="utf-8")

ENGINE = Path(__file__).resolve().parent
sys.path.insert(0, str(ENGINE))

import tws_vmoptions as tv  # noqa: E402
import tws_bringup as bringup  # noqa: E402
import tws_launch as launch  # noqa: E402


CHECKS: list[tuple[str, bool]] = []


def check(name, ok, detail=""):
    passed = bool(ok)
    CHECKS.append((name, passed))
    line = f"[{'PASS' if passed else 'FAIL'}] {name}"
    if detail and not passed:
        line += f" :: {detail}"
    print(line)


def fixture(root: Path, raw=b"-Dsample=true\r\n"):
    install = root / "Jts"
    install.mkdir(parents=True)
    executable = install / "tws.exe"
    executable.write_bytes(b"test executable")
    target = install / "tws.vmoptions"
    target.write_bytes(raw)
    return executable, target


def test_install_and_idempotence():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        original = target.read_bytes()
        result = tv.ensure_memory_cap(
            executable, today=date(2026, 7, 29))
        installed = target.read_bytes()
        backups = list(target.parent.glob(target.name + tv.BACKUP_SUFFIX + "*"))
        expected_block = (
            b"# EMA-FLEET-CAP BEGIN\r\n-Xmx1024m\r\n"
            b"# EMA-FLEET-CAP END\r\n")
        check("first install verifies and changes exactly once",
              result.outcome == "installed" and result.verified
              and result.changed and installed == original + expected_block,
              result)
        check("first install creates one exact dated original backup",
              len(backups) == 1
              and backups[0].name == "tws.vmoptions.ema-backup-20260729"
              and backups[0].read_bytes() == original)
        evidence_text = json.dumps(result.evidence(), sort_keys=True)
        check("installer evidence is controlled and path-free",
              str(Path(temp)) not in evidence_text
              and result.evidence()["cap_option"] == "-Xmx1024m"
              and result.evidence()["backup_name"] == backups[0].name)

        before = target.read_bytes()
        second = tv.ensure_memory_cap(
            executable, today=date(2030, 1, 1))
        check("exact managed block is an idempotent verified no-op",
              second.outcome == "verified" and second.verified
              and not second.changed and target.read_bytes() == before)
        check("idempotent no-op never creates another backup",
              len(list(target.parent.glob(
                  target.name + tv.BACKUP_SUFFIX + "*"))) == 1)


def test_newline_and_empty_preservation():
    for name, original, expected_prefix in (
            ("LF", b"-Dsample=true\n", b"-Dsample=true\n# EMA"),
            ("no-final-newline", b"-Dsample=true", b"-Dsample=true\n# EMA"),
            ("empty", b"", b"# EMA")):
        with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
            executable, target = fixture(Path(temp), raw=original)
            result = tv.ensure_memory_cap(
                executable, today=date(2026, 7, 29))
            check(f"{name} source bytes and newline style are preserved",
                  result.verified
                  and target.read_bytes().startswith(expected_prefix))


def test_existing_backup_is_never_overwritten():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        existing = target.with_name(
            "tws.vmoptions.ema-backup-20200101")
        existing.write_bytes(b"preserved rewind bytes")
        result = tv.ensure_memory_cap(
            executable, today=date(2026, 7, 29))
        backups = list(target.parent.glob(
            target.name + tv.BACKUP_SUFFIX + "*"))
        check("existing one-time backup is reused and never overwritten",
              result.outcome == "installed" and result.verified
              and not result.backup_created
              and result.backup_name == existing.name
              and backups == [existing]
              and existing.read_bytes() == b"preserved rewind bytes")


def test_unsafe_backup_candidates_and_partial_cleanup():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        original = target.read_bytes()
        malformed = target.with_name(
            "tws.vmoptions.ema-backup-not-a-date")
        malformed.write_bytes(b"not a managed backup")
        result = tv.ensure_memory_cap(executable)
        check("malformed backup candidate prevents a vmoptions write",
              result.outcome == "warning"
              and target.read_bytes() == original
              and malformed.read_bytes() == b"not a managed backup")

    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        with mock.patch.object(tv.os, "fsync", side_effect=OSError):
            result = tv.ensure_memory_cap(
                executable, today=date(2026, 7, 29))
        check("failed backup publication removes its partial candidate",
              result.outcome == "warning"
              and target.read_bytes() == b"-Dsample=true\r\n"
              and not list(target.parent.glob(
                  target.name + tv.BACKUP_SUFFIX + "*")))


def test_malformed_blocks_are_untouched():
    cases = {
        "partial": b"x\n# EMA-FLEET-CAP BEGIN\n-Xmx1024m\n",
        "wrong": (b"# EMA-FLEET-CAP BEGIN\n-Xmx2048m\n"
                  b"# EMA-FLEET-CAP END\n"),
        "duplicate": (b"# EMA-FLEET-CAP BEGIN\n-Xmx1024m\n"
                      b"# EMA-FLEET-CAP END\n"
                      b"# EMA-FLEET-CAP BEGIN\n-Xmx1024m\n"
                      b"# EMA-FLEET-CAP END\n"),
        "indented": (b" # EMA-FLEET-CAP BEGIN\n-Xmx1024m\n"
                     b"# EMA-FLEET-CAP END\n"),
    }
    for label, raw in cases.items():
        with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
            executable, target = fixture(Path(temp), raw=raw)
            result = tv.ensure_memory_cap(executable)
            check(f"{label} managed block is warned and untouched",
                  result.outcome == "warning" and not result.verified
                  and target.read_bytes() == raw
                  and not list(target.parent.glob(
                      target.name + tv.BACKUP_SUFFIX + "*")))


def test_nonfatal_failures():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        root = Path(temp)
        missing = tv.ensure_memory_cap(root / "missing" / "tws.exe")
        check("missing runtime executable is a nonfatal warning",
              missing.outcome == "warning" and not missing.verified)

        executable, target = fixture(root / "fixture-missing")
        target.unlink()
        no_options = tv.ensure_memory_cap(executable)
        check("missing vmoptions is a nonfatal warning",
              no_options.outcome == "warning" and not no_options.verified)

    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        original = target.read_bytes()
        real_is_symlink = Path.is_symlink

        def mark_target_symlink(path):
            return path.name == "tws.vmoptions" or real_is_symlink(path)

        with mock.patch.object(Path, "is_symlink", mark_target_symlink):
            unsafe = tv.ensure_memory_cap(executable)
        check("symlink vmoptions target is refused without a write",
              unsafe.outcome == "warning" and target.read_bytes() == original)

        with mock.patch.object(Path, "read_bytes", side_effect=PermissionError):
            unreadable = tv.ensure_memory_cap(executable)
        check("read failure is contained as a warning",
              unreadable.outcome == "warning" and not unreadable.verified)

    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        original = target.read_bytes()
        with mock.patch.object(
                tv, "_write_backup_once",
                return_value=(None, "forced backup refusal")):
            backup_fail = tv.ensure_memory_cap(executable)
        check("backup failure prevents modification and remains nonfatal",
              backup_fail.outcome == "warning"
              and target.read_bytes() == original)

        with mock.patch.object(
                tv, "_atomic_replace", side_effect=PermissionError):
            write_fail = tv.ensure_memory_cap(
                executable, today=date(2026, 7, 29))
        check("write failure preserves source and reports the retained backup",
              write_fail.outcome == "warning"
              and write_fail.backup_name ==
              "tws.vmoptions.ema-backup-20260729"
              and target.read_bytes() == original)


def test_bounds_and_policy():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(
            Path(temp), raw=b"x" * (tv.MAX_VMOPTIONS_BYTES + 1))
        oversized = tv.ensure_memory_cap(executable)
        check("oversized vmoptions fails closed without backup or rewrite",
              oversized.outcome == "warning"
              and target.stat().st_size == tv.MAX_VMOPTIONS_BYTES + 1
              and not list(target.parent.glob(
                  target.name + tv.BACKUP_SUFFIX + "*")))

    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp), raw=b"x\x00y")
        binary = tv.ensure_memory_cap(executable)
        check("binary-looking vmoptions is refused without a backup or write",
              binary.outcome == "warning" and target.read_bytes() == b"x\x00y"
              and not list(target.parent.glob(
                  target.name + tv.BACKUP_SUFFIX + "*")))

    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        executable, target = fixture(Path(temp))
        invalid_date = tv.ensure_memory_cap(executable, today=object())
        check("invalid backup date is contained before modification",
              invalid_date.outcome == "warning"
              and target.read_bytes() == b"-Dsample=true\r\n")
    check("policy constants pin the exact fenced 1 GiB heap cap",
          tv.HEAP_CAP_MB == 1024 and tv.HEAP_CAP_OPTION == "-Xmx1024m"
          and tv.MANAGED_BEGIN == "# EMA-FLEET-CAP BEGIN"
          and tv.MANAGED_END == "# EMA-FLEET-CAP END")


def test_transcript_and_launch_wiring():
    with tempfile.TemporaryDirectory(prefix="tws-vmoptions-") as temp:
        root = Path(temp)
        executable, _target = fixture(root)
        result = tv.ensure_memory_cap(
            executable, today=date(2026, 7, 29))
        recorder = bringup.FleetTranscript(
            "fleet_bringup", root=root, run_token="capwire")
        progress = []
        with (
            mock.patch.object(
                launch, "_resource_warning_sweeper",
                side_effect=lambda **_kwargs: nullcontext()),
            mock.patch.object(
                launch, "_launch_many_impl",
                return_value={"a@gmail.com": "launched"}) as implementation,
        ):
            output = launch.launch_many(
                ["a@gmail.com"], recorder=recorder,
                memory_cap_result=result, on_progress=progress.append)
        cap_events = [
            event for event in recorder.events
            if event.get("phase") == "memory_cap_install"
        ]
        encoded = json.dumps(cap_events, sort_keys=True)
        check("launch wrapper records exactly one memory-cap event",
              output == {"a@gmail.com": "launched"}
              and implementation.call_count == 1 and len(cap_events) == 1)
        check("memory-cap transcript is verified, controlled, and path-free",
              cap_events[0]["outcome"] == "succeeded"
              and cap_events[0]["memory_cap"]["outcome"] == "installed"
              and cap_events[0]["memory_cap"]["verified"] is True
              and cap_events[0]["memory_cap"]["changed"] is True
              and str(root) not in encoded)
        check("normal progress reports the controlled verified cap",
              any("-Xmx1024m verified" in line for line in progress))

        warning = tv.CapInstallResult(
            action="warning", verified=False,
            detail="forced nonfatal test warning")
        warning_recorder = bringup.FleetTranscript(
            "fleet_bringup", root=root, run_token="capwarn")
        warning_progress = []
        with (
            mock.patch.object(
                launch, "_resource_warning_sweeper",
                side_effect=lambda **_kwargs: nullcontext()),
            mock.patch.object(
                launch, "_launch_many_impl",
                return_value={"a@gmail.com": "launched"}) as implementation,
        ):
            warning_output = launch.launch_many(
                ["a@gmail.com"], recorder=warning_recorder,
                memory_cap_result=warning,
                on_progress=warning_progress.append)
        warning_events = [
            event for event in warning_recorder.events
            if event.get("phase") == "memory_cap_install"
        ]
        check("unverified cap warning is nonfatal and still evidenced once",
              warning_output == {"a@gmail.com": "launched"}
              and implementation.call_count == 1
              and len(warning_events) == 1
              and warning_events[0]["outcome"] == "failed"
              and any("forced nonfatal test warning" in line
                      for line in warning_progress))

        class BrokenEvidence:
            def evidence(self):
                raise RuntimeError("forced evidence failure")

        broken_recorder = bringup.FleetTranscript(
            "fleet_bringup", root=root, run_token="capbroken")
        broken_progress = []
        with (
            mock.patch.object(
                launch, "_resource_warning_sweeper",
                side_effect=lambda **_kwargs: nullcontext()),
            mock.patch.object(
                launch, "_launch_many_impl",
                return_value={"a@gmail.com": "launched"}) as implementation,
        ):
            broken_output = launch.launch_many(
                ["a@gmail.com"], recorder=broken_recorder,
                memory_cap_result=BrokenEvidence(),
                on_progress=broken_progress.append)
        check("malformed cap evidence cannot block fleet launch",
              broken_output == {"a@gmail.com": "launched"}
              and implementation.call_count == 1
              and len(broken_recorder.events) == 1
              and broken_recorder.events[0]["outcome"] == "failed"
              and any("result was unavailable" in line
                      for line in broken_progress))


def main():
    test_install_and_idempotence()
    test_newline_and_empty_preservation()
    test_existing_backup_is_never_overwritten()
    test_unsafe_backup_candidates_and_partial_cleanup()
    test_malformed_blocks_are_untouched()
    test_nonfatal_failures()
    test_bounds_and_policy()
    test_transcript_and_launch_wiring()
    failed = [name for name, ok in CHECKS if not ok]
    print(f"{len(CHECKS)} checks, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
