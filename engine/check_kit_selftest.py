"""Offline contract tests for :mod:`check_kit`.

This file uses an independent reporter so a broken kit cannot report its own
acceptance as green.
"""

from __future__ import annotations

import importlib
import io
import subprocess
import sys
import threading
from pathlib import Path


ENGINE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ENGINE_ROOT.parent
sys.path.insert(0, str(ENGINE_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

import check_kit  # noqa: E402


FAILURES = []
COUNT = [0]


def verify(name, condition, detail=""):
    COUNT[0] += 1
    try:
        passed = bool(condition)
    except BaseException as exc:  # noqa: BLE001 - independent oracle
        passed = False
        detail = f"oracle raised {type(exc).__name__}: {exc}"
    print(f"[{'PASS' if passed else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not passed else ""))
    if not passed:
        FAILURES.append(name)


def expect_raises(name, error, function, text=""):
    try:
        function()
    except error as exc:
        verify(name, not text or text in str(exc), str(exc))
    except BaseException as exc:  # noqa: BLE001 - classify exact contract
        verify(name, False, f"wrong {type(exc).__name__}: {exc}")
    else:
        verify(name, False, "did not raise")


class BoolProbe:
    def __init__(self, value=True, error=None):
        self.value = value
        self.error = error
        self.calls = 0

    def __bool__(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.value


class DetailProbe:
    def __init__(self, text="detail", error=None):
        self.text = text
        self.error = error
        self.calls = 0

    def __str__(self):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.text


class ReentrantStream:
    """Writable stream that attempts one kit mutation from write()."""

    def __init__(self):
        self.buffer = io.StringIO()
        self.kit = None
        self.armed = False
        self.attempted = False

    def write(self, text):
        if (self.armed and not self.attempted
                and "[PASS] reentrant write" in text):
            self.attempted = True
            self.kit.finish()
        return self.buffer.write(text)

    def flush(self):
        return self.buffer.flush()

    def getvalue(self):
        return self.buffer.getvalue()


def test_surface_and_exact_rendering():
    verify("surface publishes TEST_ONLY", check_kit.TEST_ONLY is True)
    verify("surface publishes exact 0/1/3 constants",
           (check_kit.ACCEPTED, check_kit.VIOLATION,
            check_kit.FEATURE_ABSENT) == (0, 1, 3))
    verify("surface has a narrow explicit export list",
           set(check_kit.__all__) == {
               "ACCEPTED", "CheckKit", "CheckKitError", "FEATURE_ABSENT",
               "SECTION_WIDTH", "TEST_ONLY", "VIOLATION",
           })

    stream = io.StringIO()
    kit = check_kit.CheckKit(stream=stream)
    hidden = DetailProbe(error=AssertionError("pass detail rendered"))
    passed = kit.check("passing check", BoolProbe(True), hidden)
    failed = kit.check("failing check", BoolProbe(False), "why")
    code = kit.finish(feature_absent=True)
    expected = (
        "[PASS] passing check\n"
        "[FAIL] failing check -- why\n"
        "\n"
        "2 checks, 1 failed\n"
        "FAILURES:\n"
        "  - failing check\n"
        "harness exit=1\n"
    )
    verify("check returns the coerced condition values",
           passed is True and failed is False)
    verify("passing checks never stringify failure detail", hidden.calls == 0)
    verify("failure overrides feature-absent exit 3", code == 1)
    verify("pass/fail and failure summary render exactly",
           stream.getvalue() == expected, repr(stream.getvalue()))
    verify("finish seals state and exposes immutable failures",
           kit.finished and kit.total == 2
           and kit.failures == ("failing check",))


def test_strict_arguments_and_exception_transactionality():
    stream = io.StringIO()
    kit = check_kit.CheckKit(stream=stream)
    expect_raises("reversed check arguments fail closed",
                  check_kit.CheckKitError,
                  lambda: kit.check(False, "looks truthy"), "check name")
    expect_raises("empty check name is rejected",
                  check_kit.CheckKitError,
                  lambda: kit.check("", True), "non-empty")
    expect_raises("multiline check name is rejected",
                  check_kit.CheckKitError,
                  lambda: kit.check("bad\nname", True), "single-line")
    verify("argument failures do not mutate count or output",
           kit.total == 0 and stream.getvalue() == "")

    true_probe = BoolProbe(True)
    kit.check("bool exactly once", true_probe)
    verify("condition bool is evaluated exactly once", true_probe.calls == 1)

    runtime_probe = BoolProbe(error=RuntimeError("bool boom"))
    before = (kit.total, stream.getvalue())
    expect_raises("condition RuntimeError propagates", RuntimeError,
                  lambda: kit.check("runtime", runtime_probe), "bool boom")
    verify("RuntimeError leaves check state transactional",
           runtime_probe.calls == 1
           and (kit.total, stream.getvalue()) == before)

    interrupt_probe = BoolProbe(error=KeyboardInterrupt("bool stop"))
    expect_raises("condition BaseException propagates", KeyboardInterrupt,
                  lambda: kit.check("interrupt", interrupt_probe), "bool stop")
    verify("BaseException leaves check state transactional",
           interrupt_probe.calls == 1
           and (kit.total, stream.getvalue()) == before)

    detail = DetailProbe(error=RuntimeError("detail boom"))
    expect_raises("failing detail formatting error propagates", RuntimeError,
                  lambda: kit.check("detail", False, detail), "detail boom")
    verify("detail formatting failure leaves state transactional",
           detail.calls == 1 and (kit.total, stream.getvalue()) == before)

    multiline_before = (kit.total, stream.getvalue())
    expect_raises("multiline failure detail is rejected",
                  check_kit.CheckKitError,
                  lambda: kit.check(
                      "real failure", False,
                      "first line\n[PASS] forged result"), "single-line")
    verify("detail injection cannot mutate state or forge output",
           (kit.total, stream.getvalue()) == multiline_before
           and "forged result" not in stream.getvalue())

    detail_once_stream = io.StringIO()
    detail_once_kit = check_kit.CheckKit(stream=detail_once_stream)
    detail_once = DetailProbe("rendered once")
    detail_once_kit.check("detail once", False, detail_once)
    verify("failing detail is stringified exactly once",
           detail_once.calls == 1
           and detail_once_stream.getvalue()
           == "[FAIL] detail once -- rendered once\n")

    expect_raises("non-writable stream is rejected",
                  check_kit.CheckKitError,
                  lambda: check_kit.CheckKit(stream=object()), "write")


def test_sections_pending_and_exit_selection():
    stream = io.StringIO()
    kit = check_kit.CheckKit(stream=stream)
    kit.section("[S1] short")
    long_title = "L" * 80
    kit.section(long_title)
    kit.pending("M2", "later surface absent", "required field: x", "")
    kit.check("baseline", True)
    code = kit.finish()
    expected = (
        f"{'=== [S1] short '.ljust(68, '=')}\n"
        f"=== {long_title} \n"
        "[M2 PENDING] later surface absent\n"
        "  required field: x\n"
        "  \n"
        "[PASS] baseline\n"
        "\n"
        "1 checks, 0 failed\n"
        "ALL PASS\n"
        "harness exit=0\n"
    )
    verify("section banners use width 68 without truncating long titles",
           check_kit.SECTION_WIDTH == 68)
    verify("pending is output-only and optional later work keeps exit 0",
           code == 0 and stream.getvalue() == expected,
           repr(stream.getvalue()))

    absent_stream = io.StringIO()
    absent = check_kit.CheckKit(stream=absent_stream)
    absent.check("baseline", True)
    absent.pending("M1", "current feature absent")
    absent_code = absent.finish(feature_absent=True)
    verify("green baseline plus explicit feature absence returns 3",
           absent_code == 3
           and "ALL PASS (feature pending)" in absent_stream.getvalue()
           and "harness exit=3" in absent_stream.getvalue())

    failed_stream = io.StringIO()
    failed = check_kit.CheckKit(stream=failed_stream)
    failed.check("same", False)
    failed.check("middle", True)
    failed.check("same", False)
    failed_code = failed.finish(feature_absent=True)
    verify("ordered duplicate failures are retained and 1 beats 3",
           failed_code == 1
           and failed.failures == ("same", "same")
           and failed_stream.getvalue().count("  - same\n") == 2)

    for name, function in (
            ("multiline section", lambda: check_kit.CheckKit(
                stream=io.StringIO()).section("bad\ntitle")),
            ("bracketed milestone", lambda: check_kit.CheckKit(
                stream=io.StringIO()).pending("[M1]", "bad")),
            ("multiline pending message", lambda: check_kit.CheckKit(
                stream=io.StringIO()).pending("M1", "bad\nmessage")),
            ("non-string contract", lambda: check_kit.CheckKit(
                stream=io.StringIO()).pending("M1", "bad", 1)),
    ):
        expect_raises(f"{name} is rejected", check_kit.CheckKitError,
                      function)


def test_subprocess_exit_codes():
    template = (
        "import sys\n"
        f"sys.path.insert(0, {str(ENGINE_ROOT)!r})\n"
        "from check_kit import CheckKit\n"
        "kit = CheckKit()\n"
        "{body}\n"
        "raise SystemExit(kit.finish(feature_absent={absent}))\n"
    )
    cases = [
        (0, "kit.check('baseline', True)\nkit.pending('M2', 'later')",
         "False", "[M2 PENDING]", "harness exit=0"),
        (3, "kit.check('baseline', True)\nkit.pending('M1', 'current')",
         "True", "[M1 PENDING]", "harness exit=3"),
        (1, "kit.check('baseline', False, 'broken')",
         "True", "[FAIL] baseline -- broken", "harness exit=1"),
    ]
    for expected, body, absent, marker, tail in cases:
        result = subprocess.run(
            [sys.executable, "-c", template.format(
                body=body, absent=absent)],
            cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=20)
        verify(f"subprocess proves actual exit {expected}",
               result.returncode == expected
               and marker in result.stdout and tail in result.stdout
               and not result.stderr,
               f"exit={result.returncode} out={result.stdout!r} "
               f"err={result.stderr!r}")


def test_instances_threads_and_sealing():
    package_module = importlib.import_module("engine.check_kit")
    direct_stream, package_stream = io.StringIO(), io.StringIO()
    direct = check_kit.CheckKit(stream=direct_stream)
    package = package_module.CheckKit(stream=package_stream)
    direct.check("direct failure", False)
    package.check("package pass", True)
    verify("direct/package imports have no shared mutable default state",
           direct.total == package.total == 1
           and direct.failures == ("direct failure",)
           and package.failures == ())
    verify("direct/package modules both publish the test-only sentinel",
           package_module.TEST_ONLY is check_kit.TEST_ONLY is True)
    direct.finish()
    package.finish()

    cross_stream = io.StringIO()
    cross = check_kit.CheckKit(stream=cross_stream)
    start_reporter = threading.Event()
    reporter_done = threading.Event()
    reporter_errors = []

    def reporter():
        try:
            if not start_reporter.wait(10):
                raise AssertionError("reporter start guard expired")
            cross.check("inner reporter", True)
        except BaseException as exc:  # noqa: BLE001
            reporter_errors.append(exc)
        finally:
            reporter_done.set()

    reporter_thread = threading.Thread(target=reporter)
    reporter_thread.start()

    class CrossThreadCondition:
        def __init__(self):
            self.completed = False

        def __bool__(self):
            start_reporter.set()
            self.completed = reporter_done.wait(10)
            return self.completed

    condition = CrossThreadCondition()
    cross.check("outer condition", condition)
    reporter_thread.join(20)
    verify("condition conversion holds no kit lock needed by another reporter",
           condition.completed and not reporter_errors
           and not reporter_thread.is_alive()
           and cross.total == 2 and not cross.failures,
           repr(reporter_errors))
    cross.finish()

    reentrant_stream = ReentrantStream()
    reentrant_kit = check_kit.CheckKit(stream=reentrant_stream)
    reentrant_stream.kit = reentrant_kit
    reentrant_kit.check("baseline", True)
    reentrant_before = (reentrant_kit.total, reentrant_stream.getvalue())
    reentrant_stream.armed = True
    expect_raises("output-stream re-entry fails closed",
                  check_kit.CheckKitError,
                  lambda: reentrant_kit.check("reentrant write", True),
                  "must not re-enter")
    verify("stream re-entry cannot seal a stale summary or mutate the ledger",
           reentrant_stream.attempted
           and not reentrant_kit.finished
           and (reentrant_kit.total, reentrant_stream.getvalue())
           == reentrant_before
           and "harness exit=" not in reentrant_stream.getvalue())
    reentrant_stream.armed = False
    reentrant_kit.check("recovered", True)
    verify("kit remains usable after rejected stream re-entry",
           reentrant_kit.finish() == 0 and reentrant_kit.total == 2)

    stream = io.StringIO()
    kit = check_kit.CheckKit(stream=stream)
    worker_count, per_worker = 8, 40
    barrier = threading.Barrier(worker_count + 1)
    errors = []

    def worker(worker_index):
        try:
            barrier.wait(10)
            for item in range(per_worker):
                kit.check(f"thread-{worker_index}-{item}", True)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,))
               for index in range(worker_count)]
    for thread in threads:
        thread.start()
    barrier.wait(10)
    for thread in threads:
        thread.join(20)
    code = kit.finish()
    lines = stream.getvalue().splitlines()
    pass_lines = [line for line in lines if line.startswith("[PASS] ")]
    verify("concurrent checks finish without worker errors or lost counts",
           not errors and all(not thread.is_alive() for thread in threads)
           and kit.total == worker_count * per_worker and code == 0,
           repr(errors))
    verify("thread-safe output contains one whole line per check",
           len(pass_lines) == worker_count * per_worker
           and all(line.count("[PASS]") == 1 for line in pass_lines))

    sealed_state = (kit.total, kit.failures, stream.getvalue())
    for label, function in (
            ("check", lambda: kit.check("late", True)),
            ("section", lambda: kit.section("late")),
            ("pending", lambda: kit.pending("M4", "late")),
            ("finish", lambda: kit.finish()),
    ):
        expect_raises(f"{label} is rejected after finish",
                      check_kit.CheckKitError, function, "sealed")
    verify("post-finish rejections preserve ledger and output exactly",
           (kit.total, kit.failures, stream.getvalue()) == sealed_state)

    expect_raises("zero-check finish fails closed",
                  check_kit.CheckKitError,
                  lambda: check_kit.CheckKit(
                      stream=io.StringIO()).finish(), "zero checks")
    invalid_absent = check_kit.CheckKit(stream=io.StringIO())
    invalid_absent.check("baseline", True)
    expect_raises("feature_absent requires a real bool",
                  check_kit.CheckKitError,
                  lambda: invalid_absent.finish(feature_absent=1), "bool")
    verify("invalid finish selector leaves kit open",
           not invalid_absent.finished and invalid_absent.total == 1)


def run():
    test_surface_and_exact_rendering()
    test_strict_arguments_and_exception_transactionality()
    test_sections_pending_and_exit_selection()
    test_subprocess_exit_codes()
    test_instances_threads_and_sealing()
    print(f"\n{COUNT[0] - len(FAILURES)}/{COUNT[0]} checks passed")
    if FAILURES:
        print("FAILURES:")
        for failure in FAILURES:
            print(f"  - {failure}")
        raise SystemExit(1)


if __name__ == "__main__":
    run()
