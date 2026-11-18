"""TEST-ONLY check reporting and the repository's 0/3/1 harness convention.

Use one :class:`CheckKit` instance per harness.  ``finish`` returns an exit
code; the harness remains responsible for ``raise SystemExit(code)``.
Production modules must never import this module.
"""

from __future__ import annotations

import sys
import threading


TEST_ONLY = True
ACCEPTED = 0
VIOLATION = 1
FEATURE_ABSENT = 3
SECTION_WIDTH = 68
__all__ = [
    "ACCEPTED", "CheckKit", "CheckKitError", "FEATURE_ABSENT",
    "SECTION_WIDTH", "TEST_ONLY", "VIOLATION",
]


class CheckKitError(RuntimeError):
    """The harness reporting contract was used incorrectly."""


def _single_line(label, value, *, allow_empty=False):
    if not isinstance(value, str):
        raise CheckKitError(f"{label} must be a string")
    if (not allow_empty and not value.strip()) or "\n" in value or "\r" in value:
        qualifier = "single-line" if allow_empty else "non-empty single-line"
        raise CheckKitError(f"{label} must be a {qualifier} string")
    return value


class CheckKit:
    """Per-harness, thread-safe check state and canonical text rendering."""

    def __init__(self, *, stream=None):
        self._stream = sys.stdout if stream is None else stream
        if not callable(getattr(self._stream, "write", None)):
            raise CheckKitError("stream must provide write(text)")
        self._lock = threading.RLock()
        self._rendering = threading.local()
        self._total = 0
        self._failures = []
        self._finished = False

    @property
    def total(self):
        with self._lock:
            return self._total

    @property
    def failures(self):
        with self._lock:
            return tuple(self._failures)

    @property
    def finished(self):
        with self._lock:
            return self._finished

    def check(self, name, condition, detail=""):
        """Record and print one strict name-first check, returning its truth."""
        name = _single_line("check name", name)
        with self._lock:
            self._ensure_open()

        # Run harness-owned conversion outside the state lock.  Exceptions
        # propagate without ledger mutation, and a condition cannot deadlock
        # the kit by waiting for a different reporting thread.
        passed = bool(condition)
        suffix = ""
        if not passed and detail is not None:
            detail_text = str(detail)
            detail_text = _single_line(
                "check detail", detail_text, allow_empty=True)
            if detail_text:
                suffix = f" -- {detail_text}"
        line = f"[{'PASS' if passed else 'FAIL'}] {name}{suffix}"

        with self._lock:
            self._ensure_open()
            self._write(line)
            self._total += 1
            if not passed:
                self._failures.append(name)
            return passed

    def section(self, title):
        """Print the repository's canonical 68-column section banner."""
        title = _single_line("section title", title)
        with self._lock:
            self._ensure_open()
            self._write(f"=== {title} ".ljust(SECTION_WIDTH, "="))

    def pending(self, milestone, message, *contract_lines):
        """Print a pending contract without selecting exit code 3.

        A later milestone can be pending while the current milestone is fully
        accepted.  Call ``finish(feature_absent=True)`` only when the feature
        this harness currently judges is absent.
        """
        milestone = _single_line("milestone", milestone)
        if "[" in milestone or "]" in milestone:
            raise CheckKitError("milestone must not contain brackets")
        message = _single_line("pending message", message)
        lines = tuple(_single_line("contract line", line, allow_empty=True)
                      for line in contract_lines)
        rendered = [f"[{milestone} PENDING] {message}"]
        rendered.extend(f"  {line}" for line in lines)
        with self._lock:
            self._ensure_open()
            for line in rendered:
                self._write(line)

    def finish(self, *, feature_absent=False):
        """Print the final summary, seal this kit, and return 0, 3, or 1."""
        if not isinstance(feature_absent, bool):
            raise CheckKitError("feature_absent must be a bool")
        with self._lock:
            self._ensure_open()
            if self._total == 0:
                raise CheckKitError("cannot finish a harness with zero checks")
            if self._failures:
                code = VIOLATION
            elif feature_absent:
                code = FEATURE_ABSENT
            else:
                code = ACCEPTED

            lines = ["", f"{self._total} checks, "
                     f"{len(self._failures)} failed"]
            if self._failures:
                lines.append("FAILURES:")
                lines.extend(f"  - {name}" for name in self._failures)
            else:
                lines.append("ALL PASS" + (
                    " (feature pending)" if feature_absent else ""))
            lines.append(f"harness exit={code}")
            for line in lines:
                self._write(line)
            self._finished = True
            return code

    def _ensure_open(self):
        if self._finished:
            raise CheckKitError("check kit is sealed after finish()")

    def _write(self, line):
        if getattr(self._rendering, "active", False):
            raise CheckKitError("output stream must not re-enter the check kit")
        self._rendering.active = True
        try:
            print(line, file=self._stream)
        finally:
            self._rendering.active = False
