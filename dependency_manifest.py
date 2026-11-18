"""Single, stdlib-only dependency inventory for app startup and setup.

The table is deliberately importable before any third-party module. Keep
``requirements.txt`` byte-equal to :func:`render_requirements`.
"""

from __future__ import annotations

from dataclasses import dataclass
import platform


@dataclass(frozen=True)
class Dependency:
    pip_name: str
    import_name: str
    tier: str
    feature: str
    os_filter: tuple[str, ...] = ()
    # Optional rows only: also installed by the first-time setup window when
    # that window is already open for a missing REQUIRED package. Never opens
    # the window by itself and never blocks startup. [CLAUDE 2026-11-17, user
    # choice: drag-and-drop rides along with first-time setup]
    first_setup: bool = False


DEPENDENCIES = (
    Dependency("pandas", "pandas", "required", "data frames and bank import"),
    Dependency("numpy", "numpy", "required", "numeric bank operations"),
    Dependency("pyarrow", "pyarrow", "required", "Parquet bank read/write"),
    Dependency("ib_async", "ib_async", "required", "IBKR data acquisition"),
    Dependency("tkinterdnd2", "tkinterdnd2", "optional", "drag-and-drop",
               first_setup=True),
    Dependency("openpyxl", "openpyxl", "optional", "Excel .xlsx/.xlsm input"),
    Dependency("xlrd", "xlrd", "optional", "Excel .xls input"),
    Dependency("pillow", "PIL", "optional", "chart overlays"),
    Dependency("pyautogui", "pyautogui", "optional", "TWS auto-login", ("Windows",)),
    Dependency("opencv-python", "cv2", "optional", "TWS image matching", ("Windows",)),
    Dependency("pyobjc-framework-Cocoa", "AppKit", "optional", "macOS pinch", ("Darwin",)),
)

_PIP_MARKERS = {"Windows": "win32", "Darwin": "darwin", "Linux": "linux"}


def for_platform(system: str | None = None) -> tuple[Dependency, ...]:
    """Dependencies relevant to the current OS (or an explicit test OS)."""
    system = platform.system() if system is None else system
    return tuple(row for row in DEPENDENCIES
                 if not row.os_filter or system in row.os_filter)


def packages(tier: str, system: str | None = None) -> list[tuple[str, str]]:
    """Old setup-call shape, derived from the one manifest."""
    if tier not in {"required", "optional"}:
        raise ValueError(f"unknown dependency tier: {tier}")
    return [(row.pip_name, row.import_name)
            for row in for_platform(system) if row.tier == tier]


def first_setup_extras(system: str | None = None) -> list[tuple[str, str]]:
    """Optional (pip_name, import_name) pairs the first-time setup window
    installs together with the required set on this OS."""
    return [(row.pip_name, row.import_name) for row in for_platform(system)
            if row.tier == "optional" and row.first_setup]


def render_requirements() -> str:
    """Pip file installing the same OS-relevant required and optional set."""
    lines = [
        "# Generated from dependency_manifest.py; do not maintain a second list.",
        "# Tested on Python 3.14. Install with: pip install -r requirements.txt",
    ]
    for tier in ("required", "optional"):
        lines.extend(("", f"# {tier.title()} packages"))
        for row in DEPENDENCIES:
            if row.tier != tier:
                continue
            marker = ""
            if row.os_filter:
                selectors = [f'sys_platform == "{_PIP_MARKERS[name]}"'
                             for name in row.os_filter]
                marker = "; " + " or ".join(selectors)
            lines.append(f"{row.pip_name}{marker}  # {row.feature}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    print(render_requirements(), end="")
