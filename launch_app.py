"""Windowless, self-locating entry point for the data-bank application."""

from __future__ import annotations

import multiprocessing as mp
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
ENGINE_ROOT = PROJECT_ROOT / "engine"
if str(ENGINE_ROOT) not in sys.path:
    sys.path.insert(0, str(ENGINE_ROOT))

from headless_streams import ensure_streams  # noqa: E402

ensure_streams(PROJECT_ROOT)


def _enable_sharp_rendering():
    """Declare the process DPI-aware so Windows draws the window at the
    screen's real scale instead of enlarging a 100% picture (the blur seen at
    125%/150% display scaling). System-level awareness: Tk sizes its fonts
    from the reported DPI once, at start. Must run before the first window
    exists. Best effort and silent: never a reason for the launch to fail,
    and a no-op where Windows or its DPI calls are absent.  [CLAUDE
    2026-11-17, user-assigned change]"""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
        except (AttributeError, OSError):
            ctypes.windll.user32.SetProcessDPIAware()
    except Exception:  # noqa: BLE001 - blurry beats not starting
        pass


def main():
    _enable_sharp_rendering()

    import display_data

    return display_data.main()


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
