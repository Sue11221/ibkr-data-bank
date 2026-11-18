#!/usr/bin/env python3
"""Portable engine-suite entry point for the Row 79 schedule generator battery."""

from __future__ import annotations

from pathlib import Path
import runpy
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOLS_ROOT = PROJECT_ROOT / "tools"
sys.path.insert(0, str(TOOLS_ROOT))
runpy.run_path(
    str(TOOLS_ROOT / "gen_session_schedule_selftest.py"),
    run_name="__main__",
)
