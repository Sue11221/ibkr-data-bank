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


def main():
    import display_data

    return display_data.main()


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(main())
