"""Deterministic tests for export_batch_folder.py."""

from __future__ import annotations

import datetime as dt
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_batch_folder as folder  # noqa: E402


FAILURES = []
COUNT = [0]
NOW = dt.datetime(
    2026, 7, 10, 18, 12, 34,
    tzinfo=dt.timezone(dt.timedelta(hours=-4)))


def check(name, condition, detail=""):
    COUNT[0] += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(name)


def expect_error(name, fn):
    try:
        fn()
    except folder.BatchFolderError:
        check(name, True)
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not raise")


def run():
    root = Path(tempfile.mkdtemp(prefix="export-folder-selftest-"))
    try:
        expected = "Export 2026-07-10 181234 - 502 tickers - 1m - parquet"
        check("name contract is deterministic",
              folder.folder_name(502, "1m", "parquet", now=NOW) == expected)
        check("unsafe tokens are normalized",
              folder.folder_name(2, "1m / RTH", "CSV:*", now=NOW).endswith(
                  "2 tickers - 1m-RTH - csv"))
        check("one ticker has a singular batch-folder name",
              folder.folder_name(1, "1m", "csv", now=NOW).endswith(
                  "1 ticker - 1m - csv"))
        expect_error("naive injected clock is rejected",
                     lambda: folder.folder_name(
                         2, "1m", "csv", now=dt.datetime(2026, 7, 10)))
        expect_error("empty safe token is rejected",
                     lambda: folder.safe_token("***", "format"))

        first = folder.create_batch_folder(root, 2, "1m", "csv", now=NOW)
        second = folder.create_batch_folder(root, 2, "1m", "csv", now=NOW)
        check("collision suffix starts at (2)",
              first.name == expected.replace("502", "2").replace(
                  "parquet", "csv")
              and second.name == first.name + " (2)")

        def claim(_index):
            return folder.create_batch_folder(
                root, 3, "1d", "json", now=NOW).name

        with ThreadPoolExecutor(max_workers=8) as pool:
            names = list(pool.map(claim, range(8)))
        check("concurrent claims are all unique",
              len(names) == len(set(names)) == 8)
        check("concurrent collision sequence is complete",
              sorted(names) == sorted([
                  "Export 2026-07-10 181234 - 3 tickers - 1d - json",
                  *[f"Export 2026-07-10 181234 - 3 tickers - 1d - json ({i})"
                    for i in range(2, 9)],
              ]))

        one, created = folder.prepare_destination(
            root, 1, "1m", "csv", now=NOW)
        check("single ticker prepares one stamped direct child",
              created is True and one.parent == root.resolve()
              and "1 ticker" in one.name)
        one_data, one_report = folder.bundle_paths(one, 1)
        check("single bundle stays flat",
              one_data == one
              and one_report == one / folder.HEALTH_REPORT_NAME)
        check("empty single helper folder is removed",
              folder.remove_created_folder_if_empty(one)
              and not one.exists())
        multi, created = folder.prepare_destination(
            root, 4, "1m", "csv", now=NOW)
        check("multi ticker prepares one direct child",
              created is True and multi.parent == root.resolve())
        multi_data, multi_report = folder.bundle_paths(multi, 4)
        check("multi bundle puts Data beside the report",
              multi_data == multi / folder.DATA_DIR_NAME
              and multi_report == multi / folder.HEALTH_REPORT_NAME)
        check("empty helper folder is removed",
              folder.remove_created_folder_if_empty(multi)
              and not multi.exists())

        nonempty = folder.create_batch_folder(root, 5, "1m", "csv", now=NOW)
        marker = nonempty / "keep.txt"
        marker.write_text("keep", encoding="ascii")
        check("nonempty helper folder is retained",
              folder.remove_created_folder_if_empty(nonempty) is False
              and marker.read_text(encoding="ascii") == "keep")
        arbitrary = root / "ordinary"
        arbitrary.mkdir()
        expect_error("cleanup refuses arbitrary directories",
                     lambda: folder.remove_created_folder_if_empty(arbitrary))
        expect_error("missing parent fails before creation",
                     lambda: folder.create_batch_folder(
                         root / "missing", 2, "1m", "csv", now=NOW))
        parent_file = root / "file-parent"
        parent_file.write_text("x", encoding="ascii")
        expect_error("file parent fails before creation",
                     lambda: folder.create_batch_folder(
                         parent_file, 2, "1m", "csv", now=NOW))
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    run()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)}/{COUNT[0]} checks: "
              + ", ".join(FAILURES))
        raise SystemExit(1)
    print(f"ALL PASS ({COUNT[0]}/{COUNT[0]})")
