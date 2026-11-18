"""Bounded JSON CLI for the fixed-root Tier 0 query surface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tier0_queries as queries  # noqa: E402


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise queries.Tier0Error(
            "invalid_arguments", message, exit_code=2)


def _page_args(parser, default):
    parser.add_argument("--cursor", type=int, default=0)
    parser.add_argument("--limit", type=int, default=default)


def _parser():
    parser = _Parser(description=__doc__, add_help=False)
    commands = parser.add_subparsers(dest="command", required=True)

    help_parser = commands.add_parser("help", add_help=False)
    help_parser.add_argument("--command", dest="help_for", choices=(
        "bank-health", "ticker-status", "coverage-status", "split-status",
        "cached-gaps", "repair-queue", "history-list", "run-result"))

    commands.add_parser("bank-health", add_help=False)

    ticker = commands.add_parser("ticker-status", add_help=False)
    ticker.add_argument("--ticker", required=True)

    coverage = commands.add_parser("coverage-status", add_help=False)
    coverage.add_argument("--ticker")
    _page_args(coverage, 50)

    split = commands.add_parser("split-status", add_help=False)
    split.add_argument("--ticker", required=True)
    _page_args(split, 50)

    gaps = commands.add_parser("cached-gaps", add_help=False)
    gaps.add_argument("--ticker")
    gaps.add_argument("--interval")
    _page_args(gaps, 50)

    queue = commands.add_parser("repair-queue", add_help=False)
    _page_args(queue, 50)

    history = commands.add_parser("history-list", add_help=False)
    history.add_argument("--ticker")
    history.add_argument("--kind")
    _page_args(history, 20)

    result = commands.add_parser("run-result", add_help=False)
    result.add_argument("--run-id", required=True)
    result.add_argument(
        "--view", default="summary", choices=sorted(queries.ALLOWED_VIEWS))
    _page_args(result, 50)
    return parser


def _execute(args):
    if args.command == "help":
        contracts = {
            "bank-health": [],
            "ticker-status": ["--ticker TICKER"],
            "coverage-status": ["--ticker TICKER", "--cursor N", "--limit N"],
            "split-status": ["--ticker TICKER", "--cursor N", "--limit N"],
            "cached-gaps": [
                "--ticker TICKER", "--interval INTERVAL", "--cursor N", "--limit N"],
            "repair-queue": ["--cursor N", "--limit N"],
            "history-list": [
                "--ticker TICKER", "--kind KIND", "--cursor N", "--limit N"],
            "run-result": [
                "--run-id ID", "--view VIEW", "--cursor N", "--limit N"],
        }
        help_for = args.help_for
        shown = ({help_for: contracts[help_for]} if help_for else contracts)
        return {
            "kind": "tier0_cli_help",
            "schema_version": queries.SCHEMA_VERSION,
            "fixed_roots": True,
            "commands": shown,
        }
    if args.command == "bank-health":
        return queries.bank_health_summary()
    if args.command == "ticker-status":
        return queries.ticker_status(args.ticker)
    if args.command == "coverage-status":
        return queries.coverage_status(
            ticker=args.ticker, cursor=args.cursor, limit=args.limit)
    if args.command == "split-status":
        return queries.split_status(
            args.ticker, cursor=args.cursor, limit=args.limit)
    if args.command == "cached-gaps":
        return queries.cached_gap_summary(
            ticker=args.ticker, interval=args.interval,
            cursor=args.cursor, limit=args.limit)
    if args.command == "repair-queue":
        return queries.repair_queue(cursor=args.cursor, limit=args.limit)
    if args.command == "history-list":
        return queries.history_list(
            ticker=args.ticker, kind=args.kind,
            cursor=args.cursor, limit=args.limit)
    if args.command == "run-result":
        return queries.run_result(
            args.run_id, view=args.view,
            cursor=args.cursor, limit=args.limit)
    raise queries.Tier0Error(
        "invalid_arguments", "unknown Tier 0 command", exit_code=2)


def _encoded(envelope):
    try:
        raw = json.dumps(
            envelope, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise queries.Tier0Error(
            "invalid_output", "CLI response is not valid JSON") from exc
    if len(raw) > queries.MAX_OUTPUT_BYTES:
        raise queries.Tier0Error(
            "output_too_large",
            f"CLI response exceeds the {queries.MAX_OUTPUT_BYTES}-byte cap")
    return raw.decode("utf-8")


def _error_envelope(command, exc):
    error = {
        "code": exc.code,
        "message": queries._safe_string(exc.message),
    }
    if exc.details:
        error["details"] = queries._sanitize(exc.details)
    return {
        "schema_version": queries.SCHEMA_VERSION,
        "ok": False,
        "command": command,
        "error": error,
    }


def main(argv=None):
    command = None
    try:
        argv = list(sys.argv[1:] if argv is None else argv)
        if argv in (["--help"], ["-h"]):
            argv = ["help"]
        elif len(argv) == 2 and argv[1] in {"--help", "-h"}:
            argv = ["help", "--command", argv[0]]
        args = _parser().parse_args(argv)
        command = args.command
        data = _execute(args)
        envelope = {
            "schema_version": queries.SCHEMA_VERSION,
            "ok": True,
            "command": command,
            "data": data,
        }
        print(_encoded(envelope))
        return 0
    except queries.Tier0Error as exc:
        print(_encoded(_error_envelope(command, exc)))
        return exc.exit_code
    except Exception as exc:  # noqa: BLE001 - never leak a traceback to clients
        error = queries.Tier0Error(
            "internal_error", f"Tier 0 query failed: {type(exc).__name__}",
            exit_code=4)
        print(_encoded(_error_envelope(command, error)))
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
