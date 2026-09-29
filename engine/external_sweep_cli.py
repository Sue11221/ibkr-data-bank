"""Explicit JSON CLI for the WS7 external full-range sweep."""

from __future__ import annotations

import argparse
import json
import sys

import external_sweep as sweep


class CliUsage(RuntimeError):
    pass


class JsonParser(argparse.ArgumentParser):
    def error(self, message):
        raise CliUsage(str(message))

    def exit(self, status=0, message=None):
        if status == 0:
            raise CliUsage(self.format_help())
        raise CliUsage(str(message or "command failed"))


def _parser():
    parser = JsonParser(
        prog="external_sweep_cli.py",
        description="Offline WS7 cache status or explicit external sweep")
    commands = parser.add_subparsers(dest="command")

    status = commands.add_parser("status", help="offline cache readiness")
    status.add_argument("--ticker", action="append", default=[])

    run = commands.add_parser("sweep", help="explicit external network sweep")
    run.add_argument("--allow-network", action="store_true")
    run.add_argument("--all", action="store_true")
    run.add_argument("--ticker", action="append", default=[])
    run.add_argument("--owner", default="manual")
    run.add_argument("--pace", type=float, default=sweep.MIN_REQUEST_INTERVAL)
    run.add_argument("--attempts", type=int, default=sweep.DEFAULT_ATTEMPTS)
    run.add_argument("--backoff", type=float, default=sweep.DEFAULT_BACKOFF)
    run.add_argument("--timeout", type=float, default=20.0)
    return parser


def _emit(payload):
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))


def main(argv=None, *, provider=None, root=sweep.STORAGE_ROOT,
         cache_root=sweep.CACHE_ROOT, run_logs_root=sweep.RUN_LOGS_ROOT,
         gate_path=None, clock=None, sleep_fn=None, now=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["status"]
    parser = _parser()
    network = bool(argv and argv[0] == "sweep")
    try:
        args = parser.parse_args(argv)
        if args.command == "status":
            result = sweep.offline_status(
                root, args.ticker or None, cache_root=cache_root)
            exit_code = 0
        elif args.command == "sweep":
            if not args.allow_network:
                raise CliUsage(
                    "sweep requires --allow-network")
            if args.all and args.ticker:
                raise CliUsage("use either --all or --ticker, not both")
            if not args.all and not args.ticker:
                raise CliUsage(
                    "whole-bank sweep requires --all; otherwise pass --ticker")
            if provider is None:
                provider = sweep.StockAnalysisProvider(
                    timeout=args.timeout, now=now)
            kwargs = {}
            if clock is not None:
                kwargs["clock"] = clock
            if sleep_fn is not None:
                kwargs["sleep_fn"] = sleep_fn
            result = sweep.sweep_bank(
                root, None if args.all else args.ticker,
                provider=provider, cache_root=cache_root,
                run_logs_root=run_logs_root, owner=args.owner,
                pace=args.pace, attempts=args.attempts,
                backoff=args.backoff, gate_path=gate_path,
                now=now, **kwargs)
            exit_code = 1 if result["counts"]["UNVERIFIABLE"] else 0
        else:
            raise CliUsage("an explicit command is required")
    except Exception as exc:  # noqa: BLE001 - one bounded JSON error envelope
        result = {
            "kind": "external_sweep_command_error",
            "version": sweep.REPORT_VERSION,
            "network": network,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc)[:500],
            },
        }
        if isinstance(exc, sweep.PublicationRecoveryDebt):
            result["recovery_debt"] = exc.items
            if exc.report is not None and exc.report.get("artifact"):
                result["artifact"] = exc.report["artifact"]
        exit_code = 2
    _emit(result)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
