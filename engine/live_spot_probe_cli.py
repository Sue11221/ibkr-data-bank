"""Explicit JSON CLI for offline WS8 plans and authorized live probes."""

from __future__ import annotations

import argparse
import json
import sys

import live_spot_probe as probe


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
        prog="live_spot_probe_cli.py",
        description="Plan or run a report-only deterministic WS8 probe")
    commands = parser.add_subparsers(dest="command")
    plan = commands.add_parser(
        "plan", help="show stored ticker/days that would be probed")
    plan.add_argument("--seed", type=int)
    plan.add_argument("--count", type=int, default=probe.DEFAULT_COUNT)
    plan.add_argument(
        "--interval", choices=("1m", "1d", "1d-iv", "1d-hvol"),
        default="1m")
    live = commands.add_parser(
        "probe", help="run an explicitly authorized report-only live probe")
    live.add_argument("--allow-live", action="store_true")
    live.add_argument("--seed", type=int)
    live.add_argument("--count", type=int)
    live.add_argument("--ticker")
    live.add_argument("--day")
    live.add_argument("--port", type=int, default=2000)
    live.add_argument("--owner", default="manual")
    live.add_argument(
        "--interval", choices=("1m", "1d", "1d-iv", "1d-hvol"),
        default="1m")
    return parser


def _emit(payload):
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))


def main(argv=None, *, root=probe.STORAGE_ROOT, now=None, live_fetcher=None,
         gate_path=None, run_logs_root=probe.RUN_LOGS_ROOT):
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["plan"]
    command = None
    live_allowed = False
    try:
        args = _parser().parse_args(argv)
        command = args.command
        if args.command == "plan":
            result = probe.build_plan(
                root, seed=args.seed, count=args.count, now=now,
                interval=args.interval)
            exit_code = 1 if result["status"] != "ready" else 0
        elif args.command == "probe":
            if not args.allow_live:
                raise CliUsage(
                    "live probe requires the explicit --allow-live flag")
            live_allowed = True
            if (args.ticker is None) != (args.day is None):
                raise CliUsage("--ticker and --day must be provided together")
            if args.ticker is not None and args.count is not None:
                raise CliUsage(
                    "--count cannot be combined with --ticker and --day")
            result = probe.run_live_probe(
                root, seed=args.seed,
                count=(probe.DEFAULT_COUNT
                       if args.count is None else args.count),
                ticker=args.ticker, day=args.day, port=args.port,
                owner=args.owner, now=now, live_fetcher=live_fetcher,
                gate_path=gate_path, run_logs_root=run_logs_root,
                interval=args.interval)
            exit_code = 0 if result["status"] == "complete" else 1
        else:
            raise CliUsage("an explicit command is required")
    except Exception as exc:  # noqa: BLE001 - bounded JSON error envelope
        result = {
            "kind": "live_spot_probe_command_error",
            "version": probe.REPORT_VERSION,
            "network": False,
            "written": False,
            "bank_written": False,
            "artifact_written": False,
            "live_capability": command == "probe",
            "live_authorized": live_allowed,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc)[:500],
            },
        }
        exit_code = 2
    _emit(result)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
