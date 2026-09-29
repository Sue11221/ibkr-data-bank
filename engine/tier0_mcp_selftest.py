"""Offline acceptance tests for the Tier 0 stdio MCP adapter."""

from __future__ import annotations

import ast
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import tier0_cli as cli  # noqa: E402
import tier0_mcp as mcp  # noqa: E402
import tier0_queries as queries  # noqa: E402
import tier0_queries_selftest as base  # noqa: E402


FAILURES = []
COUNT = [0]
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def check(name, condition, detail=""):
    COUNT[0] += 1
    ok = bool(condition)
    print(f"[{'PASS' if ok else 'FAIL'}] {name}"
          + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILURES.append(name)


def expect_error(name, code, fn):
    try:
        fn()
    except mcp.ProtocolError as exc:
        check(name, exc.code == code, f"got {exc.code}: {exc}")
    except Exception as exc:  # noqa: BLE001
        check(name, False, f"got {type(exc).__name__}: {exc}")
    else:
        check(name, False, "no exception")


def initialize(server, version=mcp.LATEST_PROTOCOL):
    response = server.handle({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "tier0-selftest", "version": "1"},
        },
    })
    server.handle({
        "jsonrpc": "2.0",
        "method": "notifications/initialized",
        "params": {},
    })
    return response


def tool_call(server, request_id, name, arguments=None):
    return server.handle({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments or {}},
    })


def cli_call(argv):
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        code = cli.main(argv)
    text = stream.getvalue()
    return code, json.loads(text), text


def registry_and_config_tests():
    expected = {
        "tier0_bank_health", "tier0_ticker_status", "tier0_coverage_status",
        "tier0_split_status", "tier0_cached_gaps", "tier0_repair_queue",
        "tier0_history_list", "tier0_run_result",
    }
    tools = mcp.public_tools()
    check("registry exposes exactly the eight reviewed tools",
          {tool["name"] for tool in tools} == expected and len(tools) == 8)
    check("registry names are unique and implementation keys stay private",
          len(mcp.TOOL_BY_NAME) == len(mcp.TOOLS)
          and all(not any(key.startswith("_") for key in tool)
                  for tool in tools))
    check("every input schema is a closed object",
          all(tool["inputSchema"].get("type") == "object"
              and tool["inputSchema"].get("additionalProperties") is False
              for tool in tools))
    check("every tool is read-only, non-destructive, idempotent, closed-world",
          all(tool["annotations"] == {
              "readOnlyHint": True,
              "destructiveHint": False,
              "idempotentHint": True,
              "openWorldHint": False,
          } for tool in tools))

    client_config = json.loads(
        (PROJECT_ROOT / ".mcp.json").read_text("utf-8"))
    client_server = client_config["mcpServers"]["ema_tier0"]
    check("client config launches the dependency-free entry point",
          client_server["command"] == "python"
          and client_server["args"] == ["engine/tier0_mcp.py"]
          and client_server["cwd"] == ".")
    check("client declaration contains no credentials or environment values",
          client_server.get("env") == {})

    source = Path(mcp.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    calls = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
        elif (isinstance(node, ast.Call)
              and isinstance(node.func, ast.Attribute)):
            calls.add(node.func.attr)
    check("adapter import graph has no network, process, IBKR, or shell module",
          not imports.intersection({
              "socket", "urllib", "subprocess", "stock_ibkr", "os", "shutil"})
          and "stock_ibkr" not in sys.modules, str(sorted(imports)))
    check("adapter source has no file mutation API call",
          not calls.intersection({
              "write_text", "write_bytes", "mkdir", "rename", "replace",
              "unlink", "rmdir", "touch", "save_manifest", "open",
          }), str(sorted(calls)))


def protocol_tests():
    server = mcp.Server()
    before = server.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    check("lifecycle blocks tools before initialized notification",
          before["error"]["code"] == mcp.NOT_INITIALIZED)

    response = initialize(server, "2025-03-26")
    result = response["result"]
    check("initialize echoes a supported client protocol",
          result["protocolVersion"] == "2025-03-26"
          and result["capabilities"] == {"tools": {"listChanged": False}})
    check("initialize advertises only the tools capability",
          set(result["capabilities"]) == {"tools"}
          and "read-only" in result["instructions"])
    check("ping works after initialization",
          server.handle({
              "jsonrpc": "2.0", "id": "p", "method": "ping",
              "params": {}})["result"] == {})

    listed = server.handle({
        "jsonrpc": "2.0", "id": 2, "method": "tools/list",
        "params": {"_meta": {"progressToken": "x"}}})
    check("tools/list returns all fixed tools without pagination",
          len(listed["result"]["tools"]) == 8
          and "nextCursor" not in listed["result"])
    bad_cursor = server.handle({
        "jsonrpc": "2.0", "id": 3, "method": "tools/list",
        "params": {"cursor": "unknown"}})
    check("tools/list rejects an invented cursor",
          bad_cursor["error"]["code"] == mcp.INVALID_PARAMS)

    unknown_method = server.handle({
        "jsonrpc": "2.0", "id": 4, "method": "resources/list",
        "params": {}})
    check("unregistered protocol methods fail deterministically",
          unknown_method["error"]["code"] == mcp.METHOD_NOT_FOUND)
    unknown_tool = tool_call(server, 5, "tier0_not_real")
    check("unknown tools are protocol errors",
          unknown_tool["error"]["code"] == mcp.INVALID_PARAMS)
    extra = tool_call(server, 6, "tier0_bank_health", {"root": "."})
    check("closed schemas reject caller-selected root/path fields",
          extra["error"]["code"] == mcp.INVALID_PARAMS)
    missing = tool_call(server, 7, "tier0_ticker_status", {})
    check("closed schemas enforce required fields",
          missing["error"]["code"] == mcp.INVALID_PARAMS)
    lowercase = tool_call(server, 8, "tier0_ticker_status", {"ticker": "ftnt"})
    check("schema rejects noncanonical lowercase ticker",
          lowercase["error"]["code"] == mcp.INVALID_PARAMS)
    boolean_cursor = tool_call(
        server, 9, "tier0_repair_queue", {"cursor": True})
    check("schema rejects booleans masquerading as integers",
          boolean_cursor["error"]["code"] == mcp.INVALID_PARAMS)

    fallback = mcp.Server()
    fallback_response = initialize(fallback, "2099-01-01")
    check("unknown protocol version negotiates the latest supported version",
          fallback_response["result"]["protocolVersion"]
          == mcp.LATEST_PROTOCOL)
    repeated = fallback.handle({
        "jsonrpc": "2.0", "id": 4, "method": "initialize",
        "params": {
            "protocolVersion": mcp.LATEST_PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "again", "version": "1"},
        }})
    check("second initialize request is rejected",
          repeated["error"]["code"] == mcp.INVALID_REQUEST)
    check("unknown notifications produce no response",
          fallback.handle({
              "jsonrpc": "2.0", "method": "notifications/unknown",
              "params": {}}) is None)

    invalid_id = mcp.Server().handle({
        "jsonrpc": "2.0", "id": True, "method": "ping"})
    check("invalid JSON-RPC request ids are rejected",
          invalid_id["error"]["code"] == mcp.INVALID_REQUEST
          and invalid_id["id"] is None)
    invalid_shape = mcp.Server().handle([])
    check("JSON-RPC batches/non-objects are rejected",
          invalid_shape["error"]["code"] == mcp.INVALID_REQUEST)

    raw_in = io.BytesIO(
        b"not-json\n"
        + json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": mcp.LATEST_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "transport", "version": "1"},
            }}).encode("utf-8") + b"\n")
    raw_out = io.BytesIO()
    check("stdio server exits cleanly at EOF", mcp.serve(raw_in, raw_out) == 0)
    transport_lines = raw_out.getvalue().splitlines()
    check("stdio transport emits one JSON message per response line",
          len(transport_lines) == 2
          and json.loads(transport_lines[0])["error"]["code"] == mcp.PARSE_ERROR
          and json.loads(transport_lines[1])["id"] == 1)

    too_large = io.BytesIO(b"x" * (mcp.MAX_REQUEST_BYTES + 1) + b"\n")
    too_large_out = io.BytesIO()
    mcp.serve(too_large, too_large_out)
    check("stdio transport bounds request bytes",
          json.loads(too_large_out.getvalue())["error"]["code"]
          == mcp.INVALID_REQUEST)
    expect_error(
        "protocol encoder bounds complete response bytes",
        mcp.INTERNAL_ERROR,
        lambda: mcp.encode_message({"x": "y" * mcp.MAX_MESSAGE_BYTES}))


def parity_and_safety_tests(fixture):
    cases = [
        ("tier0_bank_health", {}, ["bank-health"]),
        ("tier0_ticker_status", {"ticker": "FTNT"},
         ["ticker-status", "--ticker", "FTNT"]),
        ("tier0_coverage_status", {
            "ticker": "FTNT", "cursor": 0, "limit": 5},
         ["coverage-status", "--ticker", "FTNT", "--cursor", "0",
          "--limit", "5"]),
        ("tier0_split_status", {
            "ticker": "FTNT", "cursor": 0, "limit": 5},
         ["split-status", "--ticker", "FTNT", "--cursor", "0",
          "--limit", "5"]),
        ("tier0_cached_gaps", {
            "ticker": "FTNT", "cursor": 0, "limit": 5},
         ["cached-gaps", "--ticker", "FTNT", "--cursor", "0",
          "--limit", "5"]),
        ("tier0_repair_queue", {"cursor": 0, "limit": 5},
         ["repair-queue", "--cursor", "0", "--limit", "5"]),
        ("tier0_history_list", {
            "ticker": "FTNT", "cursor": 0, "limit": 5},
         ["history-list", "--ticker", "FTNT", "--cursor", "0",
          "--limit", "5"]),
        ("tier0_run_result", {
            "run_id": fixture["ftnt_old"], "view": "summary",
            "cursor": 0, "limit": 5},
         ["run-result", "--run-id", fixture["ftnt_old"],
          "--view", "summary", "--cursor", "0", "--limit", "5"]),
    ]
    for tool_name, arguments, cli_args in cases:
        mcp_result = mcp.call_tool(tool_name, arguments)
        code, cli_payload, cli_text = cli_call(cli_args)
        parsed_text = json.loads(mcp_result["content"][0]["text"])
        check(f"parity: {tool_name} data equals CLI data",
              code == 0 and cli_payload["ok"] is True
              and mcp_result["isError"] is False
              and mcp_result["structuredContent"] == cli_payload["data"]
              and parsed_text == cli_payload["data"], cli_text[:300])
        check(f"bounds: {tool_name} text stays within query cap",
              len(mcp_result["content"][0]["text"].encode("utf-8"))
              <= queries.MAX_OUTPUT_BYTES)

    mcp_error = mcp.call_tool("tier0_ticker_status", {"ticker": "NOPE"})
    cli_code, cli_payload, cli_text = cli_call([
        "ticker-status", "--ticker", "NOPE"])
    error_payload = json.loads(mcp_error["content"][0]["text"])
    check("parity: query validation/not-found errors keep the CLI error code",
          cli_code != 0 and mcp_error["isError"] is True
          and "structuredContent" not in mcp_error
          and error_payload["error"]["code"]
          == cli_payload["error"]["code"], cli_text)

    original_handler = mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"]
    mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"] = (
        lambda _args: (_ for _ in ()).throw(RuntimeError("secret details")))
    try:
        internal = mcp.call_tool("tier0_bank_health", {})
    finally:
        mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"] = original_handler
    internal_text = internal["content"][0]["text"]
    check("unexpected tool failures expose type only, never details/traceback",
          internal["isError"] is True
          and "RuntimeError" in internal_text
          and "secret details" not in internal_text
          and "Traceback" not in internal_text, internal_text)

    original_handler = mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"]
    mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"] = (
        lambda _args: {"rows": ["x" * 1000] * 300})
    try:
        oversized = mcp.call_tool("tier0_bank_health", {})
    finally:
        mcp.TOOL_BY_NAME["tier0_bank_health"]["_handler"] = original_handler
    oversized_payload = json.loads(oversized["content"][0]["text"])
    check("oversized MCP data becomes an error with no partial structured data",
          oversized["isError"] is True
          and "structuredContent" not in oversized
          and oversized_payload["error"]["code"] == "output_too_large")

    before = base.snapshot(fixture["bank"])
    with base.deny_writes_network_processes():
        safe_results = [
            mcp.call_tool(tool_name, arguments)
            for tool_name, arguments, _cli_args in cases
        ]
    check("purity: every MCP tool passes write/network/process traps",
          all(result["isError"] is False for result in safe_results))
    check("purity: complete fixture bank bytes/metadata stay unchanged",
          base.snapshot(fixture["bank"]) == before)


def race_tests(fixture):
    original_state = queries._file_state
    calls = [0]

    def raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == queries.HEALTH_REPORT:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["mtime_ns"] += 1
        return state

    queries._file_state = raced
    try:
        result = mcp.call_tool("tier0_bank_health", {})
    finally:
        queries._file_state = original_state
    payload = json.loads(result["content"][0]["text"])
    check("race: sidecar change fails closed as MCP tool error",
          result["isError"] is True
          and "structuredContent" not in result
          and payload["error"]["code"] == "evidence_changed", str(payload))
    check("race: no partial health payload escapes",
          "data" not in payload and "component_status" not in payload)

    record = next(
        row for row in fixture["records"]
        if row["run_id"] == fixture["ftnt_old"])
    calls = [0]

    def artifact_raced(path_arg, **kwargs):
        state = original_state(path_arg, **kwargs)
        if Path(path_arg).name == record["artifact"]["basename"]:
            calls[0] += 1
            if calls[0] == 2:
                state = dict(state)
                state["ctime_ns"] += 1
        return state

    queries._file_state = artifact_raced
    try:
        result = mcp.call_tool(
            "tier0_run_result", {"run_id": fixture["ftnt_old"]})
    finally:
        queries._file_state = original_state
    payload = json.loads(result["content"][0]["text"])
    check("race: historical artifact change fails closed without result data",
          result["isError"] is True
          and payload["error"]["code"] == "evidence_changed"
          and "structuredContent" not in result)


def _child_command(fixture):
    code = r'''
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import tier0_queries as q
import tier0_mcp as m
q.MODULE_DIR = Path(sys.argv[2])
q.PROJECT_ROOT = Path(sys.argv[3])
q.STORAGE_ROOT = Path(sys.argv[4])
q.RUN_LOGS_ROOT = Path(sys.argv[5])
q.SCRIPT_ARCHIVE_ROOT = Path(sys.argv[6])
q.CATALOG_PATH = Path(sys.argv[7])
raise SystemExit(m.serve())
'''
    return [
        sys.executable, "-c", code,
        str(Path(__file__).resolve().parent),
        str(fixture["engine"]),
        str(fixture["project"]),
        str(fixture["bank"]),
        str(fixture["run_logs"]),
        str(fixture["project"] / "archive" / "_repair_scripts_archive"
            / "2026-07"),
        str(fixture["engine"] / "tier0_history_catalog.json"),
    ]


def process_tests(fixture):
    messages = [
        {
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": mcp.LATEST_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "parallel-test", "version": "1"},
            },
        },
        {
            "jsonrpc": "2.0", "method": "notifications/initialized",
            "params": {},
        },
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "tier0_ticker_status",
                "arguments": {"ticker": "FTNT"},
            },
        },
    ]
    raw = "".join(
        json.dumps(message, separators=(",", ":")) + "\n"
        for message in messages)
    before = base.snapshot(fixture["bank"])
    processes = [
        subprocess.Popen(
            _child_command(fixture), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for _ in range(2)
    ]
    for process in processes:
        process.stdin.write(raw)
        process.stdin.close()
    outputs = []
    for process in processes:
        stdout = process.stdout.read()
        stderr = process.stderr.read()
        code = process.wait(timeout=30)
        outputs.append((code, stdout, stderr))
    parsed = [
        [json.loads(line) for line in stdout.splitlines()]
        for _code, stdout, _stderr in outputs
    ]
    check("two MCP server processes run simultaneously and exit cleanly",
          all(code == 0 and not stderr and len(rows) == 3
              for (code, _stdout, stderr), rows in zip(outputs, parsed)),
          str(outputs))
    check("two processes expose identical eight-tool registries",
          all(len(rows[1]["result"]["tools"]) == 8 for rows in parsed)
          and parsed[0][1]["result"]["tools"] == parsed[1][1]["result"]["tools"])
    check("two processes return the same structured ticker result",
          parsed[0][2]["result"]["structuredContent"]
          == parsed[1][2]["result"]["structuredContent"]
          and not parsed[0][2]["result"]["isError"])
    check("two-process split timestamps never use the evaluation clock",
          all(rows[2]["result"]["structuredContent"]["current"]["split"]
              ["generated_at"] is None for rows in parsed))
    check("two-process reads leave complete fixture bank unchanged",
          base.snapshot(fixture["bank"]) == before)


def main():
    registry_and_config_tests()
    protocol_tests()
    fixture = base.build_fixture()
    try:
        with base.fixed_roots(fixture):
            parity_and_safety_tests(fixture)
            race_tests(fixture)
            process_tests(fixture)
    finally:
        import shutil
        shutil.rmtree(fixture["project"], ignore_errors=True)
    print()
    if FAILURES:
        print(f"{COUNT[0]} checks, {len(FAILURES)} failed")
        print(f"{len(FAILURES)} FAILURE(S): {FAILURES}")
        return 1
    print(f"{COUNT[0]} checks, 0 failed")
    print(f"TIER 0 MCP SELF-TESTS PASSED: {COUNT[0]}/{COUNT[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
