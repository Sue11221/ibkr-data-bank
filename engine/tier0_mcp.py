"""Dependency-free stdio MCP adapter for the fixed Tier 0 query facade.

Only eight read-only tools are registered. The adapter owns protocol framing,
closed input schemas, and error translation; all application behavior remains
in tier0_queries.py.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tier0_queries as queries  # noqa: E402


SERVER_NAME = "ema-crossover-tier0"
SERVER_TITLE = "Data Bank Tier 0"
SERVER_VERSION = "1.0.0"
LATEST_PROTOCOL = "2025-06-18"
SUPPORTED_PROTOCOLS = frozenset({
    LATEST_PROTOCOL, "2025-03-26", "2024-11-05",
})
MAX_REQUEST_BYTES = 1024 * 1024
MAX_MESSAGE_BYTES = 2 * queries.MAX_OUTPUT_BYTES + 128 * 1024

INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
PARSE_ERROR = -32700
NOT_INITIALIZED = -32002

_TICKER = {
    "type": "string",
    "minLength": 1,
    "maxLength": 20,
    "pattern": r"^[A-Z][A-Z0-9-]*$",
    "description": "Canonical uppercase storage ticker.",
}
_CURSOR = {
    "type": "integer",
    "minimum": 0,
    "description": "Zero-based result cursor.",
}
_LIMIT = {
    "type": "integer",
    "minimum": 1,
    "maximum": queries.MAX_ROWS,
    "description": "Maximum rows to return.",
}


class ProtocolError(RuntimeError):
    def __init__(self, code, message, data=None):
        self.code = int(code)
        self.message = str(message)[:queries.MAX_STRING]
        self.data = data if isinstance(data, dict) else None
        super().__init__(self.message)


def _object_schema(properties=None, required=None):
    schema = {
        "type": "object",
        "properties": dict(properties or {}),
        "additionalProperties": False,
    }
    if required:
        schema["required"] = list(required)
    return schema


def _annotations():
    return {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }


def _tool(name, title, description, schema, command, handler):
    return {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": schema,
        "annotations": _annotations(),
        "_command": command,
        "_handler": handler,
    }


def _bank_health(_args):
    return queries.bank_health_summary()


def _ticker_status(args):
    return queries.ticker_status(args["ticker"])


def _coverage_status(args):
    return queries.coverage_status(
        ticker=args.get("ticker"), cursor=args.get("cursor", 0),
        limit=args.get("limit", 50))


def _split_status(args):
    return queries.split_status(
        args["ticker"], cursor=args.get("cursor", 0),
        limit=args.get("limit", 50))


def _cached_gaps(args):
    return queries.cached_gap_summary(
        ticker=args.get("ticker"), interval=args.get("interval"),
        cursor=args.get("cursor", 0), limit=args.get("limit", 50))


def _repair_queue(args):
    return queries.repair_queue(
        cursor=args.get("cursor", 0), limit=args.get("limit", 50))


def _history_list(args):
    return queries.history_list(
        ticker=args.get("ticker"), kind=args.get("kind"),
        cursor=args.get("cursor", 0), limit=args.get("limit", 20))


def _run_result(args):
    return queries.run_result(
        args["run_id"], view=args.get("view", "summary"),
        cursor=args.get("cursor", 0), limit=args.get("limit", 50))


TOOLS = (
    _tool(
        "tier0_bank_health", "Bank Health",
        "Read the latest fixed-root offline bank-health summary.",
        _object_schema(), "bank-health", _bank_health),
    _tool(
        "tier0_ticker_status", "Ticker Status",
        "Read current offline series, coverage, gap, correction, split, and "
        "historical references for one ticker. Split status runs one offline "
        "per-ticker audit for this call.",
        _object_schema({"ticker": _TICKER}, ["ticker"]),
        "ticker-status", _ticker_status),
    _tool(
        "tier0_coverage_status", "Coverage Status",
        "Read cached coverage status for one ticker or a bounded bank page.",
        _object_schema({"ticker": _TICKER, "cursor": _CURSOR, "limit": _LIMIT}),
        "coverage-status", _coverage_status),
    _tool(
        "tier0_split_status", "Split Status",
        "Run one offline per-ticker split audit and return its bounded "
        "split-cache/audit status.",
        _object_schema(
            {"ticker": _TICKER, "cursor": _CURSOR, "limit": _LIMIT},
            ["ticker"]),
        "split-status", _split_status),
    _tool(
        "tier0_cached_gaps", "Cached Gaps",
        "Read the cached gap report without rescanning or writing it.",
        _object_schema({
            "ticker": _TICKER,
            "interval": {
                "type": "string", "minLength": 1, "maxLength": 32,
                "description": "Canonical stored interval token.",
            },
            "cursor": _CURSOR,
            "limit": _LIMIT,
        }),
        "cached-gaps", _cached_gaps),
    _tool(
        "tier0_repair_queue", "Repair Queue",
        "Read the bounded repair queue from the cached health report.",
        _object_schema({"cursor": _CURSOR, "limit": _LIMIT}),
        "repair-queue", _repair_queue),
    _tool(
        "tier0_history_list", "Historical Runs",
        "List digest-gated historical run summaries with reviewed dispositions.",
        _object_schema({
            "ticker": _TICKER,
            "kind": {
                "type": "string", "minLength": 1, "maxLength": 64,
                "pattern": r"^[a-z][a-z0-9_]*$",
            },
            "cursor": _CURSOR,
            "limit": _LIMIT,
        }),
        "history-list", _history_list),
    _tool(
        "tier0_run_result", "Historical Run Result",
        "Read one digest-verified semantic view of a cataloged historical run.",
        _object_schema({
            "run_id": {
                "type": "string", "minLength": 1, "maxLength": 128,
                "pattern": queries.RUN_ID_RE.pattern,
            },
            "view": {
                "type": "string", "enum": sorted(queries.ALLOWED_VIEWS),
            },
            "cursor": _CURSOR,
            "limit": _LIMIT,
        }, ["run_id"]),
        "run-result", _run_result),
)

TOOL_BY_NAME = {tool["name"]: tool for tool in TOOLS}


def public_tools():
    return [
        {key: value for key, value in tool.items() if not key.startswith("_")}
        for tool in TOOLS
    ]


def _canonical_json(value):
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise queries.Tier0Error(
            "invalid_output", "MCP result is not valid JSON") from exc
    if len(encoded) > queries.MAX_OUTPUT_BYTES:
        raise queries.Tier0Error(
            "output_too_large",
            f"MCP result exceeds the {queries.MAX_OUTPUT_BYTES}-byte data cap")
    return encoded.decode("utf-8")


def _type_matches(expected, value):
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "object":
        return isinstance(value, dict)
    return False


def _validate_arguments(tool, arguments):
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise ProtocolError(INVALID_PARAMS, "tool arguments must be an object")
    schema = tool["inputSchema"]
    properties = schema.get("properties") or {}
    extras = sorted(set(arguments) - set(properties))
    if extras:
        raise ProtocolError(
            INVALID_PARAMS,
            "unknown tool argument(s): " + ", ".join(extras))
    missing = [key for key in schema.get("required", []) if key not in arguments]
    if missing:
        raise ProtocolError(
            INVALID_PARAMS,
            "missing required tool argument(s): " + ", ".join(missing))
    for key, value in arguments.items():
        rule = properties[key]
        expected = rule.get("type")
        if not _type_matches(expected, value):
            raise ProtocolError(
                INVALID_PARAMS, f"tool argument {key} must be {expected}")
        if expected == "string":
            if len(value) < int(rule.get("minLength", 0)):
                raise ProtocolError(INVALID_PARAMS, f"tool argument {key} is empty")
            if len(value) > int(rule.get("maxLength", queries.MAX_STRING)):
                raise ProtocolError(INVALID_PARAMS, f"tool argument {key} is too long")
            if "pattern" in rule and re.fullmatch(rule["pattern"], value) is None:
                raise ProtocolError(
                    INVALID_PARAMS, f"tool argument {key} is not canonical")
            if "enum" in rule and value not in rule["enum"]:
                raise ProtocolError(
                    INVALID_PARAMS, f"tool argument {key} is unsupported")
        elif expected == "integer":
            if "minimum" in rule and value < rule["minimum"]:
                raise ProtocolError(
                    INVALID_PARAMS, f"tool argument {key} is below minimum")
            if "maximum" in rule and value > rule["maximum"]:
                raise ProtocolError(
                    INVALID_PARAMS, f"tool argument {key} exceeds maximum")
    return dict(arguments)


def _tool_error(exc):
    error = {
        "schema_version": queries.SCHEMA_VERSION,
        "ok": False,
        "error": {
            "code": exc.code,
            "message": queries._safe_string(exc.message),
        },
    }
    if exc.details:
        error["error"]["details"] = queries._sanitize(exc.details)
    return {
        "content": [{"type": "text", "text": _canonical_json(error)}],
        "isError": True,
    }


def call_tool(name, arguments=None):
    tool = TOOL_BY_NAME.get(name)
    if tool is None:
        raise ProtocolError(INVALID_PARAMS, f"unknown tool: {name}")
    args = _validate_arguments(tool, arguments)
    try:
        data = tool["_handler"](args)
        text = _canonical_json(data)
        return {
            "content": [{"type": "text", "text": text}],
            "structuredContent": data,
            "isError": False,
        }
    except queries.Tier0Error as exc:
        return _tool_error(exc)
    except Exception as exc:  # noqa: BLE001 - no traceback/details cross stdout
        error = queries.Tier0Error(
            "internal_error", f"Tier 0 query failed: {type(exc).__name__}",
            exit_code=4)
        return _tool_error(error)


def _response(request_id, result):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error_response(request_id, error):
    payload = {"code": error.code, "message": error.message}
    if error.data:
        payload["data"] = error.data
    return {"jsonrpc": "2.0", "id": request_id, "error": payload}


def _request_id(message):
    if "id" not in message:
        return None, False
    value = message["id"]
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ProtocolError(INVALID_REQUEST, "request id must be a string or integer")
    return value, True


class Server:
    def __init__(self):
        self.initialized = False
        self.ready = False
        self.protocol_version = None

    def _initialize(self, params):
        if self.initialized:
            raise ProtocolError(INVALID_REQUEST, "server is already initialized")
        if not isinstance(params, dict):
            raise ProtocolError(INVALID_PARAMS, "initialize params must be an object")
        version = params.get("protocolVersion")
        if not isinstance(version, str) or not version:
            raise ProtocolError(INVALID_PARAMS, "protocolVersion is required")
        capabilities = params.get("capabilities")
        client_info = params.get("clientInfo")
        if not isinstance(capabilities, dict) or not isinstance(client_info, dict):
            raise ProtocolError(
                INVALID_PARAMS, "client capabilities and clientInfo are required")
        selected = version if version in SUPPORTED_PROTOCOLS else LATEST_PROTOCOL
        self.initialized = True
        self.protocol_version = selected
        return {
            "protocolVersion": selected,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {
                "name": SERVER_NAME,
                "title": SERVER_TITLE,
                "version": SERVER_VERSION,
            },
            "instructions": (
                "Eight fixed-root, offline, read-only Tier 0 status tools. "
                "Historical run results are dated evidence, never current health. "
                "No refresh, fetch, write, shell, or arbitrary file access exists."
            ),
        }

    def handle(self, message):
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return _error_response(
                None, ProtocolError(INVALID_REQUEST, "invalid JSON-RPC request"))
        request_id = None
        try:
            request_id, has_id = _request_id(message)
            method = message.get("method")
            if not isinstance(method, str) or not method:
                raise ProtocolError(INVALID_REQUEST, "request method is required")
            params = message.get("params", {})

            if not has_id:
                if method == "notifications/initialized" and self.initialized:
                    self.ready = True
                return None
            if method == "initialize":
                return _response(request_id, self._initialize(params))
            if method == "ping":
                return _response(request_id, {})
            if not self.initialized or not self.ready:
                raise ProtocolError(NOT_INITIALIZED, "server is not initialized")
            if method == "tools/list":
                if params is None:
                    params = {}
                if not isinstance(params, dict):
                    raise ProtocolError(INVALID_PARAMS, "tools/list params invalid")
                extras = set(params) - {"cursor", "_meta"}
                if extras or params.get("cursor") not in (None, ""):
                    raise ProtocolError(INVALID_PARAMS, "tools/list cursor invalid")
                return _response(request_id, {"tools": public_tools()})
            if method == "tools/call":
                if not isinstance(params, dict):
                    raise ProtocolError(INVALID_PARAMS, "tools/call params invalid")
                extras = set(params) - {"name", "arguments", "_meta"}
                if extras:
                    raise ProtocolError(INVALID_PARAMS, "tools/call params invalid")
                name = params.get("name")
                if not isinstance(name, str) or not name:
                    raise ProtocolError(INVALID_PARAMS, "tool name is required")
                return _response(
                    request_id, call_tool(name, params.get("arguments")))
            raise ProtocolError(METHOD_NOT_FOUND, f"method not found: {method}")
        except ProtocolError as exc:
            return _error_response(request_id, exc)
        except Exception as exc:  # noqa: BLE001 - protocol never leaks details
            return _error_response(
                request_id,
                ProtocolError(
                    INTERNAL_ERROR, f"internal server error: {type(exc).__name__}"))


def encode_message(message):
    raw = json.dumps(
        message, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("utf-8")
    if len(raw) > MAX_MESSAGE_BYTES:
        raise ProtocolError(INTERNAL_ERROR, "MCP response exceeds message cap")
    return raw + b"\n"


def _drain_line(stream):
    while True:
        chunk = stream.readline(MAX_REQUEST_BYTES + 1)
        if not chunk or chunk.endswith(b"\n"):
            return


def serve(stdin=None, stdout=None):
    stdin = stdin or sys.stdin.buffer
    stdout = stdout or sys.stdout.buffer
    server = Server()
    while True:
        raw = stdin.readline(MAX_REQUEST_BYTES + 1)
        if not raw:
            return 0
        if len(raw) > MAX_REQUEST_BYTES:
            if not raw.endswith(b"\n"):
                _drain_line(stdin)
            response = _error_response(
                None, ProtocolError(INVALID_REQUEST, "MCP request exceeds size cap"))
        else:
            try:
                message = json.loads(raw.decode("utf-8"))
            except (UnicodeError, ValueError):
                response = _error_response(
                    None, ProtocolError(PARSE_ERROR, "invalid JSON"))
            else:
                response = server.handle(message)
        if response is None:
            continue
        try:
            encoded = encode_message(response)
        except Exception as exc:  # noqa: BLE001 - bounded fallback only
            encoded = json.dumps({
                "jsonrpc": "2.0",
                "id": response.get("id") if isinstance(response, dict) else None,
                "error": {
                    "code": INTERNAL_ERROR,
                    "message": f"internal server error: {type(exc).__name__}",
                },
            }, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
        stdout.write(encoded)
        stdout.flush()


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv:
        print("tier0_mcp.py accepts no command-line arguments", file=sys.stderr)
        return 2
    return serve()


if __name__ == "__main__":
    raise SystemExit(main())
