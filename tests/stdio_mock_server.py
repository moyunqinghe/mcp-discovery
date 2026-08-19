#!/usr/bin/env python3
"""MCP stdio mock server：只实现发现流程所需的最小交互。

作为 mcp-discovery 的测试素材;`handle_jsonrpc` 同时被 HTTP/SSE
transport 的测试夹具复用。
"""
from __future__ import annotations

import json
import sys
from typing import Any

SERVER_INFO = {"name": "feibot-mock-mcp", "version": "0.1.0"}
PROTOCOL_VERSION = "2024-11-05"

TOOLS: list[dict[str, Any]] = [
    {
        "name": "echo",
        "description": "Return the input text and its length.",
        "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
    },
    {
        "name": "sum",
        "description": "Sum a list of numbers.",
        "inputSchema": {"type": "object", "properties": {"numbers": {"type": "array"}}},
    },
    {
        "name": "product_lookup",
        "title": "商品查询",
        "description": "Look up demo product price data.",
        "inputSchema": {
            "type": "object",
            "properties": {"product_id": {"type": "string"}, "product_name": {"type": "string"}},
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
        "_meta": {"ui": {"visibility": ["model"]}},
    },
]


def handle_jsonrpc(request: dict[str, Any]) -> dict[str, Any] | None:
    """处理单个 JSON-RPC 请求,返回响应信封;通知/无 id 消息返回 None。"""
    method = request.get("method")
    request_id = request.get("id")
    if request_id is None:  # notification（如 notifications/initialized）
        return None
    if method == "initialize":
        return _result(
            request_id,
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": SERVER_INFO,
            },
        )
    if method == "tools/list":
        return _result(request_id, {"tools": TOOLS})
    if method == "tools/call":
        params = request.get("params") or {}
        name = str(params.get("name") or "")
        args = params.get("arguments") or {}
        if name == "echo":
            text = str(args.get("text") or "")
            return _result(
                request_id,
                {"content": [{"type": "text", "text": f"echo: {text}"}], "isError": False},
            )
        if name == "fail":
            return _result(
                request_id,
                {"content": [{"type": "text", "text": "boom: tool failed"}], "isError": True},
            )
        return _error(request_id, -32601, f"Unknown tool: {name}")
    return _error(request_id, -32601, f"Unsupported method: {method}")


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            continue
        response = handle_jsonrpc(request)
        if response is not None:
            print(json.dumps(response, ensure_ascii=False), flush=True)


def _result(request_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


if __name__ == "__main__":
    main()
