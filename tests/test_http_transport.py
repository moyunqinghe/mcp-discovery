"""Streamable HTTP transport:JSON/SSE 双格式响应体、Mcp-Session-Id 会话、错误映射。

单元测试用 httpx.MockTransport(transport 注入,与 wechat_ilink.WeChatClient
同款做法);round-trip 用一个真实的线程 HTTP server 走 discover() 全链路。
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from mcp_discovery import (
    CLIENT_INFO,
    CONNECT_FAILED,
    PROTOCOL_ERROR,
    PROTOCOL_VERSION,
    McpDiscoveryError,
    McpServerConfig,
    discover,
)
from mcp_discovery.client import _HttpSession
from stdio_mock_server import PROTOCOL_VERSION as MOCK_PROTOCOL_VERSION
from stdio_mock_server import handle_jsonrpc

# --------------------------------------------------------------------------- #
# MockTransport 夹具
# --------------------------------------------------------------------------- #


def _make_transport(
    handler: Callable[[dict], httpx.Response],
) -> tuple[httpx.MockTransport, list[dict]]:
    """包装 handler,顺带记录每次请求的 method/id/headers/params。"""
    seen: list[dict] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        seen.append(
            {
                "method": payload.get("method"),
                "id": payload.get("id"),
                "params": payload.get("params"),
                "headers": {key.lower(): value for key, value in request.headers.items()},
            }
        )
        return handler(payload)

    return httpx.MockTransport(wrapped), seen


def _envelope_response(payload: dict, *, sse: bool = False, extra_headers: dict | None = None) -> httpx.Response:
    envelope = handle_jsonrpc(payload) or {}
    body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    if sse:
        content = b"event: message\ndata: " + body + b"\n\n"
        headers = {"content-type": "text/event-stream"}
    else:
        content = body
        headers = {"content-type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    return httpx.Response(200, content=content, headers=headers)


def _standard_handler(payload: dict) -> httpx.Response:
    if payload.get("method") == "notifications/initialized":
        return httpx.Response(202)
    extra = {"mcp-session-id": "sess-1"} if payload.get("method") == "initialize" else None
    return _envelope_response(payload, extra_headers=extra)


def _open_session(transport: httpx.MockTransport) -> _HttpSession:
    config = McpServerConfig(transport="http", url="http://mcp.test/mcp")
    return _HttpSession(config, 5.0, PROTOCOL_VERSION, transport=transport)


# --------------------------------------------------------------------------- #
# 握手与会话
# --------------------------------------------------------------------------- #


def test_http_round_trip_json_body() -> None:
    transport, seen = _make_transport(_standard_handler)

    with _open_session(transport) as session:
        server = session.initialize()
        tools = session.list_tools()

    assert server["serverInfo"] == {"name": "feibot-mock-mcp", "version": "0.1.0"}
    assert [tool["name"] for tool in tools] == ["echo", "sum", "product_lookup"]

    methods = [entry["method"] for entry in seen]
    assert methods == ["initialize", "notifications/initialized", "tools/list"]
    assert seen[1]["id"] is None  # 通知没有 id

    first_headers = seen[0]["headers"]
    assert "application/json" in first_headers["accept"]
    assert "text/event-stream" in first_headers["accept"]
    assert "mcp-session-id" not in first_headers
    assert seen[2]["headers"]["mcp-session-id"] == "sess-1"


def test_http_sse_body_is_parsed() -> None:
    def handler(payload: dict) -> httpx.Response:
        if payload.get("method") == "notifications/initialized":
            return httpx.Response(202)
        extra = {"mcp-session-id": "sess-sse"} if payload.get("method") == "initialize" else None
        return _envelope_response(payload, sse=True, extra_headers=extra)

    transport, seen = _make_transport(handler)

    with _open_session(transport) as session:
        server = session.initialize()
        tools = session.list_tools()

    assert server["protocolVersion"] == MOCK_PROTOCOL_VERSION
    assert len(tools) == 3
    assert seen[2]["headers"]["mcp-session-id"] == "sess-sse"


def test_http_initialize_params_carry_version_and_client_info() -> None:
    transport, seen = _make_transport(_standard_handler)

    config = McpServerConfig(transport="http", url="http://mcp.test/mcp")
    session = _HttpSession(
        config,
        5.0,
        "2024-11-05",
        client_info={"name": "feibot", "version": "9.9"},
        transport=transport,
    )
    with session:
        session.initialize()

    params = seen[0]["params"]
    assert params["protocolVersion"] == "2024-11-05"
    assert params["clientInfo"] == {"name": "feibot", "version": "9.9"}
    assert params["capabilities"] == {}


def test_http_default_initialize_params() -> None:
    transport, seen = _make_transport(_standard_handler)

    with _open_session(transport) as session:
        session.initialize()

    params = seen[0]["params"]
    assert params["protocolVersion"] == PROTOCOL_VERSION
    assert params["clientInfo"] == dict(CLIENT_INFO)


def test_http_custom_headers_are_sent() -> None:
    transport, seen = _make_transport(_standard_handler)

    config = McpServerConfig(
        transport="http",
        url="http://mcp.test/mcp",
        headers={"Authorization": "Bearer token-1"},
    )
    session = _HttpSession(config, 5.0, PROTOCOL_VERSION, transport=transport)
    with session:
        session.initialize()

    assert seen[0]["headers"]["authorization"] == "Bearer token-1"


# --------------------------------------------------------------------------- #
# 错误映射
# --------------------------------------------------------------------------- #


def test_http_status_error_maps_to_connect_failed() -> None:
    transport, _ = _make_transport(lambda payload: httpx.Response(500, text="boom"))

    with pytest.raises(McpDiscoveryError, match="500") as captured:
        with _open_session(transport) as session:
            session.initialize()

    assert captured.value.code == CONNECT_FAILED


def test_http_connection_error_maps_to_connect_failed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    transport = httpx.MockTransport(handler)

    with pytest.raises(McpDiscoveryError, match="connection refused") as captured:
        with _open_session(transport) as session:
            session.initialize()

    assert captured.value.code == CONNECT_FAILED
    assert isinstance(captured.value.cause, httpx.ConnectError)


def test_http_jsonrpc_error_maps_to_protocol_error() -> None:
    def handler(payload: dict) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "error": {"code": -32601, "message": "no such method"},
            },
        )

    transport, _ = _make_transport(handler)

    with pytest.raises(McpDiscoveryError, match="no such method") as captured:
        with _open_session(transport) as session:
            session.initialize()

    assert captured.value.code == PROTOCOL_ERROR


def test_http_non_object_body_maps_to_protocol_error() -> None:
    transport, _ = _make_transport(
        lambda payload: httpx.Response(200, json=[1, 2, 3])
    )

    with pytest.raises(McpDiscoveryError, match="不是 JSON-RPC object") as captured:
        with _open_session(transport) as session:
            session.initialize()

    assert captured.value.code == PROTOCOL_ERROR


def test_http_sse_body_without_data_maps_to_protocol_error() -> None:
    transport, _ = _make_transport(
        lambda payload: httpx.Response(
            200, content=b"event: ping\n\n", headers={"content-type": "text/event-stream"}
        )
    )

    with pytest.raises(McpDiscoveryError, match="data") as captured:
        with _open_session(transport) as session:
            session.initialize()

    assert captured.value.code == PROTOCOL_ERROR


# --------------------------------------------------------------------------- #
# 真实 HTTP server round-trip(走 discover() 全链路)
# --------------------------------------------------------------------------- #


class _StreamableHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self) -> None:  # noqa: N802 - http.server 命名约定
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length))
        if payload.get("method") == "notifications/initialized":
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        envelope = handle_jsonrpc(payload) or {}
        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        if payload.get("method") == "initialize":
            self.send_header("Mcp-Session-Id", "real-sess-1")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:  # 静默请求日志
        return None


@pytest.fixture()
def streamable_http_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StreamableHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def test_discover_over_real_http_server(streamable_http_server: ThreadingHTTPServer) -> None:
    host, port = streamable_http_server.server_address

    result = discover(
        McpServerConfig(transport="http", url=f"http://{host}:{port}/mcp"),
        timeout_seconds=5,
    )

    assert [tool.name for tool in result.tools] == ["echo", "sum", "product_lookup"]
    assert result.server_info == {"name": "feibot-mock-mcp", "version": "0.1.0"}
    assert result.protocol_version == MOCK_PROTOCOL_VERSION
