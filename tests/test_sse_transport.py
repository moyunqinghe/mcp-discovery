"""SSE transport(MCP 2024-11-05 HTTP+SSE):endpoint 协商与流内响应匹配。

SSE 协议天然是双连接(GET 长连接收事件 + POST 消息端点发请求),
MockTransport 装不出来,所以这里用真实的线程 HTTP server。
"""

from __future__ import annotations

import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from mcp_discovery import (
    PROTOCOL_ERROR,
    PROTOCOL_VERSION,
    TIMEOUT,
    McpDiscoveryError,
    McpServerConfig,
    discover,
)
from mcp_discovery.client import _resolve_endpoint, _SseSession
from stdio_mock_server import handle_jsonrpc

# --------------------------------------------------------------------------- #
# 真实 SSE mock server
# --------------------------------------------------------------------------- #


class _SseMcpServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, _SseHandler)
        self.active = threading.Event()
        self.active.set()
        # GET /sse 首个事件给出的消息端点(相对路径;测试可改为绝对 URL)
        self.endpoint_event_data = "/messages?sid=abc"
        # 是否在流关闭前发送 endpoint 事件(False 用于模拟坏 server)
        self.send_endpoint = True
        self.respond = handle_jsonrpc
        self.outbox: queue.Queue[dict] = queue.Queue()
        self.post_paths: list[str] = []
        self.get_accept: str | None = None


class _SseHandler(BaseHTTPRequestHandler):
    server: _SseMcpServer  # type: ignore[assignment]

    def do_GET(self) -> None:  # noqa: N802 - http.server 命名约定
        if not self.path.startswith("/sse"):
            self.send_error(404)
            return
        self.server.get_accept = self.headers.get("Accept")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if self.server.send_endpoint:
            self._send_event("endpoint", self.server.endpoint_event_data)
        else:
            self._send_event("ping", "{}")
            return  # 直接关流:模拟不发 endpoint 的坏 server
        while self.server.active.is_set():
            try:
                envelope = self.server.outbox.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                self._send_event("message", json.dumps(envelope, ensure_ascii=False))
            except OSError:
                return  # 客户端已断开

    def do_POST(self) -> None:  # noqa: N802 - http.server 命名约定
        if not self.path.startswith("/messages"):
            self.send_error(404)
            return
        self.server.post_paths.append(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        payload = json.loads(self.rfile.read(length))
        envelope = self.server.respond(payload)
        if envelope is not None:
            self.server.outbox.put(envelope)
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_event(self, event: str, data: str) -> None:
        self.wfile.write(f"event: {event}\ndata: {data}\n\n".encode("utf-8"))
        self.wfile.flush()

    def log_message(self, *args: object) -> None:  # 静默请求日志
        return None


@pytest.fixture()
def sse_server():
    server = _SseMcpServer(("127.0.0.1", 0))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.active.clear()
    server.shutdown()
    server.server_close()


def _sse_config(server: _SseMcpServer) -> McpServerConfig:
    host, port = server.server_address
    return McpServerConfig(transport="sse", url=f"http://{host}:{port}/sse")


# --------------------------------------------------------------------------- #
# endpoint 解析
# --------------------------------------------------------------------------- #


def test_resolve_endpoint_relative() -> None:
    assert (
        _resolve_endpoint("http://host:9000/sse", "/messages?sid=1")
        == "http://host:9000/messages?sid=1"
    )


def test_resolve_endpoint_absolute_passthrough() -> None:
    assert (
        _resolve_endpoint("http://host:9000/sse", "https://other.example/messages")
        == "https://other.example/messages"
    )


# --------------------------------------------------------------------------- #
# 往返
# --------------------------------------------------------------------------- #


def test_sse_round_trip_relative_endpoint(sse_server: _SseMcpServer) -> None:
    session = _SseSession(_sse_config(sse_server), 5.0, PROTOCOL_VERSION)

    with session:
        server_result = session.initialize()
        tools = session.list_tools()

    assert server_result["serverInfo"] == {"name": "feibot-mock-mcp", "version": "0.1.0"}
    assert [tool["name"] for tool in tools] == ["echo", "sum", "product_lookup"]
    assert sse_server.get_accept == "text/event-stream"
    assert all(path == "/messages?sid=abc" for path in sse_server.post_paths)
    assert len(sse_server.post_paths) == 3  # initialize + initialized 通知 + tools/list


def test_sse_round_trip_absolute_endpoint(sse_server: _SseMcpServer) -> None:
    host, port = sse_server.server_address
    sse_server.endpoint_event_data = f"http://{host}:{port}/messages"
    session = _SseSession(_sse_config(sse_server), 5.0, PROTOCOL_VERSION)

    with session:
        session.initialize()
        tools = session.list_tools()

    assert len(tools) == 3
    assert all(path == "/messages" for path in sse_server.post_paths)


def test_discover_over_real_sse_server(sse_server: _SseMcpServer) -> None:
    result = discover(_sse_config(sse_server), timeout_seconds=5)

    assert [tool.name for tool in result.tools] == ["echo", "sum", "product_lookup"]
    assert result.server_info == {"name": "feibot-mock-mcp", "version": "0.1.0"}
    assert result.capabilities == {"tools": {"listChanged": False}}


# --------------------------------------------------------------------------- #
# 错误路径
# --------------------------------------------------------------------------- #


def test_sse_missing_endpoint_event_maps_to_protocol_error(sse_server: _SseMcpServer) -> None:
    sse_server.send_endpoint = False
    session = _SseSession(_sse_config(sse_server), 2.0, PROTOCOL_VERSION)

    with pytest.raises(McpDiscoveryError, match="endpoint") as captured:
        with session:
            session.initialize()

    assert captured.value.code == PROTOCOL_ERROR


def test_sse_response_wait_is_bounded_and_maps_to_timeout(sse_server: _SseMcpServer) -> None:
    sse_server.respond = lambda payload: None  # 收到请求但永不回响应
    session = _SseSession(_sse_config(sse_server), 0.3, PROTOCOL_VERSION)
    started = time.monotonic()

    with pytest.raises(McpDiscoveryError, match="超时") as captured:
        with session:
            session.initialize()

    assert captured.value.code == TIMEOUT
    assert time.monotonic() - started < 2
