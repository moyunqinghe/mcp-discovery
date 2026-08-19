"""mcp-discovery:MCP server 工具发现的纯协议层基座。

一个 `discover()` 入口连接 MCP server(stdio / Streamable HTTP /
legacy HTTP+SSE),完成 initialize 握手并发现其工具集。
"""

from __future__ import annotations

from .client import (
    CLIENT_INFO,
    CONFIG_INVALID,
    CONNECT_FAILED,
    LAUNCH_FAILED,
    PROTOCOL_ERROR,
    PROTOCOL_VERSION,
    TIMEOUT,
    TOOL_ERROR,
    DiscoveredTool,
    DiscoveryResult,
    McpDiscoveryError,
    McpServerConfig,
    call_tool,
    discover,
)

__all__ = [
    "CLIENT_INFO",
    "CONFIG_INVALID",
    "CONNECT_FAILED",
    "LAUNCH_FAILED",
    "PROTOCOL_ERROR",
    "PROTOCOL_VERSION",
    "TIMEOUT",
    "TOOL_ERROR",
    "DiscoveredTool",
    "DiscoveryResult",
    "McpDiscoveryError",
    "McpServerConfig",
    "call_tool",
    "discover",
]
