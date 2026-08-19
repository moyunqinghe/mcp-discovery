"""call_tool 的结果归一化、配置收敛与真实 stdio 调用测试。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from mcp_discovery import CONFIG_INVALID, TOOL_ERROR, McpDiscoveryError, call_tool
from mcp_discovery.client import _content_text, _coerce_config, _extract_tool_result

_STDIO_MOCK = Path(__file__).resolve().parent / "stdio_mock_server.py"


def test_extract_text_content():
    raw = {"content": [{"type": "text", "text": "hello"}], "isError": False}
    assert _extract_tool_result(raw) == "hello"


def test_extract_multiple_text_blocks_joined():
    raw = {"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}
    assert _extract_tool_result(raw) == "a\nb"


def test_extract_prefers_structured_content():
    raw = {"structuredContent": {"k": 1}, "content": [{"type": "text", "text": "x"}]}
    assert _extract_tool_result(raw) == {"k": 1}


def test_extract_non_text_content_fallback_returns_raw():
    raw = {"content": [{"type": "image", "data": "..."}]}
    assert _extract_tool_result(raw) == raw


def test_extract_non_mapping_passthrough():
    assert _extract_tool_result("plain") == "plain"
    assert _extract_tool_result(42) == 42


def test_extract_is_error_raises_tool_error():
    raw = {"content": [{"type": "text", "text": "bad input"}], "isError": True}
    with pytest.raises(McpDiscoveryError) as ei:
        _extract_tool_result(raw)
    assert ei.value.code == TOOL_ERROR
    assert "bad input" in str(ei.value)


def test_content_text_ignores_non_text_and_handles_none():
    assert _content_text([{"type": "image"}, {"type": "text", "text": "x"}]) == "x"
    assert _content_text(None) == ""


def test_coerce_config_accepts_mapping_and_model():
    from mcp_discovery import McpServerConfig
    cfg = _coerce_config({"transport": "http", "url": "http://x"})
    assert cfg.url == "http://x"
    assert _coerce_config(cfg) is cfg


def test_coerce_config_rejects_non_mapping():
    with pytest.raises(McpDiscoveryError) as ei:
        _coerce_config("not a mapping")
    assert ei.value.code == CONFIG_INVALID


def test_coerce_config_rejects_invalid_mapping():
    with pytest.raises(McpDiscoveryError) as ei:
        _coerce_config({"transport": "stdio"})  # 缺 command
    assert ei.value.code == CONFIG_INVALID


def test_call_tool_stdio_echo():
    config = {"transport": "stdio", "command": sys.executable, "args": [str(_STDIO_MOCK)]}
    result = call_tool(config, "echo", {"text": "你好"}, timeout_seconds=15)
    assert result == "echo: 你好"


def test_call_tool_stdio_is_error_raises_tool_error():
    config = {"transport": "stdio", "command": sys.executable, "args": [str(_STDIO_MOCK)]}
    with pytest.raises(McpDiscoveryError) as ei:
        call_tool(config, "fail", {}, timeout_seconds=15)
    assert ei.value.code == TOOL_ERROR


def test_call_tool_rejects_invalid_config():
    with pytest.raises(McpDiscoveryError) as ei:
        call_tool({"transport": "stdio"}, "echo", {})
    assert ei.value.code == CONFIG_INVALID
