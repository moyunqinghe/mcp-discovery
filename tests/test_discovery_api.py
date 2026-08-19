"""公共 API、类型化配置、单一错误类型与工具定义归一化。"""

from __future__ import annotations

import dataclasses

import pytest
from pydantic import ValidationError

from mcp_discovery import (
    CONFIG_INVALID,
    PROTOCOL_VERSION,
    DiscoveredTool,
    DiscoveryResult,
    McpDiscoveryError,
    McpServerConfig,
    discover,
)
from mcp_discovery.client import _normalize_tool_definition

# --------------------------------------------------------------------------- #
# McpServerConfig：类型化配置
# --------------------------------------------------------------------------- #


def test_http_config_accepts_explicit_transport() -> None:
    config = McpServerConfig(transport="http", url="http://localhost:8000/mcp")

    assert config.transport == "http"
    assert config.url == "http://localhost:8000/mcp"
    assert config.headers == {}


def test_streamable_http_alias_normalizes_to_http() -> None:
    config = McpServerConfig(transport="streamable_http", url="http://localhost:8000/mcp")

    assert config.transport == "http"


def test_transport_inferred_from_command() -> None:
    config = McpServerConfig(command="npx", args=["-y", "@some/mcp-server"])

    assert config.transport == "stdio"


def test_transport_inferred_from_url() -> None:
    config = McpServerConfig(url="http://localhost:8000/mcp")

    assert config.transport == "http"


def test_config_without_command_and_url_rejected() -> None:
    with pytest.raises(ValidationError, match="command"):
        McpServerConfig(transport="stdio")


def test_stdio_transport_requires_command() -> None:
    with pytest.raises(ValidationError):
        McpServerConfig(transport="stdio", url="http://localhost:8000/mcp")


def test_http_transport_requires_url() -> None:
    with pytest.raises(ValidationError, match="url"):
        McpServerConfig(transport="http", command="node")


def test_args_must_be_list() -> None:
    with pytest.raises(ValidationError, match="args"):
        McpServerConfig(command="node", args="--inspect")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# McpDiscoveryError：单一错误类型
# --------------------------------------------------------------------------- #


def test_error_carries_code_message_and_cause() -> None:
    cause = ValueError("upstream")
    error = McpDiscoveryError("连接失败", code=CONFIG_INVALID, cause=cause)

    assert str(error) == "连接失败"
    assert error.code == CONFIG_INVALID
    assert error.cause is cause


def test_error_cause_defaults_to_none() -> None:
    assert McpDiscoveryError("x", code="TIMEOUT").cause is None


def test_discover_rejects_invalid_dict_config() -> None:
    with pytest.raises(McpDiscoveryError) as captured:
        discover({"transport": "stdio"})

    assert captured.value.code == CONFIG_INVALID
    assert isinstance(captured.value.cause, ValidationError)


def test_discover_rejects_unknown_transport() -> None:
    with pytest.raises(McpDiscoveryError) as captured:
        discover({"transport": "grpc", "url": "http://localhost:8000/mcp"})

    assert captured.value.code == CONFIG_INVALID


def test_discover_rejects_empty_mapping() -> None:
    with pytest.raises(McpDiscoveryError) as captured:
        discover({})

    assert captured.value.code == CONFIG_INVALID


# --------------------------------------------------------------------------- #
# 工具定义归一化
# --------------------------------------------------------------------------- #


def test_normalize_tool_definition_camel_case() -> None:
    tool = _normalize_tool_definition(
        {
            "name": "echo",
            "title": "回声",
            "description": "Return input.",
            "inputSchema": {"type": "object"},
            "outputSchema": {"type": "object"},
            "annotations": {"readOnlyHint": True},
            "_meta": {"ui": {"visibility": ["model"]}},
        }
    )

    assert tool == DiscoveredTool(
        name="echo",
        title="回声",
        description="Return input.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
        annotations={"readOnlyHint": True},
        meta={"ui": {"visibility": ["model"]}},
    )


def test_normalize_tool_definition_snake_case_fallback() -> None:
    tool = _normalize_tool_definition(
        {"name": "sum", "input_schema": {"type": "object"}, "output_schema": {"type": "array"}}
    )

    assert tool.input_schema == {"type": "object"}
    assert tool.output_schema == {"type": "array"}


def test_normalize_tool_definition_defaults_empty() -> None:
    tool = _normalize_tool_definition({})

    assert tool == DiscoveredTool(
        name="",
        title="",
        description="",
        input_schema={},
        output_schema={},
        annotations={},
        meta={},
    )


def test_normalize_tool_definition_rejects_non_dict_schemas() -> None:
    tool = _normalize_tool_definition(
        {"name": "x", "inputSchema": "junk", "outputSchema": [1], "annotations": "no", "_meta": "no"}
    )

    assert tool.input_schema == {}
    assert tool.output_schema == {}
    assert tool.annotations == {}
    assert tool.meta == {}


def test_normalize_tool_definition_strips_whitespace() -> None:
    tool = _normalize_tool_definition({"name": " echo ", "description": " d "})

    assert tool.name == "echo"
    assert tool.description == "d"


# --------------------------------------------------------------------------- #
# 结果值对象
# --------------------------------------------------------------------------- #


def test_discovered_tool_is_frozen_value() -> None:
    tool = DiscoveredTool(
        name="echo",
        title="",
        description="",
        input_schema={},
        output_schema={},
        annotations={},
        meta={},
    )

    with pytest.raises(dataclasses.FrozenInstanceError):
        tool.name = "changed"  # type: ignore[misc]


def test_discovery_result_holds_tuple_of_tools() -> None:
    result = DiscoveryResult(
        tools=(),
        protocol_version=PROTOCOL_VERSION,
        capabilities={},
        server_info={"name": "mock", "version": "1"},
    )

    assert result.tools == ()
    assert result.server_info["name"] == "mock"
