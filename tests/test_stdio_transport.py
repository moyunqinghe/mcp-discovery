"""stdio transport：线程化管道读写、超时边界与可操作的启动错误。

单元测试覆盖管道读写原语与超时边界;round-trip 用真实 Python 子进程。
"""

from __future__ import annotations

import io
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from mcp_discovery import (
    CONNECT_FAILED,
    LAUNCH_FAILED,
    PROTOCOL_ERROR,
    PROTOCOL_VERSION,
    TIMEOUT,
    McpDiscoveryError,
    McpServerConfig,
    discover,
)
from mcp_discovery.client import (
    _PipeReader,
    _read_response,
    _send_json,
    _StderrCollector,
    _StdioSession,
)

MOCK_SERVER_PATH = Path(__file__).resolve().parent / "stdio_mock_server.py"

# 永远不读 stdin 的进程:写满管道后写入方必然阻塞,用于超时测试。
_SILENT_SCRIPT = "import time\nwhile True:\n    time.sleep(1)"


class _WindowsAnonymousPipe(io.StringIO):
    def fileno(self) -> int:
        raise OSError(10038, "在一个非套接字上尝试了一个操作")


class _FakeProcess:
    def __init__(self, exit_code: int | None = None) -> None:
        self.exit_code = exit_code

    def poll(self) -> int | None:
        return self.exit_code

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        return self.exit_code or 0


class _TimeoutReader:
    def next_event(self, timeout: float) -> tuple[str, object]:
        del timeout
        raise TimeoutError


class _BlockingInput:
    def __init__(self) -> None:
        self.release = threading.Event()

    def write(self, value: str) -> int:
        self.release.wait()
        return len(value)

    def flush(self) -> None:
        return None


class _BlockedWriteProcess(_FakeProcess):
    def __init__(self) -> None:
        super().__init__()
        self.stdin = _BlockingInput()


# --------------------------------------------------------------------------- #
# 管道读写原语(不依赖 select,兼容 Windows 匿名管道)
# --------------------------------------------------------------------------- #


def test_stdio_response_readable_on_socketless_windows_pipe() -> None:
    pipe = _WindowsAnonymousPipe('{"jsonrpc":"2.0","id":7,"result":{"ok":true}}\n')

    response = _read_response(
        _FakeProcess(),  # type: ignore[arg-type]
        _PipeReader(pipe),
        expected_id=7,
        timeout_seconds=1,
    )

    assert response["result"] == {"ok": True}


def test_stdio_response_timeout_is_bounded() -> None:
    with pytest.raises(McpDiscoveryError, match="等待响应超时") as captured:
        _read_response(
            _FakeProcess(),  # type: ignore[arg-type]
            _TimeoutReader(),  # type: ignore[arg-type]
            expected_id=3,
            timeout_seconds=0.01,
        )

    assert captured.value.code == TIMEOUT


def test_stdio_write_timeout_is_bounded() -> None:
    proc = _BlockedWriteProcess()
    try:
        with pytest.raises(McpDiscoveryError, match="发送请求超时") as captured:
            _send_json(
                proc,  # type: ignore[arg-type]
                {"jsonrpc": "2.0", "method": "large", "params": {}},
                timeout_seconds=0.01,
            )
    finally:
        proc.stdin.release.set()

    assert captured.value.code == TIMEOUT


def test_stdio_rejects_oversized_single_response() -> None:
    reader = _PipeReader(io.StringIO("x" * 33), max_line_size=32)

    with pytest.raises(McpDiscoveryError, match="超过 32 字符限制") as captured:
        _read_response(
            _FakeProcess(),  # type: ignore[arg-type]
            reader,
            expected_id=1,
            timeout_seconds=1,
        )

    assert captured.value.code == PROTOCOL_ERROR


def test_stdio_early_exit_includes_exit_code_and_stderr() -> None:
    stderr = _StderrCollector(io.StringIO("Cannot find module index.js\n"))

    with pytest.raises(McpDiscoveryError) as captured:
        _read_response(
            _FakeProcess(1),  # type: ignore[arg-type]
            _PipeReader(io.StringIO("")),
            expected_id=1,
            timeout_seconds=1,
            stderr=stderr,
        )

    assert captured.value.code == CONNECT_FAILED
    assert "退出码 1" in str(captured.value)
    assert "Cannot find module index.js" in str(captured.value)


def test_stdio_blocked_stdin_times_out_and_cleans_up_process() -> None:
    config = McpServerConfig(command=sys.executable, args=["-c", _SILENT_SCRIPT])
    session = _StdioSession(config, 0.2, PROTOCOL_VERSION)
    proc = None
    started = time.monotonic()

    with pytest.raises(McpDiscoveryError, match="发送请求超时"):
        with session:
            proc = session._proc
            assert proc is not None
            _send_json(
                proc,
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"data": "x" * 2_000_000}},
                timeout_seconds=0.2,
            )

    assert time.monotonic() - started < 5
    assert proc is not None and proc.poll() is not None
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline and any(
        thread.name == "mcp-stdin-writer" and thread.is_alive() for thread in threading.enumerate()
    ):
        time.sleep(0.01)
    assert not any(
        thread.name == "mcp-stdin-writer" and thread.is_alive() for thread in threading.enumerate()
    )


# --------------------------------------------------------------------------- #
# 启动前校验:可操作的错误信息
# --------------------------------------------------------------------------- #


def test_stdio_missing_command_has_actionable_error() -> None:
    with pytest.raises(McpDiscoveryError, match="找不到命令") as captured:
        discover(
            {"transport": "stdio", "command": "feibot-command-that-does-not-exist"},
            timeout_seconds=1,
        )

    assert captured.value.code == LAUNCH_FAILED


def test_stdio_missing_working_directory_has_actionable_error(tmp_path: Path) -> None:
    missing = tmp_path / "missing"

    with pytest.raises(McpDiscoveryError, match="工作目录不存在") as captured:
        discover(
            {"transport": "stdio", "command": sys.executable, "cwd": str(missing)},
            timeout_seconds=1,
        )

    assert captured.value.code == LAUNCH_FAILED


def test_stdio_missing_node_entrypoint_has_actionable_error(tmp_path: Path) -> None:
    with pytest.raises(McpDiscoveryError, match="入口文件不存在") as captured:
        discover(
            {
                "transport": "stdio",
                "command": "node",
                "args": ["index.js"],
                "cwd": str(tmp_path),
            },
            timeout_seconds=1,
        )

    assert captured.value.code == LAUNCH_FAILED


# --------------------------------------------------------------------------- #
# 真实子进程 round-trip
# --------------------------------------------------------------------------- #


def test_stdio_real_process_round_trip() -> None:
    result = discover(
        {"command": sys.executable, "args": [str(MOCK_SERVER_PATH)]},
        timeout_seconds=10,
    )

    assert [tool.name for tool in result.tools] == ["echo", "sum", "product_lookup"]
    assert result.server_info == {"name": "feibot-mock-mcp", "version": "0.1.0"}
    assert result.protocol_version == "2024-11-05"
    assert result.capabilities == {"tools": {"listChanged": False}}
    assert result.tools[0].input_schema == {
        "type": "object",
        "properties": {"text": {"type": "string"}},
    }
    rich = result.tools[2]
    assert rich.title == "商品查询"
    assert rich.output_schema == {"type": "object"}
    assert rich.annotations == {"readOnlyHint": True}
    assert rich.meta == {"ui": {"visibility": ["model"]}}


def test_stdio_jsonrpc_error_maps_to_protocol_error() -> None:
    script = textwrap.dedent(
        """
        import json
        import sys

        request = json.loads(sys.stdin.readline())
        print(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request["id"],
                    "error": {"code": -32601, "message": "boom: unsupported"},
                }
            ),
            flush=True,
        )
        """
    )

    with pytest.raises(McpDiscoveryError) as captured:
        discover(
            McpServerConfig(command=sys.executable, args=["-c", script]),
            timeout_seconds=10,
        )

    assert captured.value.code == PROTOCOL_ERROR
    assert "boom: unsupported" in str(captured.value)
