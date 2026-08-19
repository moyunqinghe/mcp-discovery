"""MCP server 工具发现的纯协议层。

围绕「发现」这一个目标组织:类型化配置(Pydantic)→ 一个
`discover()` 入口 → 类型化的工具列表返回,全链路单一错误类型
`McpDiscoveryError`。协议内核(JSON-RPC 2.0 成帧、SSE endpoint 协商、
Streamable HTTP 会话管理、错误映射)均经实际使用场景检验。
"""

from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TextIO

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

#: 默认协商的 MCP 协议版本(server 会回复它实际支持的版本)。
PROTOCOL_VERSION = "2025-06-18"

#: 默认的客户端身份,写入 initialize 的 clientInfo。
CLIENT_INFO: Mapping[str, str] = {"name": "mcp-discovery", "version": "1.0.0"}

# --- 错误码(McpDiscoveryError.code) -------------------------------------- #
CONFIG_INVALID = "CONFIG_INVALID"      # 连接配置不合法
LAUNCH_FAILED = "LAUNCH_FAILED"        # stdio 子进程启动失败
CONNECT_FAILED = "CONNECT_FAILED"      # 连接中断/进程提前退出/HTTP 状态码异常
TIMEOUT = "TIMEOUT"                    # 读/写/等待超时
PROTOCOL_ERROR = "PROTOCOL_ERROR"      # JSON-RPC 错误、响应体不合法等协议问题
TOOL_ERROR = "TOOL_ERROR"              # 工具自身返回 isError(协议本身正常)


class McpDiscoveryError(Exception):
    """发现流程中所有可预期错误的单一类型。

    - ``code``:机器可读分类,取值为上方常量之一。
    - ``cause``:触发本错误的底层异常(如 pydantic ValidationError、
      httpx 异常),便于宿主记日志;对外展示只用 str(error)。
    """

    def __init__(self, message: str, *, code: str, cause: BaseException | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.cause = cause


class McpServerConfig(BaseModel):
    """MCP server 连接配置(类型化,冻结)。

    - stdio:``command``(+ 可选 ``args`` / ``env`` / ``cwd``)
    - http(Streamable HTTP)/ sse(2024-11-05 HTTP+SSE):``url``(+ 可选 ``headers``)

    ``transport`` 可省略:有 ``command`` 推断为 stdio,有 ``url`` 推断为
    http;``streamable_http`` 是 http 的别名,会被归一化。
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    transport: Literal["stdio", "http", "sse", "streamable_http"] | None = None
    command: str | list[str] | None = None
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    url: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _resolve_transport(self) -> McpServerConfig:
        transport: str | None = self.transport
        if transport == "streamable_http":
            transport = "http"
        if transport is None:
            if self.command:
                transport = "stdio"
            elif self.url:
                transport = "http"
        if transport is None:
            raise ValueError("缺少 command 或 url,无法确定 MCP transport")
        if transport == "stdio" and not self.command:
            raise ValueError("stdio transport 需要 command(要启动的命令)")
        if transport in {"http", "sse"} and not str(self.url or "").strip():
            raise ValueError(f"{transport} transport 需要 url")
        object.__setattr__(self, "transport", transport)
        return self


@dataclass(frozen=True)
class DiscoveredTool:
    """归一化后的单个工具定义(tools/list 的一项)。"""

    name: str
    title: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    annotations: dict[str, Any]
    meta: dict[str, Any]


@dataclass(frozen=True)
class DiscoveryResult:
    """一次发现的完整产出:工具列表 + initialize 握手的 server 元信息。"""

    tools: tuple[DiscoveredTool, ...]
    protocol_version: str
    capabilities: dict[str, Any]
    server_info: dict[str, Any]


# --------------------------------------------------------------------------- #
# 对外入口
# --------------------------------------------------------------------------- #


def _coerce_config(config: McpServerConfig | Mapping[str, Any]) -> McpServerConfig:
    """把配置收敛为 McpServerConfig;非法配置抛 McpDiscoveryError(CONFIG_INVALID)。"""
    if isinstance(config, McpServerConfig):
        return config
    if not isinstance(config, Mapping):
        raise McpDiscoveryError(
            f"MCP server 配置必须是 mapping 或 McpServerConfig,实际是 {type(config).__name__}。",
            code=CONFIG_INVALID,
        )
    try:
        return McpServerConfig.model_validate(dict(config))
    except ValidationError as exc:
        raise McpDiscoveryError(f"MCP server 配置无效：{exc}", code=CONFIG_INVALID, cause=exc) from exc


def discover(
    config: McpServerConfig | Mapping[str, Any],
    *,
    timeout_seconds: float = 10.0,
    protocol_version: str = PROTOCOL_VERSION,
    client_info: Mapping[str, Any] | None = None,
) -> DiscoveryResult:
    """连接 MCP server,完成 initialize 握手并通过 tools/list 发现工具集。

    ``config`` 可以是 `McpServerConfig`,也可以是普通 mapping(如数据库里
    存的一行配置),后者会先经过 Pydantic 校验。所有可预期失败统一抛
    `McpDiscoveryError`。
    """
    config = _coerce_config(config)

    session = _build_session(config, timeout_seconds, protocol_version, client_info)
    try:
        with session:
            server_result = session.initialize()
            raw_tools = session.list_tools()
    except McpDiscoveryError:
        raise
    except Exception as exc:  # 兜底:任何未分类异常也收敛为单一错误类型
        raise McpDiscoveryError(f"MCP 发现失败：{exc}", code=PROTOCOL_ERROR, cause=exc) from exc

    capabilities = server_result.get("capabilities")
    server_info = server_result.get("serverInfo")
    return DiscoveryResult(
        tools=tuple(
            _normalize_tool_definition(item) for item in raw_tools if isinstance(item, Mapping)
        ),
        protocol_version=str(server_result.get("protocolVersion") or ""),
        capabilities=dict(capabilities) if isinstance(capabilities, Mapping) else {},
        server_info=dict(server_info) if isinstance(server_info, Mapping) else {},
    )


def call_tool(
    config: McpServerConfig | Mapping[str, Any],
    tool_name: str,
    arguments: Mapping[str, Any] | None = None,
    *,
    timeout_seconds: float = 30.0,
    protocol_version: str = PROTOCOL_VERSION,
    client_info: Mapping[str, Any] | None = None,
) -> Any:
    """连接 MCP server,initialize 后调用单个工具,返回归一化结果数据。

    每次调用新开连接(与 discover 一致)。工具 isError 抛 TOOL_ERROR;
    连接/协议失败抛对应 code 的 McpDiscoveryError。
    """
    config = _coerce_config(config)
    session = _build_session(config, timeout_seconds, protocol_version, client_info)
    try:
        with session:
            session.initialize()
            raw = session.call_tool(tool_name, dict(arguments or {}))
    except McpDiscoveryError:
        raise
    except Exception as exc:  # 兜底:收敛为单一错误类型
        raise McpDiscoveryError(f"MCP 工具调用失败：{exc}", code=PROTOCOL_ERROR, cause=exc) from exc
    return _extract_tool_result(raw)


def _build_session(
    config: McpServerConfig,
    timeout_seconds: float,
    protocol_version: str,
    client_info: Mapping[str, Any] | None,
) -> _Session:
    if config.transport == "stdio":
        return _StdioSession(config, timeout_seconds, protocol_version, client_info)
    if config.transport == "http":
        return _HttpSession(config, timeout_seconds, protocol_version, client_info)
    if config.transport == "sse":
        return _SseSession(config, timeout_seconds, protocol_version, client_info)
    raise NotImplementedError


# --------------------------------------------------------------------------- #
# JSON-RPC 会话基类(initialize + tools/list)
# --------------------------------------------------------------------------- #


class _Session:
    """封装一次 MCP 连接的 initialize + tools/list 交互。

    子类实现 `_request`(单次 JSON-RPC 请求/响应)、`_notify`(通知)
    与上下文管理;会话结构为后续扩展 tools/call 等能力保留。
    """

    def __init__(
        self,
        config: McpServerConfig,
        timeout_seconds: float,
        protocol_version: str,
        client_info: Mapping[str, Any] | None,
    ) -> None:
        self.config = config
        self.timeout_seconds = timeout_seconds
        self._protocol_version = protocol_version
        self._client_info = dict(client_info or CLIENT_INFO)

    def initialize(self) -> dict[str, Any]:
        result = self._request("initialize", self._initialize_params())
        self._notify("notifications/initialized", {})
        return dict(result) if isinstance(result, Mapping) else {}

    def list_tools(self) -> list[Any]:
        result = self._request("tools/list", {})
        tools = result.get("tools") if isinstance(result, Mapping) else None
        return list(tools) if isinstance(tools, list) else []

    def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        return self._request("tools/call", {"name": name, "arguments": arguments})

    def _initialize_params(self) -> dict[str, Any]:
        return {
            "protocolVersion": self._protocol_version,
            "capabilities": {},
            "clientInfo": dict(self._client_info),
        }

    # 子类实现 ---------------------------------------------------------------
    def __enter__(self) -> _Session:
        return self

    def __exit__(self, *exc: Any) -> None:  # pragma: no cover - default no-op
        return None

    def _request(self, method: str, params: dict[str, Any]) -> Any:
        raise NotImplementedError

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# stdio transport
# --------------------------------------------------------------------------- #


class _PipeReader:
    """Read a text pipe without relying on select(), which rejects pipes on Windows."""

    def __init__(self, stream: TextIO, max_line_size: int = 4 * 1024 * 1024) -> None:
        self._stream = stream
        self._max_line_size = max_line_size
        self._events: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=128)
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, name="mcp-stdio-reader", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            while not self._stopped.is_set():
                line = self._stream.readline(self._max_line_size + 1)
                if not line:
                    break
                if len(line) > self._max_line_size:
                    self._put(
                        (
                            "error",
                            McpDiscoveryError(
                                f"MCP stdio 单条响应超过 {self._max_line_size} 字符限制。",
                                code=PROTOCOL_ERROR,
                            ),
                        )
                    )
                    return
                self._put(("line", line))
        except (OSError, ValueError) as exc:
            if not self._stopped.is_set():
                self._put(("error", exc))
        finally:
            self._put(("eof", None))

    def _put(self, event: tuple[str, object]) -> None:
        while not self._stopped.is_set():
            try:
                self._events.put(event, timeout=0.1)
                return
            except queue.Full:
                continue

    def next_event(self, timeout: float) -> tuple[str, object]:
        try:
            return self._events.get(timeout=max(timeout, 0))
        except queue.Empty as exc:
            raise TimeoutError from exc

    def close(self) -> None:
        self._stopped.set()
        with suppress(OSError, ValueError):
            self._stream.close()
        self._thread.join(timeout=0.2)


class _StderrCollector:
    def __init__(self, stream: TextIO, limit: int = 1000) -> None:
        self._stream = stream
        self._limit = limit
        self._parts: list[str] = []
        self._length = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="mcp-stderr-reader", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            while chunk := self._stream.readline(256):
                with self._lock:
                    if self._length < self._limit:
                        kept = chunk[: self._limit - self._length]
                        self._parts.append(kept)
                        self._length += len(kept)
        except (OSError, ValueError):
            return

    def text(self) -> str:
        with self._lock:
            value = "".join(self._parts).strip()
        return f" stderr: {value}" if value else ""

    def wait(self, timeout: float = 0.1) -> None:
        self._thread.join(timeout)

    def close(self) -> None:
        with suppress(OSError, ValueError):
            self._stream.close()
        self._thread.join(timeout=0.2)


class _ProcessGuard:
    """持有 stdio 子进程:close() 保证杀掉进程并回收管道。

    守护策略简化为 terminate→kill→wait;发现流程的进程都是短命的,
    退出路径上必然 close。
    """

    def __init__(self, process: subprocess.Popen[str]) -> None:
        self.process = process

    @classmethod
    def start(
        cls,
        command: list[str],
        *,
        cwd: str | None,
        env: dict[str, str],
    ) -> _ProcessGuard:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="strict",
            bufsize=1,
        )
        return cls(process)

    def close(self) -> None:
        process = self.process
        try:
            process.terminate()
        except OSError:
            pass
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            with suppress(OSError):
                process.kill()
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                with suppress(OSError, ValueError):
                    stream.close()


class _StdioSession(_Session):
    def __init__(
        self,
        config: McpServerConfig,
        timeout_seconds: float,
        protocol_version: str,
        client_info: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(config, timeout_seconds, protocol_version, client_info)
        self._guard: _ProcessGuard | None = None
        self._proc: subprocess.Popen[str] | None = None
        self._stdout_reader: _PipeReader | None = None
        self._stderr_collector: _StderrCollector | None = None
        self._next_id = 0

    def __enter__(self) -> _StdioSession:
        command = _stdio_command(self.config)
        cwd = str(Path(self.config.cwd).expanduser()) if self.config.cwd else None
        _validate_stdio_launch(command, cwd)
        env = os.environ.copy()
        env.update({str(key): str(value) for key, value in self.config.env.items()})
        try:
            self._guard = _ProcessGuard.start(command, cwd=cwd, env=env)
        except FileNotFoundError as exc:
            raise McpDiscoveryError(
                f"无法启动 MCP stdio：找不到命令 {command[0]!r},请确认它已安装并在 PATH 中。",
                code=LAUNCH_FAILED,
                cause=exc,
            ) from exc
        except PermissionError as exc:
            raise McpDiscoveryError(
                f"无法启动 MCP stdio：没有权限执行 {command[0]!r}。", code=LAUNCH_FAILED, cause=exc
            ) from exc
        except OSError as exc:
            raise McpDiscoveryError(
                f"无法启动 MCP stdio 命令 {command[0]!r}：{exc}", code=LAUNCH_FAILED, cause=exc
            ) from exc
        self._proc = self._guard.process
        if self._proc.stdout is None or self._proc.stderr is None:
            self._guard.close()
            self._guard = None
            self._proc = None
            raise McpDiscoveryError("MCP stdio 进程管道创建失败。", code=LAUNCH_FAILED)
        self._stdout_reader = _PipeReader(self._proc.stdout)
        self._stderr_collector = _StderrCollector(self._proc.stderr)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._guard is not None:
            self._guard.close()
            self._guard = None
        self._proc = None
        if self._stdout_reader is not None:
            self._stdout_reader.close()
        if self._stderr_collector is not None:
            self._stderr_collector.close()
        self._stdout_reader = None
        self._stderr_collector = None

    def _request(self, method: str, params: dict[str, Any]) -> Any:
        proc = self._require_proc()
        self._next_id += 1
        request_id = self._next_id
        timeout = max(self.timeout_seconds, 0.1)
        deadline = time.monotonic() + timeout
        _send_json(
            proc,
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
            timeout_seconds=timeout,
        )
        response = _read_response(
            proc,
            self._require_stdout_reader(),
            expected_id=request_id,
            timeout_seconds=max(deadline - time.monotonic(), 0),
            stderr=self._stderr_collector,
        )
        _raise_json_rpc_error(response)
        return response.get("result")

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        proc = self._require_proc()
        _send_json(
            proc,
            {"jsonrpc": "2.0", "method": method, "params": params},
            timeout_seconds=max(self.timeout_seconds, 0.1),
        )

    def _require_proc(self) -> subprocess.Popen[str]:
        if self._proc is None:
            raise McpDiscoveryError("MCP stdio 会话未启动。", code=CONNECT_FAILED)
        return self._proc

    def _require_stdout_reader(self) -> _PipeReader:
        if self._stdout_reader is None:
            raise McpDiscoveryError("MCP stdio stdout 不可用。", code=CONNECT_FAILED)
        return self._stdout_reader


def _stdio_command(config: McpServerConfig) -> list[str]:
    command = config.command
    if isinstance(command, list):
        parts = [str(part) for part in command]
    elif isinstance(command, str) and command.strip():
        parts = [command.strip()]
    else:
        raise McpDiscoveryError("stdio MCP 连接缺少 command。", code=CONFIG_INVALID)
    return [*parts, *[str(arg) for arg in config.args]]


def _validate_stdio_launch(command: list[str], cwd: str | None) -> None:
    workdir = Path(cwd).expanduser() if cwd else Path.cwd()
    if cwd and not workdir.is_dir():
        raise McpDiscoveryError(f"MCP stdio 工作目录不存在：{workdir}", code=LAUNCH_FAILED)

    executable = Path(command[0]).name.lower()
    if executable not in {"node", "node.exe", "bun", "bun.exe", "deno", "deno.exe"}:
        return
    entrypoint = next((arg for arg in command[1:] if not arg.startswith("-")), "")
    if not entrypoint or Path(entrypoint).suffix.lower() not in {".js", ".cjs", ".mjs", ".ts"}:
        return
    entrypoint_path = Path(entrypoint).expanduser()
    resolved = entrypoint_path if entrypoint_path.is_absolute() else workdir / entrypoint_path
    if not resolved.is_file():
        raise McpDiscoveryError(
            f"MCP stdio 入口文件不存在：{resolved}。请检查 args,或将 cwd 设置为入口文件所在目录。",
            code=LAUNCH_FAILED,
        )


# --------------------------------------------------------------------------- #
# HTTP (streamable_http) transport
# --------------------------------------------------------------------------- #


class _HttpSession(_Session):
    """Streamable HTTP transport:JSON-RPC over POST,响应体兼容纯 JSON 与 SSE。

    会话管理:initialize 的响应头里拿到 ``Mcp-Session-Id`` 后续请求回传。
    构造参数 ``transport`` 仅用于测试注入(httpx.MockTransport)。
    """

    def __init__(
        self,
        config: McpServerConfig,
        timeout_seconds: float,
        protocol_version: str,
        client_info: Mapping[str, Any] | None = None,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(config, timeout_seconds, protocol_version, client_info)
        self._transport = transport
        self._client: httpx.Client | None = None
        self._session_id: str | None = None
        self._next_id = 0

    def __enter__(self) -> _HttpSession:
        self._client = httpx.Client(timeout=self.timeout_seconds, transport=self._transport)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._client is not None:
            with suppress(Exception):
                self._client.close()
            self._client = None

    def _endpoint(self) -> str:
        url = str(self.config.url or "").strip()
        if not url:
            raise McpDiscoveryError("HTTP MCP 连接缺少 url。", code=CONFIG_INVALID)
        return url

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            **{str(key): str(value) for key, value in self.config.headers.items()},
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    def _request(self, method: str, params: dict[str, Any]) -> Any:
        client = self._require_client()
        self._next_id += 1
        payload = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params}
        try:
            response = client.post(self._endpoint(), headers=self._headers(), json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise McpDiscoveryError(
                f"HTTP MCP 返回异常状态码：{exc.response.status_code}", code=CONNECT_FAILED, cause=exc
            ) from exc
        except httpx.HTTPError as exc:
            raise McpDiscoveryError(f"HTTP MCP 连接失败：{exc}", code=CONNECT_FAILED, cause=exc) from exc
        session_id = response.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id
        body = _parse_http_mcp_response(response)
        if not isinstance(body, dict):
            raise McpDiscoveryError("HTTP MCP 返回内容不是 JSON-RPC object。", code=PROTOCOL_ERROR)
        _raise_json_rpc_error(body)
        return body.get("result")

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        client = self._require_client()
        payload = {"jsonrpc": "2.0", "method": method, "params": params}
        with suppress(httpx.HTTPError):
            client.post(self._endpoint(), headers=self._headers(), json=payload)

    def _require_client(self) -> httpx.Client:
        if self._client is None:
            raise McpDiscoveryError("HTTP MCP 会话未启动。", code=CONNECT_FAILED)
        return self._client


def _parse_http_mcp_response(response: httpx.Response) -> Any:
    """解析 HTTP MCP 响应,兼容纯 JSON 和 SSE 格式(text/event-stream)。"""
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        payload = _last_sse_json(response.text)
        if payload is None:
            raise McpDiscoveryError("SSE 响应中未找到有效的 JSON-RPC data 行。", code=PROTOCOL_ERROR)
        return payload
    try:
        return response.json()
    except Exception as exc:
        raise McpDiscoveryError(f"HTTP MCP 响应解析失败：{exc}", code=PROTOCOL_ERROR, cause=exc) from exc


def _last_sse_json(text: str) -> Any:
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("data:"):
            data = line[len("data:") :].strip()
            with suppress(json.JSONDecodeError):
                return json.loads(data)
    return None


# --------------------------------------------------------------------------- #
# SSE transport
# --------------------------------------------------------------------------- #


class _SseSession(_Session):
    """SSE transport（MCP 2024-11-05 HTTP+SSE）。

    连接流程：GET server url 建立 SSE 流,从首个 ``event: endpoint``
    拿到用于发送 JSON-RPC 的消息端点;后续请求 POST 到该端点,
    响应通过 SSE 流按 id 匹配返回。

    注:等 endpoint 时若用 ``read=None``,SSE 流静默会无限阻塞;
    这里读超时取 ``timeout_seconds``,把静默收敛为 TIMEOUT 错误。
    """

    def __init__(
        self,
        config: McpServerConfig,
        timeout_seconds: float,
        protocol_version: str,
        client_info: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(config, timeout_seconds, protocol_version, client_info)
        self._client: httpx.Client | None = None
        self._stream_ctx: Any = None
        self._events: Any = None
        self._message_url: str | None = None
        self._next_id = 0

    def __enter__(self) -> _SseSession:
        self._client = httpx.Client(
            timeout=httpx.Timeout(self.timeout_seconds, read=self.timeout_seconds)
        )
        url = str(self.config.url or "").strip()
        if not url:
            raise McpDiscoveryError("SSE MCP 连接缺少 url。", code=CONFIG_INVALID)
        headers = {
            "Accept": "text/event-stream",
            **{str(key): str(value) for key, value in self.config.headers.items()},
        }
        self._stream_ctx = self._client.stream("GET", url, headers=headers)
        try:
            response = self._stream_ctx.__enter__()
        except httpx.HTTPError as exc:
            self._stream_ctx = None
            self._teardown()
            raise McpDiscoveryError(f"SSE MCP 连接失败：{exc}", code=CONNECT_FAILED, cause=exc) from exc
        try:
            response.raise_for_status()
            self._events = _iter_sse_events(response)
            self._message_url = self._await_endpoint(url)
        except Exception:
            self._teardown()
            raise
        return self

    def __exit__(self, *exc: Any) -> None:
        self._teardown()

    def _teardown(self) -> None:
        if self._stream_ctx is not None:
            with suppress(Exception):
                self._stream_ctx.__exit__(None, None, None)
            self._stream_ctx = None
        if self._client is not None:
            with suppress(Exception):
                self._client.close()
            self._client = None

    def _await_endpoint(self, base_url: str) -> str:
        deadline = time.monotonic() + max(self.timeout_seconds, 0.1)
        try:
            for event, data in self._events:
                if event == "endpoint":
                    return _resolve_endpoint(base_url, data.strip())
                if time.monotonic() > deadline:
                    break
        except httpx.ReadTimeout as exc:
            raise McpDiscoveryError("SSE MCP 等待 endpoint 超时。", code=TIMEOUT, cause=exc) from exc
        raise McpDiscoveryError("SSE MCP 未返回 endpoint 事件。", code=PROTOCOL_ERROR)

    def _post_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            **{str(key): str(value) for key, value in self.config.headers.items()},
        }

    def _request(self, method: str, params: dict[str, Any]) -> Any:
        client = self._require_client()
        self._next_id += 1
        request_id = self._next_id
        payload = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        try:
            posted = client.post(str(self._message_url), headers=self._post_headers(), json=payload)
            posted.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise McpDiscoveryError(
                f"SSE MCP 返回异常状态码：{exc.response.status_code}", code=CONNECT_FAILED, cause=exc
            ) from exc
        except httpx.HTTPError as exc:
            raise McpDiscoveryError(f"SSE MCP 连接失败：{exc}", code=CONNECT_FAILED, cause=exc) from exc
        body = self._await_response(request_id)
        _raise_json_rpc_error(body)
        return body.get("result")

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        client = self._require_client()
        payload = {"jsonrpc": "2.0", "method": method, "params": params}
        with suppress(httpx.HTTPError):
            client.post(str(self._message_url), headers=self._post_headers(), json=payload)

    def _await_response(self, expected_id: int) -> dict[str, Any]:
        deadline = time.monotonic() + max(self.timeout_seconds, 0.1)
        try:
            for event, data in self._events:
                if event in {"message", ""}:
                    with suppress(json.JSONDecodeError):
                        payload = json.loads(data)
                        if isinstance(payload, dict) and payload.get("id") == expected_id:
                            return payload
                if time.monotonic() > deadline:
                    break
        except httpx.ReadTimeout as exc:
            raise McpDiscoveryError(
                f"SSE MCP 等待响应超时：id={expected_id}", code=TIMEOUT, cause=exc
            ) from exc
        raise McpDiscoveryError(f"SSE MCP 等待响应超时：id={expected_id}", code=TIMEOUT)

    def _require_client(self) -> httpx.Client:
        if self._client is None or self._message_url is None:
            raise McpDiscoveryError("SSE MCP 会话未启动。", code=CONNECT_FAILED)
        return self._client


def _iter_sse_events(response: httpx.Response):
    """迭代 SSE 流,逐个 yield (event_type, data)。"""
    event_type = ""
    data_lines: list[str] = []
    for raw_line in response.iter_lines():
        line = raw_line.rstrip("\r")
        if line == "":
            if data_lines:
                yield event_type or "message", "\n".join(data_lines)
            event_type = ""
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            event_type = line[len("event:") :].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())


def _resolve_endpoint(base_url: str, endpoint: str) -> str:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    from urllib.parse import urljoin

    return urljoin(base_url, endpoint)


# --------------------------------------------------------------------------- #
# JSON-RPC 成帧与共享工具函数
# --------------------------------------------------------------------------- #


def _send_json(
    proc: subprocess.Popen[str],
    payload: dict[str, Any],
    timeout_seconds: float | None = None,
) -> None:
    """向子进程 stdin 写一条 JSON 行;写入在独立线程里做,超时可控。"""
    if proc.stdin is None:
        raise McpDiscoveryError("MCP stdio stdin 不可用。", code=CONNECT_FAILED)
    outcome: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

    def write() -> None:
        try:
            assert proc.stdin is not None
            proc.stdin.write(json.dumps(payload, ensure_ascii=False) + "\n")
            proc.stdin.flush()
            outcome.put(None)
        except (BrokenPipeError, OSError, UnicodeError, ValueError) as exc:
            outcome.put(exc)

    writer = threading.Thread(target=write, name="mcp-stdin-writer", daemon=True)
    writer.start()
    try:
        error = outcome.get(timeout=timeout_seconds)
    except queue.Empty as exc:
        raise McpDiscoveryError("向 MCP stdio server 发送请求超时。", code=TIMEOUT, cause=exc) from exc
    if error is not None:
        exit_code = proc.poll()
        suffix = f"（退出码 {exit_code}）" if exit_code is not None else ""
        raise McpDiscoveryError(
            f"无法向 MCP stdio server 发送请求{suffix}：{error}", code=CONNECT_FAILED, cause=error
        ) from error


def _read_response(
    proc: subprocess.Popen[str],
    reader: _PipeReader,
    expected_id: int,
    timeout_seconds: float,
    stderr: _StderrCollector | None = None,
) -> dict[str, Any]:
    """读到 id 匹配的 JSON-RPC 响应为止;跳过通知与无法解析的行。"""
    deadline = time.monotonic() + max(timeout_seconds, 0)
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise McpDiscoveryError(
                f"MCP stdio 等待响应超时：id={expected_id}{_stderr_text(stderr)}", code=TIMEOUT
            )
        try:
            event, value = reader.next_event(remaining)
        except TimeoutError as exc:
            raise McpDiscoveryError(
                f"MCP stdio 等待响应超时：id={expected_id}{_stderr_text(stderr)}",
                code=TIMEOUT,
                cause=exc,
            ) from exc
        if event == "error":
            if isinstance(value, McpDiscoveryError):
                raise value
            raise McpDiscoveryError(
                f"读取 MCP stdio 响应失败：{value}{_stderr_text(stderr)}",
                code=CONNECT_FAILED,
                cause=value if isinstance(value, BaseException) else None,
            )
        if event == "eof":
            with suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=0.1)
            if stderr is not None:
                stderr.wait()
            exit_code = proc.poll()
            suffix = f"（退出码 {exit_code}）" if exit_code is not None else "（stdout 已关闭）"
            raise McpDiscoveryError(
                f"MCP stdio server 在返回响应前退出{suffix}。{_stderr_text(stderr)}".strip(),
                code=CONNECT_FAILED,
            )
        line = str(value)
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("id") == expected_id:
            return payload


def _raise_json_rpc_error(payload: Mapping[str, Any]) -> None:
    if "error" not in payload:
        return
    error = payload.get("error") or {}
    if isinstance(error, Mapping):
        message = str(error.get("message") or dict(error))
    else:
        message = str(error)
    raise McpDiscoveryError(message, code=PROTOCOL_ERROR)


def _stderr_text(collector: _StderrCollector | None) -> str:
    return collector.text() if collector is not None else ""


def _content_text(content: Any) -> str:
    """从 tools/call 的 content 列表里抽取 type==text 的文本,换行拼接。"""
    if not isinstance(content, list):
        return ""
    parts = [
        str(item.get("text") or "")
        for item in content
        if isinstance(item, Mapping) and item.get("type") == "text"
    ]
    return "\n".join(part for part in parts if part)


def _extract_tool_result(raw: Any) -> Any:
    """归一化 tools/call 的 result:isError 抛 TOOL_ERROR;优先 structuredContent,否则取文本。"""
    if not isinstance(raw, Mapping):
        return raw
    if raw.get("isError"):
        message = _content_text(raw.get("content")) or "MCP 工具返回 isError=true"
        raise McpDiscoveryError(message, code=TOOL_ERROR)
    structured = raw.get("structuredContent")
    if structured is not None:
        return structured
    text = _content_text(raw.get("content"))
    if text:
        return text
    return dict(raw)


def _normalize_tool_definition(item: Mapping[str, Any]) -> DiscoveredTool:
    """把 tools/list 的原始条目归一化为 `DiscoveredTool`。

    兼容 camelCase(inputSchema)与 snake_case(input_schema)两种键名;
    非 dict 的 schema/annotations/_meta 一律收敛为空 dict。
    """
    input_schema = item.get("inputSchema") or item.get("input_schema") or {}
    output_schema = item.get("outputSchema") or item.get("output_schema") or {}
    annotations = item.get("annotations") if isinstance(item.get("annotations"), Mapping) else {}
    meta = item.get("_meta") if isinstance(item.get("_meta"), Mapping) else {}
    return DiscoveredTool(
        name=str(item.get("name") or "").strip(),
        title=str(item.get("title") or "").strip(),
        description=str(item.get("description") or "").strip(),
        input_schema=dict(input_schema) if isinstance(input_schema, Mapping) else {},
        output_schema=dict(output_schema) if isinstance(output_schema, Mapping) else {},
        annotations=dict(annotations),
        meta=dict(meta),
    )
