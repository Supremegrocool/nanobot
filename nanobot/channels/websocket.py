"""WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/websocket.py

【功能说明】
本文件属于 P1 学习范围，重点帮助初学者理解“外部系统 ↔ nanobot 后端”之间的适配层。
阅读时可以先看类和函数的中文说明，再沿着消息、配置、异常和返回值四条线索跟代码。

【主要职责】
1. 接收配置或输入数据，整理成后端内部统一使用的结构。
2. 调用第三方 SDK、HTTP API 或公共工具函数完成实际工作。
3. 把外部返回值、错误和流式事件转换成 nanobot 可继续处理的数据。
4. 在边界处处理鉴权、限流、媒体文件、重试和日志，避免复杂度泄漏到核心 Agent。

【学习提示】
如果你是 Agent 或后端初学者，可以把本文件看成“翻译器”：它不改变核心 Agent 思路，
而是负责理解某个平台或服务商的协议，并把它翻译成项目内部约定的数据形状。
"""

from __future__ import annotations

import asyncio
import hmac
import json
import re
import ssl
import uuid
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, Self

from pydantic import Field, field_validator, model_validator
from websockets.asyncio.server import ServerConnection, serve, unix_serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request as WsRequest

from nanobot.bus.events import OUTBOUND_META_AGENT_UI, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.security.workspace_access import (
    WORKSPACE_SCOPE_METADATA_KEY,
    WorkspaceScopeError,
)
from nanobot.session.goal_state import goal_state_ws_blob
from nanobot.session.webui_turns import websocket_turn_wall_started_at
from nanobot.utils.media_decode import (
    FileSizeExceeded,
    save_base64_data_url,
)
from nanobot.webui.cli_apps_api import normalize_cli_app_mentions
from nanobot.webui.forking import handle_webui_fork_chat
from nanobot.webui.gateway_services import GatewayServices
from nanobot.webui.http_utils import (
    normalize_config_path as _normalize_config_path,
)
from nanobot.webui.http_utils import (
    parse_request_path as _parse_request_path,
)
from nanobot.webui.http_utils import (
    query_first as _query_first,
)
from nanobot.webui.mcp_presets_api import normalize_mcp_preset_mentions
from nanobot.webui.transcription_ws import webui_transcription_event
from nanobot.webui.websocket_logging import websockets_server_logger


class WebSocketConfig(Base):
    """WebSocketConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WebSocketConfig

    【功能说明】
    WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8765
    unix_socket_path: str = ""
    path: str = "/"
    token: str = ""
    token_issue_path: str = ""
    token_issue_secret: str = ""
    token_ttl_s: int = Field(default=300, ge=30, le=86_400)
    websocket_requires_token: bool = True
    allow_from: list[str] = Field(default_factory=lambda: ["*"])
    streaming: bool = True
    # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    max_message_bytes: int = Field(default=37_748_736, ge=1024, le=41_943_040)
    ping_interval_s: float = Field(default=20.0, ge=5.0, le=300.0)
    ping_timeout_s: float = Field(default=20.0, ge=5.0, le=300.0)
    ssl_certfile: str = ""
    ssl_keyfile: str = ""

    @field_validator("unix_socket_path")
    @classmethod
    def unix_socket_path_format(cls, value: str) -> str:
        """格式化内容（unix_socket_path_format = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketConfig.unix_socket_path_format` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        value = value.strip()
        if not value:
            return ""
        if "\x00" in value:
            raise ValueError("unix_socket_path must not contain NUL bytes")
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise ValueError("unix_socket_path must be an absolute path")
        return str(path)

    @field_validator("path")
    @classmethod
    def path_must_start_with_slash(cls, value: str) -> str:
        """启动流程（path_must_start_with_slash = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketConfig.path_must_start_with_slash` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not value.startswith("/"):
            raise ValueError('path must start with "/"')
        return _normalize_config_path(value)

    @field_validator("token_issue_path")
    @classmethod
    def token_issue_path_format(cls, value: str) -> str:
        """格式化内容（token_issue_path_format = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketConfig.token_issue_path_format` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        value = value.strip()
        if not value:
            return ""
        if not value.startswith("/"):
            raise ValueError('token_issue_path must start with "/"')
        return _normalize_config_path(value)

    @model_validator(mode="after")
    def token_issue_path_differs_from_ws_path(self) -> Self:
        """执行辅助逻辑（token_issue_path_differs_from_ws_path = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketConfig.token_issue_path_differs_from_ws_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.token_issue_path:
            return self
        if _normalize_config_path(self.token_issue_path) == _normalize_config_path(self.path):
            raise ValueError("token_issue_path must differ from path (the WebSocket upgrade path)")
        return self

    @model_validator(mode="after")
    def wildcard_host_requires_auth(self) -> Self:
        """执行辅助逻辑（wildcard_host_requires_auth = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketConfig.wildcard_host_requires_auth` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.host not in ("0.0.0.0", "::"):
            return self
        if self.token.strip() or self.token_issue_secret.strip():
            return self
        raise ValueError(
            "host is 0.0.0.0 (all interfaces) but neither token nor "
            "token_issue_secret is set — set one to prevent unauthenticated access"
        )


def publish_runtime_model_update(
    bus: MessageBus,
    model: str,
    model_preset: str | None,
) -> None:
    """执行辅助逻辑（publish_runtime_model_update = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `publish_runtime_model_update` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    model_preset: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    bus.outbound.put_nowait(OutboundMessage(
        channel="websocket",
        chat_id="*",
        content="",
        metadata={
            "_runtime_model_updated": True,
            "model": model,
            "model_preset": model_preset,
        },
    ))


def _parse_inbound_payload(raw: str) -> str | None:
    """解析数据（_parse_inbound_payload = 原函数名）。

    【中文名称】解析数据

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_parse_inbound_payload` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    text = raw.strip()
    if not text:
        return None
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(data, dict):
            for key in ("content", "text", "message"):
                value = data.get(key)
                if isinstance(value, str) and value.strip():
                    return value
            return None
        return None
    return text


# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
_CHAT_ID_RE = re.compile(r"^[A-Za-z0-9_:-]{1,64}$")


def _is_valid_chat_id(value: Any) -> bool:
    """判断条件是否成立（_is_valid_chat_id = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_is_valid_chat_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return isinstance(value, str) and _CHAT_ID_RE.match(value) is not None


def _parse_envelope(raw: str) -> dict[str, Any] | None:
    """解析数据（_parse_envelope = 原函数名）。

    【中文名称】解析数据

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_parse_envelope` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    text = raw.strip()
    if not text.startswith("{"):
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    t = data.get("type")
    if not isinstance(t, str):
        return None
    return data


# 中文说明：这一段围绕消息、媒体处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这一段围绕消息、图片处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
_MAX_IMAGES_PER_MESSAGE = 4
_MAX_IMAGE_BYTES = 8 * 1024 * 1024
_MAX_VIDEOS_PER_MESSAGE = 1
_MAX_VIDEO_BYTES = 20 * 1024 * 1024

# 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
_IMAGE_MIME_ALLOWED: frozenset[str] = frozenset({
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/gif",
})

_VIDEO_MIME_ALLOWED: frozenset[str] = frozenset({
    "video/mp4",
    "video/webm",
    "video/quicktime",
})

_UPLOAD_MIME_ALLOWED: frozenset[str] = _IMAGE_MIME_ALLOWED | _VIDEO_MIME_ALLOWED

_DATA_URL_MIME_RE = re.compile(r"^data:([^;,]+)(?:;[^,]*)*;base64,", re.DOTALL)


def _extract_data_url_mime(url: str) -> str | None:
    """提取信息（_extract_data_url_mime = 原函数名）。

    【中文名称】提取信息

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_extract_data_url_mime` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not isinstance(url, str):
        return None
    m = _DATA_URL_MIME_RE.match(url)
    if not m:
        return None
    return m.group(1).strip().lower() or None


def _is_websocket_upgrade(request: WsRequest) -> bool:
    """判断条件是否成立（_is_websocket_upgrade = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_is_websocket_upgrade` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    request: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    upgrade = request.headers.get("Upgrade") or request.headers.get("upgrade")
    connection = request.headers.get("Connection") or request.headers.get("connection")
    if not upgrade or "websocket" not in upgrade.lower():
        return False
    if not connection or "upgrade" not in connection.lower():
        return False
    return True


class WebSocketChannel(BaseChannel):
    """WebSocketChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WebSocketChannel

    【功能说明】
    WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "websocket"
    display_name = "WebSocket"

    def __init__(
        self,
        config: Any,
        bus: MessageBus,
        *,
        gateway: GatewayServices,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        gateway: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = WebSocketConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: WebSocketConfig = config
        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        self._subs: dict[str, set[Any]] = {}
        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        self._conn_chats: dict[Any, set[str]] = {}
        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        self._conn_default: dict[Any, str] = {}
        self._stop_event: asyncio.Event | None = None
        self._server_task: asyncio.Task[None] | None = None

        self.gateway = gateway
        self._http_router = gateway.http
        self._tokens = gateway.tokens
        self._media = gateway.media
        self._transcripts = gateway.transcripts
        self._workspaces = gateway.workspaces

        self._stream_text_buffers: dict[tuple[str, str], list[str]] = {}

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def _workspace_controls_available(self, connection: Any) -> bool:
        """执行辅助逻辑（_workspace_controls_available = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._workspace_controls_available` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return self._http_router.workspace_controls_available(connection)

    def _attach(self, connection: Any, chat_id: str) -> None:
        """执行辅助逻辑（_attach = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._attach` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._subs.setdefault(chat_id, set()).add(connection)
        self._conn_chats.setdefault(connection, set()).add(chat_id)

    def _cleanup_connection(self, connection: Any) -> None:
        """清理资源（_cleanup_connection = 原函数名）。

        【中文名称】清理资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._cleanup_connection` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        chat_ids = self._conn_chats.pop(connection, set())
        for cid in chat_ids:
            subs = self._subs.get(cid)
            if subs is None:
                continue
            subs.discard(connection)
            if not subs:
                self._subs.pop(cid, None)
        self._conn_default.pop(connection, None)

    async def _maybe_push_active_goal_state(self, chat_id: str) -> None:
        """异步执行辅助逻辑（_maybe_push_active_goal_state = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._maybe_push_active_goal_state` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.gateway.session_manager is None:
            return
        row = self.gateway.session_manager.read_session_file(f"websocket:{chat_id}")
        meta = row.get("metadata", {}) if isinstance(row, dict) else {}
        if not isinstance(meta, dict):
            meta = {}
        blob = goal_state_ws_blob(meta)
        if not blob.get("active"):
            return
        await self.send_goal_state(chat_id, blob)

    async def _maybe_push_turn_run_wall_clock(self, chat_id: str) -> None:
        """异步运行流程（_maybe_push_turn_run_wall_clock = 原函数名）。

        【中文名称】运行流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._maybe_push_turn_run_wall_clock` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        t0 = websocket_turn_wall_started_at(chat_id)
        if t0 is None:
            return
        await self.send_goal_status(chat_id, "running", started_at=t0)

    async def _hydrate_after_subscribe(self, chat_id: str) -> None:
        """异步执行辅助逻辑（_hydrate_after_subscribe = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._hydrate_after_subscribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._maybe_push_active_goal_state(chat_id)
        await self._maybe_push_turn_run_wall_clock(chat_id)

    async def _send_event(self, connection: Any, event: str, **fields: Any) -> None:
        """异步发送消息（_send_event = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._send_event` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        event: 外部平台事件对象，包含用户输入和平台元数据。
        **fields: 额外关键字参数，通常向下透传给 SDK 或工具函数。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        payload: dict[str, Any] = {"event": event}
        payload.update(fields)
        raw = json.dumps(payload, ensure_ascii=False)
        try:
            await connection.send(raw)
        except ConnectionClosed:
            self._cleanup_connection(connection)
        except Exception as e:
            self.logger.warning("failed to send {} event: {}", event, e)

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return WebSocketConfig().model_dump(by_alias=True)

    def _expected_path(self) -> str:
        """执行辅助逻辑（_expected_path = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._expected_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return _normalize_config_path(self.config.path)

    def _build_ssl_context(self) -> ssl.SSLContext | None:
        """构建对象（_build_ssl_context = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._build_ssl_context` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        cert = self.config.ssl_certfile.strip()
        key = self.config.ssl_keyfile.strip()
        if not cert and not key:
            return None
        if not cert or not key:
            raise ValueError(
                "ssl_certfile and ssl_keyfile must both be set for WSS, or both left empty"
            )
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(certfile=cert, keyfile=key)
        return ctx

    # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。

    async def _dispatch_http(self, connection: Any, request: WsRequest) -> Any:
        """异步执行辅助逻辑（_dispatch_http = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._dispatch_http` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        request: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        got, query = _parse_request_path(request.path)

        # 中文说明：这一段围绕WebSocket处理，注意输入、输出和异常路径。
        expected_ws = self._expected_path()
        if got == expected_ws and _is_websocket_upgrade(request):
            client_id = _query_first(query, "client_id") or ""
            if len(client_id) > 128:
                client_id = client_id[:128]
            if not self.is_allowed(client_id):
                return connection.respond(403, "Forbidden")
            return self._authorize_websocket_handshake(connection, query)

        # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
        return await self._http_router.dispatch(connection, request)

    def _authorize_websocket_handshake(self, connection: Any, query: dict[str, list[str]]) -> Any:
        """执行辅助逻辑（_authorize_websocket_handshake = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._authorize_websocket_handshake` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        query: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        supplied = _query_first(query, "token")
        static_token = self.config.token.strip()

        if static_token:
            if supplied and hmac.compare_digest(supplied, static_token):
                return None
            if supplied and self._tokens.take_issued_token_if_valid(supplied):
                return None
            return connection.respond(401, "Unauthorized")

        if self.config.websocket_requires_token:
            if supplied and self._tokens.take_issued_token_if_valid(supplied):
                return None
            return connection.respond(401, "Unauthorized")

        if supplied:
            self._tokens.take_issued_token_if_valid(supplied)
        return None

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        from nanobot.utils.logging_bridge import redirect_lib_logging

        redirect_lib_logging("websockets", level="WARNING")
        ws_logger = websockets_server_logger()

        self._running = True
        self._stop_event = asyncio.Event()

        ssl_context = self._build_ssl_context()
        scheme = "wss" if ssl_context else "ws"

        async def process_request(
            connection: ServerConnection,
            request: WsRequest,
        ) -> Any:
            """异步执行辅助逻辑（process_request = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `WebSocketChannel.process_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            request: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            return await self._dispatch_http(connection, request)

        async def handler(connection: ServerConnection) -> None:
            """异步执行辅助逻辑（handler = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `WebSocketChannel.handler` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await self._connection_loop(connection)

        self.logger.info(
            "WebSocket server listening on {}",
            (
                f"unix:{self.config.unix_socket_path}{self.config.path}"
                if self.config.unix_socket_path
                else f"{scheme}://{self.config.host}:{self.config.port}{self.config.path}"
            ),
        )
        if self.config.token_issue_path:
            self.logger.info(
                "WebSocket token issue route: {}",
                (
                    f"unix:{self.config.unix_socket_path}{_normalize_config_path(self.config.token_issue_path)}"
                    if self.config.unix_socket_path
                    else (
                        f"{scheme}://{self.config.host}:{self.config.port}"
                        f"{_normalize_config_path(self.config.token_issue_path)}"
                    )
                ),
            )

        async def runner() -> None:
            """异步执行辅助逻辑（runner = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `WebSocketChannel.runner` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            无显式参数。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            socket_path = self.config.unix_socket_path
            if socket_path:
                path_obj = Path(socket_path)
                path_obj.parent.mkdir(parents=True, exist_ok=True)
                with suppress(FileNotFoundError):
                    path_obj.unlink()
                server = await unix_serve(
                    handler,
                    socket_path,
                    process_request=process_request,
                    max_size=self.config.max_message_bytes,
                    ping_interval=self.config.ping_interval_s,
                    ping_timeout=self.config.ping_timeout_s,
                    logger=ws_logger,
                )
                with suppress(OSError):
                    path_obj.chmod(0o600)
            else:
                server = await serve(
                    handler,
                    self.config.host,
                    self.config.port,
                    process_request=process_request,
                    max_size=self.config.max_message_bytes,
                    ping_interval=self.config.ping_interval_s,
                    ping_timeout=self.config.ping_timeout_s,
                    ssl=ssl_context,
                    logger=ws_logger,
                )
            try:
                assert self._stop_event is not None
                await self._stop_event.wait()
            finally:
                server.close()
                await server.wait_closed()
                if socket_path:
                    with suppress(FileNotFoundError):
                        Path(socket_path).unlink()

        self._server_task = asyncio.create_task(runner())
        await self._server_task

    async def _connection_loop(self, connection: Any) -> None:
        """异步执行辅助逻辑（_connection_loop = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._connection_loop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        request = connection.request
        path_part = request.path if request else "/"
        _, query = _parse_request_path(path_part)
        client_id_raw = _query_first(query, "client_id")
        client_id = client_id_raw.strip() if client_id_raw else ""
        if not client_id:
            client_id = f"anon-{uuid.uuid4().hex[:12]}"
        elif len(client_id) > 128:
            self.logger.warning("client_id too long ({} chars), truncating", len(client_id))
            client_id = client_id[:128]

        default_chat_id = str(uuid.uuid4())

        try:
            await connection.send(
                json.dumps(
                    {
                        "event": "ready",
                        "chat_id": default_chat_id,
                        "client_id": client_id,
                    },
                    ensure_ascii=False,
                )
            )
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            self._conn_default[connection] = default_chat_id
            self._attach(connection, default_chat_id)
            await self._hydrate_after_subscribe(default_chat_id)

            async for raw in connection:
                if isinstance(raw, bytes):
                    try:
                        raw = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        self.logger.warning("ignoring non-utf8 binary frame")
                        continue

                envelope = _parse_envelope(raw)
                if envelope is not None:
                    await self._dispatch_envelope(connection, client_id, envelope)
                    continue

                content = _parse_inbound_payload(raw)
                if content is None:
                    continue
                # 中文说明：这一段围绕WebSocket、令牌处理，注意输入、输出和异常路径。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                await self._handle_message(
                    sender_id=client_id,
                    chat_id=default_chat_id,
                    content=content,
                    metadata={"remote": getattr(connection, "remote_address", None)},
                    is_dm=False,
                )
        except Exception as e:
            self.logger.debug("connection ended: {}", e)
        finally:
            self._cleanup_connection(connection)

    # 中文说明：这一段围绕WebSocket处理，注意输入、输出和异常路径。

    def _save_envelope_media(
        self,
        media: list[Any],
    ) -> tuple[list[str], str | None]:
        """保存数据（_save_envelope_media = 原函数名）。

        【中文名称】保存数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._save_envelope_media` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        image_count = 0
        video_count = 0
        for item in media:
            mime = _extract_data_url_mime(item.get("data_url", "")) if isinstance(item, dict) else None
            if mime in _VIDEO_MIME_ALLOWED:
                video_count += 1
            elif mime in _IMAGE_MIME_ALLOWED:
                image_count += 1
        if image_count > _MAX_IMAGES_PER_MESSAGE:
            return [], "too_many_images"
        if video_count > _MAX_VIDEOS_PER_MESSAGE:
            return [], "too_many_videos"

        media_dir = get_media_dir("websocket")
        paths: list[str] = []

        def _abort(reason: str) -> tuple[list[str], str]:
            """执行辅助逻辑（_abort = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `WebSocketChannel._abort` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            reason: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            for p in paths:
                try:
                    Path(p).unlink(missing_ok=True)
                except OSError as exc:
                    self.logger.warning(
                        "failed to unlink partial media {}: {}", p, exc
                    )
            return [], reason

        for item in media:
            if not isinstance(item, dict):
                return _abort("malformed")
            data_url = item.get("data_url")
            if not isinstance(data_url, str) or not data_url:
                return _abort("malformed")
            mime = _extract_data_url_mime(data_url)
            if mime is None:
                return _abort("decode")
            if mime not in _UPLOAD_MIME_ALLOWED:
                return _abort("mime")
            is_video = mime in _VIDEO_MIME_ALLOWED
            max_bytes = _MAX_VIDEO_BYTES if is_video else _MAX_IMAGE_BYTES
            try:
                saved = save_base64_data_url(
                    data_url, media_dir, max_bytes=max_bytes,
                )
            except FileSizeExceeded:
                return _abort("size")
            except Exception as exc:
                self.logger.warning("media decode failed: {}", exc)
                return _abort("decode")
            if saved is None:
                return _abort("decode")
            paths.append(saved)
        return paths, None

    async def _dispatch_envelope(
        self,
        connection: Any,
        client_id: str,
        envelope: dict[str, Any],
    ) -> None:
        """异步执行辅助逻辑（_dispatch_envelope = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._dispatch_envelope` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        client_id: 第三方 SDK 或 HTTP 客户端实例。
        envelope: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        t = envelope.get("type")
        if t == "new_chat":
            new_id = str(uuid.uuid4())
            scope = await self._workspace_scope_or_error(
                connection,
                lambda: self._workspaces.scope_for_new_chat(
                    envelope,
                    controls_available=self._workspace_controls_available(connection),
                ),
            )
            if scope is None:
                return
            self._workspaces.persist_scope(new_id, scope)
            self._attach(connection, new_id)
            await self._send_event(connection, "attached", chat_id=new_id)
            await self._send_event(
                connection,
                "session_updated",
                chat_id=new_id,
                scope="metadata",
                workspace_scope=scope.payload(),
            )
            await self._hydrate_after_subscribe(new_id)
            return
        if t == "fork_chat":
            await handle_webui_fork_chat(self, connection, envelope)
            return
        if t == "attach":
            cid = envelope.get("chat_id")
            if not _is_valid_chat_id(cid):
                await self._send_event(connection, "error", detail="invalid chat_id")
                return
            self._attach(connection, cid)
            await self._send_event(connection, "attached", chat_id=cid)
            await self._hydrate_after_subscribe(cid)
            return
        if t == "set_workspace_scope":
            cid = envelope.get("chat_id")
            if not _is_valid_chat_id(cid):
                await self._send_event(connection, "error", detail="invalid chat_id")
                return
            scope = await self._workspace_scope_or_error(
                connection,
                lambda: self._workspaces.scope_for_set_request(
                    envelope,
                    chat_id=cid,
                    chat_running=websocket_turn_wall_started_at(cid) is not None,
                    controls_available=self._workspace_controls_available(connection),
                ),
                chat_id=cid,
            )
            if scope is None:
                return
            self._workspaces.persist_scope(cid, scope)
            await self._send_event(
                connection,
                "session_updated",
                chat_id=cid,
                scope="metadata",
                workspace_scope=scope.payload(),
            )
            return
        if t == "transcribe_audio":
            event, payload = await webui_transcription_event(envelope)
            await self._send_event(connection, event, **payload)
            return
        if t == "message":
            cid = envelope.get("chat_id")
            content = envelope.get("content")
            if not _is_valid_chat_id(cid):
                await self._send_event(connection, "error", detail="invalid chat_id")
                return
            if not isinstance(content, str):
                await self._send_event(connection, "error", detail="missing content")
                return

            raw_media = envelope.get("media")
            media_paths: list[str] = []
            if raw_media is not None:
                if not isinstance(raw_media, list):
                    await self._send_event(
                        connection, "error",
                        detail="image_rejected", reason="malformed",
                    )
                    return
                media_paths, reason = self._save_envelope_media(raw_media)
                if reason is not None:
                    await self._send_event(
                        connection, "error",
                        detail="image_rejected", reason=reason,
                    )
                    return

            # 中文说明：这一段围绕媒体、图片处理，注意输入、输出和异常路径。
            if not content.strip() and not media_paths:
                await self._send_event(connection, "error", detail="missing content")
                return
            scope = await self._workspace_scope_or_error(
                connection,
                lambda: self._workspaces.scope_for_message(
                    envelope,
                    chat_id=cid,
                    chat_running=websocket_turn_wall_started_at(cid) is not None,
                    controls_available=self._workspace_controls_available(connection),
                ),
                chat_id=cid,
            )
            if scope is None:
                return

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            self._attach(connection, cid)
            await self._hydrate_after_subscribe(cid)
            metadata: dict[str, Any] = {"remote": getattr(connection, "remote_address", None)}
            if envelope.get("webui") is True:
                metadata["webui"] = True
                metadata.update(self._transcripts.client_turn_metadata(envelope.get("turn_id")))
            cli_apps = normalize_cli_app_mentions(envelope.get("cli_apps"))
            if cli_apps:
                metadata["cli_apps"] = cli_apps
            mcp_presets = normalize_mcp_preset_mentions(envelope.get("mcp_presets"))
            if mcp_presets:
                metadata["mcp_presets"] = mcp_presets
            metadata[WORKSPACE_SCOPE_METADATA_KEY] = scope.metadata()
            self._workspaces.persist_scope(cid, scope)
            image_generation = envelope.get("image_generation")
            if isinstance(image_generation, dict) and image_generation.get("enabled") is True:
                aspect_ratio = image_generation.get("aspect_ratio")
                metadata["image_generation"] = {
                    "enabled": True,
                    "aspect_ratio": aspect_ratio if isinstance(aspect_ratio, str) else None,
                }
            if metadata.get("webui") is True and self.is_allowed(client_id):
                self._transcripts.append_user_message(
                    cid,
                    content,
                    metadata=metadata,
                    media_paths=media_paths or None,
                    cli_apps=cli_apps or None,
                    mcp_presets=mcp_presets or None,
                )
            await self._handle_message(
                sender_id=client_id,
                chat_id=cid,
                content=content,
                media=media_paths or None,
                metadata=metadata,
                is_dm=False,
            )
            return
        await self._send_event(connection, "error", detail=f"unknown type: {t!r}")

    async def _workspace_scope_or_error(
        self,
        connection: Any,
        resolver: Callable[[], Any],
        *,
        chat_id: str | None = None,
    ) -> Any | None:
        """异步执行辅助逻辑（_workspace_scope_or_error = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._workspace_scope_or_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        resolver: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            return resolver()
        except WorkspaceScopeError as exc:
            await self._send_event(
                connection,
                "error",
                detail="workspace_scope_rejected",
                reason=exc.message,
                **({"chat_id": chat_id} if chat_id else {}),
            )
            return None

    # 中文说明：这一段围绕WebSocket、事件处理，注意输入、输出和异常路径。

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._running:
            return
        self._running = False
        if self._stop_event:
            self._stop_event.set()
        if self._server_task:
            try:
                await self._server_task
            except Exception as e:
                self.logger.warning("server task error during shutdown: {}", e)
            self._server_task = None
        self._subs.clear()
        self._conn_chats.clear()
        self._conn_default.clear()
        self._tokens.clear()

    async def _safe_send_to(self, connection: Any, raw: str, *, label: str = "") -> None:
        """异步发送消息（_safe_send_to = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel._safe_send_to` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        connection: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            await connection.send(raw)
        except ConnectionClosed:
            self._cleanup_connection(connection)
            self.logger.warning("connection gone{}", label)
        except Exception:
            self.logger.exception("send failed{}", label)
            raise

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if msg.metadata.get("_runtime_model_updated"):
            await self.send_runtime_model_updated(
                model_name=msg.metadata.get("model"),
                model_preset=msg.metadata.get("model_preset"),
            )
            return

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        conns = list(self._subs.get(msg.chat_id, ()))
        if not conns:
            if (
                msg.metadata.get("_progress")
                or msg.metadata.get("_file_edit_events")
                or msg.metadata.get("_turn_end")
                or msg.metadata.get("_session_updated")
                or msg.metadata.get("_goal_status")
                or msg.metadata.get("_goal_state_sync")
            ):
                self.logger.debug("no active subscribers for chat_id={}", msg.chat_id)
            else:
                self.logger.warning("no active subscribers for chat_id={}", msg.chat_id)
        if msg.metadata.get("_goal_state_sync"):
            if conns:
                blob = msg.metadata.get("goal_state")
                await self.send_goal_state(msg.chat_id, blob if isinstance(blob, dict) else {"active": False})
            return
        if msg.metadata.get("_goal_status"):
            if conns:
                status = msg.metadata.get("goal_status")
                if status in ("running", "idle"):
                    started_raw = msg.metadata.get("started_at", msg.metadata.get("goal_started_at"))
                    await self.send_goal_status(
                        msg.chat_id,
                        status,
                        started_at=float(started_raw) if isinstance(started_raw, int | float) else None,
                    )
            return
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if msg.metadata.get("_turn_end"):
            lat = msg.metadata.get("latency_ms")
            lat_i = int(lat) if isinstance(lat, (int, float)) else None
            gs = msg.metadata.get("goal_state")
            gs_blob = gs if isinstance(gs, dict) else None
            await self.send_turn_end(
                msg.chat_id,
                latency_ms=lat_i,
                goal_state=gs_blob,
                metadata=msg.metadata,
            )
            return
        if msg.metadata.get("_session_updated"):
            if conns:
                scope = msg.metadata.get("_session_update_scope")
                await self.send_session_updated(
                    msg.chat_id,
                    scope=scope if isinstance(scope, str) else None,
                )
            return
        if msg.metadata.get("_file_edit_events"):
            edits = msg.metadata.get("_file_edit_events")
            await self.send_file_edit_events(
                msg.chat_id,
                edits if isinstance(edits, list) else [],
                msg.metadata,
            )
            return
        text = msg.content
        wire_text = self._media.rewrite_local_markdown_images(text)
        payload: dict[str, Any] = {
            "event": "message",
            "chat_id": msg.chat_id,
            "text": wire_text,
        }
        if msg.media:
            payload["media"] = msg.media
            urls: list[dict[str, str]] = []
            for entry in msg.media:
                signed = self._media.sign_or_stage_media_path(Path(entry))
                if signed is not None:
                    urls.append(signed)
            if urls:
                payload["media_urls"] = urls
        if msg.reply_to:
            payload["reply_to"] = msg.reply_to
        lat = msg.metadata.get("latency_ms")
        if isinstance(lat, (int, float)):
            payload["latency_ms"] = int(lat)
        if msg.metadata.get("_tool_events"):
            payload["tool_events"] = msg.metadata["_tool_events"]
        agent_ui = msg.metadata.get(OUTBOUND_META_AGENT_UI)
        if agent_ui is not None:
            payload["agent_ui"] = agent_ui
        # 中文说明：这一段围绕工具、调用、媒体处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if msg.metadata.get("_tool_hint"):
            payload["kind"] = "tool_hint"
        elif msg.metadata.get("_progress"):
            payload["kind"] = "progress"
        phase = "activity" if payload.get("kind") in ("tool_hint", "progress") else "answer"
        self._transcripts.prepare_and_append(
            msg.chat_id,
            payload,
            metadata=msg.metadata,
            phase=phase,
            include_source=True,
            transcript_overrides={"text": text},
        )
        raw = json.dumps(payload, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" ")

    async def send_reasoning_delta(
        self,
        chat_id: str,
        delta: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """异步发送消息（send_reasoning_delta = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_reasoning_delta` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        if not delta:
            return
        meta = metadata or {}
        body: dict[str, Any] = {
            "event": "reasoning_delta",
            "chat_id": chat_id,
            "text": delta,
        }
        stream_id = meta.get("_stream_id")
        if stream_id is not None:
            body["stream_id"] = stream_id
        self._transcripts.prepare_and_append(
            chat_id,
            body,
            metadata=meta,
            phase="reasoning",
        )
        raw = json.dumps(body, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" reasoning ")

    async def send_reasoning_end(
        self,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """异步发送消息（send_reasoning_end = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_reasoning_end` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        meta = metadata or {}
        body: dict[str, Any] = {
            "event": "reasoning_end",
            "chat_id": chat_id,
        }
        stream_id = meta.get("_stream_id")
        if stream_id is not None:
            body["stream_id"] = stream_id
        self._transcripts.prepare_and_append(
            chat_id,
            body,
            metadata=meta,
            phase="reasoning",
        )
        raw = json.dumps(body, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" reasoning_end ")

    async def send_file_edit_events(
        self,
        chat_id: str,
        edits: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """异步发送消息（send_file_edit_events = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_file_edit_events` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        edits: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        payload: dict[str, Any] = {
            "event": "file_edit",
            "chat_id": chat_id,
            "edits": edits,
        }
        self._transcripts.prepare_and_append(
            chat_id,
            payload,
            metadata=metadata,
            phase="activity",
        )
        raw = json.dumps(payload, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" file_edit ")

    async def send_delta(
        self,
        chat_id: str,
        delta: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """异步发送消息（send_delta = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_delta` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        meta = metadata or {}
        stream_key = (chat_id, str(meta.get("_stream_id") or ""))
        if meta.get("_stream_end"):
            body: dict[str, Any] = {"event": "stream_end", "chat_id": chat_id}
            buffered = self._stream_text_buffers.pop(stream_key, [])
            if delta:
                buffered.append(delta)
            full_text = "".join(buffered)
            rewritten = self._media.rewrite_local_markdown_images(full_text)
            if delta or rewritten != full_text:
                body["text"] = rewritten
        else:
            body = {
                "event": "delta",
                "chat_id": chat_id,
                "text": delta,
            }
            self._stream_text_buffers.setdefault(stream_key, []).append(delta)
        if meta.get("_stream_id") is not None:
            body["stream_id"] = meta["_stream_id"]
        self._transcripts.prepare_and_append(
            chat_id,
            body,
            metadata=meta,
            phase="answer",
        )
        raw = json.dumps(body, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" stream ")

    async def send_turn_end(
        self,
        chat_id: str,
        latency_ms: int | None = None,
        *,
        goal_state: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """异步发送消息（send_turn_end = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_turn_end` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        latency_ms: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        goal_state: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        body: dict[str, Any] = {"event": "turn_end", "chat_id": chat_id}
        if latency_ms is not None:
            body["latency_ms"] = int(latency_ms)
        if goal_state is not None:
            body["goal_state"] = goal_state
        self._transcripts.prepare_and_append(
            chat_id,
            body,
            metadata=metadata,
            phase="complete",
        )
        raw = json.dumps(body, ensure_ascii=False)
        if not conns:
            return
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" turn_end ")

    async def send_goal_state(self, chat_id: str, blob: dict[str, Any]) -> None:
        """异步发送消息（send_goal_state = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_goal_state` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        blob: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        if not conns:
            return
        body = {"event": "goal_state", "chat_id": chat_id, "goal_state": blob}
        raw = json.dumps(body, ensure_ascii=False)
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" goal_state ")

    async def send_goal_status(
        self,
        chat_id: str,
        status: str,
        *,
        started_at: float | None = None,
    ) -> None:
        """异步发送消息（send_goal_status = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_goal_status` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        status: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        started_at: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        if not conns:
            return
        body: dict[str, Any] = {
            "event": "goal_status",
            "chat_id": chat_id,
            "status": status,
        }
        if status == "running" and started_at is not None:
            body["started_at"] = started_at
        raw = json.dumps(body, ensure_ascii=False)
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" goal_status ")

    async def send_session_updated(self, chat_id: str, *, scope: str | None = None) -> None:
        """异步发送消息（send_session_updated = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_session_updated` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        scope: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._subs.get(chat_id, ()))
        if not conns:
            return
        body: dict[str, Any] = {"event": "session_updated", "chat_id": chat_id}
        if scope:
            body["scope"] = scope
        raw = json.dumps(body, ensure_ascii=False)
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" session_updated ")

    async def send_runtime_model_updated(
        self,
        *,
        model_name: Any,
        model_preset: Any = None,
    ) -> None:
        """异步发送消息（send_runtime_model_updated = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WebSocket 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WebSocketChannel.send_runtime_model_updated` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        model_name: 模型名称或模型配置，用于选择具体 LLM 能力。
        model_preset: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        conns = list(self._conn_chats)
        if not conns or not isinstance(model_name, str) or not model_name.strip():
            return
        body: dict[str, Any] = {
            "event": "runtime_model_updated",
            "model_name": model_name.strip(),
        }
        if isinstance(model_preset, str) and model_preset.strip():
            body["model_preset"] = model_preset.strip()
        raw = json.dumps(body, ensure_ascii=False)
        for connection in conns:
            await self._safe_send_to(connection, raw, label=" runtime_model_updated ")

