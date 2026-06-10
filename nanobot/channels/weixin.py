"""微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/weixin.py

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
import base64
import hashlib
import json
import os
import random
import re
import time
import uuid
from collections import OrderedDict
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir, get_runtime_subdir
from nanobot.config.schema import Base
from nanobot.utils.helpers import split_message

# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
# 中文说明：这一段围绕微信处理，注意输入、输出和异常路径。
# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
ITEM_TEXT = 1
ITEM_IMAGE = 2
ITEM_VOICE = 3
ITEM_FILE = 4
ITEM_VIDEO = 5

# 中文说明：这一段围绕消息、用户处理，注意输入、输出和异常路径。
MESSAGE_TYPE_BOT = 2

# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
MESSAGE_STATE_FINISH = 2

WEIXIN_MAX_MESSAGE_LEN = 4000
WEIXIN_CHANNEL_VERSION = "2.1.1"
ILINK_APP_ID = "bot"


def _build_client_version(version: str) -> int:
    """构建对象（_build_client_version = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_build_client_version` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    version: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    parts = version.split(".")

    def _as_int(idx: int) -> int:
        """执行辅助逻辑（_as_int = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `_as_int` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        idx: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            return int(parts[idx])
        except Exception:
            return 0

    major = _as_int(0)
    minor = _as_int(1)
    patch = _as_int(2)
    return ((major & 0xFF) << 16) | ((minor & 0xFF) << 8) | (patch & 0xFF)

ILINK_APP_CLIENT_VERSION = _build_client_version(WEIXIN_CHANNEL_VERSION)
BASE_INFO: dict[str, str] = {"channel_version": WEIXIN_CHANNEL_VERSION}

# 中文说明：这一段围绕会话、错误处理，注意输入、输出和异常路径。
ERRCODE_SESSION_EXPIRED = -14
SESSION_PAUSE_DURATION_S = 60 * 60

# 中文说明：这一段围绕令牌、上下文处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这一段围绕令牌、缓存处理，注意输入、输出和异常路径。
CONTEXT_TOKEN_MAX_AGE_S = 60


# 中文说明：这一段围绕重试处理，注意输入、输出和异常路径。
MAX_CONSECUTIVE_FAILURES = 3
BACKOFF_DELAY_S = 30
RETRY_DELAY_S = 2
MAX_QR_REFRESH_COUNT = 3
TYPING_STATUS_TYPING = 1
TYPING_STATUS_CANCEL = 2
TYPING_TICKET_TTL_S = 24 * 60 * 60
TYPING_KEEPALIVE_INTERVAL_S = 5
CONFIG_CACHE_INITIAL_RETRY_S = 2
CONFIG_CACHE_MAX_RETRY_S = 60 * 60

# 中文说明：这一段围绕超时处理，注意输入、输出和异常路径。
DEFAULT_LONG_POLL_TIMEOUT_S = 35

# 中文说明：这一段围绕媒体、图片、文件处理，注意输入、输出和异常路径。
UPLOAD_MEDIA_IMAGE = 1
UPLOAD_MEDIA_VIDEO = 2
UPLOAD_MEDIA_FILE = 3
UPLOAD_MEDIA_VOICE = 4

# 中文说明：这一段围绕媒体、图片、文件处理，注意输入、输出和异常路径。
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".ico", ".svg"}
_VIDEO_EXTS = {".mp4", ".avi", ".mov", ".mkv", ".webm", ".flv"}
_VOICE_EXTS = {".mp3", ".wav", ".amr", ".silk", ".ogg", ".m4a", ".aac", ".flac"}


def _has_downloadable_media_locator(media: dict[str, Any] | None) -> bool:
    """执行辅助逻辑（_has_downloadable_media_locator = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_has_downloadable_media_locator` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    media: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not isinstance(media, dict):
        return False
    return bool(str(media.get("encrypt_query_param", "") or "") or str(media.get("full_url", "") or "").strip())


class WeixinConfig(Base):
    """WeixinConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WeixinConfig

    【功能说明】
    微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    allow_from: list[str] = Field(default_factory=list)
    base_url: str = "https://ilinkai.weixin.qq.com"
    cdn_base_url: str = "https://novac2c.cdn.weixin.qq.com/c2c"
    route_tag: str | int | None = None
    token: str = ""  # 中文说明：这一段围绕令牌处理，注意输入、输出和异常路径。
    state_dir: str = ""  # 中文说明：这一段围绕微信处理，注意输入、输出和异常路径。
    poll_timeout: int = DEFAULT_LONG_POLL_TIMEOUT_S  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


class WeixinChannel(BaseChannel):
    """WeixinChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WeixinChannel

    【功能说明】
    微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "weixin"
    display_name = "WeChat"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return WeixinConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = WeixinConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: WeixinConfig = config

        # 中文说明：State 相关逻辑。
        self._client: httpx.AsyncClient | None = None
        self._get_updates_buf: str = ""
        self._context_tokens: dict[str, str] = {}  # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        self._processed_ids: OrderedDict[str, None] = OrderedDict()
        self._state_dir: Path | None = None
        self._token: str = ""
        self._poll_task: asyncio.Task | None = None
        self._next_poll_timeout_s: int = DEFAULT_LONG_POLL_TIMEOUT_S
        self._session_pause_until: float = 0.0
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._typing_tickets: dict[str, dict[str, Any]] = {}
        self._context_token_at: dict[str, float] = {}
        self._pending_tool_hints: dict[str, list[str]] = {}

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：State persistence 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    def _get_state_dir(self) -> Path:
        """执行辅助逻辑（_get_state_dir = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._get_state_dir` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._state_dir:
            return self._state_dir
        if self.config.state_dir:
            d = Path(self.config.state_dir).expanduser()
        else:
            d = get_runtime_subdir("weixin")
        d.mkdir(parents=True, exist_ok=True)
        self._state_dir = d
        return d

    def _load_state(self) -> bool:
        """加载数据（_load_state = 原函数名）。

        【中文名称】加载数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._load_state` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        state_file = self._get_state_dir() / "account.json"
        if not state_file.exists():
            return False
        try:
            data = json.loads(state_file.read_text())
            self._token = data.get("token", "")
            self._get_updates_buf = data.get("get_updates_buf", "")
            context_tokens = data.get("context_tokens", {})
            if isinstance(context_tokens, dict):
                self._context_tokens = {
                    str(user_id): str(token)
                    for user_id, token in context_tokens.items()
                    if str(user_id).strip() and str(token).strip()
                }
            else:
                self._context_tokens = {}
            typing_tickets = data.get("typing_tickets", {})
            if isinstance(typing_tickets, dict):
                self._typing_tickets = {
                    str(user_id): ticket
                    for user_id, ticket in typing_tickets.items()
                    if str(user_id).strip() and isinstance(ticket, dict)
                }
            else:
                self._typing_tickets = {}
            base_url = data.get("base_url", "")
            if base_url:
                self.config.base_url = base_url
            return bool(self._token)
        except Exception:
            self.logger.error("Failed to load Weixin account state", exc_info=True)
            return False

    def _save_state(self) -> None:
        """保存数据（_save_state = 原函数名）。

        【中文名称】保存数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._save_state` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        state_file = self._get_state_dir() / "account.json"
        with suppress(Exception):
            data = {
                "token": self._token,
                "get_updates_buf": self._get_updates_buf,
                "context_tokens": self._context_tokens,
                "typing_tickets": self._typing_tickets,
                "base_url": self.config.base_url,
            }
            state_file.write_text(json.dumps(data, ensure_ascii=False))

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕API、HTTP处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    @staticmethod
    def _random_wechat_uin() -> str:
        """执行辅助逻辑（_random_wechat_uin = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._random_wechat_uin` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        无显式参数。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        uint32 = int.from_bytes(os.urandom(4), "big")
        return base64.b64encode(str(uint32).encode()).decode()

    def _make_headers(self, *, auth: bool = True) -> dict[str, str]:
        """执行辅助逻辑（_make_headers = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._make_headers` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        auth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        headers: dict[str, str] = {
            "X-WECHAT-UIN": self._random_wechat_uin(),
            "Content-Type": "application/json",
            "AuthorizationType": "ilink_bot_token",
            "iLink-App-Id": ILINK_APP_ID,
            "iLink-App-ClientVersion": str(ILINK_APP_CLIENT_VERSION),
        }
        if auth and self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if self.config.route_tag is not None and str(self.config.route_tag).strip():
            headers["SKRouteTag"] = str(self.config.route_tag).strip()
        return headers

    @staticmethod
    def _is_retryable_media_download_error(err: Exception) -> bool:
        """下载资源（_is_retryable_media_download_error = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._is_retryable_media_download_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        err: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(err, httpx.TimeoutException | httpx.TransportError):
            return True
        if isinstance(err, httpx.HTTPStatusError):
            status_code = err.response.status_code if err.response is not None else 0
            return status_code >= 500
        return False

    async def _api_get(
        self,
        endpoint: str,
        params: dict | None = None,
        *,
        auth: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> dict:
        """异步执行辅助逻辑（_api_get = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._api_get` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        endpoint: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        params: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        auth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        extra_headers: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        assert self._client is not None
        url = f"{self.config.base_url}/{endpoint}"
        hdrs = self._make_headers(auth=auth)
        if extra_headers:
            hdrs.update(extra_headers)
        resp = await self._client.get(url, params=params, headers=hdrs)
        resp.raise_for_status()
        return resp.json()

    async def _api_get_with_base(
        self,
        *,
        base_url: str,
        endpoint: str,
        params: dict | None = None,
        auth: bool = True,
        extra_headers: dict[str, str] | None = None,
    ) -> dict:
        """异步执行辅助逻辑（_api_get_with_base = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._api_get_with_base` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        base_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        endpoint: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        params: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        auth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        extra_headers: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        assert self._client is not None
        url = f"{base_url.rstrip('/')}/{endpoint}"
        hdrs = self._make_headers(auth=auth)
        if extra_headers:
            hdrs.update(extra_headers)
        resp = await self._client.get(url, params=params, headers=hdrs)
        resp.raise_for_status()
        return resp.json()

    async def _api_post(
        self,
        endpoint: str,
        body: dict | None = None,
        *,
        auth: bool = True,
    ) -> dict:
        """异步执行辅助逻辑（_api_post = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._api_post` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        endpoint: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        body: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        auth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        assert self._client is not None
        url = f"{self.config.base_url}/{endpoint}"
        payload = body or {}
        if "base_info" not in payload:
            payload["base_info"] = BASE_INFO
        resp = await self._client.post(url, json=payload, headers=self._make_headers(auth=auth))
        resp.raise_for_status()
        return resp.json()

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _fetch_qr_code(self) -> tuple[str, str]:
        """异步执行辅助逻辑（_fetch_qr_code = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._fetch_qr_code` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        data = await self._api_get(
            "ilink/bot/get_bot_qrcode",
            params={"bot_type": "3"},
            auth=False,
        )
        qrcode_img_content = data.get("qrcode_img_content", "")
        qrcode_id = data.get("qrcode", "")
        if not qrcode_id:
            raise RuntimeError(f"Failed to get QR code from WeChat API: {data}")
        return qrcode_id, (qrcode_img_content or qrcode_id)

    async def _qr_login(self) -> bool:
        """异步执行辅助逻辑（_qr_login = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._qr_login` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            refresh_count = 0
            qrcode_id, scan_url = await self._fetch_qr_code()
            self._print_qr_code(scan_url)
            current_poll_base_url = self.config.base_url

            while self._running:
                try:
                    status_data = await self._api_get_with_base(
                        base_url=current_poll_base_url,
                        endpoint="ilink/bot/get_qrcode_status",
                        params={"qrcode": qrcode_id},
                        auth=False,
                    )
                except Exception as e:
                    if self._is_retryable_qr_poll_error(e):
                        await asyncio.sleep(1)
                        continue
                    raise

                if not isinstance(status_data, dict):
                    await asyncio.sleep(1)
                    continue

                status = status_data.get("status", "")
                if status == "confirmed":
                    token = status_data.get("bot_token", "")
                    bot_id = status_data.get("ilink_bot_id", "")
                    base_url = status_data.get("baseurl", "")
                    user_id = status_data.get("ilink_user_id", "")
                    if token:
                        self._token = token
                        if base_url:
                            self.config.base_url = base_url
                        self._save_state()
                        self.logger.info(
                            "login successful! bot_id={} user_id={}",
                            bot_id,
                            user_id,
                        )
                        return True
                    else:
                        self.logger.error("Login confirmed but no bot_token in response")
                        return False
                elif status == "scaned_but_redirect":
                    redirect_host = str(status_data.get("redirect_host", "") or "").strip()
                    if redirect_host:
                        if redirect_host.startswith("http://") or redirect_host.startswith("https://"):
                            redirected_base = redirect_host
                        else:
                            redirected_base = f"https://{redirect_host}"
                        if redirected_base != current_poll_base_url:
                            current_poll_base_url = redirected_base
                elif status == "expired":
                    refresh_count += 1
                    if refresh_count > MAX_QR_REFRESH_COUNT:
                        self.logger.warning(
                            "QR code expired too many times ({}/{}), giving up.",
                            refresh_count - 1,
                            MAX_QR_REFRESH_COUNT,
                        )
                        return False
                    qrcode_id, scan_url = await self._fetch_qr_code()
                    current_poll_base_url = self.config.base_url
                    self._print_qr_code(scan_url)
                    continue
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

                await asyncio.sleep(1)

        except Exception:
            self.logger.exception("QR login failed")

        return False

    @staticmethod
    def _is_retryable_qr_poll_error(err: Exception) -> bool:
        """判断条件是否成立（_is_retryable_qr_poll_error = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._is_retryable_qr_poll_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        err: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(err, httpx.TimeoutException | httpx.TransportError):
            return True
        if isinstance(err, httpx.HTTPStatusError):
            status_code = err.response.status_code if err.response is not None else 0
            if status_code >= 500:
                return True
        return False

    @staticmethod
    def _print_qr_code(url: str) -> None:
        """执行辅助逻辑（_print_qr_code = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._print_qr_code` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            import qrcode as qr_lib

            qr = qr_lib.QRCode(border=1)
            qr.add_data(url)
            qr.make(fit=True)
            qr.print_ascii(invert=True)
        except ImportError:
            print(f"\nLogin URL: {url}\n")

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Channel lifecycle 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def login(self, force: bool = False) -> bool:
        """异步执行辅助逻辑（login = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.login` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        force: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if force:
            self._token = ""
            self._get_updates_buf = ""
            state_file = self._get_state_dir() / "account.json"
            if state_file.exists():
                state_file.unlink()
        if self._token or self._load_state():
            return True

        # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60, connect=30),
            follow_redirects=True,
        )
        self._running = True  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        try:
            return await self._qr_login()
        finally:
            self._running = False
            if self._client:
                await self._client.aclose()
                self._client = None

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = True
        self._next_poll_timeout_s = self.config.poll_timeout
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._next_poll_timeout_s + 10, connect=30),
            follow_redirects=True,
        )

        if self.config.token:
            self._token = self.config.token
        elif not self._load_state():
            if not await self._qr_login():
                self.logger.error("login failed. Run 'nanobot channels login weixin' to authenticate.")
                self._running = False
                return

        self.logger.info("channel starting with long-poll...")

        consecutive_failures = 0
        while self._running:
            try:
                await self._poll_once()
                consecutive_failures = 0
            except httpx.TimeoutException:
                # 中文说明：这一段围绕重试处理，注意输入、输出和异常路径。
                continue
            except Exception:
                if not self._running:
                    break
                self.logger.exception("WeChat poll loop error")
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    consecutive_failures = 0
                    await asyncio.sleep(BACKOFF_DELAY_S)
                else:
                    await asyncio.sleep(RETRY_DELAY_S)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        self._pending_tool_hints.clear()
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
        for chat_id in list(self._typing_tasks):
            await self._stop_typing(chat_id, clear_remote=False)
        if self._client:
            await self._client.aclose()
            self._client = None
        self._save_state()
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕微信、Provider处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    def _pause_session(self, duration_s: int = SESSION_PAUSE_DURATION_S) -> None:
        """执行辅助逻辑（_pause_session = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._pause_session` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        duration_s: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._session_pause_until = time.time() + duration_s

    def _session_pause_remaining_s(self) -> int:
        """执行辅助逻辑（_session_pause_remaining_s = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._session_pause_remaining_s` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        remaining = int(self._session_pause_until - time.time())
        if remaining <= 0:
            self._session_pause_until = 0.0
            return 0
        return remaining

    def _assert_session_active(self) -> None:
        """执行辅助逻辑（_assert_session_active = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._assert_session_active` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        remaining = self._session_pause_remaining_s()
        if remaining > 0:
            remaining_min = max((remaining + 59) // 60, 1)
            raise RuntimeError(
                f"WeChat session paused, {remaining_min} min remaining (errcode {ERRCODE_SESSION_EXPIRED})"
            )

    async def _poll_once(self) -> None:
        """异步执行辅助逻辑（_poll_once = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._poll_once` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        remaining = self._session_pause_remaining_s()
        if remaining > 0:
            await asyncio.sleep(remaining)
            return

        body: dict[str, Any] = {
            "get_updates_buf": self._get_updates_buf,
            "base_info": BASE_INFO,
        }

        # 中文说明：这一段围绕HTTP、超时处理，注意输入、输出和异常路径。
        assert self._client is not None
        self._client.timeout = httpx.Timeout(self._next_poll_timeout_s + 10, connect=30)

        data = await self._api_post("ilink/bot/getupdates", body)

        # 中文说明：这一段围绕API、错误处理，注意输入、输出和异常路径。
        ret = data.get("ret", 0)
        errcode = data.get("errcode", 0)

        is_error = (ret is not None and ret != 0) or (errcode is not None and errcode != 0)

        if is_error:
            if errcode == ERRCODE_SESSION_EXPIRED or ret == ERRCODE_SESSION_EXPIRED:
                self._pause_session()
                remaining = self._session_pause_remaining_s()
                self.logger.warning(
                    "session expired (errcode {}). Pausing {} min.",
                    errcode,
                    max((remaining + 59) // 60, 1),
                )
                return
            raise RuntimeError(
                f"getUpdates failed: ret={ret} errcode={errcode} errmsg={data.get('errmsg', '')}"
            )

        # 中文说明：这一段围绕超时处理，注意输入、输出和异常路径。
        server_timeout_ms = data.get("longpolling_timeout_ms")
        if server_timeout_ms and server_timeout_ms > 0:
            self._next_poll_timeout_s = max(server_timeout_ms // 1000, 5)

        # 中文说明：Update cursor 相关逻辑。
        new_buf = data.get("get_updates_buf", "")
        if new_buf:
            self._get_updates_buf = new_buf
            self._save_state()

        # 中文说明：这一段围绕微信、消息处理，注意输入、输出和异常路径。
        msgs: list[dict] = data.get("msgs", []) or []
        for msg in msgs:
            try:
                await self._process_message(msg)
            except Exception:
                self.logger.exception("Failed to process WeChat message")

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _process_message(self, msg: dict) -> None:
        """异步执行辅助逻辑（_process_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._process_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if msg.get("message_type") == MESSAGE_TYPE_BOT:
            return

        msg_id = str(msg.get("message_id", "") or msg.get("seq", ""))
        if not msg_id:
            msg_id = f"{msg.get('from_user_id', '')}_{msg.get('create_time_ms', '')}"

        from_user_id = msg.get("from_user_id", "") or ""
        if not from_user_id:
            return

        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if msg_id in self._processed_ids:
            return
        self._processed_ids[msg_id] = None
        while len(self._processed_ids) > 1000:
            self._processed_ids.popitem(last=False)

        ctx_token = msg.get("context_token", "")
        if not self.is_allowed(from_user_id):
            if from_user_id.endswith("@chatroom"):
                await self._handle_message(
                    sender_id=from_user_id,
                    chat_id=from_user_id,
                    content="",
                    metadata={"message_id": msg_id},
                    is_dm=False,
                )
                return

            if not ctx_token:
                self.logger.warning(
                    "Access denied for sender {}; cannot send WeChat pairing code without context_token",
                    from_user_id,
                )
                return

            had_ctx_token = from_user_id in self._context_tokens
            previous_ctx_token = self._context_tokens.get(from_user_id, "")
            had_ctx_token_at = from_user_id in self._context_token_at
            previous_ctx_token_at = self._context_token_at.get(from_user_id, 0.0)
            self._context_tokens[from_user_id] = ctx_token
            self._context_token_at[from_user_id] = time.time()
            try:
                await self._handle_message(
                    sender_id=from_user_id,
                    chat_id=from_user_id,
                    content="",
                    metadata={"message_id": msg_id},
                    is_dm=True,
                )
            finally:
                if had_ctx_token:
                    self._context_tokens[from_user_id] = previous_ctx_token
                else:
                    self._context_tokens.pop(from_user_id, None)
                if had_ctx_token_at:
                    self._context_token_at[from_user_id] = previous_ctx_token_at
                else:
                    self._context_token_at.pop(from_user_id, None)
            return

        # 中文说明：这一段围绕令牌、上下文、缓存处理，注意输入、输出和异常路径。
        if ctx_token:
            self._context_tokens[from_user_id] = ctx_token
            self._context_token_at[from_user_id] = time.time()
            self._save_state()

        # 中文说明：这一段围绕微信、消息处理，注意输入、输出和异常路径。
        item_list: list[dict] = msg.get("item_list") or []
        content_parts: list[str] = []
        media_paths: list[str] = []
        has_top_level_downloadable_media = False

        for item in item_list:
            item_type = item.get("type", 0)

            if item_type == ITEM_TEXT:
                text = (item.get("text_item") or {}).get("text", "")
                if text:
                    # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
                    ref = item.get("ref_msg")
                    if ref:
                        ref_item = ref.get("message_item")
                        # 中文说明：这一段围绕消息、媒体处理，注意输入、输出和异常路径。
                        if ref_item and ref_item.get("type", 0) in (
                            ITEM_IMAGE,
                            ITEM_VOICE,
                            ITEM_FILE,
                            ITEM_VIDEO,
                        ):
                            content_parts.append(text)
                        else:
                            parts: list[str] = []
                            if ref.get("title"):
                                parts.append(ref["title"])
                            if ref_item:
                                ref_text = (ref_item.get("text_item") or {}).get("text", "")
                                if ref_text:
                                    parts.append(ref_text)
                            if parts:
                                content_parts.append(f"[引用: {' | '.join(parts)}]\n{text}")
                            else:
                                content_parts.append(text)
                    else:
                        content_parts.append(text)

            elif item_type == ITEM_IMAGE:
                image_item = item.get("image_item") or {}
                if _has_downloadable_media_locator(image_item.get("media")):
                    has_top_level_downloadable_media = True
                file_path = await self._download_media_item(image_item, "image")
                if file_path:
                    content_parts.append(f"[image]\n[Image: source: {file_path}]")
                    media_paths.append(file_path)
                else:
                    content_parts.append("[image]")

            elif item_type == ITEM_VOICE:
                voice_item = item.get("voice_item") or {}
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                voice_text = voice_item.get("text", "")
                if voice_text:
                    content_parts.append(f"[voice] {voice_text}")
                else:
                    if _has_downloadable_media_locator(voice_item.get("media")):
                        has_top_level_downloadable_media = True
                    file_path = await self._download_media_item(voice_item, "voice")
                    if file_path:
                        transcription = await self.transcribe_audio(file_path)
                        if transcription:
                            content_parts.append(f"[voice] {transcription}")
                        else:
                            content_parts.append(f"[voice]\n[Audio: source: {file_path}]")
                        media_paths.append(file_path)
                    else:
                        content_parts.append("[voice]")

            elif item_type == ITEM_FILE:
                file_item = item.get("file_item") or {}
                if _has_downloadable_media_locator(file_item.get("media")):
                    has_top_level_downloadable_media = True
                file_name = file_item.get("file_name", "unknown")
                file_path = await self._download_media_item(
                    file_item,
                    "file",
                    file_name,
                )
                if file_path:
                    content_parts.append(f"[file: {file_name}]\n[File: source: {file_path}]")
                    media_paths.append(file_path)
                else:
                    content_parts.append(f"[file: {file_name}]")

            elif item_type == ITEM_VIDEO:
                video_item = item.get("video_item") or {}
                if _has_downloadable_media_locator(video_item.get("media")):
                    has_top_level_downloadable_media = True
                file_path = await self._download_media_item(video_item, "video")
                if file_path:
                    content_parts.append(f"[video]\n[Video: source: {file_path}]")
                    media_paths.append(file_path)
                else:
                    content_parts.append("[video]")

        # 中文说明：兜底。
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        if not media_paths and not has_top_level_downloadable_media:
            ref_media_item: dict[str, Any] | None = None
            for item in item_list:
                if item.get("type", 0) != ITEM_TEXT:
                    continue
                ref = item.get("ref_msg") or {}
                candidate = ref.get("message_item") or {}
                if candidate.get("type", 0) in (ITEM_IMAGE, ITEM_VOICE, ITEM_FILE, ITEM_VIDEO):
                    ref_media_item = candidate
                    break

            if ref_media_item:
                ref_type = ref_media_item.get("type", 0)
                if ref_type == ITEM_IMAGE:
                    image_item = ref_media_item.get("image_item") or {}
                    file_path = await self._download_media_item(image_item, "image")
                    if file_path:
                        content_parts.append(f"[image]\n[Image: source: {file_path}]")
                        media_paths.append(file_path)
                elif ref_type == ITEM_VOICE:
                    voice_item = ref_media_item.get("voice_item") or {}
                    file_path = await self._download_media_item(voice_item, "voice")
                    if file_path:
                        transcription = await self.transcribe_audio(file_path)
                        if transcription:
                            content_parts.append(f"[voice] {transcription}")
                        else:
                            content_parts.append(f"[voice]\n[Audio: source: {file_path}]")
                        media_paths.append(file_path)
                elif ref_type == ITEM_FILE:
                    file_item = ref_media_item.get("file_item") or {}
                    file_name = file_item.get("file_name", "unknown")
                    file_path = await self._download_media_item(file_item, "file", file_name)
                    if file_path:
                        content_parts.append(f"[file: {file_name}]\n[File: source: {file_path}]")
                        media_paths.append(file_path)
                elif ref_type == ITEM_VIDEO:
                    video_item = ref_media_item.get("video_item") or {}
                    file_path = await self._download_media_item(video_item, "video")
                    if file_path:
                        content_parts.append(f"[video]\n[Video: source: {file_path}]")
                        media_paths.append(file_path)

        content = "\n".join(content_parts)
        if not content:
            return

        self.logger.info(
            "inbound: from={} items={} bodyLen={}",
            from_user_id,
            ",".join(str(i.get("type", 0)) for i in item_list),
            len(content),
        )

        await self._start_typing(from_user_id, ctx_token)

        await self._handle_message(
            sender_id=from_user_id,
            chat_id=from_user_id,
            content=content,
            media=media_paths or None,
            metadata={"message_id": msg_id},
        )

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _download_media_item(
        self,
        typed_item: dict,
        media_type: str,
        filename: str | None = None,
    ) -> str | None:
        """异步下载资源（_download_media_item = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._download_media_item` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        typed_item: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filename: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            media = typed_item.get("media") or {}
            encrypt_query_param = str(media.get("encrypt_query_param", "") or "")
            full_url = str(media.get("full_url", "") or "").strip()

            if not encrypt_query_param and not full_url:
                return None

            # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕媒体、图片处理，注意输入、输出和异常路径。
            raw_aeskey_hex = typed_item.get("aeskey", "")
            media_aes_key_b64 = media.get("aes_key", "")

            aes_key_b64: str = ""
            if raw_aeskey_hex:
                # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
                aes_key_b64 = base64.b64encode(bytes.fromhex(raw_aeskey_hex)).decode()
            elif media_aes_key_b64:
                aes_key_b64 = media_aes_key_b64

            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
            if media_type != "image" and not aes_key_b64:
                return None

            assert self._client is not None
            fallback_url = ""
            if encrypt_query_param:
                fallback_url = (
                    f"{self.config.cdn_base_url}/download"
                    f"?encrypted_query_param={quote(encrypt_query_param)}"
                )

            download_candidates: list[tuple[str, str]] = []
            if full_url:
                download_candidates.append(("full_url", full_url))
            if fallback_url and (not full_url or fallback_url != full_url):
                download_candidates.append(("encrypt_query_param", fallback_url))

            data = b""
            for idx, (download_source, cdn_url) in enumerate(download_candidates):
                try:
                    resp = await self._client.get(cdn_url)
                    resp.raise_for_status()
                    data = resp.content
                    break
                except Exception as e:
                    has_more_candidates = idx + 1 < len(download_candidates)
                    should_fallback = (
                        download_source == "full_url"
                        and has_more_candidates
                        and self._is_retryable_media_download_error(e)
                    )
                    if should_fallback:
                        self.logger.warning(
                            "media download failed via full_url, falling back to encrypt_query_param: type={} err={}",
                            media_type,
                            e,
                        )
                        continue
                    raise

            if aes_key_b64 and data:
                data = _decrypt_aes_ecb(data, aes_key_b64)

            if not data:
                return None

            media_dir = get_media_dir("weixin")
            ext = _ext_for_type(media_type)
            if not filename:
                ts = int(time.time())
                hash_seed = encrypt_query_param or full_url
                h = abs(hash(hash_seed)) % 100000
                filename = f"{media_type}_{ts}_{h}{ext}"
            safe_name = os.path.basename(filename)
            file_path = media_dir / safe_name
            file_path.write_bytes(data)
            return str(file_path)

        except Exception:
            self.logger.exception("Error downloading media")
            return None

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕微信、消息处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _get_typing_ticket(self, user_id: str, context_token: str = "") -> str:
        """异步执行辅助逻辑（_get_typing_ticket = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._get_typing_ticket` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        context_token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        now = time.time()
        entry = self._typing_tickets.get(user_id)
        if entry and now < float(entry.get("next_fetch_at", 0)):
            return str(entry.get("ticket", "") or "")

        body: dict[str, Any] = {
            "ilink_user_id": user_id,
            "context_token": context_token or None,
            "base_info": BASE_INFO,
        }
        data = await self._api_post("ilink/bot/getconfig", body)
        if data.get("ret", 0) == 0:
            ticket = str(data.get("typing_ticket", "") or "")
            self._typing_tickets[user_id] = {
                "ticket": ticket,
                "ever_succeeded": True,
                "next_fetch_at": now + (random.random() * TYPING_TICKET_TTL_S),
                "retry_delay_s": CONFIG_CACHE_INITIAL_RETRY_S,
            }
            return ticket

        prev_delay = float(entry.get("retry_delay_s", CONFIG_CACHE_INITIAL_RETRY_S)) if entry else CONFIG_CACHE_INITIAL_RETRY_S
        next_delay = min(prev_delay * 2, CONFIG_CACHE_MAX_RETRY_S)
        if entry:
            entry["next_fetch_at"] = now + next_delay
            entry["retry_delay_s"] = next_delay
            return str(entry.get("ticket", "") or "")

        self._typing_tickets[user_id] = {
            "ticket": "",
            "ever_succeeded": False,
            "next_fetch_at": now + CONFIG_CACHE_INITIAL_RETRY_S,
            "retry_delay_s": CONFIG_CACHE_INITIAL_RETRY_S,
        }
        return ""

    async def _refresh_context_token_if_stale(
        self, chat_id: str, context_token: str
    ) -> str:
        """异步执行辅助逻辑（_refresh_context_token_if_stale = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._refresh_context_token_if_stale` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        context_token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not context_token:
            return context_token

        now = time.time()
        cached_at = self._context_token_at.get(chat_id, 0)
        age = now - cached_at

        if age < CONTEXT_TOKEN_MAX_AGE_S:
            return context_token

        self.logger.debug(
            "WeChat context_token for {} is {:.0f}s old; refreshing via getconfig",
            chat_id,
            age,
        )

        body: dict[str, Any] = {
            "ilink_user_id": chat_id,
            "context_token": context_token,
            "base_info": BASE_INFO,
        }
        try:
            data = await self._api_post("ilink/bot/getconfig", body)
        except Exception as e:
            self.logger.warning("WeChat getconfig failed for {}: {}", chat_id, e)
            return context_token

        if data.get("ret", 0) != 0:
            self.logger.warning(
                "WeChat getconfig returned ret={} for {}: {}",
                data.get("ret"),
                chat_id,
                data.get("errmsg", ""),
            )
            return context_token

        new_token = str(data.get("context_token", "") or "")
        if new_token and new_token != context_token:
            self.logger.info(
                "WeChat context_token refreshed for {} (age {:.0f}s -> fresh)",
                chat_id,
                age,
            )
            self._context_tokens[chat_id] = new_token
            self._context_token_at[chat_id] = now
            self._save_state()
            return new_token

        return context_token

    async def _flush_tool_hints(self, chat_id: str) -> None:
        """异步执行辅助逻辑（_flush_tool_hints = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._flush_tool_hints` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        hints = self._pending_tool_hints.pop(chat_id, None)
        if not hints:
            return

        self.logger.info(
            "Flushing {} buffered tool hint(s) for {}",
            len(hints),
            chat_id,
        )

        ctx_token = self._context_tokens.get(chat_id, "")
        ctx_token = await self._refresh_context_token_if_stale(chat_id, ctx_token)
        if not ctx_token:
            self.logger.warning(
                "Dropped {} buffered tool hint(s) for {}: no context_token",
                len(hints),
                chat_id,
            )
            return

        try:
            await self._send_text(chat_id, "\n\n".join(hints), ctx_token)
        except Exception:
            self.logger.exception(
                "Failed to flush buffered tool hints for {}", chat_id
            )

    async def _send_typing(self, user_id: str, typing_ticket: str, status: int) -> None:
        """异步发送消息（_send_typing = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._send_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        typing_ticket: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        status: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not typing_ticket:
            return
        body: dict[str, Any] = {
            "ilink_user_id": user_id,
            "typing_ticket": typing_ticket,
            "status": status,
            "base_info": BASE_INFO,
        }
        await self._api_post("ilink/bot/sendtyping", body)

    async def _typing_keepalive_loop(self, user_id: str, typing_ticket: str, stop_event: asyncio.Event) -> None:
        """异步执行辅助逻辑（_typing_keepalive_loop = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._typing_keepalive_loop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        typing_ticket: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        stop_event: 外部平台事件对象，包含用户输入和平台元数据。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            while not stop_event.is_set():
                await asyncio.sleep(TYPING_KEEPALIVE_INTERVAL_S)
                if stop_event.is_set():
                    break
                with suppress(Exception):
                    await self._send_typing(user_id, typing_ticket, TYPING_STATUS_TYPING)
        finally:
            pass

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client or not self._token:
            raise RuntimeError("WeChat client not initialized or not authenticated")
        self._assert_session_active()

        is_progress = bool((msg.metadata or {}).get("_progress", False))

        # 中文说明：这一段围绕工具处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if is_progress and (msg.metadata or {}).get("_tool_hint"):
            if not self.send_tool_hints:
                return
            self._pending_tool_hints.setdefault(msg.chat_id, []).append(msg.content)
            self.logger.debug(
                "Buffered tool hint for {} (count={})",
                msg.chat_id,
                len(self._pending_tool_hints[msg.chat_id]),
            )
            return

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if is_progress and (msg.metadata or {}).get("_reasoning_delta"):
            self.logger.debug(
                "Dropped invisible reasoning delta for {}", msg.chat_id
            )
            return

        content = msg.content.strip()

        # 中文说明：这一段围绕消息、工具、事件处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if is_progress and not content and not (msg.media or []):
            self.logger.debug(
                "Skipped empty progress message for {} (no visible content)",
                msg.chat_id,
            )
            return

        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        await self._flush_tool_hints(msg.chat_id)

        if not is_progress:
            await self._stop_typing(msg.chat_id, clear_remote=True)

        ctx_token = self._context_tokens.get(msg.chat_id, "")
        ctx_token = await self._refresh_context_token_if_stale(msg.chat_id, ctx_token)
        if not ctx_token:
            raise RuntimeError(
                f"WeChat context_token missing for chat_id={msg.chat_id}, cannot send"
            )

        typing_ticket = ""
        with suppress(Exception):
            typing_ticket = await self._get_typing_ticket(msg.chat_id, ctx_token)

        if typing_ticket:
            with suppress(Exception):
                await self._send_typing(msg.chat_id, typing_ticket, TYPING_STATUS_TYPING)

        typing_keepalive_stop = asyncio.Event()
        typing_keepalive_task: asyncio.Task | None = None
        if typing_ticket:
            typing_keepalive_task = asyncio.create_task(
                self._typing_keepalive_loop(msg.chat_id, typing_ticket, typing_keepalive_stop)
            )

        try:
            # 中文说明：这一段围绕Telegram、媒体、文件处理，注意输入、输出和异常路径。
            for media_path in (msg.media or []):
                try:
                    await self._send_media_file(msg.chat_id, media_path, ctx_token)
                except (httpx.TimeoutException, httpx.TransportError):
                    # 中文说明：这一段围绕错误处理，注意输入、输出和异常路径。
                    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                    self.logger.opt(exception=True).warning(
                        "Network error sending media {}",
                        media_path,
                    )
                    raise
                except httpx.HTTPStatusError as http_err:
                    status_code = (
                        http_err.response.status_code
                        if http_err.response is not None
                        else 0
                    )
                    if status_code >= 500:
                        # 中文说明：这一段围绕HTTP、重试、错误处理，注意输入、输出和异常路径。
                        self.logger.exception(
                            "Server error ({} {}) sending media {}",
                            status_code,
                            http_err.response.reason_phrase
                            if http_err.response is not None
                            else "",
                            media_path,
                        )
                        raise
                    # 中文说明：这一段围绕重试、错误处理，注意输入、输出和异常路径。
                    filename = Path(media_path).name
                    self.logger.exception("Failed to send media {}", media_path)
                    await self._send_text(
                        msg.chat_id, f"[Failed to send: {filename}]", ctx_token,
                    )
                except Exception:
                    # 中文说明：这一段围绕错误、文件、格式处理，注意输入、输出和异常路径。
                    # 中文说明：兜底。
                    filename = Path(media_path).name
                    self.logger.exception("Failed to send media {}", media_path)
                    # 中文说明：这一段围绕用户处理，注意输入、输出和异常路径。
                    await self._send_text(
                        msg.chat_id, f"[Failed to send: {filename}]", ctx_token,
                    )

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if not content:
                return

            chunks = split_message(content, WEIXIN_MAX_MESSAGE_LEN)
            for chunk in chunks:
                await self._send_text(msg.chat_id, chunk, ctx_token)
        except Exception:
            self.logger.exception("Error sending message")
            raise
        finally:
            if typing_keepalive_task:
                typing_keepalive_stop.set()
                typing_keepalive_task.cancel()
                with suppress(asyncio.CancelledError):
                    await typing_keepalive_task

            if typing_ticket and not is_progress:
                with suppress(Exception):
                    await self._send_typing(msg.chat_id, typing_ticket, TYPING_STATUS_CANCEL)

    async def send_delta(
        self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """异步发送消息（send_delta = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel.send_delta` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if metadata and metadata.get("_stream_end"):
            await self._flush_tool_hints(chat_id)

    async def _start_typing(self, chat_id: str, context_token: str = "") -> None:
        """异步启动流程（_start_typing = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._start_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        context_token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client or not self._token or not chat_id:
            return
        await self._stop_typing(chat_id, clear_remote=False)
        try:
            ticket = await self._get_typing_ticket(chat_id, context_token)
            if not ticket:
                return
            await self._send_typing(chat_id, ticket, TYPING_STATUS_TYPING)
        except Exception as e:
            self.logger.debug("typing indicator start failed for {}: {}", chat_id, e)
            return

        stop_event = asyncio.Event()

        async def keepalive() -> None:
            """异步执行辅助逻辑（keepalive = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `WeixinChannel.keepalive` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            无显式参数。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            try:
                while not stop_event.is_set():
                    await asyncio.sleep(TYPING_KEEPALIVE_INTERVAL_S)
                    if stop_event.is_set():
                        break
                    with suppress(Exception):
                        await self._send_typing(chat_id, ticket, TYPING_STATUS_TYPING)
            finally:
                pass

        task = asyncio.create_task(keepalive())
        task._typing_stop_event = stop_event  # type: ignore[attr-defined]
        self._typing_tasks[chat_id] = task

    async def _stop_typing(self, chat_id: str, *, clear_remote: bool) -> None:
        """异步停止流程（_stop_typing = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._stop_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        clear_remote: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        task = self._typing_tasks.pop(chat_id, None)
        if task and not task.done():
            stop_event = getattr(task, "_typing_stop_event", None)
            if stop_event:
                stop_event.set()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if not clear_remote:
            return
        entry = self._typing_tickets.get(chat_id)
        ticket = str(entry.get("ticket", "") or "") if isinstance(entry, dict) else ""
        if not ticket:
            return
        try:
            await self._send_typing(chat_id, ticket, TYPING_STATUS_CANCEL)
        except Exception as e:
            self.logger.debug("typing clear failed for {}: {}", chat_id, e)

    async def _send_text(
        self,
        to_user_id: str,
        text: str,
        context_token: str,
    ) -> None:
        """异步发送消息（_send_text = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._send_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        to_user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        context_token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        client_id = f"nanobot-{uuid.uuid4().hex[:12]}"

        item_list: list[dict] = []
        if text:
            item_list.append({"type": ITEM_TEXT, "text_item": {"text": text}})

        weixin_msg: dict[str, Any] = {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": MESSAGE_TYPE_BOT,
            "message_state": MESSAGE_STATE_FINISH,
        }
        if item_list:
            weixin_msg["item_list"] = item_list
        if context_token:
            weixin_msg["context_token"] = context_token

        body: dict[str, Any] = {
            "msg": weixin_msg,
            "base_info": BASE_INFO,
        }

        data = await self._api_post("ilink/bot/sendmessage", body)
        ret = data.get("ret", 0)
        errcode = data.get("errcode", 0)
        if (ret is not None and ret != 0) or (errcode is not None and errcode != 0):
            raise RuntimeError(
                f"WeChat send text error (ret={ret}, errcode={errcode}): {data.get('errmsg', '')}"
            )

    async def _send_media_file(
        self,
        to_user_id: str,
        media_path: str,
        context_token: str,
    ) -> None:
        """异步发送消息（_send_media_file = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WeixinChannel._send_media_file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        to_user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media_path: 文件或路径信息，代码会按安全边界读取或写入。
        context_token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        p = Path(media_path)
        if not p.is_file():
            raise FileNotFoundError(f"Media file not found: {media_path}")

        raw_data = p.read_bytes()
        raw_size = len(raw_data)
        raw_md5 = hashlib.md5(raw_data).hexdigest()

        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        ext = p.suffix.lower()
        if ext in _IMAGE_EXTS:
            upload_type = UPLOAD_MEDIA_IMAGE
            item_type = ITEM_IMAGE
            item_key = "image_item"
        elif ext in _VIDEO_EXTS:
            upload_type = UPLOAD_MEDIA_VIDEO
            item_type = ITEM_VIDEO
            item_key = "video_item"
        elif ext in _VOICE_EXTS:
            upload_type = UPLOAD_MEDIA_VOICE
            item_type = ITEM_VOICE
            item_key = "voice_item"
        else:
            upload_type = UPLOAD_MEDIA_FILE
            item_type = ITEM_FILE
            item_key = "file_item"

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        aes_key_raw = os.urandom(16)
        aes_key_hex = aes_key_raw.hex()

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        padded_size = ((raw_size + 1 + 15) // 16) * 16

        # 中文说明：兜底。
        file_key = os.urandom(16).hex()
        upload_body: dict[str, Any] = {
            "filekey": file_key,
            "media_type": upload_type,
            "to_user_id": to_user_id,
            "rawsize": raw_size,
            "rawfilemd5": raw_md5,
            "filesize": padded_size,
            "no_need_thumb": True,
            "aeskey": aes_key_hex,
        }

        assert self._client is not None
        upload_resp = await self._api_post("ilink/bot/getuploadurl", upload_body)

        upload_full_url = str(upload_resp.get("upload_full_url", "") or "").strip()
        upload_param = str(upload_resp.get("upload_param", "") or "")
        if not upload_full_url and not upload_param:
            raise RuntimeError(
                "getuploadurl returned no upload URL "
                f"(need upload_full_url or upload_param): {upload_resp}"
            )

        # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
        aes_key_b64 = base64.b64encode(aes_key_raw).decode()
        encrypted_data = _encrypt_aes_ecb(raw_data, aes_key_b64)

        if upload_full_url:
            cdn_upload_url = upload_full_url
        else:
            cdn_upload_url = (
                f"{self.config.cdn_base_url}/upload"
                f"?encrypted_query_param={quote(upload_param)}"
                f"&filekey={quote(file_key)}"
            )

        cdn_resp = await self._client.post(
            cdn_upload_url,
            content=encrypted_data,
            headers={"Content-Type": "application/octet-stream"},
        )
        cdn_resp.raise_for_status()

        # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
        download_param = cdn_resp.headers.get("x-encrypted-param", "")
        if not download_param:
            raise RuntimeError(
                "CDN upload response missing x-encrypted-param header; "
                f"status={cdn_resp.status_code} headers={dict(cdn_resp.headers)}"
            )

        # 中文说明：这一段围绕消息、媒体处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        cdn_aes_key_b64 = base64.b64encode(aes_key_hex.encode()).decode()

        media_item: dict[str, Any] = {
            "media": {
                "encrypt_query_param": download_param,
                "aes_key": cdn_aes_key_b64,
                "encrypt_type": 1,
            },
        }

        if item_type == ITEM_IMAGE:
            media_item["mid_size"] = padded_size
        elif item_type == ITEM_VIDEO:
            media_item["video_size"] = padded_size
        elif item_type == ITEM_FILE:
            media_item["file_name"] = p.name
            media_item["len"] = str(raw_size)

        # 中文说明：这一段围绕消息、媒体处理，注意输入、输出和异常路径。
        client_id = f"nanobot-{uuid.uuid4().hex[:12]}"
        item_list: list[dict] = [{"type": item_type, item_key: media_item}]

        weixin_msg: dict[str, Any] = {
            "from_user_id": "",
            "to_user_id": to_user_id,
            "client_id": client_id,
            "message_type": MESSAGE_TYPE_BOT,
            "message_state": MESSAGE_STATE_FINISH,
            "item_list": item_list,
        }
        if context_token:
            weixin_msg["context_token"] = context_token

        body: dict[str, Any] = {
            "msg": weixin_msg,
            "base_info": BASE_INFO,
        }

        data = await self._api_post("ilink/bot/sendmessage", body)
        ret = data.get("ret", 0)
        errcode = data.get("errcode", 0)
        if (ret is not None and ret != 0) or (errcode is not None and errcode != 0):
            raise RuntimeError(
                f"WeChat send media error (ret={ret}, errcode={errcode}): {data.get('errmsg', '')}"
            )


# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----


def _parse_aes_key(aes_key_b64: str) -> bytes:
    """解析数据（_parse_aes_key = 原函数名）。

    【中文名称】解析数据

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_parse_aes_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    aes_key_b64: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    decoded = base64.b64decode(aes_key_b64)
    if len(decoded) == 16:
        return decoded
    if len(decoded) == 32 and re.fullmatch(rb"[0-9a-fA-F]{32}", decoded):
        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        return bytes.fromhex(decoded.decode("ascii"))
    raise ValueError(
        f"aes_key must decode to 16 raw bytes or 32-char hex string, got {len(decoded)} bytes"
    )


def _encrypt_aes_ecb(data: bytes, aes_key_b64: str) -> bytes:
    """执行辅助逻辑（_encrypt_aes_ecb = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_encrypt_aes_ecb` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data: 结构化数据负载，后续会被解析或转发。
    aes_key_b64: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        key = _parse_aes_key(aes_key_b64)
    except Exception as e:
        logger.warning("Failed to parse AES key for encryption, sending raw: {}", e)
        return data

    # 中文说明：PKCS7 padding 相关逻辑。
    pad_len = 16 - len(data) % 16
    padded = data + bytes([pad_len] * pad_len)

    with suppress(ImportError):
        from Crypto.Cipher import AES

        cipher = AES.new(key, AES.MODE_ECB)
        return cipher.encrypt(padded)

    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        cipher_obj = Cipher(algorithms.AES(key), modes.ECB())
        encryptor = cipher_obj.encryptor()
        return encryptor.update(padded) + encryptor.finalize()
    except ImportError:
        logger.warning("Cannot encrypt media: install 'pycryptodome' or 'cryptography'")
        return data


def _decrypt_aes_ecb(data: bytes, aes_key_b64: str) -> bytes:
    """执行辅助逻辑（_decrypt_aes_ecb = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_decrypt_aes_ecb` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data: 结构化数据负载，后续会被解析或转发。
    aes_key_b64: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        key = _parse_aes_key(aes_key_b64)
    except Exception as e:
        logger.warning("Failed to parse AES key, returning raw data: {}", e)
        return data

    decrypted: bytes | None = None

    with suppress(ImportError):
        from Crypto.Cipher import AES

        cipher = AES.new(key, AES.MODE_ECB)
        decrypted = cipher.decrypt(data)

    if decrypted is None:
        try:
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

            cipher_obj = Cipher(algorithms.AES(key), modes.ECB())
            decryptor = cipher_obj.decryptor()
            decrypted = decryptor.update(data) + decryptor.finalize()
        except ImportError:
            logger.warning("Cannot decrypt media: install 'pycryptodome' or 'cryptography'")
            return data

    return _pkcs7_unpad_safe(decrypted)


def _pkcs7_unpad_safe(data: bytes, block_size: int = 16) -> bytes:
    """执行辅助逻辑（_pkcs7_unpad_safe = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_pkcs7_unpad_safe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data: 结构化数据负载，后续会被解析或转发。
    block_size: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not data:
        return data
    if len(data) % block_size != 0:
        return data
    pad_len = data[-1]
    if pad_len < 1 or pad_len > block_size:
        return data
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        return data
    return data[:-pad_len]


def _ext_for_type(media_type: str) -> str:
    """执行辅助逻辑（_ext_for_type = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_ext_for_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    media_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {
        "image": ".jpg",
        "voice": ".silk",
        "video": ".mp4",
        "file": "",
    }.get(media_type, "")

