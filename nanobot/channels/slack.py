"""Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/slack.py

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

import asyncio
import re
from pathlib import Path
from typing import Any

import httpx
from pydantic import Field
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.socket_mode.websockets import SocketModeClient
from slack_sdk.web.async_client import AsyncWebClient
from slackify_markdown import slackify_markdown

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.pairing import is_approved
from nanobot.utils.helpers import safe_filename, split_message


class SlackDMConfig(Base):
    """SlackDMConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SlackDMConfig

    【功能说明】
    Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = True
    policy: str = "open"
    allow_from: list[str] = Field(default_factory=list)


class SlackConfig(Base):
    """SlackConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SlackConfig

    【功能说明】
    Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    mode: str = "socket"
    webhook_path: str = "/slack/events"
    bot_token: str = ""
    app_token: str = ""
    user_token_read_only: bool = True
    reply_in_thread: bool = True
    react_emoji: str = "eyes"
    done_emoji: str = "white_check_mark"
    include_thread_context: bool = True
    thread_context_limit: int = 20
    allow_from: list[str] = Field(default_factory=list)
    group_policy: str = "mention"
    group_allow_from: list[str] = Field(default_factory=list)
    dm: SlackDMConfig = Field(default_factory=SlackDMConfig)


SLACK_MAX_MESSAGE_LEN = 39_000  # 中文说明：这一段围绕Slack、API处理，注意输入、输出和异常路径。
SLACK_DOWNLOAD_TIMEOUT = 30.0
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这一段围绕Slack、HTTP处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕Slack、WebSocket处理，注意输入、输出和异常路径。
SLACK_SOCKET_CONNECT_TIMEOUT_S = 45.0
_HTML_DOWNLOAD_PREFIXES = (b"<!doctype html", b"<html")


class SlackChannel(BaseChannel):
    """SlackChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SlackChannel

    【功能说明】
    Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "slack"
    display_name = "Slack"
    _SLACK_ID_RE = re.compile(r"^[CDGUW][A-Z0-9]{2,}$")
    _SLACK_CHANNEL_REF_RE = re.compile(r"^<#([A-Z0-9]+)(?:\|[^>]+)?>$")
    _SLACK_USER_REF_RE = re.compile(r"^<@([A-Z0-9]+)(?:\|[^>]+)?>$")

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return SlackConfig().model_dump(by_alias=True)

    _THREAD_CONTEXT_CACHE_LIMIT = 10_000

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = SlackConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: SlackConfig = config
        self._web_client: AsyncWebClient | None = None
        self._socket_client: SocketModeClient | None = None
        self._bot_user_id: str | None = None
        self._target_cache: dict[str, str] = {}
        self._thread_context_attempted: set[str] = set()

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.config.bot_token or not self.config.app_token:
            self.logger.error("bot/app token not configured")
            return
        if self.config.mode != "socket":
            self.logger.error("Unsupported mode: {}", self.config.mode)
            return

        self._running = True

        self._web_client = AsyncWebClient(token=self.config.bot_token)
        self._socket_client = SocketModeClient(
            app_token=self.config.app_token,
            web_client=self._web_client,
        )

        self._socket_client.socket_mode_request_listeners.append(self._on_socket_request)

        # 中文说明：这一段围绕用户处理，注意输入、输出和异常路径。
        try:
            auth = await self._web_client.auth_test()
            self._bot_user_id = auth.get("user_id")
            self.logger.info("bot connected as {}", self._bot_user_id)
        except Exception as e:
            self.logger.warning("auth_test failed: {}", e)

        self.logger.info("Starting Socket Mode client...")
        try:
            await asyncio.wait_for(
                self._socket_client.connect(),
                timeout=SLACK_SOCKET_CONNECT_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            self.logger.error(
                "Slack Socket Mode WebSocket handshake timed out after {:.0f}s. "
                "auth_test uses HTTPS and may still succeed while WSS is blocked. "
                "Check outbound access to Slack WebSockets; slack-sdk Socket Mode "
                "does not apply HTTP(S)_PROXY to websockets.connect.",
                SLACK_SOCKET_CONNECT_TIMEOUT_S,
            )
            await self.stop()
            raise RuntimeError("Slack Socket Mode WebSocket connect timed out") from None

        self.logger.info("Slack Socket Mode WebSocket connected (events enabled)")

        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        if self._socket_client:
            try:
                await self._socket_client.close()
            except Exception as e:
                self.logger.warning("socket close failed: {}", e)
            self._socket_client = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._web_client:
            self.logger.warning("client not running")
            return
        try:
            target_chat_id = await self._resolve_target_chat_id(msg.chat_id)
            slack_meta = msg.metadata.get("slack", {}) if msg.metadata else {}
            thread_ts = slack_meta.get("thread_ts")
            origin_chat_id = str((slack_meta.get("event", {}) or {}).get("channel") or msg.chat_id)
            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            thread_ts_param = thread_ts if thread_ts and target_chat_id == origin_chat_id else None

            is_progress = (msg.metadata or {}).get("_progress", False)
            if is_progress and not msg.content:
                pass  # 中文说明：这一段围绕消息、工具、事件处理，注意输入、输出和异常路径。
            elif msg.content or not (msg.media or []):
                mrkdwn = self._to_mrkdwn(msg.content) if msg.content else " "
                buttons = getattr(msg, "buttons", None) or []
                chunks = split_message(mrkdwn, SLACK_MAX_MESSAGE_LEN)
                for index, chunk in enumerate(chunks):
                    kwargs: dict[str, Any] = dict(
                        channel=target_chat_id, text=chunk, thread_ts=thread_ts_param,
                    )
                    if buttons and index == len(chunks) - 1:
                        kwargs["blocks"] = self._build_button_blocks(chunk, buttons)
                    await self._web_client.chat_postMessage(**kwargs)

            for media_path in msg.media or []:
                try:
                    await self._web_client.files_upload_v2(
                        channel=target_chat_id,
                        file=media_path,
                        thread_ts=thread_ts_param,
                    )
                except Exception:
                    self.logger.exception("Failed to upload file {}", media_path)

            # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
            if not (msg.metadata or {}).get("_progress"):
                event = slack_meta.get("event", {})
                await self._update_react_emoji(origin_chat_id, event.get("ts"))

        except Exception:
            self.logger.exception("Error sending message")
            raise

    async def _resolve_target_chat_id(self, target: str) -> str:
        """异步解析目标（_resolve_target_chat_id = 原函数名）。

        【中文名称】解析目标

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._resolve_target_chat_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        target: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._web_client:
            return target

        target = target.strip()
        if not target:
            return target

        if match := self._SLACK_CHANNEL_REF_RE.fullmatch(target):
            return match.group(1)
        if match := self._SLACK_USER_REF_RE.fullmatch(target):
            return await self._open_dm_for_user(match.group(1))
        if self._SLACK_ID_RE.fullmatch(target):
            if target.startswith(("U", "W")):
                return await self._open_dm_for_user(target)
            return target

        if target.startswith("#"):
            return await self._resolve_channel_name(target[1:])
        if target.startswith("@"):
            return await self._resolve_user_handle(target[1:])

        try:
            return await self._resolve_channel_name(target)
        except ValueError:
            return await self._resolve_user_handle(target)

    async def _resolve_channel_name(self, name: str) -> str:
        """异步解析目标（_resolve_channel_name = 原函数名）。

        【中文名称】解析目标

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._resolve_channel_name` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        normalized = self._normalize_target_name(name)
        if not normalized:
            raise ValueError("Slack target channel name is empty")

        cache_key = f"channel:{normalized}"
        if cache_key in self._target_cache:
            return self._target_cache[cache_key]

        cursor: str | None = None
        while True:
            response = await self._web_client.conversations_list(
                types="public_channel,private_channel",
                exclude_archived=True,
                limit=200,
                cursor=cursor,
            )
            for channel in response.get("channels", []):
                if self._normalize_target_name(str(channel.get("name") or "")) == normalized:
                    channel_id = str(channel.get("id") or "")
                    if channel_id:
                        self._target_cache[cache_key] = channel_id
                        return channel_id
            cursor = ((response.get("response_metadata") or {}).get("next_cursor") or "").strip()
            if not cursor:
                break

        raise ValueError(
            f"Slack channel '{name}' was not found. Use a joined channel name like "
            f"'#general' or a concrete channel ID."
        )

    async def _resolve_user_handle(self, handle: str) -> str:
        """异步解析目标（_resolve_user_handle = 原函数名）。

        【中文名称】解析目标

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._resolve_user_handle` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        handle: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        normalized = self._normalize_target_name(handle)
        if not normalized:
            raise ValueError("Slack target user handle is empty")

        cache_key = f"user:{normalized}"
        if cache_key in self._target_cache:
            return self._target_cache[cache_key]

        cursor: str | None = None
        while True:
            response = await self._web_client.users_list(limit=200, cursor=cursor)
            for member in response.get("members", []):
                if self._member_matches_handle(member, normalized):
                    user_id = str(member.get("id") or "")
                    if not user_id:
                        continue
                    dm_id = await self._open_dm_for_user(user_id)
                    self._target_cache[cache_key] = dm_id
                    return dm_id
            cursor = ((response.get("response_metadata") or {}).get("next_cursor") or "").strip()
            if not cursor:
                break

        raise ValueError(
            f"Slack user '{handle}' was not found. Use '@name' or a concrete DM/channel ID."
        )

    async def _open_dm_for_user(self, user_id: str) -> str:
        """异步执行辅助逻辑（_open_dm_for_user = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._open_dm_for_user` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        response = await self._web_client.conversations_open(users=user_id)
        channel_id = str(((response.get("channel") or {}).get("id")) or "")
        if not channel_id:
            raise ValueError(f"Slack DM target for user '{user_id}' could not be opened.")
        return channel_id

    @staticmethod
    def _normalize_target_name(value: str) -> str:
        """标准化数据（_normalize_target_name = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._normalize_target_name` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return value.strip().lstrip("#@").lower()

    @classmethod
    def _member_matches_handle(cls, member: dict[str, Any], normalized: str) -> bool:
        """处理事件（_member_matches_handle = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._member_matches_handle` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        member: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        normalized: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        profile = member.get("profile") or {}
        candidates = {
            str(member.get("name") or ""),
            str(profile.get("display_name") or ""),
            str(profile.get("display_name_normalized") or ""),
            str(profile.get("real_name") or ""),
            str(profile.get("real_name_normalized") or ""),
        }
        return normalized in {cls._normalize_target_name(candidate) for candidate in candidates if candidate}

    async def _on_socket_request(
        self,
        client: SocketModeClient,
        req: SocketModeRequest,
    ) -> None:
        """异步执行辅助逻辑（_on_socket_request = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._on_socket_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        client: 第三方 SDK 或 HTTP 客户端实例。
        req: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if req.type == "interactive":
            await self._on_block_action(client, req)
            return
        if req.type != "events_api":
            return

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        await client.send_socket_mode_response(
            SocketModeResponse(envelope_id=req.envelope_id)
        )

        payload = req.payload or {}
        event = payload.get("event") or {}
        event_type = event.get("type")

        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if event_type not in ("message", "app_mention"):
            return

        sender_id = event.get("user")
        chat_id = event.get("channel")

        subtype = event.get("subtype")
        # 中文说明：这一段围绕Slack、消息、用户、文件处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if subtype and subtype != "file_share":
            return
        if self._bot_user_id and sender_id == self._bot_user_id:
            return

        # 中文说明：这一段围绕Slack、消息处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        text = event.get("text") or ""
        if event_type == "message" and self._bot_user_id and f"<@{self._bot_user_id}>" in text:
            return

        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
        self.logger.debug(
            "event: type={} subtype={} user={} channel={} channel_type={} text={}",
            event_type,
            subtype,
            sender_id,
            chat_id,
            event.get("channel_type"),
            text[:80],
        )
        if not sender_id or not chat_id:
            return

        channel_type = event.get("channel_type") or ""

        if not self._is_allowed(sender_id, chat_id, channel_type):
            if channel_type == "im" and self.config.dm.enabled:
                await self._handle_message(
                    sender_id=sender_id,
                    chat_id=chat_id,
                    content="",
                    is_dm=True,
                )
            return

        if channel_type != "im" and not self._should_respond_in_channel(event_type, text, chat_id):
            return

        text = self._strip_bot_mention(text)

        event_ts = event.get("ts")
        raw_thread_ts = event.get("thread_ts")
        thread_ts = raw_thread_ts
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕用户处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if (
            self.config.reply_in_thread
            and not thread_ts
            and channel_type != "im"
        ):
            thread_ts = event_ts
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        try:
            if self._web_client and event.get("ts"):
                await self._web_client.reactions_add(
                    channel=chat_id,
                    name=self.config.react_emoji,
                    timestamp=event.get("ts"),
                )
        except Exception as e:
            self.logger.debug("reactions_add failed: {}", e)

        # 中文说明：这一段围绕会话、用户处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕会话处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕上下文处理，注意输入、输出和异常路径。
        session_key = (
            f"slack:{chat_id}:{thread_ts}" if thread_ts and raw_thread_ts else None
        )
        media_paths: list[str] = []
        file_markers: list[str] = []
        for file_info in event.get("files") or []:
            if not isinstance(file_info, dict):
                continue
            file_path, marker = await self._download_slack_file(file_info)
            if file_path:
                media_paths.append(file_path)
            if marker:
                file_markers.append(marker)

        is_slash = text.strip().startswith("/")
        content = text if is_slash else await self._with_thread_context(
            text,
            chat_id=chat_id,
            channel_type=channel_type,
            thread_ts=thread_ts,
            raw_thread_ts=raw_thread_ts,
            current_ts=event_ts,
        )
        if file_markers:
            content = "\n".join(part for part in [content, *file_markers] if part)
        if not content and not media_paths:
            return

        try:
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                media=media_paths,
                metadata={
                    "slack": {
                        "event": event,
                        "thread_ts": thread_ts,
                        "channel_type": channel_type,
                    },
                },
                session_key=session_key,
            )
        except Exception:
            self.logger.exception("Error handling message from {}", sender_id)

    async def _download_slack_file(self, file_info: dict[str, Any]) -> tuple[str | None, str]:
        """异步下载资源（_download_slack_file = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._download_slack_file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_info: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        file_id = str(file_info.get("id") or "file")
        name = str(
            file_info.get("name")
            or file_info.get("title")
            or file_info.get("id")
            or "slack-file"
        )
        marker_type = "image" if str(file_info.get("mimetype") or "").startswith("image/") else "file"
        marker = f"[{marker_type}: {name}]"
        url = str(file_info.get("url_private_download") or file_info.get("url_private") or "")
        if not url:
            return None, self._download_failure_marker(marker_type, name, "missing download url")
        if not self.config.bot_token:
            return None, self._download_failure_marker(marker_type, name, "missing bot token")

        filename = safe_filename(f"{file_id}_{name}")
        path = Path(get_media_dir("slack")) / filename
        try:
            async with httpx.AsyncClient(timeout=SLACK_DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
                response = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {self.config.bot_token}"},
                )
                response.raise_for_status()
            if self._looks_like_html_download(response):
                raise ValueError("Slack returned HTML instead of file content")
            path.write_bytes(response.content)
            return str(path), marker
        except Exception as e:
            self.logger.warning("Failed to download file {}: {}", file_id, e)
            return None, self._download_failure_marker(marker_type, name, "download failed")

    @staticmethod
    def _download_failure_marker(marker_type: str, name: str, reason: str) -> str:
        """下载资源（_download_failure_marker = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._download_failure_marker` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        marker_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reason: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return (
            f"[{marker_type}: {name}: {reason}; not available to nanobot. "
            "Check Slack files:read scope, reinstall the Slack app, and ensure the bot can access the file.]"
        )

    @staticmethod
    def _looks_like_html_download(response: httpx.Response) -> bool:
        """下载资源（_looks_like_html_download = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._looks_like_html_download` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        response: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" in content_type:
            return True
        preview = response.content[:256].lstrip().lower()
        return preview.startswith(_HTML_DOWNLOAD_PREFIXES)

    async def _on_block_action(self, client: SocketModeClient, req: SocketModeRequest) -> None:
        """异步执行辅助逻辑（_on_block_action = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._on_block_action` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        client: 第三方 SDK 或 HTTP 客户端实例。
        req: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
        payload = req.payload or {}
        actions = payload.get("actions") or []
        if not actions:
            return
        value = str(actions[0].get("value") or "")
        user_info = payload.get("user") or {}
        sender_id = str(user_info.get("id") or "")
        channel_info = payload.get("channel") or {}
        chat_id = str(channel_info.get("id") or "")
        if not sender_id or not chat_id or not value:
            return
        message_info = payload.get("message") or {}
        thread_ts = message_info.get("thread_ts") or message_info.get("ts")
        channel_type = self._infer_channel_type(chat_id)
        if not self._is_allowed(sender_id, chat_id, channel_type):
            return
        session_key = f"slack:{chat_id}:{thread_ts}" if thread_ts else None
        try:
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=value,
                metadata={"slack": {"thread_ts": thread_ts, "channel_type": channel_type}},
                session_key=session_key,
            )
        except Exception:
            self.logger.exception("Error handling button click from {}", sender_id)

    async def _with_thread_context(
        self,
        text: str,
        *,
        chat_id: str,
        channel_type: str,
        thread_ts: str | None,
        raw_thread_ts: str | None,
        current_ts: str | None,
    ) -> str:
        """异步执行辅助逻辑（_with_thread_context = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._with_thread_context` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        channel_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        thread_ts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        raw_thread_ts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        current_ts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        del channel_type  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if (
            not self.config.include_thread_context
            or not self._web_client
            or not raw_thread_ts
            or not thread_ts
            or current_ts == thread_ts
        ):
            return text

        key = f"{chat_id}:{thread_ts}"
        if key in self._thread_context_attempted:
            return text
        if len(self._thread_context_attempted) >= self._THREAD_CONTEXT_CACHE_LIMIT:
            self._thread_context_attempted.clear()
        self._thread_context_attempted.add(key)

        try:
            response = await self._web_client.conversations_replies(
                channel=chat_id,
                ts=thread_ts,
                limit=max(1, self.config.thread_context_limit),
            )
        except Exception as e:
            self.logger.warning("thread context unavailable for {}: {}", key, e)
            return text

        lines = self._format_thread_context(
            response.get("messages", []),
            current_ts=current_ts,
        )
        if not lines:
            return text
        return "Slack thread context before this mention:\n" + "\n".join(lines) + f"\n\nCurrent message:\n{text}"

    def _format_thread_context(self, messages: list[dict[str, Any]], *, current_ts: str | None) -> list[str]:
        """格式化内容（_format_thread_context = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._format_thread_context` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        current_ts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        lines: list[str] = []
        for item in messages:
            if item.get("ts") == current_ts:
                continue
            if item.get("subtype"):
                continue
            sender = str(item.get("user") or item.get("bot_id") or "unknown")
            is_bot = self._bot_user_id is not None and sender == self._bot_user_id
            label = "bot" if is_bot else f"<@{sender}>"
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            text = self._strip_bot_mention(text)
            if len(text) > 500:
                text = text[:500] + "…"
            lines.append(f"- {label}: {text}")
        return lines

    @staticmethod
    def _build_button_blocks(text: str, buttons: list[list[str]]) -> list[dict[str, Any]]:
        """构建对象（_build_button_blocks = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._build_button_blocks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        buttons: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        blocks: list[dict[str, Any]] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": text[:3000]}},
        ]
        elements = []
        for row in buttons:
            for label in row:
                elements.append({
                    "type": "button",
                    "text": {"type": "plain_text", "text": label[:75]},
                    "value": label[:75],
                    "action_id": f"btn_{label[:50]}",
                })
        if elements:
            blocks.append({"type": "actions", "elements": elements[:25]})
        return blocks

    async def _update_react_emoji(self, chat_id: str, ts: str | None) -> None:
        """异步执行辅助逻辑（_update_react_emoji = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._update_react_emoji` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        ts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._web_client or not ts:
            return
        try:
            await self._web_client.reactions_remove(
                channel=chat_id,
                name=self.config.react_emoji,
                timestamp=ts,
            )
        except Exception as e:
            self.logger.debug("reactions_remove failed: {}", e)
        if self.config.done_emoji:
            try:
                await self._web_client.reactions_add(
                    channel=chat_id,
                    name=self.config.done_emoji,
                    timestamp=ts,
                )
            except Exception as e:
                self.logger.debug("done reaction failed: {}", e)

    def _is_allowed(self, sender_id: str, chat_id: str, channel_type: str) -> bool:
        """判断条件是否成立（_is_allowed = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._is_allowed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        channel_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if channel_type == "im":
            if not self.config.dm.enabled:
                return False
            if self.config.dm.policy == "allowlist":
                return sender_id in self.config.dm.allow_from or is_approved(self.name, sender_id)
            return True

        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if self.config.group_policy == "allowlist":
            return chat_id in self.config.group_allow_from
        return True

    def _should_respond_in_channel(self, event_type: str, text: str, chat_id: str) -> bool:
        """执行辅助逻辑（_should_respond_in_channel = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._should_respond_in_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        event_type: 外部平台事件对象，包含用户输入和平台元数据。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.config.group_policy == "open":
            return True
        if self.config.group_policy == "mention":
            if event_type == "app_mention":
                return True
            return self._bot_user_id is not None and f"<@{self._bot_user_id}>" in text
        if self.config.group_policy == "allowlist":
            return chat_id in self.config.group_allow_from
        return False

    def is_allowed(self, sender_id: str) -> bool:
        # 中文说明：这一段围绕Slack、请求处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕调用处理，注意输入、输出和异常路径。
        """判断条件是否成立（is_allowed = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel.is_allowed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return True

    @staticmethod
    def _infer_channel_type(chat_id: str) -> str:
        """执行辅助逻辑（_infer_channel_type = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._infer_channel_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if chat_id.startswith("D"):
            return "im"
        if chat_id.startswith("G"):
            return "group"
        return "channel"

    def _strip_bot_mention(self, text: str) -> str:
        """执行辅助逻辑（_strip_bot_mention = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._strip_bot_mention` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not text or not self._bot_user_id:
            return text
        return re.sub(rf"<@{re.escape(self._bot_user_id)}>\s*", "", text).strip()

    _TABLE_RE = re.compile(r"(?m)^\|.*\|$(?:\n\|[\s:|-]*\|$)(?:\n\|.*\|$)*")
    _CODE_FENCE_RE = re.compile(r"```[\s\S]*?```")
    _INLINE_CODE_RE = re.compile(r"`[^`]+`")
    _LEFTOVER_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
    _LEFTOVER_HEADER_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
    _BARE_URL_RE = re.compile(r"(?<![|<])(https?://\S+)")

    @classmethod
    def _to_mrkdwn(cls, text: str) -> str:
        """执行辅助逻辑（_to_mrkdwn = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._to_mrkdwn` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not text:
            return ""
        text = cls._TABLE_RE.sub(cls._convert_table, text)
        return cls._fixup_mrkdwn(slackify_markdown(text)).rstrip("\n")

    @classmethod
    def _fixup_mrkdwn(cls, text: str) -> str:
        """执行辅助逻辑（_fixup_mrkdwn = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._fixup_mrkdwn` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        code_blocks: list[str] = []

        def _save_code(m: re.Match) -> str:
            """保存数据（_save_code = 原函数名）。

            【中文名称】保存数据

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `SlackChannel._save_code` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            m: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            code_blocks.append(m.group(0))
            return f"\x00CB{len(code_blocks) - 1}\x00"

        text = cls._CODE_FENCE_RE.sub(_save_code, text)
        text = cls._INLINE_CODE_RE.sub(_save_code, text)
        text = cls._LEFTOVER_BOLD_RE.sub(r"*\1*", text)
        text = cls._LEFTOVER_HEADER_RE.sub(r"*\1*", text)
        text = cls._BARE_URL_RE.sub(lambda m: m.group(0).replace("&amp;", "&"), text)

        for i, block in enumerate(code_blocks):
            text = text.replace(f"\x00CB{i}\x00", block)
        return text

    @staticmethod
    def _convert_table(match: re.Match) -> str:
        """转换格式（_convert_table = 原函数名）。

        【中文名称】转换格式

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Slack 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SlackChannel._convert_table` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        match: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        lines = [ln.strip() for ln in match.group(0).strip().splitlines() if ln.strip()]
        if len(lines) < 2:
            return match.group(0)
        headers = [h.strip() for h in lines[0].strip("|").split("|")]
        start = 2 if re.fullmatch(r"[|\s:\-]+", lines[1]) else 1
        rows: list[str] = []
        for line in lines[start:]:
            cells = [c.strip() for c in line.strip("|").split("|")]
            cells = (cells + [""] * len(headers))[: len(headers)]
            parts = [f"**{headers[i]}**: {cells[i]}" for i in range(len(headers)) if cells[i]]
            if parts:
                rows.append(" · ".join(parts))
        return "\n".join(rows)

