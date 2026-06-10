"""钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/dingtalk.py

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
import json
import mimetypes
import os
import time
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import httpx
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base
from nanobot.security.network import validate_resolved_url, validate_url_target

DINGTALK_MAX_REMOTE_MEDIA_BYTES = 20 * 1024 * 1024
DINGTALK_MAX_REMOTE_MEDIA_REDIRECTS = 3

try:
    from dingtalk_stream import (
        AckMessage,
        CallbackHandler,
        CallbackMessage,
        Credential,
        DingTalkStreamClient,
    )
    from dingtalk_stream.chatbot import ChatbotMessage

    DINGTALK_AVAILABLE = True
except ImportError:
    DINGTALK_AVAILABLE = False
    # 中文说明：兜底。
    CallbackHandler = object  # type: ignore[assignment,misc]
    CallbackMessage = None  # type: ignore[assignment,misc]
    AckMessage = None  # type: ignore[assignment,misc]
    ChatbotMessage = None  # type: ignore[assignment,misc]


class NanobotDingTalkHandler(CallbackHandler):
    """NanobotDingTalkHandler 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】NanobotDingTalkHandler

    【功能说明】
    钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    CallbackHandler。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(self, channel: "DingTalkChannel"):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NanobotDingTalkHandler.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        super().__init__()
        self.channel = channel

    async def process(self, message: CallbackMessage):
        """异步执行辅助逻辑（process = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NanobotDingTalkHandler.process` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            chatbot_msg = ChatbotMessage.from_dict(message.data)

            # 中文说明：提取。
            content = ""
            if chatbot_msg.text:
                content = chatbot_msg.text.content.strip()
            elif chatbot_msg.extensions.get("content", {}).get("recognition"):
                content = chatbot_msg.extensions["content"]["recognition"].strip()
            if not content:
                content = message.data.get("text", {}).get("content", "").strip()

            # 中文说明：这一段围绕消息、图片、文件处理，注意输入、输出和异常路径。
            file_paths = []
            if chatbot_msg.message_type == "picture" and chatbot_msg.image_content:
                download_code = chatbot_msg.image_content.download_code
                if download_code:
                    sender_uid = chatbot_msg.sender_staff_id or chatbot_msg.sender_id or "unknown"
                    fp = await self.channel._download_dingtalk_file(download_code, "image.jpg", sender_uid)
                    if fp:
                        file_paths.append(fp)
                        content = content or "[Image]"

            elif chatbot_msg.message_type == "file":
                download_code = message.data.get("content", {}).get("downloadCode") or message.data.get("downloadCode")
                fname = message.data.get("content", {}).get("fileName") or message.data.get("fileName") or "file"
                if download_code:
                    sender_uid = chatbot_msg.sender_staff_id or chatbot_msg.sender_id or "unknown"
                    fp = await self.channel._download_dingtalk_file(download_code, fname, sender_uid)
                    if fp:
                        file_paths.append(fp)
                        content = content or "[File]"

            elif chatbot_msg.message_type == "richText" and chatbot_msg.rich_text_content:
                rich_list = chatbot_msg.rich_text_content.rich_text_list or []
                for item in rich_list:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "text":
                        t = item.get("text", "").strip()
                        if t:
                            content = (content + " " + t).strip() if content else t
                    elif item.get("downloadCode"):
                        dc = item["downloadCode"]
                        fname = item.get("fileName") or "file"
                        sender_uid = chatbot_msg.sender_staff_id or chatbot_msg.sender_id or "unknown"
                        fp = await self.channel._download_dingtalk_file(dc, fname, sender_uid)
                        if fp:
                            file_paths.append(fp)
                            content = content or "[File]"

            if file_paths:
                file_list = "\n".join("- " + p for p in file_paths)
                content = content + "\n\nReceived files:\n" + file_list

            if not content:
                self.channel.logger.warning(
                    "Received empty or unsupported message type: {}",
                    chatbot_msg.message_type,
                )
                return AckMessage.STATUS_OK, "OK"

            sender_id = chatbot_msg.sender_staff_id or chatbot_msg.sender_id
            sender_name = chatbot_msg.sender_nick or "Unknown"

            conversation_type = message.data.get("conversationType")
            conversation_id = (
                message.data.get("conversationId")
                or message.data.get("openConversationId")
            )

            self.channel.logger.info("Received message from {} ({}): {}", sender_name, sender_id, content)

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
            task = asyncio.create_task(
                self.channel._on_message(
                    content,
                    sender_id,
                    sender_name,
                    conversation_type,
                    conversation_id,
                )
            )
            self.channel._background_tasks.add(task)
            task.add_done_callback(self.channel._background_tasks.discard)

            return AckMessage.STATUS_OK, "OK"

        except Exception:
            self.channel.logger.exception("Error processing message")
            # 中文说明：这一段围绕钉钉、重试处理，注意输入、输出和异常路径。
            return AckMessage.STATUS_OK, "Error"


class DingTalkConfig(Base):
    """DingTalkConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】DingTalkConfig

    【功能说明】
    钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    client_id: str = ""
    client_secret: str = ""
    allow_from: list[str] = Field(default_factory=list)
    allow_remote_media_redirects: bool = False
    remote_media_redirect_allowed_hosts: list[str] = Field(default_factory=list)
    group_user_isolation: bool = False  # 中文说明：这一段围绕会话、用户处理，注意输入、输出和异常路径。


class DingTalkChannel(BaseChannel):
    """DingTalkChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】DingTalkChannel

    【功能说明】
    钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "dingtalk"
    display_name = "DingTalk"
    _IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
    _AUDIO_EXTS = {".amr", ".mp3", ".wav", ".ogg", ".m4a", ".aac"}
    _VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
    _ZIP_BEFORE_UPLOAD_EXTS = {".htm", ".html"}

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return DingTalkConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = DingTalkConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: DingTalkConfig = config
        self._client: Any = None
        self._http: httpx.AsyncClient | None = None

        # 中文说明：这一段围绕消息、令牌处理，注意输入、输出和异常路径。
        self._access_token: str | None = None
        self._token_expiry: float = 0

        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
        self._background_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            if not DINGTALK_AVAILABLE:
                self.logger.error(
                    "Stream SDK not installed. Run: pip install dingtalk-stream"
                )
                return

            if not self.config.client_id or not self.config.client_secret:
                self.logger.error("client_id and client_secret not configured")
                return

            self._running = True
            self._http = httpx.AsyncClient()

            self.logger.info(
                "Initializing Stream Client with Client ID: {}...",
                self.config.client_id,
            )
            credential = Credential(self.config.client_id, self.config.client_secret)
            self._client = DingTalkStreamClient(credential)

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            handler = NanobotDingTalkHandler(self)
            self._client.register_callback_handler(ChatbotMessage.TOPIC, handler)

            self.logger.info("bot started with Stream Mode")

            # 中文说明：这一段围绕流式输出处理，注意输入、输出和异常路径。
            while self._running:
                try:
                    await self._client.start()
                except Exception as e:
                    self.logger.warning("stream error: {}", e)
                if self._running:
                    self.logger.info("Reconnecting stream in 5 seconds...")
                    await asyncio.sleep(5)

        except Exception:
            self.logger.exception("Failed to start channel")

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
        if self._http:
            await self._http.aclose()
            self._http = None
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

    async def _get_access_token(self) -> str | None:
        """异步执行辅助逻辑（_get_access_token = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._get_access_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        data = {
            "appKey": self.config.client_id,
            "appSecret": self.config.client_secret,
        }

        if not self._http:
            self.logger.warning("HTTP client not initialized, cannot refresh token")
            return None

        try:
            resp = await self._http.post(url, json=data)
            resp.raise_for_status()
            res_data = resp.json()
            self._access_token = res_data.get("accessToken")
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            self._token_expiry = time.time() + int(res_data.get("expireIn", 7200)) - 60
            return self._access_token
        except Exception:
            self.logger.exception("Failed to get access token")
            return None

    @staticmethod
    def _is_http_url(value: str) -> bool:
        """判断条件是否成立（_is_http_url = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._is_http_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return urlparse(value).scheme in ("http", "https")

    def _guess_upload_type(self, media_ref: str) -> str:
        """上传资源（_guess_upload_type = 原函数名）。

        【中文名称】上传资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._guess_upload_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        ext = Path(urlparse(media_ref).path).suffix.lower()
        if ext in self._IMAGE_EXTS:
            return "image"
        if ext in self._AUDIO_EXTS:
            return "voice"
        if ext in self._VIDEO_EXTS:
            return "video"
        return "file"

    def _guess_filename(self, media_ref: str, upload_type: str) -> str:
        """执行辅助逻辑（_guess_filename = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._guess_filename` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        upload_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        name = os.path.basename(urlparse(media_ref).path)
        return name or {"image": "image.jpg", "voice": "audio.amr", "video": "video.mp4"}.get(upload_type, "file.bin")

    @staticmethod
    def _zip_bytes(filename: str, data: bytes) -> tuple[bytes, str, str]:
        """执行辅助逻辑（_zip_bytes = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._zip_bytes` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        filename: 文件或路径信息，代码会按安全边界读取或写入。
        data: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        stem = Path(filename).stem or "attachment"
        safe_name = filename or "attachment.bin"
        zip_name = f"{stem}.zip"
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(safe_name, data)
        return buffer.getvalue(), zip_name, "application/zip"

    def _normalize_upload_payload(
        self,
        filename: str,
        data: bytes,
        content_type: str | None,
    ) -> tuple[bytes, str, str | None]:
        """标准化数据（_normalize_upload_payload = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._normalize_upload_payload` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        filename: 文件或路径信息，代码会按安全边界读取或写入。
        data: 结构化数据负载，后续会被解析或转发。
        content_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        ext = Path(filename).suffix.lower()
        if ext in self._ZIP_BEFORE_UPLOAD_EXTS or content_type == "text/html":
            self.logger.info(
                "does not accept raw HTML attachments, zipping {} before upload",
                filename,
            )
            return self._zip_bytes(filename, data)
        return data, filename, content_type

    def _validate_remote_media_url(self, media_ref: str) -> bool:
        """校验输入（_validate_remote_media_url = 原函数名）。

        【中文名称】校验输入

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._validate_remote_media_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        ok, err = validate_url_target(media_ref)
        if not ok:
            self.logger.warning("remote media URL blocked ref={} reason={}", media_ref, err)
            return False
        return True

    def _redirect_host_allowed(self, current_url: str, next_url: str) -> bool:
        """执行辅助逻辑（_redirect_host_allowed = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._redirect_host_allowed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        current_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        next_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        current_host = (urlparse(current_url).hostname or "").lower()
        next_host = (urlparse(next_url).hostname or "").lower()
        if not next_host:
            return False
        if next_host == current_host:
            return True
        allowed_hosts = {host.lower() for host in self.config.remote_media_redirect_allowed_hosts}
        return next_host in allowed_hosts

    def _next_remote_media_url(self, current_url: str, location: str | None) -> str | None:
        """执行辅助逻辑（_next_remote_media_url = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._next_remote_media_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        current_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        location: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.config.allow_remote_media_redirects:
            self.logger.warning("media download redirect refused ref={}", current_url)
            return None
        if not location:
            self.logger.warning("media download redirect without Location ref={}", current_url)
            return None
        next_url = urljoin(current_url, location)
        if not self._redirect_host_allowed(current_url, next_url):
            self.logger.warning(
                "media download cross-host redirect refused ref={} next={}",
                current_url,
                next_url,
            )
            return None
        if not self._validate_remote_media_url(next_url):
            return None
        return next_url

    async def _fetch_remote_media_bytes(
        self,
        media_ref: str,
    ) -> tuple[bytes | None, str | None]:
        """异步执行辅助逻辑（_fetch_remote_media_bytes = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._fetch_remote_media_bytes` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            return None, None

        if not self._validate_remote_media_url(media_ref):
            return None, None

        try:
            # 中文说明：流式输出。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：兜底。
            stream = getattr(self._http, "stream", None)
            if stream is not None:
                current_url = media_ref
                for _ in range(DINGTALK_MAX_REMOTE_MEDIA_REDIRECTS + 1):
                    async with stream("GET", current_url, follow_redirects=False) as resp:
                        final_ok, final_err = validate_resolved_url(str(resp.url))
                        if not final_ok:
                            self.logger.warning(
                                "remote media redirect blocked ref={} final={} reason={}",
                                media_ref,
                                resp.url,
                                final_err,
                            )
                            return None, None
                        if 300 <= resp.status_code < 400:
                            next_url = self._next_remote_media_url(
                                str(resp.url), resp.headers.get("location")
                            )
                            if not next_url:
                                return None, None
                            current_url = next_url
                            continue
                        if resp.status_code >= 400:
                            self.logger.warning(
                                "media download failed status={} ref={}",
                                resp.status_code,
                                current_url,
                            )
                            return None, None
                        chunks: list[bytes] = []
                        total = 0
                        async for chunk in resp.aiter_bytes():
                            total += len(chunk)
                            if total > DINGTALK_MAX_REMOTE_MEDIA_BYTES:
                                self.logger.warning(
                                    "media download too large ref={} bytes>{}",
                                    current_url,
                                    DINGTALK_MAX_REMOTE_MEDIA_BYTES,
                                )
                                return None, None
                            chunks.append(chunk)
                        return b"".join(chunks), (resp.headers.get("content-type") or "")
                self.logger.warning("media download exceeded redirect limit ref={}", media_ref)
                return None, None

            current_url = media_ref
            for _ in range(DINGTALK_MAX_REMOTE_MEDIA_REDIRECTS + 1):
                resp = await self._http.get(current_url, follow_redirects=False)
                final_ok, final_err = validate_resolved_url(str(getattr(resp, "url", current_url)))
                if not final_ok:
                    self.logger.warning(
                        "remote media redirect blocked ref={} final={} reason={}",
                        media_ref,
                        getattr(resp, "url", current_url),
                        final_err,
                    )
                    return None, None
                if 300 <= resp.status_code < 400:
                    next_url = self._next_remote_media_url(
                        str(getattr(resp, "url", current_url)), resp.headers.get("location")
                    )
                    if not next_url:
                        return None, None
                    current_url = next_url
                    continue
                if resp.status_code >= 400:
                    self.logger.warning(
                        "media download failed status={} ref={}",
                        resp.status_code,
                        current_url,
                    )
                    return None, None
                if len(resp.content) > DINGTALK_MAX_REMOTE_MEDIA_BYTES:
                    self.logger.warning(
                        "media download too large ref={} bytes>{}",
                        current_url,
                        DINGTALK_MAX_REMOTE_MEDIA_BYTES,
                    )
                    return None, None
                return resp.content, (resp.headers.get("content-type") or "")
            self.logger.warning("media download exceeded redirect limit ref={}", media_ref)
            return None, None
        except httpx.TransportError:
            self.logger.exception("media download network error ref={}", media_ref)
            raise
        except Exception:
            self.logger.exception("media download error ref={}", media_ref)
            return None, None

    async def _read_media_bytes(
        self,
        media_ref: str,
    ) -> tuple[bytes | None, str | None, str | None]:
        """异步执行辅助逻辑（_read_media_bytes = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._read_media_bytes` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not media_ref:
            return None, None, None

        if self._is_http_url(media_ref):
            data, raw_content_type = await self._fetch_remote_media_bytes(media_ref)
            if data is None:
                return None, None, None
            content_type = (raw_content_type or "").split(";")[0].strip()
            filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
            return data, filename, content_type or None

        try:
            if media_ref.startswith("file://"):
                parsed = urlparse(media_ref)
                local_path = Path(unquote(parsed.path))
            else:
                local_path = Path(os.path.expanduser(media_ref))
            if not local_path.is_file():
                self.logger.warning("media file not found: {}", local_path)
                return None, None, None
            data = await asyncio.to_thread(local_path.read_bytes)
            content_type = mimetypes.guess_type(local_path.name)[0]
            return data, local_path.name, content_type
        except Exception:
            self.logger.exception("media read error ref={}", media_ref)
            return None, None, None

    async def _upload_media(
        self,
        token: str,
        data: bytes,
        media_type: str,
        filename: str,
        content_type: str | None,
    ) -> str | None:
        """异步上传资源（_upload_media = 原函数名）。

        【中文名称】上传资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._upload_media` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        data: 结构化数据负载，后续会被解析或转发。
        media_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filename: 文件或路径信息，代码会按安全边界读取或写入。
        content_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            return None
        url = f"https://oapi.dingtalk.com/media/upload?access_token={token}&type={media_type}"
        mime = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"media": (filename, data, mime)}

        try:
            resp = await self._http.post(url, files=files)
            text = resp.text
            result = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            if resp.status_code >= 400:
                self.logger.error("media upload failed status={} type={} body={}", resp.status_code, media_type, text[:500])
                return None
            errcode = result.get("errcode", 0)
            if errcode != 0:
                self.logger.error("media upload api error type={} errcode={} body={}", media_type, errcode, text[:500])
                return None
            sub = result.get("result") or {}
            media_id = result.get("media_id") or result.get("mediaId") or sub.get("media_id") or sub.get("mediaId")
            if not media_id:
                self.logger.error("media upload missing media_id body={}", text[:500])
                return None
            return str(media_id)
        except httpx.TransportError:
            self.logger.exception("media upload network error type={}", media_type)
            raise
        except Exception:
            self.logger.exception("media upload error type={}", media_type)
            return None

    async def _send_batch_message(
        self,
        token: str,
        chat_id: str,
        msg_key: str,
        msg_param: dict[str, Any],
    ) -> bool:
        """异步发送消息（_send_batch_message = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._send_batch_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        msg_key: 消息数据，可能来自用户、频道、模型或工具调用。
        msg_param: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            self.logger.warning("HTTP client not initialized, cannot send")
            return False

        headers = {"x-acs-dingtalk-access-token": token}
        if chat_id.startswith("group:"):
            # 中文说明：Group chat 相关逻辑。
            url = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
            payload = {
                "robotCode": self.config.client_id,
                "openConversationId": chat_id[6:],  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                "msgKey": msg_key,
                "msgParam": json.dumps(msg_param, ensure_ascii=False),
            }
        else:
            # 中文说明：Private chat 相关逻辑。
            url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
            payload = {
                "robotCode": self.config.client_id,
                "userIds": [chat_id],
                "msgKey": msg_key,
                "msgParam": json.dumps(msg_param, ensure_ascii=False),
            }

        try:
            resp = await self._http.post(url, json=payload, headers=headers)
            body = resp.text
            if resp.status_code != 200:
                self.logger.error("send failed msgKey={} status={} body={}", msg_key, resp.status_code, body[:500])
                return False
            try:
                result = resp.json()
            except Exception:
                result = {}
            errcode = result.get("errcode")
            if errcode not in (None, 0):
                self.logger.error("send api error msgKey={} errcode={} body={}", msg_key, errcode, body[:500])
                return False
            self.logger.debug("message sent to {} with msgKey={}", chat_id, msg_key)
            return True
        except httpx.TransportError:
            self.logger.exception("network error sending message msgKey={}", msg_key)
            raise
        except Exception:
            self.logger.exception("Error sending message msgKey={}", msg_key)
            return False

    async def _send_markdown_text(self, token: str, chat_id: str, content: str) -> bool:
        """异步发送消息（_send_markdown_text = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._send_markdown_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return await self._send_batch_message(
            token,
            chat_id,
            "sampleMarkdown",
            {"text": content, "title": "Nanobot Reply"},
        )

    async def _send_media_ref(self, token: str, chat_id: str, media_ref: str) -> bool:
        """异步发送消息（_send_media_ref = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._send_media_ref` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        media_ref = (media_ref or "").strip()
        if not media_ref:
            return True

        upload_type = self._guess_upload_type(media_ref)
        if upload_type == "image" and self._is_http_url(media_ref):
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_ref},
            )
            if ok:
                return True
            self.logger.warning("image url send failed, trying upload fallback: {}", media_ref)

        data, filename, content_type = await self._read_media_bytes(media_ref)
        if not data:
            self.logger.error("media read failed: {}", media_ref)
            return False

        filename = filename or self._guess_filename(media_ref, upload_type)
        data, filename, content_type = self._normalize_upload_payload(filename, data, content_type)
        file_type = Path(filename).suffix.lower().lstrip(".")
        if not file_type:
            guessed = mimetypes.guess_extension(content_type or "")
            file_type = (guessed or ".bin").lstrip(".")
        if file_type == "jpeg":
            file_type = "jpg"

        media_id = await self._upload_media(
            token=token,
            data=data,
            media_type=upload_type,
            filename=filename,
            content_type=content_type,
        )
        if not media_id:
            return False

        if upload_type == "image":
            # 中文说明：这一段围绕媒体、图片处理，注意输入、输出和异常路径。
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_id},
            )
            if ok:
                return True
            self.logger.warning("image media_id send failed, falling back to file: {}", media_ref)

        return await self._send_batch_message(
            token,
            chat_id,
            "sampleFile",
            {"mediaId": media_id, "fileName": filename, "fileType": file_type},
        )

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        token = await self._get_access_token()
        if not token:
            return

        if msg.content and msg.content.strip():
            await self._send_markdown_text(token, msg.chat_id, msg.content.strip())

        for media_ref in msg.media or []:
            ok = await self._send_media_ref(token, msg.chat_id, media_ref)
            if ok:
                continue
            self.logger.error("media send failed for {}", media_ref)
            # 中文说明：兜底。
            filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
            await self._send_markdown_text(
                token,
                msg.chat_id,
                f"[Attachment send failed: {filename}]",
            )

    async def _on_message(
        self,
        content: str,
        sender_id: str,
        sender_name: str,
        conversation_type: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        """异步执行辅助逻辑（_on_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._on_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        conversation_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        conversation_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            self.logger.info("inbound: {} from {}", content, sender_name)
            is_group = conversation_type == "2" and conversation_id
            chat_id = f"group:{conversation_id}" if is_group else sender_id
            session_key = None
            if is_group and self.config.group_user_isolation:
                session_key = f"{self.name}:group:{conversation_id}:{sender_id}"
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=str(content),
                metadata={
                    "sender_name": sender_name,
                    "platform": "dingtalk",
                    "conversation_type": conversation_type,
                },
                session_key=session_key,
            )
        except Exception:
            self.logger.exception("Error publishing message")

    async def _download_dingtalk_file(
        self,
        download_code: str,
        filename: str,
        sender_id: str,
    ) -> str | None:
        """异步下载资源（_download_dingtalk_file = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。钉钉 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DingTalkChannel._download_dingtalk_file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        download_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filename: 文件或路径信息，代码会按安全边界读取或写入。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        from nanobot.config.paths import get_media_dir

        try:
            token = await self._get_access_token()
            if not token or not self._http:
                self.logger.error("file download: no token or http client")
                return None

            # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
            api_url = "https://api.dingtalk.com/v1.0/robot/messageFiles/download"
            headers = {"x-acs-dingtalk-access-token": token, "Content-Type": "application/json"}
            payload = {"downloadCode": download_code, "robotCode": self.config.client_id}
            resp = await self._http.post(api_url, json=payload, headers=headers)
            if resp.status_code != 200:
                self.logger.error("get download URL failed: status={}, body={}", resp.status_code, resp.text)
                return None

            result = resp.json()
            download_url = result.get("downloadUrl")
            if not download_url:
                self.logger.error("download URL not found in response: {}", result)
                return None

            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            file_resp = await self._http.get(download_url, follow_redirects=True)
            if file_resp.status_code != 200:
                self.logger.error("file download failed: status={}", file_resp.status_code)
                return None

            # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
            download_dir = get_media_dir("dingtalk") / sender_id
            download_dir.mkdir(parents=True, exist_ok=True)
            file_path = download_dir / filename
            await asyncio.to_thread(file_path.write_bytes, file_resp.content)
            self.logger.info("file saved: {}", file_path)
            return str(file_path)
        except Exception:
            self.logger.exception("file download error")
            return None

