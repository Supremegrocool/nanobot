"""企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/wecom.py

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
import base64
import hashlib
import importlib.util
import os
import re
from collections import OrderedDict
from pathlib import Path
from typing import Any

from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base

WECOM_AVAILABLE = importlib.util.find_spec("wecom_aibot_sdk") is not None

# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
WECOM_UPLOAD_MAX_BYTES = 1024 * 1024 * 200  # 中文说明：200MB 相关逻辑。

# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
_SAFE_NAME_RE = re.compile(r"[^\w.\-()\[\]（）【】\u4e00-\u9fff]+", re.UNICODE)


def _sanitize_filename(name: str) -> str:
    """执行辅助逻辑（_sanitize_filename = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_sanitize_filename` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    name = (name or "").strip()
    name = Path(name).name
    name = _SAFE_NAME_RE.sub("_", name).strip("._ ")
    return name


_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_VIDEO_EXTS = {".mp4", ".avi", ".mov"}
_AUDIO_EXTS = {".amr", ".mp3", ".wav", ".ogg"}


def _guess_wecom_media_type(filename: str) -> str:
    """执行辅助逻辑（_guess_wecom_media_type = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_guess_wecom_media_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    filename: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    ext = Path(filename).suffix.lower()
    if ext in _IMAGE_EXTS:
        return "image"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _AUDIO_EXTS:
        return "voice"
    return "file"

class WecomConfig(Base):
    """WecomConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WecomConfig

    【功能说明】
    企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    bot_id: str = ""
    secret: str = ""
    allow_from: list[str] = Field(default_factory=list)
    welcome_message: str = ""


# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
MSG_TYPE_MAP = {
    "image": "[image]",
    "voice": "[voice]",
    "file": "[file]",
    "mixed": "[mixed content]",
}


class WecomChannel(BaseChannel):
    """WecomChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WecomChannel

    【功能说明】
    企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "wecom"
    display_name = "WeCom"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return WecomConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = WecomConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: WecomConfig = config
        self._client: Any = None
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generate_req_id = None
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        self._chat_frames: dict[str, Any] = {}

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not WECOM_AVAILABLE:
            self.logger.error("SDK not installed. Run: pip install nanobot-ai[wecom]")
            return

        if not self.config.bot_id or not self.config.secret:
            self.logger.error("bot_id and secret not configured")
            return

        from wecom_aibot_sdk import WSClient, generate_req_id

        self._running = True
        self._loop = asyncio.get_running_loop()
        self._generate_req_id = generate_req_id

        # 中文说明：这一段围绕WebSocket处理，注意输入、输出和异常路径。
        self._client = WSClient({
            "bot_id": self.config.bot_id,
            "secret": self.config.secret,
            "reconnect_interval": 1000,
            "max_reconnect_attempts": -1,  # 中文说明：Infinite reconnect 相关逻辑。
            "heartbeat_interval": 30000,
        })

        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
        self._client.on("connected", self._on_connected)
        self._client.on("authenticated", self._on_authenticated)
        self._client.on("disconnected", self._on_disconnected)
        self._client.on("error", self._on_error)
        self._client.on("message.text", self._on_text_message)
        self._client.on("message.image", self._on_image_message)
        self._client.on("message.voice", self._on_voice_message)
        self._client.on("message.file", self._on_file_message)
        self._client.on("message.mixed", self._on_mixed_message)
        self._client.on("event.enter_chat", self._on_enter_chat)

        self.logger.info("bot starting with WebSocket long connection")
        self.logger.info("No public IP required - using WebSocket to receive events")

        # 中文说明：Connect 相关逻辑。
        await self._client.connect_async()

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        if self._client:
            await self._client.disconnect()
        self.logger.info("bot stopped")

    async def _on_connected(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_connected = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_connected` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.logger.info("WebSocket connected")

    async def _on_authenticated(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_authenticated = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_authenticated` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.logger.info("authenticated successfully")

    async def _on_disconnected(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_disconnected = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_disconnected` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        reason = frame.body if hasattr(frame, 'body') else str(frame)
        self.logger.warning("WebSocket disconnected: {}", reason)

    async def _on_error(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_error = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.logger.error("error: {}", frame)

    async def _on_text_message(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_text_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_text_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._process_message(frame, "text")

    async def _on_image_message(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_image_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_image_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._process_message(frame, "image")

    async def _on_voice_message(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_voice_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_voice_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._process_message(frame, "voice")

    async def _on_file_message(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_file_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_file_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._process_message(frame, "file")

    async def _on_mixed_message(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_mixed_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_mixed_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._process_message(frame, "mixed")

    async def _on_enter_chat(self, frame: Any) -> None:
        """异步执行辅助逻辑（_on_enter_chat = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._on_enter_chat` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            # 中文说明：提取。
            if hasattr(frame, 'body'):
                body = frame.body or {}
            elif isinstance(frame, dict):
                body = frame.get("body", frame)
            else:
                body = {}

            chat_id = body.get("chatid", "") if isinstance(body, dict) else ""

            if chat_id and not self.is_allowed(chat_id):
                return

            if chat_id and self.config.welcome_message:
                await self._client.reply_welcome(frame, {
                    "msgtype": "text",
                    "text": {"content": self.config.welcome_message},
                })
        except Exception:
            self.logger.exception("Error handling enter_chat")

    async def _process_message(self, frame: Any, msg_type: str) -> None:
        """异步执行辅助逻辑（_process_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._process_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        frame: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        msg_type: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            # 中文说明：提取。
            if hasattr(frame, 'body'):
                body = frame.body or {}
            elif isinstance(frame, dict):
                body = frame.get("body", frame)
            else:
                body = {}

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if not isinstance(body, dict):
                self.logger.warning("Invalid body type: {}", type(body))
                return

            # 中文说明：提取。
            msg_id = body.get("msgid", "")
            if not msg_id:
                msg_id = f"{body.get('chatid', '')}_{body.get('sendertime', '')}"

            # 中文说明：提取。
            from_info = body.get("from", {})
            sender_id = from_info.get("userid", "unknown") if isinstance(from_info, dict) else "unknown"
            if not self.is_allowed(sender_id):
                return

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if msg_id in self._processed_message_ids:
                return
            self._processed_message_ids[msg_id] = None

            # 中文说明：这一段围绕缓存处理，注意输入、输出和异常路径。
            while len(self._processed_message_ids) > 1000:
                self._processed_message_ids.popitem(last=False)

            # 中文说明：这一段围绕用户处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            chat_type = body.get("chattype", "single")
            chat_id = body.get("chatid", sender_id)

            content_parts = []
            media_paths: list[str] = []

            if msg_type == "text":
                text = body.get("text", {}).get("content", "")
                if text:
                    content_parts.append(text)

            elif msg_type == "image":
                image_info = body.get("image", {})
                file_url = image_info.get("url", "")
                aes_key = image_info.get("aeskey", "")

                if file_url and aes_key:
                    file_path = await self._download_and_save_media(file_url, aes_key, "image")
                    if file_path:
                        filename = os.path.basename(file_path)
                        content_parts.append(f"[image: {filename}]")
                        media_paths.append(file_path)
                    else:
                        content_parts.append("[image: download failed]")
                else:
                    content_parts.append("[image: download failed]")

            elif msg_type == "voice":
                voice_info = body.get("voice", {})
                # 中文说明：这一段围绕企业微信、消息处理，注意输入、输出和异常路径。
                voice_content = voice_info.get("content", "")
                if voice_content:
                    content_parts.append(f"[voice] {voice_content}")
                else:
                    content_parts.append("[voice]")

            elif msg_type == "file":
                file_info = body.get("file", {})
                file_url = file_info.get("url", "")
                aes_key = file_info.get("aeskey", "")
                file_name = file_info.get("name") or None

                if file_url and aes_key:
                    file_path = await self._download_and_save_media(file_url, aes_key, "file", file_name)
                    if file_path:
                        display_name = os.path.basename(file_path)
                        content_parts.append(f"[file: {display_name}]")
                        media_paths.append(file_path)
                    else:
                        content_parts.append(f"[file: {file_name or 'unknown'}: download failed]")
                else:
                    content_parts.append(f"[file: {file_name or 'unknown'}: download failed]")

            elif msg_type == "mixed":
                # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
                msg_items = body.get("mixed", {}).get("msg_item", [])
                for item in msg_items:
                    item_type = item.get("msgtype", "")
                    if item_type == "text":
                        text = item.get("text", {}).get("content", "")
                        if text:
                            content_parts.append(text)
                    elif item_type == "image":
                        file_url = item.get("image", {}).get("url", "")
                        aes_key = item.get("image", {}).get("aeskey", "")
                        if file_url and aes_key:
                            file_path = await self._download_and_save_media(file_url, aes_key, "image")
                            if file_path:
                                filename = os.path.basename(file_path)
                                content_parts.append(f"[image: {filename}]")
                                media_paths.append(file_path)
                    else:
                        content_parts.append(MSG_TYPE_MAP.get(item_type, f"[{item_type}]"))

            else:
                content_parts.append(MSG_TYPE_MAP.get(msg_type, f"[{msg_type}]"))

            content = "\n".join(content_parts) if content_parts else ""

            if not content:
                return

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            self._chat_frames[chat_id] = frame

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                media=media_paths or None,
                metadata={
                    "message_id": msg_id,
                    "msg_type": msg_type,
                    "chat_type": chat_type,
                }
            )

        except Exception:
            self.logger.exception("Error processing message")

    async def _download_and_save_media(
        self,
        file_url: str,
        aes_key: str,
        media_type: str,
        filename: str | None = None,
    ) -> str | None:
        """异步下载资源（_download_and_save_media = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._download_and_save_media` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_url: 文件或路径信息，代码会按安全边界读取或写入。
        aes_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filename: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            data, fname = await self._client.download_file(file_url, aes_key)

            if not data:
                self.logger.warning("Failed to download media")
                return None

            if len(data) > WECOM_UPLOAD_MAX_BYTES:
                self.logger.warning(
                    "inbound media too large: {} bytes (max {})",
                    len(data),
                    WECOM_UPLOAD_MAX_BYTES,
                )
                return None

            media_dir = get_media_dir("wecom")
            if not filename:
                filename = fname or f"{media_type}_{hash(file_url) % 100000}"
            filename = _sanitize_filename(filename)

            file_path = media_dir / filename
            await asyncio.to_thread(file_path.write_bytes, data)
            self.logger.debug("Downloaded {} to {}", media_type, file_path)
            return str(file_path)

        except Exception:
            self.logger.exception("Error downloading media")
            return None

    async def _upload_media_ws(
        self, client: Any, file_path: str,
    ) -> "tuple[str, str] | tuple[None, None]":
        """异步上传资源（_upload_media_ws = 原函数名）。

        【中文名称】上传资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel._upload_media_ws` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        client: 第三方 SDK 或 HTTP 客户端实例。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        from wecom_aibot_sdk.utils import generate_req_id as _gen_req_id

        try:
            fname = os.path.basename(file_path)
            media_type = _guess_wecom_media_type(fname)

            # 中文说明：这一段围绕事件、文件处理，注意输入、输出和异常路径。
            def _read_file():
                """执行辅助逻辑（_read_file = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `WecomChannel._read_file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                无显式参数。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                file_size = os.path.getsize(file_path)
                if file_size > WECOM_UPLOAD_MAX_BYTES:
                    raise ValueError(
                        f"File too large: {file_size} bytes (max {WECOM_UPLOAD_MAX_BYTES})"
                    )
                with open(file_path, "rb") as f:
                    return file_size, f.read()

            file_size, data = await asyncio.to_thread(_read_file)
            # 中文说明：这一段围绕文件、安全处理，注意输入、输出和异常路径。
            md5_hash = hashlib.md5(data).hexdigest()

            chunk_size = 512 * 1024  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            mv = memoryview(data)
            chunk_list = [bytes(mv[i : i + chunk_size]) for i in range(0, file_size, chunk_size)]
            n_chunks = len(chunk_list)
            del mv, data

            # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
            req_id = _gen_req_id("upload_init")
            resp = await client._ws_manager.send_reply(req_id, {
                "type": media_type,
                "filename": fname,
                "total_size": file_size,
                "total_chunks": n_chunks,
                "md5": md5_hash,
            }, "aibot_upload_media_init")
            if resp.errcode != 0:
                self.logger.warning("upload init failed ({}): {}", resp.errcode, resp.errmsg)
                return None, None
            upload_id = resp.body.get("upload_id") if resp.body else None
            if not upload_id:
                self.logger.warning("upload init: no upload_id in response")
                return None, None

            # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
            for i, chunk in enumerate(chunk_list):
                req_id = _gen_req_id("upload_chunk")
                resp = await client._ws_manager.send_reply(req_id, {
                    "upload_id": upload_id,
                    "chunk_index": i,
                    "base64_data": base64.b64encode(chunk).decode(),
                }, "aibot_upload_media_chunk")
                if resp.errcode != 0:
                    self.logger.warning("upload chunk {} failed ({}): {}", i, resp.errcode, resp.errmsg)
                    return None, None

            # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
            req_id = _gen_req_id("upload_finish")
            resp = await client._ws_manager.send_reply(req_id, {
                "upload_id": upload_id,
            }, "aibot_upload_media_finish")
            if resp.errcode != 0:
                self.logger.warning("upload finish failed ({}): {}", resp.errcode, resp.errmsg)
                return None, None

            media_id = resp.body.get("media_id") if resp.body else None
            if not media_id:
                self.logger.warning("upload finish: no media_id in response body={}", resp.body)
                return None, None

            suffix = "..." if len(media_id) > 16 else ""
            self.logger.debug("uploaded {} ({}) → media_id={}", fname, media_type, media_id[:16] + suffix)
            return media_id, media_type

        except ValueError as e:
            self.logger.warning("upload skipped for {}: {}", file_path, e)
            return None, None
        except Exception:
            self.logger.exception("_upload_media_ws error for {}", file_path)
            return None, None

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。企业微信 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WecomChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client:
            self.logger.warning("client not initialized")
            return

        try:
            content = (msg.content or "").strip()
            is_progress = bool(msg.metadata.get("_progress"))

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            frame = self._chat_frames.get(msg.chat_id)

            # 中文说明：这一段围绕WebSocket、媒体、文件处理，注意输入、输出和异常路径。
            for file_path in msg.media or []:
                if not os.path.isfile(file_path):
                    self.logger.warning("media file not found: {}", file_path)
                    continue
                media_id, media_type = await self._upload_media_ws(self._client, file_path)
                if media_id:
                    if frame:
                        await self._client.reply(frame, {
                            "msgtype": media_type,
                            media_type: {"media_id": media_id},
                        })
                    else:
                        await self._client.send_message(msg.chat_id, {
                            "msgtype": media_type,
                            media_type: {"media_id": media_id},
                        })
                    self.logger.debug("sent {} → {}", media_type, msg.chat_id)
                else:
                    content += f"\n[file upload failed: {os.path.basename(file_path)}]"

            if not content:
                return

            if frame:
                # 中文说明：这一段围绕消息、流式输出处理，注意输入、输出和异常路径。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这一段围绕企业微信、API处理，注意输入、输出和异常路径。
                stream_id = self._generate_req_id("stream")
                await self._client.reply_stream(
                    frame,
                    stream_id,
                    content,
                    finish=not is_progress,
                )
                self.logger.debug(
                    "{} sent to {}",
                    "progress" if is_progress else "message",
                    msg.chat_id,
                )
            else:
                # 中文说明：这一段围绕Markdown处理，注意输入、输出和异常路径。
                await self._client.send_message(msg.chat_id, {
                    "msgtype": "markdown",
                    "markdown": {"content": content},
                })
                self.logger.info("proactive send to {}", msg.chat_id)

        except Exception:
            self.logger.exception("Error sending message to chat_id={}", msg.chat_id)

