"""企业微信渠道适配器。

【中文名称】企业微信渠道适配器

【功能说明】
负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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

# 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
WECOM_UPLOAD_MAX_BYTES = 1024 * 1024 * 200  # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

# 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
_SAFE_NAME_RE = re.compile(r"[^\w.\-()\[\]（）【】\u4e00-\u9fff]+", re.UNICODE)


def _sanitize_filename(name: str) -> str:
    """执行 `_sanitize_filename`。

    【中文名称】_sanitize_filename

    【功能说明】
    这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    name = (name or "").strip()
    name = Path(name).name
    name = _SAFE_NAME_RE.sub("_", name).strip("._ ")
    return name


_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
_VIDEO_EXTS = {".mp4", ".avi", ".mov"}
_AUDIO_EXTS = {".amr", ".mp3", ".wav", ".ogg"}


def _guess_wecom_media_type(filename: str) -> str:
    """执行 `_guess_wecom_media_type`。

    【中文名称】_guess_wecom_media_type

    【功能说明】
    这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    ext = Path(filename).suffix.lower()
    if ext in _IMAGE_EXTS:
        return "image"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _AUDIO_EXTS:
        return "voice"
    return "file"

class WecomConfig(Base):
    """WecomConfig 类。

    【中文名称】WecomConfig

    【功能说明】
    这是 企业微信渠道适配器 中的核心数据结构或服务类。负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    bot_id: str = ""
    secret: str = ""
    allow_from: list[str] = Field(default_factory=list)
    welcome_message: str = ""


# 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
MSG_TYPE_MAP = {
    "image": "[image]",
    "voice": "[voice]",
    "file": "[file]",
    "mixed": "[mixed content]",
}


class WecomChannel(BaseChannel):
    """WecomChannel 类。

    【中文名称】WecomChannel

    【功能说明】
    这是 企业微信渠道适配器 中的核心数据结构或服务类。负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "wecom"
    display_name = "WeCom"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return WecomConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = WecomConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: WecomConfig = config
        self._client: Any = None
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._generate_req_id = None
        # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._chat_frames: dict[str, Any] = {}

    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

        # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._client = WSClient({
            "bot_id": self.config.bot_id,
            "secret": self.config.secret,
            "reconnect_interval": 1000,
            "max_reconnect_attempts": -1,  # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            "heartbeat_interval": 30000,
        })

        # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

        # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        await self._client.connect_async()

        # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False
        if self._client:
            await self._client.disconnect()
        self.logger.info("bot stopped")

    async def _on_connected(self, frame: Any) -> None:
        """异步执行 `_on_connected`。

        【中文名称】_on_connected

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self.logger.info("WebSocket connected")

    async def _on_authenticated(self, frame: Any) -> None:
        """异步执行 `_on_authenticated`。

        【中文名称】_on_authenticated

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self.logger.info("authenticated successfully")

    async def _on_disconnected(self, frame: Any) -> None:
        """异步执行 `_on_disconnected`。

        【中文名称】_on_disconnected

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        reason = frame.body if hasattr(frame, 'body') else str(frame)
        self.logger.warning("WebSocket disconnected: {}", reason)

    async def _on_error(self, frame: Any) -> None:
        """异步执行 `_on_error`。

        【中文名称】_on_error

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self.logger.error("error: {}", frame)

    async def _on_text_message(self, frame: Any) -> None:
        """异步执行 `_on_text_message`。

        【中文名称】_on_text_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._process_message(frame, "text")

    async def _on_image_message(self, frame: Any) -> None:
        """异步执行 `_on_image_message`。

        【中文名称】_on_image_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._process_message(frame, "image")

    async def _on_voice_message(self, frame: Any) -> None:
        """异步执行 `_on_voice_message`。

        【中文名称】_on_voice_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._process_message(frame, "voice")

    async def _on_file_message(self, frame: Any) -> None:
        """异步执行 `_on_file_message`。

        【中文名称】_on_file_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._process_message(frame, "file")

    async def _on_mixed_message(self, frame: Any) -> None:
        """异步执行 `_on_mixed_message`。

        【中文名称】_on_mixed_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._process_message(frame, "mixed")

    async def _on_enter_chat(self, frame: Any) -> None:
        """异步执行 `_on_enter_chat`。

        【中文名称】_on_enter_chat

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_process_message`。

        【中文名称】_process_message

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - frame: 调用方传入的 `frame` 数据；具体类型以函数签名为准。
        - msg_type: 调用方传入的 `msg_type` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if hasattr(frame, 'body'):
                body = frame.body or {}
            elif isinstance(frame, dict):
                body = frame.get("body", frame)
            else:
                body = {}

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not isinstance(body, dict):
                self.logger.warning("Invalid body type: {}", type(body))
                return

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            msg_id = body.get("msgid", "")
            if not msg_id:
                msg_id = f"{body.get('chatid', '')}_{body.get('sendertime', '')}"

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            from_info = body.get("from", {})
            sender_id = from_info.get("userid", "unknown") if isinstance(from_info, dict) else "unknown"
            if not self.is_allowed(sender_id):
                return

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if msg_id in self._processed_message_ids:
                return
            self._processed_message_ids[msg_id] = None

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            while len(self._processed_message_ids) > 1000:
                self._processed_message_ids.popitem(last=False)

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            self._chat_frames[chat_id] = frame

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_download_and_save_media`。

        【中文名称】_download_and_save_media

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - file_url: 调用方传入的 `file_url` 数据；具体类型以函数签名为准。
        - aes_key: 调用方传入的 `aes_key` 数据；具体类型以函数签名为准。
        - media_type: 调用方传入的 `media_type` 数据；具体类型以函数签名为准。
        - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_upload_media_ws`。

        【中文名称】_upload_media_ws

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。
        - file_path: 调用方传入的 `file_path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from wecom_aibot_sdk.utils import generate_req_id as _gen_req_id

        try:
            fname = os.path.basename(file_path)
            media_type = _guess_wecom_media_type(fname)

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            def _read_file():
                """执行 `_read_file`。

                【中文名称】_read_file

                【功能说明】
                这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
                阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

                【参数说明】
                - 无显式业务参数。

                【返回值】
                - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

                file_size = os.path.getsize(file_path)
                if file_size > WECOM_UPLOAD_MAX_BYTES:
                    raise ValueError(
                        f"File too large: {file_size} bytes (max {WECOM_UPLOAD_MAX_BYTES})"
                    )
                with open(file_path, "rb") as f:
                    return file_size, f.read()

            file_size, data = await asyncio.to_thread(_read_file)
            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            md5_hash = hashlib.md5(data).hexdigest()

            chunk_size = 512 * 1024  # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            mv = memoryview(data)
            chunk_list = [bytes(mv[i : i + chunk_size]) for i in range(0, file_size, chunk_size)]
            n_chunks = len(chunk_list)
            del mv, data

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 企业微信渠道适配器 中的一个步骤函数，用来支撑：负责对接企业微信 AI Bot SDK，把文本、图片、文件和流式回复在企业微信与消息总线之间转换。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._client:
            self.logger.warning("client not initialized")
            return

        try:
            content = (msg.content or "").strip()
            is_progress = bool(msg.metadata.get("_progress"))

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            frame = self._chat_frames.get(msg.chat_id)

            # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                # 说明：这里处理 企业微信渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                await self._client.send_message(msg.chat_id, {
                    "msgtype": "markdown",
                    "markdown": {"content": content},
                })
                self.logger.info("proactive send to {}", msg.chat_id)

        except Exception:
            self.logger.exception("Error sending message to chat_id={}", msg.chat_id)
