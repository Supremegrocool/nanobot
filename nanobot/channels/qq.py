"""QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/qq.py

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
import mimetypes
import os
import re
import time
from collections import deque
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import unquote, urlparse

import aiohttp
from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base
from nanobot.security.network import validate_url_target
from nanobot.utils.logging_bridge import redirect_lib_logging

try:
    from nanobot.config.paths import get_media_dir
except Exception:  # pragma: no cover
    get_media_dir = None  # type: ignore

try:
    import botpy
    from botpy.http import Route

    QQ_AVAILABLE = True
except ImportError:  # pragma: no cover
    QQ_AVAILABLE = False
    botpy = None
    Route = None

if TYPE_CHECKING:
    from botpy.message import BaseMessage, C2CMessage, GroupMessage
    from botpy.types.message import Media


# 中文说明：这一段围绕媒体、图片、文件处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕图片、文件处理，注意输入、输出和异常路径。
QQ_FILE_TYPE_IMAGE = 1
QQ_FILE_TYPE_FILE = 4

_IMAGE_EXTS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".webp",
    ".tif",
    ".tiff",
    ".ico",
    ".svg",
}

# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
_SAFE_NAME_RE = re.compile(r"[^\w.\-()\[\]（）【】\u4e00-\u9fff]+", re.UNICODE)


def _sanitize_filename(name: str) -> str:
    """执行辅助逻辑（_sanitize_filename = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
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


def _is_image_name(name: str) -> bool:
    """判断条件是否成立（_is_image_name = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_is_image_name` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return Path(name).suffix.lower() in _IMAGE_EXTS


def _guess_send_file_type(filename: str) -> int:
    """发送消息（_guess_send_file_type = 原函数名）。

    【中文名称】发送消息

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_guess_send_file_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    filename: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    ext = Path(filename).suffix.lower()
    mime, _ = mimetypes.guess_type(filename)
    if ext in _IMAGE_EXTS or (mime and mime.startswith("image/")):
        return QQ_FILE_TYPE_IMAGE
    return QQ_FILE_TYPE_FILE


def _make_bot_class(channel: QQChannel) -> type[botpy.Client]:
    """执行辅助逻辑（_make_bot_class = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_make_bot_class` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    intents = botpy.Intents(public_messages=True, direct_message=True)

    class _Bot(botpy.Client):
        """_Bot 类，封装 渠道适配器 的核心状态和行为。

        【中文名称】_Bot

        【功能说明】
        QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
        让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

        【继承关系】
        botpy.Client。继承关系决定它需要实现哪些项目约定的方法。

        【学习提示】
        先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
        """
        def __init__(self):
            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            """初始化对象（__init__ = 原函数名）。

            【中文名称】初始化对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_Bot.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            super().__init__(intents=intents, ext_handlers=False)

        async def on_ready(self):
            """异步执行辅助逻辑（on_ready = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_Bot.on_ready` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            logger.info("QQ bot ready: {}", self.robot.name)

        async def on_c2c_message_create(self, message: C2CMessage):
            """异步创建对象（on_c2c_message_create = 原函数名）。

            【中文名称】创建对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_Bot.on_c2c_message_create` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            message: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await channel._on_message(message, is_group=False)

        async def on_group_at_message_create(self, message: GroupMessage):
            """异步创建对象（on_group_at_message_create = 原函数名）。

            【中文名称】创建对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_Bot.on_group_at_message_create` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            message: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await channel._on_message(message, is_group=True)

        async def on_direct_message_create(self, message):
            """异步创建对象（on_direct_message_create = 原函数名）。

            【中文名称】创建对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_Bot.on_direct_message_create` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            message: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await channel._on_message(message, is_group=False)

    return _Bot


class QQConfig(Base):
    """QQConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】QQConfig

    【功能说明】
    QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    app_id: str = ""
    secret: str = ""
    allow_from: list[str] = Field(default_factory=list)
    msg_format: Literal["plain", "markdown"] = "plain"
    ack_message: str = "⏳ Processing..."

    # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
    media_dir: str = ""

    # 中文说明：Download tuning 相关逻辑。
    download_chunk_size: int = 1024 * 256  # 中文说明：256KB 相关逻辑。
    download_max_bytes: int = 1024 * 1024 * 200  # 中文说明：200MB safety limit 相关逻辑。


class QQChannel(BaseChannel):
    """QQChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】QQChannel

    【功能说明】
    QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "qq"
    display_name = "QQ"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return QQConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = QQConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: QQConfig = config

        self._client: botpy.Client | None = None
        self._http: aiohttp.ClientSession | None = None

        self._processed_ids: deque[str] = deque(maxlen=1000)
        self._msg_seq: int = 1  # 中文说明：这一段围绕API处理，注意输入、输出和异常路径。
        self._chat_type_cache: dict[str, str] = {}

        self._media_root: Path = self._init_media_root()

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Lifecycle 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    def _init_media_root(self) -> Path:
        """执行辅助逻辑（_init_media_root = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._init_media_root` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.config.media_dir:
            root = Path(self.config.media_dir).expanduser()
        elif get_media_dir:
            try:
                root = Path(get_media_dir("qq"))
            except Exception:
                root = Path.home() / ".nanobot" / "media" / "qq"
        else:
            root = Path.home() / ".nanobot" / "media" / "qq"

        root.mkdir(parents=True, exist_ok=True)
        self.logger.info("media directory: {}", str(root))
        return root

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        redirect_lib_logging("botpy", level="WARNING")
        if not QQ_AVAILABLE:
            self.logger.error("SDK not installed. Run: pip install qq-botpy")
            return

        if not self.config.app_id or not self.config.secret:
            self.logger.error("app_id and secret not configured")
            return

        self._running = True
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

        self._client = _make_bot_class(self)()
        self.logger.info("bot started (C2C & Group supported)")
        await self._run_bot()

    async def _run_bot(self) -> None:
        """异步运行流程（_run_bot = 原函数名）。

        【中文名称】运行流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._run_bot` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        while self._running:
            try:
                await self._client.start(appid=self.config.app_id, secret=self.config.secret)
            except Exception as e:
                self.logger.warning("bot error: {}", e)
            if self._running:
                self.logger.info("Reconnecting bot in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        if self._client:
            with suppress(Exception):
                await self._client.close()
        self._client = None

        if self._http:
            with suppress(Exception):
                await self._http.close()
        self._http = None

        self.logger.info("bot stopped")

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Outbound (send) 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            if not self._client:
                self.logger.warning("client not initialized")
                return

            msg_id = msg.metadata.get("message_id")
            chat_type = self._chat_type_cache.get(msg.chat_id, "c2c")
            is_group = chat_type == "group"

            # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
            for media_ref in msg.media or []:
                ok = await self._send_media(
                    chat_id=msg.chat_id,
                    media_ref=media_ref,
                    msg_id=msg_id,
                    is_group=is_group,
                )
                if not ok:
                    filename = (
                        os.path.basename(urlparse(media_ref).path)
                        or os.path.basename(media_ref)
                        or "file"
                    )
                    await self._send_text_only(
                        chat_id=msg.chat_id,
                        is_group=is_group,
                        msg_id=msg_id,
                        content=f"[Attachment send failed: {filename}]",
                    )

            # 中文说明：2) Send text 相关逻辑。
            if msg.content and msg.content.strip():
                await self._send_text_only(
                    chat_id=msg.chat_id,
                    is_group=is_group,
                    msg_id=msg_id,
                    content=msg.content.strip(),
                )
        except (aiohttp.ClientError, OSError):
            # 中文说明：这一段围绕重试、错误处理，注意输入、输出和异常路径。
            raise
        except Exception:
            self.logger.exception("Error sending message to chat_id={}", msg.chat_id)

    async def _send_text_only(
        self,
        chat_id: str,
        is_group: bool,
        msg_id: str | None,
        content: str,
    ) -> None:
        """异步发送消息（_send_text_only = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._send_text_only` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        is_group: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        msg_id: 消息数据，可能来自用户、频道、模型或工具调用。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client:
            return

        self._msg_seq += 1
        use_markdown = self.config.msg_format == "markdown"
        payload: dict[str, Any] = {
            "msg_type": 2 if use_markdown else 0,
            "msg_id": msg_id,
            "msg_seq": self._msg_seq,
        }
        if use_markdown:
            payload["markdown"] = {"content": content}
        else:
            payload["content"] = content

        if is_group:
            await self._client.api.post_group_message(group_openid=chat_id, **payload)
        else:
            await self._client.api.post_c2c_message(openid=chat_id, **payload)

    async def _send_media(
        self,
        chat_id: str,
        media_ref: str,
        msg_id: str | None,
        is_group: bool,
    ) -> bool:
        """异步发送消息（_send_media = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._send_media` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        msg_id: 消息数据，可能来自用户、频道、模型或工具调用。
        is_group: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client:
            return False

        data, filename = await self._read_media_bytes(media_ref)
        if not data or not filename:
            return False

        try:
            file_type = _guess_send_file_type(filename)
            file_data_b64 = base64.b64encode(data).decode()

            media_obj = await self._post_base64file(
                chat_id=chat_id,
                is_group=is_group,
                file_type=file_type,
                file_data=file_data_b64,
                file_name=filename,
                srv_send_msg=False,
            )
            if not media_obj:
                self.logger.error("media upload failed: empty response")
                return False

            self._msg_seq += 1
            if is_group:
                await self._client.api.post_group_message(
                    group_openid=chat_id,
                    msg_type=7,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                    media=media_obj,
                )
            else:
                await self._client.api.post_c2c_message(
                    openid=chat_id,
                    msg_type=7,
                    msg_id=msg_id,
                    msg_seq=self._msg_seq,
                    media=media_obj,
                )

            self.logger.info("media sent: {}", filename)
            return True
        except (aiohttp.ClientError, OSError) as e:
            # 中文说明：这一段围绕调用、重试、错误处理，注意输入、输出和异常路径。
            self.logger.warning("send media network error filename={} err={}", filename, e)
            raise
        except Exception:
            # 中文说明：兜底。
            self.logger.exception("send media failed filename={}", filename)
            return False

    async def _read_media_bytes(self, media_ref: str) -> tuple[bytes | None, str | None]:
        """异步执行辅助逻辑（_read_media_bytes = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._read_media_bytes` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        media_ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        media_ref = (media_ref or "").strip()
        if not media_ref:
            return None, None

        # 中文说明：这一段围绕文件、路径处理，注意输入、输出和异常路径。
        if not media_ref.startswith("http://") and not media_ref.startswith("https://"):
            try:
                if media_ref.startswith("file://"):
                    parsed = urlparse(media_ref)
                    # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
                    raw = parsed.path or parsed.netloc
                    local_path = Path(unquote(raw))
                else:
                    local_path = Path(os.path.expanduser(media_ref))

                if not local_path.is_file():
                    self.logger.warning("outbound media file not found: {}", str(local_path))
                    return None, None

                data = await asyncio.to_thread(local_path.read_bytes)
                return data, local_path.name
            except Exception as e:
                self.logger.warning("outbound media read error ref={} err={}", media_ref, e)
                return None, None

        # 中文说明：Remote URL 相关逻辑。
        ok, err = validate_url_target(media_ref)
        if not ok:
            self.logger.warning("outbound media URL validation failed url={} err={}", media_ref, err)
            return None, None

        if not self._http:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
        try:
            async with self._http.get(media_ref, allow_redirects=True) as resp:
                if resp.status >= 400:
                    self.logger.warning(
                        "outbound media download failed status={} url={}",
                        resp.status,
                        media_ref,
                    )
                    return None, None
                data = await resp.read()
                if not data:
                    return None, None
                filename = os.path.basename(urlparse(media_ref).path) or "file.bin"
                return data, filename
        except Exception as e:
            self.logger.warning("outbound media download error url={} err={}", media_ref, e)
            return None, None

    # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
    # 中文说明：这一段围绕消息、API、HTTP、媒体处理，注意输入、输出和异常路径。
    async def _post_base64file(
        self,
        chat_id: str,
        is_group: bool,
        file_type: int,
        file_data: str,
        file_name: str | None = None,
        srv_send_msg: bool = False,
    ) -> Media:
        """异步执行辅助逻辑（_post_base64file = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._post_base64file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        is_group: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        file_type: 文件或路径信息，代码会按安全边界读取或写入。
        file_data: 文件或路径信息，代码会按安全边界读取或写入。
        file_name: 文件或路径信息，代码会按安全边界读取或写入。
        srv_send_msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._client:
            raise RuntimeError("QQ client not initialized")

        if is_group:
            endpoint = "/v2/groups/{group_openid}/files"
            id_key = "group_openid"
        else:
            endpoint = "/v2/users/{openid}/files"
            id_key = "openid"

        payload: dict[str, Any] = {
            id_key: chat_id,
            "file_type": file_type,
            "file_data": file_data,
            "srv_send_msg": srv_send_msg,
        }
        # 中文说明：这一段围绕图片、文件处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕图片、文件处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕图片、文件处理，注意输入、输出和异常路径。
        if file_type != QQ_FILE_TYPE_IMAGE and file_name:
            payload["file_name"] = file_name

        route = Route("POST", endpoint, **{id_key: chat_id})
        result = await self._client.api._http.request(route, json=payload)

        # 中文说明：提取。
        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        if isinstance(result, dict) and "file_info" in result:
            return {"file_info": result["file_info"]}
        return result

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Inbound (receive) 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _on_message(self, data: C2CMessage | GroupMessage, is_group: bool = False) -> None:
        """异步执行辅助逻辑（_on_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._on_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        data: 结构化数据负载，后续会被解析或转发。
        is_group: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            if is_group:
                chat_id = data.group_openid
                user_id = data.author.member_openid
                chat_type = "group"
            else:
                chat_id = str(
                    getattr(data.author, "id", None)
                    or getattr(data.author, "user_openid", "unknown")
                )
                user_id = chat_id
                chat_type = "c2c"

            content = (data.content or "").strip()

            if data.id in self._processed_ids:
                return
            self._processed_ids.append(data.id)
            self._chat_type_cache[chat_id] = chat_type

            # 中文说明：这一段围绕权限处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕消息、用户处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            if not self.is_allowed(user_id):
                if not is_group:
                    await self._handle_message(
                        sender_id=user_id,
                        chat_id=chat_id,
                        content="",
                        is_dm=True,
                    )
                return

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：这一段围绕错误处理，注意输入、输出和异常路径。
            attachments = getattr(data, "attachments", None) or []
            media_paths, recv_lines, att_meta = await self._handle_attachments(attachments)

            # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
            if recv_lines:
                tag = (
                    "[Image]"
                    if any(_is_image_name(Path(p).name) for p in media_paths)
                    else "[File]"
                )
                file_block = "Received files:\n" + "\n".join(recv_lines)
                content = (
                    f"{content}\n\n{file_block}".strip() if content else f"{tag}\n{file_block}"
                )

            if not content and not media_paths:
                return

            if self.config.ack_message:
                try:
                    await self._send_text_only(
                        chat_id=chat_id,
                        is_group=is_group,
                        msg_id=data.id,
                        content=self.config.ack_message,
                    )
                except Exception:
                    self.logger.debug("ack message failed for chat_id={}", chat_id)

            await self._handle_message(
                sender_id=user_id,
                chat_id=chat_id,
                content=content,
                media=media_paths if media_paths else None,
                metadata={
                    "message_id": data.id,
                    "attachments": att_meta,
                },
                is_dm=not is_group,
            )
        except Exception:
            self.logger.exception("Error handling inbound message id={}", getattr(data, "id", "?"))

    async def _handle_attachments(
        self,
        attachments: list[BaseMessage._Attachments],
    ) -> tuple[list[str], list[str], list[dict[str, Any]]]:
        """异步处理事件（_handle_attachments = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._handle_attachments` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        attachments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        media_paths: list[str] = []
        recv_lines: list[str] = []
        att_meta: list[dict[str, Any]] = []

        if not attachments:
            return media_paths, recv_lines, att_meta

        for att in attachments:
            url = getattr(att, "url", None) or ""
            filename = getattr(att, "filename", None) or ""
            ctype = getattr(att, "content_type", None) or ""

            self.logger.info("Downloading file: {}", filename or url)
            local_path = await self._download_to_media_dir_chunked(url, filename_hint=filename)

            att_meta.append(
                {
                    "url": url,
                    "filename": filename,
                    "content_type": ctype,
                    "saved_path": local_path,
                }
            )

            if local_path:
                media_paths.append(local_path)
                shown_name = filename or os.path.basename(local_path)
                recv_lines.append(f"- {shown_name}\n  saved: {local_path}")
            else:
                shown_name = filename or url
                recv_lines.append(f"- {shown_name}\n  saved: [download failed]")

        return media_paths, recv_lines, att_meta

    async def _download_to_media_dir_chunked(
        self,
        url: str,
        filename_hint: str = "",
    ) -> str | None:
        """异步下载资源（_download_to_media_dir_chunked = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `QQChannel._download_to_media_dir_chunked` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filename_hint: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        if url.startswith("//"):
            url = f"https:{url}"

        if not self._http:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

        safe = _sanitize_filename(filename_hint)
        ts = int(time.time() * 1000)
        tmp_path: Path | None = None

        try:
            async with self._http.get(
                url,
                timeout=aiohttp.ClientTimeout(total=120),
                allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    self.logger.warning("download failed: status={} url={}", resp.status, url)
                    return None

                ctype = (resp.headers.get("Content-Type") or "").lower()

                # 中文说明：兜底。
                ext = Path(urlparse(url).path).suffix
                if not ext:
                    ext = Path(filename_hint).suffix
                if not ext:
                    if "png" in ctype:
                        ext = ".png"
                    elif "jpeg" in ctype or "jpg" in ctype:
                        ext = ".jpg"
                    elif "gif" in ctype:
                        ext = ".gif"
                    elif "webp" in ctype:
                        ext = ".webp"
                    elif "pdf" in ctype:
                        ext = ".pdf"
                    else:
                        ext = ".bin"

                if safe:
                    if not Path(safe).suffix:
                        safe = safe + ext
                    filename = safe
                else:
                    filename = f"qq_file_{ts}{ext}"

                target = self._media_root / filename
                if target.exists():
                    target = self._media_root / f"{target.stem}_{ts}{target.suffix}"

                tmp_path = target.with_suffix(target.suffix + ".part")

                # 中文说明：这一段围绕流式输出处理，注意输入、输出和异常路径。
                downloaded = 0
                chunk_size = max(1024, int(self.config.download_chunk_size or 262144))
                max_bytes = max(
                    1024 * 1024, int(self.config.download_max_bytes or (200 * 1024 * 1024))
                )

                def _open_tmp():
                    """执行辅助逻辑（_open_tmp = 原函数名）。

                    【中文名称】执行辅助逻辑

                    【功能说明】
                    这是 渠道适配器 中的一个关键步骤。QQ 官方机器人 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                    在阅读 `QQChannel._open_tmp` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                    【参数说明】
                    无显式参数。

                    【返回值】
                    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                    """
                    tmp_path.parent.mkdir(parents=True, exist_ok=True)
                    return open(tmp_path, "wb")  # noqa: SIM115

                f = await asyncio.to_thread(_open_tmp)
                try:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        if not chunk:
                            continue
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            self.logger.warning(
                                "download exceeded max_bytes={} url={} -> abort",
                                max_bytes,
                                url,
                            )
                            return None
                        await asyncio.to_thread(f.write, chunk)
                finally:
                    await asyncio.to_thread(f.close)

                # 中文说明：Atomic rename 相关逻辑。
                await asyncio.to_thread(os.replace, tmp_path, target)
                tmp_path = None  # 中文说明：mark as moved 相关逻辑。
                self.logger.info("file saved: {}", str(target))
                return str(target)

        except Exception:
            self.logger.exception("download error")
            return None
        finally:
            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            if tmp_path is not None:
                with suppress(Exception):
                    tmp_path.unlink(missing_ok=True)

