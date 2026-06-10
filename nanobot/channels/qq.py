"""QQ 官方机器人渠道适配器。

【中文名称】QQ 官方机器人渠道适配器

【功能说明】
负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
except Exception:  # pragma: no cover - 这是测试覆盖率工具指令；该分支只在特定可选依赖或平台环境下触发。
    get_media_dir = None  # type: ignore

try:
    import botpy
    from botpy.http import Route

    QQ_AVAILABLE = True
except ImportError:  # pragma: no cover - 这是测试覆盖率工具指令；该分支只在特定可选依赖或平台环境下触发。
    QQ_AVAILABLE = False
    botpy = None
    Route = None

if TYPE_CHECKING:
    from botpy.message import BaseMessage, C2CMessage, GroupMessage
    from botpy.types.message import Media


# 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

# 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
_SAFE_NAME_RE = re.compile(r"[^\w.\-()\[\]（）【】\u4e00-\u9fff]+", re.UNICODE)


def _sanitize_filename(name: str) -> str:
    """执行 `_sanitize_filename`。

    【中文名称】_sanitize_filename

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    name = (name or "").strip()
    name = Path(name).name
    name = _SAFE_NAME_RE.sub("_", name).strip("._ ")
    return name


def _is_image_name(name: str) -> bool:
    """执行 `_is_image_name`。

    【中文名称】_is_image_name

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return Path(name).suffix.lower() in _IMAGE_EXTS


def _guess_send_file_type(filename: str) -> int:
    """执行 `_guess_send_file_type`。

    【中文名称】_guess_send_file_type

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    ext = Path(filename).suffix.lower()
    mime, _ = mimetypes.guess_type(filename)
    if ext in _IMAGE_EXTS or (mime and mime.startswith("image/")):
        return QQ_FILE_TYPE_IMAGE
    return QQ_FILE_TYPE_FILE


def _make_bot_class(channel: QQChannel) -> type[botpy.Client]:
    """执行 `_make_bot_class`。

    【中文名称】_make_bot_class

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - channel: 调用方传入的 `channel` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    intents = botpy.Intents(public_messages=True, direct_message=True)

    class _Bot(botpy.Client):
        """_Bot 类。

        【中文名称】_Bot

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的核心数据结构或服务类。负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。

        【学习重点】
        - 类属性/字段通常描述外部平台、模型或工具的配置。
        - public 方法通常是其他模块会调用的入口。
        - private 方法通常负责协议细节、格式转换或异常兜底。"""

        def __init__(self):
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            """执行 `__init__`。

            【中文名称】__init__

            【功能说明】
            这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - 无显式业务参数。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            super().__init__(intents=intents, ext_handlers=False)

        async def on_ready(self):
            """异步执行 `on_ready`。

            【中文名称】on_ready

            【功能说明】
            这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - 无显式业务参数。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            logger.info("QQ bot ready: {}", self.robot.name)

        async def on_c2c_message_create(self, message: C2CMessage):
            """异步执行 `on_c2c_message_create`。

            【中文名称】on_c2c_message_create

            【功能说明】
            这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            await channel._on_message(message, is_group=False)

        async def on_group_at_message_create(self, message: GroupMessage):
            """异步执行 `on_group_at_message_create`。

            【中文名称】on_group_at_message_create

            【功能说明】
            这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            await channel._on_message(message, is_group=True)

        async def on_direct_message_create(self, message):
            """异步执行 `on_direct_message_create`。

            【中文名称】on_direct_message_create

            【功能说明】
            这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            await channel._on_message(message, is_group=False)

    return _Bot


class QQConfig(Base):
    """QQConfig 类。

    【中文名称】QQConfig

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的核心数据结构或服务类。负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    app_id: str = ""
    secret: str = ""
    allow_from: list[str] = Field(default_factory=list)
    msg_format: Literal["plain", "markdown"] = "plain"
    ack_message: str = "⏳ Processing..."

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    media_dir: str = ""

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    download_chunk_size: int = 1024 * 256  # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    download_max_bytes: int = 1024 * 1024 * 200  # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


class QQChannel(BaseChannel):
    """QQChannel 类。

    【中文名称】QQChannel

    【功能说明】
    这是 QQ 官方机器人渠道适配器 中的核心数据结构或服务类。负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "qq"
    display_name = "QQ"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return QQConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = QQConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: QQConfig = config

        self._client: botpy.Client | None = None
        self._http: aiohttp.ClientSession | None = None

        self._processed_ids: deque[str] = deque(maxlen=1000)
        self._msg_seq: int = 1  # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._chat_type_cache: dict[str, str] = {}

        self._media_root: Path = self._init_media_root()

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    def _init_media_root(self) -> Path:
        """执行 `_init_media_root`。

        【中文名称】_init_media_root

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_run_bot`。

        【中文名称】_run_bot

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        while self._running:
            try:
                await self._client.start(appid=self.config.app_id, secret=self.config.secret)
            except Exception as e:
                self.logger.warning("bot error: {}", e)
            if self._running:
                self.logger.info("Reconnecting bot in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            if not self._client:
                self.logger.warning("client not initialized")
                return

            msg_id = msg.metadata.get("message_id")
            chat_type = self._chat_type_cache.get(msg.chat_id, "c2c")
            is_group = chat_type == "group"

            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if msg.content and msg.content.strip():
                await self._send_text_only(
                    chat_id=msg.chat_id,
                    is_group=is_group,
                    msg_id=msg_id,
                    content=msg.content.strip(),
                )
        except (aiohttp.ClientError, OSError):
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_send_text_only`。

        【中文名称】_send_text_only

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - is_group: 调用方传入的 `is_group` 数据；具体类型以函数签名为准。
        - msg_id: 调用方传入的 `msg_id` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_send_media`。

        【中文名称】_send_media

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - media_ref: 调用方传入的 `media_ref` 数据；具体类型以函数签名为准。
        - msg_id: 调用方传入的 `msg_id` 数据；具体类型以函数签名为准。
        - is_group: 调用方传入的 `is_group` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            self.logger.warning("send media network error filename={} err={}", filename, e)
            raise
        except Exception:
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            self.logger.exception("send media failed filename={}", filename)
            return False

    async def _read_media_bytes(self, media_ref: str) -> tuple[bytes | None, str | None]:
        """异步执行 `_read_media_bytes`。

        【中文名称】_read_media_bytes

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - media_ref: 调用方传入的 `media_ref` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        media_ref = (media_ref or "").strip()
        if not media_ref:
            return None, None

        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if not media_ref.startswith("http://") and not media_ref.startswith("https://"):
            try:
                if media_ref.startswith("file://"):
                    parsed = urlparse(media_ref)
                    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    async def _post_base64file(
        self,
        chat_id: str,
        is_group: bool,
        file_type: int,
        file_data: str,
        file_name: str | None = None,
        srv_send_msg: bool = False,
    ) -> Media:
        """异步执行 `_post_base64file`。

        【中文名称】_post_base64file

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - is_group: 调用方传入的 `is_group` 数据；具体类型以函数签名为准。
        - file_type: 调用方传入的 `file_type` 数据；具体类型以函数签名为准。
        - file_data: 调用方传入的 `file_data` 数据；具体类型以函数签名为准。
        - file_name: 调用方传入的 `file_name` 数据；具体类型以函数签名为准。
        - srv_send_msg: 调用方传入的 `srv_send_msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if file_type != QQ_FILE_TYPE_IMAGE and file_name:
            payload["file_name"] = file_name

        route = Route("POST", endpoint, **{id_key: chat_id})
        result = await self._client.api._http.request(route, json=payload)

        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if isinstance(result, dict) and "file_info" in result:
            return {"file_info": result["file_info"]}
        return result

    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _on_message(self, data: C2CMessage | GroupMessage, is_group: bool = False) -> None:
        """异步执行 `_on_message`。

        【中文名称】_on_message

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。
        - is_group: 调用方传入的 `is_group` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not self.is_allowed(user_id):
                if not is_group:
                    await self._handle_message(
                        sender_id=user_id,
                        chat_id=chat_id,
                        content="",
                        is_dm=True,
                    )
                return

            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            attachments = getattr(data, "attachments", None) or []
            media_paths, recv_lines, att_meta = await self._handle_attachments(attachments)

            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_handle_attachments`。

        【中文名称】_handle_attachments

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - attachments: 调用方传入的 `attachments` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_download_to_media_dir_chunked`。

        【中文名称】_download_to_media_dir_chunked

        【功能说明】
        这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - url: 调用方传入的 `url` 数据；具体类型以函数签名为准。
        - filename_hint: 调用方传入的 `filename_hint` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

                # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

                # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                downloaded = 0
                chunk_size = max(1024, int(self.config.download_chunk_size or 262144))
                max_bytes = max(
                    1024 * 1024, int(self.config.download_max_bytes or (200 * 1024 * 1024))
                )

                def _open_tmp():
                    """执行 `_open_tmp`。

                    【中文名称】_open_tmp

                    【功能说明】
                    这是 QQ 官方机器人渠道适配器 中的一个步骤函数，用来支撑：负责对接 qq-botpy SDK，处理群聊/私聊/富媒体消息，并把回复、文件和流式状态发回 QQ。
                    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

                    【参数说明】
                    - 无显式业务参数。

                    【返回值】
                    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

                # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                await asyncio.to_thread(os.replace, tmp_path, target)
                tmp_path = None  # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                self.logger.info("file saved: {}", str(target))
                return str(target)

        except Exception:
            self.logger.exception("download error")
            return None
        finally:
            # 说明：这里处理 QQ 官方机器人渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if tmp_path is not None:
                with suppress(Exception):
                    tmp_path.unlink(missing_ok=True)
