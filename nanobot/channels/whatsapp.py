"""WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/whatsapp.py

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
import hashlib
import json
import mimetypes
import os
import secrets
import shutil
import subprocess
from collections import OrderedDict
from contextlib import suppress
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base


class WhatsAppConfig(Base):
    """WhatsAppConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WhatsAppConfig

    【功能说明】
    WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    bridge_url: str = "ws://localhost:3001"
    bridge_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["open", "mention"] = "open"  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


def _bridge_token_path() -> Path:
    """执行辅助逻辑（_bridge_token_path = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_bridge_token_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    from nanobot.config.paths import get_runtime_subdir

    return get_runtime_subdir("whatsapp-auth") / "bridge-token"


def _load_or_create_bridge_token(path: Path) -> str:
    """加载数据（_load_or_create_bridge_token = 原函数名）。

    【中文名称】加载数据

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_load_or_create_bridge_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if path.exists():
        token = path.read_text(encoding="utf-8").strip()
        if token:
            return token

    path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    path.write_text(token, encoding="utf-8")
    with suppress(OSError):
        path.chmod(0o600)
    return token


class WhatsAppChannel(BaseChannel):
    """WhatsAppChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】WhatsAppChannel

    【功能说明】
    WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "whatsapp"
    display_name = "WhatsApp"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return WhatsAppConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = WhatsAppConfig.model_validate(config)
        super().__init__(config, bus)
        self._ws = None
        self._connected = False
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()
        self._lid_to_phone: dict[str, str] = {}
        self._bridge_token: str | None = None

    def _effective_bridge_token(self) -> str:
        """执行辅助逻辑（_effective_bridge_token = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel._effective_bridge_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._bridge_token is not None:
            return self._bridge_token
        configured = self.config.bridge_token.strip()
        if configured:
            self._bridge_token = configured
        else:
            self._bridge_token = _load_or_create_bridge_token(_bridge_token_path())
        return self._bridge_token

    async def login(self, force: bool = False) -> bool:
        """异步执行辅助逻辑（login = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.login` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        force: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            bridge_dir = _ensure_bridge_setup()
        except RuntimeError:
            self.logger.exception("bridge setup failed")
            return False

        env = {**os.environ}
        env["BRIDGE_TOKEN"] = self._effective_bridge_token()
        env["AUTH_DIR"] = str(_bridge_token_path().parent)

        self.logger.info("Starting WhatsApp bridge for QR login...")
        try:
            subprocess.run(
                [shutil.which("npm"), "start"], cwd=bridge_dir, check=True, env=env
            )
        except subprocess.CalledProcessError:
            return False

        return True

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        import websockets

        bridge_url = self.config.bridge_url

        self.logger.info("Connecting to WhatsApp bridge at {}...", bridge_url)

        self._running = True

        while self._running:
            try:
                async with websockets.connect(bridge_url) as ws:
                    self._ws = ws
                    await ws.send(
                        json.dumps({"type": "auth", "token": self._effective_bridge_token()})
                    )
                    self._connected = True
                    self.logger.info("Connected to WhatsApp bridge")

                    # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
                    async for message in ws:
                        try:
                            await self._handle_bridge_message(message)
                        except Exception:
                            self.logger.exception("Error handling bridge message")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                self._ws = None
                self.logger.warning("WhatsApp bridge connection error: {}", e)

                if self._running:
                    self.logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        self._connected = False

        if self._ws:
            await self._ws.close()
            self._ws = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._ws or not self._connected:
            self.logger.warning("WhatsApp bridge not connected")
            return

        chat_id = msg.chat_id

        if msg.content:
            try:
                payload = {"type": "send", "to": chat_id, "text": msg.content}
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
            except Exception:
                self.logger.exception("Error sending message")
                raise

        for media_path in msg.media or []:
            try:
                mime, _ = mimetypes.guess_type(media_path)
                payload = {
                    "type": "send_media",
                    "to": chat_id,
                    "filePath": media_path,
                    "mimetype": mime or "application/octet-stream",
                    "fileName": media_path.rsplit("/", 1)[-1],
                }
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
            except Exception:
                self.logger.exception("Error sending media {}", media_path)
                raise

    async def _handle_bridge_message(self, raw: str) -> None:
        """异步处理事件（_handle_bridge_message = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `WhatsAppChannel._handle_bridge_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.warning("Invalid JSON from bridge: {}", raw[:100])
            return

        msg_type = data.get("type")

        if msg_type == "message":
            # 中文说明：这一段围绕WhatsApp、消息处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕WhatsApp、调用处理，注意输入、输出和异常路径。
            pn = data.get("pn", "")
            # 中文说明：这一段围绕调用处理，注意输入、输出和异常路径。
            sender = data.get("sender", "")
            content = data.get("content", "")
            message_id = data.get("id", "")

            # 中文说明：提取。
            is_group = data.get("isGroup", False)
            was_mentioned = bool(data.get("wasMentioned", False) or data.get("isReplyToBot", False))

            if is_group and getattr(self.config, "group_policy", "open") == "mention":
                if not was_mentioned:
                    return

            # 中文说明：这一段围绕WhatsApp处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            raw_a = pn or ""
            participant = data.get("participant", "")
            raw_b = participant or sender or ""
            id_a = raw_a.split("@")[0] if "@" in raw_a else raw_a
            id_b = raw_b.split("@")[0] if "@" in raw_b else raw_b

            phone_id = ""
            lid_id = ""
            for raw, extracted in [(raw_a, id_a), (raw_b, id_b)]:
                if "@s.whatsapp.net" in raw:
                    phone_id = extracted
                elif "@lid.whatsapp.net" in raw:
                    lid_id = extracted
                elif extracted and not phone_id:
                    phone_id = extracted  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

            sender_id = phone_id or self._lid_to_phone.get(lid_id, "") or lid_id or id_a or id_b
            if not self.is_allowed(sender_id):
                return

            if message_id:
                if message_id in self._processed_message_ids:
                    return
                self._processed_message_ids[message_id] = None
                while len(self._processed_message_ids) > 1000:
                    self._processed_message_ids.popitem(last=False)

            if phone_id and lid_id:
                self._lid_to_phone[lid_id] = phone_id

            self.logger.info("Sender phone={} lid={} → sender_id={}", phone_id or "(empty)", lid_id or "(empty)", sender_id)

            # 中文说明：提取。
            media_paths = data.get("media") or []

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            if content == "[Voice Message]":
                if media_paths:
                    self.logger.info("Transcribing voice message from {}...", sender_id)
                    transcription = await self.transcribe_audio(media_paths[0])
                    if transcription:
                        content = transcription
                        media_paths = []
                        self.logger.info("Transcribed voice from {}: {}...", sender_id, transcription[:50])
                    else:
                        content = "[Voice Message: Transcription failed]"
                else:
                    content = "[Voice Message: Audio not available]"

            # 中文说明：这一段围绕Telegram、图片、文件、路径处理，注意输入、输出和异常路径。
            if media_paths:
                for p in media_paths:
                    mime, _ = mimetypes.guess_type(p)
                    media_type = "image" if mime and mime.startswith("image/") else "file"
                    media_tag = f"[{media_type}: {p}]"
                    content = f"{content}\n{media_tag}" if content else media_tag

            await self._handle_message(
                sender_id=sender_id,
                chat_id=sender,  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                content=content,
                media=media_paths,
                metadata={
                    "message_id": message_id,
                    "timestamp": data.get("timestamp"),
                    "is_group": data.get("isGroup", False),
                    "participant": participant or None,
                    "is_reply_to_bot": data.get("isReplyToBot", False),
                },
            )

        elif msg_type == "status":
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            status = data.get("status")
            self.logger.info("Status: {}", status)

            if status == "connected":
                self._connected = True
            elif status == "disconnected":
                self._connected = False

        elif msg_type == "qr":
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            self.logger.info("Scan QR code in the bridge terminal to connect WhatsApp")

        elif msg_type == "error":
            self.logger.error("Bridge error: {}", data.get("error"))


def _ensure_bridge_setup() -> Path:
    """确保前置条件成立（_ensure_bridge_setup = 原函数名）。

    【中文名称】确保前置条件成立

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_ensure_bridge_setup` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    from nanobot.config.paths import get_bridge_install_dir

    user_bridge = get_bridge_install_dir()
    stamp_file = user_bridge / ".nanobot-bridge-source-hash"

    # 中文说明：Find source bridge 相关逻辑。
    current_file = Path(__file__)
    pkg_bridge = current_file.parent.parent / "bridge"
    src_bridge = current_file.parent.parent.parent / "bridge"

    source = None
    if (pkg_bridge / "package.json").exists():
        source = pkg_bridge
    elif (src_bridge / "package.json").exists():
        source = src_bridge

    if not source:
        raise RuntimeError(
            "WhatsApp bridge source not found. "
            "Try reinstalling: pip install --force-reinstall nanobot"
        )

    def source_hash(root: Path) -> str:
        """执行辅助逻辑（source_hash = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。WhatsApp 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `source_hash` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        root: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        digest = hashlib.sha256()
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root)
            if rel.parts and rel.parts[0] in {"node_modules", "dist"}:
                continue
            digest.update(rel.as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
        return digest.hexdigest()

    expected_hash = source_hash(source)
    current_hash = stamp_file.read_text().strip() if stamp_file.exists() else None

    if (user_bridge / "dist" / "index.js").exists() and current_hash == expected_hash:
        return user_bridge

    if (user_bridge / "dist" / "index.js").exists() and current_hash != expected_hash:
        logger.info("WhatsApp bridge source changed; rebuilding bridge...")

    npm_path = shutil.which("npm")
    if not npm_path:
        raise RuntimeError("npm not found. Please install Node.js >= 18.")

    logger.info("Setting up WhatsApp bridge...")
    user_bridge.parent.mkdir(parents=True, exist_ok=True)
    if user_bridge.exists():
        shutil.rmtree(user_bridge)
    shutil.copytree(source, user_bridge, ignore=shutil.ignore_patterns("node_modules", "dist"))

    logger.info("  Installing dependencies...")
    subprocess.run([npm_path, "install"], cwd=user_bridge, check=True, capture_output=True)

    logger.info("  Building...")
    subprocess.run([npm_path, "run", "build"], cwd=user_bridge, check=True, capture_output=True)
    stamp_file.write_text(expected_hash + "\n")

    logger.info("Bridge ready")
    return user_bridge

