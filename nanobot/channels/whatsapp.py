"""WhatsApp 渠道适配器。

【中文名称】WhatsApp 渠道适配器

【功能说明】
负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """WhatsAppConfig 类。

    【中文名称】WhatsAppConfig

    【功能说明】
    这是 WhatsApp 渠道适配器 中的核心数据结构或服务类。负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    bridge_url: str = "ws://localhost:3001"
    bridge_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["open", "mention"] = "open"  # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


def _bridge_token_path() -> Path:
    """执行 `_bridge_token_path`。

    【中文名称】_bridge_token_path

    【功能说明】
    这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    from nanobot.config.paths import get_runtime_subdir

    return get_runtime_subdir("whatsapp-auth") / "bridge-token"


def _load_or_create_bridge_token(path: Path) -> str:
    """执行 `_load_or_create_bridge_token`。

    【中文名称】_load_or_create_bridge_token

    【功能说明】
    这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """WhatsAppChannel 类。

    【中文名称】WhatsAppChannel

    【功能说明】
    这是 WhatsApp 渠道适配器 中的核心数据结构或服务类。负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "whatsapp"
    display_name = "WhatsApp"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return WhatsAppConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = WhatsAppConfig.model_validate(config)
        super().__init__(config, bus)
        self._ws = None
        self._connected = False
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()
        self._lid_to_phone: dict[str, str] = {}
        self._bridge_token: str | None = None

    def _effective_bridge_token(self) -> str:
        """执行 `_effective_bridge_token`。

        【中文名称】_effective_bridge_token

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._bridge_token is not None:
            return self._bridge_token
        configured = self.config.bridge_token.strip()
        if configured:
            self._bridge_token = configured
        else:
            self._bridge_token = _load_or_create_bridge_token(_bridge_token_path())
        return self._bridge_token

    async def login(self, force: bool = False) -> bool:
        """异步执行 `login`。

        【中文名称】login

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - force: 调用方传入的 `force` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

                    # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False
        self._connected = False

        if self._ws:
            await self._ws.close()
            self._ws = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_handle_bridge_message`。

        【中文名称】_handle_bridge_message

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - raw: 调用方传入的 `raw` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            self.logger.warning("Invalid JSON from bridge: {}", raw[:100])
            return

        msg_type = data.get("type")

        if msg_type == "message":
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            pn = data.get("pn", "")
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            sender = data.get("sender", "")
            content = data.get("content", "")
            message_id = data.get("id", "")

            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            is_group = data.get("isGroup", False)
            was_mentioned = bool(data.get("wasMentioned", False) or data.get("isReplyToBot", False))

            if is_group and getattr(self.config, "group_policy", "open") == "mention":
                if not was_mentioned:
                    return

            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                    phone_id = extracted  # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

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

            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            media_paths = data.get("media") or []

            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if media_paths:
                for p in media_paths:
                    mime, _ = mimetypes.guess_type(p)
                    media_type = "image" if mime and mime.startswith("image/") else "file"
                    media_tag = f"[{media_type}: {p}]"
                    content = f"{content}\n{media_tag}" if content else media_tag

            await self._handle_message(
                sender_id=sender_id,
                chat_id=sender,  # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            status = data.get("status")
            self.logger.info("Status: {}", status)

            if status == "connected":
                self._connected = True
            elif status == "disconnected":
                self._connected = False

        elif msg_type == "qr":
            # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            self.logger.info("Scan QR code in the bridge terminal to connect WhatsApp")

        elif msg_type == "error":
            self.logger.error("Bridge error: {}", data.get("error"))


def _ensure_bridge_setup() -> Path:
    """执行 `_ensure_bridge_setup`。

    【中文名称】_ensure_bridge_setup

    【功能说明】
    这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    from nanobot.config.paths import get_bridge_install_dir

    user_bridge = get_bridge_install_dir()
    stamp_file = user_bridge / ".nanobot-bridge-source-hash"

    # 说明：这里处理 WhatsApp 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `source_hash`。

        【中文名称】source_hash

        【功能说明】
        这是 WhatsApp 渠道适配器 中的一个步骤函数，用来支撑：负责通过桥接进程接收 WhatsApp 消息、下载媒体，并把回复转发到 WhatsApp 聊天。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - root: 调用方传入的 `root` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
