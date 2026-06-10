"""NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/napcat.py

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
import json
import os
import random
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Annotated, Any, Literal

import aiohttp
from loguru import logger
from pydantic import Field
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.security.network import validate_url_target
from nanobot.utils.helpers import safe_filename

_DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=60)
_ACTION_TIMEOUT = 20.0


# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
GroupPolicy = Literal["mention", "open"] | Annotated[float, Field(ge=0.0, le=1.0)]


class NapcatConfig(Base):
    """NapcatConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】NapcatConfig

    【功能说明】
    NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    ws_url: str = "ws://127.0.0.1:3001"
    access_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: GroupPolicy = "mention"
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    group_policy_overrides: dict[str, GroupPolicy] = Field(default_factory=dict)
    welcome_new_members: bool = True
    # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
    max_image_bytes: int = Field(default=20 * 1024 * 1024, ge=1)


class NapcatChannel(BaseChannel):
    """NapcatChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】NapcatChannel

    【功能说明】
    NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "napcat"
    display_name = "Napcat (QQ)"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return NapcatConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = NapcatConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: NapcatConfig = config

        self._ws: ClientConnection | None = None
        self._http: aiohttp.ClientSession | None = None
        self._media_root: Path = get_media_dir("napcat")
        self._self_id: int | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._processed_ids: deque[int] = deque(maxlen=2000)
        self._bot_outbound_ids: deque[int] = deque(maxlen=2000)
        self._background_tasks: set[asyncio.Task[None]] = set()

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Lifecycle 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.config.ws_url:
            logger.error("napcat: ws_url not configured")
            return

        self._running = True
        self._http = aiohttp.ClientSession(timeout=_DOWNLOAD_TIMEOUT)

        backoff = iter((5, 10))  # 中文说明：then 30s forever 相关逻辑。
        while self._running:
            try:
                await self._run_once()
                backoff = iter((5, 10))  # 中文说明：这一段围绕会话处理，注意输入、输出和异常路径。
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("napcat: connection lost: {}", e)
            if self._running:
                await asyncio.sleep(next(backoff, 30))

    async def _run_once(self) -> None:
        """异步运行流程（_run_once = 原函数名）。

        【中文名称】运行流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._run_once` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        headers = []
        if self.config.access_token:
            headers.append(("Authorization", f"Bearer {self.config.access_token}"))

        logger.info("napcat: connecting to {}", self.config.ws_url)
        async with ws_connect(self.config.ws_url, additional_headers=headers) as ws:
            self._ws = ws
            logger.info("napcat: connected")
            try:
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
                # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
                echo = uuid.uuid4().hex
                await ws.send(
                    json.dumps(
                        {"action": "get_login_info", "params": {}, "echo": echo},
                        ensure_ascii=False,
                    )
                )
                deadline = asyncio.get_running_loop().time() + _ACTION_TIMEOUT
                while True:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise asyncio.TimeoutError("get_login_info timed out")
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(payload, dict) and payload.get("echo") == echo:
                        data = payload.get("data") or {}
                        logger.info(
                            "napcat: logged in as {} (user_id={})",
                            data.get("nickname"),
                            data.get("user_id"),
                        )
                        break
                    await self._dispatch_frame(raw)

                async for raw in ws:
                    await self._dispatch_frame(raw)
            finally:
                self._ws = None
                self._fail_pending(RuntimeError("napcat: websocket disconnected"))

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._http is not None:
            try:
                await self._http.close()
            except Exception:
                pass
            self._http = None
        self._fail_pending(RuntimeError("napcat: stopped"))
        tasks = list(self._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    def _fail_pending(self, err: BaseException) -> None:
        """执行辅助逻辑（_fail_pending = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._fail_pending` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        err: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Frame dispatch 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _dispatch_frame(self, raw: str | bytes) -> None:
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        """异步执行辅助逻辑（_dispatch_frame = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._dispatch_frame` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("napcat: dropping non-JSON frame")
            return
        if not isinstance(payload, dict):
            return

        # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
        if "echo" in payload and payload.get("post_type") is None:
            echo = payload.get("echo")
            fut = self._pending.pop(echo, None) if isinstance(echo, str) else None
            if fut and not fut.done():
                fut.set_result(payload)
            return

        if (sid := payload.get("self_id")) is not None:
            try:
                self._self_id = int(sid)
            except (TypeError, ValueError):
                pass

        post_type = payload.get("post_type")
        if post_type == "message":
            self._create_background_task(self._on_message(payload), "message")
        elif post_type == "notice":
            self._create_background_task(self._on_notice(payload), "notice")

    def _create_background_task(self, coro: Any, kind: str) -> None:
        """创建对象（_create_background_task = 原函数名）。

        【中文名称】创建对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._create_background_task` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        coro: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        kind: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)

        def _done(done: asyncio.Task[None]) -> None:
            """执行辅助逻辑（_done = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `NapcatChannel._done` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            done: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            self._background_tasks.discard(done)
            try:
                done.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("napcat: {} handler failed: {}", kind, e)

        task.add_done_callback(_done)

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _on_message(self, ev: dict[str, Any]) -> None:
        """异步执行辅助逻辑（_on_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._on_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        ev: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        msg_id = ev.get("message_id")
        if isinstance(msg_id, int):
            if msg_id in self._processed_ids:
                return
            self._processed_ids.append(msg_id)

        message_type = ev.get("message_type")
        user_id = ev.get("user_id")
        if user_id is None or message_type not in ("group", "private"):
            return

        segments = self._normalize_segments(ev.get("message"))
        text, images, mentioned_self, reply_to_id = self._parse_segments(segments)

        media_paths: list[str] = []
        for info in images:
            if local := await self._download_image(info):
                media_paths.append(local)

        sender = ev.get("sender") or {}
        nickname = sender.get("card") or sender.get("nickname")

        if message_type == "group":
            group_id = ev.get("group_id")
            if group_id is None:
                return

            replying_to_bot = (
                isinstance(reply_to_id, int) and reply_to_id in self._bot_outbound_ids
            )
            if not self._should_reply_in_group(
                group_id=group_id,
                mentioned_self=mentioned_self,
                replying_to_bot=replying_to_bot,
            ):
                return

            chat_id = f"group:{group_id}"
            content = self._format_group_content(
                text=text,
                nickname=nickname,
                user_id=user_id,
            )
        else:
            chat_id = f"private:{user_id}"
            content = text

        if not content and not media_paths:
            return

        await self._handle_message(
            sender_id=str(user_id),
            chat_id=chat_id,
            content=content,
            media=media_paths or None,
            metadata={
                "message_id": msg_id,
                "is_group": message_type == "group",
                "nickname": nickname,
                "reply_to": reply_to_id,
            },
        )

    @staticmethod
    def _normalize_segments(message: Any) -> list[dict[str, Any]]:
        # 中文说明：这一段围绕格式处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕用户、配置处理，注意输入、输出和异常路径。
        """标准化数据（_normalize_segments = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._normalize_segments` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(message, list):
            return [seg for seg in message if isinstance(seg, dict)]
        if isinstance(message, str) and message:
            return [{"type": "text", "data": {"text": message}}]
        return []

    def _parse_segments(
        self, segments: list[dict[str, Any]]
    ) -> tuple[str, list[dict[str, Any]], bool, int | None]:
        """解析数据（_parse_segments = 原函数名）。

        【中文名称】解析数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._parse_segments` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        segments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        parts: list[str] = []
        images: list[dict[str, Any]] = []
        mentioned_self = False
        reply_to: int | None = None
        self_id_str = str(self._self_id) if self._self_id is not None else None

        for seg in segments:
            stype = seg.get("type")
            data = seg.get("data") or {}
            if stype == "text":
                if txt := data.get("text"):
                    parts.append(str(txt))
            elif stype == "image":
                # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
                # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
                # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
                url = data.get("url")
                if isinstance(url, str) and url.startswith(("http://", "https://")):
                    images.append(
                        {
                            "url": url,
                            "file": data.get("file"),
                            "file_size": data.get("file_size"),
                        }
                    )
                else:
                    logger.warning("napcat: received invalid image url: {}", url)
            elif stype == "at":
                qq = str(data.get("qq", ""))
                if self_id_str and qq == self_id_str:
                    mentioned_self = True
                else:
                    parts.append(f"@{qq}")
            elif stype == "reply":
                rid = data.get("id")
                try:
                    reply_to = int(rid) if rid is not None else None
                except (TypeError, ValueError):
                    pass
            elif stype == "face":
                parts.append(f"[face:{data.get('id', '')}]")

        text = " ".join(p.strip() for p in parts if p.strip()).strip()
        return text, images, mentioned_self, reply_to

    def _should_reply_in_group(
        self, *, group_id: Any, mentioned_self: bool, replying_to_bot: bool
    ) -> bool:
        """执行辅助逻辑（_should_reply_in_group = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._should_reply_in_group` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        group_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        mentioned_self: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        replying_to_bot: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if mentioned_self or replying_to_bot:
            return True
        policy = self.config.group_policy_overrides.get(str(group_id), self.config.group_policy)
        if policy == "open":
            return True
        if policy == "mention":
            return False
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        return random.random() < float(policy)

    @staticmethod
    def _format_group_content(
        *,
        text: str,
        nickname: str,
        user_id: Any,
    ) -> str:
        """格式化内容（_format_group_content = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._format_group_content` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        nickname: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        label = nickname or str(user_id)
        return f"{label}: {text}"

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _on_notice(self, ev: dict[str, Any]) -> None:
        """异步执行辅助逻辑（_on_notice = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._on_notice` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        ev: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if ev.get("notice_type") != "group_increase" or not self.config.welcome_new_members:
            return

        group_id = ev.get("group_id")
        user_id = ev.get("user_id")
        if group_id is None or user_id is None:
            return

        try:
            group_id_int = int(group_id)
            user_id_int = int(user_id)
        except (TypeError, ValueError):
            logger.warning("napcat: invalid group_increase ids group_id={} user_id={}", group_id, user_id)
            return

        nickname = await self._lookup_member_name(group_id_int, user_id_int)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这一段围绕用户处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕模型、消息处理，注意输入、输出和异常路径。
        await self._handle_message(
            sender_id=str(user_id),
            chat_id=f"group:{group_id}",
            content=f"[group event] new member {nickname} joined group {group_id}",
            metadata={
                "is_group": True,
                "event": "group_increase",
            },
        )

    async def _lookup_member_name(self, group_id: int, user_id: int) -> str:
        """异步执行辅助逻辑（_lookup_member_name = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._lookup_member_name` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        group_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            resp = await self._call_action(
                "get_group_member_info",
                {"group_id": group_id, "user_id": user_id, "no_cache": True},
            )
            data = resp.get("data", {})
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            return data.get("card") or data.get("nickname") or str(user_id)
        except Exception as e:
            logger.warning("napcat: get_group_member_info failed: {}", e)
            return str(user_id)

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Outbound 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._ws is None:
            logger.warning("napcat: not connected, dropping outbound message")
            return

        kind, _, target = msg.chat_id.partition(":")
        if kind not in ("private", "group") or not target:
            logger.error("napcat: invalid chat_id '{}'", msg.chat_id)
            return

        segments: list[dict[str, Any]] = []
        for ref in msg.media or []:
            if seg := await self._build_image_segment(ref):
                segments.append(seg)
        if text := (msg.content or "").strip():
            segments.append({"type": "text", "data": {"text": text}})
        if not segments:
            return

        params: dict[str, Any] = {"message": segments}
        if kind == "group":
            params["message_type"] = "group"
            params["group_id"] = int(target)
        else:
            params["message_type"] = "private"
            params["user_id"] = int(target)

        resp = await self._call_action("send_msg", params)
        data = resp.get("data") or {}
        if (mid := data.get("message_id")) is not None:
            self._bot_outbound_ids.append(int(mid))

    async def _build_image_segment(self, ref: str) -> dict[str, Any] | None:
        """异步构建对象（_build_image_segment = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._build_image_segment` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        ref: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        ref = (ref or "").strip()
        if not ref:
            return None
        if ref.startswith(("http://", "https://")):
            ok, err = validate_url_target(ref)
            if not ok:
                logger.warning("napcat: rejected remote image '{}': {}", ref, err)
                return None
            return {"type": "image", "data": {"file": ref}}
        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        path = Path(os.path.expanduser(ref)).resolve()
        if not path.is_file():
            logger.warning("napcat: local image not found: {}", path)
            return None
        data = await asyncio.to_thread(path.read_bytes)
        return {"type": "image", "data": {"file": "base64://" + base64.b64encode(data).decode()}}

    async def _call_action(
        self,
        action: str,
        params: dict[str, Any],
        timeout: float = _ACTION_TIMEOUT,
    ) -> dict[str, Any]:
        """异步调用服务（_call_action = 原函数名）。

        【中文名称】调用服务

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._call_action` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        action: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        params: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        timeout: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._ws is None:
            raise RuntimeError("napcat: not connected")
        echo = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[echo] = fut
        try:
            await self._ws.send(
                json.dumps({"action": action, "params": params, "echo": echo}, ensure_ascii=False)
            )
            resp = await asyncio.wait_for(fut, timeout=timeout)
            status = resp.get("status")
            retcode = resp.get("retcode")
            if (status and status != "ok") or (retcode not in (None, 0)):
                raise RuntimeError(
                    f"napcat: action {action} failed status={status!r} retcode={retcode!r}"
                )
            return resp
        finally:
            self._pending.pop(echo, None)

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def _download_image(self, info: dict[str, Any]) -> str | None:
        """异步下载资源（_download_image = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。NapCat / QQ 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `NapcatChannel._download_image` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        info: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        url = info.get("url")
        if not isinstance(url, str):
            return None
        # 中文说明：这一段围绕图片处理，注意输入、输出和异常路径。
        if self._http is None:
            return None
        ok, err = validate_url_target(url)
        if not ok:
            logger.warning("napcat: skip image '{}': {}", url, err)
            return None
        max_bytes = self.config.max_image_bytes

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        try:
            declared_size = int(info["file_size"])
            if declared_size > max_bytes:
                logger.warning(
                    "napcat: image declared size={} exceeds max_image_bytes={} url={}",
                    declared_size,
                    max_bytes,
                    url,
                )
                return None
        except (TypeError, KeyError):
            pass

        try:
            async with self._http.get(url, allow_redirects=False) as resp:
                if 300 <= resp.status < 400:
                    logger.warning("napcat: image download redirect rejected url={}", url)
                    return None
                if resp.status >= 400:
                    logger.warning("napcat: image download status={} url={}", resp.status, url)
                    return None
                # 中文说明：这一段围绕流式输出处理，注意输入、输出和异常路径。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这一段围绕响应、图片处理，注意输入、输出和异常路径。
                buf = bytearray()
                truncated = False
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    buf.extend(chunk)
                    if len(buf) > max_bytes:
                        truncated = True
                        break
                if truncated:
                    logger.warning(
                        "napcat: image exceeds max_image_bytes={} url={}", max_bytes, url
                    )
                    return None
                data = bytes(buf)
        except Exception as e:
            logger.warning("napcat: image download error url={} err={}", url, e)
            return None

        filename_hint = info.get("file")
        if filename_hint:
            name = safe_filename(filename_hint)
        else:
            name = f"{int(time.time() * 1000)}.jpg"
        path = self._media_root / name
        try:
            await asyncio.to_thread(path.write_bytes, data)
        except OSError as e:
            logger.warning("napcat: failed to save image: {}", e)
            return None
        return str(path)

