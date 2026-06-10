"""NapCat QQ 渠道适配器。

【中文名称】NapCat QQ 渠道适配器

【功能说明】
负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。

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


# 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
GroupPolicy = Literal["mention", "open"] | Annotated[float, Field(ge=0.0, le=1.0)]


class NapcatConfig(Base):
    """NapcatConfig 类。

    【中文名称】NapcatConfig

    【功能说明】
    这是 NapCat QQ 渠道适配器 中的核心数据结构或服务类。负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    ws_url: str = "ws://127.0.0.1:3001"
    access_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    group_policy: GroupPolicy = "mention"
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    group_policy_overrides: dict[str, GroupPolicy] = Field(default_factory=dict)
    welcome_new_members: bool = True
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    max_image_bytes: int = Field(default=20 * 1024 * 1024, ge=1)


class NapcatChannel(BaseChannel):
    """NapcatChannel 类。

    【中文名称】NapcatChannel

    【功能说明】
    这是 NapCat QQ 渠道适配器 中的核心数据结构或服务类。负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "napcat"
    display_name = "Napcat (QQ)"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return NapcatConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not self.config.ws_url:
            logger.error("napcat: ws_url not configured")
            return

        self._running = True
        self._http = aiohttp.ClientSession(timeout=_DOWNLOAD_TIMEOUT)

        backoff = iter((5, 10))  # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        while self._running:
            try:
                await self._run_once()
                backoff = iter((5, 10))  # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("napcat: connection lost: {}", e)
            if self._running:
                await asyncio.sleep(next(backoff, 30))

    async def _run_once(self) -> None:
        """异步执行 `_run_once`。

        【中文名称】_run_once

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        headers = []
        if self.config.access_token:
            headers.append(("Authorization", f"Bearer {self.config.access_token}"))

        logger.info("napcat: connecting to {}", self.config.ws_url)
        async with ws_connect(self.config.ws_url, additional_headers=headers) as ws:
            self._ws = ws
            logger.info("napcat: connected")
            try:
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """执行 `_fail_pending`。

        【中文名称】_fail_pending

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - err: 调用方传入的 `err` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _dispatch_frame(self, raw: str | bytes) -> None:
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """异步执行 `_dispatch_frame`。

        【中文名称】_dispatch_frame

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - raw: 调用方传入的 `raw` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("napcat: dropping non-JSON frame")
            return
        if not isinstance(payload, dict):
            return

        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `_create_background_task`。

        【中文名称】_create_background_task

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - coro: 调用方传入的 `coro` 数据；具体类型以函数签名为准。
        - kind: 调用方传入的 `kind` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        task = asyncio.create_task(coro)
        self._background_tasks.add(task)

        def _done(done: asyncio.Task[None]) -> None:
            """执行 `_done`。

            【中文名称】_done

            【功能说明】
            这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - done: 调用方传入的 `done` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            self._background_tasks.discard(done)
            try:
                done.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("napcat: {} handler failed: {}", kind, e)

        task.add_done_callback(_done)

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _on_message(self, ev: dict[str, Any]) -> None:
        """异步执行 `_on_message`。

        【中文名称】_on_message

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - ev: 调用方传入的 `ev` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_normalize_segments`。

        【中文名称】_normalize_segments

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(message, list):
            return [seg for seg in message if isinstance(seg, dict)]
        if isinstance(message, str) and message:
            return [{"type": "text", "data": {"text": message}}]
        return []

    def _parse_segments(
        self, segments: list[dict[str, Any]]
    ) -> tuple[str, list[dict[str, Any]], bool, int | None]:
        """执行 `_parse_segments`。

        【中文名称】_parse_segments

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - segments: 调用方传入的 `segments` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `_should_reply_in_group`。

        【中文名称】_should_reply_in_group

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - group_id: 调用方传入的 `group_id` 数据；具体类型以函数签名为准。
        - mentioned_self: 调用方传入的 `mentioned_self` 数据；具体类型以函数签名为准。
        - replying_to_bot: 调用方传入的 `replying_to_bot` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if mentioned_self or replying_to_bot:
            return True
        policy = self.config.group_policy_overrides.get(str(group_id), self.config.group_policy)
        if policy == "open":
            return True
        if policy == "mention":
            return False
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        return random.random() < float(policy)

    @staticmethod
    def _format_group_content(
        *,
        text: str,
        nickname: str,
        user_id: Any,
    ) -> str:
        """执行 `_format_group_content`。

        【中文名称】_format_group_content

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - nickname: 调用方传入的 `nickname` 数据；具体类型以函数签名为准。
        - user_id: 调用方传入的 `user_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        label = nickname or str(user_id)
        return f"{label}: {text}"

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _on_notice(self, ev: dict[str, Any]) -> None:
        """异步执行 `_on_notice`。

        【中文名称】_on_notice

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - ev: 调用方传入的 `ev` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_lookup_member_name`。

        【中文名称】_lookup_member_name

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - group_id: 调用方传入的 `group_id` 数据；具体类型以函数签名为准。
        - user_id: 调用方传入的 `user_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            resp = await self._call_action(
                "get_group_member_info",
                {"group_id": group_id, "user_id": user_id, "no_cache": True},
            )
            data = resp.get("data", {})
            # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            return data.get("card") or data.get("nickname") or str(user_id)
        except Exception as e:
            logger.warning("napcat: get_group_member_info failed: {}", e)
            return str(user_id)

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `_build_image_segment`。

        【中文名称】_build_image_segment

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - ref: 调用方传入的 `ref` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        ref = (ref or "").strip()
        if not ref:
            return None
        if ref.startswith(("http://", "https://")):
            ok, err = validate_url_target(ref)
            if not ok:
                logger.warning("napcat: rejected remote image '{}': {}", ref, err)
                return None
            return {"type": "image", "data": {"file": ref}}
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `_call_action`。

        【中文名称】_call_action

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - action: 调用方传入的 `action` 数据；具体类型以函数签名为准。
        - params: 调用方传入的 `params` 数据；具体类型以函数签名为准。
        - timeout: 调用方传入的 `timeout` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _download_image(self, info: dict[str, Any]) -> str | None:
        """异步执行 `_download_image`。

        【中文名称】_download_image

        【功能说明】
        这是 NapCat QQ 渠道适配器 中的一个步骤函数，用来支撑：负责通过 NapCat HTTP/WebSocket 协议接入 QQ 消息、群聊、图片和文件，并把 nanobot 回复转换成 QQ 消息段。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - info: 调用方传入的 `info` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        url = info.get("url")
        if not isinstance(url, str):
            return None
        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if self._http is None:
            return None
        ok, err = validate_url_target(url)
        if not ok:
            logger.warning("napcat: skip image '{}': {}", url, err)
            return None
        max_bytes = self.config.max_image_bytes

        # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 NapCat QQ 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
