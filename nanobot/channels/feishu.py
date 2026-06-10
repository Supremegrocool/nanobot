"""飞书渠道适配器。

【中文名称】飞书渠道适配器

【功能说明】
负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.utils.helpers import safe_filename
from nanobot.utils.logging_bridge import redirect_lib_logging

if TYPE_CHECKING:
    from lark_oapi.api.im.v1.model import MentionEvent, P2ImMessageReceiveV1

FEISHU_AVAILABLE = importlib.util.find_spec("lark_oapi") is not None


def _load_lark_runtime() -> tuple[Any, str, str]:
    """执行 `_load_lark_runtime`。

    【中文名称】_load_lark_runtime

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    import sys

    ws_client_already_imported = "lark_oapi.ws.client" in sys.modules
    import lark_oapi as lark
    import lark_oapi.ws.client as lark_ws_client
    from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN

    if (
        not ws_client_already_imported
        and threading.current_thread() is not threading.main_thread()
    ):
        import_loop = getattr(lark_ws_client, "loop", None)
        if (
            import_loop is not None
            and not import_loop.is_running()
            and not import_loop.is_closed()
        ):
            import_loop.close()
        lark_ws_client.loop = None
        with suppress(Exception):
            asyncio.set_event_loop(None)

    return lark, FEISHU_DOMAIN, LARK_DOMAIN

# 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
MSG_TYPE_MAP = {
    "image": "[image]",
    "audio": "[audio]",
    "file": "[file]",
    "sticker": "[sticker]",
}


def _extract_share_card_content(content_json: dict, msg_type: str) -> str:
    """执行 `_extract_share_card_content`。

    【中文名称】_extract_share_card_content

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content_json: 调用方传入的 `content_json` 数据；具体类型以函数签名为准。
    - msg_type: 调用方传入的 `msg_type` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    parts = []

    if msg_type == "share_chat":
        parts.append(f"[shared chat: {content_json.get('chat_id', '')}]")
    elif msg_type == "share_user":
        parts.append(f"[shared user: {content_json.get('user_id', '')}]")
    elif msg_type == "interactive":
        parts.extend(_extract_interactive_content(content_json))
    elif msg_type == "share_calendar_event":
        parts.append(f"[shared calendar event: {content_json.get('event_key', '')}]")
    elif msg_type == "system":
        parts.append("[system message]")
    elif msg_type == "merge_forward":
        parts.append("[merged forward messages]")

    return "\n".join(parts) if parts else f"[{msg_type}]"


def _extract_interactive_content(content: dict) -> list[str]:
    """执行 `_extract_interactive_content`。

    【中文名称】_extract_interactive_content

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    parts = []

    if isinstance(content, str):
        try:
            content = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return [content] if content.strip() else []

    if not isinstance(content, dict):
        return parts

    if "title" in content:
        title = content["title"]
        if isinstance(title, dict):
            title_content = title.get("content", "") or title.get("text", "")
            if title_content:
                parts.append(f"title: {title_content}")
        elif isinstance(title, str):
            parts.append(f"title: {title}")

    for elements in (
        content.get("elements", []) if isinstance(content.get("elements"), list) else []
    ):
        for element in elements:
            parts.extend(_extract_element_content(element))

    card = content.get("card", {})
    if card:
        parts.extend(_extract_interactive_content(card))

    header = content.get("header", {})
    if header:
        header_title = header.get("title", {})
        if isinstance(header_title, dict):
            header_text = header_title.get("content", "") or header_title.get("text", "")
            if header_text:
                parts.append(f"title: {header_text}")

    return parts


def _extract_element_content(element: dict) -> list[str]:
    """执行 `_extract_element_content`。

    【中文名称】_extract_element_content

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - element: 调用方传入的 `element` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    parts = []

    if not isinstance(element, dict):
        return parts

    tag = element.get("tag", "")

    if tag in ("markdown", "lark_md"):
        content = element.get("content", "")
        if content:
            parts.append(content)

    elif tag == "div":
        text = element.get("text", {})
        if isinstance(text, dict):
            text_content = text.get("content", "") or text.get("text", "")
            if text_content:
                parts.append(text_content)
        elif isinstance(text, str):
            parts.append(text)
        for field in element.get("fields", []):
            if isinstance(field, dict):
                field_text = field.get("text", {})
                if isinstance(field_text, dict):
                    c = field_text.get("content", "")
                    if c:
                        parts.append(c)

    elif tag == "a":
        href = element.get("href", "")
        text = element.get("text", "")
        if href:
            parts.append(f"link: {href}")
        if text:
            parts.append(text)

    elif tag == "button":
        text = element.get("text", {})
        if isinstance(text, dict):
            c = text.get("content", "")
            if c:
                parts.append(c)
        url = element.get("url", "") or element.get("multi_url", {}).get("url", "")
        if url:
            parts.append(f"link: {url}")

    elif tag == "img":
        alt = element.get("alt", {})
        parts.append(alt.get("content", "[image]") if isinstance(alt, dict) else "[image]")

    elif tag == "note":
        for ne in element.get("elements", []):
            parts.extend(_extract_element_content(ne))

    elif tag == "column_set":
        for col in element.get("columns", []):
            for ce in col.get("elements", []):
                parts.extend(_extract_element_content(ce))

    elif tag == "plain_text":
        content = element.get("content", "")
        if content:
            parts.append(content)

    else:
        for ne in element.get("elements", []):
            parts.extend(_extract_element_content(ne))

    return parts


def _extract_post_content(content_json: dict) -> tuple[str, list[str]]:
    """执行 `_extract_post_content`。

    【中文名称】_extract_post_content

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content_json: 调用方传入的 `content_json` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    def _parse_block(block: dict) -> tuple[str | None, list[str]]:
        """执行 `_parse_block`。

        【中文名称】_parse_block

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - block: 调用方传入的 `block` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not isinstance(block, dict) or not isinstance(block.get("content"), list):
            return None, []
        texts, images = [], []
        if title := block.get("title"):
            texts.append(title)
        for row in block["content"]:
            if not isinstance(row, list):
                continue
            for el in row:
                if not isinstance(el, dict):
                    continue
                tag = el.get("tag")
                if tag in ("text", "a"):
                    texts.append(el.get("text", ""))
                elif tag == "at":
                    texts.append(f"@{el.get('user_name', 'user')}")
                elif tag == "code_block":
                    lang = el.get("language", "")
                    code_text = el.get("text", "")
                    texts.append(f"\n```{lang}\n{code_text}\n```\n")
                elif tag == "img" and (key := el.get("image_key")):
                    images.append(key)
        return (" ".join(texts).strip() or None), images

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    root = content_json
    if isinstance(root, dict) and isinstance(root.get("post"), dict):
        root = root["post"]
    if not isinstance(root, dict):
        return "", []

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    if "content" in root:
        text, imgs = _parse_block(root)
        if text or imgs:
            return text or "", imgs

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    for key in ("zh_cn", "en_us", "ja_jp"):
        if key in root:
            text, imgs = _parse_block(root[key])
            if text or imgs:
                return text or "", imgs
    for val in root.values():
        if isinstance(val, dict):
            text, imgs = _parse_block(val)
            if text or imgs:
                return text or "", imgs

    return "", []


def _extract_post_text(content_json: dict) -> str:
    """执行 `_extract_post_text`。

    【中文名称】_extract_post_text

    【功能说明】
    这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content_json: 调用方传入的 `content_json` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    text, _ = _extract_post_content(content_json)
    return text


class FeishuConfig(Base):
    """FeishuConfig 类。

    【中文名称】FeishuConfig

    【功能说明】
    这是 飞书渠道适配器 中的核心数据结构或服务类。负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    app_id: str = ""
    app_secret: str = ""
    encrypt_key: str = ""
    verification_token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    react_emoji: str = "THUMBSUP"
    done_emoji: str | None = None  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    tool_hint_prefix: str = "\U0001f527"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    group_policy: Literal["open", "mention"] = "mention"
    reply_to_message: bool = False  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    streaming: bool = True
    domain: Literal["feishu", "lark"] = "feishu"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    topic_isolation: bool = True  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


_STREAM_ELEMENT_ID = "streaming_md"


@dataclass
class _FeishuStreamBuf:
    """_FeishuStreamBuf 类。

    【中文名称】_FeishuStreamBuf

    【功能说明】
    这是 飞书渠道适配器 中的核心数据结构或服务类。负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    text: str = ""
    card_id: str | None = None
    sequence: int = 0
    last_edit: float = 0.0


class FeishuChannel(BaseChannel):
    """FeishuChannel 类。

    【中文名称】FeishuChannel

    【功能说明】
    这是 飞书渠道适配器 中的核心数据结构或服务类。负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "feishu"
    display_name = "Feishu"

    _STREAM_EDIT_INTERVAL = 0.5  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return FeishuConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = FeishuConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: FeishuConfig = config
        self._client: Any = None
        self._ws_client: Any = None
        self._ws_thread: threading.Thread | None = None
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stream_bufs: dict[str, _FeishuStreamBuf] = {}
        self._bot_open_id: str | None = None
        self._background_tasks: set[asyncio.Task] = set()
        self._reaction_ids: dict[str, str] = {}  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    @staticmethod
    def _register_optional_event(builder: Any, method_name: str, handler: Any) -> Any:
        """执行 `_register_optional_event`。

        【中文名称】_register_optional_event

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - builder: 调用方传入的 `builder` 数据；具体类型以函数签名为准。
        - method_name: 调用方传入的 `method_name` 数据；具体类型以函数签名为准。
        - handler: 调用方传入的 `handler` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        method = getattr(builder, method_name, None)
        return method(handler) if callable(method) else builder

    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not FEISHU_AVAILABLE:
            self.logger.error("SDK not installed. Run: pip install lark-oapi")
            return

        if not self.config.app_id or not self.config.app_secret:
            self.logger.error("app_id and app_secret not configured")
            return

        lark, feishu_domain, lark_domain = await asyncio.to_thread(_load_lark_runtime)

        redirect_lib_logging("Lark")

        self._running = True
        self._loop = asyncio.get_running_loop()

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        domain = lark_domain if self.config.domain == "lark" else feishu_domain
        self._client = (
            lark.Client.builder()
            .app_id(self.config.app_id)
            .app_secret(self.config.app_secret)
            .domain(domain)
            .log_level(lark.LogLevel.INFO)
            .build()
        )
        builder = lark.EventDispatcherHandler.builder(
            self.config.encrypt_key or "",
            self.config.verification_token or "",
        ).register_p2_im_message_receive_v1(self._on_message_sync)
        builder = self._register_optional_event(
            builder, "register_p2_im_message_reaction_created_v1", self._on_reaction_created
        )
        builder = self._register_optional_event(
            builder, "register_p2_im_message_reaction_deleted_v1", self._on_reaction_deleted
        )
        builder = self._register_optional_event(
            builder, "register_p2_im_message_message_read_v1", self._on_message_read
        )
        builder = self._register_optional_event(
            builder,
            "register_p2_im_chat_access_event_bot_p2p_chat_entered_v1",
            self._on_bot_p2p_chat_entered,
        )
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        builder = self._register_optional_event(
            builder,
            "register_p2_im_chat_member_bot_added_v1",
            lambda _: None,
        )
        builder = self._register_optional_event(
            builder,
            "register_p2_im_chat_member_bot_deleted_v1",
            lambda _: None,
        )
        event_handler = builder.build()

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._ws_client = lark.ws.Client(
            self.config.app_id,
            self.config.app_secret,
            domain=domain,
            event_handler=event_handler,
            log_level=lark.LogLevel.INFO,
        )

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        def run_ws():
            """执行 `run_ws`。

            【中文名称】run_ws

            【功能说明】
            这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - 无显式业务参数。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            import time

            import lark_oapi.ws.client as _lark_ws_client

            previous_loop = getattr(_lark_ws_client, "loop", None)
            ws_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(ws_loop)
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            _lark_ws_client.loop = ws_loop
            try:
                while self._running:
                    try:
                        self._ws_client.start()
                    except Exception as e:
                        self.logger.warning("WebSocket error: {}", e)
                    if self._running:
                        time.sleep(5)
            finally:
                if getattr(_lark_ws_client, "loop", None) is ws_loop:
                    _lark_ws_client.loop = previous_loop
                with suppress(Exception):
                    asyncio.set_event_loop(None)
                ws_loop.close()

        self._ws_thread = threading.Thread(target=run_ws, daemon=True)
        self._ws_thread.start()

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._bot_open_id = await asyncio.get_running_loop().run_in_executor(
            None, self._fetch_bot_open_id
        )
        if self._bot_open_id:
            self.logger.info("bot open_id: {}", self._bot_open_id)
        else:
            self.logger.warning("Could not fetch bot open_id; @mention matching may be inaccurate")

        self.logger.info("bot started with WebSocket long connection")
        self.logger.info("No public IP required - using WebSocket to receive events")

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False
        self.logger.info("bot stopped")

    def _fetch_bot_open_id(self) -> str | None:
        """执行 `_fetch_bot_open_id`。

        【中文名称】_fetch_bot_open_id

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            import lark_oapi as lark

            request = (
                lark.BaseRequest.builder()
                .http_method(lark.HttpMethod.GET)
                .uri("/open-apis/bot/v3/info")
                .token_types({lark.AccessTokenType.APP})
                .build()
            )
            response = self._client.request(request)
            if response.success():
                import json

                data = json.loads(response.raw.content)
                bot = (data.get("data") or data).get("bot") or data.get("bot") or {}
                return bot.get("open_id")
            self.logger.warning("Failed to get bot info: code={}, msg={}", response.code, response.msg)
            return None
        except Exception as e:
            self.logger.warning("Error fetching bot info: {}", e)
            return None

    @staticmethod
    def _resolve_mentions(text: str, mentions: list[MentionEvent] | None) -> str:
        """执行 `_resolve_mentions`。

        【中文名称】_resolve_mentions

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not mentions or not text:
            return text

        for mention in mentions:
            key = mention.key or None
            if not key:
                continue
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            pattern = rf"{re.escape(key)}(?![A-Za-z0-9_])"
            if not re.search(pattern, text):
                continue

            user_id_obj = mention.id or None
            if not user_id_obj:
                continue

            open_id = user_id_obj.open_id
            user_id = user_id_obj.user_id
            name = mention.name or key

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if open_id and user_id:
                replacement = f"@{name} ({open_id}, user id: {user_id})"
            elif open_id:
                replacement = f"@{name} ({open_id})"
            else:
                replacement = f"@{name}"

            text = re.sub(pattern, replacement, text)

        return text

    def _is_bot_mention_event(self, mention: Any) -> bool:
        """执行 `_is_bot_mention_event`。

        【中文名称】_is_bot_mention_event

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - mention: 调用方传入的 `mention` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        mid = getattr(mention, "id", None)
        if not mid:
            return False

        mention_open_id = getattr(mid, "open_id", None) or ""
        bot_open_id = getattr(self, "_bot_open_id", None) or ""
        if bot_open_id:
            return mention_open_id == bot_open_id

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        return not getattr(mid, "user_id", None) and mention_open_id.startswith("ou_")

    def _strip_leading_bot_mention(
        self, text: str, mentions: list[MentionEvent] | None
    ) -> str:
        """执行 `_strip_leading_bot_mention`。

        【中文名称】_strip_leading_bot_mention

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not mentions or not text:
            return text

        candidate = text.lstrip()
        for mention in mentions:
            key = getattr(mention, "key", None) or ""
            if not key or not re.match(rf"{re.escape(key)}(?![A-Za-z0-9_])", candidate):
                continue
            if not self._is_bot_mention_event(mention):
                continue

            stripped = candidate[len(key) :].strip()
            return stripped or text

        return text

    def _is_bot_mentioned(self, message: Any) -> bool:
        """执行 `_is_bot_mentioned`。

        【中文名称】_is_bot_mentioned

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        raw_content = message.content or ""
        if "@_all" in raw_content:
            return True

        for mention in getattr(message, "mentions", None) or []:
            if self._is_bot_mention_event(mention):
                return True
        return False

    def _is_group_message_for_bot(self, message: Any) -> bool:
        """执行 `_is_group_message_for_bot`。

        【中文名称】_is_group_message_for_bot

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self.config.group_policy == "open":
            return True
        return self._is_bot_mentioned(message)

    def _add_reaction_sync(self, message_id: str, emoji_type: str) -> str | None:
        """执行 `_add_reaction_sync`。

        【中文名称】_add_reaction_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - emoji_type: 调用方传入的 `emoji_type` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import (
            CreateMessageReactionRequest,
            CreateMessageReactionRequestBody,
            Emoji,
        )

        try:
            request = (
                CreateMessageReactionRequest.builder()
                .message_id(message_id)
                .request_body(
                    CreateMessageReactionRequestBody.builder()
                    .reaction_type(Emoji.builder().emoji_type(emoji_type).build())
                    .build()
                )
                .build()
            )

            response = self._client.im.v1.message_reaction.create(request)

            if not response.success():
                self.logger.warning(
                    "Failed to add reaction: code={}, msg={}", response.code, response.msg
                )
                return None
            else:
                self.logger.debug("Added {} reaction to message {}", emoji_type, message_id)
                return response.data.reaction_id if response.data else None
        except Exception as e:
            self.logger.warning("Error adding reaction: {}", e)
            return None

    async def _add_reaction(self, message_id: str, emoji_type: str = "THUMBSUP") -> str | None:
        """异步执行 `_add_reaction`。

        【中文名称】_add_reaction

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - emoji_type: 调用方传入的 `emoji_type` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._client:
            return None

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._add_reaction_sync, message_id, emoji_type)

    def _remove_reaction_sync(self, message_id: str, reaction_id: str) -> None:
        """执行 `_remove_reaction_sync`。

        【中文名称】_remove_reaction_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - reaction_id: 调用方传入的 `reaction_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import DeleteMessageReactionRequest

        try:
            request = (
                DeleteMessageReactionRequest.builder()
                .message_id(message_id)
                .reaction_id(reaction_id)
                .build()
            )

            response = self._client.im.v1.message_reaction.delete(request)
            if response.success():
                self.logger.debug("Removed reaction {} from message {}", reaction_id, message_id)
            else:
                self.logger.debug(
                    "Failed to remove reaction: code={}, msg={}", response.code, response.msg
                )
        except Exception as e:
            self.logger.debug("Error removing reaction: {}", e)

    async def _remove_reaction(self, message_id: str, reaction_id: str) -> None:
        """异步执行 `_remove_reaction`。

        【中文名称】_remove_reaction

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - reaction_id: 调用方传入的 `reaction_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._client or not reaction_id:
            return

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._remove_reaction_sync, message_id, reaction_id)

    def _on_background_task_done(self, task: asyncio.Task) -> None:
        """执行 `_on_background_task_done`。

        【中文名称】_on_background_task_done

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - task: 调用方传入的 `task` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception as exc:
            self.logger.warning("Background task failed: {}", exc)

    def _on_reaction_added(self, message_id: str, task: asyncio.Task) -> None:
        """执行 `_on_reaction_added`。

        【中文名称】_on_reaction_added

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - task: 调用方传入的 `task` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if task.cancelled():
            return
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        with suppress(Exception):
            reaction_id = task.result()
            if reaction_id:
                self._reaction_ids[message_id] = reaction_id
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if len(self._reaction_ids) > 500:
            self._reaction_ids.pop(next(iter(self._reaction_ids)))

    @staticmethod
    def _stream_key(chat_id: str, metadata: dict[str, Any] | None = None) -> str:
        """执行 `_stream_key`。

        【中文名称】_stream_key

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        meta = metadata or {}
        return meta.get("message_id") or chat_id

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _TABLE_RE = re.compile(
        r"((?:^[ \t]*\|.+\|[ \t]*\n)(?:^[ \t]*\|[-:\s|]+\|[ \t]*\n)(?:^[ \t]*\|.+\|[ \t]*\n?)+)",
        re.MULTILINE,
    )

    _HEADING_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)

    _CODE_BLOCK_RE = re.compile(r"(```[\s\S]*?```)", re.MULTILINE)

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _MD_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
    _MD_BOLD_UNDERSCORE_RE = re.compile(r"__(.+?)__")
    _MD_ITALIC_RE = re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)")
    _MD_STRIKE_RE = re.compile(r"~~(.+?)~~")

    @classmethod
    def _strip_md_formatting(cls, text: str) -> str:
        """执行 `_strip_md_formatting`。

        【中文名称】_strip_md_formatting

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        text = cls._MD_BOLD_RE.sub(r"\1", text)
        text = cls._MD_BOLD_UNDERSCORE_RE.sub(r"\1", text)
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        text = cls._MD_ITALIC_RE.sub(r"\1", text)
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        text = cls._MD_STRIKE_RE.sub(r"\1", text)
        return text

    @classmethod
    def _parse_md_table(cls, table_text: str) -> dict | None:
        """执行 `_parse_md_table`。

        【中文名称】_parse_md_table

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - table_text: 调用方传入的 `table_text` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        lines = [_line.strip() for _line in table_text.strip().split("\n") if _line.strip()]
        if len(lines) < 3:
            return None

        def split(_line: str) -> list[str]:
            """执行 `split`。

            【中文名称】split

            【功能说明】
            这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - _line: 调用方传入的 `_line` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            return [c.strip() for c in _line.strip("|").split("|")]

        headers = [cls._strip_md_formatting(h) for h in split(lines[0])]
        rows = [[cls._strip_md_formatting(c) for c in split(_line)] for _line in lines[2:]]
        columns = [
            {"tag": "column", "name": f"c{i}", "display_name": h, "width": "auto"}
            for i, h in enumerate(headers)
        ]
        return {
            "tag": "table",
            "page_size": len(rows) + 1,
            "columns": columns,
            "rows": [
                {f"c{i}": r[i] if i < len(r) else "" for i in range(len(headers))} for r in rows
            ],
        }

    def _build_card_elements(self, content: str) -> list[dict]:
        """执行 `_build_card_elements`。

        【中文名称】_build_card_elements

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        elements, last_end = [], 0
        for m in self._TABLE_RE.finditer(content):
            before = content[last_end : m.start()]
            if before.strip():
                elements.extend(self._split_headings(before))
            elements.append(
                self._parse_md_table(m.group(1)) or {"tag": "markdown", "content": m.group(1)}
            )
            last_end = m.end()
        remaining = content[last_end:]
        if remaining.strip():
            elements.extend(self._split_headings(remaining))
        return elements or [{"tag": "markdown", "content": content}]

    @staticmethod
    def _split_elements_by_table_limit(
        elements: list[dict], max_tables: int = 1
    ) -> list[list[dict]]:
        """执行 `_split_elements_by_table_limit`。

        【中文名称】_split_elements_by_table_limit

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - elements: 调用方传入的 `elements` 数据；具体类型以函数签名为准。
        - max_tables: 调用方传入的 `max_tables` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not elements:
            return [[]]
        groups: list[list[dict]] = []
        current: list[dict] = []
        table_count = 0
        for el in elements:
            if el.get("tag") == "table":
                if table_count >= max_tables:
                    if current:
                        groups.append(current)
                    current = []
                    table_count = 0
                current.append(el)
                table_count += 1
            else:
                current.append(el)
        if current:
            groups.append(current)
        return groups or [[]]

    def _split_headings(self, content: str) -> list[dict]:
        """执行 `_split_headings`。

        【中文名称】_split_headings

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        protected = content
        code_blocks = []
        for m in self._CODE_BLOCK_RE.finditer(content):
            code_blocks.append(m.group(1))
            protected = protected.replace(m.group(1), f"\x00CODE{len(code_blocks) - 1}\x00", 1)

        elements = []
        last_end = 0
        for m in self._HEADING_RE.finditer(protected):
            before = protected[last_end : m.start()].strip()
            if before:
                elements.append({"tag": "markdown", "content": before})
            text = self._strip_md_formatting(m.group(2).strip())
            display_text = f"**{text}**" if text else ""
            elements.append(
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": display_text,
                    },
                }
            )
            last_end = m.end()
        remaining = protected[last_end:].strip()
        if remaining:
            elements.append({"tag": "markdown", "content": remaining})

        for i, cb in enumerate(code_blocks):
            for el in elements:
                if el.get("tag") == "markdown":
                    el["content"] = el["content"].replace(f"\x00CODE{i}\x00", cb)

        return elements or [{"tag": "markdown", "content": content}]

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _COMPLEX_MD_RE = re.compile(
        r"```"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        r"|^\|.+\|.*\n\s*\|[-:\s|]+\|"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        r"|^#{1,6}\s+",  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        re.MULTILINE,
    )

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _SIMPLE_MD_RE = re.compile(
        r"\*\*.+?\*\*"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        r"|__.+?__"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        r"|(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)"  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        r"|~~.+?~~",  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        re.DOTALL,
    )

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _MD_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\)]+)\)")

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _LIST_RE = re.compile(r"^[\s]*[-*+]\s+", re.MULTILINE)

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _OLIST_RE = re.compile(r"^[\s]*\d+\.\s+", re.MULTILINE)

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _TEXT_MAX_LEN = 200

    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _POST_MAX_LEN = 2000

    @classmethod
    def _detect_msg_format(cls, content: str) -> str:
        """执行 `_detect_msg_format`。

        【中文名称】_detect_msg_format

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        stripped = content.strip()

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if cls._COMPLEX_MD_RE.search(stripped):
            return "interactive"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if len(stripped) > cls._POST_MAX_LEN:
            return "interactive"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if cls._SIMPLE_MD_RE.search(stripped):
            return "interactive"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if cls._LIST_RE.search(stripped) or cls._OLIST_RE.search(stripped):
            return "interactive"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if cls._MD_LINK_RE.search(stripped):
            return "post"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if len(stripped) <= cls._TEXT_MAX_LEN:
            return "text"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        return "post"

    @classmethod
    def _markdown_to_post(cls, content: str) -> str:
        """执行 `_markdown_to_post`。

        【中文名称】_markdown_to_post

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        lines = content.strip().split("\n")
        paragraphs: list[list[dict]] = []

        for line in lines:
            elements: list[dict] = []
            last_end = 0

            for m in cls._MD_LINK_RE.finditer(line):
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                before = line[last_end : m.start()]
                if before:
                    elements.append({"tag": "text", "text": before})
                elements.append(
                    {
                        "tag": "a",
                        "text": m.group(1),
                        "href": m.group(2),
                    }
                )
                last_end = m.end()

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            remaining = line[last_end:]
            if remaining:
                elements.append({"tag": "text", "text": remaining})

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not elements:
                elements.append({"tag": "text", "text": ""})

            paragraphs.append(elements)

        post_body = {
            "zh_cn": {
                "content": paragraphs,
            }
        }
        return json.dumps(post_body, ensure_ascii=False)

    _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp", ".ico", ".tiff", ".tif"}
    _AUDIO_EXTS = {".opus"}
    _VIDEO_EXTS = {".mp4", ".mov", ".avi"}
    _FILE_TYPE_MAP = {
        ".opus": "opus",
        ".mp4": "mp4",
        ".pdf": "pdf",
        ".doc": "doc",
        ".docx": "doc",
        ".xls": "xls",
        ".xlsx": "xls",
        ".ppt": "ppt",
        ".pptx": "ppt",
    }

    def _upload_image_sync(self, file_path: str) -> str | None:
        """执行 `_upload_image_sync`。

        【中文名称】_upload_image_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - file_path: 调用方传入的 `file_path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import CreateImageRequest, CreateImageRequestBody

        try:
            with open(file_path, "rb") as f:
                request = (
                    CreateImageRequest.builder()
                    .request_body(
                        CreateImageRequestBody.builder().image_type("message").image(f).build()
                    )
                    .build()
                )
                response = self._client.im.v1.image.create(request)
                if response.success():
                    image_key = response.data.image_key
                    self.logger.debug("Uploaded image {}: {}", os.path.basename(file_path), image_key)
                    return image_key
                else:
                    self.logger.error(
                        "Failed to upload image: code={}, msg={}", response.code, response.msg
                    )
                    return None
        except Exception:
            self.logger.exception("Error uploading image {}", file_path)
            return None

    def _upload_file_sync(self, file_path: str) -> str | None:
        """执行 `_upload_file_sync`。

        【中文名称】_upload_file_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - file_path: 调用方传入的 `file_path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody

        ext = os.path.splitext(file_path)[1].lower()
        file_type = self._FILE_TYPE_MAP.get(ext, "stream")
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, "rb") as f:
                request = (
                    CreateFileRequest.builder()
                    .request_body(
                        CreateFileRequestBody.builder()
                        .file_type(file_type)
                        .file_name(file_name)
                        .file(f)
                        .build()
                    )
                    .build()
                )
                response = self._client.im.v1.file.create(request)
                if response.success():
                    file_key = response.data.file_key
                    self.logger.debug("Uploaded file {}: {}", file_name, file_key)
                    return file_key
                else:
                    self.logger.error(
                        "Failed to upload file: code={}, msg={}", response.code, response.msg
                    )
                    return None
        except Exception:
            self.logger.exception("Error uploading file {}", file_path)
            return None

    def _download_image_sync(
        self, message_id: str, image_key: str
    ) -> tuple[bytes | None, str | None]:
        """执行 `_download_image_sync`。

        【中文名称】_download_image_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - image_key: 调用方传入的 `image_key` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import GetMessageResourceRequest

        try:
            request = (
                GetMessageResourceRequest.builder()
                .message_id(message_id)
                .file_key(image_key)
                .type("image")
                .build()
            )
            response = self._client.im.v1.message_resource.get(request)
            if response.success():
                file_data = response.file
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                if hasattr(file_data, "read"):
                    file_data = file_data.read()
                return file_data, response.file_name
            else:
                self.logger.error(
                    "Failed to download image: code={}, msg={}", response.code, response.msg
                )
                return None, None
        except Exception:
            self.logger.exception("Error downloading image {}", image_key)
            return None, None

    def _download_file_sync(
        self, message_id: str, file_key: str, resource_type: str = "file"
    ) -> tuple[bytes | None, str | None]:
        """执行 `_download_file_sync`。

        【中文名称】_download_file_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - file_key: 调用方传入的 `file_key` 数据；具体类型以函数签名为准。
        - resource_type: 调用方传入的 `resource_type` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import GetMessageResourceRequest

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if resource_type in ("audio", "media"):
            resource_type = "file"

        try:
            request = (
                GetMessageResourceRequest.builder()
                .message_id(message_id)
                .file_key(file_key)
                .type(resource_type)
                .build()
            )
            response = self._client.im.v1.message_resource.get(request)
            if response.success():
                file_data = response.file
                if hasattr(file_data, "read"):
                    file_data = file_data.read()
                return file_data, response.file_name
            else:
                self.logger.error(
                    "Failed to download {}: code={}, msg={}",
                    resource_type,
                    response.code,
                    response.msg,
                )
                return None, None
        except Exception:
            self.logger.exception("Error downloading {} {}", resource_type, file_key)
            return None, None

    @staticmethod
    def _safe_media_filename(filename: str | None, fallback: str) -> str:
        """执行 `_safe_media_filename`。

        【中文名称】_safe_media_filename

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。
        - fallback: 调用方传入的 `fallback` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        candidate = filename or fallback
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        candidate = os.path.basename(candidate.replace("\\", "/"))
        candidate = safe_filename(candidate)
        if candidate in ("", ".", ".."):
            return safe_filename(fallback) or uuid.uuid4().hex
        return candidate

    async def _download_and_save_media(
        self, msg_type: str, content_json: dict, message_id: str | None = None
    ) -> tuple[str | None, str]:
        """异步执行 `_download_and_save_media`。

        【中文名称】_download_and_save_media

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg_type: 调用方传入的 `msg_type` 数据；具体类型以函数签名为准。
        - content_json: 调用方传入的 `content_json` 数据；具体类型以函数签名为准。
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        loop = asyncio.get_running_loop()
        media_dir = get_media_dir("feishu")

        data, filename = None, None
        fallback_filename = uuid.uuid4().hex

        if msg_type == "image":
            image_key = content_json.get("image_key")
            if image_key and message_id:
                fallback_filename = f"{image_key[:16]}.jpg"
                data, filename = await loop.run_in_executor(
                    None, self._download_image_sync, message_id, image_key
                )
                if not filename:
                    filename = fallback_filename

        elif msg_type in ("audio", "file", "media"):
            file_key = content_json.get("file_key")
            if not file_key:
                self.logger.warning("{} message missing file_key: {}", msg_type, content_json)
                return None, f"[{msg_type}: missing file_key]"
            if not message_id:
                self.logger.warning("{} message missing message_id", msg_type)
                return None, f"[{msg_type}: missing message_id]"

            fallback_filename = file_key[:16]
            data, filename = await loop.run_in_executor(
                None, self._download_file_sync, message_id, file_key, msg_type
            )

            if not data:
                self.logger.warning("{} download failed: file_key={}", msg_type, file_key)
                return None, f"[{msg_type}: download failed]"

            if not filename:
                filename = fallback_filename

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if msg_type == "audio":
                if not any(filename.endswith(ext) for ext in (".opus", ".ogg", ".oga")):
                    filename = f"{filename}.ogg"

        if data and filename:
            filename = self._safe_media_filename(filename, fallback_filename)
            file_path = media_dir / filename
            file_path.write_bytes(data)
            path_str = str(file_path)
            self.logger.debug("Downloaded {} to {}", msg_type, path_str)
            return path_str, f"[{msg_type}: {path_str}]"

        return None, f"[{msg_type}: download failed]"

    _REPLY_CONTEXT_MAX_LEN = 200

    def _get_message_content_sync(self, message_id: str) -> str | None:
        """执行 `_get_message_content_sync`。

        【中文名称】_get_message_content_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import GetMessageRequest

        try:
            request = GetMessageRequest.builder().message_id(message_id).build()
            response = self._client.im.v1.message.get(request)
            if not response.success():
                self.logger.debug(
                    "could not fetch parent message {}: code={}, msg={}",
                    message_id,
                    response.code,
                    response.msg,
                )
                return None
            items = getattr(response.data, "items", None)
            if not items:
                return None
            msg_obj = items[0]
            raw_content = getattr(msg_obj, "body", None)
            raw_content = getattr(raw_content, "content", None) if raw_content else None
            if not raw_content:
                return None
            try:
                content_json = json.loads(raw_content)
            except (json.JSONDecodeError, TypeError):
                return None
            msg_type = getattr(msg_obj, "msg_type", "")
            if msg_type == "text":
                text = content_json.get("text", "").strip()
            elif msg_type == "post":
                text, _ = _extract_post_content(content_json)
                text = text.strip()
            else:
                text = ""
            if not text:
                return None
            if len(text) > self._REPLY_CONTEXT_MAX_LEN:
                text = text[: self._REPLY_CONTEXT_MAX_LEN] + "..."
            return f"[Reply to: {text}]"
        except Exception as e:
            self.logger.debug("error fetching parent message {}: {}", message_id, e)
            return None

    def _reply_message_sync(self, parent_message_id: str, msg_type: str, content: str, *, reply_in_thread: bool = False) -> bool:
        """执行 `_reply_message_sync`。

        【中文名称】_reply_message_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - parent_message_id: 调用方传入的 `parent_message_id` 数据；具体类型以函数签名为准。
        - msg_type: 调用方传入的 `msg_type` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
        - reply_in_thread: 调用方传入的 `reply_in_thread` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import ReplyMessageRequest, ReplyMessageRequestBody

        try:
            body_builder = ReplyMessageRequestBody.builder().msg_type(msg_type).content(content)
            if reply_in_thread:
                body_builder = body_builder.reply_in_thread(True)
            request = (
                ReplyMessageRequest.builder()
                .message_id(parent_message_id)
                .request_body(body_builder.build())
                .build()
            )
            response = self._client.im.v1.message.reply(request)
            if not response.success():
                self.logger.error(
                    "Failed to reply to message {}: code={}, msg={}, log_id={}",
                    parent_message_id,
                    response.code,
                    response.msg,
                    response.get_log_id(),
                )
                return False
            self.logger.debug("reply sent to message {}", parent_message_id)
            return True
        except Exception:
            self.logger.exception("Error replying to message {}", parent_message_id)
            return False

    def _should_use_reply_in_thread(self, metadata: dict[str, Any]) -> bool:
        """执行 `_should_use_reply_in_thread`。

        【中文名称】_should_use_reply_in_thread

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return metadata.get("chat_type", "group") == "group" and self.config.reply_to_message

    def _thread_reply_target(self, metadata: dict[str, Any]) -> str | None:
        """执行 `_thread_reply_target`。

        【中文名称】_thread_reply_target

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if metadata.get("chat_type", "group") != "group":
            return None
        message_id = metadata.get("message_id")
        if not message_id:
            return None
        if metadata.get("thread_id") or self.config.reply_to_message:
            return message_id
        return None

    def _send_message_sync(
        self, receive_id_type: str, receive_id: str, msg_type: str, content: str
    ) -> str | None:
        """执行 `_send_message_sync`。

        【中文名称】_send_message_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - receive_id_type: 调用方传入的 `receive_id_type` 数据；具体类型以函数签名为准。
        - receive_id: 调用方传入的 `receive_id` 数据；具体类型以函数签名为准。
        - msg_type: 调用方传入的 `msg_type` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.im.v1 import CreateMessageRequest, CreateMessageRequestBody

        try:
            request = (
                CreateMessageRequest.builder()
                .receive_id_type(receive_id_type)
                .request_body(
                    CreateMessageRequestBody.builder()
                    .receive_id(receive_id)
                    .msg_type(msg_type)
                    .content(content)
                    .build()
                )
                .build()
            )
            response = self._client.im.v1.message.create(request)
            if not response.success():
                self.logger.error(
                    "Failed to send {} message: code={}, msg={}, log_id={}",
                    msg_type,
                    response.code,
                    response.msg,
                    response.get_log_id(),
                )
                return None
            msg_id = getattr(response.data, "message_id", None)
            self.logger.debug("{} message sent to {}: {}", msg_type, receive_id, msg_id)
            return msg_id
        except Exception:
            self.logger.exception("Error sending {} message", msg_type)
            return None

    def _create_streaming_card_sync(
        self,
        receive_id_type: str,
        chat_id: str,
        reply_message_id: str | None = None,
        *,
        reply_in_thread: bool = False,
    ) -> str | None:
        """执行 `_create_streaming_card_sync`。

        【中文名称】_create_streaming_card_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - receive_id_type: 调用方传入的 `receive_id_type` 数据；具体类型以函数签名为准。
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - reply_message_id: 调用方传入的 `reply_message_id` 数据；具体类型以函数签名为准。
        - reply_in_thread: 调用方传入的 `reply_in_thread` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.cardkit.v1 import CreateCardRequest, CreateCardRequestBody

        card_json = {
            "schema": "2.0",
            "config": {"wide_screen_mode": True, "update_multi": True, "streaming_mode": True},
            "body": {
                "elements": [{"tag": "markdown", "content": "", "element_id": _STREAM_ELEMENT_ID}]
            },
        }
        try:
            request = (
                CreateCardRequest.builder()
                .request_body(
                    CreateCardRequestBody.builder()
                    .type("card_json")
                    .data(json.dumps(card_json, ensure_ascii=False))
                    .build()
                )
                .build()
            )
            response = self._client.cardkit.v1.card.create(request)
            if not response.success():
                self.logger.warning(
                    "Failed to create streaming card: code={}, msg={}", response.code, response.msg
                )
                return None
            card_id = getattr(response.data, "card_id", None)
            if card_id:
                card_content = json.dumps(
                    {"type": "card", "data": {"card_id": card_id}}, ensure_ascii=False
                )
                if reply_message_id:
                    sent = self._reply_message_sync(
                        reply_message_id, "interactive", card_content,
                        reply_in_thread=reply_in_thread,
                    )
                else:
                    sent = self._send_message_sync(
                        receive_id_type, chat_id, "interactive", card_content,
                    ) is not None
                if sent:
                    return card_id
                self.logger.warning(
                    "Created streaming card {} but failed to send it to {}", card_id, chat_id
                )
            return None
        except Exception as e:
            self.logger.warning("Error creating streaming card: {}", e)
            return None

    def _stream_update_text_sync(self, card_id: str, content: str, sequence: int) -> bool:
        """执行 `_stream_update_text_sync`。

        【中文名称】_stream_update_text_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - card_id: 调用方传入的 `card_id` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
        - sequence: 调用方传入的 `sequence` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.cardkit.v1 import (
            ContentCardElementRequest,
            ContentCardElementRequestBody,
        )

        try:
            request = (
                ContentCardElementRequest.builder()
                .card_id(card_id)
                .element_id(_STREAM_ELEMENT_ID)
                .request_body(
                    ContentCardElementRequestBody.builder()
                    .content(content)
                    .sequence(sequence)
                    .build()
                )
                .build()
            )
            response = self._client.cardkit.v1.card_element.content(request)
            if not response.success():
                self.logger.warning(
                    "Failed to stream-update card {}: code={}, msg={}",
                    card_id,
                    response.code,
                    response.msg,
                )
                return False
            return True
        except Exception as e:
            self.logger.warning("Error stream-updating card {}: {}", card_id, e)
            return False

    def _close_streaming_mode_sync(self, card_id: str, sequence: int) -> bool:
        """执行 `_close_streaming_mode_sync`。

        【中文名称】_close_streaming_mode_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - card_id: 调用方传入的 `card_id` 数据；具体类型以函数签名为准。
        - sequence: 调用方传入的 `sequence` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from lark_oapi.api.cardkit.v1 import SettingsCardRequest, SettingsCardRequestBody

        settings_payload = json.dumps({"config": {"streaming_mode": False}}, ensure_ascii=False)
        try:
            request = (
                SettingsCardRequest.builder()
                .card_id(card_id)
                .request_body(
                    SettingsCardRequestBody.builder()
                    .settings(settings_payload)
                    .sequence(sequence)
                    .uuid(str(uuid.uuid4()))
                    .build()
                )
                .build()
            )
            response = self._client.cardkit.v1.card.settings(request)
            if not response.success():
                self.logger.warning(
                    "Failed to close streaming on card {}: code={}, msg={}",
                    card_id,
                    response.code,
                    response.msg,
                )
                return False
            return True
        except Exception as e:
            self.logger.warning("Error closing streaming on card {}: {}", card_id, e)
            return False

    async def send_delta(
        self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """异步执行 `send_delta`。

        【中文名称】send_delta

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - delta: 调用方传入的 `delta` 数据；具体类型以函数签名为准。
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._client:
            return
        meta = metadata or {}
        stream_key = self._stream_key(chat_id, meta)
        loop = asyncio.get_running_loop()
        rid_type = "chat_id" if chat_id.startswith("oc_") else "open_id"

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if meta.get("_stream_end"):
            message_id = meta.get("message_id")
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if message_id and not meta.get("_resuming"):
                reaction_id = self._reaction_ids.pop(message_id, None)
                if reaction_id:
                    await self._remove_reaction(message_id, reaction_id)
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                if self.config.done_emoji:
                    await self._add_reaction(message_id, self.config.done_emoji)

            buf = self._stream_bufs.pop(stream_key, None)
            if not buf or not buf.text:
                return
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if buf.card_id:
                buf.sequence += 1
                ok = await loop.run_in_executor(
                    None,
                    self._stream_update_text_sync,
                    buf.card_id,
                    buf.text,
                    buf.sequence,
                )
                if ok:
                    buf.sequence += 1
                    await loop.run_in_executor(
                        None,
                        self._close_streaming_mode_sync,
                        buf.card_id,
                        buf.sequence,
                    )
                    return
                self.logger.warning(
                    "Streaming card {} final update failed, falling back to regular card",
                    buf.card_id,
                )
            for chunk in self._split_elements_by_table_limit(
                self._build_card_elements(buf.text)
            ):
                card = json.dumps(
                    {"config": {"wide_screen_mode": True}, "elements": chunk},
                    ensure_ascii=False,
                )
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                fallback_msg_id = self._thread_reply_target(meta)
                if fallback_msg_id:
                    await loop.run_in_executor(
                        None, lambda: self._reply_message_sync(
                            fallback_msg_id, "interactive", card,
                            reply_in_thread=self._should_use_reply_in_thread(meta),
                        ),
                    )
                else:
                    await loop.run_in_executor(
                        None, self._send_message_sync, rid_type, chat_id, "interactive", card
                    )
            return

        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        buf = self._stream_bufs.get(stream_key)
        if buf is None:
            buf = _FeishuStreamBuf()
            self._stream_bufs[stream_key] = buf
        buf.text += delta
        if not buf.text.strip():
            return

        now = time.monotonic()
        if buf.card_id is None:
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            use_reply_in_thread = self._should_use_reply_in_thread(meta)
            reply_msg_id = self._thread_reply_target(meta)
            card_id = await loop.run_in_executor(
                None,
                lambda: self._create_streaming_card_sync(
                    rid_type,
                    chat_id,
                    reply_msg_id,
                    reply_in_thread=use_reply_in_thread,
                ),
            )
            if card_id:
                buf.card_id = card_id
                buf.sequence = 1
                await loop.run_in_executor(
                    None, self._stream_update_text_sync, card_id, buf.text, 1
                )
                buf.last_edit = now
        elif (now - buf.last_edit) >= self._STREAM_EDIT_INTERVAL:
            buf.sequence += 1
            await loop.run_in_executor(
                None, self._stream_update_text_sync, buf.card_id, buf.text, buf.sequence
            )
            buf.last_edit = now

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._client:
            self.logger.warning("client not initialized")
            return

        try:
            receive_id_type = "chat_id" if msg.chat_id.startswith("oc_") else "open_id"
            loop = asyncio.get_running_loop()

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if msg.metadata.get("_tool_hint"):
                hint = (msg.content or "").strip()
                if not hint:
                    return
                buf = self._stream_bufs.get(self._stream_key(msg.chat_id, msg.metadata))
                if buf and buf.card_id:
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    await self.send_delta(
                        msg.chat_id,
                        "\n\n" + self._format_tool_hint_delta(hint) + "\n\n",
                    )
                    return
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                card = json.dumps(
                    {"config": {"wide_screen_mode": True}, "elements": [
                        {"tag": "markdown", "content": self._format_tool_hint_delta(hint)},
                    ]},
                    ensure_ascii=False,
                )
                _th_msg_id = self._thread_reply_target(msg.metadata)
                if _th_msg_id:
                    await loop.run_in_executor(
                        None, lambda: self._reply_message_sync(
                            _th_msg_id, "interactive", card,
                            reply_in_thread=self._should_use_reply_in_thread(msg.metadata),
                        ),
                    )
                else:
                    await loop.run_in_executor(
                        None, self._send_message_sync, receive_id_type, msg.chat_id, "interactive", card
                    )
                return

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            reply_message_id: str | None = None
            _msg_id = msg.metadata.get("message_id")
            has_thread_id = msg.metadata.get("thread_id")
            if self.config.reply_to_message and not msg.metadata.get("_progress", False):
                reply_message_id = _msg_id
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            elif has_thread_id:
                reply_message_id = _msg_id

            first_send = True  # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

            def _do_send(m_type: str, content: str) -> None:
                """执行 `_do_send`。

                【中文名称】_do_send

                【功能说明】
                这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
                阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

                【参数说明】
                - m_type: 调用方传入的 `m_type` 数据；具体类型以函数签名为准。
                - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

                【返回值】
                - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
                nonlocal first_send
                if reply_message_id:
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    if has_thread_id:
                        ok = self._reply_message_sync(
                            reply_message_id, m_type, content,
                            reply_in_thread=self._should_use_reply_in_thread(msg.metadata),
                        )
                        if ok:
                            return
                    elif first_send:
                        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        first_send = False
                        ok = self._reply_message_sync(
                            reply_message_id, m_type, content,
                            reply_in_thread=self._should_use_reply_in_thread(msg.metadata),
                        )
                        if ok:
                            return
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                self._send_message_sync(receive_id_type, msg.chat_id, m_type, content)

            for file_path in msg.media:
                if not os.path.isfile(file_path):
                    self.logger.warning("Media file not found: {}", file_path)
                    continue
                ext = os.path.splitext(file_path)[1].lower()
                if ext in self._IMAGE_EXTS:
                    key = await loop.run_in_executor(None, self._upload_image_sync, file_path)
                    if key:
                        await loop.run_in_executor(
                            None,
                            _do_send,
                            "image",
                            json.dumps({"image_key": key}, ensure_ascii=False),
                        )
                else:
                    key = await loop.run_in_executor(None, self._upload_file_sync, file_path)
                    if key:
                        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        if ext in self._AUDIO_EXTS:
                            media_type = "audio"
                        elif ext in self._VIDEO_EXTS:
                            media_type = "media"
                        else:
                            media_type = "file"
                        await loop.run_in_executor(
                            None,
                            _do_send,
                            media_type,
                            json.dumps({"file_key": key}, ensure_ascii=False),
                        )

            if msg.content and msg.content.strip():
                fmt = self._detect_msg_format(msg.content)

                if fmt == "text":
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    text_body = json.dumps({"text": msg.content.strip()}, ensure_ascii=False)
                    await loop.run_in_executor(None, _do_send, "text", text_body)

                elif fmt == "post":
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    post_body = self._markdown_to_post(msg.content)
                    await loop.run_in_executor(None, _do_send, "post", post_body)

                else:
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    elements = self._build_card_elements(msg.content)
                    for chunk in self._split_elements_by_table_limit(elements):
                        card = {"config": {"wide_screen_mode": True}, "elements": chunk}
                        await loop.run_in_executor(
                            None,
                            _do_send,
                            "interactive",
                            json.dumps(card, ensure_ascii=False),
                        )

        except Exception:
            self.logger.exception("Error sending message")
            raise

    def _on_message_sync(self, data: Any) -> None:
        """执行 `_on_message_sync`。

        【中文名称】_on_message_sync

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._on_message(data), self._loop)

    async def _on_message(self, data: P2ImMessageReceiveV1) -> None:
        """异步执行 `_on_message`。

        【中文名称】_on_message

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            event = data.event
            message = event.message
            sender = event.sender

            self.logger.debug("raw message: {}", message.content)
            self.logger.debug("mentions: {}", getattr(message, "mentions", None))

            message_id = message.message_id

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if sender.sender_type == "bot":
                return

            sender_id = sender.sender_id.open_id if sender.sender_id else "unknown"
            chat_id = message.chat_id
            chat_type = message.chat_type
            msg_type = message.message_type

            if chat_type == "group" and not self._is_group_message_for_bot(message):
                self.logger.debug("skipping group message (not mentioned)")
                return

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if message_id in self._processed_message_ids:
                return
            self._processed_message_ids[message_id] = None

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            while len(self._processed_message_ids) > 1000:
                self._processed_message_ids.popitem(last=False)

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not self.is_allowed(sender_id):
                if chat_type == "p2p":
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    await self._handle_message(
                        sender_id=sender_id,
                        chat_id=sender_id,
                        content="",
                        is_dm=True,
                    )
                return

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            task = asyncio.create_task(
                self._add_reaction(message_id, self.config.react_emoji)
            )
            self._background_tasks.add(task)
            task.add_done_callback(self._on_background_task_done)
            task.add_done_callback(lambda t: self._on_reaction_added(message_id, t))

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            content_parts = []
            media_paths = []

            try:
                content_json = json.loads(message.content) if message.content else {}
            except json.JSONDecodeError:
                content_json = {}

            if msg_type == "text":
                text = content_json.get("text", "")
                if text:
                    mentions = getattr(message, "mentions", None)
                    text = self._strip_leading_bot_mention(text, mentions)
                    text = self._resolve_mentions(text, mentions)
                    content_parts.append(text)

            elif msg_type == "post":
                text, image_keys = _extract_post_content(content_json)
                if text:
                    content_parts.append(text)
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                for img_key in image_keys:
                    file_path, content_text = await self._download_and_save_media(
                        "image", {"image_key": img_key}, message_id
                    )
                    if file_path:
                        media_paths.append(file_path)
                    content_parts.append(content_text)

            elif msg_type in ("image", "audio", "file", "media"):
                file_path, content_text = await self._download_and_save_media(
                    msg_type, content_json, message_id
                )
                if file_path:
                    media_paths.append(file_path)

                if msg_type == "audio" and file_path:
                    transcription = await self.transcribe_audio(file_path)
                    if transcription:
                        content_text = f"[transcription: {transcription}]"

                content_parts.append(content_text)

            elif msg_type in (
                "share_chat",
                "share_user",
                "interactive",
                "share_calendar_event",
                "system",
                "merge_forward",
            ):
                # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                text = _extract_share_card_content(content_json, msg_type)
                if text:
                    content_parts.append(text)

            else:
                content_parts.append(MSG_TYPE_MAP.get(msg_type, f"[{msg_type}]"))

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            parent_id = getattr(message, "parent_id", None) or None
            root_id = getattr(message, "root_id", None) or None
            thread_id = getattr(message, "thread_id", None) or None

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if parent_id and self._client:
                loop = asyncio.get_running_loop()
                reply_ctx = await loop.run_in_executor(
                    None, self._get_message_content_sync, parent_id
                )
                if reply_ctx:
                    content_parts.insert(0, reply_ctx)

            content = "\n".join(content_parts) if content_parts else ""

            if not content and not media_paths:
                return

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if chat_type == "group":
                if self.config.topic_isolation:
                    session_key = f"feishu:{chat_id}:{root_id or message_id}"
                else:
                    session_key = f"feishu:{chat_id}"
            else:
                session_key = None

            # 说明：这里处理 飞书渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            reply_to = chat_id if chat_type == "group" else sender_id
            await self._handle_message(
                sender_id=sender_id,
                chat_id=reply_to,
                content=content,
                media=media_paths,
                metadata={
                    "message_id": message_id,
                    "chat_type": chat_type,
                    "msg_type": msg_type,
                    "parent_id": parent_id,
                    "root_id": root_id,
                    "thread_id": thread_id,
                },
                session_key=session_key,
                is_dm=chat_type == "p2p",
            )

        except Exception:
            self.logger.exception("Error processing message")

    def _on_reaction_created(self, data: Any) -> None:
        """执行 `_on_reaction_created`。

        【中文名称】_on_reaction_created

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        pass

    def _on_reaction_deleted(self, data: Any) -> None:
        """执行 `_on_reaction_deleted`。

        【中文名称】_on_reaction_deleted

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        pass

    def _on_message_read(self, data: Any) -> None:
        """执行 `_on_message_read`。

        【中文名称】_on_message_read

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        pass

    def _on_bot_p2p_chat_entered(self, data: Any) -> None:
        """执行 `_on_bot_p2p_chat_entered`。

        【中文名称】_on_bot_p2p_chat_entered

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self.logger.debug("Bot entered p2p chat (user opened chat window)")
        pass

    @staticmethod
    def _format_tool_hint_lines(tool_hint: str) -> str:
        """执行 `_format_tool_hint_lines`。

        【中文名称】_format_tool_hint_lines

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - tool_hint: 调用方传入的 `tool_hint` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        parts: list[str] = []
        buf: list[str] = []
        depth = 0
        in_string = False
        quote_char = ""
        escaped = False

        for i, ch in enumerate(tool_hint):
            buf.append(ch)

            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == quote_char:
                    in_string = False
                continue

            if ch in {'"', "'"}:
                in_string = True
                quote_char = ch
                continue

            if ch == "(":
                depth += 1
                continue

            if ch == ")" and depth > 0:
                depth -= 1
                continue

            if ch == "," and depth == 0:
                next_char = tool_hint[i + 1] if i + 1 < len(tool_hint) else ""
                if next_char == " ":
                    parts.append("".join(buf).rstrip())
                    buf = []

        if buf:
            parts.append("".join(buf).strip())

        return "\n".join(part for part in parts if part)

    def _format_tool_hint_delta(self, tool_hint: str) -> str:
        """执行 `_format_tool_hint_delta`。

        【中文名称】_format_tool_hint_delta

        【功能说明】
        这是 飞书渠道适配器 中的一个步骤函数，用来支撑：负责接入飞书开放平台事件、下载媒体、处理卡片交互和流式更新，并把飞书消息转换成 nanobot 的统一消息格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - tool_hint: 调用方传入的 `tool_hint` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        lines = self.__class__._format_tool_hint_lines(tool_hint).split("\n")
        return "\n".join(
            f"{self.config.tool_hint_prefix} {ln}" for ln in lines if ln.strip()
        )
