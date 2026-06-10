"""Telegram 渠道适配器。

【中文名称】Telegram 渠道适配器

【功能说明】
负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import asyncio
import re
import time
import unicodedata
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import Field, field_validator, model_validator
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReactionTypeEmoji,
    ReplyParameters,
    Update,
)
from telegram.error import BadRequest, NetworkError, TimedOut
from telegram.ext import Application, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from telegram.request import HTTPXRequest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.command.builtin import build_help_text
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.security.network import validate_url_target
from nanobot.utils.helpers import split_message

TELEGRAM_MAX_MESSAGE_LEN = 4000  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
TELEGRAM_HTML_MAX_LEN = 4096
TELEGRAM_REPLY_CONTEXT_MAX_LEN = TELEGRAM_MAX_MESSAGE_LEN  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


def _escape_telegram_html(text: str) -> str:
    """执行 `_escape_telegram_html`。

    【中文名称】_escape_telegram_html

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _tool_hint_to_telegram_blockquote(text: str) -> str:
    """执行 `_tool_hint_to_telegram_blockquote`。

    【中文名称】_tool_hint_to_telegram_blockquote

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return f"<blockquote expandable>{_escape_telegram_html(text)}</blockquote>" if text else ""


def _strip_md(s: str) -> str:
    """执行 `_strip_md`。

    【中文名称】_strip_md

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    s = re.sub(r'\*\*(.+?)\*\*', r'\1', s)
    s = re.sub(r'__(.+?)__', r'\1', s)
    s = re.sub(r'~~(.+?)~~', r'\1', s)
    s = re.sub(r'`([^`]+)`', r'\1', s)
    return s.strip()


def _strip_md_block(text: str) -> str:
    """执行 `_strip_md_block`。

    【中文名称】_strip_md_block

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'```[\w]*\n?([\s\S]*?)```', r'\1', text)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^#{1,6}\s+(.+)$', r'\1', text, flags=re.MULTILINE)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^>\s*(.*)$', r'\1', text, flags=re.MULTILINE)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    text = re.sub(r'__(.+?)__', r'\1', text)
    text = re.sub(r'(?<![a-zA-Z0-9])_([^_]+)_(?![a-zA-Z0-9])', r'\1', text)
    text = re.sub(r'~~(.+?)~~', r'\1', text)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'`([^`]+)`', r'\1', text)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^[-*]\s+', '• ', text, flags=re.MULTILINE)
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^(\d+)\.\s+', r'\1. ', text, flags=re.MULTILINE)
    return text


def _render_table_box(table_lines: list[str]) -> str:
    """执行 `_render_table_box`。

    【中文名称】_render_table_box

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - table_lines: 调用方传入的 `table_lines` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    def dw(s: str) -> int:
        """执行 `dw`。

        【中文名称】dw

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)

    rows: list[list[str]] = []
    has_sep = False
    for line in table_lines:
        cells = [_strip_md(c) for c in line.strip().strip('|').split('|')]
        if all(re.match(r'^:?-+:?$', c) for c in cells if c):
            has_sep = True
            continue
        rows.append(cells)
    if not rows or not has_sep:
        return '\n'.join(table_lines)

    ncols = max(len(r) for r in rows)
    for r in rows:
        r.extend([''] * (ncols - len(r)))
    widths = [max(dw(r[c]) for r in rows) for c in range(ncols)]

    def dr(cells: list[str]) -> str:
        """执行 `dr`。

        【中文名称】dr

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - cells: 调用方传入的 `cells` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return '  '.join(f'{c}{" " * (w - dw(c))}' for c, w in zip(cells, widths))

    out = [dr(rows[0])]
    out.append('  '.join('─' * w for w in widths))
    for row in rows[1:]:
        out.append(dr(row))
    return '\n'.join(out)


def _markdown_to_telegram_html(text: str) -> str:
    """执行 `_markdown_to_telegram_html`。

    【中文名称】_markdown_to_telegram_html

    【功能说明】
    这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not text:
        return ""

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    code_blocks: list[str] = []
    def save_code_block(m: re.Match) -> str:
        """执行 `save_code_block`。

        【中文名称】save_code_block

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - m: 调用方传入的 `m` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        code_blocks.append(m.group(1))
        return f"\x00CB{len(code_blocks) - 1}\x00"

    text = re.sub(r'```[\w]*\n?([\s\S]*?)```', save_code_block, text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    lines = text.split('\n')
    rebuilt: list[str] = []
    li = 0
    while li < len(lines):
        if re.match(r'^\s*\|.+\|', lines[li]):
            tbl: list[str] = []
            while li < len(lines) and re.match(r'^\s*\|.+\|', lines[li]):
                tbl.append(lines[li])
                li += 1
            box = _render_table_box(tbl)
            if box != '\n'.join(tbl):
                code_blocks.append(box)
                rebuilt.append(f"\x00CB{len(code_blocks) - 1}\x00")
            else:
                rebuilt.extend(tbl)
        else:
            rebuilt.append(lines[li])
            li += 1
    text = '\n'.join(rebuilt)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    inline_codes: list[str] = []
    def save_inline_code(m: re.Match) -> str:
        """执行 `save_inline_code`。

        【中文名称】save_inline_code

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - m: 调用方传入的 `m` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        inline_codes.append(m.group(1))
        return f"\x00IC{len(inline_codes) - 1}\x00"

    text = re.sub(r'`([^`]+)`', save_inline_code, text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^#{1,6}\s+(.+)$', r'⟪B⟫\1⟪/B⟫', text, flags=re.MULTILINE)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^>\s*(.*)$', r'\1', text, flags=re.MULTILINE)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = _escape_telegram_html(text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'__(.+?)__', r'<b>\1</b>', text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'(?<![a-zA-Z0-9])_([^_]+)_(?![a-zA-Z0-9])', r'<i>\1</i>', text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^[-*]\s+', '• ', text, flags=re.MULTILINE)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r'^(\d+)\.\s+', r'\1. ', text, flags=re.MULTILINE)

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    for i, code in enumerate(inline_codes):
        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        escaped = _escape_telegram_html(code)
        text = text.replace(f"\x00IC{i}\x00", f"<code>{escaped}</code>")

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    for i, code in enumerate(code_blocks):
        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        escaped = _escape_telegram_html(code)
        text = text.replace(f"\x00CB{i}\x00", f"<pre><code>{escaped}</code></pre>")

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    text = text.replace('⟪B⟫', '<b>').replace('⟪/B⟫', '</b>')

    return text


_SEND_MAX_RETRIES = 3
_SEND_RETRY_BASE_DELAY = 0.5  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
_STREAM_EDIT_INTERVAL_DEFAULT = 0.6  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


@dataclass
class _StreamBuf:
    """_StreamBuf 类。

    【中文名称】_StreamBuf

    【功能说明】
    这是 Telegram 渠道适配器 中的核心数据结构或服务类。负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""
    text: str = ""
    message_id: int | None = None
    last_edit: float = 0.0
    stream_id: str | None = None


@dataclass
class _QueuedTelegramUpdate:
    """_QueuedTelegramUpdate 类。

    【中文名称】_QueuedTelegramUpdate

    【功能说明】
    这是 Telegram 渠道适配器 中的核心数据结构或服务类。负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    kind: Literal["command", "message"]
    update: Update
    context: Any
    sort_key: tuple[int, int]


class TelegramConfig(Base):
    """TelegramConfig 类。

    【中文名称】TelegramConfig

    【功能说明】
    这是 Telegram 渠道适配器 中的核心数据结构或服务类。负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    token: str = ""
    mode: Literal["polling", "webhook"] = "polling"
    allow_from: list[str] = Field(default_factory=list)
    proxy: str | None = None
    reply_to_message: bool = False
    react_emoji: str = "👀"
    group_policy: Literal["open", "mention"] = "mention"
    connection_pool_size: int = 32
    pool_timeout: float = 5.0
    streaming: bool = True
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    inline_keyboards: bool = False
    stream_edit_interval: float = Field(default=_STREAM_EDIT_INTERVAL_DEFAULT, ge=0.1)
    webhook_url: str = ""
    webhook_listen_host: str = "127.0.0.1"
    webhook_listen_port: int = Field(default=8081, ge=1, le=65535)
    webhook_path: str = "/telegram"
    webhook_secret_token: str = ""
    webhook_max_connections: int = Field(default=4, ge=1, le=100)

    @field_validator("webhook_path")
    @classmethod
    def webhook_path_must_start_with_slash(cls, value: str) -> str:
        """执行 `webhook_path_must_start_with_slash`。

        【中文名称】webhook_path_must_start_with_slash

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        value = value.strip() or "/telegram"
        if not value.startswith("/"):
            raise ValueError('webhook_path must start with "/"')
        return value

    @model_validator(mode="after")
    def validate_webhook_config(self) -> "TelegramConfig":
        """执行 `validate_webhook_config`。

        【中文名称】validate_webhook_config

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if self.mode != "webhook":
            return self

        url = self.webhook_url.strip()
        if not url:
            raise ValueError("webhook_url is required when Telegram mode is webhook")
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("webhook_url must be a public HTTPS URL")
        secret = self.webhook_secret_token.strip()
        if not secret:
            raise ValueError("webhook_secret_token is required when Telegram mode is webhook")
        if len(secret) > 256 or re.match(r"^[A-Za-z0-9_-]+$", secret) is None:
            raise ValueError(
                "webhook_secret_token must be 1-256 characters using only A-Z, a-z, 0-9, _ and -"
            )
        return self


class TelegramChannel(BaseChannel):
    """TelegramChannel 类。

    【中文名称】TelegramChannel

    【功能说明】
    这是 Telegram 渠道适配器 中的核心数据结构或服务类。负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "telegram"
    display_name = "Telegram"

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    BOT_COMMANDS = [
        BotCommand("start", "Start the bot"),
        BotCommand("new", "Start a new conversation"),
        BotCommand("stop", "Stop the current task"),
        BotCommand("restart", "Restart the bot"),
        BotCommand("status", "Show bot status"),
        BotCommand("history", "Show recent conversation messages"),
        BotCommand("goal", "Start a sustained objective (long-running task)"),
        BotCommand("pairing", "Manage DM pairing (approve/deny/list)"),
        BotCommand("model", "Switch runtime model preset"),
        BotCommand("skill", "List enabled skills"),
        BotCommand("dream", "Run Dream memory consolidation now"),
        BotCommand("dream_log", "Show the latest Dream memory change"),
        BotCommand("dream_restore", "Restore Dream memory to an earlier version"),
        BotCommand("help", "Show available commands"),
    ]

    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    TELEGRAM_BUS_SLASH_COMMAND_RE = re.compile(
        r"^/(?:new|stop|restart|status|dream|history|goal|pairing|model|skill)(?:@\w+)?(?:\s+.*)?$"
    )

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return TelegramConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = TelegramConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: TelegramConfig = config
        self._app: Application | None = None
        self._chat_ids: dict[str, int] = {}  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._typing_tasks: dict[str, asyncio.Task] = {}  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._media_group_buffers: dict[str, dict] = {}
        self._media_group_tasks: dict[str, asyncio.Task] = {}
        self._message_threads: dict[tuple[str, int], int] = {}
        self._bot_user_id: int | None = None
        self._bot_username: str | None = None
        self._stream_bufs: dict[str, _StreamBuf] = {}  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._inbound_buffers: dict[str, list[_QueuedTelegramUpdate]] = {}
        self._inbound_workers: dict[str, asyncio.Task] = {}

    def is_allowed(self, sender_id: str) -> bool:
        """执行 `is_allowed`。

        【中文名称】is_allowed

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if super().is_allowed(sender_id):
            return True

        allow_list = getattr(self.config, "allow_from", [])
        if not allow_list or "*" in allow_list:
            return False

        sender_str = str(sender_id)
        if sender_str.count("|") != 1:
            return False

        sid, username = sender_str.split("|", 1)
        if not sid.isdigit() or not username:
            return False

        return sid in allow_list or username in allow_list

    @staticmethod
    def _normalize_telegram_command(content: str) -> str:
        """执行 `_normalize_telegram_command`。

        【中文名称】_normalize_telegram_command

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not content.startswith("/"):
            return content
        if content == "/dream_log" or content.startswith("/dream_log "):
            return content.replace("/dream_log", "/dream-log", 1)
        if content == "/dream_restore" or content.startswith("/dream_restore "):
            return content.replace("/dream_restore", "/dream-restore", 1)
        return content

    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.config.token:
            self.logger.error("bot token not configured")
            return

        self._running = True

        proxy = self.config.proxy or None

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        api_request = HTTPXRequest(
            connection_pool_size=self.config.connection_pool_size,
            pool_timeout=self.config.pool_timeout,
            connect_timeout=30.0,
            read_timeout=30.0,
            proxy=proxy,
        )
        poll_request = HTTPXRequest(
            connection_pool_size=4,
            pool_timeout=self.config.pool_timeout,
            connect_timeout=30.0,
            read_timeout=30.0,
            proxy=proxy,
        )
        builder = (
            Application.builder()
            .token(self.config.token)
            .request(api_request)
            .get_updates_request(poll_request)
        )
        self._app = builder.build()
        self._app.add_error_handler(self._on_error)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._app.add_handler(MessageHandler(filters.Regex(r"^/start(?:@\w+)?$"), self._on_start))
        self._app.add_handler(
            MessageHandler(
                filters.Regex(TelegramChannel.TELEGRAM_BUS_SLASH_COMMAND_RE),
                self._forward_command,
            )
        )
        self._app.add_handler(
            MessageHandler(
                filters.Regex(r"^/(dream-log|dream_log|dream-restore|dream_restore)(?:@\w+)?(?:\s+.*)?$"),
                self._forward_command,
            )
        )
        self._app.add_handler(MessageHandler(filters.Regex(r"^/help(?:@\w+)?$"), self._on_help))

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._app.add_handler(
            MessageHandler(
                (filters.TEXT | filters.PHOTO | filters.VIDEO | filters.VIDEO_NOTE
                 | filters.ANIMATION | filters.VOICE | filters.AUDIO
                 | filters.Document.ALL | filters.LOCATION)
                & ~filters.COMMAND,
                self._on_message
            )
        )

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if self.config.inline_keyboards:
            self._app.add_handler(CallbackQueryHandler(self._on_callback_query))
            allowed_updates = ["message", "callback_query"]
            self.logger.debug("inline keyboards enabled")
        else:
            allowed_updates = ["message"]

        if self.config.mode == "webhook":
            self.logger.info("Starting bot (webhook mode)...")
        else:
            self.logger.info("Starting bot (polling mode)...")

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        await self._app.initialize()
        await self._app.start()

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        bot_info = await self._app.bot.get_me()
        self._bot_user_id = getattr(bot_info, "id", None)
        self._bot_username = getattr(bot_info, "username", None)
        self.logger.info("bot @{} connected", bot_info.username)

        try:
            await self._app.bot.set_my_commands(self.BOT_COMMANDS)
            self.logger.debug("bot commands registered")
        except Exception as e:
            self.logger.warning("Failed to register bot commands: {}", e)

        if self.config.mode == "webhook":
            # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            await self._app.updater.start_webhook(
                listen=self.config.webhook_listen_host,
                port=self.config.webhook_listen_port,
                url_path=self.config.webhook_path.lstrip("/"),
                webhook_url=self.config.webhook_url.strip(),
                allowed_updates=allowed_updates,
                drop_pending_updates=False,
                secret_token=self.config.webhook_secret_token.strip(),
                max_connections=self.config.webhook_max_connections,
            )
        else:
            # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            await self._app.updater.start_polling(
                allowed_updates=allowed_updates,
                drop_pending_updates=False,  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                error_callback=self._on_polling_error,
            )

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        for chat_id in list(self._typing_tasks):
            self._stop_typing(chat_id)

        for task in self._media_group_tasks.values():
            task.cancel()
        self._media_group_tasks.clear()
        self._media_group_buffers.clear()

        for task in self._inbound_workers.values():
            task.cancel()
        self._inbound_workers.clear()
        self._inbound_buffers.clear()

        if self._app:
            self.logger.info("Stopping bot...")
            await self._app.updater.stop()
            await self._app.stop()
            await self._app.shutdown()
            self._app = None

    @staticmethod
    def _get_media_type(path: str) -> str:
        """执行 `_get_media_type`。

        【中文名称】_get_media_type

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        if ext in ("jpg", "jpeg", "png", "gif", "webp"):
            return "photo"
        if ext in ("mp4", "mov", "avi", "mkv", "webm", "3gp"):
            return "video"
        if ext == "ogg":
            return "voice"
        if ext in ("mp3", "m4a", "wav", "aac"):
            return "audio"
        return "document"

    @staticmethod
    def _is_remote_media_url(path: str) -> bool:
        """执行 `_is_remote_media_url`。

        【中文名称】_is_remote_media_url

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return path.startswith(("http://", "https://"))

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._app:
            self.logger.warning("bot not running")
            return

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if not msg.metadata.get("_progress", False):
            self._stop_typing(msg.chat_id)
            if reply_to_message_id := msg.metadata.get("message_id"):
                with suppress(ValueError):
                    await self._remove_reaction(msg.chat_id, int(reply_to_message_id))

        try:
            chat_id = int(msg.chat_id)
        except ValueError:
            self.logger.exception("Invalid chat_id: {}", msg.chat_id)
            return
        reply_to_message_id = msg.metadata.get("message_id")
        message_thread_id = msg.metadata.get("message_thread_id")
        if message_thread_id is None and reply_to_message_id is not None:
            message_thread_id = self._message_threads.get((msg.chat_id, reply_to_message_id))
        thread_kwargs = {}
        if message_thread_id is not None:
            thread_kwargs["message_thread_id"] = message_thread_id

        reply_params = None
        if self.config.reply_to_message:
            if reply_to_message_id:
                reply_params = ReplyParameters(
                    message_id=reply_to_message_id,
                    allow_sending_without_reply=True
                )

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        for media_path in (msg.media or []):
            try:
                media_type = self._get_media_type(media_path)
                sender = {
                    "photo": self._app.bot.send_photo,
                    "video": self._app.bot.send_video,
                    "voice": self._app.bot.send_voice,
                    "audio": self._app.bot.send_audio,
                }.get(media_type, self._app.bot.send_document)
                param = {
                    "photo": "photo",
                    "video": "video",
                    "voice": "voice",
                    "audio": "audio",
                }.get(media_type, "document")
                extra: dict[str, Any] = {}
                if media_type == "video":
                    extra["supports_streaming"] = True

                # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                if self._is_remote_media_url(media_path):
                    ok, error = validate_url_target(media_path)
                    if not ok:
                        raise ValueError(f"unsafe media URL: {error}")
                    await self._call_with_retry(
                        sender,
                        chat_id=chat_id,
                        **{param: media_path},
                        reply_parameters=reply_params,
                        **thread_kwargs,
                        **extra,
                    )
                    continue

                media_bytes = Path(media_path).read_bytes()
                filename = Path(media_path).name
                send_kwargs = {param: media_bytes, "filename": filename}
                await self._call_with_retry(
                    sender,
                    chat_id=chat_id,
                    reply_parameters=reply_params,
                    **thread_kwargs,
                    **extra,
                    **send_kwargs,
                )
            except Exception:
                filename = media_path.rsplit("/", 1)[-1]
                self.logger.exception("Failed to send media {}", media_path)
                await self._app.bot.send_message(
                    chat_id=chat_id,
                    text=f"[Failed to send: {filename}]",
                    reply_parameters=reply_params,
                    **thread_kwargs,
                )

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if msg.content and msg.content != "[empty message]":
            render_as_blockquote = bool(msg.metadata.get("_tool_hint"))
            buttons = getattr(msg, "buttons", None) or []
            reply_markup = self._build_keyboard(buttons) if buttons else None
            text = msg.content
            # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if buttons and reply_markup is None:
                text = f"{text}\n\n{self._buttons_as_text(buttons)}"
            chunks = split_message(text, TELEGRAM_MAX_MESSAGE_LEN)
            for i, chunk in enumerate(chunks):
                is_last = (i == len(chunks) - 1)
                await self._send_text(
                    chat_id, chunk, reply_params, thread_kwargs,
                    render_as_blockquote=render_as_blockquote,
                    reply_markup=reply_markup if is_last else None,
                )

    async def _call_with_retry(self, fn, *args, **kwargs):
        """异步执行 `_call_with_retry`。

        【中文名称】_call_with_retry

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - fn: 调用方传入的 `fn` 数据；具体类型以函数签名为准。
        - *args: 调用方传入的 `args` 数据；具体类型以函数签名为准。
        - **kwargs: 调用方传入的 `kwargs` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        from telegram.error import RetryAfter

        for attempt in range(1, _SEND_MAX_RETRIES + 1):
            try:
                return await fn(*args, **kwargs)
            except TimedOut:
                if attempt == _SEND_MAX_RETRIES:
                    raise
                delay = _SEND_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                self.logger.warning(
                    "timeout (attempt {}/{}), retrying in {:.1f}s",
                    attempt, _SEND_MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)
            except RetryAfter as e:
                if attempt == _SEND_MAX_RETRIES:
                    raise
                delay = float(e.retry_after)
                self.logger.warning(
                    "Flood Control (attempt {}/{}), retrying in {:.1f}s",
                    attempt, _SEND_MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)

    async def _send_text(
        self,
        chat_id: int,
        text: str,
        reply_params=None,
        thread_kwargs: dict | None = None,
        render_as_blockquote: bool = False,
        reply_markup=None,
    ) -> None:
        """异步执行 `_send_text`。

        【中文名称】_send_text

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - reply_params: 调用方传入的 `reply_params` 数据；具体类型以函数签名为准。
        - thread_kwargs: 调用方传入的 `thread_kwargs` 数据；具体类型以函数签名为准。
        - render_as_blockquote: 调用方传入的 `render_as_blockquote` 数据；具体类型以函数签名为准。
        - reply_markup: 调用方传入的 `reply_markup` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            html = _tool_hint_to_telegram_blockquote(text) if render_as_blockquote else _markdown_to_telegram_html(text)
            await self._call_with_retry(
                self._app.bot.send_message,
                chat_id=chat_id, text=html, parse_mode="HTML",
                reply_parameters=reply_params,
                reply_markup=reply_markup,
                **(thread_kwargs or {}),
            )
        except BadRequest as e:
            self.logger.warning("HTML parse failed, falling back to plain text: {}", e)
            try:
                await self._call_with_retry(
                    self._app.bot.send_message,
                    chat_id=chat_id,
                    text=text,
                    reply_parameters=reply_params,
                    reply_markup=reply_markup,
                    **(thread_kwargs or {}),
                )
            except Exception:
                self.logger.exception("Error sending message")
                raise

    @staticmethod
    def _is_not_modified_error(exc: Exception) -> bool:
        """执行 `_is_not_modified_error`。

        【中文名称】_is_not_modified_error

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return isinstance(exc, BadRequest) and "message is not modified" in str(exc).lower()

    async def send_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        """异步执行 `send_delta`。

        【中文名称】send_delta

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - delta: 调用方传入的 `delta` 数据；具体类型以函数签名为准。
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._app:
            return
        meta = metadata or {}
        int_chat_id = int(chat_id)
        stream_id = meta.get("_stream_id")

        if meta.get("_stream_end"):
            buf = self._stream_bufs.get(chat_id)
            if not buf or not buf.message_id or not buf.text:
                return
            if stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id:
                return
            self._stop_typing(chat_id)
            if reply_to_message_id := meta.get("message_id"):
                with suppress(ValueError):
                    await self._remove_reaction(chat_id, int(reply_to_message_id))
            thread_kwargs = {}
            if message_thread_id := meta.get("message_thread_id"):
                thread_kwargs["message_thread_id"] = message_thread_id
            raw_text = buf.text
            html = _markdown_to_telegram_html(raw_text)
            if len(html) <= TELEGRAM_HTML_MAX_LEN:
                primary_html = html
                extra_html_chunks = []
            else:
                html_chunks = split_message(html, TELEGRAM_HTML_MAX_LEN)
                primary_html = html_chunks[0]
                extra_html_chunks = html_chunks[1:]
            try:
                await self._call_with_retry(
                    self._app.bot.edit_message_text,
                    chat_id=int_chat_id, message_id=buf.message_id,
                    text=primary_html, parse_mode="HTML",
                )
            except BadRequest as e:
                # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                if self._is_not_modified_error(e):
                    self.logger.debug("Final stream edit already applied for {}", chat_id)
                    self._stream_bufs.pop(chat_id, None)
                    return
                self.logger.debug("Final stream edit failed (HTML), trying plain: {}", e)
                # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                primary_plain = split_message(raw_text, TELEGRAM_MAX_MESSAGE_LEN)[0] if len(raw_text) > TELEGRAM_MAX_MESSAGE_LEN else raw_text
                try:
                    await self._call_with_retry(
                        self._app.bot.edit_message_text,
                        chat_id=int_chat_id, message_id=buf.message_id,
                        text=primary_plain,
                    )
                except Exception as e2:
                    if self._is_not_modified_error(e2):
                        self.logger.debug("Final stream plain edit already applied for {}", chat_id)
                    else:
                        self.logger.warning("Final stream edit failed: {}", e2)
                        raise  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            for extra_html_chunk in extra_html_chunks:
                try:
                    await self._call_with_retry(
                        self._app.bot.send_message,
                        chat_id=int_chat_id, text=extra_html_chunk,
                        parse_mode="HTML",
                        **thread_kwargs,
                    )
                except Exception:
                    # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    await self._send_text(int_chat_id, extra_html_chunk)
            self._stream_bufs.pop(chat_id, None)
            return

        buf = self._stream_bufs.get(chat_id)
        if buf is None or (stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id):
            buf = _StreamBuf(stream_id=stream_id)
            self._stream_bufs[chat_id] = buf
        elif buf.stream_id is None:
            buf.stream_id = stream_id
        buf.text += delta

        if not buf.text.strip():
            return

        now = time.monotonic()
        thread_kwargs = {}
        if message_thread_id := meta.get("message_thread_id"):
            thread_kwargs["message_thread_id"] = message_thread_id
        if buf.message_id is None:
            preview = _strip_md_block(buf.text)
            try:
                sent = await self._call_with_retry(
                    self._app.bot.send_message,
                    chat_id=int_chat_id, text=preview,
                    **thread_kwargs,
                )
                buf.message_id = sent.message_id
                buf.last_edit = now
            except Exception as e:
                self.logger.warning("Stream initial send failed: {}", e)
                raise  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        elif (now - buf.last_edit) >= self.config.stream_edit_interval:
            if len(buf.text) > TELEGRAM_MAX_MESSAGE_LEN:
                await self._flush_stream_overflow(int_chat_id, buf, thread_kwargs)
                buf.last_edit = now
                return
            preview = _strip_md_block(buf.text)
            try:
                await self._call_with_retry(
                    self._app.bot.edit_message_text,
                    chat_id=int_chat_id, message_id=buf.message_id,
                    text=preview,
                )
                buf.last_edit = now
            except Exception as e:
                if self._is_not_modified_error(e):
                    buf.last_edit = now
                    return
                self.logger.warning("Stream edit failed: {}", e)
                raise  # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _flush_stream_overflow(
        self,
        chat_id: int,
        buf: "_StreamBuf",
        thread_kwargs: dict,
    ) -> None:
        """异步执行 `_flush_stream_overflow`。

        【中文名称】_flush_stream_overflow

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - buf: 调用方传入的 `buf` 数据；具体类型以函数签名为准。
        - thread_kwargs: 调用方传入的 `thread_kwargs` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        chunks = split_message(buf.text, TELEGRAM_MAX_MESSAGE_LEN)
        if len(chunks) <= 1:
            return
        try:
            await self._call_with_retry(
                self._app.bot.edit_message_text,
                chat_id=chat_id, message_id=buf.message_id,
                text=chunks[0],
            )
        except Exception as e:
            if not self._is_not_modified_error(e):
                self.logger.warning("Stream overflow edit failed: {}", e)
                raise
        for chunk in chunks[1:-1]:
            await self._call_with_retry(
                self._app.bot.send_message,
                chat_id=chat_id, text=chunk, **thread_kwargs,
            )
        tail = chunks[-1]
        sent = await self._call_with_retry(
            self._app.bot.send_message,
            chat_id=chat_id, text=tail, **thread_kwargs,
        )
        buf.message_id = sent.message_id
        buf.text = tail

    async def _on_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_on_start`。

        【中文名称】_on_start

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not update.message or not update.effective_user:
            return

        user = update.effective_user
        sender_id = self._sender_id(user)
        if not self.is_allowed(sender_id):
            await self._send_pairing_code_if_private(sender_id, update.message, user)
            return
        await update.message.reply_text(
            f"👋 Hi {user.first_name}! I'm nanobot.\n\n"
            "Send me a message and I'll respond!\n"
            "Type /help to see available commands."
        )

    async def _on_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_on_help`。

        【中文名称】_on_help

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not update.message or not update.effective_user:
            return
        user = update.effective_user
        sender_id = self._sender_id(user)
        if not self.is_allowed(sender_id):
            await self._send_pairing_code_if_private(sender_id, update.message, user)
            return
        await update.message.reply_text(build_help_text())

    @staticmethod
    def _sender_id(user) -> str:
        """执行 `_sender_id`。

        【中文名称】_sender_id

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - user: 调用方传入的 `user` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        sid = str(user.id)
        return f"{sid}|{user.username}" if user.username else sid

    async def _send_pairing_code_if_private(self, sender_id: str, message, user) -> None:
        """异步执行 `_send_pairing_code_if_private`。

        【中文名称】_send_pairing_code_if_private

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。
        - user: 调用方传入的 `user` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if message.chat.type != "private":
            return
        await self._handle_message(
            sender_id=sender_id,
            chat_id=str(message.chat_id),
            content="",
            metadata=self._build_message_metadata(message, user),
            is_dm=True,
        )

    @staticmethod
    def _derive_topic_session_key(message) -> str | None:
        """执行 `_derive_topic_session_key`。

        【中文名称】_derive_topic_session_key

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        message_thread_id = getattr(message, "message_thread_id", None)
        if message_thread_id is None:
            return None
        return f"telegram:{message.chat_id}:topic:{message_thread_id}"

    @staticmethod
    def _build_message_metadata(message, user) -> dict:
        """执行 `_build_message_metadata`。

        【中文名称】_build_message_metadata

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。
        - user: 调用方传入的 `user` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        reply_to = getattr(message, "reply_to_message", None)
        return {
            "message_id": message.message_id,
            "user_id": user.id,
            "username": user.username,
            "first_name": user.first_name,
            "is_group": message.chat.type != "private",
            "message_thread_id": getattr(message, "message_thread_id", None),
            "is_forum": bool(getattr(message.chat, "is_forum", False)),
            "reply_to_message_id": getattr(reply_to, "message_id", None) if reply_to else None,
        }

    async def _extract_reply_context(self, message) -> str | None:
        """异步执行 `_extract_reply_context`。

        【中文名称】_extract_reply_context

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        reply = getattr(message, "reply_to_message", None)
        if not reply:
            return None
        text = getattr(reply, "text", None) or getattr(reply, "caption", None) or ""
        if len(text) > TELEGRAM_REPLY_CONTEXT_MAX_LEN:
            text = text[:TELEGRAM_REPLY_CONTEXT_MAX_LEN] + "..."

        if not text:
            return None

        bot_id, _ = await self._ensure_bot_identity()
        reply_user = getattr(reply, "from_user", None)

        if bot_id and reply_user and getattr(reply_user, "id", None) == bot_id:
            return f"[Reply to bot: {text}]"
        elif reply_user and getattr(reply_user, "username", None):
            return f"[Reply to @{reply_user.username}: {text}]"
        elif reply_user and getattr(reply_user, "first_name", None):
            return f"[Reply to {reply_user.first_name}: {text}]"
        else:
            return f"[Reply to: {text}]"

    async def _download_message_media(
        self, msg, *, add_failure_content: bool = False
    ) -> tuple[list[str], list[str]]:
        """异步执行 `_download_message_media`。

        【中文名称】_download_message_media

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。
        - add_failure_content: 调用方传入的 `add_failure_content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        media_file = None
        media_type = None
        if getattr(msg, "photo", None):
            media_file = msg.photo[-1]
            media_type = "image"
        elif getattr(msg, "voice", None):
            media_file = msg.voice
            media_type = "voice"
        elif getattr(msg, "audio", None):
            media_file = msg.audio
            media_type = "audio"
        elif getattr(msg, "document", None):
            media_file = msg.document
            media_type = "file"
        elif getattr(msg, "video", None):
            media_file = msg.video
            media_type = "video"
        elif getattr(msg, "video_note", None):
            media_file = msg.video_note
            media_type = "video"
        elif getattr(msg, "animation", None):
            media_file = msg.animation
            media_type = "animation"
        if not media_file or not self._app:
            return [], []
        try:
            file = await self._app.bot.get_file(media_file.file_id)
            ext = self._get_extension(
                media_type,
                getattr(media_file, "mime_type", None),
                getattr(media_file, "file_name", None),
            )
            media_dir = get_media_dir("telegram")
            unique_id = getattr(media_file, "file_unique_id", media_file.file_id)
            file_path = media_dir / f"{unique_id}{ext}"
            await file.download_to_drive(str(file_path))
            path_str = str(file_path)
            if media_type in ("voice", "audio"):
                transcription = await self.transcribe_audio(file_path)
                if transcription:
                    self.logger.info("Transcribed {}: {}...", media_type, transcription[:50])
                    return [path_str], [f"[transcription: {transcription}]"]
                return [path_str], [f"[{media_type}: {path_str}]"]
            return [path_str], [f"[{media_type}: {path_str}]"]
        except Exception as e:
            self.logger.warning("Failed to download message media: {}", e)
            if add_failure_content:
                return [], [f"[{media_type}: download failed]"]
            return [], []

    async def _ensure_bot_identity(self) -> tuple[int | None, str | None]:
        """异步执行 `_ensure_bot_identity`。

        【中文名称】_ensure_bot_identity

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._bot_user_id is not None or self._bot_username is not None:
            return self._bot_user_id, self._bot_username
        if not self._app:
            return None, None
        bot_info = await self._app.bot.get_me()
        self._bot_user_id = getattr(bot_info, "id", None)
        self._bot_username = getattr(bot_info, "username", None)
        return self._bot_user_id, self._bot_username

    @staticmethod
    def _has_mention_entity(
        text: str,
        entities,
        bot_username: str,
        bot_id: int | None,
    ) -> bool:
        """执行 `_has_mention_entity`。

        【中文名称】_has_mention_entity

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - entities: 调用方传入的 `entities` 数据；具体类型以函数签名为准。
        - bot_username: 调用方传入的 `bot_username` 数据；具体类型以函数签名为准。
        - bot_id: 调用方传入的 `bot_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        handle = f"@{bot_username}".lower()
        for entity in entities or []:
            entity_type = getattr(entity, "type", None)
            if entity_type == "text_mention":
                user = getattr(entity, "user", None)
                if user is not None and bot_id is not None and getattr(user, "id", None) == bot_id:
                    return True
                continue
            if entity_type != "mention":
                continue
            offset = getattr(entity, "offset", None)
            length = getattr(entity, "length", None)
            if offset is None or length is None:
                continue
            if text[offset : offset + length].lower() == handle:
                return True
        return handle in text.lower()

    async def _is_group_message_for_bot(self, message) -> bool:
        """异步执行 `_is_group_message_for_bot`。

        【中文名称】_is_group_message_for_bot

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if message.chat.type == "private" or self.config.group_policy == "open":
            return True

        bot_id, bot_username = await self._ensure_bot_identity()
        if bot_username:
            text = message.text or ""
            caption = message.caption or ""
            if self._has_mention_entity(
                text,
                getattr(message, "entities", None),
                bot_username,
                bot_id,
            ):
                return True
            if self._has_mention_entity(
                caption,
                getattr(message, "caption_entities", None),
                bot_username,
                bot_id,
            ):
                return True

        reply_user = getattr(getattr(message, "reply_to_message", None), "from_user", None)
        return bool(bot_id and reply_user and reply_user.id == bot_id)

    def _remember_thread_context(self, message) -> None:
        """执行 `_remember_thread_context`。

        【中文名称】_remember_thread_context

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        message_thread_id = getattr(message, "message_thread_id", None)
        if message_thread_id is None:
            return
        key = (str(message.chat_id), message.message_id)
        self._message_threads[key] = message_thread_id
        if len(self._message_threads) > 1000:
            self._message_threads.pop(next(iter(self._message_threads)))

    @staticmethod
    def _queue_key_for_message(message) -> str:
        """执行 `_queue_key_for_message`。

        【中文名称】_queue_key_for_message

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return TelegramChannel._derive_topic_session_key(message) or f"telegram:{message.chat_id}"

    @staticmethod
    def _sort_key_for_update(update: Update) -> tuple[int, int]:
        """执行 `_sort_key_for_update`。

        【中文名称】_sort_key_for_update

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        message = getattr(update, "message", None)
        message_id = int(getattr(message, "message_id", 0) or 0)
        update_id = int(getattr(update, "update_id", 0) or 0)
        return (message_id, update_id)

    def _enqueue_ordered_update(
        self,
        *,
        kind: Literal["command", "message"],
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """执行 `_enqueue_ordered_update`。

        【中文名称】_enqueue_ordered_update

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - kind: 调用方传入的 `kind` 数据；具体类型以函数签名为准。
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        message = update.message
        key = self._queue_key_for_message(message)
        self._inbound_buffers.setdefault(key, []).append(
            _QueuedTelegramUpdate(
                kind=kind,
                update=update,
                context=context,
                sort_key=self._sort_key_for_update(update),
            )
        )
        if key not in self._inbound_workers:
            self._inbound_workers[key] = asyncio.create_task(
                self._drain_ordered_updates(key)
            )

    async def _drain_ordered_updates(self, key: str) -> None:
        """异步执行 `_drain_ordered_updates`。

        【中文名称】_drain_ordered_updates

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - key: 调用方传入的 `key` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            while self._running:
                await asyncio.sleep(0.2)
                batch = self._inbound_buffers.get(key, [])
                if not batch:
                    break
                self._inbound_buffers[key] = []
                batch.sort(key=lambda item: item.sort_key)
                for item in batch:
                    try:
                        if item.kind == "command":
                            await self._process_forward_command(item.update, item.context)
                        else:
                            await self._process_message_update(item.update, item.context)
                    except Exception as e:
                        self.logger.warning(
                            "Telegram queued update handling failed for {}: {}",
                            key,
                            e,
                        )
            if not self._inbound_buffers.get(key):
                self._inbound_buffers.pop(key, None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.warning("Telegram ordered update worker failed for {}: {}", key, e)
        finally:
            if not self._inbound_buffers.get(key):
                self._inbound_workers.pop(key, None)

    async def _forward_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_forward_command`。

        【中文名称】_forward_command

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not update.message or not update.effective_user:
            return
        if not self._running:
            await self._process_forward_command(update, context)
            return
        self._enqueue_ordered_update(kind="command", update=update, context=context)

    async def _process_forward_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_process_forward_command`。

        【中文名称】_process_forward_command

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        message = update.message
        user = update.effective_user
        sender_id = self._sender_id(user)
        if not self.is_allowed(sender_id):
            await self._send_pairing_code_if_private(sender_id, message, user)
            return
        self._remember_thread_context(message)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        content = message.text or ""
        if content.startswith("/") and "@" in content:
            cmd_part, *rest = content.split(" ", 1)
            cmd_part = cmd_part.split("@")[0]
            content = f"{cmd_part} {rest[0]}" if rest else cmd_part
        content = self._normalize_telegram_command(content)

        await self._handle_message(
            sender_id=sender_id,
            chat_id=str(message.chat_id),
            content=content,
            metadata=self._build_message_metadata(message, user),
            session_key=self._derive_topic_session_key(message),
            is_dm=message.chat.type == "private",
        )

    async def _on_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_on_message`。

        【中文名称】_on_message

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not update.message or not update.effective_user:
            return
        if not self._running:
            await self._process_message_update(update, context)
            return
        self._enqueue_ordered_update(kind="message", update=update, context=context)

    async def _process_message_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_process_message_update`。

        【中文名称】_process_message_update

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        message = update.message
        user = update.effective_user
        chat_id = message.chat_id
        sender_id = self._sender_id(user)
        if not self.is_allowed(sender_id):
            await self._send_pairing_code_if_private(sender_id, message, user)
            return
        self._remember_thread_context(message)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._chat_ids[sender_id] = chat_id

        if not await self._is_group_message_for_bot(message):
            return

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        content_parts = []
        media_paths = []

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if message.text:
            content_parts.append(message.text)
        if message.caption:
            content_parts.append(message.caption)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if message.location:
            lat = message.location.latitude
            lon = message.location.longitude
            content_parts.append(f"[location: {lat}, {lon}]")

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        current_media_paths, current_media_parts = await self._download_message_media(
            message, add_failure_content=True
        )
        media_paths.extend(current_media_paths)
        content_parts.extend(current_media_parts)
        if current_media_paths:
            self.logger.debug("Downloaded message media to {}", current_media_paths[0])

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        reply = getattr(message, "reply_to_message", None)
        if reply is not None:
            reply_ctx = await self._extract_reply_context(message)
            reply_media, reply_media_parts = await self._download_message_media(reply)
            if reply_media:
                media_paths = reply_media + media_paths
                self.logger.debug("Attached replied-to media: {}", reply_media[0])
            tag = reply_ctx or (f"[Reply to: {reply_media_parts[0]}]" if reply_media_parts else None)
            if tag:
                content_parts.insert(0, tag)
        content = "\n".join(content_parts) if content_parts else "[empty message]"

        self.logger.debug("message from {}: {}...", sender_id, content[:50])

        str_chat_id = str(chat_id)
        metadata = self._build_message_metadata(message, user)
        session_key = self._derive_topic_session_key(message)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if media_group_id := getattr(message, "media_group_id", None):
            key = f"{str_chat_id}:{media_group_id}"
            if key not in self._media_group_buffers:
                self._media_group_buffers[key] = {
                    "sender_id": sender_id, "chat_id": str_chat_id,
                    "contents": [], "media": [],
                    "metadata": metadata,
                    "session_key": session_key,
                }
                self._start_typing(str_chat_id)
                await self._add_reaction(str_chat_id, message.message_id, self.config.react_emoji)
            buf = self._media_group_buffers[key]
            if content and content != "[empty message]":
                buf["contents"].append(content)
            buf["media"].extend(media_paths)
            if key not in self._media_group_tasks:
                self._media_group_tasks[key] = asyncio.create_task(self._flush_media_group(key))
            return

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._start_typing(str_chat_id)
        await self._add_reaction(str_chat_id, message.message_id, self.config.react_emoji)

        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        await self._handle_message(
            sender_id=sender_id,
            chat_id=str_chat_id,
            content=content,
            media=media_paths,
            metadata=metadata,
            session_key=session_key,
        )

    async def _flush_media_group(self, key: str) -> None:
        """异步执行 `_flush_media_group`。

        【中文名称】_flush_media_group

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - key: 调用方传入的 `key` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            await asyncio.sleep(0.6)
            if not (buf := self._media_group_buffers.pop(key, None)):
                return
            content = "\n".join(buf["contents"]) or "[empty message]"
            await self._handle_message(
                sender_id=buf["sender_id"], chat_id=buf["chat_id"],
                content=content, media=list(dict.fromkeys(buf["media"])),
                metadata=buf["metadata"],
                session_key=buf.get("session_key"),
            )
        finally:
            self._media_group_tasks.pop(key, None)

    def _start_typing(self, chat_id: str) -> None:
        """执行 `_start_typing`。

        【中文名称】_start_typing

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._stop_typing(chat_id)
        self._typing_tasks[chat_id] = asyncio.create_task(self._typing_loop(chat_id))

    def _stop_typing(self, chat_id: str) -> None:
        """执行 `_stop_typing`。

        【中文名称】_stop_typing

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        task = self._typing_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()

    async def _add_reaction(self, chat_id: str, message_id: int, emoji: str) -> None:
        """异步执行 `_add_reaction`。

        【中文名称】_add_reaction

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。
        - emoji: 调用方传入的 `emoji` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._app or not emoji:
            return
        try:
            await self._app.bot.set_message_reaction(
                chat_id=int(chat_id),
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)],
            )
        except Exception as e:
            self.logger.debug("reaction failed: {}", e)

    async def _remove_reaction(self, chat_id: str, message_id: int) -> None:
        """异步执行 `_remove_reaction`。

        【中文名称】_remove_reaction

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - message_id: 调用方传入的 `message_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._app:
            return
        try:
            await self._app.bot.set_message_reaction(
                chat_id=int(chat_id),
                message_id=message_id,
                reaction=[],
            )
        except Exception as e:
            self.logger.debug("reaction removal failed: {}", e)

    async def _typing_loop(self, chat_id: str) -> None:
        """异步执行 `_typing_loop`。

        【中文名称】_typing_loop

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            with suppress(asyncio.CancelledError):
                while self._app:
                    await self._app.bot.send_chat_action(chat_id=int(chat_id), action="typing")
                    await asyncio.sleep(4)
        except Exception as e:
            self.logger.debug("Typing indicator stopped for {}: {}", chat_id, e)

    @staticmethod
    def _format_telegram_error(exc: Exception) -> str:
        """执行 `_format_telegram_error`。

        【中文名称】_format_telegram_error

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        text = str(exc).strip()
        if text:
            return text
        if exc.__cause__ is not None:
            cause = exc.__cause__
            cause_text = str(cause).strip()
            if cause_text:
                return f"{exc.__class__.__name__} ({cause_text})"
            return f"{exc.__class__.__name__} ({cause.__class__.__name__})"
        return exc.__class__.__name__

    def _on_polling_error(self, exc: Exception) -> None:
        """执行 `_on_polling_error`。

        【中文名称】_on_polling_error

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        summary = self._format_telegram_error(exc)
        if isinstance(exc, (NetworkError, TimedOut)):
            self.logger.warning("polling network issue: {}", summary)
        else:
            self.logger.error("polling error: {}", summary)

    async def _on_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_on_error`。

        【中文名称】_on_error

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        summary = self._format_telegram_error(context.error)

        if isinstance(context.error, (NetworkError, TimedOut)):
            self.logger.warning("network issue: {}", summary)
        else:
            self.logger.error("error: {}", summary)

    def _get_extension(
        self,
        media_type: str,
        mime_type: str | None,
        filename: str | None = None,
    ) -> str:
        """执行 `_get_extension`。

        【中文名称】_get_extension

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - media_type: 调用方传入的 `media_type` 数据；具体类型以函数签名为准。
        - mime_type: 调用方传入的 `mime_type` 数据；具体类型以函数签名为准。
        - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if mime_type:
            ext_map = {
                "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
                "image/webp": ".webp",
                "audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/mp4": ".m4a",
                "video/mp4": ".mp4", "video/quicktime": ".mov", "video/webm": ".webm",
                "video/x-matroska": ".mkv", "video/3gpp": ".3gp",
            }
            if mime_type in ext_map:
                return ext_map[mime_type]

        type_map = {"image": ".jpg", "voice": ".ogg", "audio": ".mp3", "video": ".mp4", "file": ""}
        if ext := type_map.get(media_type, ""):
            return ext

        if filename:
            return "".join(Path(filename).suffixes)

        return ""

    def _build_keyboard(self, buttons: list) -> InlineKeyboardMarkup | None:
        """执行 `_build_keyboard`。

        【中文名称】_build_keyboard

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - buttons: 调用方传入的 `buttons` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not buttons or not self.config.inline_keyboards:
            return None
        keyboard = [
            [InlineKeyboardButton(label, callback_data=self._safe_callback_data(label)) for label in row]
            for row in buttons
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def _safe_callback_data(label: str) -> str:
        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_safe_callback_data`。

        【中文名称】_safe_callback_data

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - label: 调用方传入的 `label` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        encoded = label.encode("utf-8")
        if len(encoded) <= 64:
            return label
        return encoded[:64].decode("utf-8", errors="ignore")

    @staticmethod
    def _buttons_as_text(buttons: list[list[str]]) -> str:
        # 说明：这里处理 Telegram 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_buttons_as_text`。

        【中文名称】_buttons_as_text

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - buttons: 调用方传入的 `buttons` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return "\n".join(" ".join(f"[{label}]" for label in row) for row in buttons if row)

    async def _on_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """异步执行 `_on_callback_query`。

        【中文名称】_on_callback_query

        【功能说明】
        这是 Telegram 渠道适配器 中的一个步骤函数，用来支撑：负责使用 python-telegram-bot 接收 Telegram 消息、附件、命令和回调按钮，并处理 Telegram HTML/Markdown 限制。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - update: 调用方传入的 `update` 数据；具体类型以函数签名为准。
        - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not update.callback_query or not update.effective_user:
            return
        query = update.callback_query
        user = update.effective_user
        chat_id = query.message.chat_id if query.message else None
        sender_id = self._sender_id(user)
        if not chat_id:
            self.logger.warning("Callback query without chat_id")
            return
        if not self.is_allowed(sender_id):
            return
        button_label = query.data or ""
        await query.answer()
        if query.message:
            with suppress(Exception):
                await query.message.edit_reply_markup(reply_markup=None)
        self.logger.debug("Inline button tap from {}: {}", sender_id, button_label)
        self._start_typing(str(chat_id))
        await self._handle_message(
            sender_id=sender_id,
            chat_id=str(chat_id),
            content=button_label,
            metadata={
                "callback_query_id": query.id,
                "button_label": button_label,
                "user_id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "is_callback": True,
            },
        )
