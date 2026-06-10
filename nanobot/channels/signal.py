"""Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/signal.py

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
import json
import re
import shutil
import unicodedata
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from pydantic import Field, computed_field, field_validator

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.pairing import is_approved
from nanobot.utils.helpers import safe_filename, split_message


@dataclass
class _Run:
    """_Run 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】_Run

    【功能说明】
    Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """
    text: str
    styles: frozenset[str] = field(default_factory=frozenset)
    opaque: bool = False  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


_SIG_CODE_BLOCK_RE = re.compile(r"```(?:\w+)?\n?([\s\S]*?)```")
_SIG_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_SIG_HEADER_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
_SIG_BLOCKQUOTE_RE = re.compile(r"^>\s*(.*)$", re.MULTILINE)
_SIG_BULLET_RE = re.compile(r"^[-*]\s+", re.MULTILINE)
_SIG_OLIST_RE = re.compile(r"^(\d+)\.\s+", re.MULTILINE)
_SIG_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_SIG_BOLD_RE = re.compile(r"\*\*(.+?)\*\*|__(.+?)__", re.DOTALL)
_SIG_ITALIC_RE = re.compile(
    r"(?<!\*)\*([^*\n]+)\*(?!\*)|(?<![a-zA-Z0-9_])_([^_\n]+)_(?![a-zA-Z0-9_])"
)
_SIG_STRIKE_RE = re.compile(r"~~(.+?)~~|(?<![~\w])~([^~\n]+)~(?![~\w])", re.DOTALL)
_SIG_TOKEN_RE = re.compile(r"\x00C(\d+)\x00")

# 中文说明：这一段围绕Markdown处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
_SIG_CELL_STRIP_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),
    (re.compile(r"__(.+?)__"), r"\1"),
    (re.compile(r"~~(.+?)~~"), r"\1"),
    (re.compile(r"`([^`]+)`"), r"\1"),
)


def _utf16_len(s: str) -> int:
    """执行辅助逻辑（_utf16_len = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_utf16_len` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    s: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return len(s.encode("utf-16-le")) // 2


def _sig_strip_cell(s: str) -> str:
    """执行辅助逻辑（_sig_strip_cell = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_sig_strip_cell` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    s: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    for pattern, repl in _SIG_CELL_STRIP_PATTERNS:
        s = pattern.sub(repl, s)
    return s.strip()


def _sig_render_table(table_lines: list[str]) -> str:
    """渲染内容（_sig_render_table = 原函数名）。

    【中文名称】渲染内容

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_sig_render_table` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    table_lines: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """

    def dw(s: str) -> int:
        """执行辅助逻辑（dw = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `dw` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        s: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in s)

    rows: list[list[str]] = []
    has_sep = False
    for line in table_lines:
        cells = [_sig_strip_cell(c) for c in line.strip().strip("|").split("|")]
        if all(re.match(r"^:?-+:?$", c) for c in cells if c):
            has_sep = True
            continue
        rows.append(cells)
    if not rows or not has_sep:
        return "\n".join(table_lines)

    ncols = max(len(r) for r in rows)
    for r in rows:
        r.extend([""] * (ncols - len(r)))
    widths = [max(dw(r[c]) for r in rows) for c in range(ncols)]

    def dr(cells: list[str]) -> str:
        """执行辅助逻辑（dr = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `dr` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cells: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return "  ".join(f"{c}{' ' * (w - dw(c))}" for c, w in zip(cells, widths))

    out = [dr(rows[0])]
    out.append("  ".join("─" * w for w in widths))
    for row in rows[1:]:
        out.append(dr(row))
    return "\n".join(out)


def _markdown_to_signal(text: str) -> tuple[str, list[str]]:
    """执行辅助逻辑（_markdown_to_signal = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_markdown_to_signal` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not text:
        return text, []

    # 中文说明：提取。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    protected: list[str] = []

    def save_code(m: re.Match) -> str:
        """保存数据（save_code = 原函数名）。

        【中文名称】保存数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `save_code` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        m: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        protected.append(m.group(1))
        return f"\x00C{len(protected) - 1}\x00"

    text = _SIG_CODE_BLOCK_RE.sub(save_code, text)

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    lines = text.split("\n")
    rebuilt: list[str] = []
    i = 0
    while i < len(lines):
        if re.match(r"^\s*\|.+\|", lines[i]):
            tbl: list[str] = []
            while i < len(lines) and re.match(r"^\s*\|.+\|", lines[i]):
                tbl.append(lines[i])
                i += 1
            rendered = _sig_render_table(tbl)
            if rendered != "\n".join(tbl):
                protected.append(rendered)
                rebuilt.append(f"\x00C{len(protected) - 1}\x00")
            else:
                rebuilt.extend(tbl)
        else:
            rebuilt.append(lines[i])
            i += 1
    text = "\n".join(rebuilt)

    # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
    runs: list[_Run] = [_Run(text)]

    def transform(
        pattern: re.Pattern,
        make_runs: Callable[[re.Match, frozenset[str]], list[_Run]],
    ) -> None:
        """执行辅助逻辑（transform = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `transform` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        pattern: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        make_runs: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        new_runs: list[_Run] = []
        for run in runs:
            if run.opaque:
                new_runs.append(run)
                continue
            pos = 0
            for m in pattern.finditer(run.text):
                if m.start() > pos:
                    new_runs.append(_Run(run.text[pos : m.start()], run.styles))
                new_runs.extend(make_runs(m, run.styles))
                pos = m.end()
            if pos < len(run.text):
                new_runs.append(_Run(run.text[pos:], run.styles))
        runs[:] = new_runs

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    transform(
        _SIG_TOKEN_RE,
        lambda m, s: [_Run(protected[int(m.group(1))], s | {"MONOSPACE"}, opaque=True)],
    )

    # 中文说明：行内代码。
    transform(_SIG_INLINE_CODE_RE, lambda m, s: [_Run(m.group(1), s | {"MONOSPACE"}, opaque=True)])

    # 中文说明：普通文本。
    transform(_SIG_HEADER_RE, lambda m, s: [_Run(m.group(1), s | {"BOLD"})])

    # 中文说明：引用块。
    transform(_SIG_BLOCKQUOTE_RE, lambda m, s: [_Run(m.group(1), s)])

    # 中文说明：项目符号列表。
    transform(_SIG_BULLET_RE, lambda m, s: [_Run("• ", s)])

    # 中文说明：有序编号列表。
    transform(_SIG_OLIST_RE, lambda m, s: [_Run(m.group(1) + ". ", s)])

    # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
    def _link_runs(m: re.Match, s: frozenset) -> list[_Run]:
        """执行辅助逻辑（_link_runs = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `_link_runs` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        m: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        s: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        link_text, url = m.group(1), m.group(2)

        def _norm(u: str) -> str:
            """执行辅助逻辑（_norm = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `_norm` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            u: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            return re.sub(r"^https?://(www\.)?", "", u).rstrip("/").lower()

        if _norm(url) == _norm(link_text):
            return [_Run(url, s)]
        return [_Run(f"{link_text} ({url})", s)]

    transform(_SIG_LINK_RE, _link_runs)

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    transform(_SIG_BOLD_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"BOLD"})])

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    transform(_SIG_ITALIC_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"ITALIC"})])

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    transform(_SIG_STRIKE_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"STRIKETHROUGH"})])

    # 中文说明：这里标记当前处理阶段，便于按执行顺序跟读代码。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    plain_text = ""
    text_styles: list[str] = []
    utf16_offset = 0
    for run in runs:
        if not run.text:
            continue
        plain_text += run.text
        start = utf16_offset
        length = _utf16_len(run.text)
        utf16_offset += length
        for style in sorted(run.styles):
            text_styles.append(f"{start}:{length}:{style}")

    return plain_text, text_styles


def _partition_styles(
    plain_text: str, chunks: list[str], text_styles: list[str]
) -> list[list[str]]:
    """执行辅助逻辑（_partition_styles = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
    在阅读 `_partition_styles` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    plain_text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    chunks: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    text_styles: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not chunks:
        return []
    if not text_styles:
        return [[] for _ in chunks]

    # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    chunk_ranges: list[tuple[int, int]] = []
    cursor = 0  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    for i, chunk in enumerate(chunks):
        if i > 0:
            while cursor < len(plain_text) and plain_text[cursor].isspace():
                cursor += 1
        utf16_start = _utf16_len(plain_text[:cursor])
        utf16_end = utf16_start + _utf16_len(chunk)
        chunk_ranges.append((utf16_start, utf16_end))
        cursor += len(chunk)

    result: list[list[str]] = [[] for _ in chunks]
    for entry in text_styles:
        s, ln, style = entry.split(":", 2)
        r_start = int(s)
        r_end = r_start + int(ln)
        for i, (c_start, c_end) in enumerate(chunk_ranges):
            if r_end <= c_start or r_start >= c_end:
                continue
            new_start = max(r_start, c_start) - c_start
            new_end = min(r_end, c_end) - c_start
            new_length = new_end - new_start
            if new_length > 0:
                result[i].append(f"{new_start}:{new_length}:{style}")
    return result


class SignalDMConfig(Base):
    """SignalDMConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SignalDMConfig

    【功能说明】
    Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    policy: str = "allowlist"  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    allow_from: list[str] = Field(default_factory=list)  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


class SignalGroupConfig(Base):
    """SignalGroupConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SignalGroupConfig

    【功能说明】
    Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    policy: str = "allowlist"  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    allow_from: list[str] = Field(default_factory=list)  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    require_mention: bool = True  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


class SignalConfig(Base):
    """SignalConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SignalConfig

    【功能说明】
    Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    phone_number: str = ""  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    daemon_host: str = "localhost"
    daemon_port: int = 8080
    group_message_buffer_size: int = 20  # 中文说明：这一段围绕消息、上下文处理，注意输入、输出和异常路径。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
    attachments_dir: str | None = None
    dm: SignalDMConfig = Field(default_factory=SignalDMConfig)
    group: SignalGroupConfig = Field(default_factory=SignalGroupConfig)

    @field_validator("group_message_buffer_size")
    @classmethod
    def _validate_buffer_size(cls, v: int) -> int:
        """校验输入（_validate_buffer_size = 原函数名）。

        【中文名称】校验输入

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalConfig._validate_buffer_size` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        v: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if v <= 0:
            raise ValueError("group_message_buffer_size must be > 0")
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def allow_from(self) -> list[str]:
        """执行辅助逻辑（allow_from = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalConfig.allow_from` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return list(dict.fromkeys(self.dm.allow_from + self.group.allow_from))


class SignalChannel(BaseChannel):
    """SignalChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】SignalChannel

    【功能说明】
    Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "signal"
    display_name = "Signal"
    _TYPING_REFRESH_SECONDS = 10.0
    _MAX_MESSAGE_LEN = 64_000  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    _HTTP_TIMEOUT_SECONDS = 60.0

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return SignalConfig().model_dump(by_alias=True)

    def __init__(self, config: SignalConfig, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = SignalConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: SignalConfig = config
        self._http: httpx.AsyncClient | None = None
        self._request_id = 0
        self._sse_task: asyncio.Task | None = None
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._typing_uuid_warnings: set[str] = set()
        self._account_id_aliases: set[str] = set()
        self._remember_account_id_alias(self.config.phone_number)

        # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        self._group_buffers: dict[str, deque] = {}

    def is_allowed(self, sender_id: str) -> bool:
        """判断条件是否成立（is_allowed = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.is_allowed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        allow_list = self.config.allow_from
        if "*" in allow_list:
            return True
        if self._sender_matches_allowlist(sender_id, allow_list):
            return True
        if self._sender_approved_via_pairing(sender_id):
            return True
        if not allow_list:
            self.logger.warning("allow_from is empty — all access denied")
        return False

    def _sender_approved_via_pairing(self, sender_id: str) -> bool:
        """发送输出消息（_sender_approved_via_pairing = 原函数名）。

        【中文名称】发送输出消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._sender_approved_via_pairing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        for part in str(sender_id).split("|"):
            for variant in self._normalize_signal_id(part):
                if is_approved(self.name, variant):
                    return True
        return False

    async def _handle_message(
        self,
        sender_id: str,
        chat_id: str,
        content: str,
        media: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        session_key: str | None = None,
        is_dm: bool = False,
    ) -> None:
        """异步处理事件（_handle_message = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._handle_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        media: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。
        session_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        is_dm: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        meta = metadata or {}
        if self.supports_streaming:
            meta = {**meta, "_wants_stream": True}
        await self.bus.publish_inbound(
            InboundMessage(
                channel=self.name,
                sender_id=str(sender_id),
                chat_id=str(chat_id),
                content=content,
                media=media or [],
                metadata=meta,
                session_key_override=session_key,
            )
        )

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.config.phone_number:
            self.logger.error("Signal account not configured")
            return

        self._running = True
        await self._start_http_mode()

    async def _start_http_mode(self) -> None:
        """异步启动流程（_start_http_mode = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._start_http_mode` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        base_url = f"http://{self.config.daemon_host}:{self.config.daemon_port}"
        reconnect_delay_s = 1.0
        max_reconnect_delay_s = 30.0

        while self._running:
            try:
                self.logger.info("Connecting to signal-cli daemon at {}...", base_url)

                # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
                self._http = httpx.AsyncClient(
                    timeout=self._HTTP_TIMEOUT_SECONDS, base_url=base_url
                )

                # 中文说明：Test connection 相关逻辑。
                try:
                    response = await self._http.get("/api/v1/check")
                    if response.status_code == 200:
                        self.logger.info("Connected to signal-cli daemon")
                    else:
                        raise ConnectionRefusedError(
                            f"signal-cli daemon check returned status {response.status_code}"
                        )
                except Exception as e:
                    raise ConnectionRefusedError(f"signal-cli daemon not responding: {e}")

                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                reconnect_delay_s = 1.0

                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                await self._ensure_typing_indicators_enabled()

                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                self._sse_task = asyncio.create_task(self._sse_receive_loop())
                await self._sse_task
                if self._running:
                    raise ConnectionError("Signal SSE stream ended unexpectedly")

            except asyncio.CancelledError:
                break
            except ConnectionRefusedError as e:
                self.logger.error(
                    "{}. Make sure signal-cli daemon is running: "
                    "signal-cli -a {} daemon --http {}:{}",
                    e,
                    self.config.phone_number,
                    self.config.daemon_host,
                    self.config.daemon_port,
                )
            except Exception as e:
                self.logger.error("Signal channel error: {}", e)
            finally:
                if self._sse_task:
                    if not self._sse_task.done():
                        self._sse_task.cancel()
                    try:
                        await self._sse_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                    self._sse_task = None
                if self._http:
                    await self._http.aclose()
                    self._http = None

            if self._running:
                self.logger.info(
                    "Reconnecting to signal-cli daemon in {:.0f} seconds...", reconnect_delay_s
                )
                await asyncio.sleep(reconnect_delay_s)
                reconnect_delay_s = min(reconnect_delay_s * 2, max_reconnect_delay_s)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False

        # 中文说明：Stop SSE task 相关逻辑。
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for chat_id in list(self._typing_tasks):
            await self._stop_typing(chat_id)

        # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
        if self._http:
            await self._http.aclose()
            self._http = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        is_progress_message = bool(msg.metadata.get("_progress"))
        try:
            plain_text, text_styles = _markdown_to_signal(msg.content)
            if not plain_text and not msg.media:
                return
            recipient_params = self._recipient_params(msg.chat_id)

            chunks = split_message(plain_text, self._MAX_MESSAGE_LEN) if plain_text else [""]
            chunk_styles = _partition_styles(plain_text, chunks, text_styles)
            for i, chunk in enumerate(chunks):
                params: dict[str, Any] = {"message": chunk}
                if chunk_styles[i]:
                    params["textStyle"] = chunk_styles[i]
                params.update(recipient_params)
                if msg.media and i == 0:
                    params["attachments"] = msg.media

                response = await self._send_request("send", params)

                if "error" in response:
                    self.logger.error("Error sending Signal message: {}", response['error'])
                    raise RuntimeError(f"signal-cli send failed: {response['error']}")
                else:
                    self.logger.debug(
                        f"Signal message sent, timestamp: {response.get('result', {}).get('timestamp')}"
                    )

        except Exception:
            self.logger.exception("Error sending Signal message")
            raise
        finally:
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if not is_progress_message:
                # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                await self._stop_typing(msg.chat_id, send_stop=False)

    async def _sse_receive_loop(self) -> None:
        """异步接收消息（_sse_receive_loop = 原函数名）。

        【中文名称】接收消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._sse_receive_loop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            raise RuntimeError("HTTP client not initialized for Signal SSE stream")

        self.logger.info("Started Signal message receive loop (SSE)")

        try:
            async with self._http.stream("GET", "/api/v1/events") as response:
                if response.status_code != 200:
                    raise ConnectionError(
                        f"SSE connection failed with status {response.status_code}"
                    )

                self.logger.info("Subscribed to Signal messages via SSE")

                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                event_buffer = []

                async for line in response.aiter_lines():
                    if not self._running:
                        break

                    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                    if line and line != ":":
                        self.logger.debug("SSE line received: {}", line[:200])

                    # 中文说明：这一段围绕格式处理，注意输入、输出和异常路径。
                    if isinstance(line, str):
                        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
                        if not line or line == ":":
                            if event_buffer:
                                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                                data_str = ""
                                try:
                                    data_str = "\n".join(event_buffer)
                                    data = json.loads(data_str)
                                    self.logger.debug("SSE event parsed: {}", data)
                                    await self._handle_receive_notification(data)
                                except json.JSONDecodeError as e:
                                    self.logger.warning(
                                        "Invalid JSON in SSE buffer: {}, data: {}",
                                        e,
                                        data_str[:200],
                                    )
                                finally:
                                    event_buffer = []

                        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                        elif line.startswith("data:"):
                            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                            event_buffer.append(line[6:] if line[5:6] == " " else line[5:])

                        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
                        elif line.startswith("event:"):
                            pass  # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。

                if self._running:
                    raise ConnectionError("Signal SSE stream closed by remote endpoint")

        except asyncio.CancelledError:
            self.logger.info("SSE receive loop cancelled")
            raise
        except Exception as e:
            self.logger.error("Error in SSE receive loop: {}", e)
            raise

    @asynccontextmanager
    async def _safe_handle(self, action: str, payload: Any = None) -> AsyncIterator[None]:
        """异步处理事件（_safe_handle = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._safe_handle` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        action: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        payload: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            yield
        except Exception as e:
            snippet = repr(payload)[:200] if payload is not None else ""
            text = f"Error in {action}: {e}"
            if snippet:
                text += f" | payload={snippet}"
            self.logger.opt(exception=True).error(text)

    async def _handle_receive_notification(self, params: dict[str, Any]) -> None:
        """异步处理事件（_handle_receive_notification = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._handle_receive_notification` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        params: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.logger.debug("_handle_receive_notification called with: {}", params)
        async with self._safe_handle("receive notification", params):
            # 中文说明：提取。
            envelope = params.get("envelope", {})

            self.logger.debug("Extracted envelope: {}", envelope)

            if not envelope:
                self.logger.debug("No envelope found in params")
                return

            # 中文说明：提取。
            sender_parts = self._collect_sender_id_parts(envelope)
            source_name = envelope.get("sourceName")

            if not sender_parts:
                self.logger.debug("Received message without source, skipping")
                return

            sender_number = self._primary_sender_id(sender_parts)
            sender_id = "|".join(sender_parts)

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if any(self._id_matches_account(part) for part in sender_parts):
                for part in sender_parts:
                    self._remember_account_id_alias(part)

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            data_message = envelope.get("dataMessage")
            sync_message = envelope.get("syncMessage")
            typing_message = envelope.get("typingMessage")
            receipt_message = envelope.get("receiptMessage")

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            if receipt_message:
                return

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            if data_message:
                await self._handle_data_message(sender_id, sender_number, data_message, source_name)

            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            elif sync_message and sync_message.get("sentMessage"):
                sent_msg = sync_message["sentMessage"]
                destination = sent_msg.get("destination") or sent_msg.get("destinationNumber")
                if destination:
                    self.logger.debug(
                        "Sync message sent to {}: {}", destination, sent_msg.get("message", "")[:50]
                    )

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            elif typing_message:
                pass  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    async def _handle_data_message(
        self,
        sender_id: str,
        sender_number: str,
        data_message: dict[str, Any],
        sender_name: str | None,
    ) -> None:
        """异步处理事件（_handle_data_message = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._handle_data_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_number: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        data_message: 消息数据，可能来自用户、频道、模型或工具调用。
        sender_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        message_text = data_message.get("message") or ""
        attachments = data_message.get("attachments", [])
        mentions = data_message.get("mentions", [])
        timestamp = data_message.get("timestamp")

        self.logger.info(
            "Data message from {}: groupInfo={}, groupV2={}, keys={}",
            sender_number,
            data_message.get("groupInfo"),
            data_message.get("groupV2"),
            list(data_message.keys()),
        )

        if data_message.get("reaction"):
            self.logger.debug(
                "Ignoring reaction message from {}: {}", sender_number, data_message["reaction"]
            )
            return
        if not message_text and not attachments:
            self.logger.debug("Ignoring empty message from {}", sender_number)
            return

        group_info = data_message.get("groupInfo")
        group_v2 = data_message.get("groupV2")
        is_group_message = group_info is not None or group_v2 is not None
        group_id = self._extract_group_id(group_info, group_v2)

        allowed, chat_id = self._check_inbound_policy(
            sender_id=sender_id,
            sender_number=sender_number,
            group_id=group_id,
            is_group_message=is_group_message,
            message_text=message_text,
            mentions=mentions,
            sender_name=sender_name,
            timestamp=timestamp,
        )
        if not allowed:
            # 中文说明：这一段围绕Slack处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if not is_group_message and self.config.dm.enabled:
                await super()._handle_message(
                    sender_id=sender_id,
                    chat_id=chat_id,
                    content="",
                    is_dm=True,
                )
            return

        content, media_paths = self._assemble_inbound_content(
            sender_name=sender_name,
            sender_number=sender_number,
            message_text=message_text,
            attachments=attachments,
            mentions=mentions,
            is_group_message=is_group_message,
            chat_id=chat_id,
        )

        self.logger.debug("Signal message from {}: {}...", sender_number, content[:50])

        await self._start_typing(chat_id)
        try:
            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                media=media_paths,
                metadata={
                    "timestamp": timestamp,
                    "sender_name": sender_name,
                    "sender_number": sender_number,
                    "is_group": is_group_message,
                    "group_id": group_id,
                },
                is_dm=not is_group_message,
            )
        except Exception:
            await self._stop_typing(chat_id)
            raise

    def _check_inbound_policy(
        self,
        *,
        sender_id: str,
        sender_number: str,
        group_id: str | None,
        is_group_message: bool,
        message_text: str,
        mentions: list,
        sender_name: str | None,
        timestamp: int | None,
    ) -> tuple[bool, str]:
        """执行辅助逻辑（_check_inbound_policy = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._check_inbound_policy` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_number: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        group_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        is_group_message: 消息数据，可能来自用户、频道、模型或工具调用。
        message_text: 消息数据，可能来自用户、频道、模型或工具调用。
        mentions: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        timestamp: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if is_group_message:
            chat_id = group_id or sender_number
            if not self.config.group.enabled:
                self.logger.info("Ignoring group message from {} (groups disabled)", chat_id)
                return False, chat_id
            if (
                self.config.group.policy == "allowlist"
                and chat_id not in self.config.group.allow_from
            ):
                self.logger.info(
                    "Ignoring group message from {} (policy: {})",
                    chat_id,
                    self.config.group.policy,
                )
                return False, chat_id

            self._add_to_group_buffer(
                group_id=chat_id,
                sender_name=sender_name or sender_number,
                sender_number=sender_number,
                message_text=message_text,
                timestamp=timestamp,
            )

            is_command = bool(message_text and message_text.strip().startswith("/"))
            if not is_command and not self._should_respond_in_group(message_text, mentions):
                self.logger.info(
                    "Ignoring group message (require_mention: {})",
                    self.config.group.require_mention,
                )
                return False, chat_id
            return True, chat_id

        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        chat_id = sender_number
        if not self.config.dm.enabled:
            self.logger.debug("Ignoring DM from {} (DMs disabled)", sender_id)
            return False, chat_id
        if self.config.dm.policy == "allowlist":
            if not self._sender_matches_allowlist(sender_id, self.config.dm.allow_from):
                self.logger.debug(
                    "Ignoring DM from {} (policy: {})", sender_id, self.config.dm.policy
                )
                return False, chat_id
        return True, chat_id

    def _assemble_inbound_content(
        self,
        *,
        sender_name: str | None,
        sender_number: str,
        message_text: str,
        attachments: list,
        mentions: list,
        is_group_message: bool,
        chat_id: str,
    ) -> tuple[str, list[str]]:
        """执行辅助逻辑（_assemble_inbound_content = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._assemble_inbound_content` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_number: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        message_text: 消息数据，可能来自用户、频道、模型或工具调用。
        attachments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        mentions: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        is_group_message: 消息数据，可能来自用户、频道、模型或工具调用。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        content_parts: list[str] = []
        media_paths: list[str] = []

        if is_group_message:
            buffer_context = self._get_group_buffer_context(chat_id)
            if buffer_context:
                content_parts.append(f"[Recent group messages for context:]\n{buffer_context}\n---")

        if message_text:
            if is_group_message:
                message_text = self._strip_bot_mention(message_text, mentions)
                display_name = sender_name or sender_number
                message_text = f"[{display_name}]: {message_text}"
            content_parts.append(message_text)

        if attachments:
            media_dir = get_media_dir("signal")
            for attachment in attachments:
                attachment_id = attachment.get("id")
                content_type = attachment.get("contentType", "")
                filename = attachment.get("filename") or f"attachment_{attachment_id}"
                if not attachment_id:
                    continue
                try:
                    source_path = self._signal_attachments_dir() / attachment_id
                    if source_path.exists():
                        dest_path = media_dir / f"signal_{safe_filename(filename)}"
                        shutil.copy2(source_path, dest_path)
                        media_paths.append(str(dest_path))
                        media_type = content_type.split("/")[0] if "/" in content_type else "file"
                        if media_type not in ("image", "audio", "video"):
                            media_type = "file"
                        content_parts.append(f"[{media_type}: {dest_path}]")
                        self.logger.debug("Downloaded attachment: {} -> {}", filename, dest_path)
                    else:
                        self.logger.warning("Attachment not found: {}", source_path)
                        content_parts.append(f"[attachment: {filename} - not found]")
                except Exception as e:
                    self.logger.warning("Failed to process attachment {}: {}", filename, e)
                    content_parts.append(f"[attachment: {filename} - error]")

        content = "\n".join(content_parts) if content_parts else "[empty message]"
        return content, media_paths

    def _add_to_group_buffer(
        self,
        group_id: str,
        sender_name: str,
        sender_number: str,
        message_text: str,
        timestamp: int | None,
    ) -> None:
        """执行辅助逻辑（_add_to_group_buffer = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._add_to_group_buffer` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        group_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sender_number: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        message_text: 消息数据，可能来自用户、频道、模型或工具调用。
        timestamp: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if group_id not in self._group_buffers:
            self._group_buffers[group_id] = deque(maxlen=self.config.group_message_buffer_size)

        # 中文说明：这一段围绕消息、调用处理，注意输入、输出和异常路径。
        self._group_buffers[group_id].append(
            {
                "sender_name": sender_name,
                "sender_number": sender_number,
                "content": message_text,
                "timestamp": timestamp,
            }
        )

        self.logger.debug(
            "Added message to group buffer {}: {}/{}",
            group_id,
            len(self._group_buffers[group_id]),
            self.config.group_message_buffer_size,
        )

    def _get_group_buffer_context(self, group_id: str) -> str:
        """执行辅助逻辑（_get_group_buffer_context = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._get_group_buffer_context` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        group_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if group_id not in self._group_buffers:
            return ""

        buffer = self._group_buffers[group_id]
        if len(buffer) <= 1:  # 中文说明：这一段围绕消息、上下文处理，注意输入、输出和异常路径。
            return ""

        # 中文说明：这一段围绕消息、格式处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕上下文处理，注意输入、输出和异常路径。
        context_messages = list(buffer)[:-1]  # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。

        lines = []
        for msg in context_messages:
            sender = msg["sender_name"]
            content = msg["content"][:200]  # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
            lines.append(f"{sender}: {content}")

        return "\n".join(lines)

    def _signal_attachments_dir(self) -> Path:
        """执行辅助逻辑（_signal_attachments_dir = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._signal_attachments_dir` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        configured = self.config.attachments_dir
        if configured:
            return Path(configured).expanduser()
        return Path.home() / ".local/share/signal-cli/attachments"

    @staticmethod
    def _normalize_signal_id(value: str) -> list[str]:
        """标准化数据（_normalize_signal_id = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._normalize_signal_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        raw = value.strip()
        if not raw:
            return []

        normalized = [raw, raw.lower()]
        if raw.startswith("+") and len(raw) > 1:
            normalized.append(raw[1:])
        elif raw.isdigit():
            normalized.append(f"+{raw}")
        return list(dict.fromkeys(normalized))

    @classmethod
    def _sender_matches_allowlist(cls, sender_id: str, allow_list: list[str]) -> bool:
        """发送输出消息（_sender_matches_allowlist = 原函数名）。

        【中文名称】发送输出消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._sender_matches_allowlist` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        allow_list: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not allow_list:
            return False
        sender_variants: set[str] = set()
        for part in str(sender_id).split("|"):
            sender_variants.update(cls._normalize_signal_id(part))
        if not sender_variants:
            return False
        allow_variants: set[str] = set()
        for entry in allow_list:
            for part in str(entry).split("|"):
                allow_variants.update(cls._normalize_signal_id(part))
        return bool(sender_variants & allow_variants)

    def _remember_account_id_alias(self, value: str | None) -> None:
        """执行辅助逻辑（_remember_account_id_alias = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._remember_account_id_alias` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not value:
            return
        if not isinstance(value, str):
            return
        for candidate in self._normalize_signal_id(value):
            self._account_id_aliases.add(candidate)

    def _id_matches_account(self, value: str | None) -> bool:
        """执行辅助逻辑（_id_matches_account = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._id_matches_account` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not value:
            return False
        if not isinstance(value, str):
            return False
        return any(
            candidate in self._account_id_aliases for candidate in self._normalize_signal_id(value)
        )

    @staticmethod
    def _collect_sender_id_parts(envelope: dict[str, Any]) -> list[str]:
        """执行辅助逻辑（_collect_sender_id_parts = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._collect_sender_id_parts` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        envelope: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        parts: list[str] = []
        for key in (
            "sourceNumber",
            "source",
            "sourceUuid",
            "sourceServiceId",
            "sourceAci",
            "sourceACI",
        ):
            value = envelope.get(key)
            if not isinstance(value, str):
                continue
            candidate = value.strip()
            if candidate and candidate not in parts:
                parts.append(candidate)
        return parts

    @staticmethod
    def _primary_sender_id(sender_parts: list[str]) -> str:
        """执行辅助逻辑（_primary_sender_id = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._primary_sender_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        sender_parts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        for part in sender_parts:
            if part.startswith("+") or part.isdigit():
                return part
        return sender_parts[0] if sender_parts else ""

    @staticmethod
    def _extract_group_id(group_info: Any, group_v2: Any) -> str | None:
        """提取信息（_extract_group_id = 原函数名）。

        【中文名称】提取信息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._extract_group_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        group_info: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        group_v2: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        for group_obj in (group_info, group_v2):
            if not isinstance(group_obj, dict):
                continue
            for key in ("groupId", "id", "groupID"):
                value = group_obj.get(key)
                if isinstance(value, str) and value:
                    return value
        return None

    @staticmethod
    def _mention_id_candidates(mention: dict[str, Any]) -> list[str]:
        """执行辅助逻辑（_mention_id_candidates = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._mention_id_candidates` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        mention: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        ids: list[str] = []

        def _walk(value: dict[str, Any] | Any, depth: int = 0) -> None:
            """执行辅助逻辑（_walk = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `SignalChannel._walk` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            depth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if depth > 2:
                return
            if not isinstance(value, dict):
                return
            for key, child in value.items():
                key_lower = str(key).lower()
                if isinstance(child, str) and child:
                    if any(token in key_lower for token in ("number", "uuid", "serviceid", "aci")):
                        ids.append(child)
                elif isinstance(child, dict):
                    _walk(child, depth + 1)

        _walk(mention)
        return list(dict.fromkeys(ids))

    @staticmethod
    def _mention_span(mention: dict[str, Any]) -> tuple[int, int] | None:
        """执行辅助逻辑（_mention_span = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._mention_span` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        mention: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            start = int(mention.get("start", 0))
            length = int(mention.get("length", 0))
        except (TypeError, ValueError):
            return None

        if start < 0 or length <= 0:
            return None
        return (start, length)

    @staticmethod
    def _leading_placeholder_span(text: str | None) -> tuple[int, int] | None:
        """执行辅助逻辑（_leading_placeholder_span = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._leading_placeholder_span` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not text:
            return None

        start = 0
        while start < len(text) and text[start].isspace():
            start += 1

        if start >= len(text):
            return None

        marker = text[start]
        if marker not in ("\ufffc", "\ufffd", "\x1b"):
            return None

        next_index = start + 1
        if next_index < len(text) and not text[next_index].isspace():
            return None

        return (start, 1)

    def _should_respond_in_group(self, message_text: str, mentions: list[dict[str, Any]]) -> bool:
        """执行辅助逻辑（_should_respond_in_group = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._should_respond_in_group` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message_text: 消息数据，可能来自用户、频道、模型或工具调用。
        mentions: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if not self.config.group.require_mention:
            return True

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            for mention_id in self._mention_id_candidates(mention):
                if self._id_matches_account(mention_id):
                    return True

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            if self._mention_id_candidates(mention):
                continue
            span = self._mention_span(mention)
            if not span:
                continue
            start, _ = span
            if message_text is not None and not message_text[:start].strip():
                self.logger.debug("Accepting identifier-less leading mention as bot mention")
                return True

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这一段围绕消息处理，注意输入、输出和异常路径。
        if not mentions and self._leading_placeholder_span(message_text):
            self.logger.debug("Accepting leading placeholder mention without mention metadata")
            return True

        # 中文说明：普通文本。
        if message_text and self.config.phone_number:
            for account_id in self._normalize_signal_id(self.config.phone_number):
                if account_id and account_id in message_text:
                    return True

        return False

    def _strip_bot_mention(self, text: str, mentions: list[dict[str, Any]]) -> str:
        """执行辅助逻辑（_strip_bot_mention = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._strip_bot_mention` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        mentions: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not text:
            return text

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        bot_mentions = []
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            mention_ids = self._mention_id_candidates(mention)
            span = self._mention_span(mention)
            if not span:
                continue

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if any(self._id_matches_account(mention_id) for mention_id in mention_ids):
                bot_mentions.append(span)
                continue

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            if not mention_ids:
                start, _ = span
                if not text[:start].strip():
                    bot_mentions.append(span)

        if not bot_mentions:
            placeholder_span = self._leading_placeholder_span(text)
            if placeholder_span:
                bot_mentions.append(placeholder_span)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这一段围绕事件处理，注意输入、输出和异常路径。
        bot_mentions.sort(reverse=True)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for start, length in bot_mentions:
            if start >= len(text):
                continue
            end = min(len(text), start + length)
            text = text[:start] + text[end:]

        return text.strip()

    @staticmethod
    def _is_group_chat_id(chat_id: str) -> bool:
        """判断条件是否成立（_is_group_chat_id = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._is_group_chat_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return "=" in chat_id or (len(chat_id) > 40 and "-" not in chat_id)

    def _recipient_params(self, chat_id: str) -> dict[str, Any]:
        """执行辅助逻辑（_recipient_params = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._recipient_params` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._is_group_chat_id(chat_id):
            return {"groupId": chat_id}
        return {"recipient": [chat_id]}

    async def _start_typing(self, chat_id: str) -> None:
        """异步启动流程（_start_typing = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._start_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._stop_typing(chat_id, send_stop=False)
        await self._send_typing(chat_id)
        self._typing_tasks[chat_id] = asyncio.create_task(self._typing_loop(chat_id))

    async def _stop_typing(self, chat_id: str, send_stop: bool = True) -> None:
        """异步停止流程（_stop_typing = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._stop_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        send_stop: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        task = self._typing_tasks.pop(chat_id, None)
        had_task = task is not None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if send_stop and had_task:
            await self._send_typing(chat_id, stop=True)

    async def _typing_loop(self, chat_id: str) -> None:
        """异步执行辅助逻辑（_typing_loop = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._typing_loop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            while self._running:
                await asyncio.sleep(self._TYPING_REFRESH_SECONDS)
                await self._send_typing(chat_id, quiet_success=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.debug("Typing indicator loop stopped for {}: {}", chat_id, e)

    async def _send_typing(
        self, chat_id: str, stop: bool = False, quiet_success: bool = False
    ) -> None:
        """异步发送消息（_send_typing = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._send_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        stop: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        quiet_success: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        action = "stop" if stop else "start"
        if (
            not self._is_group_chat_id(chat_id)
            and chat_id.startswith("+") is False
            and chat_id not in self._typing_uuid_warnings
        ):
            self._typing_uuid_warnings.add(chat_id)
            self.logger.warning(
                "Signal DM recipient is UUID-only (no phone number in envelope). "
                "Some Signal clients may not render typing indicators for this recipient form."
            )
        candidate_params: list[dict[str, Any]]
        if self._is_group_chat_id(chat_id):
            candidate_params = [{"groupId": chat_id}, {"groupId": [chat_id]}]
        else:
            candidate_params = [{"recipient": chat_id}, {"recipient": [chat_id]}]

        last_error: Any | None = None
        for params in candidate_params:
            if stop:
                params["stop"] = True
            try:
                response = await self._send_request("sendTyping", params)
            except Exception as e:
                last_error = str(e)
                continue

            if "error" not in response:
                if not quiet_success:
                    self.logger.info("Signal typing {} sent for {}", action, chat_id)
                return

            last_error = response["error"]

        self.logger.warning(
            "Failed to send Signal typing {} for {}: {}", action, chat_id, last_error
        )

    async def _ensure_typing_indicators_enabled(self) -> None:
        """异步确保前置条件成立（_ensure_typing_indicators_enabled = 原函数名）。

        【中文名称】确保前置条件成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._ensure_typing_indicators_enabled` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        response = await self._send_request("updateConfiguration", {"typingIndicators": True})
        if "error" in response:
            self.logger.warning(
                "Failed to enable Signal typing indicators: {}", response["error"]
            )
        else:
            self.logger.info("Signal typing indicators enabled on account configuration")

    async def _send_request(
        self, method: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """异步发送消息（_send_request = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._send_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        method: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        params: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这一段围绕请求处理，注意输入、输出和异常路径。
        self._request_id += 1
        request_id = self._request_id

        # 中文说明：这一段围绕请求、JSON处理，注意输入、输出和异常路径。
        request = {"jsonrpc": "2.0", "method": method, "id": request_id}

        if params:
            request["params"] = params

        return await self._send_http_request(request)

    async def _send_http_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """异步发送消息（_send_http_request = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Signal 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `SignalChannel._send_http_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        request: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            raise RuntimeError("Not connected to signal-cli daemon")

        try:
            response = await self._http.post("/api/v1/rpc", json=request)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error("HTTP request failed: {}", e)
            return {"error": {"message": str(e)}}

