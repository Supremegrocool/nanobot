"""Signal 渠道适配器。

【中文名称】Signal 渠道适配器

【功能说明】
负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """_Run 类。

    【中文名称】_Run

    【功能说明】
    这是 Signal 渠道适配器 中的核心数据结构或服务类。负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    text: str
    styles: frozenset[str] = field(default_factory=frozenset)
    opaque: bool = False  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


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

# 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
_SIG_CELL_STRIP_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"\*\*(.+?)\*\*"), r"\1"),
    (re.compile(r"__(.+?)__"), r"\1"),
    (re.compile(r"~~(.+?)~~"), r"\1"),
    (re.compile(r"`([^`]+)`"), r"\1"),
)


def _utf16_len(s: str) -> int:
    """执行 `_utf16_len`。

    【中文名称】_utf16_len

    【功能说明】
    这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return len(s.encode("utf-16-le")) // 2


def _sig_strip_cell(s: str) -> str:
    """执行 `_sig_strip_cell`。

    【中文名称】_sig_strip_cell

    【功能说明】
    这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    for pattern, repl in _SIG_CELL_STRIP_PATTERNS:
        s = pattern.sub(repl, s)
    return s.strip()


def _sig_render_table(table_lines: list[str]) -> str:
    """执行 `_sig_render_table`。

    【中文名称】_sig_render_table

    【功能说明】
    这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - table_lines: 调用方传入的 `table_lines` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    def dw(s: str) -> int:
        """执行 `dw`。

        【中文名称】dw

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """执行 `dr`。

        【中文名称】dr

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - cells: 调用方传入的 `cells` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return "  ".join(f"{c}{' ' * (w - dw(c))}" for c, w in zip(cells, widths))

    out = [dr(rows[0])]
    out.append("  ".join("─" * w for w in widths))
    for row in rows[1:]:
        out.append(dr(row))
    return "\n".join(out)


def _markdown_to_signal(text: str) -> tuple[str, list[str]]:
    """执行 `_markdown_to_signal`。

    【中文名称】_markdown_to_signal

    【功能说明】
    这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not text:
        return text, []

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    protected: list[str] = []

    def save_code(m: re.Match) -> str:
        """执行 `save_code`。

        【中文名称】save_code

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - m: 调用方传入的 `m` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        protected.append(m.group(1))
        return f"\x00C{len(protected) - 1}\x00"

    text = _SIG_CODE_BLOCK_RE.sub(save_code, text)

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    runs: list[_Run] = [_Run(text)]

    def transform(
        pattern: re.Pattern,
        make_runs: Callable[[re.Match, frozenset[str]], list[_Run]],
    ) -> None:
        """执行 `transform`。

        【中文名称】transform

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - pattern: 调用方传入的 `pattern` 数据；具体类型以函数签名为准。
        - make_runs: 调用方传入的 `make_runs` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(
        _SIG_TOKEN_RE,
        lambda m, s: [_Run(protected[int(m.group(1))], s | {"MONOSPACE"}, opaque=True)],
    )

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_INLINE_CODE_RE, lambda m, s: [_Run(m.group(1), s | {"MONOSPACE"}, opaque=True)])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_HEADER_RE, lambda m, s: [_Run(m.group(1), s | {"BOLD"})])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_BLOCKQUOTE_RE, lambda m, s: [_Run(m.group(1), s)])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_BULLET_RE, lambda m, s: [_Run("• ", s)])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_OLIST_RE, lambda m, s: [_Run(m.group(1) + ". ", s)])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    def _link_runs(m: re.Match, s: frozenset) -> list[_Run]:
        """执行 `_link_runs`。

        【中文名称】_link_runs

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - m: 调用方传入的 `m` 数据；具体类型以函数签名为准。
        - s: 调用方传入的 `s` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        link_text, url = m.group(1), m.group(2)

        def _norm(u: str) -> str:
            """执行 `_norm`。

            【中文名称】_norm

            【功能说明】
            这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - u: 调用方传入的 `u` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            return re.sub(r"^https?://(www\.)?", "", u).rstrip("/").lower()

        if _norm(url) == _norm(link_text):
            return [_Run(url, s)]
        return [_Run(f"{link_text} ({url})", s)]

    transform(_SIG_LINK_RE, _link_runs)

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_BOLD_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"BOLD"})])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_ITALIC_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"ITALIC"})])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    transform(_SIG_STRIKE_RE, lambda m, s: [_Run(m.group(1) or m.group(2), s | {"STRIKETHROUGH"})])

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
    """执行 `_partition_styles`。

    【中文名称】_partition_styles

    【功能说明】
    这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - plain_text: 调用方传入的 `plain_text` 数据；具体类型以函数签名为准。
    - chunks: 调用方传入的 `chunks` 数据；具体类型以函数签名为准。
    - text_styles: 调用方传入的 `text_styles` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not chunks:
        return []
    if not text_styles:
        return [[] for _ in chunks]

    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    chunk_ranges: list[tuple[int, int]] = []
    cursor = 0  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
    """SignalDMConfig 类。

    【中文名称】SignalDMConfig

    【功能说明】
    这是 Signal 渠道适配器 中的核心数据结构或服务类。负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    policy: str = "allowlist"  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    allow_from: list[str] = Field(default_factory=list)  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


class SignalGroupConfig(Base):
    """SignalGroupConfig 类。

    【中文名称】SignalGroupConfig

    【功能说明】
    这是 Signal 渠道适配器 中的核心数据结构或服务类。负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    policy: str = "allowlist"  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    allow_from: list[str] = Field(default_factory=list)  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    require_mention: bool = True  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。


class SignalConfig(Base):
    """SignalConfig 类。

    【中文名称】SignalConfig

    【功能说明】
    这是 Signal 渠道适配器 中的核心数据结构或服务类。负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    phone_number: str = ""  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    daemon_host: str = "localhost"
    daemon_port: int = 8080
    group_message_buffer_size: int = 20  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    attachments_dir: str | None = None
    dm: SignalDMConfig = Field(default_factory=SignalDMConfig)
    group: SignalGroupConfig = Field(default_factory=SignalGroupConfig)

    @field_validator("group_message_buffer_size")
    @classmethod
    def _validate_buffer_size(cls, v: int) -> int:
        """执行 `_validate_buffer_size`。

        【中文名称】_validate_buffer_size

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - v: 调用方传入的 `v` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if v <= 0:
            raise ValueError("group_message_buffer_size must be > 0")
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def allow_from(self) -> list[str]:
        """执行 `allow_from`。

        【中文名称】allow_from

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return list(dict.fromkeys(self.dm.allow_from + self.group.allow_from))


class SignalChannel(BaseChannel):
    """SignalChannel 类。

    【中文名称】SignalChannel

    【功能说明】
    这是 Signal 渠道适配器 中的核心数据结构或服务类。负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "signal"
    display_name = "Signal"
    _TYPING_REFRESH_SECONDS = 10.0
    _MAX_MESSAGE_LEN = 64_000  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    _HTTP_TIMEOUT_SECONDS = 60.0

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return SignalConfig().model_dump(by_alias=True)

    def __init__(self, config: SignalConfig, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._group_buffers: dict[str, deque] = {}

    def is_allowed(self, sender_id: str) -> bool:
        """执行 `is_allowed`。

        【中文名称】is_allowed

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_sender_approved_via_pairing`。

        【中文名称】_sender_approved_via_pairing

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_handle_message`。

        【中文名称】_handle_message

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
        - media: 调用方传入的 `media` 数据；具体类型以函数签名为准。
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。
        - session_key: 调用方传入的 `session_key` 数据；具体类型以函数签名为准。
        - is_dm: 调用方传入的 `is_dm` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.config.phone_number:
            self.logger.error("Signal account not configured")
            return

        self._running = True
        await self._start_http_mode()

    async def _start_http_mode(self) -> None:
        """异步执行 `_start_http_mode`。

        【中文名称】_start_http_mode

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        base_url = f"http://{self.config.daemon_host}:{self.config.daemon_port}"
        reconnect_delay_s = 1.0
        max_reconnect_delay_s = 30.0

        while self._running:
            try:
                self.logger.info("Connecting to signal-cli daemon at {}...", base_url)

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                self._http = httpx.AsyncClient(
                    timeout=self._HTTP_TIMEOUT_SECONDS, base_url=base_url
                )

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                reconnect_delay_s = 1.0

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                await self._ensure_typing_indicators_enabled()

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if self._sse_task:
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        for chat_id in list(self._typing_tasks):
            await self._stop_typing(chat_id)

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if self._http:
            await self._http.aclose()
            self._http = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not is_progress_message:
                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                await self._stop_typing(msg.chat_id, send_stop=False)

    async def _sse_receive_loop(self) -> None:
        """异步执行 `_sse_receive_loop`。

        【中文名称】_sse_receive_loop

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                event_buffer = []

                async for line in response.aiter_lines():
                    if not self._running:
                        break

                    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    if line and line != ":":
                        self.logger.debug("SSE line received: {}", line[:200])

                    # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    if isinstance(line, str):
                        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        if not line or line == ":":
                            if event_buffer:
                                # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

                        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        elif line.startswith("data:"):
                            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                            event_buffer.append(line[6:] if line[5:6] == " " else line[5:])

                        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                        elif line.startswith("event:"):
                            pass  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

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
        """异步执行 `_safe_handle`。

        【中文名称】_safe_handle

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - action: 调用方传入的 `action` 数据；具体类型以函数签名为准。
        - payload: 调用方传入的 `payload` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        try:
            yield
        except Exception as e:
            snippet = repr(payload)[:200] if payload is not None else ""
            text = f"Error in {action}: {e}"
            if snippet:
                text += f" | payload={snippet}"
            self.logger.opt(exception=True).error(text)

    async def _handle_receive_notification(self, params: dict[str, Any]) -> None:
        """异步执行 `_handle_receive_notification`。

        【中文名称】_handle_receive_notification

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - params: 调用方传入的 `params` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self.logger.debug("_handle_receive_notification called with: {}", params)
        async with self._safe_handle("receive notification", params):
            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            envelope = params.get("envelope", {})

            self.logger.debug("Extracted envelope: {}", envelope)

            if not envelope:
                self.logger.debug("No envelope found in params")
                return

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            sender_parts = self._collect_sender_id_parts(envelope)
            source_name = envelope.get("sourceName")

            if not sender_parts:
                self.logger.debug("Received message without source, skipping")
                return

            sender_number = self._primary_sender_id(sender_parts)
            sender_id = "|".join(sender_parts)

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if any(self._id_matches_account(part) for part in sender_parts):
                for part in sender_parts:
                    self._remember_account_id_alias(part)

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            data_message = envelope.get("dataMessage")
            sync_message = envelope.get("syncMessage")
            typing_message = envelope.get("typingMessage")
            receipt_message = envelope.get("receiptMessage")

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if receipt_message:
                return

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if data_message:
                await self._handle_data_message(sender_id, sender_number, data_message, source_name)

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            elif sync_message and sync_message.get("sentMessage"):
                sent_msg = sync_message["sentMessage"]
                destination = sent_msg.get("destination") or sent_msg.get("destinationNumber")
                if destination:
                    self.logger.debug(
                        "Sync message sent to {}: {}", destination, sent_msg.get("message", "")[:50]
                    )

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            elif typing_message:
                pass  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    async def _handle_data_message(
        self,
        sender_id: str,
        sender_number: str,
        data_message: dict[str, Any],
        sender_name: str | None,
    ) -> None:
        """异步执行 `_handle_data_message`。

        【中文名称】_handle_data_message

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。
        - sender_number: 调用方传入的 `sender_number` 数据；具体类型以函数签名为准。
        - data_message: 调用方传入的 `data_message` 数据；具体类型以函数签名为准。
        - sender_name: 调用方传入的 `sender_name` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `_check_inbound_policy`。

        【中文名称】_check_inbound_policy

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。
        - sender_number: 调用方传入的 `sender_number` 数据；具体类型以函数签名为准。
        - group_id: 调用方传入的 `group_id` 数据；具体类型以函数签名为准。
        - is_group_message: 调用方传入的 `is_group_message` 数据；具体类型以函数签名为准。
        - message_text: 调用方传入的 `message_text` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。
        - sender_name: 调用方传入的 `sender_name` 数据；具体类型以函数签名为准。
        - timestamp: 调用方传入的 `timestamp` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `_assemble_inbound_content`。

        【中文名称】_assemble_inbound_content

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_name: 调用方传入的 `sender_name` 数据；具体类型以函数签名为准。
        - sender_number: 调用方传入的 `sender_number` 数据；具体类型以函数签名为准。
        - message_text: 调用方传入的 `message_text` 数据；具体类型以函数签名为准。
        - attachments: 调用方传入的 `attachments` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。
        - is_group_message: 调用方传入的 `is_group_message` 数据；具体类型以函数签名为准。
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_add_to_group_buffer`。

        【中文名称】_add_to_group_buffer

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - group_id: 调用方传入的 `group_id` 数据；具体类型以函数签名为准。
        - sender_name: 调用方传入的 `sender_name` 数据；具体类型以函数签名为准。
        - sender_number: 调用方传入的 `sender_number` 数据；具体类型以函数签名为准。
        - message_text: 调用方传入的 `message_text` 数据；具体类型以函数签名为准。
        - timestamp: 调用方传入的 `timestamp` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if group_id not in self._group_buffers:
            self._group_buffers[group_id] = deque(maxlen=self.config.group_message_buffer_size)

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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
        """执行 `_get_group_buffer_context`。

        【中文名称】_get_group_buffer_context

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - group_id: 调用方传入的 `group_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if group_id not in self._group_buffers:
            return ""

        buffer = self._group_buffers[group_id]
        if len(buffer) <= 1:  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            return ""

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        context_messages = list(buffer)[:-1]  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

        lines = []
        for msg in context_messages:
            sender = msg["sender_name"]
            content = msg["content"][:200]  # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            lines.append(f"{sender}: {content}")

        return "\n".join(lines)

    def _signal_attachments_dir(self) -> Path:
        """执行 `_signal_attachments_dir`。

        【中文名称】_signal_attachments_dir

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        configured = self.config.attachments_dir
        if configured:
            return Path(configured).expanduser()
        return Path.home() / ".local/share/signal-cli/attachments"

    @staticmethod
    def _normalize_signal_id(value: str) -> list[str]:
        """执行 `_normalize_signal_id`。

        【中文名称】_normalize_signal_id

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_sender_matches_allowlist`。

        【中文名称】_sender_matches_allowlist

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_id: 调用方传入的 `sender_id` 数据；具体类型以函数签名为准。
        - allow_list: 调用方传入的 `allow_list` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_remember_account_id_alias`。

        【中文名称】_remember_account_id_alias

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not value:
            return
        if not isinstance(value, str):
            return
        for candidate in self._normalize_signal_id(value):
            self._account_id_aliases.add(candidate)

    def _id_matches_account(self, value: str | None) -> bool:
        """执行 `_id_matches_account`。

        【中文名称】_id_matches_account

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not value:
            return False
        if not isinstance(value, str):
            return False
        return any(
            candidate in self._account_id_aliases for candidate in self._normalize_signal_id(value)
        )

    @staticmethod
    def _collect_sender_id_parts(envelope: dict[str, Any]) -> list[str]:
        """执行 `_collect_sender_id_parts`。

        【中文名称】_collect_sender_id_parts

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - envelope: 调用方传入的 `envelope` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_primary_sender_id`。

        【中文名称】_primary_sender_id

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender_parts: 调用方传入的 `sender_parts` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        for part in sender_parts:
            if part.startswith("+") or part.isdigit():
                return part
        return sender_parts[0] if sender_parts else ""

    @staticmethod
    def _extract_group_id(group_info: Any, group_v2: Any) -> str | None:
        """执行 `_extract_group_id`。

        【中文名称】_extract_group_id

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - group_info: 调用方传入的 `group_info` 数据；具体类型以函数签名为准。
        - group_v2: 调用方传入的 `group_v2` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_mention_id_candidates`。

        【中文名称】_mention_id_candidates

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - mention: 调用方传入的 `mention` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        ids: list[str] = []

        def _walk(value: dict[str, Any] | Any, depth: int = 0) -> None:
            """执行 `_walk`。

            【中文名称】_walk

            【功能说明】
            这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。
            - depth: 调用方传入的 `depth` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """执行 `_mention_span`。

        【中文名称】_mention_span

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - mention: 调用方传入的 `mention` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_leading_placeholder_span`。

        【中文名称】_leading_placeholder_span

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_should_respond_in_group`。

        【中文名称】_should_respond_in_group

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message_text: 调用方传入的 `message_text` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if not self.config.group.require_mention:
            return True

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            for mention_id in self._mention_id_candidates(mention):
                if self._id_matches_account(mention_id):
                    return True

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
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

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if not mentions and self._leading_placeholder_span(message_text):
            self.logger.debug("Accepting leading placeholder mention without mention metadata")
            return True

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if message_text and self.config.phone_number:
            for account_id in self._normalize_signal_id(self.config.phone_number):
                if account_id and account_id in message_text:
                    return True

        return False

    def _strip_bot_mention(self, text: str, mentions: list[dict[str, Any]]) -> str:
        """执行 `_strip_bot_mention`。

        【中文名称】_strip_bot_mention

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
        - mentions: 调用方传入的 `mentions` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not text:
            return text

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        bot_mentions = []
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            mention_ids = self._mention_id_candidates(mention)
            span = self._mention_span(mention)
            if not span:
                continue

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if any(self._id_matches_account(mention_id) for mention_id in mention_ids):
                bot_mentions.append(span)
                continue

            # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if not mention_ids:
                start, _ = span
                if not text[:start].strip():
                    bot_mentions.append(span)

        if not bot_mentions:
            placeholder_span = self._leading_placeholder_span(text)
            if placeholder_span:
                bot_mentions.append(placeholder_span)

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        bot_mentions.sort(reverse=True)

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        for start, length in bot_mentions:
            if start >= len(text):
                continue
            end = min(len(text), start + length)
            text = text[:start] + text[end:]

        return text.strip()

    @staticmethod
    def _is_group_chat_id(chat_id: str) -> bool:
        """执行 `_is_group_chat_id`。

        【中文名称】_is_group_chat_id

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return "=" in chat_id or (len(chat_id) > 40 and "-" not in chat_id)

    def _recipient_params(self, chat_id: str) -> dict[str, Any]:
        """执行 `_recipient_params`。

        【中文名称】_recipient_params

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._is_group_chat_id(chat_id):
            return {"groupId": chat_id}
        return {"recipient": [chat_id]}

    async def _start_typing(self, chat_id: str) -> None:
        """异步执行 `_start_typing`。

        【中文名称】_start_typing

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._stop_typing(chat_id, send_stop=False)
        await self._send_typing(chat_id)
        self._typing_tasks[chat_id] = asyncio.create_task(self._typing_loop(chat_id))

    async def _stop_typing(self, chat_id: str, send_stop: bool = True) -> None:
        """异步执行 `_stop_typing`。

        【中文名称】_stop_typing

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - send_stop: 调用方传入的 `send_stop` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_typing_loop`。

        【中文名称】_typing_loop

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_send_typing`。

        【中文名称】_send_typing

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - stop: 调用方传入的 `stop` 数据；具体类型以函数签名为准。
        - quiet_success: 调用方传入的 `quiet_success` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_ensure_typing_indicators_enabled`。

        【中文名称】_ensure_typing_indicators_enabled

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `_send_request`。

        【中文名称】_send_request

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - method: 调用方传入的 `method` 数据；具体类型以函数签名为准。
        - params: 调用方传入的 `params` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._request_id += 1
        request_id = self._request_id

        # 说明：这里处理 Signal 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        request = {"jsonrpc": "2.0", "method": method, "id": request_id}

        if params:
            request["params"] = params

        return await self._send_http_request(request)

    async def _send_http_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """异步执行 `_send_http_request`。

        【中文名称】_send_http_request

        【功能说明】
        这是 Signal 渠道适配器 中的一个步骤函数，用来支撑：负责驱动 signal-cli JSON-RPC，解析 Signal 消息、附件和群组元数据，并通过 Signal 发送 Agent 回复。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - request: 调用方传入的 `request` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._http:
            raise RuntimeError("Not connected to signal-cli daemon")

        try:
            response = await self._http.post("/api/v1/rpc", json=request)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error("HTTP request failed: {}", e)
            return {"error": {"message": str(e)}}
