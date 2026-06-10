"""通用辅助函数。

【中文名称】通用辅助函数

【功能说明】
负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

import base64
import json
import re
import shutil
import time
import uuid
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import tiktoken
from loguru import logger


def strip_think(text: str) -> str:
    """执行 `strip_think`。

    【中文名称】strip_think

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r"<think>[\s\S]*?</think>", "", text)
    text = re.sub(r"^\s*<think>[\s\S]*$", "", text)
    text = re.sub(r"<thought>[\s\S]*?</thought>", "", text)
    text = re.sub(r"^\s*<thought>[\s\S]*$", "", text)
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r"<think(?![A-Za-z0-9_\-:>/])", "", text)
    text = re.sub(r"<thought(?![A-Za-z0-9_\-:>/])", "", text)
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r"^\s*</think>\s*", "", text)
    text = re.sub(r"\s*</think>\s*$", "", text)
    text = re.sub(r"^\s*</thought>\s*", "", text)
    text = re.sub(r"\s*</thought>\s*$", "", text)
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    text = re.sub(r"^\s*<\|?channel\|?>\s*", "", text)
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    partial_control_tag = (
        r"</?(?:t|th|thi|thin|think|tho|thou|thoug|though|thought)>?"
        r"|<\|?(?:c|ch|cha|chan|chann|channe|channel)(?:\|?>?)?"
    )
    text = re.sub(rf"(?:{partial_control_tag})$", "", text)
    text = re.sub(r"^\s*<\|?$", "", text)
    return text.strip()


def extract_think(text: str) -> tuple[str | None, str]:
    """执行 `extract_think`。

    【中文名称】extract_think

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    parts: list[str] = []
    for m in re.finditer(r"<think>([\s\S]*?)</think>", text):
        parts.append(m.group(1).strip())
    for m in re.finditer(r"<thought>([\s\S]*?)</thought>", text):
        parts.append(m.group(1).strip())
    thinking = "\n\n".join(parts) if parts else None
    return thinking, strip_think(text)


class IncrementalThinkExtractor:
    """IncrementalThinkExtractor 类。

    【中文名称】IncrementalThinkExtractor

    【功能说明】
    这是 通用辅助函数 中的核心数据结构或服务类。负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    __slots__ = ("_emitted",)

    def __init__(self) -> None:
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._emitted = ""

    def reset(self) -> None:
        """执行 `reset`。

        【中文名称】reset

        【功能说明】
        这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._emitted = ""

    async def feed(self, buf: str, emit: Any) -> bool:
        """异步执行 `feed`。

        【中文名称】feed

        【功能说明】
        这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - buf: 调用方传入的 `buf` 数据；具体类型以函数签名为准。
        - emit: 调用方传入的 `emit` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        thinking, _ = extract_think(buf)
        if not thinking or thinking == self._emitted:
            return False
        new = thinking[len(self._emitted):].strip()
        self._emitted = thinking
        if not new:
            return False
        await emit(new)
        return True


def extract_reasoning(
    reasoning_content: str | None,
    thinking_blocks: list[dict[str, Any]] | None,
    content: str | None,
) -> tuple[str | None, str | None]:
    """执行 `extract_reasoning`。

    【中文名称】extract_reasoning

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - reasoning_content: 调用方传入的 `reasoning_content` 数据；具体类型以函数签名为准。
    - thinking_blocks: 调用方传入的 `thinking_blocks` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if reasoning_content:
        return reasoning_content, strip_think(content) if content else content
    if thinking_blocks:
        parts = [
            tb.get("thinking", "")
            for tb in thinking_blocks
            if isinstance(tb, dict) and tb.get("type") == "thinking"
        ]
        joined = "\n\n".join(p for p in parts if p)
        return (joined or None), strip_think(content) if content else content
    if content:
        return extract_think(content)
    return None, content


def detect_image_mime(data: bytes) -> str | None:
    """执行 `detect_image_mime`。

    【中文名称】detect_image_mime

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def build_image_content_blocks(
    raw: bytes, mime: str, path: str, label: str
) -> list[dict[str, Any]]:
    """执行 `build_image_content_blocks`。

    【中文名称】build_image_content_blocks

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - raw: 调用方传入的 `raw` 数据；具体类型以函数签名为准。
    - mime: 调用方传入的 `mime` 数据；具体类型以函数签名为准。
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。
    - label: 调用方传入的 `label` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    b64 = base64.b64encode(raw).decode()
    return [
        {
            "type": "image_url",
            "image_url": {"url": f"data:{mime};base64,{b64}"},
            "_meta": {"path": path},
        },
        {"type": "text", "text": label},
    ]


def ensure_dir(path: Path) -> Path:
    """执行 `ensure_dir`。

    【中文名称】ensure_dir

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp() -> str:
    """执行 `timestamp`。

    【中文名称】timestamp

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return datetime.now().isoformat()


def current_time_str(timezone: str | None = None) -> str:
    """执行 `current_time_str`。

    【中文名称】current_time_str

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - timezone: 调用方传入的 `timezone` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    from zoneinfo import ZoneInfo

    try:
        tz = ZoneInfo(timezone) if timezone else None
    except (KeyError, Exception):
        tz = None

    now = datetime.now(tz=tz) if tz else datetime.now().astimezone()
    offset = now.strftime("%z")
    offset_fmt = f"{offset[:3]}:{offset[3:]}" if len(offset) == 5 else offset
    tz_name = timezone or (time.strftime("%Z") or "UTC")
    return f"{now.strftime('%Y-%m-%d %H:%M (%A)')} ({tz_name}, UTC{offset_fmt})"


_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*]')
_TOOL_RESULT_PREVIEW_CHARS = 1200
_TOOL_RESULTS_DIR = ".nanobot/tool-results"
_TOOL_RESULT_RETENTION_SECS = 7 * 24 * 60 * 60
_TOOL_RESULT_MAX_BUCKETS = 32


def safe_filename(name: str) -> str:
    """执行 `safe_filename`。

    【中文名称】safe_filename

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return _UNSAFE_CHARS.sub("_", name).strip()


def image_placeholder_text(path: str | None, *, empty: str = "[image]") -> str:
    """执行 `image_placeholder_text`。

    【中文名称】image_placeholder_text

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。
    - empty: 调用方传入的 `empty` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return f"[image: {path}]" if path else empty


def truncate_text(text: str, max_chars: int) -> str:
    """执行 `truncate_text`。

    【中文名称】truncate_text

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
    - max_chars: 调用方传入的 `max_chars` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... (truncated)"


def find_legal_message_start(messages: list[dict[str, Any]]) -> int:
    """执行 `find_legal_message_start`。

    【中文名称】find_legal_message_start

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    declared: set[str] = set()
    start = 0
    for i, msg in enumerate(messages):
        role = msg.get("role")
        if role == "assistant":
            for tc in msg.get("tool_calls") or []:
                if isinstance(tc, dict) and tc.get("id"):
                    declared.add(str(tc["id"]))
        elif role == "tool":
            tid = msg.get("tool_call_id")
            if tid and str(tid) not in declared:
                start = i + 1
                declared.clear()
    return start


def stringify_text_blocks(content: list[dict[str, Any]]) -> str | None:
    """执行 `stringify_text_blocks`。

    【中文名称】stringify_text_blocks

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            return None
        if block.get("type") != "text":
            return None
        text = block.get("text")
        if not isinstance(text, str):
            return None
        parts.append(text)
    return "\n".join(parts)


def _render_tool_result_reference(
    filepath: Path,
    *,
    original_size: int,
    preview: str,
    truncated_preview: bool,
) -> str:
    """执行 `_render_tool_result_reference`。

    【中文名称】_render_tool_result_reference

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - filepath: 调用方传入的 `filepath` 数据；具体类型以函数签名为准。
    - original_size: 调用方传入的 `original_size` 数据；具体类型以函数签名为准。
    - preview: 调用方传入的 `preview` 数据；具体类型以函数签名为准。
    - truncated_preview: 调用方传入的 `truncated_preview` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    result = (
        f"[tool output persisted]\n"
        f"Full output saved to: {filepath}\n"
        f"Original size: {original_size} chars\n"
        f"Preview:\n{preview}"
    )
    if truncated_preview:
        result += "\n...\n(Read the saved file if you need the full output.)"
    return result


def _bucket_mtime(path: Path) -> float:
    """执行 `_bucket_mtime`。

    【中文名称】_bucket_mtime

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _cleanup_tool_result_buckets(root: Path, current_bucket: Path) -> None:
    """执行 `_cleanup_tool_result_buckets`。

    【中文名称】_cleanup_tool_result_buckets

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - root: 调用方传入的 `root` 数据；具体类型以函数签名为准。
    - current_bucket: 调用方传入的 `current_bucket` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    siblings = [path for path in root.iterdir() if path.is_dir() and path != current_bucket]
    cutoff = time.time() - _TOOL_RESULT_RETENTION_SECS
    for path in siblings:
        if _bucket_mtime(path) < cutoff:
            shutil.rmtree(path, ignore_errors=True)
    keep = max(_TOOL_RESULT_MAX_BUCKETS - 1, 0)
    siblings = [path for path in siblings if path.exists()]
    if len(siblings) <= keep:
        return
    siblings.sort(key=_bucket_mtime, reverse=True)
    for path in siblings[keep:]:
        shutil.rmtree(path, ignore_errors=True)


def _write_text_atomic(path: Path, content: str) -> None:
    """执行 `_write_text_atomic`。

    【中文名称】_write_text_atomic

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def maybe_persist_tool_result(
    workspace: Path | None,
    session_key: str | None,
    tool_call_id: str,
    content: Any,
    *,
    max_chars: int,
) -> Any:
    """执行 `maybe_persist_tool_result`。

    【中文名称】maybe_persist_tool_result

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - workspace: 调用方传入的 `workspace` 数据；具体类型以函数签名为准。
    - session_key: 调用方传入的 `session_key` 数据；具体类型以函数签名为准。
    - tool_call_id: 调用方传入的 `tool_call_id` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - max_chars: 调用方传入的 `max_chars` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if workspace is None or max_chars <= 0:
        return content

    text_payload: str | None = None
    suffix = "txt"
    if isinstance(content, str):
        text_payload = content
    elif isinstance(content, list):
        text_payload = stringify_text_blocks(content)
        if text_payload is None:
            return content
        suffix = "json"
    else:
        return content

    if len(text_payload) <= max_chars:
        return content

    root = ensure_dir(workspace / _TOOL_RESULTS_DIR)
    bucket = ensure_dir(root / safe_filename(session_key or "default"))
    try:
        _cleanup_tool_result_buckets(root, bucket)
    except Exception:
        logger.exception("Failed to clean stale tool result buckets in {}", root)
    path = bucket / f"{safe_filename(tool_call_id)}.{suffix}"
    if not path.exists():
        if suffix == "json" and isinstance(content, list):
            _write_text_atomic(path, json.dumps(content, ensure_ascii=False, indent=2))
        else:
            _write_text_atomic(path, text_payload)

    preview = text_payload[:_TOOL_RESULT_PREVIEW_CHARS]
    return _render_tool_result_reference(
        path,
        original_size=len(text_payload),
        preview=preview,
        truncated_preview=len(text_payload) > _TOOL_RESULT_PREVIEW_CHARS,
    )


def split_message(content: str, max_len: int = 2000) -> list[str]:
    """执行 `split_message`。

    【中文名称】split_message

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - max_len: 调用方传入的 `max_len` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not content:
        return []
    if len(content) <= max_len:
        return [content]
    chunks: list[str] = []
    while content:
        if len(content) <= max_len:
            chunks.append(content)
            break
        cut = content[:max_len]
        # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
        pos = cut.rfind("\n")
        if pos <= 0:
            pos = cut.rfind(" ")
        if pos <= 0:
            pos = max_len
        chunks.append(content[:pos])
        content = content[pos:].lstrip()
    return chunks


def build_assistant_message(
    content: str | None,
    tool_calls: list[dict[str, Any]] | None = None,
    reasoning_content: str | None = None,
    thinking_blocks: list[dict] | None = None,
) -> dict[str, Any]:
    """执行 `build_assistant_message`。

    【中文名称】build_assistant_message

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - tool_calls: 调用方传入的 `tool_calls` 数据；具体类型以函数签名为准。
    - reasoning_content: 调用方传入的 `reasoning_content` 数据；具体类型以函数签名为准。
    - thinking_blocks: 调用方传入的 `thinking_blocks` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    msg: dict[str, Any] = {"role": "assistant", "content": content or ""}
    if tool_calls:
        msg["tool_calls"] = tool_calls
    if reasoning_content is not None or thinking_blocks:
        msg["reasoning_content"] = reasoning_content if reasoning_content is not None else ""
    if thinking_blocks:
        msg["thinking_blocks"] = thinking_blocks
    return msg


def estimate_prompt_tokens(
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
) -> int:
    """执行 `estimate_prompt_tokens`。

    【中文名称】estimate_prompt_tokens

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
    - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        enc = tiktoken.get_encoding("cl100k_base")
        parts: list[str] = []
        for msg in messages:
            content = msg.get("content")
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        txt = part.get("text", "")
                        if txt:
                            parts.append(txt)

            tc = msg.get("tool_calls")
            if tc:
                parts.append(json.dumps(tc, ensure_ascii=False))

            rc = msg.get("reasoning_content")
            if isinstance(rc, str) and rc:
                parts.append(rc)

            for key in ("name", "tool_call_id"):
                value = msg.get(key)
                if isinstance(value, str) and value:
                    parts.append(value)

        if tools:
            parts.append(json.dumps(tools, ensure_ascii=False))

        per_message_overhead = len(messages) * 4
        return len(enc.encode("\n".join(parts))) + per_message_overhead
    except Exception:
        return 0


def estimate_message_tokens(message: dict[str, Any]) -> int:
    """执行 `estimate_message_tokens`。

    【中文名称】estimate_message_tokens

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    content = message.get("content")
    parts: list[str] = []
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                text = part.get("text", "")
                if text:
                    parts.append(text)
            else:
                parts.append(json.dumps(part, ensure_ascii=False))
    elif content is not None:
        parts.append(json.dumps(content, ensure_ascii=False))

    for key in ("name", "tool_call_id"):
        value = message.get(key)
        if isinstance(value, str) and value:
            parts.append(value)
    if message.get("tool_calls"):
        parts.append(json.dumps(message["tool_calls"], ensure_ascii=False))

    rc = message.get("reasoning_content")
    if isinstance(rc, str) and rc:
        parts.append(rc)

    payload = "\n".join(parts)
    if not payload:
        return 4
    try:
        enc = tiktoken.get_encoding("cl100k_base")
        return max(4, len(enc.encode(payload)) + 4)
    except Exception:
        return max(4, len(payload) // 4 + 4)


def estimate_prompt_tokens_chain(
    provider: Any,
    model: str | None,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
) -> tuple[int, str]:
    """执行 `estimate_prompt_tokens_chain`。

    【中文名称】estimate_prompt_tokens_chain

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - provider: 调用方传入的 `provider` 数据；具体类型以函数签名为准。
    - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
    - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    provider_counter = getattr(provider, "estimate_prompt_tokens", None)
    if callable(provider_counter):
        with suppress(Exception):
            tokens, source = provider_counter(messages, tools, model)
            if isinstance(tokens, (int, float)) and tokens > 0:
                return int(tokens), str(source or "provider_counter")
    estimated = estimate_prompt_tokens(messages, tools)
    if estimated > 0:
        return int(estimated), "tiktoken"
    return 0, "none"


def build_status_content(
    *,
    version: str,
    model: str,
    start_time: float,
    last_usage: dict[str, int],
    context_window_tokens: int,
    session_msg_count: int,
    context_tokens_estimate: int,
    search_usage_text: str | None = None,
    active_task_count: int = 0,
    max_completion_tokens: int = 8192,
) -> str:
    """执行 `build_status_content`。

    【中文名称】build_status_content

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - version: 调用方传入的 `version` 数据；具体类型以函数签名为准。
    - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
    - start_time: 调用方传入的 `start_time` 数据；具体类型以函数签名为准。
    - last_usage: 调用方传入的 `last_usage` 数据；具体类型以函数签名为准。
    - context_window_tokens: 调用方传入的 `context_window_tokens` 数据；具体类型以函数签名为准。
    - session_msg_count: 调用方传入的 `session_msg_count` 数据；具体类型以函数签名为准。
    - context_tokens_estimate: 调用方传入的 `context_tokens_estimate` 数据；具体类型以函数签名为准。
    - search_usage_text: 调用方传入的 `search_usage_text` 数据；具体类型以函数签名为准。
    - active_task_count: 调用方传入的 `active_task_count` 数据；具体类型以函数签名为准。
    - max_completion_tokens: 调用方传入的 `max_completion_tokens` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    uptime_s = int(time.time() - start_time)
    uptime = (
        f"{uptime_s // 3600}h {(uptime_s % 3600) // 60}m"
        if uptime_s >= 3600
        else f"{uptime_s // 60}m {uptime_s % 60}s"
    )
    last_in = last_usage.get("prompt_tokens", 0)
    last_out = last_usage.get("completion_tokens", 0)
    cached = last_usage.get("cached_tokens", 0)
    ctx_total = max(context_window_tokens, 0)
    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    ctx_budget = max(ctx_total - int(max_completion_tokens) - 1024, 1)
    ctx_pct = min(int((context_tokens_estimate / ctx_budget) * 100), 999) if ctx_budget > 0 else 0
    ctx_used_str = (
        f"{context_tokens_estimate // 1000}k"
        if context_tokens_estimate >= 1000
        else str(context_tokens_estimate)
    )
    ctx_total_str = f"{ctx_total // 1000}k" if ctx_total > 0 else "n/a"
    token_line = f"\U0001f4ca Tokens: {last_in} in / {last_out} out"
    if cached and last_in:
        token_line += f" ({cached * 100 // last_in}% cached)"
    lines = [
        f"\U0001f408 nanobot v{version}",
        f"\U0001f9e0 Model: {model}",
        token_line,
        f"\U0001f4da Context: {ctx_used_str}/{ctx_total_str} ({ctx_pct}% of input budget)",
        f"\U0001f4ac Session: {session_msg_count} messages",
        f"\u23f1 Uptime: {uptime}",
        f"\u26a1 Tasks: {active_task_count} active",
    ]
    if search_usage_text:
        lines.append(search_usage_text)
    return "\n".join(lines)


def sync_workspace_templates(workspace: Path, silent: bool = False) -> list[str]:
    """执行 `sync_workspace_templates`。

    【中文名称】sync_workspace_templates

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - workspace: 调用方传入的 `workspace` 数据；具体类型以函数签名为准。
    - silent: 调用方传入的 `silent` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    from importlib.resources import files as pkg_files

    try:
        tpl = pkg_files("nanobot") / "templates"
    except Exception:
        return []
    if not tpl.is_dir():
        return []

    added: list[str] = []

    def _write(src, dest: Path):
        """执行 `_write`。

        【中文名称】_write

        【功能说明】
        这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - src: 调用方传入的 `src` 数据；具体类型以函数签名为准。
        - dest: 调用方传入的 `dest` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        content = src.read_text(encoding="utf-8") if src else ""
        if dest.exists():
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
        added.append(str(dest.relative_to(workspace)))

    for item in tpl.iterdir():
        if item.name.endswith(".md") and not item.name.startswith("."):
            _write(item, workspace / item.name)
    _write(tpl / "memory" / "MEMORY.md", workspace / "memory" / "MEMORY.md")
    _write(None, workspace / "memory" / "history.jsonl")
    (workspace / "skills").mkdir(exist_ok=True)

    if added and not silent:
        from rich.console import Console

        for name in added:
            Console().print(f"  [dim]Created {name}[/dim]")

    # 说明：这里处理 通用辅助函数 的协议细节或边界情况，避免外部差异影响核心流程。
    try:
        from nanobot.utils.gitstore import GitStore

        gs = GitStore(
            workspace,
            tracked_files=[
                "SOUL.md",
                "USER.md",
                "memory/MEMORY.md",
            ],
        )
        gs.init()
    except Exception:
        logger.exception("Failed to initialize git store for {}", workspace)

    return added


def load_bundled_template(template_name: str) -> str | None:
    """执行 `load_bundled_template`。

    【中文名称】load_bundled_template

    【功能说明】
    这是 通用辅助函数 中的一个步骤函数，用来支撑：负责消息切分、模板同步、目录复制、文本处理等多个子系统共享的小工具。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - template_name: 调用方传入的 `template_name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    from importlib.resources import files as pkg_files

    with suppress(Exception):
        tpl = pkg_files("nanobot") / "templates" / template_name
        if tpl.is_file():
            return tpl.read_text(encoding="utf-8")
    return None
