"""提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。

【中文名称】工具模块：nanobot/utils/helpers.py

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
    """执行辅助逻辑（strip_think = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `strip_think` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    text = re.sub(r"<think>[\s\S]*?</think>", "", text)
    text = re.sub(r"^\s*<think>[\s\S]*$", "", text)
    text = re.sub(r"<thought>[\s\S]*?</thought>", "", text)
    text = re.sub(r"^\s*<thought>[\s\S]*$", "", text)
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    # the primary fix for `<think广场…` leaks.
    text = re.sub(r"<think(?![A-Za-z0-9_\-:>/])", "", text)
    text = re.sub(r"<thought(?![A-Za-z0-9_\-:>/])", "", text)
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    text = re.sub(r"^\s*</think>\s*", "", text)
    text = re.sub(r"\s*</think>\s*$", "", text)
    text = re.sub(r"^\s*</thought>\s*", "", text)
    text = re.sub(r"\s*</thought>\s*$", "", text)
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    text = re.sub(r"^\s*<\|?channel\|?>\s*", "", text)
    # 中文说明：这一段围绕流式输出处理，注意输入、输出和异常路径。
    # 中文说明：这一段围绕令牌处理，注意输入、输出和异常路径。
    partial_control_tag = (
        r"</?(?:t|th|thi|thin|think|tho|thou|thoug|though|thought)>?"
        r"|<\|?(?:c|ch|cha|chan|chann|channe|channel)(?:\|?>?)?"
    )
    text = re.sub(rf"(?:{partial_control_tag})$", "", text)
    text = re.sub(r"^\s*<\|?$", "", text)
    return text.strip()


def extract_think(text: str) -> tuple[str | None, str]:
    """提取信息（extract_think = 原函数名）。

    【中文名称】提取信息

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `extract_think` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    parts: list[str] = []
    for m in re.finditer(r"<think>([\s\S]*?)</think>", text):
        parts.append(m.group(1).strip())
    for m in re.finditer(r"<thought>([\s\S]*?)</thought>", text):
        parts.append(m.group(1).strip())
    thinking = "\n\n".join(parts) if parts else None
    return thinking, strip_think(text)


class IncrementalThinkExtractor:
    """IncrementalThinkExtractor 类，封装 工具模块 的核心状态和行为。

    【中文名称】IncrementalThinkExtractor

    【功能说明】
    提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    __slots__ = ("_emitted",)

    def __init__(self) -> None:
        self._emitted = ""

    def reset(self) -> None:
        self._emitted = ""

    async def feed(self, buf: str, emit: Any) -> bool:
        """异步执行辅助逻辑（feed = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
        在阅读 `IncrementalThinkExtractor.feed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        buf: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        emit: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
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
    """提取信息（extract_reasoning = 原函数名）。

    【中文名称】提取信息

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `extract_reasoning` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    reasoning_content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    thinking_blocks: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（detect_image_mime = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `detect_image_mime` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """构建对象（build_image_content_blocks = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `build_image_content_blocks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    mime: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。
    label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """确保前置条件成立（ensure_dir = 原函数名）。

    【中文名称】确保前置条件成立

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `ensure_dir` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp() -> str:
    """执行辅助逻辑（timestamp = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `timestamp` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return datetime.now().isoformat()


def current_time_str(timezone: str | None = None) -> str:
    """执行辅助逻辑（current_time_str = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `current_time_str` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    timezone: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（safe_filename = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `safe_filename` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return _UNSAFE_CHARS.sub("_", name).strip()


def image_placeholder_text(path: str | None, *, empty: str = "[image]") -> str:
    """执行辅助逻辑（image_placeholder_text = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `image_placeholder_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。
    empty: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return f"[image: {path}]" if path else empty


def truncate_text(text: str, max_chars: int) -> str:
    """执行辅助逻辑（truncate_text = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `truncate_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_chars: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... (truncated)"


def find_legal_message_start(messages: list[dict[str, Any]]) -> int:
    """启动流程（find_legal_message_start = 原函数名）。

    【中文名称】启动流程

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `find_legal_message_start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    messages: 消息数据，可能来自用户、频道、模型或工具调用。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（stringify_text_blocks = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `stringify_text_blocks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """渲染内容（_render_tool_result_reference = 原函数名）。

    【中文名称】渲染内容

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `_render_tool_result_reference` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    filepath: 文件或路径信息，代码会按安全边界读取或写入。
    original_size: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preview: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    truncated_preview: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（_bucket_mtime = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `_bucket_mtime` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _cleanup_tool_result_buckets(root: Path, current_bucket: Path) -> None:
    """清理资源（_cleanup_tool_result_buckets = 原函数名）。

    【中文名称】清理资源

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `_cleanup_tool_result_buckets` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    root: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    current_bucket: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（_write_text_atomic = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `_write_text_atomic` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（maybe_persist_tool_result = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `maybe_persist_tool_result` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    workspace: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    session_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    tool_call_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_chars: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """切分内容（split_message = 原函数名）。

    【中文名称】切分内容

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `split_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_len: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
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
    """构建对象（build_assistant_message = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `build_assistant_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    tool_calls: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    reasoning_content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    thinking_blocks: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（estimate_prompt_tokens = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `estimate_prompt_tokens` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    messages: 消息数据，可能来自用户、频道、模型或工具调用。
    tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（estimate_message_tokens = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `estimate_message_tokens` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    message: 消息数据，可能来自用户、频道、模型或工具调用。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（estimate_prompt_tokens_chain = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `estimate_prompt_tokens_chain` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    provider: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    messages: 消息数据，可能来自用户、频道、模型或工具调用。
    tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """构建对象（build_status_content = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `build_status_content` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    version: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    start_time: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    last_usage: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    context_window_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    session_msg_count: 消息数据，可能来自用户、频道、模型或工具调用。
    context_tokens_estimate: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    search_usage_text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    active_task_count: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_completion_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
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
    """执行辅助逻辑（sync_workspace_templates = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `sync_workspace_templates` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    workspace: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    silent: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    from importlib.resources import files as pkg_files

    try:
        tpl = pkg_files("nanobot") / "templates"
    except Exception:
        return []
    if not tpl.is_dir():
        return []

    added: list[str] = []

    def _write(src, dest: Path):
        """执行辅助逻辑（_write = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
        在阅读 `_write` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        src: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        dest: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
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

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
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
    """加载数据（load_bundled_template = 原函数名）。

    【中文名称】加载数据

    【功能说明】
    这是 工具模块 中的一个关键步骤。提供文本清洗、消息切分、媒体识别和通用转换等辅助函数。
    在阅读 `load_bundled_template` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    template_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    from importlib.resources import files as pkg_files

    with suppress(Exception):
        tpl = pkg_files("nanobot") / "templates" / template_name
        if tpl.is_file():
            return tpl.read_text(encoding="utf-8")
    return None

