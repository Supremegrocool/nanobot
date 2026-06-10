"""把子 Agent 的来源和状态整理成频道可读的展示文本。

【中文名称】工具模块：nanobot/utils/subagent_channel_display.py

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

from typing import Any

# 中文说明：这一段围绕WebSocket、会话处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕WebSocket、API处理，注意输入、输出和异常路径。
_SUBAGENT_CHANNEL_RESULT_MAX_CHARS = 800


def scrub_subagent_announce_body(content: str) -> str:
    """执行辅助逻辑（scrub_subagent_announce_body = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。把子 Agent 的来源和状态整理成频道可读的展示文本。
    在阅读 `scrub_subagent_announce_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    stripped = content.replace("\r\n", "\n").strip()
    lines = stripped.splitlines()
    header = ""
    if lines and lines[0].startswith("[Subagent"):
        header = lines[0].strip()

    lower = stripped.lower()
    key = "\nresult:\n"
    ri = lower.find(key)
    if ri == -1:
        key = "\nresult:"
        ri = lower.find(key)
    if ri == -1:
        return header if header else stripped

    after = stripped[ri + len(key) :].lstrip()
    summ_marker = "summarize this naturally"
    si = after.lower().find(summ_marker)
    if si != -1:
        after = after[:si].rstrip()

    body = after.strip()
    limit = _SUBAGENT_CHANNEL_RESULT_MAX_CHARS
    if limit and len(body) > limit:
        body = body[: limit - 1].rstrip() + "…"
    if header and body:
        return f"{header}\n\n{body}"
    return header or body or stripped


def scrub_subagent_messages_for_channel(messages: list[dict[str, Any]]) -> None:
    """执行辅助逻辑（scrub_subagent_messages_for_channel = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。把子 Agent 的来源和状态整理成频道可读的展示文本。
    在阅读 `scrub_subagent_messages_for_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    messages: 消息数据，可能来自用户、频道、模型或工具调用。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("injected_event") != "subagent_result":
            continue
        raw = msg.get("content")
        if not isinstance(raw, str) or not raw.strip():
            continue
        msg["content"] = scrub_subagent_announce_body(raw)

