"""子 Agent 展示工具。

【中文名称】子 Agent 展示工具

【功能说明】
负责把子 Agent 的渠道名称、状态和输出整理成主会话可读的展示文本。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

from typing import Any

# 说明：这里处理 子 Agent 展示工具 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 子 Agent 展示工具 的协议细节或边界情况，避免外部差异影响核心流程。
_SUBAGENT_CHANNEL_RESULT_MAX_CHARS = 800


def scrub_subagent_announce_body(content: str) -> str:
    """执行 `scrub_subagent_announce_body`。

    【中文名称】scrub_subagent_announce_body

    【功能说明】
    这是 子 Agent 展示工具 中的一个步骤函数，用来支撑：负责把子 Agent 的渠道名称、状态和输出整理成主会话可读的展示文本。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `scrub_subagent_messages_for_channel`。

    【中文名称】scrub_subagent_messages_for_channel

    【功能说明】
    这是 子 Agent 展示工具 中的一个步骤函数，用来支撑：负责把子 Agent 的渠道名称、状态和输出整理成主会话可读的展示文本。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("injected_event") != "subagent_result":
            continue
        raw = msg.get("content")
        if not isinstance(raw, str) or not raw.strip():
            continue
        msg["content"] = scrub_subagent_announce_body(raw)
