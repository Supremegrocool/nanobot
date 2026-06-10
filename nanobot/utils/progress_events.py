"""进度事件工具。

【中文名称】进度事件工具

【功能说明】
负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from nanobot.agent.hook import AgentHookContext


def on_progress_accepts_tool_events(cb: Callable[..., Any]) -> bool:
    """执行 `on_progress_accepts_tool_events`。

    【中文名称】on_progress_accepts_tool_events

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - cb: 调用方传入的 `cb` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return _on_progress_accepts(cb, "tool_events")


def on_progress_accepts_file_edit_events(cb: Callable[..., Any]) -> bool:
    """执行 `on_progress_accepts_file_edit_events`。

    【中文名称】on_progress_accepts_file_edit_events

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - cb: 调用方传入的 `cb` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return _on_progress_accepts(cb, "file_edit_events")


def _on_progress_accepts(cb: Callable[..., Any], name: str) -> bool:
    """执行 `_on_progress_accepts`。

    【中文名称】_on_progress_accepts

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - cb: 调用方传入的 `cb` 数据；具体类型以函数签名为准。
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    try:
        sig = inspect.signature(cb)
    except (TypeError, ValueError):
        return False
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        return True
    return name in sig.parameters


async def invoke_on_progress(
    on_progress: Callable[..., Awaitable[None]],
    content: str,
    *,
    tool_hint: bool = False,
    tool_events: list[dict[str, Any]] | None = None,
) -> None:
    """异步执行 `invoke_on_progress`。

    【中文名称】invoke_on_progress

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - on_progress: 调用方传入的 `on_progress` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - tool_hint: 调用方传入的 `tool_hint` 数据；具体类型以函数签名为准。
    - tool_events: 调用方传入的 `tool_events` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    if tool_events and on_progress_accepts_tool_events(on_progress):
        await on_progress(content, tool_hint=tool_hint, tool_events=tool_events)
        return
    await on_progress(content, tool_hint=tool_hint)


async def invoke_file_edit_progress(
    on_progress: Callable[..., Awaitable[None]],
    file_edit_events: list[dict[str, Any]],
) -> None:
    """异步执行 `invoke_file_edit_progress`。

    【中文名称】invoke_file_edit_progress

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - on_progress: 调用方传入的 `on_progress` 数据；具体类型以函数签名为准。
    - file_edit_events: 调用方传入的 `file_edit_events` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    if not file_edit_events or not on_progress_accepts_file_edit_events(on_progress):
        return
    await on_progress("", file_edit_events=file_edit_events)


def _tool_event_arguments(tool_call: Any) -> dict[str, Any]:
    """执行 `_tool_event_arguments`。

    【中文名称】_tool_event_arguments

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_call: 调用方传入的 `tool_call` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    arguments = getattr(tool_call, "arguments", {}) or {}
    return arguments if isinstance(arguments, dict) else {}


def build_tool_event_start_payload(tool_call: Any) -> dict[str, Any]:
    """执行 `build_tool_event_start_payload`。

    【中文名称】build_tool_event_start_payload

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_call: 调用方传入的 `tool_call` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return {
        "version": 1,
        "phase": "start",
        "call_id": str(getattr(tool_call, "id", "") or ""),
        "name": getattr(tool_call, "name", ""),
        "arguments": _tool_event_arguments(tool_call),
        "result": None,
        "error": None,
        "files": [],
        "embeds": [],
    }


def tool_event_result_extras(result: Any) -> tuple[list[Any], list[Any]]:
    """执行 `tool_event_result_extras`。

    【中文名称】tool_event_result_extras

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - result: 调用方传入的 `result` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    if not isinstance(result, dict):
        return [], []
    files = result.get("files") if isinstance(result.get("files"), list) else []
    embeds = result.get("embeds") if isinstance(result.get("embeds"), list) else []
    return files, embeds


def build_tool_event_finish_payloads(context: AgentHookContext) -> list[dict[str, Any]]:
    """执行 `build_tool_event_finish_payloads`。

    【中文名称】build_tool_event_finish_payloads

    【功能说明】
    这是 进度事件工具 中的一个步骤函数，用来支撑：负责构造 Agent/工具执行过程中的结构化进度事件，供渠道和 WebUI 统一展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - context: 调用方传入的 `context` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    payloads: list[dict[str, Any]] = []
    count = min(len(context.tool_calls), len(context.tool_results), len(context.tool_events))
    for idx in range(count):
        tool_call = context.tool_calls[idx]
        result = context.tool_results[idx]
        event = context.tool_events[idx] if isinstance(context.tool_events[idx], dict) else {}
        status = event.get("status")
        phase = "end" if status == "ok" else "error"
        files, embeds = tool_event_result_extras(result)
        payload = {
            "version": 1,
            "phase": phase,
            "call_id": str(getattr(tool_call, "id", "") or ""),
            "name": getattr(tool_call, "name", ""),
            "arguments": _tool_event_arguments(tool_call),
            "result": result if phase == "end" else None,
            "error": None,
            "files": files,
            "embeds": embeds,
        }
        if phase == "error":
            if isinstance(result, str) and result.strip():
                payload["error"] = result.strip()
            else:
                payload["error"] = str(event.get("detail") or "Tool execution failed")
        payloads.append(payload)
    return payloads
