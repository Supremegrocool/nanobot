"""OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。

【中文名称】Provider 辅助模块：nanobot/providers/openai_responses/converters.py

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

import json
from typing import Any

from nanobot.providers.base import tool_arguments_json_for_replay


def convert_messages(messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """转换格式（convert_messages = 原函数名）。

    【中文名称】转换格式

    【功能说明】
    这是 Provider 辅助模块 中的一个关键步骤。OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。
    在阅读 `convert_messages` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    messages: 消息数据，可能来自用户、频道、模型或工具调用。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    system_prompt = ""
    input_items: list[dict[str, Any]] = []
    used_item_ids: set[str] = set()

    for idx, msg in enumerate(messages):
        role = msg.get("role")
        content = msg.get("content")

        if role == "system":
            system_prompt = content if isinstance(content, str) else ""
            continue

        if role == "user":
            input_items.append(convert_user_message(content))
            continue

        if role == "assistant":
            if isinstance(content, str) and content:
                message_id = _unique_item_id(f"msg_{idx}", used_item_ids)
                input_items.append({
                    "type": "message", "role": "assistant",
                    "content": [{"type": "output_text", "text": content}],
                    "status": "completed", "id": message_id,
                })
            for tool_call in msg.get("tool_calls", []) or []:
                fn = tool_call.get("function") or {}
                call_id, item_id = split_tool_call_id(tool_call.get("id"))
                response_item_id = _unique_item_id(item_id or f"fc_{idx}", used_item_ids)
                input_items.append({
                    "type": "function_call",
                    "id": response_item_id,
                    "call_id": call_id or f"call_{idx}",
                    "name": fn.get("name"),
                    "arguments": tool_arguments_json_for_replay(fn.get("arguments")),
                })
            continue

        if role == "tool":
            call_id, _ = split_tool_call_id(msg.get("tool_call_id"))
            output_text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
            input_items.append({"type": "function_call_output", "call_id": call_id, "output": output_text})

    return system_prompt, input_items


def convert_user_message(content: Any) -> dict[str, Any]:
    """转换格式（convert_user_message = 原函数名）。

    【中文名称】转换格式

    【功能说明】
    这是 Provider 辅助模块 中的一个关键步骤。OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。
    在阅读 `convert_user_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if isinstance(content, str):
        return {"role": "user", "content": [{"type": "input_text", "text": content}]}
    if isinstance(content, list):
        converted: list[dict[str, Any]] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                converted.append({"type": "input_text", "text": item.get("text", "")})
            elif item.get("type") == "image_url":
                url = (item.get("image_url") or {}).get("url")
                if url:
                    converted.append({"type": "input_image", "image_url": url, "detail": "auto"})
        if converted:
            return {"role": "user", "content": converted}
    return {"role": "user", "content": [{"type": "input_text", "text": ""}]}


def convert_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """转换格式（convert_tools = 原函数名）。

    【中文名称】转换格式

    【功能说明】
    这是 Provider 辅助模块 中的一个关键步骤。OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。
    在阅读 `convert_tools` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    converted: list[dict[str, Any]] = []
    for tool in tools:
        fn = (tool.get("function") or {}) if tool.get("type") == "function" else tool
        name = fn.get("name")
        if not name:
            continue
        params = fn.get("parameters") or {}
        converted.append({
            "type": "function",
            "name": name,
            "description": fn.get("description") or "",
            "parameters": params if isinstance(params, dict) else {},
        })
    return converted


def _unique_item_id(item_id: str, used: set[str]) -> str:
    """执行辅助逻辑（_unique_item_id = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 辅助模块 中的一个关键步骤。OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。
    在阅读 `_unique_item_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    item_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    used: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if item_id not in used:
        used.add(item_id)
        return item_id

    suffix = 2
    while f"{item_id}_{suffix}" in used:
        suffix += 1
    unique = f"{item_id}_{suffix}"
    used.add(unique)
    return unique


def split_tool_call_id(tool_call_id: Any) -> tuple[str, str | None]:
    """切分内容（split_tool_call_id = 原函数名）。

    【中文名称】切分内容

    【功能说明】
    这是 Provider 辅助模块 中的一个关键步骤。OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。
    在阅读 `split_tool_call_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_call_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if isinstance(tool_call_id, str) and tool_call_id:
        if "|" in tool_call_id:
            call_id, item_id = tool_call_id.split("|", 1)
            return call_id, item_id or None
        return tool_call_id, None
    return "call_0", None

