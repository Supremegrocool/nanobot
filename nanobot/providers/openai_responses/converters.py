"""OpenAI Responses API 转换器。

【中文名称】OpenAI Responses API 转换器

【功能说明】
负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import json
from typing import Any

from nanobot.providers.base import tool_arguments_json_for_replay


def convert_messages(messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """执行 `convert_messages`。

    【中文名称】convert_messages

    【功能说明】
    这是 OpenAI Responses API 转换器 中的一个步骤函数，用来支撑：负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `convert_user_message`。

    【中文名称】convert_user_message

    【功能说明】
    这是 OpenAI Responses API 转换器 中的一个步骤函数，用来支撑：负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `convert_tools`。

    【中文名称】convert_tools

    【功能说明】
    这是 OpenAI Responses API 转换器 中的一个步骤函数，用来支撑：负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_unique_item_id`。

    【中文名称】_unique_item_id

    【功能说明】
    这是 OpenAI Responses API 转换器 中的一个步骤函数，用来支撑：负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - item_id: 调用方传入的 `item_id` 数据；具体类型以函数签名为准。
    - used: 调用方传入的 `used` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `split_tool_call_id`。

    【中文名称】split_tool_call_id

    【功能说明】
    这是 OpenAI Responses API 转换器 中的一个步骤函数，用来支撑：负责把 nanobot 内部消息和工具 schema 转换成 Responses API 接受的 input/tools 结构。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_call_id: 调用方传入的 `tool_call_id` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if isinstance(tool_call_id, str) and tool_call_id:
        if "|" in tool_call_id:
            call_id, item_id = tool_call_id.split("|", 1)
            return call_id, item_id or None
        return tool_call_id, None
    return "call_0", None
