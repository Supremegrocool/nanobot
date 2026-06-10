"""OpenAI Responses API 的辅助转换模块，负责在内部消息结构和 Responses 事件之间做适配。

【中文名称】Provider 辅助模块：nanobot/providers/openai_responses/__init__.py

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

from nanobot.providers.openai_responses.converters import (
    convert_messages,
    convert_tools,
    convert_user_message,
    split_tool_call_id,
)
from nanobot.providers.openai_responses.parsing import (
    FINISH_REASON_MAP,
    consume_sdk_stream,
    consume_sse,
    consume_sse_with_reasoning,
    iter_sse,
    map_finish_reason,
    parse_response_output,
)

__all__ = [
    "convert_messages",
    "convert_tools",
    "convert_user_message",
    "split_tool_call_id",
    "iter_sse",
    "consume_sse",
    "consume_sse_with_reasoning",
    "consume_sdk_stream",
    "map_finish_reason",
    "parse_response_output",
    "FINISH_REASON_MAP",
]

