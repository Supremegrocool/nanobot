"""OpenAI Responses API 适配入口。

【中文名称】OpenAI Responses API 适配入口

【功能说明】
集中导出 Responses API 的消息转换、工具转换、流解析和输出解析工具，供 OpenAI 兼容 Provider 复用。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
