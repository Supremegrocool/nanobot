"""OpenAI Responses API Provider 的共享辅助模块（如 Codex、Azure OpenAI）。"""

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
