"""进度回调辅助函数：把 Agent 的“处理中事件”转成用户可见消息。

这里处理的是“正在生成中 / 正在调工具 / 正在展示思维链片段”这类面向用户的
动态进度输出。它和 ``nanobot.bus.runtime_events`` 的职责不同：

- 本模块：把进度包装成 ``OutboundMessage``，最终会发到用户界面
- ``runtime_events``：进程内状态事件，更多给 WebUI/观察者做运行时同步
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus


def build_bus_progress_callback(
    bus: MessageBus,
    msg: InboundMessage,
) -> Callable[..., Awaitable[None]]:
    """构造一个“发布进度消息到消息总线”的回调函数。

    AgentRunner / Hook 层只知道“我要报告一个进度事件”，不直接操作渠道。
    这个函数把那种抽象的回调，转换成真正的 ``OutboundMessage`` 投递逻辑。
    """

    async def _publish_progress(
        content: str,
        *,
        tool_hint: bool = False,
        tool_events: list[dict[str, Any]] | None = None,
        file_edit_events: list[dict[str, Any]] | None = None,
        reasoning: bool = False,
        reasoning_end: bool = False,
    ) -> None:
        # 这里会继承原始入站消息的 metadata，
        # 这样渠道层仍然能拿到 message_id、thread_id 等路由上下文。
        meta = dict(msg.metadata or {})
        meta["_progress"] = True
        meta["_tool_hint"] = tool_hint
        if reasoning:
            meta["_reasoning_delta"] = True
        if reasoning_end:
            meta["_reasoning_end"] = True
        if tool_events:
            meta["_tool_events"] = tool_events
        if file_edit_events:
            meta["_file_edit_events"] = file_edit_events
        await bus.publish_outbound(
            OutboundMessage(
                channel=msg.channel,
                chat_id=msg.chat_id,
                content=content,
                metadata=meta,
            )
        )

    async def _bus_progress(
        content: str,
        *,
        tool_hint: bool = False,
        tool_events: list[dict[str, Any]] | None = None,
        file_edit_events: list[dict[str, Any]] | None = None,
        reasoning: bool = False,
        reasoning_end: bool = False,
    ) -> None:
        await _publish_progress(
            content,
            tool_hint=tool_hint,
            tool_events=tool_events,
            file_edit_events=file_edit_events,
            reasoning=reasoning,
            reasoning_end=reasoning_end,
        )

    return _bus_progress
