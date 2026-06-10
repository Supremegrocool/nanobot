"""内部 turn 续跑策略辅助函数。

这个模块解决的是一个很具体的问题：

“如果当前 turn 因为工具调用预算耗尽而被迫停下，但任务其实还没完成，要不要自动续跑？”

为了不把这套策略写死在 ``AgentLoop`` 里，这里把相关判断和元数据操作独立出来。
"""

from __future__ import annotations

import dataclasses
from typing import Any, Mapping, MutableMapping

from loguru import logger

from nanobot.session.goal_state import (
    goal_state_runtime_lines,
    sustained_goal_active,
    sustained_goal_turn,
)

INTERNAL_CONTINUATION_META = "_internal_continuation"
INTERNAL_CONTINUATION_KIND_META = "_internal_continuation_kind"
INTERNAL_CONTINUATION_PENDING_META = "_internal_continuation_pending"
INTERNAL_CONTINUATION_RUN_STARTED_AT_META = "_internal_continuation_run_started_at"

_GOAL_CONTINUATION_KIND = "sustained_goal"
_GOAL_CONTINUATION_SENDER = "system:continuation"
_GOAL_CONTINUATION_ROUNDS_KEY = "_sustained_goal_continuation_rounds"
_MAX_GOAL_CONTINUATION_ROUNDS = 12
_STRIPPED_INBOUND_META_KEYS = {
    "_stream_id",
    "_stream_delta",
    "_stream_end",
    "_resuming",
    INTERNAL_CONTINUATION_PENDING_META,
}


def internal_continuation_inbound(metadata: Mapping[str, Any] | None) -> bool:
    """判断当前入站消息是否是“内部续跑策略”伪造出来的。"""
    return bool(metadata and metadata.get(INTERNAL_CONTINUATION_META) is True)


def internal_continuation_pending(metadata: Mapping[str, Any] | None) -> bool:
    """判断当前 turn 是否已经安排了一个不可见的续跑切片。"""
    return bool(metadata and metadata.get(INTERNAL_CONTINUATION_PENDING_META) is True)


def internal_continuation_run_started_at(metadata: Mapping[str, Any] | None) -> float | None:
    """返回跨续跑切片传播的“用户可见开始时间”。"""
    if not metadata:
        return None
    value = metadata.get(INTERNAL_CONTINUATION_RUN_STARTED_AT_META)
    if not isinstance(value, int | float):
        return None
    started_at = float(value)
    return started_at if started_at > 0 else None


def should_persist_user_message(metadata: Mapping[str, Any] | None) -> bool:
    """判断这条入站消息是否应该当作用户消息持久化进 session。"""
    return not internal_continuation_inbound(metadata)


def should_stream_budget_response(
    *,
    stop_reason: str,
    pending_queue_available: bool,
    session_metadata: Mapping[str, Any] | None,
    message_metadata: Mapping[str, Any] | None = None,
) -> bool:
    """判断“达到预算边界时的回复”是否应该直接发给用户。"""
    if stop_reason != "max_iterations":
        return True
    return should_finalize_on_max_iterations(
        pending_queue_available=pending_queue_available,
        session_metadata=session_metadata,
        message_metadata=message_metadata,
    )


def should_finalize_on_max_iterations(
    *,
    pending_queue_available: bool,
    session_metadata: Mapping[str, Any] | None,
    message_metadata: Mapping[str, Any] | None = None,
) -> bool:
    """判断在达到最大迭代数时，当前切片是否应该直接产出最终回复。

    如果当前是可内部续跑的持续目标，那么这一片段不应该再额外花一次
    “无工具 finalization” 调用，而是把后续回复交给下一片续跑。
    """
    return not (
        pending_queue_available
        and _goal_continuation_available(
            session_metadata,
            message_metadata=message_metadata,
        )
    )


async def maybe_continue_turn(ctx: Any) -> bool:
    """如果策略允许，就为当前 turn 安排一个内部续跑消息。"""
    if ctx.session is None or ctx.pending_queue is None:
        return False
    if not _continuation_available(
        stop_reason=ctx.stop_reason,
        pending_queue_available=True,
        session_metadata=ctx.session.metadata,
        message_metadata=ctx.msg.metadata,
    ):
        return False

    metadata = _internal_continuation_metadata(
        ctx.msg.metadata,
        run_started_at=getattr(ctx, "visible_run_started_at", None),
    )
    content = _goal_continuation_prompt(ctx.session.metadata)
    messages = _strip_terminal_assistant(ctx.all_messages, ctx.final_content)
    _increment_goal_continuation_round(ctx.session.metadata)

    logger.info("Turn budget reached; scheduling internal continuation")
    # 这里把当前 slice 伪装成“没有最终回答”，并往 pending_queue 里塞一条
    # 系统续跑消息，让同一会话的后台任务在本轮结束后自动接着跑。
    ctx.msg.metadata[INTERNAL_CONTINUATION_PENDING_META] = True
    ctx.final_content = ""
    ctx.all_messages = messages
    ctx.suppress_response = True
    await ctx.pending_queue.put(
        dataclasses.replace(
            ctx.msg,
            sender_id=_GOAL_CONTINUATION_SENDER,
            content=content,
            media=[],
            metadata=metadata,
            session_key_override=ctx.session_key,
        )
    )
    return True


def prepare_save_boundary(ctx: Any) -> None:
    """准备续跑相关 bookkeeping，并计算本 turn 写入历史的起始边界。"""
    if ctx.session is not None:
        clear_internal_continuation_state(ctx.session.metadata)

    ctx.save_skip = _save_skip_for_turn(
        message_metadata=ctx.msg.metadata,
        initial_message_count=len(ctx.initial_messages),
        history_count=len(ctx.history),
        user_persisted_early=ctx.user_persisted_early,
    )


def _continuation_available(
    *,
    stop_reason: str,
    pending_queue_available: bool,
    session_metadata: Mapping[str, Any] | None,
    message_metadata: Mapping[str, Any] | None = None,
) -> bool:
    if stop_reason != "max_iterations" or not pending_queue_available:
        return False
    return _goal_continuation_available(
        session_metadata,
        message_metadata=message_metadata,
    )


def clear_internal_continuation_state(metadata: MutableMapping[str, Any]) -> None:
    """当持续目标模式失效时，清理内部续跑计数状态。"""
    if not sustained_goal_active(metadata):
        metadata.pop(_GOAL_CONTINUATION_ROUNDS_KEY, None)


def _save_skip_for_turn(
    *,
    message_metadata: Mapping[str, Any] | None,
    initial_message_count: int,
    history_count: int,
    user_persisted_early: bool,
) -> int:
    """计算这次 turn 在保存消息时应该跳过多少前缀。

    这是一个很关键的边界值，因为 AgentRunner 里的 ``messages`` 不只包含本轮新消息，
    还包含历史回放和 system message。保存时必须只截取“本 turn 新增部分”。
    """
    if internal_continuation_inbound(message_metadata):
        return initial_message_count
    return 1 + history_count + (1 if user_persisted_early else 0)


def _goal_continuation_available(
    session_metadata: Mapping[str, Any] | None,
    *,
    message_metadata: Mapping[str, Any] | None = None,
    max_rounds: int = _MAX_GOAL_CONTINUATION_ROUNDS,
) -> bool:
    """判断当前是否允许做持续目标续跑。"""
    if not sustained_goal_turn(session_metadata, message_metadata=message_metadata):
        return False
    if not sustained_goal_active(session_metadata):
        return False
    try:
        rounds = int((session_metadata or {}).get(_GOAL_CONTINUATION_ROUNDS_KEY) or 0)
    except (TypeError, ValueError):
        rounds = 0
    return rounds < max(0, max_rounds)


def _increment_goal_continuation_round(session_metadata: MutableMapping[str, Any]) -> None:
    """续跑轮次 +1，防止无限内部续跑。"""
    try:
        rounds = int(session_metadata.get(_GOAL_CONTINUATION_ROUNDS_KEY) or 0)
    except (TypeError, ValueError):
        rounds = 0
    session_metadata[_GOAL_CONTINUATION_ROUNDS_KEY] = rounds + 1


def _internal_continuation_metadata(
    message_metadata: Mapping[str, Any] | None,
    *,
    run_started_at: float | None = None,
) -> dict[str, Any]:
    """构造内部续跑消息要携带的 metadata。"""
    metadata = dict(message_metadata or {})
    metadata[INTERNAL_CONTINUATION_META] = True
    metadata[INTERNAL_CONTINUATION_KIND_META] = _GOAL_CONTINUATION_KIND
    if run_started_at is not None:
        metadata[INTERNAL_CONTINUATION_RUN_STARTED_AT_META] = float(run_started_at)
    for key in _STRIPPED_INBOUND_META_KEYS:
        metadata.pop(key, None)
    return metadata


def _goal_continuation_prompt(metadata: Mapping[str, Any] | None) -> str:
    """生成发给下一片续跑切片的系统式提示内容。"""
    lines = goal_state_runtime_lines(metadata)
    if lines:
        goal = "\n".join(lines)
        return (
            "Continue the active sustained goal after the previous turn reached "
            "its tool-call budget.\n\n"
            f"{goal}\n\n"
            "Continue from the saved context. Do not mention the continuation "
            "boundary to the user. Use tools as needed, and call complete_goal "
            "when the objective is truly finished."
        )
    return (
        "Continue the active sustained goal after the previous turn reached "
        "its tool-call budget. Continue from the saved context. Do not mention "
        "the continuation boundary to the user. Use tools as needed, and call "
        "complete_goal when the objective is truly finished."
    )


def _strip_terminal_assistant(
    messages: list[dict[str, Any]],
    final_content: str | None,
) -> list[dict[str, Any]]:
    """保存历史前，移除因 max-iteration 产生的合成 assistant 结尾消息。"""
    if not messages:
        return messages
    last = messages[-1]
    if last.get("role") != "assistant":
        return messages
    if final_content is None or last.get("content") != final_content:
        return messages
    if last.get("tool_calls"):
        return messages
    return messages[:-1]
