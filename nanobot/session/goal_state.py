"""持续目标（sustained goal）相关的 Session 元数据辅助函数。

这里服务的是 ``long_task`` / ``complete_goal`` 这类“可能跨多个 turn 持续推进”的能力。
它们会把目标状态写进 ``session.metadata``，而 AgentLoop、WebUI、Runner 等其它模块
则通过这里的辅助函数读取这些状态，而不必直接依赖具体工具实现。
"""

from __future__ import annotations

import json
from typing import Any, Mapping, MutableMapping

from nanobot.session.manager import SessionManager

GOAL_STATE_KEY = "goal_state"
# Older builds stored the same JSON blob under this key.
_LEGACY_GOAL_STATE_SESSION_KEY = "thread_goal"
_MAX_OBJECTIVE_IN_RUNTIME = 4000
_MAX_OBJECTIVE_WS = 600


def _session_goal_raw(metadata: Mapping[str, Any] | None) -> Any:
    """从新旧字段中取出原始 goal blob。"""
    if not metadata:
        return None
    if GOAL_STATE_KEY in metadata:
        return metadata.get(GOAL_STATE_KEY)
    return metadata.get(_LEGACY_GOAL_STATE_SESSION_KEY)


def discard_legacy_goal_state_key(metadata: MutableMapping[str, Any]) -> None:
    """在新写入逻辑迁移完成后，删除旧版 goal 元数据键。"""
    metadata.pop(_LEGACY_GOAL_STATE_SESSION_KEY, None)


def goal_state_raw(metadata: Mapping[str, Any] | None) -> Any:
    """返回 session 中保存的 goal 原始数据，兼容旧字段名。"""
    return _session_goal_raw(metadata)


def sustained_goal_active(metadata: Mapping[str, Any] | None) -> bool:
    """判断当前会话是否存在“激活中的持续目标”。"""
    goal = parse_goal_state(goal_state_raw(metadata))
    return isinstance(goal, dict) and goal.get("status") == "active"


def sustained_goal_turn(
    metadata: Mapping[str, Any] | None,
    *,
    message_metadata: Mapping[str, Any] | None = None,
) -> bool:
    """判断当前 turn 是否应该启用“持续目标模式”的运行时限制。"""
    if sustained_goal_active(metadata):
        return True
    if not message_metadata:
        return False
    return str(message_metadata.get("original_command") or "").strip() == "/goal"


def parse_goal_state(blob: Any) -> dict[str, Any] | None:
    """把原始 goal blob 解析成标准字典结构。"""
    if blob is None:
        return None
    if isinstance(blob, dict):
        return blob
    if isinstance(blob, str):
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def goal_state_runtime_lines(metadata: Mapping[str, Any] | None) -> list[str]:
    """在 goal 激活时，生成要附加到 Runtime Context 的文本行。"""
    if not metadata:
        return []
    goal = parse_goal_state(_session_goal_raw(metadata))
    if not isinstance(goal, dict) or goal.get("status") != "active":
        return []
    objective = str(goal.get("objective") or "").strip()
    if not objective:
        return ["Goal: active (no objective text stored)."]
    if len(objective) > _MAX_OBJECTIVE_IN_RUNTIME:
        objective = objective[:_MAX_OBJECTIVE_IN_RUNTIME].rstrip() + "\n… (truncated)"
    out = ["Goal (active):", objective]
    hint = str(goal.get("ui_summary") or "").strip()
    if hint:
        out.append(f"Summary: {hint}")
    return out


def goal_state_ws_blob(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    """生成适合通过 WebSocket 推送给前端的 goal 状态快照。"""
    goal = parse_goal_state(_session_goal_raw(metadata)) if metadata else None
    if isinstance(goal, dict) and goal.get("status") == "active":
        objective = str(goal.get("objective") or "").strip()
        if len(objective) > _MAX_OBJECTIVE_WS:
            objective = objective[:_MAX_OBJECTIVE_WS].rstrip() + "…"
        summary = str(goal.get("ui_summary") or "").strip()[:120]
        blob: dict[str, Any] = {"active": True}
        if summary:
            blob["ui_summary"] = summary
        if objective:
            blob["objective"] = objective
        return blob
    return {"active": False}


def runner_wall_llm_timeout_s(
    sessions: SessionManager,
    session_key: str | None,
    *,
    metadata: Mapping[str, Any] | None = None,
    message_metadata: Mapping[str, Any] | None = None,
) -> float | None:
    """决定 Runner 在本 turn 上调用 LLM 时的整体超时策略。

    约定：
    - 返回 ``0.0``：关闭 ``asyncio.wait_for`` 的硬超时包裹
    - 返回 ``None``：沿用环境变量/默认超时逻辑

    持续目标往往需要跑更久，所以这里会给它放宽约束。
    """
    meta: Mapping[str, Any] | None = metadata
    if meta is None and session_key:
        meta = sessions.get_or_create(session_key).metadata
    return 0.0 if sustained_goal_turn(meta, message_metadata=message_metadata) else None
