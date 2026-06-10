"""运行时状态工具。

【中文名称】运行时状态工具

【功能说明】
负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.utils.helpers import stringify_text_blocks

_MAX_REPEAT_EXTERNAL_LOOKUPS = 2

# 说明：这里处理 运行时状态工具 的协议细节或边界情况，避免外部差异影响核心流程。
_MAX_REPEAT_WORKSPACE_VIOLATIONS = 2

EMPTY_FINAL_RESPONSE_MESSAGE = (
    "I completed the tool steps but couldn't produce a final answer. "
    "Please try again or narrow the task."
)

FINALIZATION_RETRY_PROMPT = (
    "Please provide your response to the user based on the conversation above."
)

BUDGET_EXHAUSTED_FINALIZATION_PROMPT = (
    "The tool-call budget for this turn is exhausted. Based only on the "
    "conversation and tool results above, provide a concise final response to "
    "the user. Do not call or request tools. Do not claim the task is complete "
    "unless the evidence above clearly shows it is complete. State what was "
    "done, what remains, and the best next step if anything is incomplete."
)

LENGTH_RECOVERY_PROMPT = (
    "Output limit reached. Continue exactly where you left off "
    "— no recap, no apology. Break remaining work into smaller steps if needed."
)

SUSTAINED_GOAL_CONTINUE_PROMPT = (
    "You have an active sustained goal. Please continue working toward the "
    "objective using your tools, or call complete_goal if the work is truly finished."
)


def empty_tool_result_message(tool_name: str) -> str:
    """执行 `empty_tool_result_message`。

    【中文名称】empty_tool_result_message

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return f"({tool_name} completed with no output)"


def ensure_nonempty_tool_result(tool_name: str, content: Any) -> Any:
    """执行 `ensure_nonempty_tool_result`。

    【中文名称】ensure_nonempty_tool_result

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if content is None:
        return empty_tool_result_message(tool_name)
    if isinstance(content, str) and not content.strip():
        return empty_tool_result_message(tool_name)
    if isinstance(content, list):
        if not content:
            return empty_tool_result_message(tool_name)
        text_payload = stringify_text_blocks(content)
        if text_payload is not None and not text_payload.strip():
            return empty_tool_result_message(tool_name)
    return content


def is_blank_text(content: str | None) -> bool:
    """执行 `is_blank_text`。

    【中文名称】is_blank_text

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return content is None or not content.strip()


def build_finalization_retry_message() -> dict[str, str]:
    """执行 `build_finalization_retry_message`。

    【中文名称】build_finalization_retry_message

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return {"role": "user", "content": FINALIZATION_RETRY_PROMPT}


def build_budget_exhausted_finalization_message() -> dict[str, str]:
    """执行 `build_budget_exhausted_finalization_message`。

    【中文名称】build_budget_exhausted_finalization_message

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return {"role": "user", "content": BUDGET_EXHAUSTED_FINALIZATION_PROMPT}


def build_length_recovery_message() -> dict[str, str]:
    """执行 `build_length_recovery_message`。

    【中文名称】build_length_recovery_message

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return {"role": "user", "content": LENGTH_RECOVERY_PROMPT}


def build_goal_continue_message(custom: str | None = None) -> dict[str, str]:
    """执行 `build_goal_continue_message`。

    【中文名称】build_goal_continue_message

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - custom: 调用方传入的 `custom` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return {"role": "user", "content": custom or SUSTAINED_GOAL_CONTINUE_PROMPT}


def external_lookup_signature(tool_name: str, arguments: Any) -> str | None:
    """执行 `external_lookup_signature`。

    【中文名称】external_lookup_signature

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。
    - arguments: 调用方传入的 `arguments` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not isinstance(arguments, dict):
        return None
    if tool_name == "web_fetch":
        url = str(arguments.get("url") or "").strip()
        if url:
            return f"web_fetch:{url.lower()}"
    if tool_name == "web_search":
        query = str(arguments.get("query") or arguments.get("search_term") or "").strip()
        if query:
            return f"web_search:{query.lower()}"
    return None


def repeated_external_lookup_error(
    tool_name: str,
    arguments: Any,
    seen_counts: dict[str, int],
) -> str | None:
    """执行 `repeated_external_lookup_error`。

    【中文名称】repeated_external_lookup_error

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。
    - arguments: 调用方传入的 `arguments` 数据；具体类型以函数签名为准。
    - seen_counts: 调用方传入的 `seen_counts` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    signature = external_lookup_signature(tool_name, arguments)
    if signature is None:
        return None
    count = seen_counts.get(signature, 0) + 1
    seen_counts[signature] = count
    if count <= _MAX_REPEAT_EXTERNAL_LOOKUPS:
        return None
    logger.warning(
        "Blocking repeated external lookup {} on attempt {}",
        signature[:160],
        count,
    )
    return (
        "Error: repeated external lookup blocked. "
        "Use the results you already have to answer, or try a meaningfully different source."
    )


# 说明：这里处理 运行时状态工具 的协议细节或边界情况，避免外部差异影响核心流程。

_OUTSIDE_PATH_PATTERN = re.compile(r"(?:^|[\s|>'\"])((?:/[^\s\"'>;|<]+)|(?:~[^\s\"'>;|<]+))")


def workspace_violation_signature(
    tool_name: str,
    arguments: Any,
) -> str | None:
    """执行 `workspace_violation_signature`。

    【中文名称】workspace_violation_signature

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。
    - arguments: 调用方传入的 `arguments` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not isinstance(arguments, dict):
        return None
    for key in ("path", "file_path", "target", "source", "destination"):
        val = arguments.get(key)
        if isinstance(val, str) and val.strip():
            return _normalize_violation_target(val.strip())

    if tool_name in {"exec", "shell"}:
        cmd = str(arguments.get("command") or "").strip()
        if cmd:
            match = _OUTSIDE_PATH_PATTERN.search(cmd)
            if match:
                return _normalize_violation_target(match.group(1))
        cwd = str(arguments.get("working_dir") or "").strip()
        if cwd:
            return _normalize_violation_target(cwd)

    return None


def _normalize_violation_target(raw: str) -> str:
    """执行 `_normalize_violation_target`。

    【中文名称】_normalize_violation_target

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - raw: 调用方传入的 `raw` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        normalized = Path(raw).expanduser().resolve().as_posix()
    except Exception:
        normalized = raw.replace("\\", "/")
    return f"violation:{normalized}".lower()


def repeated_workspace_violation_error(
    tool_name: str,
    arguments: Any,
    seen_counts: dict[str, int],
) -> str | None:
    """执行 `repeated_workspace_violation_error`。

    【中文名称】repeated_workspace_violation_error

    【功能说明】
    这是 运行时状态工具 中的一个步骤函数，用来支撑：负责管理进程级运行时标记、后台任务和可观测状态，帮助不同子系统共享状态。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_name: 调用方传入的 `tool_name` 数据；具体类型以函数签名为准。
    - arguments: 调用方传入的 `arguments` 数据；具体类型以函数签名为准。
    - seen_counts: 调用方传入的 `seen_counts` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    signature = workspace_violation_signature(tool_name, arguments)
    if signature is None:
        return None
    count = seen_counts.get(signature, 0) + 1
    seen_counts[signature] = count
    if count <= _MAX_REPEAT_WORKSPACE_VIOLATIONS:
        return None
    logger.warning(
        "Escalating repeated workspace bypass attempt {} (attempt {})",
        signature[:160],
        count,
    )
    target = signature.split("violation:", 1)[1] if "violation:" in signature else signature
    return (
        "Error: refusing repeated workspace-bypass attempts.\n"
        f"You have tried to access '{target}' (or an equivalent path) "
        f"{count} times in this turn. This is a hard policy boundary -- "
        "switching tools, shell tricks, working_dir overrides, symlinks, "
        "or base64 piping will NOT change the answer. Stop retrying. "
        "If the user genuinely needs this resource, tell them you cannot "
        "access it and ask how they want to proceed (e.g. copy the file "
        "into the workspace, or disable restrict_to_workspace for this run)."
    )
