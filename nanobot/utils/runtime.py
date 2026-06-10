"""识别当前运行环境、命令来源和终端能力。

【中文名称】工具模块：nanobot/utils/runtime.py

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

import re
from pathlib import Path
from typing import Any

from loguru import logger

from nanobot.utils.helpers import stringify_text_blocks

_MAX_REPEAT_EXTERNAL_LOOKUPS = 2

# 中文说明：这一段围绕重试处理，注意输入、输出和异常路径。
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
    """执行辅助逻辑（empty_tool_result_message = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `empty_tool_result_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return f"({tool_name} completed with no output)"


def ensure_nonempty_tool_result(tool_name: str, content: Any) -> Any:
    """确保前置条件成立（ensure_nonempty_tool_result = 原函数名）。

    【中文名称】确保前置条件成立

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `ensure_nonempty_tool_result` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """判断条件是否成立（is_blank_text = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `is_blank_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return content is None or not content.strip()


def build_finalization_retry_message() -> dict[str, str]:
    """构建对象（build_finalization_retry_message = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `build_finalization_retry_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {"role": "user", "content": FINALIZATION_RETRY_PROMPT}


def build_budget_exhausted_finalization_message() -> dict[str, str]:
    """构建对象（build_budget_exhausted_finalization_message = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `build_budget_exhausted_finalization_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {"role": "user", "content": BUDGET_EXHAUSTED_FINALIZATION_PROMPT}


def build_length_recovery_message() -> dict[str, str]:
    """构建对象（build_length_recovery_message = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `build_length_recovery_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {"role": "user", "content": LENGTH_RECOVERY_PROMPT}


def build_goal_continue_message(custom: str | None = None) -> dict[str, str]:
    """构建对象（build_goal_continue_message = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `build_goal_continue_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    custom: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {"role": "user", "content": custom or SUSTAINED_GOAL_CONTINUE_PROMPT}


def external_lookup_signature(tool_name: str, arguments: Any) -> str | None:
    """执行辅助逻辑（external_lookup_signature = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `external_lookup_signature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    arguments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（repeated_external_lookup_error = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `repeated_external_lookup_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    arguments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    seen_counts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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


# 中文说明：这一段围绕错误处理，注意输入、输出和异常路径。

_OUTSIDE_PATH_PATTERN = re.compile(r"(?:^|[\s|>'\"])((?:/[^\s\"'>;|<]+)|(?:~[^\s\"'>;|<]+))")


def workspace_violation_signature(
    tool_name: str,
    arguments: Any,
) -> str | None:
    """执行辅助逻辑（workspace_violation_signature = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `workspace_violation_signature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    arguments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """标准化数据（_normalize_violation_target = 原函数名）。

    【中文名称】标准化数据

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `_normalize_violation_target` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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
    """执行辅助逻辑（repeated_workspace_violation_error = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别当前运行环境、命令来源和终端能力。
    在阅读 `repeated_workspace_violation_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    arguments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    seen_counts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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

