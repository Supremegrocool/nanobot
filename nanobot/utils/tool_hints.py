"""从工具调用结果中提取提示信息，辅助模型继续推理。

【中文名称】工具模块：nanobot/utils/tool_hints.py

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

from nanobot.utils.path import abbreviate_path

# 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
_TOOL_FORMATS: dict[str, tuple[list[str], str, bool, bool]] = {
    "read_file":  (["path", "file_path"],              "read {}",     True,  False),
    "write_file": (["path", "file_path"],              "write {}",    True,  False),
    "edit":       (["file_path", "path"],              "edit {}",     True,  False),
    "find_files": (["query", "glob", "path"],           "find {}",     False, False),
    "grep":       (["pattern"],                        'grep "{}"',   False, False),
    "exec":       (["command"],                        "$ {}",        False, True),
    "list_exec_sessions": ([],                          "exec sessions", False, False),
    "web_search": (["query"],                          'search "{}"', False, False),
    "web_fetch":  (["url"],                            "fetch {}",    True,  False),
    "list_dir":   (["path"],                           "ls {}",       True,  False),
}

# 中文说明：这一段围绕文件、路径处理，注意输入、输出和异常路径。
_PATH_IN_CMD_RE = re.compile(
    r'"(?P<double>(?:[A-Za-z]:[/\\]|~/|/)[^"]+)"'
    r"|'(?P<single>(?:[A-Za-z]:[/\\]|~/|/)[^']+)'"
    r"|(?P<bare>(?:[A-Za-z]:[/\\]|~/|(?<=\s)/)[^\s;&|<>\"']+)"
)


def format_tool_hints(tool_calls: list, max_length: int = 40) -> str:
    """格式化内容（format_tool_hints = 原函数名）。

    【中文名称】格式化内容

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `format_tool_hints` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tool_calls: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_length: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not tool_calls:
        return ""

    formatted = []
    for tc in tool_calls:
        fmt = _TOOL_FORMATS.get(tc.name)
        if fmt:
            formatted.append(_fmt_known(tc, fmt, max_length))
        elif tc.name.startswith("mcp_"):
            formatted.append(_fmt_mcp(tc, max_length))
        else:
            formatted.append(_fmt_fallback(tc, max_length))

    hints = []
    for hint in formatted:
        if hints and hints[-1][0] == hint:
            hints[-1] = (hint, hints[-1][1] + 1)
        else:
            hints.append((hint, 1))

    return ", ".join(
        f"{h} \u00d7 {c}" if c > 1 else h for h, c in hints
    )


def _get_args(tc) -> dict:
    """执行辅助逻辑（_get_args = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_get_args` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if tc.arguments is None:
        return {}
    if isinstance(tc.arguments, list):
        return tc.arguments[0] if tc.arguments else {}
    if isinstance(tc.arguments, dict):
        return tc.arguments
    return {}


def _extract_arg(tc, key_args: list[str]) -> str | None:
    """提取信息（_extract_arg = 原函数名）。

    【中文名称】提取信息

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_extract_arg` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    key_args: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    args = _get_args(tc)
    if not isinstance(args, dict):
        return None
    for key in key_args:
        val = args.get(key)
        if isinstance(val, str) and val:
            return val
    for val in args.values():
        if isinstance(val, str) and val:
            return val
    return None


def _fmt_known(tc, fmt: tuple, max_length: int = 40) -> str:
    """执行辅助逻辑（_fmt_known = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_fmt_known` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    fmt: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_length: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not fmt[0] and "{}" not in fmt[1]:
        return fmt[1]
    val = _extract_arg(tc, fmt[0])
    if val is None:
        return tc.name
    if fmt[2]:  # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
        val = abbreviate_path(val, max_len=max_length)
    elif fmt[3]:  # 中文说明：is command 相关逻辑。
        val = _abbreviate_command(val, max_len=max_length)
    return fmt[1].format(val)


def _abbreviate_command(cmd: str, max_len: int = 40) -> str:
    """执行辅助逻辑（_abbreviate_command = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_abbreviate_command` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    cmd: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_len: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    path_max = max(max_len // 2, 25)

    def _replace_path(match: re.Match[str]) -> str:
        """执行辅助逻辑（_replace_path = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
        在阅读 `_replace_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        match: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if match.group("double") is not None:
            return f'"{abbreviate_path(match.group("double"), max_len=path_max)}"'
        if match.group("single") is not None:
            return f"'{abbreviate_path(match.group('single'), max_len=path_max)}'"
        return abbreviate_path(match.group("bare"), max_len=path_max)

    abbreviated = _PATH_IN_CMD_RE.sub(_replace_path, cmd)
    if len(abbreviated) <= max_len:
        return abbreviated
    return abbreviated[:max_len - 1] + "\u2026"


def _fmt_mcp(tc, max_length: int = 40) -> str:
    """执行辅助逻辑（_fmt_mcp = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_fmt_mcp` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_length: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    name = tc.name
    if "__" in name:
        parts = name.split("__", 1)
        server = parts[0].removeprefix("mcp_")
        tool = parts[1]
    else:
        rest = name.removeprefix("mcp_")
        parts = rest.split("_", 1)
        server = parts[0] if parts else rest
        tool = parts[1] if len(parts) > 1 else ""
    if not tool:
        return name
    args = _get_args(tc)
    val = next((v for v in args.values() if isinstance(v, str) and v), None)
    if val is None:
        return f"{server}::{tool}"
    return f'{server}::{tool}("{abbreviate_path(val, max_length)}")'


def _fmt_fallback(tc, max_length: int = 40) -> str:
    """执行辅助逻辑（_fmt_fallback = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。从工具调用结果中提取提示信息，辅助模型继续推理。
    在阅读 `_fmt_fallback` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_length: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    args = _get_args(tc)
    val = next(iter(args.values()), None) if isinstance(args, dict) else None
    if not isinstance(val, str):
        return tc.name
    return f'{tc.name}("{abbreviate_path(val, max_length)}")' if len(val) > max_length else f'{tc.name}("{val}")'

