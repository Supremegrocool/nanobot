"""工具提示工具。

【中文名称】工具提示工具

【功能说明】
负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import re

from nanobot.utils.path import abbreviate_path

# 说明：这里处理 工具提示工具 的协议细节或边界情况，避免外部差异影响核心流程。
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

# 说明：这里处理 工具提示工具 的协议细节或边界情况，避免外部差异影响核心流程。
_PATH_IN_CMD_RE = re.compile(
    r'"(?P<double>(?:[A-Za-z]:[/\\]|~/|/)[^"]+)"'
    r"|'(?P<single>(?:[A-Za-z]:[/\\]|~/|/)[^']+)'"
    r"|(?P<bare>(?:[A-Za-z]:[/\\]|~/|(?<=\s)/)[^\s;&|<>\"']+)"
)


def format_tool_hints(tool_calls: list, max_length: int = 40) -> str:
    """执行 `format_tool_hints`。

    【中文名称】format_tool_hints

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tool_calls: 调用方传入的 `tool_calls` 数据；具体类型以函数签名为准。
    - max_length: 调用方传入的 `max_length` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_get_args`。

    【中文名称】_get_args

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tc: 调用方传入的 `tc` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if tc.arguments is None:
        return {}
    if isinstance(tc.arguments, list):
        return tc.arguments[0] if tc.arguments else {}
    if isinstance(tc.arguments, dict):
        return tc.arguments
    return {}


def _extract_arg(tc, key_args: list[str]) -> str | None:
    """执行 `_extract_arg`。

    【中文名称】_extract_arg

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tc: 调用方传入的 `tc` 数据；具体类型以函数签名为准。
    - key_args: 调用方传入的 `key_args` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_fmt_known`。

    【中文名称】_fmt_known

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tc: 调用方传入的 `tc` 数据；具体类型以函数签名为准。
    - fmt: 调用方传入的 `fmt` 数据；具体类型以函数签名为准。
    - max_length: 调用方传入的 `max_length` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not fmt[0] and "{}" not in fmt[1]:
        return fmt[1]
    val = _extract_arg(tc, fmt[0])
    if val is None:
        return tc.name
    if fmt[2]:  # 说明：这里处理 工具提示工具 的协议细节或边界情况，避免外部差异影响核心流程。
        val = abbreviate_path(val, max_len=max_length)
    elif fmt[3]:  # 说明：这里处理 工具提示工具 的协议细节或边界情况，避免外部差异影响核心流程。
        val = _abbreviate_command(val, max_len=max_length)
    return fmt[1].format(val)


def _abbreviate_command(cmd: str, max_len: int = 40) -> str:
    """执行 `_abbreviate_command`。

    【中文名称】_abbreviate_command

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - cmd: 调用方传入的 `cmd` 数据；具体类型以函数签名为准。
    - max_len: 调用方传入的 `max_len` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    path_max = max(max_len // 2, 25)

    def _replace_path(match: re.Match[str]) -> str:
        """执行 `_replace_path`。

        【中文名称】_replace_path

        【功能说明】
        这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - match: 调用方传入的 `match` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
    """执行 `_fmt_mcp`。

    【中文名称】_fmt_mcp

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tc: 调用方传入的 `tc` 数据；具体类型以函数签名为准。
    - max_length: 调用方传入的 `max_length` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_fmt_fallback`。

    【中文名称】_fmt_fallback

    【功能说明】
    这是 工具提示工具 中的一个步骤函数，用来支撑：负责把工具调用提示整理成不同渠道可显示的短文本，帮助用户理解 Agent 正在做什么。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tc: 调用方传入的 `tc` 数据；具体类型以函数签名为准。
    - max_length: 调用方传入的 `max_length` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    args = _get_args(tc)
    val = next(iter(args.values()), None) if isinstance(args, dict) else None
    if not isinstance(val, str):
        return tc.name
    return f'{tc.name}("{abbreviate_path(val, max_length)}")' if len(val) > max_length else f'{tc.name}("{val}")'
