"""统一处理用户路径、工作区路径和安全路径解析。

【中文名称】工具模块：nanobot/utils/path.py

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

import os
import re
from urllib.parse import urlparse


def abbreviate_path(path: str, max_len: int = 40) -> str:
    """执行辅助逻辑（abbreviate_path = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。统一处理用户路径、工作区路径和安全路径解析。
    在阅读 `abbreviate_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。
    max_len: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not path:
        return path

    # 中文说明：保留。
    if re.match(r"https?://", path):
        return _abbreviate_url(path, max_len)

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    normalized = path.replace("\\", "/")

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    home = os.path.expanduser("~").replace("\\", "/")
    if normalized.startswith(home + "/"):
        normalized = "~" + normalized[len(home):]
    elif normalized == home:
        normalized = "~"

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    if len(normalized) <= max_len:
        return normalized

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    parts = normalized.rstrip("/").split("/")
    if len(parts) <= 1:
        return normalized[:max_len - 1] + "\u2026"

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    basename = parts[-1]
    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    budget = max_len - len(basename) - 3  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    kept: list[str] = []
    for seg in reversed(parts[:-1]):
        needed = len(seg) + 1  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if not kept and needed <= budget:
            kept.append(seg)
            budget -= needed
        elif kept:
            needed_with_sep = len(seg) + 1
            if needed_with_sep <= budget:
                kept.append(seg)
                budget -= needed_with_sep
            else:
                break
        else:
            break

    kept.reverse()
    if kept:
        return "\u2026/" + "/".join(kept) + "/" + basename
    return "\u2026/" + basename


def _abbreviate_url(url: str, max_len: int = 40) -> str:
    """执行辅助逻辑（_abbreviate_url = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。统一处理用户路径、工作区路径和安全路径解析。
    在阅读 `_abbreviate_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_len: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if len(url) <= max_len:
        return url

    parsed = urlparse(url)
    domain = parsed.netloc  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    path_part = parsed.path  # 中文说明：这一段围绕API、JSON处理，注意输入、输出和异常路径。

    # 中文说明：提取。
    segments = path_part.rstrip("/").split("/")
    basename = segments[-1] if segments else ""

    if not basename:
        # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
        return url[: max_len - 1] + "\u2026"

    budget = max_len - len(domain) - len(basename) - 4  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    if budget < 0:
        trunc = max_len - len(domain) - 5  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        return domain + "/\u2026/" + (basename[:trunc] if trunc > 0 else "")

    # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
    kept: list[str] = []
    for seg in reversed(segments[:-1]):
        if len(seg) + 1 <= budget:
            kept.append(seg)
            budget -= len(seg) + 1
        else:
            break

    kept.reverse()
    if kept:
        return domain + "/\u2026/" + "/".join(kept) + "/" + basename
    return domain + "/\u2026/" + basename

