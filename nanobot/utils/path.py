"""路径安全工具。

【中文名称】路径安全工具

【功能说明】
负责把用户输入路径解析到工作区内，防止越界访问和平台路径差异。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import os
import re
from urllib.parse import urlparse


def abbreviate_path(path: str, max_len: int = 40) -> str:
    """执行 `abbreviate_path`。

    【中文名称】abbreviate_path

    【功能说明】
    这是 路径安全工具 中的一个步骤函数，用来支撑：负责把用户输入路径解析到工作区内，防止越界访问和平台路径差异。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。
    - max_len: 调用方传入的 `max_len` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not path:
        return path

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    if re.match(r"https?://", path):
        return _abbreviate_url(path, max_len)

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    normalized = path.replace("\\", "/")

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    home = os.path.expanduser("~").replace("\\", "/")
    if normalized.startswith(home + "/"):
        normalized = "~" + normalized[len(home):]
    elif normalized == home:
        normalized = "~"

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    if len(normalized) <= max_len:
        return normalized

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    parts = normalized.rstrip("/").split("/")
    if len(parts) <= 1:
        return normalized[:max_len - 1] + "\u2026"

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    basename = parts[-1]
    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    budget = max_len - len(basename) - 3  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    kept: list[str] = []
    for seg in reversed(parts[:-1]):
        needed = len(seg) + 1  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
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
    """执行 `_abbreviate_url`。

    【中文名称】_abbreviate_url

    【功能说明】
    这是 路径安全工具 中的一个步骤函数，用来支撑：负责把用户输入路径解析到工作区内，防止越界访问和平台路径差异。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - url: 调用方传入的 `url` 数据；具体类型以函数签名为准。
    - max_len: 调用方传入的 `max_len` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if len(url) <= max_len:
        return url

    parsed = urlparse(url)
    domain = parsed.netloc  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    path_part = parsed.path  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    segments = path_part.rstrip("/").split("/")
    basename = segments[-1] if segments else ""

    if not basename:
        # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
        return url[: max_len - 1] + "\u2026"

    budget = max_len - len(domain) - len(basename) - 4  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
    if budget < 0:
        trunc = max_len - len(domain) - 5  # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
        return domain + "/\u2026/" + (basename[:trunc] if trunc > 0 else "")

    # 说明：这里处理 路径安全工具 的协议细节或边界情况，避免外部差异影响核心流程。
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
