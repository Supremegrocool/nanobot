"""估算搜索结果占用的上下文预算，避免塞入过长内容。

【中文名称】工具模块：nanobot/utils/searchusage.py

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
from dataclasses import dataclass
from typing import Any


@dataclass
class SearchUsageInfo:
    """SearchUsageInfo 类，封装 工具模块 的核心状态和行为。

    【中文名称】SearchUsageInfo

    【功能说明】
    估算搜索结果占用的上下文预算，避免塞入过长内容。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    provider: str
    supported: bool = False          # 中文说明：这一段围绕Provider、API处理，注意输入、输出和异常路径。
    error: str | None = None         # 中文说明：这一段围绕调用、API处理，注意输入、输出和异常路径。

    # 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
    used: int | None = None
    limit: int | None = None
    remaining: int | None = None
    reset_date: str | None = None    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    search_used: int | None = None
    extract_used: int | None = None
    crawl_used: int | None = None

    def format(self) -> str:
        """格式化内容（format = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 工具模块 中的一个关键步骤。估算搜索结果占用的上下文预算，避免塞入过长内容。
        在阅读 `SearchUsageInfo.format` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        lines = [f"🔍 Web Search: {self.provider}"]

        if not self.supported:
            lines.append("   Usage tracking: not available for this provider")
            return "\n".join(lines)

        if self.error:
            lines.append(f"   Usage: unavailable ({self.error})")
            return "\n".join(lines)

        if self.used is not None and self.limit is not None:
            lines.append(f"   Usage: {self.used} / {self.limit} requests")
        elif self.used is not None:
            lines.append(f"   Usage: {self.used} requests")

        # 中文说明：Tavily breakdown 相关逻辑。
        breakdown_parts = []
        if self.search_used is not None:
            breakdown_parts.append(f"Search: {self.search_used}")
        if self.extract_used is not None:
            breakdown_parts.append(f"Extract: {self.extract_used}")
        if self.crawl_used is not None:
            breakdown_parts.append(f"Crawl: {self.crawl_used}")
        if breakdown_parts:
            lines.append(f"   Breakdown: {' | '.join(breakdown_parts)}")

        if self.remaining is not None:
            lines.append(f"   Remaining: {self.remaining} requests")

        if self.reset_date:
            lines.append(f"   Resets: {self.reset_date}")

        return "\n".join(lines)


async def fetch_search_usage(
    provider: str,
    api_key: str | None = None,
) -> SearchUsageInfo:
    """异步执行辅助逻辑（fetch_search_usage = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。估算搜索结果占用的上下文预算，避免塞入过长内容。
    在阅读 `fetch_search_usage` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    provider: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    p = (provider or "duckduckgo").strip().lower()

    if p == "tavily":
        return await _fetch_tavily_usage(api_key)
    else:
        # 中文说明：这一段围绕API处理，注意输入、输出和异常路径。
        return SearchUsageInfo(provider=p, supported=False)


# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
# 中文说明：Tavily 相关逻辑。
# ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

async def _fetch_tavily_usage(api_key: str | None) -> SearchUsageInfo:
    """异步执行辅助逻辑（_fetch_tavily_usage = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。估算搜索结果占用的上下文预算，避免塞入过长内容。
    在阅读 `_fetch_tavily_usage` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    import httpx

    key = api_key or os.environ.get("TAVILY_API_KEY", "")
    if not key:
        return SearchUsageInfo(
            provider="tavily",
            supported=True,
            error="TAVILY_API_KEY not configured",
        )

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(
                "https://api.tavily.com/usage",
                headers={"Authorization": f"Bearer {key}"},
            )
            r.raise_for_status()
        data: dict[str, Any] = r.json()
        return _parse_tavily_usage(data)
    except httpx.HTTPStatusError as e:
        return SearchUsageInfo(
            provider="tavily",
            supported=True,
            error=f"HTTP {e.response.status_code}",
        )
    except Exception as e:
        return SearchUsageInfo(
            provider="tavily",
            supported=True,
            error=str(e)[:80],
        )


def _parse_tavily_usage(data: dict[str, Any]) -> SearchUsageInfo:
    """解析数据（_parse_tavily_usage = 原函数名）。

    【中文名称】解析数据

    【功能说明】
    这是 工具模块 中的一个关键步骤。估算搜索结果占用的上下文预算，避免塞入过长内容。
    在阅读 `_parse_tavily_usage` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    account = data.get("account") or {}
    used = account.get("plan_usage")
    limit = account.get("plan_limit")

    # 中文说明：Compute remaining 相关逻辑。
    remaining = None
    if used is not None and limit is not None:
        remaining = max(0, limit - used)

    return SearchUsageInfo(
        provider="tavily",
        supported=True,
        used=used,
        limit=limit,
        remaining=remaining,
        search_used=account.get("search_usage"),
        extract_used=account.get("extract_usage"),
        crawl_used=account.get("crawl_usage"),
    )



