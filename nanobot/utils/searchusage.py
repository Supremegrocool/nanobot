"""搜索用量工具。

【中文名称】搜索用量工具

【功能说明】
负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass
class SearchUsageInfo:
    """SearchUsageInfo 类。

    【中文名称】SearchUsageInfo

    【功能说明】
    这是 搜索用量工具 中的核心数据结构或服务类。负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    provider: str
    supported: bool = False          # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
    error: str | None = None         # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。

    # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
    used: int | None = None
    limit: int | None = None
    remaining: int | None = None
    reset_date: str | None = None    # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。

    # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
    search_used: int | None = None
    extract_used: int | None = None
    crawl_used: int | None = None

    def format(self) -> str:
        """执行 `format`。

        【中文名称】format

        【功能说明】
        这是 搜索用量工具 中的一个步骤函数，用来支撑：负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

        # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
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
    """异步执行 `fetch_search_usage`。

    【中文名称】fetch_search_usage

    【功能说明】
    这是 搜索用量工具 中的一个步骤函数，用来支撑：负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - provider: 调用方传入的 `provider` 数据；具体类型以函数签名为准。
    - api_key: 调用方传入的 `api_key` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    p = (provider or "duckduckgo").strip().lower()

    if p == "tavily":
        return await _fetch_tavily_usage(api_key)
    else:
        # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
        return SearchUsageInfo(provider=p, supported=False)


# 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。

async def _fetch_tavily_usage(api_key: str | None) -> SearchUsageInfo:
    """异步执行 `_fetch_tavily_usage`。

    【中文名称】_fetch_tavily_usage

    【功能说明】
    这是 搜索用量工具 中的一个步骤函数，用来支撑：负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - api_key: 调用方传入的 `api_key` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_parse_tavily_usage`。

    【中文名称】_parse_tavily_usage

    【功能说明】
    这是 搜索用量工具 中的一个步骤函数，用来支撑：负责统计和限制搜索工具调用频率，避免一次任务中过度请求外部搜索服务。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - data: 调用方传入的 `data` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    account = data.get("account") or {}
    used = account.get("plan_usage")
    limit = account.get("plan_limit")

    # 说明：这里处理 搜索用量工具 的协议细节或边界情况，避免外部差异影响核心流程。
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


