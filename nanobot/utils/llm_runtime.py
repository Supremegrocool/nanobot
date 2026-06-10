"""把当前 LLM Provider 与模型名作为一个运行时上下文传递。

【中文名称】工具模块：nanobot/utils/llm_runtime.py

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

from collections.abc import Callable
from dataclasses import dataclass

from nanobot.providers.base import LLMProvider


@dataclass(frozen=True)
class LLMRuntime:
    """LLMRuntime 类，封装 工具模块 的核心状态和行为。

    【中文名称】LLMRuntime

    【功能说明】
    把当前 LLM Provider 与模型名作为一个运行时上下文传递。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """
    provider: LLMProvider
    model: str


LLMRuntimeResolver = Callable[[], LLMRuntime]


def static_llm_runtime(provider: LLMProvider, model: str) -> LLMRuntimeResolver:
    """执行辅助逻辑（static_llm_runtime = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。把当前 LLM Provider 与模型名作为一个运行时上下文传递。
    在阅读 `static_llm_runtime` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    provider: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    runtime = LLMRuntime(provider=provider, model=model)
    return lambda: runtime

