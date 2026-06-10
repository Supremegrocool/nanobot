"""  init   Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/__init__.py

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

from importlib import import_module
from typing import TYPE_CHECKING

from nanobot.providers.base import LLMProvider, LLMResponse

__all__ = [
    "LLMProvider",
    "LLMResponse",
    "AnthropicProvider",
    "OpenAICompatProvider",
    "OpenAICodexProvider",
    "GitHubCopilotProvider",
    "AzureOpenAIProvider",
    "BedrockProvider",
]

_LAZY_IMPORTS = {
    "AnthropicProvider": ".anthropic_provider",
    "OpenAICompatProvider": ".openai_compat_provider",
    "OpenAICodexProvider": ".openai_codex_provider",
    "GitHubCopilotProvider": ".github_copilot_provider",
    "AzureOpenAIProvider": ".azure_openai_provider",
    "BedrockProvider": ".bedrock_provider",
}

if TYPE_CHECKING:
    from nanobot.providers.anthropic_provider import AnthropicProvider
    from nanobot.providers.azure_openai_provider import AzureOpenAIProvider
    from nanobot.providers.bedrock_provider import BedrockProvider
    from nanobot.providers.github_copilot_provider import GitHubCopilotProvider
    from nanobot.providers.openai_codex_provider import OpenAICodexProvider
    from nanobot.providers.openai_compat_provider import OpenAICompatProvider


def __getattr__(name: str):
    """执行辅助逻辑（__getattr__ = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。  init   Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `__getattr__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    module_name = _LAZY_IMPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    return getattr(module, name)

