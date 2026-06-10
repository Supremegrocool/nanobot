"""LLM 运行时工具。

【中文名称】LLM 运行时工具

【功能说明】
负责判断运行时是否支持 token 用量、reasoning、工具调用等模型能力。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from nanobot.providers.base import LLMProvider


@dataclass(frozen=True)
class LLMRuntime:
    """LLMRuntime 类。

    【中文名称】LLMRuntime

    【功能说明】
    这是 LLM 运行时工具 中的核心数据结构或服务类。负责判断运行时是否支持 token 用量、reasoning、工具调用等模型能力。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    provider: LLMProvider
    model: str


LLMRuntimeResolver = Callable[[], LLMRuntime]


def static_llm_runtime(provider: LLMProvider, model: str) -> LLMRuntimeResolver:
    """执行 `static_llm_runtime`。

    【中文名称】static_llm_runtime

    【功能说明】
    这是 LLM 运行时工具 中的一个步骤函数，用来支撑：负责判断运行时是否支持 token 用量、reasoning、工具调用等模型能力。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - provider: 调用方传入的 `provider` 数据；具体类型以函数签名为准。
    - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    runtime = LLMRuntime(provider=provider, model=model)
    return lambda: runtime
