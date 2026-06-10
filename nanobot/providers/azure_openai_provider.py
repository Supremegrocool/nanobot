"""Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/azure_openai_provider.py

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

import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from openai import AsyncOpenAI

from nanobot.providers.base import LLMProvider, LLMResponse
from nanobot.providers.openai_responses import (
    consume_sdk_stream,
    convert_messages,
    convert_tools,
    parse_response_output,
)

_AZURE_OPENAI_SCOPE = "https://cognitiveservices.azure.com/.default"


class _AzureTokenProvider:
    """_AzureTokenProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】_AzureTokenProvider

    【功能说明】
    Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(self, scope: str = _AZURE_OPENAI_SCOPE) -> None:
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `_AzureTokenProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        scope: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            from azure.identity.aio import DefaultAzureCredential
        except ImportError as exc:
            raise RuntimeError(
                "Azure OpenAI AAD authentication requires the 'azure-identity' package. "
                "Install it with: pip install 'nanobot-ai[azure]'"
            ) from exc

        self._scope = scope
        self._credential = DefaultAzureCredential()

    async def __call__(self) -> str:
        """异步调用服务（__call__ = 原函数名）。

        【中文名称】调用服务

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `_AzureTokenProvider.__call__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        access_token = await self._credential.get_token(self._scope)
        return access_token.token

    async def aclose(self) -> None:
        """异步执行辅助逻辑（aclose = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `_AzureTokenProvider.aclose` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        close = getattr(self._credential, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass


class AzureOpenAIProvider(LLMProvider):
    """AzureOpenAIProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】AzureOpenAIProvider

    【功能说明】
    Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    LLMProvider。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str = "",
        api_base: str = "",
        default_model: str = "gpt-5.2-chat",
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        default_model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        super().__init__(api_key, api_base)
        self.default_model = default_model

        if not api_base:
            raise ValueError("Azure OpenAI api_base is required")

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if not api_base.endswith("/"):
            api_base += "/"
        self.api_base = api_base

        # 中文说明：这一段围绕API处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这一段围绕调用、请求、API处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕令牌处理，注意输入、输出和异常路径。
        self._token_provider: _AzureTokenProvider | None = None
        client_api_key: str | Callable[[], Awaitable[str]]
        if api_key:
            client_api_key = api_key
        else:
            self._token_provider = _AzureTokenProvider()
            client_api_key = self._token_provider

        # 中文说明：这一段围绕响应、API处理，注意输入、输出和异常路径。
        base_url = f"{api_base.rstrip('/')}/openai/v1/"
        self._client = AsyncOpenAI(
            api_key=client_api_key,
            base_url=base_url,
            default_headers={"x-session-affinity": uuid.uuid4().hex},
            max_retries=0,
        )

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Helpers 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    @staticmethod
    def _supports_temperature(
        deployment_name: str,
        reasoning_effort: str | None = None,
    ) -> bool:
        """执行辅助逻辑（_supports_temperature = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider._supports_temperature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        deployment_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if reasoning_effort and reasoning_effort.lower() != "none":
            return False
        name = deployment_name.lower()
        return not any(token in name for token in ("gpt-5", "o1", "o3", "o4"))

    def _build_body(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        model: str | None,
        max_tokens: int,
        temperature: float,
        reasoning_effort: str | None,
        tool_choice: str | dict[str, Any] | None,
    ) -> dict[str, Any]:
        """构建对象（_build_body = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider._build_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        max_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        temperature: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        deployment = model or self.default_model
        instructions, input_items = convert_messages(self._sanitize_empty_content(messages))

        body: dict[str, Any] = {
            "model": deployment,
            "instructions": instructions or None,
            "input": input_items,
            "max_output_tokens": max(1, max_tokens),
            "store": False,
            "stream": False,
        }

        if self._supports_temperature(deployment, reasoning_effort):
            body["temperature"] = temperature

        if reasoning_effort and reasoning_effort.lower() != "none":
            body["reasoning"] = {"effort": reasoning_effort}
            body["include"] = ["reasoning.encrypted_content"]

        if tools:
            body["tools"] = convert_tools(tools)
            body["tool_choice"] = tool_choice or "auto"

        return body

    @staticmethod
    def _handle_error(e: Exception) -> LLMResponse:
        """处理事件（_handle_error = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider._handle_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        e: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        response = getattr(e, "response", None)
        body = getattr(e, "body", None) or getattr(response, "text", None)
        body_text = str(body).strip() if body is not None else ""
        msg = f"Error: {body_text[:500]}" if body_text else f"Error calling Azure OpenAI: {e}"
        retry_after = LLMProvider._extract_retry_after_from_headers(getattr(response, "headers", None))
        if retry_after is None:
            retry_after = LLMProvider._extract_retry_after(msg)
        return LLMResponse(content=msg, finish_reason="error", retry_after=retry_after)

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕API处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
    ) -> LLMResponse:
        """异步执行辅助逻辑（chat = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider.chat` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        max_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        temperature: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        body = self._build_body(
            messages, tools, model, max_tokens, temperature,
            reasoning_effort, tool_choice,
        )
        try:
            response = await self._client.responses.create(**body)
            return parse_response_output(response)
        except Exception as e:
            return self._handle_error(e)

    async def chat_stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        on_content_delta: Callable[[str], Awaitable[None]] | None = None,
        on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
        on_tool_call_delta: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> LLMResponse:
        """异步流式处理（chat_stream = 原函数名）。

        【中文名称】流式处理

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider.chat_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        max_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        temperature: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_content_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_thinking_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_tool_call_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        _ = on_thinking_delta
        body = self._build_body(
            messages, tools, model, max_tokens, temperature,
            reasoning_effort, tool_choice,
        )
        body["stream"] = True

        try:
            stream = await self._client.responses.create(**body)
            content, tool_calls, finish_reason, usage, reasoning_content = (
                await consume_sdk_stream(stream, on_content_delta, on_tool_call_delta)
            )
            return LLMResponse(
                content=content or None,
                tool_calls=tool_calls,
                finish_reason=finish_reason,
                usage=usage,
                reasoning_content=reasoning_content,
            )
        except Exception as e:
            return self._handle_error(e)

    def get_default_model(self) -> str:
        """执行辅助逻辑（get_default_model = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Azure OpenAI Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AzureOpenAIProvider.get_default_model` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return self.default_model

