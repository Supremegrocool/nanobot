"""Azure OpenAI Provider 实现。

【中文名称】Azure OpenAI Provider 实现

【功能说明】
负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """_AzureTokenProvider 类。

    【中文名称】_AzureTokenProvider

    【功能说明】
    这是 Azure OpenAI Provider 实现 中的核心数据结构或服务类。负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    def __init__(self, scope: str = _AZURE_OPENAI_SCOPE) -> None:
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - scope: 调用方传入的 `scope` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `__call__`。

        【中文名称】__call__

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        access_token = await self._credential.get_token(self._scope)
        return access_token.token

    async def aclose(self) -> None:
        """异步执行 `aclose`。

        【中文名称】aclose

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        close = getattr(self._credential, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass


class AzureOpenAIProvider(LLMProvider):
    """AzureOpenAIProvider 类。

    【中文名称】AzureOpenAIProvider

    【功能说明】
    这是 Azure OpenAI Provider 实现 中的核心数据结构或服务类。负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    def __init__(
        self,
        api_key: str = "",
        api_base: str = "",
        default_model: str = "gpt-5.2-chat",
    ):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - api_key: 调用方传入的 `api_key` 数据；具体类型以函数签名为准。
        - api_base: 调用方传入的 `api_base` 数据；具体类型以函数签名为准。
        - default_model: 调用方传入的 `default_model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        super().__init__(api_key, api_base)
        self.default_model = default_model

        if not api_base:
            raise ValueError("Azure OpenAI api_base is required")

        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        if not api_base.endswith("/"):
            api_base += "/"
        self.api_base = api_base

        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        self._token_provider: _AzureTokenProvider | None = None
        client_api_key: str | Callable[[], Awaitable[str]]
        if api_key:
            client_api_key = api_key
        else:
            self._token_provider = _AzureTokenProvider()
            client_api_key = self._token_provider

        # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        base_url = f"{api_base.rstrip('/')}/openai/v1/"
        self._client = AsyncOpenAI(
            api_key=client_api_key,
            base_url=base_url,
            default_headers={"x-session-affinity": uuid.uuid4().hex},
            max_retries=0,
        )

    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。

    @staticmethod
    def _supports_temperature(
        deployment_name: str,
        reasoning_effort: str | None = None,
    ) -> bool:
        """执行 `_supports_temperature`。

        【中文名称】_supports_temperature

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - deployment_name: 调用方传入的 `deployment_name` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_build_body`。

        【中文名称】_build_body

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_handle_error`。

        【中文名称】_handle_error

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - e: 调用方传入的 `e` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        response = getattr(e, "response", None)
        body = getattr(e, "body", None) or getattr(response, "text", None)
        body_text = str(body).strip() if body is not None else ""
        msg = f"Error: {body_text[:500]}" if body_text else f"Error calling Azure OpenAI: {e}"
        retry_after = LLMProvider._extract_retry_after_from_headers(getattr(response, "headers", None))
        if retry_after is None:
            retry_after = LLMProvider._extract_retry_after(msg)
        return LLMResponse(content=msg, finish_reason="error", retry_after=retry_after)

    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 Azure OpenAI Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。

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
        """异步执行 `chat`。

        【中文名称】chat

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `chat_stream`。

        【中文名称】chat_stream

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。
        - on_content_delta: 调用方传入的 `on_content_delta` 数据；具体类型以函数签名为准。
        - on_thinking_delta: 调用方传入的 `on_thinking_delta` 数据；具体类型以函数签名为准。
        - on_tool_call_delta: 调用方传入的 `on_tool_call_delta` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """执行 `get_default_model`。

        【中文名称】get_default_model

        【功能说明】
        这是 Azure OpenAI Provider 实现 中的一个步骤函数，用来支撑：负责在 OpenAI 兼容 Provider 的基础上补齐 Azure endpoint、deployment、api-version 和鉴权差异。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self.default_model
