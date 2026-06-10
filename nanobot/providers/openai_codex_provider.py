"""OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/openai_codex_provider.py

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

import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from loguru import logger
from oauth_cli_kit import get_token as get_codex_token

from nanobot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from nanobot.providers.openai_responses import (
    consume_sse_with_reasoning,
    convert_messages,
    convert_tools,
)

DEFAULT_CODEX_URL = "https://chatgpt.com/backend-api/codex/responses"
DEFAULT_ORIGINATOR = "nanobot"


class OpenAICodexProvider(LLMProvider):
    """OpenAICodexProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】OpenAICodexProvider

    【功能说明】
    OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    LLMProvider。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    supports_progress_deltas = True

    def __init__(self, default_model: str = "openai-codex/gpt-5.1-codex"):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICodexProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        default_model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        super().__init__(api_key=None, api_base=None)
        self.default_model = default_model

    async def _call_codex(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        model: str | None,
        reasoning_effort: str | None,
        tool_choice: str | dict[str, Any] | None,
        on_content_delta: Callable[[str], Awaitable[None]] | None = None,
        on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
        on_tool_call_delta: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> LLMResponse:
        """异步调用服务（_call_codex = 原函数名）。

        【中文名称】调用服务

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICodexProvider._call_codex` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_content_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_thinking_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_tool_call_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        model = model or self.default_model
        system_prompt, input_items = convert_messages(messages)

        token = await asyncio.to_thread(get_codex_token)
        headers = _build_headers(token.account_id, token.access)

        body: dict[str, Any] = {
            "model": _strip_model_prefix(model),
            "store": False,
            "stream": True,
            "instructions": system_prompt,
            "input": input_items,
            "text": {"verbosity": "medium"},
            "include": ["reasoning.encrypted_content"],
            "prompt_cache_key": _prompt_cache_key(messages[:2]),
            "tool_choice": tool_choice or "auto",
            "parallel_tool_calls": True,
        }
        reasoning_options = _build_reasoning_options(reasoning_effort)
        if reasoning_options:
            body["reasoning"] = reasoning_options
        if tools:
            body["tools"] = convert_tools(tools)

        try:
            try:
                content, tool_calls, finish_reason, usage, reasoning_content = await _request_codex(
                    DEFAULT_CODEX_URL, headers, body, verify=True,
                    on_content_delta=on_content_delta,
                    on_thinking_delta=on_thinking_delta,
                    on_tool_call_delta=on_tool_call_delta,
                )
            except Exception as e:
                if "CERTIFICATE_VERIFY_FAILED" not in str(e):
                    raise
                logger.warning("SSL verification failed for Codex API; retrying with verify=False")
                content, tool_calls, finish_reason, usage, reasoning_content = await _request_codex(
                    DEFAULT_CODEX_URL, headers, body, verify=False,
                    on_content_delta=on_content_delta,
                    on_thinking_delta=on_thinking_delta,
                    on_tool_call_delta=on_tool_call_delta,
                )
            return LLMResponse(
                content=content,
                tool_calls=tool_calls,
                finish_reason=finish_reason,
                usage=usage,
                reasoning_content=reasoning_content,
            )
        except Exception as e:
            response = _codex_error_response(e)
            exc_type = "CodexHTTPError" if isinstance(e, _CodexHTTPError) else type(e).__name__
            logger.warning(
                "Codex API request failed: type={} kind={} retryable={} status={} "
                "error_type={} error_code={} retry_after={} summary={}",
                exc_type,
                response.error_kind,
                response.error_should_retry,
                response.error_status_code,
                response.error_type,
                response.error_code,
                response.retry_after,
                _codex_log_summary(exc_type, response),
            )
            return response

    async def chat(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None,
        model: str | None = None, max_tokens: int = 4096, temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
    ) -> LLMResponse:
        return await self._call_codex(messages, tools, model, reasoning_effort, tool_choice)

    async def chat_stream(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None,
        model: str | None = None, max_tokens: int = 4096, temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        on_content_delta: Callable[[str], Awaitable[None]] | None = None,
        on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
        on_tool_call_delta: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> LLMResponse:
        """异步流式处理（chat_stream = 原函数名）。

        【中文名称】流式处理

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICodexProvider.chat_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

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
        return await self._call_codex(
            messages,
            tools,
            model,
            reasoning_effort,
            tool_choice,
            on_content_delta,
            on_thinking_delta,
            on_tool_call_delta,
        )

    def get_default_model(self) -> str:
        return self.default_model


def _strip_model_prefix(model: str) -> str:
    """执行辅助逻辑（_strip_model_prefix = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_strip_model_prefix` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    model: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if model.startswith("openai-codex/") or model.startswith("openai_codex/"):
        return model.split("/", 1)[1]
    return model


def _build_reasoning_options(reasoning_effort: str | None) -> dict[str, str] | None:
    """构建对象（_build_reasoning_options = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_build_reasoning_options` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if reasoning_effort and reasoning_effort.lower() == "none":
        return {"effort": "none"}
    options = {"summary": "auto"}
    if reasoning_effort:
        options["effort"] = reasoning_effort
    return options


def _build_headers(account_id: str, token: str) -> dict[str, str]:
    """构建对象（_build_headers = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_build_headers` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    account_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {
        "Authorization": f"Bearer {token}",
        "chatgpt-account-id": account_id,
        "OpenAI-Beta": "responses=experimental",
        "originator": DEFAULT_ORIGINATOR,
        "User-Agent": "nanobot (python)",
        "accept": "text/event-stream",
        "content-type": "application/json",
    }


class _CodexHTTPError(RuntimeError):
    """_CodexHTTPError 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】_CodexHTTPError

    【功能说明】
    OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    RuntimeError。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
        error_type: str | None = None,
        error_code: str | None = None,
        should_retry: bool | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `_CodexHTTPError.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。
        status_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        retry_after: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        error_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        error_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        should_retry: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after
        self.error_type = error_type
        self.error_code = error_code
        self.should_retry = should_retry


async def _request_codex(
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
    verify: bool,
    on_content_delta: Callable[[str], Awaitable[None]] | None = None,
    on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
    on_tool_call_delta: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
) -> tuple[str, list[ToolCallRequest], str, dict[str, int], str | None]:
    """异步执行辅助逻辑（_request_codex = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_request_codex` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    headers: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    body: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    verify: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    on_content_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    on_thinking_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    on_tool_call_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    idle_timeout_s = int(os.environ.get("NANOBOT_STREAM_IDLE_TIMEOUT_S", "90"))
    async with httpx.AsyncClient(timeout=idle_timeout_s, verify=verify) as client:
        async with client.stream("POST", url, headers=headers, json=body) as response:
            if response.status_code != 200:
                text = await response.aread()
                raw = text.decode("utf-8", "ignore")
                retry_after = LLMProvider._extract_retry_after_from_headers(response.headers)
                error_type, error_code = LLMProvider._extract_error_type_code(raw)
                raise _CodexHTTPError(
                    _friendly_error(response.status_code, raw),
                    status_code=response.status_code,
                    retry_after=retry_after,
                    error_type=error_type,
                    error_code=error_code,
                    should_retry=_should_retry_status(response.status_code, error_type, error_code, raw),
                )
            return await consume_sse_with_reasoning(
                response,
                on_content_delta=on_content_delta,
                on_tool_call_delta=on_tool_call_delta,
                on_reasoning_delta=on_thinking_delta,
            )


def _prompt_cache_key(messages: list[dict[str, Any]]) -> str:
    """执行辅助逻辑（_prompt_cache_key = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_prompt_cache_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    messages: 消息数据，可能来自用户、频道、模型或工具调用。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    raw = json.dumps(messages, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _friendly_error(status_code: int, raw: str) -> str:
    """执行辅助逻辑（_friendly_error = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_friendly_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    status_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    raw: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    _ = raw
    if status_code == 429:
        return "ChatGPT usage quota exceeded or rate limit triggered. Please try again later."
    return f"HTTP {status_code}: Codex API request failed"


def _codex_error_response(exc: Exception) -> LLMResponse:
    """执行辅助逻辑（_codex_error_response = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_codex_error_response` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    exc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    exc_type = "CodexHTTPError" if isinstance(exc, _CodexHTTPError) else type(exc).__name__
    detail = str(exc).strip()

    status_code = getattr(exc, "status_code", None)
    error_kind: str | None = None
    default_detail: str | None = None
    should_retry: bool | None = getattr(exc, "should_retry", None)

    if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
        error_kind = "timeout"
        default_detail = "timed out waiting for response"
        should_retry = True if should_retry is None else should_retry
    elif isinstance(exc, httpx.RemoteProtocolError):
        error_kind = "connection"
        default_detail = "network protocol error while reading response"
        should_retry = True if should_retry is None else should_retry
    elif isinstance(exc, (httpx.NetworkError, httpx.TransportError)):
        error_kind = "connection"
        default_detail = "network connection failed"
        should_retry = True if should_retry is None else should_retry
    elif isinstance(exc, _CodexHTTPError):
        error_kind = "http"
        default_detail = "HTTP request failed"

    if status_code is not None and should_retry is None:
        retry_content = None if int(status_code) == 429 and isinstance(exc, _CodexHTTPError) else detail
        should_retry = _should_retry_status(
            int(status_code),
            getattr(exc, "error_type", None),
            getattr(exc, "error_code", None),
            retry_content,
        )

    detail = detail or default_detail or "unexpected error"
    message = f"Error calling Codex ({exc_type}): {detail}"
    retry_after = getattr(exc, "retry_after", None) or LLMProvider._extract_retry_after(message)
    return LLMResponse(
        content=message,
        finish_reason="error",
        retry_after=retry_after,
        error_status_code=int(status_code) if status_code is not None else None,
        error_kind=error_kind,
        error_type=getattr(exc, "error_type", None),
        error_code=getattr(exc, "error_code", None),
        error_retry_after_s=retry_after,
        error_should_retry=should_retry,
    )


def _codex_log_summary(exc_type: str, response: LLMResponse) -> str:
    """执行辅助逻辑（_codex_log_summary = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_codex_log_summary` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    exc_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    response: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if response.error_status_code is not None:
        parts = [f"HTTP {response.error_status_code}"]
        if response.error_type:
            parts.append(f"type={response.error_type}")
        if response.error_code:
            parts.append(f"code={response.error_code}")
        return " ".join(parts)

    kind = (response.error_kind or "").strip()
    if kind:
        return f"{exc_type} {kind}"

    return exc_type


def _should_retry_status(
    status_code: int,
    error_type: str | None,
    error_code: str | None,
    content: str | None,
) -> bool:
    """执行辅助逻辑（_should_retry_status = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI Codex Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_should_retry_status` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    status_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    error_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    error_code: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if status_code == 429:
        return LLMProvider._is_retryable_429_response(
            LLMResponse(
                content=content or "",
                finish_reason="error",
                error_status_code=status_code,
                error_type=error_type,
                error_code=error_code,
            )
        )
    return status_code in LLMProvider._RETRYABLE_STATUS_CODES or status_code >= 500

