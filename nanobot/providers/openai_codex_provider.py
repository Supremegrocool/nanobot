"""OpenAI Codex Provider 实现。

【中文名称】OpenAI Codex Provider 实现

【功能说明】
负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """OpenAICodexProvider 类。

    【中文名称】OpenAICodexProvider

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的核心数据结构或服务类。负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    supports_progress_deltas = True

    def __init__(self, default_model: str = "openai-codex/gpt-5.1-codex"):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - default_model: 调用方传入的 `default_model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `_call_codex`。

        【中文名称】_call_codex

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。
        - on_content_delta: 调用方传入的 `on_content_delta` 数据；具体类型以函数签名为准。
        - on_thinking_delta: 调用方传入的 `on_thinking_delta` 数据；具体类型以函数签名为准。
        - on_tool_call_delta: 调用方传入的 `on_tool_call_delta` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """异步执行 `chat`。

        【中文名称】chat

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
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
        """异步执行 `chat_stream`。

        【中文名称】chat_stream

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
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
        """执行 `get_default_model`。

        【中文名称】get_default_model

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self.default_model


def _strip_model_prefix(model: str) -> str:
    """执行 `_strip_model_prefix`。

    【中文名称】_strip_model_prefix

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    if model.startswith("openai-codex/") or model.startswith("openai_codex/"):
        return model.split("/", 1)[1]
    return model


def _build_reasoning_options(reasoning_effort: str | None) -> dict[str, str] | None:
    """执行 `_build_reasoning_options`。

    【中文名称】_build_reasoning_options

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if reasoning_effort and reasoning_effort.lower() == "none":
        return {"effort": "none"}
    options = {"summary": "auto"}
    if reasoning_effort:
        options["effort"] = reasoning_effort
    return options


def _build_headers(account_id: str, token: str) -> dict[str, str]:
    """执行 `_build_headers`。

    【中文名称】_build_headers

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - account_id: 调用方传入的 `account_id` 数据；具体类型以函数签名为准。
    - token: 调用方传入的 `token` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
    """_CodexHTTPError 类。

    【中文名称】_CodexHTTPError

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的核心数据结构或服务类。负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

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
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。
        - status_code: 调用方传入的 `status_code` 数据；具体类型以函数签名为准。
        - retry_after: 调用方传入的 `retry_after` 数据；具体类型以函数签名为准。
        - error_type: 调用方传入的 `error_type` 数据；具体类型以函数签名为准。
        - error_code: 调用方传入的 `error_code` 数据；具体类型以函数签名为准。
        - should_retry: 调用方传入的 `should_retry` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
    """异步执行 `_request_codex`。

    【中文名称】_request_codex

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - url: 调用方传入的 `url` 数据；具体类型以函数签名为准。
    - headers: 调用方传入的 `headers` 数据；具体类型以函数签名为准。
    - body: 调用方传入的 `body` 数据；具体类型以函数签名为准。
    - verify: 调用方传入的 `verify` 数据；具体类型以函数签名为准。
    - on_content_delta: 调用方传入的 `on_content_delta` 数据；具体类型以函数签名为准。
    - on_thinking_delta: 调用方传入的 `on_thinking_delta` 数据；具体类型以函数签名为准。
    - on_tool_call_delta: 调用方传入的 `on_tool_call_delta` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
    """执行 `_prompt_cache_key`。

    【中文名称】_prompt_cache_key

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    raw = json.dumps(messages, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _friendly_error(status_code: int, raw: str) -> str:
    """执行 `_friendly_error`。

    【中文名称】_friendly_error

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - status_code: 调用方传入的 `status_code` 数据；具体类型以函数签名为准。
    - raw: 调用方传入的 `raw` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    _ = raw
    if status_code == 429:
        return "ChatGPT usage quota exceeded or rate limit triggered. Please try again later."
    return f"HTTP {status_code}: Codex API request failed"


def _codex_error_response(exc: Exception) -> LLMResponse:
    """执行 `_codex_error_response`。

    【中文名称】_codex_error_response

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_codex_log_summary`。

    【中文名称】_codex_log_summary

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - exc_type: 调用方传入的 `exc_type` 数据；具体类型以函数签名为准。
    - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_should_retry_status`。

    【中文名称】_should_retry_status

    【功能说明】
    这是 OpenAI Codex Provider 实现 中的一个步骤函数，用来支撑：负责对接 Codex CLI/服务的模型接口，处理认证、会话和流式输出。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - status_code: 调用方传入的 `status_code` 数据；具体类型以函数签名为准。
    - error_type: 调用方传入的 `error_type` 数据；具体类型以函数签名为准。
    - error_code: 调用方传入的 `error_code` 数据；具体类型以函数签名为准。
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
