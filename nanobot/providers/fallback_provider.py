"""Fallback Provider 实现。

【中文名称】Fallback Provider 实现

【功能说明】
负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from nanobot.providers.base import LLMProvider, LLMResponse

# 说明：这里处理 Fallback Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
_PRIMARY_FAILURE_THRESHOLD = 3
_PRIMARY_COOLDOWN_S = 60
_MISSING = object()
_FALLBACK_ERROR_KINDS = frozenset({
    "timeout",
    "connection",
    "server_error",
    "rate_limit",
    "overloaded",
})
_NON_FALLBACK_ERROR_KINDS = frozenset({
    "authentication",
    "auth",
    "permission",
    "content_filter",
    "refusal",
    "context_length",
    "invalid_request",
})
_FALLBACK_ERROR_TOKENS = (
    "rate_limit",
    "rate limit",
    "too_many_requests",
    "too many requests",
    "overloaded",
    "server_error",
    "server error",
    "temporarily unavailable",
    "timeout",
    "timed out",
    "connection",
    "insufficient_quota",
    "insufficient quota",
    "quota_exceeded",
    "quota exceeded",
    "quota_exhausted",
    "quota exhausted",
    "billing_hard_limit",
    "insufficient_balance",
    "balance",
    "out of credits",
)


class FallbackProvider(LLMProvider):
    """FallbackProvider 类。

    【中文名称】FallbackProvider

    【功能说明】
    这是 Fallback Provider 实现 中的核心数据结构或服务类。负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    supports_stream_recover_callback = True

    def __init__(
        self,
        primary: LLMProvider,
        fallback_presets: list[Any],
        provider_factory: Callable[[Any], LLMProvider],
    ):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - primary: 调用方传入的 `primary` 数据；具体类型以函数签名为准。
        - fallback_presets: 调用方传入的 `fallback_presets` 数据；具体类型以函数签名为准。
        - provider_factory: 调用方传入的 `provider_factory` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._primary = primary
        self._fallback_presets = list(fallback_presets)
        self._provider_factory = provider_factory
        self._has_fallbacks = bool(fallback_presets)
        self._primary_failures = 0
        self._primary_tripped_at: float | None = None

    @property
    def generation(self):
        """执行 `generation`。

        【中文名称】generation

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self._primary.generation

    @generation.setter
    def generation(self, value):
        """执行 `generation`。

        【中文名称】generation

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._primary.generation = value

    def get_default_model(self) -> str:
        """执行 `get_default_model`。

        【中文名称】get_default_model

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self._primary.get_default_model()

    @property
    def supports_progress_deltas(self) -> bool:
        """执行 `supports_progress_deltas`。

        【中文名称】supports_progress_deltas

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return bool(getattr(self._primary, "supports_progress_deltas", False))

    def _primary_available(self) -> bool:
        """执行 `_primary_available`。

        【中文名称】_primary_available

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._primary_tripped_at is None:
            return True
        if time.monotonic() - self._primary_tripped_at >= _PRIMARY_COOLDOWN_S:
            # 说明：这里处理 Fallback Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
            return True
        return False

    async def chat(self, **kwargs: Any) -> LLMResponse:
        """异步执行 `chat`。

        【中文名称】chat

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - **kwargs: 调用方传入的 `kwargs` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not self._has_fallbacks:
            return await self._primary.chat(**kwargs)
        return await self._try_with_fallback(
            lambda p, kw: p.chat(**kw), kwargs, has_streamed=None
        )

    async def chat_stream(self, **kwargs: Any) -> LLMResponse:
        """异步执行 `chat_stream`。

        【中文名称】chat_stream

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - **kwargs: 调用方传入的 `kwargs` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        on_stream_recover = kwargs.pop("on_stream_recover", None)
        if not self._has_fallbacks:
            return await self._primary.chat_stream(**kwargs)

        has_streamed: list[bool] = [False]
        original_delta = kwargs.get("on_content_delta")

        async def _tracking_delta(text: str) -> None:
            """异步执行 `_tracking_delta`。

            【中文名称】_tracking_delta

            【功能说明】
            这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            if text:
                has_streamed[0] = True
            if original_delta:
                await original_delta(text)

        kwargs["on_content_delta"] = _tracking_delta
        return await self._try_with_fallback(
            lambda p, kw: p.chat_stream(**kw),
            kwargs,
            has_streamed=has_streamed,
            on_stream_recover=on_stream_recover,
        )

    async def _try_with_fallback(
        self,
        call: Callable[[LLMProvider, dict[str, Any]], Awaitable[LLMResponse]],
        kwargs: dict[str, Any],
        has_streamed: list[bool] | None,
        on_stream_recover: Callable[[], Awaitable[None]] | None = None,
    ) -> LLMResponse:
        """异步执行 `_try_with_fallback`。

        【中文名称】_try_with_fallback

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - call: 调用方传入的 `call` 数据；具体类型以函数签名为准。
        - kwargs: 调用方传入的 `kwargs` 数据；具体类型以函数签名为准。
        - has_streamed: 调用方传入的 `has_streamed` 数据；具体类型以函数签名为准。
        - on_stream_recover: 调用方传入的 `on_stream_recover` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        primary_model = kwargs.get("model") or self._primary.get_default_model()

        if self._primary_available():
            response = await call(self._primary, kwargs)
            if response.finish_reason != "error":
                self._primary_failures = 0
                self._primary_tripped_at = None
                return response

            if has_streamed is not None and has_streamed[0]:
                is_timeout = (response.error_kind or "").lower() == "timeout"
                if is_timeout:
                    logger.warning(
                        "Primary model '{}' stream stalled after content was emitted; "
                        "attempting failover anyway",
                        primary_model,
                    )
                    has_streamed[0] = False
                    if on_stream_recover:
                        await on_stream_recover()
                    else:
                        kwargs["on_content_delta"] = None
                else:
                    logger.warning(
                        "Primary model error but content already streamed; skipping failover"
                    )
                    return response

            if not self._should_fallback(response):
                logger.warning(
                    "Primary model '{}' returned non-fallbackable error: {}",
                    primary_model,
                    (response.content or "")[:120],
                )
                return response

            self._primary_failures += 1
            if self._primary_failures >= _PRIMARY_FAILURE_THRESHOLD:
                self._primary_tripped_at = time.monotonic()
                logger.warning(
                    "Primary model '{}' circuit open after {} consecutive failures",
                    primary_model, self._primary_failures,
                )
        else:
            logger.debug("Primary model '{}' circuit open; skipping", primary_model)

        last_response: LLMResponse | None = None
        primary_skipped = not self._primary_available()
        for idx, fallback in enumerate(self._fallback_presets):
            fallback_model = fallback.model
            if has_streamed is not None and has_streamed[0]:
                is_timeout = (
                    last_response is not None
                    and (last_response.error_kind or "").lower() == "timeout"
                )
                if is_timeout and on_stream_recover:
                    logger.warning(
                        "Fallback model '{}' stream stalled after content was emitted; "
                        "starting a new stream segment and trying next fallback",
                        self._fallback_presets[idx - 1].model if idx > 0 else primary_model,
                    )
                    has_streamed[0] = False
                    await on_stream_recover()
                else:
                    break
            if idx == 0 and primary_skipped:
                logger.info(
                    "Primary model '{}' circuit open, trying fallback '{}'",
                    primary_model, fallback_model,
                )
            elif idx == 0:
                logger.info(
                    "Primary model '{}' failed, trying fallback '{}'",
                    primary_model, fallback_model,
                )
            else:
                logger.info(
                    "Fallback '{}' also failed, trying next fallback '{}'",
                    self._fallback_presets[idx - 1].model, fallback_model,
                )
            try:
                fallback_provider = self._provider_factory(fallback)
            except Exception as exc:
                logger.warning(
                    "Failed to create provider for fallback '{}': {}", fallback_model, exc
                )
                continue

            original_values = {
                name: kwargs.get(name, _MISSING)
                for name in ("model", "max_tokens", "temperature", "reasoning_effort")
            }
            kwargs["model"] = fallback_model
            kwargs["max_tokens"] = fallback.max_tokens
            kwargs["temperature"] = fallback.temperature
            if fallback.reasoning_effort is None:
                kwargs.pop("reasoning_effort", None)
            else:
                kwargs["reasoning_effort"] = fallback.reasoning_effort
            try:
                fallback_response = await call(fallback_provider, kwargs)
            finally:
                for name, value in original_values.items():
                    if value is _MISSING:
                        kwargs.pop(name, None)
                    else:
                        kwargs[name] = value

            if fallback_response.finish_reason != "error":
                logger.info(
                    "Fallback '{}' succeeded after primary '{}' failed",
                    fallback_model, primary_model,
                )
                return fallback_response

            last_response = fallback_response
            logger.warning(
                "Fallback '{}' also failed: {}",
                fallback_model,
                (fallback_response.content or "")[:120],
            )

        logger.warning(
            "All {} fallback model(s) failed",
            len(self._fallback_presets),
        )
        # 说明：这里处理 Fallback Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        if last_response is not None:
            return last_response
        # 说明：这里处理 Fallback Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
        return LLMResponse(
            content=f"Primary model '{primary_model}' circuit open and no fallbacks available",
            finish_reason="error",
        )

    @staticmethod
    def _should_fallback(response: LLMResponse) -> bool:
        """执行 `_should_fallback`。

        【中文名称】_should_fallback

        【功能说明】
        这是 Fallback Provider 实现 中的一个步骤函数，用来支撑：负责按顺序尝试多个模型 Provider，在主模型失败时自动切换备用模型并保留统一调用接口。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if response.error_should_retry is False:
            return False
        status = response.error_status_code
        kind = (response.error_kind or "").lower()
        error_type = (response.error_type or "").lower()
        code = (response.error_code or "").lower()
        text = (response.content or "").lower()

        if status in {400, 401, 403, 404, 422}:
            return False
        if kind in _NON_FALLBACK_ERROR_KINDS:
            return False
        if any(token in value for value in (kind, error_type, code) for token in _NON_FALLBACK_ERROR_KINDS):
            return False
        if response.error_should_retry is True:
            return True
        if status is not None and (status in {408, 409, 429} or 500 <= status <= 599):
            return True
        if kind in _FALLBACK_ERROR_KINDS:
            return True
        return any(token in value for value in (kind, error_type, code, text) for token in _FALLBACK_ERROR_TOKENS)
