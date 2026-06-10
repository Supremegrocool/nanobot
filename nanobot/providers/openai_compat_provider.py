"""OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/openai_compat_provider.py

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
import importlib.util
import json
import os
import secrets
import string
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from ipaddress import ip_address
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from loguru import logger

from nanobot.providers.base import (
    LLMProvider,
    LLMResponse,
    ToolCallRequest,
    parse_tool_arguments,
    tool_arguments_json_for_replay,
)
from nanobot.providers.openai_responses import (
    consume_sdk_stream,
    convert_messages,
    convert_tools,
    parse_response_output,
)

if TYPE_CHECKING:
    from openai import AsyncOpenAI as AsyncOpenAIType

    from nanobot.providers.registry import ProviderSpec

# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
AsyncOpenAI: Any = None

_ALLOWED_MSG_KEYS = frozenset({
    "role", "content", "tool_calls", "tool_call_id", "name",
    "reasoning_content", "extra_content",
})
_ALNUM = string.ascii_letters + string.digits

_STANDARD_TC_KEYS = frozenset({"id", "type", "index", "function"})
_STANDARD_FN_KEYS = frozenset({"name", "arguments"})
_DEFAULT_OPENROUTER_HEADERS = {
    "HTTP-Referer": "https://github.com/HKUDS/nanobot",
    "X-OpenRouter-Title": "nanobot",
    "X-OpenRouter-Categories": "cli-agent,personal-agent",
}
_KIMI_THINKING_MODELS: frozenset[str] = frozenset({
    "kimi-k2.5",
    "kimi-k2.6",
    "k2.6-code-preview",
})
# 中文说明：这一段围绕模型处理，注意输入、输出和异常路径。
# 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
_MIMO_THINKING_MODELS: frozenset[str] = frozenset({
    "mimo-v2.5-pro",
    "mimo-v2.5",
    "mimo-v2-pro",
    "mimo-v2-omni",
})
_OPENAI_COMPAT_REQUEST_TIMEOUT_S = 120.0

# 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
# 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
# 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
_THINKING_STYLE_MAP: dict[str, Any] = {
    "thinking_type": lambda on: {"thinking": {"type": "enabled" if on else "disabled"}},
    "enable_thinking": lambda on: {"enable_thinking": on},
    "reasoning_split": lambda on: {"reasoning_split": on},
}
_GATEWAY_REASONING_STYLE_MAP: dict[str, Any] = {
    "reasoning_effort": lambda effort: {"reasoning": {"effort": effort}},
}
_MODEL_THINKING_STYLES: dict[str, str] = {
    **dict.fromkeys(_KIMI_THINKING_MODELS, "thinking_type"),
    **dict.fromkeys(_MIMO_THINKING_MODELS, "thinking_type"),
}


def _model_slug(model_name: str) -> str:
    return model_name.lower().rsplit("/", 1)[-1]


def _requires_max_completion_tokens(model_name: str) -> bool:
    """执行辅助逻辑（_requires_max_completion_tokens = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_requires_max_completion_tokens` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    model_name: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    slug = _model_slug(model_name)
    return "gpt-5" in slug or any(
        slug == p or slug.startswith((p + "-", p + ".")) for p in ("o1", "o3", "o4")
    )


def _model_thinking_style(model_name: str) -> str:
    return _MODEL_THINKING_STYLES.get(_model_slug(model_name), "")


def _thinking_styles_for(spec: ProviderSpec | None, model_name: str) -> list[str]:
    """执行辅助逻辑（_thinking_styles_for = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_thinking_styles_for` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    spec: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model_name: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    styles: list[str] = []
    if spec and spec.thinking_style:
        styles.append(spec.thinking_style)
    model_style = _model_thinking_style(model_name)
    if model_style and model_style not in styles:
        styles.append(model_style)
    return styles


def _thinking_extra_body(style: str, thinking_enabled: bool) -> dict[str, Any] | None:
    """执行辅助逻辑（_thinking_extra_body = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_thinking_extra_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    style: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    thinking_enabled: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    builder = _THINKING_STYLE_MAP.get(style)
    return builder(thinking_enabled) if builder else None


def _gateway_reasoning_extra_body(style: str, effort: str | None) -> dict[str, Any] | None:
    """执行辅助逻辑（_gateway_reasoning_extra_body = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_gateway_reasoning_extra_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    style: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not effort:
        return None
    builder = _GATEWAY_REASONING_STYLE_MAP.get(style)
    return builder(effort) if builder else None


def _openai_compat_timeout_s() -> float:
    """执行辅助逻辑（_openai_compat_timeout_s = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_openai_compat_timeout_s` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return _float_env("NANOBOT_OPENAI_COMPAT_TIMEOUT_S", _OPENAI_COMPAT_REQUEST_TIMEOUT_S)


def _float_env(name: str, default: float) -> float:
    """执行辅助逻辑（_float_env = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_float_env` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    default: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid {}={!r}; using {}", name, raw, default)
        return default
    if value <= 0:
        logger.warning("Ignoring non-positive {}={!r}; using {}", name, raw, default)
        return default
    return value


def _short_tool_id() -> str:
    """执行辅助逻辑（_short_tool_id = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_short_tool_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return "".join(secrets.choice(_ALNUM) for _ in range(9))


def _get(obj: Any, key: str) -> Any:
    """执行辅助逻辑（_get = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_get` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    obj: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _coerce_dict(value: Any) -> dict[str, Any] | None:
    """执行辅助逻辑（_coerce_dict = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_coerce_dict` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value if value else None
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict) and dumped:
            return dumped
    return None


def _extract_tc_extras(tc: Any) -> tuple[
    dict[str, Any] | None,
    dict[str, Any] | None,
    dict[str, Any] | None,
]:
    """提取信息（_extract_tc_extras = 原函数名）。

    【中文名称】提取信息

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_extract_tc_extras` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    extra_content = _coerce_dict(_get(tc, "extra_content"))

    tc_dict = _coerce_dict(tc)
    prov = None
    fn_prov = None
    if tc_dict is not None:
        leftover = {k: v for k, v in tc_dict.items()
                    if k not in _STANDARD_TC_KEYS and k != "extra_content" and v is not None}
        if leftover:
            prov = leftover
        fn = _coerce_dict(tc_dict.get("function"))
        if fn is not None:
            fn_leftover = {k: v for k, v in fn.items()
                          if k not in _STANDARD_FN_KEYS and v is not None}
            if fn_leftover:
                fn_prov = fn_leftover
    else:
        prov = _coerce_dict(_get(tc, "provider_specific_fields"))
        fn_obj = _get(tc, "function")
        if fn_obj is not None:
            fn_prov = _coerce_dict(_get(fn_obj, "provider_specific_fields"))

    return extra_content, prov, fn_prov


def _uses_openrouter_attribution(spec: "ProviderSpec | None", api_base: str | None) -> bool:
    """执行辅助逻辑（_uses_openrouter_attribution = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_uses_openrouter_attribution` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    spec: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if spec and spec.name == "openrouter":
        return True
    return bool(api_base and "openrouter" in api_base.lower())


_RESPONSES_FAILURE_THRESHOLD = 3
_RESPONSES_PROBE_INTERVAL_S = 300  # 中文说明：5 minutes 相关逻辑。


def _is_local_endpoint(
    spec: "ProviderSpec | None",
    api_base: str | None,
) -> bool:
    """判断条件是否成立（_is_local_endpoint = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_is_local_endpoint` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    spec: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if spec and spec.is_local:
        return True
    if not api_base:
        return False
    raw = api_base.strip().lower()
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    try:
        host = parsed.hostname
    except ValueError:
        return False
    if host in {"localhost", "host.docker.internal"}:
        return True
    if not host:
        return False
    try:
        addr = ip_address(host)
    except ValueError:
        return False
    return addr.is_loopback or addr.is_private


def _is_direct_openai_base(api_base: str | None) -> bool:
    """判断条件是否成立（_is_direct_openai_base = 原函数名）。

    【中文名称】判断条件是否成立

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_is_direct_openai_base` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not api_base:
        return True
    normalized = api_base.strip().lower().rstrip("/")
    return "api.openai.com" in normalized and "openrouter" not in normalized


def _responses_circuit_key(
    model: str | None,
    default_model: str,
    reasoning_effort: str | None,
) -> str:
    """执行辅助逻辑（_responses_circuit_key = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_responses_circuit_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    default_model: 模型名称或模型配置，用于选择具体 LLM 能力。
    reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    model_name = (model or default_model).lower()
    effort = reasoning_effort.lower() if isinstance(reasoning_effort, str) else ""
    return f"{model_name}:{effort}"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """合并内容（_deep_merge = 原函数名）。

    【中文名称】合并内容

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_deep_merge` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    override: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    merged = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _merge_unique_list(base: Any, override: Any) -> Any:
    """合并内容（_merge_unique_list = 原函数名）。

    【中文名称】合并内容

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_merge_unique_list` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    override: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not isinstance(base, list) or not isinstance(override, list):
        return override
    result: list[Any] = []
    seen: set[str] = set()
    for value in [*base, *override]:
        try:
            key = json.dumps(value, sort_keys=True, ensure_ascii=False)
        except Exception:
            key = repr(value)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _merge_responses_extra_body(
    body: dict[str, Any],
    extra_body: dict[str, Any],
) -> dict[str, Any]:
    """合并内容（_merge_responses_extra_body = 原函数名）。

    【中文名称】合并内容

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_merge_responses_extra_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    body: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    extra_body: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    reserved = {"include", "tools"}
    regular_extra = {key: value for key, value in extra_body.items() if key not in reserved}
    merged = _deep_merge(body, regular_extra)

    if "include" in extra_body:
        merged["include"] = _merge_unique_list(body.get("include"), extra_body["include"])

    if "tools" in extra_body:
        current_tools = body.get("tools")
        configured_tools = extra_body["tools"]
        if isinstance(current_tools, list) and isinstance(configured_tools, list):
            merged["tools"] = [*current_tools, *configured_tools]
        else:
            merged["tools"] = configured_tools

    return merged


class OpenAICompatProvider(LLMProvider):
    """OpenAICompatProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】OpenAICompatProvider

    【功能说明】
    OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    LLMProvider。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        default_model: str = "gpt-4o",
        extra_headers: dict[str, str] | None = None,
        spec: ProviderSpec | None = None,
        extra_body: dict[str, Any] | None = None,
        api_type: str = "auto",
        extra_query: dict[str, str] | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        default_model: 模型名称或模型配置，用于选择具体 LLM 能力。
        extra_headers: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        spec: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        extra_body: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_type: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        extra_query: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        super().__init__(api_key, api_base)
        self.default_model = default_model
        self.extra_headers = extra_headers or {}
        self._spec = spec
        self._extra_body = extra_body or {}
        self._api_type = api_type if spec and spec.name == "openai" else "auto"
        self._extra_query = extra_query or {}

        if api_key and spec and spec.env_key:
            self._setup_env(api_key, api_base)

        effective_base = api_base or (spec.default_api_base if spec else None) or None
        self._effective_base = effective_base
        self._default_headers = {"x-session-affinity": uuid.uuid4().hex}
        if _uses_openrouter_attribution(spec, effective_base):
            self._default_headers.update(_DEFAULT_OPENROUTER_HEADERS)
        if extra_headers:
            self._default_headers.update(extra_headers)
        self._api_key_for_client = api_key or "no-key"
        self._is_local = _is_local_endpoint(spec, effective_base)

        # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        self._client: AsyncOpenAIType | None = None
        self._client_lock = asyncio.Lock()

        # 中文说明：这一段围绕响应、API处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
        self._responses_failures: dict[str, int] = {}
        self._responses_tripped_at: dict[str, float] = {}

    def _build_client(self) -> None:
        """构建对象（_build_client = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._build_client` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        import httpx

        timeout_s = _openai_compat_timeout_s()
        http_client: httpx.AsyncClient | None = None
        if self._is_local:
            # 中文说明：这一段围绕模型处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕HTTP处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕调用处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕调用处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕API、错误处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：这一段围绕请求处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            http_client = httpx.AsyncClient(
                limits=httpx.Limits(keepalive_expiry=0),
                timeout=timeout_s,
            )
        self._client = AsyncOpenAI(
            api_key=self._api_key_for_client,
            base_url=self._effective_base,
            default_headers=self._default_headers,
            default_query=self._extra_query or None,
            max_retries=0,
            timeout=timeout_s,
            http_client=http_client,
        )

    async def _ensure_client(self):
        """异步确保前置条件成立（_ensure_client = 原函数名）。

        【中文名称】确保前置条件成立

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._ensure_client` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._client is not None:
            return self._client
        async with self._client_lock:
            if self._client is not None:
                return self._client
            global AsyncOpenAI
            if AsyncOpenAI is None:
                if os.environ.get("LANGFUSE_SECRET_KEY") and importlib.util.find_spec("langfuse"):
                    from langfuse.openai import AsyncOpenAI as _AsyncOpenAI
                else:
                    if os.environ.get("LANGFUSE_SECRET_KEY"):
                        logger.warning(
                            "LANGFUSE_SECRET_KEY is set but langfuse is not installed; "
                            "install with `pip install langfuse` to enable tracing"
                        )
                    from openai import AsyncOpenAI as _AsyncOpenAI
                AsyncOpenAI = _AsyncOpenAI

            self._build_client()
            return self._client

    def _setup_env(self, api_key: str, api_base: str | None) -> None:
        """执行辅助逻辑（_setup_env = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._setup_env` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        spec = self._spec
        if not spec or not spec.env_key:
            return
        if spec.is_gateway:
            os.environ[spec.env_key] = api_key
        else:
            os.environ.setdefault(spec.env_key, api_key)
        effective_base = api_base or spec.default_api_base
        for env_name, env_val in spec.env_extras:
            resolved = env_val.replace("{api_key}", api_key).replace("{api_base}", effective_base)
            os.environ.setdefault(env_name, resolved)

    @classmethod
    def _apply_cache_control(
        cls,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]] | None]:
        """执行辅助逻辑（_apply_cache_control = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._apply_cache_control` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        cache_marker = {"type": "ephemeral"}
        new_messages = list(messages)

        def _mark(msg: dict[str, Any]) -> dict[str, Any]:
            """执行辅助逻辑（_mark = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider._mark` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            msg: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            content = msg.get("content")
            if isinstance(content, str):
                return {**msg, "content": [
                    {"type": "text", "text": content, "cache_control": cache_marker},
                ]}
            if isinstance(content, list) and content:
                nc = list(content)
                nc[-1] = {**nc[-1], "cache_control": cache_marker}
                return {**msg, "content": nc}
            return msg

        if new_messages and new_messages[0].get("role") == "system":
            new_messages[0] = _mark(new_messages[0])
        if len(new_messages) >= 3:
            new_messages[-2] = _mark(new_messages[-2])

        new_tools = tools
        if tools:
            new_tools = list(tools)
            for idx in cls._tool_cache_marker_indices(new_tools):
                new_tools[idx] = {**new_tools[idx], "cache_control": cache_marker}
        return new_messages, new_tools

    @staticmethod
    def _normalize_tool_call_id(tool_call_id: Any) -> Any:
        """标准化数据（_normalize_tool_call_id = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._normalize_tool_call_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        tool_call_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not isinstance(tool_call_id, str):
            return tool_call_id
        if len(tool_call_id) == 9 and tool_call_id.isalnum():
            return tool_call_id
        return hashlib.sha1(tool_call_id.encode()).hexdigest()[:9]

    def _should_normalize_tool_call_ids(self) -> bool:
        """标准化数据（_should_normalize_tool_call_ids = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._should_normalize_tool_call_ids` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return bool(self._spec and self._spec.name == "mistral")

    @staticmethod
    def _coerce_content_to_string(content: Any) -> str | None:
        """执行辅助逻辑（_coerce_content_to_string = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._coerce_content_to_string` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if content is None or isinstance(content, str):
            return content
        text = OpenAICompatProvider._extract_text_content(content)
        if isinstance(text, str) and text:
            return text
        try:
            dumped = json.dumps(content, ensure_ascii=False)
        except Exception:
            dumped = str(content)
        return dumped or "(empty)"

    def _sanitize_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """执行辅助逻辑（_sanitize_messages = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._sanitize_messages` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        sanitized = LLMProvider._sanitize_request_messages(messages, _ALLOWED_MSG_KEYS)
        id_map: dict[str, str] = {}
        pending_tool_ids: dict[str, deque[str]] = {}
        force_string_content = bool(self._spec and self._spec.name == "deepseek")
        normalize_tool_ids = self._should_normalize_tool_call_ids()

        def map_id(value: Any) -> Any:
            """执行辅助逻辑（map_id = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider.map_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if not isinstance(value, str):
                return value
            if not normalize_tool_ids:
                return value
            return id_map.setdefault(value, self._normalize_tool_call_id(value))

        def unique_tool_id(value: Any, used_ids: set[str], idx: int) -> str:
            """执行辅助逻辑（unique_tool_id = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider.unique_tool_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            used_ids: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            idx: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if isinstance(value, str) and value:
                base = map_id(value)
            else:
                base = _short_tool_id()
            if not isinstance(base, str) or not base:
                base = _short_tool_id()
            if base not in used_ids:
                return base
            seed = value if isinstance(value, str) and value else base
            salt = 1
            while True:
                candidate = self._normalize_tool_call_id(f"{seed}:{idx}:{salt}")
                if isinstance(candidate, str) and candidate not in used_ids:
                    return candidate
                salt += 1

        def map_tool_result_id(value: Any) -> Any:
            """执行辅助逻辑（map_tool_result_id = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider.map_tool_result_id` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if not isinstance(value, str):
                return value
            queue = pending_tool_ids.get(value)
            if queue:
                mapped = queue.popleft()
                if not queue:
                    pending_tool_ids.pop(value, None)
                return mapped
            return map_id(value)

        for clean in sanitized:
            if isinstance(clean.get("tool_calls"), list):
                normalized = []
                used_ids: set[str] = set()
                for idx, tc in enumerate(clean["tool_calls"]):
                    if not isinstance(tc, dict):
                        normalized.append(tc)
                        continue
                    tc_clean = dict(tc)
                    raw_id = tc_clean.get("id")
                    mapped_id = unique_tool_id(raw_id, used_ids, idx)
                    tc_clean["id"] = mapped_id
                    used_ids.add(mapped_id)
                    if isinstance(raw_id, str) and raw_id:
                        pending_tool_ids.setdefault(raw_id, deque()).append(mapped_id)
                    function = tc_clean.get("function")
                    if isinstance(function, dict):
                        function_clean = dict(function)
                        if "arguments" in function_clean:
                            function_clean["arguments"] = tool_arguments_json_for_replay(
                                function_clean.get("arguments")
                            )
                        else:
                            function_clean["arguments"] = "{}"
                        tc_clean["function"] = function_clean
                    normalized.append(tc_clean)
                clean["tool_calls"] = normalized
                if clean.get("role") == "assistant":
                    # 中文说明：这一段围绕消息、助手处理，注意输入、输出和异常路径。
                    # 中文说明：这一段围绕工具、调用处理，注意输入、输出和异常路径。
                    clean["content"] = None
            if "tool_call_id" in clean and clean["tool_call_id"]:
                clean["tool_call_id"] = map_tool_result_id(clean["tool_call_id"])
            if (
                force_string_content
                and not (clean.get("role") == "assistant" and clean.get("tool_calls"))
            ):
                clean["content"] = self._coerce_content_to_string(clean.get("content"))
        return self._enforce_role_alternation(sanitized)

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：Build kwargs 相关逻辑。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    @staticmethod
    def _supports_temperature(
        model_name: str,
        reasoning_effort: str | None = None,
    ) -> bool:
        """执行辅助逻辑（_supports_temperature = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._supports_temperature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        model_name: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if reasoning_effort and reasoning_effort.lower() != "none":
            return False
        name = model_name.lower()
        return not any(token in name for token in ("gpt-5", "o1", "o3", "o4"))

    def _build_kwargs(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        model: str | None,
        max_tokens: int,
        temperature: float,
        reasoning_effort: str | None,
        tool_choice: str | dict[str, Any] | None,
    ) -> dict[str, Any]:
        """构建对象（_build_kwargs = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._build_kwargs` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

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
        model_name = model or self.default_model
        spec = self._spec

        if spec and spec.supports_prompt_caching:
            model_name = model or self.default_model
            if any(model_name.lower().startswith(k) for k in ("anthropic/", "claude")):
                messages, tools = self._apply_cache_control(messages, tools)

        if spec and spec.strip_model_prefix:
            model_name = model_name.split("/")[-1]

        kwargs: dict[str, Any] = {
            "model": model_name,
            "messages": self._sanitize_messages(self._sanitize_empty_content(messages)),
        }

        # 中文说明：这一段围绕模型处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if self._supports_temperature(model_name, reasoning_effort):
            kwargs["temperature"] = temperature

        if (
            spec and getattr(spec, "supports_max_completion_tokens", False)
        ) or _requires_max_completion_tokens(model_name):
            kwargs["max_completion_tokens"] = max(1, max_tokens)
        else:
            kwargs["max_tokens"] = max(1, max_tokens)

        if spec:
            model_lower = model_name.lower()
            for pattern, overrides in spec.model_overrides:
                if pattern in model_lower:
                    kwargs.update(overrides)
                    break

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        semantic_effort: str | None = None
        if isinstance(reasoning_effort, str):
            semantic_effort = reasoning_effort.lower()
            if semantic_effort == "minimum":
                semantic_effort = "minimal"

        wire_effort = reasoning_effort
        if spec and spec.name == "dashscope" and semantic_effort == "minimal":
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            wire_effort = "minimum"

        if wire_effort and semantic_effort != "none":
            kwargs["reasoning_effort"] = wire_effort

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：保留。
        if reasoning_effort is not None:
            thinking_enabled = semantic_effort not in ("none", "minimal")
            for thinking_style in _thinking_styles_for(spec, model_name):
                extra = _thinking_extra_body(thinking_style, thinking_enabled)
                if extra:
                    kwargs.setdefault("extra_body", {}).update(extra)
            gateway_style = getattr(spec, "gateway_reasoning_style", "") if spec else ""
            if gateway_style and _model_thinking_style(model_name):
                extra = _gateway_reasoning_extra_body(gateway_style, semantic_effort)
                if extra:
                    kwargs.setdefault("extra_body", {}).update(extra)

            # 中文说明：这一段围绕请求处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            # 中文说明：这一段围绕Provider、用户处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕模型处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕API处理，注意输入、输出和异常路径。
            if _model_slug(model_name) in _KIMI_THINKING_MODELS:
                kwargs.pop("reasoning_effort", None)

        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = tool_choice or "auto"

        # 中文说明：这一段围绕助手处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕历史记录处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        explicit_thinking = (
            reasoning_effort is not None
            and semantic_effort not in ("none", "minimal")
            and (
                (spec and spec.thinking_style)
                or _model_thinking_style(model_name)
            )
        )
        implicit_deepseek_thinking = (
            spec is not None
            and spec.name == "deepseek"
            and semantic_effort not in ("none", "minimal", "minimum")
            and any(t in model_name.lower() for t in ("deepseek-v4", "deepseek-reasoner"))
        )
        if explicit_thinking or implicit_deepseek_thinking:
            for msg in kwargs["messages"]:
                if msg.get("role") == "assistant" and "reasoning_content" not in msg:
                    msg["reasoning_content"] = ""

        # 中文说明：这一段围绕用户、配置处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕JSON处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if self._extra_body:
            existing = kwargs.get("extra_body", {})
            kwargs["extra_body"] = _deep_merge(existing, self._extra_body)

        return kwargs

    def _should_use_responses_api(
        self,
        model: str | None,
        reasoning_effort: str | None,
    ) -> bool:
        """执行辅助逻辑（_should_use_responses_api = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._should_use_responses_api` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._api_type == "chat_completions":
            return False
        if self._spec and self._spec.name not in ("openai", "github_copilot"):
            return False
        if self._api_type == "responses":
            # 中文说明：这一段围绕响应、配置处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            return True
        if self._spec is None or self._spec.name != "github_copilot":
            if not _is_direct_openai_base(self._effective_base):
                return False

        model_name = (model or self.default_model).lower()
        wants = False
        if reasoning_effort and reasoning_effort.lower() != "none":
            wants = True
        elif any(token in model_name for token in ("gpt-5", "o1", "o3", "o4")):
            wants = True
        if not wants:
            return False

        return self._responses_circuit_allows_probe(model, reasoning_effort)

    def _responses_circuit_allows_probe(
        self,
        model: str | None,
        reasoning_effort: str | None,
    ) -> bool:
        """执行辅助逻辑（_responses_circuit_allows_probe = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._responses_circuit_allows_probe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        key = _responses_circuit_key(model, self.default_model, reasoning_effort)
        failures = self._responses_failures.get(key, 0)
        if failures >= _RESPONSES_FAILURE_THRESHOLD:
            tripped = self._responses_tripped_at.get(key, 0.0)
            if (time.monotonic() - tripped) < _RESPONSES_PROBE_INTERVAL_S:
                return False
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        return True

    def _record_responses_failure(self, model: str | None, reasoning_effort: str | None) -> None:
        """执行辅助逻辑（_record_responses_failure = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._record_responses_failure` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        key = _responses_circuit_key(model, self.default_model, reasoning_effort)
        count = self._responses_failures.get(key, 0) + 1
        self._responses_failures[key] = count
        if count >= _RESPONSES_FAILURE_THRESHOLD:
            self._responses_tripped_at[key] = time.monotonic()
            logger.warning(
                "Responses API circuit open for {} — falling back to Chat Completions",
                key,
            )

    def _record_responses_success(self, model: str | None, reasoning_effort: str | None) -> None:
        """执行辅助逻辑（_record_responses_success = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._record_responses_success` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        key = _responses_circuit_key(model, self.default_model, reasoning_effort)
        self._responses_failures.pop(key, None)
        self._responses_tripped_at.pop(key, None)

    @staticmethod
    def _should_fallback_from_responses_error(e: Exception) -> bool:
        """执行辅助逻辑（_should_fallback_from_responses_error = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._should_fallback_from_responses_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        e: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        response = getattr(e, "response", None)
        status_code = getattr(e, "status_code", None)
        if status_code is None and response is not None:
            status_code = getattr(response, "status_code", None)
        if status_code not in {400, 404, 422}:
            return False

        body = (
            getattr(e, "body", None)
            or getattr(e, "doc", None)
            or getattr(response, "text", None)
        )
        body_text = str(body).lower() if body is not None else ""
        compatibility_markers = (
            "responses",
            "response api",
            "max_output_tokens",
            "instructions",
            "previous_response",
            "unsupported",
            "not supported",
            "unknown parameter",
            "unrecognized request argument",
        )
        return any(marker in body_text for marker in compatibility_markers)

    def _build_responses_body(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        model: str | None,
        max_tokens: int,
        temperature: float,
        reasoning_effort: str | None,
        tool_choice: str | dict[str, Any] | None,
    ) -> dict[str, Any]:
        """构建对象（_build_responses_body = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._build_responses_body` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

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
        model_name = model or self.default_model
        if self._spec and self._spec.strip_model_prefix:
            model_name = model_name.split("/")[-1]
        sanitized_messages = self._sanitize_messages(self._sanitize_empty_content(messages))
        instructions, input_items = convert_messages(sanitized_messages)

        body: dict[str, Any] = {
            "model": model_name,
            "instructions": instructions or None,
            "input": input_items,
            "max_output_tokens": max(1, max_tokens),
            "store": False,
            "stream": False,
        }

        if self._supports_temperature(model_name, reasoning_effort):
            body["temperature"] = temperature

        if reasoning_effort and reasoning_effort.lower() != "none":
            body["reasoning"] = {"effort": reasoning_effort}
            body["include"] = ["reasoning.encrypted_content"]

        if tools:
            body["tools"] = convert_tools(tools)
            body["tool_choice"] = tool_choice or "auto"

        extra_body = getattr(self, "_extra_body", {})
        if extra_body:
            body = _merge_responses_extra_body(body, extra_body)

        return body

    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----
    # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
    # ---- 中文分隔线：下面进入同一主题的下一组逻辑 ----

    @staticmethod
    def _maybe_mapping(value: Any) -> dict[str, Any] | None:
        """执行辅助逻辑（_maybe_mapping = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._maybe_mapping` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(value, dict):
            return value
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, dict):
                return dumped
        return None

    @classmethod
    def _extract_text_content(cls, value: Any) -> str | None:
        """提取信息（_extract_text_content = 原函数名）。

        【中文名称】提取信息

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._extract_text_content` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                item_map = cls._maybe_mapping(item)
                if item_map:
                    text = item_map.get("text")
                    if isinstance(text, str):
                        parts.append(text)
                        continue
                text = getattr(item, "text", None)
                if isinstance(text, str):
                    parts.append(text)
                    continue
                if isinstance(item, str):
                    parts.append(item)
            return "".join(parts) or None
        return str(value)

    @classmethod
    def _extract_usage(cls, response: Any) -> dict[str, int]:
        """提取信息（_extract_usage = 原函数名）。

        【中文名称】提取信息

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._extract_usage` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        response: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        usage_obj = None
        response_map = cls._maybe_mapping(response)
        if response_map is not None:
            usage_obj = response_map.get("usage")
        elif hasattr(response, "usage") and response.usage:
            usage_obj = response.usage

        usage_map = cls._maybe_mapping(usage_obj)
        if usage_map is not None:
            result = {
                "prompt_tokens": int(usage_map.get("prompt_tokens") or 0),
                "completion_tokens": int(usage_map.get("completion_tokens") or 0),
                "total_tokens": int(usage_map.get("total_tokens") or 0),
            }
        elif usage_obj:
            result = {
                "prompt_tokens": getattr(usage_obj, "prompt_tokens", 0) or 0,
                "completion_tokens": getattr(usage_obj, "completion_tokens", 0) or 0,
                "total_tokens": getattr(usage_obj, "total_tokens", 0) or 0,
            }
        else:
            return {}

        # 中文说明：这一段围绕Provider、令牌、缓存处理，注意输入、输出和异常路径。
        # 中文说明：这一段围绕路径处理，注意输入、输出和异常路径。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        for path in (
            ("prompt_tokens_details", "cached_tokens"),  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            ("cached_tokens",),                          # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            ("prompt_cache_hit_tokens",),                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        ):
            cached = cls._get_nested_int(usage_map, path)
            if not cached and usage_obj:
                cached = cls._get_nested_int(usage_obj, path)
            if cached:
                result["cached_tokens"] = cached
                break

        return result

    @staticmethod
    def _get_nested_int(obj: Any, path: tuple[str, ...]) -> int:
        """执行辅助逻辑（_get_nested_int = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._get_nested_int` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        obj: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        current = obj
        for segment in path:
            if current is None:
                return 0
            if isinstance(current, dict):
                current = current.get(segment)
            else:
                current = getattr(current, segment, None)
        return int(current or 0) if current is not None else 0

    def _parse(self, response: Any) -> LLMResponse:
        """解析数据（_parse = 原函数名）。

        【中文名称】解析数据

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._parse` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        response: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(response, str):
            return LLMResponse(content=response, finish_reason="stop")

        response_map = self._maybe_mapping(response)
        if response_map is not None:
            choices = response_map.get("choices") or []
            if not choices:
                content = self._extract_text_content(
                    response_map.get("content") or response_map.get("output_text")
                )
                reasoning_content = self._extract_text_content(
                    response_map.get("reasoning_content")
                )
                if content is not None:
                    return LLMResponse(
                        content=content,
                        reasoning_content=reasoning_content,
                        finish_reason=str(response_map.get("finish_reason") or "stop"),
                        usage=self._extract_usage(response_map),
                    )
                return LLMResponse(content="Error: API returned empty choices.", finish_reason="error")

            choice0 = self._maybe_mapping(choices[0]) or {}
            msg0 = self._maybe_mapping(choice0.get("message")) or {}
            content = self._extract_text_content(msg0.get("content"))
            finish_reason = str(choice0.get("finish_reason") or "stop")

            raw_tool_calls: list[Any] = []
            # 中文说明：兜底。
            if not content and msg0.get("reasoning") and self._spec and self._spec.reasoning_as_content:
                content = self._extract_text_content(msg0.get("reasoning"))
            reasoning_content = msg0.get("reasoning_content")
            if reasoning_content is None and msg0.get("reasoning"):
                reasoning_content = self._extract_text_content(msg0.get("reasoning"))
            for ch in choices:
                ch_map = self._maybe_mapping(ch) or {}
                m = self._maybe_mapping(ch_map.get("message")) or {}
                tool_calls = m.get("tool_calls")
                if isinstance(tool_calls, list) and tool_calls:
                    raw_tool_calls.extend(tool_calls)
                    if ch_map.get("finish_reason") in ("tool_calls", "stop"):
                        finish_reason = str(ch_map["finish_reason"])
                if not content:
                    content = self._extract_text_content(m.get("content"))
                if reasoning_content is None:
                    reasoning_content = m.get("reasoning_content")

            parsed_tool_calls = []
            for tc in raw_tool_calls:
                tc_map = self._maybe_mapping(tc) or {}
                fn = self._maybe_mapping(tc_map.get("function")) or {}
                args = parse_tool_arguments(fn.get("arguments", {}))
                ec, prov, fn_prov = _extract_tc_extras(tc)
                parsed_tool_calls.append(ToolCallRequest(
                    id=str(tc_map.get("id") or _short_tool_id()),
                    name=str(fn.get("name") or ""),
                    arguments=args,
                    extra_content=ec,
                    provider_specific_fields=prov,
                    function_provider_specific_fields=fn_prov,
                ))

            return LLMResponse(
                content=content,
                tool_calls=parsed_tool_calls,
                finish_reason=finish_reason,
                usage=self._extract_usage(response_map),
                reasoning_content=reasoning_content if isinstance(reasoning_content, str) else None,
            )

        if not response.choices:
            return LLMResponse(content="Error: API returned empty choices.", finish_reason="error")

        choice = response.choices[0]
        msg = choice.message
        content = msg.content
        finish_reason = choice.finish_reason

        raw_tool_calls: list[Any] = []
        for ch in response.choices:
            m = ch.message
            if hasattr(m, "tool_calls") and m.tool_calls:
                raw_tool_calls.extend(m.tool_calls)
                if ch.finish_reason in ("tool_calls", "stop"):
                    finish_reason = ch.finish_reason
            if not content and m.content:
                content = m.content
            if not content and getattr(m, "reasoning", None) and self._spec and self._spec.reasoning_as_content:
                content = m.reasoning

        tool_calls = []
        for tc in raw_tool_calls:
            args = parse_tool_arguments(tc.function.arguments)
            ec, prov, fn_prov = _extract_tc_extras(tc)
            tool_calls.append(ToolCallRequest(
                id=str(getattr(tc, "id", None) or _short_tool_id()),
                name=tc.function.name,
                arguments=args,
                extra_content=ec,
                provider_specific_fields=prov,
                function_provider_specific_fields=fn_prov,
            ))

        reasoning_content = getattr(msg, "reasoning_content", None)
        if reasoning_content is None and getattr(msg, "reasoning", None):
            reasoning_content = msg.reasoning

        return LLMResponse(
            content=content,
            tool_calls=tool_calls,
            finish_reason=finish_reason or "stop",
            usage=self._extract_usage(response),
            reasoning_content=reasoning_content,
        )

    @classmethod
    def _parse_chunks(cls, chunks: list[Any]) -> LLMResponse:
        """解析数据（_parse_chunks = 原函数名）。

        【中文名称】解析数据

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._parse_chunks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chunks: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tc_bufs: dict[int, dict[str, Any]] = {}
        finish_reason = "stop"
        usage: dict[str, int] = {}

        def _accum_tc(tc: Any, idx_hint: int) -> None:
            """执行辅助逻辑（_accum_tc = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider._accum_tc` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            tc: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            idx_hint: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            tc_index: int = _get(tc, "index") if _get(tc, "index") is not None else idx_hint
            buf = tc_bufs.setdefault(tc_index, {
                "id": "", "name": "", "arguments": "",
                "extra_content": None, "prov": None, "fn_prov": None,
            })
            tc_id = _get(tc, "id")
            if tc_id:
                buf["id"] = str(tc_id)
            fn = _get(tc, "function")
            if fn is not None:
                fn_name = _get(fn, "name")
                if fn_name:
                    buf["name"] = str(fn_name)
                fn_args = _get(fn, "arguments")
                if fn_args:
                    buf["arguments"] += str(fn_args)
            ec, prov, fn_prov = _extract_tc_extras(tc)
            if ec:
                buf["extra_content"] = ec
            if prov:
                buf["prov"] = prov
            if fn_prov:
                buf["fn_prov"] = fn_prov

        def _accum_legacy_function_call(function_call: Any) -> None:
            """调用服务（_accum_legacy_function_call = 原函数名）。

            【中文名称】调用服务

            【功能说明】
            这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
            在阅读 `OpenAICompatProvider._accum_legacy_function_call` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            function_call: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if not function_call:
                return
            buf = tc_bufs.setdefault(0, {
                "id": "", "name": "", "arguments": "",
                "extra_content": None, "prov": None, "fn_prov": None,
            })
            fn_name = _get(function_call, "name")
            if fn_name:
                buf["name"] = str(fn_name)
            fn_args = _get(function_call, "arguments")
            if fn_args:
                buf["arguments"] += str(fn_args)

        for chunk in chunks:
            if isinstance(chunk, str):
                content_parts.append(chunk)
                continue

            chunk_map = cls._maybe_mapping(chunk)
            if chunk_map is not None:
                choices = chunk_map.get("choices") or []
                if not choices:
                    usage = cls._extract_usage(chunk_map) or usage
                    text = cls._extract_text_content(
                        chunk_map.get("content") or chunk_map.get("output_text")
                    )
                    if text:
                        content_parts.append(text)
                    continue
                choice = cls._maybe_mapping(choices[0]) or {}
                if choice.get("finish_reason"):
                    finish_reason = str(choice["finish_reason"])
                delta = cls._maybe_mapping(choice.get("delta")) or {}
                text = cls._extract_text_content(delta.get("content"))
                if text:
                    content_parts.append(text)
                text = cls._extract_text_content(delta.get("reasoning_content"))
                if not text:
                    text = cls._extract_text_content(delta.get("reasoning"))
                if text:
                    reasoning_parts.append(text)
                for idx, tc in enumerate(delta.get("tool_calls") or []):
                    _accum_tc(tc, idx)
                _accum_legacy_function_call(delta.get("function_call"))
                usage = cls._extract_usage(chunk_map) or usage
                continue

            if not chunk.choices:
                usage = cls._extract_usage(chunk) or usage
                continue
            choice = chunk.choices[0]
            if choice.finish_reason:
                finish_reason = choice.finish_reason
            delta = choice.delta
            if delta and delta.content:
                content_parts.append(delta.content)
            if delta:
                reasoning = getattr(delta, "reasoning_content", None)
                if not reasoning:
                    reasoning = getattr(delta, "reasoning", None)
                if reasoning:
                    reasoning_parts.append(reasoning)
            for tc in (getattr(delta, "tool_calls", None) or []) if delta else []:
                _accum_tc(tc, getattr(tc, "index", 0))
            if delta:
                _accum_legacy_function_call(getattr(delta, "function_call", None))

        # 中文说明：这一段围绕Provider、工具、调用处理，注意输入、输出和异常路径。
        # 中文说明：工具调用。
        # 中文说明：这一段围绕消息、工具、流式输出、响应处理，注意输入、输出和异常路径。
        _seen_tc_ids: set[str] = set()
        for b in tc_bufs.values():
            if not b["id"] or b["id"] in _seen_tc_ids:
                b["id"] = _short_tool_id()
            _seen_tc_ids.add(b["id"])

        return LLMResponse(
            content="".join(content_parts) or None,
            tool_calls=[
                ToolCallRequest(
                    id=b["id"] or _short_tool_id(),
                    name=b["name"],
                    arguments=parse_tool_arguments(b["arguments"]),
                    extra_content=b.get("extra_content"),
                    provider_specific_fields=b.get("prov"),
                    function_provider_specific_fields=b.get("fn_prov"),
                )
                for b in tc_bufs.values()
            ],
            finish_reason=finish_reason,
            usage=usage,
            reasoning_content="".join(reasoning_parts) or None,
        )

    @classmethod
    def _extract_error_metadata(cls, e: Exception) -> dict[str, Any]:
        """提取信息（_extract_error_metadata = 原函数名）。

        【中文名称】提取信息

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._extract_error_metadata` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        e: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        response = getattr(e, "response", None)
        headers = getattr(response, "headers", None)
        payload = (
            getattr(e, "body", None)
            or getattr(e, "doc", None)
            or getattr(response, "text", None)
        )
        if payload is None and response is not None:
            response_json = getattr(response, "json", None)
            if callable(response_json):
                try:
                    payload = response_json()
                except Exception:
                    payload = None
        error_type, error_code = LLMProvider._extract_error_type_code(payload)

        status_code = getattr(e, "status_code", None)
        if status_code is None and response is not None:
            status_code = getattr(response, "status_code", None)

        should_retry: bool | None = None
        if headers is not None:
            raw = headers.get("x-should-retry")
            if isinstance(raw, str):
                lowered = raw.strip().lower()
                if lowered == "true":
                    should_retry = True
                elif lowered == "false":
                    should_retry = False

        error_kind: str | None = None
        error_name = e.__class__.__name__.lower()
        if "timeout" in error_name:
            error_kind = "timeout"
        elif "connection" in error_name:
            error_kind = "connection"

        return {
            "error_status_code": int(status_code) if status_code is not None else None,
            "error_kind": error_kind,
            "error_type": error_type,
            "error_code": error_code,
            "error_retry_after_s": cls._extract_retry_after_from_headers(headers),
            "error_should_retry": should_retry,
        }

    @staticmethod
    def _handle_error(
        e: Exception,
        *,
        spec: ProviderSpec | None = None,
        api_base: str | None = None,
    ) -> LLMResponse:
        """处理事件（_handle_error = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider._handle_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        e: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        spec: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        body = (
            getattr(e, "doc", None)
            or getattr(e, "body", None)
            or getattr(getattr(e, "response", None), "text", None)
        )
        body_text = body if isinstance(body, str) else str(body) if body is not None else ""
        msg = f"Error: {body_text.strip()[:500]}" if body_text.strip() else f"Error calling LLM: {e}"

        text = f"{body_text} {e}".lower()
        if spec and spec.is_local and ("502" in text or "connection" in text or "refused" in text):
            msg += (
                "\nHint: this is a local model endpoint. Check that the local server is reachable at "
                f"{api_base or spec.default_api_base}, and if you are using a proxy/tunnel, make sure it "
                "can reach your local Ollama/vLLM service instead of routing localhost through the remote host."
            )

        response = getattr(e, "response", None)
        retry_after = LLMProvider._extract_retry_after_from_headers(getattr(response, "headers", None))
        if retry_after is None:
            retry_after = LLMProvider._extract_retry_after(msg)
        return LLMResponse(
            content=msg,
            finish_reason="error",
            retry_after=retry_after,
            **OpenAICompatProvider._extract_error_metadata(e),
        )

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
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider.chat` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

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
        await self._ensure_client()
        try:
            if self._should_use_responses_api(model, reasoning_effort):
                try:
                    body = self._build_responses_body(
                        messages, tools, model, max_tokens, temperature,
                        reasoning_effort, tool_choice,
                    )
                    result = parse_response_output(await self._client.responses.create(**body))
                    self._record_responses_success(model, reasoning_effort)
                    return result
                except Exception as responses_error:
                    if self._spec and self._spec.name == "github_copilot":
                        # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
                        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                        # 中文说明：这一段围绕错误处理，注意输入、输出和异常路径。
                        raise
                    if self._api_type == "responses":
                        raise
                    if not self._should_fallback_from_responses_error(responses_error):
                        raise
                    self._record_responses_failure(model, reasoning_effort)

            kwargs = self._build_kwargs(
                messages, tools, model, max_tokens, temperature,
                reasoning_effort, tool_choice,
            )
            return self._parse(await self._client.chat.completions.create(**kwargs))
        except Exception as e:
            return self._handle_error(e, spec=self._spec, api_base=self.api_base)

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
        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAICompatProvider.chat_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

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
        await self._ensure_client()
        idle_timeout_s = int(os.environ.get("NANOBOT_STREAM_IDLE_TIMEOUT_S", "90"))
        try:
            if self._should_use_responses_api(model, reasoning_effort):
                try:
                    body = self._build_responses_body(
                        messages, tools, model, max_tokens, temperature,
                        reasoning_effort, tool_choice,
                    )
                    body["stream"] = True
                    stream = await self._client.responses.create(**body)

                    async def _timed_stream():
                        """异步流式处理（_timed_stream = 原函数名）。

                        【中文名称】流式处理

                        【功能说明】
                        这是 Provider 实现 中的一个关键步骤。OpenAI 兼容接口 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
                        在阅读 `OpenAICompatProvider._timed_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                        【参数说明】
                        无显式参数。

                        【返回值】
                        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                        """
                        stream_iter = stream.__aiter__()
                        while True:
                            try:
                                yield await asyncio.wait_for(
                                    stream_iter.__anext__(),
                                    timeout=idle_timeout_s,
                                )
                            except StopAsyncIteration:
                                break

                    (
                        content,
                        tool_calls,
                        finish_reason,
                        usage,
                        reasoning_content,
                    ) = await consume_sdk_stream(
                        _timed_stream(),
                        on_content_delta,
                        on_tool_call_delta=on_tool_call_delta,
                    )
                    self._record_responses_success(model, reasoning_effort)
                    return LLMResponse(
                        content=content or None,
                        tool_calls=tool_calls,
                        finish_reason=finish_reason,
                        usage=usage,
                        reasoning_content=reasoning_content,
                    )
                except Exception as responses_error:
                    if self._spec and self._spec.name == "github_copilot":
                        # 中文说明：这一段围绕响应处理，注意输入、输出和异常路径。
                        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                        # 中文说明：这一段围绕错误处理，注意输入、输出和异常路径。
                        raise
                    if self._api_type == "responses":
                        raise
                    if not self._should_fallback_from_responses_error(responses_error):
                        raise
                    self._record_responses_failure(model, reasoning_effort)

            kwargs = self._build_kwargs(
                messages, tools, model, max_tokens, temperature,
                reasoning_effort, tool_choice,
            )
            if self._spec and self._spec.name == "zhipu" and tools and on_tool_call_delta:
                # 中文说明：流式输出。
                # 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
                # 中文说明：这一段围绕工具、调用、路径处理，注意输入、输出和异常路径。
                # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
                kwargs.setdefault("extra_body", {})["tool_stream"] = True
            kwargs["stream"] = True
            kwargs["stream_options"] = {"include_usage": True}
            stream = await self._client.chat.completions.create(**kwargs)
            chunks: list[Any] = []
            stream_iter = stream.__aiter__()
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        stream_iter.__anext__(),
                        timeout=idle_timeout_s,
                    )
                except StopAsyncIteration:
                    break
                chunks.append(chunk)
                if chunk.choices:
                    delta_obj = chunk.choices[0].delta
                    if on_content_delta:
                        text = getattr(delta_obj, "content", None)
                        if text:
                            await on_content_delta(text)
                    if on_thinking_delta:
                        reasoning = getattr(delta_obj, "reasoning_content", None) or getattr(
                            delta_obj, "reasoning", None,
                        )
                        r_text = self._extract_text_content(reasoning)
                        if r_text:
                            await on_thinking_delta(r_text)
                    if on_tool_call_delta:
                        for idx, tool_delta in enumerate(
                            getattr(delta_obj, "tool_calls", None) or []
                        ):
                            fn = _get(tool_delta, "function")
                            tool_index = _get(tool_delta, "index")
                            await on_tool_call_delta({
                                "index": tool_index if tool_index is not None else idx,
                                "call_id": str(_get(tool_delta, "id") or ""),
                                "name": str(_get(fn, "name") or "") if fn is not None else "",
                                "arguments_delta": (
                                    str(_get(fn, "arguments") or "") if fn is not None else ""
                                ),
                            })
                        function_call = getattr(delta_obj, "function_call", None)
                        if function_call:
                            await on_tool_call_delta({
                                "index": 0,
                                "call_id": "",
                                "name": str(_get(function_call, "name") or ""),
                                "arguments_delta": str(_get(function_call, "arguments") or ""),
                            })
            return self._parse_chunks(chunks)
        except asyncio.TimeoutError:
            return LLMResponse(
                content=(
                    f"Error calling LLM: stream stalled for more than "
                    f"{idle_timeout_s} seconds"
                ),
                finish_reason="error",
                error_kind="timeout",
            )
        except Exception as e:
            return self._handle_error(e, spec=self._spec, api_base=self.api_base)

    def get_default_model(self) -> str:
        return self.default_model

