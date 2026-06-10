"""Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/factory.py

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

from dataclasses import dataclass
from pathlib import Path

from nanobot.config.schema import Config, InlineFallbackConfig, ModelPresetConfig
from nanobot.providers.base import LLMProvider
from nanobot.providers.fallback_provider import FallbackProvider
from nanobot.providers.registry import find_by_name


@dataclass(frozen=True)
class ProviderSnapshot:
    """ProviderSnapshot 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】ProviderSnapshot

    【功能说明】
    Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """
    provider: LLMProvider
    model: str
    context_window_tokens: int
    signature: tuple[object, ...]


def _resolve_model_preset(
    config: Config,
    *,
    preset_name: str | None = None,
    preset: ModelPresetConfig | None = None,
) -> ModelPresetConfig:
    """解析目标（_resolve_model_preset = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_model_preset` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return preset if preset is not None else config.resolve_preset(preset_name)


def _make_provider_core(
    config: Config,
    *,
    preset_name: str | None = None,
    preset: ModelPresetConfig | None = None,
    model: str | None = None,
) -> LLMProvider:
    """执行辅助逻辑（_make_provider_core = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_make_provider_core` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    resolved = _resolve_model_preset(config, preset_name=preset_name, preset=preset)
    model = model or resolved.model
    provider_name = config.get_provider_name(model, preset=resolved)
    p = config.get_provider(model, preset=resolved)
    spec = find_by_name(provider_name) if provider_name else None
    if spec and spec.is_transcription_only:
        raise ValueError(f"Provider '{provider_name}' only supports transcription.")
    backend = spec.backend if spec else "openai_compat"

    # 中文说明：这一段围绕Provider处理，注意输入、输出和异常路径。
    if backend == "azure_openai":
        if not p or not p.api_base:
            raise ValueError("Azure OpenAI requires api_base in config.")
    elif backend == "openai_compat" and not model.startswith("bedrock/"):
        needs_key = not (p and p.api_key)
        exempt = spec and (spec.is_oauth or spec.is_local or spec.is_direct)
        if needs_key and not exempt:
            raise ValueError(f"No API key configured for provider '{provider_name}'.")

    if backend == "openai_codex":
        from nanobot.providers.openai_codex_provider import OpenAICodexProvider

        provider = OpenAICodexProvider(default_model=model)
    elif backend == "azure_openai":
        from nanobot.providers.azure_openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider(
            api_key=p.api_key or "",
            api_base=p.api_base,
            default_model=model,
        )
    elif backend == "github_copilot":
        from nanobot.providers.github_copilot_provider import GitHubCopilotProvider

        provider = GitHubCopilotProvider(default_model=model)
    elif backend == "anthropic":
        from nanobot.providers.anthropic_provider import AnthropicProvider

        provider = AnthropicProvider(
            api_key=p.api_key if p else None,
            api_base=config.get_api_base(model, preset=resolved),
            default_model=model,
            extra_headers=p.extra_headers if p else None,
        )
    elif backend == "bedrock":
        from nanobot.providers.bedrock_provider import BedrockProvider

        provider = BedrockProvider(
            api_key=p.api_key if p else None,
            api_base=p.api_base if p else None,
            default_model=model,
            region=getattr(p, "region", None) if p else None,
            profile=getattr(p, "profile", None) if p else None,
            extra_body=p.extra_body if p else None,
        )
    else:
        from nanobot.providers.openai_compat_provider import OpenAICompatProvider

        provider = OpenAICompatProvider(
            api_key=p.api_key if p else None,
            api_base=config.get_api_base(model, preset=resolved),
            default_model=model,
            extra_headers=p.extra_headers if p else None,
            spec=spec,
            extra_body=p.extra_body if p else None,
            api_type=p.api_type if p and provider_name == "openai" else "auto",
            extra_query=p.extra_query if p else None,
        )

    provider.generation = resolved.to_generation_settings()
    return provider


def _inline_fallback_preset(
    primary: ModelPresetConfig,
    fallback: InlineFallbackConfig,
) -> ModelPresetConfig:
    """执行辅助逻辑（_inline_fallback_preset = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_inline_fallback_preset` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    primary: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    fallback: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return ModelPresetConfig(
        model=fallback.model,
        provider=fallback.provider,
        max_tokens=fallback.max_tokens if fallback.max_tokens is not None else primary.max_tokens,
        context_window_tokens=(
            fallback.context_window_tokens
            if fallback.context_window_tokens is not None
            else primary.context_window_tokens
        ),
        temperature=(
            fallback.temperature if fallback.temperature is not None else primary.temperature
        ),
        reasoning_effort=fallback.reasoning_effort,
    )


def _resolve_fallback_presets(config: Config, primary: ModelPresetConfig) -> list[ModelPresetConfig]:
    """解析目标（_resolve_fallback_presets = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_fallback_presets` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    primary: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    presets: list[ModelPresetConfig] = []
    for fallback in config.agents.defaults.fallback_models:
        if isinstance(fallback, str):
            presets.append(config.model_presets[fallback])
        else:
            presets.append(_inline_fallback_preset(primary, fallback))
    return presets


def make_provider(
    config: Config,
    *,
    preset_name: str | None = None,
    preset: ModelPresetConfig | None = None,
    model: str | None = None,
) -> LLMProvider:
    """执行辅助逻辑（make_provider = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `make_provider` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    resolved = _resolve_model_preset(config, preset_name=preset_name, preset=preset)
    provider = _make_provider_core(config, preset_name=preset_name, preset=preset, model=model)
    fallback_presets = _resolve_fallback_presets(config, resolved)

    if fallback_presets:
        provider = FallbackProvider(
            primary=provider,
            fallback_presets=fallback_presets,
            provider_factory=lambda fb: _make_provider_core(
                config, preset_name=preset_name, preset=fb
            ),
        )

    return provider


def provider_signature(
    config: Config,
    *,
    preset_name: str | None = None,
    preset: ModelPresetConfig | None = None,
) -> tuple[object, ...]:
    """执行辅助逻辑（provider_signature = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `provider_signature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    resolved = _resolve_model_preset(config, preset_name=preset_name, preset=preset)
    p = config.get_provider(resolved.model, preset=resolved)
    fallback_presets = _resolve_fallback_presets(config, resolved)

    def _fallback_signature(fallback: ModelPresetConfig) -> tuple[object, ...]:
        """执行辅助逻辑（_fallback_signature = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `_fallback_signature` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        fallback: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        fp = config.get_provider(fallback.model, preset=fallback)
        return (
            fallback.model,
            fallback.provider,
            config.get_provider_name(fallback.model, preset=fallback),
            config.get_api_key(fallback.model, preset=fallback),
            config.get_api_base(fallback.model, preset=fallback),
            fp.extra_headers if fp else None,
            fp.extra_body if fp else None,
            fp.api_type if fp else "auto",
            fp.extra_query if fp else None,
            getattr(fp, "region", None) if fp else None,
            getattr(fp, "profile", None) if fp else None,
            fallback.max_tokens,
            fallback.temperature,
            fallback.reasoning_effort,
            fallback.context_window_tokens,
        )

    return (
        resolved.model,
        resolved.provider,
        config.get_provider_name(resolved.model, preset=resolved),
        config.get_api_key(resolved.model, preset=resolved),
        config.get_api_base(resolved.model, preset=resolved),
        p.extra_headers if p else None,
        p.extra_body if p else None,
        p.api_type if p else "auto",
        p.extra_query if p else None,
        getattr(p, "region", None) if p else None,
        getattr(p, "profile", None) if p else None,
        resolved.max_tokens,
        resolved.temperature,
        resolved.reasoning_effort,
        resolved.context_window_tokens,
        tuple(_fallback_signature(fallback) for fallback in fallback_presets),
    )


def build_provider_snapshot(
    config: Config,
    *,
    preset_name: str | None = None,
    preset: ModelPresetConfig | None = None,
) -> ProviderSnapshot:
    """构建对象（build_provider_snapshot = 原函数名）。

    【中文名称】构建对象

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `build_provider_snapshot` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    resolved = _resolve_model_preset(config, preset_name=preset_name, preset=preset)
    fallback_windows = [
        fallback.context_window_tokens
        for fallback in _resolve_fallback_presets(config, resolved)
    ]
    return ProviderSnapshot(
        provider=make_provider(config, preset=resolved),
        model=resolved.model,
        context_window_tokens=min([resolved.context_window_tokens, *fallback_windows]),
        signature=provider_signature(config, preset=resolved),
    )


def load_provider_snapshot(
    config_path: Path | None = None,
    *,
    preset_name: str | None = None,
) -> ProviderSnapshot:
    """加载数据（load_provider_snapshot = 原函数名）。

    【中文名称】加载数据

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。Provider 工厂 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `load_provider_snapshot` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    config_path: 配置对象或配置片段，决定该逻辑如何连接外部服务。
    preset_name: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    from nanobot.config.loader import load_config, resolve_config_env_vars

    return build_provider_snapshot(
        resolve_config_env_vars(load_config(config_path)),
        preset_name=preset_name,
    )

