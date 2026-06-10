"""运行时模型预设辅助函数。

这个模块的职责是把“用户配置中的模型预设”转换成 AgentLoop 运行时可切换的快照。
它不直接发请求给模型，而是负责：

- 解析预设名
- 构造 ProviderSnapshot
- 支持运行时在不同模型/参数组合之间切换
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from nanobot.config.schema import ModelPresetConfig
from nanobot.providers.base import LLMProvider
from nanobot.providers.factory import ProviderSnapshot, build_provider_snapshot

PresetSnapshotLoader = Callable[[str], ProviderSnapshot]


def default_selection_signature(signature: tuple[object, ...] | None) -> tuple[object, ...] | None:
    """提取“默认模型选择”的签名片段，用于判断默认提供方是否变化。"""
    return signature[:2] if signature else None


def configured_model_presets(config: Any) -> dict[str, ModelPresetConfig]:
    """返回“显式预设 + 隐式 default 预设”的完整预设表。"""
    return {**config.model_presets, "default": config.resolve_default_preset()}


def make_preset_snapshot_loader(
    config: Any,
    provider_snapshot_loader: Callable[..., ProviderSnapshot] | None,
) -> PresetSnapshotLoader:
    """构造一个“按预设名加载 ProviderSnapshot”的函数。"""
    if provider_snapshot_loader is not None:
        return lambda name: provider_snapshot_loader(preset_name=name)
    return lambda name: build_provider_snapshot(config, preset_name=name)


def build_static_preset_snapshot(
    provider: LLMProvider,
    name: str,
    preset: ModelPresetConfig,
) -> ProviderSnapshot:
    """基于现有 provider 和静态 preset 直接构造快照。

    这种模式适合“不需要重新实例化 provider，只调整 generation 参数”的场景。
    """
    provider.generation = preset.to_generation_settings()
    return ProviderSnapshot(
        provider=provider,
        model=preset.model,
        context_window_tokens=preset.context_window_tokens,
        signature=("model_preset", name, preset.model_dump_json()),
    )


def build_runtime_preset_snapshot(
    *,
    name: str,
    presets: dict[str, ModelPresetConfig],
    provider: LLMProvider,
    loader: PresetSnapshotLoader | None,
) -> ProviderSnapshot:
    """按名称构造运行时预设快照，优先使用外部 loader。"""
    if loader is not None:
        return loader(name)
    return build_static_preset_snapshot(provider, name, presets[name])


def normalize_preset_name(name: str | None, presets: dict[str, ModelPresetConfig]) -> str:
    """校验并标准化预设名。"""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("model_preset must be a non-empty string")
    name = name.strip()
    if name not in presets:
        raise KeyError(f"model_preset {name!r} not found. Available: {', '.join(presets) or '(none)'}")
    return name

