"""渠道自动发现机制。

这个模块负责回答：
“当前有哪些可用渠道？哪些是内置的？哪些是插件提供的？”

发现过程分两层：
- 内置渠道：扫描 ``nanobot.channels`` 包
- 外部插件：扫描 entry_points 里的 ``nanobot.channels`` 分组
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from nanobot.channels.base import BaseChannel

_INTERNAL = frozenset({"base", "manager", "registry"})


def discover_channel_names() -> list[str]:
    """通过扫描包目录返回所有内置渠道模块名，不触发真正导入。"""
    import nanobot.channels as pkg

    return [
        name
        for _, name, ispkg in pkgutil.iter_modules(pkg.__path__)
        if name not in _INTERNAL and not ispkg
    ]


def load_channel_class(module_name: str) -> type[BaseChannel]:
    """导入某个渠道模块，并返回其中第一个 ``BaseChannel`` 子类。"""
    from nanobot.channels.base import BaseChannel as _Base

    mod = importlib.import_module(f"nanobot.channels.{module_name}")
    for attr in dir(mod):
        obj = getattr(mod, attr)
        if isinstance(obj, type) and issubclass(obj, _Base) and obj is not _Base:
            return obj
    raise ImportError(f"No BaseChannel subclass in nanobot.channels.{module_name}")


def discover_plugins(enabled_names: set[str] | None = None) -> dict[str, type[BaseChannel]]:
    """发现通过 entry_points 注册的外部渠道插件。"""
    from importlib.metadata import entry_points

    plugins: dict[str, type[BaseChannel]] = {}
    for ep in entry_points(group="nanobot.channels"):
        if enabled_names is not None and ep.name not in enabled_names:
            continue
        try:
            cls = ep.load()
            plugins[ep.name] = cls
        except Exception as e:
            logger.warning("Failed to load channel plugin '{}': {}", ep.name, e)
    return plugins


def discover_enabled(
    enabled_names: set[str],
    *,
    _names: list[str] | None = None,
    _include_all_external: bool = False,
) -> dict[str, type[BaseChannel]]:
    """返回启用名单中的渠道类。

    这里先做廉价的模块名扫描，再只导入真正启用的渠道，
    这样可以避免把一堆未启用渠道的第三方 SDK 都提前加载进来。
    """
    names = _names if _names is not None else discover_channel_names()
    result: dict[str, type[BaseChannel]] = {}
    for modname in names:
        if modname not in enabled_names:
            continue
        try:
            result[modname] = load_channel_class(modname)
        except ImportError as e:
            logger.debug("Skipping built-in channel '{}': {}", modname, e)

    external = discover_plugins(None if _include_all_external else enabled_names)
    shadowed = set(external) & set(result)
    if shadowed:
        logger.warning("Plugin(s) shadowed by built-in channels (ignored): {}", shadowed)
    if _include_all_external:
        result.update({k: v for k, v in external.items() if k not in shadowed})
    else:
        result.update({k: v for k, v in external.items() if k not in shadowed and k in enabled_names})

    return result


def discover_all() -> dict[str, type[BaseChannel]]:
    """返回所有渠道：内置渠道 + 外部插件渠道。

    约定内置渠道优先级更高，外部插件不能覆盖同名内置渠道。
    """
    names = discover_channel_names()
    return discover_enabled(set(names), _names=names, _include_all_external=True)
