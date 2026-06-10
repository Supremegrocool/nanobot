"""配置加载工具。

这个模块负责把磁盘上的 ``config.json`` 变成程序内可用的 ``Config`` 对象，
并在需要时处理三件关键事情：

1. 旧配置迁移：兼容历史字段结构
2. 环境变量替换：把 ``${VAR}`` 解析成真实环境变量值
3. 安全联动：把 SSRF 白名单配置同步到网络安全模块
"""

import json
import os
import re
from pathlib import Path
from typing import Any

import pydantic
from loguru import logger
from pydantic import BaseModel

from nanobot.config.schema import Config, _resolve_tool_config_refs

# 当前生效配置文件路径的全局缓存。
# 这样可以支持“同一进程切换到另一个配置文件实例”的场景。
_current_config_path: Path | None = None
_schema_refs_ready = False


def set_config_path(path: Path) -> None:
    """设置当前配置文件路径。

    之所以要单独缓存它，是因为很多运行时目录（日志、媒体、webui 数据等）
    都是相对于配置文件所在目录派生出来的。
    """
    global _current_config_path
    _current_config_path = path


def get_config_path() -> Path:
    """获取当前配置文件路径。"""
    if _current_config_path:
        return _current_config_path
    return Path.home() / ".nanobot" / "config.json"


def load_config(config_path: Path | None = None) -> Config:
    """加载配置文件，失败时回退到默认配置。

    【执行流程】
    1. 先确保 ``ToolsConfig`` 的延迟类型引用已经解析完成
    2. 决定要读取哪个配置文件路径
    3. 如果文件存在，就读取 JSON -> 执行历史配置迁移 -> 用 Pydantic 校验
    4. 如果读取失败，则记录日志并回退到默认 ``Config()``
    5. 最后把配置里的 SSRF 白名单同步到安全模块

    【为什么不用“读取失败直接崩溃”】
    因为 nanobot 希望在配置损坏或缺失时仍然能启动一个最小可运行实例，
    方便用户修复配置。
    """
    global _schema_refs_ready
    if not _schema_refs_ready:
        _resolve_tool_config_refs()
        _schema_refs_ready = True

    path = config_path or get_config_path()

    config = Config()
    if path.exists():
        try:
            # 读取磁盘 JSON 后，先做“格式迁移”，再做“结构校验”。
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            data = _migrate_config(data)
            config = Config.model_validate(data)
        except (json.JSONDecodeError, ValueError, pydantic.ValidationError) as e:
            logger.warning("Failed to load config from {}: {}", path, e)
            logger.warning("Using default configuration.")

    _apply_ssrf_whitelist(config)
    return config


def _apply_ssrf_whitelist(config: Config) -> None:
    """把配置中的 SSRF 白名单应用到网络安全模块。"""
    from nanobot.security.network import configure_ssrf_whitelist

    configure_ssrf_whitelist(config.tools.ssrf_whitelist)


def save_config(config: Config, config_path: Path | None = None) -> None:
    """把 ``Config`` 对象写回磁盘。

    这里使用 ``model_dump(by_alias=True)``，是为了把内部的 snake_case 字段
    重新序列化成用户配置中更常见的 camelCase 形式。
    """
    path = config_path or get_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    data = config.model_dump(mode="json", by_alias=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


_ENV_REF_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def resolve_config_env_vars(config: Config) -> Config:
    """解析配置对象中的 ``${VAR}`` 环境变量引用。

    注意这不是 shell 风格的“带默认值表达式”，这里只支持最简单的
    ``${ENV_NAME}`` 形式；如果环境变量不存在，会直接抛 ``ValueError``。
    """
    return _resolve_in_place(config)


def _resolve_in_place(obj: Any) -> Any:
    """递归解析任意配置对象中的环境变量引用。

    这里需要同时支持：
    - 普通字符串
    - Pydantic 模型
    - dict
    - list
    """
    if isinstance(obj, str):
        new = _ENV_REF_PATTERN.sub(_env_replace, obj)
        return new if new != obj else obj
    if isinstance(obj, BaseModel):
        updates: dict[str, Any] = {}
        for name in type(obj).model_fields:
            old = getattr(obj, name)
            new = _resolve_in_place(old)
            if new is not old:
                updates[name] = new
        extras = obj.__pydantic_extra__
        new_extras: dict[str, Any] | None = None
        if extras:
            resolved = {k: _resolve_in_place(v) for k, v in extras.items()}
            if any(resolved[k] is not extras[k] for k in extras):
                new_extras = resolved
        if not updates and new_extras is None:
            return obj
        copy = obj.model_copy(update=updates) if updates else obj.model_copy()
        if new_extras is not None:
            copy.__pydantic_extra__ = new_extras
        return copy
    if isinstance(obj, dict):
        resolved = {k: _resolve_in_place(v) for k, v in obj.items()}
        return resolved if any(resolved[k] is not obj[k] for k in obj) else obj
    if isinstance(obj, list):
        resolved = [_resolve_in_place(v) for v in obj]
        return resolved if any(nv is not ov for nv, ov in zip(resolved, obj)) else obj
    return obj


def _resolve_env_vars(obj: object) -> object:
    """递归解析普通字符串 / dict / list 中的 ``${VAR}`` 引用。"""
    if isinstance(obj, str):
        return _ENV_REF_PATTERN.sub(_env_replace, obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(v) for v in obj]
    return obj


def _env_replace(match: re.Match[str]) -> str:
    name = match.group(1)
    value = os.environ.get(name)
    if value is None:
        raise ValueError(
            f"Environment variable '{name}' referenced in config is not set"
        )
    return value


def _migrate_config(data: dict) -> dict:
    """把旧版配置结构迁移到当前格式。

    这一步发生在 Pydantic 校验之前，目的是兼容老用户留下的历史配置文件。
    """
    # 旧字段：tools.exec.restrictToWorkspace
    # 新字段：tools.restrictToWorkspace
    tools = data.get("tools", {})
    exec_cfg = tools.get("exec", {})
    if "restrictToWorkspace" in exec_cfg and "restrictToWorkspace" not in tools:
        tools["restrictToWorkspace"] = exec_cfg.pop("restrictToWorkspace")

    # 旧字段：tools.myEnabled / tools.mySet
    # 新字段：tools.my.enable / tools.my.allowSet
    # 这样可以把 my 工具也收敛到与 web / exec 一致的子配置结构里。
    if "myEnabled" in tools or "mySet" in tools:
        my_cfg = tools.setdefault("my", {})
        if "myEnabled" in tools and "enable" not in my_cfg:
            my_cfg["enable"] = tools.pop("myEnabled")
        else:
            tools.pop("myEnabled", None)
        if "mySet" in tools and "allowSet" not in my_cfg:
            my_cfg["allowSet"] = tools.pop("mySet")
        else:
            tools.pop("mySet", None)

    return data
