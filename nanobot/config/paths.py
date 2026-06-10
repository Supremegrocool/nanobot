"""运行时路径辅助函数。

这个模块的职责是统一回答一个问题：
“nanobot 在当前实例下，配置文件、数据目录、日志目录、媒体目录、工作区目录分别在哪？”

把这些路径统一收口之后，其他模块就不需要自己拼路径，也更容易支持多实例运行。
"""

from __future__ import annotations

from pathlib import Path

from nanobot.utils.helpers import ensure_dir


def get_config_path() -> Path:
    """获取配置文件路径。

    这里使用“延迟导入”的方式去调用 ``nanobot.config.loader.get_config_path``，
    目的是打破启动阶段可能出现的循环导入问题。
    """
    from nanobot.config.loader import get_config_path as _loader_get_config_path
    return _loader_get_config_path()


def get_data_dir() -> Path:
    """返回当前实例级别的数据目录。"""
    return ensure_dir(get_config_path().parent)


def get_runtime_subdir(name: str) -> Path:
    """返回数据目录下某个具名子目录，并确保目录存在。"""
    return ensure_dir(get_data_dir() / name)


def get_media_dir(channel: str | None = None) -> Path:
    """返回媒体目录；如果给定 channel，则按渠道进一步分目录。"""
    base = get_runtime_subdir("media")
    return ensure_dir(base / channel) if channel else base


def get_cron_dir() -> Path:
    """返回 cron 持久化目录。"""
    return get_runtime_subdir("cron")


def get_logs_dir() -> Path:
    """返回日志目录。"""
    return get_runtime_subdir("logs")


def get_webui_dir() -> Path:
    """返回 WebUI 专用的持久化展示线程目录。"""
    return get_runtime_subdir("webui")


def get_workspace_path(workspace: str | None = None) -> Path:
    """解析并确保 Agent 工作区目录存在。"""
    path = Path(workspace).expanduser() if workspace else Path.home() / ".nanobot" / "workspace"
    return ensure_dir(path)


def is_default_workspace(workspace: str | Path | None) -> bool:
    """判断某个工作区路径是否等于 nanobot 默认工作区。"""
    current = Path(workspace).expanduser() if workspace is not None else Path.home() / ".nanobot" / "workspace"
    default = Path.home() / ".nanobot" / "workspace"
    return current.resolve(strict=False) == default.resolve(strict=False)


def get_cli_history_path() -> Path:
    """返回 CLI 共用历史记录文件路径。"""
    return Path.home() / ".nanobot" / "history" / "cli_history"


def get_bridge_install_dir() -> Path:
    """返回 WhatsApp bridge 的共享安装目录。"""
    return Path.home() / ".nanobot" / "bridge"


def get_legacy_sessions_dir() -> Path:
    """返回旧版全局 session 目录，用于向新目录结构迁移时兜底。"""
    return Path.home() / ".nanobot" / "sessions"
