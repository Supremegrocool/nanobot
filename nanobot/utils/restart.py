"""重启辅助工具。

【中文名称】重启辅助工具

【功能说明】
负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import json
import os
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

RESTART_NOTIFY_CHANNEL_ENV = "NANOBOT_RESTART_NOTIFY_CHANNEL"
RESTART_NOTIFY_CHAT_ID_ENV = "NANOBOT_RESTART_NOTIFY_CHAT_ID"
RESTART_NOTIFY_METADATA_ENV = "NANOBOT_RESTART_NOTIFY_METADATA"
RESTART_STARTED_AT_ENV = "NANOBOT_RESTART_STARTED_AT"


@dataclass(frozen=True)
class RestartNotice:
    """RestartNotice 类。

    【中文名称】RestartNotice

    【功能说明】
    这是 重启辅助工具 中的核心数据结构或服务类。负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    channel: str
    chat_id: str
    started_at_raw: str
    metadata: dict[str, Any] = field(default_factory=dict)


def format_restart_completed_message(started_at_raw: str) -> str:
    """执行 `format_restart_completed_message`。

    【中文名称】format_restart_completed_message

    【功能说明】
    这是 重启辅助工具 中的一个步骤函数，用来支撑：负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - started_at_raw: 调用方传入的 `started_at_raw` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    elapsed_suffix = ""
    if started_at_raw:
        with suppress(ValueError):
            elapsed_s = max(0.0, time.time() - float(started_at_raw))
            elapsed_suffix = f" in {elapsed_s:.1f}s"
    return f"Restart completed{elapsed_suffix}."


def set_restart_notice_to_env(
    *, channel: str, chat_id: str, metadata: dict[str, Any] | None = None,
) -> None:
    """执行 `set_restart_notice_to_env`。

    【中文名称】set_restart_notice_to_env

    【功能说明】
    这是 重启辅助工具 中的一个步骤函数，用来支撑：负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - channel: 调用方传入的 `channel` 数据；具体类型以函数签名为准。
    - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
    - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    os.environ[RESTART_NOTIFY_CHANNEL_ENV] = channel
    os.environ[RESTART_NOTIFY_CHAT_ID_ENV] = chat_id
    os.environ[RESTART_STARTED_AT_ENV] = str(time.time())
    if metadata:
        try:
            os.environ[RESTART_NOTIFY_METADATA_ENV] = json.dumps(metadata, default=str)
        except (TypeError, ValueError):
            os.environ.pop(RESTART_NOTIFY_METADATA_ENV, None)
    else:
        os.environ.pop(RESTART_NOTIFY_METADATA_ENV, None)


def consume_restart_notice_from_env() -> RestartNotice | None:
    """执行 `consume_restart_notice_from_env`。

    【中文名称】consume_restart_notice_from_env

    【功能说明】
    这是 重启辅助工具 中的一个步骤函数，用来支撑：负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    channel = os.environ.pop(RESTART_NOTIFY_CHANNEL_ENV, "").strip()
    chat_id = os.environ.pop(RESTART_NOTIFY_CHAT_ID_ENV, "").strip()
    started_at_raw = os.environ.pop(RESTART_STARTED_AT_ENV, "").strip()
    metadata_raw = os.environ.pop(RESTART_NOTIFY_METADATA_ENV, "").strip()
    if not (channel and chat_id):
        return None
    metadata: dict[str, Any] = {}
    if metadata_raw:
        try:
            parsed = json.loads(metadata_raw)
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, dict):
            metadata = parsed
    return RestartNotice(
        channel=channel,
        chat_id=chat_id,
        started_at_raw=started_at_raw,
        metadata=metadata,
    )


def should_show_cli_restart_notice(notice: RestartNotice, session_id: str) -> bool:
    """执行 `should_show_cli_restart_notice`。

    【中文名称】should_show_cli_restart_notice

    【功能说明】
    这是 重启辅助工具 中的一个步骤函数，用来支撑：负责在 CLI/WebUI 场景下生成安全的重启命令和进程替换流程。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - notice: 调用方传入的 `notice` 数据；具体类型以函数签名为准。
    - session_id: 调用方传入的 `session_id` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if notice.channel != "cli":
        return False
    if ":" in session_id:
        _, cli_chat_id = session_id.split(":", 1)
    else:
        cli_chat_id = session_id
    return not notice.chat_id or notice.chat_id == cli_chat_id
