"""媒体解码工具。

【中文名称】媒体解码工具

【功能说明】
负责从 data URL、base64 或普通 URL 中解析媒体类型和二进制内容。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import base64
import mimetypes
import re
import uuid
from pathlib import Path

from nanobot.utils.helpers import safe_filename

DEFAULT_MAX_BYTES = 10 * 1024 * 1024
MAX_FILE_SIZE = DEFAULT_MAX_BYTES

_DATA_URL_RE = re.compile(r"^data:([^;,]+)(?:;[^,]*)*;base64,(.+)$", re.DOTALL)
_MIME_EXTENSION_OVERRIDES = {
    # 说明：这里处理 媒体解码工具 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 媒体解码工具 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 媒体解码工具 的协议细节或边界情况，避免外部差异影响核心流程。
    "application/ogg": ".ogg",
    "audio/ogg": ".ogg",
    "audio/mpga": ".mpga",
    "audio/wav": ".wav",
    "audio/webm": ".webm",
    "audio/x-m4a": ".m4a",
    "audio/x-wav": ".wav",
    "audio/vnd.wave": ".wav",
    "video/webm": ".webm",
}


class FileSizeExceededError(Exception):
    """FileSizeExceededError 类。

    【中文名称】FileSizeExceededError

    【功能说明】
    这是 媒体解码工具 中的核心数据结构或服务类。负责从 data URL、base64 或普通 URL 中解析媒体类型和二进制内容。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""


FileSizeExceeded = FileSizeExceededError


def save_base64_data_url(
    data_url: str,
    media_dir: Path,
    *,
    max_bytes: int | None = None,
) -> str | None:
    """执行 `save_base64_data_url`。

    【中文名称】save_base64_data_url

    【功能说明】
    这是 媒体解码工具 中的一个步骤函数，用来支撑：负责从 data URL、base64 或普通 URL 中解析媒体类型和二进制内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - data_url: 调用方传入的 `data_url` 数据；具体类型以函数签名为准。
    - media_dir: 调用方传入的 `media_dir` 数据；具体类型以函数签名为准。
    - max_bytes: 调用方传入的 `max_bytes` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    m = _DATA_URL_RE.match(data_url)
    if not m:
        return None
    mime_type, b64_payload = m.group(1).strip().lower(), m.group(2)
    try:
        raw = base64.b64decode(b64_payload)
    except Exception:
        return None
    limit = DEFAULT_MAX_BYTES if max_bytes is None else max_bytes
    if len(raw) > limit:
        raise FileSizeExceeded(f"File exceeds {limit // (1024 * 1024)}MB limit")
    ext = _MIME_EXTENSION_OVERRIDES.get(mime_type) or mimetypes.guess_extension(mime_type) or ".bin"
    filename = f"{uuid.uuid4().hex[:12]}{ext}"
    dest = media_dir / safe_filename(filename)
    dest.write_bytes(raw)
    return str(dest)
