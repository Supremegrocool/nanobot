"""解析 data URL 和媒体字节，判断媒体类型与扩展名。

【中文名称】工具模块：nanobot/utils/media_decode.py

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
    # 中文说明：这一段围绕音频处理，注意输入、输出和异常路径。
    # 中文说明：这一段围绕API、音频处理，注意输入、输出和异常路径。
    # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
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
    """FileSizeExceededError 类，封装 工具模块 的核心状态和行为。

    【中文名称】FileSizeExceededError

    【功能说明】
    解析 data URL 和媒体字节，判断媒体类型与扩展名。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Exception。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """


FileSizeExceeded = FileSizeExceededError


def save_base64_data_url(
    data_url: str,
    media_dir: Path,
    *,
    max_bytes: int | None = None,
) -> str | None:
    """保存数据（save_base64_data_url = 原函数名）。

    【中文名称】保存数据

    【功能说明】
    这是 工具模块 中的一个关键步骤。解析 data URL 和媒体字节，判断媒体类型与扩展名。
    在阅读 `save_base64_data_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data_url: 结构化数据负载，后续会被解析或转发。
    media_dir: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    max_bytes: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
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

