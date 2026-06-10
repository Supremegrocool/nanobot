"""管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。

【中文名称】工具模块：nanobot/utils/artifacts.py

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
import binascii
import json
import re
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from nanobot.config.paths import get_media_dir
from nanobot.utils.helpers import detect_image_mime, ensure_dir

_DATA_IMAGE_RE = re.compile(r"^data:(image/[A-Za-z0-9.+-]+);base64,(.*)$", re.DOTALL)
_MIME_EXTENSIONS = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/webp": ".webp",
    "image/gif": ".gif",
}

class ArtifactError(ValueError):
    """ArtifactError 类，封装 工具模块 的核心状态和行为。

    【中文名称】ArtifactError

    【功能说明】
    管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    ValueError。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """


def decode_image_data_url(data_url: str) -> tuple[bytes, str]:
    """执行辅助逻辑（decode_image_data_url = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。
    在阅读 `decode_image_data_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data_url: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    match = _DATA_IMAGE_RE.match(data_url.strip())
    if match is None:
        raise ArtifactError("expected a base64 image data URL")

    declared_mime, encoded = match.groups()
    try:
        raw = base64.b64decode(encoded, validate=True)
    except binascii.Error as exc:
        raise ArtifactError("invalid base64 image payload") from exc

    detected_mime = detect_image_mime(raw)
    if detected_mime is None:
        raise ArtifactError("unsupported or unrecognized image data")
    if declared_mime != detected_mime:
        declared_mime = detected_mime
    return raw, declared_mime


def _safe_relative_dir(save_dir: str) -> Path:
    """执行辅助逻辑（_safe_relative_dir = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。
    在阅读 `_safe_relative_dir` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    save_dir: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    normalized = save_dir.replace("\\", "/").strip("/")
    if not normalized:
        raise ArtifactError("save_dir must not be empty")
    rel = PurePosixPath(normalized)
    if rel.is_absolute() or any(part in {"", ".", ".."} for part in rel.parts):
        raise ArtifactError("save_dir must be a safe relative path")
    return Path(*rel.parts)


def _artifact_root(save_dir: str) -> Path:
    """执行辅助逻辑（_artifact_root = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。
    在阅读 `_artifact_root` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    save_dir: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    media_root = get_media_dir().resolve()
    root = (media_root / _safe_relative_dir(save_dir)).resolve()
    try:
        root.relative_to(media_root)
    except ValueError as exc:
        raise ArtifactError("artifact directory escapes media root") from exc
    return root


def store_generated_image_artifact(
    data_url: str,
    *,
    prompt: str,
    model: str,
    source_images: list[str] | None = None,
    save_dir: str = "generated",
    provider: str = "openrouter",
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """执行辅助逻辑（store_generated_image_artifact = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。
    在阅读 `store_generated_image_artifact` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    data_url: 结构化数据负载，后续会被解析或转发。
    prompt: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    source_images: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    save_dir: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    provider: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    created_at: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    raw, mime = decode_image_data_url(data_url)
    ext = _MIME_EXTENSIONS.get(mime)
    if ext is None:
        raise ArtifactError(f"unsupported image MIME type: {mime}")

    now = created_at or datetime.now().astimezone()
    day_dir = ensure_dir(_artifact_root(save_dir) / now.strftime("%Y-%m-%d"))
    artifact_id = f"img_{uuid.uuid4().hex[:12]}"
    image_path = day_dir / f"{artifact_id}{ext}"
    metadata_path = day_dir / f"{artifact_id}.json"

    image_path.write_bytes(raw)
    metadata: dict[str, Any] = {
        "id": artifact_id,
        "path": str(image_path),
        "mime": mime,
        "prompt": prompt,
        "model": model,
        "provider": provider,
        "source_images": list(source_images or []),
        "created_at": now.isoformat(),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return metadata


def generated_image_tool_result(artifacts: list[dict[str, Any]]) -> str:
    """执行辅助逻辑（generated_image_tool_result = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。管理工具执行产物，把文件路径、预览文本和下载信息整理成统一结构。
    在阅读 `generated_image_tool_result` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    artifacts: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return json.dumps(
        {
            "artifacts": artifacts,
            "next_step": (
                "Use these artifact paths as reference_images for follow-up edits. "
                "Call the message tool with the artifact paths in the media parameter "
                "to deliver the images to the user. Keep raw paths internal unless the "
                "user asks for debug details."
            ),
        },
        ensure_ascii=False,
    )

