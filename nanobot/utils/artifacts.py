"""产物路径工具。

【中文名称】产物路径工具

【功能说明】
负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """ArtifactError 类。

    【中文名称】ArtifactError

    【功能说明】
    这是 产物路径工具 中的核心数据结构或服务类。负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""


def decode_image_data_url(data_url: str) -> tuple[bytes, str]:
    """执行 `decode_image_data_url`。

    【中文名称】decode_image_data_url

    【功能说明】
    这是 产物路径工具 中的一个步骤函数，用来支撑：负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - data_url: 调用方传入的 `data_url` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `_safe_relative_dir`。

    【中文名称】_safe_relative_dir

    【功能说明】
    这是 产物路径工具 中的一个步骤函数，用来支撑：负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - save_dir: 调用方传入的 `save_dir` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    normalized = save_dir.replace("\\", "/").strip("/")
    if not normalized:
        raise ArtifactError("save_dir must not be empty")
    rel = PurePosixPath(normalized)
    if rel.is_absolute() or any(part in {"", ".", ".."} for part in rel.parts):
        raise ArtifactError("save_dir must be a safe relative path")
    return Path(*rel.parts)


def _artifact_root(save_dir: str) -> Path:
    """执行 `_artifact_root`。

    【中文名称】_artifact_root

    【功能说明】
    这是 产物路径工具 中的一个步骤函数，用来支撑：负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - save_dir: 调用方传入的 `save_dir` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
    """执行 `store_generated_image_artifact`。

    【中文名称】store_generated_image_artifact

    【功能说明】
    这是 产物路径工具 中的一个步骤函数，用来支撑：负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - data_url: 调用方传入的 `data_url` 数据；具体类型以函数签名为准。
    - prompt: 调用方传入的 `prompt` 数据；具体类型以函数签名为准。
    - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
    - source_images: 调用方传入的 `source_images` 数据；具体类型以函数签名为准。
    - save_dir: 调用方传入的 `save_dir` 数据；具体类型以函数签名为准。
    - provider: 调用方传入的 `provider` 数据；具体类型以函数签名为准。
    - created_at: 调用方传入的 `created_at` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """执行 `generated_image_tool_result`。

    【中文名称】generated_image_tool_result

    【功能说明】
    这是 产物路径工具 中的一个步骤函数，用来支撑：负责为工具调用、图片生成等运行产物创建稳定目录和安全文件名，便于 WebUI 展示和后续引用。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - artifacts: 调用方传入的 `artifacts` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
