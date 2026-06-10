"""识别用户是否在请求生成或编辑图片。

【中文名称】工具模块：nanobot/utils/image_generation_intent.py

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

from typing import Any

IMAGE_GENERATION_METADATA_KEY = "image_generation"


def image_generation_prompt(content: str, metadata: dict[str, Any] | None) -> str:
    """执行辅助逻辑（image_generation_prompt = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。识别用户是否在请求生成或编辑图片。
    在阅读 `image_generation_prompt` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    metadata: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    raw = (metadata or {}).get(IMAGE_GENERATION_METADATA_KEY)
    if not isinstance(raw, dict) or raw.get("enabled") is not True:
        return content

    aspect_ratio = raw.get("aspect_ratio")
    if isinstance(aspect_ratio, str) and aspect_ratio.strip():
        instruction = (
            "The user selected WebUI image generation mode. Use the generate_image tool. "
            f"When calling generate_image, pass aspect_ratio={aspect_ratio!r}."
        )
    else:
        instruction = (
            "The user selected WebUI image generation mode. Use the generate_image tool. "
            "Choose the most suitable aspect_ratio yourself from the prompt and intended use."
        )
    return f"{content}\n\n[WebUI image generation instruction: {instruction}]"

