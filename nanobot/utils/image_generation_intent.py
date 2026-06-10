"""图像意图识别工具。

【中文名称】图像意图识别工具

【功能说明】
负责从用户自然语言中识别“生成图片/修改图片”意图，避免把图像任务误当普通聊天。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

from typing import Any

IMAGE_GENERATION_METADATA_KEY = "image_generation"


def image_generation_prompt(content: str, metadata: dict[str, Any] | None) -> str:
    """执行 `image_generation_prompt`。

    【中文名称】image_generation_prompt

    【功能说明】
    这是 图像意图识别工具 中的一个步骤函数，用来支撑：负责从用户自然语言中识别“生成图片/修改图片”意图，避免把图像任务误当普通聊天。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
