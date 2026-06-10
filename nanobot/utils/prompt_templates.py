"""提示词模板工具。

【中文名称】提示词模板工具

【功能说明】
负责加载和渲染内置提示词模板，避免调用处散落字符串拼接逻辑。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from functools import lru_cache
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_ROOT = Path(__file__).resolve().parent.parent / "templates"


@lru_cache
def _environment() -> Environment:
    # 说明：这里处理 提示词模板工具 的协议细节或边界情况，避免外部差异影响核心流程。
    """执行 `_environment`。

    【中文名称】_environment

    【功能说明】
    这是 提示词模板工具 中的一个步骤函数，用来支撑：负责加载和渲染内置提示词模板，避免调用处散落字符串拼接逻辑。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES_ROOT)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def render_template(name: str, *, strip: bool = False, **kwargs: Any) -> str:
    """执行 `render_template`。

    【中文名称】render_template

    【功能说明】
    这是 提示词模板工具 中的一个步骤函数，用来支撑：负责加载和渲染内置提示词模板，避免调用处散落字符串拼接逻辑。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。
    - strip: 调用方传入的 `strip` 数据；具体类型以函数签名为准。
    - **kwargs: 调用方传入的 `kwargs` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    text = _environment().get_template(name).render(**kwargs)
    return text.rstrip() if strip else text
