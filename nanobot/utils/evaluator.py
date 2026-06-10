"""让后台任务在结束后自评是否需要主动通知用户。

【中文名称】工具模块：nanobot/utils/evaluator.py

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

from typing import TYPE_CHECKING

from loguru import logger

from nanobot.utils.prompt_templates import render_template

if TYPE_CHECKING:
    from nanobot.providers.base import LLMProvider

_EVALUATE_TOOL = [
    {
        "type": "function",
        "function": {
            "name": "evaluate_notification",
            "description": "Decide whether the user should be notified about this background task result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "should_notify": {
                        "type": "boolean",
                        "description": "true = result contains actionable/important info the user should see; false = routine or empty, safe to suppress",
                    },
                    "reason": {
                        "type": "string",
                        "description": "One-sentence reason for the decision",
                    },
                },
                "required": ["should_notify"],
            },
        },
    }
]

async def evaluate_response(
    response: str,
    task_context: str,
    provider: LLMProvider,
    model: str,
    default_notify: bool = True,
) -> bool:
    """异步执行辅助逻辑（evaluate_response = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。让后台任务在结束后自评是否需要主动通知用户。
    在阅读 `evaluate_response` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    response: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    task_context: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    provider: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    default_notify: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        llm_response = await provider.chat_with_retry(
            messages=[
                {"role": "system", "content": render_template("agent/evaluator.md", part="system")},
                {"role": "user", "content": render_template(
                    "agent/evaluator.md",
                    part="user",
                    task_context=task_context,
                    response=response,
                )},
            ],
            tools=_EVALUATE_TOOL,
            model=model,
            max_tokens=256,
            temperature=0.0,
        )

        if not llm_response.should_execute_tools:
            if llm_response.has_tool_calls:
                logger.warning(
                    "evaluate_response: ignoring tool calls under finish_reason='{}', "
                    "defaulting to notify={}",
                    llm_response.finish_reason,
                    default_notify,
                )
            else:
                logger.warning(
                    "evaluate_response: no tool call returned, defaulting to notify={}",
                    default_notify,
                )
            return default_notify

        args = llm_response.tool_calls[0].arguments
        should_notify = args.get("should_notify", default_notify)
        reason = args.get("reason", "")
        logger.info("evaluate_response: should_notify={}, reason={}", should_notify, reason)
        return bool(should_notify)

    except Exception:
        logger.exception("evaluate_response failed, defaulting to notify={}", default_notify)
        return default_notify

