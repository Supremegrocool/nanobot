"""Agent 运行生命周期 Hook 抽象。

你可以把 Hook 理解成“在 AgentRunner 主循环的关键节点插进去的扩展点”。
它本身不负责决定业务逻辑，而是负责让日志、进度 UI、SDK 捕获器等模块能够
在不污染主循环代码的前提下插入自己的行为。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from nanobot.providers.base import LLMResponse, ToolCallRequest


@dataclass(slots=True)
class AgentHookContext:
    """单次 iteration 级别的可变上下文。

    每一轮“请求模型 -> 可能调工具 -> 再继续”的循环，都会复用这一份上下文，
    Hook 可以在这里看到当轮响应、工具调用、使用量、最终文本等信息。
    """

    iteration: int
    messages: list[dict[str, Any]]
    response: LLMResponse | None = None
    usage: dict[str, int] = field(default_factory=dict)
    tool_calls: list[ToolCallRequest] = field(default_factory=list)
    tool_results: list[Any] = field(default_factory=list)
    tool_events: list[dict[str, str]] = field(default_factory=list)
    streamed_content: bool = False
    streamed_reasoning: bool = False
    final_content: str | None = None
    stop_reason: str | None = None
    error: str | None = None
    session_key: str | None = None


@dataclass(slots=True)
class AgentRunHookContext:
    """一次完整 run 级别的上下文快照。"""

    messages: list[dict[str, Any]]
    final_content: str | None = None
    tools_used: list[str] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)
    stop_reason: str | None = None
    error: str | None = None
    tool_events: list[dict[str, str]] = field(default_factory=list)
    had_injections: bool = False
    exception: BaseException | None = None


class AgentHook:
    """AgentRunner 可插拔生命周期接口。

    这是最小抽象基类。大多数方法默认空实现，子类只需要重写自己关心的节点。
    """

    def __init__(self, reraise: bool = False) -> None:
        self._reraise = reraise

    def wants_streaming(self) -> bool:
        return False

    async def before_run(self, context: AgentRunHookContext) -> None:
        pass

    async def after_run(self, context: AgentRunHookContext) -> None:
        pass

    async def on_error(self, context: AgentRunHookContext) -> None:
        pass

    async def on_finally(self, context: AgentRunHookContext) -> None:
        pass

    async def before_iteration(self, context: AgentHookContext) -> None:
        pass

    async def on_stream(self, context: AgentHookContext, delta: str) -> None:
        pass

    async def on_stream_end(self, context: AgentHookContext, *, resuming: bool) -> None:
        pass

    async def before_execute_tools(self, context: AgentHookContext) -> None:
        pass

    async def emit_reasoning(self, reasoning_content: str | None) -> None:
        pass

    async def emit_reasoning_end(self) -> None:
        """标记“推理流”已经结束。

        如果某个 Hook 是按增量片段缓存 reasoning 的，那么它会在这里把缓存刷出、
        结束当前分组；如果是一次性输出型 Hook，则可以直接忽略。
        """
        pass

    async def after_iteration(self, context: AgentHookContext) -> None:
        pass

    def finalize_content(self, context: AgentHookContext, content: str | None) -> str | None:
        return content


class CompositeHook(AgentHook):
    """组合 Hook：把同一个生命周期事件广播给多个 Hook。

    【为什么需要它】
    一个 turn 里我们可能同时需要：
    - 进度显示 Hook
    - SDK 捕获 Hook
    - 自定义日志 Hook

    让 AgentRunner 只面对一个 Hook 对象，内部再由 CompositeHook 分发，会更整洁。

    【错误隔离策略】
    大多数异步 Hook 方法都会单独捕获异常，避免一个坏 Hook 直接拖垮整个主循环。
    但 ``finalize_content`` 不做隔离，因为这一步改的是最终内容，逻辑错误应该暴露。
    """

    __slots__ = ("_hooks",)

    def __init__(self, hooks: list[AgentHook]) -> None:
        super().__init__()
        self._hooks = list(hooks)

    def wants_streaming(self) -> bool:
        return any(h.wants_streaming() for h in self._hooks)

    async def _for_each_hook_safe(self, method_name: str, *args: Any, **kwargs: Any) -> None:
        for h in self._hooks:
            if getattr(h, "_reraise", False):
                await getattr(h, method_name)(*args, **kwargs)
                continue

            try:
                await getattr(h, method_name)(*args, **kwargs)
            except Exception:
                logger.exception("AgentHook.{} error in {}", method_name, type(h).__name__)

    async def before_iteration(self, context: AgentHookContext) -> None:
        await self._for_each_hook_safe("before_iteration", context)

    async def before_run(self, context: AgentRunHookContext) -> None:
        await self._for_each_hook_safe("before_run", context)

    async def after_run(self, context: AgentRunHookContext) -> None:
        await self._for_each_hook_safe("after_run", context)

    async def on_error(self, context: AgentRunHookContext) -> None:
        await self._for_each_hook_safe("on_error", context)

    async def on_finally(self, context: AgentRunHookContext) -> None:
        await self._for_each_hook_safe("on_finally", context)

    async def on_stream(self, context: AgentHookContext, delta: str) -> None:
        await self._for_each_hook_safe("on_stream", context, delta)

    async def on_stream_end(self, context: AgentHookContext, *, resuming: bool) -> None:
        await self._for_each_hook_safe("on_stream_end", context, resuming=resuming)

    async def before_execute_tools(self, context: AgentHookContext) -> None:
        await self._for_each_hook_safe("before_execute_tools", context)

    async def emit_reasoning(self, reasoning_content: str | None) -> None:
        await self._for_each_hook_safe("emit_reasoning", reasoning_content)

    async def emit_reasoning_end(self) -> None:
        await self._for_each_hook_safe("emit_reasoning_end")

    async def after_iteration(self, context: AgentHookContext) -> None:
        await self._for_each_hook_safe("after_iteration", context)

    def finalize_content(self, context: AgentHookContext, content: str | None) -> str | None:
        for h in self._hooks:
            content = h.finalize_content(context, content)
        return content


class SDKCaptureHook(AgentHook):
    """给 SDK 调用方抓取最终消息列表和工具使用情况。

    AgentRunner 在循环中会原地修改 ``context.messages``，所以这里每次 iteration
    结束都会刷新一次快照。最终 run 结束时，再用 run 级上下文补一份权威结果。
    """

    def __init__(self) -> None:
        super().__init__()
        self.tools_used: list[str] = []
        self.messages: list[dict[str, Any]] = []

    async def after_iteration(self, context: AgentHookContext) -> None:
        for call in context.tool_calls:
            self.tools_used.append(call.name)
        self.messages = list(context.messages)

    async def after_run(self, context: AgentRunHookContext) -> None:
        self.tools_used = list(context.tools_used)
        self.messages = list(context.messages)
