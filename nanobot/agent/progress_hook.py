"""把 AgentRunner 事件适配成“用户可见进度 UI”的 Hook。"""

from __future__ import annotations

import inspect
import json
from typing import Any, Awaitable, Callable

from loguru import logger

from nanobot.agent.hook import AgentHook, AgentHookContext
from nanobot.utils.helpers import IncrementalThinkExtractor, strip_think
from nanobot.utils.progress_events import (
    build_tool_event_finish_payloads,
    build_tool_event_start_payload,
    invoke_on_progress,
    on_progress_accepts_tool_events,
)
from nanobot.utils.tool_hints import format_tool_hints


class AgentProgressHook(AgentHook):
    """把 Runner 生命周期事件翻译成用户可见进度信号。

    这是 AgentRunner 和 ChannelManager/前端展示层之间的重要桥梁：
    Runner 只管说“模型开始流式输出了”“即将调用这些工具”“本轮结束了”，
    这个 Hook 负责把它们转换成 on_progress / on_stream 这类更贴近 UI 的事件。
    """

    def __init__(
        self,
        on_progress: Callable[..., Awaitable[None]] | None = None,
        on_stream: Callable[[str], Awaitable[None]] | None = None,
        on_stream_end: Callable[..., Awaitable[None]] | None = None,
        *,
        channel: str = "cli",
        chat_id: str = "direct",
        message_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        session_key: str | None = None,
        tool_hint_max_length: int = 40,
        set_tool_context: Callable[..., None] | None = None,
        on_iteration: Callable[[int], None] | None = None,
    ) -> None:
        super().__init__(reraise=True)
        self._on_progress = on_progress
        self._on_stream = on_stream
        self._on_stream_end = on_stream_end
        self._channel = channel
        self._chat_id = chat_id
        self._message_id = message_id
        self._metadata = metadata or {}
        self._session_key = session_key
        self._tool_hint_max_length = tool_hint_max_length
        self._set_tool_context = set_tool_context
        self._on_iteration = on_iteration
        self._stream_buf = ""  # 累积流式文本，用来做增量切片与 think 标签剥离
        self._think_extractor = IncrementalThinkExtractor()
        self._reasoning_open = False  # 标记当前是否已经打开 reasoning 段

    def wants_streaming(self) -> bool:
        return self._on_stream is not None

    @staticmethod
    def _strip_think(text: str | None) -> str | None:
        if not text:
            return None
        return strip_think(text) or None

    def _tool_hint(self, tool_calls: list[Any]) -> str:
        return format_tool_hints(tool_calls, max_length=self._tool_hint_max_length)

    @staticmethod
    def _on_progress_accepts(cb: Callable[..., Any], name: str) -> bool:
        try:
            sig = inspect.signature(cb)
        except (TypeError, ValueError):
            return False
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
            return True
        return name in sig.parameters

    async def on_stream(self, context: AgentHookContext, delta: str) -> None:
        # 模型流式内容里可能夹带 <think>...</think> 一类内部思考标签，
        # UI 展示给用户时需要把“答案正文”和“推理痕迹”拆开。
        prev_clean = strip_think(self._stream_buf)
        self._stream_buf += delta
        new_clean = strip_think(self._stream_buf)
        incremental = new_clean[len(prev_clean) :]

        if await self._think_extractor.feed(self._stream_buf, self.emit_reasoning):
            context.streamed_reasoning = True

        if incremental:
            # 一旦正式答案开始输出，就关闭 reasoning 段。
            # 这样前端可以先把“思考气泡”固定住，再在下方开始渲染答案正文。
            await self.emit_reasoning_end()
            if self._on_stream:
                await self._on_stream(incremental)

    async def on_stream_end(self, context: AgentHookContext, *, resuming: bool) -> None:
        await self.emit_reasoning_end()
        if self._on_stream_end:
            await self._on_stream_end(resuming=resuming)
        self._stream_buf = ""
        self._think_extractor.reset()

    async def before_iteration(self, context: AgentHookContext) -> None:
        if self._on_iteration:
            self._on_iteration(context.iteration)
        logger.debug(
            "Starting agent loop iteration {} for session {}",
            context.iteration,
            self._session_key,
        )

    async def before_execute_tools(self, context: AgentHookContext) -> None:
        if self._on_progress:
            if not self._on_stream and not context.streamed_content:
                thought = self._strip_think(context.response.content if context.response else None)
                if thought:
                    await self._on_progress(thought)
            tool_hint = self._strip_think(self._tool_hint(context.tool_calls))
            tool_events = [build_tool_event_start_payload(tc) for tc in context.tool_calls]
            await invoke_on_progress(
                self._on_progress,
                tool_hint,
                tool_hint=True,
                tool_events=tool_events,
            )
        for tc in context.tool_calls:
            args_str = json.dumps(tc.arguments, ensure_ascii=False)
            logger.info("Tool call: {}({})", tc.name, args_str[:200])
        if self._set_tool_context:
            self._set_tool_context(
                self._channel,
                self._chat_id,
                self._message_id,
                self._metadata,
                session_key=self._session_key,
            )

    async def emit_reasoning(self, reasoning_content: str | None) -> None:
        """发布一段 reasoning 增量内容；具体要不要显示由渠道插件决定。"""
        if (
            self._on_progress
            and reasoning_content
            and self._on_progress_accepts(self._on_progress, "reasoning")
        ):
            self._reasoning_open = True
            await self._on_progress(reasoning_content, reasoning=True)

    async def emit_reasoning_end(self) -> None:
        """结束当前 reasoning 流片段。"""
        if self._reasoning_open and self._on_progress:
            self._reasoning_open = False
            await self._on_progress("", reasoning_end=True)
        else:
            self._reasoning_open = False

    async def after_iteration(self, context: AgentHookContext) -> None:
        if (
            self._on_progress
            and context.tool_calls
            and context.tool_events
            and on_progress_accepts_tool_events(self._on_progress)
        ):
            tool_events = build_tool_event_finish_payloads(context)
            if tool_events:
                await invoke_on_progress(
                    self._on_progress,
                    "",
                    tool_hint=False,
                    tool_events=tool_events,
                )
        u = context.usage or {}
        logger.debug(
            "LLM usage: prompt={} completion={} cached={}",
            u.get("prompt_tokens", 0),
            u.get("completion_tokens", 0),
            u.get("cached_tokens", 0),
        )

    def finalize_content(self, context: AgentHookContext, content: str | None) -> str | None:
        return self._strip_think(content)
