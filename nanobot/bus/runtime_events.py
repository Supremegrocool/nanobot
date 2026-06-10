"""运行时事件总线：用于发布 Agent 内部状态变化通知。

注意它和 ``nanobot.bus.queue`` 不是一回事：

- ``bus.queue`` 负责“真实聊天消息”的进出
- 本模块负责“运行时状态事件”的进程内广播

例如：
- 某个 turn 开始了
- 某个 turn 进入 running 状态
- 某个会话的 goal 状态变了
- 当前运行模型被切换了

这些信息未必都要直接发给用户，但 WebUI、监控层、调试层往往需要订阅。
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from nanobot.bus.events import InboundMessage


@dataclass(frozen=True)
class RuntimeEventContext:
    """turn 级运行时事件的公共路由上下文。"""

    channel: str
    chat_id: str
    session_key: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionTurnStarted:
    """表示一个用户/系统回合已经拿到 Session，即将开始构建上下文。"""

    context: RuntimeEventContext


@dataclass(frozen=True)
class TurnRunStatusChanged:
    """表示某个回合的可见运行状态发生变化。"""

    context: RuntimeEventContext
    status: str
    started_at: float | None = None


@dataclass(frozen=True)
class TurnCompleted:
    """表示某个回合已经产出最终用户可见回复。"""

    context: RuntimeEventContext
    latency_ms: int | None = None
    runtime: Any | None = None


@dataclass(frozen=True)
class GoalStateChanged:
    """表示某个会话的持续目标（sustained goal）状态发生变化。"""

    context: RuntimeEventContext
    session_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeModelChanged:
    """表示当前运行使用的模型或预设被切换。"""

    model: str
    model_preset: str | None


RuntimeEvent = (
    SessionTurnStarted
    | TurnRunStatusChanged
    | TurnCompleted
    | GoalStateChanged
    | RuntimeModelChanged
)
RuntimeEventType = (
    type[SessionTurnStarted]
    | type[TurnRunStatusChanged]
    | type[TurnCompleted]
    | type[GoalStateChanged]
    | type[RuntimeModelChanged]
)
RuntimeEventHandler = Callable[[Any], Awaitable[None] | None]
_HandlerEntry = tuple[RuntimeEventType | None, RuntimeEventHandler]


class RuntimeEventBus:
    """轻量级进程内发布订阅总线。

    【适用场景】
    当 Agent 内部想广播“状态变化”而不是“聊天消息”时，就走这里。

    【两个发布方式】
    - ``publish``：异步等待所有订阅者处理完成，适合对顺序敏感的场景
    - ``publish_nowait``：只负责投递，不等待结果，适合同步上下文里快速通知
    """

    def __init__(self) -> None:
        self._handlers: list[_HandlerEntry] = []

    def subscribe(
        self,
        handler: RuntimeEventHandler,
        event_type: RuntimeEventType | None = None,
    ) -> Callable[[], None]:
        entry = (event_type, handler)
        self._handlers.append(entry)

        def _unsubscribe() -> None:
            # 返回一个取消订阅函数，调用者自己决定何时解绑。
            with contextlib.suppress(ValueError):
                self._handlers.remove(entry)

        return _unsubscribe

    async def publish(self, event: RuntimeEvent) -> None:
        for event_type, handler in list(self._handlers):
            if event_type is not None and not isinstance(event, event_type):
                continue
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("runtime event handler failed for {}", type(event).__name__)

    def publish_nowait(self, event: RuntimeEvent) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("dropping runtime event without a running loop: {}", type(event).__name__)
            return
        loop.create_task(self.publish(event))


class RuntimeEventPublisher:
    """面向 Agent 业务层的便捷发布器。

    AgentLoop 不需要手工拼每一种事件对象，只要调用这里的高层方法即可。
    这样“什么时候发事件”由 Agent 逻辑决定，而“事件对象怎么组装”集中在这里。
    """

    def __init__(self, bus: RuntimeEventBus | None = None) -> None:
        self.bus = bus or RuntimeEventBus()
        self._turn_latency_ms: dict[str, int] = {}
        self._turn_runtime: dict[str, Any] = {}

    @staticmethod
    def _context(
        *,
        channel: str,
        chat_id: str,
        session_key: str,
        metadata: dict[str, Any] | None,
    ) -> RuntimeEventContext:
        return RuntimeEventContext(
            channel=channel,
            chat_id=chat_id,
            session_key=session_key,
            metadata=dict(metadata or {}),
        )

    def record_turn_runtime(self, session_key: str, runtime: Any) -> None:
        """缓存某个 turn 的运行时信息，供 turn 完成事件一并带出。"""
        self._turn_runtime[session_key] = runtime

    def record_turn_latency(self, session_key: str, latency_ms: int | None) -> None:
        """缓存某个 turn 的耗时，等 turn 完成时一起发布。"""
        if latency_ms is not None:
            self._turn_latency_ms[session_key] = int(latency_ms)

    def clear_turn(self, session_key: str) -> None:
        """清理某个 turn 的暂存发布数据。"""
        self._turn_latency_ms.pop(session_key, None)
        self._turn_runtime.pop(session_key, None)

    async def session_turn_started(
        self,
        msg: InboundMessage,
        session_key: str,
    ) -> None:
        await self.bus.publish(
            SessionTurnStarted(
                context=self._context(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    session_key=session_key,
                    metadata=msg.metadata,
                )
            )
        )

    async def run_status_changed(
        self,
        msg: InboundMessage,
        session_key: str,
        status: str,
        *,
        started_at: float | None = None,
    ) -> None:
        await self.bus.publish(
            TurnRunStatusChanged(
                context=self._context(
                    channel=msg.channel,
                    chat_id=msg.chat_id,
                    session_key=session_key,
                    metadata=msg.metadata,
                ),
                status=status,
                started_at=started_at,
            )
        )

    async def turn_completed(
        self,
        *,
        channel: str,
        chat_id: str,
        session_key: str,
        metadata: dict[str, Any] | None,
    ) -> None:
        await self.bus.publish(
            TurnCompleted(
                context=self._context(
                    channel=channel,
                    chat_id=chat_id,
                    session_key=session_key,
                    metadata=metadata,
                ),
                latency_ms=self._turn_latency_ms.pop(session_key, None),
                runtime=self._turn_runtime.pop(session_key, None),
            )
        )

    def runtime_model_changed(self, model: str, model_preset: str | None) -> None:
        self.bus.publish_nowait(
            RuntimeModelChanged(model=model, model_preset=model_preset)
        )


def ensure_runtime_event_publisher(owner: Any) -> RuntimeEventPublisher:
    """确保某个对象上存在 ``RuntimeEventPublisher``，没有就懒创建。

    这是一种“按需补齐依赖”的写法，适合被多个入口复用的宿主对象。
    """
    publisher = getattr(owner, "runtime_event_publisher", None)
    if isinstance(publisher, RuntimeEventPublisher):
        return publisher

    bus = getattr(owner, "runtime_events", None)
    if not isinstance(bus, RuntimeEventBus):
        bus = RuntimeEventBus()
        owner.runtime_events = bus

    publisher = RuntimeEventPublisher(bus)
    owner.runtime_event_publisher = publisher
    return publisher
