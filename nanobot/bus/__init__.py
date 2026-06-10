"""消息总线模块：用于解耦渠道层与 Agent 核心之间的通信。"""

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus

__all__ = ["MessageBus", "InboundMessage", "OutboundMessage"]
