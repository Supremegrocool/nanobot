"""Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/msteams.py

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

import asyncio
import html
import importlib.util
import json
import os
import re
import tempfile
import threading
import time
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

try:  # pragma: no cover - Windows fallback path
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None

import httpx
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_workspace_path
from nanobot.config.schema import Base

MSTEAMS_AVAILABLE = (
    importlib.util.find_spec("jwt") is not None
    and importlib.util.find_spec("cryptography") is not None
)

if TYPE_CHECKING:
    import jwt

if MSTEAMS_AVAILABLE:
    import jwt

MSTEAMS_REF_TTL_DAYS = 30
MSTEAMS_WEBCHAT_HOST = "webchat.botframework.com"
MSTEAMS_DEFAULT_TRUSTED_SERVICE_URL_HOSTS = [
    "smba.trafficmanager.net",
    "smba.infra.gcc.teams.microsoft.com",
    "smba.infra.gov.teams.microsoft.us",
    "smba.infra.dod.teams.microsoft.us",
    "*.botframework.com",
]
MSTEAMS_REF_META_FILENAME = "msteams_conversations_meta.json"
MSTEAMS_REF_LOCK_FILENAME = "msteams_conversations.lock"
MSTEAMS_REF_TOUCH_INTERVAL_S = 300


class MSTeamsConfig(Base):
    """MSTeamsConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】MSTeamsConfig

    【功能说明】
    Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    app_id: str = ""
    app_password: str = ""
    tenant_id: str = ""
    host: str = "0.0.0.0"
    port: int = 3978
    path: str = "/api/messages"
    allow_from: list[str] = Field(default_factory=list)
    reply_in_thread: bool = True
    mention_only_response: str = "Hi — what can I help with?"
    validate_inbound_auth: bool = True
    ref_ttl_days: int = Field(default=MSTEAMS_REF_TTL_DAYS, ge=1)
    prune_web_chat_refs: bool = True
    prune_non_personal_refs: bool = True
    ref_touch_interval_s: int = Field(default=MSTEAMS_REF_TOUCH_INTERVAL_S, ge=0)
    trusted_service_url_hosts: list[str] = Field(
        default_factory=lambda: MSTEAMS_DEFAULT_TRUSTED_SERVICE_URL_HOSTS.copy()
    )


@dataclass
class ConversationRef:
    """ConversationRef 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】ConversationRef

    【功能说明】
    Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    service_url: str
    conversation_id: str
    bot_id: str | None = None
    activity_id: str | None = None
    conversation_type: str | None = None
    tenant_id: str | None = None
    updated_at: float | None = None


class MSTeamsChannel(BaseChannel):
    """MSTeamsChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】MSTeamsChannel

    【功能说明】
    Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "msteams"
    display_name = "Microsoft Teams"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return MSTeamsConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = MSTeamsConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: MSTeamsConfig = config
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._http: httpx.AsyncClient | None = None
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._botframework_openid_config_url = (
            "https://login.botframework.com/v1/.well-known/openidconfiguration"
        )
        self._botframework_openid_config: dict[str, Any] | None = None
        self._botframework_openid_config_expires_at: float = 0.0
        self._botframework_jwks: dict[str, Any] | None = None
        self._botframework_jwks_expires_at: float = 0.0
        self._refs_path = get_workspace_path() / "state" / "msteams_conversations.json"
        self._refs_path.parent.mkdir(parents=True, exist_ok=True)
        self._refs_meta_path = self._refs_path.parent / MSTEAMS_REF_META_FILENAME
        self._refs_lock_path = self._refs_path.parent / MSTEAMS_REF_LOCK_FILENAME
        self._refs_guard = threading.RLock()
        self._conversation_refs: dict[str, ConversationRef] = self._load_refs()
        with self._refs_guard:
            if self._prune_conversation_refs():
                self._save_refs_locked(prune=True)

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not MSTEAMS_AVAILABLE:
            self.logger.error("PyJWT not installed. Run: pip install nanobot-ai[msteams]")
            return

        if not self.config.app_id or not self.config.app_password:
            self.logger.error("app_id/app_password not configured")
            return

        if not self.config.validate_inbound_auth:
            self.logger.warning(
                "Inbound auth validation was explicitly DISABLED in config. "
                "Anyone who knows the webhook URL can send messages as any user. "
                "Only disable this for local development or controlled testing."
            )

        self._loop = asyncio.get_running_loop()
        self._http = httpx.AsyncClient(timeout=30.0)
        self._running = True

        channel = self

        class Handler(BaseHTTPRequestHandler):
            """Handler 类，封装 渠道适配器 的核心状态和行为。

            【中文名称】Handler

            【功能说明】
            Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
            让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

            【继承关系】
            BaseHTTPRequestHandler。继承关系决定它需要实现哪些项目约定的方法。

            【学习提示】
            先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
            """
            def do_POST(self) -> None:
                """执行辅助逻辑（do_POST = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `Handler.do_POST` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                self: 当前对象或类本身，用于访问配置、客户端和共享状态。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                if self.path != channel.config.path:
                    self.send_response(404)
                    self.end_headers()
                    return

                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    raw = self.rfile.read(length) if length > 0 else b"{}"
                    payload = json.loads(raw.decode("utf-8"))
                except Exception as e:
                    channel.logger.warning("Invalid request body: {}", e)
                    self.send_response(400)
                    self.end_headers()
                    return

                auth_header = self.headers.get("Authorization", "")
                if channel.config.validate_inbound_auth:
                    try:
                        fut = asyncio.run_coroutine_threadsafe(
                            channel._validate_inbound_auth(auth_header, payload),
                            channel._loop,
                        )
                        fut.result(timeout=15)
                    except Exception as e:
                        channel.logger.warning("Inbound auth validation failed: {}", e)
                        self.send_response(401)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(b'{"error":"unauthorized"}')
                        return
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        channel._handle_activity(payload),
                        channel._loop,
                    )
                    fut.result(timeout=15)
                except Exception as e:
                    channel.logger.warning("Activity handling failed: {}", e)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, format: str, *args: Any) -> None:
                """执行辅助逻辑（log_message = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `Handler.log_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                self: 当前对象或类本身，用于访问配置、客户端和共享状态。
                format: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
                *args: 可变位置参数，承载数量不固定的输入。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                return

        self._server = ThreadingHTTPServer((self.config.host, self.config.port), Handler)
        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            name="nanobot-msteams",
            daemon=True,
        )
        self._server_thread.start()

        self.logger.info(
            "Webhook listening on http://{}:{}{}",
            self.config.host,
            self.config.port,
            self.config.path,
        )

        while self._running:
            await asyncio.sleep(1)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._server_thread and self._server_thread.is_alive():
            self._server_thread.join(timeout=2)
        self._server_thread = None
        if self._http:
            await self._http.aclose()
            self._http = None

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._http:
            raise RuntimeError("MSTeams HTTP client not initialized")

        ref = self._conversation_refs.get(str(msg.chat_id))
        if not ref:
            raise RuntimeError(f"MSTeams conversation ref not found for chat_id={msg.chat_id}")

        if not self._is_trusted_service_url(ref.service_url):
            raise RuntimeError(
                f"MSTeams conversation ref has untrusted service_url for chat_id={msg.chat_id}"
            )

        token = await self._get_access_token()
        base_url = f"{ref.service_url.rstrip('/')}/v3/conversations/{ref.conversation_id}/activities"
        use_thread_reply = self.config.reply_in_thread and bool(ref.activity_id)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "type": "message",
            "text": msg.content or " ",
        }
        if use_thread_reply:
            payload["replyToId"] = ref.activity_id

        try:
            resp = await self._http.post(base_url, headers=headers, json=payload)
            resp.raise_for_status()
            self.logger.info("Message sent to {}", ref.conversation_id)
            self._touch_conversation_ref(str(msg.chat_id), persist=True)
        except Exception:
            self.logger.exception("Send failed")
            raise

    async def _handle_activity(self, activity: dict[str, Any]) -> None:
        """异步处理事件（_handle_activity = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._handle_activity` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        activity: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if activity.get("type") != "message":
            return

        conversation = activity.get("conversation") or {}
        from_user = activity.get("from") or {}
        recipient = activity.get("recipient") or {}
        channel_data = activity.get("channelData") or {}

        sender_id = str(from_user.get("aadObjectId") or from_user.get("id") or "").strip()
        conversation_id = str(conversation.get("id") or "").strip()
        service_url = str(activity.get("serviceUrl") or "").strip()
        activity_id = str(activity.get("id") or "").strip()
        conversation_type = str(conversation.get("conversationType") or "").strip()

        if not sender_id or not conversation_id or not service_url:
            return

        if not self._is_trusted_service_url(service_url):
            self.logger.warning(
                "Ignoring MSTeams activity with untrusted serviceUrl host: {}",
                service_url,
            )
            return

        if recipient.get("id") and from_user.get("id") == recipient.get("id"):
            return

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        if conversation_type and conversation_type not in ("personal", ""):
            self.logger.debug("Ignoring non-DM conversation {}", conversation_type)
            return

        text = self._sanitize_inbound_text(activity)
        if not text:
            text = self.config.mention_only_response.strip()
            if not text:
                self.logger.debug("Ignoring empty message after Teams text sanitization")
                return

        if not self.is_allowed(sender_id):
            self.logger.warning(
                "Access denied for sender {} on channel {}. "
                "Add them to allowFrom list in config to grant access.",
                sender_id, self.name,
            )
            return

        with self._refs_guard:
            self._conversation_refs[conversation_id] = ConversationRef(
                service_url=service_url,
                conversation_id=conversation_id,
                bot_id=str(recipient.get("id") or "") or None,
                activity_id=activity_id or None,
                conversation_type=conversation_type or None,
                tenant_id=str((channel_data.get("tenant") or {}).get("id") or "") or None,
                updated_at=time.time(),
            )
            self._save_refs_locked()

        await self._handle_message(
            sender_id=sender_id,
            chat_id=conversation_id,
            content=text,
            metadata={
                "msteams": {
                    "activity_id": activity_id,
                    "conversation_id": conversation_id,
                    "conversation_type": conversation_type or "personal",
                    "from_name": from_user.get("name"),
                }
            },
        )

    def _sanitize_inbound_text(self, activity: dict[str, Any]) -> str:
        """执行辅助逻辑（_sanitize_inbound_text = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._sanitize_inbound_text` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        activity: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        text = str(activity.get("text") or "")
        text = self._strip_possible_bot_mention(text)
        text = self._normalize_html_whitespace(text)

        channel_data = activity.get("channelData") or {}
        reply_to_id = str(activity.get("replyToId") or "").strip()
        normalized_preview = html.unescape(text).replace("&rsquo", "’").strip()
        normalized_preview = normalized_preview.replace("\xa0", " ")
        normalized_preview = normalized_preview.replace("\r\n", "\n").replace("\r", "\n")
        preview_lines = [line.strip() for line in normalized_preview.split("\n")]
        while preview_lines and not preview_lines[0]:
            preview_lines.pop(0)
        first_line = preview_lines[0] if preview_lines else ""
        looks_like_quote_wrapper = first_line.lower().startswith("replying to ") or first_line.startswith("Reply wrapper")

        if reply_to_id or channel_data.get("messageType") == "reply" or looks_like_quote_wrapper:
            text = self._normalize_teams_reply_quote(text)

        return text.strip()

    def _strip_possible_bot_mention(self, text: str) -> str:
        """执行辅助逻辑（_strip_possible_bot_mention = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._strip_possible_bot_mention` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        cleaned = re.sub(r"<at\b[^>]*>.*?</at>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        cleaned = re.sub(r"[^\S\r\n]+", " ", cleaned)
        cleaned = re.sub(r"(?:\r?\n){3,}", "\n\n", cleaned)
        return cleaned.strip()

    def _normalize_html_whitespace(self, text: str) -> str:
        """标准化数据（_normalize_html_whitespace = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._normalize_html_whitespace` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        normalized = html.unescape(text).replace("&rsquo", "’")
        normalized = normalized.replace("\xa0", " ")
        return normalized

    def _normalize_teams_reply_quote(self, text: str) -> str:
        """标准化数据（_normalize_teams_reply_quote = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._normalize_teams_reply_quote` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        cleaned = self._normalize_html_whitespace(text).strip()
        if not cleaned:
            return ""

        normalized_newlines = cleaned.replace("\r\n", "\n").replace("\r", "\n")
        lines = [line.strip() for line in normalized_newlines.split("\n")]
        while lines and not lines[0]:
            lines.pop(0)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：actual reply text 相关逻辑。
        if len(lines) >= 2 and lines[0].lower().startswith("replying to "):
            quoted = lines[0][len("replying to ") :].strip(" :")
            reply = "\n".join(lines[1:]).strip()
            return self._format_reply_with_quote(quoted, reply)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        # 中文说明：兜底。
        if lines and lines[0].strip().startswith("Reply wrapper"):
            body = normalized_newlines.split("\n", 1)[1] if "\n" in normalized_newlines else ""
            body = body.lstrip()
            parts = re.split(r"\n\s*\n", body, maxsplit=1)
            if len(parts) == 2:
                quoted = re.sub(r"\s+", " ", parts[0]).strip()
                reply = re.sub(r"\s+", " ", parts[1]).strip()
                if quoted or reply:
                    return self._format_reply_with_quote(quoted, reply)

            body_lines = [line.strip() for line in body.split("\n") if line.strip()]
            if body_lines:
                quoted = " ".join(body_lines[:-1]).strip()
                reply = body_lines[-1].strip()
                if quoted and reply:
                    return self._format_reply_with_quote(quoted, reply)

        # 中文说明：兜底。
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        compact = re.sub(r"\s+", " ", normalized_newlines).strip()
        if compact.startswith("Reply wrapper "):
            compact = compact[len("Reply wrapper ") :].strip()
            for boundary in (". ", "! ", "? ", "… "):
                idx = compact.rfind(boundary)
                if idx == -1:
                    continue
                quoted = compact[: idx + 1].strip()
                reply = compact[idx + len(boundary) :].strip()
                if quoted and reply and len(reply) <= 160:
                    return self._format_reply_with_quote(quoted, reply)

        return cleaned

    def _format_reply_with_quote(self, quoted: str, reply: str) -> str:
        """格式化内容（_format_reply_with_quote = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._format_reply_with_quote` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        quoted: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reply: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        quoted = quoted.strip()
        reply = reply.strip()
        if quoted and reply:
            return f"User is replying to: {quoted}\nUser reply: {reply}"
        if reply:
            return reply
        return quoted

    async def _validate_inbound_auth(self, auth_header: str, activity: dict[str, Any]) -> None:
        """异步校验输入（_validate_inbound_auth = 原函数名）。

        【中文名称】校验输入

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._validate_inbound_auth` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        auth_header: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        activity: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not MSTEAMS_AVAILABLE:
            raise RuntimeError("PyJWT not installed. Run: pip install nanobot-ai[msteams]")

        if not auth_header.lower().startswith("bearer "):
            raise ValueError("missing bearer token")

        token = auth_header.split(" ", 1)[1].strip()
        if not token:
            raise ValueError("empty bearer token")

        header = jwt.get_unverified_header(token)
        kid = str(header.get("kid") or "").strip()
        if not kid:
            raise ValueError("missing token kid")

        jwks = await self._get_botframework_jwks()
        keys = jwks.get("keys") or []
        jwk = next((key for key in keys if key.get("kid") == kid), None)
        if not jwk:
            raise ValueError(f"signing key not found for kid={kid}")

        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
        claims = jwt.decode(
            token,
            key=public_key,
            algorithms=["RS256"],
            audience=self.config.app_id,
            issuer="https://api.botframework.com",
            options={
                "require": ["exp", "nbf", "iss", "aud"],
            },
        )

        claim_service_url = str(
            claims.get("serviceurl") or claims.get("serviceUrl") or "",
        ).strip()
        activity_service_url = str(activity.get("serviceUrl") or "").strip()
        if claim_service_url and activity_service_url and claim_service_url != activity_service_url:
            raise ValueError("serviceUrl claim mismatch")

    async def _get_botframework_openid_config(self) -> dict[str, Any]:
        """异步执行辅助逻辑（_get_botframework_openid_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._get_botframework_openid_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """

        now = time.time()
        if self._botframework_openid_config and now < self._botframework_openid_config_expires_at:
            return self._botframework_openid_config

        if not self._http:
            raise RuntimeError("MSTeams HTTP client not initialized")

        resp = await self._http.get(self._botframework_openid_config_url)
        resp.raise_for_status()
        self._botframework_openid_config = resp.json()
        self._botframework_openid_config_expires_at = now + 3600
        return self._botframework_openid_config

    async def _get_botframework_jwks(self) -> dict[str, Any]:
        """异步执行辅助逻辑（_get_botframework_jwks = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._get_botframework_jwks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """

        now = time.time()
        if self._botframework_jwks and now < self._botframework_jwks_expires_at:
            return self._botframework_jwks

        if not self._http:
            raise RuntimeError("MSTeams HTTP client not initialized")

        openid_config = await self._get_botframework_openid_config()
        jwks_uri = str(openid_config.get("jwks_uri") or "").strip()
        if not jwks_uri:
            raise RuntimeError("Bot Framework OpenID config missing jwks_uri")

        resp = await self._http.get(jwks_uri)
        resp.raise_for_status()
        self._botframework_jwks = resp.json()
        self._botframework_jwks_expires_at = now + 3600
        return self._botframework_jwks

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """执行辅助逻辑（_safe_float = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._safe_float` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            out = float(value)
            if out > 0:
                return out
        except (TypeError, ValueError):
            return None
        return None

    def _normalize_ref_record(self, value: Any) -> ConversationRef | None:
        """标准化数据（_normalize_ref_record = 原函数名）。

        【中文名称】标准化数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._normalize_ref_record` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        value: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not isinstance(value, dict):
            return None
        service_url = str(value.get("service_url") or "").strip()
        conversation_id = str(value.get("conversation_id") or "").strip()
        if not service_url or not conversation_id:
            return None
        return ConversationRef(
            service_url=service_url,
            conversation_id=conversation_id,
            bot_id=str(value.get("bot_id") or "") or None,
            activity_id=str(value.get("activity_id") or "") or None,
            conversation_type=str(value.get("conversation_type") or "") or None,
            tenant_id=str(value.get("tenant_id") or "") or None,
            updated_at=self._safe_float(value.get("updated_at")),
        )

    def _load_refs_raw(self) -> tuple[dict[str, Any], dict[str, Any], bool]:
        """加载数据（_load_refs_raw = 原函数名）。

        【中文名称】加载数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._load_refs_raw` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        main_data: dict[str, Any] = {}
        meta_data: dict[str, Any] = {}
        meta_exists = self._refs_meta_path.exists()

        if self._refs_path.exists():
            try:
                loaded = json.loads(self._refs_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    main_data = loaded
            except Exception as e:
                self.logger.warning("Failed to load conversation refs: {}", e)

        if meta_exists:
            try:
                loaded_meta = json.loads(self._refs_meta_path.read_text(encoding="utf-8"))
                if isinstance(loaded_meta, dict):
                    meta_data = loaded_meta
            except Exception as e:
                self.logger.warning("Failed to load conversation refs metadata: {}", e)

        return main_data, meta_data, meta_exists

    def _load_refs_from_disk(self) -> dict[str, ConversationRef]:
        """加载数据（_load_refs_from_disk = 原函数名）。

        【中文名称】加载数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._load_refs_from_disk` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        main_data, meta_data, meta_exists = self._load_refs_raw()
        if not main_data:
            return {}

        out: dict[str, ConversationRef] = {}
        now = time.time()
        for key, value in main_data.items():
            ref = self._normalize_ref_record(value)
            if not ref:
                continue

            meta_entry = meta_data.get(key) if isinstance(meta_data, dict) else None
            meta_ts = None
            if isinstance(meta_entry, dict):
                meta_ts = self._safe_float(meta_entry.get("updated_at"))
            elif meta_entry is not None:
                meta_ts = self._safe_float(meta_entry)

            if meta_ts is not None:
                ref.updated_at = meta_ts
            elif not meta_exists:
                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
                ref.updated_at = now
            elif ref.updated_at is None:
                ref.updated_at = now

            out[key] = ref
        return out

    def _load_refs(self) -> dict[str, ConversationRef]:
        """加载数据（_load_refs = 原函数名）。

        【中文名称】加载数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._load_refs` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return self._load_refs_from_disk()

    @contextmanager
    def _refs_file_lock(self):
        """执行辅助逻辑（_refs_file_lock = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._refs_file_lock` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._refs_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fp = self._refs_lock_path.open("a+", encoding="utf-8")
        try:
            if fcntl is not None:
                fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                if fcntl is not None:
                    fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)
            finally:
                lock_fp.close()

    def _is_webchat_service_url(self, service_url: str) -> bool:
        """判断条件是否成立（_is_webchat_service_url = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._is_webchat_service_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        service_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        normalized = service_url.strip()
        if not normalized:
            return False
        host = (urlparse(normalized).hostname or "").strip().lower()
        if host:
            return host == MSTEAMS_WEBCHAT_HOST or host.endswith(f".{MSTEAMS_WEBCHAT_HOST}")
        return MSTEAMS_WEBCHAT_HOST in normalized.lower()

    def _is_trusted_service_url(self, service_url: str) -> bool:
        """判断条件是否成立（_is_trusted_service_url = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._is_trusted_service_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        service_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        parsed = urlparse(service_url.strip())
        if parsed.scheme.lower() != "https":
            return False

        host = (parsed.hostname or "").strip().lower().rstrip(".")
        if not host:
            return False

        for pattern in self.config.trusted_service_url_hosts:
            trusted_host = str(pattern or "").strip().lower().rstrip(".")
            if not trusted_host:
                continue
            if trusted_host.startswith("*."):
                suffix = trusted_host[1:]
                if host.endswith(suffix) and host != suffix.lstrip("."):
                    return True
                continue
            if host == trusted_host:
                return True
        return False

    def _prune_conversation_refs(self, *, now: float | None = None) -> bool:
        """执行辅助逻辑（_prune_conversation_refs = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._prune_conversation_refs` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        now: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self._conversation_refs:
            return False

        now_ts = time.time() if now is None else now
        ttl_days = int(self.config.ref_ttl_days)
        stale_before = now_ts - (ttl_days * 24 * 60 * 60)
        keys_to_drop: list[str] = []

        for key, ref in self._conversation_refs.items():
            if not self._is_trusted_service_url(ref.service_url):
                keys_to_drop.append(key)
                continue

            if self.config.prune_web_chat_refs and self._is_webchat_service_url(ref.service_url):
                keys_to_drop.append(key)
                continue

            conv_type = str(ref.conversation_type or "").strip().lower()
            if self.config.prune_non_personal_refs and conv_type and conv_type != "personal":
                keys_to_drop.append(key)
                continue

            try:
                updated_at = float(ref.updated_at) if ref.updated_at is not None else 0.0
            except (TypeError, ValueError):
                updated_at = 0.0
            if updated_at <= 0 or updated_at < stale_before:
                keys_to_drop.append(key)

        if not keys_to_drop:
            return False

        for key in keys_to_drop:
            self._conversation_refs.pop(key, None)
        self.logger.info(
            "Pruned {} stale/unsupported conversation refs (ttl={} days)",
            len(keys_to_drop),
            ttl_days,
        )
        return True

    def _merge_refs_from_disk_locked(self) -> None:
        """合并内容（_merge_refs_from_disk_locked = 原函数名）。

        【中文名称】合并内容

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._merge_refs_from_disk_locked` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        disk_refs = self._load_refs_from_disk()
        for key, disk_ref in disk_refs.items():
            mem_ref = self._conversation_refs.get(key)
            if mem_ref is None:
                self._conversation_refs[key] = disk_ref
                continue
            disk_ts = self._safe_float(disk_ref.updated_at) or 0.0
            mem_ts = self._safe_float(mem_ref.updated_at) or 0.0
            if disk_ts > mem_ts:
                self._conversation_refs[key] = disk_ref

    def _touch_conversation_ref(self, chat_id: str, *, persist: bool = False) -> None:
        """执行辅助逻辑（_touch_conversation_ref = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._touch_conversation_ref` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        persist: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        with self._refs_guard:
            ref = self._conversation_refs.get(str(chat_id))
            if not ref:
                return
            now = time.time()
            prev = self._safe_float(ref.updated_at) or 0.0
            min_interval = max(0, int(self.config.ref_touch_interval_s))
            if min_interval > 0 and prev > 0 and now - prev < min_interval:
                return
            ref.updated_at = now
            if persist:
                self._save_refs_locked()

    def _write_json_atomically(self, path, data: dict[str, Any]) -> None:
        """执行辅助逻辑（_write_json_atomically = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._write_json_atomically` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        path: 文件或路径信息，代码会按安全边界读取或写入。
        data: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        payload = json.dumps(data, indent=2)
        tmp_path: str | None = None
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=str(path.parent),
                prefix=f"{path.name}.",
                suffix=".tmp",
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                with suppress(OSError):
                    os.unlink(tmp_path)

    def _save_refs_locked(self, *, prune: bool = True) -> None:
        """保存数据（_save_refs_locked = 原函数名）。

        【中文名称】保存数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._save_refs_locked` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        prune: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            with self._refs_file_lock():
                self._merge_refs_from_disk_locked()
                if prune:
                    self._prune_conversation_refs()
                refs_data = {
                    key: {
                        "service_url": ref.service_url,
                        "conversation_id": ref.conversation_id,
                        "bot_id": ref.bot_id,
                        "activity_id": ref.activity_id,
                        "conversation_type": ref.conversation_type,
                        "tenant_id": ref.tenant_id,
                    }
                    for key, ref in self._conversation_refs.items()
                }
                refs_meta = {
                    key: {
                        "updated_at": self._safe_float(ref.updated_at),
                    }
                    for key, ref in self._conversation_refs.items()
                }
                self._write_json_atomically(self._refs_path, refs_data)
                self._write_json_atomically(self._refs_meta_path, refs_meta)
        except Exception as e:
            self.logger.warning("Failed to save conversation refs: {}", e)

    def _save_refs(self, *, prune: bool = True) -> None:
        """保存数据（_save_refs = 原函数名）。

        【中文名称】保存数据

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._save_refs` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        prune: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        with self._refs_guard:
            self._save_refs_locked(prune=prune)

    async def _get_access_token(self) -> str:
        """异步执行辅助逻辑（_get_access_token = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Microsoft Teams 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `MSTeamsChannel._get_access_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """

        now = time.time()
        if self._token and now < self._token_expires_at - 60:
            return self._token

        if not self._http:
            raise RuntimeError("MSTeams HTTP client not initialized")

        tenant = (self.config.tenant_id or "").strip() or "botframework.com"
        token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.app_id,
            "client_secret": self.config.app_password,
            "scope": "https://api.botframework.com/.default",
        }
        resp = await self._http.post(token_url, data=data)
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expires_at = now + int(payload.get("expires_in", 3600))
        return self._token

