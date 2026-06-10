"""邮件渠道适配器。

【中文名称】邮件渠道适配器

【功能说明】
负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

import asyncio
import html
import imaplib
import mimetypes
import re
import smtplib
import ssl
from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import parseaddr
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.utils.helpers import safe_filename


class EmailConfig(Base):
    """EmailConfig 类。

    【中文名称】EmailConfig

    【功能说明】
    这是 邮件渠道适配器 中的核心数据结构或服务类。负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    consent_granted: bool = False

    imap_host: str = ""
    imap_port: int = 993
    imap_username: str = ""
    imap_password: str = ""
    imap_mailbox: str = "INBOX"
    imap_use_ssl: bool = True

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False
    from_address: str = ""

    auto_reply_enabled: bool = True
    poll_interval_seconds: int = 30
    mark_seen: bool = True
    post_action: Literal["delete", "move"] | None = None
    post_action_move_mailbox: str | None = None
    post_action_expunge: bool = False
    post_action_ignore_skipped: bool = True
    max_body_chars: int = 12000
    subject_prefix: str = "Re: "
    allow_from: list[str] = Field(default_factory=list)

    # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    verify_dkim: bool = True   # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    verify_spf: bool = True    # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。

    # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    allowed_attachment_types: list[str] = Field(default_factory=list)
    max_attachment_size: int = 2_000_000  # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    max_attachments_per_email: int = 5


@dataclass
class _ServerFeatures:
    """_ServerFeatures 类。

    【中文名称】_ServerFeatures

    【功能说明】
    这是 邮件渠道适配器 中的核心数据结构或服务类。负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    move: bool
    uidplus: bool
    uid_store: bool | None = None


class EmailChannel(BaseChannel):
    """EmailChannel 类。

    【中文名称】EmailChannel

    【功能说明】
    这是 邮件渠道适配器 中的核心数据结构或服务类。负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "email"
    display_name = "Email"
    _IMAP_MONTHS = (
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    )
    _IMAP_RECONNECT_MARKERS = (
        "disconnected for inactivity",
        "eof occurred in violation of protocol",
        "socket error",
        "connection reset",
        "broken pipe",
        "bye",
    )
    _IMAP_MISSING_MAILBOX_MARKERS = (
        "mailbox doesn't exist",
        "select failed",
        "no such mailbox",
        "can't open mailbox",
        "does not exist",
    )

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return EmailConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = EmailConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: EmailConfig = config
        self._self_addresses = self._collect_self_addresses()
        self._last_subject_by_chat: dict[str, str] = {}
        self._last_message_id_by_chat: dict[str, str] = {}
        self._processed_uids: set[str] = set()  # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        self._MAX_PROCESSED_UIDS = 100000

    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.config.consent_granted:
            self.logger.warning(
                "Email channel disabled: consent_granted is false. "
                "Set channels.email.consentGranted=true after explicit user permission."
            )
            return

        if not self._validate_config():
            return

        self._running = True
        if not self.config.verify_dkim and not self.config.verify_spf:
            self.logger.warning(
                "DKIM and SPF verification are both DISABLED. "
                "Emails with spoofed From headers will be accepted. "
                "Set verify_dkim=true and verify_spf=true for anti-spoofing protection."
            )
        self.logger.info("Starting Email channel (IMAP polling mode)...")

        poll_seconds = max(5, int(self.config.poll_interval_seconds))
        while self._running:
            try:
                inbound_items, skipped_uids = await asyncio.to_thread(self._fetch_new_messages)
                should_apply_post_action = self._should_apply_post_action()
                post_actions_uids: set[str] = set()
                for item in inbound_items:
                    sender = item["sender"]
                    subject = item.get("subject", "")
                    message_id = item.get("message_id", "")

                    if subject:
                        self._last_subject_by_chat[sender] = subject
                    if message_id:
                        self._last_message_id_by_chat[sender] = message_id

                    try:
                        await self._handle_message(
                            sender_id=sender,
                            chat_id=sender,
                            content=item["content"],
                            media=item.get("media") or None,
                            metadata=item.get("metadata", {}),
                        )
                    except Exception:
                        self.logger.exception("Error delivering email from {}", sender)
                        continue

                    uid = str((item.get("metadata") or {}).get("uid") or "")
                    if uid and should_apply_post_action:
                        post_actions_uids.add(uid)

                if should_apply_post_action and not self.config.post_action_ignore_skipped:
                    post_actions_uids.update(skipped_uids)

                if post_actions_uids:
                    await asyncio.to_thread(self._apply_post_actions_batch, sorted(post_actions_uids))
            except Exception:
                self.logger.exception("Polling error")

            await asyncio.sleep(poll_seconds)

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.config.consent_granted:
            self.logger.warning("Skip email send: consent_granted is false")
            return

        if not self.config.smtp_host:
            self.logger.warning("SMTP host not configured")
            return

        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if (msg.metadata or {}).get("_progress"):
            self.logger.debug("Skip progress message to {}", msg.chat_id)
            return

        to_addr = msg.chat_id.strip()
        if not to_addr:
            self.logger.warning("Missing recipient address")
            return

        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        is_reply = to_addr in self._last_subject_by_chat
        force_send = bool((msg.metadata or {}).get("force_send"))

        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        if is_reply and not self.config.auto_reply_enabled and not force_send:
            self.logger.info("Skip automatic reply to {}: auto_reply_enabled is false", to_addr)
            return

        base_subject = self._last_subject_by_chat.get(to_addr, "nanobot reply")
        subject = self._reply_subject(base_subject)
        if msg.metadata and isinstance(msg.metadata.get("subject"), str):
            override = msg.metadata["subject"].strip()
            if override:
                subject = override

        attachments: list[tuple[bytes, str, str, str]] = []
        failed_attachments: list[str] = []
        max_attachment_size = max(0, int(self.config.max_attachment_size))
        max_attachment_count = max(0, int(self.config.max_attachments_per_email))
        for media_path in msg.media or []:
            path = Path(media_path)
            filename = path.name or "attachment"
            if len(attachments) >= max_attachment_count:
                failed_attachments.append(f"[attachment: {filename} - too many attachments]")
                self.logger.warning("Attachment count limit reached, skipping: {}", media_path)
                continue
            if not path.is_file():
                failed_attachments.append(f"[attachment: {filename} - send failed]")
                self.logger.warning("Attachment not found, skipping: {}", media_path)
                continue
            try:
                size = path.stat().st_size
                if max_attachment_size <= 0 or size > max_attachment_size:
                    failed_attachments.append(f"[attachment: {filename} - too large]")
                    self.logger.warning(
                        "Attachment too large, skipping: {} ({} > {} bytes)",
                        media_path,
                        size,
                        max_attachment_size,
                    )
                    continue
                data = path.read_bytes()
                ctype, _ = mimetypes.guess_type(str(path))
                if ctype is None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)
                attachments.append((data, maintype, subtype, filename))
                self.logger.info("Attached file: {}", filename)
            except Exception:
                failed_attachments.append(f"[attachment: {filename} - send failed]")
                self.logger.exception("Failed to attach file {}", media_path)

        content = msg.content or ""
        if failed_attachments:
            fallback = "\n".join(failed_attachments)
            content = f"{content.rstrip()}\n\n{fallback}" if content.strip() else fallback

        email_msg = EmailMessage()
        email_msg["From"] = self.config.from_address or self.config.smtp_username or self.config.imap_username
        email_msg["To"] = to_addr
        email_msg["Subject"] = subject
        email_msg.set_content(content)

        for data, maintype, subtype, filename in attachments:
            email_msg.add_attachment(
                data,
                maintype=maintype,
                subtype=subtype,
                filename=filename,
            )

        in_reply_to = self._last_message_id_by_chat.get(to_addr)
        if in_reply_to:
            email_msg["In-Reply-To"] = in_reply_to
            email_msg["References"] = in_reply_to

        try:
            await asyncio.to_thread(self._smtp_send, email_msg)
        except Exception:
            self.logger.exception("Error sending to {}", to_addr)
            raise

    def _validate_config(self) -> bool:
        """执行 `_validate_config`。

        【中文名称】_validate_config

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        missing = []
        if not self.config.imap_host:
            missing.append("imap_host")
        if not self.config.imap_username:
            missing.append("imap_username")
        if not self.config.imap_password:
            missing.append("imap_password")
        if not self.config.smtp_host:
            missing.append("smtp_host")
        if not self.config.smtp_username:
            missing.append("smtp_username")
        if not self.config.smtp_password:
            missing.append("smtp_password")

        if self.config.post_action == "move" and not (self.config.post_action_move_mailbox or "").strip():
            missing.append("post_action_move_mailbox")

        if missing:
            self.logger.error("Channel not configured, missing: {}", ', '.join(missing))
            return False
        return True

    def _smtp_send(self, msg: EmailMessage) -> None:
        """执行 `_smtp_send`。

        【中文名称】_smtp_send

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        timeout = 30
        if self.config.smtp_use_ssl:
            with smtplib.SMTP_SSL(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=timeout,
            ) as smtp:
                smtp.login(self.config.smtp_username, self.config.smtp_password)
                smtp.send_message(msg)
            return

        with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port, timeout=timeout) as smtp:
            if self.config.smtp_use_tls:
                smtp.starttls(context=ssl.create_default_context())
            smtp.login(self.config.smtp_username, self.config.smtp_password)
            smtp.send_message(msg)

    def _fetch_new_messages(self) -> tuple[list[dict[str, Any]], set[str]]:
        """执行 `_fetch_new_messages`。

        【中文名称】_fetch_new_messages

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return self._fetch_messages(
            search_criteria=("UNSEEN",),
            mark_seen=self.config.mark_seen,
            dedupe=True,
            limit=0,
        )

    def fetch_messages_between_dates(
        self,
        start_date: date,
        end_date: date,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """执行 `fetch_messages_between_dates`。

        【中文名称】fetch_messages_between_dates

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - start_date: 调用方传入的 `start_date` 数据；具体类型以函数签名为准。
        - end_date: 调用方传入的 `end_date` 数据；具体类型以函数签名为准。
        - limit: 调用方传入的 `limit` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if end_date <= start_date:
            return []

        messages, _ = self._fetch_messages(
            search_criteria=(
                "SINCE",
                self._format_imap_date(start_date),
                "BEFORE",
                self._format_imap_date(end_date),
            ),
            mark_seen=False,
            dedupe=False,
            limit=max(1, int(limit)),
        )
        return messages

    def _fetch_messages(
        self,
        search_criteria: tuple[str, ...],
        mark_seen: bool,
        dedupe: bool,
        limit: int,
    ) -> tuple[list[dict[str, Any]], set[str]]:
        """执行 `_fetch_messages`。

        【中文名称】_fetch_messages

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - search_criteria: 调用方传入的 `search_criteria` 数据；具体类型以函数签名为准。
        - mark_seen: 调用方传入的 `mark_seen` 数据；具体类型以函数签名为准。
        - dedupe: 调用方传入的 `dedupe` 数据；具体类型以函数签名为准。
        - limit: 调用方传入的 `limit` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        messages: list[dict[str, Any]] = []
        skipped_uids: set[str] = set()
        cycle_uids: set[str] = set()

        for attempt in range(2):
            try:
                self._fetch_messages_once(
                    search_criteria,
                    mark_seen,
                    dedupe,
                    limit,
                    messages,
                    skipped_uids,
                    cycle_uids,
                )
                return messages, skipped_uids
            except Exception as exc:
                if attempt == 1 or not self._is_stale_imap_error(exc):
                    raise
                self.logger.warning("IMAP connection went stale, retrying once: {}", exc)

        return messages, skipped_uids

    def _fetch_messages_once(
        self,
        search_criteria: tuple[str, ...],
        mark_seen: bool,
        dedupe: bool,
        limit: int,
        messages: list[dict[str, Any]],
        skipped_uids: set[str],
        cycle_uids: set[str],
    ) -> None:
        """执行 `_fetch_messages_once`。

        【中文名称】_fetch_messages_once

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - search_criteria: 调用方传入的 `search_criteria` 数据；具体类型以函数签名为准。
        - mark_seen: 调用方传入的 `mark_seen` 数据；具体类型以函数签名为准。
        - dedupe: 调用方传入的 `dedupe` 数据；具体类型以函数签名为准。
        - limit: 调用方传入的 `limit` 数据；具体类型以函数签名为准。
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - skipped_uids: 调用方传入的 `skipped_uids` 数据；具体类型以函数签名为准。
        - cycle_uids: 调用方传入的 `cycle_uids` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        mailbox = self.config.imap_mailbox or "INBOX"

        client = self._open_imap_client(mailbox=mailbox, missing_mailbox_ok=True)
        if client is None:
            return messages

        try:
            status, data = client.search(None, *search_criteria)
            if status != "OK" or not data:
                return messages

            ids = data[0].split()
            if limit > 0 and len(ids) > limit:
                ids = ids[-limit:]
            for imap_id in ids:
                status, fetched = client.fetch(imap_id, "(BODY.PEEK[] UID)")
                if status != "OK" or not fetched:
                    continue

                raw_bytes = self._extract_message_bytes(fetched)
                if raw_bytes is None:
                    continue

                uid = self._extract_uid(fetched)
                if uid and uid in cycle_uids:
                    continue
                if dedupe and uid and uid in self._processed_uids:
                    continue

                parsed = BytesParser(policy=policy.default).parsebytes(raw_bytes)
                sender = parseaddr(parsed.get("From", ""))[1].strip().lower()
                if not sender:
                    continue
                if self._is_self_address(sender):
                    self.logger.info("From {} ignored: matches bot-owned address", sender)
                    self._remember_processed_uid(uid, dedupe, cycle_uids)
                    if mark_seen:
                        client.store(imap_id, "+FLAGS", "\\Seen")
                    if uid:
                        skipped_uids.add(uid)
                    continue

                # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                spf_pass, dkim_pass = self._check_authentication_results(parsed)
                if self.config.verify_spf and not spf_pass:
                    self.logger.warning(
                        "From {} rejected: SPF verification failed "
                        "(no 'spf=pass' in Authentication-Results header)",
                        sender,
                    )
                    self._remember_processed_uid(uid, dedupe, cycle_uids)
                    if uid:
                        skipped_uids.add(uid)
                    continue
                if self.config.verify_dkim and not dkim_pass:
                    self.logger.warning(
                        "From {} rejected: DKIM verification failed "
                        "(no 'dkim=pass' in Authentication-Results header)",
                        sender,
                    )
                    self._remember_processed_uid(uid, dedupe, cycle_uids)
                    if uid:
                        skipped_uids.add(uid)
                    continue

                if not self.is_allowed(sender):
                    self._remember_processed_uid(uid, dedupe, cycle_uids)
                    if mark_seen:
                        client.store(imap_id, "+FLAGS", "\\Seen")
                    if uid:
                        skipped_uids.add(uid)
                    continue

                subject = self._decode_header_value(parsed.get("Subject", ""))
                date_value = parsed.get("Date", "")
                message_id = parsed.get("Message-ID", "").strip()
                body = self._extract_text_body(parsed)

                if not body:
                    body = "(empty email body)"

                body = body[: self.config.max_body_chars]
                content = (
                    f"[EMAIL-CONTEXT] Email received.\n"
                    f"From: {sender}\n"
                    f"Subject: {subject}\n"
                    f"Date: {date_value}\n\n"
                    f"{body}"
                )

                # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                attachment_paths: list[str] = []
                if self.config.allowed_attachment_types:
                    saved = self._extract_attachments(
                        parsed,
                        uid or "noid",
                        allowed_types=self.config.allowed_attachment_types,
                        max_size=self.config.max_attachment_size,
                        max_count=self.config.max_attachments_per_email,
                    )
                    for p in saved:
                        attachment_paths.append(str(p))
                        content += f"\n[attachment: {p.name} — saved to {p}]"

                metadata = {
                    "message_id": message_id,
                    "subject": subject,
                    "date": date_value,
                    "sender_email": sender,
                    "uid": uid,
                }
                messages.append(
                    {
                        "sender": sender,
                        "subject": subject,
                        "message_id": message_id,
                        "content": content,
                        "metadata": metadata,
                        "media": attachment_paths,
                    }
                )

                self._remember_processed_uid(uid, dedupe, cycle_uids)

                if mark_seen:
                    client.store(imap_id, "+FLAGS", "\\Seen")
        finally:
            self._close_imap_client(client)

    def _open_imap_client(self, mailbox: str, *, missing_mailbox_ok: bool = False) -> Any | None:
        """执行 `_open_imap_client`。

        【中文名称】_open_imap_client

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - mailbox: 调用方传入的 `mailbox` 数据；具体类型以函数签名为准。
        - missing_mailbox_ok: 调用方传入的 `missing_mailbox_ok` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if self.config.imap_use_ssl:
            client: Any = imaplib.IMAP4_SSL(self.config.imap_host, self.config.imap_port)
        else:
            client = imaplib.IMAP4(self.config.imap_host, self.config.imap_port)

        try:
            client.login(self.config.imap_username, self.config.imap_password)
            try:
                status, _ = client.select(mailbox)
            except Exception as exc:
                if missing_mailbox_ok and self._is_missing_mailbox_error(exc):
                    self.logger.warning("Mailbox unavailable, skipping poll for {}: {}", mailbox, exc)
                    self._close_imap_client(client)
                    return None
                raise

            if status != "OK":
                self.logger.warning("Mailbox select returned {}, skipping poll for {}", status, mailbox)
                self._close_imap_client(client)
                return None
        except Exception:
            self._close_imap_client(client)
            raise

        return client

    @staticmethod
    def _close_imap_client(client: Any) -> None:
        """执行 `_close_imap_client`。

        【中文名称】_close_imap_client

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        with suppress(Exception):
            client.logout()

    def _collect_self_addresses(self) -> set[str]:
        """执行 `_collect_self_addresses`。

        【中文名称】_collect_self_addresses

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        candidates = (
            self.config.from_address,
            self.config.smtp_username,
            self.config.imap_username,
        )
        normalized = {
            addr
            for candidate in candidates
            if (addr := self._normalize_address(candidate))
        }
        return normalized

    @staticmethod
    def _normalize_address(value: str) -> str:
        """执行 `_normalize_address`。

        【中文名称】_normalize_address

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        raw = (value or "").strip()
        if not raw:
            return ""
        parsed = parseaddr(raw)[1].strip().lower()
        if parsed:
            return parsed
        if "@" in raw:
            return raw.lower()
        return ""

    def _is_self_address(self, sender: str) -> bool:
        """执行 `_is_self_address`。

        【中文名称】_is_self_address

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender: 调用方传入的 `sender` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        normalized_sender = self._normalize_address(sender)
        return bool(normalized_sender) and normalized_sender in self._self_addresses

    def _remember_processed_uid(self, uid: str, dedupe: bool, cycle_uids: set[str]) -> None:
        """执行 `_remember_processed_uid`。

        【中文名称】_remember_processed_uid

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。
        - dedupe: 调用方传入的 `dedupe` 数据；具体类型以函数签名为准。
        - cycle_uids: 调用方传入的 `cycle_uids` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not uid:
            return
        cycle_uids.add(uid)
        if dedupe:
            self._processed_uids.add(uid)
            # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            if len(self._processed_uids) > self._MAX_PROCESSED_UIDS:
                # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                self._processed_uids = set(list(self._processed_uids)[len(self._processed_uids) // 2:])

    def _should_apply_post_action(self) -> bool:
        """执行 `_should_apply_post_action`。

        【中文名称】_should_apply_post_action

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self.config.post_action in {"delete", "move"}

    def _apply_post_actions_batch(self, post_actions_uids: list[str]) -> None:
        """执行 `_apply_post_actions_batch`。

        【中文名称】_apply_post_actions_batch

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - post_actions_uids: 调用方传入的 `post_actions_uids` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not self._should_apply_post_action() or not post_actions_uids:
            return

        mailbox = self.config.imap_mailbox or "INBOX"
        client = self._open_imap_client(mailbox=mailbox)
        if client is None:
            return

        try:
            features = self._server_features(client)
            # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            for uid in post_actions_uids:
                if uid:
                    self._apply_post_action(client, uid, features)
        finally:
            self._close_imap_client(client)

    def _apply_post_action(
        self,
        client: Any,
        uid: str,
        features: _ServerFeatures,
    ) -> None:
        """执行 `_apply_post_action`。

        【中文名称】_apply_post_action

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。
        - features: 调用方传入的 `features` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        action = self.config.post_action

        if action == "delete":
            if not self._uid_store_deleted(client, uid, features):
                return
            self._uid_expunge_or_fallback(client, uid, features)
            return

        if action == "move":
            target = (self.config.post_action_move_mailbox or "").strip()
            if features.move:
                status, _ = client.uid("MOVE", uid, target)
                if status != "OK":
                    self.logger.warning("Post-action move failed (UID MOVE) for UID {} to mailbox {}", uid, target)
                return

            status, _ = client.uid("COPY", uid, target)
            if status != "OK":
                self.logger.warning("Post-action move failed (UID COPY) for UID {} to mailbox {}", uid, target)
                return
            if not self._uid_store_deleted(client, uid, features):
                return
            self._uid_expunge_or_fallback(client, uid, features)

    @staticmethod
    def _server_features(client: Any) -> _ServerFeatures:
        """执行 `_server_features`。

        【中文名称】_server_features

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        caps: set[str] = set()
        with suppress(Exception):
            status, data = client.capability()
            if status == "OK" and data:
                for raw in data:
                    if isinstance(raw, (bytes, bytearray)):
                        caps.update(token.upper() for token in raw.decode("utf-8", errors="ignore").split())
                    elif isinstance(raw, str):
                        caps.update(token.upper() for token in raw.split())
        return _ServerFeatures(move="MOVE" in caps, uidplus="UIDPLUS" in caps)

    @staticmethod
    def _lookup_imap_id_by_uid(client: Any, uid: str) -> bytes | None:
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_lookup_imap_id_by_uid`。

        【中文名称】_lookup_imap_id_by_uid

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        status, data = client.search(None, "UID", uid)
        if status != "OK" or not data or not data[0]:
            return None
        return data[0].split()[0]

    def _uid_store_deleted(self, client: Any, uid: str, features: _ServerFeatures) -> bool:
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_uid_store_deleted`。

        【中文名称】_uid_store_deleted

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。
        - features: 调用方传入的 `features` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if features.uid_store is not False:
            status, _ = client.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
            if status == "OK":
                features.uid_store = True
                return True
            features.uid_store = False

        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        imap_id = self._lookup_imap_id_by_uid(client, uid)
        if not imap_id:
            self.logger.warning("Post-action skipped: UID {} not found", uid)
            return False

        status, _ = client.store(imap_id, "+FLAGS", "\\Deleted")
        if status != "OK":
            self.logger.warning("Post-action failed: could not mark UID {} as deleted", uid)
            return False
        return True

    def _uid_expunge_or_fallback(self, client: Any, uid: str, features: _ServerFeatures) -> None:
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        # 说明：这里处理 邮件渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        """执行 `_uid_expunge_or_fallback`。

        【中文名称】_uid_expunge_or_fallback

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。
        - features: 调用方传入的 `features` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if features.uidplus:
            status, _ = client.uid("EXPUNGE", uid)
            if status == "OK":
                return
            self.logger.warning("UID EXPUNGE failed for UID {}, falling back to EXPUNGE", uid)
        if self.config.post_action_expunge:
            client.expunge()

    @classmethod
    def _is_stale_imap_error(cls, exc: Exception) -> bool:
        """执行 `_is_stale_imap_error`。

        【中文名称】_is_stale_imap_error

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        message = str(exc).lower()
        return any(marker in message for marker in cls._IMAP_RECONNECT_MARKERS)

    @classmethod
    def _is_missing_mailbox_error(cls, exc: Exception) -> bool:
        """执行 `_is_missing_mailbox_error`。

        【中文名称】_is_missing_mailbox_error

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - exc: 调用方传入的 `exc` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        message = str(exc).lower()
        return any(marker in message for marker in cls._IMAP_MISSING_MAILBOX_MARKERS)

    @classmethod
    def _format_imap_date(cls, value: date) -> str:
        """执行 `_format_imap_date`。

        【中文名称】_format_imap_date

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        month = cls._IMAP_MONTHS[value.month - 1]
        return f"{value.day:02d}-{month}-{value.year}"

    @staticmethod
    def _extract_message_bytes(fetched: list[Any]) -> bytes | None:
        """执行 `_extract_message_bytes`。

        【中文名称】_extract_message_bytes

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - fetched: 调用方传入的 `fetched` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        for item in fetched:
            if isinstance(item, tuple) and len(item) >= 2 and isinstance(item[1], (bytes, bytearray)):
                return bytes(item[1])
        return None

    @staticmethod
    def _extract_uid(fetched: list[Any]) -> str:
        """执行 `_extract_uid`。

        【中文名称】_extract_uid

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - fetched: 调用方传入的 `fetched` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        for item in fetched:
            if isinstance(item, tuple) and item and isinstance(item[0], (bytes, bytearray)):
                head = bytes(item[0]).decode("utf-8", errors="ignore")
                m = re.search(r"UID\s+(\d+)", head)
                if m:
                    return m.group(1)
        return ""

    @staticmethod
    def _decode_header_value(value: str) -> str:
        """执行 `_decode_header_value`。

        【中文名称】_decode_header_value

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not value:
            return ""
        try:
            return str(make_header(decode_header(value)))
        except Exception:
            return value

    @classmethod
    def _extract_text_body(cls, msg: Any) -> str:
        """执行 `_extract_text_body`。

        【中文名称】_extract_text_body

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if msg.is_multipart():
            plain_parts: list[str] = []
            html_parts: list[str] = []
            for part in msg.walk():
                if part.get_content_disposition() == "attachment":
                    continue
                content_type = part.get_content_type()
                try:
                    payload = part.get_content()
                except Exception:
                    payload_bytes = part.get_payload(decode=True) or b""
                    charset = part.get_content_charset() or "utf-8"
                    payload = payload_bytes.decode(charset, errors="replace")
                if not isinstance(payload, str):
                    continue
                if content_type == "text/plain":
                    plain_parts.append(payload)
                elif content_type == "text/html":
                    html_parts.append(payload)
            if plain_parts:
                return "\n\n".join(plain_parts).strip()
            if html_parts:
                return cls._html_to_text("\n\n".join(html_parts)).strip()
            return ""

        try:
            payload = msg.get_content()
        except Exception:
            payload_bytes = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            payload = payload_bytes.decode(charset, errors="replace")
        if not isinstance(payload, str):
            return ""
        if msg.get_content_type() == "text/html":
            return cls._html_to_text(payload).strip()
        return payload.strip()

    @staticmethod
    def _check_authentication_results(parsed_msg: Any) -> tuple[bool, bool]:
        """执行 `_check_authentication_results`。

        【中文名称】_check_authentication_results

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - parsed_msg: 调用方传入的 `parsed_msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        spf_pass = False
        dkim_pass = False
        for ar_header in parsed_msg.get_all("Authentication-Results") or []:
            ar_lower = ar_header.lower()
            if re.search(r"\bspf\s*=\s*pass\b", ar_lower):
                spf_pass = True
            if re.search(r"\bdkim\s*=\s*pass\b", ar_lower):
                dkim_pass = True
        return spf_pass, dkim_pass

    @classmethod
    def _extract_attachments(
        cls,
        msg: Any,
        uid: str,
        *,
        allowed_types: list[str],
        max_size: int,
        max_count: int,
    ) -> list[Path]:
        """执行 `_extract_attachments`。

        【中文名称】_extract_attachments

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。
        - uid: 调用方传入的 `uid` 数据；具体类型以函数签名为准。
        - allowed_types: 调用方传入的 `allowed_types` 数据；具体类型以函数签名为准。
        - max_size: 调用方传入的 `max_size` 数据；具体类型以函数签名为准。
        - max_count: 调用方传入的 `max_count` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not msg.is_multipart():
            return []

        saved: list[Path] = []
        media_dir = get_media_dir("email")

        for part in msg.walk():
            if len(saved) >= max_count:
                break
            if part.get_content_disposition() != "attachment":
                continue

            content_type = part.get_content_type()
            if not any(fnmatch(content_type, pat) for pat in allowed_types):
                logger.debug("Attachment skipped (type {}): not in allowed list", content_type)
                continue

            payload = part.get_payload(decode=True)
            if payload is None:
                continue
            if len(payload) > max_size:
                logger.warning(
                    "Attachment skipped: size {} exceeds limit {}",
                    len(payload),
                    max_size,
                )
                continue

            raw_name = part.get_filename() or "attachment"
            sanitized = safe_filename(raw_name) or "attachment"
            dest = media_dir / f"{uid}_{sanitized}"

            try:
                dest.write_bytes(payload)
                saved.append(dest)
                logger.info("Attachment saved: {}", dest)
            except Exception as exc:
                logger.warning("Failed to save attachment {}: {}", dest, exc)

        return saved

    @staticmethod
    def _html_to_text(raw_html: str) -> str:
        """执行 `_html_to_text`。

        【中文名称】_html_to_text

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - raw_html: 调用方传入的 `raw_html` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        text = re.sub(r"<\s*br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
        text = re.sub(r"<\s*/\s*p\s*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        return html.unescape(text)

    def _reply_subject(self, base_subject: str) -> str:
        """执行 `_reply_subject`。

        【中文名称】_reply_subject

        【功能说明】
        这是 邮件渠道适配器 中的一个步骤函数，用来支撑：负责通过 IMAP/SMTP 轮询邮件、提取正文和附件、构造会话键，并把 Agent 回复以邮件形式发回原发件人。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - base_subject: 调用方传入的 `base_subject` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        subject = (base_subject or "").strip() or "nanobot reply"
        prefix = self.config.subject_prefix or "Re: "
        if subject.lower().startswith("re:"):
            return subject
        return f"{prefix}{subject}"
