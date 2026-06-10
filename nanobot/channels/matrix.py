"""Matrix 渠道适配器。

【中文名称】Matrix 渠道适配器

【功能说明】
负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。

【在整体架构中的位置】
该文件属于 P1 范围的渠道适配器代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

import asyncio
import json
import mimetypes
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypeAlias
from urllib.parse import quote, urlparse

from pydantic import Field

from nanobot.security.workspace_policy import is_path_within

try:
    import aiohttp
    import nh3
    from mistune import create_markdown
    from nio import (
        AsyncClient,
        AsyncClientConfig,
        InviteEvent,
        JoinError,
        KeyVerificationCancel,
        KeyVerificationEvent,
        KeyVerificationKey,
        KeyVerificationMac,
        KeyVerificationStart,
        LoginResponse,
        MatrixRoom,
        RoomEncryptedMedia,
        RoomMessage,
        RoomMessageMedia,
        RoomMessageText,
        RoomSendError,
        RoomSendResponse,
        RoomTypingError,
        SyncError,
        ToDeviceError,
        UploadError,
    )
    from nio.crypto.attachments import decrypt_attachment
    from nio.exceptions import EncryptionError
except ImportError as e:
    raise ImportError(
        "Matrix dependencies not installed. Run: pip install nanobot-ai[matrix]"
    ) from e

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_data_dir, get_media_dir
from nanobot.config.schema import Base
from nanobot.utils.helpers import safe_filename
from nanobot.utils.logging_bridge import redirect_lib_logging

TYPING_NOTICE_TIMEOUT_MS = 30_000
# 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
TYPING_KEEPALIVE_INTERVAL_MS = 20_000
MATRIX_HTML_FORMAT = "org.matrix.custom.html"
_ATTACH_MARKER = "[attachment: {}]"
_ATTACH_TOO_LARGE = "[attachment: {} - too large]"
_ATTACH_FAILED = "[attachment: {} - download failed]"
_ATTACH_UPLOAD_FAILED = "[attachment: {} - upload failed]"
_DEFAULT_ATTACH_NAME = "attachment"
_MSGTYPE_MAP = {"m.image": "image", "m.audio": "audio", "m.video": "video", "m.file": "file"}

MATRIX_MEDIA_EVENT_FILTER = (RoomMessageMedia, RoomEncryptedMedia)
MatrixMediaEvent: TypeAlias = RoomMessageMedia | RoomEncryptedMedia


class _MediaTooLargeError(Exception):
    """_MediaTooLargeError 类。

    【中文名称】_MediaTooLargeError

    【功能说明】
    这是 Matrix 渠道适配器 中的核心数据结构或服务类。负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

MATRIX_MARKDOWN = create_markdown(
    escape=True,
    plugins=["table", "strikethrough", "url", "superscript", "subscript"],
)

MATRIX_ALLOWED_HTML_TAGS = {
    "p", "a", "strong", "em", "del", "code", "pre", "blockquote",
    "ul", "ol", "li", "h1", "h2", "h3", "h4", "h5", "h6",
    "hr", "br", "table", "thead", "tbody", "tr", "th", "td",
    "caption", "sup", "sub", "img",
}
MATRIX_ALLOWED_HTML_ATTRIBUTES: dict[str, set[str]] = {
    "a": {"href"}, "code": {"class"}, "ol": {"start"},
    "img": {"src", "alt", "title", "width", "height"},
}
MATRIX_ALLOWED_URL_SCHEMES = {"https", "http", "matrix", "mailto", "mxc"}


def _filter_matrix_html_attribute(tag: str, attr: str, value: str) -> str | None:
    """执行 `_filter_matrix_html_attribute`。

    【中文名称】_filter_matrix_html_attribute

    【功能说明】
    这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - tag: 调用方传入的 `tag` 数据；具体类型以函数签名为准。
    - attr: 调用方传入的 `attr` 数据；具体类型以函数签名为准。
    - value: 调用方传入的 `value` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if tag == "a" and attr == "href":
        return value if value.lower().startswith(("https://", "http://", "matrix:", "mailto:")) else None
    if tag == "img" and attr == "src":
        return value if value.lower().startswith("mxc://") else None
    if tag == "code" and attr == "class":
        classes = [c for c in value.split() if c.startswith("language-") and not c.startswith("language-_")]
        return " ".join(classes) if classes else None
    return value


MATRIX_HTML_CLEANER = nh3.Cleaner(
    tags=MATRIX_ALLOWED_HTML_TAGS,
    attributes=MATRIX_ALLOWED_HTML_ATTRIBUTES,
    attribute_filter=_filter_matrix_html_attribute,
    url_schemes=MATRIX_ALLOWED_URL_SCHEMES,
    strip_comments=True,
    link_rel="noopener noreferrer",
)

@dataclass
class _StreamBuf:
    """_StreamBuf 类。

    【中文名称】_StreamBuf

    【功能说明】
    这是 Matrix 渠道适配器 中的核心数据结构或服务类。负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""
    text: str = ""
    event_id: str | None = None
    last_edit: float = 0.0

def _render_markdown_html(text: str) -> str | None:
    """执行 `_render_markdown_html`。

    【中文名称】_render_markdown_html

    【功能说明】
    这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        formatted = MATRIX_HTML_CLEANER.clean(MATRIX_MARKDOWN(text)).strip()
    except Exception:
        return None
    if not formatted:
        return None
    # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    if formatted.startswith("<p>") and formatted.endswith("</p>"):
        inner = formatted[3:-4]
        if "<" not in inner and ">" not in inner:
            return None
    return formatted


def _build_matrix_text_content(
    text: str,
    event_id: str | None = None,
    thread_relates_to: dict[str, object] | None = None,
) -> dict[str, object]:
    """执行 `_build_matrix_text_content`。

    【中文名称】_build_matrix_text_content

    【功能说明】
    这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
    - event_id: 调用方传入的 `event_id` 数据；具体类型以函数签名为准。
    - thread_relates_to: 调用方传入的 `thread_relates_to` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    content: dict[str, object] = {"msgtype": "m.text", "body": text, "m.mentions": {}}
    if html := _render_markdown_html(text):
        content["format"] = MATRIX_HTML_FORMAT
        content["formatted_body"] = html
    if event_id:
        content["m.new_content"] = {
            "body": text,
            "msgtype": "m.text",
        }
        content["m.relates_to"] = {
            "rel_type": "m.replace",
            "event_id": event_id,
        }
        if thread_relates_to:
            content["m.new_content"]["m.relates_to"] = thread_relates_to
    elif thread_relates_to:
        content["m.relates_to"] = thread_relates_to

    return content


class MatrixConfig(Base):
    """MatrixConfig 类。

    【中文名称】MatrixConfig

    【功能说明】
    这是 Matrix 渠道适配器 中的核心数据结构或服务类。负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    enabled: bool = False
    homeserver: str = "https://matrix.org"
    user_id: str = ""
    password: str = ""
    access_token: str = ""
    device_id: str = ""
    e2ee_enabled: bool = Field(default=True, alias="e2eeEnabled")
    sas_verification: bool = Field(default=False, alias="sasVerification")
    sync_stop_grace_seconds: int = 2
    max_media_bytes: int = 20 * 1024 * 1024
    max_concurrent_media_downloads: int = 2
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["open", "mention", "allowlist"] = "open"
    group_allow_from: list[str] = Field(default_factory=list)
    allow_room_mentions: bool = False
    streaming: bool = False


class MatrixChannel(BaseChannel):
    """MatrixChannel 类。

    【中文名称】MatrixChannel

    【功能说明】
    这是 Matrix 渠道适配器 中的核心数据结构或服务类。负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    name = "matrix"
    display_name = "Matrix"
    _STREAM_EDIT_INTERVAL = 2 # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
    monotonic_time = time.monotonic

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行 `default_config`。

        【中文名称】default_config

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return MatrixConfig().model_dump(by_alias=True)

    def __init__(
        self,
        config: Any,
        bus: MessageBus,
        *,
        restrict_to_workspace: bool = False,
        workspace: str | Path | None = None,
    ):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - config: 调用方传入的 `config` 数据；具体类型以函数签名为准。
        - bus: 调用方传入的 `bus` 数据；具体类型以函数签名为准。
        - restrict_to_workspace: 调用方传入的 `restrict_to_workspace` 数据；具体类型以函数签名为准。
        - workspace: 调用方传入的 `workspace` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(config, dict):
            config = MatrixConfig.model_validate(config)
        super().__init__(config, bus)
        self.client: AsyncClient | None = None
        self._sync_task: asyncio.Task | None = None
        self._typing_tasks: dict[str, asyncio.Task] = {}
        self._restrict_to_workspace = bool(restrict_to_workspace)
        self._workspace = (
            Path(workspace).expanduser().resolve(strict=False) if workspace is not None else None
        )
        self._server_upload_limit_bytes: int | None = None
        self._server_upload_limit_checked = False
        self._stream_bufs: dict[str, _StreamBuf] = {}
        self._started_at_ms: int = 0
        self._media_download_semaphore = asyncio.Semaphore(
            max(1, int(self.config.max_concurrent_media_downloads))
        )


    async def start(self) -> None:
        """异步执行 `start`。

        【中文名称】start

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = True
        self._started_at_ms = int(time.time() * 1000)
        redirect_lib_logging("nio", level="WARNING")

        self.store_path = get_data_dir() / "matrix-store"
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.session_path = self.store_path / "session.json"

        # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
        safe_store_name = self.config.user_id.replace(":", "_") + f"_{self.config.device_id}.db"

        self.client = AsyncClient(
            homeserver=self.config.homeserver,
            user=self.config.user_id,
            store_path=self.store_path,
            config=AsyncClientConfig(
                store_sync_tokens=True,
                encryption_enabled=self.config.e2ee_enabled,
                store_name=safe_store_name,
            ),
        )

        self._register_event_callbacks()
        self._register_to_device_callbacks()
        self._register_response_callbacks()

        if not self.config.e2ee_enabled:
            self.logger.warning("E2EE disabled; encrypted rooms may be undecryptable.")

        if self.config.password:
            if self.config.access_token or self.config.device_id:
                self.logger.warning("Password-based login active; access_token and device_id fields will be ignored.")

            create_new_session = True
            if self.session_path.exists():
                self.logger.info("Found session.json at {}; attempting to use existing session...", self.session_path)
                try:
                    with open(self.session_path, "r", encoding="utf-8") as f:
                        session = json.load(f)
                    self.client.user_id = self.config.user_id
                    self.client.access_token = session["access_token"]
                    self.client.device_id = session["device_id"]
                    self.client.load_store()
                    self.logger.info("Successfully loaded from existing session")
                    create_new_session = False
                except Exception as e:
                    self.logger.warning("Failed to load from existing session: {}", e)
                    self.logger.info("Falling back to password login...")

            if create_new_session:
                self.logger.info("Using password login...")
                resp = await self.client.login(self.config.password)
                if isinstance(resp, LoginResponse):
                    self.logger.info("Logged in using a password; saving details to disk")
                    self._write_session_to_disk(resp)
                else:
                    self.logger.error("Failed to log in: {}", resp)
                    return

        elif self.config.access_token and self.config.device_id:
            try:
                self.client.user_id = self.config.user_id
                self.client.access_token = self.config.access_token
                self.client.device_id = self.config.device_id
                self.client.load_store()
                self.logger.info("Successfully loaded from existing session")
            except Exception as e:
                self.logger.warning("Failed to load from existing session: {}", e)

        else:
            self.logger.warning("Unable to load a session due to missing password, access_token, or device_id; encryption may not work")
            return

        self._sync_task = asyncio.create_task(self._sync_loop())

    async def stop(self) -> None:
        """异步执行 `stop`。

        【中文名称】stop

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        self._running = False
        for room_id in list(self._typing_tasks):
            await self._stop_typing_keepalive(room_id, clear_typing=False)
        if self.client:
            self.client.stop_sync_forever()
        if self._sync_task:
            try:
                await asyncio.wait_for(asyncio.shield(self._sync_task),
                                       timeout=self.config.sync_stop_grace_seconds)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._sync_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._sync_task
        if self.client:
            await self.client.close()

    def _write_session_to_disk(self, resp: LoginResponse) -> None:
        """执行 `_write_session_to_disk`。

        【中文名称】_write_session_to_disk

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - resp: 调用方传入的 `resp` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        session = {
            "access_token": resp.access_token,
            "device_id": resp.device_id,
        }
        try:
            with open(self.session_path, "w", encoding="utf-8") as f:
                json.dump(session, f, indent=2)
            self.logger.info("Session saved to {}", self.session_path)
        except Exception as e:
            self.logger.warning("Failed to save session: {}", e)

    def _is_workspace_path_allowed(self, path: Path) -> bool:
        """执行 `_is_workspace_path_allowed`。

        【中文名称】_is_workspace_path_allowed

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self._restrict_to_workspace or not self._workspace:
            return True
        return is_path_within(path, self._workspace)

    def _collect_outbound_media_candidates(self, media: list[str]) -> list[Path]:
        """执行 `_collect_outbound_media_candidates`。

        【中文名称】_collect_outbound_media_candidates

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - media: 调用方传入的 `media` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        seen: set[str] = set()
        candidates: list[Path] = []
        for raw in media:
            if not isinstance(raw, str) or not raw.strip():
                continue
            path = Path(raw.strip()).expanduser()
            try:
                key = str(path.resolve(strict=False))
            except OSError:
                key = str(path)
            if key not in seen:
                seen.add(key)
                candidates.append(path)
        return candidates

    @staticmethod
    def _build_outbound_attachment_content(
        *, filename: str, mime: str, size_bytes: int,
        mxc_url: str, encryption_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """执行 `_build_outbound_attachment_content`。

        【中文名称】_build_outbound_attachment_content

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。
        - mime: 调用方传入的 `mime` 数据；具体类型以函数签名为准。
        - size_bytes: 调用方传入的 `size_bytes` 数据；具体类型以函数签名为准。
        - mxc_url: 调用方传入的 `mxc_url` 数据；具体类型以函数签名为准。
        - encryption_info: 调用方传入的 `encryption_info` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        prefix = mime.split("/")[0]
        msgtype = {"image": "m.image", "audio": "m.audio", "video": "m.video"}.get(prefix, "m.file")
        content: dict[str, Any] = {
            "msgtype": msgtype, "body": filename, "filename": filename,
            "info": {"mimetype": mime, "size": size_bytes}, "m.mentions": {},
        }
        if encryption_info:
            content["file"] = {**encryption_info, "url": mxc_url}
        else:
            content["url"] = mxc_url
        return content

    def _is_encrypted_room(self, room_id: str) -> bool:
        """执行 `_is_encrypted_room`。

        【中文名称】_is_encrypted_room

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not self.client:
            return False
        room = getattr(self.client, "rooms", {}).get(room_id)
        return bool(getattr(room, "encrypted", False))

    async def _send_room_content(self, room_id: str,
                                 content: dict[str, Any]) -> None | RoomSendResponse | RoomSendError:
        """异步执行 `_send_room_content`。

        【中文名称】_send_room_content

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.client:
            return None
        kwargs: dict[str, Any] = {"room_id": room_id, "message_type": "m.room.message", "content": content}

        if self.config.e2ee_enabled:
            kwargs["ignore_unverified_devices"] = True
        response = await self.client.room_send(**kwargs)
        return response

    async def _resolve_server_upload_limit_bytes(self) -> int | None:
        """异步执行 `_resolve_server_upload_limit_bytes`。

        【中文名称】_resolve_server_upload_limit_bytes

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if self._server_upload_limit_checked:
            return self._server_upload_limit_bytes
        self._server_upload_limit_checked = True
        if not self.client:
            return None
        try:
            response = await self.client.content_repository_config()
        except Exception:
            self.logger.error("Failed to fetch server upload limit", exc_info=True)
            return None
        upload_size = getattr(response, "upload_size", None)
        if isinstance(upload_size, int) and upload_size > 0:
            self._server_upload_limit_bytes = upload_size
            return upload_size
        return None

    async def _effective_media_limit_bytes(self) -> int:
        """异步执行 `_effective_media_limit_bytes`。

        【中文名称】_effective_media_limit_bytes

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        local_limit = max(int(self.config.max_media_bytes), 0)
        server_limit = await self._resolve_server_upload_limit_bytes()
        if server_limit is None:
            return local_limit
        return min(local_limit, server_limit) if local_limit else 0

    async def _upload_and_send_attachment(
        self, room_id: str, path: Path, limit_bytes: int,
        relates_to: dict[str, Any] | None = None,
    ) -> str | None:
        """异步执行 `_upload_and_send_attachment`。

        【中文名称】_upload_and_send_attachment

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。
        - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。
        - limit_bytes: 调用方传入的 `limit_bytes` 数据；具体类型以函数签名为准。
        - relates_to: 调用方传入的 `relates_to` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.client:
            return _ATTACH_UPLOAD_FAILED.format(path.name or _DEFAULT_ATTACH_NAME)

        resolved = path.expanduser().resolve(strict=False)
        filename = safe_filename(resolved.name) or _DEFAULT_ATTACH_NAME
        fail = _ATTACH_UPLOAD_FAILED.format(filename)

        if not resolved.is_file() or not self._is_workspace_path_allowed(resolved):
            return fail
        try:
            size_bytes = resolved.stat().st_size
        except OSError:
            return fail
        if limit_bytes <= 0 or size_bytes > limit_bytes:
            return _ATTACH_TOO_LARGE.format(filename)

        mime = mimetypes.guess_type(filename, strict=False)[0] or "application/octet-stream"
        try:
            with resolved.open("rb") as f:
                upload_result = await self.client.upload(
                    f, content_type=mime, filename=filename,
                    encrypt=self.config.e2ee_enabled and self._is_encrypted_room(room_id),
                    filesize=size_bytes,
                )
        except Exception:
            self.logger.error("Matrix media upload failed for %s", filename, exc_info=True)
            return fail

        upload_response = upload_result[0] if isinstance(upload_result, tuple) else upload_result
        encryption_info = upload_result[1] if isinstance(upload_result, tuple) and isinstance(upload_result[1], dict) else None
        if isinstance(upload_response, UploadError):
            return fail
        mxc_url = getattr(upload_response, "content_uri", None)
        if not isinstance(mxc_url, str) or not mxc_url.startswith("mxc://"):
            return fail

        content = self._build_outbound_attachment_content(
            filename=filename, mime=mime, size_bytes=size_bytes,
            mxc_url=mxc_url, encryption_info=encryption_info,
        )
        if relates_to:
            content["m.relates_to"] = relates_to
        try:
            await self._send_room_content(room_id, content)
        except Exception:
            self.logger.error("Matrix room content send failed for room_id=%s", room_id, exc_info=True)
            return fail
        return None

    async def send(self, msg: OutboundMessage) -> None:
        """异步执行 `send`。

        【中文名称】send

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.client:
            return
        text = msg.content or ""
        candidates = self._collect_outbound_media_candidates(msg.media)
        relates_to = self._build_thread_relates_to(msg.metadata)
        is_progress = bool((msg.metadata or {}).get("_progress"))
        try:
            failures: list[str] = []
            if candidates:
                limit_bytes = await self._effective_media_limit_bytes()
                for path in candidates:
                    if fail := await self._upload_and_send_attachment(
                        room_id=msg.chat_id,
                        path=path,
                        limit_bytes=limit_bytes,
                        relates_to=relates_to,
                    ):
                        failures.append(fail)
            if failures:
                text = f"{text.rstrip()}\n{chr(10).join(failures)}" if text.strip() else "\n".join(failures)
            if text.strip():
                content = _build_matrix_text_content(text)
                if relates_to:
                    content["m.relates_to"] = relates_to
                await self._send_room_content(msg.chat_id, content)
        finally:
            if not is_progress:
                await self._stop_typing_keepalive(msg.chat_id, clear_typing=True)

    async def send_delta(self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None) -> None:
        """异步执行 `send_delta`。

        【中文名称】send_delta

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - chat_id: 调用方传入的 `chat_id` 数据；具体类型以函数签名为准。
        - delta: 调用方传入的 `delta` 数据；具体类型以函数签名为准。
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        meta = metadata or {}
        relates_to = self._build_thread_relates_to(metadata)

        if meta.get("_stream_end"):
            buf = self._stream_bufs.pop(chat_id, None)
            if not buf or not buf.event_id or not buf.text:
                return

            await self._stop_typing_keepalive(chat_id, clear_typing=True)

            content = _build_matrix_text_content(
                buf.text,
                buf.event_id,
                thread_relates_to=relates_to,
            )
            await self._send_room_content(chat_id, content)
            return

        buf = self._stream_bufs.get(chat_id)
        if buf is None:
            buf = _StreamBuf()
            self._stream_bufs[chat_id] = buf
        buf.text += delta

        if not buf.text.strip():
            return

        now = self.monotonic_time()

        if not buf.last_edit or (now - buf.last_edit) >= self._STREAM_EDIT_INTERVAL:
            try:
                content = _build_matrix_text_content(
                    buf.text,
                    buf.event_id,
                    thread_relates_to=relates_to,
                )
                response = await self._send_room_content(chat_id, content)
                buf.last_edit = now
                if not buf.event_id:
                    # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
                    buf.event_id = response.event_id
            except Exception:
                self.logger.error("Stream send/edit failed for chat_id=%s", chat_id, exc_info=True)
                await self._stop_typing_keepalive(chat_id, clear_typing=True)


    def _register_event_callbacks(self) -> None:
        """执行 `_register_event_callbacks`。

        【中文名称】_register_event_callbacks

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self.client.add_event_callback(self._on_message, RoomMessageText)
        self.client.add_event_callback(self._on_media_message, MATRIX_MEDIA_EVENT_FILTER)
        self.client.add_event_callback(self._on_room_invite, InviteEvent)

    def _register_to_device_callbacks(self) -> None:
        """执行 `_register_to_device_callbacks`。

        【中文名称】_register_to_device_callbacks

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if self.config.e2ee_enabled and self.config.sas_verification:
            self.client.add_to_device_callback(
                self._on_key_verification_event,
                (KeyVerificationEvent,),
            )

    def _register_response_callbacks(self) -> None:
        """执行 `_register_response_callbacks`。

        【中文名称】_register_response_callbacks

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self.client.add_response_callback(self._on_sync_error, SyncError)
        self.client.add_response_callback(self._on_join_error, JoinError)
        self.client.add_response_callback(self._on_send_error, RoomSendError)

    def _is_sas_sender_allowed(self, sender: str) -> bool:
        """执行 `_is_sas_sender_allowed`。

        【中文名称】_is_sas_sender_allowed

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sender: 调用方传入的 `sender` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return bool(sender and self.is_allowed(sender))

    async def _on_key_verification_event(self, event: KeyVerificationEvent) -> None:
        """异步执行 `_on_key_verification_event`。

        【中文名称】_on_key_verification_event

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        try:
            await self._handle_key_verification_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Matrix SAS verification handling failed")

    async def _handle_key_verification_event(self, event: KeyVerificationEvent) -> None:
        """异步执行 `_handle_key_verification_event`。

        【中文名称】_handle_key_verification_event

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not (self.config.e2ee_enabled and self.config.sas_verification):
            return
        if not self.client:
            return

        sender = str(getattr(event, "sender", "") or "")
        transaction_id = str(getattr(event, "transaction_id", "") or "")
        if not transaction_id or not self._is_sas_sender_allowed(sender):
            return

        if isinstance(event, KeyVerificationStart):
            if "emoji" not in (getattr(event, "short_authentication_string", None) or []):
                self.logger.info(
                    "Ignoring Matrix SAS verification from {} without emoji support",
                    sender,
                )
                return

            response = await self.client.accept_key_verification(transaction_id)
            if isinstance(response, ToDeviceError):
                self.logger.warning("Matrix SAS accept failed for {}: {}", sender, response)
            return

        if isinstance(event, KeyVerificationKey):
            responses = await self.client.send_to_device_messages()
            if any(isinstance(response, ToDeviceError) for response in responses):
                self.logger.warning("Matrix SAS key share failed for {}", sender)
                return

            response = await self.client.confirm_short_auth_string(transaction_id)
            if isinstance(response, ToDeviceError):
                self.logger.warning("Matrix SAS confirm failed for {}: {}", sender, response)
            return

        if isinstance(event, KeyVerificationMac):
            sas = getattr(self.client, "key_verifications", {}).get(transaction_id)
            if sas is not None and getattr(sas, "verified", False):
                self.logger.info("Matrix SAS verification completed for {}", sender)
            return

        if isinstance(event, KeyVerificationCancel):
            self.logger.info(
                "Matrix SAS verification cancelled by {}: {}",
                sender,
                getattr(event, "reason", ""),
            )

    def _is_fatal_auth_response(self, response: Any) -> bool:
        """执行 `_is_fatal_auth_response`。

        【中文名称】_is_fatal_auth_response

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        code = getattr(response, "status_code", None)
        is_auth = code in {"M_UNKNOWN_TOKEN", "M_FORBIDDEN", "M_UNAUTHORIZED"}
        return is_auth or bool(getattr(response, "soft_logout", False))

    def _log_response_error(self, label: str, response: Any) -> None:
        """执行 `_log_response_error`。

        【中文名称】_log_response_error

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - label: 调用方传入的 `label` 数据；具体类型以函数签名为准。
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        is_fatal = self._is_fatal_auth_response(response)
        (self.logger.error if is_fatal else self.logger.warning)("{} failed: {}", label, response)

    async def _on_sync_error(self, response: SyncError) -> None:
        """异步执行 `_on_sync_error`。

        【中文名称】_on_sync_error

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._log_response_error("sync", response)
        if self._is_fatal_auth_response(response):
            # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Matrix 渠道适配器 的协议细节或边界情况，避免外部差异影响核心流程。
            self.logger.error("Authentication failed irrecoverably; stopping sync loop")
            self._running = False
            if self.client:
                with suppress(Exception):
                    self.client.stop_sync_forever()

    async def _on_join_error(self, response: JoinError) -> None:
        """异步执行 `_on_join_error`。

        【中文名称】_on_join_error

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._log_response_error("join", response)

    async def _on_send_error(self, response: RoomSendError) -> None:
        """异步执行 `_on_send_error`。

        【中文名称】_on_send_error

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._log_response_error("send", response)

    async def _set_typing(self, room_id: str, typing: bool) -> None:
        """异步执行 `_set_typing`。

        【中文名称】_set_typing

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。
        - typing: 调用方传入的 `typing` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.client:
            return
        with suppress(Exception):
            response = await self.client.room_typing(room_id=room_id, typing_state=typing,
                                                     timeout=TYPING_NOTICE_TIMEOUT_MS)
            if isinstance(response, RoomTypingError):
                self.logger.debug("typing failed for {}: {}", room_id, response)

    async def _start_typing_keepalive(self, room_id: str) -> None:
        """异步执行 `_start_typing_keepalive`。

        【中文名称】_start_typing_keepalive

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        await self._stop_typing_keepalive(room_id, clear_typing=False)
        await self._set_typing(room_id, True)
        if not self._running:
            return

        async def loop() -> None:
            """异步执行 `loop`。

            【中文名称】loop

            【功能说明】
            这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
            阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

            【参数说明】
            - 无显式业务参数。

            【返回值】
            - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

            with suppress(asyncio.CancelledError):
                while self._running:
                    await asyncio.sleep(TYPING_KEEPALIVE_INTERVAL_MS / 1000)
                    await self._set_typing(room_id, True)

        self._typing_tasks[room_id] = asyncio.create_task(loop())

    async def _stop_typing_keepalive(self, room_id: str, *, clear_typing: bool) -> None:
        """异步执行 `_stop_typing_keepalive`。

        【中文名称】_stop_typing_keepalive

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room_id: 调用方传入的 `room_id` 数据；具体类型以函数签名为准。
        - clear_typing: 调用方传入的 `clear_typing` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if task := self._typing_tasks.pop(room_id, None):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if clear_typing:
            await self._set_typing(room_id, False)

    async def _sync_loop(self) -> None:
        """异步执行 `_sync_loop`。

        【中文名称】_sync_loop

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        backoff = 2.0
        while self._running:
            try:
                await self.client.sync_forever(timeout=30000, full_state=True)
                backoff = 2.0
            except asyncio.CancelledError:
                break
            except Exception:
                if not self._running:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _on_room_invite(self, room: MatrixRoom, event: InviteEvent) -> None:
        """异步执行 `_on_room_invite`。

        【中文名称】_on_room_invite

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if self.is_allowed(event.sender):
            await self.client.join(room.room_id)

    def _is_direct_room(self, room: MatrixRoom) -> bool:
        """执行 `_is_direct_room`。

        【中文名称】_is_direct_room

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        count = getattr(room, "member_count", None)
        return isinstance(count, int) and count <= 2

    def _is_bot_mentioned(self, event: RoomMessage) -> bool:
        """执行 `_is_bot_mentioned`。

        【中文名称】_is_bot_mentioned

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        source = getattr(event, "source", None)
        if not isinstance(source, dict):
            return False
        mentions = (source.get("content") or {}).get("m.mentions")
        if not isinstance(mentions, dict):
            return False
        user_ids = mentions.get("user_ids")
        if isinstance(user_ids, list) and self.config.user_id in user_ids:
            return True
        return bool(self.config.allow_room_mentions and mentions.get("room") is True)

    def _is_pre_startup_event(self, event: RoomMessage) -> bool:
        """执行 `_is_pre_startup_event`。

        【中文名称】_is_pre_startup_event

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        ts = getattr(event, "server_timestamp", None)
        return isinstance(ts, int) and ts < self._started_at_ms

    def _should_process_message(self, room: MatrixRoom, event: RoomMessage) -> bool:
        """执行 `_should_process_message`。

        【中文名称】_should_process_message

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.is_allowed(event.sender):
            return False
        if self._is_direct_room(room):
            return True
        policy = self.config.group_policy
        if policy == "open":
            return True
        if policy == "allowlist":
            return room.room_id in (self.config.group_allow_from or [])
        if policy == "mention":
            return self._is_bot_mentioned(event)
        return False

    def _media_dir(self) -> Path:
        """执行 `_media_dir`。

        【中文名称】_media_dir

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return get_media_dir("matrix")

    @staticmethod
    def _event_source_content(event: RoomMessage) -> dict[str, Any]:
        """执行 `_event_source_content`。

        【中文名称】_event_source_content

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        source = getattr(event, "source", None)
        if not isinstance(source, dict):
            return {}
        content = source.get("content")
        return content if isinstance(content, dict) else {}

    def _event_thread_root_id(self, event: RoomMessage) -> str | None:
        """执行 `_event_thread_root_id`。

        【中文名称】_event_thread_root_id

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        relates_to = self._event_source_content(event).get("m.relates_to")
        if not isinstance(relates_to, dict) or relates_to.get("rel_type") != "m.thread":
            return None
        root_id = relates_to.get("event_id")
        return root_id if isinstance(root_id, str) and root_id else None

    def _thread_metadata(self, event: RoomMessage) -> dict[str, str] | None:
        """执行 `_thread_metadata`。

        【中文名称】_thread_metadata

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not (root_id := self._event_thread_root_id(event)):
            return None
        meta: dict[str, str] = {"thread_root_event_id": root_id}
        if isinstance(reply_to := getattr(event, "event_id", None), str) and reply_to:
            meta["thread_reply_to_event_id"] = reply_to
        return meta

    @staticmethod
    def _build_thread_relates_to(metadata: dict[str, Any] | None) -> dict[str, Any] | None:
        """执行 `_build_thread_relates_to`。

        【中文名称】_build_thread_relates_to

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - metadata: 调用方传入的 `metadata` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not metadata:
            return None
        root_id = metadata.get("thread_root_event_id")
        if not isinstance(root_id, str) or not root_id:
            return None
        reply_to = metadata.get("thread_reply_to_event_id") or metadata.get("event_id")
        if not isinstance(reply_to, str) or not reply_to:
            return None
        return {"rel_type": "m.thread", "event_id": root_id,
                "m.in_reply_to": {"event_id": reply_to}, "is_falling_back": True}

    def _event_attachment_type(self, event: MatrixMediaEvent) -> str:
        """执行 `_event_attachment_type`。

        【中文名称】_event_attachment_type

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        msgtype = self._event_source_content(event).get("msgtype")
        return _MSGTYPE_MAP.get(msgtype, "file")

    @staticmethod
    def _is_encrypted_media_event(event: MatrixMediaEvent) -> bool:
        """执行 `_is_encrypted_media_event`。

        【中文名称】_is_encrypted_media_event

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return (isinstance(getattr(event, "key", None), dict)
                and isinstance(getattr(event, "hashes", None), dict)
                and isinstance(getattr(event, "iv", None), str))

    def _event_declared_size_bytes(self, event: MatrixMediaEvent) -> int | None:
        """执行 `_event_declared_size_bytes`。

        【中文名称】_event_declared_size_bytes

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        info = self._event_source_content(event).get("info")
        size = info.get("size") if isinstance(info, dict) else None
        return size if type(size) is int and size >= 0 else None

    def _event_mime(self, event: MatrixMediaEvent) -> str | None:
        """执行 `_event_mime`。

        【中文名称】_event_mime

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        info = self._event_source_content(event).get("info")
        if isinstance(info, dict) and isinstance(m := info.get("mimetype"), str) and m:
            return m
        m = getattr(event, "mimetype", None)
        return m if isinstance(m, str) and m else None

    def _event_filename(self, event: MatrixMediaEvent, attachment_type: str) -> str:
        """执行 `_event_filename`。

        【中文名称】_event_filename

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。
        - attachment_type: 调用方传入的 `attachment_type` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        body = getattr(event, "body", None)
        if isinstance(body, str) and body.strip():
            if candidate := safe_filename(Path(body).name):
                return candidate
        return _DEFAULT_ATTACH_NAME if attachment_type == "file" else attachment_type

    def _build_attachment_path(self, event: MatrixMediaEvent, attachment_type: str,
                               filename: str, mime: str | None) -> Path:
        """执行 `_build_attachment_path`。

        【中文名称】_build_attachment_path

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。
        - attachment_type: 调用方传入的 `attachment_type` 数据；具体类型以函数签名为准。
        - filename: 调用方传入的 `filename` 数据；具体类型以函数签名为准。
        - mime: 调用方传入的 `mime` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        safe_name = safe_filename(Path(filename).name) or _DEFAULT_ATTACH_NAME
        suffix = Path(safe_name).suffix
        if not suffix and mime:
            if guessed := mimetypes.guess_extension(mime, strict=False):
                safe_name, suffix = f"{safe_name}{guessed}", guessed
        stem = (Path(safe_name).stem or attachment_type)[:72]
        suffix = suffix[:16]
        event_id = safe_filename(str(getattr(event, "event_id", "") or "evt").lstrip("$"))
        event_prefix = (event_id[:24] or "evt").strip("_")
        return self._media_dir() / f"{event_prefix}_{stem}{suffix}"

    async def _download_media_bytes(self, mxc_url: str, limit_bytes: int) -> bytes | None:
        """异步执行 `_download_media_bytes`。

        【中文名称】_download_media_bytes

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - mxc_url: 调用方传入的 `mxc_url` 数据；具体类型以函数签名为准。
        - limit_bytes: 调用方传入的 `limit_bytes` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not self.client or limit_bytes <= 0:
            raise _MediaTooLargeError

        parsed = urlparse(mxc_url)
        if parsed.scheme != "mxc" or not parsed.netloc or not parsed.path.strip("/"):
            return None

        homeserver = str(getattr(self.client, "homeserver", "") or self.config.homeserver).rstrip("/")
        media_url = (
            f"{homeserver}/_matrix/client/v1/media/download/"
            f"{quote(parsed.netloc, safe='')}/{quote(parsed.path.strip('/'), safe='')}"
        )
        token = getattr(self.client, "access_token", None) or self.config.access_token
        headers = {"Authorization": f"Bearer {token}"} if token else None
        timeout = aiohttp.ClientTimeout(total=None)

        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(media_url, params={"allow_remote": "true"}) as response:
                    if response.status >= 400:
                        self.logger.warning("download failed for {}: HTTP {}", mxc_url, response.status)
                        return None
                    content_length = response.headers.get("Content-Length")
                    if content_length is not None:
                        try:
                            if int(content_length) > limit_bytes:
                                raise _MediaTooLargeError
                        except ValueError:
                            pass

                    chunks = bytearray()
                    async for chunk in response.content.iter_chunked(64 * 1024):
                        chunks.extend(chunk)
                        if len(chunks) > limit_bytes:
                            raise _MediaTooLargeError
                    return bytes(chunks)
        except _MediaTooLargeError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            self.logger.warning("download failed for {}", mxc_url, exc_info=True)
            return None

    def _decrypt_media_bytes(self, event: MatrixMediaEvent, ciphertext: bytes) -> bytes | None:
        """执行 `_decrypt_media_bytes`。

        【中文名称】_decrypt_media_bytes

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。
        - ciphertext: 调用方传入的 `ciphertext` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        key_obj, hashes, iv = getattr(event, "key", None), getattr(event, "hashes", None), getattr(event, "iv", None)
        key = key_obj.get("k") if isinstance(key_obj, dict) else None
        sha256 = hashes.get("sha256") if isinstance(hashes, dict) else None
        if not all(isinstance(v, str) for v in (key, sha256, iv)):
            return None
        try:
            return decrypt_attachment(ciphertext, key, sha256, iv)
        except (EncryptionError, ValueError, TypeError):
            self.logger.warning("decrypt failed for event {}", getattr(event, "event_id", ""))
            return None

    async def _fetch_media_attachment(
        self, room: MatrixRoom, event: MatrixMediaEvent,
    ) -> tuple[dict[str, Any] | None, str]:
        """异步执行 `_fetch_media_attachment`。

        【中文名称】_fetch_media_attachment

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        atype = self._event_attachment_type(event)
        mime = self._event_mime(event)
        filename = self._event_filename(event, atype)
        mxc_url = getattr(event, "url", None)
        fail = _ATTACH_FAILED.format(filename)

        if not isinstance(mxc_url, str) or not mxc_url.startswith("mxc://"):
            return None, fail

        limit_bytes = await self._effective_media_limit_bytes()
        declared = self._event_declared_size_bytes(event)
        if declared is None or declared > limit_bytes:
            return None, _ATTACH_TOO_LARGE.format(filename)

        try:
            async with self._media_download_semaphore:
                downloaded = await self._download_media_bytes(mxc_url, limit_bytes)
        except _MediaTooLargeError:
            return None, _ATTACH_TOO_LARGE.format(filename)
        if downloaded is None:
            return None, fail

        encrypted = self._is_encrypted_media_event(event)
        data = downloaded
        if encrypted:
            if (data := self._decrypt_media_bytes(event, downloaded)) is None:
                return None, fail

        if len(data) > limit_bytes:
            return None, _ATTACH_TOO_LARGE.format(filename)

        path = self._build_attachment_path(event, atype, filename, mime)
        try:
            path.write_bytes(data)
        except OSError:
            return None, fail

        attachment = {
            "type": atype, "mime": mime, "filename": filename,
            "event_id": str(getattr(event, "event_id", "") or ""),
            "encrypted": encrypted, "size_bytes": len(data),
            "path": str(path), "mxc_url": mxc_url,
        }
        return attachment, _ATTACH_MARKER.format(path)

    def _base_metadata(self, room: MatrixRoom, event: RoomMessage) -> dict[str, Any]:
        """执行 `_base_metadata`。

        【中文名称】_base_metadata

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        meta: dict[str, Any] = {"room": getattr(room, "display_name", room.room_id)}
        if isinstance(eid := getattr(event, "event_id", None), str) and eid:
            meta["event_id"] = eid
        if thread := self._thread_metadata(event):
            meta.update(thread)
        return meta

    async def _on_message(self, room: MatrixRoom, event: RoomMessageText) -> None:
        """异步执行 `_on_message`。

        【中文名称】_on_message

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if (
            event.sender == self.config.user_id
            or self._is_pre_startup_event(event)
            or not self._should_process_message(room, event)
        ):
            return
        await self._start_typing_keepalive(room.room_id)
        try:
            await self._handle_message(
                sender_id=event.sender, chat_id=room.room_id,
                content=event.body, metadata=self._base_metadata(room, event),
                is_dm=self._is_direct_room(room),
            )
        except Exception:
            await self._stop_typing_keepalive(room.room_id, clear_typing=True)
            raise

    async def _on_media_message(self, room: MatrixRoom, event: MatrixMediaEvent) -> None:
        """异步执行 `_on_media_message`。

        【中文名称】_on_media_message

        【功能说明】
        这是 Matrix 渠道适配器 中的一个步骤函数，用来支撑：负责连接 Matrix 房间、同步消息和附件、处理加密/未加密房间差异，并把回复发送回对应 room。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - room: 调用方传入的 `room` 数据；具体类型以函数签名为准。
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if (
            event.sender == self.config.user_id
            or self._is_pre_startup_event(event)
            or not self._should_process_message(room, event)
        ):
            return
        attachment, marker = await self._fetch_media_attachment(room, event)
        parts: list[str] = []
        if isinstance(body := getattr(event, "body", None), str) and body.strip():
            parts.append(body.strip())

        if attachment and attachment.get("type") == "audio":
            transcription = await self.transcribe_audio(attachment["path"])
            if transcription:
                parts.append(f"[transcription: {transcription}]")
            else:
                parts.append(marker)
        elif marker:
            parts.append(marker)

        await self._start_typing_keepalive(room.room_id)
        try:
            meta = self._base_metadata(room, event)
            meta["attachments"] = []
            if attachment:
                meta["attachments"] = [attachment]
            await self._handle_message(
                sender_id=event.sender, chat_id=room.room_id,
                content="\n".join(parts),
                media=[attachment["path"]] if attachment else [],
                metadata=meta,
                is_dm=self._is_direct_room(room),
            )
        except Exception:
            await self._stop_typing_keepalive(room.room_id, clear_typing=True)
            raise
