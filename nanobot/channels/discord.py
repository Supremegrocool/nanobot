"""Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。

【中文名称】渠道适配器：nanobot/channels/discord.py

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
import importlib.util
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.command.builtin import build_help_text
from nanobot.config.paths import get_media_dir
from nanobot.config.schema import Base
from nanobot.utils.helpers import safe_filename, split_message

DISCORD_AVAILABLE = importlib.util.find_spec("discord") is not None
if TYPE_CHECKING:
    import aiohttp
    import discord
    from discord import app_commands
    from discord.abc import Messageable

if DISCORD_AVAILABLE:
    import discord
    from discord import app_commands
    from discord.abc import Messageable

MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024  # 中文说明：20MB 相关逻辑。
MAX_MESSAGE_LEN = 2000  # 中文说明：消息字符长度上限。
TYPING_INTERVAL_S = 8


@dataclass
class _StreamBuf:
    """_StreamBuf 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】_StreamBuf

    【功能说明】
    Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    text: str = ""
    message: Any | None = None
    last_edit: float = 0.0
    stream_id: str | None = None


class DiscordConfig(Base):
    """DiscordConfig 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】DiscordConfig

    【功能说明】
    Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    Base。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    enabled: bool = False
    token: str = ""
    allow_from: list[str] = Field(default_factory=list)
    allow_channels: list[str] = Field(default_factory=list)  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    intents: int = 37377
    group_policy: Literal["mention", "open"] = "mention"
    read_receipt_emoji: str = "👀"
    working_emoji: str = "🔧"
    working_emoji_delay: float = 2.0
    streaming: bool = True
    proxy: str | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None


if DISCORD_AVAILABLE:

    class DiscordBotClient(discord.Client):
        """DiscordBotClient 类，封装 渠道适配器 的核心状态和行为。

        【中文名称】DiscordBotClient

        【功能说明】
        Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
        让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

        【继承关系】
        discord.Client。继承关系决定它需要实现哪些项目约定的方法。

        【学习提示】
        先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
        """

        def __init__(
            self,
            channel: DiscordChannel,
            *,
            intents: discord.Intents,
            proxy: str | None = None,
            proxy_auth: aiohttp.BasicAuth | None = None,
        ) -> None:
            """初始化对象（__init__ = 原函数名）。

            【中文名称】初始化对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            intents: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            proxy: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            proxy_auth: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            super().__init__(intents=intents, proxy=proxy, proxy_auth=proxy_auth)
            self._channel = channel
            self.tree = app_commands.CommandTree(self)
            self._register_app_commands()

        async def on_ready(self) -> None:
            """异步执行辅助逻辑（on_ready = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.on_ready` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            self._channel._bot_user_id = str(self.user.id) if self.user else None
            self._channel.logger.info("bot connected as user {}", self._channel._bot_user_id)
            try:
                synced = await self.tree.sync()
                self._channel.logger.info("app commands synced: {}", len(synced))
            except Exception as e:
                self._channel.logger.warning("app command sync failed: {}", e)

        async def on_message(self, message: discord.Message) -> None:
            """异步执行辅助逻辑（on_message = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.on_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            message: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await self._channel._handle_discord_message(message)

        async def on_thread_delete(self, thread: discord.Thread) -> None:
            """异步执行辅助逻辑（on_thread_delete = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.on_thread_delete` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            thread: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            self._channel._forget_channel(thread)

        async def on_thread_update(self, before: discord.Thread, after: discord.Thread) -> None:
            """异步执行辅助逻辑（on_thread_update = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.on_thread_update` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            before: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            after: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            if getattr(after, "archived", False):
                self._channel._forget_channel(after)
            else:
                self._channel._remember_channel(after)

        async def _reply_ephemeral(self, interaction: discord.Interaction, text: str) -> bool:
            """异步执行辅助逻辑（_reply_ephemeral = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._reply_ephemeral` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            try:
                await interaction.response.send_message(text, ephemeral=True)
                return True
            except Exception as e:
                self._channel.logger.warning("interaction response failed: {}", e)
                return False

        async def _resolve_interaction_channel(
            self,
            interaction: discord.Interaction,
        ) -> Any | None:
            """异步解析目标（_resolve_interaction_channel = 原函数名）。

            【中文名称】解析目标

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._resolve_interaction_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            channel_id = interaction.channel_id
            if channel_id is None:
                return None
            channel = getattr(interaction, "channel", None) or self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as e:
                    self._channel.logger.warning("interaction channel {} unavailable: {}", channel_id, e)
                    return None
            self._channel._remember_channel(channel)
            return channel

        async def _interaction_channel_allowed(
            self,
            interaction: discord.Interaction,
            channel: Any | None,
        ) -> bool:
            """异步执行辅助逻辑（_interaction_channel_allowed = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._interaction_channel_allowed` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            allow_channels = self._channel.config.allow_channels
            if not allow_channels:
                return True
            if channel is None:
                channel_id = interaction.channel_id
                return channel_id is not None and str(channel_id) in allow_channels
            channel_ids = self._channel._channel_allow_keys(channel)
            return not channel_ids.isdisjoint(allow_channels)

        async def _forward_slash_command(
            self,
            interaction: discord.Interaction,
            command_text: str,
        ) -> None:
            """异步执行辅助逻辑（_forward_slash_command = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._forward_slash_command` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            command_text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            sender_id = str(interaction.user.id)
            channel_id = interaction.channel_id

            if channel_id is None:
                self._channel.logger.warning("slash command missing channel_id: {}", command_text)
                return

            if not self._channel.is_allowed(sender_id):
                await self._reply_ephemeral(interaction, "You are not allowed to use this bot.")
                return

            channel = await self._resolve_interaction_channel(interaction)
            if not await self._interaction_channel_allowed(interaction, channel):
                await self._reply_ephemeral(interaction, "This channel is not allowed for this bot.")
                return

            await self._reply_ephemeral(interaction, f"Processing {command_text}...")

            metadata: dict[str, Any] = {
                "interaction_id": str(interaction.id),
                "guild_id": str(interaction.guild_id) if interaction.guild_id else None,
                "is_slash_command": True,
            }
            session_key = None
            if channel is not None:
                parent_channel_id = self._channel._channel_parent_key(channel)
                if parent_channel_id is not None:
                    metadata["parent_channel_id"] = parent_channel_id
                    metadata["context_chat_id"] = parent_channel_id
                    metadata["thread_id"] = str(channel_id)
                    session_key = f"{self._channel.name}:{parent_channel_id}:thread:{channel_id}"

            await self._channel._handle_message(
                sender_id=sender_id,
                chat_id=str(channel_id),
                content=command_text,
                metadata=metadata,
                session_key=session_key,
            )

        def _register_app_commands(self) -> None:
            """执行辅助逻辑（_register_app_commands = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._register_app_commands` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            commands = (
                ("new", "Stop current task and start a new conversation", "/new"),
                ("stop", "Stop the current task", "/stop"),
                ("restart", "Restart the bot", "/restart"),
                ("status", "Show bot status", "/status"),
                ("history", "Show recent conversation messages", "/history"),
            )

            for name, description, command_text in commands:

                @self.tree.command(name=name, description=description)
                async def command_handler(
                    interaction: discord.Interaction,
                    _command_text: str = command_text,
                ) -> None:
                    """异步执行辅助逻辑（command_handler = 原函数名）。

                    【中文名称】执行辅助逻辑

                    【功能说明】
                    这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                    在阅读 `DiscordBotClient.command_handler` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                    【参数说明】
                    interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
                    _command_text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

                    【返回值】
                    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                    """
                    await self._forward_slash_command(interaction, _command_text)

            @self.tree.command(name="model", description="Show or switch runtime model preset")
            @app_commands.describe(preset="Optional model preset name, such as default")
            async def model_command(
                interaction: discord.Interaction,
                preset: str | None = None,
            ) -> None:
                """异步执行辅助逻辑（model_command = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `DiscordBotClient.model_command` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
                preset: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                preset = (preset or "").strip()
                command_text = f"/model {preset}" if preset else "/model"
                await self._forward_slash_command(interaction, command_text)

            @self.tree.command(name="help", description="Show available commands")
            async def help_command(interaction: discord.Interaction) -> None:
                """异步执行辅助逻辑（help_command = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `DiscordBotClient.help_command` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                sender_id = str(interaction.user.id)
                if not self._channel.is_allowed(sender_id):
                    await self._reply_ephemeral(interaction, "You are not allowed to use this bot.")
                    return
                channel = await self._resolve_interaction_channel(interaction)
                if not await self._interaction_channel_allowed(interaction, channel):
                    await self._reply_ephemeral(interaction, "This channel is not allowed for this bot.")
                    return
                await self._reply_ephemeral(interaction, build_help_text())

            @self.tree.error
            async def on_app_command_error(
                interaction: discord.Interaction,
                error: app_commands.AppCommandError,
            ) -> None:
                """异步执行辅助逻辑（on_app_command_error = 原函数名）。

                【中文名称】执行辅助逻辑

                【功能说明】
                这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
                在阅读 `DiscordBotClient.on_app_command_error` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

                【参数说明】
                interaction: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
                error: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

                【返回值】
                返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
                """
                command_name = interaction.command.qualified_name if interaction.command else "?"
                self._channel.logger.warning(
                    "app command failed user={} channel={} cmd={} error={}",
                    interaction.user.id,
                    interaction.channel_id,
                    command_name,
                    error,
                )

        async def send_outbound(self, msg: OutboundMessage) -> None:
            """异步发送消息（send_outbound = 原函数名）。

            【中文名称】发送消息

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient.send_outbound` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            msg: 消息数据，可能来自用户、频道、模型或工具调用。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            channel_id = int(msg.chat_id)

            channel = self._channel._known_channels.get(msg.chat_id) or self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception as e:
                    self._channel.logger.warning("channel {} unavailable: {}", msg.chat_id, e)
                    return

            reference, mention_settings = self._build_reply_context(channel, msg.reply_to)
            sent_media = False
            failed_media: list[str] = []

            for index, media_path in enumerate(msg.media or []):
                if await self._send_file(
                    channel,
                    media_path,
                    reference=reference if index == 0 else None,
                    mention_settings=mention_settings,
                ):
                    sent_media = True
                else:
                    failed_media.append(Path(media_path).name)

            for index, chunk in enumerate(
                self._build_chunks(msg.content or "", failed_media, sent_media)
            ):
                kwargs: dict[str, Any] = {"content": chunk}
                if index == 0 and reference is not None and not sent_media:
                    kwargs["reference"] = reference
                    kwargs["allowed_mentions"] = mention_settings
                await channel.send(**kwargs)

        async def _send_file(
            self,
            channel: Messageable,
            file_path: str,
            *,
            reference: discord.PartialMessage | None,
            mention_settings: discord.AllowedMentions,
        ) -> bool:
            """异步发送消息（_send_file = 原函数名）。

            【中文名称】发送消息

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._send_file` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            file_path: 文件或路径信息，代码会按安全边界读取或写入。
            reference: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            mention_settings: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            path = Path(file_path)
            if not path.is_file():
                self._channel.logger.warning("file not found, skipping: {}", file_path)
                return False

            if path.stat().st_size > MAX_ATTACHMENT_BYTES:
                self._channel.logger.warning("file too large (>20MB), skipping: {}", path.name)
                return False

            try:
                kwargs: dict[str, Any] = {"file": discord.File(path)}
                if reference is not None:
                    kwargs["reference"] = reference
                    kwargs["allowed_mentions"] = mention_settings
                await channel.send(**kwargs)
                self._channel.logger.info("file sent: {}", path.name)
                return True
            except Exception:
                self._channel.logger.exception("Error sending file {}", path.name)
                return False

        @staticmethod
        def _build_chunks(content: str, failed_media: list[str], sent_media: bool) -> list[str]:
            """构建对象（_build_chunks = 原函数名）。

            【中文名称】构建对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._build_chunks` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            failed_media: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            sent_media: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            chunks = split_message(content, MAX_MESSAGE_LEN)
            if chunks or not failed_media or sent_media:
                return chunks
            fallback = "\n".join(f"[attachment: {name} - send failed]" for name in failed_media)
            return split_message(fallback, MAX_MESSAGE_LEN)

        def _build_reply_context(
            self,
            channel: Messageable,
            reply_to: str | None,
        ) -> tuple[discord.PartialMessage | None, discord.AllowedMentions]:
            """构建对象（_build_reply_context = 原函数名）。

            【中文名称】构建对象

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordBotClient._build_reply_context` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            self: 当前对象或类本身，用于访问配置、客户端和共享状态。
            channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
            reply_to: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            mention_settings = discord.AllowedMentions(replied_user=False)
            if not reply_to:
                return None, mention_settings
            try:
                message_id = int(reply_to)
            except ValueError:
                self._channel.logger.warning("Invalid reply target: {}", reply_to)
                return None, mention_settings

            return channel.get_partial_message(message_id), mention_settings


class DiscordChannel(BaseChannel):
    """DiscordChannel 类，封装 渠道适配器 的核心状态和行为。

    【中文名称】DiscordChannel

    【功能说明】
    Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    BaseChannel。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    name = "discord"
    display_name = "Discord"
    _STREAM_EDIT_INTERVAL = 0.8

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        """执行辅助逻辑（default_config = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.default_config` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return DiscordConfig().model_dump(by_alias=True)

    @staticmethod
    def _channel_key(channel_or_id: Any) -> str:
        """执行辅助逻辑（_channel_key = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._channel_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        channel_or_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        channel_id = getattr(channel_or_id, "id", channel_or_id)
        return str(channel_id)

    @classmethod
    def _channel_allow_keys(cls, channel: Any) -> set[str]:
        """执行辅助逻辑（_channel_allow_keys = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._channel_allow_keys` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        keys = {cls._channel_key(channel)}
        if parent_key := cls._channel_parent_key(channel):
            keys.add(parent_key)
        return keys

    @classmethod
    def _channel_parent_key(cls, channel: Any) -> str | None:
        """执行辅助逻辑（_channel_parent_key = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._channel_parent_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        cls: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        parent_id = getattr(channel, "parent_id", None)
        if parent_id is not None:
            return cls._channel_key(parent_id)
        parent = getattr(channel, "parent", None)
        if parent is not None:
            return cls._channel_key(parent)
        return None

    def __init__(self, config: Any, bus: MessageBus):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        config: 配置对象或配置片段，决定该逻辑如何连接外部服务。
        bus: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if isinstance(config, dict):
            config = DiscordConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: DiscordConfig = config
        self._client: DiscordBotClient | None = None
        self._typing_tasks: dict[str, asyncio.Task[None]] = {}
        self._bot_user_id: str | None = None
        self._pending_reactions: dict[str, Any] = {}  # 中文说明：这里描述一次数据形态转换，左边是输入形态，右边是输出形态。
        self._working_emoji_tasks: dict[str, asyncio.Task[None]] = {}
        self._stream_bufs: dict[str, _StreamBuf] = {}
        self._known_channels: dict[str, Any] = {}

    def _remember_channel(self, channel: Any) -> None:
        """执行辅助逻辑（_remember_channel = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._remember_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._known_channels[self._channel_key(channel)] = channel

    def _forget_channel(self, channel_or_id: Any) -> None:
        """执行辅助逻辑（_forget_channel = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._forget_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel_or_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._known_channels.pop(self._channel_key(channel_or_id), None)

    async def start(self) -> None:
        """异步启动流程（start = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.start` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not DISCORD_AVAILABLE:
            self.logger.error("discord.py not installed. Run: pip install nanobot-ai[discord]")
            return

        if not self.config.token:
            self.logger.error("bot token not configured")
            return

        try:
            intents = discord.Intents.none()
            intents.value = self.config.intents

            proxy_auth = None
            has_user = bool(self.config.proxy_username)
            has_pass = bool(self.config.proxy_password)
            if has_user and has_pass:
                import aiohttp

                proxy_auth = aiohttp.BasicAuth(
                    login=self.config.proxy_username,
                    password=self.config.proxy_password,
                )
            elif has_user != has_pass:
                self.logger.warning(
                    "proxy auth incomplete: both proxy_username and "
                    "proxy_password must be set; ignoring partial credentials",
                )

            self._client = DiscordBotClient(
                self,
                intents=intents,
                proxy=self.config.proxy,
                proxy_auth=proxy_auth,
            )
        except Exception:
            self.logger.exception("Failed to initialize client")
            self._client = None
            self._running = False
            return

        self._running = True
        self.logger.info("Starting client via discord.py...")

        try:
            await self._client.start(self.config.token)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("client startup failed")
        finally:
            self._running = False
            await self._reset_runtime_state(close_client=True)

    async def stop(self) -> None:
        """异步停止流程（stop = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.stop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._running = False
        await self._reset_runtime_state(close_client=True)

    async def send(self, msg: OutboundMessage) -> None:
        """异步发送消息（send = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.send` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        msg: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        client = self._client
        if client is None or not client.is_ready():
            self.logger.warning("client not ready; dropping outbound message")
            return

        is_progress = bool((msg.metadata or {}).get("_progress"))

        try:
            await client.send_outbound(msg)
        except Exception:
            self.logger.exception("Error sending message")
            raise
        finally:
            if not is_progress:
                await self._stop_typing(msg.chat_id)
                await self._clear_reactions(msg.chat_id)

    async def send_delta(
        self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """异步发送消息（send_delta = 原函数名）。

        【中文名称】发送消息

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel.send_delta` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        metadata: 结构化数据负载，后续会被解析或转发。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        client = self._client
        if client is None or not client.is_ready():
            self.logger.warning("client not ready; dropping stream delta")
            return

        meta = metadata or {}
        stream_id = meta.get("_stream_id")

        if meta.get("_stream_end"):
            buf = self._stream_bufs.get(chat_id)
            if not buf or buf.message is None or not buf.text:
                return
            if stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id:
                return
            await self._finalize_stream(chat_id, buf)
            return

        buf = self._stream_bufs.get(chat_id)
        if buf is None or (
            stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id
        ):
            buf = _StreamBuf(stream_id=stream_id)
            self._stream_bufs[chat_id] = buf
        elif buf.stream_id is None:
            buf.stream_id = stream_id

        buf.text += delta
        if not buf.text.strip():
            return

        target = await self._resolve_channel(chat_id)
        if target is None:
            self.logger.warning("stream target {} unavailable", chat_id)
            return

        now = time.monotonic()
        if buf.message is None:
            try:
                buf.message = await target.send(content=buf.text)
                buf.last_edit = now
            except Exception as e:
                self.logger.warning("stream initial send failed: {}", e)
                raise
            return

        if (now - buf.last_edit) < self._STREAM_EDIT_INTERVAL:
            return

        try:
            await buf.message.edit(content=DiscordBotClient._build_chunks(buf.text, [], False)[0])
            buf.last_edit = now
        except Exception as e:
            self.logger.warning("stream edit failed: {}", e)
            raise

    async def _handle_discord_message(self, message: discord.Message) -> None:
        """异步处理事件（_handle_discord_message = 原函数名）。

        【中文名称】处理事件

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._handle_discord_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self._bot_user_id is not None and str(message.author.id) == self._bot_user_id:
            return
        if self._is_system_message(message):
            return

        sender_id = str(message.author.id)
        channel_id = self._channel_key(message.channel)
        self._remember_channel(message.channel)
        content = message.content or ""

        if not self._should_accept_inbound(message, sender_id, content):
            return

        media_paths, attachment_markers = await self._download_attachments(message.attachments)
        full_content = self._compose_inbound_content(content, attachment_markers)
        metadata = self._build_inbound_metadata(message)
        parent_channel_id = self._channel_parent_key(message.channel)
        session_key = None
        if parent_channel_id is not None:
            metadata["parent_channel_id"] = parent_channel_id
            metadata["context_chat_id"] = parent_channel_id
            metadata["thread_id"] = channel_id
            session_key = f"{self.name}:{parent_channel_id}:thread:{channel_id}"

        await self._start_typing(message.channel)

        # 中文说明：这一段围绕媒体处理，注意输入、输出和异常路径。
        try:
            await message.add_reaction(self.config.read_receipt_emoji)
            self._pending_reactions[channel_id] = message
        except Exception as e:
            self.logger.debug("Failed to add read receipt reaction: {}", e)

        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        async def _delayed_working_emoji() -> None:
            """异步执行辅助逻辑（_delayed_working_emoji = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordChannel._delayed_working_emoji` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            无显式参数。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            await asyncio.sleep(self.config.working_emoji_delay)
            with suppress(Exception):
                await message.add_reaction(self.config.working_emoji)

        self._working_emoji_tasks[channel_id] = asyncio.create_task(_delayed_working_emoji())

        try:
            await self._handle_message(
                sender_id=sender_id,
                chat_id=channel_id,
                content=full_content,
                media=media_paths,
                metadata=metadata,
                session_key=session_key,
                is_dm=message.guild is None,
            )
        except Exception:
            await self._clear_reactions(channel_id)
            await self._stop_typing(channel_id)
            raise

    async def _on_message(self, message: discord.Message) -> None:
        """异步执行辅助逻辑（_on_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._on_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._handle_discord_message(message)

    async def _resolve_channel(self, chat_id: str) -> Any | None:
        """异步解析目标（_resolve_channel = 原函数名）。

        【中文名称】解析目标

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._resolve_channel` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        client = self._client
        if client is None or not client.is_ready():
            return None
        channel = self._known_channels.get(chat_id)
        if channel is not None:
            return channel
        channel_id = int(chat_id)
        channel = client.get_channel(channel_id)
        if channel is not None:
            return channel
        try:
            return await client.fetch_channel(channel_id)
        except Exception as e:
            self.logger.warning("channel {} unavailable: {}", chat_id, e)
            return None

    async def _finalize_stream(self, chat_id: str, buf: _StreamBuf) -> None:
        """异步流式处理（_finalize_stream = 原函数名）。

        【中文名称】流式处理

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._finalize_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        buf: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        chunks = DiscordBotClient._build_chunks(buf.text, [], False)
        if not chunks:
            self._stream_bufs.pop(chat_id, None)
            return

        try:
            await buf.message.edit(content=chunks[0])
        except Exception as e:
            self.logger.warning("final stream edit failed: {}", e)
            raise

        target = getattr(buf.message, "channel", None) or await self._resolve_channel(chat_id)
        if target is None:
            self.logger.warning("stream follow-up target {} unavailable", chat_id)
            self._stream_bufs.pop(chat_id, None)
            return

        for extra_chunk in chunks[1:]:
            await target.send(content=extra_chunk)

        self._stream_bufs.pop(chat_id, None)
        await self._stop_typing(chat_id)
        await self._clear_reactions(chat_id)

    def _should_accept_inbound(
        self,
        message: discord.Message,
        sender_id: str,
        content: str,
    ) -> bool:
        """执行辅助逻辑（_should_accept_inbound = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._should_accept_inbound` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。
        sender_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.is_allowed(sender_id):
            return False
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        allow_channels = self.config.allow_channels
        if allow_channels:
            channel_ids = self._channel_allow_keys(message.channel)
            if channel_ids.isdisjoint(allow_channels):
                return False
        if message.guild is not None and not self._should_respond_in_group(message, content):
            return False
        return True

    async def _download_attachments(
        self,
        attachments: list[discord.Attachment],
    ) -> tuple[list[str], list[str]]:
        """异步下载资源（_download_attachments = 原函数名）。

        【中文名称】下载资源

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._download_attachments` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        attachments: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        media_paths: list[str] = []
        markers: list[str] = []
        media_dir = get_media_dir("discord")

        for attachment in attachments:
            filename = attachment.filename or "attachment"
            if attachment.size and attachment.size > MAX_ATTACHMENT_BYTES:
                markers.append(f"[attachment: {filename} - too large]")
                continue
            try:
                media_dir.mkdir(parents=True, exist_ok=True)
                safe_name = safe_filename(filename)
                file_path = media_dir / f"{attachment.id}_{safe_name}"
                await attachment.save(file_path)
                media_paths.append(str(file_path))
                markers.append(f"[attachment: {file_path.name}]")
            except Exception as e:
                self.logger.warning("Failed to download attachment: {}", e)
                markers.append(f"[attachment: {filename} - download failed]")

        return media_paths, markers

    @staticmethod
    def _compose_inbound_content(content: str, attachment_markers: list[str]) -> str:
        """执行辅助逻辑（_compose_inbound_content = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._compose_inbound_content` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        attachment_markers: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        content_parts = [content] if content else []
        content_parts.extend(attachment_markers)
        return "\n".join(part for part in content_parts if part) or "[empty message]"

    @staticmethod
    def _is_system_message(message: discord.Message) -> bool:
        """判断条件是否成立（_is_system_message = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._is_system_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        message_type = getattr(message, "type", discord.MessageType.default)
        return message_type not in {discord.MessageType.default, discord.MessageType.reply}

    @staticmethod
    def _build_inbound_metadata(message: discord.Message) -> dict[str, str | None]:
        """构建对象（_build_inbound_metadata = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._build_inbound_metadata` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        reply_to = (
            str(message.reference.message_id)
            if message.reference and message.reference.message_id
            else None
        )
        return {
            "message_id": str(message.id),
            "guild_id": str(message.guild.id) if message.guild else None,
            "reply_to": reply_to,
        }

    def _should_respond_in_group(self, message: discord.Message, content: str) -> bool:
        """执行辅助逻辑（_should_respond_in_group = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._should_respond_in_group` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。
        content: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.config.group_policy == "open":
            return True

        if self.config.group_policy == "mention":
            bot_user_id = self._bot_user_id
            if bot_user_id is None and self._client and self._client.user:
                bot_user_id = str(self._client.user.id)
            if bot_user_id is None:
                self.logger.debug(
                    "message in {} ignored (bot identity unavailable)", message.channel.id
                )
                return False

            if any(str(user.id) == bot_user_id for user in message.mentions):
                return True
            if bot_user_id in {str(user_id) for user_id in getattr(message, "raw_mentions", [])}:
                return True
            if f"<@{bot_user_id}>" in content or f"<@!{bot_user_id}>" in content:
                return True
            if self._references_bot_message(message, bot_user_id):
                return True

            self.logger.debug("message in {} ignored (bot not mentioned)", message.channel.id)
            return False

        return True

    @staticmethod
    def _references_bot_message(message: discord.Message, bot_user_id: str) -> bool:
        """执行辅助逻辑（_references_bot_message = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._references_bot_message` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        message: 消息数据，可能来自用户、频道、模型或工具调用。
        bot_user_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        reference = getattr(message, "reference", None)
        if reference is None:
            return False
        referenced_message = getattr(reference, "resolved", None) or getattr(
            reference, "cached_message", None
        )
        author = getattr(referenced_message, "author", None)
        return str(getattr(author, "id", "")) == bot_user_id

    async def _start_typing(self, channel: Messageable) -> None:
        """异步启动流程（_start_typing = 原函数名）。

        【中文名称】启动流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._start_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        channel_id = self._channel_key(channel)
        await self._stop_typing(channel_id)

        async def typing_loop() -> None:
            """异步执行辅助逻辑（typing_loop = 原函数名）。

            【中文名称】执行辅助逻辑

            【功能说明】
            这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
            在阅读 `DiscordChannel.typing_loop` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

            【参数说明】
            无显式参数。

            【返回值】
            返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
            """
            while self._running:
                try:
                    async with channel.typing():
                        await asyncio.sleep(TYPING_INTERVAL_S)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    self.logger.debug("typing indicator failed for {}: {}", channel_id, e)
                    return

        self._typing_tasks[channel_id] = asyncio.create_task(typing_loop())

    async def _stop_typing(self, channel_id: str) -> None:
        """异步停止流程（_stop_typing = 原函数名）。

        【中文名称】停止流程

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._stop_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        channel_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        task = self._typing_tasks.pop(self._channel_key(channel_id), None)
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _clear_reactions(self, chat_id: str) -> None:
        """异步执行辅助逻辑（_clear_reactions = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._clear_reactions` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        chat_id: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
        task = self._working_emoji_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()

        msg_obj = self._pending_reactions.pop(chat_id, None)
        if msg_obj is None:
            return
        bot_user = self._client.user if self._client else None
        for emoji in (self.config.read_receipt_emoji, self.config.working_emoji):
            with suppress(Exception):
                await msg_obj.remove_reaction(emoji, bot_user)

    async def _cancel_all_typing(self) -> None:
        """异步执行辅助逻辑（_cancel_all_typing = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._cancel_all_typing` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        channel_ids = list(self._typing_tasks)
        for channel_id in channel_ids:
            await self._stop_typing(channel_id)

    async def _reset_runtime_state(self, close_client: bool) -> None:
        """异步执行辅助逻辑（_reset_runtime_state = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 渠道适配器 中的一个关键步骤。Discord 渠道适配器，负责把外部平台消息接入 nanobot，并把 Agent 回复发送回该平台。
        在阅读 `DiscordChannel._reset_runtime_state` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        close_client: 第三方 SDK 或 HTTP 客户端实例。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._cancel_all_typing()
        self._stream_bufs.clear()
        self._known_channels.clear()
        if close_client and self._client is not None and not self._client.is_closed():
            try:
                await self._client.close()
            except Exception as e:
                self.logger.warning("client close failed: {}", e)
        self._client = None
        self._bot_user_id = None

