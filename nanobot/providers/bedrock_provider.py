"""AWS Bedrock Provider 实现。

【中文名称】AWS Bedrock Provider 实现

【功能说明】
负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

from nanobot.providers.base import (
    LLMProvider,
    LLMResponse,
    ToolCallRequest,
    parse_tool_arguments,
    tool_arguments_object_for_replay,
)

_IMAGE_DATA_URL = re.compile(r"^data:image/([a-zA-Z0-9.+-]+);base64,(.*)$", re.DOTALL)
_TEXT_BLOCK_TYPES = {"text", "input_text", "output_text"}
_TEMPERATURE_UNSUPPORTED_MODEL_TOKENS = ("claude-opus-4-7",)
_ADAPTIVE_THINKING_ONLY_MODEL_TOKENS = ("claude-opus-4-7",)
_NOOP_TOOL_NAME = "nanobot_noop"


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """执行 `_deep_merge`。

    【中文名称】_deep_merge

    【功能说明】
    这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - base: 调用方传入的 `base` 数据；具体类型以函数签名为准。
    - override: 调用方传入的 `override` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _next_or_none(iterator: Iterator[dict[str, Any]]) -> dict[str, Any] | None:
    """执行 `_next_or_none`。

    【中文名称】_next_or_none

    【功能说明】
    这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - iterator: 调用方传入的 `iterator` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    try:
        return next(iterator)
    except StopIteration:
        return None


class BedrockProvider(LLMProvider):
    """BedrockProvider 类。

    【中文名称】BedrockProvider

    【功能说明】
    这是 AWS Bedrock Provider 实现 中的核心数据结构或服务类。负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        default_model: str = "bedrock/global.anthropic.claude-opus-4-7",
        *,
        region: str | None = None,
        profile: str | None = None,
        extra_body: dict[str, Any] | None = None,
        client: Any | None = None,
    ):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - api_key: 调用方传入的 `api_key` 数据；具体类型以函数签名为准。
        - api_base: 调用方传入的 `api_base` 数据；具体类型以函数签名为准。
        - default_model: 调用方传入的 `default_model` 数据；具体类型以函数签名为准。
        - region: 调用方传入的 `region` 数据；具体类型以函数签名为准。
        - profile: 调用方传入的 `profile` 数据；具体类型以函数签名为准。
        - extra_body: 调用方传入的 `extra_body` 数据；具体类型以函数签名为准。
        - client: 调用方传入的 `client` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        super().__init__(api_key, api_base)
        self.default_model = default_model
        self.region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        self.profile = profile
        self._extra_body = extra_body or {}
        self._client = client if client is not None else self._make_client()

    def _make_client(self) -> Any:
        """执行 `_make_client`。

        【中文名称】_make_client

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if self.api_key:
            os.environ["AWS_BEARER_TOKEN_BEDROCK"] = self.api_key
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - 这是测试覆盖率工具指令；该分支只在特定可选依赖或平台环境下触发。
            raise RuntimeError(
                "AWS Bedrock provider requires boto3. Install it with `pip install boto3`."
            ) from exc

        session_kwargs: dict[str, Any] = {}
        if self.profile:
            session_kwargs["profile_name"] = self.profile
        session = boto3.Session(**session_kwargs)

        client_kwargs: dict[str, Any] = {}
        if self.region:
            client_kwargs["region_name"] = self.region
        if self.api_base:
            client_kwargs["endpoint_url"] = self.api_base
        return session.client("bedrock-runtime", **client_kwargs)

    @staticmethod
    def _strip_prefix(model: str) -> str:
        """执行 `_strip_prefix`。

        【中文名称】_strip_prefix

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if model.startswith("bedrock/"):
            return model[len("bedrock/"):]
        return model

    @staticmethod
    def _matches_model_token(model: str, tokens: tuple[str, ...]) -> bool:
        """执行 `_matches_model_token`。

        【中文名称】_matches_model_token

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - tokens: 调用方传入的 `tokens` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        model_lower = model.lower()
        return any(token in model_lower for token in tokens)

    @classmethod
    def _supports_temperature(cls, model: str) -> bool:
        """执行 `_supports_temperature`。

        【中文名称】_supports_temperature

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return not cls._matches_model_token(model, _TEMPERATURE_UNSUPPORTED_MODEL_TOKENS)

    @classmethod
    def _uses_adaptive_thinking_only(cls, model: str) -> bool:
        """执行 `_uses_adaptive_thinking_only`。

        【中文名称】_uses_adaptive_thinking_only

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return cls._matches_model_token(model, _ADAPTIVE_THINKING_ONLY_MODEL_TOKENS)

    @staticmethod
    def _image_url_block(block: dict[str, Any]) -> dict[str, Any] | None:
        """执行 `_image_url_block`。

        【中文名称】_image_url_block

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - block: 调用方传入的 `block` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        url = (block.get("image_url") or {}).get("url", "")
        if not isinstance(url, str) or not url:
            return None
        match = _IMAGE_DATA_URL.match(url)
        if not match:
            return {"text": f"(image URL: {url})"}
        fmt = match.group(1).lower()
        if fmt == "jpg":
            fmt = "jpeg"
        try:
            data = base64.b64decode(match.group(2), validate=False)
        except Exception:
            return {"text": "(invalid image data)"}
        return {"image": {"format": fmt, "source": {"bytes": data}}}

    @classmethod
    def _content_blocks(cls, content: Any, *, for_tool_result: bool = False) -> list[dict[str, Any]]:
        """执行 `_content_blocks`。

        【中文名称】_content_blocks

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
        - for_tool_result: 调用方传入的 `for_tool_result` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if isinstance(content, str) or content is None:
            return [{"text": content or "(empty)"}]
        if not isinstance(content, list):
            if for_tool_result and isinstance(content, dict):
                return [{"json": content}]
            return [{"text": str(content)}]

        blocks: list[dict[str, Any]] = []
        for item in content:
            if not isinstance(item, dict):
                blocks.append({"text": str(item)})
                continue

            item_type = item.get("type")
            if item_type in _TEXT_BLOCK_TYPES or "text" in item:
                text = item.get("text")
                if text:
                    blocks.append({"text": str(text)})
                continue
            if item_type == "image_url":
                converted = cls._image_url_block(item)
                if converted:
                    blocks.append(converted)
                continue

            # 说明：这里处理 AWS Bedrock Provider 实现 的协议细节或边界情况，避免外部差异影响核心流程。
            for key in ("text", "image", "document", "video", "json", "searchResult"):
                if key in item:
                    blocks.append({key: item[key]})
                    break
            else:
                blocks.append({"json": item} if for_tool_result else {"text": json.dumps(item)})

        return blocks or [{"text": "(empty)"}]

    @classmethod
    def _system_blocks(cls, content: Any) -> list[dict[str, Any]]:
        """执行 `_system_blocks`。

        【中文名称】_system_blocks

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return [
            block for block in cls._content_blocks(content)
            if "text" in block or "cachePoint" in block or "guardContent" in block
        ]

    @classmethod
    def _tool_result_block(cls, msg: dict[str, Any]) -> dict[str, Any]:
        """执行 `_tool_result_block`。

        【中文名称】_tool_result_block

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return {
            "toolResult": {
                "toolUseId": str(msg.get("tool_call_id") or ""),
                "content": cls._content_blocks(msg.get("content"), for_tool_result=True),
                "status": "success",
            }
        }

    @staticmethod
    def _tool_use_block(tool_call: dict[str, Any]) -> dict[str, Any] | None:
        """执行 `_tool_use_block`。

        【中文名称】_tool_use_block

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - tool_call: 调用方传入的 `tool_call` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        function = tool_call.get("function")
        if not isinstance(function, dict):
            return None
        args = tool_arguments_object_for_replay(function.get("arguments", {}))
        return {
            "toolUse": {
                "toolUseId": str(tool_call.get("id") or ""),
                "name": str(function.get("name") or ""),
                "input": args,
            }
        }

    @staticmethod
    def _reasoning_block(block: dict[str, Any]) -> dict[str, Any] | None:
        """执行 `_reasoning_block`。

        【中文名称】_reasoning_block

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - block: 调用方传入的 `block` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if block.get("type") not in {"thinking", "reasoning", "redacted_thinking"}:
            return None
        text = block.get("thinking") or block.get("text")
        signature = block.get("signature")
        if text and signature:
            return {
                "reasoningContent": {
                    "reasoningText": {"text": str(text), "signature": str(signature)}
                }
            }
        redacted = block.get("redactedContent")
        if redacted is None and isinstance(block.get("redactedContentBase64"), str):
            try:
                redacted = base64.b64decode(block["redactedContentBase64"])
            except Exception:
                redacted = None
        if redacted is not None:
            return {"reasoningContent": {"redactedContent": redacted}}
        return None

    @classmethod
    def _assistant_blocks(cls, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """执行 `_assistant_blocks`。

        【中文名称】_assistant_blocks

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        blocks: list[dict[str, Any]] = []

        for thinking in msg.get("thinking_blocks") or []:
            if isinstance(thinking, dict):
                reasoning = cls._reasoning_block(thinking)
                if reasoning:
                    blocks.append(reasoning)

        content = msg.get("content")
        if isinstance(content, str) and content:
            blocks.append({"text": content})
        elif isinstance(content, list):
            blocks.extend(block for block in cls._content_blocks(content) if "text" in block)

        for tool_call in msg.get("tool_calls") or []:
            if isinstance(tool_call, dict):
                block = cls._tool_use_block(tool_call)
                if block:
                    blocks.append(block)

        return blocks or [{"text": ""}]

    @staticmethod
    def _has_tool_use(msg: dict[str, Any]) -> bool:
        """执行 `_has_tool_use`。

        【中文名称】_has_tool_use

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - msg: 调用方传入的 `msg` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        content = msg.get("content")
        return isinstance(content, list) and any(
            isinstance(block, dict) and "toolUse" in block for block in content
        )

    @staticmethod
    def _merge_consecutive(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """执行 `_merge_consecutive`。

        【中文名称】_merge_consecutive

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        merged: list[dict[str, Any]] = []
        for msg in messages:
            if merged and merged[-1].get("role") == msg.get("role"):
                prev = merged[-1].setdefault("content", [])
                cur = msg.get("content") or []
                if not isinstance(prev, list):
                    prev = [{"text": str(prev)}]
                    merged[-1]["content"] = prev
                if isinstance(cur, list):
                    prev.extend(cur)
                else:
                    prev.append({"text": str(cur)})
            else:
                merged.append(msg)

        last_popped: dict[str, Any] | None = None
        while merged and merged[-1].get("role") == "assistant":
            last_popped = merged.pop()
        if not merged and last_popped is not None and not BedrockProvider._has_tool_use(last_popped):
            merged.append({"role": "user", "content": last_popped.get("content") or [{"text": "(empty)"}]})
        if merged and merged[0].get("role") == "assistant" and not BedrockProvider._has_tool_use(merged[0]):
            merged.insert(0, {"role": "user", "content": [{"text": "(conversation continued)"}]})
        return merged

    def _convert_messages(
        self,
        messages: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """执行 `_convert_messages`。

        【中文名称】_convert_messages

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        system: list[dict[str, Any]] = []
        converted: list[dict[str, Any]] = []

        for msg in messages:
            role = msg.get("role")
            content = msg.get("content")
            if role == "system":
                system.extend(self._system_blocks(content))
                continue
            if role == "tool":
                block = self._tool_result_block(msg)
                if converted and converted[-1].get("role") == "user":
                    converted[-1].setdefault("content", []).append(block)
                else:
                    converted.append({"role": "user", "content": [block]})
                continue
            if role == "assistant":
                converted.append({"role": "assistant", "content": self._assistant_blocks(msg)})
                continue
            if role == "user":
                converted.append({"role": "user", "content": self._content_blocks(content)})

        return system, self._merge_consecutive(converted)

    @staticmethod
    def _convert_tools(tools: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
        """执行 `_convert_tools`。

        【中文名称】_convert_tools

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not tools:
            return None
        result: list[dict[str, Any]] = []
        for tool in tools:
            func = tool.get("function") if isinstance(tool.get("function"), dict) else tool
            if not isinstance(func, dict):
                continue
            name = str(func.get("name") or "")
            if not name:
                continue
            spec: dict[str, Any] = {
                "name": name,
                "inputSchema": {
                    "json": func.get("parameters") or {"type": "object", "properties": {}}
                },
            }
            description = func.get("description")
            if description:
                spec["description"] = str(description)
            strict = func.get("strict", tool.get("strict"))
            if isinstance(strict, bool):
                spec["strict"] = strict
            result.append({"toolSpec": spec})
        return result or None

    @staticmethod
    def _contains_tool_blocks(messages: list[dict[str, Any]]) -> bool:
        """执行 `_contains_tool_blocks`。

        【中文名称】_contains_tool_blocks

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        for msg in messages:
            content = msg.get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if isinstance(block, dict) and ("toolUse" in block or "toolResult" in block):
                    return True
        return False

    @staticmethod
    def _noop_tool() -> dict[str, Any]:
        """执行 `_noop_tool`。

        【中文名称】_noop_tool

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return {
            "toolSpec": {
                "name": _NOOP_TOOL_NAME,
                "description": "Internal placeholder for Bedrock tool history validation.",
                "inputSchema": {"json": {"type": "object", "properties": {}}},
            }
        }

    @staticmethod
    def _convert_tool_choice(
        tool_choice: str | dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """执行 `_convert_tool_choice`。

        【中文名称】_convert_tool_choice

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if tool_choice is None or tool_choice == "auto":
            return {"auto": {}}
        if tool_choice == "required":
            return {"any": {}}
        if tool_choice == "none":
            return None
        if isinstance(tool_choice, dict):
            name = tool_choice.get("function", {}).get("name")
            if name:
                return {"tool": {"name": str(name)}}
        return {"auto": {}}

    @staticmethod
    def _adaptive_thinking(reasoning_effort: str | None) -> dict[str, Any] | None:
        """执行 `_adaptive_thinking`。

        【中文名称】_adaptive_thinking

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not reasoning_effort:
            return None
        effort = reasoning_effort.lower()
        if effort == "none":
            return None
        thinking: dict[str, Any] = {"type": "adaptive"}
        if effort != "adaptive":
            thinking["effort"] = effort
        return thinking

    def _build_kwargs(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
        model: str | None,
        max_tokens: int,
        temperature: float,
        reasoning_effort: str | None,
        tool_choice: str | dict[str, Any] | None,
    ) -> dict[str, Any]:
        """执行 `_build_kwargs`。

        【中文名称】_build_kwargs

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        model_id = self._strip_prefix(model or self.default_model)
        system, bedrock_messages = self._convert_messages(self._sanitize_empty_content(messages))
        if not bedrock_messages:
            bedrock_messages = [{"role": "user", "content": [{"text": "(empty)"}]}]

        kwargs: dict[str, Any] = {
            "modelId": model_id,
            "messages": bedrock_messages,
            "inferenceConfig": {"maxTokens": max(1, max_tokens)},
        }
        if system:
            kwargs["system"] = system
        if self._supports_temperature(model_id):
            kwargs["inferenceConfig"]["temperature"] = temperature

        additional: dict[str, Any] = {}
        if self._uses_adaptive_thinking_only(model_id):
            thinking = self._adaptive_thinking(reasoning_effort)
            if thinking:
                additional["thinking"] = thinking
        if self._extra_body:
            additional = _deep_merge(additional, self._extra_body)
        if additional:
            kwargs["additionalModelRequestFields"] = additional

        bedrock_tools = self._convert_tools(tools)
        tool_config: dict[str, Any] | None = None
        if bedrock_tools:
            tool_config = {"tools": bedrock_tools}
            choice = self._convert_tool_choice(tool_choice)
            if choice:
                tool_config["toolChoice"] = choice
        elif self._contains_tool_blocks(bedrock_messages):
            tool_config = {"tools": [self._noop_tool()]}

        if tool_config:
            kwargs["toolConfig"] = tool_config

        return kwargs

    @staticmethod
    def _finish_reason(stop_reason: str | None) -> str:
        """执行 `_finish_reason`。

        【中文名称】_finish_reason

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - stop_reason: 调用方传入的 `stop_reason` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return {
            "end_turn": "stop",
            "tool_use": "tool_calls",
            "max_tokens": "length",
        }.get(stop_reason or "", stop_reason or "stop")

    @staticmethod
    def _usage(usage: dict[str, Any] | None) -> dict[str, int]:
        """执行 `_usage`。

        【中文名称】_usage

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - usage: 调用方传入的 `usage` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if not usage:
            return {}
        prompt = int(usage.get("inputTokens") or 0)
        completion = int(usage.get("outputTokens") or 0)
        total = int(usage.get("totalTokens") or prompt + completion)
        result = {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": total,
        }
        cache_read = int(usage.get("cacheReadInputTokens") or 0)
        cache_write = int(usage.get("cacheWriteInputTokens") or 0)
        if cache_read:
            result["cached_tokens"] = cache_read
            result["cache_read_input_tokens"] = cache_read
        if cache_write:
            result["cache_creation_input_tokens"] = cache_write
        return result

    @staticmethod
    def _parse_reasoning(block: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
        """执行 `_parse_reasoning`。

        【中文名称】_parse_reasoning

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - block: 调用方传入的 `block` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        reasoning = block.get("reasoningContent")
        if not isinstance(reasoning, dict):
            return None, None
        text_obj = reasoning.get("reasoningText")
        if isinstance(text_obj, dict):
            text = text_obj.get("text")
            if isinstance(text, str):
                return text, {
                    "type": "thinking",
                    "thinking": text,
                    "signature": text_obj.get("signature", ""),
                }
        redacted = reasoning.get("redactedContent")
        if redacted is not None:
            if isinstance(redacted, (bytes, bytearray)):
                encoded = base64.b64encode(bytes(redacted)).decode("ascii")
                return None, {"type": "redacted_thinking", "redactedContentBase64": encoded}
            return None, {"type": "redacted_thinking", "redactedContent": redacted}
        return None, None

    @classmethod
    def _parse_response(cls, response: dict[str, Any]) -> LLMResponse:
        """执行 `_parse_response`。

        【中文名称】_parse_response

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - response: 调用方传入的 `response` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        tool_calls: list[ToolCallRequest] = []
        thinking_blocks: list[dict[str, Any]] = []
        message = (response.get("output") or {}).get("message") or {}

        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            if isinstance(block.get("text"), str):
                content_parts.append(block["text"])
            tool_use = block.get("toolUse")
            if isinstance(tool_use, dict):
                arguments = tool_use.get("input", {})
                tool_calls.append(ToolCallRequest(
                    id=str(tool_use.get("toolUseId") or ""),
                    name=str(tool_use.get("name") or ""),
                    arguments=arguments,
                ))
            reasoning_text, thinking = cls._parse_reasoning(block)
            if reasoning_text:
                reasoning_parts.append(reasoning_text)
            if thinking:
                thinking_blocks.append(thinking)

        return LLMResponse(
            content="".join(content_parts) or None,
            tool_calls=tool_calls,
            finish_reason=cls._finish_reason(response.get("stopReason")),
            usage=cls._usage(response.get("usage")),
            reasoning_content="".join(reasoning_parts) or None,
            thinking_blocks=thinking_blocks or None,
        )

    @classmethod
    def _parse_stream_event(
        cls,
        event: dict[str, Any],
        *,
        content_parts: list[str],
        reasoning_parts: list[str],
        thinking_blocks: list[dict[str, Any]],
        tool_buffers: dict[int, dict[str, Any]],
        state: dict[str, Any],
    ) -> str | None:
        """执行 `_parse_stream_event`。

        【中文名称】_parse_stream_event

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - event: 调用方传入的 `event` 数据；具体类型以函数签名为准。
        - content_parts: 调用方传入的 `content_parts` 数据；具体类型以函数签名为准。
        - reasoning_parts: 调用方传入的 `reasoning_parts` 数据；具体类型以函数签名为准。
        - thinking_blocks: 调用方传入的 `thinking_blocks` 数据；具体类型以函数签名为准。
        - tool_buffers: 调用方传入的 `tool_buffers` 数据；具体类型以函数签名为准。
        - state: 调用方传入的 `state` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        if "contentBlockStart" in event:
            data = event["contentBlockStart"]
            idx = int(data.get("contentBlockIndex") or 0)
            start = data.get("start") or {}
            tool_use = start.get("toolUse")
            if isinstance(tool_use, dict):
                tool_buffers[idx] = {
                    "id": str(tool_use.get("toolUseId") or ""),
                    "name": str(tool_use.get("name") or ""),
                    "input": "",
                }
            return None

        if "contentBlockDelta" in event:
            data = event["contentBlockDelta"]
            idx = int(data.get("contentBlockIndex") or 0)
            delta = data.get("delta") or {}
            text = delta.get("text")
            if isinstance(text, str):
                content_parts.append(text)
                return text
            tool_delta = delta.get("toolUse")
            if isinstance(tool_delta, dict):
                buf = tool_buffers.setdefault(idx, {"id": "", "name": "", "input": ""})
                if isinstance(tool_delta.get("input"), str):
                    buf["input"] += tool_delta["input"]
            reasoning = delta.get("reasoningContent")
            if isinstance(reasoning, dict):
                buf = state.setdefault("reasoning_buffers", {}).setdefault(
                    idx, {"text": "", "signature": "", "redactedContent": None}
                )
                if isinstance(reasoning.get("text"), str):
                    buf["text"] += reasoning["text"]
                    reasoning_parts.append(reasoning["text"])
                if isinstance(reasoning.get("signature"), str):
                    buf["signature"] = reasoning["signature"]
                if reasoning.get("redactedContent") is not None:
                    buf["redactedContent"] = reasoning["redactedContent"]
            return None

        if "contentBlockStop" in event:
            idx = int((event["contentBlockStop"] or {}).get("contentBlockIndex") or 0)
            reasoning_buf = state.setdefault("reasoning_buffers", {}).pop(idx, None)
            if reasoning_buf:
                if reasoning_buf.get("text"):
                    thinking_blocks.append({
                        "type": "thinking",
                        "thinking": reasoning_buf["text"],
                        "signature": reasoning_buf.get("signature", ""),
                    })
                elif reasoning_buf.get("redactedContent") is not None:
                    redacted = reasoning_buf["redactedContent"]
                    if isinstance(redacted, (bytes, bytearray)):
                        redacted_block = {
                            "type": "redacted_thinking",
                            "redactedContentBase64": base64.b64encode(bytes(redacted)).decode("ascii"),
                        }
                    else:
                        redacted_block = {
                            "type": "redacted_thinking",
                            "redactedContent": redacted,
                        }
                    thinking_blocks.append({
                        **redacted_block,
                    })
            return None

        if "messageStop" in event:
            state["stop_reason"] = (event["messageStop"] or {}).get("stopReason")
            return None

        if "metadata" in event:
            metadata = event["metadata"] or {}
            if isinstance(metadata.get("usage"), dict):
                state["usage"] = metadata["usage"]
            return None

        return None

    @classmethod
    def _stream_result(
        cls,
        *,
        content_parts: list[str],
        reasoning_parts: list[str],
        thinking_blocks: list[dict[str, Any]],
        tool_buffers: dict[int, dict[str, Any]],
        state: dict[str, Any],
    ) -> LLMResponse:
        """执行 `_stream_result`。

        【中文名称】_stream_result

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - content_parts: 调用方传入的 `content_parts` 数据；具体类型以函数签名为准。
        - reasoning_parts: 调用方传入的 `reasoning_parts` 数据；具体类型以函数签名为准。
        - thinking_blocks: 调用方传入的 `thinking_blocks` 数据；具体类型以函数签名为准。
        - tool_buffers: 调用方传入的 `tool_buffers` 数据；具体类型以函数签名为准。
        - state: 调用方传入的 `state` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        tool_calls: list[ToolCallRequest] = []
        for buf in tool_buffers.values():
            args: Any = {}
            if buf.get("input"):
                args = parse_tool_arguments(buf["input"])
            tool_calls.append(ToolCallRequest(
                id=buf.get("id") or "",
                name=buf.get("name") or "",
                arguments=args,
            ))
        return LLMResponse(
            content="".join(content_parts) or None,
            tool_calls=tool_calls,
            finish_reason=cls._finish_reason(state.get("stop_reason")),
            usage=cls._usage(state.get("usage")),
            reasoning_content="".join(reasoning_parts) or None,
            thinking_blocks=thinking_blocks or None,
        )

    @classmethod
    def _handle_error(cls, e: Exception) -> LLMResponse:
        """执行 `_handle_error`。

        【中文名称】_handle_error

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - e: 调用方传入的 `e` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        response = getattr(e, "response", None)
        metadata = response.get("ResponseMetadata", {}) if isinstance(response, dict) else {}
        headers = metadata.get("HTTPHeaders") if isinstance(metadata, dict) else None
        error_obj = response.get("Error", {}) if isinstance(response, dict) else {}
        message = error_obj.get("Message") if isinstance(error_obj, dict) else None
        code = error_obj.get("Code") if isinstance(error_obj, dict) else None
        status_code = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        body = message or str(e)
        retry_after = cls._extract_retry_after_from_headers(headers)
        if retry_after is None:
            retry_after = cls._extract_retry_after(body)

        error_name = e.__class__.__name__.lower()
        error_kind = None
        if "timeout" in error_name:
            error_kind = "timeout"
        elif "connection" in error_name or "endpoint" in error_name:
            error_kind = "connection"

        code_text = str(code or "").lower()
        should_retry = None
        if status_code is not None:
            should_retry = int(status_code) == 429 or int(status_code) >= 500
        if any(token in code_text for token in ("throttl", "timeout", "unavailable", "modelnotready")):
            should_retry = True

        return LLMResponse(
            content=f"Error: {str(body).strip()[:500]}",
            finish_reason="error",
            retry_after=retry_after,
            error_status_code=int(status_code) if status_code is not None else None,
            error_kind=error_kind,
            error_type=code_text or None,
            error_code=code_text or None,
            error_retry_after_s=retry_after,
            error_should_retry=should_retry,
        )

    async def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
    ) -> LLMResponse:
        """异步执行 `chat`。

        【中文名称】chat

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        try:
            kwargs = self._build_kwargs(
                messages, tools, model, max_tokens, temperature, reasoning_effort, tool_choice
            )
            response = await asyncio.to_thread(self._client.converse, **kwargs)
            return self._parse_response(response)
        except Exception as e:
            return self._handle_error(e)

    async def chat_stream(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, Any] | None = None,
        on_content_delta: Callable[[str], Awaitable[None]] | None = None,
        on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
        on_tool_call_delta: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> LLMResponse:
        """异步执行 `chat_stream`。

        【中文名称】chat_stream

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - messages: 调用方传入的 `messages` 数据；具体类型以函数签名为准。
        - tools: 调用方传入的 `tools` 数据；具体类型以函数签名为准。
        - model: 调用方传入的 `model` 数据；具体类型以函数签名为准。
        - max_tokens: 调用方传入的 `max_tokens` 数据；具体类型以函数签名为准。
        - temperature: 调用方传入的 `temperature` 数据；具体类型以函数签名为准。
        - reasoning_effort: 调用方传入的 `reasoning_effort` 数据；具体类型以函数签名为准。
        - tool_choice: 调用方传入的 `tool_choice` 数据；具体类型以函数签名为准。
        - on_content_delta: 调用方传入的 `on_content_delta` 数据；具体类型以函数签名为准。
        - on_thinking_delta: 调用方传入的 `on_thinking_delta` 数据；具体类型以函数签名为准。
        - on_tool_call_delta: 调用方传入的 `on_tool_call_delta` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        _ = on_thinking_delta, on_tool_call_delta
        idle_timeout_s = int(os.environ.get("NANOBOT_STREAM_IDLE_TIMEOUT_S", "90"))
        content_parts: list[str] = []
        reasoning_parts: list[str] = []
        thinking_blocks: list[dict[str, Any]] = []
        tool_buffers: dict[int, dict[str, Any]] = {}
        state: dict[str, Any] = {}

        try:
            kwargs = self._build_kwargs(
                messages, tools, model, max_tokens, temperature, reasoning_effort, tool_choice
            )
            response = await asyncio.to_thread(self._client.converse_stream, **kwargs)
            stream = iter(response.get("stream") or [])
            while True:
                event = await asyncio.wait_for(
                    asyncio.to_thread(_next_or_none, stream),
                    timeout=idle_timeout_s,
                )
                if event is None:
                    break
                delta = self._parse_stream_event(
                    event,
                    content_parts=content_parts,
                    reasoning_parts=reasoning_parts,
                    thinking_blocks=thinking_blocks,
                    tool_buffers=tool_buffers,
                    state=state,
                )
                if delta and on_content_delta:
                    await on_content_delta(delta)
            return self._stream_result(
                content_parts=content_parts,
                reasoning_parts=reasoning_parts,
                thinking_blocks=thinking_blocks,
                tool_buffers=tool_buffers,
                state=state,
            )
        except asyncio.TimeoutError:
            return LLMResponse(
                content=(
                    f"Error calling LLM: stream stalled for more than "
                    f"{idle_timeout_s} seconds"
                ),
                finish_reason="error",
                error_kind="timeout",
            )
        except Exception as e:
            return self._handle_error(e)

    def get_default_model(self) -> str:
        """执行 `get_default_model`。

        【中文名称】get_default_model

        【功能说明】
        这是 AWS Bedrock Provider 实现 中的一个步骤函数，用来支撑：负责使用 boto3 调用 Bedrock Converse/ConverseStream，把 AWS 响应转换成统一 LLMResponse。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        return self.default_model
