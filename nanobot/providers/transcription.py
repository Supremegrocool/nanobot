"""音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/transcription.py

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

import asyncio
import base64
import json
import mimetypes
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
from loguru import logger

_CHAT_COMPLETIONS_PATH = "chat/completions"
_TRANSCRIPTIONS_PATH = "audio/transcriptions"
_STEPFUN_ASR_PATH = "audio/asr/sse"
_ASSEMBLYAI_DEFAULT_API_BASE = "https://api.assemblyai.com/v2"
_ASSEMBLYAI_POLL_ATTEMPTS = 60
_ASSEMBLYAI_POLL_INTERVAL_S = 2.0
_AUDIO_MIME_OVERRIDES = {
    ".m4a": "audio/mp4",
    ".mpga": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".opus": "audio/ogg",
    ".wav": "audio/wav",
    ".weba": "audio/webm",
    ".webm": "audio/webm",
}
_FORMAT_ALIASES = {
    "oga": "ogg",
    "opus": "ogg",
    "mpga": "mp3",
    "mpeg": "mp3",
    "mp4": "m4a",
}


def _resolve_transcription_url(api_base: str | None, default_url: str) -> str:
    """解析目标（_resolve_transcription_url = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_transcription_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    default_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not api_base:
        return default_url
    base = api_base.rstrip("/")
    if base.endswith(_TRANSCRIPTIONS_PATH):
        return base
    return f"{base}/{_TRANSCRIPTIONS_PATH}"


def _resolve_chat_completions_url(api_base: str | None, default_url: str) -> str:
    """解析目标（_resolve_chat_completions_url = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_chat_completions_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    default_url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    if not api_base:
        return default_url
    base = api_base.rstrip("/")
    if base.endswith(_CHAT_COMPLETIONS_PATH):
        return base
    return f"{base}/{_CHAT_COMPLETIONS_PATH}"


def _resolve_api_path(api_base: str | None, default_base: str, path: str) -> str:
    """解析目标（_resolve_api_path = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_api_path` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    default_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    base = (api_base or default_base).rstrip("/")
    return f"{base}/{path.lstrip('/')}"


def _resolve_stepfun_asr_url(api_base: str | None) -> str:
    """解析目标（_resolve_stepfun_asr_url = 原函数名）。

    【中文名称】解析目标

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_resolve_stepfun_asr_url` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    base = (api_base or "https://api.stepfun.com/v1").rstrip("/")
    if base.endswith(_STEPFUN_ASR_PATH):
        return base
    return f"{base}/{_STEPFUN_ASR_PATH}"


def _audio_mime_type(path: Path) -> str:
    """执行辅助逻辑（_audio_mime_type = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_audio_mime_type` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return (
        _AUDIO_MIME_OVERRIDES.get(path.suffix.lower())
        or mimetypes.guess_type(path.name)[0]
        or "application/octet-stream"
    )


def _audio_format(path: Path) -> str:
    """格式化内容（_audio_format = 原函数名）。

    【中文名称】格式化内容

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_audio_format` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    path: 文件或路径信息，代码会按安全边界读取或写入。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    ext = path.suffix.lstrip(".").lower()
    return _FORMAT_ALIASES.get(ext, ext)


# 最多重试 3 次（共 4 次尝试），并采用指数退避。
# 语音转写接口在高负载或移动网络环境下经常出现瞬时失败；
# 如果完全不重试，一条语音消息很容易直接变成空字符串。
_MAX_RETRIES = 3
_BACKOFF_S = (1.0, 2.0, 4.0)
_RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
_RETRYABLE_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.WriteError,
    httpx.RemoteProtocolError,
)


async def _request_json_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    provider_label: str,
    **kwargs: object,
) -> dict[str, Any] | None:
    """异步执行辅助逻辑（_request_json_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_request_json_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    client: 第三方 SDK 或 HTTP 客户端实例。
    method: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    **kwargs: 额外关键字参数，通常向下透传给 SDK 或工具函数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    for attempt in range(_MAX_RETRIES + 1):
        try:
            request = getattr(client, method.lower(), None)
            if request is None:
                response = await client.request(method, url, **kwargs)
            else:
                response = await request(url, **kwargs)
        except _RETRYABLE_EXCEPTIONS as e:
            if attempt < _MAX_RETRIES:
                logger.warning(
                    "{} transcription transient error (attempt {}/{}): {}",
                    provider_label,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                    e,
                )
                await asyncio.sleep(_BACKOFF_S[attempt])
                continue
            logger.exception(
                "{} transcription error after {} attempts: {}",
                provider_label,
                _MAX_RETRIES + 1,
                e,
            )
            return None
        except Exception as e:
            logger.exception("{} transcription error: {}", provider_label, e)
            return None

        if response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
            logger.warning(
                "{} transcription transient HTTP {} (attempt {}/{})",
                provider_label,
                response.status_code,
                attempt + 1,
                _MAX_RETRIES + 1,
            )
            await asyncio.sleep(_BACKOFF_S[attempt])
            continue

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            body = response.text.strip().replace("\n", " ")[:500]
            logger.error(
                "{} transcription HTTP {}{}{}",
                provider_label,
                response.status_code,
                f" {response.reason_phrase}" if response.reason_phrase else "",
                f": {body}" if body else "",
            )
            return None
        except Exception as e:
            logger.exception("{} transcription error: {}", provider_label, e)
            return None

        try:
            payload = response.json()
        except Exception as e:
            logger.exception(
                "{} transcription error: malformed response body: {}",
                provider_label,
                e,
            )
            return None
        if not isinstance(payload, dict):
            logger.error(
                "{} transcription error: unexpected response shape: {!r}",
                provider_label,
                type(payload).__name__,
            )
            return None
        return payload
    return None


async def _post_transcription_with_retry(
    url: str,
    *,
    api_key: str | None,
    path: Path,
    model: str,
    provider_label: str,
    language: str | None = None,
) -> str:
    """异步执行辅助逻辑（_post_transcription_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_post_transcription_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        data = path.read_bytes()
    except OSError as e:
        logger.exception("{} transcription error: cannot read audio file: {}", provider_label, e)
        return ""
    headers = {"Authorization": f"Bearer {api_key}"}

    def build_request() -> dict[str, Any]:
        """构建对象（build_request = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `build_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        无显式参数。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        files = {
            "file": (path.name, data, _audio_mime_type(path)),
            "model": (None, model),
        }
        if language:
            files["language"] = (None, language)
        return {"url": url, "headers": headers, "files": files, "timeout": 60.0}

    return await _post_with_retry(build_request, provider_label, _text_from_transcription_payload)


async def _post_json_transcription_with_retry(
    url: str,
    *,
    api_key: str | None,
    path: Path,
    model: str,
    provider_label: str,
    language: str | None = None,
) -> str:
    """异步执行辅助逻辑（_post_json_transcription_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_post_json_transcription_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        data = path.read_bytes()
    except OSError as e:
        logger.exception("{} transcription error: cannot read audio file: {}", provider_label, e)
        return ""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def build_request() -> dict[str, Any]:
        """构建对象（build_request = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `build_request` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        无显式参数。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        body: dict[str, object] = {
            "model": model,
            "input_audio": {
                "data": base64.b64encode(data).decode(),
                "format": _audio_format(path),
            },
        }
        if language:
            body["language"] = language
        return {"url": url, "headers": headers, "json": body, "timeout": 60.0}

    return await _post_with_retry(build_request, provider_label, _text_from_transcription_payload)


async def _post_xiaomi_mimo_asr_with_retry(
    url: str,
    *,
    api_key: str | None,
    path: Path,
    model: str,
    provider_label: str,
    language: str | None = None,
) -> str:
    """异步执行辅助逻辑（_post_xiaomi_mimo_asr_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_post_xiaomi_mimo_asr_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        data = path.read_bytes()
    except OSError as e:
        logger.exception("{} transcription error: cannot read audio file: {}", provider_label, e)
        return ""

    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": (
                                f"data:{_audio_mime_type(path)};base64,"
                                f"{base64.b64encode(data).decode('ascii')}"
                            ),
                        },
                    }
                ],
            }
        ],
    }
    if language:
        body["asr_options"] = {"language": language}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    def build_request() -> dict[str, Any]:
        return {"url": url, "headers": headers, "json": body, "timeout": 60.0}

    return await _post_with_retry(build_request, provider_label, _text_from_chat_payload)


async def _post_stepfun_asr_with_retry(
    url: str,
    *,
    api_key: str | None,
    path: Path,
    model: str,
    provider_label: str,
    language: str | None = None,
) -> str:
    """异步执行辅助逻辑（_post_stepfun_asr_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_post_stepfun_asr_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    url: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    path: 文件或路径信息，代码会按安全边界读取或写入。
    model: 模型名称或模型配置，用于选择具体 LLM 能力。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        data = path.read_bytes()
    except OSError as e:
        logger.exception("{} transcription error: cannot read audio file: {}", provider_label, e)
        return ""

    suffix = path.suffix.lstrip(".").lower()
    audio_type = suffix if suffix in ("ogg", "mp3", "wav", "pcm") else "wav"

    body: dict[str, Any] = {
        "audio": {
            "data": base64.b64encode(data).decode("ascii"),
            "input": {
                "transcription": {
                    "model": model,
                    "enable_itn": True,
                },
                "format": {"type": audio_type},
            },
        },
    }
    if language:
        body["audio"]["input"]["transcription"]["language"] = language

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }

    async with httpx.AsyncClient() as client:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                async with client.stream(
                    "POST", url, headers=headers, json=body, timeout=60.0
                ) as resp:
                    if resp.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                        logger.warning(
                            "{} transcription transient HTTP {} (attempt {}/{})",
                            provider_label,
                            resp.status_code,
                            attempt + 1,
                            _MAX_RETRIES + 1,
                        )
                        await asyncio.sleep(_BACKOFF_S[attempt])
                        continue
                    resp.raise_for_status()
                    final_text = None
                    async for line in resp.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        payload_str = line[len("data:") :].strip()
                        if not payload_str:
                            continue
                        try:
                            payload = json.loads(payload_str)
                        except (json.JSONDecodeError, ValueError):
                            continue
                        event_type = payload.get("type", "")
                        if event_type == "error":
                            msg = payload.get("message", "unknown error")
                            logger.error("{} ASR error: {}", provider_label, msg)
                            return ""
                        if event_type == "transcript.text.done":
                            final_text = payload.get("text", "")
                            break
                    if final_text is not None:
                        return final_text
                    # 流提前结束但没有 final event，若还有次数就重试。
                    if attempt < _MAX_RETRIES:
                        logger.warning(
                            "{} transcription: no final event (attempt {}/{})",
                            provider_label,
                            attempt + 1,
                            _MAX_RETRIES + 1,
                        )
                        await asyncio.sleep(_BACKOFF_S[attempt])
                        continue
                    logger.error(
                        "{} transcription: stream ended without final text after {} attempts",
                        provider_label,
                        _MAX_RETRIES + 1,
                    )
                    return ""
            except httpx.HTTPStatusError as e:
                if e.response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                    await asyncio.sleep(_BACKOFF_S[attempt])
                    continue
                logger.error(
                    "{} transcription HTTP {}{}",
                    provider_label,
                    e.response.status_code,
                    f" {e.response.reason_phrase}" if e.response.reason_phrase else "",
                )
                return ""
            except (httpx.RequestError, Exception):
                if attempt < _MAX_RETRIES:
                    await asyncio.sleep(_BACKOFF_S[attempt])
                    continue
                logger.exception("{} transcription request error", provider_label)
                return ""
    return ""


async def _post_with_retry(
    build_request: Callable[[], dict[str, Any]],
    provider_label: str,
    extract_text: Callable[[dict[str, Any]], str],
) -> str:
    """异步执行辅助逻辑（_post_with_retry = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_post_with_retry` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    build_request: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    provider_label: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    extract_text: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    async with httpx.AsyncClient() as client:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                response = await client.post(**build_request())
            except _RETRYABLE_EXCEPTIONS as e:
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        "{} transcription transient error (attempt {}/{}): {}",
                        provider_label,
                        attempt + 1,
                        _MAX_RETRIES + 1,
                        e,
                    )
                    await asyncio.sleep(_BACKOFF_S[attempt])
                    continue
                logger.exception(
                    "{} transcription error after {} attempts: {}",
                    provider_label,
                    _MAX_RETRIES + 1,
                    e,
                )
                return ""
            except Exception as e:
                logger.exception("{} transcription error: {}", provider_label, e)
                return ""

            if response.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
                logger.warning(
                    "{} transcription transient HTTP {} (attempt {}/{})",
                    provider_label,
                    response.status_code,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                )
                await asyncio.sleep(_BACKOFF_S[attempt])
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError:
                body = response.text.strip().replace("\n", " ")[:500]
                logger.error(
                    "{} transcription HTTP {}{}{}",
                    provider_label,
                    response.status_code,
                    f" {response.reason_phrase}" if response.reason_phrase else "",
                    f": {body}" if body else "",
                )
                return ""
            except Exception as e:
                logger.exception("{} transcription error: {}", provider_label, e)
                return ""

            try:
                payload = response.json()
            except Exception as e:
                logger.exception(
                    "{} transcription error: malformed response body: {}",
                    provider_label,
                    e,
                )
                return ""
            if not isinstance(payload, dict):
                logger.error(
                    "{} transcription error: unexpected response shape: {!r}",
                    provider_label,
                    type(payload).__name__,
                )
                return ""
            return extract_text(payload)
    return ""


def _text_from_transcription_payload(payload: dict[str, Any]) -> str:
    """执行辅助逻辑（_text_from_transcription_payload = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_text_from_transcription_payload` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    payload: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    text = payload.get("text")
    return text if isinstance(text, str) else ""


def _text_from_chat_payload(payload: dict[str, Any]) -> str:
    """执行辅助逻辑（_text_from_chat_payload = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_text_from_chat_payload` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    payload: 结构化数据负载，后续会被解析或转发。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    try:
        text = payload["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return ""
    return text if isinstance(text, str) else ""


def _assemblyai_speech_models(model: str | None) -> list[str]:
    return [part for part in (part.strip() for part in (model or "").split(",")) if part]


class AssemblyAITranscriptionProvider:
    """AssemblyAITranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】AssemblyAITranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AssemblyAITranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        base = api_base or os.environ.get("ASSEMBLYAI_BASE_URL")
        self.api_key = api_key or os.environ.get("ASSEMBLYAI_API_KEY")
        self.upload_url = _resolve_api_path(base, _ASSEMBLYAI_DEFAULT_API_BASE, "upload")
        self.transcript_url = _resolve_api_path(base, _ASSEMBLYAI_DEFAULT_API_BASE, "transcript")
        self.language = language or None
        self.model = model or "universal-3-pro,universal-2"
        logger.debug("AssemblyAI transcription endpoint: {}", self.transcript_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `AssemblyAITranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("AssemblyAI API key not configured for transcription")
            return ""
        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""
        try:
            data = path.read_bytes()
        except OSError as e:
            logger.exception("AssemblyAI transcription error: cannot read audio file: {}", e)
            return ""

        headers = {"Authorization": self.api_key}
        async with httpx.AsyncClient() as client:
            upload = await _request_json_with_retry(
                client,
                "POST",
                self.upload_url,
                provider_label="AssemblyAI",
                headers={**headers, "Content-Type": "application/octet-stream"},
                content=data,
                timeout=60.0,
            )
            upload_url = upload.get("upload_url") if upload else None
            if not isinstance(upload_url, str) or not upload_url:
                logger.error("AssemblyAI transcription error: upload_url missing")
                return ""

            body: dict[str, object] = {"audio_url": upload_url}
            speech_models = _assemblyai_speech_models(self.model)
            if speech_models:
                body["speech_models"] = speech_models
            if self.language:
                body["language_code"] = self.language

            transcript = await _request_json_with_retry(
                client,
                "POST",
                self.transcript_url,
                provider_label="AssemblyAI",
                headers=headers,
                json=body,
                timeout=30.0,
            )
            transcript_id = transcript.get("id") if transcript else None
            if not isinstance(transcript_id, str) or not transcript_id:
                logger.error("AssemblyAI transcription error: transcript id missing")
                return ""

            poll_url = f"{self.transcript_url.rstrip('/')}/{transcript_id}"
            for attempt in range(_ASSEMBLYAI_POLL_ATTEMPTS):
                payload = await _request_json_with_retry(
                    client,
                    "GET",
                    poll_url,
                    provider_label="AssemblyAI",
                    headers=headers,
                    timeout=30.0,
                )
                if not payload:
                    return ""
                status = str(payload.get("status") or "").lower()
                if status == "completed":
                    text = payload.get("text")
                    return text if isinstance(text, str) else ""
                if status in {"error", "failed"}:
                    logger.error(
                        "AssemblyAI transcription failed: {}",
                        payload.get("error") or payload,
                    )
                    return ""
                if attempt < _ASSEMBLYAI_POLL_ATTEMPTS - 1:
                    await asyncio.sleep(_ASSEMBLYAI_POLL_INTERVAL_S)
            logger.error("AssemblyAI transcription timed out while polling transcript")
            return ""


class OpenAITranscriptionProvider:
    """OpenAITranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】OpenAITranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAITranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.api_url = _resolve_transcription_url(
            api_base or os.environ.get("OPENAI_TRANSCRIPTION_BASE_URL"),
            "https://api.openai.com/v1/audio/transcriptions",
        )
        self.language = language or None
        self.model = model or "whisper-1"
        logger.debug("OpenAI transcription endpoint: {}", self.api_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenAITranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("OpenAI API key not configured for transcription")
            return ""
        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""
        return await _post_transcription_with_retry(
            self.api_url,
            api_key=self.api_key,
            path=path,
            model=self.model,
            provider_label="OpenAI",
            language=self.language,
        )


class GroqTranscriptionProvider:
    """GroqTranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】GroqTranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GroqTranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        self.api_url = _resolve_transcription_url(
            api_base or os.environ.get("GROQ_BASE_URL"),
            "https://api.groq.com/openai/v1/audio/transcriptions",
        )
        self.language = language or None
        self.model = model or "whisper-large-v3"
        logger.debug("Groq transcription endpoint: {}", self.api_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GroqTranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("Groq API key not configured for transcription")
            return ""

        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""

        return await _post_transcription_with_retry(
            self.api_url,
            api_key=self.api_key,
            path=path,
            model=self.model,
            provider_label="Groq",
            language=self.language,
        )


class OpenRouterTranscriptionProvider:
    """OpenRouterTranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】OpenRouterTranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenRouterTranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        self.api_url = _resolve_transcription_url(
            api_base or os.environ.get("OPENROUTER_BASE_URL"),
            "https://openrouter.ai/api/v1/audio/transcriptions",
        )
        self.language = language or None
        self.model = model or "openai/whisper-1"
        logger.debug("OpenRouter transcription endpoint: {}", self.api_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `OpenRouterTranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("OpenRouter API key not configured for transcription")
            return ""

        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""

        return await _post_json_transcription_with_retry(
            self.api_url,
            api_key=self.api_key,
            path=path,
            model=self.model,
            provider_label="OpenRouter",
            language=self.language,
        )


class XiaomiMiMoTranscriptionProvider:
    """XiaomiMiMoTranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】XiaomiMiMoTranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `XiaomiMiMoTranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.api_key = api_key or os.environ.get("MIMO_API_KEY")
        self.api_url = _resolve_chat_completions_url(
            api_base or os.environ.get("MIMO_API_BASE"),
            "https://api.xiaomimimo.com/v1/chat/completions",
        )
        self.language = language or None
        self.model = model or "mimo-v2.5-asr"
        logger.debug("Xiaomi MiMo transcription endpoint: {}", self.api_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `XiaomiMiMoTranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("Xiaomi MiMo API key not configured for transcription")
            return ""

        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""

        return await _post_xiaomi_mimo_asr_with_retry(
            self.api_url,
            api_key=self.api_key,
            path=path,
            model=self.model,
            provider_label="Xiaomi MiMo",
            language=self.language,
        )


class StepFunTranscriptionProvider:
    """StepFunTranscriptionProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】StepFunTranscriptionProvider

    【功能说明】
    音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    _DEFAULT_URL = "https://api.stepfun.com/v1/audio/asr/sse"

    def __init__(
        self,
        api_key: str | None = None,
        api_base: str | None = None,
        language: str | None = None,
        model: str | None = None,
    ):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `StepFunTranscriptionProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        api_key: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        api_base: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        language: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self.api_key = api_key or os.environ.get("STEPFUN_API_KEY")
        # api_base 既可以是 StepFun 的 base URL，也可以直接是完整 SSE endpoint。
        self.api_url = _resolve_stepfun_asr_url(api_base)
        self.language = language or None
        self.model = model or "stepaudio-2.5-asr"
        logger.debug("StepFun transcription endpoint: {}", self.api_url)

    async def transcribe(self, file_path: str | Path) -> str:
        """异步转写音频（transcribe = 原函数名）。

        【中文名称】转写音频

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。音频转写 Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `StepFunTranscriptionProvider.transcribe` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.api_key:
            logger.warning("StepFun API key not configured for transcription")
            return ""

        path = Path(file_path)
        if not path.exists():
            logger.error("Audio file not found: {}", file_path)
            return ""

        return await _post_stepfun_asr_with_retry(
            self.api_url,
            api_key=self.api_key,
            path=path,
            model=self.model,
            provider_label="StepFun",
            language=self.language,
        )

