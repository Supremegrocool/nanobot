"""GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。

【中文名称】Provider 实现：nanobot/providers/github_copilot_provider.py

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

import time
import webbrowser
from collections.abc import Awaitable, Callable
from contextlib import suppress

import httpx
from oauth_cli_kit.models import OAuthToken
from oauth_cli_kit.storage import FileTokenStorage

from nanobot.providers.openai_compat_provider import OpenAICompatProvider

DEFAULT_GITHUB_DEVICE_CODE_URL = "https://github.com/login/device/code"
DEFAULT_GITHUB_ACCESS_TOKEN_URL = "https://github.com/login/oauth/access_token"
DEFAULT_GITHUB_USER_URL = "https://api.github.com/user"
DEFAULT_COPILOT_TOKEN_URL = "https://api.github.com/copilot_internal/v2/token"
DEFAULT_COPILOT_BASE_URL = "https://api.githubcopilot.com"
GITHUB_COPILOT_CLIENT_ID = "Iv1.b507a08c87ecfe98"
GITHUB_COPILOT_SCOPE = "read:user"
TOKEN_FILENAME = "github-copilot.json"
TOKEN_APP_NAME = "nanobot"
USER_AGENT = "nanobot/0.1"
EDITOR_VERSION = "vscode/1.99.0"
EDITOR_PLUGIN_VERSION = "copilot-chat/0.26.0"
_EXPIRY_SKEW_SECONDS = 60
_LONG_LIVED_TOKEN_SECONDS = 315360000


def get_storage() -> FileTokenStorage:
    """执行辅助逻辑（get_storage = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `get_storage` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return FileTokenStorage(
        token_filename=TOKEN_FILENAME,
        app_name=TOKEN_APP_NAME,
        import_codex_cli=False,
    )


def _copilot_headers(token: str) -> dict[str, str]:
    """执行辅助逻辑（_copilot_headers = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_copilot_headers` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    token: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return {
        "Authorization": f"token {token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "Editor-Version": EDITOR_VERSION,
        "Editor-Plugin-Version": EDITOR_PLUGIN_VERSION,
    }


def _load_github_token() -> OAuthToken | None:
    """加载数据（_load_github_token = 原函数名）。

    【中文名称】加载数据

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `_load_github_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    token = get_storage().load()
    if not token or not token.access:
        return None
    return token


def get_github_copilot_login_status() -> OAuthToken | None:
    """执行辅助逻辑（get_github_copilot_login_status = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `get_github_copilot_login_status` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    无显式参数。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    return _load_github_token()


def login_github_copilot(
    print_fn: Callable[[str], None] | None = None,
    prompt_fn: Callable[[str], str] | None = None,
) -> OAuthToken:
    """执行辅助逻辑（login_github_copilot = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
    在阅读 `login_github_copilot` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    print_fn: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
    prompt_fn: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    del prompt_fn
    printer = print_fn or print
    timeout = httpx.Timeout(20.0, connect=20.0)

    with httpx.Client(timeout=timeout, follow_redirects=True, trust_env=True) as client:
        response = client.post(
            DEFAULT_GITHUB_DEVICE_CODE_URL,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            data={"client_id": GITHUB_COPILOT_CLIENT_ID, "scope": GITHUB_COPILOT_SCOPE},
        )
        response.raise_for_status()
        payload = response.json()

        device_code = str(payload["device_code"])
        user_code = str(payload["user_code"])
        verify_url = str(payload.get("verification_uri") or payload.get("verification_uri_complete") or "")
        verify_complete = str(payload.get("verification_uri_complete") or verify_url)
        interval = max(1, int(payload.get("interval") or 5))
        expires_in = int(payload.get("expires_in") or 900)

        printer(f"Open: {verify_url}")
        printer(f"Code: {user_code}")
        if verify_complete:
            with suppress(Exception):
                webbrowser.open(verify_complete)

        deadline = time.time() + expires_in
        current_interval = interval
        access_token = None
        token_expires_in = _LONG_LIVED_TOKEN_SECONDS
        while time.time() < deadline:
            poll = client.post(
                DEFAULT_GITHUB_ACCESS_TOKEN_URL,
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                data={
                    "client_id": GITHUB_COPILOT_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
            )
            poll.raise_for_status()
            poll_payload = poll.json()

            access_token = poll_payload.get("access_token")
            if access_token:
                token_expires_in = int(poll_payload.get("expires_in") or _LONG_LIVED_TOKEN_SECONDS)
                break

            error = poll_payload.get("error")
            if error == "authorization_pending":
                time.sleep(current_interval)
                continue
            if error == "slow_down":
                current_interval += 5
                time.sleep(current_interval)
                continue
            if error == "expired_token":
                raise RuntimeError("GitHub device code expired. Please run login again.")
            if error == "access_denied":
                raise RuntimeError("GitHub device flow was denied.")
            if error:
                desc = poll_payload.get("error_description") or error
                raise RuntimeError(str(desc))
            time.sleep(current_interval)
        else:
            raise RuntimeError("GitHub device flow timed out.")

        user = client.get(
            DEFAULT_GITHUB_USER_URL,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": USER_AGENT,
            },
        )
        user.raise_for_status()
        user_payload = user.json()
        account_id = user_payload.get("login") or str(user_payload.get("id") or "") or None

    expires_ms = int((time.time() + token_expires_in) * 1000)
    token = OAuthToken(
        access=str(access_token),
        refresh="",
        expires=expires_ms,
        account_id=str(account_id) if account_id else None,
    )
    get_storage().save(token)
    return token


class GitHubCopilotProvider(OpenAICompatProvider):
    """GitHubCopilotProvider 类，封装 Provider 实现 的核心状态和行为。

    【中文名称】GitHubCopilotProvider

    【功能说明】
    GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    OpenAICompatProvider。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(self, default_model: str = "github-copilot/gpt-4.1"):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GitHubCopilotProvider.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        default_model: 模型名称或模型配置，用于选择具体 LLM 能力。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        from nanobot.providers.registry import find_by_name

        self._copilot_access_token: str | None = None
        self._copilot_expires_at: float = 0.0
        super().__init__(
            api_key="no-key",
            api_base=DEFAULT_COPILOT_BASE_URL,
            default_model=default_model,
            extra_headers={
                "Editor-Version": EDITOR_VERSION,
                "Editor-Plugin-Version": EDITOR_PLUGIN_VERSION,
                "User-Agent": USER_AGENT,
            },
            spec=find_by_name("github_copilot"),
        )

    async def _get_copilot_access_token(self) -> str:
        """异步执行辅助逻辑（_get_copilot_access_token = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GitHubCopilotProvider._get_copilot_access_token` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        now = time.time()
        if self._copilot_access_token and now < self._copilot_expires_at - _EXPIRY_SKEW_SECONDS:
            return self._copilot_access_token

        github_token = _load_github_token()
        if not github_token or not github_token.access:
            raise RuntimeError("GitHub Copilot is not logged in. Run: nanobot provider login github-copilot")

        timeout = httpx.Timeout(20.0, connect=20.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, trust_env=True) as client:
            response = await client.get(
                DEFAULT_COPILOT_TOKEN_URL,
                headers=_copilot_headers(github_token.access),
            )
            response.raise_for_status()
            payload = response.json()

        token = payload.get("token")
        if not token:
            raise RuntimeError("GitHub Copilot token exchange returned no token.")

        expires_at = payload.get("expires_at")
        if isinstance(expires_at, (int, float)):
            self._copilot_expires_at = float(expires_at)
        else:
            refresh_in = payload.get("refresh_in") or 1500
            self._copilot_expires_at = time.time() + int(refresh_in)
        self._copilot_access_token = str(token)
        return self._copilot_access_token

    async def _refresh_client_api_key(self) -> str:
        """异步执行辅助逻辑（_refresh_client_api_key = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GitHubCopilotProvider._refresh_client_api_key` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        token = await self._get_copilot_access_token()
        client = await self._ensure_client()
        self.api_key = token
        client.api_key = token
        return token

    async def chat(
        self,
        messages: list[dict[str, object]],
        tools: list[dict[str, object]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, object] | None = None,
    ):
        """异步执行辅助逻辑（chat = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GitHubCopilotProvider.chat` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        max_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        temperature: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._refresh_client_api_key()
        return await super().chat(
            messages=messages,
            tools=tools,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            reasoning_effort=reasoning_effort,
            tool_choice=tool_choice,
        )

    async def chat_stream(
        self,
        messages: list[dict[str, object]],
        tools: list[dict[str, object]] | None = None,
        model: str | None = None,
        max_tokens: int = 4096,
        temperature: float = 0.7,
        reasoning_effort: str | None = None,
        tool_choice: str | dict[str, object] | None = None,
        on_content_delta: Callable[[str], None] | None = None,
        on_thinking_delta: Callable[[str], Awaitable[None]] | None = None,
        on_tool_call_delta: Callable[[dict[str, object]], Awaitable[None]] | None = None,
    ):
        """异步流式处理（chat_stream = 原函数名）。

        【中文名称】流式处理

        【功能说明】
        这是 Provider 实现 中的一个关键步骤。GitHub Copilot Provider 实现，负责把 nanobot 的统一 LLM 请求转换为具体服务商 API 调用。
        在阅读 `GitHubCopilotProvider.chat_stream` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        messages: 消息数据，可能来自用户、频道、模型或工具调用。
        tools: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        model: 模型名称或模型配置，用于选择具体 LLM 能力。
        max_tokens: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        temperature: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        reasoning_effort: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tool_choice: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_content_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_thinking_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        on_tool_call_delta: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        await self._refresh_client_api_key()
        return await super().chat_stream(
            messages=messages,
            tools=tools,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            reasoning_effort=reasoning_effort,
            tool_choice=tool_choice,
            on_content_delta=on_content_delta,
            on_thinking_delta=on_thinking_delta,
            on_tool_call_delta=on_tool_call_delta,
        )

