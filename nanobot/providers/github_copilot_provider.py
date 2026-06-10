"""GitHub Copilot Provider 实现。

【中文名称】GitHub Copilot Provider 实现

【功能说明】
负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。

【在整体架构中的位置】
该文件属于 P1 范围的模型 Provider代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

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
    """执行 `get_storage`。

    【中文名称】get_storage

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return FileTokenStorage(
        token_filename=TOKEN_FILENAME,
        app_name=TOKEN_APP_NAME,
        import_codex_cli=False,
    )


def _copilot_headers(token: str) -> dict[str, str]:
    """执行 `_copilot_headers`。

    【中文名称】_copilot_headers

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - token: 调用方传入的 `token` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    return {
        "Authorization": f"token {token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "Editor-Version": EDITOR_VERSION,
        "Editor-Plugin-Version": EDITOR_PLUGIN_VERSION,
    }


def _load_github_token() -> OAuthToken | None:
    """执行 `_load_github_token`。

    【中文名称】_load_github_token

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

    token = get_storage().load()
    if not token or not token.access:
        return None
    return token


def get_github_copilot_login_status() -> OAuthToken | None:
    """执行 `get_github_copilot_login_status`。

    【中文名称】get_github_copilot_login_status

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - 无显式业务参数。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return _load_github_token()


def login_github_copilot(
    print_fn: Callable[[str], None] | None = None,
    prompt_fn: Callable[[str], str] | None = None,
) -> OAuthToken:
    """执行 `login_github_copilot`。

    【中文名称】login_github_copilot

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - print_fn: 调用方传入的 `print_fn` 数据；具体类型以函数签名为准。
    - prompt_fn: 调用方传入的 `prompt_fn` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
    """GitHubCopilotProvider 类。

    【中文名称】GitHubCopilotProvider

    【功能说明】
    这是 GitHub Copilot Provider 实现 中的核心数据结构或服务类。负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    def __init__(self, default_model: str = "github-copilot/gpt-4.1"):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - default_model: 调用方传入的 `default_model` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `_get_copilot_access_token`。

        【中文名称】_get_copilot_access_token

        【功能说明】
        这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `_refresh_client_api_key`。

        【中文名称】_refresh_client_api_key

        【功能说明】
        这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """异步执行 `chat`。

        【中文名称】chat

        【功能说明】
        这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
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
        """异步执行 `chat_stream`。

        【中文名称】chat_stream

        【功能说明】
        这是 GitHub Copilot Provider 实现 中的一个步骤函数，用来支撑：负责复用 OpenAI 兼容 Provider，同时自动获取和刷新 Copilot API 所需的临时访问令牌。
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
