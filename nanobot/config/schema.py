"""基于 Pydantic 的配置模型定义。

这个文件是 nanobot 配置系统的“结构真相来源（single source of truth）”。
用户写在 ``config.json`` 里的内容，最终都要在这里找到对应的数据模型。

学习这个文件时，建议你把它当成“项目有哪些可调开关”的总目录：
- ``agents``：Agent 主行为
- ``channels``：聊天渠道
- ``providers``：模型服务商
- ``tools``：工具能力
- ``api / gateway``：对外服务
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator
from pydantic.alias_generators import to_camel
from pydantic_settings import BaseSettings

from nanobot.cron.types import CronSchedule

if TYPE_CHECKING:
    from nanobot.agent.tools.cli_apps import CliAppsToolConfig
    from nanobot.agent.tools.image_generation import ImageGenerationToolConfig
    from nanobot.agent.tools.self import MyToolConfig
    from nanobot.agent.tools.shell import ExecToolConfig
    from nanobot.agent.tools.web import WebToolsConfig


class Base(BaseModel):
    """所有配置模型的基础类。

    通过 ``alias_generator=to_camel``，它能同时接受：
    - Python 代码里常见的 ``snake_case``
    - JSON 配置里常见的 ``camelCase``
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class ChannelsConfig(Base):
    """聊天渠道总配置。

    这里既包含“所有渠道共享的公共开关”，也允许把具体渠道配置作为额外字段挂进来。
    例如 ``channels.telegram``、``channels.discord``、插件渠道等。
    """

    model_config = ConfigDict(extra="allow")

    send_progress: bool = True  # 是否把“生成中进度”推送给渠道
    send_tool_hints: bool = False  # 是否把工具调用提示（如 read_file(...)）推送给渠道
    show_reasoning: bool = True  # 如果渠道支持，是否展示模型 reasoning / thinking
    extract_document_text: bool = True  # 是否在把附件交给模型前先提取文档文本
    send_max_retries: int = Field(default=3, ge=0, le=10)  # 发送失败时的最大尝试次数（含首次）
    transcription_provider: str = "groq"  # 已废弃：请改用顶层 transcription.provider
    transcription_language: str | None = Field(default=None, pattern=r"^[a-z]{2,3}$")  # 已废弃：请改用顶层 transcription.language


class TranscriptionConfig(Base):
    """跨渠道音频转写配置。"""

    enabled: bool = True
    provider: str | None = None  # Validated by nanobot.audio.transcription_registry.
    model: str | None = None
    language: str | None = Field(default=None, pattern=r"^[a-z]{2,3}$")
    max_duration_sec: int = Field(default=120, ge=1, le=600)
    max_upload_mb: int = Field(default=25, ge=1, le=100)


class DreamConfig(Base):
    """Dream 记忆整合配置。

    Dream 可以理解为一个“后台整理记忆”的周期任务，
    它会把零散历史逐步浓缩成更适合长期记忆保存的形式。
    """

    _HOUR_MS = 3_600_000

    enabled: bool = True  # 启动时是否注册 Dream 周期任务
    interval_h: int = Field(default=2, ge=1)  # 默认每 2 小时执行一次
    cron: str | None = Field(default=None, exclude=True)  # 旧版 cron 表达式覆盖入口
    model_override: str | None = Field(
        default=None,
        validation_alias=AliasChoices("modelOverride", "model", "model_override"),
    )  # 为 Dream 会话单独指定模型（预留能力，尚未完全实现）
    max_batch_size: int = Field(default=20, ge=1)  # 已废弃：不再使用
    max_iterations: int = Field(default=15, ge=1)  # 已废弃：不再使用
    annotate_line_ages: bool = True  # 已废弃：不再使用

    def build_schedule(self, timezone: str) -> CronSchedule:
        """构建运行时调度对象，优先兼容旧版 cron 配置。"""
        if self.cron:
            return CronSchedule(kind="cron", expr=self.cron, tz=timezone)
        return CronSchedule(kind="every", every_ms=self.interval_h * self._HOUR_MS)

    def describe_schedule(self) -> str:
        """返回适合日志和启动提示的人类可读调度描述。"""
        if self.cron:
            return f"cron {self.cron} (legacy)"
        hours = self.interval_h
        return f"every {hours}h"


class InlineFallbackConfig(Base):
    """单个内联兜底模型配置。"""

    model: str
    provider: str
    max_tokens: int | None = None
    context_window_tokens: int | None = None
    temperature: float | None = None
    reasoning_effort: str | None = None


FallbackCandidate = str | InlineFallbackConfig


class ModelPresetConfig(Base):
    """模型预设配置。

    它把“模型名 + provider + 生成参数”打包成一个可命名、可切换的预设。
    这样运行时切模型不需要改一堆散字段。
    """

    label: str | None = None
    model: str
    provider: str = "auto"
    max_tokens: int = 8192
    context_window_tokens: int = 65_536
    temperature: float = 0.1
    reasoning_effort: str | None = None

    def to_generation_settings(self) -> Any:
        from nanobot.providers.base import GenerationSettings
        return GenerationSettings(
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            reasoning_effort=self.reasoning_effort,
        )


class AgentDefaults(Base):
    """Agent 默认行为配置。

    这是最值得新同学优先阅读的配置块之一，因为它决定了：
    - 默认用哪个模型
    - 最多允许调多少轮工具
    - 上下文窗口预算
    - 会话是否自动压缩
    - 是否启用统一会话
    """

    workspace: str = "~/.nanobot/workspace"
    model_preset: str | None = None  # 当前激活的预设名；一旦设置，优先级高于下面散字段
    model: str = "anthropic/claude-opus-4-5"
    provider: str = (
        "auto"  # provider 名称，如 anthropic/openrouter；auto 表示自动匹配
    )
    max_tokens: int = 8192
    context_window_tokens: int = 65_536
    context_block_limit: int | None = None
    temperature: float = 0.1
    fallback_models: list[FallbackCandidate] = Field(default_factory=list)
    max_tool_iterations: int = 200
    max_concurrent_subagents: int = Field(default=1, ge=1)
    max_tool_result_chars: int = 16_000
    provider_retry_mode: Literal["standard", "persistent"] = "standard"
    tool_hint_max_length: int = Field(
        default=40,
        ge=20,
        le=500,
        validation_alias=AliasChoices("toolHintMaxLength"),
        serialization_alias="toolHintMaxLength",
    )  # 工具提示展示给用户时允许的最大字符数
    reasoning_effort: str | None = None  # 模型思考强度：low / medium / high / adaptive / none
    timezone: str = "UTC"  # IANA 时区名，例如 Asia/Shanghai
    bot_name: str = "nanobot"  # CLI 中展示给用户看的机器人名字
    bot_icon: str = "🐈"  # CLI 中机器人名字旁边显示的短图标；空串表示不显示
    unified_session: bool = False  # 是否跨渠道共享一个会话（单用户多设备场景）
    disabled_skills: list[str] = Field(default_factory=list)  # 要禁用的技能名列表
    session_ttl_minutes: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices("idleCompactAfterMinutes", "sessionTtlMinutes"),
        serialization_alias="idleCompactAfterMinutes",
    )  # 会话空闲多久后自动压缩，单位分钟；0 表示关闭
    max_messages: int = Field(
        default=120,
        ge=0,
    )  # 从 session 历史最多回放多少条消息到模型上下文
    consolidation_ratio: float = Field(
        default=0.5,
        ge=0.1,
        le=0.95,
        validation_alias=AliasChoices("consolidationRatio"),
        serialization_alias="consolidationRatio",
    )  # 压缩目标比例，例如 0.5 表示压缩后保留约 50% 预算
    dream: DreamConfig = Field(default_factory=DreamConfig)


class AgentsConfig(Base):
    """Agent 配置总入口。"""

    defaults: AgentDefaults = Field(default_factory=AgentDefaults)


class ProviderConfig(Base):
    """单个 LLM Provider 的通用配置。"""

    api_key: str | None = Field(default=None, repr=False)
    api_base: str | None = None
    api_type: Literal["auto", "chat_completions", "responses"] = "auto"  # Request API surface
    extra_headers: dict[str, str] | None = None  # Custom headers (e.g. APP-Code for AiHubMix)
    extra_body: dict[str, Any] | None = None  # Extra provider request fields; shape depends on provider/API surface
    extra_query: dict[str, str] | None = None  # Extra query params (e.g. api-version for Azure-style gateways)


class BedrockProviderConfig(ProviderConfig):
    """AWS Bedrock 专用配置。"""

    region: str | None = None  # AWS region, falls back to AWS_REGION/AWS_DEFAULT_REGION/profile
    profile: str | None = None  # Optional AWS shared config profile


class ProvidersConfig(Base):
    """所有 Provider 的配置集合。

    这里列出的字段不只是“可选服务商列表”，也是自动匹配和状态展示的配置基础。
    """

    custom: ProviderConfig = Field(default_factory=ProviderConfig)  # Any OpenAI-compatible endpoint
    azure_openai: ProviderConfig = Field(default_factory=ProviderConfig)  # Azure OpenAI (model = deployment name)
    bedrock: BedrockProviderConfig = Field(default_factory=BedrockProviderConfig)  # AWS Bedrock Converse
    anthropic: ProviderConfig = Field(default_factory=ProviderConfig)
    openai: ProviderConfig = Field(default_factory=ProviderConfig)
    openrouter: ProviderConfig = Field(default_factory=ProviderConfig)
    assemblyai: ProviderConfig = Field(default_factory=ProviderConfig)  # AssemblyAI voice transcription
    huggingface: ProviderConfig = Field(default_factory=ProviderConfig)
    skywork: ProviderConfig = Field(default_factory=ProviderConfig)  # Skywork / APIFree API gateway
    deepseek: ProviderConfig = Field(default_factory=ProviderConfig)
    groq: ProviderConfig = Field(default_factory=ProviderConfig)
    zhipu: ProviderConfig = Field(default_factory=ProviderConfig)
    dashscope: ProviderConfig = Field(default_factory=ProviderConfig)
    vllm: ProviderConfig = Field(default_factory=ProviderConfig)
    ollama: ProviderConfig = Field(default_factory=ProviderConfig)  # Ollama local models
    lm_studio: ProviderConfig = Field(default_factory=ProviderConfig)  # LM Studio local models
    atomic_chat: ProviderConfig = Field(default_factory=ProviderConfig)  # Atomic Chat local models
    ovms: ProviderConfig = Field(default_factory=ProviderConfig)  # OpenVINO Model Server (OVMS)
    gemini: ProviderConfig = Field(default_factory=ProviderConfig)
    moonshot: ProviderConfig = Field(default_factory=ProviderConfig)
    minimax: ProviderConfig = Field(default_factory=ProviderConfig)
    minimax_anthropic: ProviderConfig = Field(default_factory=ProviderConfig)  # MiniMax Anthropic endpoint (thinking)
    mistral: ProviderConfig = Field(default_factory=ProviderConfig)
    stepfun: ProviderConfig = Field(default_factory=ProviderConfig)  # Step Fun (阶跃星辰) — LLM + ASR (set apiBase to Plan URL for ASR)
    xiaomi_mimo: ProviderConfig = Field(default_factory=ProviderConfig)  # Xiaomi MIMO (小米)
    longcat: ProviderConfig = Field(default_factory=ProviderConfig)  # LongCat
    ant_ling: ProviderConfig = Field(default_factory=ProviderConfig)  # Ant Ling
    aihubmix: ProviderConfig = Field(default_factory=ProviderConfig)  # AiHubMix API gateway
    siliconflow: ProviderConfig = Field(default_factory=ProviderConfig)  # SiliconFlow (硅基流动)
    novita: ProviderConfig = Field(default_factory=ProviderConfig)  # Novita AI
    volcengine: ProviderConfig = Field(default_factory=ProviderConfig)  # VolcEngine (火山引擎)
    volcengine_coding_plan: ProviderConfig = Field(default_factory=ProviderConfig)  # VolcEngine Coding Plan
    byteplus: ProviderConfig = Field(default_factory=ProviderConfig)  # BytePlus (VolcEngine international)
    byteplus_coding_plan: ProviderConfig = Field(default_factory=ProviderConfig)  # BytePlus Coding Plan
    openai_codex: ProviderConfig = Field(default_factory=ProviderConfig, exclude=True)  # OpenAI Codex (OAuth)
    github_copilot: ProviderConfig = Field(default_factory=ProviderConfig, exclude=True)  # Github Copilot (OAuth)
    qianfan: ProviderConfig = Field(default_factory=ProviderConfig)  # Qianfan (百度千帆)
    nvidia: ProviderConfig = Field(default_factory=ProviderConfig)  # NVIDIA NIM (nvapi- keys)

    @model_validator(mode="after")
    def _validate_api_type_scope(self) -> "ProvidersConfig":
        """限制 ``api_type`` 目前只允许出现在 providers.openai 下。"""
        for name in self.__class__.model_fields:
            if name == "openai":
                continue
            provider = getattr(self, name, None)
            if isinstance(provider, ProviderConfig) and provider.api_type != "auto":
                raise ValueError("providers.<name>.api_type is only supported for providers.openai")
        return self


class HeartbeatConfig(Base):
    """Heartbeat 服务配置。"""

    enabled: bool = True
    interval_s: int = 30 * 60  # 30 minutes
    keep_recent_messages: int = 8


class ApiConfig(Base):
    """OpenAI 兼容 API 服务配置。"""

    host: str = "127.0.0.1"  # 更安全的默认值：只监听本机
    port: int = 8900
    timeout: float = 120.0  # Per-request timeout in seconds.


class GatewayConfig(Base):
    """Gateway / WebUI 服务配置。"""

    host: str = "127.0.0.1"  # 更安全的默认值：只监听本机
    port: int = 18790
    heartbeat: HeartbeatConfig = Field(default_factory=HeartbeatConfig)


class MCPServerConfig(Base):
    """MCP 服务器连接配置。

    支持两大类传输方式：
    - stdio：把某个本地命令当成 MCP Server 启起来
    - HTTP/SSE：连接远程 MCP Server
    """

    type: Literal["stdio", "sse", "streamableHttp"] | None = None  # 省略时自动推断传输方式
    command: str = ""  # stdio 模式下要启动的命令
    args: list[str] = Field(default_factory=list)  # stdio 模式下命令参数
    env: dict[str, str] = Field(default_factory=dict)  # stdio 模式下额外环境变量
    cwd: str = ""  # stdio 模式下工作目录
    url: str = ""  # HTTP/SSE 模式下服务地址
    headers: dict[str, str] = Field(default_factory=dict)  # HTTP/SSE 模式下自定义请求头
    tool_timeout: int = 30  # 一次 MCP 工具调用超时时间（秒）
    enabled_tools: list[str] = Field(default_factory=lambda: ["*"])  # 允许注册哪些 MCP 工具


def _lazy_default(module_path: str, class_name: str) -> Any:
    """延迟导入工具配置类，避免循环导入。"""
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, class_name)()


class ToolsConfig(Base):
    """工具系统配置。

    工具模块本身又会反向 import ``schema.py`` 里的 ``Base``，
    所以这里采用“延迟解析字段类型”的方式来避免循环导入。
    """

    web: WebToolsConfig = Field(default_factory=lambda: _lazy_default("nanobot.agent.tools.web", "WebToolsConfig"))
    exec: ExecToolConfig = Field(default_factory=lambda: _lazy_default("nanobot.agent.tools.shell", "ExecToolConfig"))
    cli_apps: CliAppsToolConfig = Field(default_factory=lambda: _lazy_default("nanobot.agent.tools.cli_apps", "CliAppsToolConfig"))
    my: MyToolConfig = Field(default_factory=lambda: _lazy_default("nanobot.agent.tools.self", "MyToolConfig"))
    image_generation: ImageGenerationToolConfig = Field(
        default_factory=lambda: _lazy_default("nanobot.agent.tools.image_generation", "ImageGenerationToolConfig"),
    )
    restrict_to_workspace: bool = False  # 是否尽量把工具访问限制在 workspace 内
    webui_allow_local_service_access: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "webuiAllowLocalServiceAccess",
            "webui_allow_local_service_access",
            "allowLocalPreviewAccess",
            "allow_local_preview_access",
        ),
    )  # 是否允许 WebUI Full Access 访问 localhost 服务
    mcp_servers: dict[str, MCPServerConfig] = Field(default_factory=dict)
    ssrf_whitelist: list[str] = Field(default_factory=list)  # 免于 SSRF 拦截的 CIDR 白名单


class Config(BaseSettings):
    """nanobot 根配置对象。"""

    agents: AgentsConfig = Field(default_factory=AgentsConfig)
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    transcription: TranscriptionConfig = Field(default_factory=TranscriptionConfig)
    providers: ProvidersConfig = Field(default_factory=ProvidersConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    gateway: GatewayConfig = Field(default_factory=GatewayConfig)
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
    model_presets: dict[str, ModelPresetConfig] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("modelPresets", "model_presets"),
    )

    def __init__(self, **values: Any) -> None:
        """初始化根配置前，先确保工具子配置类型已解析完成。"""
        if not type(self).__pydantic_complete__:
            _resolve_tool_config_refs()
        super().__init__(**values)

    @model_validator(mode="after")
    def _validate_model_preset(self) -> "Config":
        """校验默认预设与 fallback 预设引用是否合法。"""
        if "default" in self.model_presets:
            raise ValueError("model_preset name 'default' is reserved for agents.defaults")
        name = self.agents.defaults.model_preset
        if name and name != "default" and name not in self.model_presets:
            raise ValueError(f"model_preset {name!r} not found in model_presets")
        for fallback in self.agents.defaults.fallback_models:
            if isinstance(fallback, str) and fallback not in self.model_presets:
                raise ValueError(f"fallback_models entry {fallback!r} not found in model_presets")
        return self

    def resolve_default_preset(self) -> ModelPresetConfig:
        """从 ``agents.defaults`` 散字段合成隐式的 ``default`` 预设。"""
        d = self.agents.defaults
        return ModelPresetConfig(
            model=d.model, provider=d.provider, max_tokens=d.max_tokens,
            context_window_tokens=d.context_window_tokens,
            temperature=d.temperature, reasoning_effort=d.reasoning_effort,
        )

    def resolve_preset(self, name: str | None = None) -> ModelPresetConfig:
        """按名称解析生效中的模型预设；空名时回退到隐式 default。"""
        name = self.agents.defaults.model_preset if name is None else name
        if not name or name == "default":
            return self.resolve_default_preset()
        if name not in self.model_presets:
            raise KeyError(f"model_preset {name!r} not found in model_presets")
        return self.model_presets[name]

    @property
    def workspace_path(self) -> Path:
        """返回展开 ``~`` 之后的工作区路径。"""
        return Path(self.agents.defaults.workspace).expanduser()

    def _match_provider(
        self, model: str | None = None,
        *,
        preset: ModelPresetConfig | None = None,
    ) -> tuple["ProviderConfig | None", str | None]:
        """匹配某个模型应使用哪个 provider 配置。

        返回 ``(provider_config, provider_name)``。

        这是整个 provider 自动匹配逻辑的核心入口，匹配顺序大致是：
        1. 如果 preset 明确指定 provider，则直接按名字找
        2. 根据模型前缀匹配，例如 ``anthropic/...``
        3. 根据模型关键词匹配，例如包含 ``claude`` / ``gpt`` / ``qwen``
        4. 对本地 provider 做特殊兜底
        5. 最后从“已配置好 key 的 provider”里选择一个可用兜底
        """
        from nanobot.providers.registry import PROVIDERS, find_by_name

        resolved = preset or self.resolve_preset()
        forced = resolved.provider
        if forced != "auto":
            spec = find_by_name(forced)
            if spec:
                p = getattr(self.providers, spec.name, None)
                return (p, spec.name) if p else (None, None)
            return None, None

        model_lower = (model or resolved.model).lower()
        model_normalized = model_lower.replace("-", "_")
        model_prefix = model_lower.split("/", 1)[0] if "/" in model_lower else ""
        normalized_prefix = model_prefix.replace("-", "_")

        def _kw_matches(kw: str) -> bool:
            kw = kw.lower()
            return kw in model_lower or kw.replace("-", "_") in model_normalized

        # 显式 provider 前缀优先，避免类似
        # `github-copilot/...codex` 被误判成 openai_codex。
        for spec in PROVIDERS:
            if spec.is_transcription_only:
                continue
            p = getattr(self.providers, spec.name, None)
            if p and model_prefix and normalized_prefix == spec.name:
                if spec.is_oauth or spec.is_local or spec.is_direct or p.api_key:
                    return p, spec.name

        # 再按关键词匹配，优先级顺序由 PROVIDERS 注册表决定。
        for spec in PROVIDERS:
            if spec.is_transcription_only:
                continue
            p = getattr(self.providers, spec.name, None)
            if p and any(_kw_matches(kw) for kw in spec.keywords):
                if spec.is_oauth or spec.is_local or spec.is_direct or p.api_key:
                    return p, spec.name

        # 本地 provider 的兜底逻辑：
        # 有些模型名本身不带 provider 关键词，例如 ollama 上的 "llama3.2"。
        # 这时优先根据 api_base 特征来判断。
        local_fallback: tuple[ProviderConfig, str] | None = None
        for spec in PROVIDERS:
            if not spec.is_local:
                continue
            p = getattr(self.providers, spec.name, None)
            if not (p and p.api_base):
                continue
            if spec.detect_by_base_keyword and spec.detect_by_base_keyword in p.api_base:
                return p, spec.name
            if local_fallback is None:
                local_fallback = (p, spec.name)
        if local_fallback:
            return local_fallback

        # 最后兜底：按 PROVIDERS 顺序找一个已配置好的 provider。
        # 但 OAuth provider 不能做兜底，因为它们通常要求显式模型选择。
        for spec in PROVIDERS:
            if spec.is_oauth or spec.is_transcription_only:
                continue
            p = getattr(self.providers, spec.name, None)
            if p and p.api_key:
                return p, spec.name
        return None, None

    def get_provider(
        self,
        model: str | None = None,
        *,
        preset: ModelPresetConfig | None = None,
    ) -> ProviderConfig | None:
        """获取匹配到的 ProviderConfig。"""
        p, _ = self._match_provider(model, preset=preset)
        return p

    def get_provider_name(
        self,
        model: str | None = None,
        *,
        preset: ModelPresetConfig | None = None,
    ) -> str | None:
        """获取匹配到的 provider 注册名，例如 ``deepseek``。"""
        _, name = self._match_provider(model, preset=preset)
        return name

    def get_api_key(
        self,
        model: str | None = None,
        *,
        preset: ModelPresetConfig | None = None,
    ) -> str | None:
        """获取某个模型对应 provider 的 API key。"""
        p = self.get_provider(model, preset=preset)
        return p.api_key if p else None

    def get_api_base(
        self,
        model: str | None = None,
        *,
        preset: ModelPresetConfig | None = None,
    ) -> str | None:
        """获取某个模型对应 provider 的 API Base。

        如果用户配置里没填 ``api_base``，则回退到 provider 注册表中的默认值。
        """
        from nanobot.providers.registry import find_by_name

        p, name = self._match_provider(model, preset=preset)
        if p and p.api_base:
            return p.api_base
        if name:
            spec = find_by_name(name)
            if spec and spec.default_api_base:
                return spec.default_api_base
        return None

    model_config = ConfigDict(env_prefix="NANOBOT_", env_nested_delimiter="__")


def _resolve_tool_config_refs() -> None:
    """解析 ToolsConfig 里的前向引用类型。

    因为工具模块会反向依赖 ``schema.py``，这里只能在模块加载后再把实际工具配置类
    导入进来，并触发 ``model_rebuild()`` 完成类型绑定。
    """
    import sys

    from nanobot.agent.tools.cli_apps import CliAppsToolConfig
    from nanobot.agent.tools.image_generation import ImageGenerationToolConfig
    from nanobot.agent.tools.self import MyToolConfig
    from nanobot.agent.tools.shell import ExecToolConfig
    from nanobot.agent.tools.web import WebFetchConfig, WebSearchConfig, WebToolsConfig

    # 重新导出到当前模块命名空间，兼容历史导入路径。
    mod = sys.modules[__name__]
    mod.ExecToolConfig = ExecToolConfig  # type: ignore[attr-defined]
    mod.CliAppsToolConfig = CliAppsToolConfig  # type: ignore[attr-defined]
    mod.WebToolsConfig = WebToolsConfig  # type: ignore[attr-defined]
    mod.WebSearchConfig = WebSearchConfig  # type: ignore[attr-defined]
    mod.WebFetchConfig = WebFetchConfig  # type: ignore[attr-defined]
    mod.MyToolConfig = MyToolConfig  # type: ignore[attr-defined]
    mod.ImageGenerationToolConfig = ImageGenerationToolConfig  # type: ignore[attr-defined]

    ToolsConfig.model_rebuild()
    Config.model_rebuild()


# 如果当前导入链允许，就尽早解析；
# 如果一开始还是撞上循环导入，就等第一次真正用到 Config/ToolsConfig 时再懒解析。
try:
    _resolve_tool_config_refs()
except ImportError:
    pass
