"""Provider 注册表：LLM 服务商元数据的单一事实来源。

如果你想新增一个 Provider，通常只需要两步：
1. 在下面的 ``PROVIDERS`` 里加一条 ``ProviderSpec``
2. 在 ``config/schema.py`` 的 ``ProvidersConfig`` 里加对应字段

之后大部分自动匹配、环境变量推断、状态展示都会跟着这里走。

特别注意：``PROVIDERS`` 的顺序有意义，它会影响匹配优先级和 fallback 顺序。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic.alias_generators import to_snake


@dataclass(frozen=True)
class ProviderSpec:
    """单个 Provider 的元数据描述。

    你可以把它理解成“Provider 的配置说明书 + 匹配说明书”。
    Factory、Config 自动匹配等逻辑都会参考这里。
    """

    # identity
    name: str  # 配置字段名，例如 "dashscope"
    keywords: tuple[str, ...]  # 模型名关键词，用于自动匹配
    env_key: str  # API key 对应环境变量名
    display_name: str = ""  # 给用户展示的名字

    # 选择哪种底层 Provider 实现
    backend: str = "openai_compat"

    # 额外需要注入的环境变量
    env_extras: tuple[tuple[str, str], ...] = ()

    # 网关 / 本地部署识别
    is_gateway: bool = False  # 是否是可转发任意模型的网关型 Provider
    is_local: bool = False  # 是否是本地部署 Provider
    detect_by_key_prefix: str = ""  # 通过 api_key 前缀识别
    detect_by_base_keyword: str = ""  # 通过 api_base 中的关键字识别
    default_api_base: str = ""  # 默认 API Base

    # 网关行为
    strip_model_prefix: bool = False  # 发送给网关前是否移除 "provider/" 前缀
    supports_max_completion_tokens: bool = False

    # 针对特定模型的参数覆盖
    model_overrides: tuple[tuple[str, dict[str, Any]], ...] = ()

    # OAuth 类 Provider（如 OpenAI Codex）不依赖传统 API key
    is_oauth: bool = False

    # Direct Provider 跳过 API key 校验
    is_direct: bool = False

    # 这个 Provider 只用于转写等能力，不能承载普通 chat completion
    is_transcription_only: bool = False

    # 是否支持 content block 级别的 prompt caching
    supports_prompt_caching: bool = False

    # 如何向 extra_body 注入 thinking 开关
    thinking_style: str = ""

    # 网关原生 reasoning 控制方式
    gateway_reasoning_style: str = ""

    # 某些 Provider 把真正回答放在 reasoning 字段而不是 content 字段
    reasoning_as_content: bool = False

    @property
    def label(self) -> str:
        """返回更适合展示给用户的 Provider 名称。"""
        return self.display_name or self.name.title()


# ---------------------------------------------------------------------------
# PROVIDERS 注册表本体。顺序 = 匹配优先级。
# ---------------------------------------------------------------------------

PROVIDERS: tuple[ProviderSpec, ...] = (
    # === Custom (direct OpenAI-compatible endpoint) ========================
    ProviderSpec(
        name="custom",
        keywords=(),
        env_key="",
        display_name="Custom",
        backend="openai_compat",
        is_direct=True,
    ),

    # === Azure OpenAI (direct API calls with API version 2024-10-21) =====
    ProviderSpec(
        name="azure_openai",
        keywords=("azure", "azure-openai"),
        env_key="",
        display_name="Azure OpenAI",
        backend="azure_openai",
        is_direct=True,
    ),
    # === AWS Bedrock (native Converse API via bedrock-runtime) =============
    ProviderSpec(
        name="bedrock",
        keywords=(
            "bedrock",
            "anthropic.claude",
            "amazon.nova",
            "meta.",
            "mistral.",
            "cohere.",
            "qwen.",
            "deepseek.",
            "openai.gpt-oss",
            "ai21.",
            "moonshot.",
            "writer.",
            "zai.",
        ),
        env_key="AWS_BEARER_TOKEN_BEDROCK",
        display_name="AWS Bedrock",
        backend="bedrock",
        is_direct=True,
    ),
    # === Gateways (detected by api_key / api_base, not model name) =========
    # Gateways can route any model, so they win in fallback.
    # OpenRouter: global gateway, keys start with "sk-or-"
    ProviderSpec(
        name="openrouter",
        keywords=("openrouter",),
        env_key="OPENROUTER_API_KEY",
        display_name="OpenRouter",
        backend="openai_compat",
        is_gateway=True,
        detect_by_key_prefix="sk-or-",
        detect_by_base_keyword="openrouter",
        default_api_base="https://openrouter.ai/api/v1",
        supports_prompt_caching=True,
        gateway_reasoning_style="reasoning_effort",
    ),
    # Hugging Face Inference Providers: OpenAI-compatible router for chat models.
    ProviderSpec(
        name="huggingface",
        keywords=("huggingface", "hugging-face"),
        env_key="HF_TOKEN",
        display_name="Hugging Face",
        backend="openai_compat",
        is_gateway=True,
        detect_by_key_prefix="hf_",
        detect_by_base_keyword="huggingface",
        default_api_base="https://router.huggingface.co/v1",
    ),
    # Skywork API platform (APIFree): OpenAI-compatible MaaS gateway.
    ProviderSpec(
        name="skywork",
        keywords=("skywork", "skyclaw", "apifree"),
        env_key="SKYWORK_API_KEY",
        display_name="Skywork",
        backend="openai_compat",
        env_extras=(("APIFREE_API_KEY", "{api_key}"),),
        is_gateway=True,
        detect_by_base_keyword="apifree.ai",
        default_api_base="https://api.apifree.ai/agent/v1",
    ),
    # AiHubMix: global gateway, OpenAI-compatible interface.
    # strip_model_prefix=True: doesn't understand "anthropic/claude-3",
    # strips to bare "claude-3".
    ProviderSpec(
        name="aihubmix",
        keywords=("aihubmix",),
        env_key="OPENAI_API_KEY",
        display_name="AiHubMix",
        backend="openai_compat",
        is_gateway=True,
        detect_by_base_keyword="aihubmix",
        default_api_base="https://aihubmix.com/v1",
        strip_model_prefix=True,
    ),
    # SiliconFlow (硅基流动): OpenAI-compatible gateway, model names keep org prefix
    ProviderSpec(
        name="siliconflow",
        keywords=("siliconflow",),
        env_key="OPENAI_API_KEY",
        display_name="SiliconFlow",
        backend="openai_compat",
        is_gateway=True,
        detect_by_base_keyword="siliconflow",
        default_api_base="https://api.siliconflow.cn/v1",
    ),

    # Novita AI: OpenAI-compatible gateway for hosted model APIs.
    ProviderSpec(
        name="novita",
        keywords=("novita",),
        env_key="NOVITA_API_KEY",
        display_name="Novita AI",
        backend="openai_compat",
        is_gateway=True,
        detect_by_base_keyword="novita",
        default_api_base="https://api.novita.ai/openai",
    ),

    # VolcEngine (火山引擎): OpenAI-compatible gateway, pay-per-use models
    ProviderSpec(
        name="volcengine",
        keywords=("volcengine", "volces", "ark"),
        env_key="OPENAI_API_KEY",
        display_name="VolcEngine",
        backend="openai_compat",
        is_gateway=True,
        detect_by_base_keyword="volces",
        default_api_base="https://ark.cn-beijing.volces.com/api/v3",
        thinking_style="thinking_type",
        supports_max_completion_tokens=True,
    ),

    # VolcEngine Coding Plan (火山引擎 Coding Plan): same key as volcengine
    ProviderSpec(
        name="volcengine_coding_plan",
        keywords=("volcengine-plan",),
        env_key="OPENAI_API_KEY",
        display_name="VolcEngine Coding Plan",
        backend="openai_compat",
        is_gateway=True,
        default_api_base="https://ark.cn-beijing.volces.com/api/coding/v3",
        strip_model_prefix=True,
        thinking_style="thinking_type",
        supports_max_completion_tokens=True,
    ),

    # BytePlus: VolcEngine international, pay-per-use models
    ProviderSpec(
        name="byteplus",
        keywords=("byteplus",),
        env_key="OPENAI_API_KEY",
        display_name="BytePlus",
        backend="openai_compat",
        is_gateway=True,
        detect_by_base_keyword="bytepluses",
        default_api_base="https://ark.ap-southeast.bytepluses.com/api/v3",
        strip_model_prefix=True,
        thinking_style="thinking_type",
    ),

    # BytePlus Coding Plan: same key as byteplus
    ProviderSpec(
        name="byteplus_coding_plan",
        keywords=("byteplus-plan",),
        env_key="OPENAI_API_KEY",
        display_name="BytePlus Coding Plan",
        backend="openai_compat",
        is_gateway=True,
        default_api_base="https://ark.ap-southeast.bytepluses.com/api/coding/v3",
        strip_model_prefix=True,
        thinking_style="thinking_type",
    ),


    # === Standard providers (matched by model-name keywords) ===============
    # Anthropic: native Anthropic SDK
    ProviderSpec(
        name="anthropic",
        keywords=("anthropic", "claude"),
        env_key="ANTHROPIC_API_KEY",
        display_name="Anthropic",
        backend="anthropic",
        supports_prompt_caching=True,
    ),
    # OpenAI: SDK default base URL (no override needed)
    ProviderSpec(
        name="openai",
        keywords=("openai", "gpt"),
        env_key="OPENAI_API_KEY",
        display_name="OpenAI",
        backend="openai_compat",
        supports_max_completion_tokens=True,
    ),
    # OpenAI Codex: OAuth-based, dedicated provider
    ProviderSpec(
        name="openai_codex",
        keywords=("openai-codex",),
        env_key="",
        display_name="OpenAI Codex",
        backend="openai_codex",
        detect_by_base_keyword="codex",
        default_api_base="https://chatgpt.com/backend-api",
        is_oauth=True,
    ),
    # GitHub Copilot: OAuth-based
    ProviderSpec(
        name="github_copilot",
        keywords=("github_copilot", "copilot"),
        env_key="",
        display_name="Github Copilot",
        backend="github_copilot",
        default_api_base="https://api.githubcopilot.com",
        strip_model_prefix=True,
        is_oauth=True,
        supports_max_completion_tokens=True,
    ),
    # DeepSeek: OpenAI-compatible at api.deepseek.com
    ProviderSpec(
        name="deepseek",
        keywords=("deepseek",),
        env_key="DEEPSEEK_API_KEY",
        display_name="DeepSeek",
        backend="openai_compat",
        default_api_base="https://api.deepseek.com",
        thinking_style="thinking_type",
    ),
    # Gemini: Google's OpenAI-compatible endpoint
    ProviderSpec(
        name="gemini",
        keywords=("gemini", "gemma"),
        env_key="GEMINI_API_KEY",
        display_name="Gemini",
        backend="openai_compat",
        default_api_base="https://generativelanguage.googleapis.com/v1beta/openai/",
    ),
    # Zhipu (智谱): OpenAI-compatible at open.bigmodel.cn
    ProviderSpec(
        name="zhipu",
        keywords=("zhipu", "glm", "zai"),
        env_key="ZAI_API_KEY",
        display_name="Zhipu AI",
        backend="openai_compat",
        env_extras=(("ZHIPUAI_API_KEY", "{api_key}"),),
        default_api_base="https://open.bigmodel.cn/api/paas/v4",
    ),
    # DashScope (通义): Qwen models, OpenAI-compatible endpoint
    ProviderSpec(
        name="dashscope",
        keywords=("qwen", "dashscope"),
        env_key="DASHSCOPE_API_KEY",
        display_name="DashScope",
        backend="openai_compat",
        default_api_base="https://dashscope.aliyuncs.com/compatible-mode/v1",
        thinking_style="enable_thinking",
    ),
    # Moonshot (月之暗面): Kimi K2.5 / K2.6 enforce temperature >= 1.0.
    ProviderSpec(
        name="moonshot",
        keywords=("moonshot", "kimi"),
        env_key="MOONSHOT_API_KEY",
        display_name="Moonshot",
        backend="openai_compat",
        default_api_base="https://api.moonshot.ai/v1",
        model_overrides=(
            ("kimi-k2.5", {"temperature": 1.0}),
            ("kimi-k2.6", {"temperature": 1.0}),
        ),
    ),
    # MiniMax: OpenAI-compatible API
    ProviderSpec(
        name="minimax",
        keywords=("minimax",),
        env_key="MINIMAX_API_KEY",
        display_name="MiniMax",
        backend="openai_compat",
        default_api_base="https://api.minimax.io/v1",
        thinking_style="reasoning_split",
    ),
    # MiniMax Anthropic-compatible endpoint: supports thinking mode
    ProviderSpec(
        name="minimax_anthropic",
        keywords=("minimax_anthropic",),
        env_key="MINIMAX_API_KEY",
        display_name="MiniMax (Anthropic)",
        backend="anthropic",
        default_api_base="https://api.minimax.io/anthropic",
    ),
    # Mistral AI: OpenAI-compatible API
    ProviderSpec(
        name="mistral",
        keywords=("mistral",),
        env_key="MISTRAL_API_KEY",
        display_name="Mistral",
        backend="openai_compat",
        default_api_base="https://api.mistral.ai/v1",
    ),
    # Step Fun (阶跃星辰): OpenAI-compatible API
    ProviderSpec(
        name="stepfun",
        keywords=("stepfun", "step"),
        env_key="STEPFUN_API_KEY",
        display_name="Step Fun",
        backend="openai_compat",
        default_api_base="https://api.stepfun.com/v1",
        reasoning_as_content=True,
    ),
    # Xiaomi MIMO (小米): OpenAI-compatible API
    # Hosted API (api.xiaomimimo.com) accepts {"thinking": {"type": "enabled"|"disabled"}}
    # to toggle reasoning, matching the existing thinking_type style.
    ProviderSpec(
        name="xiaomi_mimo",
        keywords=("xiaomi_mimo", "mimo"),
        env_key="XIAOMIMIMO_API_KEY",
        display_name="Xiaomi MIMO",
        backend="openai_compat",
        default_api_base="https://api.xiaomimimo.com/v1",
        thinking_style="thinking_type",
    ),
    # LongCat: OpenAI-compatible API
    ProviderSpec(
        name="longcat",
        keywords=("longcat",),
        env_key="LONGCAT_API_KEY",
        display_name="LongCat",
        backend="openai_compat",
        default_api_base="https://api.longcat.chat/openai/v1",
    ),
    # Ant Ling: OpenAI-compatible API for Ling/Ring model families.
    ProviderSpec(
        name="ant_ling",
        keywords=("ant_ling", "ant-ling", "ling-", "ring-"),
        env_key="ANT_LING_API_KEY",
        display_name="Ant Ling",
        backend="openai_compat",
        detect_by_base_keyword="ant-ling.com",
        default_api_base="https://api.ant-ling.com/v1",
    ),
    # === Local deployment (matched by config key, NOT by api_base) =========
    # vLLM / any OpenAI-compatible local server
    ProviderSpec(
        name="vllm",
        keywords=("vllm",),
        env_key="HOSTED_VLLM_API_KEY",
        display_name="vLLM",
        backend="openai_compat",
        is_local=True,
    ),
    # Ollama (local, OpenAI-compatible)
    ProviderSpec(
        name="ollama",
        keywords=("ollama", "nemotron"),
        env_key="OLLAMA_API_KEY",
        display_name="Ollama",
        backend="openai_compat",
        is_local=True,
        detect_by_base_keyword="11434",
        default_api_base="http://localhost:11434/v1",
    ),
    # LM Studio (local, OpenAI-compatible)
    ProviderSpec(
        name="lm_studio",
        keywords=("lm-studio", "lmstudio", "lm_studio"),
        env_key="LM_STUDIO_API_KEY",
        display_name="LM Studio",
        backend="openai_compat",
        is_local=True,
        detect_by_base_keyword="1234",
        default_api_base="http://localhost:1234/v1",
    ),
    # Atomic Chat (local, OpenAI-compatible) — https://atomic.chat/
    ProviderSpec(
        name="atomic_chat",
        keywords=("atomic-chat", "atomic_chat", "atomicchat"),
        env_key="ATOMIC_CHAT_API_KEY",
        display_name="Atomic Chat",
        backend="openai_compat",
        is_local=True,
        detect_by_base_keyword="1337",
        default_api_base="http://localhost:1337/v1",
    ),
    # === OpenVINO Model Server (direct, local, OpenAI-compatible at /v3) ===
    ProviderSpec(
        name="ovms",
        keywords=("openvino", "ovms"),
        env_key="",
        display_name="OpenVINO Model Server",
        backend="openai_compat",
        is_direct=True,
        is_local=True,
        default_api_base="http://localhost:8000/v3",
    ),
    # === NVIDIA NIM (NVIDIA Inference Microservices) =======================
    # Keys start with "nvapi-", base URL at integrate.api.nvidia.com
    ProviderSpec(
        name="nvidia",
        keywords=("nvidia", "nemotron", "nvapi"),
        env_key="NVIDIA_NIM_API_KEY",
        display_name="NVIDIA NIM",
        backend="openai_compat",
        is_gateway=False,
        detect_by_key_prefix="nvapi-",
        detect_by_base_keyword="nvidia.com",
        default_api_base="https://integrate.api.nvidia.com/v1",
    ),
    # === Auxiliary (not a primary LLM provider) ============================
    # Groq: mainly used for Whisper voice transcription, also usable for LLM
    ProviderSpec(
        name="groq",
        keywords=("groq",),
        env_key="GROQ_API_KEY",
        display_name="Groq",
        backend="openai_compat",
        default_api_base="https://api.groq.com/openai/v1",
    ),
    # AssemblyAI: voice transcription only. It appears in provider settings so
    # users can manage credentials, but WebUI excludes it from chat model pickers.
    ProviderSpec(
        name="assemblyai",
        keywords=("assemblyai",),
        env_key="ASSEMBLYAI_API_KEY",
        display_name="AssemblyAI",
        backend="openai_compat",
        default_api_base="https://api.assemblyai.com/v2",
        is_transcription_only=True,
    ),
    # Qianfan (百度千帆): OpenAI-compatible API
    ProviderSpec(
        name="qianfan",
        keywords=("qianfan", "ernie"),
        env_key="QIANFAN_API_KEY",
        display_name="Qianfan",
        backend="openai_compat",
        default_api_base="https://qianfan.baidubce.com/v2"
    ),
)


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------


def find_by_name(name: str) -> ProviderSpec | None:
    """按配置字段名查找 ProviderSpec。"""
    normalized = to_snake(name.replace("-", "_"))
    for spec in PROVIDERS:
        if spec.name == normalized:
            return spec
    return None
