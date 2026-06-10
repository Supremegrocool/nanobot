"""工具系统基础抽象。

这个文件定义了 nanobot 工具系统最底层的两个概念：

1. ``Schema``：工具参数的 JSON Schema 片段描述
2. ``Tool``：一个可被模型调用的能力单元

理解这里之后，再看 ``filesystem.py`` / ``shell.py`` / ``web.py`` 等具体工具实现，
会清楚很多。
"""
from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from collections.abc import Callable
from copy import deepcopy
from typing import Any, TypeVar

if typing.TYPE_CHECKING:
    from pydantic import BaseModel

    from nanobot.agent.tools.context import ToolContext

_ToolT = TypeVar("_ToolT", bound="Tool")

# 这个映射定义了 JSON Schema 基本类型到 Python 运行时类型的对应关系。
# ``Tool._cast_value`` 和 ``Schema.validate_json_schema_value`` 都会用到它。
_JSON_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "array": list,
    "object": dict,
}


class Schema(ABC):
    """工具参数 JSON Schema 片段的抽象基类。

    具体子类在 ``nanobot.agent.tools.schema`` 里，比如：
    - ``StringSchema``
    - ``IntegerSchema``
    - ``ObjectSchema``

    这些类的意义是：让工具作者可以用更 Python 化的方式描述参数结构，
    最后再统一导出成模型函数调用所需的 JSON Schema。
    """

    @staticmethod
    def resolve_json_schema_type(t: Any) -> str | None:
        """从 JSON Schema ``type`` 中提取非 null 的主类型。"""
        if isinstance(t, list):
            return next((x for x in t if x != "null"), None)
        return t  # type: ignore[return-value]

    @staticmethod
    def subpath(path: str, key: str) -> str:
        """拼接错误路径，例如 ``foo.bar``。"""
        return f"{path}.{key}" if path else key

    @staticmethod
    def validate_json_schema_value(val: Any, schema: dict[str, Any], path: str = "") -> list[str]:
        """按 JSON Schema 片段校验一个值，返回错误信息列表。

        返回空列表表示校验通过。
        这是 ``Tool.validate_params`` 以及各具体 Schema 的公共校验核心。
        """
        raw_type = schema.get("type")
        nullable = (isinstance(raw_type, list) and "null" in raw_type) or schema.get("nullable", False)
        t = Schema.resolve_json_schema_type(raw_type)
        label = path or "parameter"

        if nullable and val is None:
            return []
        if t == "integer" and (not isinstance(val, int) or isinstance(val, bool)):
            return [f"{label} should be integer"]
        if t == "number" and (
            not isinstance(val, _JSON_TYPE_MAP["number"]) or isinstance(val, bool)
        ):
            return [f"{label} should be number"]
        if t in _JSON_TYPE_MAP and t not in ("integer", "number") and not isinstance(val, _JSON_TYPE_MAP[t]):
            return [f"{label} should be {t}"]

        errors: list[str] = []
        if "enum" in schema and val not in schema["enum"]:
            errors.append(f"{label} must be one of {schema['enum']}")
        if t in ("integer", "number"):
            if "minimum" in schema and val < schema["minimum"]:
                errors.append(f"{label} must be >= {schema['minimum']}")
            if "maximum" in schema and val > schema["maximum"]:
                errors.append(f"{label} must be <= {schema['maximum']}")
        if t == "string":
            if "minLength" in schema and len(val) < schema["minLength"]:
                errors.append(f"{label} must be at least {schema['minLength']} chars")
            if "maxLength" in schema and len(val) > schema["maxLength"]:
                errors.append(f"{label} must be at most {schema['maxLength']} chars")
        if t == "object":
            props = schema.get("properties", {})
            for k in schema.get("required", []):
                if k not in val:
                    errors.append(f"missing required {Schema.subpath(path, k)}")
            for k, v in val.items():
                if k in props:
                    errors.extend(Schema.validate_json_schema_value(v, props[k], Schema.subpath(path, k)))
        if t == "array":
            if "minItems" in schema and len(val) < schema["minItems"]:
                errors.append(f"{label} must have at least {schema['minItems']} items")
            if "maxItems" in schema and len(val) > schema["maxItems"]:
                errors.append(f"{label} must be at most {schema['maxItems']} items")
            if "items" in schema:
                prefix = f"{path}[{{}}]" if path else "[{}]"
                for i, item in enumerate(val):
                    errors.extend(
                        Schema.validate_json_schema_value(item, schema["items"], prefix.format(i))
                    )
        return errors

    @staticmethod
    def fragment(value: Any) -> dict[str, Any]:
        """把 Schema 实例或原始 dict 统一规范成 JSON Schema 片段 dict。"""
        # 先尝试 ``to_json_schema()``，以区分“Schema 实例”和“本来就是 dict 的 schema”
        to_js = getattr(value, "to_json_schema", None)
        if callable(to_js):
            return to_js()
        if isinstance(value, dict):
            return value
        raise TypeError(f"Expected schema object or dict, got {type(value).__name__}")

    @abstractmethod
    def to_json_schema(self) -> dict[str, Any]:
        """导出成 JSON Schema 片段字典。"""
        ...

    def validate_value(self, value: Any, path: str = "") -> list[str]:
        """校验单个值；返回空列表表示通过。"""
        return Schema.validate_json_schema_value(value, self.to_json_schema(), path)


class Tool(ABC):
    """Agent 可调用工具的抽象基类。

    你可以把 Tool 理解成“暴露给模型的一项能力”，例如：
    - 读文件
    - 写文件
    - 执行 shell
    - 网页搜索
    - 发送消息
    """

    _TYPE_MAP = _JSON_TYPE_MAP
    _BOOL_TRUE = frozenset(("true", "1", "yes"))
    _BOOL_FALSE = frozenset(("false", "0", "no"))

    @staticmethod
    def _resolve_type(t: Any) -> str | None:
        """Pick first non-null type from JSON Schema unions like ``['string','null']``."""
        return Schema.resolve_json_schema_type(t)

    @property
    @abstractmethod
    def name(self) -> str:
        """工具名：模型发起函数调用时使用的唯一名字。"""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """工具说明：会进入 prompt，帮助模型理解这个工具做什么。"""
        ...

    @property
    @abstractmethod
    def parameters(self) -> dict[str, Any]:
        """工具参数的 JSON Schema。"""
        ...

    @property
    def read_only(self) -> bool:
        """该工具是否无副作用，并适合并行执行。"""
        return False

    @property
    def concurrency_safe(self) -> bool:
        """该工具是否可以与其他并发安全工具一起执行。"""
        return self.read_only and not self.exclusive

    @property
    def exclusive(self) -> bool:
        """即便启用了并发，这个工具是否也必须独占执行。"""
        return False

    # --- 插件元数据：供 ToolLoader / 插件系统使用 ---

    config_key: str = ""
    _plugin_discoverable: bool = True
    _scopes: set[str] = {"core"}

    @classmethod
    def config_cls(cls) -> type[BaseModel] | None:
        """返回该工具对应的配置模型类；没有则返回 ``None``。"""
        return None

    @classmethod
    def enabled(cls, ctx: ToolContext) -> bool:
        """判断当前上下文下该工具是否应启用。"""
        return True

    @classmethod
    def create(cls, ctx: ToolContext) -> Tool:
        """按上下文创建工具实例。"""
        return cls()

    @abstractmethod
    async def execute(self, **kwargs: Any) -> Any:
        """执行工具主体。"""
        ...

    def _cast_object(self, obj: Any, schema: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(obj, dict):
            return obj
        props = schema.get("properties", {})
        return {k: self._cast_value(v, props[k]) if k in props else v for k, v in obj.items()}

    def cast_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """在校验前做一层安全的、基于 schema 的类型转换。"""
        schema = self.parameters or {}
        if schema.get("type", "object") != "object":
            return params
        return self._cast_object(params, schema)

    def _cast_value(self, val: Any, schema: dict[str, Any]) -> Any:
        t = self._resolve_type(schema.get("type"))

        if t == "boolean" and isinstance(val, bool):
            return val
        if t == "integer" and isinstance(val, int) and not isinstance(val, bool):
            return val
        if t in self._TYPE_MAP and t not in ("boolean", "integer", "array", "object"):
            expected = self._TYPE_MAP[t]
            if isinstance(val, expected):
                return val

        if isinstance(val, str) and t in ("integer", "number"):
            try:
                return int(val) if t == "integer" else float(val)
            except ValueError:
                return val

        if t == "string":
            return val if val is None else str(val)

        if t == "boolean" and isinstance(val, str):
            low = val.lower()
            if low in self._BOOL_TRUE:
                return True
            if low in self._BOOL_FALSE:
                return False
            return val

        if t == "array" and isinstance(val, list):
            items = schema.get("items")
            return [self._cast_value(x, items) for x in val] if items else val

        if t == "object" and isinstance(val, dict):
            return self._cast_object(val, schema)

        return val

    def validate_params(self, params: dict[str, Any]) -> list[str]:
        """按 JSON Schema 校验参数。"""
        if not isinstance(params, dict):
            return [f"parameters must be an object, got {type(params).__name__}"]
        schema = self.parameters or {}
        if schema.get("type", "object") != "object":
            raise ValueError(f"Schema must be object type, got {schema.get('type')!r}")
        return Schema.validate_json_schema_value(params, {**schema, "type": "object"}, "")

    def to_schema(self) -> dict[str, Any]:
        """导出成 OpenAI 风格函数调用 schema。"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


def tool_parameters(schema: dict[str, Any]) -> Callable[[type[_ToolT]], type[_ToolT]]:
    """类装饰器：把 JSON Schema 挂到 ``Tool`` 子类上。

    工具作者可以不用手写 ``@property def parameters``，直接用这个装饰器即可。

    示例::

        @tool_parameters({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        })
        class ReadFileTool(Tool):
            ...
    """

    def decorator(cls: type[_ToolT]) -> type[_ToolT]:
        frozen = deepcopy(schema)

        @property
        def parameters(self: Any) -> dict[str, Any]:
            return deepcopy(frozen)

        cls.parameters = parameters  # type: ignore[assignment]

        abstract = getattr(cls, "__abstractmethods__", None)
        if abstract is not None and "parameters" in abstract:
            cls.__abstractmethods__ = frozenset(abstract - {"parameters"})  # type: ignore[misc]

        return cls

    return decorator
