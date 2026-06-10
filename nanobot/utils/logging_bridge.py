"""日志桥接工具。

【中文名称】日志桥接工具

【功能说明】
负责把标准 logging 日志转接到 loguru，统一 nanobot 的日志输出格式。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""
from __future__ import annotations

import logging

from loguru import logger


class _LoguruBridge(logging.Handler):
    """_LoguruBridge 类。

    【中文名称】_LoguruBridge

    【功能说明】
    这是 日志桥接工具 中的核心数据结构或服务类。负责把标准 logging 日志转接到 loguru，统一 nanobot 的日志输出格式。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    _LEVEL_MAP: dict[int, str] = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def __init__(self, lib_name: str) -> None:
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 日志桥接工具 中的一个步骤函数，用来支撑：负责把标准 logging 日志转接到 loguru，统一 nanobot 的日志输出格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - lib_name: 调用方传入的 `lib_name` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        super().__init__()
        self.lib_name = lib_name

    def emit(self, record: logging.LogRecord) -> None:
        """执行 `emit`。

        【中文名称】emit

        【功能说明】
        这是 日志桥接工具 中的一个步骤函数，用来支撑：负责把标准 logging 日志转接到 loguru，统一 nanobot 的日志输出格式。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - record: 调用方传入的 `record` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        level = self._LEVEL_MAP.get(record.levelno, "INFO")
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame, depth = frame.f_back, depth + 1
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, "[{lib}] {message}", lib=self.lib_name, message=record.getMessage()
        )


def redirect_lib_logging(name: str, level: str | None = None) -> None:
    """执行 `redirect_lib_logging`。

    【中文名称】redirect_lib_logging

    【功能说明】
    这是 日志桥接工具 中的一个步骤函数，用来支撑：负责把标准 logging 日志转接到 loguru，统一 nanobot 的日志输出格式。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - name: 调用方传入的 `name` 数据；具体类型以函数签名为准。
    - level: 调用方传入的 `level` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    lib_logger = logging.getLogger(name)
    if not any(isinstance(h, _LoguruBridge) for h in lib_logger.handlers):
        handler = _LoguruBridge(name)
        if level is not None:
            handler.setLevel(getattr(logging, level.upper(), logging.WARNING))
        lib_logger.handlers = [handler]
        lib_logger.propagate = False
