"""
日志核心 (Logger Core)
=======================

参考设计:
- structlog: BoundLogger + 处理器链
- loguru: 简洁 API + 延迟格式化
- Python logging: 标准接口兼容

提供统一的日志接口，支持结构化日志和上下文绑定。
"""
from __future__ import annotations
from datetime import datetime
from enum import IntEnum
from typing import Any, Dict, List, Optional
import threading
import logging as stdlib_logging

from .context import get_context
from .formatters import Formatter, ColoredFormatter
from .handlers import LogHandler, ConsoleHandler


class LogLevel(IntEnum):
    """日志级别"""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50
    FATAL = 60

    @classmethod
    def from_string(cls, level: str) -> 'LogLevel':
        """从字符串解析日志级别"""
        mapping = {
            'DEBUG': cls.DEBUG,
            'INFO': cls.INFO,
            'WARNING': cls.WARNING,
            'WARN': cls.WARNING,
            'ERROR': cls.ERROR,
            'CRITICAL': cls.CRITICAL,
            'FATAL': cls.FATAL,
        }
        return mapping.get(level.upper(), cls.INFO)


class AStockLogger:
    """AStock 结构化日志器

    核心特性:
    - 结构化日志：支持任意 key-value 数据
    - 上下文绑定：自动携带 LogContext 数据
    - 延迟求值：支持 lambda 延迟格式化
    - 多处理器：同时输出到多个目标

    Example:
        logger = AStockLogger("my.module")
        logger.info("Processing", count=100, file="data.csv")

        # 绑定上下文
        bound = logger.bind(request_id="abc")
        bound.info("In request")  # 自动携带 request_id

        # 延迟求值
        logger.debug("Expensive: {}", lambda: expensive_computation())
    """

    def __init__(
        self,
        name: str,
        level: LogLevel = LogLevel.DEBUG,
        handlers: Optional[List[LogHandler]] = None,
        propagate: bool = True,
    ):
        self.name = name
        self.level = level
        self._handlers = handlers or []
        self.propagate = propagate
        self._bound_context: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def bind(self, **kwargs) -> 'AStockLogger':
        """绑定上下文数据，返回新的 logger

        Example:
            bound = logger.bind(user_id="user1")
            bound.info("User action")  # 携带 user_id
        """
        new_logger = AStockLogger(
            name=self.name,
            level=self.level,
            handlers=self._handlers,
            propagate=self.propagate,
        )
        new_logger._bound_context = {**self._bound_context, **kwargs}
        return new_logger

    def unbind(self, *keys: str) -> 'AStockLogger':
        """解除绑定，返回新的 logger"""
        new_logger = AStockLogger(
            name=self.name,
            level=self.level,
            handlers=self._handlers,
            propagate=self.propagate,
        )
        new_logger._bound_context = {
            k: v for k, v in self._bound_context.items() if k not in keys
        }
        return new_logger

    def add_handler(self, handler: LogHandler) -> 'AStockLogger':
        """添加处理器"""
        self._handlers.append(handler)
        return self

    def remove_handler(self, handler: LogHandler) -> 'AStockLogger':
        """移除处理器"""
        if handler in self._handlers:
            self._handlers.remove(handler)
        return self

    def _log(
        self,
        level: LogLevel,
        message: str,
        *args,
        exception: Optional[BaseException] = None,
        **kwargs
    ) -> None:
        """内部日志方法"""
        if level < self.level:
            return

        # 格式化消息（支持 {} 占位符和延迟求值）
        if args:
            resolved_args = []
            for arg in args:
                if callable(arg):
                    try:
                        resolved_args.append(arg())
                    except Exception as e:
                        resolved_args.append(f"<error: {e}>")
                else:
                    resolved_args.append(arg)
            try:
                message = message.format(*resolved_args)
            except Exception:
                pass

        # 合并上下文
        context = {
            **get_context(),  # LogContext 上下文
            **self._bound_context,  # 绑定上下文
            **kwargs,  # 调用时上下文
        }

        timestamp = datetime.now()
        level_name = level.name

        # 发送到所有处理器
        for handler in self._handlers:
            try:
                handler.emit(level_name, message, self.name, timestamp, context, exception)
            except Exception:
                pass  # 处理器错误不应影响主流程

        # 传播到全局处理器
        if self.propagate:
            for handler in _global_handlers:
                try:
                    handler.emit(level_name, message, self.name, timestamp, context, exception)
                except Exception:
                    pass

    def debug(self, message: str, *args, **kwargs) -> None:
        """记录 DEBUG 级别日志"""
        self._log(LogLevel.DEBUG, message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs) -> None:
        """记录 INFO 级别日志"""
        self._log(LogLevel.INFO, message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs) -> None:
        """记录 WARNING 级别日志"""
        self._log(LogLevel.WARNING, message, *args, **kwargs)

    def warn(self, message: str, *args, **kwargs) -> None:
        """warning 的别名"""
        self.warning(message, *args, **kwargs)

    def error(self, message: str, *args, exception: Optional[BaseException] = None, **kwargs) -> None:
        """记录 ERROR 级别日志"""
        self._log(LogLevel.ERROR, message, *args, exception=exception, **kwargs)

    def critical(self, message: str, *args, exception: Optional[BaseException] = None, **kwargs) -> None:
        """记录 CRITICAL 级别日志"""
        self._log(LogLevel.CRITICAL, message, *args, exception=exception, **kwargs)

    def fatal(self, message: str, *args, exception: Optional[BaseException] = None, **kwargs) -> None:
        """记录 FATAL 级别日志"""
        self._log(LogLevel.FATAL, message, *args, exception=exception, **kwargs)

    def exception(self, message: str, *args, **kwargs) -> None:
        """记录异常（自动附加当前异常信息）"""
        import sys
        exc_info = sys.exc_info()
        exception = exc_info[1] if exc_info[1] else None
        self._log(LogLevel.ERROR, message, *args, exception=exception, **kwargs)

    def log(self, level: str | LogLevel, message: str, *args, **kwargs) -> None:
        """通用日志方法"""
        if isinstance(level, str):
            level = LogLevel.from_string(level)
        self._log(level, message, *args, **kwargs)


# 全局处理器列表
_global_handlers: List[LogHandler] = []
_loggers: Dict[str, AStockLogger] = {}
_lock = threading.Lock()


def get_logger(name: str = "__main__") -> AStockLogger:
    """获取或创建 logger

    Example:
        logger = get_logger(__name__)
        logger.info("Hello")
    """
    with _lock:
        if name not in _loggers:
            _loggers[name] = AStockLogger(name)
        return _loggers[name]


def configure_logging(
    level: str | LogLevel = LogLevel.INFO,
    handlers: Optional[List[LogHandler]] = None,
    formatter: Optional[Formatter] = None,
    enable_stdlib: bool = True,
) -> None:
    """配置全局日志

    Args:
        level: 最小日志级别
        handlers: 处理器列表（默认 ConsoleHandler）
        formatter: 格式化器
        enable_stdlib: 是否同时配置标准库 logging

    Example:
        configure_logging(
            level="DEBUG",
            handlers=[
                ConsoleHandler(formatter=ColoredFormatter()),
                FileHandler("app.log"),
            ]
        )
    """
    global _global_handlers

    if isinstance(level, str):
        level = LogLevel.from_string(level)

    # 设置全局处理器
    if handlers:
        _global_handlers = handlers
    else:
        default_handler = ConsoleHandler(
            formatter=formatter or ColoredFormatter(),
            min_level=level.name,
        )
        _global_handlers = [default_handler]

    # 更新已有 logger 的级别
    with _lock:
        for logger in _loggers.values():
            logger.level = level

    # 配置标准库 logging
    if enable_stdlib:
        stdlib_logging.basicConfig(
            level=getattr(stdlib_logging, level.name, stdlib_logging.INFO),
            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        )


def reset_logging() -> None:
    """重置日志配置"""
    global _global_handlers, _loggers

    # 关闭所有处理器
    for handler in _global_handlers:
        try:
            handler.close()
        except Exception:
            pass

    _global_handlers = []

    with _lock:
        _loggers.clear()


# 便捷接口：模块级别的 logger
_root_logger: Optional[AStockLogger] = None


def _get_root() -> AStockLogger:
    global _root_logger
    if _root_logger is None:
        _root_logger = get_logger("astock")
    return _root_logger


def debug(message: str, *args, **kwargs) -> None:
    _get_root().debug(message, *args, **kwargs)


def info(message: str, *args, **kwargs) -> None:
    _get_root().info(message, *args, **kwargs)


def warning(message: str, *args, **kwargs) -> None:
    _get_root().warning(message, *args, **kwargs)


def error(message: str, *args, **kwargs) -> None:
    _get_root().error(message, *args, **kwargs)


def critical(message: str, *args, **kwargs) -> None:
    _get_root().critical(message, *args, **kwargs)
