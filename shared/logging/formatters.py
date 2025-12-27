"""
日志格式化器 (Log Formatters)
==============================

参考设计:
- structlog: 处理器链式转换
- loguru: 彩色输出
- Python logging: Formatter 接口

支持多种输出格式：纯文本、彩色、JSON。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional
import json
import sys


class Formatter(ABC):
    """格式化器基类"""

    @abstractmethod
    def format(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> str:
        """格式化日志记录"""
        pass


class ConsoleFormatter(Formatter):
    """控制台格式化器

    人类可读的纯文本格式。

    Output:
        2024-01-15 10:30:45.123 | INFO | module.name | Processing started | {"count": 100}
    """

    def __init__(
        self,
        timestamp_format: str = "%Y-%m-%d %H:%M:%S.%f",
        include_context: bool = True,
        truncate_context: int = 200,
    ):
        self.timestamp_format = timestamp_format
        self.include_context = include_context
        self.truncate_context = truncate_context

    def format(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> str:
        ts = timestamp.strftime(self.timestamp_format)[:23]  # 截断微秒

        parts = [ts, level.upper().ljust(8), logger_name, message]

        if self.include_context and context:
            ctx_str = json.dumps(context, ensure_ascii=False, default=str)
            if len(ctx_str) > self.truncate_context:
                ctx_str = ctx_str[:self.truncate_context] + "..."
            parts.append(ctx_str)

        line = " | ".join(parts)

        if exception:
            import traceback
            line += "\n" + "".join(traceback.format_exception(
                type(exception), exception, exception.__traceback__
            ))

        return line


class ColoredFormatter(Formatter):
    """彩色格式化器

    参考 loguru 的彩色输出风格。
    """

    # ANSI 颜色码
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'FATAL': '\033[41m',     # Red background
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'

    def __init__(
        self,
        timestamp_format: str = "%H:%M:%S",
        include_context: bool = True,
        force_colors: bool = False,
    ):
        self.timestamp_format = timestamp_format
        self.include_context = include_context
        # 检测是否支持颜色
        self.use_colors = force_colors or (
            hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
        )

    def _colorize(self, text: str, color: str) -> str:
        if self.use_colors:
            return f"{color}{text}{self.RESET}"
        return text

    def format(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> str:
        level_upper = level.upper()
        color = self.COLORS.get(level_upper, '')

        ts = timestamp.strftime(self.timestamp_format)
        ts_colored = self._colorize(ts, self.DIM)
        level_colored = self._colorize(level_upper.ljust(8), color)
        name_colored = self._colorize(logger_name, self.DIM)
        msg_colored = self._colorize(message, self.BOLD if level_upper in ('ERROR', 'CRITICAL') else '')

        parts = [ts_colored, level_colored, name_colored, msg_colored]

        if self.include_context and context:
            ctx_str = " ".join(f"{k}={v}" for k, v in context.items())
            parts.append(self._colorize(ctx_str, self.DIM))

        line = " | ".join(parts)

        if exception:
            import traceback
            exc_text = "".join(traceback.format_exception(
                type(exception), exception, exception.__traceback__
            ))
            line += "\n" + self._colorize(exc_text, self.COLORS['ERROR'])

        return line


class JSONFormatter(Formatter):
    """JSON 格式化器

    参考 structlog 的 JSON 处理器。
    适合日志收集系统（ELK, Loki 等）。

    Output:
        {"timestamp": "2024-01-15T10:30:45.123", "level": "INFO", "logger": "module", "message": "...", "context": {...}}
    """

    def __init__(
        self,
        timestamp_key: str = "timestamp",
        level_key: str = "level",
        logger_key: str = "logger",
        message_key: str = "message",
        context_key: str = "context",
        exception_key: str = "exception",
        flatten_context: bool = False,
        indent: Optional[int] = None,
    ):
        self.timestamp_key = timestamp_key
        self.level_key = level_key
        self.logger_key = logger_key
        self.message_key = message_key
        self.context_key = context_key
        self.exception_key = exception_key
        self.flatten_context = flatten_context
        self.indent = indent

    def format(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> str:
        record = {
            self.timestamp_key: timestamp.isoformat(),
            self.level_key: level.upper(),
            self.logger_key: logger_name,
            self.message_key: message,
        }

        if context:
            if self.flatten_context:
                # 扁平化：直接合并到顶层
                record.update(context)
            else:
                record[self.context_key] = context

        if exception:
            import traceback
            record[self.exception_key] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                ),
            }

        return json.dumps(record, ensure_ascii=False, default=str, indent=self.indent)


class CompactFormatter(Formatter):
    """紧凑格式化器

    适合高性能场景，最小化输出。
    """

    def format(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> str:
        # 超紧凑格式：[LEVEL] message
        line = f"[{level[0]}] {message}"
        if exception:
            line += f" | {type(exception).__name__}: {exception}"
        return line


# 默认格式化器
DEFAULT_FORMATTER = ColoredFormatter()
