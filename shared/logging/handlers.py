"""
日志处理器 (Log Handlers)
==========================

参考设计:
- Python logging: Handler 接口
- loguru: sink 概念
- structlog: 处理器链

支持多种输出目标：控制台、文件、EventBus。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, TextIO
import sys
import threading
import queue
import atexit

from .formatters import Formatter, ConsoleFormatter, JSONFormatter


class LogHandler(ABC):
    """日志处理器基类"""

    def __init__(
        self,
        formatter: Optional[Formatter] = None,
        min_level: str = "DEBUG",
    ):
        self.formatter = formatter or ConsoleFormatter()
        self.min_level = min_level
        self._level_order = {
            'DEBUG': 10,
            'INFO': 20,
            'WARNING': 30,
            'ERROR': 40,
            'CRITICAL': 50,
            'FATAL': 60,
        }

    def should_handle(self, level: str) -> bool:
        """检查是否应该处理此级别的日志"""
        return self._level_order.get(level.upper(), 0) >= self._level_order.get(self.min_level.upper(), 0)

    @abstractmethod
    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        """发送日志记录"""

    def close(self) -> None:
        """关闭处理器"""


class ConsoleHandler(LogHandler):
    """控制台输出处理器"""

    def __init__(
        self,
        stream: TextIO = None,
        formatter: Optional[Formatter] = None,
        min_level: str = "DEBUG",
        error_stream: TextIO = None,
    ):
        super().__init__(formatter, min_level)
        self.stream = stream or sys.stdout
        self.error_stream = error_stream or sys.stderr
        self._lock = threading.Lock()

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        if not self.should_handle(level):
            return

        formatted = self.formatter.format(
            level, message, logger_name, timestamp, context, exception
        )

        # 错误级别以上写入 stderr
        stream = self.error_stream if level.upper() in ('ERROR', 'CRITICAL', 'FATAL') else self.stream

        with self._lock:
            try:
                stream.write(formatted + '\n')
                stream.flush()
            except Exception:
                pass  # 防止日志系统自身出错


class FileHandler(LogHandler):
    """文件输出处理器"""

    def __init__(
        self,
        file_path: str | Path,
        formatter: Optional[Formatter] = None,
        min_level: str = "DEBUG",
        encoding: str = "utf-8",
        mode: str = "a",
    ):
        # 文件日志默认使用 JSON 格式
        super().__init__(formatter or JSONFormatter(), min_level)
        self.file_path = Path(file_path)
        self.encoding = encoding
        self._file: Optional[TextIO] = None
        self._lock = threading.Lock()

        # 确保目录存在
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.file_path, mode, encoding=encoding)

        # 注册清理函数
        atexit.register(self.close)

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        if not self.should_handle(level) or not self._file:
            return

        formatted = self.formatter.format(
            level, message, logger_name, timestamp, context, exception
        )

        with self._lock:
            try:
                self._file.write(formatted + '\n')
                self._file.flush()
            except Exception:
                pass

    def close(self) -> None:
        with self._lock:
            if self._file:
                try:
                    self._file.close()
                except Exception:
                    pass
                self._file = None


class RotatingFileHandler(FileHandler):
    """轮转文件处理器

    支持按大小或时间轮转日志文件。
    """

    def __init__(
        self,
        file_path: str | Path,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        formatter: Optional[Formatter] = None,
        min_level: str = "DEBUG",
        encoding: str = "utf-8",
    ):
        super().__init__(file_path, formatter, min_level, encoding)
        self.max_bytes = max_bytes
        self.backup_count = backup_count

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        # 检查是否需要轮转
        if self._should_rotate():
            self._rotate()

        super().emit(level, message, logger_name, timestamp, context, exception)

    def _should_rotate(self) -> bool:
        if not self._file:
            return False
        try:
            return self.file_path.stat().st_size >= self.max_bytes
        except OSError:
            return False

    def _rotate(self) -> None:
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None

            # 轮转文件
            for i in range(self.backup_count - 1, 0, -1):
                src = Path(f"{self.file_path}.{i}")
                dst = Path(f"{self.file_path}.{i + 1}")
                if src.exists():
                    src.rename(dst)

            # 当前文件变成 .1
            if self.file_path.exists():
                self.file_path.rename(Path(f"{self.file_path}.1"))

            # 重新打开文件
            self._file = open(self.file_path, 'w', encoding=self.encoding)


class EventBusHandler(LogHandler):
    """EventBus 日志处理器

    将日志事件发布到 EventBus，支持订阅和监控。
    """

    def __init__(
        self,
        min_level: str = "WARNING",
        event_type: str = "log",
    ):
        super().__init__(None, min_level)  # EventBus 不需要 formatter
        self.event_type = event_type

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        if not self.should_handle(level):
            return

        try:
            from shared.event_bus import EventBus

            # 构建日志事件
            event_data = {
                'level': level.upper(),
                'message': message,
                'logger': logger_name,
                'timestamp': timestamp.isoformat(),
                'context': context,
            }

            if exception:
                event_data['exception'] = {
                    'type': type(exception).__name__,
                    'message': str(exception),
                }

            # 发布事件
            EventBus.get().emit_raw(self.event_type, event_data)
        except ImportError:
            pass  # EventBus 不可用
        except Exception:
            pass  # 防止日志系统出错


class AsyncHandler(LogHandler):
    """异步日志处理器

    使用队列实现异步写入，避免阻塞主线程。
    """

    def __init__(
        self,
        wrapped_handler: LogHandler,
        queue_size: int = 10000,
    ):
        super().__init__(wrapped_handler.formatter, wrapped_handler.min_level)
        self._wrapped = wrapped_handler
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

        atexit.register(self.close)

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        if not self.should_handle(level):
            return

        try:
            self._queue.put_nowait((level, message, logger_name, timestamp, context, exception))
        except queue.Full:
            pass  # 队列满了就丢弃

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                record = self._queue.get(timeout=0.1)
                self._wrapped.emit(*record)
            except queue.Empty:
                continue
            except Exception:
                pass

    def close(self) -> None:
        self._stop_event.set()
        self._worker.join(timeout=2.0)
        self._wrapped.close()


class NullHandler(LogHandler):
    """空处理器

    用于禁用日志输出。
    """

    def emit(
        self,
        level: str,
        message: str,
        logger_name: str,
        timestamp: datetime,
        context: Dict[str, Any],
        exception: Optional[BaseException] = None,
    ) -> None:
        pass
