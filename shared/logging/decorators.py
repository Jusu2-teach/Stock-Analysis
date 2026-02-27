"""
日志装饰器 (Log Decorators)
============================

参考设计:
- loguru: @logger.catch
- structlog: 自动记录函数调用

提供便捷的装饰器用于记录函数调用。
"""
from __future__ import annotations
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar
import time

from .logger import get_logger, LogLevel

F = TypeVar('F', bound=Callable[..., Any])


def log_call(
    level: str | LogLevel = LogLevel.DEBUG,
    logger_name: Optional[str] = None,
    log_args: bool = True,
    log_result: bool = False,
    max_arg_length: int = 100,
) -> Callable[[F], F]:
    """记录函数调用的装饰器

    Example:
        @log_call()
        def process_data(df, config):
            ...

        # 输出: DEBUG | Calling process_data(df=<DataFrame>, config=...)
        # 输出: DEBUG | process_data returned in 1.23s
    """
    if isinstance(level, str):
        level = LogLevel.from_string(level)

    def decorator(func: F) -> F:
        nonlocal logger_name
        if logger_name is None:
            logger_name = func.__module__

        logger = get_logger(logger_name)

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__qualname__

            # 记录调用
            if log_args:
                # 格式化参数（截断长参数）
                arg_strs = []
                for i, arg in enumerate(args):
                    arg_repr = _truncate_repr(arg, max_arg_length)
                    arg_strs.append(arg_repr)
                for key, value in kwargs.items():
                    value_repr = _truncate_repr(value, max_arg_length)
                    arg_strs.append(f"{key}={value_repr}")

                args_str = ", ".join(arg_strs)
                logger.log(level, f"Calling {func_name}({args_str})")
            else:
                logger.log(level, f"Calling {func_name}()")

            start_time = time.perf_counter()

            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start_time

                if log_result:
                    result_repr = _truncate_repr(result, max_arg_length)
                    logger.log(level, f"{func_name} returned {result_repr} in {duration:.3f}s")
                else:
                    logger.log(level, f"{func_name} completed in {duration:.3f}s")

                return result

            except Exception as e:
                duration = time.perf_counter() - start_time
                logger.error(
                    f"{func_name} failed after {duration:.3f}s: {type(e).__name__}: {e}",
                    exception=e,
                )
                raise

        return wrapper  # type: ignore

    return decorator


def log_errors(
    *exception_types: Type[BaseException],
    level: str | LogLevel = LogLevel.ERROR,
    logger_name: Optional[str] = None,
    reraise: bool = True,
    message: Optional[str] = None,
) -> Callable[[F], F]:
    """捕获并记录异常的装饰器

    Example:
        @log_errors(ValueError, TypeError)
        def risky_operation():
            ...

        @log_errors(reraise=False, message="Optional operation failed")
        def optional_task():
            ...
    """
    if isinstance(level, str):
        level = LogLevel.from_string(level)

    if not exception_types:
        exception_types = (Exception,)

    def decorator(func: F) -> F:
        nonlocal logger_name
        if logger_name is None:
            logger_name = func.__module__

        logger = get_logger(logger_name)

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_types as e:
                msg = message or f"{func.__qualname__} raised {type(e).__name__}"
                logger.log(level, msg, exception=e)
                if reraise:
                    raise
                return None

        return wrapper  # type: ignore

    return decorator


def timed(
    level: str | LogLevel = LogLevel.INFO,
    logger_name: Optional[str] = None,
    threshold_ms: Optional[float] = None,
) -> Callable[[F], F]:
    """记录执行时间的装饰器

    Args:
        level: 日志级别
        logger_name: logger 名称
        threshold_ms: 仅当执行时间超过此阈值时记录（毫秒）

    Example:
        @timed()
        def slow_operation():
            ...

        @timed(threshold_ms=100)  # 仅记录超过 100ms 的调用
        def maybe_slow():
            ...
    """
    if isinstance(level, str):
        level = LogLevel.from_string(level)

    def decorator(func: F) -> F:
        nonlocal logger_name
        if logger_name is None:
            logger_name = func.__module__

        logger = get_logger(logger_name)

        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration_ms = (time.perf_counter() - start) * 1000

                if threshold_ms is None or duration_ms >= threshold_ms:
                    logger.log(
                        level,
                        f"{func.__qualname__} took {duration_ms:.2f}ms",
                        duration_ms=duration_ms,
                    )

        return wrapper  # type: ignore

    return decorator


class LogScope:
    """日志作用域

    用于记录代码块的开始和结束。

    Example:
        with LogScope("data_processing"):
            process_data()

        # 输出:
        # INFO | Starting data_processing
        # INFO | Finished data_processing in 1.23s
    """

    def __init__(
        self,
        name: str,
        level: str | LogLevel = LogLevel.INFO,
        logger_name: Optional[str] = None,
        log_start: bool = True,
        **context
    ):
        self.name = name
        self.level = LogLevel.from_string(level) if isinstance(level, str) else level
        self.logger = get_logger(logger_name or __name__)
        self.log_start = log_start
        self.context = context
        self._start_time: float = 0

    def __enter__(self) -> 'LogScope':
        self._start_time = time.perf_counter()
        if self.log_start:
            self.logger.log(self.level, f"Starting {self.name}", **self.context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration = time.perf_counter() - self._start_time

        if exc_type:
            self.logger.error(
                f"{self.name} failed after {duration:.3f}s: {exc_type.__name__}",
                exception=exc_val,
                **self.context,
            )
        else:
            self.logger.log(
                self.level,
                f"Finished {self.name} in {duration:.3f}s",
                duration_s=duration,
                **self.context,
            )


def _truncate_repr(obj: Any, max_length: int) -> str:
    """截断对象的字符串表示"""
    try:
        # 特殊处理常见类型
        type_name = type(obj).__name__

        if type_name == 'DataFrame':
            return f"<DataFrame shape={obj.shape}>"
        elif type_name == 'Series':
            return f"<Series len={len(obj)}>"
        elif type_name == 'ndarray':
            return f"<ndarray shape={obj.shape}>"
        elif isinstance(obj, (list, tuple)):
            if len(obj) > 5:
                return f"<{type_name} len={len(obj)}>"
        elif isinstance(obj, dict):
            if len(obj) > 5:
                return f"<dict len={len(obj)}>"

        s = repr(obj)
        if len(s) > max_length:
            return s[:max_length] + "..."
        return s
    except Exception:
        return f"<{type(obj).__name__}>"
