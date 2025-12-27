"""
处理器 (Error Handlers)
========================

错误处理策略、恢复机制、上报逻辑。

参考设计:
- Express.js: 中间件错误处理
- FastAPI: exception_handler 装饰器
- Sentry: 错误聚合与上报
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Type, TypeVar
import logging
import functools

from .base import AStockError, ErrorContext
from .codes import ErrorSeverity

T = TypeVar('T')
E = TypeVar('E', bound=AStockError)


@dataclass
class ErrorHandlerResult:
    """错误处理结果"""
    handled: bool = False           # 是否已处理
    should_propagate: bool = True   # 是否继续传播
    retry: bool = False             # 是否应该重试
    fallback_value: Optional[T] = None  # 降级返回值
    modified_error: Optional[AStockError] = None  # 修改后的错误


class ErrorHandler(ABC):
    """错误处理器基类"""

    @abstractmethod
    def can_handle(self, error: AStockError) -> bool:
        """判断是否可以处理此错误"""
        pass

    @abstractmethod
    def handle(self, error: AStockError) -> ErrorHandlerResult:
        """处理错误"""
        pass


class LoggingErrorHandler(ErrorHandler):
    """日志记录处理器"""

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        min_severity: ErrorSeverity = ErrorSeverity.WARNING,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.min_severity = min_severity

    def can_handle(self, error: AStockError) -> bool:
        return error.severity.value >= self.min_severity.value

    def handle(self, error: AStockError) -> ErrorHandlerResult:
        level = self._severity_to_level(error.severity)
        self.logger.log(
            level,
            f"[{error.error_code}] {error.message}",
            extra={
                'error_id': error.error_id,
                'context': error.context.to_dict(),
            },
            exc_info=error.cause,
        )
        return ErrorHandlerResult(handled=True, should_propagate=True)

    def _severity_to_level(self, severity: ErrorSeverity) -> int:
        mapping = {
            ErrorSeverity.DEBUG: logging.DEBUG,
            ErrorSeverity.INFO: logging.INFO,
            ErrorSeverity.WARNING: logging.WARNING,
            ErrorSeverity.ERROR: logging.ERROR,
            ErrorSeverity.CRITICAL: logging.CRITICAL,
            ErrorSeverity.FATAL: logging.CRITICAL,
        }
        return mapping.get(severity, logging.ERROR)


class RetryHandler(ErrorHandler):
    """重试处理器"""

    def __init__(
        self,
        retryable_codes: Optional[List[str]] = None,
        max_retries: int = 3,
    ):
        self.retryable_codes = retryable_codes or [
            'SYS-902',  # 网络错误
            'SYS-903',  # 资源暂时不可用
        ]
        self.max_retries = max_retries
        self._retry_counts: Dict[str, int] = {}

    def can_handle(self, error: AStockError) -> bool:
        return error.error_code in self.retryable_codes

    def handle(self, error: AStockError) -> ErrorHandlerResult:
        error_id = error.error_id
        current_count = self._retry_counts.get(error_id, 0)

        if current_count < self.max_retries:
            self._retry_counts[error_id] = current_count + 1
            return ErrorHandlerResult(
                handled=True,
                should_propagate=False,
                retry=True,
            )

        # 超过重试次数
        del self._retry_counts[error_id]
        return ErrorHandlerResult(handled=False, should_propagate=True)


class FallbackHandler(ErrorHandler):
    """降级处理器"""

    def __init__(
        self,
        fallback_map: Optional[Dict[Type[AStockError], Callable[[], T]]] = None,
    ):
        self.fallback_map = fallback_map or {}

    def can_handle(self, error: AStockError) -> bool:
        return type(error) in self.fallback_map

    def handle(self, error: AStockError) -> ErrorHandlerResult:
        fallback_fn = self.fallback_map.get(type(error))
        if fallback_fn:
            return ErrorHandlerResult(
                handled=True,
                should_propagate=False,
                fallback_value=fallback_fn(),
            )
        return ErrorHandlerResult(handled=False)


@dataclass
class ErrorHandlerChain:
    """错误处理器链

    类似 Express.js 中间件链，按顺序执行处理器。

    Example:
        chain = ErrorHandlerChain()
        chain.add_handler(LoggingErrorHandler())
        chain.add_handler(RetryHandler())
        chain.add_handler(FallbackHandler({DataError: lambda: pd.DataFrame()}))

        result = chain.process(error)
    """
    handlers: List[ErrorHandler] = field(default_factory=list)

    def add_handler(self, handler: ErrorHandler) -> 'ErrorHandlerChain':
        """添加处理器（链式调用）"""
        self.handlers.append(handler)
        return self

    def process(self, error: AStockError) -> ErrorHandlerResult:
        """处理错误"""
        final_result = ErrorHandlerResult()

        for handler in self.handlers:
            if handler.can_handle(error):
                result = handler.handle(error)

                if result.handled:
                    final_result.handled = True

                if result.fallback_value is not None:
                    final_result.fallback_value = result.fallback_value

                if result.retry:
                    final_result.retry = True
                    final_result.should_propagate = False
                    break

                if not result.should_propagate:
                    final_result.should_propagate = False
                    break

                if result.modified_error:
                    error = result.modified_error

        return final_result


def error_handler(
    *error_types: Type[AStockError],
    reraise: bool = True,
    fallback: Optional[T] = None,
):
    """错误处理装饰器

    参考 FastAPI 的 exception_handler 设计。

    Example:
        @error_handler(DataError, ValidationError)
        def process_data(df):
            ...

        @error_handler(DataError, fallback=pd.DataFrame())
        def load_optional_data():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except error_types as e:
                # 发布错误事件
                try:
                    from shared import EventBus, ErrorEvent
                    EventBus.get().emit(ErrorEvent(
                        error_code=e.error_code if isinstance(e, AStockError) else 'UNKNOWN',
                        error_message=str(e),
                        severity=e.severity.name if isinstance(e, AStockError) else 'ERROR',
                        context={'function': func.__name__},
                        source=func.__module__,
                    ))
                except ImportError:
                    pass

                if fallback is not None:
                    return fallback
                if reraise:
                    raise
                return None

        return wrapper
    return decorator


def safe_execute(
    func: Callable[..., T],
    *args,
    default: Optional[T] = None,
    error_context: Optional[Dict[str, any]] = None,
    **kwargs,
) -> T:
    """安全执行函数

    捕获所有异常，返回默认值。

    Example:
        result = safe_execute(
            risky_function,
            arg1, arg2,
            default=pd.DataFrame(),
            error_context={'step': 'data_load'}
        )
    """
    try:
        return func(*args, **kwargs)
    except AStockError as e:
        if error_context:
            e.with_context(**error_context)
        logging.getLogger(__name__).warning(
            f"safe_execute caught error: {e}",
            exc_info=True,
        )
        return default
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"safe_execute caught unexpected error: {e}",
            exc_info=True,
        )
        return default
