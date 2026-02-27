"""
EventBus Middleware - 中间件系统
================================

可插拔的中间件管道，支持事件处理的横切关注点。

设计参考：
- Express.js/Koa: 中间件链式调用
- Python ASGI: 中间件协议

Features:
1. MiddlewarePipeline - 中间件链管理
2. 内置中间件：日志、追踪、重试、超时、验证、指标、熔断
"""

from __future__ import annotations

import logging
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# =============================================================================
# Context
# =============================================================================

@dataclass
class MiddlewareContext:
    """中间件上下文

    在中间件链中传递的上下文对象。
    """
    event: Any  # Event 对象
    handler: Callable
    handler_name: str = ""
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # 控制标志
    skip_handler: bool = False
    abort_chain: bool = False

    # 结果
    result: Any = None
    error: Optional[Exception] = None

    @property
    def elapsed_ms(self) -> float:
        """已用时间（毫秒）"""
        return (time.time() - self.start_time) * 1000


# =============================================================================
# Base Middleware
# =============================================================================

class Middleware(ABC):
    """中间件基类

    子类需要实现 process 方法，该方法接收上下文和 next 函数。

    Example:
        class LoggingMiddleware(Middleware):
            def process(self, ctx, next_fn):
                print(f"Before: {ctx.event.type}")
                next_fn()
                print(f"After: {ctx.event.type}")
    """

    name: str = "base"
    order: int = 100  # 越小越先执行

    @abstractmethod
    def process(
        self,
        ctx: MiddlewareContext,
        next_fn: Callable[[], None],
    ) -> None:
        """处理事件

        Args:
            ctx: 中间件上下文
            next_fn: 调用下一个中间件的函数
        """
        raise NotImplementedError


class FunctionMiddleware(Middleware):
    """函数式中间件

    将普通函数包装为中间件。

    Example:
        def my_middleware(ctx, next_fn):
            print("Before")
            next_fn()
            print("After")

        bus.use(FunctionMiddleware(my_middleware))
    """

    def __init__(
        self,
        fn: Callable[[MiddlewareContext, Callable], None],
        name: str = "function",
        order: int = 100,
    ):
        self._fn = fn
        self.name = name
        self.order = order

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        self._fn(ctx, next_fn)


# =============================================================================
# Middleware Pipeline
# =============================================================================

class MiddlewarePipeline:
    """中间件管道

    管理中间件链的执行。

    Example:
        pipeline = MiddlewarePipeline()
        pipeline.use(LoggingMiddleware())
        pipeline.use(TimingMiddleware())

        pipeline.execute(ctx, final_handler)
    """

    def __init__(self):
        self._middlewares: List[Middleware] = []
        self._lock = threading.Lock()

    def use(self, middleware: Middleware) -> 'MiddlewarePipeline':
        """添加中间件"""
        with self._lock:
            self._middlewares.append(middleware)
            # 按 order 排序
            self._middlewares.sort(key=lambda m: m.order)
        return self

    def remove(self, middleware_type: type) -> bool:
        """移除指定类型的中间件"""
        with self._lock:
            for i, m in enumerate(self._middlewares):
                if isinstance(m, middleware_type):
                    self._middlewares.pop(i)
                    return True
        return False

    def execute(
        self,
        ctx: MiddlewareContext,
        final_handler: Callable[[MiddlewareContext], None],
    ) -> None:
        """执行中间件链

        Args:
            ctx: 上下文
            final_handler: 最终处理函数
        """
        def build_chain(index: int) -> Callable[[], None]:
            if index >= len(self._middlewares):
                # 到达链尾，执行最终处理器
                return lambda: final_handler(ctx)

            middleware = self._middlewares[index]
            next_fn = build_chain(index + 1)

            return lambda: middleware.process(ctx, next_fn)

        chain = build_chain(0)
        chain()

    def __len__(self) -> int:
        return len(self._middlewares)


# =============================================================================
# Built-in Middlewares
# =============================================================================

class LoggingMiddleware(Middleware):
    """日志中间件

    记录事件处理的开始和结束。
    """

    name = "logging"
    order = 10

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._logger = logger or logging.getLogger(__name__)

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        event_type = getattr(ctx.event, 'type', type(ctx.event).__name__)
        self._logger.debug(f"▶ Processing event: {event_type}")

        try:
            next_fn()
            self._logger.debug(
                f"✓ Event processed: {event_type} ({ctx.elapsed_ms:.1f}ms)"
            )
        except Exception as e:
            self._logger.error(
                f"✗ Event failed: {event_type} ({ctx.elapsed_ms:.1f}ms) - {e}"
            )
            raise


class TracingMiddleware(Middleware):
    """追踪中间件

    添加追踪信息到上下文。
    """

    name = "tracing"
    order = 5

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        import uuid

        # 生成或获取 trace_id
        trace_id = getattr(ctx.event, 'trace_id', None) or str(uuid.uuid4())[:8]
        ctx.metadata['trace_id'] = trace_id
        ctx.metadata['span_id'] = str(uuid.uuid4())[:8]

        next_fn()


class RetryMiddleware(Middleware):
    """重试中间件

    在处理失败时自动重试。
    """

    name = "retry"
    order = 50

    def __init__(
        self,
        max_attempts: int = 3,
        delay_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
    ):
        self._max_attempts = max_attempts
        self._delay = delay_seconds
        self._backoff = backoff_multiplier

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        last_error: Optional[Exception] = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                next_fn()
                return
            except Exception as e:
                last_error = e
                if attempt < self._max_attempts:
                    delay = self._delay * (self._backoff ** (attempt - 1))
                    logger.warning(
                        f"Retry {attempt}/{self._max_attempts} for "
                        f"{ctx.handler_name}: {e}, waiting {delay:.1f}s"
                    )
                    time.sleep(delay)

        if last_error:
            raise last_error


class TimeoutMiddleware(Middleware):
    """超时中间件

    限制处理时间。
    """

    name = "timeout"
    order = 20

    def __init__(self, timeout_seconds: float = 30.0):
        self._timeout = timeout_seconds

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(next_fn)
            try:
                future.result(timeout=self._timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError(
                    f"Event handler {ctx.handler_name} timed out "
                    f"after {self._timeout}s"
                )


class ValidationMiddleware(Middleware):
    """验证中间件

    验证事件数据。
    """

    name = "validation"
    order = 15

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        event = ctx.event

        # 如果事件有 validate 方法，调用它
        if hasattr(event, 'validate'):
            event.validate()

        next_fn()


class MetricsMiddleware(Middleware):
    """指标中间件

    收集处理指标。
    """

    name = "metrics"
    order = 1  # 最先执行，确保计时准确

    def __init__(self):
        self._lock = threading.Lock()
        self._total_events = 0
        self._total_errors = 0
        self._total_time_ms = 0.0

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        start = time.time()

        try:
            next_fn()
        except Exception:
            with self._lock:
                self._total_errors += 1
            raise
        finally:
            elapsed = (time.time() - start) * 1000
            with self._lock:
                self._total_events += 1
                self._total_time_ms += elapsed

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._lock:
            return {
                'total_events': self._total_events,
                'total_errors': self._total_errors,
                'total_time_ms': self._total_time_ms,
                'avg_time_ms': (
                    self._total_time_ms / self._total_events
                    if self._total_events > 0 else 0
                ),
                'error_rate': (
                    self._total_errors / self._total_events
                    if self._total_events > 0 else 0
                ),
            }


class CircuitBreakerMiddleware(Middleware):
    """熔断器中间件

    在错误率过高时熔断。

    状态：
    - CLOSED: 正常状态，允许请求
    - OPEN: 熔断状态，拒绝请求
    - HALF_OPEN: 半开状态，允许部分请求测试
    """

    name = "circuit_breaker"
    order = 30

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls

        self._state = "CLOSED"
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    def process(self, ctx: MiddlewareContext, next_fn: Callable[[], None]) -> None:
        with self._lock:
            if self._state == "OPEN":
                if self._should_try_recovery():
                    self._state = "HALF_OPEN"
                    self._half_open_calls = 0
                else:
                    raise RuntimeError("Circuit breaker is OPEN")

            if self._state == "HALF_OPEN":
                if self._half_open_calls >= self._half_open_max_calls:
                    raise RuntimeError("Circuit breaker HALF_OPEN limit reached")
                self._half_open_calls += 1

        try:
            next_fn()
            self._on_success()
        except Exception:
            self._on_failure()
            raise

    def _should_try_recovery(self) -> bool:
        """检查是否应该尝试恢复"""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self._recovery_timeout

    def _on_success(self) -> None:
        """成功回调"""
        with self._lock:
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
            self._failure_count = 0

    def _on_failure(self) -> None:
        """失败回调"""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._failure_count >= self._failure_threshold:
                self._state = "OPEN"
                logger.warning(
                    f"Circuit breaker opened after {self._failure_count} failures"
                )

    @property
    def state(self) -> str:
        """当前状态"""
        return self._state


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Base
    "Middleware",
    "MiddlewarePipeline",
    "MiddlewareContext",
    "FunctionMiddleware",
    # Built-in
    "LoggingMiddleware",
    "TracingMiddleware",
    "RetryMiddleware",
    "TimeoutMiddleware",
    "ValidationMiddleware",
    "MetricsMiddleware",
    "CircuitBreakerMiddleware",
]
