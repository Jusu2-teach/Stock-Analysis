"""
Pipeline Events Middleware - 可组合的事件处理中间件
==================================================

提供常用的事件处理中间件，可组合使用。

中间件类型:
1. LoggingMiddleware - 日志记录
2. MetricsMiddleware - 指标收集
3. RetryMiddleware - 失败重试
4. FilterMiddleware - 事件过滤
5. ThrottleMiddleware - 限流控制
6. AsyncMiddleware - 异步化处理

使用方式:
    bus = EventBus()
    bus.use(LoggingMiddleware())
    bus.use(MetricsMiddleware())
    bus.use(RetryMiddleware(max_retries=3))
"""

from __future__ import annotations

import functools
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set

from .bus import Event, Middleware

__all__ = [
    "LoggingMiddleware",
    "MetricsMiddleware",
    "RetryMiddleware",
    "FilterMiddleware",
    "ThrottleMiddleware",
    "CircuitBreakerMiddleware",
    # 配置类
    "RetryConfig",
    "ThrottleConfig",
    "CircuitBreakerConfig",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Logging Middleware
# =============================================================================

class LoggingMiddleware(Middleware):
    """日志中间件

    记录事件处理的详细日志，包括:
    - 事件接收
    - 处理耗时
    - 处理结果/错误

    Examples:
        bus.use(LoggingMiddleware())
        bus.use(LoggingMiddleware(level=logging.DEBUG))
        bus.use(LoggingMiddleware(include_payload=True))
    """

    def __init__(
        self,
        level: int = logging.INFO,
        include_payload: bool = False,
        exclude_types: Set[str] = None,
    ):
        """
        Args:
            level: 日志级别
            include_payload: 是否记录负载内容
            exclude_types: 排除的事件类型 (不记录日志)
        """
        self._level = level
        self._include_payload = include_payload
        self._exclude_types = exclude_types or set()

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        # 检查是否排除
        if event.type in self._exclude_types:
            return next_fn()

        handler_name = handler.__name__

        # 记录开始
        start_msg = f"📨 [{event.type}] → {handler_name}"
        if self._include_payload:
            start_msg += f" | payload={event.payload}"
        if event.trace_id:
            start_msg += f" | trace={event.trace_id}"
        logger.log(self._level, start_msg)

        start_time = time.time()
        try:
            result = next_fn()
            duration_ms = (time.time() - start_time) * 1000

            logger.log(
                self._level,
                f"✅ [{event.type}] ← {handler_name} ({duration_ms:.2f}ms)"
            )
            return result

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(
                f"❌ [{event.type}] ← {handler_name} ({duration_ms:.2f}ms) | error={e}"
            )
            raise


# =============================================================================
# Metrics Middleware
# =============================================================================

@dataclass
class HandlerMetrics:
    """处理器指标"""
    handler_name: str
    call_count: int = 0
    success_count: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None

    @property
    def avg_duration_ms(self) -> float:
        if self.call_count == 0:
            return 0.0
        return self.total_duration_ms / self.call_count

    @property
    def success_rate(self) -> float:
        if self.call_count == 0:
            return 0.0
        return self.success_count / self.call_count


class MetricsMiddleware(Middleware):
    """指标收集中间件

    收集事件处理的性能指标:
    - 调用次数
    - 成功/失败次数
    - 平均耗时
    - 错误率

    Examples:
        metrics_mw = MetricsMiddleware()
        bus.use(metrics_mw)
        ...
        # 获取指标
        stats = metrics_mw.get_stats()
    """

    def __init__(self):
        self._metrics: Dict[str, Dict[str, HandlerMetrics]] = defaultdict(dict)
        self._lock = threading.Lock()

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        handler_name = handler.__name__

        start_time = time.time()
        error = None

        try:
            result = next_fn()
            return result
        except Exception as e:
            error = e
            raise
        finally:
            duration_ms = (time.time() - start_time) * 1000
            self._record(event.type, handler_name, duration_ms, error)

    def _record(
        self,
        event_type: str,
        handler_name: str,
        duration_ms: float,
        error: Optional[Exception],
    ) -> None:
        """记录指标"""
        with self._lock:
            if handler_name not in self._metrics[event_type]:
                self._metrics[event_type][handler_name] = HandlerMetrics(
                    handler_name=handler_name
                )

            m = self._metrics[event_type][handler_name]
            m.call_count += 1
            m.total_duration_ms += duration_ms

            if error:
                m.error_count += 1
                m.last_error = str(error)
                m.last_error_time = datetime.now()
            else:
                m.success_count += 1

    def get_stats(self, event_type: str = None) -> Dict[str, Any]:
        """获取指标统计

        Args:
            event_type: 指定事件类型 (None 返回全部)

        Returns:
            指标字典
        """
        with self._lock:
            if event_type:
                handlers = self._metrics.get(event_type, {})
                return {
                    name: {
                        "calls": m.call_count,
                        "success": m.success_count,
                        "errors": m.error_count,
                        "avg_duration_ms": m.avg_duration_ms,
                        "success_rate": m.success_rate,
                    }
                    for name, m in handlers.items()
                }

            return {
                event_type: {
                    name: {
                        "calls": m.call_count,
                        "success": m.success_count,
                        "errors": m.error_count,
                        "avg_duration_ms": m.avg_duration_ms,
                        "success_rate": m.success_rate,
                    }
                    for name, m in handlers.items()
                }
                for event_type, handlers in self._metrics.items()
            }

    def reset(self) -> None:
        """重置所有指标"""
        with self._lock:
            self._metrics.clear()


# =============================================================================
# Retry Middleware
# =============================================================================

@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    base_delay_ms: float = 100.0
    max_delay_ms: float = 5000.0
    exponential_backoff: bool = True
    retry_on: Set[type] = field(default_factory=lambda: {Exception})


class RetryMiddleware(Middleware):
    """重试中间件

    处理器执行失败时自动重试。

    Features:
    - 指数退避
    - 可配置重试次数
    - 可配置重试的异常类型

    Examples:
        bus.use(RetryMiddleware(max_retries=3))
        bus.use(RetryMiddleware(RetryConfig(
            max_retries=5,
            base_delay_ms=200,
            exponential_backoff=True,
        )))
    """

    def __init__(self, config: RetryConfig = None, max_retries: int = None):
        """
        Args:
            config: 重试配置
            max_retries: 简化参数 (快速设置重试次数)
        """
        if config:
            self._config = config
        else:
            self._config = RetryConfig(max_retries=max_retries or 3)

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        last_error = None

        for attempt in range(self._config.max_retries + 1):
            try:
                return next_fn()
            except tuple(self._config.retry_on) as e:
                last_error = e

                if attempt < self._config.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"🔄 [{event.type}] {handler.__name__} retry {attempt + 1}"
                        f"/{self._config.max_retries} in {delay:.0f}ms: {e}"
                    )
                    time.sleep(delay / 1000.0)

        # 所有重试失败
        logger.error(
            f"💥 [{event.type}] {handler.__name__} failed after "
            f"{self._config.max_retries} retries"
        )
        raise last_error

    def _calculate_delay(self, attempt: int) -> float:
        """计算重试延迟 (指数退避)"""
        if self._config.exponential_backoff:
            delay = self._config.base_delay_ms * (2 ** attempt)
        else:
            delay = self._config.base_delay_ms

        return min(delay, self._config.max_delay_ms)


# =============================================================================
# Filter Middleware
# =============================================================================

class FilterMiddleware(Middleware):
    """过滤中间件

    基于条件过滤事件，不满足条件的事件不会传递给处理器。

    Examples:
        # 只处理特定 flow 的事件
        bus.use(FilterMiddleware(
            lambda event: event.payload.get('flow_id') == 'main'
        ))

        # 排除 debug 事件
        bus.use(FilterMiddleware(
            lambda event: not event.type.startswith('debug.')
        ))
    """

    def __init__(self, predicate: Callable[[Event], bool]):
        """
        Args:
            predicate: 过滤函数，返回 True 则继续处理
        """
        self._predicate = predicate

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        if self._predicate(event):
            return next_fn()
        return None


# =============================================================================
# Throttle Middleware
# =============================================================================

@dataclass
class ThrottleConfig:
    """限流配置"""
    max_events: int = 100
    window_seconds: float = 1.0
    drop_when_full: bool = True  # True: 丢弃超限事件; False: 阻塞等待


class ThrottleMiddleware(Middleware):
    """限流中间件

    限制事件处理速率，防止系统过载。

    Examples:
        # 每秒最多处理 100 个事件
        bus.use(ThrottleMiddleware(max_events=100, window_seconds=1.0))
    """

    def __init__(
        self,
        config: ThrottleConfig = None,
        max_events: int = None,
        window_seconds: float = None,
    ):
        if config:
            self._config = config
        else:
            self._config = ThrottleConfig(
                max_events=max_events or 100,
                window_seconds=window_seconds or 1.0,
            )

        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        now = time.time()
        window_start = now - self._config.window_seconds

        with self._lock:
            # 清除过期记录
            self._timestamps = [
                ts for ts in self._timestamps if ts > window_start
            ]

            # 检查是否超限
            if len(self._timestamps) >= self._config.max_events:
                if self._config.drop_when_full:
                    logger.warning(
                        f"⏸️ [{event.type}] throttled, dropping event"
                    )
                    return None
                else:
                    # 阻塞等待
                    wait_time = self._timestamps[0] - window_start
                    time.sleep(wait_time)

            self._timestamps.append(now)

        return next_fn()


# =============================================================================
# Circuit Breaker Middleware
# =============================================================================

@dataclass
class CircuitBreakerConfig:
    """熔断器配置"""
    failure_threshold: int = 5       # 失败次数阈值
    success_threshold: int = 2       # 恢复所需成功次数
    timeout_seconds: float = 30.0    # 熔断超时时间


class _CircuitState:
    """熔断器状态"""
    CLOSED = "closed"      # 正常
    OPEN = "open"          # 熔断
    HALF_OPEN = "half_open"  # 半开 (尝试恢复)


class CircuitBreakerMiddleware(Middleware):
    """熔断器中间件

    当处理器连续失败超过阈值时，进入熔断状态，
    在熔断期间直接拒绝请求，保护系统。

    状态流转:
    CLOSED → (failures >= threshold) → OPEN
    OPEN → (timeout) → HALF_OPEN
    HALF_OPEN → (success) → CLOSED
    HALF_OPEN → (failure) → OPEN

    Examples:
        bus.use(CircuitBreakerMiddleware(failure_threshold=5))
    """

    def __init__(
        self,
        config: CircuitBreakerConfig = None,
        failure_threshold: int = None,
    ):
        if config:
            self._config = config
        else:
            self._config = CircuitBreakerConfig(
                failure_threshold=failure_threshold or 5
            )

        # 状态跟踪 (per handler)
        self._states: Dict[str, str] = defaultdict(lambda: _CircuitState.CLOSED)
        self._failures: Dict[str, int] = defaultdict(int)
        self._successes: Dict[str, int] = defaultdict(int)
        self._last_failure: Dict[str, float] = {}
        self._lock = threading.Lock()

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        handler_name = handler.__name__

        with self._lock:
            state = self._states[handler_name]

            # 检查熔断超时
            if state == _CircuitState.OPEN:
                if self._should_attempt_reset(handler_name):
                    self._states[handler_name] = _CircuitState.HALF_OPEN
                    state = _CircuitState.HALF_OPEN
                else:
                    logger.warning(
                        f"⚡ [{event.type}] circuit breaker OPEN for {handler_name}"
                    )
                    raise CircuitBreakerOpenError(handler_name)

        try:
            result = next_fn()

            with self._lock:
                if self._states[handler_name] == _CircuitState.HALF_OPEN:
                    self._successes[handler_name] += 1
                    if self._successes[handler_name] >= self._config.success_threshold:
                        self._reset(handler_name)
                        logger.info(
                            f"✅ [{event.type}] circuit breaker CLOSED for {handler_name}"
                        )

            return result

        except Exception as e:
            with self._lock:
                self._failures[handler_name] += 1
                self._last_failure[handler_name] = time.time()

                if self._failures[handler_name] >= self._config.failure_threshold:
                    self._states[handler_name] = _CircuitState.OPEN
                    logger.error(
                        f"⚡ [{event.type}] circuit breaker OPEN for {handler_name}"
                    )

            raise

    def _should_attempt_reset(self, handler_name: str) -> bool:
        """检查是否应该尝试重置"""
        last = self._last_failure.get(handler_name, 0)
        return time.time() - last >= self._config.timeout_seconds

    def _reset(self, handler_name: str) -> None:
        """重置熔断器"""
        self._states[handler_name] = _CircuitState.CLOSED
        self._failures[handler_name] = 0
        self._successes[handler_name] = 0

    def get_state(self, handler_name: str) -> str:
        """获取处理器的熔断状态"""
        return self._states.get(handler_name, _CircuitState.CLOSED)


class CircuitBreakerOpenError(Exception):
    """熔断器打开异常"""

    def __init__(self, handler_name: str):
        super().__init__(f"Circuit breaker is open for handler: {handler_name}")
        self.handler_name = handler_name
