"""
中间件管道模块
==============

参考 Express.js / Koa 中间件模式，提供：
1. 可插拔的中间件架构
2. 前置/后置处理钩子
3. 链式调用
4. 内置实用中间件

使用示例：

    bus = EventBusV6.get()
    
    # 添加中间件
    bus.use(TracingMiddleware(tracer))
    bus.use(RetryMiddleware(max_retries=3))
    bus.use(TimeoutMiddleware(timeout_sec=5))
    
    # 自定义中间件
    @bus.middleware
    def my_middleware(event, context, next_fn):
        # 前置处理
        print(f"Before: {event.event_type}")
        
        # 调用下一个中间件
        result = next_fn(event, context)
        
        # 后置处理
        print(f"After: {event.event_type}")
        return result
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Callable, Optional, List, Any, Dict, TypeVar, Generic,
    TYPE_CHECKING
)
from datetime import datetime
import logging
import threading
import time
import functools
import traceback
import uuid

if TYPE_CHECKING:
    from ..events import Event

logger = logging.getLogger(__name__)

E = TypeVar('E')
NextFn = Callable[[Any, 'MiddlewareContext'], Any]


@dataclass
class MiddlewareContext:
    """中间件上下文
    
    在整个中间件链中传递的上下文对象。
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    event_type: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # 追踪信息
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    
    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # 执行信息
    start_time: float = field(default_factory=time.time)
    middleware_times: Dict[str, float] = field(default_factory=dict)
    
    # 错误信息
    errors: List[str] = field(default_factory=list)
    
    # 状态标记
    should_continue: bool = True  # 是否继续执行
    skip_handlers: bool = False    # 是否跳过处理器
    
    def elapsed_ms(self) -> float:
        """已耗时（毫秒）"""
        return (time.time() - self.start_time) * 1000
    
    def set(self, key: str, value: Any):
        """设置元数据"""
        self.metadata[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取元数据"""
        return self.metadata.get(key, default)
    
    def abort(self, reason: str = ""):
        """中止执行"""
        self.should_continue = False
        if reason:
            self.errors.append(reason)


class Middleware(ABC):
    """中间件基类"""
    
    name: str = "base_middleware"
    order: int = 100  # 执行顺序，越小越先执行
    
    @abstractmethod
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        """处理事件
        
        Args:
            event: 事件对象
            context: 上下文
            next_fn: 下一个中间件
            
        Returns:
            处理结果
        """
        pass


class FunctionMiddleware(Middleware):
    """函数包装中间件"""
    
    def __init__(self, fn: Callable, name: Optional[str] = None, order: int = 100):
        self._fn = fn
        self.name = name or fn.__name__
        self.order = order
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        return self._fn(event, context, next_fn)


# ============================================================================
# 内置中间件
# ============================================================================

class LoggingMiddleware(Middleware):
    """日志中间件"""
    
    name = "logging"
    order = 10
    
    def __init__(
        self,
        level: int = logging.DEBUG,
        include_payload: bool = False
    ):
        self.level = level
        self.include_payload = include_payload
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        event_type = getattr(event, 'event_type', type(event).__name__)
        
        # 前置日志
        msg = f"📨 Event: {event_type} [id={context.event_id}]"
        if self.include_payload and hasattr(event, '__dict__'):
            msg += f" payload={event.__dict__}"
        logger.log(self.level, msg)
        
        start = time.time()
        try:
            result = next_fn(event, context)
            elapsed = (time.time() - start) * 1000
            logger.log(
                self.level,
                f"✅ Event completed: {event_type} [{elapsed:.2f}ms]"
            )
            return result
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            logger.error(
                f"❌ Event failed: {event_type} [{elapsed:.2f}ms] error={e}"
            )
            raise


class TracingMiddleware(Middleware):
    """追踪中间件（分布式追踪）"""
    
    name = "tracing"
    order = 5
    
    def __init__(self, tracer: Optional[Any] = None):
        """
        Args:
            tracer: 可选的追踪器实例（如 OpenTelemetry tracer）
        """
        self._tracer = tracer
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        # 生成追踪 ID
        if not context.trace_id:
            context.trace_id = str(uuid.uuid4())
        context.span_id = str(uuid.uuid4())[:16]
        
        event_type = getattr(event, 'event_type', type(event).__name__)
        
        # 如果有外部追踪器
        if self._tracer and hasattr(self._tracer, 'start_span'):
            with self._tracer.start_span(f"event:{event_type}") as span:
                span.set_attribute("event.type", event_type)
                span.set_attribute("event.id", context.event_id)
                return next_fn(event, context)
        else:
            # 简单追踪日志
            logger.debug(
                f"🔍 Trace: {event_type} "
                f"[trace_id={context.trace_id}, span_id={context.span_id}]"
            )
            return next_fn(event, context)


class RetryMiddleware(Middleware):
    """重试中间件（指数退避）"""
    
    name = "retry"
    order = 20
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 10.0,
        exponential: bool = True,
        retry_on: Optional[tuple] = None
    ):
        """
        Args:
            max_retries: 最大重试次数
            base_delay: 基础延迟（秒）
            max_delay: 最大延迟（秒）
            exponential: 是否指数退避
            retry_on: 可重试的异常类型，None 表示所有异常
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential = exponential
        self.retry_on = retry_on or (Exception,)
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return next_fn(event, context)
            except self.retry_on as e:
                last_error = e
                
                if attempt < self.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"⚠️ Retry {attempt + 1}/{self.max_retries}: "
                        f"{type(e).__name__}: {e}, waiting {delay:.2f}s"
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"❌ All retries exhausted for event: "
                        f"{getattr(event, 'event_type', type(event).__name__)}"
                    )
        
        raise last_error
    
    def _calculate_delay(self, attempt: int) -> float:
        if self.exponential:
            delay = self.base_delay * (2 ** attempt)
        else:
            delay = self.base_delay
        return min(delay, self.max_delay)


class TimeoutMiddleware(Middleware):
    """超时中间件"""
    
    name = "timeout"
    order = 15
    
    def __init__(self, timeout_sec: float = 30.0):
        self.timeout = timeout_sec
        self._executor: Optional[Any] = None
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        import concurrent.futures
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(next_fn, event, context)
            try:
                return future.result(timeout=self.timeout)
            except concurrent.futures.TimeoutError:
                event_type = getattr(event, 'event_type', type(event).__name__)
                raise TimeoutError(
                    f"Event handler timeout after {self.timeout}s: {event_type}"
                )


class ValidationMiddleware(Middleware):
    """验证中间件"""
    
    name = "validation"
    order = 25
    
    def __init__(self, validators: Optional[Dict[str, Callable]] = None):
        """
        Args:
            validators: 事件类型 -> 验证函数的映射
        """
        self._validators = validators or {}
    
    def add_validator(self, event_type: str, validator: Callable[[Any], bool]):
        """添加验证器"""
        self._validators[event_type] = validator
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        event_type = getattr(event, 'event_type', type(event).__name__)
        
        if event_type in self._validators:
            validator = self._validators[event_type]
            if not validator(event):
                raise ValueError(f"Event validation failed: {event_type}")
        
        return next_fn(event, context)


class MetricsMiddleware(Middleware):
    """指标收集中间件"""
    
    name = "metrics"
    order = 8
    
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}
        self._latencies: Dict[str, List[float]] = {}
        self._errors: Dict[str, int] = {}
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        event_type = getattr(event, 'event_type', type(event).__name__)
        start = time.time()
        
        try:
            result = next_fn(event, context)
            self._record_success(event_type, time.time() - start)
            return result
        except Exception as e:
            self._record_error(event_type, time.time() - start)
            raise
    
    def _record_success(self, event_type: str, latency: float):
        with self._lock:
            self._counters[event_type] = self._counters.get(event_type, 0) + 1
            if event_type not in self._latencies:
                self._latencies[event_type] = []
            self._latencies[event_type].append(latency * 1000)  # ms
    
    def _record_error(self, event_type: str, latency: float):
        with self._lock:
            self._errors[event_type] = self._errors.get(event_type, 0) + 1
    
    def get_metrics(self) -> dict:
        """获取指标"""
        with self._lock:
            result = {}
            for event_type, count in self._counters.items():
                latencies = self._latencies.get(event_type, [])
                errors = self._errors.get(event_type, 0)
                result[event_type] = {
                    'count': count,
                    'errors': errors,
                    'avg_latency_ms': sum(latencies) / len(latencies) if latencies else 0,
                    'max_latency_ms': max(latencies) if latencies else 0,
                    'error_rate': errors / (count + errors) if (count + errors) > 0 else 0,
                }
            return result
    
    def reset(self):
        """重置指标"""
        with self._lock:
            self._counters.clear()
            self._latencies.clear()
            self._errors.clear()


class CircuitBreakerMiddleware(Middleware):
    """熔断器中间件"""
    
    name = "circuit_breaker"
    order = 12
    
    # 状态
    CLOSED = "closed"      # 正常
    OPEN = "open"          # 熔断
    HALF_OPEN = "half_open"  # 半开
    
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        half_open_requests: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_requests = half_open_requests
        
        self._state = self.CLOSED
        self._failures = 0
        self._last_failure_time = 0.0
        self._half_open_successes = 0
        self._lock = threading.Lock()
    
    def __call__(
        self,
        event: Any,
        context: MiddlewareContext,
        next_fn: NextFn
    ) -> Any:
        with self._lock:
            self._check_state_transition()
            
            if self._state == self.OPEN:
                event_type = getattr(event, 'event_type', type(event).__name__)
                raise RuntimeError(
                    f"Circuit breaker is OPEN for event: {event_type}"
                )
        
        try:
            result = next_fn(event, context)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _check_state_transition(self):
        """检查状态转换"""
        if self._state == self.OPEN:
            if time.time() - self._last_failure_time >= self.reset_timeout:
                self._state = self.HALF_OPEN
                self._half_open_successes = 0
                logger.info("🔄 Circuit breaker: OPEN -> HALF_OPEN")
    
    def _on_success(self):
        with self._lock:
            if self._state == self.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_requests:
                    self._state = self.CLOSED
                    self._failures = 0
                    logger.info("✅ Circuit breaker: HALF_OPEN -> CLOSED")
            elif self._state == self.CLOSED:
                self._failures = 0
    
    def _on_failure(self):
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            
            if self._state == self.HALF_OPEN:
                self._state = self.OPEN
                logger.warning("⚠️ Circuit breaker: HALF_OPEN -> OPEN")
            elif self._state == self.CLOSED:
                if self._failures >= self.failure_threshold:
                    self._state = self.OPEN
                    logger.warning(
                        f"🔴 Circuit breaker: CLOSED -> OPEN "
                        f"(failures: {self._failures})"
                    )
    
    @property
    def state(self) -> str:
        return self._state


# ============================================================================
# 中间件管道
# ============================================================================

class MiddlewarePipeline:
    """中间件管道
    
    管理和执行中间件链。
    """
    
    def __init__(self):
        self._middlewares: List[Middleware] = []
        self._lock = threading.Lock()
    
    def use(self, middleware: Middleware):
        """添加中间件
        
        Args:
            middleware: 中间件实例
        """
        with self._lock:
            self._middlewares.append(middleware)
            self._middlewares.sort(key=lambda m: m.order)
        
        logger.debug(f"⚙️ Middleware added: {middleware.name} (order={middleware.order})")
    
    def use_fn(
        self,
        fn: Callable,
        name: Optional[str] = None,
        order: int = 100
    ):
        """添加函数作为中间件"""
        self.use(FunctionMiddleware(fn, name, order))
    
    def remove(self, name: str) -> bool:
        """移除中间件"""
        with self._lock:
            for m in self._middlewares:
                if m.name == name:
                    self._middlewares.remove(m)
                    return True
        return False
    
    def execute(
        self,
        event: Any,
        final_handler: Callable[[Any, MiddlewareContext], Any],
        context: Optional[MiddlewareContext] = None
    ) -> Any:
        """执行中间件链
        
        Args:
            event: 事件对象
            final_handler: 最终处理器
            context: 可选的上下文
            
        Returns:
            处理结果
        """
        ctx = context or MiddlewareContext(
            event_type=getattr(event, 'event_type', type(event).__name__)
        )
        
        # 构建调用链
        def build_chain(middlewares: List[Middleware], idx: int) -> NextFn:
            if idx >= len(middlewares):
                return final_handler
            
            middleware = middlewares[idx]
            next_fn = build_chain(middlewares, idx + 1)
            
            def wrapped(e: Any, c: MiddlewareContext) -> Any:
                if not c.should_continue:
                    return None
                
                start = time.time()
                result = middleware(e, c, next_fn)
                c.middleware_times[middleware.name] = (time.time() - start) * 1000
                return result
            
            return wrapped
        
        chain = build_chain(list(self._middlewares), 0)
        return chain(event, ctx)
    
    def list_middlewares(self) -> List[str]:
        """列出所有中间件名称"""
        return [m.name for m in self._middlewares]
    
    def clear(self):
        """清空中间件"""
        with self._lock:
            self._middlewares.clear()
