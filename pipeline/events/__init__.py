"""
Pipeline Events Module v2.0
===========================

高性能事件发布/订阅系统，专为 Pipeline 工作流设计。

核心组件:
- EventBus: 层级路由事件总线
- Event: 泛型事件对象
- Middleware: 可组合的中间件

快速开始:
    from pipeline.events import EventBus, Event, TaskEvents, on

    # 方式1: 装饰器订阅
    @on("task.completed")
    def handle_task(event):
        print(f"Task {event.task_id} done")

    # 方式2: 方法订阅
    bus = EventBus.instance()
    bus.subscribe("flow.started", on_flow_start)

    # 发布事件
    bus.emit(TaskEvents.completed("t1", "Analyze", duration_ms=100))

特性:
- 层级路由: 订阅 "task" 会收到 "task.*" 所有事件
- 类型安全: Event[T] 泛型支持
- 优先级: 控制处理器执行顺序
- 中间件: 日志/指标/重试/熔断
- 线程安全: 支持多线程环境

版本: 2.0
变更: 从 ~4000 行精简至 ~1200 行，功能更强
"""

from __future__ import annotations

# =============================================================================
# Core - 核心组件
# =============================================================================

from .bus import (
    # 主类
    EventBus,
    Event,
    # 订阅
    Subscription,
    # 优先级
    Priority,
    # 结果
    EmitResult,
    # 中间件基类
    Middleware,
    MiddlewareChain,
    # 装饰器
    on,
)

# =============================================================================
# Types - 预定义事件类型
# =============================================================================

from .types import (
    # 负载类型
    FlowPayload,
    TaskPayload,
    DataPayload,
    CachePayload,
    SystemPayload,
    ErrorPayload,
    # 事件工厂
    FlowEvents,
    TaskEvents,
    DataEvents,
    CacheEvents,
    SystemEvents,
    # 事件类型常量
    EventType,
)

# =============================================================================
# Middleware - 中间件
# =============================================================================

from .middleware import (
    LoggingMiddleware,
    MetricsMiddleware,
    RetryMiddleware,
    FilterMiddleware,
    ThrottleMiddleware,
    CircuitBreakerMiddleware,
    # 配置
    RetryConfig,
    ThrottleConfig,
    CircuitBreakerConfig,
    # 异常
    CircuitBreakerOpenError,
)

# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # Core
    "EventBus",
    "Event",
    "Subscription",
    "Priority",
    "EmitResult",
    "Middleware",
    "MiddlewareChain",
    "on",
    # Payloads
    "FlowPayload",
    "TaskPayload",
    "DataPayload",
    "CachePayload",
    "SystemPayload",
    "ErrorPayload",
    # Event factories
    "FlowEvents",
    "TaskEvents",
    "DataEvents",
    "CacheEvents",
    "SystemEvents",
    "EventType",
    # Middleware
    "LoggingMiddleware",
    "MetricsMiddleware",
    "RetryMiddleware",
    "FilterMiddleware",
    "ThrottleMiddleware",
    "CircuitBreakerMiddleware",
    "RetryConfig",
    "ThrottleConfig",
    "CircuitBreakerConfig",
    "CircuitBreakerOpenError",
]

__version__ = "2.0.0"


# =============================================================================
# Convenience: Default Bus instance
# =============================================================================

def get_bus() -> EventBus:
    """获取全局 EventBus 实例

    Examples:
        from pipeline.events import get_bus, TaskEvents

        bus = get_bus()
        bus.emit(TaskEvents.completed("t1", "Analyze", duration_ms=100))
    """
    return EventBus.instance()


def emit(event: Event) -> EmitResult:
    """便捷函数: 发布事件到全局 EventBus

    Examples:
        from pipeline.events import emit, TaskEvents

        emit(TaskEvents.completed("t1", "Analyze", duration_ms=100))
    """
    return EventBus.instance().emit(event)


def subscribe(event_type: str, handler, priority: int = Priority.NORMAL) -> Subscription:
    """便捷函数: 订阅全局 EventBus

    Examples:
        from pipeline.events import subscribe

        sub = subscribe("task.completed", my_handler)
    """
    return EventBus.instance().subscribe(event_type, handler, priority)


# 添加到 __all__
__all__.extend(["get_bus", "emit", "subscribe"])


# =============================================================================
# Quick Setup: Common configurations
# =============================================================================

def setup_default_middleware(
    enable_logging: bool = True,
    enable_metrics: bool = True,
    log_level: int = None,
) -> EventBus:
    """快速配置常用中间件

    Examples:
        from pipeline.events import setup_default_middleware

        bus = setup_default_middleware(enable_logging=True)
    """
    import logging

    bus = EventBus.instance()

    if enable_logging:
        level = log_level or logging.INFO
        bus.use(LoggingMiddleware(level=level))

    if enable_metrics:
        bus.use(MetricsMiddleware())

    return bus


__all__.append("setup_default_middleware")
