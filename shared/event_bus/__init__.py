"""
EventBus - 增强版事件总线
========================

整合多种优秀开源设计模式的生产级事件总线实现。

参考设计：
- pytest/pluggy: HookSpec 类型安全、call_historic 历史重放
- Google Guava: DeadEvent 死信处理
- Project Reactor: 异步背压、Scheduler
- RxPY: Disposable 订阅管理

Features:
1. HookSpec - 类型安全的事件规格定义
2. DeadLetterQueue - 无订阅者事件的死信处理
3. HistoricEventStore - 历史事件存储与重放
4. MiddlewarePipeline - 可插拔的中间件管道
5. Subscription - 可取消的订阅管理
6. AsyncEventBus - 异步事件支持

使用示例：

    from shared.event_bus import EventBus, LoggingMiddleware

    bus = EventBus.get()

    # 添加中间件
    bus.use(LoggingMiddleware())

    # 订阅事件
    @bus.on("user.created")
    def on_user_created(event):
        print(f"User created: {event.user_id}")

    # 发布事件
    bus.emit(UserCreatedEvent(user_id="123"))
"""

# 配置和模型
from .config import EventBusConfig
from .models import HandlerInfo, EmitResult, EventPriority, EventBusStats, HandlerType

# HookSpec（Pluggy 风格）
from .specs import HookSpec, HookSpecRegistry

# 死信处理（Guava 风格）
from .dead_letter import DeadEvent, DeadLetterQueue

# 历史事件（Pluggy call_historic）
from .historic import HistoricEventStore, HistoricEntry

# 中间件（Express/Koa 风格）
from .middleware import (
    Middleware,
    MiddlewarePipeline,
    MiddlewareContext,
    FunctionMiddleware,
    LoggingMiddleware,
    TracingMiddleware,
    RetryMiddleware,
    TimeoutMiddleware,
    ValidationMiddleware,
    MetricsMiddleware,
    CircuitBreakerMiddleware,
)

# 订阅管理（RxPY 风格）
from .subscription import (
    Disposable,
    EmptyDisposable,
    Subscription,
    CompositeDisposable,
    SerialDisposable,
    SingleAssignmentDisposable,
    RefCountDisposable,
    SubscriptionManager,
)

# 异步支持（Reactor 风格）
from .async_support import AsyncEventBus, AsyncEmitResult, to_async, run_sync

# 核心 EventBus（重命名 EventBusV6 -> EventBus）
from .bus import EventBusV6 as EventBus, get_bus, emit, on, off

# 事件定义
from .events import (
    Event,
    MethodRegisteredEvent,
    MethodExecutedEvent,
    MethodSelectedEvent,
    RegistryRefreshedEvent,
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineErrorEvent,
    NodeStartedEvent,
    NodeCompletedEvent,
    CacheHitEvent,
    CacheInvalidatedEvent,
    SystemReadyEvent,
    ComponentLoadedEvent,
    ErrorEvent,
    MetricEvent,
    DataLoadedEvent,
    DataTransformedEvent,
)


__all__ = [
    # 核心
    'EventBus',
    'EventBusConfig',

    # 模型
    'HandlerInfo',
    'HandlerType',
    'EmitResult',
    'EventPriority',
    'EventBusStats',

    # 事件定义
    'Event',
    'MethodRegisteredEvent',
    'MethodExecutedEvent',
    'MethodSelectedEvent',
    'RegistryRefreshedEvent',
    'PipelineStartedEvent',
    'PipelineCompletedEvent',
    'PipelineErrorEvent',
    'NodeStartedEvent',
    'NodeCompletedEvent',
    'CacheHitEvent',
    'CacheInvalidatedEvent',
    'SystemReadyEvent',
    'ComponentLoadedEvent',
    'ErrorEvent',
    'MetricEvent',
    'DataLoadedEvent',
    'DataTransformedEvent',

    # HookSpec (Pluggy 风格)
    'HookSpec',
    'HookSpecRegistry',

    # 死信处理 (Guava 风格)
    'DeadEvent',
    'DeadLetterQueue',

    # 历史事件 (Pluggy call_historic)
    'HistoricEventStore',
    'HistoricEntry',

    # 中间件 (Express/Koa 风格)
    'Middleware',
    'MiddlewarePipeline',
    'MiddlewareContext',
    'FunctionMiddleware',
    'LoggingMiddleware',
    'TracingMiddleware',
    'RetryMiddleware',
    'TimeoutMiddleware',
    'ValidationMiddleware',
    'MetricsMiddleware',
    'CircuitBreakerMiddleware',

    # 订阅管理 (RxPY 风格)
    'Disposable',
    'EmptyDisposable',
    'Subscription',
    'CompositeDisposable',
    'SerialDisposable',
    'SingleAssignmentDisposable',
    'RefCountDisposable',
    'SubscriptionManager',

    # 异步支持 (Reactor 风格)
    'AsyncEventBus',
    'AsyncEmitResult',
    'to_async',
    'run_sync',

    # 便捷函数
    'get_bus',
    'emit',
    'on',
    'off',
]


__version__ = '6.0.0'
