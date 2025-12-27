"""
AStock Shared Module - 统一事件总线与命名规范系统
===============================================

提供全系统级别的解耦机制和统一规范：

核心组件：
- EventBus: 增强版事件总线 (发布/订阅模式 + 中间件 + 死信队列)
- Events: 标准化事件定义
- Protocols: 组件间接口契约
- NamingConvention: 统一命名规范系统 (v1.0.0)

EventBus 增强功能 (v6.0):
- HookSpec: 类型安全的事件规格定义 (Pluggy 风格)
- DeadLetterQueue: 无订阅者事件死信处理 (Guava 风格)
- HistoricEventStore: 历史事件存储与重放
- MiddlewarePipeline: 可插拔中间件管道 (Express/Koa 风格)
- Subscription: 可取消的订阅管理 (RxPY 风格)
- AsyncEventBus: 异步事件支持 (Reactor 风格)

架构设计：
┌──────────────────────────────────────────────────────────────────┐
│                        shared (核心层)                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  EventBus   │  │   Events    │  │      Protocols          │  │
│  │  (发布/订阅) │  │  (事件定义)  │  │  (接口契约/类型定义)     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              NamingConvention (命名规范)                      │ │
│  │  MetricRegistry | FieldRegistry | ColumnBuilder | PathConv.  │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              ▲
           ┌──────────────────┼──────────────────┐
           │                  │                  │
   ┌───────┴───────┐  ┌───────┴───────┐  ┌───────┴───────┐
   │  orchestrator │  │   pipeline    │  │     src       │
   │   (订阅/发布)  │  │  (订阅/发布)  │  │   (发布)       │
   └───────────────┘  └───────────────┘  └───────────────┘

设计原则：
1. 零依赖：shared 不依赖任何业务组件
2. 类型安全：所有事件有明确的 payload 类型
3. 可追溯：事件自带元数据（时间戳、来源）
4. 高性能：支持同步/异步、优先级、过滤器
5. 单一真相源：所有指标/字段配置集中管理
"""
# 新版 EventBus (v6.0)
from .event_bus import (
    # 核心
    EventBus,
    EventBusConfig,
    get_bus,
    emit,
    on,
    off,
    # 模型
    HandlerInfo,
    EmitResult,
    EventPriority,
    EventBusStats,
    # 中间件
    Middleware,
    MiddlewarePipeline,
    LoggingMiddleware,
    TracingMiddleware,
    RetryMiddleware,
    MetricsMiddleware,
    CircuitBreakerMiddleware,
    # 死信 & 历史
    DeadEvent,
    DeadLetterQueue,
    HistoricEventStore,
    # 订阅管理
    Subscription,
    Disposable,
    CompositeDisposable,
    # 异步
    AsyncEventBus,
    # HookSpec
    HookSpec,
    HookSpecRegistry,
)

# 事件定义
from .event_bus.events import (
    # 基础事件
    Event,
    # 注册相关
    MethodRegisteredEvent,
    MethodExecutedEvent,
    MethodSelectedEvent,
    RegistryRefreshedEvent,
    # Pipeline 相关
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineErrorEvent,
    NodeStartedEvent,
    NodeCompletedEvent,
    CacheHitEvent,
    CacheInvalidatedEvent,
    # 系统级
    SystemReadyEvent,
    ComponentLoadedEvent,
    ErrorEvent,
    MetricEvent,
    # 数据相关
    DataLoadedEvent,
    DataTransformedEvent,
)

# 命名规范系统
from .naming_convention import (
    # 枚举
    MetricCategory,
    DataStage,
    FieldCategory,
    # 数据类
    MetricConfig,
    FieldConfig,
    # 注册表
    MetricRegistry,
    FieldRegistry,
    # 工具类
    ColumnBuilder,
    PathConvention,
    LegacyAdapter,
    # 快捷映射
    METRIC_SOURCE_MAP,
    METRIC_PREFIX_MAP,
    METRIC_DISPLAY_MAP,
    SOURCE_TO_BUSINESS_MAP,
    PREFIX_TO_BUSINESS_MAP,
)

__version__ = "7.0.0"

# 新增独立模块 (v7.0)
# 这些模块可以独立导入，也可以通过 shared 命名空间访问
# 例如: from shared.errors import AStockError
#      from shared.logging import get_logger
#      from shared.cache import Cache, cached
#      from shared.config import Config, Settings
#      from shared.io import read_csv, write_parquet

__all__ = [
    # Core EventBus
    'EventBus',
    'EventBusConfig',
    'get_bus',
    'emit',
    'on',
    'off',
    'HandlerInfo',
    'EmitResult',
    'EventPriority',
    'EventBusStats',
    # 中间件
    'Middleware',
    'MiddlewarePipeline',
    'LoggingMiddleware',
    'TracingMiddleware',
    'RetryMiddleware',
    'MetricsMiddleware',
    'CircuitBreakerMiddleware',
    # 死信 & 历史
    'DeadEvent',
    'DeadLetterQueue',
    'HistoricEventStore',
    # 订阅管理
    'Subscription',
    'Disposable',
    'CompositeDisposable',
    # 异步
    'AsyncEventBus',
    # HookSpec
    'HookSpec',
    'HookSpecRegistry',
    # Events
    'Event',
    # Registration Events
    'MethodRegisteredEvent',
    'MethodExecutedEvent',
    'MethodSelectedEvent',
    'RegistryRefreshedEvent',
    # Pipeline Events
    'PipelineStartedEvent',
    'PipelineCompletedEvent',
    'PipelineErrorEvent',
    'NodeStartedEvent',
    'NodeCompletedEvent',
    'CacheHitEvent',
    'CacheInvalidatedEvent',
    # System Events
    'SystemReadyEvent',
    'ComponentLoadedEvent',
    'ErrorEvent',
    'MetricEvent',
    # Data Events
    'DataLoadedEvent',
    'DataTransformedEvent',
    # Naming Convention
    'MetricCategory',
    'DataStage',
    'FieldCategory',
    'MetricConfig',
    'FieldConfig',
    'MetricRegistry',
    'FieldRegistry',
    'ColumnBuilder',
    'PathConvention',
    'LegacyAdapter',
    'METRIC_SOURCE_MAP',
    'METRIC_PREFIX_MAP',
    'METRIC_DISPLAY_MAP',
    'SOURCE_TO_BUSINESS_MAP',
    'PREFIX_TO_BUSINESS_MAP',
    # New Independent Modules (v7.0)
    # - shared.errors: 统一错误系统 (参考 Django, FastAPI, requests)
    # - shared.logging: 结构化日志系统 (参考 structlog, loguru)
    # - shared.cache: 多层缓存系统 (参考 cachetools, diskcache)
    # - shared.config: 配置管理系统 (参考 pydantic-settings, dynaconf)
    # - shared.io: 统一 I/O 系统 (参考 kedro, fsspec)
]
