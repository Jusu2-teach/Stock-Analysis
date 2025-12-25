"""
AStock Shared Module - 统一事件总线与命名规范系统
===============================================

提供全系统级别的解耦机制和统一规范：

核心组件：
- EventBus: 统一事件总线 (发布/订阅模式)
- Events: 标准化事件定义
- Protocols: 组件间接口契约
- NamingConvention: 统一命名规范系统 (v1.0.0)

命名规范系统 (naming_convention):
- MetricRegistry: 指标配置注册表 (单一真相源)
- FieldRegistry: 字段配置注册表
- ColumnBuilder: 列名构建器
- PathConvention: 路径命名规范

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
from .event_bus import EventBus, EventPriority
from .events import (
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
from .protocols import (
    OrchestratorProtocol,
    RegistryProtocol,
    ExecutorProtocol,
    PipelineContextProtocol,
    DataEngineProtocol,
    CacheProtocol,
    EventBusProtocol,
    HookManagerProtocol,
    MethodHandleProtocol,
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

__version__ = "1.1.0"
__all__ = [
    # Core
    'EventBus',
    'EventPriority',
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
    # Protocols
    'OrchestratorProtocol',
    'RegistryProtocol',
    'ExecutorProtocol',
    'PipelineContextProtocol',
    'DataEngineProtocol',
    'CacheProtocol',
    'EventBusProtocol',
    'HookManagerProtocol',
    'MethodHandleProtocol',
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
]
