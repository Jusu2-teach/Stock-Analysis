"""Pipeline Protocols - 企业级通用协议层
=========================================

这是 Pipeline 2.0 的核心协议定义，采用行业最佳实践设计。

设计哲学：
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  "协议是系统的契约，而非实现的枷锁"                                       │
    │                                                                          │
    │  本协议层的设计原则：                                                     │
    │  1. 通用性：不绑定任何特定外部系统                                        │
    │  2. 可扩展性：支持任意数量的适配器实现                                    │
    │  3. 类型安全：完整的泛型和运行时检查                                      │
    │  4. 零耦合：Pipeline 只依赖协议，永不依赖具体实现                         │
    └─────────────────────────────────────────────────────────────────────────┘

参考的行业标准：
    - Apache Airflow: TaskSDK, Executor, XCom, Hooks
    - Dagster: Ops, Resources, IOManager, Config
    - Prefect: Task, Flow, TaskRunner, CachePolicy
    - Luigi: Task, Target, Parameter, Events

协议分层架构：
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                         Layer 1: Core Protocols                         │
    │                    (最核心的抽象，任何实现都必须遵循)                       │
    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
    │  │ Executable   │ │ Resolvable   │ │ Configurable │ │ Serializable │   │
    │  │ Protocol     │ │ Protocol     │ │ Protocol     │ │ Protocol     │   │
    │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
    └─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                       Layer 2: Domain Protocols                         │
    │                       (面向特定领域的协议组合)                             │
    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
    │  │ TaskProtocol │ │ IOProtocol   │ │ Resource     │ │ Context      │   │
    │  │              │ │              │ │ Protocol     │ │ Protocol     │   │
    │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
    └─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     Layer 3: Integration Protocols                      │
    │                     (适配器实现层，连接外部系统)                           │
    │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
    │  │ Method       │ │ Storage      │ │ Notification │ │ Metric       │   │
    │  │ Resolver     │ │ Backend      │ │ Channel      │ │ Collector    │   │
    │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
    └─────────────────────────────────────────────────────────────────────────┘

版本: 2.0.0
"""

__version__ = "2.0.0"

# =============================================================================
# Layer 1: Core Protocols
# =============================================================================
from .core import (
    # 可执行协议
    ExecutableProtocol,
    ExecutionResult,
    ExecutionStatus,

    # 可解析协议
    ResolvableProtocol,
    ResolveResult,

    # 可配置协议
    ConfigurableProtocol,
    ConfigSchema,

    # 可序列化协议
    SerializableProtocol,
)

# =============================================================================
# Layer 2: Domain Protocols
# =============================================================================
from .domain import (
    # 任务协议
    TaskProtocol,
    TaskInfo,
    TaskCapabilities,

    # IO 协议
    IOProtocol,
    InputSpec,
    OutputSpec,

    # 资源协议
    ResourceProtocol,
    ResourceSpec,

    # 执行上下文协议
    ContextProtocol,
    ExecutionContext,
)

# =============================================================================
# Layer 3: Integration Protocols
# =============================================================================
from .integration import (
    # 方法解析器
    MethodResolverProtocol,
    MethodInfo,
    MethodSelectorProtocol,

    # 存储后端
    StorageBackendProtocol,
    StorageResult,

    # 通知渠道
    NotificationChannelProtocol,
    NotificationPayload,

    # 指标收集
    MetricCollectorProtocol,
    MetricValue,
)

__all__ = [
    # Core
    "ExecutableProtocol",
    "ExecutionResult",
    "ExecutionStatus",
    "ResolvableProtocol",
    "ResolveResult",
    "ConfigurableProtocol",
    "ConfigSchema",
    "SerializableProtocol",

    # Domain
    "TaskProtocol",
    "TaskInfo",
    "TaskCapabilities",
    "IOProtocol",
    "InputSpec",
    "OutputSpec",
    "ResourceProtocol",
    "ResourceSpec",
    "ContextProtocol",
    "ExecutionContext",

    # Integration
    "MethodResolverProtocol",
    "MethodInfo",
    "MethodSelectorProtocol",
    "StorageBackendProtocol",
    "StorageResult",
    "NotificationChannelProtocol",
    "NotificationPayload",
    "MetricCollectorProtocol",
    "MetricValue",
]
