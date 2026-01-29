"""
Pipeline - Enterprise-grade workflow execution engine.

Architecture:
    Config (YAML) → Core (Spec/Run/State) → Execution (Runner/Executor) → Data (Catalog)

Modules:
    - core/: 核心模型 (FlowSpec, TaskSpec, FlowRun, TaskRun, State, DAG, Container)
    - events/: 企业级事件总线 (EventBus, HookSpec, EventMiddleware, EventStore)
    - aggregation/: PDDA 数据聚合框架 (Scope, Collector, Injector, Lineage)
    - execution/: 执行引擎 (FlowRunner, TaskExecutor, ExecutionMiddlewareBase)
    - catalog/: 数据目录管理
    - cache/: 缓存后端
    - config/: 配置加载

Usage:
    from pipeline import load_flow, FlowRunner
    from pipeline.core.container import get_container

    # Load and run with DI
    spec = load_flow("workflow/analysis.yaml")
    container = get_container()
    runner = FlowRunner(container=container)
    result = runner.run(spec)

    # Check result
    print(result.state)  # FlowState.SUCCESS

    # Events (via Container)
    from pipeline.events import EventBus
    bus = container.resolve(EventBus)
    bus.on("task.completed", lambda e: print(e))

    # PDDA (via Scoped Container)
    from pipeline.aggregation import AggregatableResult, Collector
    with container.create_scope() as scope:
        collector = scope.resolve(Collector)
        result = AggregatableResult(key="metric", value=df)
        collector.collect(result, scope=scope)
"""

__version__ = "2.0.0"

# Core models
from pipeline.core.spec import FlowSpec, TaskSpec, TaskInputSpec, TaskOutputSpec
from pipeline.core.run import FlowRun, TaskRun
from pipeline.core.state import TaskState, FlowState
from pipeline.core.policy import RetryPolicy
from pipeline.core.dag import DAG

# Data catalog
from pipeline.catalog.catalog import DataCatalog
from pipeline.catalog.entry import DataEntry

# Events (v2.0 - 层级路由 + 类型安全)
from pipeline.events.bus import (
    EventBus,
    Event,
    Priority,
    Subscription,
    on,
)
from pipeline.events.types import (
    FlowPayload,
    TaskPayload,
    FlowEvents,
    TaskEvents,
    DataEvents,
    EventType,
)
from pipeline.events.middleware import (
    LoggingMiddleware,
    MetricsMiddleware,
    RetryMiddleware,
)

# Aggregation (v2.0 - PDDA 智能注入)
from pipeline.aggregation.core import (
    AggregatableResult,
    AggregationScope,
    ScopeManager,
    Collector,
    ConflictStrategy,
)
from pipeline.aggregation.inject import (
    Injector,
    inject,
    Aggregated,
)
from pipeline.aggregation.lineage import (
    DataLineage,
    LineageTracker,
    LineageQuery,
)

# Execution
from pipeline.execution.middleware import ExecutionMiddlewareBase, ExecutionMiddlewareChain
from pipeline.execution.executor import TaskExecutor
from pipeline.execution.runner import FlowRunner, RunnerConfig, DryRunResult

# Protocols (新架构 - 推荐使用)
from pipeline.protocols import (
    # Core Protocols
    ExecutableProtocol,
    ExecutionResult,
    ExecutionStatus,
    ResolvableProtocol,
    ResolveResult,
    ConfigurableProtocol,
    ConfigSchema,
    SerializableProtocol,

    # Domain Protocols
    TaskProtocol,
    TaskInfo,
    TaskCapabilities,
    IOProtocol,
    InputSpec,
    OutputSpec,
    ResourceProtocol,
    ResourceSpec,
    ContextProtocol,
    ExecutionContext,

    # Integration Protocols
    MethodResolverProtocol,
    MethodInfo,
    MethodSelectorProtocol,
    StorageBackendProtocol,
    StorageResult,
    NotificationChannelProtocol,
    NotificationPayload,
    MetricCollectorProtocol,
    MetricValue,
)

# Cache
from pipeline.cache.backends import (
    CacheBackend,
    NullCacheBackend,
    MemoryCacheBackend,
    FileCacheBackend,
    TieredCacheBackend,
)

from pipeline.cache.router import CacheBackendRouter

# Config
from pipeline.config.loader import YAMLLoader, load_flow, load_flow_string
from pipeline.config.resolver import ReferenceResolver

__all__ = [
    # Core
    "FlowSpec",
    "TaskSpec",
    "TaskInputSpec",
    "TaskOutputSpec",
    "FlowRun",
    "TaskRun",
    "TaskState",
    "FlowState",
    "RetryPolicy",
    "DAG",
    # Data Catalog
    "DataCatalog",
    "DataEntry",
    # Events (v2.0)
    "EventBus",
    "Event",
    "Priority",
    "Subscription",
    "on",
    "FlowPayload",
    "TaskPayload",
    "FlowEvents",
    "TaskEvents",
    "DataEvents",
    "EventType",
    "LoggingMiddleware",
    "MetricsMiddleware",
    "RetryMiddleware",
    # Aggregation (v2.0 PDDA)
    "AggregatableResult",
    "AggregationScope",
    "ScopeManager",
    "Collector",
    "ConflictStrategy",
    "Injector",
    "inject",
    "Aggregated",
    "DataLineage",
    "LineageTracker",
    "LineageQuery",
    # Execution
    "ExecutionMiddlewareBase",
    "ExecutionMiddlewareChain",
    "TaskExecutor",
    "FlowRunner",
    "RunnerConfig",
    "DryRunResult",

    # Protocols - Core
    "ExecutableProtocol",
    "ExecutionResult",
    "ExecutionStatus",
    "ResolvableProtocol",
    "ResolveResult",
    "ConfigurableProtocol",
    "ConfigSchema",
    "SerializableProtocol",

    # Protocols - Domain
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

    # Protocols - Integration (向后兼容)
    "MethodResolverProtocol",
    "MethodInfo",
    "MethodSelectorProtocol",
    "StorageBackendProtocol",
    "StorageResult",
    "NotificationChannelProtocol",
    "NotificationPayload",
    "MetricCollectorProtocol",
    "MetricValue",
    # Cache
    "CacheBackend",
    "NullCacheBackend",
    "MemoryCacheBackend",
    "FileCacheBackend",
    "TieredCacheBackend",
    "CacheBackendRouter",
    # Config
    "YAMLLoader",
    "load_flow",
    "load_flow_string",
    "ReferenceResolver",
]
