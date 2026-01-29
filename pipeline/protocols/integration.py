"""Pipeline Protocols - Layer 3: Integration Protocols
=====================================================

适配器实现层协议，定义与外部系统集成的接口。

这些协议是 Pipeline 与外部世界的桥梁：
- MethodResolver: 连接任意方法注册中心
- StorageBackend: 连接任意存储系统
- NotificationChannel: 连接任意通知系统
- MetricCollector: 连接任意监控系统

设计原则：
    - 松耦合：Pipeline 不知道具体实现
    - 可插拔：支持任意数量的适配器
    - 向后兼容：保持与旧 API 的兼容性
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Protocol,
    Set,
    Type,
    TypeVar,
    Union,
    runtime_checkable,
)

from .core import ExecutionResult, ExecutionStatus, ResolveResult

__all__ = [
    # 方法解析器
    "MethodResolverProtocol",
    "MethodInfo",
    "MethodSelectorProtocol",
    "ResolutionStrategy",

    # 存储后端
    "StorageBackendProtocol",
    "StorageResult",
    "StorageOperation",

    # 通知渠道
    "NotificationChannelProtocol",
    "NotificationPayload",
    "NotificationLevel",

    # 指标收集
    "MetricCollectorProtocol",
    "MetricValue",
    "MetricType",
]


# =============================================================================
# 类型变量
# =============================================================================

T = TypeVar("T")


# =============================================================================
# 方法信息 (增强版)
# =============================================================================

@dataclass
class MethodInfo:
    """方法信息（增强版）

    封装可调用方法的完整元数据。

    这是 Pipeline 层面对方法的抽象描述，不依赖任何具体注册中心实现。

    增强特性：
    - 支持能力标记
    - 支持版本管理
    - 支持元数据扩展

    向后兼容：
    - 保持与旧版 MethodInfo 相同的核心属性
    """

    __slots__ = (
        'name', 'component', 'engine', 'callable',
        'description', 'version', 'priority', 'tags', 'metadata',
        'deprecated', 'deprecation_message', 'capabilities'
    )

    def __init__(
        self,
        name: str,
        component: str = "",
        engine: str = "",
        callable: Optional[Callable] = None,
        description: str = "",
        version: str = "1.0.0",
        priority: int = 0,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        deprecated: bool = False,
        deprecation_message: str = "",
        capabilities: Optional[Set[str]] = None,
    ):
        self.name = name
        self.component = component
        self.engine = engine
        self.callable = callable
        self.description = description
        self.version = version
        self.priority = priority
        self.tags = tags or []
        self.metadata = metadata or {}
        self.deprecated = deprecated
        self.deprecation_message = deprecation_message
        self.capabilities = capabilities or set()

    @property
    def full_key(self) -> str:
        """完整键: component::engine::name

        向后兼容旧版格式。
        """
        if self.component and self.engine:
            return f"{self.component}::{self.engine}::{self.name}"
        return self.name

    @property
    def short_key(self) -> str:
        """简短键: engine::name"""
        if self.engine:
            return f"{self.engine}::{self.name}"
        return self.name

    def has_capability(self, cap: str) -> bool:
        """检查是否具有某能力"""
        return cap in self.capabilities

    def __repr__(self) -> str:
        return f"MethodInfo({self.full_key})"

    def __hash__(self) -> int:
        return hash(self.full_key)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, MethodInfo):
            return self.full_key == other.full_key
        return False


# =============================================================================
# 解析策略
# =============================================================================

class ResolutionStrategy(Enum):
    """解析策略枚举

    定义方法解析的策略。
    """
    DEFAULT = auto()     # 默认（通常是第一个匹配）
    PRIORITY = auto()    # 按优先级选择最高
    VERSION = auto()     # 按版本选择最新
    RANDOM = auto()      # 随机选择（负载均衡）
    ROUND_ROBIN = auto() # 轮询选择


# =============================================================================
# 方法解析器协议 (增强版)
# =============================================================================

@runtime_checkable
class MethodResolverProtocol(Protocol):
    """方法解析器协议 (增强版)

    定义 Pipeline 需要的方法解析能力。

    这是一个 **协议（接口）**，Pipeline 只依赖这个协议，不提供任何实现。

    设计原则：
    - 不绑定任何特定注册中心实现
    - 支持多种解析策略
    - 支持能力发现
    - 完全向后兼容

    已知实现：
    - RegistryMethodResolver (orchestrator/adapters) - 适配 orchestrator.Registry
    - (可扩展) K8sMethodResolver - 适配 Kubernetes 服务发现
    - (可扩展) GRPCMethodResolver - 适配 gRPC 服务

    Usage (在 Pipeline 内部):
        def execute(self, resolver: MethodResolverProtocol):
            method = resolver.resolve("business_engine", "duckdb", "analyze_metric")
            if method:
                result = method.callable(*args, **kwargs)

    Usage (在应用入口层):
        from orchestrator.adapters import RegistryMethodResolver

        resolver = RegistryMethodResolver()
        runner = FlowRunner(container, method_resolver=resolver)
    """

    def resolve(
        self,
        component: str,
        engine: str,
        method: str,
    ) -> Optional[MethodInfo]:
        """解析方法（兼容旧版）

        Args:
            component: 组件类型 (如 "business_engine", "data_engine")
            engine: 引擎类型 (如 "duckdb", "polars", "pandas")
            method: 方法名称 (如 "analyze_metric_trend")

        Returns:
            MethodInfo 或 None (如果未找到)
        """
        ...

    def resolve_by_key(
        self,
        key: str,
        strategy: ResolutionStrategy = ResolutionStrategy.DEFAULT,
    ) -> Optional[MethodInfo]:
        """按键解析方法（新增，更灵活）

        Args:
            key: 方法键（支持多种格式）
                 - "method_name" (简单名称)
                 - "engine::method_name" (带引擎)
                 - "component::engine::method_name" (完整键)
            strategy: 解析策略

        Returns:
            MethodInfo 或 None
        """
        ...

    def can_resolve(self, method: str) -> bool:
        """检查方法是否可解析

        Args:
            method: 方法名称

        Returns:
            是否存在该方法
        """
        ...

    def list_methods(
        self,
        component: Optional[str] = None,
        engine: Optional[str] = None,
        tags: Optional[List[str]] = None,
        capabilities: Optional[Set[str]] = None,
    ) -> Dict[str, MethodInfo]:
        """列出方法（增强版）

        Args:
            component: 可选，过滤组件类型
            engine: 可选，过滤引擎类型
            tags: 可选，过滤标签
            capabilities: 可选，过滤能力

        Returns:
            方法字典 {full_key: MethodInfo}
        """
        ...

    def resolve_callable(
        self,
        component: str,
        engine: str,
        method: str,
    ) -> Optional[Callable]:
        """快捷方法：直接解析为可调用对象

        等价于 resolve(...).callable，但更方便。
        """
        ...

    def discover_capabilities(self) -> Set[str]:
        """发现所有可用能力

        Returns:
            所有方法的能力集合
        """
        ...


# =============================================================================
# 方法选择协议 (高级)
# =============================================================================

@runtime_checkable
class MethodSelectorProtocol(Protocol):
    """方法选择协议（高级功能）

    支持多实现选择策略，用于复杂场景：
    - 同一方法多个实现（不同引擎）
    - 灰度发布
    - A/B 测试
    - 负载均衡
    """

    def select(
        self,
        component: str,
        method: str,
        *,
        strategy: ResolutionStrategy = ResolutionStrategy.DEFAULT,
        preferred_engine: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[MethodInfo]:
        """根据策略选择最佳方法实现

        Args:
            component: 组件类型
            method: 方法名称
            strategy: 选择策略
            preferred_engine: 偏好引擎
            filters: 额外过滤条件

        Returns:
            选中的 MethodInfo 或 None
        """
        ...

    def select_all(
        self,
        component: str,
        method: str,
    ) -> List[MethodInfo]:
        """获取所有匹配的实现

        Args:
            component: 组件类型
            method: 方法名称

        Returns:
            所有匹配的 MethodInfo 列表
        """
        ...


# =============================================================================
# 存储操作类型
# =============================================================================

class StorageOperation(Enum):
    """存储操作类型"""
    READ = auto()
    WRITE = auto()
    DELETE = auto()
    LIST = auto()
    EXISTS = auto()


# =============================================================================
# 存储结果
# =============================================================================

@dataclass
class StorageResult(Generic[T]):
    """存储操作结果"""
    success: bool
    operation: StorageOperation
    data: Optional[T] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def ok(cls, operation: StorageOperation, data: T = None) -> "StorageResult[T]":
        return cls(success=True, operation=operation, data=data)

    @classmethod
    def fail(cls, operation: StorageOperation, error: str) -> "StorageResult[T]":
        return cls(success=False, operation=operation, error=error)


# =============================================================================
# 存储后端协议
# =============================================================================

@runtime_checkable
class StorageBackendProtocol(Protocol[T]):
    """存储后端协议

    定义数据持久化的标准接口。

    设计参考:
        - Dagster: IOManager
        - Prefect: Result storage
        - Airflow: XCom backends

    这个协议抽象了所有存储操作，支持：
    - 文件系统
    - 对象存储 (S3, GCS)
    - 数据库
    - 缓存系统

    Example:
        class S3StorageBackend(StorageBackendProtocol[bytes]):
            def write(self, key: str, data: bytes) -> StorageResult:
                s3.put_object(Bucket=self.bucket, Key=key, Body=data)
                return StorageResult.ok(StorageOperation.WRITE)
    """

    def read(self, key: str) -> StorageResult[T]:
        """读取数据

        Args:
            key: 存储键

        Returns:
            StorageResult
        """
        ...

    def write(self, key: str, data: T, **options: Any) -> StorageResult[T]:
        """写入数据

        Args:
            key: 存储键
            data: 数据
            **options: 额外选项

        Returns:
            StorageResult
        """
        ...

    def delete(self, key: str) -> StorageResult[None]:
        """删除数据

        Args:
            key: 存储键

        Returns:
            StorageResult
        """
        ...

    def exists(self, key: str) -> bool:
        """检查是否存在

        Args:
            key: 存储键

        Returns:
            是否存在
        """
        ...

    def list_keys(self, prefix: str = "") -> List[str]:
        """列出键

        Args:
            prefix: 前缀过滤

        Returns:
            键列表
        """
        ...


# =============================================================================
# 通知级别
# =============================================================================

class NotificationLevel(Enum):
    """通知级别"""
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


# =============================================================================
# 通知负载
# =============================================================================

@dataclass
class NotificationPayload:
    """通知负载"""
    title: str
    message: str
    level: NotificationLevel = NotificationLevel.INFO
    source: str = ""
    timestamp: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # 关联信息
    flow_run_id: Optional[str] = None
    task_run_id: Optional[str] = None


# =============================================================================
# 通知渠道协议
# =============================================================================

@runtime_checkable
class NotificationChannelProtocol(Protocol):
    """通知渠道协议

    定义通知发送的标准接口。

    支持的渠道类型（通过适配器实现）：
    - Email
    - Slack
    - Teams
    - Webhook
    - SMS

    Example:
        class SlackChannel(NotificationChannelProtocol):
            def send(self, payload: NotificationPayload) -> bool:
                slack.post_message(
                    channel=self.channel,
                    text=payload.message,
                )
                return True
    """

    @property
    def channel_name(self) -> str:
        """渠道名称"""
        ...

    def send(self, payload: NotificationPayload) -> bool:
        """发送通知

        Args:
            payload: 通知负载

        Returns:
            是否成功
        """
        ...

    def send_batch(self, payloads: List[NotificationPayload]) -> List[bool]:
        """批量发送通知

        Args:
            payloads: 通知负载列表

        Returns:
            每个通知的发送结果
        """
        ...

    def supports_level(self, level: NotificationLevel) -> bool:
        """检查是否支持某通知级别

        Args:
            level: 通知级别

        Returns:
            是否支持
        """
        ...


# =============================================================================
# 指标类型
# =============================================================================

class MetricType(Enum):
    """指标类型"""
    COUNTER = auto()    # 计数器
    GAUGE = auto()      # 瞬时值
    HISTOGRAM = auto()  # 直方图
    SUMMARY = auto()    # 摘要


# =============================================================================
# 指标值
# =============================================================================

@dataclass
class MetricValue:
    """指标值"""
    name: str
    value: float
    type: MetricType = MetricType.GAUGE
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: Optional[float] = None
    description: str = ""


# =============================================================================
# 指标收集协议
# =============================================================================

@runtime_checkable
class MetricCollectorProtocol(Protocol):
    """指标收集协议

    定义指标收集的标准接口。

    支持的后端（通过适配器实现）：
    - Prometheus
    - StatsD
    - OpenTelemetry
    - Custom

    Example:
        class PrometheusCollector(MetricCollectorProtocol):
            def record(self, metric: MetricValue) -> None:
                if metric.type == MetricType.COUNTER:
                    self.counters[metric.name].labels(**metric.labels).inc(metric.value)
    """

    @property
    def collector_name(self) -> str:
        """收集器名称"""
        ...

    def record(self, metric: MetricValue) -> None:
        """记录指标

        Args:
            metric: 指标值
        """
        ...

    def record_batch(self, metrics: List[MetricValue]) -> None:
        """批量记录指标

        Args:
            metrics: 指标值列表
        """
        ...

    def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """增加计数器

        Args:
            name: 指标名称
            value: 增量
            labels: 标签
        """
        ...

    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """设置瞬时值

        Args:
            name: 指标名称
            value: 值
            labels: 标签
        """
        ...

    def timing(
        self,
        name: str,
        duration_ms: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """记录耗时

        Args:
            name: 指标名称
            duration_ms: 耗时（毫秒）
            labels: 标签
        """
        ...

    def flush(self) -> None:
        """刷新缓冲区"""
        ...
