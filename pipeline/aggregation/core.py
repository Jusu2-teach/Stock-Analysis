"""
Aggregation Core - 聚合系统核心
================================

统一的数据聚合基础设施，提供：
1. AggregatableResult - 类型安全的可聚合结果
2. AggregationScope - FlowRun 级别数据隔离
3. Collector - 流式数据收集 API

设计原则：
- 类型安全：完整泛型支持 + 运行时验证
- 智能推断：基于类型注解自动推断配置
- 渐进复杂度：简单场景零配置，复杂场景完整控制
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
import uuid
import weakref
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

try:
    from typing import Self  # Python 3.11+
except ImportError:
    from typing_extensions import Self

__all__ = [
    # 结果类型
    "AggregatableResult",
    "ResultMetadata",
    # 作用域
    "AggregationScope",
    "ScopeManager",
    # 收集器
    "Collector",
    # 策略
    "ConflictStrategy",
    # 异常
    "AggregationError",
    "KeyConflictError",
    "NamespaceNotFoundError",
    "ValidationError",
]

logger = logging.getLogger(__name__)

# =============================================================================
# Type Variables
# =============================================================================

K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type
T = TypeVar("T")  # Generic type


# =============================================================================
# Exceptions
# =============================================================================

class AggregationError(Exception):
    """聚合系统基础异常"""
    pass


class KeyConflictError(AggregationError):
    """键冲突异常"""

    def __init__(self, namespace: str, key: str, existing_producer: str, new_producer: str):
        self.namespace = namespace
        self.key = key
        self.existing_producer = existing_producer
        self.new_producer = new_producer
        super().__init__(
            f"Key conflict in '{namespace}.{key}': "
            f"already produced by '{existing_producer}', "
            f"attempted by '{new_producer}'"
        )


class NamespaceNotFoundError(AggregationError):
    """命名空间不存在异常"""

    def __init__(self, namespace: str, available: List[str]):
        self.namespace = namespace
        self.available = available
        super().__init__(
            f"Namespace '{namespace}' not found. "
            f"Available: {', '.join(available) if available else '(none)'}"
        )


class ValidationError(AggregationError):
    """数据验证异常"""

    def __init__(self, message: str, key: str = None, expected: Type = None, actual: Type = None):
        self.key = key
        self.expected = expected
        self.actual = actual
        super().__init__(message)


# =============================================================================
# Enums
# =============================================================================

class ConflictStrategy(Enum):
    """键冲突处理策略"""
    ERROR = auto()      # 抛出异常 (默认，严格模式)
    REPLACE = auto()    # 新值覆盖旧值
    KEEP = auto()       # 保留旧值，忽略新值
    MERGE = auto()      # 尝试合并 (DataFrame concat, dict update)


# =============================================================================
# Result Metadata
# =============================================================================

@dataclass(frozen=True)
class ResultMetadata:
    """结果元数据 - 记录数据来源和特征

    Attributes:
        producer: 生产者名称 (step name 或 method name)
        produced_at: 生产时间戳
        checksum: 数据校验和 (可选，用于去重/缓存)
        row_count: 行数 (DataFrame 适用)
        column_count: 列数 (DataFrame 适用)
        tags: 自定义标签
    """
    producer: str
    produced_at: float = field(default_factory=time.time)
    checksum: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    tags: Tuple[Tuple[str, str], ...] = ()  # frozen dict alternative

    @classmethod
    def create(
        cls,
        producer: str,
        value: Any = None,
        tags: Dict[str, str] = None,
    ) -> "ResultMetadata":
        """工厂方法 - 自动推断元数据"""
        row_count = None
        column_count = None
        checksum = None

        # 推断 DataFrame 元数据
        if hasattr(value, "shape"):
            shape = value.shape
            row_count = shape[0] if len(shape) > 0 else None
            column_count = shape[1] if len(shape) > 1 else None

        # 计算校验和 (轻量级)
        if value is not None:
            try:
                checksum = cls._compute_checksum(value)
            except Exception:
                pass  # 校验和是可选的

        return cls(
            producer=producer,
            row_count=row_count,
            column_count=column_count,
            checksum=checksum,
            tags=tuple(sorted(tags.items())) if tags else (),
        )

    @staticmethod
    def _compute_checksum(value: Any) -> str:
        """计算轻量级校验和"""
        # 基于类型和大小的快速哈希
        type_name = type(value).__name__
        size_hint = 0

        if hasattr(value, "__len__"):
            size_hint = len(value)
        elif hasattr(value, "shape"):
            size_hint = sum(value.shape)

        return hashlib.md5(
            f"{type_name}:{size_hint}".encode()
        ).hexdigest()[:8]

    def get_tag(self, key: str, default: str = None) -> Optional[str]:
        """获取标签值"""
        for k, v in self.tags:
            if k == key:
                return v
        return default


# =============================================================================
# Aggregatable Result
# =============================================================================

@dataclass(frozen=True)
class AggregatableResult(Generic[K, V]):
    """可聚合结果 - 生产者输出的标准格式

    这是 PDDA (Producer-Driven Data Aggregation) 的核心数据结构。
    生产者返回此类型，消费者通过 Injector 自动接收聚合后的数据。

    Type Parameters:
        K: 键类型 (通常是 str)
        V: 值类型 (通常是 DataFrame 或 Dict)

    Attributes:
        key: 唯一标识符，在 namespace 内唯一
        value: 实际数据
        namespace: 命名空间，用于数据隔离和注入匹配
        metadata: 元数据 (可选)

    Examples:
        # 基础用法
        result = AggregatableResult(key="roic", value=df, namespace="trends")

        # 带元数据
        result = AggregatableResult(
            key="roic",
            value=df,
            namespace="trends",
            metadata=ResultMetadata.create("analyze_metric_trend", df),
        )

        # 链式创建
        result = AggregatableResult.of("roic", df).in_namespace("trends")
    """
    key: K
    value: V
    namespace: str = "default"
    metadata: Optional[ResultMetadata] = None

    def __post_init__(self):
        """验证必需字段"""
        if self.key is None:
            raise ValidationError("key cannot be None")
        if self.value is None:
            raise ValidationError("value cannot be None", key=str(self.key))

    # -------------------------------------------------------------------------
    # Factory Methods (链式 API)
    # -------------------------------------------------------------------------

    @classmethod
    def of(cls, key: K, value: V) -> "AggregatableResult[K, V]":
        """创建结果 (链式 API 起点)

        Example:
            result = AggregatableResult.of("roic", df).in_namespace("trends")
        """
        return cls(key=key, value=value)

    def in_namespace(self, namespace: str) -> "AggregatableResult[K, V]":
        """设置命名空间

        Example:
            result = AggregatableResult.of("roic", df).in_namespace("trends")
        """
        # frozen dataclass 需要创建新实例
        return AggregatableResult(
            key=self.key,
            value=self.value,
            namespace=namespace,
            metadata=self.metadata,
        )

    def with_metadata(
        self,
        producer: str = None,
        tags: Dict[str, str] = None,
    ) -> "AggregatableResult[K, V]":
        """添加元数据

        Example:
            result = (
                AggregatableResult.of("roic", df)
                .in_namespace("trends")
                .with_metadata(producer="analyze_trend", tags={"version": "1.0"})
            )
        """
        meta = ResultMetadata.create(
            producer=producer or "unknown",
            value=self.value,
            tags=tags,
        )
        return AggregatableResult(
            key=self.key,
            value=self.value,
            namespace=self.namespace,
            metadata=meta,
        )

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    @property
    def full_key(self) -> str:
        """完整键 (namespace.key)"""
        return f"{self.namespace}.{self.key}"

    def validate_type(self, expected_type: Type[V]) -> bool:
        """验证值类型"""
        return isinstance(self.value, expected_type)

    def map_value(self, func: Callable[[V], T]) -> "AggregatableResult[K, T]":
        """转换值 (函数式 API)

        Example:
            result.map_value(lambda df: df.head(10))
        """
        return AggregatableResult(
            key=self.key,
            value=func(self.value),
            namespace=self.namespace,
            metadata=self.metadata,
        )


# =============================================================================
# Scope Entry (Internal)
# =============================================================================

@dataclass
class _ScopeEntry:
    """作用域条目 (内部使用)"""
    value: Any
    metadata: Optional[ResultMetadata]
    created_at: float = field(default_factory=time.time)
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)

    def touch(self) -> None:
        """记录访问"""
        self.access_count += 1
        self.last_accessed = time.time()


# =============================================================================
# Aggregation Scope
# =============================================================================

class AggregationScope:
    """聚合作用域 - FlowRun 级别数据隔离

    提供线程安全的数据存储，确保不同 FlowRun 之间数据不会混淆。

    Features:
        - 命名空间隔离
        - 冲突检测与处理
        - 数据验证
        - 访问统计
        - 上下文管理器支持

    Examples:
        # 基础用法
        scope = AggregationScope(flow_id="flow-123")
        scope.set("trends", "roic", df)
        data = scope.get("trends", "roic")

        # 获取整个命名空间
        all_trends = scope.get_namespace("trends")

        # 使用收集器
        scope.collect(AggregatableResult.of("roic", df).in_namespace("trends"))

        # 上下文管理器
        with ScopeManager.create(flow_id="flow-123") as scope:
            ...
    """

    def __init__(
        self,
        flow_id: str = None,
        conflict_strategy: ConflictStrategy = ConflictStrategy.ERROR,
        validate_types: bool = True,
    ):
        """初始化作用域

        Args:
            flow_id: 关联的 FlowRun ID
            conflict_strategy: 键冲突处理策略
            validate_types: 是否启用类型验证
        """
        self._scope_id = str(uuid.uuid4())[:12]
        self._flow_id = flow_id
        self._conflict_strategy = conflict_strategy
        self._validate_types = validate_types

        # 数据存储: namespace -> key -> _ScopeEntry
        self._data: Dict[str, Dict[str, _ScopeEntry]] = {}
        self._lock = threading.RLock()

        # 类型注册表: namespace -> expected_type
        self._type_registry: Dict[str, Type] = {}

        # 状态
        self._disposed = False
        self._created_at = time.time()

        logger.debug(f"🔒 Scope created: {self._scope_id} (flow={flow_id})")

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def scope_id(self) -> str:
        return self._scope_id

    @property
    def flow_id(self) -> Optional[str]:
        return self._flow_id

    @property
    def is_disposed(self) -> bool:
        return self._disposed

    # -------------------------------------------------------------------------
    # Core Operations
    # -------------------------------------------------------------------------

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        metadata: ResultMetadata = None,
        producer: str = None,
    ) -> None:
        """存储数据

        Args:
            namespace: 命名空间
            key: 键
            value: 值
            metadata: 元数据 (可选)
            producer: 生产者名称 (用于元数据和冲突报告)

        Raises:
            KeyConflictError: 键冲突且策略为 ERROR
            ValidationError: 类型验证失败
        """
        self._check_disposed()

        with self._lock:
            # 确保命名空间存在
            if namespace not in self._data:
                self._data[namespace] = {}

            ns_data = self._data[namespace]

            # 检查冲突
            if key in ns_data:
                existing = ns_data[key]
                existing_producer = (
                    existing.metadata.producer if existing.metadata else "unknown"
                )

                if self._conflict_strategy == ConflictStrategy.ERROR:
                    raise KeyConflictError(
                        namespace, key, existing_producer, producer or "unknown"
                    )
                elif self._conflict_strategy == ConflictStrategy.KEEP:
                    logger.debug(f"Keeping existing value for {namespace}.{key}")
                    return
                elif self._conflict_strategy == ConflictStrategy.MERGE:
                    value = self._merge_values(existing.value, value, namespace, key)
                # REPLACE: 继续执行覆盖

            # 类型验证
            if self._validate_types and namespace in self._type_registry:
                expected = self._type_registry[namespace]
                if not isinstance(value, expected):
                    raise ValidationError(
                        f"Type mismatch for {namespace}.{key}: "
                        f"expected {expected.__name__}, got {type(value).__name__}",
                        key=key,
                        expected=expected,
                        actual=type(value),
                    )

            # 创建元数据
            if metadata is None and producer:
                metadata = ResultMetadata.create(producer, value)

            # 存储
            ns_data[key] = _ScopeEntry(value=value, metadata=metadata)
            logger.debug(f"📦 Stored: {namespace}.{key}")

    def get(
        self,
        namespace: str,
        key: str,
        default: T = None,
    ) -> Union[Any, T]:
        """获取单个数据

        Args:
            namespace: 命名空间
            key: 键
            default: 默认值 (键不存在时返回)

        Returns:
            存储的值或默认值
        """
        self._check_disposed()

        with self._lock:
            ns_data = self._data.get(namespace, {})
            entry = ns_data.get(key)

            if entry is None:
                return default

            entry.touch()
            return entry.value

    def get_namespace(self, namespace: str) -> Dict[str, Any]:
        """获取整个命名空间的数据

        Args:
            namespace: 命名空间

        Returns:
            命名空间内所有数据的字典 {key: value}
        """
        self._check_disposed()

        with self._lock:
            ns_data = self._data.get(namespace, {})
            result = {}

            for key, entry in ns_data.items():
                entry.touch()
                result[key] = entry.value

            return result

    def get_with_metadata(
        self,
        namespace: str,
        key: str,
    ) -> Optional[Tuple[Any, Optional[ResultMetadata]]]:
        """获取数据及其元数据

        Returns:
            (value, metadata) 元组，或 None
        """
        self._check_disposed()

        with self._lock:
            ns_data = self._data.get(namespace, {})
            entry = ns_data.get(key)

            if entry is None:
                return None

            entry.touch()
            return (entry.value, entry.metadata)

    # -------------------------------------------------------------------------
    # Collection API
    # -------------------------------------------------------------------------

    def collect(self, result: AggregatableResult) -> None:
        """收集聚合结果

        这是推荐的数据存储方式，自动处理命名空间和元数据。

        Args:
            result: 可聚合结果

        Example:
            scope.collect(AggregatableResult.of("roic", df).in_namespace("trends"))
        """
        producer = result.metadata.producer if result.metadata else None
        self.set(
            namespace=result.namespace,
            key=str(result.key),
            value=result.value,
            metadata=result.metadata,
            producer=producer,
        )

    def collect_many(self, results: List[AggregatableResult]) -> int:
        """批量收集结果

        Args:
            results: 结果列表

        Returns:
            成功收集的数量
        """
        count = 0
        for result in results:
            try:
                self.collect(result)
                count += 1
            except AggregationError as e:
                logger.warning(f"Failed to collect {result.full_key}: {e}")
        return count

    # -------------------------------------------------------------------------
    # Query API
    # -------------------------------------------------------------------------

    def has(self, namespace: str, key: str) -> bool:
        """检查键是否存在"""
        with self._lock:
            return key in self._data.get(namespace, {})

    def has_namespace(self, namespace: str) -> bool:
        """检查命名空间是否存在"""
        with self._lock:
            return namespace in self._data and len(self._data[namespace]) > 0

    def namespaces(self) -> List[str]:
        """列出所有非空命名空间"""
        with self._lock:
            return [ns for ns, data in self._data.items() if data]

    def keys(self, namespace: str) -> List[str]:
        """列出命名空间内所有键"""
        with self._lock:
            return list(self._data.get(namespace, {}).keys())

    def __contains__(self, namespace: str) -> bool:
        """支持 `in` 操作符"""
        return self.has_namespace(namespace)

    # -------------------------------------------------------------------------
    # Type Registry
    # -------------------------------------------------------------------------

    def register_type(self, namespace: str, expected_type: Type) -> None:
        """注册命名空间的期望类型

        Args:
            namespace: 命名空间
            expected_type: 期望的值类型

        Example:
            scope.register_type("trends", pd.DataFrame)
        """
        self._type_registry[namespace] = expected_type

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        """获取作用域统计信息"""
        with self._lock:
            total_items = sum(len(ns) for ns in self._data.values())
            namespace_stats = {
                ns: {
                    "count": len(data),
                    "keys": list(data.keys()),
                }
                for ns, data in self._data.items()
            }

            return {
                "scope_id": self._scope_id,
                "flow_id": self._flow_id,
                "total_items": total_items,
                "namespaces": namespace_stats,
                "age_seconds": time.time() - self._created_at,
            }

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def clear(self, namespace: str = None) -> None:
        """清空数据

        Args:
            namespace: 指定命名空间 (None 表示全部清空)
        """
        with self._lock:
            if namespace:
                self._data.pop(namespace, None)
            else:
                self._data.clear()

    def dispose(self) -> None:
        """释放作用域"""
        if self._disposed:
            return

        self._disposed = True
        self.clear()
        logger.debug(f"🔓 Scope disposed: {self._scope_id}")

    def _check_disposed(self) -> None:
        """检查是否已释放"""
        if self._disposed:
            raise AggregationError(
                f"Scope {self._scope_id} has been disposed"
            )

    def _merge_values(
        self,
        old: Any,
        new: Any,
        namespace: str = "",
        key: str = "",
    ) -> Any:
        """合并两个值

        Args:
            old: 旧值
            new: 新值
            namespace: 命名空间 (用于错误消息)
            key: 键名 (用于错误消息)

        Returns:
            合并后的值

        支持的合并类型:
        - pandas.DataFrame: pd.concat 纵向拼接
        - polars.DataFrame: pl.concat 纵向拼接
        - dict: 字典合并 (update)
        - list: 列表拼接

        对于不支持的类型，记录警告并返回新值。
        """
        # pandas DataFrame 合并
        try:
            import pandas as pd
            if isinstance(old, pd.DataFrame) and isinstance(new, pd.DataFrame):
                return pd.concat([old, new], ignore_index=True)
        except ImportError:
            pass

        # polars DataFrame 合并
        try:
            import polars as pl
            if isinstance(old, pl.DataFrame) and isinstance(new, pl.DataFrame):
                return pl.concat([old, new])
        except ImportError:
            pass

        # Dict 合并
        if isinstance(old, dict) and isinstance(new, dict):
            merged = old.copy()
            merged.update(new)
            return merged

        # List 合并
        if isinstance(old, list) and isinstance(new, list):
            return old + new

        # Set 合并
        if isinstance(old, set) and isinstance(new, set):
            return old | new

        # 不支持的类型: 记录警告并返回新值
        location = f"{namespace}.{key}" if namespace and key else "unknown"
        logger.warning(
            f"Cannot merge values at '{location}': "
            f"unsupported types {type(old).__name__} and {type(new).__name__}. "
            f"Returning new value."
        )
        return new

    def __enter__(self) -> "AggregationScope":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.dispose()


# =============================================================================
# Scope Manager
# =============================================================================

class ScopeManager:
    """作用域管理器 - 管理多个作用域的生命周期

    提供:
    - 作用域创建和销毁
    - 当前作用域跟踪 (ContextVar)
    - 嵌套作用域支持

    Examples:
        manager = ScopeManager()

        with manager.create(flow_id="flow-123") as scope:
            scope.set("trends", "roic", df)
            ...  # 退出时自动清理
    """

    _instance: Optional["ScopeManager"] = None
    _class_lock = threading.Lock()  # 类级别锁 (单例同步)

    def __init__(self):
        self._scopes: Dict[str, AggregationScope] = {}
        self._current_scope: Optional[AggregationScope] = None
        self._scope_stack: List[AggregationScope] = []
        self._instance_lock = threading.RLock()  # 实例级别锁 (数据操作)

    @classmethod
    def instance(cls) -> "ScopeManager":
        """获取单例实例"""
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @contextmanager
    def create(
        self,
        flow_id: str = None,
        **kwargs,
    ) -> Iterator[AggregationScope]:
        """创建并进入作用域

        Args:
            flow_id: 关联的 FlowRun ID
            **kwargs: 传递给 AggregationScope 的参数

        Yields:
            AggregationScope 实例

        Example:
            with manager.create(flow_id="flow-123") as scope:
                ...
        """
        scope = AggregationScope(flow_id=flow_id, **kwargs)

        with self._instance_lock:
            self._scopes[scope.scope_id] = scope
            self._scope_stack.append(scope)
            self._current_scope = scope

        try:
            yield scope
        finally:
            with self._instance_lock:
                self._scopes.pop(scope.scope_id, None)
                if self._scope_stack and self._scope_stack[-1] is scope:
                    self._scope_stack.pop()
                self._current_scope = (
                    self._scope_stack[-1] if self._scope_stack else None
                )
            scope.dispose()

    @property
    def current(self) -> Optional[AggregationScope]:
        """获取当前作用域"""
        return self._current_scope

    def get_scope(self, scope_id: str) -> Optional[AggregationScope]:
        """根据 ID 获取作用域"""
        return self._scopes.get(scope_id)


# =============================================================================
# Collector (Convenience API)
# =============================================================================

class Collector:
    """数据收集器 - 流式 API

    封装 AggregationScope，提供更便捷的收集接口。

    Examples:
        collector = Collector(scope)

        # 流式 API
        (collector
            .namespace("trends")
            .put("roic", roic_df)
            .put("roe", roe_df)
            .done())

        # 装饰器 API
        @collector.auto_collect
        def analyze_metric(...) -> AggregatableResult:
            return AggregatableResult.of("roic", df).in_namespace("trends")
    """

    def __init__(self, scope: AggregationScope):
        self._scope = scope
        self._current_namespace = "default"
        self._producer: Optional[str] = None

    # -------------------------------------------------------------------------
    # Fluent API
    # -------------------------------------------------------------------------

    def namespace(self, ns: str) -> Self:
        """设置当前命名空间"""
        self._current_namespace = ns
        return self

    def producer(self, name: str) -> Self:
        """设置生产者名称"""
        self._producer = name
        return self

    def put(self, key: str, value: Any) -> Self:
        """存储数据

        Example:
            collector.namespace("trends").put("roic", df).put("roe", df2)
        """
        self._scope.set(
            namespace=self._current_namespace,
            key=key,
            value=value,
            producer=self._producer,
        )
        return self

    def put_result(self, result: AggregatableResult) -> Self:
        """存储聚合结果"""
        self._scope.collect(result)
        return self

    def done(self) -> AggregationScope:
        """完成收集，返回作用域"""
        return self._scope

    # -------------------------------------------------------------------------
    # Decorator API
    # -------------------------------------------------------------------------

    def auto_collect(self, func: Callable[..., AggregatableResult]) -> Callable:
        """装饰器: 自动收集函数返回的结果

        Example:
            @collector.auto_collect
            def analyze(data) -> AggregatableResult:
                return AggregatableResult.of("roic", result)
        """
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, AggregatableResult):
                self._scope.collect(result)
            return result

        return wrapper

    # -------------------------------------------------------------------------
    # Batch API
    # -------------------------------------------------------------------------

    def put_many(self, items: Dict[str, Any]) -> Self:
        """批量存储

        Example:
            collector.namespace("trends").put_many({"roic": df1, "roe": df2})
        """
        for key, value in items.items():
            self.put(key, value)
        return self

    def put_results(self, results: List[AggregatableResult]) -> Self:
        """批量存储聚合结果"""
        self._scope.collect_many(results)
        return self

    # -------------------------------------------------------------------------
    # TaskExecutor Integration API
    # -------------------------------------------------------------------------

    def set_scope(self, scope: AggregationScope) -> None:
        """设置作用域（用于 TaskExecutor 动态设置）"""
        self._scope = scope

    def collect_from_task_result(
        self,
        task_name: str,
        result: Any,
        scope: AggregationScope = None,
    ) -> None:
        """从任务结果中收集聚合数据

        如果 result 是 AggregatableResult，自动收集。
        """
        target_scope = scope or self._scope
        if target_scope is None:
            return

        if isinstance(result, AggregatableResult):
            # 更新元数据中的生产者
            if result.metadata is None:
                result = result.with_metadata(producer=task_name)
            target_scope.collect(result)

        # Support foreign/adapter Aggregatable objects via duck-typing (e.g. shared.aggregation.AggregatableResult)
        # Keep pipeline fully decoupled: do NOT import shared here.
        elif (
            result is not None
            and hasattr(result, 'get_aggregation_key')
            and hasattr(result, 'get_aggregation_value')
        ):
            try:
                key = result.get_aggregation_key()
                value = result.get_aggregation_value()
                metadata_obj = result.get_metadata() if hasattr(result, 'get_metadata') else None

                # Determine namespace
                namespace = getattr(result, 'namespace', None)
                if not namespace:
                    # Heuristic for business trends producers
                    tn = str(task_name).lower()
                    producer_method = getattr(metadata_obj, 'producer_method', '') if metadata_obj is not None else ''
                    producer_method = str(producer_method).lower()
                    if 'trend' in tn or 'trend' in producer_method:
                        namespace = 'trends'
                    else:
                        namespace = 'default'

                # Convert metadata tags if available
                tags: Dict[str, str] = {}
                raw_tags = getattr(metadata_obj, 'tags', None) if metadata_obj is not None else None
                if isinstance(raw_tags, dict):
                    for k, v in raw_tags.items():
                        try:
                            tags[str(k)] = str(v)
                        except Exception:
                            continue

                meta = ResultMetadata.create(producer=task_name, value=value, tags=tags)
                target_scope.collect(AggregatableResult(key=key, value=value, namespace=namespace, metadata=meta))
            except Exception:
                # Aggregation is best-effort; never fail the task because of collection.
                return

    def prepare_consumer_inputs(
        self,
        consumer_task: str,
        param_name: str,
        namespace: str,
        scope: AggregationScope = None,
    ) -> Dict[str, Any]:
        """为消费者任务准备聚合输入

        Args:
            consumer_task: 消费者任务名称
            param_name: 参数名称
            namespace: 命名空间
            scope: 作用域

        Returns:
            包含聚合数据的字典 {param_name: aggregated_data}
        """
        target_scope = scope or self._scope
        if target_scope is None:
            return {}

        # 获取命名空间内的所有数据
        aggregated = target_scope.get_namespace(namespace)

        if aggregated:
            return {param_name: aggregated}

        return {}


# =============================================================================
# Module-level convenience functions
# =============================================================================

def get_current_scope() -> Optional[AggregationScope]:
    """获取当前作用域"""
    return ScopeManager.instance().current
