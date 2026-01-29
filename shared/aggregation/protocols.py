"""
PDDA Type System - Protocol Definitions
========================================

定义聚合系统的核心协议和类型。

设计原则：
1. 完全通用，零业务耦合
2. 基于 Python Protocol (PEP 544)
3. 支持泛型和类型推断
4. 可扩展和可组合

协议层次：
- Aggregatable[K, V]: 基础协议，定义聚合能力
- AggregatableResult: 具体实现基类
"""

from __future__ import annotations
from typing import Protocol, TypeVar, Generic, Any, Dict, Optional, runtime_checkable
from dataclasses import dataclass, field
from datetime import datetime
from abc import abstractmethod

# 泛型类型变量
K = TypeVar('K')  # Key type (如 str, int)
V = TypeVar('V')  # Value type (如 DataFrame, Dict, List)
T = TypeVar('T')  # Generic type


@dataclass
class AggregationMetadata:
    """
    聚合元数据

    用于携带额外的上下文信息，不影响核心聚合逻辑
    """
    producer_method: Optional[str] = None      # 生产者方法名
    timestamp: datetime = field(default_factory=datetime.now)
    version: str = "1.0.0"
    tags: Dict[str, Any] = field(default_factory=dict)

    # 配置项
    auto_collect: bool = True                   # 是否自动收集
    cache_enabled: bool = False                 # 是否启用缓存
    cache_ttl: int = 3600                       # 缓存过期时间（秒）
    priority: int = 0                           # 收集优先级


@runtime_checkable
class Aggregatable(Protocol[K, V]):
    """
    可聚合协议（顶层抽象）

    任何实现此协议的类型都可以被 PDDA 系统自动聚合。

    核心理念：
    - 定义"聚合能力"而非具体实现
    - 通过 key-value 抽象统一不同数据类型
    - 零假设，完全由实现者控制

    类型参数：
        K: 聚合键类型（如 str 表示 metric_name, int 表示索引）
        V: 聚合值类型（如 DataFrame, Dict, List, 自定义类型）

    使用场景：
        - 趋势分析结果: Aggregatable[str, DataFrame]
        - 宏观指标: Aggregatable[str, Dict]
        - 股票估值: Aggregatable[str, float]
        - 时序数据: Aggregatable[datetime, np.ndarray]
    """

    @abstractmethod
    def get_aggregation_key(self) -> K:
        """
        返回聚合键

        聚合键用于在聚合容器中唯一标识数据项。

        示例：
            - TrendAnalysisResult: 返回 metric_name ("roic", "roe")
            - MacroIndicatorResult: 返回 indicator_name ("gdp", "cpi")
            - StockValuation: 返回 ts_code ("000001.SZ")

        注意：
            - 键必须是可哈希的（hashable）
            - 相同的键会覆盖旧值（Dict 语义）
        """
        ...

    @abstractmethod
    def get_aggregation_value(self) -> V:
        """
        返回聚合值

        聚合值是实际需要被聚合和传递的数据。

        示例：
            - TrendAnalysisResult: 返回包含探针结果的 DataFrame
            - MacroIndicatorResult: 返回指标数据的 Dict
            - StockValuation: 返回估值浮点数

        注意：
            - 值可以是任意类型
            - 建议返回不可变类型或深拷贝以避免意外修改
        """
        ...

    def get_metadata(self) -> AggregationMetadata:
        """
        返回元数据（可选）

        默认实现返回空元数据，子类可以覆盖以提供更多信息。
        """
        return AggregationMetadata()


@dataclass
class AggregatableResult(Aggregatable[K, V], Generic[K, V]):
    """
    可聚合结果的通用实现

    这是一个具体的实现类，提供了 Aggregatable 协议的默认行为。
    业务代码可以直接使用此类，也可以实现自己的 Aggregatable。

    特性：
    - 简单的 key-value 包装
    - 可选的元数据支持
    - 类型安全的泛型

    使用示例：
        # 直接创建
        result = AggregatableResult(
            key="roic",
            value=trend_df,
            metadata=AggregationMetadata(producer_method="analyze_metric_trend")
        )

        # 工厂方法
        result = AggregatableResult.create(
            key="roic",
            value=trend_df,
            producer_method="analyze_metric_trend"
        )
    """

    key: K
    value: V
    metadata: AggregationMetadata = field(default_factory=AggregationMetadata)

    def get_aggregation_key(self) -> K:
        """返回聚合键"""
        return self.key

    def get_aggregation_value(self) -> V:
        """返回聚合值"""
        return self.value

    def get_metadata(self) -> AggregationMetadata:
        """返回元数据"""
        return self.metadata

    @classmethod
    def create(
        cls,
        key: K,
        value: V,
        producer_method: Optional[str] = None,
        auto_collect: bool = True,
        **metadata_kwargs
    ) -> AggregatableResult[K, V]:
        """
        工厂方法：创建 AggregatableResult 实例

        Args:
            key: 聚合键
            value: 聚合值
            producer_method: 生产者方法名
            auto_collect: 是否自动收集
            **metadata_kwargs: 其他元数据字段

        Returns:
            AggregatableResult 实例
        """
        metadata = AggregationMetadata(
            producer_method=producer_method,
            auto_collect=auto_collect,
            **metadata_kwargs
        )
        return cls(key=key, value=value, metadata=metadata)

    def __repr__(self) -> str:
        """友好的字符串表示"""
        return (
            f"AggregatableResult("
            f"key={self.key!r}, "
            f"value_type={type(self.value).__name__}, "
            f"producer={self.metadata.producer_method})"
        )


# ═══════════════════════════════════════════════════════════════
# 类型别名：常见场景的便捷定义
# ═══════════════════════════════════════════════════════════════

# 用于趋势分析等返回 DataFrame 的场景
DataFrameAggregatable = Aggregatable[str, Any]  # V 通常是 pd.DataFrame

# 用于字典数据的场景
DictAggregatable = Aggregatable[str, Dict[str, Any]]

# 用于列表数据的场景
ListAggregatable = Aggregatable[str, list]

# 用于数值数据的场景
NumericAggregatable = Aggregatable[str, float]
