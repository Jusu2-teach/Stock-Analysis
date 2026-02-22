"""
PDDA 数据类型 - 业务层公共 API
================================

提供 AggregatableResult 和 AggregationMetadata 类型定义，
供业务引擎（trend/evaluators/truth）使用。

运行时聚合引擎位于 pipeline.aggregation，本模块仅提供类型层。

使用示例：
    from shared.aggregation import AggregatableResult, AggregationMetadata

    @register_method(...)
    def analyze_metric_trend(...) -> AggregatableResult[str, pd.DataFrame]:
        return AggregatableResult(key="roic", value=df)
"""

from .protocols import (
    AggregatableResult,
    AggregationMetadata,
)

__all__ = [
    'AggregatableResult',
    'AggregationMetadata',
]

__version__ = '2.1.0'
