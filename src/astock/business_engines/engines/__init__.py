"""
业务引擎模块 v2.0
===============

清晰的分层架构:

数据层 (data/):
- duckdb_connector: 连接管理
- query_builder: SQL构建
- data_loader: 文件加载

业务层 (business/):
- industry_analyzer: 行业聚合
- outperform_analyzer: 超越筛选

扩展模块:
- duckdb_trend: 趋势分析
- scoring: 质量评分
- duckdb: DuckDB引擎
"""

from .data import (
    DuckDBConnector,
    get_duckdb_connection,
    QueryBuilder,
    DataLoader,
    quote_identifier,
)

from .business import (
    calc_industry_avg,
    filter_outperform_industry,
    IndustryAggregator,
    OutperformFilter,
)

# 导入扩展模块以触发方法注册
from . import duckdb_trend  # noqa: F401
from . import scoring  # noqa: F401
from . import duckdb  # noqa: F401

__all__ = [
    # 数据层
    'DuckDBConnector',
    'get_duckdb_connection',
    'QueryBuilder',
    'DataLoader',
    'quote_identifier',

    # 业务层
    'calc_industry_avg',
    'filter_outperform_industry',
    'IndustryAggregator',
    'OutperformFilter',
]