"""
业务引擎模块（重构版）
===================

提供业务分析功能：
- engines: DuckDB业务方法（已合并简化）
- trend: 趋势分析
- scoring: 质量评分
- reporting: 报告生成
"""

from orchestrator import Registry
from .engines import duckdb_core, duckdb_trend, scoring

# Scan DuckDB Core
Registry.get().scan(
    module=duckdb_core,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "analysis")
)

# Scan DuckDB Trend
Registry.get().scan(
    module=duckdb_trend,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "trend")
)

# Scan Scoring
Registry.get().scan(
    module=scoring,
    component_type="business_engine",
    engine_type="scoring",
    tags=("scoring", "quality")
)

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]