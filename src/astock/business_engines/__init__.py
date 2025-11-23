"""
业务引擎模块（重构版）
===================

提供业务分析功能：
- trend: 趋势分析 (DuckDB Engine)
- scoring: 质量评分 (Generic Scorer)
- reporting: 报告生成 (Generic Reporter)
"""

from orchestrator import Registry
from .trend import duckdb_engine as trend_engine
from .scoring import engine as scoring_engine
from .reporting import engine as reporting_engine
from .analysis import duckdb_engine as analysis_engine

# Scan Analysis (General)
Registry.get().scan(
    module=analysis_engine,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "analysis", "general")
)

# Scan DuckDB Trend
Registry.get().scan(
    module=trend_engine,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "trend")
)

# Scan Scoring
Registry.get().scan(
    module=scoring_engine,
    component_type="business_engine",
    engine_type="scoring",
    tags=("scoring", "quality")
)

# Scan Reporting
Registry.get().scan(
    module=reporting_engine,
    component_type="business_engine",
    engine_type="reporting",
    tags=("reporting", "generic")
)

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]