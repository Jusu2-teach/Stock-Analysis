"""
业务引擎模块（重构版 v2）
========================

提供业务分析功能：
- analyzers/trend: 趋势分析 (DuckDB Engine)
- analyzers/quality: 质量分析 (预留)
- analyzers/valuation: 估值分析 (预留)
- scorers: 评分器 (Generic Scorer)
- reporters: 报告生成 (Generic Reporter)
- analysis: 通用分析 (DuckDB Engine)
"""

from orchestrator import Registry
from .analyzers.trend import duckdb_engine as trend_engine
from .scorers import engine as scoring_engine
from .reporters import engine as reporting_engine
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