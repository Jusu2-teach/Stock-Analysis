"""
业务引擎模块（重构版 v3.0）
========================

提供业务分析功能：
- trend: 趋势分析探针（纯数学计算）
- evaluators/threshold: 阈值评估（规则驱动）
- reporters: 报告生成（综合报告 + T.R.U.T.H. 报告）
- truth: T.R.U.T.H. 计算引擎（六大基因+三大求解器）
- analysis: 通用分析 (DuckDB Engine)
- core/probe_engine: 统一探针接口（ProbeOutputs 构建）

架构说明：
    trend/probes (数学计算) → ProbeOutputs → truth (基因+求解器)
"""

from orchestrator import Registry
from .reporters import engine as reporting_engine
from .analysis import engine as analysis_engine
from .trend import engine as trend_engine
from .truth import truth_engine  # T.R.U.T.H. 处理引擎

# Scan Analysis (General)
Registry.get().scan(
    module=analysis_engine,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "analysis", "general")
)

# Scan Trend Analysis (趋势分析)
Registry.get().scan(
    module=trend_engine,
    component_type="business_engine",
    engine_type="duckdb",
    tags=("duckdb", "trend", "analysis")
)

# Scan Reporting (包含规则驱动和T.R.U.T.H.数据驱动两套系统)
Registry.get().scan(
    module=reporting_engine,
    component_type="business_engine",
    engine_type="reporting",
    tags=("reporting", "comprehensive", "truth", "data-driven")
)

# Scan T.R.U.T.H. Engine (六大基因+三大求解器)
Registry.get().scan(
    module=truth_engine,
    component_type="business_engine",
    engine_type="truth",
    tags=("truth", "genes", "solvers")
)

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]