"""
业务引擎模块（重构版）
===================

提供业务分析功能：
- engines: DuckDB业务方法（已合并简化）
- trend: 趋势分析
- scoring: 质量评分
- reporting: 报告生成
"""

# 导入引擎以触发注册（使用新的 duckdb_core）
from .engines import duckdb_core

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]