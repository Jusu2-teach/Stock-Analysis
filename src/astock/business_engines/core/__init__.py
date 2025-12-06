"""
核心模块 (Core Module)
=====================

提供业务引擎的核心基础设施：
- interfaces: 分析引擎接口定义
- duckdb_utils: DuckDB工具函数
- config_loader: 配置加载器

作者: AStock Analysis System
日期: 2025-12-06
"""

from .interfaces import AnalysisResult, ScoreResult, IAnalyzer, IScorer, IReporter
from .duckdb_utils import _q, _get_duckdb_module, _init_duckdb_and_source
from .config_loader import (
    ConfigLoader,
    EngineConfig,
    load_yaml_config,
    merge_configs,
    get_engine_config,
    DEFAULT_ENGINE_CONFIG,
)

__all__ = [
    # 接口
    "AnalysisResult",
    "ScoreResult",
    "IAnalyzer",
    "IScorer",
    "IReporter",
    # DuckDB工具
    "_q",
    "_get_duckdb_module",
    "_init_duckdb_and_source",
    # 配置
    "ConfigLoader",
    "EngineConfig",
    "load_yaml_config",
    "merge_configs",
    "get_engine_config",
    "DEFAULT_ENGINE_CONFIG",
]
