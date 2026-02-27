"""
核心模块 (Core Module)
=====================

提供业务引擎的核心基础设施：
- duckdb_utils: DuckDB工具函数
- probe_engine: 探针引擎（纯数学计算层）

架构层次：
    Raw Data
        ↓
    UnifiedProbeEngine (探针引擎 - 配置驱动)
        ↓
    DynamicProbeOutputs (动态输出容器)
        ↓
    ┌─────────────────┬─────────────────┐
    │ ThresholdEvaluator │   T.R.U.T.H.   │
    │ (规则评估器)        │ (基因计算器)   │
    └─────────────────┴─────────────────┘
"""

from .duckdb_utils import _q, _get_duckdb_module, _init_duckdb_and_source

# 统一探针引擎
from .probe_engine import (
    # Unified Engine
    UnifiedProbeEngine,
    UnifiedProbeAdapter,
    ProbeSpec,
    DynamicProbeOutputs,
    DynamicMultiIndicatorOutputs,
    create_default_engine,
    create_core_engine,
    PROBE_SPECS,
    ProbeCategory,
    list_all_probes,
    # Interface
    ProbeProtocol,
    ProbeResult,
    # Data Models
    ProbeOutputs,
    MultiIndicatorProbeOutputs,
    ProbeOutputBuilder,
    MultiIndicatorProbeOutputBuilder,
)

__all__ = [
    # DuckDB工具
    "_q",
    "_get_duckdb_module",
    "_init_duckdb_and_source",
    # 统一探针引擎
    "UnifiedProbeEngine",
    "UnifiedProbeAdapter",
    "ProbeSpec",
    "DynamicProbeOutputs",
    "DynamicMultiIndicatorOutputs",
    "create_default_engine",
    "create_core_engine",
    "PROBE_SPECS",
    "ProbeCategory",
    "list_all_probes",
    # Interface
    "ProbeProtocol",
    "ProbeResult",
    # Data Models
    "ProbeOutputs",
    "MultiIndicatorProbeOutputs",
    "ProbeOutputBuilder",
    "MultiIndicatorProbeOutputBuilder",
]
