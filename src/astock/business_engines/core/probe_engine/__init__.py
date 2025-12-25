"""
Probe Engine - Core Module
==========================

探针引擎核心模块，提供统一的探针执行框架。

此包包含：
- unified.py: UnifiedProbeEngine + DynamicProbeOutputs
- specs.py: 探针规格配置
- interface.py: ProbeProtocol 探针接口协议
- builders.py: ProbeOutputs 数据模型 (用于 truth 模块)
"""

# 统一引擎
from .unified import (
    UnifiedProbeProtocol,
    ProbeSpec,
    UnifiedProbeAdapter,
    UnifiedProbeEngine,
    DynamicProbeOutputs,
    DynamicMultiIndicatorOutputs,
    create_probe_spec,
)

from .specs import (
    create_default_engine,
    create_core_engine,
    PROBE_SPECS,
    ProbeCategory,
    list_all_probes,
    list_probes_by_category,
    get_probe_spec,
)

# 接口协议
from .interface import ProbeProtocol, ProbeResult

# ProbeOutputs 数据模型 (被 truth 模块使用)
from .builders import (
    ProbeOutputs,
    MultiIndicatorProbeOutputs,
    ProbeOutputBuilder,
    MultiIndicatorProbeOutputBuilder,
)

__all__ = [
    # Unified Engine
    "UnifiedProbeProtocol",
    "ProbeSpec",
    "UnifiedProbeAdapter",
    "UnifiedProbeEngine",
    "DynamicProbeOutputs",
    "DynamicMultiIndicatorOutputs",
    "create_probe_spec",
    "create_default_engine",
    "create_core_engine",
    "PROBE_SPECS",
    "ProbeCategory",
    "list_all_probes",
    "list_probes_by_category",
    "get_probe_spec",
    # Interface
    "ProbeProtocol",
    "ProbeResult",
    # Data Models
    "ProbeOutputs",
    "MultiIndicatorProbeOutputs",
    "ProbeOutputBuilder",
    "MultiIndicatorProbeOutputBuilder",
]
