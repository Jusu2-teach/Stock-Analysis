"""业务趋势分析子系统（重构版）"""

# 核心组件
from .trend_analyzer import TrendAnalyzer
from .metric_probes import MetricProbe, MetricProbeContext, get_default_metric_probes
from .trend_schema import trend_field_schema
from .trend_settings import TrendAnalyzerConfig, TrendSeriesConfig

# 新的合并模块（保持向后兼容）
from . import services  # 服务类（合并 services/）
from . import config    # 配置（合并 config/）
from . import derivers  # 派生器（合并 derivers/）

__all__ = [
    # 核心类
    "TrendAnalyzer",
    "TrendAnalyzerConfig",
    "TrendSeriesConfig",
    # 探针
    "MetricProbe",
    "MetricProbeContext",
    "get_default_metric_probes",
    # 字段
    "trend_field_schema",
    # 模块
    "services",
    "config",
    "derivers",
]