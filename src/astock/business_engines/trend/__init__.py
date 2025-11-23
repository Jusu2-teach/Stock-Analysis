"""业务趋势分析子系统（重构版）"""

from .core import (
    TrendAnalyzer,
    MetricProbe,
    MetricProbeContext,
    get_default_metric_probes,
    TrendRuleEngine,
    trend_rule_engine,
    ConfigResolver,
    TrendEvaluator,
    TrendResultCollector,
)
from .models import (
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendSnapshot,
    TrendVector,
    TrendEvaluationResult,
)
from .config import (
    trend_field_schema,
    get_default_config,
    TrendAnalysisConfig,
)

__all__ = [
    "TrendAnalyzer",
    "TrendAnalyzerConfig",
    "TrendSeriesConfig",
    "TrendSnapshot",
    "TrendVector",
    "TrendEvaluationResult",
    "MetricProbe",
    "MetricProbeContext",
    "get_default_metric_probes",
    "TrendRuleEngine",
    "trend_rule_engine",
    "trend_field_schema",
    "get_default_config",
    "TrendAnalysisConfig",
    "ConfigResolver",
    "TrendEvaluator",
    "TrendResultCollector",
]