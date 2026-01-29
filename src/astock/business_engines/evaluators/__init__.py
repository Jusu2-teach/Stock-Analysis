"""
Evaluators Module
=================

评估器模块，提供不同类型的分析评估。

包含：
- threshold/: 阈值评估器（规则驱动）

架构层次：
    ProbeOutputs (统一接口)
        ↓
    ┌─────────────────────────────────┐
    │         Evaluators              │
    ├─────────────────────────────────┤
    │ TrendEvaluator (规则引擎包装)   │
    │   - 规则驱动                    │
    │   - 29+ 业务规则                │
    │   - 4 投资策略                  │
    │   - 输出: Pass/Fail + Score     │
    ├─────────────────────────────────┤
    │ T.R.U.T.H. (truth/ 模块)        │
    │   - 模型驱动                    │
    │   - 六维基因计算                │
    │   - 物理求解器                  │
    │   - 输出: Genome + Report       │
    └─────────────────────────────────┘

使用示例：
    from astock.business_engines.evaluators import TrendEvaluator

    # 创建评估器
    evaluator = TrendEvaluator()

    # 评估
    result = evaluator.evaluate(probe_outputs)
    print(f"Pass: {result.passes}, Score: {result.score}")
"""

from .threshold import (
    # Engine
    RuleEngine,
    TrendEvaluator,
    RuleExecutionSummary,

    # Results
    EvaluationResult,
    RuleResult,
    StrategyResult,

    # Models & Context
    TrendContext,
    TrendMetrics,
    VolatilityMetrics,
    DeteriorationMetrics,
    InflectionMetrics,
    CyclicalMetrics,
    DataQualityMetrics,

    # Enums
    RuleCategory,
    TrendDirection,
    VolatilityRegime,
    CyclePhase,
    DeteriorationSeverity,

    # Config
    RuleConfig,

    # Factory
    get_default_factory,
)

# Orchestrator 入口
from .engine import run_evaluator, run_evaluator_single

__all__ = [
    # 🆕 Orchestrator 入口
    "run_evaluator",
    "run_evaluator_single",

    # 规则引擎主入口
    "RuleEngine",
    "TrendEvaluator",
    "RuleExecutionSummary",

    # 结果类
    "EvaluationResult",
    "RuleResult",
    "StrategyResult",

    # 领域模型
    "TrendContext",
    "TrendMetrics",
    "VolatilityMetrics",
    "DeteriorationMetrics",
    "InflectionMetrics",
    "CyclicalMetrics",
    "DataQualityMetrics",

    # 枚举
    "RuleCategory",
    "TrendDirection",
    "VolatilityRegime",
    "CyclePhase",
    "DeteriorationSeverity",

    # 配置
    "RuleConfig",

    # 工厂
    "get_default_factory",
]
