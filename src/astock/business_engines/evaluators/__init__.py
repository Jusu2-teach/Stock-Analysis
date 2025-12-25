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
    │         Evaluators               │
    ├─────────────────────────────────┤
    │ ThresholdEvaluator (本模块)      │
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
    from astock.business_engines.evaluators import ThresholdEvaluator

    # 创建评估器
    evaluator = ThresholdEvaluator.with_default_rules()

    # 评估
    result = evaluator.evaluate(probe_outputs)
    print(f"Pass: {result.passes}, Score: {result.score}")
"""

from .threshold import (
    # Engine
    ThresholdEvaluator,
    ThresholdEvaluatorConfig,
    # Models
    ThresholdEvaluationResult,
    RuleResult,
    RuleCategory,
    ThresholdRule,
    StrategyMatchResult,
    # Context
    EvaluationContext,
    EvaluationContextBuilder,
)

__all__ = [
    # Threshold Evaluator
    "ThresholdEvaluator",
    "ThresholdEvaluatorConfig",
    "ThresholdEvaluationResult",
    "RuleResult",
    "RuleCategory",
    "ThresholdRule",
    "StrategyMatchResult",
    "EvaluationContext",
    "EvaluationContextBuilder",
]
