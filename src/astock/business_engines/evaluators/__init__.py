"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v3.0 - 精简评估引擎
═══════════════════════════════════════════════════════════════════════════════

v3.0 精简架构，4 个核心模块：

    evaluators/
    ├── engine.py              # 主入口 (run_causal_bayesian_evaluator)
    ├── adaptive_threshold.py  # 自适应阈值
    ├── explanation.py         # 可解释 AI
    ├── rule_engine.py         # YAML 声明式规则引擎
    └── config/                # 规则 + 阈值配置

使用示例:
    >>> from src.astock.business_engines.evaluators import run_causal_bayesian_evaluator
    >>> results = run_causal_bayesian_evaluator(aggregated_trends)

版本: 3.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

__version__ = "3.0.0"
__author__ = "AStock Team"

# 主入口
from .engine import (
    run_causal_bayesian_evaluator,
    CausalBayesianEvaluator,
    EvaluatorConfig,
    PDDAColumns,
)

# 核心组件
from .adaptive_threshold import AdaptiveThresholdEngine, AdaptiveContext
from .explanation import DecisionExplainer, DecisionType, Factor
from .rule_engine import RuleEngine, RuleEngineResult, get_default_rule_engine

__all__ = [
    # 主入口
    "run_causal_bayesian_evaluator",
    "CausalBayesianEvaluator",
    "EvaluatorConfig",
    "PDDAColumns",
    # 自适应阈值
    "AdaptiveThresholdEngine",
    "AdaptiveContext",
    # 规则引擎
    "RuleEngine",
    "RuleEngineResult",
    "get_default_rule_engine",
    # 可解释 AI
    "DecisionExplainer",
    "DecisionType",
    "Factor",
]
