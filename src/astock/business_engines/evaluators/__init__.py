"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 因果贝叶斯网络 + 状态机评估引擎
═══════════════════════════════════════════════════════════════════════════════

扁平化架构，8 个核心模块：

    evaluators/
    ├── engine.py              # 主入口 (run_causal_bayesian_evaluator)
    ├── causal_graph.py        # Pearl 因果推断
    ├── state_machine.py       # HMM 生命周期
    ├── copula_fusion.py       # Copula 相关性建模
    ├── dempster_shafer.py     # Dempster-Shafer 证据融合
    ├── adaptive_threshold.py  # 自适应阈值
    ├── explanation.py         # 可解释 AI
    ├── temporal.py            # 时间衰减 (保留)
    └── config.py              # 配置

使用示例:
    >>> from src.astock.business_engines.evaluators import run_causal_bayesian_evaluator
    >>> results = run_causal_bayesian_evaluator(aggregated_trends)

版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

__version__ = "2.0.0"
__author__ = "AStock Team"

# 主入口
from .engine import (
    run_causal_bayesian_evaluator,
    CausalBayesianEvaluator,
    EvaluatorConfig,
    PDDAColumns,
)

# 核心组件 (按需导入)
from .causal_graph import CausalGraph, create_financial_causal_graph
from .state_machine import CompanyStateMachine, CompanyState, StateInference
from .copula_fusion import CopulaEvidenceFusion, Evidence
from .dempster_shafer import DSEvidenceEvaluator, DSEvaluationResult
from .adaptive_threshold import AdaptiveThresholdEngine, AdaptiveContext
from .explanation import DecisionExplainer, DecisionType, Factor
from .rule_engine import RuleEngine, RuleEngineResult, get_default_rule_engine

__all__ = [
    # 主入口
    "run_causal_bayesian_evaluator",
    "CausalBayesianEvaluator",
    "EvaluatorConfig",
    "PDDAColumns",
    # 因果推断
    "CausalGraph",
    "create_financial_causal_graph",
    # 状态机
    "CompanyStateMachine",
    "CompanyState",
    "StateInference",
    # Copula 融合
    "CopulaEvidenceFusion",
    "Evidence",
    # DS 证据
    "DSEvidenceEvaluator",
    "DSEvaluationResult",
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
