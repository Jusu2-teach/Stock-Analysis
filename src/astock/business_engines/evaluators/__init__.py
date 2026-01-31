"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 因果贝叶斯网络 + 状态机评估引擎
═══════════════════════════════════════════════════════════════════════════════

公开 API 导出

设计哲学：
- 因果推断（Pearl do-calculus）替代简单规则
- 状态机（HMM）建模公司生命周期
- Copula 处理证据相关性
- Dempster-Shafer 融合不确定性证据
- 时间衰减使近期数据权重更高
- 自适应阈值根据行业/规模动态调整

使用示例：
    >>> from src.astock.business_engines.evaluators import (
    ...     CausalBayesianEvaluator,
    ...     EvaluatorConfig,
    ...     evaluate_single_company,
    ...     run_causal_bayesian_evaluator
    ... )
    >>>
    >>> # 方式1：使用主引擎
    >>> evaluator = CausalBayesianEvaluator()
    >>> result = evaluator.evaluate_company(
    ...     ts_code="000001.SZ",
    ...     trend_data=aggregated_trends,
    ...     company_info={"name": "平安银行", "industry": "银行"}
    ... )
    >>>
    >>> # 方式2：Pipeline 集成
    >>> results = run_causal_bayesian_evaluator(
    ...     aggregated_trends=aggregated_trends,
    ...     company_list=companies
    ... )

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

__version__ = "2.0.0"
__author__ = "AStock Team"

# ═══════════════════════════════════════════════════════════════════════════════
# 主引擎
# ═══════════════════════════════════════════════════════════════════════════════

from .engine import (
    # 主类
    CausalBayesianEvaluator,
    EvaluatorConfig,
    CompanyEvaluation,

    # Pipeline 入口
    run_causal_bayesian_evaluator,

    # 便捷函数
    evaluate_single_company,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 时间衰减
# ═══════════════════════════════════════════════════════════════════════════════

from .temporal import (
    TemporalDecay,
    TemporalConfig,
    TemporalWeights,
    create_decay,
    DEFAULT_DECAY,
    AGGRESSIVE_DECAY,
    CONSERVATIVE_DECAY,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 自适应阈值
# ═══════════════════════════════════════════════════════════════════════════════

from .adaptive_threshold import (
    AdaptiveThresholdEngine,
    AdaptiveContext,
    ThresholdSet,
    IndustryCategory,
    SizeTier,
    MarketCycle,
    get_default_engine as get_threshold_engine,
    adaptive_evaluate,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 因果推断
# ═══════════════════════════════════════════════════════════════════════════════

from .causal_graph import (
    CausalGraph,
    CausalNode,
    CausalEdge,
    CausalEffect,
    CausalDiagnosis,
    EffectType,
    create_financial_causal_graph,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 状态机
# ═══════════════════════════════════════════════════════════════════════════════

from .state_machine import (
    CompanyStateMachine,
    CompanyState,
    StateProfile,
    StateInference,
    QualityClass,
    get_default_state_machine,
    infer_company_state,
)

# ═══════════════════════════════════════════════════════════════════════════════
# Copula 融合
# ═══════════════════════════════════════════════════════════════════════════════

from .copula_fusion import (
    Evidence,
    CopulaEvidenceFusion,
    CopulaFusionResult,
    GaussianCopula,
    get_default_fusion,
    fuse_evidences,
)

# ═══════════════════════════════════════════════════════════════════════════════
# Dempster-Shafer 证据理论
# ═══════════════════════════════════════════════════════════════════════════════

from .dempster_shafer import (
    MassFunction,
    DempsterShaferCombiner,
    CombinationResult,
    DSEvidenceEvaluator,
    DSEvaluationResult,
    make_hypothesis_set,
    quick_ds_evaluate,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 解释生成
# ═══════════════════════════════════════════════════════════════════════════════

from .explanation import (
    DecisionExplainer,
    DecisionType,
    Factor,
    ExplanationResult,
    ExplanationLevel,
    quick_explain,
)

# ═══════════════════════════════════════════════════════════════════════════════
# 导出列表
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    # 版本
    "__version__",
    "__author__",

    # 主引擎
    "CausalBayesianEvaluator",
    "EvaluatorConfig",
    "CompanyEvaluation",
    "run_causal_bayesian_evaluator",
    "evaluate_single_company",

    # 时间衰减
    "TemporalDecay",
    "TemporalConfig",
    "TemporalWeights",
    "create_decay",
    "DEFAULT_DECAY",
    "AGGRESSIVE_DECAY",
    "CONSERVATIVE_DECAY",

    # 自适应阈值
    "AdaptiveThresholdEngine",
    "AdaptiveContext",
    "ThresholdSet",
    "IndustryCategory",
    "SizeTier",
    "MarketCycle",
    "get_threshold_engine",
    "adaptive_evaluate",

    # 因果推断
    "CausalGraph",
    "CausalNode",
    "CausalEdge",
    "CausalEffect",
    "CausalDiagnosis",
    "EffectType",
    "create_financial_causal_graph",

    # 状态机
    "CompanyStateMachine",
    "CompanyState",
    "StateProfile",
    "StateInference",
    "QualityClass",
    "get_default_state_machine",
    "infer_company_state",

    # Copula 融合
    "Evidence",
    "CopulaEvidenceFusion",
    "CopulaFusionResult",
    "GaussianCopula",
    "get_default_fusion",
    "fuse_evidences",

    # Dempster-Shafer
    "MassFunction",
    "DempsterShaferCombiner",
    "CombinationResult",
    "DSEvidenceEvaluator",
    "DSEvaluationResult",
    "make_hypothesis_set",
    "quick_ds_evaluate",

    # 解释
    "DecisionExplainer",
    "DecisionType",
    "Factor",
    "ExplanationResult",
    "ExplanationLevel",
    "quick_explain",
]
