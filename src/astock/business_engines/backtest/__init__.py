"""
Backtest Module — 滚动窗口基本面质量持续性回测

核心验证: 系统的评分是否具有预测能力？

用法:
    python -m src.astock.business_engines.backtest           # 完整回测
    python -m src.astock.business_engines.backtest --fast     # 快速模式 (3年训练)
"""

from .engine import (
    FundamentalBacktester,
    EvaluatorBacktester,
    BacktestReport,
    WindowResult,
    compute_piotroski_fscore,
    compute_beneish_mscore,
    compute_consensus_meta_score,
    detect_ic_decay,
    compute_ic_optimal_weights,
    diagnose_factor_orthogonality,
    analyze_quality_stability,
)

__all__ = [
    "FundamentalBacktester",
    "EvaluatorBacktester",
    "BacktestReport",
    "WindowResult",
    "compute_piotroski_fscore",
    "compute_beneish_mscore",
    "compute_consensus_meta_score",
    "detect_ic_decay",
    "compute_ic_optimal_weights",
    "diagnose_factor_orthogonality",
    "analyze_quality_stability",
]
