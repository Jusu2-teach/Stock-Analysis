"""
规则模块 (Rules Module)
========================

按职责分类的规则集合：
- veto: 一票否决规则 (6个)
- penalty: 扣分规则 (8个)
- bonus: 加分规则 (5个)
- validation: 交叉验证规则 (4个)

总计 23 个规则，从原 36 个规则精简整合而来。
"""

from .veto import (
    rule_min_latest_value_veto,
    rule_severe_trend_decline_veto,
    rule_severe_deterioration_veto,
    rule_peak_decline_veto,
    rule_cumulative_collapse_veto,
    rule_roiic_capital_destruction_veto,
    VETO_RULES,
)

from .penalty import (
    rule_mild_decline_penalty,
    rule_deterioration_penalty,
    rule_volatility_penalty,
    rule_relative_decline_penalty,
    rule_single_year_decline_penalty,
    rule_consecutive_decline_penalty,
    rule_roiic_negative_penalty,
    rule_roiic_divergence_penalty,
    PENALTY_RULES,
)

from .bonus import (
    rule_growth_momentum_bonus,
    rule_inflection_recovery_bonus,
    rule_mean_reversion_bonus,
    rule_cyclical_position_bonus,
    rule_roiic_positive_bonus,
    BONUS_RULES,
)

from .validation import (
    rule_earnings_quality_check,
    rule_dupont_consistency_check,
    rule_fcf_quality_check,
    rule_sustainable_growth_check,
    VALIDATION_RULES,
)

from .base import RuleResult, Rule, TrendContext, RuleConfig

# 分类导出
ALL_VETO_RULES = VETO_RULES
ALL_PENALTY_RULES = PENALTY_RULES
ALL_BONUS_RULES = BONUS_RULES
ALL_VALIDATION_RULES = VALIDATION_RULES

# 完整规则链 (按执行顺序)
ALL_RULES = VETO_RULES + PENALTY_RULES + BONUS_RULES + VALIDATION_RULES


__all__ = [
    # 否决规则
    'rule_min_latest_value_veto',
    'rule_severe_trend_decline_veto',
    'rule_severe_deterioration_veto',
    'rule_peak_decline_veto',
    'rule_cumulative_collapse_veto',
    'rule_roiic_capital_destruction_veto',
    'VETO_RULES',
    'ALL_VETO_RULES',

    # 扣分规则
    'rule_mild_decline_penalty',
    'rule_deterioration_penalty',
    'rule_volatility_penalty',
    'rule_relative_decline_penalty',
    'rule_single_year_decline_penalty',
    'rule_consecutive_decline_penalty',
    'rule_roiic_negative_penalty',
    'rule_roiic_divergence_penalty',
    'PENALTY_RULES',
    'ALL_PENALTY_RULES',

    # 加分规则
    'rule_growth_momentum_bonus',
    'rule_inflection_recovery_bonus',
    'rule_mean_reversion_bonus',
    'rule_cyclical_position_bonus',
    'rule_roiic_positive_bonus',
    'BONUS_RULES',
    'ALL_BONUS_RULES',

    # 交叉验证规则
    'rule_earnings_quality_check',
    'rule_dupont_consistency_check',
    'rule_fcf_quality_check',
    'rule_sustainable_growth_check',
    'VALIDATION_RULES',
    'ALL_VALIDATION_RULES',

    # 全部规则
    'ALL_RULES',

    # 基础类型
    'RuleResult',
    'Rule',
    'TrendContext',
    'RuleConfig',
]
