"""
规则模块 (Rules Module)
==============================

按职责分类的规则集合（全部类化）：
- veto: 一票否决规则 (6个)
- penalty: 扣分规则 (8个)
- bonus: 加分规则 (5个)
- validation: 交叉验证规则 (4个)

总计 23 个规则类，DDD + Protocol-based 架构。
"""

# 导入所有规则模块（用于工厂自动发现）
from . import veto
from . import penalty
from . import bonus
from . import validation

# 导出规则类
from .veto import (
    MinLatestValueVetoRule,
    SevereTrendDeclineVetoRule,
    SevereDeteriorationVetoRule,
    PeakDeclineVetoRule,
    CumulativeCollapseVetoRule,
    ROIICCapitalDestructionVetoRule,
)

from .penalty import (
    MildDeclinePenaltyRule,
    DeteriorationPenaltyRule,
    VolatilityPenaltyRule,
    RelativeDeclinePenaltyRule,
    SingleYearDeclinePenaltyRule,
    ConsecutiveDeclinePenaltyRule,
    ROIICNegativePenaltyRule,
    ROIICDivergencePenaltyRule,
)

from .bonus import (
    GrowthMomentumBonusRule,
    InflectionRecoveryBonusRule,
    MeanReversionBonusRule,
    CyclicalPositionBonusRule,
    ROIICPositiveBonusRule,
)

from .validation import (
    EarningsQualityCheckRule,
    DupontConsistencyCheckRule,
    FCFQualityCheckRule,
    SustainableGrowthCheckRule,
)

__all__ = [
    # 模块
    'veto',
    'penalty',
    'bonus',
    'validation',
    # 否决规则类
    'MinLatestValueVetoRule',
    'SevereTrendDeclineVetoRule',
    'SevereDeteriorationVetoRule',
    'PeakDeclineVetoRule',
    'CumulativeCollapseVetoRule',
    'ROIICCapitalDestructionVetoRule',  # 正确的类名
    # 扣分规则类
    'MildDeclinePenaltyRule',
    'DeteriorationPenaltyRule',
    'VolatilityPenaltyRule',
    'RelativeDeclinePenaltyRule',
    'SingleYearDeclinePenaltyRule',
    'ConsecutiveDeclinePenaltyRule',
    'ROIICNegativePenaltyRule',
    'ROIICDivergencePenaltyRule',
    # 加分规则类
    'GrowthMomentumBonusRule',
    'InflectionRecoveryBonusRule',
    'MeanReversionBonusRule',
    'CyclicalPositionBonusRule',
    'ROIICPositiveBonusRule',
    # 验证规则类
    'EarningsQualityCheckRule',
    'DupontConsistencyCheckRule',
    'FCFQualityCheckRule',
    'SustainableGrowthCheckRule',
]
