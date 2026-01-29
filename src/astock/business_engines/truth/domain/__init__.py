"""T.R.U.T.H. 领域模型 (v3.0).

核心数据结构:
    - 枚举: FactorId (六因子), SolverId (三求解器), TruthSignal, TruthGrade
    - 输入: ProbeInput
    - 输出: FactorResult, SolverResult, DynamicThreshold
    - 最终: TruthProfile, TruthRunResult

版本: 3.0.0
"""

from .models import (
    # 枚举
    FactorId,
    SolverId,
    TruthSignal,
    TruthGrade,
    WarningLevel,
    # 输入
    ProbeInput,
    # 因子输出
    FactorResult,
    # 求解器输出
    DynamicThreshold,
    SolverResult,
    # 预警
    TruthWarning,
    # 最终输出
    TruthProfile,
    TruthRunResult,
)

__all__ = [
    "FactorId",
    "SolverId",
    "TruthSignal",
    "TruthGrade",
    "WarningLevel",
    "ProbeInput",
    "FactorResult",
    "DynamicThreshold",
    "SolverResult",
    "TruthWarning",
    "TruthProfile",
    "TruthRunResult",
]
