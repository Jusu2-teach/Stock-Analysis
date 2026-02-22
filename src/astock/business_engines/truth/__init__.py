"""T.R.U.T.H. v3.3 - 六维基因测序 × 三大物理求解器

扁平架构:
    truth/
    ├── engine.py       # 主入口 (run_truth)
    ├── factors.py      # 6 因子 (α/β/γ/δ_fraud/δ_decay/V)
    ├── solvers.py      # 3 求解器 (Gravity/Velocity/Structure)
    ├── models.py       # 领域模型
    └── config.py       # 配置

版本: 3.3.0
"""

# 主入口
from .engine import run_truth

# 领域模型
from .models import (
    FactorId,
    SolverId,
    TruthSignal,
    TruthGrade,
    WarningLevel,
    ProbeInput,
    FactorResult,
    DynamicThreshold,
    SolverResult,
    TruthWarning,
    TruthProfile,
)

# 因子
from .factors import (
    AlphaFactor,
    BetaFactor,
    GammaFactor,
    DeltaFraudFactor,
    DeltaDecayFactor,
    VerificationFactor,
)

# 求解器
from .solvers import (
    GravitySolver,
    VelocitySolver,
    StructureSolver,
)

# 配置
from .config import (
    TruthConfig,
    get_default_config,
    get_conservative_config,
    get_growth_focused_config,
)

__all__ = [
    # 主入口
    "run_truth",
    # 枚举
    "FactorId",
    "SolverId",
    "TruthSignal",
    "TruthGrade",
    "WarningLevel",
    # 数据模型
    "ProbeInput",
    "FactorResult",
    "DynamicThreshold",
    "SolverResult",
    "TruthWarning",
    "TruthProfile",
    # 因子
    "AlphaFactor",
    "BetaFactor",
    "GammaFactor",
    "DeltaFraudFactor",
    "DeltaDecayFactor",
    "VerificationFactor",
    # 求解器
    "GravitySolver",
    "VelocitySolver",
    "StructureSolver",
    # 配置
    "TruthConfig",
    "get_default_config",
    "get_conservative_config",
    "get_growth_focused_config",
]
