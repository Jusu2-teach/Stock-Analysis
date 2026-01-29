"""T.R.U.T.H. 子系统统一入口 (v3.3).

四层架构:
    - ``truth.domain``: 领域模型 (Probe/Factor/Solver/Profile 等)
    - ``truth.config``: TRUTH 运行配置 (四层配置系统)
    - ``truth.core``: 因子 + 求解器 + 四层管道 + 工厂 + 协议
    - ``truth.integration``: orchestrator/pipeline 集成入口

六维因子: α(周期性), β(资本密度), γ(成长), δ_fraud(欺诈熵), δ_decay(衰退熵), V(验证)
三大求解器: Gravity(ROIC阈值), Velocity(增长边界), Structure(护城河)

v3.3 新特性:
    - typing.Protocol 替代 ABC (鸭子类型)
    - explain() 方法生成人类可读解释
    - 依赖注入: 工厂模式支持自定义因子/求解器

版本: 3.3.0
"""

# 领域模型
from .domain import (
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
    TruthRunResult,
)

# 配置
from .config import (
    TruthConfig,
    TimeDecayConfig,
    AlphaFactorConfig,
    BetaFactorConfig,
    GammaFactorConfig,
    DeltaFraudFactorConfig,
    DeltaDecayFactorConfig,
    VerificationFactorConfig,
    GravitySolverConfig,
    VelocitySolverConfig,
    StructureSolverConfig,
    CalibrationConfig,
    ScoringConfig,
    get_default_config,
    get_conservative_config,
    get_growth_focused_config,
)

# 核心组件
from .core import (
    # 因子
    TruthFactor,
    AlphaFactor,
    BetaFactor,
    GammaFactor,
    DeltaFraudFactor,
    DeltaDecayFactor,
    VerificationFactor,
    get_all_factors,
    get_factor_by_id,
    # 求解器
    TruthSolver,
    GravitySolver,
    VelocitySolver,
    StructureSolver,
    get_all_solvers,
    get_solver_by_id,
    # 管道
    TruthPipeline,
    create_pipeline,
    process_single,
)


__all__ = [
    # 枚举
    "FactorId",
    "SolverId",
    "TruthSignal",
    "TruthGrade",
    "WarningLevel",
    # 输入
    "ProbeInput",
    # 输出
    "FactorResult",
    "DynamicThreshold",
    "SolverResult",
    "TruthWarning",
    "TruthProfile",
    "TruthRunResult",
    # 配置
    "TruthConfig",
    "TimeDecayConfig",
    "get_default_config",
    "get_conservative_config",
    "get_growth_focused_config",
    # 因子
    "TruthFactor",
    "AlphaFactor",
    "BetaFactor",
    "GammaFactor",
    "DeltaFraudFactor",
    "DeltaDecayFactor",
    "VerificationFactor",
    "get_all_factors",
    "get_factor_by_id",
    # 求解器
    "TruthSolver",
    "GravitySolver",
    "VelocitySolver",
    "StructureSolver",
    "get_all_solvers",
    "get_solver_by_id",
    # 管道
    "TruthPipeline",
    "create_pipeline",
    "process_single",
]


