"""T.R.U.T.H. 核心流水线与组件 (v3.3).

四层架构:
    - Layer 0: TimeDecay - 时序衰减预处理
    - Layer 1: Factors - 六维因子计算 (α/β/γ/δ_fraud/δ_decay/V)
    - Layer 2: Solvers - 物理求解器 (Gravity/Velocity/Structure)
    - Layer 3: Calibration - 校准层

特性:
    - 使用 typing.Protocol 实现鸭子类型 (更 Pythonic)
    - 每个因子/求解器提供 explain() 方法生成人类可读解释
    - 支持依赖注入: 通过工厂注入自定义因子/求解器

版本: 3.3.0
"""

# 协议定义 (Protocol)
from .protocols import (
    FactorProtocol,
    SolverProtocol,
    FactorFactoryProtocol,
    SolverFactoryProtocol,
)

# 六维因子
from .factors import (
    TruthFactor,
    AlphaFactor,
    BetaFactor,
    GammaFactor,
    DeltaFraudFactor,
    DeltaDecayFactor,
    VerificationFactor,
    get_all_factors,
    get_factor_by_id,
)

# 三大求解器
from .solvers import (
    TruthSolver,
    GravitySolver,
    VelocitySolver,
    StructureSolver,
    get_all_solvers,
    get_solver_by_id,
)

# 工厂 (依赖注入)
from .factory import (
    DefaultFactorFactory,
    ConfigurableFactorFactory,
    CustomFactorFactory,
    DefaultSolverFactory,
    ConfigurableSolverFactory,
    CustomSolverFactory,
    TruthComponentFactory,
    create_default_factory,
    create_test_factory,
)

# 四层管道
from .pipeline import (
    TimeDecayProcessor,
    FactorCalculator,
    SolverExecutor,
    CalibrationEngine,
    TruthPipeline,
    create_pipeline,
    process_single,
)

# 特征注册表
from .feature_registry import (
    METADATA_COLUMNS,
    is_metadata_column,
)


__all__ = [
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
    "TimeDecayProcessor",
    "FactorCalculator",
    "SolverExecutor",
    "CalibrationEngine",
    "TruthPipeline",
    "create_pipeline",
    "process_single",
    # 特征注册
    "METADATA_COLUMNS",
    "is_metadata_column",
]
