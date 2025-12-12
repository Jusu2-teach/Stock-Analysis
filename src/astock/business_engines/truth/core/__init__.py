"""
T.R.U.T.H. System - Core Module
===============================

核心计算模块，包含六维基因和三大求解器。

结构：
├── genes/        # 六维基因计算
│   ├── alpha.py           # 周期性基因 α
│   ├── beta.py            # 资本密度基因 β
│   ├── gamma.py           # 成长动能基因 γ
│   ├── delta_fraud.py     # 欺诈熵基因 δ_fraud
│   ├── delta_decay.py     # 衰退熵基因 δ_decay
│   ├── verification.py    # 验证因子 V
│   └── genome_assembler.py # 基因组装器
│
└── solvers/      # 三大求解器
    ├── gravity_solver.py   # 重力求解器 G
    ├── velocity_solver.py  # 速度求解器 V
    └── structure_solver.py # 结构求解器 S

作者: AStock Analysis System
日期: 2025-01
"""

# ============================================================================
# 基因模块
# ============================================================================
from .genes import (
    # 基因类
    AlphaGene,
    BetaGene,
    GammaGene,
    DeltaFraudGene,
    DeltaDecayGene,
    VerificationGene,

    # 计算函数
    compute_alpha_from_probes,
    compute_beta_from_probes,
    compute_gamma_from_probes,
    compute_delta_fraud_from_probes,
    compute_delta_decay_from_probes,
    compute_verification_from_probes,
    compute_genome_from_probes,
)

# ============================================================================
# 求解器模块
# ============================================================================
from .solvers import (
    # 求解器函数
    gravity_solver,
    velocity_solver,
    structure_solver,

    # 工厂函数
    create_gravity_result,
    create_velocity_result,
    create_structure_result,

    # 结果类
    GravitySolverResult,
    VelocitySolverResult,
    StructureSolverResult,
)

__all__ = [
    # 基因类
    "AlphaGene",
    "BetaGene",
    "GammaGene",
    "DeltaFraudGene",
    "DeltaDecayGene",
    "VerificationGene",

    # 基因计算
    "compute_alpha_from_probes",
    "compute_beta_from_probes",
    "compute_gamma_from_probes",
    "compute_delta_fraud_from_probes",
    "compute_delta_decay_from_probes",
    "compute_verification_from_probes",
    "compute_genome_from_probes",

    # 求解器
    "gravity_solver",
    "velocity_solver",
    "structure_solver",

    # 工厂函数
    "create_gravity_result",
    "create_velocity_result",
    "create_structure_result",

    # 求解结果
    "GravitySolverResult",
    "VelocitySolverResult",
    "StructureSolverResult",
]
