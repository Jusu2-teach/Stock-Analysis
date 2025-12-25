"""
T.R.U.T.H. System - Threshold Rendering Using True History
==========================================================

基于六维基因测序和物理求解器的动态阈值系统。

与 analyzers 同级，负责聚合分析结果并生成最终评估。

目录结构：
truth/
├── __init__.py           # 主模块导出
├── config.py             # 配置
├── models.py             # 数据模型
├── engine.py             # 主引擎
├── adapter.py            # 探针适配器
├── visualizer.py         # 可视化
├── clusterer.py          # 聚类器
├── calibrator.py         # 校准器
└── core/                 # 核心计算
    ├── genes/            # 六维基因
    └── solvers/          # 三大求解器

设计哲学：
"让数据自己说话，而不是让分析师替数据说话"

使用方式：
```python
from business_engines.truth import TruthEngine, compute_genome_from_probes
from business_engines.truth.core import gravity_solver, velocity_solver
```
"""

# ============================================================================
# 数据模型
# ============================================================================
from .models import (
    CompanyGenome,
    TruthResult,
    ClusterProfile,
    CalibrationResult,
    FraudCheckResult,
    RepresentativeMetrics,
    ThresholdResult,
    GrowthBoundResult,
    SlopeResult,
    SignalType,
    GradeLevel,
    BatchResult,
)

# ============================================================================
# 配置
# ============================================================================
from .config import TruthConfig, get_default_truth_config

# ============================================================================
# 主引擎
# ============================================================================
from .engine import TruthEngine

# ============================================================================
# 探针适配器
# ============================================================================
from .adapter import (
    ProbeAdapter,
    ProbeOutputs,
    MultiIndicatorProbeOutputs,
    GenomeInput,
    AlphaGeneInput,
    BetaGeneInput,
    GammaGeneInput,
    DeltaFraudInput,
    DeltaDecayInput,
    VFactorInput,
    create_probe_outputs_from_dict,
)

# ============================================================================
# 核心计算（genes + solvers）
# ============================================================================
from .core import (
    # 基因计算
    compute_genome_from_probes,
    compute_alpha_from_probes,
    compute_beta_from_probes,
    compute_gamma_from_probes,
    compute_delta_fraud_from_probes,
    compute_delta_decay_from_probes,
    compute_verification_from_probes,
    AlphaGene,
    BetaGene,
    GammaGene,
    DeltaFraudGene,
    DeltaDecayGene,
    VerificationGene,
    # 求解器
    gravity_solver,
    velocity_solver,
    structure_solver,
    GravitySolverResult,
    VelocitySolverResult,
    StructureSolverResult,
)

# ============================================================================
# 聚类器和校准器
# ============================================================================
from .clusterer import GenomeClusterer
from .calibrator import AdaptiveCalibrator

# ============================================================================
# 处理器（专业基因-指标映射）
# ============================================================================
from .processor import (
    TruthProcessor,
    TruthProcessResult,
    BatchProcessResult,
    GeneExtractionResult,
    SolverExecutionResult,
    CausalValidation,
    ProfessionalGeneMapper,
    CausalNetworkValidator,
)

# ============================================================================
# 可视化
# ============================================================================
from .visualizer import (
    GenomeVisualization,
    plot_genome_radar,
    generate_genome_interpretation,
    export_genome_section_markdown,
    export_genome_table_markdown,
    batch_generate_visualizations,
)

__all__ = [
    # Models
    "CompanyGenome",
    "TruthResult",
    "ClusterProfile",
    "CalibrationResult",
    "FraudCheckResult",
    "RepresentativeMetrics",
    "ThresholdResult",
    "GrowthBoundResult",
    "SlopeResult",
    "SignalType",
    "GradeLevel",
    "BatchResult",
    # Config
    "TruthConfig",
    "get_default_truth_config",
    # Engine
    "TruthEngine",
    # Adapter
    "ProbeAdapter",
    "ProbeOutputs",
    "MultiIndicatorProbeOutputs",
    "GenomeInput",
    "AlphaGeneInput",
    "BetaGeneInput",
    "GammaGeneInput",
    "DeltaFraudInput",
    "DeltaDecayInput",
    "VFactorInput",
    "create_probe_outputs_from_dict",
    # Core - Genes
    "compute_genome_from_probes",
    "compute_alpha_from_probes",
    "compute_beta_from_probes",
    "compute_gamma_from_probes",
    "compute_delta_fraud_from_probes",
    "compute_delta_decay_from_probes",
    "compute_verification_from_probes",
    "AlphaGene",
    "BetaGene",
    "GammaGene",
    "DeltaFraudGene",
    "DeltaDecayGene",
    "VerificationGene",
    # Core - Solvers
    "gravity_solver",
    "velocity_solver",
    "structure_solver",
    "GravitySolverResult",
    "VelocitySolverResult",
    "StructureSolverResult",
    # Clustering & Calibration
    "GenomeClusterer",
    "AdaptiveCalibrator",
    # Processor (Professional Gene-Indicator Mapping)
    "TruthProcessor",
    "TruthProcessResult",
    "BatchProcessResult",
    "GeneExtractionResult",
    "SolverExecutionResult",
    "CausalValidation",
    "ProfessionalGeneMapper",
    "CausalNetworkValidator",
    # Visualization
    "GenomeVisualization",
    "plot_genome_radar",
    "generate_genome_interpretation",
    "export_genome_section_markdown",
    "export_genome_table_markdown",
    "batch_generate_visualizations",
]

__version__ = "2.0.0"
