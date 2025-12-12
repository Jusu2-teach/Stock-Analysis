"""
T.R.U.T.H. System - Gene Computation Module
===========================================

六维基因计算模块，每个基因独立文件：

- alpha.py: 周期性基因 α（周期股识别）
- beta.py: 资本密度基因 β（轻重资产区分）
- gamma.py: 成长动能基因 γ（成长性评估）
- delta_fraud.py: 欺诈熵基因 δ_fraud（财务造假风险）
- delta_decay.py: 衰退熵基因 δ_decay（恶化趋势检测）
- verification.py: 真相验证基因 V（现金流验证）

使用方法：
```python
from .genes import compute_genome_from_probes, AlphaGene, BetaGene, ...
```
"""

from .alpha import AlphaGene, compute_alpha_from_probes
from .beta import BetaGene, compute_beta_from_probes
from .gamma import GammaGene, compute_gamma_from_probes
from .delta_fraud import DeltaFraudGene, compute_delta_fraud_from_probes
from .delta_decay import DeltaDecayGene, compute_delta_decay_from_probes
from .verification import VerificationGene, compute_verification_from_probes
from .genome_assembler import compute_genome_from_probes

__all__ = [
    # 基因类
    "AlphaGene",
    "BetaGene",
    "GammaGene",
    "DeltaFraudGene",
    "DeltaDecayGene",
    "VerificationGene",

    # 计算函数
    "compute_alpha_from_probes",
    "compute_beta_from_probes",
    "compute_gamma_from_probes",
    "compute_delta_fraud_from_probes",
    "compute_delta_decay_from_probes",
    "compute_verification_from_probes",
    "compute_genome_from_probes",
]
