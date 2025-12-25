"""
Genome Assembler - 基因组装器
============================

将六个独立的基因计算组合成完整的 CompanyGenome。

这是 genes/ 模块的核心入口，调用各个基因计算函数并组装结果。

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Optional
import logging

from ...adapter import GenomeInput
from ...config import TruthConfig, get_default_truth_config
from ...models import CompanyGenome

from .alpha import compute_alpha_from_probes
from .beta import compute_beta_from_probes
from .gamma import compute_gamma_from_probes
from .delta_fraud import compute_delta_fraud_from_probes
from .delta_decay import compute_delta_decay_from_probes
from .verification import compute_verification_from_probes

logger = logging.getLogger(__name__)


def compute_genome_from_probes(
    genome_input: GenomeInput,
    config: TruthConfig = None,
) -> CompanyGenome:
    """
    从探针输入计算完整的六维基因组

    这是 T.R.U.T.H. 系统的核心入口：
    1. 接收 ProbeAdapter 转换后的 GenomeInput
    2. 调用各基因计算函数
    3. 返回完整的 CompanyGenome

    Args:
        genome_input: 从探针适配器获取的标准化输入
        config: T.R.U.T.H. 配置，None则使用默认配置

    Returns:
        CompanyGenome: 完整的六维基因组
    """
    if config is None:
        config = get_default_truth_config()

    # 计算各基因
    alpha, alpha_breakdown = compute_alpha_from_probes(genome_input.alpha, config)
    beta, beta_breakdown = compute_beta_from_probes(genome_input.beta, config)
    gamma, gamma_breakdown = compute_gamma_from_probes(genome_input.gamma, config)
    delta_fraud, fraud_result = compute_delta_fraud_from_probes(genome_input.delta_fraud, config)
    delta_decay, decay_breakdown = compute_delta_decay_from_probes(genome_input.delta_decay, config)
    v_factor, v_breakdown = compute_verification_from_probes(genome_input.v_factor, config)

    # 计算数据质量分数
    degradation_count = sum([
        1 if genome_input.alpha.is_degraded else 0,
        1 if genome_input.beta.is_degraded else 0,
        1 if genome_input.gamma.is_degraded else 0,
        1 if genome_input.delta_fraud.is_degraded else 0,
        1 if genome_input.delta_decay.is_degraded else 0,
        1 if genome_input.v_factor.is_degraded else 0,
    ])
    data_quality_score = 1.0 - (degradation_count * 0.1)

    # 组装基因组
    genome = CompanyGenome(
        ts_code=genome_input.company_code,
        company_name=genome_input.company_name,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        delta_fraud=delta_fraud,
        delta_decay=delta_decay,
        verification=v_factor,
        data_years=genome_input.data_years,
        data_quality_score=data_quality_score,
    )

    # 检查降级情况（仅调试级别，避免大量重复警告）
    degradations = genome_input.get_degradation_summary()
    if degradations:
        logger.debug(f"公司 {genome_input.company_code} 存在降级计算: {degradations}")

    return genome
