"""
T.R.U.T.H. System - Adaptive Calibrator
========================================

自适应校准器，实现双层校准：
1. 聚类残差校准 - 基于同类公司的实际表现
2. 规模残差校准 - 基于市值分层的系统偏差

设计原则：
1. 完全数据驱动，无先验假设
2. 残差学习有阻尼（避免过拟合）
3. 置信度折扣（低质量数据降权）
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
import logging

from .models import (
    CompanyGenome,
    TruthResult,
    ThresholdResult,
    CalibrationResult,
    ClusterProfile,
)
from .config import TruthConfig, get_default_truth_config
from .clusterer import GenomeClusterer

logger = logging.getLogger(__name__)


# ============================================================================
# 规模分层定义
# ============================================================================

SIZE_BUCKET_NAMES = ['micro', 'small', 'mid', 'large', 'mega']

SIZE_BUCKET_DESCRIPTIONS = {
    'micro': '微型 (0-10%)',
    'small': '小型 (10-30%)',
    'mid': '中型 (30-60%)',
    'large': '大型 (60-85%)',
    'mega': '巨型 (85-100%)',
}


# ============================================================================
# 校准器
# ============================================================================

class AdaptiveCalibrator:
    """
    自适应校准器

    通过双层校准消除系统性偏差：
    1. 聚类残差：同类公司的阈值偏差
    2. 规模残差：不同市值公司的系统偏差
    """

    def __init__(
        self,
        config: TruthConfig = None,
        clusterer: GenomeClusterer = None,
    ):
        """
        初始化校准器

        Args:
            config: T.R.U.T.H.配置
            clusterer: 基因聚类器
        """
        self.config = config or get_default_truth_config()
        self.clusterer = clusterer

        # 校准参数
        self.cluster_residuals: Dict[int, float] = {}
        self.size_residuals: Dict[str, float] = {}

        # 校准状态
        self._is_calibrated = False
        self._calibration_sample_size = 0
        self._last_calibration_date = ""

    def calibrate(
        self,
        results: List[TruthResult],
        market_caps: Dict[str, float] = None,
    ) -> CalibrationResult:
        """
        执行校准

        Args:
            results: T.R.U.T.H.计算结果列表（校准前）
            market_caps: 市值字典 {ts_code: market_cap}

        Returns:
            CalibrationResult 校准结果
        """
        warnings = []

        if len(results) < 50:
            warnings.append(f"样本量过小 ({len(results)})，校准可能不可靠")

        # 1. 聚类残差校准
        if self.clusterer and self.clusterer._is_fitted:
            self.cluster_residuals = self._compute_cluster_residuals(results)
        else:
            warnings.append("聚类器未就绪，跳过聚类残差校准")
            self.cluster_residuals = {}

        # 2. 规模残差校准
        if market_caps:
            self.size_residuals = self._compute_size_residuals(results, market_caps)
        else:
            warnings.append("无市值数据，跳过规模残差校准")
            self.size_residuals = {}

        # 更新状态
        self._is_calibrated = True
        self._calibration_sample_size = len(results)
        from datetime import datetime
        self._last_calibration_date = datetime.now().isoformat()

        # 计算收敛指标
        convergence = self._compute_convergence_metric(results)

        return CalibrationResult(
            cluster_residuals=self.cluster_residuals.copy(),
            size_residuals=self.size_residuals.copy(),
            calibration_date=self._last_calibration_date,
            sample_size=self._calibration_sample_size,
            convergence_metric=convergence,
            warnings=warnings,
        )

    def _compute_cluster_residuals(
        self,
        results: List[TruthResult],
    ) -> Dict[int, float]:
        """
        计算聚类残差

        残差 = Top20%公司的实际ROIC中位数 - 平均理论阈值
        Δ_cluster = 残差 × λ
        """
        params = self.config.calibration
        residuals = {}

        # 按聚类分组
        cluster_groups: Dict[int, List[TruthResult]] = {}
        for r in results:
            if r.cluster_id >= 0:
                if r.cluster_id not in cluster_groups:
                    cluster_groups[r.cluster_id] = []
                cluster_groups[r.cluster_id].append(r)

        for cluster_id, group in cluster_groups.items():
            if len(group) < 5:
                residuals[cluster_id] = 0.0
                continue

            # 按ROIC排序，取Top20%
            sorted_group = sorted(
                group,
                key=lambda r: r.rep_roic.final_value if r.rep_roic else 0,
                reverse=True
            )
            top20_count = max(1, int(len(sorted_group) * params.top_percentile))
            top20 = sorted_group[:top20_count]

            # Top20%的ROIC中位数
            top20_roics = [r.rep_roic.final_value for r in top20 if r.rep_roic]
            if not top20_roics:
                residuals[cluster_id] = 0.0
                continue
            actual_median = np.median(top20_roics)

            # 整个聚类的平均理论阈值
            thresholds = [r.threshold.theory_threshold for r in group if r.threshold]
            if not thresholds:
                residuals[cluster_id] = 0.0
                continue
            theory_mean = np.mean(thresholds)

            # 残差 = 实际 - 理论
            raw_residual = actual_median - theory_mean

            # 阻尼学习
            residuals[cluster_id] = raw_residual * params.residual_lambda

        logger.info(f"聚类残差校准完成: {len(residuals)} clusters")
        return residuals

    def _compute_size_residuals(
        self,
        results: List[TruthResult],
        market_caps: Dict[str, float],
    ) -> Dict[str, float]:
        """
        计算规模残差

        按市值分层，计算每层的系统偏差
        """
        params = self.config.calibration
        residuals = {}

        # 计算市值分位数
        codes_with_cap = [r.ts_code for r in results if r.ts_code in market_caps]
        if len(codes_with_cap) < 10:
            return {}

        caps = np.array([market_caps[c] for c in codes_with_cap])
        percentiles = np.percentile(caps, [10, 30, 60, 85, 100])

        # 分层
        def get_size_bucket(cap: float) -> str:
            if cap < percentiles[0]:
                return 'micro'
            elif cap < percentiles[1]:
                return 'small'
            elif cap < percentiles[2]:
                return 'mid'
            elif cap < percentiles[3]:
                return 'large'
            else:
                return 'mega'

        # 按规模分组
        size_groups: Dict[str, List[TruthResult]] = {b: [] for b in SIZE_BUCKET_NAMES}
        for r in results:
            if r.ts_code in market_caps:
                bucket = get_size_bucket(market_caps[r.ts_code])
                size_groups[bucket].append(r)

        # 计算各层残差
        for bucket, group in size_groups.items():
            if len(group) < 5:
                residuals[bucket] = 0.0
                continue

            # 该层的平均超额收益
            excess_returns = [r.excess_return for r in group]
            avg_excess = np.mean(excess_returns)

            # 残差 = 平均超额（如果系统性偏高/偏低）
            # 但我们限制残差大小
            raw_residual = avg_excess
            capped_residual = np.clip(
                raw_residual,
                -params.size_residual_cap,
                params.size_residual_cap
            )

            residuals[bucket] = capped_residual * params.residual_lambda

        logger.info(f"规模残差校准完成: {residuals}")
        return residuals

    def _compute_convergence_metric(self, results: List[TruthResult]) -> float:
        """计算收敛指标（残差平方和的相对变化）"""
        # 简化实现：返回残差的平均绝对值
        all_residuals = list(self.cluster_residuals.values()) + list(self.size_residuals.values())
        if not all_residuals:
            return 0.0
        return float(np.mean(np.abs(all_residuals)))

    def apply_calibration(
        self,
        result: TruthResult,
        market_cap: float = None,
    ) -> TruthResult:
        """
        应用校准到单个结果

        Args:
            result: 原始结果
            market_cap: 市值（用于规模校准）

        Returns:
            校准后的TruthResult
        """
        if not self._is_calibrated:
            logger.warning("校准器未校准，返回原始结果")
            return result

        # 获取聚类残差
        cluster_residual = self.cluster_residuals.get(result.cluster_id, 0.0)

        # 获取规模残差
        size_residual = 0.0
        if market_cap is not None and self.size_residuals:
            bucket = self._get_size_bucket(market_cap)
            size_residual = self.size_residuals.get(bucket, 0.0)

        # 应用校准到阈值
        if result.threshold:
            calibrated_threshold = result.threshold.theory_threshold + cluster_residual + size_residual

            # 阈值保护
            solver = self.config.solver
            calibrated_threshold = max(calibrated_threshold, solver.threshold_floor)
            calibrated_threshold = min(calibrated_threshold, solver.threshold_ceiling)

            # 创建校准后的ThresholdResult
            new_threshold = ThresholdResult(
                base_rate=result.threshold.base_rate,
                alpha_premium=result.threshold.alpha_premium,
                beta_premium=result.threshold.beta_premium,
                growth_discount=result.threshold.growth_discount,
                verification_bonus=result.threshold.verification_bonus,
                cluster_residual=cluster_residual,
                size_residual=size_residual,
                theory_threshold=result.threshold.theory_threshold,
                final_threshold=calibrated_threshold,
                min_threshold=result.threshold.min_threshold,
            )

            # 重新计算通过状态和超额收益
            new_excess = result.rep_roic.final_value - calibrated_threshold if result.rep_roic else 0
            new_passes = (
                new_excess >= 0
                and not result.genome.is_fraud_risk
            )

            # 重新确定信号和评级
            from .engine import determine_signal, determine_grade
            new_signal = determine_signal(new_passes, new_excess, result.genome, self.config)
            new_grade = determine_grade(new_excess, result.genome, result.confidence)

            # 创建新结果（由于frozen=True，需要创建新对象）
            return TruthResult(
                ts_code=result.ts_code,
                company_name=result.company_name,
                genome=result.genome,
                rep_roic=result.rep_roic,
                threshold=new_threshold,
                passes_screen=new_passes,
                signal=new_signal,
                grade=new_grade,
                excess_return=new_excess,
                confidence=result.confidence,
                cluster_id=result.cluster_id,
                cluster_archetype=result.cluster_archetype,
                warnings=result.warnings,
                breakdown=result.breakdown,
            )

        return result

    def apply_calibration_batch(
        self,
        results: List[TruthResult],
        market_caps: Dict[str, float] = None,
    ) -> List[TruthResult]:
        """批量应用校准"""
        calibrated = []
        for r in results:
            cap = market_caps.get(r.ts_code) if market_caps else None
            calibrated.append(self.apply_calibration(r, cap))
        return calibrated

    def _get_size_bucket(self, market_cap: float) -> str:
        """根据市值确定规模分层（简化版）"""
        # 这里使用固定阈值，实际应该用分位数
        # 单位假设为亿元
        if market_cap < 30:
            return 'micro'
        elif market_cap < 100:
            return 'small'
        elif market_cap < 500:
            return 'mid'
        elif market_cap < 2000:
            return 'large'
        else:
            return 'mega'

    def get_calibration_summary(self) -> Dict[str, Any]:
        """获取校准摘要"""
        return {
            'is_calibrated': self._is_calibrated,
            'sample_size': self._calibration_sample_size,
            'calibration_date': self._last_calibration_date,
            'cluster_residuals': self.cluster_residuals,
            'size_residuals': self.size_residuals,
            'cluster_count': len(self.cluster_residuals),
        }


# ============================================================================
# 置信度计算
# ============================================================================

def compute_confidence(
    genome: CompanyGenome,
    data_years: int,
    config: TruthConfig,
) -> float:
    """
    计算置信度

    考虑因素：
    1. 数据年数
    2. 数据质量
    3. 基因稳定性
    """
    params = config.calibration

    # 基础置信度（基于数据年数）
    if data_years <= 3:
        base_confidence = 0.3
    elif data_years <= 5:
        base_confidence = params.five_year_confidence_ceiling
    elif data_years <= 7:
        base_confidence = 0.70
    elif data_years <= 10:
        base_confidence = params.ten_year_confidence_ceiling
    else:
        base_confidence = 0.90

    # 数据质量调整
    quality_factor = genome.data_quality_score

    # 欺诈风险降低置信度
    fraud_penalty = 0.2 if genome.delta_fraud > 0.4 else 0

    # 最终置信度
    confidence = base_confidence * quality_factor - fraud_penalty

    return max(0.1, min(0.95, confidence))


# ============================================================================
# 集成管道
# ============================================================================

class TruthPipeline:
    """
    T.R.U.T.H. 完整处理管道

    整合：引擎 + 聚类器 + 校准器
    """

    def __init__(self, config: TruthConfig = None):
        self.config = config or get_default_truth_config()

        # 初始化组件
        from .engine import TruthEngine
        self.engine = TruthEngine(self.config)
        self.clusterer = GenomeClusterer(
            n_clusters=self.config.calibration.n_clusters,
            random_state=self.config.calibration.random_state,
        )
        self.calibrator = AdaptiveCalibrator(self.config, self.clusterer)

    def process(
        self,
        companies_data: List[Dict[str, Any]],
        market_caps: Dict[str, float] = None,
    ) -> "BatchResult":
        """
        完整处理流程

        Args:
            companies_data: 公司数据列表
            market_caps: 市值字典

        Returns:
            BatchResult 批量结果
        """
        import time
        from .models import BatchResult, SignalType, GradeLevel

        start_time = time.time()

        # 1. 基因测序
        logger.info(f"开始基因测序: {len(companies_data)} companies")
        genomes = []
        for data in companies_data:
            try:
                genome = self.engine.sequence_genome(**data)
                genomes.append(genome)
            except Exception as e:
                logger.error(f"基因测序失败 {data.get('ts_code')}: {e}")

        # 2. 聚类
        logger.info("开始聚类...")
        self.clusterer.fit(genomes)

        # 3. 分配聚类ID
        for genome in genomes:
            cluster_id = self.clusterer.predict(genome)
            # 由于genome是frozen，我们需要在后续结果中处理

        # 4. 计算T.R.U.T.H.结果
        logger.info("计算T.R.U.T.H.结果...")
        results = []
        for genome in genomes:
            result = self.engine.compute_truth(genome)

            # 添加聚类信息
            cluster_id = self.clusterer.predict(genome)
            archetype = self.clusterer.get_archetype(cluster_id)

            # 创建带聚类信息的结果
            result = TruthResult(
                ts_code=result.ts_code,
                company_name=result.company_name,
                genome=result.genome,
                rep_roic=result.rep_roic,
                threshold=result.threshold,
                passes_screen=result.passes_screen,
                signal=result.signal,
                grade=result.grade,
                excess_return=result.excess_return,
                confidence=result.confidence,
                cluster_id=cluster_id,
                cluster_archetype=archetype,
                warnings=result.warnings,
                breakdown=result.breakdown,
            )
            results.append(result)

        # 5. 校准
        logger.info("执行校准...")
        calibration = self.calibrator.calibrate(results, market_caps)

        # 6. 应用校准
        logger.info("应用校准...")
        calibrated_results = self.calibrator.apply_calibration_batch(results, market_caps)

        # 7. 统计
        passed = [r for r in calibrated_results if r.passes_screen]

        signal_dist = {s: 0 for s in SignalType}
        grade_dist = {g: 0 for g in GradeLevel}
        for r in calibrated_results:
            signal_dist[r.signal] += 1
            grade_dist[r.grade] += 1

        elapsed = time.time() - start_time
        logger.info(f"处理完成: {len(calibrated_results)} results, {len(passed)} passed, {elapsed:.2f}s")

        return BatchResult(
            results=calibrated_results,
            cluster_profiles=list(self.clusterer.cluster_profiles.values()),
            calibration=calibration,
            total_count=len(calibrated_results),
            passed_count=len(passed),
            pass_rate=len(passed) / len(calibrated_results) if calibrated_results else 0,
            signal_distribution=signal_dist,
            grade_distribution=grade_dist,
            computation_time_seconds=elapsed,
            config_version=self.config.version,
        )

    def get_cluster_report(self) -> str:
        """获取聚类报告"""
        return self.clusterer.visualize_text()

    def get_calibration_report(self) -> Dict[str, Any]:
        """获取校准报告"""
        return self.calibrator.get_calibration_summary()
