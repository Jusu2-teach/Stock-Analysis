"""T.R.U.T.H. 四层处理管道 - 专业级批量计算

四层流水线架构:
    Layer 0: TimeDecay - 时序衰减预处理 (EWMA/Bootstrap)
    Layer 1: Factors   - 六维因子计算 (α/β/γ/δ_fraud/δ_decay/V)
    Layer 2: Solvers   - 物理求解器 (Gravity/Velocity/Structure)
    Layer 3: Calibration - 校准层 (规模调整/置信度/市场锚定)

设计原则:
    - 单股票批量探针 → 完整 TruthProfile
    - 熔断机制: δ_fraud > 0.58 触发熔断
    - 多重输出: signal + grade + warnings + dynamic_thresholds
    - 依赖注入: 因子/求解器可通过工厂注入 (便于测试)

版本: 3.3.0
日期: 2026-01-06
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, Callable, TYPE_CHECKING

from ..domain import (
    DynamicThreshold,
    FactorId,
    FactorResult,
    ProbeInput,
    SolverId,
    SolverResult,
    TruthGrade,
    TruthProfile,
    TruthSignal,
    TruthWarning,
    WarningLevel,
)
from ..config import TruthConfig, CalibrationConfig
from .factors import TruthFactor, get_all_factors
from .solvers import TruthSolver, get_all_solvers

# 类型检查时导入 Protocol
if TYPE_CHECKING:
    from .protocols import FactorProtocol, SolverProtocol


# ============================================================================
# Layer 0: TimeDecay 预处理器
# ============================================================================

@dataclass
class TimeDecayProcessor:
    """Layer 0: 时序衰减预处理

    对探针特征应用时间衰减权重:
        - EWMA (指数加权移动平均) 用于近期偏向
        - Bootstrap 重采样用于置信区间估计

    这一层主要在探针阶段已完成，这里提供后处理入口
    """

    config: TruthConfig

    def process(self,
                ts_code: str,
                probes: Sequence[ProbeInput]) -> Tuple[Sequence[ProbeInput], Dict[str, float]]:
        """应用时序衰减

        Returns:
            (processed_probes, decay_metadata)
        """
        decay_config = self.config.time_decay
        metadata = {
            "half_life_years": decay_config.half_life_years,
            "ewma_alpha": decay_config.ewma_alpha,
            "applied": True,
        }

        # 探针已包含时序衰减特征 (如 weighted_avg)
        return probes, metadata


# ============================================================================
# Layer 1: Factors 因子计算层
# ============================================================================

@dataclass
class FactorCalculator:
    """Layer 1: 六维因子计算

    支持依赖注入:
        - 默认使用 get_all_factors() 获取全部因子
        - 可传入自定义 factors 列表用于测试或定制化
    """

    config: TruthConfig
    factors: Optional[List] = None  # 类型: List[FactorProtocol]

    def __post_init__(self):
        if self.factors is None:
            self.factors = get_all_factors()

    def calculate(self,
                  ts_code: str,
                  probes: Sequence[ProbeInput]) -> Tuple[Dict[FactorId, FactorResult], List[TruthWarning]]:
        """计算所有因子

        Returns:
            (factor_results, all_warnings)
        """
        results: Dict[FactorId, FactorResult] = {}
        all_warnings: List[TruthWarning] = []

        for factor in self.factors:
            try:
                result, warnings = factor.evaluate(ts_code, probes, self.config)
                results[factor.factor_id] = result
                all_warnings.extend(warnings)
            except Exception as e:
                # 因子计算失败，记录警告但不中断
                all_warnings.append(TruthWarning(
                    code=f"FACTOR_{factor.factor_id.name}_ERROR",
                    level=WarningLevel.CRITICAL,
                    title=f"{factor.factor_id.name} 因子计算失败",
                    message=str(e),
                    source="factor_calculator",
                ))

        return results, all_warnings

    def check_meltdown(self,
                       factors: Dict[FactorId, FactorResult]) -> Tuple[bool, Optional[TruthWarning]]:
        """检查熔断条件

        Returns:
            (is_meltdown, meltdown_warning)
        """
        delta_fraud = factors.get(FactorId.DELTA_FRAUD)
        if delta_fraud is None:
            return False, None

        threshold = self.config.delta_fraud_config.meltdown_threshold

        if delta_fraud.score > threshold:
            warning = TruthWarning(
                code="MELTDOWN_TRIGGERED",
                level=WarningLevel.FATAL,
                title="🚨 T.R.U.T.H. 熔断",
                message=f"δ_fraud={delta_fraud.score:.3f} > {threshold}，分析终止",
                source="meltdown_check",
                values={"delta_fraud": delta_fraud.score, "threshold": threshold},
            )
            return True, warning

        return False, None


# ============================================================================
# Layer 2: Solvers 求解层
# ============================================================================

@dataclass
class SolverExecutor:
    """Layer 2: 物理求解器执行

    支持依赖注入:
        - 默认使用 get_all_solvers() 获取全部求解器
        - 可传入自定义 solvers 列表用于测试或定制化
    """

    config: TruthConfig
    solvers: Optional[List] = None  # 类型: List[SolverProtocol]

    def __post_init__(self):
        if self.solvers is None:
            self.solvers = get_all_solvers()

    def execute(self,
                ts_code: str,
                factors: Dict[FactorId, FactorResult]) -> Tuple[Dict[SolverId, SolverResult], List[TruthWarning]]:
        """执行所有求解器

        Returns:
            (solver_results, all_warnings)
        """
        results: Dict[SolverId, SolverResult] = {}
        all_warnings: List[TruthWarning] = []

        for solver in self.solvers:
            try:
                result, warnings = solver.solve(ts_code, factors, self.config)
                results[solver.solver_id] = result
                all_warnings.extend(warnings)
            except Exception as e:
                all_warnings.append(TruthWarning(
                    code=f"SOLVER_{solver.solver_id.name}_ERROR",
                    level=WarningLevel.CRITICAL,
                    title=f"{solver.solver_id.name} 求解器失败",
                    message=str(e),
                    source="solver_executor",
                ))

        return results, all_warnings


# ============================================================================
# Layer 3: Calibration 校准层
# ============================================================================

@dataclass
class CalibrationEngine:
    """Layer 3: 校准层

    功能:
        1. 规模调整: 小盘股/大盘股的评分修正
        2. 置信度计算: 基于数据质量的置信度
        3. 市场锚定: 相对市场中位数的调整
        4. 最终评分: 综合因子和求解器输出
    """

    config: TruthConfig

    def calibrate(self,
                  ts_code: str,
                  factors: Dict[FactorId, FactorResult],
                  solvers: Dict[SolverId, SolverResult],
                  metadata: Optional[Dict[str, float]] = None) -> Tuple[float, float, Dict[str, float]]:
        """校准最终评分

        Args:
            ts_code: 股票代码
            factors: 因子结果
            solvers: 求解器结果
            metadata: 额外元数据 (如市值、行业)

        Returns:
            (final_score, confidence, calibration_details)
        """
        calib_config = self.config.calibration
        details: Dict[str, float] = {}

        # ============================================================
        # Step 1: 基础评分 (因子加权)
        # ============================================================

        factor_weights = self.config.scoring.factor_weights
        factor_score = 0.0
        factor_weight_sum = 0.0

        for fid, result in factors.items():
            if result is None or result.score is None:
                continue
            # 使用因子名称查找权重
            w = factor_weights.get(fid.name, factor_weights.get(fid.value, 1.0 / max(len(factors), 1)))

            # 特殊处理: δ_fraud 和 δ_decay 是惩罚项
            if fid in (FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY):
                # 这些分数高 = 不好，需要反向处理
                adjusted_score = 1.0 - result.score
            else:
                adjusted_score = result.score

            factor_score += w * adjusted_score
            factor_weight_sum += w

        if factor_weight_sum > 0:
            factor_score /= factor_weight_sum
        else:
            factor_score = 0.5

        details["factor_score"] = factor_score

        # ============================================================
        # Step 2: 求解器评分 (求解器加权)
        # ============================================================

        solver_weights = self.config.scoring.solver_weights
        solver_score = 0.0
        solver_weight_sum = 0.0

        for sid, result in solvers.items():
            if result is None or result.score is None:
                continue
            # 使用求解器名称查找权重
            w = solver_weights.get(sid.name, solver_weights.get(sid.value, 1.0 / max(len(solvers), 1)))
            solver_score += w * result.score
            solver_weight_sum += w

        if solver_weight_sum > 0:
            solver_score /= solver_weight_sum
        else:
            solver_score = 0.5

        details["solver_score"] = solver_score

        # ============================================================
        # Step 3: 综合评分 (因子 vs 求解器权重)
        # ============================================================

        factor_vs_solver = self.config.scoring.factor_vs_solver_weight
        raw_score = factor_vs_solver * factor_score + (1 - factor_vs_solver) * solver_score
        details["raw_score"] = raw_score

        # ============================================================
        # Step 4: 规模调整 (基于 size_adjustments 映射)
        # ============================================================

        size_adjustment = 0.0
        if metadata and "market_cap" in metadata:
            market_cap = metadata["market_cap"]  # 单位: 亿元
            # 确定市值分层
            if market_cap > 1000:
                size_tier = "mega"      # 超大盘
            elif market_cap > 300:
                size_tier = "large"     # 大盘
            elif market_cap > 100:
                size_tier = "mid"       # 中盘
            elif market_cap > 30:
                size_tier = "small"     # 小盘
            else:
                size_tier = "micro"     # 微盘

            # 获取对应的调整值 (转换为小数)
            size_adjustment = calib_config.size_adjustments.get(size_tier, 0.0) / 100.0
            details["size_tier"] = size_tier

        details["size_adjustment"] = size_adjustment

        # ============================================================
        # Step 5: 行业调整
        # ============================================================

        industry_adjustment = 0.0
        if calib_config.industry_adjustment_enabled and metadata:
            if "sector_median" in metadata:
                sector_median = metadata["sector_median"]
                # 相对行业中位数的调整
                relative_perf = raw_score - sector_median
                industry_adjustment = relative_perf * calib_config.industry_adjustment_weight

        details["industry_adjustment"] = industry_adjustment

        # ============================================================
        # Step 6: 计算最终分数
        # ============================================================

        final_score = raw_score + size_adjustment + industry_adjustment
        final_score = max(0.0, min(1.0, final_score))
        details["final_score"] = final_score
        details["final_score"] = final_score

        # ============================================================
        # Step 7: 计算置信度
        # ============================================================

        confidence = self._calculate_confidence(factors, solvers)
        details["confidence"] = confidence

        return final_score, confidence, details

    def _calculate_confidence(self,
                              factors: Dict[FactorId, FactorResult],
                              solvers: Dict[SolverId, SolverResult]) -> float:
        """计算综合置信度"""

        confidences = []

        # 因子置信度
        for result in factors.values():
            if result and result.confidence is not None:
                confidences.append(result.confidence)

        # 求解器置信度
        for result in solvers.values():
            if result and result.confidence is not None:
                confidences.append(result.confidence)

        if not confidences:
            return 0.5

        # 加权平均 (偏向较低的置信度)
        min_conf = min(confidences)
        avg_conf = sum(confidences) / len(confidences)

        # 混合: 60% 平均 + 40% 最低
        return 0.6 * avg_conf + 0.4 * min_conf


# ============================================================================
# TruthPipeline 主管道
# ============================================================================

@dataclass
class TruthPipeline:
    """T.R.U.T.H. 四层处理管道

    完整流程:
        probes → TimeDecay → Factors → Solvers → Calibration → TruthProfile

    特性:
        - 熔断机制: 欺诈熵过高时终止
        - 完整输出: 因子、求解器、阈值、警告
        - 可配置: 通过 TruthConfig 调整所有参数
        - 依赖注入: 因子/求解器可通过构造函数注入

    依赖注入示例:
        >>> # 默认使用全部因子和求解器
        >>> pipeline = TruthPipeline()
        >>>
        >>> # 注入自定义因子 (测试用)
        >>> from .factory import create_test_factory
        >>> factory = create_test_factory(mock_factors=[MockAlphaFactor()])
        >>> pipeline = TruthPipeline(
        ...     factor_calculator=FactorCalculator(config, factors=factory.create_factors(config))
        ... )
    """

    config: TruthConfig = field(default_factory=TruthConfig)

    # 四层处理器 (支持依赖注入)
    time_decay: Optional[TimeDecayProcessor] = None
    factor_calculator: Optional[FactorCalculator] = None
    solver_executor: Optional[SolverExecutor] = None
    calibration: Optional[CalibrationEngine] = None

    # 回调 (可选)
    on_meltdown: Optional[Callable[[str, TruthWarning], None]] = None

    def __post_init__(self):
        # 未注入时使用默认实现
        if self.time_decay is None:
            self.time_decay = TimeDecayProcessor(self.config)
        if self.factor_calculator is None:
            self.factor_calculator = FactorCalculator(self.config)
        if self.solver_executor is None:
            self.solver_executor = SolverExecutor(self.config)
        if self.calibration is None:
            self.calibration = CalibrationEngine(self.config)

    def process(self,
                ts_code: str,
                probes: Sequence[ProbeInput],
                metadata: Optional[Dict[str, float]] = None) -> TruthProfile:
        """处理单支股票

        Args:
            ts_code: 股票代码
            probes: 探针输入列表 (通常 8 个: ROIC/ROE/Revenue/Profit/OCF/Margin等)
            metadata: 可选元数据 (市值、行业等)

        Returns:
            完整的 TruthProfile
        """
        start_time = time.time()
        all_warnings: List[TruthWarning] = []

        # ============================================================
        # Layer 0: TimeDecay
        # ============================================================

        processed_probes, decay_meta = self.time_decay.process(ts_code, probes)

        # ============================================================
        # Layer 1: Factors
        # ============================================================

        factors, factor_warnings = self.factor_calculator.calculate(ts_code, processed_probes)
        all_warnings.extend(factor_warnings)

        # ============================================================
        # Meltdown Check (熔断检测)
        # ============================================================

        is_meltdown, meltdown_warning = self.factor_calculator.check_meltdown(factors)

        if is_meltdown and meltdown_warning:
            all_warnings.append(meltdown_warning)

            if self.on_meltdown:
                self.on_meltdown(ts_code, meltdown_warning)

            # 熔断时返回特殊 Profile
            return self._create_meltdown_profile(ts_code, factors, all_warnings)

        # ============================================================
        # Layer 2: Solvers
        # ============================================================

        solvers, solver_warnings = self.solver_executor.execute(ts_code, factors)
        all_warnings.extend(solver_warnings)

        # ============================================================
        # Layer 3: Calibration
        # ============================================================

        final_score, confidence, calib_details = self.calibration.calibrate(
            ts_code, factors, solvers, metadata
        )

        # ============================================================
        # 构建最终输出
        # ============================================================

        # 信号和等级
        signal = self._determine_signal(final_score, factors, all_warnings)
        grade = self._determine_grade(final_score)

        # 计算处理时间
        elapsed_ms = (time.time() - start_time) * 1000

        # 数据质量评估
        data_quality = "good"
        if confidence < 0.5:
            data_quality = "poor"
        elif confidence < 0.7:
            data_quality = "fair"

        return TruthProfile(
            ts_code=ts_code,
            factors=factors,
            solvers=solvers,
            final_score=final_score,
            signal=signal,
            grade=grade,
            confidence=confidence,
            warnings=tuple(all_warnings),
            data_quality=data_quality,
        )

    def process_batch(self,
                      stocks: Sequence[Tuple[str, Sequence[ProbeInput]]],
                      metadata_map: Optional[Dict[str, Dict[str, float]]] = None) -> List[TruthProfile]:
        """批量处理多支股票

        Args:
            stocks: [(ts_code, probes), ...] 列表
            metadata_map: {ts_code: metadata} 映射

        Returns:
            TruthProfile 列表
        """
        profiles = []

        for ts_code, probes in stocks:
            meta = metadata_map.get(ts_code) if metadata_map else None
            profile = self.process(ts_code, probes, meta)
            profiles.append(profile)

        return profiles

    def _create_meltdown_profile(self,
                                  ts_code: str,
                                  factors: Dict[FactorId, FactorResult],
                                  warnings: List[TruthWarning]) -> TruthProfile:
        """创建欺诈熔断状态的 Profile"""
        return TruthProfile(
            ts_code=ts_code,
            factors=factors,
            solvers={},
            final_score=0.0,
            signal=TruthSignal.FRAUD_ALERT,
            grade=TruthGrade.F,
            confidence=0.95,  # 熔断判定很确定
            warnings=tuple(warnings),
            data_quality="fraud_alert",
        )

    def _determine_signal(self,
                          score: float,
                          factors: Dict[FactorId, FactorResult],
                          warnings: List[TruthWarning]) -> TruthSignal:
        """确定交易信号"""

        # 检查是否有 FATAL 警告
        fatal_count = sum(1 for w in warnings if w.level == WarningLevel.FATAL)
        if fatal_count > 0:
            return TruthSignal.FRAUD_ALERT

        # 检查 critical 警告
        critical_count = sum(1 for w in warnings if w.level == WarningLevel.CRITICAL)

        # 基于分数和警告确定信号
        thresholds = self.config.scoring.signal_thresholds

        if score >= thresholds["strong_buy"] and critical_count == 0:
            return TruthSignal.STRONG_BUY
        elif score >= thresholds["buy"]:
            return TruthSignal.BUY
        elif score >= thresholds["hold"]:
            return TruthSignal.HOLD
        elif score >= thresholds["caution"]:
            return TruthSignal.CAUTION
        else:
            return TruthSignal.SELL

    def _determine_grade(self, score: float) -> TruthGrade:
        """确定评级等级"""
        thresholds = self.config.scoring.grade_thresholds

        if score >= thresholds["A"]:
            return TruthGrade.A
        elif score >= thresholds["B"]:
            return TruthGrade.B
        elif score >= thresholds["C"]:
            return TruthGrade.C
        elif score >= thresholds["D"]:
            return TruthGrade.D
        else:
            return TruthGrade.F

    def _merge_thresholds(self,
                          solvers: Dict[SolverId, SolverResult]) -> Dict[str, DynamicThreshold]:
        """合并所有求解器的动态阈值"""
        merged = {}
        for result in solvers.values():
            if result and result.thresholds:
                merged.update(result.thresholds)
        return merged


# ============================================================================
# 便捷函数
# ============================================================================

def create_pipeline(config: Optional[TruthConfig] = None) -> TruthPipeline:
    """创建管道实例"""
    return TruthPipeline(config=config or TruthConfig())


def process_single(ts_code: str,
                   probes: Sequence[ProbeInput],
                   config: Optional[TruthConfig] = None) -> TruthProfile:
    """处理单支股票的便捷函数"""
    pipeline = create_pipeline(config)
    return pipeline.process(ts_code, probes)


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 层级处理器
    "TimeDecayProcessor",
    "FactorCalculator",
    "SolverExecutor",
    "CalibrationEngine",
    # 主管道
    "TruthPipeline",
    # 便捷函数
    "create_pipeline",
    "process_single",
]
