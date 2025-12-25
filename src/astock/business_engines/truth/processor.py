"""
T.R.U.T.H. Processor - 专业基因-指标映射处理器
==============================================

核心职责：
1. 接收8个探针的分析结果
2. 执行专业的基因-指标映射
3. 应用智能聚合策略
4. 运行三大求解器
5. 输出完整的处理结果

基因-指标映射关系（专业版）：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
│ 基因 │ 核心指标 │ 聚合策略 │ 计算逻辑 │
├──────┼──────────┼──────────┼──────────┤
│ α    │ ROIC,ROE │ max      │ 周期性最强的那个 │
│ β    │ ROIC     │ 单一     │ 资本密度检测 │
│ γ    │ 营收,利润│ 调和平均 │ 增长动能综合 │
│ δ_f  │ 利润,OCF │ 逻辑OR   │ 任一异常即触发 │
│ δ_d  │ 所有效率 │ max      │ 最严重的衰退 │
│ V    │ OCF      │ 单一     │ 现金验证 │
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

求解器数据流：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
│ 求解器 │ 核心输入 │ 用途 │
├────────┼──────────┼──────┤
│ gravity │ ROIC的α,β │ 计算动态ROIC阈值 │
│ velocity│ 营收/利润γ │ 计算增长速度边界 │
│ structure│ 毛利率斜率 │ 检测护城河侵蚀 │
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

因果网络验证层：
    营收增长 → 利润增长 → 现金流增长
        ↓           ↓
       ROE ← ROIC
        ↓
    周期性波动

作者: AStock Analysis System
日期: 2025-01
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
import pandas as pd
import logging

from .config import TruthConfig, get_default_truth_config
from .models import CompanyGenome, TruthResult
# 导入探针结果模型（从analyzers/trend/models.py）
from ..analyzers.trend.models import (
    LogTrendResult,
    VolatilityResult,
    CyclicalPatternResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    RobustTrendResult,
    InflectionResult,
    DataQualitySummary,
    TrendWarning,
)
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
)
from .core.genes import (
    compute_alpha_from_probes,
    compute_beta_from_probes,
    compute_gamma_from_probes,
    compute_delta_fraud_from_probes,
    compute_delta_decay_from_probes,
    compute_verification_from_probes,
    compute_genome_from_probes,  # 核心：专业基因组计算入口
)
from .core.solvers import (
    gravity_solver,
    velocity_solver,
    structure_solver,
    GravitySolverResult,
    VelocitySolverResult,
    StructureSolverResult,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 处理结果数据模型
# ============================================================================

@dataclass
class GeneExtractionResult:
    """单个基因的提取结果"""
    gene_name: str
    value: float
    source_indicators: List[str]
    aggregation_method: str
    breakdown: Dict[str, Any] = field(default_factory=dict)
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class SolverExecutionResult:
    """求解器执行结果"""
    solver_name: str
    result: Any  # GravitySolverResult | VelocitySolverResult | StructureSolverResult
    input_genes: Dict[str, float]
    interpretation: str = ""


@dataclass
class CausalValidation:
    """因果网络验证结果"""
    revenue_profit_consistent: bool  # 营收增长→利润增长
    profit_ocf_consistent: bool      # 利润增长→现金流增长
    roe_roic_consistent: bool        # ROE与ROIC一致性
    cycle_transmission: bool         # 周期性传导合理
    overall_score: float             # 因果一致性总分
    warnings: List[str] = field(default_factory=list)


@dataclass
class TruthProcessResult:
    """
    T.R.U.T.H. 处理结果（单个公司）

    包含：
    1. 六维基因提取结果
    2. 三大求解器结果
    3. 因果网络验证
    4. 最终评估
    """
    ts_code: str
    company_name: str = ""

    # 六维基因
    genome: Optional[CompanyGenome] = None
    gene_extractions: Dict[str, GeneExtractionResult] = field(default_factory=dict)

    # 三大求解器
    solver_results: Dict[str, SolverExecutionResult] = field(default_factory=dict)

    # 因果验证
    causal_validation: Optional[CausalValidation] = None

    # 综合评估
    final_score: float = 0.0
    signal: str = "NEUTRAL"
    grade: str = "B"
    warnings: List[str] = field(default_factory=list)

    # 元数据
    data_quality_score: float = 1.0
    processing_notes: List[str] = field(default_factory=list)


@dataclass
class BatchProcessResult:
    """批量处理结果"""
    results: List[TruthProcessResult]
    summary: Dict[str, Any] = field(default_factory=dict)
    processing_stats: Dict[str, int] = field(default_factory=dict)


# ============================================================================
# DataFrame → ProbeOutputs 转换器（关键桥接层）
# ============================================================================

class DataFrameToProbeConverter:
    """
    将 Pipeline 输出的 DataFrame 转换为探针结果对象

    这是连接 Pipeline 和 T.R.U.T.H. 系统的关键桥梁：
    - 输入: DataFrame 行（包含 roic_cv, roic_detrended_cv 等列）
    - 输出: ProbeOutputs（包含 LogTrendResult, VolatilityResult 等对象）

    映射关系：
    - DataFrame 列名格式: {metric}_{field}，如 roic_cv, roic_detrended_cv
    - 探针结果对象: LogTrendResult, VolatilityResult, CyclicalPatternResult 等
    """

    # 指标名到 DataFrame 前缀的映射
    METRIC_PREFIX_MAP = {
        'roic': 'roic',
        'roe': 'roe',
        'roiic': 'roiic',
        'gross_margin': 'grossprofit_margin',
        'net_margin': 'netprofit_margin',
        'revenue': 'total_revenue_ps',
        'profit': 'eps',
        'ocf': 'ocfps',
    }

    def convert_row_to_probe_outputs(
        self,
        row: pd.Series,
        metric_key: str,
    ) -> ProbeOutputs:
        """
        将 DataFrame 的一行转换为 ProbeOutputs 对象

        Args:
            row: DataFrame 的一行数据
            metric_key: 指标键名（如 'roic', 'roe'）

        Returns:
            ProbeOutputs 对象（包含专业的探针结果结构）
        """
        prefix = self.METRIC_PREFIX_MAP.get(metric_key, metric_key)

        # 创建各探针结果对象
        log_trend = self._build_log_trend(row, prefix)
        volatility = self._build_volatility(row, prefix)
        cyclical = self._build_cyclical(row, prefix)
        deterioration = self._build_deterioration(row, prefix)
        rolling = self._build_rolling(row, prefix)
        robust = self._build_robust(row, prefix)
        inflection = self._build_inflection(row, prefix)

        return ProbeOutputs(
            indicator_name=metric_key,
            log_trend=log_trend,
            volatility=volatility,
            cyclical=cyclical,
            deterioration=deterioration,
            rolling=rolling,
            robust=robust,
            inflection=inflection,
        )

    def _safe_get(self, row: pd.Series, col: str, default: Any = 0.0) -> Any:
        """安全获取列值"""
        if col not in row.index:
            return default
        val = row[col]
        if pd.isna(val):
            return default
        return val

    def _build_log_trend(self, row: pd.Series, prefix: str) -> LogTrendResult:
        """构建 LogTrendResult"""
        # 创建数据质量摘要
        quality = DataQualitySummary(
            original=str(self._safe_get(row, f'{prefix}_data_quality_original', 'unknown')),
            cleaned=str(self._safe_get(row, f'{prefix}_data_quality_cleaned', 'unknown')),
            effective=str(self._safe_get(row, f'{prefix}_data_quality', 'unknown')),
            has_loss_years=bool(self._safe_get(row, f'{prefix}_has_loss_years', False)),
            loss_year_count=int(self._safe_get(row, f'{prefix}_loss_year_count', 0)),
            has_near_zero_years=bool(self._safe_get(row, f'{prefix}_has_near_zero_years', False)),
            near_zero_count=int(self._safe_get(row, f'{prefix}_near_zero_count', 0)),
            has_loss_years_cleaned=bool(self._safe_get(row, f'{prefix}_has_loss_years_cleaned', False)),
            loss_year_count_cleaned=int(self._safe_get(row, f'{prefix}_loss_year_count_cleaned', 0)),
            has_near_zero_years_cleaned=bool(self._safe_get(row, f'{prefix}_has_near_zero_years_cleaned', False)),
            near_zero_count_cleaned=int(self._safe_get(row, f'{prefix}_near_zero_count_cleaned', 0)),
        )

        return LogTrendResult(
            log_slope=float(self._safe_get(row, f'{prefix}_log_slope', 0.0)),
            slope=float(self._safe_get(row, f'{prefix}_slope', 0.0)),
            intercept=0.0,
            r_squared=float(self._safe_get(row, f'{prefix}_r_squared', 0.5)),
            p_value=float(self._safe_get(row, f'{prefix}_p_value', 0.05)),
            std_err=0.0,
            cagr_approx=float(self._safe_get(row, f'{prefix}_cagr', 0.0)),
            crosses_zero=False,
            used_cleaned_data=True,
            quality=quality,
            outliers=None,
            metadata={'fused_slope': float(self._safe_get(row, f'{prefix}_fused_slope', 0.0))},
            warnings=[],
        )

    def _build_volatility(self, row: pd.Series, prefix: str) -> VolatilityResult:
        """构建 VolatilityResult"""
        return VolatilityResult(
            cv=float(self._safe_get(row, f'{prefix}_cv', 0.2)),
            std_dev=float(self._safe_get(row, f'{prefix}_std_dev', 0.0)),
            range_ratio=float(self._safe_get(row, f'{prefix}_range_ratio', 1.0)),
            volatility_type=str(self._safe_get(row, f'{prefix}_volatility_type', 'moderate')),
            mean_near_zero=bool(self._safe_get(row, f'{prefix}_vol_mean_near_zero', False)),
            detrended_cv=float(self._safe_get(row, f'{prefix}_detrended_cv', 0.2)),
            has_arch_effect=bool(self._safe_get(row, f'{prefix}_has_arch_effect', False)),
            volatility_regime=str(self._safe_get(row, f'{prefix}_volatility_regime', 'stable')),
            warnings=[],
        )

    def _build_cyclical(self, row: pd.Series, prefix: str) -> CyclicalPatternResult:
        """构建 CyclicalPatternResult"""
        return CyclicalPatternResult(
            is_cyclical=bool(self._safe_get(row, f'{prefix}_is_cyclical', False)),
            peak_to_trough_ratio=float(self._safe_get(row, f'{prefix}_peak_to_trough_ratio', 1.0)),
            has_middle_peak=bool(self._safe_get(row, f'{prefix}_has_middle_peak', False)),
            has_wave_pattern=bool(self._safe_get(row, f'{prefix}_has_wave_pattern', False)),
            trend_r_squared=float(self._safe_get(row, f'{prefix}_trend_r_squared', 0.5)),
            cv=float(self._safe_get(row, f'{prefix}_cyclical_cv', 0.2)),
            current_phase=str(self._safe_get(row, f'{prefix}_current_phase', 'stable')),
            cycle_position='unknown',
            fft_dominant_period=0.0,
            industry_cyclical=bool(self._safe_get(row, f'{prefix}_industry_cyclical', False)),
            cyclical_confidence=float(self._safe_get(row, f'{prefix}_cyclical_confidence', 0.0)),
            peak_to_trough_threshold=float(self._safe_get(row, f'{prefix}_peak_to_trough_threshold', 3.0)),
            trend_r_squared_max=float(self._safe_get(row, f'{prefix}_trend_r_squared_max', 0.3)),
            cv_threshold=float(self._safe_get(row, f'{prefix}_cv_threshold', 0.5)),
            industry=str(self._safe_get(row, f'{prefix}_cyclical_industry', '')),
            confidence_factors=[],
            warnings=[],
        )

    def _build_deterioration(self, row: pd.Series, prefix: str) -> RecentDeteriorationResult:
        """构建 RecentDeteriorationResult"""
        return RecentDeteriorationResult(
            has_deterioration=bool(self._safe_get(row, f'{prefix}_has_deterioration', False)),
            severity=str(self._safe_get(row, f'{prefix}_deterioration_severity', 'none')),
            year4_to_5_change=float(self._safe_get(row, f'{prefix}_year4_to_5_change', 0.0)),
            year3_to_4_change=float(self._safe_get(row, f'{prefix}_year3_to_4_change', 0.0)),
            year4_to_5_pct=float(self._safe_get(row, f'{prefix}_year4_to_5_pct', 0.0)),
            year3_to_4_pct=float(self._safe_get(row, f'{prefix}_year3_to_4_pct', 0.0)),
            total_decline_pct=float(self._safe_get(row, f'{prefix}_total_decline_pct', 0.0)),
            is_high_level_stable=bool(self._safe_get(row, f'{prefix}_is_high_level_stable', False)),
            decline_threshold_pct=float(self._safe_get(row, f'{prefix}_decline_threshold_pct', -5.0)),
            decline_threshold_abs=float(self._safe_get(row, f'{prefix}_decline_threshold_abs', -2.0)),
            industry=str(self._safe_get(row, f'{prefix}_deterioration_industry', '')),
            consecutive_decline_years=int(self._safe_get(row, f'{prefix}_consecutive_decline_years', 0)),
            deterioration_acceleration=float(self._safe_get(row, f'{prefix}_deterioration_acceleration', 0.0)),
            deterioration_pattern=str(self._safe_get(row, f'{prefix}_deterioration_pattern', 'none')),
            deterioration_probability=float(self._safe_get(row, f'{prefix}_deterioration_probability', 0.0)),
            warnings=[],
        )

    def _build_rolling(self, row: pd.Series, prefix: str) -> RollingTrendResult:
        """构建 RollingTrendResult"""
        return RollingTrendResult(
            recent_3y_slope=float(self._safe_get(row, f'{prefix}_recent_3y_slope', 0.0)),
            recent_3y_r_squared=float(self._safe_get(row, f'{prefix}_recent_3y_r_squared', 0.5)),
            full_5y_slope=float(self._safe_get(row, f'{prefix}_full_5y_slope', 0.0)),
            full_5y_r_squared=float(self._safe_get(row, f'{prefix}_full_5y_r_squared', 0.5)),
            trend_acceleration=float(self._safe_get(row, f'{prefix}_trend_acceleration', 0.0)),
            acceleration_confidence=0.5,
            is_accelerating=bool(self._safe_get(row, f'{prefix}_is_accelerating', False)),
            is_decelerating=bool(self._safe_get(row, f'{prefix}_is_decelerating', False)),
            early_3y_slope=float(self._safe_get(row, f'{prefix}_early_slope', 0.0)),
            early_3y_r_squared=float(self._safe_get(row, f'{prefix}_inflection_early_r2', 0.5)),
            warnings=[],
        )

    def _build_robust(self, row: pd.Series, prefix: str) -> Optional[RobustTrendResult]:
        """构建 RobustTrendResult"""
        robust_slope = self._safe_get(row, f'{prefix}_robust_slope', None)
        if robust_slope is None:
            return None

        return RobustTrendResult(
            robust_slope=float(robust_slope),
            robust_intercept=0.0,
            robust_slope_ci_low=0.0,
            robust_slope_ci_high=0.0,
            mann_kendall_tau=float(self._safe_get(row, f'{prefix}_mk_tau', 0.0)),
            mann_kendall_p_value=float(self._safe_get(row, f'{prefix}_mk_p_value', 1.0)),
            is_valid=True,
            warnings=[],
        )

    def _build_inflection(self, row: pd.Series, prefix: str) -> Optional[InflectionResult]:
        """构建 InflectionResult"""
        has_inflection = bool(self._safe_get(row, f'{prefix}_has_inflection', False))
        if not has_inflection:
            return None

        return InflectionResult(
            has_inflection=True,
            inflection_type=str(self._safe_get(row, f'{prefix}_inflection_type', 'none')),
            early_slope=float(self._safe_get(row, f'{prefix}_early_slope', 0.0)),
            middle_slope=float(self._safe_get(row, f'{prefix}_middle_slope', 0.0)),
            recent_slope=float(self._safe_get(row, f'{prefix}_recent_slope', 0.0)),
            slope_change=float(self._safe_get(row, f'{prefix}_slope_change', 0.0)),
            confidence=float(self._safe_get(row, f'{prefix}_inflection_confidence', 0.0)),
            early_r_squared=float(self._safe_get(row, f'{prefix}_inflection_early_r2', 0.0)),
            recent_r_squared=float(self._safe_get(row, f'{prefix}_inflection_recent_r2', 0.0)),
            warnings=[],
        )


# ============================================================================
# 指标数据提取器（旧版，保留兼容性）
# ============================================================================

class IndicatorExtractor:
    """
    从探针 DataFrame 中提取单个指标的数据
    """

    @staticmethod
    def extract_for_company(
        df: pd.DataFrame,
        ts_code: str,
        metric_name: str
    ) -> Dict[str, Any]:
        """
        提取单个公司的指标数据

        Returns:
            包含趋势、波动性、周期性等信息的字典
        """
        if df is None or df.empty:
            return {}

        row = df[df['ts_code'] == ts_code]
        if row.empty:
            return {}

        row = row.iloc[0]

        return {
            'metric_name': metric_name,
            'ts_code': ts_code,
            # 趋势信息
            'log_slope': row.get('log_slope', 0.0),
            'r_squared': row.get('r_squared', 0.0),
            'cagr': row.get('cagr', 0.0),
            # 波动性信息
            'cv': row.get('cv', 0.0),
            'detrended_cv': row.get('detrended_cv', 0.0),
            # 周期性信息
            'is_cyclical': row.get('is_cyclical', False),
            'cyclical_confidence': row.get('cyclical_confidence', 0.0),
            'peak_trough_ratio': row.get('peak_trough_ratio', 1.0),
            # 衰退信息
            'deterioration_prob': row.get('deterioration_probability', 0.0),
            'has_deterioration': row.get('has_deterioration', False),
            # 近期动态
            'recent_trend': row.get('recent_3y_slope', 0.0),
            'momentum': row.get('trend_acceleration', 0.0),
            # 原始值（如果有）
            'mean_value': row.get('mean_value', 0.0),
            'latest_value': row.get('latest_value', 0.0),
        }


# ============================================================================
# 专业基因-指标映射器
# ============================================================================

class ProfessionalGeneMapper:
    """
    专业基因-指标映射器

    实现专业的映射关系：
    - α (周期性): 效率指标 (ROIC, ROE)，使用max聚合
    - β (资本密度): ROIC单一来源
    - γ (成长动能): 增长指标 (营收, 利润)，使用调和平均
    - δ_fraud (欺诈熵): 利润vs现金流，逻辑OR
    - δ_decay (衰退熵): 所有效率指标，使用max
    - V (验证): 现金流单一来源
    """

    def __init__(self, config: TruthConfig = None):
        self.config = config or get_default_truth_config()

    def extract_alpha(
        self,
        roic_data: Dict[str, Any],
        roe_data: Dict[str, Any],
    ) -> GeneExtractionResult:
        """
        提取 α (周期性) 基因

        来源: ROIC, ROE (效率指标)
        聚合: max - 选择周期性最强的
        逻辑: 公司周期性由最强的周期性指标决定
        """
        alpha_values = []
        source_indicators = []
        breakdown = {}

        # 从 ROIC 提取 α
        if roic_data:
            roic_alpha = self._compute_single_alpha(roic_data)
            alpha_values.append(roic_alpha)
            source_indicators.append('ROIC')
            breakdown['roic_alpha'] = roic_alpha

        # 从 ROE 提取 α
        if roe_data:
            roe_alpha = self._compute_single_alpha(roe_data)
            alpha_values.append(roe_alpha)
            source_indicators.append('ROE')
            breakdown['roe_alpha'] = roe_alpha

        if not alpha_values:
            return GeneExtractionResult(
                gene_name='alpha',
                value=0.5,
                source_indicators=[],
                aggregation_method='default',
                is_degraded=True,
                degradation_reason='缺少效率指标数据'
            )

        # max 聚合：周期性最强的那个
        final_alpha = max(alpha_values)
        breakdown['aggregation'] = 'max'
        breakdown['final'] = final_alpha

        return GeneExtractionResult(
            gene_name='alpha',
            value=final_alpha,
            source_indicators=source_indicators,
            aggregation_method='max',
            breakdown=breakdown
        )

    def _compute_single_alpha(self, data: Dict[str, Any]) -> float:
        """从单个指标计算 α 值"""
        # 核心指标：去趋势CV, 周期置信度, R²
        detrended_cv = float(data.get('detrended_cv', 0.0))
        cyclical_conf = float(data.get('cyclical_confidence', 0.0))
        r_squared = float(data.get('r_squared', 0.5))
        pt_ratio = float(data.get('peak_trough_ratio', 1.0))

        # v2.0 Signal Fusion 公式（简化版）
        # α = 0.4×CV_norm + 0.3×P_cyc + 0.2×(1-R²) + 0.1×PT_norm
        cv_norm = min(detrended_cv * 2, 1.0)  # CV饱和在0.5
        pt_norm = min((pt_ratio - 1) / 3, 1.0)  # PT饱和在4
        low_r2 = 1 - r_squared

        alpha = (
            0.4 * cv_norm +
            0.3 * cyclical_conf +
            0.2 * low_r2 +
            0.1 * pt_norm
        )

        return max(0.0, min(1.0, alpha))

    def extract_beta(
        self,
        roic_data: Dict[str, Any],
        ocf_data: Dict[str, Any] = None,
        profit_data: Dict[str, Any] = None,
        revenue_data: Dict[str, Any] = None,
    ) -> GeneExtractionResult:
        """
        提取 β (资本密度) 基因

        来源: ROIC (主), OCF波动性, 利润/营收DOL
        逻辑: 检测"隐性重资产"
        """
        breakdown = {}
        source_indicators = ['ROIC']

        if not roic_data:
            return GeneExtractionResult(
                gene_name='beta',
                value=0.5,
                source_indicators=[],
                aggregation_method='default',
                is_degraded=True,
                degradation_reason='缺少ROIC数据'
            )

        # 1. ROIC波动性 (40%)
        roic_cv = float(roic_data.get('detrended_cv', 0.0))
        vol_score = min(roic_cv / 0.15, 1.0)  # 饱和在15%
        breakdown['roic_volatility'] = roic_cv
        breakdown['vol_score'] = vol_score

        # 2. OCF波动性 (30%)
        ocf_cv = float(ocf_data.get('cv', 0.0)) if ocf_data else 0.3
        ocf_score = min(ocf_cv / 0.5, 1.0)  # OCF波动性更高
        breakdown['ocf_cv'] = ocf_cv
        breakdown['ocf_score'] = ocf_score
        if ocf_data:
            source_indicators.append('OCF')

        # 3. DOL 经营杠杆检测 (30%)
        # 利润波动/营收波动 > 1 表示高经营杠杆
        dol_score = 0.5
        if profit_data and revenue_data:
            profit_cv = float(profit_data.get('cv', 0.0))
            revenue_cv = float(revenue_data.get('cv', 0.01))
            if revenue_cv > 0.001:
                dol = profit_cv / revenue_cv
                dol_score = min(dol / 3, 1.0)  # DOL > 3 视为高杠杆
                breakdown['dol'] = dol
                source_indicators.extend(['Profit', 'Revenue'])
        breakdown['dol_score'] = dol_score

        # 加权计算
        beta = 0.4 * vol_score + 0.3 * ocf_score + 0.3 * dol_score
        breakdown['final'] = beta

        return GeneExtractionResult(
            gene_name='beta',
            value=max(0.0, min(1.0, beta)),
            source_indicators=source_indicators,
            aggregation_method='weighted',
            breakdown=breakdown
        )

    def extract_gamma(
        self,
        revenue_data: Dict[str, Any],
        profit_data: Dict[str, Any],
        ocf_data: Dict[str, Any] = None,
    ) -> GeneExtractionResult:
        """
        提取 γ (成长动能) 基因

        来源: 营收, 利润, OCF (增长指标)
        聚合: 调和平均 - 惩罚不平衡增长
        逻辑: 真正的增长应该是营收/利润/现金流同步
        """
        growth_values = []
        source_indicators = []
        breakdown = {}

        # 营收增长 (权重0.4)
        if revenue_data:
            rev_gamma = self._compute_single_gamma(revenue_data)
            growth_values.append(('revenue', rev_gamma, 0.4))
            source_indicators.append('Revenue')
            breakdown['revenue_gamma'] = rev_gamma

        # 利润增长 (权重0.4)
        if profit_data:
            profit_gamma = self._compute_single_gamma(profit_data)
            growth_values.append(('profit', profit_gamma, 0.4))
            source_indicators.append('Profit')
            breakdown['profit_gamma'] = profit_gamma

        # 现金流增长 (权重0.2)
        if ocf_data:
            ocf_gamma = self._compute_single_gamma(ocf_data)
            growth_values.append(('ocf', ocf_gamma, 0.2))
            source_indicators.append('OCF')
            breakdown['ocf_gamma'] = ocf_gamma

        if not growth_values:
            return GeneExtractionResult(
                gene_name='gamma',
                value=0.3,
                source_indicators=[],
                aggregation_method='default',
                is_degraded=True,
                degradation_reason='缺少增长指标数据'
            )

        # 调和平均（惩罚不平衡）
        # 如果只有部分数据，重新归一化权重
        total_weight = sum(w for _, _, w in growth_values)
        weighted_sum = sum(v * (w / total_weight) for _, v, w in growth_values)

        # 计算一致性惩罚
        values = [v for _, v, _ in growth_values]
        if len(values) >= 2:
            consistency = 1 - np.std(values)  # 标准差越小，一致性越好
            final_gamma = weighted_sum * (0.8 + 0.2 * consistency)
        else:
            final_gamma = weighted_sum

        breakdown['weighted_avg'] = weighted_sum
        breakdown['consistency'] = consistency if len(values) >= 2 else 1.0
        breakdown['final'] = final_gamma
        breakdown['aggregation'] = 'harmonic_weighted'

        return GeneExtractionResult(
            gene_name='gamma',
            value=max(0.0, min(1.0, final_gamma)),
            source_indicators=source_indicators,
            aggregation_method='harmonic_weighted',
            breakdown=breakdown
        )

    def _compute_single_gamma(self, data: Dict[str, Any]) -> float:
        """从单个指标计算 γ 值"""
        cagr = float(data.get('cagr', 0.0))
        recent_trend = float(data.get('recent_trend', 0.0))
        momentum = float(data.get('momentum', 0.0))

        # 映射 CAGR 到 [0, 1]
        # -10% → 0.1, 0% → 0.3, 15% → 0.6, 30% → 0.9
        if cagr <= -0.1:
            base_gamma = 0.1
        elif cagr <= 0:
            base_gamma = 0.1 + 0.2 * ((cagr + 0.1) / 0.1)
        elif cagr <= 0.15:
            base_gamma = 0.3 + 0.3 * (cagr / 0.15)
        elif cagr <= 0.30:
            base_gamma = 0.6 + 0.3 * ((cagr - 0.15) / 0.15)
        else:
            base_gamma = 0.9 + 0.1 * min((cagr - 0.30) / 0.20, 1)

        # 动量调整 (±10%)
        momentum_adj = 0.1 * np.sign(momentum) * min(abs(momentum), 1.0)

        return max(0.0, min(1.0, base_gamma + momentum_adj))

    def extract_delta_fraud(
        self,
        profit_data: Dict[str, Any],
        ocf_data: Dict[str, Any],
        roic_data: Dict[str, Any] = None,
        roe_data: Dict[str, Any] = None,
    ) -> GeneExtractionResult:
        """
        提取 δ_fraud (欺诈熵) 基因

        来源: 利润 vs 现金流, ROE vs ROIC
        聚合: 逻辑OR - 任一异常即触发
        逻辑: 欺诈信号是"或"关系，一处可疑就需警惕
        """
        fraud_signals = []
        breakdown = {}
        source_indicators = []

        # 1. 利润 vs 现金流 背离 (最重要)
        if profit_data and ocf_data:
            profit_cagr = float(profit_data.get('cagr', 0.0))
            ocf_cagr = float(ocf_data.get('cagr', 0.0))

            # 利润增长但现金流下降 = 红旗
            if profit_cagr > 0.05 and ocf_cagr < -0.05:
                divergence = profit_cagr - ocf_cagr
                fraud_score = min(divergence / 0.3, 1.0)  # 30%背离满分
                fraud_signals.append(fraud_score)
                breakdown['profit_ocf_divergence'] = divergence
                breakdown['profit_ocf_fraud'] = fraud_score
            source_indicators.extend(['Profit', 'OCF'])

        # 2. ROE vs ROIC 背离 (杠杆操纵)
        if roe_data and roic_data:
            roe_val = float(roe_data.get('mean_value', 0.15))
            roic_val = float(roic_data.get('mean_value', 0.10))

            # ROE >> ROIC 说明高杠杆驱动
            if roe_val > roic_val * 2 and roe_val > 0.15:
                leverage_signal = min((roe_val / roic_val - 1) / 2, 1.0)
                fraud_signals.append(leverage_signal * 0.5)  # 权重较低
                breakdown['leverage_signal'] = leverage_signal
            source_indicators.extend(['ROE', 'ROIC'])

        # 3. 趋势一致性检查
        # 如果利润趋势和现金流趋势方向相反超过3年
        if profit_data and ocf_data:
            profit_slope = float(profit_data.get('log_slope', 0.0))
            ocf_slope = float(ocf_data.get('log_slope', 0.0))

            if (profit_slope > 0.05 and ocf_slope < -0.05) or \
               (profit_slope < -0.05 and ocf_slope > 0.05):
                trend_divergence = abs(profit_slope - ocf_slope)
                fraud_signals.append(min(trend_divergence / 0.2, 1.0) * 0.3)
                breakdown['trend_divergence'] = trend_divergence

        # 逻辑OR聚合：取最大值
        if fraud_signals:
            final_fraud = max(fraud_signals)
        else:
            final_fraud = 0.0

        breakdown['fraud_signals'] = fraud_signals
        breakdown['final'] = final_fraud
        breakdown['aggregation'] = 'logical_or_max'

        return GeneExtractionResult(
            gene_name='delta_fraud',
            value=max(0.0, min(1.0, final_fraud)),
            source_indicators=source_indicators,
            aggregation_method='logical_or_max',
            breakdown=breakdown
        )

    def extract_delta_decay(
        self,
        roic_data: Dict[str, Any],
        roe_data: Dict[str, Any],
        gross_margin_data: Dict[str, Any],
        net_margin_data: Dict[str, Any],
    ) -> GeneExtractionResult:
        """
        提取 δ_decay (衰退熵) 基因

        来源: 所有效率指标
        聚合: max - 最严重的衰退信号
        逻辑: 一处衰退可能蔓延全局
        """
        decay_values = []
        breakdown = {}
        source_indicators = []

        for name, data in [
            ('ROIC', roic_data),
            ('ROE', roe_data),
            ('GrossMargin', gross_margin_data),
            ('NetMargin', net_margin_data),
        ]:
            if data:
                decay = self._compute_single_decay(data)
                decay_values.append(decay)
                source_indicators.append(name)
                breakdown[f'{name.lower()}_decay'] = decay

        if not decay_values:
            return GeneExtractionResult(
                gene_name='delta_decay',
                value=0.3,
                source_indicators=[],
                aggregation_method='default',
                is_degraded=True,
                degradation_reason='缺少效率指标数据'
            )

        # max 聚合：最严重的衰退
        final_decay = max(decay_values)
        breakdown['max_decay'] = final_decay
        breakdown['aggregation'] = 'max'

        return GeneExtractionResult(
            gene_name='delta_decay',
            value=max(0.0, min(1.0, final_decay)),
            source_indicators=source_indicators,
            aggregation_method='max',
            breakdown=breakdown
        )

    def _compute_single_decay(self, data: Dict[str, Any]) -> float:
        """从单个指标计算衰退分数"""
        deterioration_prob = float(data.get('deterioration_prob', 0.0))
        has_deterioration = data.get('has_deterioration', False)
        recent_trend = float(data.get('recent_trend', 0.0))
        log_slope = float(data.get('log_slope', 0.0))

        # 综合衰退信号
        decay_score = 0.0

        # 恶化概率 (50%)
        decay_score += 0.5 * deterioration_prob

        # 近期趋势下行 (30%)
        if recent_trend < 0:
            decay_score += 0.3 * min(abs(recent_trend) / 0.1, 1.0)

        # 长期斜率下行 (20%)
        if log_slope < 0:
            decay_score += 0.2 * min(abs(log_slope) / 0.1, 1.0)

        return decay_score

    def extract_verification(
        self,
        ocf_data: Dict[str, Any],
        profit_data: Dict[str, Any] = None,
    ) -> GeneExtractionResult:
        """
        提取 V (验证因子) 基因

        来源: 现金流
        逻辑: 现金是最终的验证
        """
        breakdown = {}

        if not ocf_data:
            return GeneExtractionResult(
                gene_name='verification',
                value=0.5,
                source_indicators=[],
                aggregation_method='default',
                is_degraded=True,
                degradation_reason='缺少现金流数据'
            )

        # OCF趋势
        ocf_slope = float(ocf_data.get('log_slope', 0.0))
        ocf_cagr = float(ocf_data.get('cagr', 0.0))

        # 现金流质量
        v_score = 0.5  # 基准

        # 正向现金流趋势加分
        if ocf_slope > 0:
            v_score += 0.3 * min(ocf_slope / 0.1, 1.0)
        else:
            v_score -= 0.3 * min(abs(ocf_slope) / 0.1, 1.0)

        # CAGR调整
        if ocf_cagr > 0:
            v_score += 0.2 * min(ocf_cagr / 0.15, 1.0)

        breakdown['ocf_slope'] = ocf_slope
        breakdown['ocf_cagr'] = ocf_cagr
        breakdown['v_score'] = v_score

        # 利润/现金流比率（如果有利润数据）
        if profit_data:
            profit_cagr = float(profit_data.get('cagr', 0.0))
            if abs(profit_cagr) > 0.01:
                cash_ratio = ocf_cagr / profit_cagr if profit_cagr != 0 else 1.0
                # 现金增长 >= 利润增长 是好的
                if cash_ratio >= 1:
                    v_score = min(v_score + 0.1, 1.0)
                breakdown['cash_profit_ratio'] = cash_ratio

        breakdown['final'] = v_score

        return GeneExtractionResult(
            gene_name='verification',
            value=max(0.0, min(1.0, v_score)),
            source_indicators=['OCF'],
            aggregation_method='single_source',
            breakdown=breakdown
        )


# ============================================================================
# 因果网络验证器
# ============================================================================

class CausalNetworkValidator:
    """
    因果网络验证器

    验证指标之间的因果关系：
    - 营收增长 → 利润增长
    - 利润增长 → 现金流增长
    - ROIC → ROE (排除杠杆虚假)
    - 周期性传导
    """

    def validate(
        self,
        revenue_data: Dict[str, Any],
        profit_data: Dict[str, Any],
        ocf_data: Dict[str, Any],
        roic_data: Dict[str, Any],
        roe_data: Dict[str, Any],
    ) -> CausalValidation:
        """验证因果关系"""
        warnings = []
        scores = []

        # 1. 营收 → 利润
        rev_profit_consistent = True
        if revenue_data and profit_data:
            rev_cagr = float(revenue_data.get('cagr', 0.0))
            profit_cagr = float(profit_data.get('cagr', 0.0))

            # 营收增长但利润不增长（可能存在问题）
            if rev_cagr > 0.1 and profit_cagr < 0:
                rev_profit_consistent = False
                warnings.append(f"⚠️ 因果异常：营收增长{rev_cagr:.1%}但利润下滑{profit_cagr:.1%}")
            scores.append(1.0 if rev_profit_consistent else 0.3)

        # 2. 利润 → 现金流
        profit_ocf_consistent = True
        if profit_data and ocf_data:
            profit_cagr = float(profit_data.get('cagr', 0.0))
            ocf_cagr = float(ocf_data.get('cagr', 0.0))

            # 利润增长但现金流下降（欺诈信号）
            if profit_cagr > 0.1 and ocf_cagr < -0.05:
                profit_ocf_consistent = False
                warnings.append(f"🚨 因果断裂：利润增长{profit_cagr:.1%}但现金流下滑{ocf_cagr:.1%}")
            scores.append(1.0 if profit_ocf_consistent else 0.2)

        # 3. ROIC → ROE
        roe_roic_consistent = True
        if roic_data and roe_data:
            roic_val = float(roic_data.get('mean_value', 0.1))
            roe_val = float(roe_data.get('mean_value', 0.15))

            # ROE远高于ROIC（高杠杆警告）
            if roic_val > 0 and roe_val > roic_val * 2.5:
                roe_roic_consistent = False
                warnings.append(f"⚠️ 杠杆风险：ROE={roe_val:.1%}远高于ROIC={roic_val:.1%}")
            scores.append(1.0 if roe_roic_consistent else 0.5)

        # 4. 周期性传导
        cycle_transmission = True
        if roic_data and revenue_data:
            roic_cyclical = roic_data.get('is_cyclical', False)
            rev_cyclical = revenue_data.get('is_cyclical', False)

            # ROIC周期但营收不周期（可能数据问题）
            if roic_cyclical and not rev_cyclical:
                cycle_transmission = False
                warnings.append("⚠️ 周期异常：ROIC显示周期性但营收不显示")
            scores.append(1.0 if cycle_transmission else 0.7)

        # 综合得分
        overall_score = np.mean(scores) if scores else 0.5

        return CausalValidation(
            revenue_profit_consistent=rev_profit_consistent,
            profit_ocf_consistent=profit_ocf_consistent,
            roe_roic_consistent=roe_roic_consistent,
            cycle_transmission=cycle_transmission,
            overall_score=overall_score,
            warnings=warnings
        )


# ============================================================================
# T.R.U.T.H. 处理器主类
# ============================================================================

class TruthProcessor:
    """
    T.R.U.T.H. 处理器

    核心处理流程（专业版）：
    1. DataFrame → ProbeOutputs → MultiIndicatorProbeOutputs（使用 DataFrameToProbeConverter）
    2. MultiIndicatorProbeOutputs → GenomeInput（使用 ProbeAdapter.adapt()）
    3. GenomeInput → CompanyGenome（使用 compute_genome_from_probes()）
    4. 运行3大求解器
    5. 执行因果网络验证
    6. 生成综合评估结果

    关键：使用 core/genes/ 下的专业基因计算函数，而非简化版本
    """

    def __init__(self, config: TruthConfig = None):
        self.config = config or get_default_truth_config()
        self.df_converter = DataFrameToProbeConverter()  # DataFrame → ProbeOutputs
        self.probe_adapter = ProbeAdapter()              # ProbeOutputs → GenomeInput
        # 保留旧组件兼容
        self.gene_mapper = ProfessionalGeneMapper(self.config)
        self.causal_validator = CausalNetworkValidator()
        self.extractor = IndicatorExtractor()

    def process_company(
        self,
        ts_code: str,
        probe_data: Dict[str, pd.DataFrame],
        company_name: str = "",
    ) -> TruthProcessResult:
        """
        处理单个公司（专业版入口）

        使用完整的专业处理链：
        DataFrame → ProbeOutputs → GenomeInput → compute_genome_from_probes → CompanyGenome

        Args:
            ts_code: 股票代码
            probe_data: 8个探针的DataFrame字典
            company_name: 公司名称

        Returns:
            TruthProcessResult: 完整处理结果
        """
        result = TruthProcessResult(ts_code=ts_code, company_name=company_name)

        try:
            # Step 1: DataFrame → MultiIndicatorProbeOutputs
            multi_probe_outputs = self._convert_to_multi_probe_outputs(
                ts_code, probe_data, company_name
            )

            if multi_probe_outputs is None:
                result.warnings.append("⚠️ 无法提取探针数据")
                result.processing_notes.append("DataFrame → ProbeOutputs 转换失败")
                # 回退到旧版处理
                return self._process_company_fallback(ts_code, probe_data, company_name)

            result.processing_notes.append("✓ DataFrame → ProbeOutputs 完成")

            # Step 2: MultiIndicatorProbeOutputs → GenomeInput (via ProbeAdapter)
            genome_input = self.probe_adapter.adapt(multi_probe_outputs)
            result.processing_notes.append("✓ ProbeAdapter.adapt() 完成")

            # Step 3: GenomeInput → CompanyGenome (via compute_genome_from_probes)
            # 这里调用的是 core/genes/genome_assembler.py 中的专业函数
            genome = compute_genome_from_probes(genome_input, self.config)
            result.genome = genome
            result.processing_notes.append("✓ compute_genome_from_probes() 完成 [专业基因计算]")

            # Step 4: 运行三大求解器
            self._run_solvers_professional(result, probe_data)
            result.processing_notes.append("✓ 三大求解器执行完成")

            # Step 5: 提取旧版指标数据用于因果验证
            indicator_data = self._extract_all_indicators(ts_code, probe_data)
            if indicator_data:
                causal = self.causal_validator.validate(
                    revenue_data=indicator_data.get('revenue', {}),
                    profit_data=indicator_data.get('profit', {}),
                    ocf_data=indicator_data.get('ocf', {}),
                    roic_data=indicator_data.get('roic', {}),
                    roe_data=indicator_data.get('roe', {}),
                )
                result.causal_validation = causal
                result.warnings.extend(causal.warnings)

            # Step 6: 综合评分
            self._compute_final_assessment(result)

        except Exception as e:
            logger.error(f"处理公司 {ts_code} 失败: {e}")
            result.warnings.append(f"处理异常: {str(e)}")
            result.processing_notes.append(f"✗ 处理失败: {e}")
            # 尝试回退处理
            return self._process_company_fallback(ts_code, probe_data, company_name)

        return result

    def _convert_to_multi_probe_outputs(
        self,
        ts_code: str,
        probe_data: Dict[str, pd.DataFrame],
        company_name: str,
    ) -> Optional[MultiIndicatorProbeOutputs]:
        """
        将 DataFrame 字典转换为 MultiIndicatorProbeOutputs

        这是连接 Pipeline 和 T.R.U.T.H. 系统的关键桥梁
        """
        # 创建多指标探针输出对象
        multi_outputs = MultiIndicatorProbeOutputs(
            company_code=ts_code,
            company_name=company_name,
        )

        # 指标映射：DataFrame键 → MultiIndicatorProbeOutputs属性
        indicator_mapping = {
            'roic': 'roic',
            'gross_margin': 'gross_margin',
            'revenue': 'revenue',
            'ocf': 'ocf',
            'profit': 'net_profit',
        }

        for data_key, attr_name in indicator_mapping.items():
            df = probe_data.get(data_key)
            if df is None or df.empty:
                continue

            # 查找该公司的行
            if 'ts_code' in df.columns:
                row = df[df['ts_code'] == ts_code]
            elif df.index.name == 'ts_code':
                row = df.loc[[ts_code]] if ts_code in df.index else pd.DataFrame()
            else:
                continue

            if row.empty:
                continue

            # 转换为 ProbeOutputs
            row_series = row.iloc[0]
            probe_outputs = self.df_converter.convert_row_to_probe_outputs(
                row_series, data_key
            )

            # 设置到对应属性
            if hasattr(multi_outputs, attr_name):
                setattr(multi_outputs, attr_name, probe_outputs)

        # 检查是否有足够数据（至少需要 ROIC）
        if multi_outputs.roic is None:
            logger.warning(f"公司 {ts_code} 缺少 ROIC 数据")
            return None

        return multi_outputs

    def _run_solvers_professional(
        self,
        result: TruthProcessResult,
        probe_data: Dict[str, pd.DataFrame],
    ) -> None:
        """运行三大求解器（专业版）"""
        genome = result.genome
        if genome is None:
            return

        ts_code = result.ts_code

        # 1. 重力求解器 (ROIC阈值)
        gravity_result = gravity_solver(genome, self.config)
        result.solver_results['gravity'] = SolverExecutionResult(
            solver_name='gravity',
            result=gravity_result,
            input_genes={'alpha': genome.alpha, 'beta': genome.beta},
            interpretation=f"动态ROIC阈值: {gravity_result.final_threshold:.1%}" if gravity_result else ""
        )

        # 2. 速度求解器 (增长边界)
        velocity_result = velocity_solver(genome, self.config)
        result.solver_results['velocity'] = SolverExecutionResult(
            solver_name='velocity',
            result=velocity_result,
            input_genes={'gamma': genome.gamma},
            interpretation=f"最大可持续增长: {velocity_result.max_sustainable_growth:.1%}" if velocity_result else ""
        )

        # 3. 结构求解器 (斜率预测)
        # structure_solver 签名: (genome, config) -> SlopeResult
        structure_result = structure_solver(genome, self.config)

        # 生成警告信息
        has_warning = False
        warning_msg = ""
        if structure_result.expected_slope < -0.03:
            has_warning = True
            warning_msg = f"⚠️ 预期斜率: {structure_result.expected_slope:.1%}/年"
            if not structure_result.gate_passed:
                warning_msg = f"🚨 V因子熔断: 预期负斜率 {structure_result.expected_slope:.1%}"

        result.solver_results['structure'] = SolverExecutionResult(
            solver_name='structure',
            result=structure_result,
            input_genes={'delta_decay': genome.delta_decay, 'beta': genome.beta},
            interpretation=f"预期斜率: {structure_result.expected_slope:.1%}/年 ({structure_result.channel_name})" if structure_result else "未计算"
        )

        if has_warning:
            result.warnings.append(warning_msg)

    def _process_company_fallback(
        self,
        ts_code: str,
        probe_data: Dict[str, pd.DataFrame],
        company_name: str = "",
    ) -> TruthProcessResult:
        """
        回退处理（使用旧版 ProfessionalGeneMapper）

        当专业处理链失败时使用
        """
        result = TruthProcessResult(ts_code=ts_code, company_name=company_name)
        result.processing_notes.append("⚠️ 使用回退模式（旧版基因映射）")

        # 使用旧版提取逻辑
        indicator_data = self._extract_all_indicators(ts_code, probe_data)
        if not indicator_data:
            result.warnings.append("⚠️ 无法提取指标数据")
            return result

        # 使用旧版基因映射
        gene_extractions = self._extract_all_genes(indicator_data)
        result.gene_extractions = gene_extractions

        # 构建基因组
        genome = self._build_genome(ts_code, company_name, gene_extractions)
        result.genome = genome

        # 运行求解器（使用旧版）
        solver_results = self._run_solvers(genome, indicator_data)
        result.solver_results = solver_results

        # 因果验证
        causal = self.causal_validator.validate(
            revenue_data=indicator_data.get('revenue', {}),
            profit_data=indicator_data.get('profit', {}),
            ocf_data=indicator_data.get('ocf', {}),
            roic_data=indicator_data.get('roic', {}),
            roe_data=indicator_data.get('roe', {}),
        )
        result.causal_validation = causal
        result.warnings.extend(causal.warnings)

        self._compute_final_assessment(result)

        return result

    def _extract_all_indicators(
        self,
        ts_code: str,
        probe_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, Dict[str, Any]]:
        """提取所有指标数据"""
        indicator_mapping = {
            'roic': 'roic',
            'roe': 'roe',
            'roiic': 'roiic',
            'gross_margin': 'gross_margin',
            'net_margin': 'net_margin',
            'revenue': 'revenue',
            'profit': 'profit',
            'ocf': 'ocf',
        }

        result = {}
        for key, metric_name in indicator_mapping.items():
            df = probe_data.get(key)
            if df is not None and not df.empty:
                data = self.extractor.extract_for_company(df, ts_code, metric_name)
                if data:
                    result[key] = data

        return result

    def _extract_all_genes(
        self,
        indicator_data: Dict[str, Dict[str, Any]],
    ) -> Dict[str, GeneExtractionResult]:
        """提取所有六维基因"""
        genes = {}

        # α (周期性)
        genes['alpha'] = self.gene_mapper.extract_alpha(
            roic_data=indicator_data.get('roic', {}),
            roe_data=indicator_data.get('roe', {}),
        )

        # β (资本密度)
        genes['beta'] = self.gene_mapper.extract_beta(
            roic_data=indicator_data.get('roic', {}),
            ocf_data=indicator_data.get('ocf', {}),
            profit_data=indicator_data.get('profit', {}),
            revenue_data=indicator_data.get('revenue', {}),
        )

        # γ (成长动能)
        genes['gamma'] = self.gene_mapper.extract_gamma(
            revenue_data=indicator_data.get('revenue', {}),
            profit_data=indicator_data.get('profit', {}),
            ocf_data=indicator_data.get('ocf', {}),
        )

        # δ_fraud (欺诈熵)
        genes['delta_fraud'] = self.gene_mapper.extract_delta_fraud(
            profit_data=indicator_data.get('profit', {}),
            ocf_data=indicator_data.get('ocf', {}),
            roic_data=indicator_data.get('roic', {}),
            roe_data=indicator_data.get('roe', {}),
        )

        # δ_decay (衰退熵)
        genes['delta_decay'] = self.gene_mapper.extract_delta_decay(
            roic_data=indicator_data.get('roic', {}),
            roe_data=indicator_data.get('roe', {}),
            gross_margin_data=indicator_data.get('gross_margin', {}),
            net_margin_data=indicator_data.get('net_margin', {}),
        )

        # V (验证因子)
        genes['verification'] = self.gene_mapper.extract_verification(
            ocf_data=indicator_data.get('ocf', {}),
            profit_data=indicator_data.get('profit', {}),
        )

        return genes

    def _build_genome(
        self,
        ts_code: str,
        company_name: str,
        gene_extractions: Dict[str, GeneExtractionResult],
    ) -> CompanyGenome:
        """构建基因组对象"""
        # 计算数据质量
        degraded_count = sum(
            1 for g in gene_extractions.values() if g.is_degraded
        )
        data_quality = 1.0 - (degraded_count * 0.15)

        return CompanyGenome(
            ts_code=ts_code,
            company_name=company_name,
            alpha=gene_extractions['alpha'].value,
            beta=gene_extractions['beta'].value,
            gamma=gene_extractions['gamma'].value,
            delta_fraud=gene_extractions['delta_fraud'].value,
            delta_decay=gene_extractions['delta_decay'].value,
            verification=gene_extractions['verification'].value,
            data_quality_score=data_quality,
        )

    def _run_solvers(
        self,
        genome: CompanyGenome,
        indicator_data: Dict[str, Dict[str, Any]],
    ) -> Dict[str, SolverExecutionResult]:
        """运行三大求解器"""
        results = {}

        # 1. 重力求解器 (ROIC阈值)
        gravity_result = gravity_solver(genome, self.config)
        results['gravity'] = SolverExecutionResult(
            solver_name='gravity',
            result=gravity_result,
            input_genes={'alpha': genome.alpha, 'beta': genome.beta},
            interpretation=f"动态ROIC阈值: {gravity_result.final_threshold:.1%}"
        )

        # 2. 速度求解器 (增长边界)
        velocity_result = velocity_solver(genome, self.config)
        results['velocity'] = SolverExecutionResult(
            solver_name='velocity',
            result=velocity_result,
            input_genes={'gamma': genome.gamma},
            interpretation=f"最大可持续增长: {velocity_result.max_sustainable_growth:.1%}"
        )

        # 3. 结构求解器 (斜率预测)
        # structure_solver 签名: (genome, config) -> SlopeResult
        structure_result = structure_solver(genome, self.config)

        # 生成警告
        has_warning = structure_result.expected_slope < -0.03
        warning_msg = f"预期斜率: {structure_result.expected_slope:.1%}/年"

        results['structure'] = SolverExecutionResult(
            solver_name='structure',
            result=structure_result,
            input_genes={'delta_decay': genome.delta_decay, 'beta': genome.beta},
            interpretation=warning_msg if has_warning else "斜率稳定"
        )

        return results

    def _compute_final_assessment(self, result: TruthProcessResult) -> None:
        """计算最终评估"""
        genome = result.genome
        if not genome:
            result.final_score = 0.0
            result.signal = "ERROR"
            result.grade = "N/A"
            return

        # 基础分数计算
        # 正向因子：γ, V
        # 负向因子：α, β, δ_fraud, δ_decay

        positive_score = (
            0.4 * genome.gamma +
            0.3 * genome.verification +
            0.3 * (1 - genome.alpha) * (1 - genome.beta)  # 低周期低资本密度
        )

        negative_score = (
            0.4 * genome.delta_fraud +
            0.4 * genome.delta_decay +
            0.2 * genome.alpha * genome.beta  # 高周期高资本密度
        )

        # 综合分数
        raw_score = positive_score - 0.5 * negative_score

        # 因果一致性调整
        if result.causal_validation:
            causal_adj = result.causal_validation.overall_score
            raw_score = raw_score * (0.8 + 0.2 * causal_adj)

        result.final_score = max(0.0, min(1.0, raw_score))

        # 信号判定
        gravity_threshold = 0.0
        if 'gravity' in result.solver_results:
            gravity_result = result.solver_results['gravity'].result
            gravity_threshold = gravity_result.final_threshold

        if genome.delta_fraud > 0.7:
            result.signal = "FRAUD_RISK"
            result.grade = "D"
        elif genome.delta_decay > 0.7:
            result.signal = "DECAY_WARNING"
            result.grade = "C"
        elif result.final_score > 0.7:
            result.signal = "STRONG_BUY"
            result.grade = "A"
        elif result.final_score > 0.5:
            result.signal = "BUY"
            result.grade = "B"
        elif result.final_score > 0.3:
            result.signal = "NEUTRAL"
            result.grade = "C"
        else:
            result.signal = "SELL"
            result.grade = "D"

        # 数据质量
        result.data_quality_score = genome.data_quality_score

    def process_batch(
        self,
        probe_data: Dict[str, pd.DataFrame],
    ) -> BatchProcessResult:
        """
        批量处理所有公司

        Args:
            probe_data: 8个探针的DataFrame字典

        Returns:
            BatchProcessResult: 批量处理结果
        """
        # 获取所有公司代码
        all_codes = set()
        for df in probe_data.values():
            if df is not None and not df.empty and 'ts_code' in df.columns:
                all_codes.update(df['ts_code'].unique())

        results = []
        stats = {'total': len(all_codes), 'success': 0, 'failed': 0}

        for ts_code in all_codes:
            try:
                result = self.process_company(ts_code, probe_data)
                results.append(result)
                stats['success'] += 1
            except Exception as e:
                logger.error(f"处理公司 {ts_code} 失败: {e}")
                stats['failed'] += 1

        # 生成摘要
        summary = {
            'total_companies': len(results),
            'signal_distribution': {},
            'grade_distribution': {},
            'avg_score': np.mean([r.final_score for r in results]) if results else 0,
        }

        for result in results:
            signal = result.signal
            grade = result.grade
            summary['signal_distribution'][signal] = summary['signal_distribution'].get(signal, 0) + 1
            summary['grade_distribution'][grade] = summary['grade_distribution'].get(grade, 0) + 1

        return BatchProcessResult(
            results=results,
            summary=summary,
            processing_stats=stats
        )

    def get_results_dataframe(
        self,
        batch_result: BatchProcessResult,
    ) -> pd.DataFrame:
        """
        将批量处理结果转换为DataFrame

        方便后续报告生成器使用
        """
        records = []

        for r in batch_result.results:
            record = {
                'ts_code': r.ts_code,
                'company_name': r.company_name,
                # 六维基因
                'alpha': r.genome.alpha if r.genome else None,
                'beta': r.genome.beta if r.genome else None,
                'gamma': r.genome.gamma if r.genome else None,
                'delta_fraud': r.genome.delta_fraud if r.genome else None,
                'delta_decay': r.genome.delta_decay if r.genome else None,
                'verification': r.genome.verification if r.genome else None,
                # 评估结果
                'final_score': r.final_score,
                'signal': r.signal,
                'grade': r.grade,
                'data_quality': r.data_quality_score,
                # 因果验证
                'causal_score': r.causal_validation.overall_score if r.causal_validation else None,
                # 警告数
                'warning_count': len(r.warnings),
            }

            # 添加求解器结果
            if 'gravity' in r.solver_results:
                gravity = r.solver_results['gravity'].result
                record['roic_threshold'] = gravity.final_threshold

            if 'velocity' in r.solver_results:
                velocity = r.solver_results['velocity'].result
                record['max_sustainable_growth'] = velocity.max_sustainable_growth

            records.append(record)

        return pd.DataFrame(records)
