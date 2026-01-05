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

from shared.naming_convention import METRIC_PREFIX_MAP as NAMING_METRIC_PREFIX_MAP

from .config import TruthConfig, get_default_truth_config
from .models import CompanyGenome, TruthResult
# 导入探针结果模型（从trend/models.py）
from ..trend.models import (
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

    def __init__(self):
        """初始化转换器，强制使用统一命名规范并进行自检"""
        self.metric_prefix_map = NAMING_METRIC_PREFIX_MAP

        # 自检：确保所有关键指标在命名映射中存在
        required_metrics = (
            'roic',
            'roe',
            'roiic',
            'gross_margin',
            'net_margin',
            'revenue',
            'profit',
            'ocf',
        )
        missing = [m for m in required_metrics if m not in self.metric_prefix_map]
        if missing:
            raise KeyError(
                f"缺少必要的指标前缀映射: {missing}，"
                "请检查 shared.naming_convention.METRIC_PREFIX_MAP 配置。"
            )

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
        prefix = self.metric_prefix_map.get(metric_key, metric_key)

        # 创建各探针结果对象
        log_trend = self._build_log_trend(row, prefix)
        volatility = self._build_volatility(row, prefix)
        cyclical = self._build_cyclical(row, prefix)
        deterioration = self._build_deterioration(row, prefix)
        rolling = self._build_rolling(row, prefix)
        robust = self._build_robust(row, prefix)
        inflection = self._build_inflection(row, prefix)

        # 提取 raw_values（用于 delta_fraud 等需要最新值的场景）
        # 优先使用 latest_value 列构建单元素数组
        raw_values = None
        latest_value = self._safe_get(row, f'{prefix}_latest_value', None)
        if latest_value is not None and not pd.isna(latest_value):
            raw_values = np.array([float(latest_value)])

        return ProbeOutputs(
            indicator_name=metric_key,
            log_trend=log_trend,
            volatility=volatility,
            cyclical=cyclical,
            deterioration=deterioration,
            rolling=rolling,
            robust=robust,
            inflection=inflection,
            raw_values=raw_values,
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
                # 无法获取探针数据时直接返回错误结果
                self._compute_final_assessment(result)
                return result

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

            # Step 5: 综合评分
            self._compute_final_assessment(result)

        except Exception as e:
            logger.error(f"处理公司 {ts_code} 失败: {e}")
            result.warnings.append(f"处理异常: {str(e)}")
            result.processing_notes.append(f"✗ 处理失败: {e}")
            # 发生异常时不再回退到旧链路，直接给出错误评估结果
            self._compute_final_assessment(result)
            return result

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
        degradation_count = 0  # 统计有降级的公司数

        for ts_code in all_codes:
            try:
                result = self.process_company(ts_code, probe_data)
                results.append(result)
                stats['success'] += 1
                # 统计降级（通过warnings中包含降级相关信息判断）
                if result.genome and result.genome.data_quality_score < 1.0:
                    degradation_count += 1
            except Exception as e:
                logger.error(f"处理公司 {ts_code} 失败: {e}")
                stats['failed'] += 1

        # 生成摘要
        summary = {
            'total_companies': len(results),
            'signal_distribution': {},
            'grade_distribution': {},
            'avg_score': np.mean([r.final_score for r in results]) if results else 0,
            'degradation_count': degradation_count,
        }

        for result in results:
            signal = result.signal
            grade = result.grade
            summary['signal_distribution'][signal] = summary['signal_distribution'].get(signal, 0) + 1
            summary['grade_distribution'][grade] = summary['grade_distribution'].get(grade, 0) + 1

        # 汇总日志（避免逐个公司输出）
        if degradation_count > 0:
            logger.info(
                f"📊 批量处理完成: 共 {stats['total']} 家公司, "
                f"成功 {stats['success']}, 失败 {stats['failed']}, "
                f"存在数据降级 {degradation_count} 家"
            )

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
