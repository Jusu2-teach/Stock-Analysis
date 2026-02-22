"""
═══════════════════════════════════════════════════════════════════════════════
T.R.U.T.H. v3.0 - 六维基因测序 × 三大物理求解器
═══════════════════════════════════════════════════════════════════════════════

精简扁平架构:

    truth/
    ├── engine.py       # 主入口 (run_truth)
    ├── factors.py      # 6 个因子 (α/β/γ/δ_fraud/δ_decay/V)
    ├── solvers.py      # 3 个求解器 (Gravity/Velocity/Structure)
    ├── models.py       # 领域模型
    └── config.py       # 配置

设计理念:
    - 去标签化: 用数据驱动的六维基因描述公司特征
    - 动态阈值: 求解器输出阈值而非简单分数
    - 物理隐喻: 重力场/速度场/结构场

Pipeline 集成:
    - 输入: aggregated_trends (来自 PDDA)
    - 输出: profiles + summary

版本: 3.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, List, Mapping, Optional
import logging

import pandas as pd

from orchestrator.decorators.register import register_method

from .models import (
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
    DynamicThreshold,
)
from .factors import (
    AlphaFactor,
    BetaFactor,
    GammaFactor,
    DeltaFraudFactor,
    DeltaDecayFactor,
    VerificationFactor,
)
from .solvers import (
    GravitySolver,
    VelocitySolver,
    StructureSolver,
)
from .config import TruthConfig, ScoringConfig, get_default_config
from ..pdda_columns import PDDAColumns
from ..trend.probes.financial_context_probe import FinancialContextProbe

logger = logging.getLogger(__name__)


# ============================================================================
# 元数据列识别 (使用共享的 PDDAColumns)
# ============================================================================

def is_metadata_column(column_name: str) -> bool:
    """检查列是否为元数据"""
    return PDDAColumns.is_metadata(column_name)


# ============================================================================
# 数据转换
# ============================================================================

def _build_probes_from_dataframes(
    probe_frames: Mapping[str, pd.DataFrame],
) -> Dict[str, List[ProbeInput]]:
    """将 DataFrame 探针结果转换为 ProbeInput 列表"""
    probes_by_ts: Dict[str, List[ProbeInput]] = {}

    for probe_name, df in probe_frames.items():
        if df is None or df.empty:
            continue
        if "ts_code" not in df.columns:
            continue

        col_index = {c: i for i, c in enumerate(df.columns)}
        ts_idx = col_index.get('ts_code')
        if ts_idx is None:
            continue

        # 只保留数值型特征列
        feature_cols = [
            c for c in df.columns
            if c != "ts_code"
            and pd.api.types.is_numeric_dtype(df[c])
            and not is_metadata_column(c)
        ]
        feature_indices = [(c, col_index[c]) for c in feature_cols if c in col_index]

        for row in df.itertuples(index=False, name=None):
            ts_code = str(row[ts_idx])
            features = {}
            for col, idx in feature_indices:
                val = row[idx]
                if pd.notna(val):
                    features[col] = float(val)
            probe = ProbeInput(ts_code=ts_code, probe_name=probe_name, features=features)
            probes_by_ts.setdefault(ts_code, []).append(probe)

    return probes_by_ts


# ============================================================================
# Financial Context 探针集成
# ============================================================================

def _build_financial_context_probes(
    raw_financial_data: pd.DataFrame,
) -> Dict[str, ProbeInput]:
    """从原始资产负债表数据构建 financial_context 探针

    为每个 ts_code 取最新一期数据，调用 FinancialContextProbe.compute() 计算
    资产结构比率和风险标志，供 β 和 δ_fraud 因子使用。

    Args:
        raw_financial_data: 原始财务数据 DataFrame
            必须包含 ts_code 列，其余列为资产负债表字段
            (fix_assets, total_assets, goodwill, equity, ...)

    Returns:
        {ts_code: ProbeInput(probe_name="financial_context", features={...})}
    """
    if raw_financial_data is None or raw_financial_data.empty:
        return {}

    if "ts_code" not in raw_financial_data.columns:
        logger.warning("raw_financial_data 缺少 ts_code 列，跳过 financial_context")
        return {}

    probe = FinancialContextProbe()
    result_map: Dict[str, ProbeInput] = {}

    # 按 ts_code 分组，每组取最新一期（如有 end_date/ann_date 则按其排序）
    sort_cols = []
    for col in ["end_date", "ann_date", "f_ann_date"]:
        if col in raw_financial_data.columns:
            sort_cols.append(col)

    if sort_cols:
        df_sorted = raw_financial_data.sort_values(sort_cols, ascending=False)
    else:
        df_sorted = raw_financial_data

    # 按 ts_code 分组取第一行（即最新期）
    for ts_code, group in df_sorted.groupby("ts_code"):
        ts_code = str(ts_code)
        latest_row = group.iloc[0]

        # 构建财务数据字典
        financial_data: Dict[str, Any] = {}
        for col in latest_row.index:
            if col == "ts_code":
                continue
            val = latest_row[col]
            if pd.notna(val):
                try:
                    financial_data[col] = float(val)
                except (ValueError, TypeError):
                    # 非数值列（如日期字符串），跳过
                    pass

        if not financial_data:
            continue

        try:
            ctx_result = probe.compute(financial_data)
            features = ctx_result.to_features_dict()

            # 过滤掉 NaN/Inf
            clean_features = {
                k: v for k, v in features.items()
                if isinstance(v, (int, float)) and not (v != v) and abs(v) != float('inf')
            }

            if clean_features:
                result_map[ts_code] = ProbeInput(
                    ts_code=ts_code,
                    probe_name="financial_context",
                    features=clean_features,
                )
        except Exception as e:
            logger.warning(f"FinancialContextProbe failed for {ts_code}: {e}")

    logger.info(f"构建 financial_context 探针: {len(result_map)} 只股票")
    return result_map


# ============================================================================
# 实际值注入 (使 DynamicThreshold.passed 真正生效)
# ============================================================================

def _extract_actual_from_probes(
    probes: List[ProbeInput],
    metric: str,
    feature_suffix: str = "weighted_avg",
) -> Optional[float]:
    """从探针中提取某指标的实际值

    PDDA 探针特征命名: {metric}_{feature_suffix}
    例如: roic_weighted_avg, revenue_latest_value
    """
    for probe in probes:
        key = f"{metric}_{feature_suffix}"
        if key in probe.features:
            val = probe.features[key]
            if val is not None and not (val != val):  # not NaN
                return float(val)
    return None


def _inject_actual_values(
    solvers: Dict[SolverId, SolverResult],
    probes: List[ProbeInput],
) -> Dict[SolverId, SolverResult]:
    """注入实际观测值到动态阈值，使 DynamicThreshold.passed 生效

    - Gravity solver: 注入实际 ROIC (weighted_avg) 到 roic 阈值
    - Velocity solver: 注入实际 revenue 增长率到 growth 阈值
    - Structure solver: 注入实际 gross_margin 到 moat_width 阈值
    """
    updated: Dict[SolverId, SolverResult] = {}

    for sid, result in solvers.items():
        if not result.thresholds:
            updated[sid] = result
            continue

        new_thresholds: Dict[str, DynamicThreshold] = {}
        changed = False

        for th_name, th in result.thresholds.items():
            actual = None

            if sid == SolverId.GRAVITY and th_name == "roic":
                # Gravity: 实际 ROIC 加权平均值
                actual = _extract_actual_from_probes(probes, "roic", "weighted_avg")
                if actual is None:
                    actual = _extract_actual_from_probes(probes, "roic", "latest_value")

            elif sid == SolverId.VELOCITY:
                if th_name == "growth_ceiling":
                    # Velocity: 实际收入增长率 (latest_vs_weighted 近似年增长)
                    ratio = _extract_actual_from_probes(probes, "revenue", "latest_vs_weighted_ratio")
                    if ratio is not None:
                        actual = (ratio - 1.0) * 100.0  # 转为百分比
                    else:
                        slope = _extract_actual_from_probes(probes, "revenue", "weighted_avg")
                        if slope is not None:
                            actual = slope  # 已经是百分比形式

            elif sid == SolverId.STRUCTURE and th_name == "moat_width":
                # Structure: 用 gross_margin 水平作为护城河代理
                gm = _extract_actual_from_probes(probes, "gross_margin", "weighted_avg")
                if gm is not None:
                    # 毛利率 → 0-100 护城河分数 (30% → 50分, 60% → 80分)
                    actual = min(100.0, max(0.0, gm * 1.2 + 14.0))

            if actual is not None:
                new_thresholds[th_name] = replace(th, actual_value=actual)
                changed = True
            else:
                new_thresholds[th_name] = th

        if changed:
            updated[sid] = replace(result, thresholds=new_thresholds)
        else:
            updated[sid] = result

    return updated


# ============================================================================
# 核心处理逻辑 (直接实例化，无工厂)
# ============================================================================

def _estimate_data_years(probes: List[ProbeInput]) -> int:
    """从探针特征中估算可用数据年限

    【T-H2】使用 data_points / n_years 等特征推断数据时间跨度
    """
    year_indicators = []
    for probe in probes:
        for fname, fval in probe.features.items():
            if fname.endswith("_n_years") or fname == "n_years" or fname.endswith("_data_points"):
                if not (fval != fval) and fval > 0:  # not NaN and positive
                    year_indicators.append(fval)
    if year_indicators:
        return int(max(year_indicators))
    # 如果探针中没有 n_years 信息，默认假设 10 年
    return 10

def _process_single(ts_code: str, probes: List[ProbeInput], config: TruthConfig) -> TruthProfile:
    """处理单个股票的 TRUTH 分析"""
    all_warnings: List[TruthWarning] = []

    # ========== 数据年限估算（用于 CalibrationConfig）==========
    data_years = _estimate_data_years(probes)

    # ========== Layer 1: 计算 6 因子 ==========
    factors: Dict[FactorId, FactorResult] = {}
    factor_instances = [
        AlphaFactor(),
        BetaFactor(),
        GammaFactor(),
        DeltaFraudFactor(),
        DeltaDecayFactor(),
        VerificationFactor(),
    ]

    for factor in factor_instances:
        try:
            result, warnings = factor.evaluate(ts_code, probes, config)
            factors[factor.factor_id] = result
            all_warnings.extend(warnings)
        except Exception as e:
            all_warnings.append(TruthWarning(
                code=f"FACTOR_{factor.factor_id.name}_ERROR",
                level=WarningLevel.CRITICAL,
                title=f"{factor.factor_id.name} 因子计算失败",
                message=str(e),
                source="factor_calculator",
            ))

    # ========== 熔断检查 ==========
    is_meltdown = False
    delta_fraud = factors.get(FactorId.DELTA_FRAUD)
    if delta_fraud and delta_fraud.score > config.delta_fraud_config.meltdown_threshold:
        is_meltdown = True
        all_warnings.append(TruthWarning(
            code="MELTDOWN_TRIGGERED",
            level=WarningLevel.FATAL,
            title="🚨 T.R.U.T.H. 熔断",
            message=f"δ_fraud={delta_fraud.score:.3f} 超过阈值",
            source="meltdown_check",
        ))

    # ========== Layer 2: 计算 3 求解器 ==========
    solvers: Dict[SolverId, SolverResult] = {}
    if not is_meltdown:
        solver_instances = [
            GravitySolver(),
            VelocitySolver(),
            StructureSolver(),
        ]

        for solver in solver_instances:
            try:
                result, warnings = solver.solve(ts_code, factors, config)
                solvers[solver.solver_id] = result
                all_warnings.extend(warnings)
            except Exception as e:
                all_warnings.append(TruthWarning(
                    code=f"SOLVER_{solver.solver_id.name}_ERROR",
                    level=WarningLevel.CRITICAL,
                    title=f"{solver.solver_id.name} 求解器失败",
                    message=str(e),
                    source="solver_executor",
                ))

    # ========== Layer 2.5: 注入实际值到动态阈值 ==========
    if solvers:
        solvers = _inject_actual_values(solvers, probes)

    # ========== Layer 3: 综合评分 ==========
    final_score, signal, grade, confidence = _calibrate(
        factors, solvers, is_meltdown, config, data_years=data_years
    )

    # 数据年限不足时追加警告
    cal = config.calibration
    if data_years < cal.min_data_years:
        all_warnings.append(TruthWarning(
            code="DATA_INSUFFICIENT",
            level=WarningLevel.CRITICAL,
            title="数据年限不足",
            message=f"仅有约{data_years}年数据，低于最低要求{cal.min_data_years}年",
            source="calibration",
            values={"data_years": float(data_years), "min_required": float(cal.min_data_years)},
        ))

    return TruthProfile(
        ts_code=ts_code,
        factors=factors,
        solvers=solvers,
        final_score=final_score,
        signal=signal,
        grade=grade,
        confidence=confidence,
        warnings=tuple(all_warnings),
    )


def _calibrate(
    factors: Dict[FactorId, FactorResult],
    solvers: Dict[SolverId, SolverResult],
    is_meltdown: bool,
    config: TruthConfig,
    data_years: int = 10,
) -> tuple:
    """校准层: 计算最终评分、信号、评级

    【T-H1 修复】使用 ScoringConfig 中的权重，而非硬编码
    【T-H2 修复】整合 CalibrationConfig 的数据年限置信度调整
    """
    if is_meltdown:
        return 0.0, TruthSignal.FRAUD_ALERT, TruthGrade.F, 0.0

    scoring = config.scoring

    # 因子加权平均（从 ScoringConfig.factor_weights 获取）
    factor_weight_map = {
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.10),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.10),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.25),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.15),
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.15),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.25),
    }

    weighted_sum = 0.0
    total_weight = 0.0
    for fid, weight in factor_weight_map.items():
        result = factors.get(fid)
        if result and result.score is not None:
            # δ_fraud 和 δ_decay 是负向指标 (越低越好)
            if fid in (FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY):
                weighted_sum += (1.0 - result.score) * weight
            else:
                weighted_sum += result.score * weight
            total_weight += weight

    factor_score = weighted_sum / total_weight if total_weight > 0 else 0.5

    # 求解器加权（从 ScoringConfig.solver_weights 获取）
    solver_weight_map = {
        SolverId.GRAVITY: scoring.solver_weights.get("GRAVITY", 0.40),
        SolverId.VELOCITY: scoring.solver_weights.get("VELOCITY", 0.30),
        SolverId.STRUCTURE: scoring.solver_weights.get("STRUCTURE", 0.30),
    }

    solver_sum = 0.0
    solver_total = 0.0
    for sid, weight in solver_weight_map.items():
        result = solvers.get(sid)
        if result and result.score is not None:
            solver_sum += result.score * weight
            solver_total += weight

    solver_score = solver_sum / solver_total if solver_total > 0 else 0.5

    # 最终分数 = 因子 × factor_vs_solver_weight + 求解器 × (1 - factor_vs_solver_weight)
    factor_ratio = scoring.factor_vs_solver_weight
    final_score = factor_score * factor_ratio + solver_score * (1.0 - factor_ratio)

    # 计算置信度
    confidences = [r.confidence for r in factors.values() if r.confidence]
    confidences.extend([r.confidence for r in solvers.values() if r.confidence])
    confidence = sum(confidences) / len(confidences) if confidences else 0.5

    # 【T-H2 修复】数据年限置信度调整
    cal = config.calibration
    if data_years < cal.full_confidence_years:
        # 数据不满 full_confidence_years 时，压低置信度
        # 不满 min_data_years 时 cap 在 max_confidence_5y 以下
        year_ratio = data_years / cal.full_confidence_years
        if data_years <= cal.min_data_years:
            confidence = min(confidence, cal.max_confidence_5y * 0.6)
        else:
            confidence *= max(year_ratio, cal.max_confidence_5y)

    # 信号和评级（使用 ScoringConfig 阈值）
    signal = _score_to_signal(final_score, scoring)
    grade = _score_to_grade(final_score, scoring)

    return final_score, signal, grade, confidence


def _score_to_signal(score: float, scoring: 'ScoringConfig' = None) -> TruthSignal:
    """分数转信号

    【T-H1 修复】使用 ScoringConfig.signal_thresholds
    """
    if scoring and scoring.signal_thresholds:
        th = scoring.signal_thresholds
        if score >= th.get("strong_buy", 0.85):
            return TruthSignal.STRONG_BUY
        elif score >= th.get("buy", 0.72):
            return TruthSignal.BUY
        elif score >= th.get("hold", 0.55):
            return TruthSignal.HOLD
        elif score >= th.get("caution", 0.40):
            return TruthSignal.CAUTION
        else:
            return TruthSignal.SELL

    # fallback 默认值（与 ScoringConfig 默认一致）
    if score >= 0.75:
        return TruthSignal.STRONG_BUY
    elif score >= 0.62:
        return TruthSignal.BUY
    elif score >= 0.48:
        return TruthSignal.HOLD
    elif score >= 0.35:
        return TruthSignal.CAUTION
    else:
        return TruthSignal.SELL


def _score_to_grade(score: float, scoring: 'ScoringConfig' = None) -> TruthGrade:
    """分数转评级

    【T-H1 修复】使用 ScoringConfig.grade_thresholds
    """
    if scoring and scoring.grade_thresholds:
        th = scoring.grade_thresholds
        a_th = th.get("A", 0.85)
        b_th = th.get("B", 0.72)
        c_th = th.get("C", 0.55)
        d_th = th.get("D", 0.40)

        if score >= a_th + 0.05:
            return TruthGrade.A_PLUS
        elif score >= a_th:
            return TruthGrade.A
        elif score >= (a_th + b_th) / 2:
            return TruthGrade.B_PLUS
        elif score >= b_th:
            return TruthGrade.B
        elif score >= c_th:
            return TruthGrade.C
        elif score >= d_th:
            return TruthGrade.D
        else:
            return TruthGrade.F

    # fallback
    if score >= 0.80:
        return TruthGrade.A_PLUS
    elif score >= 0.75:
        return TruthGrade.A
    elif score >= 0.68:
        return TruthGrade.B_PLUS
    elif score >= 0.62:
        return TruthGrade.B
    elif score >= 0.48:
        return TruthGrade.C
    elif score >= 0.35:
        return TruthGrade.D
    else:
        return TruthGrade.F


# ============================================================================
# 输出格式化
# ============================================================================

def _profile_to_dict(profile: TruthProfile) -> Dict[str, Any]:
    """TruthProfile 转字典"""
    return {
        "ts_code": profile.ts_code,
        "name": profile.name,
        "industry": profile.industry,
        "factors": {
            fid.value: {
                "score": fr.score,
                "confidence": fr.confidence,
                "components": dict(fr.components),
            }
            for fid, fr in profile.factors.items()
        },
        "solvers": {
            sid.value: {
                "score": sr.score,
                "confidence": sr.confidence,
                "thresholds": {
                    name: {
                        "value": th.value,
                        "lower": th.lower_bound,
                        "upper": th.upper_bound,
                        "description": th.description,
                    }
                    for name, th in sr.thresholds.items()
                } if sr.thresholds else {},
            }
            for sid, sr in profile.solvers.items()
        },
        "final_score": profile.final_score,
        "signal": profile.signal.value if profile.signal else None,
        "grade": profile.grade.value if profile.grade else None,
        "confidence": profile.confidence,
        "warnings": [
            {"code": w.code, "level": w.level.value, "title": w.title, "message": w.message}
            for w in profile.warnings
        ],
        "dynamic_thresholds": {
            name: {
                "value": th.value,
                "lower": th.lower_bound,
                "upper": th.upper_bound,
                "unit": th.unit,
                "description": th.description,
            }
            for name, th in profile.get_all_thresholds().items()
        },
    }


def _calculate_summary(profiles: List[TruthProfile]) -> Dict[str, Any]:
    """计算汇总统计"""
    if not profiles:
        return {}

    signal_dist = {}
    grade_dist = {}
    for p in profiles:
        sig = p.signal.value if p.signal else "unknown"
        signal_dist[sig] = signal_dist.get(sig, 0) + 1
        grade = p.grade.value if p.grade else "unknown"
        grade_dist[grade] = grade_dist.get(grade, 0) + 1

    scores = [p.final_score for p in profiles if p.final_score is not None]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    return {
        "signal_distribution": signal_dist,
        "grade_distribution": grade_dist,
        "average_score": round(avg_score, 4),
        "top_picks_count": sum(1 for p in profiles if p.grade in (TruthGrade.A_PLUS, TruthGrade.A)),
        "meltdown_count": sum(1 for p in profiles if p.signal == TruthSignal.FRAUD_ALERT),
    }


# ============================================================================
# 公开 API
# ============================================================================

@register_method(
    engine_name="run_truth",
    component_type="business_engine",
    engine_type="truth",
    description="Run T.R.U.T.H. pipeline with six factors and three solvers.",
)
def run_truth(
    aggregated_trends: Dict[str, pd.DataFrame],
    raw_financial_data: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """TRUTH 主入口

    输入:
        aggregated_trends: PDDA 聚合数据 {metric_name: DataFrame}
        raw_financial_data: 原始资产负债表数据 (可选)
            包含 ts_code + 资产负债表字段 (fix_assets, total_assets, goodwill, ...)
            用于 β 因子和 δ_fraud 因子的财务结构分析

    输出:
        {"profiles": [...], "summary": {...}, "metadata": {...}}
    """
    logger.info(f"✅ PDDA: 使用聚合数据，包含 {len(aggregated_trends)} 个指标: {list(aggregated_trends.keys())}")

    probes_by_ts = _build_probes_from_dataframes(aggregated_trends)

    # 接入 Financial Context 探针
    has_financial_context = False
    if raw_financial_data is not None and not raw_financial_data.empty:
        fc_probes = _build_financial_context_probes(raw_financial_data)
        if fc_probes:
            has_financial_context = True
            for ts_code, fc_probe in fc_probes.items():
                probes_by_ts.setdefault(ts_code, []).append(fc_probe)
            logger.info(f"✅ Financial Context: 已为 {len(fc_probes)} 只股票注入资产结构数据")
    else:
        logger.warning("⚠️ 未提供 raw_financial_data，β 和 δ_fraud 因子将使用默认值")

    config = get_default_config()

    profiles = []
    for ts_code, probes in probes_by_ts.items():
        profile = _process_single(ts_code, probes, config)
        profiles.append(profile)

    profiles_dict = [_profile_to_dict(p) for p in profiles]
    summary = _calculate_summary(profiles)

    return {
        "metadata": {
            "algo_version": "3.4.0",
            "config_version": config.config_version,
            "universe_size": len(profiles),
            "factor_count": 6,
            "solver_count": 3,
            "has_financial_context": has_financial_context,
        },
        "profiles": profiles_dict,
        "summary": summary,
    }


__all__ = ["run_truth", "TruthConfig", "get_default_config"]
