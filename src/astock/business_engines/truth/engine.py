"""
═══════════════════════════════════════════════════════════════════════════════
T.R.U.T.H. v7.0 - 八维基因测序 × 三大物理求解器
═══════════════════════════════════════════════════════════════════════════════

精简扁平架构:

    truth/
    ├── engine.py       # 主入口 (run_truth)
    ├── factors.py      # 8 个因子 (α/β/γ/π/λ/δ_fraud/δ_decay/V)
    ├── solvers.py      # 3 个求解器 (Gravity/Velocity/Structure)
    ├── models.py       # 领域模型
    └── config.py       # 配置

设计理念:
    - 去标签化: 用数据驱动的八维基因描述公司特征
    - 动态阈值: 求解器输出阈值而非简单分数
    - 物理隐喻: 重力场/速度场/结构场

v7.0 新增:
    - π (Pi) 盈利能力因子: GP/Assets (Novy-Marx) + ROIC/ROE水平 + 资产周转率
    - 填补 AQR QMJ Profitability、MSCI Quality ROE Level 维度缺失

Pipeline 集成:
    - 输入: aggregated_trends (来自 PDDA)
    - 输出: profiles + summary

版本: 7.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import math
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
    PiFactor,
    LambdaFactor,
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
# Financial Context 探针 — 现在由 PDDA 统一管理
# ============================================================================
# financial_context 数据由 trend/engine.py::build_financial_context 步骤
# 通过 PDDA 聚合到 aggregated_trends["financial_context"]，
# 再由 _build_probes_from_dataframes 自动转为 ProbeInput。
# β 和 δ_fraud 因子无需任何修改即可消费。
# ============================================================================


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
      v4.8: 同时注入 roic_latest_value 到 components (供 ROIC 门控使用)
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
        # v4.8: 收集额外组件值
        extra_components: Dict[str, float] = {}

        for th_name, th in result.thresholds.items():
            actual = None

            if sid == SolverId.GRAVITY and th_name == "roic":
                # Gravity: 实际 ROIC 加权平均值
                actual = _extract_actual_from_probes(probes, "roic", "weighted_avg")
                if actual is None:
                    actual = _extract_actual_from_probes(probes, "roic", "latest_value")
                # v4.8: 同时提取 latest_value (供 ROIC 绝对水平门控使用)
                # weighted_avg 可能掩盖近期恶化 (如: 华宝新能 weighted=34%, latest=3.3%)
                roic_latest = _extract_actual_from_probes(probes, "roic", "latest_value")
                if roic_latest is not None:
                    extra_components["roic_latest_value"] = roic_latest

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
            # v4.8: 合并额外组件值到 solver result
            merged_components = dict(result.components)
            merged_components.update(extra_components)
            new_score = result.score  # 默认: 保持原始分数

            # ══════ v6.0: Gravity ROIC Excess Return Enhancement ══════
            # 问题: 原始 Gravity 分数 = sigmoid(-(threshold-10)/4), 即"门槛高低"
            #       这是因子的非线性变换, 不包含独立信息 (100%依赖因子输入)
            # 解决: 混合"门槛风险分" + "实际超额回报分", 注入真实业绩数据
            #       excess = actual_ROIC - required_ROIC
            #       超额回报高 = 公司远超风险调整后的最低要求 = 高质量
            # 学术参考:
            #   - GMO Quality: Profitability (ROIC - WACC) 是核心维度
            #   - AQR "Quality Minus Junk": Profitability = GPOA, ROE, ROA, CFOA
            #   - Greenblatt "Magic Formula": Earnings Yield = EBIT/EV (excess return proxy)
            # 混合权重: 50% 原始(风险面) + 50% 超额回报(业绩面)
            if sid == SolverId.GRAVITY:
                roic_th = new_thresholds.get("roic")
                if roic_th and hasattr(roic_th, 'actual_value') and roic_th.actual_value is not None:
                    threshold = result.components.get("roic_threshold", 12.0)
                    # v6.1: 优先使用 latest_value (当前业绩) 而非 weighted_avg (历史均值)
                    # weighted_avg 可能掩盖近期恶化 (如华宝新能: weighted=34%, latest=3.3%)
                    # 而 excess return 应反映 CURRENT 盈利能力 vs 要求
                    roic_for_excess = merged_components.get("roic_latest_value", roic_th.actual_value)
                    excess = roic_for_excess - threshold
                    # sigmoid: ±5pp ROIC excess → ±1, 区分度充分
                    # 例: ROIC=20%, threshold=12% → excess=8pp → sigmoid(1.6)=0.83
                    #     ROIC=5%, threshold=12% → excess=-7pp → sigmoid(-1.4)=0.20
                    excess_score = 1.0 / (1.0 + math.exp(-excess / 5.0))
                    new_score = 0.50 * result.score + 0.50 * excess_score
                    merged_components["roic_excess_return"] = round(excess, 2)
                    merged_components["gravity_excess_score"] = round(excess_score, 4)

            updated[sid] = replace(result, score=new_score, thresholds=new_thresholds, components=merged_components)
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

    # ========== Layer 1: 计算 7 因子 ==========
    factors: Dict[FactorId, FactorResult] = {}
    factor_instances = [
        AlphaFactor(),
        BetaFactor(),
        GammaFactor(),
        PiFactor(),
        LambdaFactor(),
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
    """v5.0 校准层: 计算原始加权分数

    v5.0 重大架构变更:
      - 删除 v4.x 的 6 层门控级联 (δ_decay门控 / ROIC阶梯 / 成长折扣 / 余裕度调整)
      - 原始得分仅用于 _cross_sectional_normalize() 的百分位排名输入
      - 所有硬约束 (ROIC地板) 移至 _cross_sectional_normalize()
      - 仅保留: 熔断 → 0 分

    设计原理 (AQR QMJ / MSCI Quality 方法论):
      绝对分数的门控 + 阈值会导致:
        1) 分数聚集 (159家公司在cap值上)
        2) 阈值附近的排名扭曲 (ROIC=11.9% vs 12.1% 命运迥异)
        3) 手工调参黑洞 (6层×N参数, 每改一个连锁反应)
      百分位排名的优势:
        1) 天然均匀分布 → 无聚集
        2) 无边界效应 → 排名连续
        3) 零手工参数 → 数据自适应
    """
    if is_meltdown:
        return 0.0, TruthSignal.FRAUD_ALERT, TruthGrade.F, 0.0

    scoring = config.scoring

    # ── Factor weighted average (raw) ──
    factor_weight_map = {
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.12),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.08),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.18),
        FactorId.LAMBDA: scoring.factor_weights.get("LAMBDA", 0.12),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.16),
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.10),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.08),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.14),
        FactorId.PI: scoring.factor_weights.get("PI", 0.15),
        FactorId.LAMBDA: scoring.factor_weights.get("LAMBDA", 0.10),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.15),
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.16),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.12),
    }
    # 负向因子: score 越高越差 → 反转
    # v5.2: β加入负向 — 重资产(高β)在质量因子中应为负面，轻资产=高质量
    # v7.0: π(盈利能力)是正向因子
    _NEGATIVE_FACTORS = {FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.LAMBDA, FactorId.ALPHA, FactorId.BETA}

    weighted_sum = 0.0
    total_weight = 0.0
    for fid, weight in factor_weight_map.items():
        result = factors.get(fid)
        if result and result.score is not None:
            s = (1.0 - result.score) if fid in _NEGATIVE_FACTORS else result.score
            weighted_sum += s * weight
            total_weight += weight
    factor_score = weighted_sum / total_weight if total_weight > 0 else 0.5

    # ── Solver weighted average (raw) ──
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

    # ── Combined raw score ──
    factor_ratio = scoring.factor_vs_solver_weight
    final_score = factor_score * factor_ratio + solver_score * (1.0 - factor_ratio)

    # ── Confidence (data years scaling) ──
    confidences = [r.confidence for r in factors.values() if r.confidence]
    confidences.extend([r.confidence for r in solvers.values() if r.confidence])
    confidence = sum(confidences) / len(confidences) if confidences else 0.5

    cal = config.calibration
    if data_years < cal.full_confidence_years:
        if data_years <= cal.min_data_years:
            year_factor = cal.min_confidence_3y if hasattr(cal, 'min_confidence_3y') else 0.60
        else:
            progress = (data_years - cal.min_data_years) / (cal.full_confidence_years - cal.min_data_years)
            year_factor = cal.max_confidence_5y + (1.0 - cal.max_confidence_5y) * progress
        confidence = confidence * year_factor

    # 初始信号/评级 (将被 _cross_sectional_normalize 覆盖)
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
                "details": dict(fr.details),
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
        "top_picks_count": sum(1 for p in profiles if p.grade in (TruthGrade.A_PLUS, TruthGrade.A, TruthGrade.B_PLUS)),
        "meltdown_count": sum(1 for p in profiles if p.signal == TruthSignal.FRAUD_ALERT),
    }


# ============================================================================
# v5.0 Cross-Sectional Percentile Normalization
# ============================================================================

def _percentile_ranks(values: List[Optional[float]]) -> List[float]:
    """Raw scores → percentile ranks [0.0, 1.0].

    None → 0.5 (中位数). Ties → 平均排名.
    纯 Python 实现, 无 scipy/numpy 依赖.

    算法: 排序 → 分配名次 (相同值取平均) → 线性映射到 [0, 1]
    """
    n = len(values)
    result = [0.5] * n

    valid = [(i, v) for i, v in enumerate(values) if v is not None]
    n_valid = len(valid)
    if n_valid < 2:
        return result

    sorted_valid = sorted(valid, key=lambda x: x[1])

    # 分配排名 (ties → 平均排名)
    i = 0
    while i < n_valid:
        j = i + 1
        while j < n_valid and sorted_valid[j][1] == sorted_valid[i][1]:
            j += 1
        avg_rank = (i + j - 1) / 2.0
        percentile = avg_rank / (n_valid - 1)
        for k in range(i, j):
            result[sorted_valid[k][0]] = percentile
        i = j

    return result


def _extract_roic_actual(profile: TruthProfile) -> Optional[float]:
    """从 Gravity 求解器提取实际 ROIC (%).

    优先使用 latest_value (更准确反映当前盈利能力),
    回退到 weighted_avg (actual_value in thresholds).
    """
    gravity = profile.solvers.get(SolverId.GRAVITY)
    if not gravity:
        return None
    # 优先 latest (v4.8: 防止 weighted_avg 掩盖近期崩塌)
    latest = gravity.components.get("roic_latest_value")
    if latest is not None:
        return float(latest)
    # 回退 weighted_avg
    if gravity.thresholds:
        roic_th = gravity.thresholds.get("roic")
        if roic_th and hasattr(roic_th, 'actual_value') and roic_th.actual_value is not None:
            return float(roic_th.actual_value)
    return None


def _cross_sectional_normalize(
    profiles: List[TruthProfile],
    config: TruthConfig,
) -> List[TruthProfile]:
    """v6.0 Cross-Sectional Percentile Normalization + Industry Neutralization + Momentum + Dispersion

    方法论来源: AQR Quality-Minus-Junk (QMJ), MSCI Quality Index, GMO Quality
    核心思路: 原始分数 → 行业内z-score → 全样本百分位排名 → 加权合成 → 百分位评级

    v5.2 新增:
    ┌─────────────────────────────────────────────────┐
    │ 行业中性化 (Industry Neutralization)             │
    │                                                   │
    │ 问题: 软件业天然ROIC=30%, 钢铁业天然ROIC=5%     │
    │   → 全市场直接排名导致软件业系统性偏低           │
    │                                                   │
    │ 解决: 行业内z-score → 比较的是"同行中的相对位置" │
    │   z_{i,ind} = (x_i - μ_ind) / σ_ind             │
    │   然后再做全样本百分位排名                        │
    └─────────────────────────────────────────────────┘
    """
    if len(profiles) < 5:
        logger.warning(f"样本量不足 ({len(profiles)} < 5), 跳过 cross-sectional normalization")
        return profiles

    scoring = config.scoring

    # ══════ Step 1: 提取原始分数矩阵 ══════
    factor_ids = [
        FactorId.ALPHA, FactorId.BETA, FactorId.GAMMA, FactorId.PI,
        FactorId.LAMBDA, FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.VERIFICATION,
    ]
    solver_ids = [SolverId.GRAVITY, SolverId.VELOCITY, SolverId.STRUCTURE]

    factor_raw: Dict[FactorId, List[Optional[float]]] = {fid: [] for fid in factor_ids}
    solver_raw: Dict[SolverId, List[Optional[float]]] = {sid: [] for sid in solver_ids}

    for p in profiles:
        for fid in factor_ids:
            r = p.factors.get(fid)
            factor_raw[fid].append(r.score if r and r.score is not None else None)
        for sid in solver_ids:
            r = p.solvers.get(sid)
            solver_raw[sid].append(r.score if r and r.score is not None else None)

    # ══════ Step 1.5: v5.2 行业中性化 (Industry Neutralization) ══════
    # AQR QMJ / MSCI Quality 标准做法:
    #   在行业内做 z-score 标准化, 消除行业系统性差异
    #   z_{i,ind} = (x_i - μ_ind) / σ_ind
    #   例: 软件行业天然 ROIC 高, 钢铁天然 ROIC 低 → 直接比不公平
    #   行业内 z-score 后, 比较的是 "在同行中的相对位置"
    MIN_INDUSTRY_SIZE = 8  # 行业内样本少于此阈值时不做行业调整

    # 构建行业索引
    industry_map: Dict[str, List[int]] = {}
    for i, p in enumerate(profiles):
        ind = getattr(p, 'industry', '') or '__unknown__'
        industry_map.setdefault(ind, []).append(i)

    def _winsorize_stats(values: list) -> tuple:
        """v5.5: 计算 winsorized μ/σ (2.5th/97.5th 百分位)

        防止单个极端异常值扭曲整个行业的 z-score 分布.
        例: 一家 δ_fraud=0.95 的公司会使行业 σ 膨胀,
        压缩其余所有公司的 z-score 到接近 0.

        学术依据: Tukey (1977) fence + MSCI Barra 风险模型标准做法.
        """
        n = len(values)
        if n < 5:
            mu = sum(values) / n
            var = sum((v - mu) ** 2 for v in values) / n
            return mu, var ** 0.5
        sorted_v = sorted(values)
        lo = sorted_v[max(0, int(n * 0.025))]    # 2.5th percentile
        hi = sorted_v[min(n - 1, int(n * 0.975))]  # 97.5th percentile
        clipped = [max(lo, min(hi, v)) for v in values]
        mu = sum(clipped) / n
        var = sum((v - mu) ** 2 for v in clipped) / n
        return mu, var ** 0.5

    def _industry_zscore(raw: Dict[Any, List[Optional[float]]]
                         ) -> Dict[Any, List[Optional[float]]]:
        """对每个因子/求解器, 在行业内做 z-score 标准化

        v5.3 改进: 小行业 (<MIN_INDUSTRY_SIZE) 退化为全样本 z-score,
        而非保留 raw score. 保留 raw score 导致它们与已 z-scored 的
        大行业数据处于不同尺度, 后续百分位排名时产生偏差.

        v5.5 改进: winsorized z-score (2.5th/97.5th) 防止异常值扭曲行业排名.
        """
        result = {}
        for key, vals in raw.items():
            zscored = list(vals)  # shallow copy

            # 收集小行业成员索引 (用于全样本 z-score fallback)
            small_industry_indices = []

            for ind, indices in industry_map.items():
                if len(indices) < MIN_INDUSTRY_SIZE:
                    small_industry_indices.extend(indices)
                    continue  # 小行业不做行业内 z-score
                ind_vals = [vals[j] for j in indices if vals[j] is not None]
                if len(ind_vals) < 3:
                    small_industry_indices.extend(indices)
                    continue
                # v5.5: winsorized μ/σ 防止异常值扭曲
                mu, sigma = _winsorize_stats(ind_vals)
                if sigma < 1e-10:
                    continue  # 行业内无方差, 跳过
                for j in indices:
                    if vals[j] is not None:
                        zscored[j] = (vals[j] - mu) / sigma

            # v6.0: 小行业退化为全样本 z-score (全局池)
            # v5.3 bug: 仅池化小行业公司(如石油3家+航天2家+白酒3家=8家),
            #   不同行业混合z-score无意义. v6.0: 使用全样本(1800+家)做global z-score,
            #   确保小行业公司至少与全市场比较, 而非与随机小行业混合池比较.
            # 学术参考: MSCI Barra 对小行业使用 sector-level 或 global z-score.
            if small_industry_indices:
                all_valid = [v for v in vals if v is not None]
                if len(all_valid) >= 5:
                    mu_global, sigma_global = _winsorize_stats(all_valid)
                    if sigma_global > 1e-10:
                        for j in small_industry_indices:
                            if vals[j] is not None:
                                zscored[j] = (vals[j] - mu_global) / sigma_global

            result[key] = zscored
        return result

    factor_adj = _industry_zscore(factor_raw)
    solver_adj = _industry_zscore(solver_raw)

    n_neutralized = sum(1 for indices in industry_map.values() if len(indices) >= MIN_INDUSTRY_SIZE)
    logger.info(
        f"v5.2 Industry Neutralization: "
        f"{len(industry_map)} industries, {n_neutralized} neutralized (>={MIN_INDUSTRY_SIZE} members)"
    )

    # ══════ Step 2: 百分位排名 (对行业中性化后的值) ══════
    factor_pct = {fid: _percentile_ranks(factor_adj[fid]) for fid in factor_ids}
    solver_pct = {sid: _percentile_ranks(solver_adj[sid]) for sid in solver_ids}

    # ══════ Step 3: 加权合成 + 硬约束 → 评级 ══════
    factor_weight_map = {
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.10),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.08),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.14),
        FactorId.PI: scoring.factor_weights.get("PI", 0.15),
        FactorId.LAMBDA: scoring.factor_weights.get("LAMBDA", 0.10),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.15),
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.16),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.12),
    }
    solver_weight_map = {
        SolverId.GRAVITY: scoring.solver_weights.get("GRAVITY", 0.50),
        SolverId.VELOCITY: scoring.solver_weights.get("VELOCITY", 0.40),
        SolverId.STRUCTURE: scoring.solver_weights.get("STRUCTURE", 0.10),
    }

    # 负向因子: raw score 越高越差 → percentile 高 = 差 → 需要反转
    # v5.2: β加入负向 — 重资产(高β)在质量因子中应为负面，轻资产=高质量
    # v7.0: π(盈利能力)是正向因子 — 高π = 高盈利 = 高质量
    _NEGATIVE_FACTORS = {FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.LAMBDA, FactorId.ALPHA, FactorId.BETA}
    factor_ratio = scoring.factor_vs_solver_weight

    # ══════ v5.4: 数据驱动衰退惩罚阈值 ══════
    # v5.3 使用硬编码绝对阈值 (0.60, 0.50), 市场环境变化时可能失效.
    # v5.4: 用 raw δ_decay 的百分位分布确定惩罚线:
    #   top 10% → severe_penalty (0.80×)
    #   top 10-20% + low γ → moderate_penalty (0.85×)
    # 这样惩罚永远针对"当前宇宙集中衰退最严重的10-20%", 自动适应。
    _raw_decays_valid = [
        v for v in factor_raw[FactorId.DELTA_DECAY] if v is not None
    ]
    if _raw_decays_valid:
        _raw_decays_sorted = sorted(_raw_decays_valid)
        _n_rd = len(_raw_decays_sorted)
        _severe_threshold = _raw_decays_sorted[int(_n_rd * 0.90)]  # top 10%
        _moderate_threshold = _raw_decays_sorted[int(_n_rd * 0.80)]  # top 20%
    else:
        _severe_threshold, _moderate_threshold = 0.60, 0.50  # fallback

    new_profiles: List[TruthProfile] = []
    n_hard_constrained = 0

    for i, p in enumerate(profiles):
        # 熔断 → 保持原样
        if p.signal == TruthSignal.FRAUD_ALERT:
            new_profiles.append(p)
            continue

        # ── Factor percentile composite (v5.2: confidence-weighted) ──
        f_sum, f_total = 0.0, 0.0
        for fid, w in factor_weight_map.items():
            pct = factor_pct[fid][i]
            # 负向因子: high raw = bad → high percentile → invert to low
            if fid in _NEGATIVE_FACTORS:
                pct = 1.0 - pct
            # 置信度缩放: 低置信因子贡献降低
            r = p.factors.get(fid)
            conf = r.confidence if r and r.confidence is not None else 0.5
            eff_w = w * conf
            f_sum += pct * eff_w
            f_total += eff_w
        factor_score = f_sum / f_total if f_total > 0 else 0.5

        # ── Solver percentile composite (v5.2: confidence-weighted) ──
        s_sum, s_total = 0.0, 0.0
        for sid, w in solver_weight_map.items():
            pct = solver_pct[sid][i]
            r = p.solvers.get(sid)
            conf = r.confidence if r and r.confidence is not None else 0.5
            eff_w = w * conf
            s_sum += pct * eff_w
            s_total += eff_w
        solver_score = s_sum / s_total if s_total > 0 else 0.5

        # ── Combined score ──
        final_score = factor_score * factor_ratio + solver_score * (1.0 - factor_ratio)

        # ══════ Hard constraints (v6.2: 扩展ROIC约束 + 绝对水平奖励) ══════
        #
        # v5.3: 仅约束 ROIC<3% (3档), 导致 ROIC=3-5% 的低质量公司
        # 通过趋势信号进入 quality (假阳性). 核心问题:
        #   - 纯百分位评级 + 弱绝对约束 → ROIC<5% 可排入 top 10%
        #   - 同时, ROIC>15% 的成熟优质公司因趋势下行被归为 poor (假阴性)
        #
        # v6.2 改进 (双向调整):
        # 1. 向下: 扩展 ROIC 硬约束到 WACC 区域 (3-8%), 消除假阳性
        #    ROIC < WACC (≈8%) 的公司不可能是"优质"
        # 2. 向上: 高 ROIC 绝对水平奖励, 缓解假阴性
        #    AQR QMJ / Novy-Marx (2013): 当前盈利能力是质量的首要维度
        #    公司赚取远超 WACC 的回报, 即使趋势温和下行, 仍属优质
        #
        # 学术参考:
        #   - AQR "Quality Minus Junk": Profitability = GPOA, ROE, ROA, CFOA
        #   - GMO Quality: Profitability (ROIC - WACC) 是核心质量维度
        #   - Greenblatt "Magic Formula": Earnings Yield ∝ ROIC
        #   - Novy-Marx (2013): Gross profitability 独立于趋势预测回报
        roic_actual = _extract_roic_actual(p)
        if roic_actual is not None:
            # ── 向下约束: 低 ROIC 硬封顶 (anti-false-positive) ──
            if roic_actual < 0.0:
                final_score = min(final_score, 0.20)
                n_hard_constrained += 1
            elif roic_actual < 1.5:
                final_score = min(final_score, 0.28)
                n_hard_constrained += 1
            elif roic_actual < 3.0:
                final_score = min(final_score, 0.35)
                n_hard_constrained += 1
            elif roic_actual < 5.0:
                # v6.2 NEW: ROIC 3-5% — 低于任何行业 WACC 下界
                # cap 0.45 确保无法进入 A+/A 评级 (quality 需 top 10%)
                final_score = min(final_score, 0.45)
                n_hard_constrained += 1
            elif roic_actual < 8.0:
                # v6.2 NEW: ROIC 5-8% — 接近但可能低于 WACC
                # 软惩罚 × 0.90, 降低但不封死 (B+ 评级仍可达)
                final_score *= 0.90
                n_hard_constrained += 1

            # ── 向上奖励: 高 ROIC 绝对水平溢价 (anti-false-negative) ──
            # 公司赚取远超 WACC 的回报 = 结构性竞争优势
            # 即使趋势温和下行 (如海康 ROIC 20%→13%), 绝对水平仍优秀
            # 奖励幅度保守 (2-8%), 不应覆盖严重衰退信号
            if roic_actual >= 25.0:
                final_score *= 1.10  # 极优: ROIC > 25% (如锦波, 特宝)
            elif roic_actual >= 20.0:
                final_score *= 1.08  # 卓越: ROIC 20-25%
            elif roic_actual >= 15.0:
                final_score *= 1.05  # 优秀: ROIC 15-20%
            elif roic_actual >= 10.0:
                final_score *= 1.02  # 良好: ROIC 10-15% (海康 13%)

        # ══════ v5.4: 数据驱动衰退惩罚 ══════
        # v5.3 硬编码 δ_decay>=0.60 → 0.80×, 不适应分布变化.
        # v5.4: 使用当前宇宙集的百分位阈值 (top 10%/20%)
        # RAW (非行业中性化) δ_decay 保留绝对水平信息.
        raw_decay = factor_raw[FactorId.DELTA_DECAY][i]
        raw_gamma = factor_raw[FactorId.GAMMA][i]
        if raw_decay is not None:
            if raw_decay >= _severe_threshold:
                # top 10% 衰退: 强惩罚
                final_score *= 0.80
            elif raw_decay >= _moderate_threshold and (raw_gamma is None or raw_gamma < 0.45):
                # top 10-20% 衰退 + 低成长: 中等惩罚
                final_score *= 0.85

        # ══════ v6.0: Fundamental Momentum Adjustment ══════
        # Novy-Marx (2015): "Fundamentally, Momentum is Fundamental"
        # AQR QMJ 也使用 quality momentum (质量变化率) 作为信号维度
        # 核心: 近期趋势 vs 长期趋势 → 基本面加速 / 减速
        #   acceleration > 0: 近期增长快于长期 = 正向动量 (如拐点向上)
        #   acceleration < 0: 近期增长慢于长期 = 负向动量 (如拐点向下)
        # 权重保守 (±3%): 动量是辅助信号, 不应覆盖核心因子判断
        gamma_result = p.factors.get(FactorId.GAMMA)
        if gamma_result and gamma_result.components:
            gc = gamma_result.components
            recent = gc.get("recent_3y_slope")
            full = gc.get("log_slope")
            if recent is not None and full is not None:
                momentum = recent - full
                if momentum > 0.03:
                    # 显著加速: 最高+3%奖励
                    final_score *= min(1.03, 1.0 + momentum * 0.5)
                elif momentum < -0.03:
                    # 显著减速: 最高-3%惩罚
                    final_score *= max(0.97, 1.0 + momentum * 0.5)

        # ══════ v6.0: Sustainable Growth Interaction Term ══════
        # 学术参考: AQR QMJ 将质量分解为 Profitability × Growth × Safety
        #   线性模型假设因子独立, 但实际中交互效应显著:
        #   - High γ + Low δ_decay = 可持续增长 (premium) → 奖励
        #   - High γ + High δ_decay = 增长在衰退 ("价值陷阱") → 惩罚
        #   - Novy-Marx (2013): Interaction terms improve quality factor IC by 15-20%
        # 实现: SG = γ × (1 - δ_decay), raw scores [0,1]
        #   SG > 0.40 → sustainable growth premium (+2%)
        #   SG < 0.15 且 γ > 0.40 → value trap penalty (-2%)
        if raw_gamma is not None and raw_decay is not None:
            sustainable_growth = raw_gamma * (1.0 - raw_decay)
            if sustainable_growth > 0.40:
                # 可持续增长: 最高+2%奖励
                final_score *= min(1.02, 1.0 + (sustainable_growth - 0.40) * 0.05)
            elif sustainable_growth < 0.15 and raw_gamma > 0.40:
                # 价值陷阱: 有增长但在衰退, 最高-2%惩罚
                final_score *= max(0.98, 1.0 - (0.15 - sustainable_growth) * 0.05)

        # ══════ v6.2: Score Cap ══════
        # 多重乘法调整 (ROIC bonus × momentum × SG) 可导致 final_score > 1.0
        # 例: 0.95 × 1.10 × 1.03 × 1.02 = 1.10
        # cap 在 1.0 确保报告中不出现 >100% 的分数
        # 不影响 percentile grading (排名不变)
        final_score = min(1.0, final_score)

        new_profiles.append(replace(p, final_score=final_score))

    # ══════ Step 4: v5.1 百分位评级 ══════
    # 用 final_score 排名分配评级，而非绝对阈值
    # 保证评级分布稳定，不受市场环境偏移影响
    # 目标分布: A+ top5%, A next5%, B+ next10%, B next15%, C next30%, D next20%, F bottom15%
    scored = [(i, p.final_score) for i, p in enumerate(new_profiles)
              if p.signal != TruthSignal.FRAUD_ALERT and p.final_score is not None]
    scored.sort(key=lambda x: x[1], reverse=True)
    n_scored = len(scored)

    # 百分位边界 (累积比例)
    _GRADE_BANDS = [
        (0.05, TruthGrade.A_PLUS),   # top 5%
        (0.10, TruthGrade.A),        # next 5%  (cum 10%)
        (0.20, TruthGrade.B_PLUS),   # next 10% (cum 20%)
        (0.35, TruthGrade.B),        # next 15% (cum 35%)
        (0.65, TruthGrade.C),        # next 30% (cum 65%)
        (0.85, TruthGrade.D),        # next 20% (cum 85%)
        (1.00, TruthGrade.F),        # bottom 15%
    ]

    grade_map: Dict[int, TruthGrade] = {}
    for rank, (idx, _) in enumerate(scored):
        pct = (rank + 1) / n_scored if n_scored > 0 else 1.0
        for cum_pct, grade in _GRADE_BANDS:
            if pct <= cum_pct:
                grade_map[idx] = grade
                break

    # 分配 signal 和 grade
    final_profiles: List[TruthProfile] = []
    n_momentum_pos, n_momentum_neg = 0, 0
    for i, p in enumerate(new_profiles):
        if p.signal == TruthSignal.FRAUD_ALERT:
            final_profiles.append(p)
            continue
        grade = grade_map.get(i, TruthGrade.C)
        signal = _score_to_signal(p.final_score, scoring)

        # ══════ v6.0: Factor Dispersion Confidence Adjustment ══════
        # 当因子强烈分歧时 (如高γ但高δ_decay), 合成分数掩盖了不确定性.
        # 计算因子百分位方差: 高方差 → 冲突信号 → 降低置信度.
        # 学术参考: Bayesian model uncertainty — conflicting priors increase posterior variance.
        factor_pcts_i = []
        for fid in factor_ids:
            pct = factor_pct[fid][i]
            if fid in _NEGATIVE_FACTORS:
                pct = 1.0 - pct
            factor_pcts_i.append(pct)
        adjusted_confidence = p.confidence
        if len(factor_pcts_i) >= 3:
            f_mean = sum(factor_pcts_i) / len(factor_pcts_i)
            f_var = sum((x - f_mean) ** 2 for x in factor_pcts_i) / len(factor_pcts_i)
            # 高方差 (>0.08 ≈ σ>0.28) → 置信度最多降低 20%
            dispersion_penalty = min(0.20, f_var / 0.08 * 0.20)
            adjusted_confidence = p.confidence * (1.0 - dispersion_penalty)

        # 统计 momentum
        gamma_r = p.factors.get(FactorId.GAMMA)
        if gamma_r and gamma_r.components:
            gc = gamma_r.components
            _rec = gc.get("recent_3y_slope")
            _ful = gc.get("log_slope")
            if _rec is not None and _ful is not None:
                if _rec - _ful > 0.03:
                    n_momentum_pos += 1
                elif _rec - _ful < -0.03:
                    n_momentum_neg += 1

        final_profiles.append(replace(p, signal=signal, grade=grade, confidence=adjusted_confidence))

    logger.info(
        f"v7.0 Cross-Sectional Normalization: "
        f"{len(profiles)} companies, "
        f"{n_hard_constrained} hard-constrained (ROIC<8% penalized, ROIC>10% rewarded), "
        f"momentum +{n_momentum_pos}/-{n_momentum_neg}, "
        f"percentile grading applied"
    )

    # v6.1: Score distribution diagnostics (运行时健康检查)
    all_scores = sorted([p.final_score for p in final_profiles
                         if p.final_score is not None and p.signal != TruthSignal.FRAUD_ALERT])
    if all_scores:
        n = len(all_scores)
        p5 = all_scores[int(n * 0.05)]
        p25 = all_scores[int(n * 0.25)]
        p50 = all_scores[int(n * 0.50)]
        p75 = all_scores[int(n * 0.75)]
        p95 = all_scores[min(n - 1, int(n * 0.95))]
        avg = sum(all_scores) / n
        logger.info(
            f"  Score distribution: "
            f"avg={avg:.3f} | p5={p5:.3f} p25={p25:.3f} p50={p50:.3f} p75={p75:.3f} p95={p95:.3f}"
        )

    return final_profiles


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
) -> Dict[str, Any]:
    """TRUTH 主入口

    输入:
        aggregated_trends: PDDA 聚合数据 {metric_name: DataFrame}
            包含 8 个趋势指标 + financial_context (资产结构探针)
            financial_context 由 build_financial_context 步骤提供，
            在 _build_probes_from_dataframes 中自动转为 ProbeInput。

    输出:
        {"profiles": [...], "summary": {...}, "metadata": {...}}
    """
    logger.info(f"✅ PDDA: 使用聚合数据，包含 {len(aggregated_trends)} 个指标: {list(aggregated_trends.keys())}")

    probes_by_ts = _build_probes_from_dataframes(aggregated_trends)

    # 从 PDDA 聚合数据中提取公司名称/行业映射（趋势分析已携带 name/industry 列）
    _company_info: Dict[str, Dict[str, str]] = {}
    for df in aggregated_trends.values():
        if df is not None and not df.empty and "name" in df.columns:
            cols = ["ts_code", "name", "industry"] if "industry" in df.columns else ["ts_code", "name"]
            for _, row in df[cols].drop_duplicates("ts_code").iterrows():
                ts = row["ts_code"]
                if ts not in _company_info:
                    _company_info[ts] = {
                        "name": str(row.get("name", "") or ""),
                        "industry": str(row.get("industry", "") or ""),
                    }
            break  # 任意一个 DataFrame 即可
    if _company_info:
        logger.info(f"Extracted {len(_company_info)} company names from aggregated_trends")

    # Financial Context 探针 — 通过 PDDA 自动注入
    has_financial_context = "financial_context" in aggregated_trends
    if has_financial_context:
        fc_df = aggregated_trends["financial_context"]
        fc_count = len(fc_df) if fc_df is not None and not fc_df.empty else 0
        logger.info(f"✅ Financial Context: {fc_count} 只股票的资产结构数据 (来自 PDDA)")
    else:
        logger.warning("⚠️ aggregated_trends 中无 financial_context，β 和 δ_fraud 因子将使用默认值")

    config = get_default_config()

    profiles = []
    for ts_code, probes in probes_by_ts.items():
        profile = _process_single(ts_code, probes, config)
        # 注入公司名称和行业
        info = _company_info.get(ts_code, {})
        if info:
            object.__setattr__(profile, 'name', info.get('name', ''))
            object.__setattr__(profile, 'industry', info.get('industry', ''))
        profiles.append(profile)

    # ══════ v5.0: Cross-Sectional Percentile Normalization ══════
    # _process_single 产出原始加权分数, 这里执行全样本百分位排名重评分
    # 替代了 v4.x 的 6 层门控级联 (δ_decay门控 / ROIC阶梯 / 余裕度调整等)
    profiles = _cross_sectional_normalize(profiles, config)

    profiles_dict = [_profile_to_dict(p) for p in profiles]
    summary = _calculate_summary(profiles)

    return {
        "metadata": {
            "algo_version": "7.0.0",
            "config_version": config.config_version,
            "universe_size": len(profiles),
            "factor_count": 8,
            "solver_count": 3,
            "has_financial_context": has_financial_context,
        },
        "profiles": profiles_dict,
        "summary": summary,
    }


__all__ = ["run_truth", "TruthConfig", "get_default_config"]
