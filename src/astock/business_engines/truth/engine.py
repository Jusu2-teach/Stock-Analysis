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
            if extra_components:
                merged_components = dict(result.components)
                merged_components.update(extra_components)
                updated[sid] = replace(result, thresholds=new_thresholds, components=merged_components)
            else:
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

    # ========== Layer 1: 计算 7 因子 ==========
    factors: Dict[FactorId, FactorResult] = {}
    factor_instances = [
        AlphaFactor(),
        BetaFactor(),
        GammaFactor(),
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
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.18),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.16),
    }
    # 负向因子: score 越高越差 → 反转
    # v5.2: β加入负向 — 重资产(高β)在质量因子中应为负面，轻资产=高质量
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
    """v5.2 Cross-Sectional Percentile Normalization + Industry Neutralization

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
        FactorId.ALPHA, FactorId.BETA, FactorId.GAMMA, FactorId.LAMBDA,
        FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.VERIFICATION,
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

    def _industry_zscore(raw: Dict[Any, List[Optional[float]]]
                         ) -> Dict[Any, List[Optional[float]]]:
        """对每个因子/求解器, 在行业内做 z-score 标准化"""
        result = {}
        for key, vals in raw.items():
            zscored = list(vals)  # shallow copy
            for ind, indices in industry_map.items():
                if len(indices) < MIN_INDUSTRY_SIZE:
                    continue  # 小行业样本不足, 保留原始值
                ind_vals = [vals[j] for j in indices if vals[j] is not None]
                if len(ind_vals) < 3:
                    continue
                mu = sum(ind_vals) / len(ind_vals)
                var = sum((v - mu) ** 2 for v in ind_vals) / len(ind_vals)
                sigma = var ** 0.5
                if sigma < 1e-10:
                    continue  # 行业内无方差, 跳过
                for j in indices:
                    if vals[j] is not None:
                        zscored[j] = (vals[j] - mu) / sigma
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
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.12),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.08),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.18),
        FactorId.LAMBDA: scoring.factor_weights.get("LAMBDA", 0.12),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.16),
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.18),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.16),
    }
    solver_weight_map = {
        SolverId.GRAVITY: scoring.solver_weights.get("GRAVITY", 0.40),
        SolverId.VELOCITY: scoring.solver_weights.get("VELOCITY", 0.30),
        SolverId.STRUCTURE: scoring.solver_weights.get("STRUCTURE", 0.30),
    }

    # 负向因子: raw score 越高越差 → percentile 高 = 差 → 需要反转
    # v5.2: β加入负向 — 重资产(高β)在质量因子中应为负面，轻资产=高质量
    # Structure solver 的 capital_barrier 维度已单独捕获重资产护城河效应
    _NEGATIVE_FACTORS = {FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.LAMBDA, FactorId.ALPHA, FactorId.BETA}
    factor_ratio = scoring.factor_vs_solver_weight

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

        # ══════ Hard constraints (v5.1: 仅真正价值毁灭) ══════
        # v5.0 的 ROIC<5%/8% 双阈值导致 73% 公司被硬约束，
        # 使百分位排名对大部分公司无效。v5.1 只保留 ROIC<3%
        # (低于任何行业 WACC 下界，真正的资本毁灭)。
        # ROIC 的信号已通过 Gravity solver 在因子端充分表达。
        roic_actual = _extract_roic_actual(p)
        if roic_actual is not None:
            if roic_actual < 3.0:
                # ROIC < 3%: 显著价值毁灭，不应高于中等评级
                final_score = min(final_score, 0.40)
                n_hard_constrained += 1

        # ══════ v5.3: 高衰退惩罚 (从 reporter 移入引擎层) ══════
        # 问题: 行业中性化后 δ_decay 的绝对水平丢失，V factor 系统性给出
        # 高分 (≈1.00)，导致衰退严重的公司仍被百分位排名推至 top tier。
        # 之前在 reporter._infer_decision_from_truth 做事后覆写，
        # 造成 score/grade 与 decision 不一致 (e.g. 89.4% A+ 显示为 AVERAGE)。
        # 修复: 用 RAW (非行业中性化) δ_decay 分值在合成分阶段施加惩罚，
        # 使 final_score 和百分位评级自然反映衰退风险。
        raw_decay = factor_raw[FactorId.DELTA_DECAY][i]
        raw_gamma = factor_raw[FactorId.GAMMA][i]
        if raw_decay is not None:
            if raw_decay >= 0.60:
                # 极度衰退: 最多打 8 折
                decay_penalty = 0.80
                final_score *= decay_penalty
            elif raw_decay >= 0.50 and (raw_gamma is None or raw_gamma < 0.45):
                # 严重衰退 + 低成长: 最多打 85 折
                decay_penalty = 0.85
                final_score *= decay_penalty

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
    for i, p in enumerate(new_profiles):
        if p.signal == TruthSignal.FRAUD_ALERT:
            final_profiles.append(p)
            continue
        grade = grade_map.get(i, TruthGrade.C)
        signal = _score_to_signal(p.final_score, scoring)
        final_profiles.append(replace(p, signal=signal, grade=grade))

    logger.info(
        f"v5.1 Cross-Sectional Normalization: "
        f"{len(profiles)} companies, "
        f"{n_hard_constrained} hard-constrained (ROIC<3%), "
        f"percentile grading applied"
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
            "algo_version": "5.2.0",
            "config_version": config.config_version,
            "universe_size": len(profiles),
            "factor_count": 7,
            "solver_count": 3,
            "has_financial_context": has_financial_context,
        },
        "profiles": profiles_dict,
        "summary": summary,
    }


__all__ = ["run_truth", "TruthConfig", "get_default_config"]
