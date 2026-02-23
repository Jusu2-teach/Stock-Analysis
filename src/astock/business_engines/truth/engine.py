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
    """校准层: 计算最终评分、信号、评级

    【T-H1 修复】使用 ScoringConfig 中的权重，而非硬编码
    【T-H2 修复】整合 CalibrationConfig 的数据年限置信度调整
    """
    if is_meltdown:
        return 0.0, TruthSignal.FRAUD_ALERT, TruthGrade.F, 0.0

    scoring = config.scoring

    # 因子加权平均（从 ScoringConfig.factor_weights 获取）
    # v4.6: fallback 值与 config.py ScoringConfig 默认值保持一致
    factor_weight_map = {
        FactorId.ALPHA: scoring.factor_weights.get("ALPHA", 0.12),
        FactorId.BETA: scoring.factor_weights.get("BETA", 0.08),
        FactorId.GAMMA: scoring.factor_weights.get("GAMMA", 0.18),
        FactorId.LAMBDA: scoring.factor_weights.get("LAMBDA", 0.12),
        FactorId.DELTA_FRAUD: scoring.factor_weights.get("DELTA_FRAUD", 0.16),
        FactorId.DELTA_DECAY: scoring.factor_weights.get("DELTA_DECAY", 0.18),
        FactorId.VERIFICATION: scoring.factor_weights.get("VERIFICATION", 0.16),
    }

    weighted_sum = 0.0
    total_weight = 0.0
    for fid, weight in factor_weight_map.items():
        result = factors.get(fid)
        if result and result.score is not None:
            # 负向指标 (越低越好): δ_fraud, δ_decay, λ
            # v4.7: α (周期性) 改为反向 — 低周期性 = 业务稳定性高 = 投资质量优势
            # 原版正向使用导致: 迈瑞/恒瑞等稳定公司因低α被惩罚, 周期性强的公司反被奖励
            if fid in (FactorId.DELTA_FRAUD, FactorId.DELTA_DECAY, FactorId.LAMBDA, FactorId.ALPHA):
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

    # ====== v4.6: Gravity 实际 ROIC vs 动态阈值的余裕度调整 ======
    # 审计发现: Gravity 求解器仅计算阈值的"宽松程度"作为分数,
    # 完全不比较实际 ROIC 是否超过阈值 → 两家因子相同但 ROIC=5% vs 25% 的公司得分相同
    # 修复: 从已注入的 actual_value 计算余裕度, 调整 solver_score
    gravity_result = solvers.get(SolverId.GRAVITY)
    if gravity_result and gravity_result.thresholds:
        roic_th = gravity_result.thresholds.get("roic")
        if roic_th and hasattr(roic_th, 'actual_value') and roic_th.actual_value is not None:
            # actual_value = 公司实际加权ROIC(%), value = 动态阈值(%)
            clearance_pct = roic_th.actual_value - roic_th.value  # 百分点差值
            # v4.7: 扩大余裕度封顶 0.12→0.18
            # 原版问题: ROIC=30% 和 ROIC=15% 获得相同奖励(0.12)
            # 30%的护城河明显优于15%, 应有更大区分度
            clearance_bonus = max(-0.18, min(0.18, clearance_pct / 10.0 * 0.12))
            solver_score = max(0.0, min(1.0, solver_score + clearance_bonus))

    # 最终分数 = 因子 × factor_vs_solver_weight + 求解器 × (1 - factor_vs_solver_weight)
    factor_ratio = scoring.factor_vs_solver_weight
    final_score = factor_score * factor_ratio + solver_score * (1.0 - factor_ratio)

    # ====== v4.8: 提取 ROIC 实际值供多个门控使用 ======
    # roic_for_gate = weighted_avg (用于 clearance_bonus 等趋势性判断)
    # roic_latest  = latest_value  (用于绝对水平门控 — 当前真实盈利能力)
    # 设计原因: weighted_avg 可能掩盖近期恶化
    #   华宝新能: weighted_avg=34.3%, latest=3.3% → weighted 看着很好但实际已崩塌
    roic_for_gate = None
    roic_latest = None
    gravity_result_gate = solvers.get(SolverId.GRAVITY)
    if gravity_result_gate:
        if gravity_result_gate.thresholds:
            roic_th_gate = gravity_result_gate.thresholds.get("roic")
            if roic_th_gate and hasattr(roic_th_gate, 'actual_value'):
                roic_for_gate = roic_th_gate.actual_value  # weighted_avg
        # v4.8: 从 components 获取 latest_value (由 _inject_actual_values 注入)
        roic_latest = gravity_result_gate.components.get("roic_latest_value")
    # 门控用最新值 (如果有), 否则 fallback 到 weighted_avg
    roic_for_level_gate = roic_latest if roic_latest is not None else roic_for_gate

    # ====== v4.6: δ_decay 硬性门控 ======
    # 审计发现: 22.1% 的"优质"公司带 δ_decay>0.30 的衰退信号通过评估
    #   - 三生国健 δ_decay=0.58 却获 A+ (75.50%)
    #   - 国邦医药 处于"衰退期" + δ_decay=0.55 获 A (68.2%)
    # 根因: δ_decay 权重(0.12) 被 V(0.23)+γ(0.22) 淹没
    # 修复: 类似 Evaluator 的 ROIC 硬封顶, 加 δ_decay 后置门控
    delta_decay = factors.get(FactorId.DELTA_DECAY)
    gamma = factors.get(FactorId.GAMMA)
    if delta_decay and delta_decay.score is not None:
        dd = delta_decay.score
        g = gamma.score if gamma and gamma.score is not None else 0.5

        # v4.7: 绝对水平豁免 — ROIC 卓越的公司即使有轻微衰退也不应被硬封顶
        # 问题: 迈瑞医疗 ROIC=30% 从32%→30%(微小波动) → δ_decay≈0.20→35
        #       触发 "中度衰退"封顶 0.72, 永远拿不到 A+
        # 修复: ROIC>20%的公司, 封顶阈值上移; ROIC>25%取消封顶
        # v4.8: 同时检查 latest ROIC — 防止 weighted_avg 掩盖崩塌
        #   华宝新能: weighted=34% → 豁免, 但 latest=3.3% → 不应豁免!
        # 豁免条件: weighted_avg ≥ 阈值 AND latest ≥ 阈值的一半 (容忍波动但不容忍崩塌)
        roic_gate_val = roic_for_gate  # weighted_avg
        roic_latest_check = roic_latest if roic_latest is not None else roic_gate_val
        # 卓越水平豁免: ROIC >= 25% → 完全取消δ封顶, 但 latest 必须 >= 15%
        # ROIC 20-25% → 放宽封顶, 但 latest 必须 >= 12%
        if (roic_gate_val is not None and roic_gate_val >= 25.0
                and roic_latest_check is not None and roic_latest_check >= 15.0):
            pass  # 不应用任何封顶 (世界级 ROIC + 轻微衰退 = 正常波动)
        elif (roic_gate_val is not None and roic_gate_val >= 20.0
                and roic_latest_check is not None and roic_latest_check >= 12.0):
            # ROIC 20-25%: 提升封顶阈值 (+0.05)
            if dd >= 0.50 and g < 0.45:
                final_score = min(final_score, 0.67)
            elif dd >= 0.50:
                final_score = min(final_score, 0.73)
            elif dd >= 0.35:
                final_score = min(final_score, 0.77)
        else:
            # v4.9: 成长语境感知的 δ_decay 门控
            # v4.8 问题: dd≥0.50→cap0.68 和 dd≥0.35→cap0.72 导致过度惩罚
            #   药明康德 dd=0.89(行业周期性下行) + γ=0.67 + V=0.82 → raw=76.6% capped at 68%
            #   因子权重(0.18)已充分惩罚衰退, 硬门控是二次处罚
            # v4.9: 仅在衰退+停滞同时发生时硬性封顶
            #   高增长抵消衰退 = 周期性下行(CRO行业整体不景气), 非结构性恶化
            if dd >= 0.50 and g < 0.45:
                # 结构性衰退: 显著衰退 + 无增长 → 严格封顶
                final_score = min(final_score, 0.62)
            elif dd >= 0.70 and g < 0.55:
                # 严重衰退 + 增长乏力 → 中度封顶
                final_score = min(final_score, 0.68)
            # 移除: dd≥0.50→cap0.68 (因子权重已充分体现, 避免二次处罚)
            # 移除: dd≥0.35→cap0.72 (阈值过低, 正常波动也被捕获)

    # ====== v4.9: ROIC 绝对水平 — 连续软惩罚 ======
    # v4.8 使用阶梯函数(8-10→0.65, 10-12→0.72) 造成大量分数聚集
    #   中科曙光 ROIC=8.8% raw=77.9% → capped at 65.0% (lost 12.9pp)
    #   扬杰科技 ROIC=8.5% raw=74.9% → capped at 65.0% (lost 9.9pp)
    # 世界级量化系统(AQR QMJ, GMO)使用连续惩罚而非离散阶梯
    # v4.9 修复: 连续惩罚函数 + 成长调整 + 硬性地板
    #   经济学逻辑: ROIC每低于12%一个百分点 → 递增惩罚
    #   但高成长公司(γ≥0.60)惩罚减半(当前低ROIC可能因大量资本开支, 未来将改善)
    if roic_for_level_gate is not None:
        if roic_for_level_gate < 5.0:
            # ROIC < 5%: 严重价值毁灭, 硬性地板
            final_score = min(final_score, 0.45)
        elif roic_for_level_gate < 12.0:
            # 连续惩罚: 每低于12%一个百分点 → 惩罚 1.2pp
            # ROIC=11% → -0.012, ROIC=10% → -0.024, ROIC=8% → -0.048
            roic_penalty = (12.0 - roic_for_level_gate) * 0.012
            # 成长调整: γ≥0.60 的高增长公司, 惩罚打折
            # 逻辑: 比亚迪/宁德时代等公司资本开支大→当前ROIC被压低
            #       但高增长意味着产能利用率将提升→未来ROIC改善
            gamma_val = factors.get(FactorId.GAMMA)
            if gamma_val and gamma_val.score is not None and gamma_val.score >= 0.60:
                # γ=0.60→0%折扣, γ=0.80→50%折扣 (线性插值)
                growth_discount = min(0.50, (gamma_val.score - 0.60) * 2.5)
                roic_penalty *= (1.0 - growth_discount)
            final_score -= roic_penalty
            # 安全地板: ROIC<6% 绝不应是 A, ROIC<8% 绝不应是 A+
            if roic_for_level_gate < 6.0:
                final_score = min(final_score, 0.48)  # C+ 地板
            elif roic_for_level_gate < 8.0:
                final_score = min(final_score, 0.58)  # B 地板

    # 计算置信度
    confidences = [r.confidence for r in factors.values() if r.confidence]
    confidences.extend([r.confidence for r in solvers.values() if r.confidence])
    confidence = sum(confidences) / len(confidences) if confidences else 0.5

    # 【T-H2 v3.4 修复】数据年限置信度调整 — 平滑曲线替代硬性cap
    cal = config.calibration
    if data_years < cal.full_confidence_years:
        # 平滑曲线: 3年→60%, 5年→85%, 7年→100%
        if data_years <= cal.min_data_years:
            year_factor = cal.min_confidence_3y if hasattr(cal, 'min_confidence_3y') else 0.60
        else:
            # 线性插值: min_data_years → max_confidence_5y → full
            progress = (data_years - cal.min_data_years) / (cal.full_confidence_years - cal.min_data_years)
            year_factor = cal.max_confidence_5y + (1.0 - cal.max_confidence_5y) * progress
        confidence = confidence * year_factor
    # 不再使用 min() 硬性 cap, 让高质量公司可以突破天花板

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

    profiles_dict = [_profile_to_dict(p) for p in profiles]
    summary = _calculate_summary(profiles)

    return {
        "metadata": {
            "algo_version": "4.6.0",
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
