"""
OOS Validation — 验证策略模块

四大策略:
    1. PerturbationStrategy  — Monte Carlo 参数扰动
    2. BootstrapStrategy     — 公司自举重采样
    3. AblationStrategy      — 因子/权重消融
    4. CrossEngineStrategy   — 双引擎一致性深度分析
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace as dc_replace
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from . import metrics
from .data_loader import filter_by_companies, get_all_ts_codes

logger = logging.getLogger("oos_validation")


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: Score / Grade 提取
# ═══════════════════════════════════════════════════════════════════════════════

def extract_truth_scores(result: Dict[str, Any]) -> Dict[str, float]:
    """从 TRUTH 结果中提取 {ts_code: final_score}。"""
    return {p["ts_code"]: p["final_score"] for p in result["profiles"]}


def extract_truth_grades(result: Dict[str, Any]) -> Dict[str, str]:
    """从 TRUTH 结果中提取 {ts_code: grade}。"""
    return {p["ts_code"]: p["grade"] for p in result["profiles"]}


def extract_eval_scores(result: Dict[str, Any]) -> Dict[str, float]:
    """从 Evaluator 结果中提取 {ts_code: score}。"""
    return {e["ts_code"]: e["score"] for e in result["evaluations"]}


def extract_eval_decisions(result: Dict[str, Any]) -> Dict[str, str]:
    """从 Evaluator 结果中提取 {ts_code: decision}。"""
    return {e["ts_code"]: e["decision"] for e in result["evaluations"]}


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: 带配置覆盖的引擎调用
# ═══════════════════════════════════════════════════════════════════════════════

def run_truth_with_config(
    aggregated_trends: Dict[str, pd.DataFrame], config
) -> Dict[str, Any]:
    """以自定义 TruthConfig 运行 TRUTH (monkey-patch get_default_config)。

    TRUTH 的 run_truth() 不接受 config 参数，内部调用 get_default_config()。
    通过临时替换模块级引用实现配置注入。
    """
    import src.astock.business_engines.truth.engine as truth_mod

    original_fn = truth_mod.get_default_config
    truth_mod.get_default_config = lambda: config
    try:
        return truth_mod.run_truth(aggregated_trends)
    finally:
        truth_mod.get_default_config = original_fn


def run_evaluator_with_config(
    aggregated_trends: Dict[str, pd.DataFrame],
    config_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """运行 Evaluator (支持字典式配置覆盖)。"""
    from src.astock.business_engines.evaluators.engine import (
        run_causal_bayesian_evaluator,
    )

    return run_causal_bayesian_evaluator(aggregated_trends, config=config_override)


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: TruthConfig / EvaluatorConfig 扰动
# ═══════════════════════════════════════════════════════════════════════════════

def _perturb_weights(
    weights: dict, noise_std: float, rng: np.random.Generator
) -> dict:
    """对权重字典施加高斯噪声并归一化。"""
    new_w = {}
    for k, v in weights.items():
        new_w[k] = max(0.01, v * (1 + rng.normal(0, noise_std)))
    total = sum(new_w.values())
    return {k: v / total for k, v in new_w.items()}


def perturb_truth_config(config, noise_std: float, rng: np.random.Generator):
    """创建扰动后的 TruthConfig。

    扰动范围:
        - ScoringConfig.factor_weights (8 维)
        - ScoringConfig.solver_weights (3 维)
        - ScoringConfig.factor_vs_solver_weight
        - 各因子配置的 component_weights
    """
    # 1) 扰动 Layer 3 评分权重
    new_fw = _perturb_weights(dict(config.scoring.factor_weights), noise_std, rng)
    new_sw = _perturb_weights(dict(config.scoring.solver_weights), noise_std, rng)
    new_fvs = float(
        np.clip(
            config.scoring.factor_vs_solver_weight * (1 + rng.normal(0, noise_std)),
            0.2,
            0.9,
        )
    )
    new_scoring = dc_replace(
        config.scoring,
        factor_weights=new_fw,
        solver_weights=new_sw,
        factor_vs_solver_weight=new_fvs,
    )

    # 2) 扰动各因子的 component_weights
    factor_configs = {}
    for attr in [
        "alpha_config",
        "beta_config",
        "gamma_config",
        "pi_config",
        "lambda_config",
        "delta_fraud_config",
        "delta_decay_config",
        "verification_config",
    ]:
        fc = getattr(config, attr)
        if hasattr(fc, "component_weights"):
            new_cw = _perturb_weights(
                dict(fc.component_weights), noise_std, rng
            )
            factor_configs[attr] = dc_replace(fc, component_weights=new_cw)

    return dc_replace(config, scoring=new_scoring, **factor_configs)


def perturb_eval_config(noise_std: float, rng: np.random.Generator) -> Dict[str, Any]:
    """生成扰动后的 EvaluatorConfig 覆盖字典。"""
    base_weights = {
        "roic_trend": 0.22,
        "roe_trend": 0.08,
        "revenue_trend": 0.12,
        "gross_margin_trend": 0.14,
        "net_margin_trend": 0.10,
        "ocf_trend": 0.14,
        "roiic_trend": 0.10,
        "profit_trend": 0.10,
    }
    new_weights = _perturb_weights(base_weights, noise_std, rng)
    new_qt = float(np.clip(72.0 * (1 + rng.normal(0, noise_std)), 50, 90))
    return {"score_weights": new_weights, "quality_threshold": new_qt}


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy 1: Monte Carlo Parameter Perturbation
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class PerturbationResult:
    noise_level: float
    iterations: int
    truth_rhos: List[float]
    eval_rhos: List[float]
    cross_rhos: List[float]
    truth_top50_overlaps: List[float]
    eval_top50_overlaps: List[float]
    truth_grade_consistency: List[float]
    eval_decision_consistency: List[float]
    elapsed_seconds: float


def run_perturbation_strategy(
    aggregated_trends: Dict[str, pd.DataFrame],
    baseline_truth: Dict[str, Any],
    baseline_eval: Dict[str, Any],
    noise_levels: Optional[List[float]] = None,
    iterations: int = 5,
    seed: int = 42,
) -> List[PerturbationResult]:
    """Monte Carlo 参数扰动: 对所有超参数施加高斯噪声，测量排名稳定性。

    测试目的:
        若 ρ 在 ±5% 噪声下 > 0.95 → 排名对参数微调不敏感 (稳健)
        若 ρ 在 ±5% 噪声下 < 0.90 → 参数过度拟合 (脆弱)
    """
    if noise_levels is None:
        noise_levels = [0.05, 0.10, 0.20]

    from src.astock.business_engines.truth.config import get_default_config

    base_config = get_default_config()
    base_truth_scores = extract_truth_scores(baseline_truth)
    base_truth_grades = extract_truth_grades(baseline_truth)
    base_eval_scores = extract_eval_scores(baseline_eval)
    base_eval_decisions = extract_eval_decisions(baseline_eval)

    rng = np.random.default_rng(seed)
    results: List[PerturbationResult] = []

    for noise in noise_levels:
        logger.info(f"  扰动 ±{noise*100:.0f}%: {iterations} 次迭代...")
        t0 = time.time()

        truth_rhos, eval_rhos, cross_rhos = [], [], []
        truth_overlaps, eval_overlaps = [], []
        truth_grades, eval_decisions = [], []

        for i in range(iterations):
            logger.info(f"    [{i+1}/{iterations}]")

            # TRUTH 扰动
            perturbed_config = perturb_truth_config(base_config, noise, rng)
            truth_result = run_truth_with_config(aggregated_trends, perturbed_config)
            t_scores = extract_truth_scores(truth_result)
            t_grades = extract_truth_grades(truth_result)

            # Evaluator 扰动
            eval_override = perturb_eval_config(noise, rng)
            eval_result = run_evaluator_with_config(aggregated_trends, eval_override)
            e_scores = extract_eval_scores(eval_result)
            e_decisions = extract_eval_decisions(eval_result)

            # 与基线比较
            truth_rhos.append(metrics.spearman_rho(base_truth_scores, t_scores))
            eval_rhos.append(metrics.spearman_rho(base_eval_scores, e_scores))
            cross_rhos.append(metrics.spearman_rho(t_scores, e_scores))
            truth_overlaps.append(
                metrics.top_k_overlap(base_truth_scores, t_scores, 50)
            )
            eval_overlaps.append(
                metrics.top_k_overlap(base_eval_scores, e_scores, 50)
            )
            truth_grades.append(
                metrics.grade_consistency(base_truth_grades, t_grades)
            )
            eval_decisions.append(
                metrics.grade_consistency(base_eval_decisions, e_decisions)
            )

        elapsed = time.time() - t0
        results.append(
            PerturbationResult(
                noise_level=noise,
                iterations=iterations,
                truth_rhos=truth_rhos,
                eval_rhos=eval_rhos,
                cross_rhos=cross_rhos,
                truth_top50_overlaps=truth_overlaps,
                eval_top50_overlaps=eval_overlaps,
                truth_grade_consistency=truth_grades,
                eval_decision_consistency=eval_decisions,
                elapsed_seconds=elapsed,
            )
        )
        logger.info(
            f"    ±{noise*100:.0f}% 完成: "
            f"TRUTH ρ={np.mean(truth_rhos):.4f}, "
            f"EVAL ρ={np.mean(eval_rhos):.4f} "
            f"({elapsed:.1f}s)"
        )

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy 2: Company Bootstrap Resampling
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class BootstrapResult:
    sample_fraction: float
    iterations: int
    truth_rhos: List[float]
    eval_rhos: List[float]
    cross_rhos: List[float]
    truth_top50_overlaps: List[float]
    eval_top50_overlaps: List[float]
    sample_sizes: List[int]
    elapsed_seconds: float


def run_bootstrap_strategy(
    aggregated_trends: Dict[str, pd.DataFrame],
    baseline_truth: Dict[str, Any],
    baseline_eval: Dict[str, Any],
    sample_fraction: float = 0.80,
    iterations: int = 10,
    seed: int = 1042,
) -> BootstrapResult:
    """公司自举: 随机抽取 N% 公司 × K 次，测量排名稳定性。

    测试目的:
        若 ρ > 0.90 → 排名不受个别公司支配 (稳健)
        若 ρ < 0.80 → 少数公司对排名有决定性影响 (脆弱)
    """
    all_ts_codes = get_all_ts_codes(aggregated_trends)
    sample_size = int(len(all_ts_codes) * sample_fraction)

    base_truth_scores = extract_truth_scores(baseline_truth)
    base_eval_scores = extract_eval_scores(baseline_eval)

    rng = np.random.default_rng(seed)

    truth_rhos, eval_rhos, cross_rhos = [], [], []
    truth_overlaps, eval_overlaps = [], []
    sample_sizes: List[int] = []

    logger.info(
        f"  自举: {iterations} 次, "
        f"{sample_fraction*100:.0f}% 采样 ({sample_size}/{len(all_ts_codes)})"
    )
    t0 = time.time()

    from src.astock.business_engines.truth.engine import run_truth
    from src.astock.business_engines.evaluators.engine import (
        run_causal_bayesian_evaluator,
    )

    for i in range(iterations):
        logger.info(f"    [{i+1}/{iterations}]")

        sampled = set(rng.choice(all_ts_codes, size=sample_size, replace=False))
        filtered = filter_by_companies(aggregated_trends, sampled)
        sample_sizes.append(len(sampled))

        truth_result = run_truth(filtered)
        eval_result = run_causal_bayesian_evaluator(filtered)

        t_scores = extract_truth_scores(truth_result)
        e_scores = extract_eval_scores(eval_result)

        # 与基线比较 (仅重叠公司)
        truth_rhos.append(metrics.spearman_rho(base_truth_scores, t_scores))
        eval_rhos.append(metrics.spearman_rho(base_eval_scores, e_scores))
        cross_rhos.append(metrics.spearman_rho(t_scores, e_scores))
        truth_overlaps.append(
            metrics.top_k_overlap(base_truth_scores, t_scores, 50)
        )
        eval_overlaps.append(
            metrics.top_k_overlap(base_eval_scores, e_scores, 50)
        )

    elapsed = time.time() - t0
    logger.info(
        f"    自举完成: TRUTH ρ={np.mean(truth_rhos):.4f}, "
        f"EVAL ρ={np.mean(eval_rhos):.4f} ({elapsed:.1f}s)"
    )

    return BootstrapResult(
        sample_fraction=sample_fraction,
        iterations=iterations,
        truth_rhos=truth_rhos,
        eval_rhos=eval_rhos,
        cross_rhos=cross_rhos,
        truth_top50_overlaps=truth_overlaps,
        eval_top50_overlaps=eval_overlaps,
        sample_sizes=sample_sizes,
        elapsed_seconds=elapsed,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy 3: Factor / Weight Ablation
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class AblationItem:
    removed_factor: str
    engine: str  # "TRUTH" | "Evaluator"
    rho_vs_baseline: float
    cross_rho: float
    quality_delta: int
    info: str


@dataclass
class AblationResult:
    items: List[AblationItem]
    elapsed_seconds: float


def run_ablation_strategy(
    aggregated_trends: Dict[str, pd.DataFrame],
    baseline_truth: Dict[str, Any],
    baseline_eval: Dict[str, Any],
) -> AblationResult:
    """因子消融: 逐一移除因子/权重，测量对排名的影响。

    测试目的:
        - 识别哪些因子对排名影响最大 (关键因子)
        - 若移除某因子后 ρ 反而升高 → 该因子可能引入噪声
        - 若单个因子移除导致 ρ 剧降 → 系统过度依赖该因子
    """
    from src.astock.business_engines.truth.config import get_default_config

    base_config = get_default_config()
    base_truth_scores = extract_truth_scores(baseline_truth)
    base_eval_scores = extract_eval_scores(baseline_eval)

    # 基线优质股数量
    baseline_truth_quality = sum(
        1 for p in baseline_truth["profiles"] if p["grade"] in ("A+", "A", "B+")
    )
    baseline_eval_quality = len(baseline_eval.get("quality_companies", []))

    items: List[AblationItem] = []
    t0 = time.time()

    # ── TRUTH 因子消融 ──
    factor_names = list(base_config.scoring.factor_weights.keys())
    logger.info(
        f"  消融: {len(factor_names)} TRUTH 因子 + "
        f"{len(base_config.scoring.solver_weights)} 求解器 + "
        f"8 Evaluator 权重"
    )

    for factor in factor_names:
        logger.info(f"    TRUTH 消融: {factor}")

        new_fw = dict(base_config.scoring.factor_weights)
        new_fw[factor] = 0.0
        remaining = sum(v for k, v in new_fw.items() if k != factor)
        if remaining > 0:
            for k in new_fw:
                if k != factor:
                    new_fw[k] /= remaining

        new_scoring = dc_replace(base_config.scoring, factor_weights=new_fw)
        new_config = dc_replace(base_config, scoring=new_scoring)

        truth_result = run_truth_with_config(aggregated_trends, new_config)
        t_scores = extract_truth_scores(truth_result)

        rho = metrics.spearman_rho(base_truth_scores, t_scores)
        quality_count = sum(
            1 for p in truth_result["profiles"] if p["grade"] in ("A+", "A", "B+")
        )
        delta = quality_count - baseline_truth_quality

        items.append(
            AblationItem(
                removed_factor=factor,
                engine="TRUTH",
                rho_vs_baseline=rho,
                cross_rho=metrics.spearman_rho(t_scores, base_eval_scores),
                quality_delta=delta,
                info=f"优质 {quality_count} (Δ{delta:+d})",
            )
        )

    # ── TRUTH 求解器消融 ──
    solver_names = list(base_config.scoring.solver_weights.keys())
    for solver in solver_names:
        logger.info(f"    TRUTH 消融求解器: {solver}")

        new_sw = dict(base_config.scoring.solver_weights)
        new_sw[solver] = 0.0
        remaining = sum(v for k, v in new_sw.items() if k != solver)
        if remaining > 0:
            for k in new_sw:
                if k != solver:
                    new_sw[k] /= remaining

        new_scoring = dc_replace(base_config.scoring, solver_weights=new_sw)
        new_config = dc_replace(base_config, scoring=new_scoring)

        truth_result = run_truth_with_config(aggregated_trends, new_config)
        t_scores = extract_truth_scores(truth_result)

        rho = metrics.spearman_rho(base_truth_scores, t_scores)
        quality_count = sum(
            1 for p in truth_result["profiles"] if p["grade"] in ("A+", "A", "B+")
        )
        delta = quality_count - baseline_truth_quality

        items.append(
            AblationItem(
                removed_factor=f"Solver_{solver}",
                engine="TRUTH",
                rho_vs_baseline=rho,
                cross_rho=metrics.spearman_rho(t_scores, base_eval_scores),
                quality_delta=delta,
                info=f"优质 {quality_count} (Δ{delta:+d})",
            )
        )

    # ── Evaluator 权重消融 ──
    eval_base_weights = {
        "roic_trend": 0.22,
        "roe_trend": 0.08,
        "revenue_trend": 0.12,
        "gross_margin_trend": 0.14,
        "net_margin_trend": 0.10,
        "ocf_trend": 0.14,
        "roiic_trend": 0.10,
        "profit_trend": 0.10,
    }

    for weight_name in eval_base_weights:
        logger.info(f"    Evaluator 消融: {weight_name}")

        new_weights = dict(eval_base_weights)
        new_weights[weight_name] = 0.0
        remaining = sum(v for k, v in new_weights.items() if k != weight_name)
        if remaining > 0:
            for k in new_weights:
                if k != weight_name:
                    new_weights[k] /= remaining

        eval_result = run_evaluator_with_config(
            aggregated_trends, {"score_weights": new_weights}
        )
        e_scores = extract_eval_scores(eval_result)

        rho = metrics.spearman_rho(base_eval_scores, e_scores)
        eval_quality = len(eval_result.get("quality_companies", []))
        delta = eval_quality - baseline_eval_quality

        items.append(
            AblationItem(
                removed_factor=weight_name,
                engine="Evaluator",
                rho_vs_baseline=rho,
                cross_rho=metrics.spearman_rho(base_truth_scores, e_scores),
                quality_delta=delta,
                info=f"优质 {eval_quality} (Δ{delta:+d})",
            )
        )

    elapsed = time.time() - t0
    logger.info(f"    消融完成 ({elapsed:.1f}s)")
    return AblationResult(items=items, elapsed_seconds=elapsed)


# ═══════════════════════════════════════════════════════════════════════════════
# Strategy 4: Cross-Engine Consistency Deep Dive
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class CrossEngineResult:
    spearman_rho: float
    kendall_tau: float
    top_50_overlap: float
    top_100_overlap: float
    top_200_overlap: float
    grade_vs_decision_match: float
    max_divergence_shift: int
    max_divergence_company: str
    truth_quality_count: int
    eval_quality_count: int
    overlap_quality: int
    signal_alignment_pct: float
    elapsed_seconds: float


def run_cross_engine_strategy(
    baseline_truth: Dict[str, Any],
    baseline_eval: Dict[str, Any],
) -> CrossEngineResult:
    """双引擎一致性: TRUTH 百分位制 vs Evaluator 绝对分制深度对比。

    这是最核心的验证: 两个独立设计的引擎对同一数据的评估是否一致。
    """
    t0 = time.time()

    truth_scores = extract_truth_scores(baseline_truth)
    eval_scores = extract_eval_scores(baseline_eval)
    truth_grades = extract_truth_grades(baseline_truth)
    eval_decisions = extract_eval_decisions(baseline_eval)

    rho = metrics.spearman_rho(truth_scores, eval_scores)
    tau = metrics.kendall_tau(truth_scores, eval_scores)
    top50 = metrics.top_k_overlap(truth_scores, eval_scores, 50)
    top100 = metrics.top_k_overlap(truth_scores, eval_scores, 100)
    top200 = metrics.top_k_overlap(truth_scores, eval_scores, 200)

    # 信号对齐: TRUTH A+/A → Eval quality ?
    truth_quality_set = {k for k, v in truth_grades.items() if v in ("A+", "A")}
    eval_quality_set = set(baseline_eval.get("quality_companies", []))
    overlap = truth_quality_set & eval_quality_set

    # 等级→决策一致性 (简化分组)
    grade_to_group = {"A+": "high", "A": "high", "B+": "mid", "B": "mid"}
    decision_to_group = {"quality": "high", "average": "mid"}
    g_a = {k: grade_to_group.get(v, "low") for k, v in truth_grades.items()}
    g_b = {k: decision_to_group.get(v, "low") for k, v in eval_decisions.items()}
    grade_match = metrics.grade_consistency(g_a, g_b)

    # 信号对齐百分比
    set(truth_scores.keys()) & set(eval_scores.keys())
    if truth_quality_set:
        signal_aligned = sum(1 for ts in truth_quality_set if ts in eval_quality_set)
        signal_pct = signal_aligned / len(truth_quality_set) * 100
    else:
        signal_pct = 0.0

    max_shift, worst = metrics.max_rank_shift(truth_scores, eval_scores)

    elapsed = time.time() - t0

    return CrossEngineResult(
        spearman_rho=rho,
        kendall_tau=tau,
        top_50_overlap=top50,
        top_100_overlap=top100,
        top_200_overlap=top200,
        grade_vs_decision_match=grade_match,
        max_divergence_shift=max_shift,
        max_divergence_company=worst,
        truth_quality_count=len(truth_quality_set),
        eval_quality_count=len(eval_quality_set),
        overlap_quality=len(overlap),
        signal_alignment_pct=signal_pct,
        elapsed_seconds=elapsed,
    )
