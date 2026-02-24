"""
OOS Validation — 报告生成模块

生成 Markdown 格式的验证报告，包含:
    1. 执行摘要 (Pass/Fail + 稳健性评分)
    2. 参数扰动详情
    3. 公司自举详情
    4. 因子消融详情
    5. 双引擎一致性
    6. 结论与建议
"""

from __future__ import annotations

import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Tuple

from . import metrics


def generate_report(
    results: Dict[str, Any],
    config: Any,
    total_elapsed: float,
) -> str:
    """生成 OOS 验证 Markdown 报告。"""
    lines: List[str] = []
    verdicts: List[Tuple[str, float, bool, float, float, float]] = []

    # ═══════════════════════════════════════════════════════════════════
    # 0. 标题
    # ═══════════════════════════════════════════════════════════════════
    lines.append("# OOS 验证报告 (Out-of-Sample Validation)")
    lines.append("")
    lines.append(f"> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"> 总耗时: {total_elapsed:.1f}s | 随机种子: {config.seed}")
    lines.append(f"> 框架版本: v1.0.0")
    lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 1. 执行摘要
    # ═══════════════════════════════════════════════════════════════════
    overfitting_score = 100.0

    # 收集 Perturbation verdicts
    if "perturbation" in results:
        for pr in results["perturbation"]:
            mean_truth = float(np.mean(pr.truth_rhos))
            mean_eval = float(np.mean(pr.eval_rhos))
            if pr.noise_level <= 0.05:
                threshold = config.perturbation_5pct_pass
            elif pr.noise_level <= 0.10:
                threshold = config.perturbation_10pct_pass
            else:
                threshold = config.perturbation_20pct_pass

            passed = mean_truth >= threshold and mean_eval >= threshold
            verdicts.append(
                ("perturbation", pr.noise_level, passed, mean_truth, mean_eval, threshold)
            )
            if not passed:
                gap = 1 - min(mean_truth, mean_eval) / threshold
                overfitting_score -= min(gap * 30, 15)

    # Bootstrap verdict
    if "bootstrap" in results:
        br = results["bootstrap"]
        mean_truth = float(np.mean(br.truth_rhos))
        mean_eval = float(np.mean(br.eval_rhos))
        passed = mean_truth >= config.bootstrap_pass and mean_eval >= config.bootstrap_pass
        verdicts.append(("bootstrap", 0, passed, mean_truth, mean_eval, config.bootstrap_pass))
        if not passed:
            overfitting_score -= 20

    # Ablation verdict
    if "ablation" in results:
        ar = results["ablation"]
        rho_values = []
        for item in ar.items:
            if not np.isnan(item.rho_vs_baseline):
                rho_values.append(item.rho_vs_baseline)
        min_rho = min(rho_values) if rho_values else 1.0
        passed = (1 - min_rho) <= config.ablation_max_impact
        verdicts.append(("ablation", 0, passed, min_rho, 0, 1 - config.ablation_max_impact))
        if not passed:
            overfitting_score -= 15

    # Cross-engine verdict
    if "cross_engine" in results:
        cr = results["cross_engine"]
        passed = cr.spearman_rho >= config.cross_engine_pass
        verdicts.append(("cross_engine", 0, passed, cr.spearman_rho, 0, config.cross_engine_pass))
        if not passed:
            overfitting_score -= 25

    overfitting_score = max(0, min(100, overfitting_score))

    # Grade
    if overfitting_score >= 90:
        grade_str = "🟢 A — 极其稳健 (无过拟合迹象)"
    elif overfitting_score >= 75:
        grade_str = "🟡 B — 稳健 (轻微敏感性属正常范围)"
    elif overfitting_score >= 60:
        grade_str = "🟠 C — 轻微过拟合风险"
    elif overfitting_score >= 40:
        grade_str = "🔴 D — 中度过拟合"
    else:
        grade_str = "⛔ F — 严重过拟合"

    lines.append("---")
    lines.append("")
    lines.append("## 1. 执行摘要")
    lines.append("")
    lines.append(f"**OOS 稳健性评分: {overfitting_score:.1f}/100** → {grade_str}")
    lines.append("")
    lines.append("| 策略 | TRUTH ρ | EVAL ρ | 阈值 | 结果 |")
    lines.append("|------|---------|--------|------|------|")

    for name, noise, passed, truth_rho, eval_rho, threshold in verdicts:
        label = "✅ PASS" if passed else "❌ FAIL"
        if name == "perturbation":
            lines.append(
                f"| 参数扰动 ±{noise*100:.0f}% | {truth_rho:.4f} | {eval_rho:.4f} | ≥{threshold:.2f} | {label} |"
            )
        elif name == "bootstrap":
            lines.append(
                f"| 公司自举 80% | {truth_rho:.4f} | {eval_rho:.4f} | ≥{threshold:.2f} | {label} |"
            )
        elif name == "ablation":
            lines.append(
                f"| 因子消融 | min={truth_rho:.4f} | — | ≥{threshold:.2f} | {label} |"
            )
        elif name == "cross_engine":
            lines.append(
                f"| 双引擎一致性 | {truth_rho:.4f} | — | ≥{threshold:.2f} | {label} |"
            )

    lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 2. 参数扰动详情
    # ═══════════════════════════════════════════════════════════════════
    if "perturbation" in results:
        lines.append("---")
        lines.append("")
        lines.append("## 2. 参数扰动测试 (Monte Carlo Perturbation)")
        lines.append("")
        lines.append(
            "对 TRUTH (8因子权重 + 3求解器权重 + 各因子组件权重) 和 "
            "Evaluator (8指标权重 + 质量阈值) 的所有超参数施加高斯噪声 $N(0, \\sigma)$，"
            "测量排名稳定性。"
        )
        lines.append("")

        for pr in results["perturbation"]:
            lines.append(
                f"### ±{pr.noise_level*100:.0f}% 噪声 "
                f"({pr.iterations} 次迭代, {pr.elapsed_seconds:.1f}s)"
            )
            lines.append("")

            ts = metrics.aggregate_stability(pr.truth_rhos)
            es = metrics.aggregate_stability(pr.eval_rhos)
            cs = metrics.aggregate_stability(pr.cross_rhos)
            to = metrics.aggregate_stability(pr.truth_top50_overlaps)
            eo = metrics.aggregate_stability(pr.eval_top50_overlaps)
            tg = metrics.aggregate_stability(pr.truth_grade_consistency)
            ed = metrics.aggregate_stability(pr.eval_decision_consistency)

            lines.append("| 指标 | TRUTH | Evaluator |")
            lines.append("|------|-------|-----------|")
            lines.append(
                f"| Spearman ρ (mean±std) | {ts['mean']:.4f}±{ts['std']:.4f} | "
                f"{es['mean']:.4f}±{es['std']:.4f} |"
            )
            lines.append(
                f"| ρ 95% CI | [{ts['ci_95_lower']:.4f}, {ts['ci_95_upper']:.4f}] | "
                f"[{es['ci_95_lower']:.4f}, {es['ci_95_upper']:.4f}] |"
            )
            lines.append(
                f"| ρ min / max | {ts['min']:.4f} / {ts['max']:.4f} | "
                f"{es['min']:.4f} / {es['max']:.4f} |"
            )
            lines.append(
                f"| Top-50 Jaccard | {to['mean']:.4f}±{to['std']:.4f} | "
                f"{eo['mean']:.4f}±{eo['std']:.4f} |"
            )
            lines.append(
                f"| 等级一致率 | {tg['mean']:.1%}±{tg['std']:.1%} | "
                f"{ed['mean']:.1%}±{ed['std']:.1%} |"
            )
            lines.append(
                f"| 交叉 ρ (TRUTH↔Eval) | {cs['mean']:.4f}±{cs['std']:.4f} | — |"
            )
            lines.append("")

        lines.append(
            "> **解读**: ρ 接近 1.0 且 std 小 → 参数在该噪声水平下稳定; "
            "ρ 显著下降 → 参数过拟合到当前精确值。"
        )
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 3. 公司自举详情
    # ═══════════════════════════════════════════════════════════════════
    if "bootstrap" in results:
        lines.append("---")
        lines.append("")
        lines.append("## 3. 公司自举测试 (Bootstrap Resampling)")
        lines.append("")

        br = results["bootstrap"]
        lines.append(
            f"随机抽取 {br.sample_fraction*100:.0f}% 的公司 "
            f"({br.sample_sizes[0] if br.sample_sizes else '?'}) "
            f"× {br.iterations} 次迭代, 总耗时 {br.elapsed_seconds:.1f}s"
        )
        lines.append("")

        ts = metrics.aggregate_stability(br.truth_rhos)
        es = metrics.aggregate_stability(br.eval_rhos)
        cs = metrics.aggregate_stability(br.cross_rhos)
        to = metrics.aggregate_stability(br.truth_top50_overlaps)
        eo = metrics.aggregate_stability(br.eval_top50_overlaps)

        lines.append("| 指标 | TRUTH | Evaluator |")
        lines.append("|------|-------|-----------|")
        lines.append(
            f"| Spearman ρ (mean±std) | {ts['mean']:.4f}±{ts['std']:.4f} | "
            f"{es['mean']:.4f}±{es['std']:.4f} |"
        )
        lines.append(
            f"| ρ 95% CI | [{ts['ci_95_lower']:.4f}, {ts['ci_95_upper']:.4f}] | "
            f"[{es['ci_95_lower']:.4f}, {es['ci_95_upper']:.4f}] |"
        )
        lines.append(
            f"| ρ min / max | {ts['min']:.4f} / {ts['max']:.4f} | "
            f"{es['min']:.4f} / {es['max']:.4f} |"
        )
        lines.append(
            f"| Top-50 Jaccard | {to['mean']:.4f}±{to['std']:.4f} | "
            f"{eo['mean']:.4f}±{eo['std']:.4f} |"
        )
        lines.append(
            f"| 交叉 ρ (TRUTH↔Eval) | {cs['mean']:.4f}±{cs['std']:.4f} | — |"
        )
        lines.append("")
        lines.append(
            "> **解读**: ρ > 0.90 表示排名不受个别公司支配; "
            "ρ < 0.80 表示某些公司对整体排名影响过大。"
        )
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 4. 因子消融详情
    # ═══════════════════════════════════════════════════════════════════
    if "ablation" in results:
        lines.append("---")
        lines.append("")
        lines.append("## 4. 因子消融测试 (Factor Ablation)")
        lines.append("")

        ar = results["ablation"]
        lines.append(
            f"逐一移除因子/权重，测量对排名的影响。"
            f"总耗时 {ar.elapsed_seconds:.1f}s"
        )
        lines.append("")

        # TRUTH 部分
        truth_items = [i for i in ar.items if i.engine == "TRUTH"]
        if truth_items:
            lines.append("### TRUTH 因子 & 求解器")
            lines.append("")
            lines.append("| 移除项 | ρ vs 基线 | 影响程度 | 交叉 ρ | 优质股变化 |")
            lines.append("|--------|-----------|----------|--------|-----------|")

            for item in sorted(truth_items, key=lambda x: x.rho_vs_baseline):
                impact = 1 - item.rho_vs_baseline
                impact_str = _impact_label(impact)
                lines.append(
                    f"| {item.removed_factor} | {item.rho_vs_baseline:.4f} | "
                    f"{impact_str} | {item.cross_rho:.4f} | {item.info} |"
                )
            lines.append("")

        # Evaluator 部分
        eval_items = [i for i in ar.items if i.engine == "Evaluator"]
        if eval_items:
            lines.append("### Evaluator 权重")
            lines.append("")
            lines.append("| 移除项 | ρ vs 基线 | 影响程度 | 交叉 ρ | 优质股变化 |")
            lines.append("|--------|-----------|----------|--------|-----------|")

            for item in sorted(eval_items, key=lambda x: x.rho_vs_baseline):
                impact = 1 - item.rho_vs_baseline
                impact_str = _impact_label(impact)
                lines.append(
                    f"| {item.removed_factor} | {item.rho_vs_baseline:.4f} | "
                    f"{impact_str} | {item.cross_rho:.4f} | {item.info} |"
                )
            lines.append("")

        lines.append(
            "> **解读**: 影响越大 = 该因子对排名越关键。"
            "若移除某因子后交叉 ρ 升高 → 该因子可能引入与另一引擎不一致的噪声。"
        )
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 5. 双引擎一致性
    # ═══════════════════════════════════════════════════════════════════
    if "cross_engine" in results:
        lines.append("---")
        lines.append("")
        lines.append("## 5. 双引擎一致性 (Cross-Engine Consistency)")
        lines.append("")

        cr = results["cross_engine"]
        lines.append(
            "TRUTH (百分位归一化) vs Evaluator (绝对分制) 的独立验证。"
            "两个引擎设计理念不同、评分体系不同，高一致性意味着信号来自数据而非模型偏差。"
        )
        lines.append("")

        lines.append("| 指标 | 值 | 评价 |")
        lines.append("|------|-----|------|")

        rho_label = "🟢 优" if cr.spearman_rho >= 0.80 else ("🟡 良" if cr.spearman_rho >= 0.70 else "🔴 差")
        lines.append(f"| Spearman ρ | {cr.spearman_rho:.4f} | {rho_label} |")
        lines.append(f"| Kendall τ | {cr.kendall_tau:.4f} | — |")
        lines.append(f"| Top-50 Jaccard | {cr.top_50_overlap:.4f} | — |")
        lines.append(f"| Top-100 Jaccard | {cr.top_100_overlap:.4f} | — |")
        lines.append(f"| Top-200 Jaccard | {cr.top_200_overlap:.4f} | — |")
        lines.append(
            f"| 分类一致率 (high/mid/low) | {cr.grade_vs_decision_match:.1%} | — |"
        )
        lines.append(
            f"| 信号对齐 (TRUTH A+/A → Eval quality) | {cr.signal_alignment_pct:.1f}% | — |"
        )
        lines.append(
            f"| 最大排名偏差 | {cr.max_divergence_shift} 位 | {cr.max_divergence_company} |"
        )
        lines.append(
            f"| TRUTH 优质股数 | {cr.truth_quality_count} | — |"
        )
        lines.append(
            f"| Evaluator 优质股数 | {cr.eval_quality_count} | — |"
        )
        lines.append(
            f"| 两引擎重叠优质 | {cr.overlap_quality} | — |"
        )
        lines.append("")

    # ═══════════════════════════════════════════════════════════════════
    # 6. 结论与建议
    # ═══════════════════════════════════════════════════════════════════
    lines.append("---")
    lines.append("")
    lines.append("## 6. 结论与建议")
    lines.append("")

    pass_count = sum(1 for _, _, p, *_ in verdicts if p)
    fail_count = sum(1 for _, _, p, *_ in verdicts if not p)
    total_tests = pass_count + fail_count

    lines.append(f"- **通过**: {pass_count}/{total_tests}")
    lines.append(f"- **失败**: {fail_count}/{total_tests}")
    lines.append(f"- **稳健性评分**: {overfitting_score:.1f}/100")
    lines.append("")

    if overfitting_score >= 85:
        lines.append("### ✅ 结论: 系统高度稳健")
        lines.append("")
        lines.append(
            "当前超参数配置在参数扰动、公司采样变化和因子移除下均保持良好的排名稳定性。"
            "双引擎独立验证一致性高。**过拟合风险极低**。"
        )
        lines.append("")
        lines.append("建议:")
        lines.append("1. 定期 (季度/年度) 重新运行 OOS 验证，确保新数据下仍然稳健")
        lines.append("2. 可在此稳健基础上进一步微调参数")
    elif overfitting_score >= 60:
        lines.append("### ⚠️ 结论: 系统基本稳健，存在轻微敏感性")
        lines.append("")
        lines.append("建议:")
        lines.append("1. 检查消融测试中影响最大的因子，确认其权重是否合理")
        lines.append("2. 考虑对高敏感参数增加正则化约束")
        lines.append("3. 增加 bootstrap 迭代次数以获得更精确的置信区间")
    else:
        lines.append("### ❌ 结论: 存在过拟合风险")
        lines.append("")
        lines.append("建议:")
        lines.append("1. 减少手工调参的超参数数量")
        lines.append("2. 使用更均匀的权重分配 (等权 → 数据驱动)")
        lines.append("3. 引入时序交叉验证 (Walk-Forward)")
        lines.append("4. 考虑参数贝叶斯优化替代手工调参")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*报告由 OOS Validation Framework v1.0 自动生成*")

    return "\n".join(lines)


def _impact_label(impact: float) -> str:
    """将影响值转换为可读标签。"""
    if np.isnan(impact):
        return "—"
    if impact < 0.03:
        return "🟢 微弱"
    if impact < 0.07:
        return "🟡 中等"
    if impact < 0.12:
        return "🟠 显著"
    return "🔴 关键"
