"""Reporting Engine Entry Point (v4.0).

报告类型:
    1. ``report_comprehensive``: 规则驱动的综合趋势分析报告 (基于 evaluators)
    2. ``report_truth``: 基于 T.R.U.T.H. 六因子三求解器的专业分析报告

架构说明:
    ┌─────────────────┐
    │  trend/engine   │  (8个探针)
    └────────┬────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌──────────┐    ┌──────────┐
│evaluators│    │  truth   │
│(规则引擎)│    │(基因求解)│
└────┬─────┘    └────┬─────┘
     │               │
     ▼               ▼
┌──────────┐    ┌──────────┐
│report_   │    │report_   │
│comprehen.│    │truth     │
└──────────┘    └──────────┘

版本: 4.0.0
更新: 2026-01-22 - 架构重构，evaluators 与 truth 并行，各自产出独立报告
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

from orchestrator.decorators.register import register_method


# ═══════════════════════════════════════════════════════════════════════════════
# 综合报告引擎 (基于 Evaluators 规则引擎)
# ═══════════════════════════════════════════════════════════════════════════════

# 评级 emoji 映射
GRADE_EMOJI = {
    "A": "⭐⭐⭐",
    "B": "⭐⭐",
    "C": "⭐",
    "D": "⚠️",
    "F": "❌",
}

# 策略名称映射
STRATEGY_NAMES = {
    "high_growth": "🚀 高成长",
    "turnaround": "🔄 困境反转",
    "stable_dividend": "💰 稳定分红",
    "cyclical_bottom": "📉 周期底部",
    "moat_defense": "🏰 护城河防守",
}


def _generate_comprehensive_section(evaluation: Dict[str, Any]) -> List[str]:
    """生成单支股票的报告章节"""
    lines = []
    ts_code = evaluation.get("ts_code", "未知")
    grade = evaluation.get("grade", "F")
    score = evaluation.get("composite_score", 0)
    passes = evaluation.get("passes", False)
    strategies = evaluation.get("matched_strategies", [])

    # 标题
    grade_emoji = GRADE_EMOJI.get(grade, "")
    status = "✅" if passes else "❌"

    lines.append(f"### {ts_code} {status} {grade_emoji} ({grade}级)")
    lines.append("")

    # 综合评分
    lines.append(f"**综合评分**: {score:.1f}/100 | **评级**: {grade}")
    if not passes:
        reasons = evaluation.get("elimination_reasons", [])
        if reasons:
            lines.append(f"**淘汰原因**: {'; '.join(reasons)}")
    lines.append("")

    # 命中策略
    if strategies:
        strategy_labels = [STRATEGY_NAMES.get(s, s) for s in strategies]
        lines.append(f"**投资策略**: {', '.join(strategy_labels)}")
        lines.append("")

    # 指标详情表
    metric_results = evaluation.get("metric_results", {})
    if metric_results:
        lines.append("#### 指标评估详情")
        lines.append("")
        lines.append("| 指标 | 得分 | 评级 | 扣分明细 |")
        lines.append("|------|------|------|----------|")

        for metric_name, result in metric_results.items():
            m_score = result.get("score", 0)
            m_grade = result.get("grade", "F")
            m_penalties = result.get("penalty_details", [])
            penalty_str = "; ".join(m_penalties[:2]) if m_penalties else "-"
            lines.append(f"| {metric_name} | {m_score:.1f} | {m_grade} | {penalty_str} |")

        lines.append("")

    lines.append("---")
    lines.append("")

    return lines


@register_method(
    engine_name="report_comprehensive",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate comprehensive analysis report from evaluator results (rule-based)",
)
def report_comprehensive(
    evaluator_result: Dict[str, Any],
    output_path: str = "data/comprehensive_analysis_report.md",
) -> str:
    """
    生成综合趋势分析报告（规则驱动，基于 evaluators 结果）

    数据流:
        trend/engine (8个探针)
            ↓ aggregated_trends
        evaluators/engine (run_evaluator)
            ↓ evaluator_result
        reporters/engine (本方法)
            ↓
        Markdown 报告

    Args:
        evaluator_result: evaluators/engine.run_evaluator 的输出结果
            {
                "algo_version": "1.0.0",
                "universe_size": N,
                "rule_count": 29,
                "strategy_count": 5,
                "evaluations": [...],
                "summary": {...}
            }
        output_path: 输出报告路径

    Returns:
        报告内容字符串
    """
    if not evaluator_result:
        raise ValueError("evaluator_result 不能为空，请先运行 run_evaluator")

    evaluations = evaluator_result.get("evaluations", [])
    summary = evaluator_result.get("summary", {})

    lines: List[str] = []

    # ============================================================
    # 报告头部
    # ============================================================

    lines.append("# 📊 AStock 综合基本面分析报告")
    lines.append("")
    lines.append("> 规则驱动 × 29条评估规则 × 5种投资策略")
    lines.append("")

    # ============================================================
    # 元数据
    # ============================================================

    lines.append("## 📋 报告概要")
    lines.append("")
    lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **算法版本**: {evaluator_result.get('algo_version', '1.0.0')}")
    lines.append(f"- **分析股票数**: {evaluator_result.get('universe_size', len(evaluations))}")
    lines.append(f"- **评估规则数**: {evaluator_result.get('rule_count', 29)}")
    lines.append(f"- **投资策略数**: {evaluator_result.get('strategy_count', 5)}")
    lines.append("")

    # ============================================================
    # 汇总统计
    # ============================================================

    if summary:
        lines.append("## 📊 汇总统计")
        lines.append("")

        # 评级分布
        grade_dist = summary.get("grade_distribution", {})
        if grade_dist:
            lines.append("### 评级分布")
            lines.append("")
            lines.append("| 评级 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            total = sum(grade_dist.values())
            for grade in ["A", "B", "C", "D", "F"]:
                count = grade_dist.get(grade, 0)
                pct = count / total * 100 if total > 0 else 0
                emoji = GRADE_EMOJI.get(grade, "")
                lines.append(f"| {emoji} {grade} | {count} | {pct:.1f}% |")
            lines.append("")

        # 通过/淘汰统计
        lines.append("### 筛选结果")
        lines.append("")
        lines.append(f"- **通过**: {summary.get('pass_count', 0)} 家")
        lines.append(f"- **淘汰**: {summary.get('fail_count', 0)} 家")
        lines.append(f"- **平均分数**: {summary.get('average_score', 0):.1f}")
        lines.append(f"- **精选数量** (A/B级): {summary.get('top_picks_count', 0)}")
        lines.append("")

        # 策略命中统计
        strategy_dist = summary.get("strategy_distribution", {})
        if strategy_dist:
            lines.append("### 策略命中分布")
            lines.append("")
            lines.append("| 策略 | 命中数 |")
            lines.append("|------|--------|")
            for s, count in sorted(strategy_dist.items(), key=lambda x: -x[1]):
                name = STRATEGY_NAMES.get(s, s)
                lines.append(f"| {name} | {count} |")
            lines.append("")

    # ============================================================
    # 精选股票 (A 和 B 评级)
    # ============================================================

    top_picks = [e for e in evaluations if e.get("grade") in ("A", "B") and e.get("passes")]
    if top_picks:
        lines.append("## ⭐ 精选股票 (A/B 评级)")
        lines.append("")
        lines.append("> 以下股票通过全部规则评估，综合评分优异")
        lines.append("")

        # 按评分排序
        top_picks_sorted = sorted(top_picks, key=lambda x: -x.get("composite_score", 0))
        for evaluation in top_picks_sorted:
            lines.extend(_generate_comprehensive_section(evaluation))

    # ============================================================
    # 高成长股票 (GARP)
    # ============================================================

    garp_picks = [
        e for e in evaluations
        if "high_growth" in e.get("matched_strategies", []) and e.get("passes")
    ]
    if garp_picks:
        lines.append("## 🚀 高成长优质公司 (GARP)")
        lines.append("")
        lines.append("> 高增长 + 高质量 + 合理估值")
        lines.append("")
        for evaluation in sorted(garp_picks, key=lambda x: -x.get("composite_score", 0))[:10]:
            lines.extend(_generate_comprehensive_section(evaluation))

    # ============================================================
    # 困境反转股票
    # ============================================================

    turnaround_picks = [
        e for e in evaluations
        if "turnaround" in e.get("matched_strategies", []) and e.get("passes")
    ]
    if turnaround_picks:
        lines.append("## 🔄 困境反转候选")
        lines.append("")
        lines.append("> 触底信号 + 质量改善 + 规模保障")
        lines.append("")
        for evaluation in sorted(turnaround_picks, key=lambda x: -x.get("composite_score", 0))[:10]:
            lines.extend(_generate_comprehensive_section(evaluation))

    # ============================================================
    # 淘汰股票
    # ============================================================

    eliminated = [e for e in evaluations if not e.get("passes")]
    if eliminated:
        lines.append("## ❌ 淘汰股票")
        lines.append("")
        lines.append(f"> 共 {len(eliminated)} 家公司未通过规则评估")
        lines.append("")
        lines.append("| 代码 | 评级 | 得分 | 淘汰原因 |")
        lines.append("|------|------|------|----------|")
        for e in eliminated[:30]:  # 最多显示30家
            ts_code = e.get("ts_code", "")
            grade = e.get("grade", "F")
            score = e.get("composite_score", 0)
            reasons = e.get("elimination_reasons", [])
            reason_str = reasons[0] if reasons else "-"
            lines.append(f"| {ts_code} | {grade} | {score:.1f} | {reason_str} |")
        if len(eliminated) > 30:
            lines.append(f"| ... | | | 还有 {len(eliminated) - 30} 家 |")
        lines.append("")

    # ============================================================
    # 方法论说明
    # ============================================================

    lines.append("## 📖 方法论说明")
    lines.append("")
    lines.append("### 评估规则体系")
    lines.append("")
    lines.append("```")
    lines.append("第1层: 否决规则 (Veto Rules)")
    lines.append("  ├─ 连续亏损 → 直接淘汰")
    lines.append("  ├─ 严重恶化 (恶化概率>80%) → 直接淘汰")
    lines.append("  └─ 断崖式下跌 (>60%) → 直接淘汰")
    lines.append("")
    lines.append("第2层: 扣分规则 (Penalty Rules)")
    lines.append("  ├─ 趋势恶化 → 扣分")
    lines.append("  ├─ 波动过大 → 扣分")
    lines.append("  └─ 交叉验证失败 → 扣分")
    lines.append("")
    lines.append("第3层: 加分规则 (Bonus Rules)")
    lines.append("  ├─ 高位稳定 → 加分")
    lines.append("  ├─ 持续改善 → 加分")
    lines.append("  └─ 趋势显著 (MK检验) → 加分")
    lines.append("```")
    lines.append("")

    lines.append("### 投资策略")
    lines.append("")
    lines.append("| 策略 | 特征 | 适合投资者 |")
    lines.append("|------|------|------------|")
    lines.append("| 🚀 高成长 | 高增长+高质量+低波动 | 进攻型 |")
    lines.append("| 🔄 困境反转 | 触底+改善信号 | 逆向投资 |")
    lines.append("| 💰 稳定分红 | 高位稳定+低波动 | 防守型 |")
    lines.append("| 📉 周期底部 | 周期股底部信号 | 择时型 |")
    lines.append("| 🏰 护城河 | 高壁垒+稳定趋势 | 长期投资 |")
    lines.append("")

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 AStock Evaluators v1.0 系统自动生成*")
    lines.append("")
    lines.append("**免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，决策需谨慎。")
    lines.append("")
    lines.append("> 如需 T.R.U.T.H. 数据驱动报告（六维基因+动态阈值），请使用 `report_truth` 方法。")

    content = "\n".join(lines)

    # 写入文件
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(content, encoding="utf-8")
        logger.info(f"✅ 综合报告已生成: {output_path}")
    except Exception as e:
        logger.warning(f"写入报告失败: {e}")

    return content
FACTOR_NAMES = {
    "alpha": "α 周期性",
    "ALPHA": "α 周期性",
    "beta": "β 资本密度",
    "BETA": "β 资本密度",
    "gamma": "γ 成长动能",
    "GAMMA": "γ 成长动能",
    "delta_fraud": "δ_fraud 欺诈熵",
    "DELTA_FRAUD": "δ_fraud 欺诈熵",
    "delta_decay": "δ_decay 衰退熵",
    "DELTA_DECAY": "δ_decay 衰退熵",
    "verification": "V 验证因子",
    "VERIFICATION": "V 验证因子",
}

# 求解器名称映射
SOLVER_NAMES = {
    "gravity": "重力场 (ROIC阈值)",
    "GRAVITY": "重力场 (ROIC阈值)",
    "velocity": "速度场 (增长边界)",
    "VELOCITY": "速度场 (增长边界)",
    "structure": "结构场 (护城河)",
    "STRUCTURE": "结构场 (护城河)",
}

# 信号 emoji 映射
SIGNAL_EMOJI = {
    "strong_buy": "🟢🟢",
    "STRONG_BUY": "🟢🟢",
    "buy": "🟢",
    "BUY": "🟢",
    "hold": "🟡",
    "HOLD": "🟡",
    "sell": "🔴",
    "SELL": "🔴",
    "strong_sell": "🔴🔴",
    "STRONG_SELL": "🔴🔴",
    "meltdown": "🚨",
    "MELTDOWN": "🚨",
}

# 评级 emoji 映射
GRADE_EMOJI = {
    "A+": "⭐⭐⭐",
    "A": "⭐⭐",
    "B+": "⭐",
    "B": "✅",
    "C": "➖",
    "D": "⚠️",
    "F": "❌",
}


def _format_factor_score(score: float) -> str:
    """格式化因子分数"""
    if score >= 0.8:
        return f"**{score:.2f}** 🔥"
    elif score >= 0.6:
        return f"{score:.2f} ✓"
    elif score <= 0.3:
        return f"{score:.2f} ⚠"
    return f"{score:.2f}"


def _format_threshold(threshold: Dict[str, Any]) -> str:
    """格式化动态阈值"""
    value = threshold.get("value", 0)
    lower = threshold.get("lower", value)
    upper = threshold.get("upper", value)
    unit = threshold.get("unit", "")

    if unit in ("percent", "percent_annual"):
        return f"{value:.1f}% ({lower:.1f}%-{upper:.1f}%)"
    elif unit == "score_0_100":
        return f"{value:.0f}/100 ({lower:.0f}-{upper:.0f})"
    elif unit == "years":
        return f"{value:.1f}年 ({lower:.1f}-{upper:.1f}年)"
    return f"{value:.2f}"


def _generate_profile_section(profile: Dict[str, Any]) -> List[str]:
    """生成单支股票的报告章节"""
    lines = []
    ts_code = profile.get("ts_code", "未知")

    # 标题
    signal = profile.get("signal", "")
    grade = profile.get("grade", "")
    emoji = SIGNAL_EMOJI.get(signal, "")
    grade_emoji = GRADE_EMOJI.get(grade, "")

    lines.append(f"### {ts_code} {emoji} {grade_emoji}")
    lines.append("")

    # 综合评分
    final_score = profile.get("final_score")
    confidence = profile.get("confidence")
    if final_score is not None:
        lines.append(f"**综合评分**: {final_score:.2%} | **评级**: {grade} | **信号**: {signal} | **置信度**: {confidence:.0%}")
        lines.append("")

    # 六维因子表
    factors = profile.get("factors", {})
    if factors:
        lines.append("#### 六维基因图谱")
        lines.append("")
        lines.append("| 因子 | 分数 | 置信度 | 关键组件 |")
        lines.append("|------|------|--------|----------|")

        for fid, fdata in factors.items():
            name = FACTOR_NAMES.get(fid, fid)
            if isinstance(fdata, dict):
                score = fdata.get("score", 0)
                conf = fdata.get("confidence", 0)
                components = fdata.get("components", {})
                comp_str = ", ".join(f"{k}={v:.2f}" for k, v in list(components.items())[:3])
            else:
                score = fdata
                conf = 1.0
                comp_str = "-"

            lines.append(f"| {name} | {_format_factor_score(score)} | {conf:.0%} | {comp_str} |")
        lines.append("")

    # 三大求解器表
    solvers = profile.get("solvers", {})
    if solvers:
        lines.append("#### 物理求解器")
        lines.append("")
        lines.append("| 求解器 | 评分 | 动态阈值 |")
        lines.append("|--------|------|----------|")

        for sid, sdata in solvers.items():
            name = SOLVER_NAMES.get(sid, sid)
            if isinstance(sdata, dict):
                score = sdata.get("score", 0)
                thresholds = sdata.get("thresholds", {})
                if thresholds:
                    th_str = " | ".join(
                        f"{k}: {_format_threshold(v)}"
                        for k, v in thresholds.items()
                    )
                else:
                    th_str = "-"
            else:
                score = sdata
                th_str = "-"

            lines.append(f"| {name} | {_format_factor_score(score)} | {th_str} |")
        lines.append("")

    # 动态阈值汇总
    dyn_thresholds = profile.get("dynamic_thresholds", {})
    if dyn_thresholds:
        lines.append("#### 投资决策阈值")
        lines.append("")
        for name, th in dyn_thresholds.items():
            desc = th.get("description", "")
            value_str = _format_threshold(th)
            lines.append(f"- **{name}**: {value_str}")
            if desc:
                lines.append(f"  - {desc}")
        lines.append("")

    # 警告
    warnings = profile.get("warnings", [])
    if warnings:
        lines.append("#### ⚠️ 风险提示")
        lines.append("")
        for w in warnings:
            level = w.get("level", "INFO")
            title = w.get("title", "")
            message = w.get("message", "")
            level_icon = "🚨" if level in ("FATAL", "CRITICAL") else "⚠️" if level == "WARNING" else "ℹ️"
            lines.append(f"- {level_icon} **{title}**: {message}")
        lines.append("")

    lines.append("---")
    lines.append("")

    return lines


@register_method(
    engine_name="report_truth",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. report from run_truth results (v3.0: six factors + three solvers + dynamic thresholds)",
)
def report_truth(
    truth_result: Dict[str, Any],
    output_path: str = "data/truth_analysis_report.md",
) -> str:
    """基于 T.R.U.T.H. v3.0 结果生成专业 Markdown 报告.

    输入结构 (来自 run_truth):
        {
            "metadata": {
                "algo_version": "3.0.0",
                "universe_size": N,
                "factor_count": 6,
                "solver_count": 3,
            },
            "profiles": [
                {
                    "ts_code": "...",
                    "factors": {"alpha": {"score": 0.x, ...}, ...},
                    "solvers": {"gravity": {"score": 0.x, "thresholds": {...}}, ...},
                    "final_score": 0.x,
                    "signal": "buy/sell/hold/...",
                    "grade": "A/B/C/...",
                    "warnings": [...],
                    "dynamic_thresholds": {...},
                },
                ...
            ],
            "summary": {...},
        }
    """

    if not truth_result:
        raise ValueError("truth_result 不能为空, 请先运行 run_truth")

    metadata = truth_result.get("metadata", {}) or {}
    profiles = truth_result.get("profiles", []) or []
    summary = truth_result.get("summary", {}) or {}

    lines: List[str] = []

    # ============================================================
    # 报告头部
    # ============================================================

    lines.append("# 🧬 T.R.U.T.H. 基因分析报告")
    lines.append("")
    lines.append("> **T**ransparent **R**isk-adjusted **U**nified **T**hreshold **H**euristic")
    lines.append(">")
    lines.append("> 六维基因测序 × 三大物理求解器 × 动态阈值决策系统")
    lines.append("")

    # ============================================================
    # 元数据
    # ============================================================

    lines.append("## 📋 报告概要")
    lines.append("")
    lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **算法版本**: {metadata.get('algo_version', '3.0.0')}")
    lines.append(f"- **配置版本**: {metadata.get('config_version', '3.0.0')}")
    lines.append(f"- **分析股票数**: {metadata.get('universe_size', len(profiles))}")
    lines.append(f"- **因子数量**: {metadata.get('factor_count', 6)} (α/β/γ/δ_fraud/δ_decay/V)")
    lines.append(f"- **求解器数量**: {metadata.get('solver_count', 3)} (Gravity/Velocity/Structure)")
    lines.append("")

    # ============================================================
    # 汇总统计
    # ============================================================

    if summary:
        lines.append("## 📊 汇总统计")
        lines.append("")

        # 信号分布
        signal_dist = summary.get("signal_distribution", {})
        if signal_dist:
            lines.append("### 信号分布")
            lines.append("")
            lines.append("| 信号 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            total = sum(signal_dist.values())
            for sig, count in sorted(signal_dist.items(), key=lambda x: -x[1]):
                emoji = SIGNAL_EMOJI.get(sig, "")
                pct = count / total * 100 if total > 0 else 0
                lines.append(f"| {emoji} {sig} | {count} | {pct:.1f}% |")
            lines.append("")

        # 评级分布
        grade_dist = summary.get("grade_distribution", {})
        if grade_dist:
            lines.append("### 评级分布")
            lines.append("")
            lines.append("| 评级 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            total = sum(grade_dist.values())
            for grade, count in sorted(grade_dist.items()):
                emoji = GRADE_EMOJI.get(grade, "")
                pct = count / total * 100 if total > 0 else 0
                lines.append(f"| {emoji} {grade} | {count} | {pct:.1f}% |")
            lines.append("")

        # 关键数字
        lines.append("### 关键指标")
        lines.append("")
        lines.append(f"- **平均分数**: {summary.get('average_score', 0):.2%}")
        lines.append(f"- **精选数量** (A+/A): {summary.get('top_picks_count', 0)}")
        lines.append(f"- **熔断警报**: {summary.get('meltdown_count', 0)}")
        lines.append("")

    # ============================================================
    # 精选股票 (A+ 和 A 评级)
    # ============================================================

    top_picks = [p for p in profiles if p.get("grade") in ("A+", "A")]
    if top_picks:
        lines.append("## ⭐ 精选股票 (A+/A 评级)")
        lines.append("")
        for profile in top_picks:
            lines.extend(_generate_profile_section(profile))

    # ============================================================
    # 熔断警报
    # ============================================================

    meltdowns = [p for p in profiles if p.get("signal") in ("meltdown", "MELTDOWN")]
    if meltdowns:
        lines.append("## 🚨 熔断警报")
        lines.append("")
        lines.append("> 以下股票触发欺诈熵熔断，建议回避")
        lines.append("")
        for profile in meltdowns:
            lines.extend(_generate_profile_section(profile))

    # ============================================================
    # 所有股票详情
    # ============================================================

    lines.append("## 📈 完整分析")
    lines.append("")

    # 按评分排序
    sorted_profiles = sorted(
        profiles,
        key=lambda p: p.get("final_score", 0) or 0,
        reverse=True
    )

    for profile in sorted_profiles:
        # 跳过已在精选或熔断中展示的
        if profile in top_picks or profile in meltdowns:
            continue
        lines.extend(_generate_profile_section(profile))

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 T.R.U.T.H. v3.0 系统自动生成*")
    lines.append("")
    lines.append("**免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，决策需谨慎。")

    content = "\n".join(lines)

    # 写入文件
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"TRUTH 报告已生成: {output_path}")
    except Exception as e:
        logger.warning(f"写入 TRUTH 报告失败: {e}")

    return content

