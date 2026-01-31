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
    """
    生成单支股票的报告章节

    适配 v2 Evaluator 输出格式:
    - decision: QUALITY/VETO/HOLD/UNCERTAIN
    - score: 0-100
    - confidence: 0-1
    - company_state: GROWTH/MATURE/DECLINE/TURNAROUND/DISTRESS
    - factors: [{name, value, contribution, direction}]
    """
    lines = []
    ts_code = evaluation.get("ts_code", "未知")
    name = evaluation.get("name") or ""  # 避免显示 None
    industry = evaluation.get("industry") or ""

    # v2 字段
    decision = evaluation.get("decision", "uncertain")
    score = evaluation.get("score", 0)
    confidence = evaluation.get("confidence", 0)
    company_state = evaluation.get("company_state") or ""
    factors = evaluation.get("factors", [])

    # 决策映射
    decision_emoji = {
        "quality": "✅ 优质",
        "veto": "❌ 否决",
        "average": "🟡 一般",
        "poor": "🟠 较差",
        "uncertain": "❓ 待定"
    }

    # 状态映射
    state_emoji = {
        "emerging": "🚀 成长期",
        "mature": "🏔️ 成熟期",
        "declining": "📉 衰退期",
        "turnaround": "🔄 反转期",
        "distressed": "⚠️ 困境期",
        "cash_cow": "💰 现金牛"
    }

    # 标题
    decision_str = decision_emoji.get(decision, decision)
    title_parts = [f"### {ts_code}"]
    if name:
        title_parts.append(name)
    title_parts.append(decision_str)
    lines.append(" ".join(title_parts))
    if industry:
        lines.append(f"*行业: {industry}*")
    lines.append("")

    # 核心指标
    lines.append(f"- **综合评分**: {score:.1f}/100")
    lines.append(f"- **置信度**: {confidence:.1%}")
    if company_state:
        state_str = state_emoji.get(company_state, company_state)
        lines.append(f"- **生命周期**: {state_str}")
    lines.append("")

    # 因素分析
    if factors:
        lines.append("#### 关键因素")
        lines.append("")
        lines.append("| 因素 | 数值 | 贡献 | 方向 |")
        lines.append("|------|------|------|------|")
        for f in factors[:6]:  # 最多显示6个
            f_name = f.get("name", "")
            f_value = f.get("value", 0)
            f_contrib = f.get("contribution", 0)
            f_dir = f.get("direction", "")
            dir_emoji = "↑" if f_dir == "positive" else "↓" if f_dir == "negative" else "→"
            lines.append(f"| {f_name} | {f_value:.3f} | {f_contrib:+.1f} | {dir_emoji} |")
        lines.append("")

    lines.append("---")
    lines.append("")

    return lines


@register_method(
    engine_name="report_comprehensive",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate comprehensive analysis report from evaluator results (v2 Causal Bayesian)",
)
def report_comprehensive(
    evaluator_result: Dict[str, Any],
    output_path: str = "data/comprehensive_analysis_report.md",
) -> str:
    """
    生成综合趋势分析报告（v2 因果贝叶斯评估器）

    数据流:
        trend/engine (8个探针)
            ↓ aggregated_trends (PDDA)
        evaluators/engine (run_causal_bayesian_evaluator)
            ↓ evaluator_result
        reporters/engine (本方法)
            ↓
        Markdown 报告

    Args:
        evaluator_result: evaluators/engine.run_causal_bayesian_evaluator 的输出结果
            {
                "evaluations": [{ts_code, decision, score, confidence, ...}],
                "summary": {total_evaluated, quality_count, veto_count, ...},
                "quality_companies": [...],
                "veto_companies": [...]
            }
        output_path: 输出报告路径

    Returns:
        报告内容字符串
    """
    if not evaluator_result:
        raise ValueError("evaluator_result 不能为空，请先运行 run_causal_bayesian_evaluator")

    evaluations = evaluator_result.get("evaluations", [])
    summary = evaluator_result.get("summary", {})
    quality_companies = evaluator_result.get("quality_companies", [])
    veto_companies = evaluator_result.get("veto_companies", [])

    lines: List[str] = []

    # ============================================================
    # 报告头部
    # ============================================================

    lines.append("# 📊 AStock 综合基本面分析报告")
    lines.append("")
    lines.append("> 因果贝叶斯网络 × 状态机推断 × Dempster-Shafer证据融合")
    lines.append("")

    # ============================================================
    # 元数据
    # ============================================================

    lines.append("## 📋 报告概要")
    lines.append("")
    lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **算法版本**: v2.0 (Causal Bayesian)")
    lines.append(f"- **分析股票数**: {summary.get('total_evaluated', len(evaluations))}")
    lines.append(f"- **优质公司数**: {summary.get('quality_count', len(quality_companies))}")
    lines.append(f"- **否决公司数**: {summary.get('veto_count', len(veto_companies))}")
    lines.append("")

    # ============================================================
    # 汇总统计
    # ============================================================

    lines.append("## 📊 汇总统计")
    lines.append("")

    # 决策分布
    decision_dist = {}
    for e in evaluations:
        d = e.get("decision", "uncertain")
        decision_dist[d] = decision_dist.get(d, 0) + 1

    if decision_dist:
        lines.append("### 决策分布")
        lines.append("")
        lines.append("| 决策 | 数量 | 占比 |")
        lines.append("|------|------|------|")
        total = len(evaluations)
        decision_labels = {
            "quality": "✅ 优质",
            "veto": "❌ 否决",
            "average": "🟡 一般",
            "poor": "🟠 较差",
            "uncertain": "❓ 待定"
        }
        for d in ["quality", "average", "poor", "uncertain", "veto"]:
            count = decision_dist.get(d, 0)
            pct = count / total * 100 if total > 0 else 0
            label = decision_labels.get(d, d)
            lines.append(f"| {label} | {count} | {pct:.1f}% |")
        lines.append("")

    # 状态分布
    state_dist = {}
    for e in evaluations:
        s = e.get("company_state")
        if s:
            state_dist[s] = state_dist.get(s, 0) + 1

    if state_dist:
        lines.append("### 生命周期分布")
        lines.append("")
        lines.append("| 状态 | 数量 |")
        lines.append("|------|------|")
        state_labels = {
            "emerging": "🚀 成长期",
            "mature": "🏔️ 成熟期",
            "declining": "📉 衰退期",
            "turnaround": "🔄 反转期",
            "distressed": "⚠️ 困境期",
            "cash_cow": "💰 现金牛"
        }
        for s, count in sorted(state_dist.items(), key=lambda x: -x[1]):
            label = state_labels.get(s, s)
            lines.append(f"| {label} | {count} |")
        lines.append("")

    # ============================================================
    # 优质股票完整列表（表格形式）
    # ============================================================

    quality_evals = [e for e in evaluations if e.get("decision") == "quality"]
    if quality_evals:
        lines.append("## ⭐ 优质公司完整列表 (QUALITY)")
        lines.append("")
        lines.append(f"> 共 {len(quality_evals)} 家公司通过因果贝叶斯评估（得分≥70）")
        lines.append("")

        # 表格形式显示全部
        lines.append("| 代码 | 得分 | 置信度 | 生命周期 | 主要因素 |")
        lines.append("|------|------|--------|----------|----------|")
        state_labels = {
            "emerging": "🚀成长", "mature": "🏔️成熟", "declining": "📉衰退",
            "turnaround": "🔄反转", "distressed": "⚠️困境", "cash_cow": "💰现金牛"
        }
        for e in sorted(quality_evals, key=lambda x: -x.get("score", 0)):
            ts_code = e.get("ts_code", "")
            score = e.get("score", 0)
            conf = e.get("confidence", 0)
            state = e.get("company_state", "")
            state_str = state_labels.get(state, state)
            # 主要贡献因素
            factors = e.get("factors", [])
            top_factors = sorted(factors, key=lambda f: abs(f.get("contribution", 0)), reverse=True)[:2]
            factor_str = ", ".join([f"{f.get('name', '')}:{f.get('value', 0):.2f}" for f in top_factors])
            lines.append(f"| {ts_code} | {score:.1f} | {conf:.0%} | {state_str} | {factor_str} |")
        lines.append("")

        # 详细展示 Top 10
        lines.append("### 🏆 Top 10 详细分析")
        lines.append("")
        quality_sorted = sorted(quality_evals, key=lambda x: -x.get("score", 0))
        for evaluation in quality_sorted[:10]:
            lines.extend(_generate_comprehensive_section(evaluation))

    # ============================================================
    # 一般公司（表格）
    # ============================================================

    average_evals = [e for e in evaluations if e.get("decision") == "average"]
    if average_evals:
        lines.append("## 🟡 一般公司 (AVERAGE)")
        lines.append("")
        lines.append(f"> 共 {len(average_evals)} 家（得分 50-70）")
        lines.append("")
        lines.append("| 代码 | 得分 | 生命周期 | 代码 | 得分 | 生命周期 |")
        lines.append("|------|------|----------|------|------|----------|")
        sorted_avg = sorted(average_evals, key=lambda x: -x.get("score", 0))
        state_labels = {
            "emerging": "🚀", "mature": "🏔️", "declining": "📉",
            "turnaround": "🔄", "distressed": "⚠️", "cash_cow": "💰"
        }
        # 两列显示
        for i in range(0, len(sorted_avg), 2):
            e1 = sorted_avg[i]
            row = f"| {e1.get('ts_code', '')} | {e1.get('score', 0):.1f} | {state_labels.get(e1.get('company_state', ''), '')} "
            if i + 1 < len(sorted_avg):
                e2 = sorted_avg[i + 1]
                row += f"| {e2.get('ts_code', '')} | {e2.get('score', 0):.1f} | {state_labels.get(e2.get('company_state', ''), '')} |"
            else:
                row += "| | | |"
            lines.append(row)
        lines.append("")

    # ============================================================
    # 按生命周期分组的非否决公司
    # ============================================================

    lines.append("## 📊 按生命周期分组（非否决）")
    lines.append("")

    non_veto = [e for e in evaluations if e.get("decision") != "veto"]
    state_groups = {}
    for e in non_veto:
        state = e.get("company_state", "unknown")
        if state not in state_groups:
            state_groups[state] = []
        state_groups[state].append(e)

    state_order = ["cash_cow", "mature", "emerging", "turnaround", "declining", "distressed"]
    state_labels_full = {
        "emerging": "🚀 成长期", "mature": "🏔️ 成熟期", "declining": "📉 衰退期",
        "turnaround": "🔄 反转期", "distressed": "⚠️ 困境期", "cash_cow": "💰 现金牛"
    }
    decision_emoji = {"quality": "⭐", "average": "🟡", "poor": "🟠"}

    for state in state_order:
        if state in state_groups:
            group = state_groups[state]
            label = state_labels_full.get(state, state)
            lines.append(f"### {label} ({len(group)} 家)")
            lines.append("")
            lines.append("| 代码 | 决策 | 得分 | 置信度 |")
            lines.append("|------|------|------|--------|")
            for e in sorted(group, key=lambda x: -x.get("score", 0))[:20]:
                dec = e.get("decision", "")
                dec_str = decision_emoji.get(dec, "") + dec
                lines.append(f"| {e.get('ts_code', '')} | {dec_str} | {e.get('score', 0):.1f} | {e.get('confidence', 0):.0%} |")
            if len(group) > 20:
                lines.append(f"| ... | | | 还有 {len(group) - 20} 家 |")
            lines.append("")

    # ============================================================
    # 否决股票（简化）
    # ============================================================

    veto_evals = [e for e in evaluations if e.get("decision") == "veto"]
    if veto_evals:
        lines.append("## ❌ 否决公司 (VETO)")
        lines.append("")
        lines.append(f"> 共 {len(veto_evals)} 家公司被否决（得分<30 或 DS判定reject）")
        lines.append("")
        lines.append("| 代码 | 得分 | 置信度 | 状态 |")
        lines.append("|------|------|--------|------|")
        for e in sorted(veto_evals, key=lambda x: x.get("score", 0))[:30]:
            ts_code = e.get("ts_code", "")
            score = e.get("score", 0)
            confidence = e.get("confidence", 0)
            state = e.get("company_state", "-")
            lines.append(f"| {ts_code} | {score:.1f} | {confidence:.1%} | {state} |")
        if len(veto_evals) > 30:
            lines.append(f"| ... | | | 还有 {len(veto_evals) - 30} 家 |")
        lines.append("")

    # ============================================================
    # 方法论说明
    # ============================================================

    lines.append("## 📖 方法论说明")
    lines.append("")
    lines.append("### 因果贝叶斯评估架构")
    lines.append("")
    lines.append("```")
    lines.append("PDDA 聚合趋势数据 (单行/公司/指标)")
    lines.append("        ↓")
    lines.append("┌─────────────────────────────────────┐")
    lines.append("│     CausalBayesianEvaluator         │")
    lines.append("├─────────────────────────────────────┤")
    lines.append("│  1. 特征提取 (PDDA 40+列)           │")
    lines.append("│  2. 自适应阈值 (行业/规模)          │")
    lines.append("│  3. 因果推断 (Pearl do-calculus)    │")
    lines.append("│  4. 状态机推断 (HMM 5状态)          │")
    lines.append("│  5. Copula 证据融合                 │")
    lines.append("│  6. Dempster-Shafer 不确定性合并    │")
    lines.append("└─────────────────────────────────────┘")
    lines.append("        ↓")
    lines.append("   决策: QUALITY / HOLD / VETO")
    lines.append("```")
    lines.append("")

    lines.append("### 公司生命周期状态")
    lines.append("")
    lines.append("| 状态 | 特征 | 投资含义 |")
    lines.append("|------|------|----------|")
    lines.append("| 🚀 GROWTH | 高增长+扩张 | 成长股机会 |")
    lines.append("| 🏔️ MATURE | 稳定+分红 | 价值股/蓝筹 |")
    lines.append("| 📉 DECLINE | 收缩+下降 | 规避风险 |")
    lines.append("| 🔄 TURNAROUND | 触底+改善 | 逆向投资 |")
    lines.append("| ⚠️ DISTRESS | 困境+风险 | 高度警惕 |")
    lines.append("")

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 AStock Evaluators v2.0 (Causal Bayesian) 系统自动生成*")
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

