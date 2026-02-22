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
        "growth": "📈 高增长",
        "mature": "🏔️ 成熟期",
        "declining": "📉 衰退期",
        "slowing": "📊 放缓期",
        "turnaround": "🔄 反转期",
        "distressed": "⚠️ 困境期",
        "cash_cow": "💰 现金牛",
        "cyclical_peak": "🔝 周期顶部",
        "cyclical_trough": "🔻 周期底部",
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
            "growth": "📈 高增长",
            "mature": "🏔️ 成熟期",
            "declining": "📉 衰退期",
            "slowing": "📊 放缓期",
            "turnaround": "🔄 反转期",
            "distressed": "⚠️ 困境期",
            "cash_cow": "💰 现金牛",
            "cyclical_peak": "🔝 周期顶部",
            "cyclical_trough": "🔻 周期底部",
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
            "emerging": "🚀成长", "growth": "📈高增长", "mature": "🏔️成熟", "declining": "📉衰退",
            "slowing": "📊放缓", "turnaround": "🔄反转", "distressed": "⚠️困境", "cash_cow": "💰现金牛",
            "cyclical_peak": "🔝周期顶", "cyclical_trough": "🔻周期底",
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
            "emerging": "🚀", "growth": "📈", "mature": "🏔️", "declining": "📉",
            "slowing": "📊", "turnaround": "🔄", "distressed": "⚠️", "cash_cow": "💰",
            "cyclical_peak": "🔝", "cyclical_trough": "🔻",
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

    state_order = ["cash_cow", "mature", "emerging", "growth", "turnaround", "slowing", "declining", "distressed", "cyclical_peak", "cyclical_trough"]
    state_labels_full = {
        "emerging": "🚀 成长期", "growth": "📈 高增长", "mature": "🏔️ 成熟期",
        "declining": "📉 衰退期", "slowing": "📊 放缓期",
        "turnaround": "🔄 反转期", "distressed": "⚠️ 困境期", "cash_cow": "💰 现金牛",
        "cyclical_peak": "🔝 周期顶部", "cyclical_trough": "🔻 周期底部",
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


def _generate_compact_row(profile: Dict[str, Any]) -> str:
    """生成紧凑的表格行（用于汇总列表）"""
    ts_code = profile.get("ts_code", "")
    final_score = profile.get("final_score", 0) or 0
    grade = profile.get("grade", "-")
    signal = profile.get("signal", "-")
    confidence = profile.get("confidence", 0) or 0

    # 提取关键因子分数
    factors = profile.get("factors", {})
    gamma_score = 0
    alpha_score = 0
    if isinstance(factors, dict):
        gamma_data = factors.get("gamma") or factors.get("GAMMA", {})
        alpha_data = factors.get("alpha") or factors.get("ALPHA", {})
        gamma_score = gamma_data.get("score", 0) if isinstance(gamma_data, dict) else gamma_data or 0
        alpha_score = alpha_data.get("score", 0) if isinstance(alpha_data, dict) else alpha_data or 0

    # 提取关键求解器分数
    solvers = profile.get("solvers", {})
    gravity_score = 0
    if isinstance(solvers, dict):
        gravity_data = solvers.get("gravity") or solvers.get("GRAVITY", {})
        gravity_score = gravity_data.get("score", 0) if isinstance(gravity_data, dict) else gravity_data or 0

    grade_emoji = GRADE_EMOJI.get(grade, "")
    signal_emoji = SIGNAL_EMOJI.get(signal, "")

    return f"| {ts_code} | {final_score:.1%} | {grade_emoji}{grade} | {signal_emoji}{signal} | {confidence:.0%} | γ:{gamma_score:.2f} α:{alpha_score:.2f} | G:{gravity_score:.2f} |"


def _get_top_warning(profile: Dict[str, Any]) -> str:
    """获取最重要的警告信息"""
    warnings = profile.get("warnings", [])
    if not warnings:
        return "-"
    # 取第一个警告的标题
    w = warnings[0]
    title = w.get("title", "") if isinstance(w, dict) else str(w)
    return title[:20] + "..." if len(title) > 20 else title


@register_method(
    engine_name="report_truth",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. report from run_truth results (v3.1: layered display like evaluators)",
)
def report_truth(
    truth_result: Dict[str, Any],
    output_path: str = "data/truth_analysis_report.md",
) -> str:
    """基于 T.R.U.T.H. v3.0 结果生成专业 Markdown 报告（v3.1 分层展示）.

    v3.1 改进（参照 evaluators 报告格式）：
    - 分层展示：汇总 → 完整列表(表格) → Top10详细 → 按评级分组
    - 只对精选(A+/A)和Top10展开详细六维基因
    - 其余股票用紧凑表格展示
    - 大幅减少报告行数（从7万行降到约2000行）

    输入结构 (来自 run_truth):
        {
            "metadata": {...},
            "profiles": [{ts_code, factors, solvers, final_score, signal, grade, ...}],
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
        lines.append(f"- **精选数量** (A+/A/B+): {summary.get('top_picks_count', 0)}")
        lines.append(f"- **熔断警报**: {summary.get('meltdown_count', 0)}")
        lines.append("")

    # ============================================================
    # 按评分排序所有股票
    # ============================================================

    sorted_profiles = sorted(
        profiles,
        key=lambda p: p.get("final_score", 0) or 0,
        reverse=True
    )

    # 分类
    top_picks = [p for p in sorted_profiles if p.get("grade") in ("A+", "A", "B+")]
    good_stocks = [p for p in sorted_profiles if p.get("grade") in ("B",)]
    average_stocks = [p for p in sorted_profiles if p.get("grade") in ("C",)]
    poor_stocks = [p for p in sorted_profiles if p.get("grade") in ("D",)]
    reject_stocks = [p for p in sorted_profiles if p.get("grade") in ("F",)]
    meltdowns = [p for p in sorted_profiles if p.get("signal") in ("meltdown", "MELTDOWN")]

    # ============================================================
    # 完整股票列表（表格形式，按评分排序）
    # ============================================================

    lines.append("## ⭐ 完整股票排名")
    lines.append("")
    lines.append(f"> 共 {len(sorted_profiles)} 家公司，按综合评分排序")
    lines.append("")
    lines.append("| 代码 | 评分 | 评级 | 信号 | 置信度 | 关键因子 | 求解器 |")
    lines.append("|------|------|------|------|--------|----------|--------|")

    # 显示前100名
    for profile in sorted_profiles[:100]:
        lines.append(_generate_compact_row(profile))

    if len(sorted_profiles) > 100:
        lines.append(f"| ... | | | | | | 还有 {len(sorted_profiles) - 100} 家 |")
    lines.append("")

    # ============================================================
    # 精选股票详细分析 (A+/A/B+ 评级)
    # ============================================================

    if top_picks:
        lines.append("## 🏆 精选股票详细分析 (A+/A/B+ 评级)")
        lines.append("")
        lines.append(f"> 共 {len(top_picks)} 家公司达到精选标准")
        lines.append("")

        # 先显示完整列表
        lines.append("### 精选列表")
        lines.append("")
        lines.append("| 代码 | 评分 | 评级 | 信号 | 置信度 | 关键因子 | 求解器 |")
        lines.append("|------|------|------|------|--------|----------|--------|")
        for profile in top_picks:
            lines.append(_generate_compact_row(profile))
        lines.append("")

        # 仅展开 Top 10 详细
        lines.append("### 🔍 Top 10 详细基因分析")
        lines.append("")
        for profile in top_picks[:10]:
            lines.extend(_generate_profile_section(profile))
    else:
        lines.append("## 🏆 精选股票 (A+/A/B+ 评级)")
        lines.append("")
        lines.append("> ⚠️ 本次分析暂无达到精选标准的股票")
        lines.append("")

    # ============================================================
    # 良好股票 (B 评级) - 表格展示
    # ============================================================

    if good_stocks:
        lines.append("## ✅ 良好股票 (B 评级)")
        lines.append("")
        lines.append(f"> 共 {len(good_stocks)} 家")
        lines.append("")
        lines.append("| 代码 | 评分 | 信号 | 置信度 | 关键因子 |")
        lines.append("|------|------|------|--------|----------|")
        for p in good_stocks[:50]:
            ts_code = p.get("ts_code", "")
            score = p.get("final_score", 0) or 0
            signal = p.get("signal", "-")
            conf = p.get("confidence", 0) or 0
            factors = p.get("factors", {})
            gamma = factors.get("gamma", {}).get("score", 0) if isinstance(factors.get("gamma"), dict) else 0
            lines.append(f"| {ts_code} | {score:.1%} | {signal} | {conf:.0%} | γ:{gamma:.2f} |")
        if len(good_stocks) > 50:
            lines.append(f"| ... | | | | 还有 {len(good_stocks) - 50} 家 |")
        lines.append("")

    # ============================================================
    # 一般股票 (C 评级) - 紧凑表格
    # ============================================================

    if average_stocks:
        lines.append("## ➖ 一般股票 (C 评级)")
        lines.append("")
        lines.append(f"> 共 {len(average_stocks)} 家（表格展示前50家）")
        lines.append("")
        lines.append("| 代码 | 评分 | 信号 | 代码 | 评分 | 信号 |")
        lines.append("|------|------|------|------|------|------|")
        # 两列显示
        for i in range(0, min(50, len(average_stocks)), 2):
            p1 = average_stocks[i]
            row = f"| {p1.get('ts_code', '')} | {(p1.get('final_score', 0) or 0):.1%} | {p1.get('signal', '')} "
            if i + 1 < len(average_stocks):
                p2 = average_stocks[i + 1]
                row += f"| {p2.get('ts_code', '')} | {(p2.get('final_score', 0) or 0):.1%} | {p2.get('signal', '')} |"
            else:
                row += "| | | |"
            lines.append(row)
        if len(average_stocks) > 50:
            lines.append(f"| ... | | | | | 还有 {len(average_stocks) - 50} 家 |")
        lines.append("")

    # ============================================================
    # 较差股票 (D 评级) - 仅统计
    # ============================================================

    if poor_stocks:
        lines.append("## ⚠️ 较差股票 (D 评级)")
        lines.append("")
        lines.append(f"> 共 {len(poor_stocks)} 家，建议谨慎")
        lines.append("")
        lines.append("| 代码 | 评分 | 信号 | 代码 | 评分 | 信号 |")
        lines.append("|------|------|------|------|------|------|")
        # 两列显示，最多30行
        for i in range(0, min(60, len(poor_stocks)), 2):
            p1 = poor_stocks[i]
            row = f"| {p1.get('ts_code', '')} | {(p1.get('final_score', 0) or 0):.1%} | {p1.get('signal', '')} "
            if i + 1 < len(poor_stocks):
                p2 = poor_stocks[i + 1]
                row += f"| {p2.get('ts_code', '')} | {(p2.get('final_score', 0) or 0):.1%} | {p2.get('signal', '')} |"
            else:
                row += "| | | |"
            lines.append(row)
        if len(poor_stocks) > 60:
            lines.append(f"| ... | | | | | 还有 {len(poor_stocks) - 60} 家 |")
        lines.append("")

    # ============================================================
    # 否决股票 (F 评级) - 仅列出代码
    # ============================================================

    if reject_stocks:
        lines.append("## ❌ 否决股票 (F 评级)")
        lines.append("")
        lines.append(f"> 共 {len(reject_stocks)} 家，建议回避")
        lines.append("")
        # 简单列出代码，每行10个
        codes = [p.get("ts_code", "") for p in reject_stocks]
        for i in range(0, len(codes), 10):
            batch = codes[i:i+10]
            lines.append("| " + " | ".join(batch) + " |")
        lines.append("")

    # ============================================================
    # 熔断警报 - 详细展示
    # ============================================================

    if meltdowns:
        lines.append("## 🚨 熔断警报")
        lines.append("")
        lines.append(f"> {len(meltdowns)} 家股票触发欺诈熵熔断，**强烈建议回避**")
        lines.append("")
        # 展示熔断股票详情（这些需要详细分析原因）
        for profile in meltdowns[:5]:  # 最多5个详细
            lines.extend(_generate_profile_section(profile))
        if len(meltdowns) > 5:
            lines.append(f"*还有 {len(meltdowns) - 5} 家熔断股票，此处省略*")
            lines.append("")

    # ============================================================
    # 方法论说明
    # ============================================================

    lines.append("## 📖 方法论说明")
    lines.append("")
    lines.append("### T.R.U.T.H. 六维基因模型")
    lines.append("")
    lines.append("```")
    lines.append("┌─────────────────────────────────────────────────────────┐")
    lines.append("│                  六维基因图谱                            │")
    lines.append("├─────────────────────────────────────────────────────────┤")
    lines.append("│  α (Alpha)     : 周期性因子 - 业务稳定性                 │")
    lines.append("│  β (Beta)      : 资本密度因子 - 资本效率                 │")
    lines.append("│  γ (Gamma)     : 成长动能因子 - 增长潜力                 │")
    lines.append("│  δ_fraud       : 欺诈熵因子 - 财务造假风险               │")
    lines.append("│  δ_decay       : 衰退熵因子 - 业务恶化风险               │")
    lines.append("│  V (Verify)    : 验证因子 - 数据质量校验                 │")
    lines.append("└─────────────────────────────────────────────────────────┘")
    lines.append("                           ↓")
    lines.append("┌─────────────────────────────────────────────────────────┐")
    lines.append("│                  三大物理求解器                          │")
    lines.append("├─────────────────────────────────────────────────────────┤")
    lines.append("│  Gravity   : 重力场求解器 - ROIC 动态阈值               │")
    lines.append("│  Velocity  : 速度场求解器 - 增长边界估计                │")
    lines.append("│  Structure : 结构场求解器 - 护城河宽度                  │")
    lines.append("└─────────────────────────────────────────────────────────┘")
    lines.append("                           ↓")
    lines.append("              最终评分 + 评级 + 信号")
    lines.append("```")
    lines.append("")

    lines.append("### 评级标准")
    lines.append("")
    lines.append("| 评级 | 分数区间 | 投资建议 |")
    lines.append("|------|----------|----------|")
    lines.append("| ⭐⭐⭐ A+ | ≥85% | 极度优质，强烈推荐 |")
    lines.append("| ⭐⭐ A | 75-85% | 优质标的，建议关注 |")
    lines.append("| ⭐ B+ | 65-75% | 良好标的，可考虑 |")
    lines.append("| ✅ B | 55-65% | 中等偏上，观察 |")
    lines.append("| ➖ C | 45-55% | 一般，持有观望 |")
    lines.append("| ⚠️ D | 35-45% | 较差，谨慎 |")
    lines.append("| ❌ F | <35% | 否决，回避 |")
    lines.append("")

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 T.R.U.T.H. v3.1 系统自动生成*")
    lines.append("")
    lines.append("**免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，决策需谨慎。")

    content = "\n".join(lines)

    # 写入文件
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)
        logger.info(f"✅ TRUTH 报告已生成: {output_path} (约 {len(lines)} 行)")
    except Exception as e:
        logger.warning(f"写入 TRUTH 报告失败: {e}")

    return content


# ═══════════════════════════════════════════════════════════════════════════════
# 交叉验证报告 (Phase 4+5: 两系统对比 + 精选推荐 + 风险预警)
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_spearman(x: List[float], y: List[float]) -> float:
    """计算 Spearman 秩相关系数（无外部依赖）"""
    n = len(x)
    if n < 3:
        return 0.0

    def _rank(lst):
        indexed = sorted(enumerate(lst), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = avg_rank
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)
    d_sq_sum = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1.0 - 6.0 * d_sq_sum / (n * (n * n - 1))


@register_method(
    engine_name="report_cross_validation",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate cross-validation report comparing T.R.U.T.H. and Evaluator results",
)
def report_cross_validation(
    truth_result: Dict[str, Any],
    evaluator_result: Dict[str, Any],
    output_path: str = "data/cross_validation_report.md",
) -> str:
    """生成交叉验证报告 — 融合两个引擎的结论

    Phase 4: 引擎一致性验证
    Phase 5: 精选推荐 + 风险预警 + 统计验证

    Args:
        truth_result: run_truth() 的输出
        evaluator_result: run_causal_bayesian_evaluator() 的输出
        output_path: 输出路径

    Returns:
        报告 Markdown 内容
    """
    if not truth_result or not evaluator_result:
        raise ValueError("两个引擎的结果均不能为空")

    truth_profiles = truth_result.get("profiles", [])
    eval_results = evaluator_result.get("evaluations", [])

    # 构建 ts_code 索引
    truth_by_ts = {p.get("ts_code"): p for p in truth_profiles}
    eval_by_ts = {e.get("ts_code"): e for e in eval_results}
    common_ts = set(truth_by_ts.keys()) & set(eval_by_ts.keys())

    lines: List[str] = []

    # ============================================================
    # 报告头部
    # ============================================================

    lines.append("# 🔬 双引擎交叉验证报告")
    lines.append("")
    lines.append("> T.R.U.T.H. (数据驱动基因分析) × Evaluators (因果贝叶斯规则) 交叉验证")
    lines.append("")
    lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **T.R.U.T.H. 股票数**: {len(truth_profiles)}")
    lines.append(f"- **Evaluators 股票数**: {len(eval_results)}")
    lines.append(f"- **共同覆盖**: {len(common_ts)}")
    lines.append("")

    # ============================================================
    # 引擎一致性统计 (Phase 4)
    # ============================================================

    lines.append("## 📊 引擎一致性分析")
    lines.append("")

    # 收集共同股票的评分对
    truth_scores = []
    eval_scores = []
    agreements = 0
    divergences = []

    # 信号映射到数值
    truth_signal_rank = {
        "STRONG_BUY": 5, "strong_buy": 5,
        "BUY": 4, "buy": 4,
        "HOLD": 3, "hold": 3,
        "CAUTION": 2, "caution": 2,
        "SELL": 1, "sell": 1,
        "FRAUD_ALERT": 0, "fraud_alert": 0, "meltdown": 0,
    }
    eval_decision_rank = {
        "quality": 5, "average": 3, "poor": 2, "uncertain": 2, "veto": 0,
    }

    for ts_code in common_ts:
        tp = truth_by_ts[ts_code]
        ep = eval_by_ts[ts_code]

        t_score = tp.get("final_score", 0) or 0
        e_score = (ep.get("score", 0) or 0) / 100.0  # 归一化到 0-1

        truth_scores.append(t_score)
        eval_scores.append(e_score)

        # 一致性判断
        t_signal = tp.get("signal", "hold")
        e_decision = ep.get("decision", "uncertain")
        t_rank = truth_signal_rank.get(t_signal, 3)
        e_rank = eval_decision_rank.get(e_decision, 2)

        # 同向 (都看好 or 都看空) 算一致
        t_positive = t_rank >= 4
        e_positive = e_rank >= 5
        t_negative = t_rank <= 1
        e_negative = e_rank <= 0

        if (t_positive and e_positive) or (t_negative and e_negative) or (not t_positive and not t_negative and not e_positive and not e_negative):
            agreements += 1
        else:
            # 严重分歧: 一个看好一个看空
            if (t_positive and e_negative) or (t_negative and e_positive):
                divergences.append({
                    "ts_code": ts_code,
                    "truth_signal": t_signal,
                    "truth_score": t_score,
                    "eval_decision": e_decision,
                    "eval_score": ep.get("score", 0),
                })

    # Spearman 相关
    spearman = _compute_spearman(truth_scores, eval_scores) if len(common_ts) >= 3 else 0.0
    agreement_rate = agreements / len(common_ts) * 100 if common_ts else 0

    lines.append("### 相关性指标")
    lines.append("")
    lines.append(f"- **Spearman 秩相关系数**: {spearman:.3f}")
    if spearman >= 0.7:
        lines.append("  - 高度一致 — 两引擎结论高度可信")
    elif spearman >= 0.5:
        lines.append("  - 中度一致 — 结论基本可信，关注分歧")
    elif spearman >= 0.3:
        lines.append("  - 低度一致 — 两引擎存在较大分歧，需审慎参考")
    else:
        lines.append("  - 几乎无相关 — 引擎校准可能存在问题")
    lines.append(f"- **信号方向一致率**: {agreement_rate:.1f}%")
    lines.append(f"- **严重分歧数**: {len(divergences)}")
    lines.append("")

    # 严重分歧列表
    if divergences:
        lines.append("### 严重分歧列表")
        lines.append("")
        lines.append("> 一个引擎看好、另一个看空的股票，需重点审查")
        lines.append("")
        lines.append("| 代码 | T.R.U.T.H. 信号 | T.R.U.T.H. 分 | Eval 决策 | Eval 分 |")
        lines.append("|------|------------------|---------------|-----------|---------|")
        for d in sorted(divergences, key=lambda x: -abs(x["truth_score"] - x["eval_score"] / 100)):
            t_emoji = SIGNAL_EMOJI.get(d["truth_signal"], "")
            lines.append(
                f"| {d['ts_code']} | {t_emoji}{d['truth_signal']} | {d['truth_score']:.1%} "
                f"| {d['eval_decision']} | {d['eval_score']:.1f} |"
            )
        lines.append("")

    # ============================================================
    # 双引擎共识精选 Top 20 (Phase 5)
    # ============================================================

    lines.append("## 双引擎共识精选 (Top 20)")
    lines.append("")
    lines.append("> 两个独立引擎都给出积极评价的股票")
    lines.append("")

    # 共识评分 = T.R.U.T.H. 分 × 0.5 + Eval 分(归一化) × 0.5
    consensus_list = []
    for ts_code in common_ts:
        tp = truth_by_ts[ts_code]
        ep = eval_by_ts[ts_code]

        t_score = tp.get("final_score", 0) or 0
        e_score = (ep.get("score", 0) or 0) / 100.0
        consensus_score = t_score * 0.5 + e_score * 0.5

        consensus_list.append({
            "ts_code": ts_code,
            "consensus_score": consensus_score,
            "truth_profile": tp,
            "eval_result": ep,
        })

    consensus_list.sort(key=lambda x: -x["consensus_score"])
    top_20 = consensus_list[:20]

    if top_20:
        lines.append("| # | 代码 | 共识分 | T.R.U.T.H. | Eval | 基因特征 | 生命周期 |")
        lines.append("|---|------|--------|------------|------|----------|----------|")

        for i, item in enumerate(top_20, 1):
            tp = item["truth_profile"]
            ep = item["eval_result"]
            ts_code = item["ts_code"]
            cs = item["consensus_score"]

            # T.R.U.T.H. 信息
            t_grade = tp.get("grade", "-")
            t_signal = tp.get("signal", "-")
            t_emoji = SIGNAL_EMOJI.get(t_signal, "")

            # Eval 信息
            e_decision = ep.get("decision", "-")
            e_score = ep.get("score", 0)

            # 基因特征摘要
            factors = tp.get("factors", {})
            gene_parts = []
            for fid in ["gamma", "GAMMA"]:
                fd = factors.get(fid)
                if isinstance(fd, dict):
                    gene_parts.append(f"γ:{fd.get('score', 0):.2f}")
                    break
            for fid in ["alpha", "ALPHA"]:
                fd = factors.get(fid)
                if isinstance(fd, dict):
                    gene_parts.append(f"α:{fd.get('score', 0):.2f}")
                    break
            for fid in ["verification", "VERIFICATION"]:
                fd = factors.get(fid)
                if isinstance(fd, dict):
                    gene_parts.append(f"V:{fd.get('score', 0):.2f}")
                    break
            gene_str = " ".join(gene_parts) if gene_parts else "-"

            # 生命周期
            state = ep.get("company_state", "-")
            state_labels = {
                "emerging": "🚀", "growth": "📈", "mature": "🏔️",
                "declining": "📉", "turnaround": "🔄", "distressed": "⚠️",
                "cash_cow": "💰", "slowing": "📊",
            }
            state_str = state_labels.get(state, "") + state

            lines.append(
                f"| {i} | {ts_code} | {cs:.1%} | {t_emoji}{t_grade} | {e_decision}({e_score:.0f}) "
                f"| {gene_str} | {state_str} |"
            )

        lines.append("")

        # Top 5 详细基因分析
        lines.append("### Top 5 详细分析")
        lines.append("")
        for item in top_20[:5]:
            tp = item["truth_profile"]
            ep = item["eval_result"]
            ts_code = item["ts_code"]

            lines.append(f"#### {ts_code} (共识分: {item['consensus_score']:.1%})")
            lines.append("")

            # T.R.U.T.H. 六维基因
            factors = tp.get("factors", {})
            if factors:
                lines.append("**六维基因图谱:**")
                lines.append("")
                lines.append("| 因子 | 分数 | 说明 |")
                lines.append("|------|------|------|")
                for fid, fdata in factors.items():
                    fname = FACTOR_NAMES.get(fid, fid)
                    if isinstance(fdata, dict):
                        score = fdata.get("score", 0)
                        lines.append(f"| {fname} | {_format_factor_score(score)} | |")
                lines.append("")

            # 求解器阈值
            solvers = tp.get("solvers", {})
            if solvers:
                lines.append("**物理求解器:**")
                lines.append("")
                for sid, sdata in solvers.items():
                    sname = SOLVER_NAMES.get(sid, sid)
                    if isinstance(sdata, dict):
                        th = sdata.get("thresholds", {})
                        for thname, thdata in th.items():
                            if isinstance(thdata, dict):
                                lines.append(f"- {sname}: {thdata.get('description', _format_threshold(thdata))}")
                lines.append("")

            # Eval 关键因素
            eval_factors = ep.get("factors", [])
            if eval_factors:
                top_f = sorted(eval_factors, key=lambda f: abs(f.get("contribution", 0)), reverse=True)[:3]
                lines.append("**关键驱动因素:**")
                for f in top_f:
                    dir_icon = "↑" if f.get("direction") == "positive" else "↓"
                    lines.append(f"- {dir_icon} {f.get('name', '')}: {f.get('value', 0):.3f}")
                lines.append("")

            lines.append("---")
            lines.append("")

    # ============================================================
    # 风险预警 (Phase 5)
    # ============================================================

    lines.append("## 风险预警")
    lines.append("")

    # 收集高风险股票
    risk_stocks = []
    for ts_code in common_ts:
        tp = truth_by_ts[ts_code]
        ep = eval_by_ts[ts_code]

        warnings = tp.get("warnings", [])
        fatal_warnings = [w for w in warnings if isinstance(w, dict) and w.get("level") in ("FATAL", "CRITICAL")]

        t_signal = tp.get("signal", "")
        e_decision = ep.get("decision", "")

        if t_signal in ("FRAUD_ALERT", "fraud_alert", "meltdown"):
            risk_stocks.append({
                "ts_code": ts_code, "reason": "欺诈熵熔断",
                "truth_signal": t_signal, "eval_decision": e_decision,
                "warnings": fatal_warnings,
            })
        elif fatal_warnings:
            risk_stocks.append({
                "ts_code": ts_code, "reason": fatal_warnings[0].get("title", "严重警告"),
                "truth_signal": t_signal, "eval_decision": e_decision,
                "warnings": fatal_warnings,
            })

    if risk_stocks:
        lines.append(f"> 共 {len(risk_stocks)} 家公司触发高级别风险警告")
        lines.append("")
        lines.append("| 代码 | 风险原因 | T.R.U.T.H. | Eval | 详情 |")
        lines.append("|------|----------|------------|------|------|")
        for r in risk_stocks[:30]:
            detail = ""
            if r["warnings"]:
                detail = r["warnings"][0].get("message", "")[:40]
            lines.append(
                f"| {r['ts_code']} | {r['reason']} | {r['truth_signal']} "
                f"| {r['eval_decision']} | {detail} |"
            )
        if len(risk_stocks) > 30:
            lines.append(f"| ... | | | | 还有 {len(risk_stocks) - 30} 家 |")
        lines.append("")
    else:
        lines.append("> 本次分析未发现高级别风险警告")
        lines.append("")

    # ============================================================
    # 统计验证节 (Phase 5)
    # ============================================================

    lines.append("## 统计验证")
    lines.append("")

    if truth_scores:
        import statistics
        t_mean = statistics.mean(truth_scores)
        t_stdev = statistics.stdev(truth_scores) if len(truth_scores) > 1 else 0
        e_mean = statistics.mean(eval_scores)
        e_stdev = statistics.stdev(eval_scores) if len(eval_scores) > 1 else 0

        lines.append("### 分数分布")
        lines.append("")
        lines.append("| 统计量 | T.R.U.T.H. | Evaluators |")
        lines.append("|--------|------------|------------|")
        lines.append(f"| 均值 | {t_mean:.2%} | {e_mean:.2%} |")
        lines.append(f"| 标准差 | {t_stdev:.2%} | {e_stdev:.2%} |")
        lines.append(f"| 最高 | {max(truth_scores):.2%} | {max(eval_scores):.2%} |")
        lines.append(f"| 最低 | {min(truth_scores):.2%} | {min(eval_scores):.2%} |")
        lines.append("")

        # T.R.U.T.H. 评级分布
        truth_summary = truth_result.get("summary", {})
        grade_dist = truth_summary.get("grade_distribution", {})
        signal_dist = truth_summary.get("signal_distribution", {})

        if grade_dist:
            lines.append("### T.R.U.T.H. 评级分布")
            lines.append("")
            total = sum(grade_dist.values())
            lines.append("| 评级 | 数量 | 占比 |")
            lines.append("|------|------|------|")
            for g in ["A+", "A", "B+", "B", "C", "D", "F"]:
                cnt = grade_dist.get(g, 0)
                pct = cnt / total * 100 if total > 0 else 0
                lines.append(f"| {GRADE_EMOJI.get(g, '')} {g} | {cnt} | {pct:.1f}% |")
            lines.append("")

        # Evaluators 决策分布
        eval_summary = evaluator_result.get("summary", {})
        e_total = eval_summary.get("total_evaluated", len(eval_results))
        e_quality = eval_summary.get("quality_count", 0)
        e_veto = eval_summary.get("veto_count", 0)

        lines.append("### Evaluators 决策分布")
        lines.append("")
        lines.append(f"- 优质: {e_quality} ({e_quality / e_total * 100:.1f}%)" if e_total > 0 else "- 优质: 0")
        lines.append(f"- 否决: {e_veto} ({e_veto / e_total * 100:.1f}%)" if e_total > 0 else "- 否决: 0")
        lines.append(f"- 其他: {e_total - e_quality - e_veto}" if e_total > 0 else "- 其他: 0")
        lines.append("")

    # ============================================================
    # 数据质量
    # ============================================================

    lines.append("### 数据质量")
    lines.append("")
    truth_meta = truth_result.get("metadata", {})
    has_fc = truth_meta.get("has_financial_context", False)
    lines.append(f"- **Financial Context 探针**: {'已接入' if has_fc else '未接入'}")
    lines.append(f"- **因子数量**: {truth_meta.get('factor_count', 6)}")
    lines.append(f"- **求解器数量**: {truth_meta.get('solver_count', 3)}")
    lines.append(f"- **算法版本**: T.R.U.T.H. {truth_meta.get('algo_version', '?')} / Eval v2.0")
    lines.append("")

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 AStock 双引擎交叉验证系统自动生成*")
    lines.append("")
    lines.append("**免责声明**: 本报告仅供参考，不构成投资建议。投资有风险，决策需谨慎。")

    content = "\n".join(lines)

    # 写入文件
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(content, encoding="utf-8")
        logger.info(f"✅ 交叉验证报告已生成: {output_path} (约 {len(lines)} 行)")
    except Exception as e:
        logger.warning(f"写入交叉验证报告失败: {e}")

    return content

