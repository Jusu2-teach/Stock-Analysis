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
from typing import Dict, Any, List, Optional, Tuple

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
    truth_result: Optional[Dict[str, Any]] = None,
    output_path: str = "data/comprehensive_analysis_report.md",
) -> str:
    """
    生成综合趋势分析报告（v4.2 因果贝叶斯评估器 + TRUTH 交叉验证）

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
    lines.append("> 29条规则引擎 × 生命周期推断 × 8维指标趋势分析")
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
        lines.append("| 代码 | 名称 | 行业 | 得分 | 置信度 | 生命周期 | 主要驱动因素 |")
        lines.append("|------|------|------|------|--------|----------|-------------|")
        state_labels = {
            "emerging": "🚀成长", "growth": "📈高增长", "mature": "🏔️成熟", "declining": "📉衰退",
            "slowing": "📊放缓", "turnaround": "🔄反转", "distressed": "⚠️困境", "cash_cow": "💰现金牛",
            "cyclical_peak": "🔝周期顶", "cyclical_trough": "🔻周期底",
        }
        for e in sorted(quality_evals, key=lambda x: -x.get("score", 0)):
            ts_code = e.get("ts_code", "")
            name = e.get("name", "") or ""
            industry = e.get("industry", "") or ""
            score = e.get("score", 0)
            conf = e.get("confidence", 0)
            state = e.get("company_state", "")
            state_str = state_labels.get(state, state)
            # 主要贡献因素（取top3，用箭头标方向）
            factors = e.get("factors", [])
            top_factors = sorted(factors, key=lambda f: abs(f.get("contribution", 0)), reverse=True)[:3]
            factor_parts = []
            for f in top_factors:
                arrow = "↑" if f.get("direction") == "positive" else "↓"
                factor_parts.append(f"{arrow}{f.get('name', '')}:{f.get('value', 0):.2f}")
            factor_str = ", ".join(factor_parts)
            lines.append(f"| {ts_code} | {name[:6]} | {industry[:6]} | {score:.1f} | {conf:.0%} | {state_str} | {factor_str} |")
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
        lines.append("| 代码 | 名称 | 得分 | 置信度 | 生命周期 | 主要因素 |")
        lines.append("|------|------|------|--------|----------|----------|")
        sorted_avg = sorted(average_evals, key=lambda x: -x.get("score", 0))
        state_labels = {
            "emerging": "🚀", "growth": "📈", "mature": "🏔️", "declining": "📉",
            "slowing": "📊", "turnaround": "🔄", "distressed": "⚠️", "cash_cow": "💰",
            "cyclical_peak": "🔝", "cyclical_trough": "🔻",
        }
        for e in sorted_avg[:80]:
            name = (e.get('name') or '')[:6]
            factors = e.get('factors', [])
            top_f = sorted(factors, key=lambda f: abs(f.get('contribution', 0)), reverse=True)[:2]
            f_str = ', '.join(f"{f.get('name','')}" for f in top_f)
            lines.append(f"| {e.get('ts_code', '')} | {name} | {e.get('score', 0):.1f} | {e.get('confidence', 0):.0%} | {state_labels.get(e.get('company_state', ''), '')} | {f_str} |")
        if len(sorted_avg) > 80:
            lines.append(f"| ... | | | | | 还有 {len(sorted_avg) - 80} 家 |")
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
            lines.append("| 代码 | 名称 | 决策 | 得分 | 置信度 |")
            lines.append("|------|------|------|------|--------|")
            for e in sorted(group, key=lambda x: -x.get("score", 0))[:20]:
                dec = e.get("decision", "")
                dec_str = decision_emoji.get(dec, "") + dec
                name = (e.get('name') or '')[:6]
                lines.append(f"| {e.get('ts_code', '')} | {name} | {dec_str} | {e.get('score', 0):.1f} | {e.get('confidence', 0):.0%} |")
            if len(group) > 20:
                lines.append(f"| ... | | | | 还有 {len(group) - 20} 家 |")
            lines.append("")

    # ============================================================
    # 按行业分组统计（新增）
    # ============================================================

    industry_stats = {}
    for e in evaluations:
        ind = e.get("industry") or "未知"
        if ind not in industry_stats:
            industry_stats[ind] = {"count": 0, "quality": 0, "veto": 0, "total_score": 0}
        industry_stats[ind]["count"] += 1
        industry_stats[ind]["total_score"] += e.get("score", 0)
        if e.get("decision") == "quality":
            industry_stats[ind]["quality"] += 1
        elif e.get("decision") == "veto":
            industry_stats[ind]["veto"] += 1

    if len(industry_stats) > 1:
        lines.append("## 🏭 行业分析")
        lines.append("")
        lines.append("| 行业 | 总数 | 优质 | 否决 | 优质率 | 平均分 |")
        lines.append("|------|------|------|------|--------|--------|")
        for ind, stats in sorted(industry_stats.items(), key=lambda x: -x[1]["quality"]):
            avg = stats["total_score"] / stats["count"] if stats["count"] > 0 else 0
            quality_rate = stats["quality"] / stats["count"] * 100 if stats["count"] > 0 else 0
            lines.append(f"| {ind[:8]} | {stats['count']} | {stats['quality']} | {stats['veto']} | {quality_rate:.0f}% | {avg:.1f} |")
        lines.append("")

    # ============================================================
    # 否决股票（简化）
    # ============================================================

    veto_evals = [e for e in evaluations if e.get("decision") == "veto"]
    if veto_evals:
        lines.append("## ❌ 否决公司 (VETO)")
        lines.append("")
        lines.append(f"> 共 {len(veto_evals)} 家公司被否决（≥ 3 指标共识否决）")
        lines.append("")
        lines.append("| 代码 | 名称 | 行业 | 得分 | 置信度 | 否决原因 |")
        lines.append("|------|------|------|------|--------|--------|")
        for e in sorted(veto_evals, key=lambda x: x.get("score", 0))[:50]:
            ts_code = e.get("ts_code", "")
            name = (e.get("name") or "")[:6]
            industry = (e.get("industry") or "")[:6]
            score = e.get("score", 0)
            confidence = e.get("confidence", 0)
            veto_reason = (e.get("veto_reason") or "")[:30]
            lines.append(f"| {ts_code} | {name} | {industry} | {score:.1f} | {confidence:.1%} | {veto_reason} |")
        if len(veto_evals) > 50:
            lines.append(f"| ... | | | | | 还有 {len(veto_evals) - 50} 家 |")
        lines.append("")

    # ============================================================
    # 🔬 双引擎交叉验证摘要 (v4.2 新增)
    # ============================================================

    if truth_result:
        truth_profiles = truth_result.get("profiles", [])
        truth_map = {p.get("ts_code", ""): p for p in truth_profiles}

        lines.append("## 🔬 双引擎交叉验证")
        lines.append("")
        lines.append("> 对比 Evaluator (规则驱动) vs T.R.U.T.H. (数据驱动) 的选股结论")
        lines.append("")

        # 共识优质
        consensus_quality = []
        e_quality_t_poor = []
        for e in evaluations:
            if e.get("decision") != "quality":
                continue
            ts = e.get("ts_code", "")
            t = truth_map.get(ts, {})
            if not t:
                continue
            t_dec = _infer_decision_from_truth(t)
            t_lc, _ = _infer_lifecycle_from_truth(t)
            if t_dec == "quality":
                factors = t.get("factors", {})
                gamma_s = 0
                gamma_fd = factors.get("gamma", {})
                if isinstance(gamma_fd, dict):
                    gamma_s = gamma_fd.get("score", 0)
                # v7.0: π因子盈利能力
                pi_s = 0
                pi_fd = factors.get("pi_profitability", {})
                if isinstance(pi_fd, dict):
                    pi_s = pi_fd.get("score", 0)
                gq = _get_factor_detail(factors, "verification", "growth_quality", "")
                consensus_quality.append({
                    "ts_code": ts,
                    "name": (e.get("name") or t.get("name", ""))[:6],
                    "industry": (e.get("industry") or t.get("industry", ""))[:6],
                    "e_score": e.get("score", 0),
                    "t_score": t.get("final_score", 0),
                    "t_grade": t.get("grade", ""),
                    "e_lifecycle": e.get("company_state", ""),
                    "t_lifecycle": t_lc,
                    "gamma": gamma_s,
                    "pi": pi_s,
                    "growth_quality": gq,
                })
            elif t_dec == "poor":
                e_quality_t_poor.append({
                    "ts_code": ts,
                    "name": (e.get("name") or "")[:6],
                    "e_score": e.get("score", 0),
                    "t_score": t.get("final_score", 0),
                    "t_grade": t.get("grade", ""),
                })

        e_q_count = sum(1 for e in evaluations if e.get("decision") == "quality")
        t_q_count = sum(1 for p in truth_profiles if _infer_decision_from_truth(p) == "quality")

        lines.append(f"- Evaluator 优质: **{e_q_count}** 家")
        lines.append(f"- T.R.U.T.H. 优质: **{t_q_count}** 家")
        lines.append(f"- **双引擎共识**: **{len(consensus_quality)}** 家 ({len(consensus_quality)/max(1,e_q_count)*100:.0f}% of Evaluator)")
        lines.append(f"- Evaluator优质但TRUTH较差: {len(e_quality_t_poor)} 家")
        lines.append("")

        if consensus_quality:
            consensus_quality.sort(key=lambda x: -(x["e_score"] + x["t_score"] * 100) / 2)
            lines.append("### ⭐ 双引擎共识优质 (最高信度)")
            lines.append("")
            lines.append("| 代码 | 名称 | 行业 | E评分 | T评分 | T评级 | E周期 | T周期 | γ | π | 成长质量 |")
            lines.append("|------|------|------|-------|-------|-------|-------|-------|---|---|----------|")
            for item in consensus_quality:
                lines.append(f"| {item['ts_code']} | {item['name']} | {item['industry']} | "
                           f"{item['e_score']:.1f} | {item['t_score']:.1%} | {item['t_grade']} | "
                           f"{item['e_lifecycle']} | {item['t_lifecycle']} | "
                           f"{item['gamma']:.2f} | {item['pi']:.2f} | {item['growth_quality']} |")
            lines.append("")

        if e_quality_t_poor:
            lines.append("### ⚠️ 分歧警告 (Evaluator优质 / TRUTH较差)")
            lines.append("")
            lines.append("| 代码 | 名称 | E评分 | T评分 | T评级 | 风险提示 |")
            lines.append("|------|------|-------|-------|-------|----------|")
            for item in sorted(e_quality_t_poor, key=lambda x: -x["e_score"])[:10]:
                lines.append(f"| {item['ts_code']} | {item['name']} | "
                           f"{item['e_score']:.1f} | {item['t_score']:.1%} | {item['t_grade']} | "
                           f"水平高但成长存疑 |")
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
    lines.append("│  5. 规则驱动融合 (50% Rules)        │")
    lines.append("│  6. DS/Copula 辅助验证 (各15%)      │")
    lines.append("└─────────────────────────────────────┘")
    lines.append("        ↓")
    lines.append("   决策: QUALITY / AVERAGE / POOR / VETO")
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
    lines.append("> 如需 T.R.U.T.H. 数据驱动报告（八维基因+动态阈值），请使用 `report_truth` 方法。")

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
    "pi_profitability": "π 盈利能力",
    "PI": "π 盈利能力",
    "lambda_leverage": "λ 杠杆风险",
    "LAMBDA": "λ 杠杆风险",
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
    name = profile.get("name") or ""
    industry = profile.get("industry") or ""

    # 标题
    signal = profile.get("signal", "")
    grade = profile.get("grade", "")
    emoji = SIGNAL_EMOJI.get(signal, "")
    grade_emoji = GRADE_EMOJI.get(grade, "")

    name_str = f" {name}" if name else ""
    industry_str = f" [{industry}]" if industry else ""
    lines.append(f"### {ts_code}{name_str}{industry_str} {emoji} {grade_emoji}")
    lines.append("")

    # 综合评分
    final_score = profile.get("final_score")
    confidence = profile.get("confidence")
    if final_score is not None:
        lines.append(f"**综合评分**: {final_score:.2%} | **评级**: {grade} | **信号**: {signal} | **置信度**: {confidence:.0%}")
        lines.append("")

    # 八维因子表
    factors = profile.get("factors", {})
    if factors:
        lines.append("#### 八维基因图谱")
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
    """生成紧凑的表格行（用于汇总列表）——展示全部6因子+3求解器"""
    ts_code = profile.get("ts_code", "")
    name = (profile.get("name") or "")[:8]
    industry = (profile.get("industry") or "")[:6]
    final_score = profile.get("final_score", 0) or 0
    grade = profile.get("grade", "-")
    signal = profile.get("signal", "-")
    confidence = profile.get("confidence", 0) or 0

    # 提取全部因子分数
    factors = profile.get("factors", {})
    def _get_factor(fid_lower, fid_upper):
        fd = factors.get(fid_lower) or factors.get(fid_upper, {})
        return fd.get("score", 0) if isinstance(fd, dict) else (fd or 0)

    alpha = _get_factor("alpha", "ALPHA")
    beta = _get_factor("beta", "BETA")
    gamma = _get_factor("gamma", "GAMMA")
    d_fraud = _get_factor("delta_fraud", "DELTA_FRAUD")
    d_decay = _get_factor("delta_decay", "DELTA_DECAY")
    verif = _get_factor("verification", "VERIFICATION")

    # 提取全部求解器分数
    solvers = profile.get("solvers", {})
    def _get_solver(sid_lower, sid_upper):
        sd = solvers.get(sid_lower) or solvers.get(sid_upper, {})
        return sd.get("score", 0) if isinstance(sd, dict) else (sd or 0)

    gravity = _get_solver("gravity", "GRAVITY")
    velocity = _get_solver("velocity", "VELOCITY")
    structure = _get_solver("structure", "STRUCTURE")

    grade_emoji = GRADE_EMOJI.get(grade, "")
    signal_emoji = SIGNAL_EMOJI.get(signal, "")

    factor_str = f"α:{alpha:.2f} β:{beta:.2f} γ:{gamma:.2f} δf:{d_fraud:.2f} δd:{d_decay:.2f} V:{verif:.2f}"
    solver_str = f"G:{gravity:.2f} V:{velocity:.2f} S:{structure:.2f}"

    return f"| {ts_code} | {name} | {industry} | {final_score:.1%} | {grade_emoji}{grade} | {signal_emoji}{signal} | {confidence:.0%} | {factor_str} | {solver_str} |"


def _generate_compact_row_v2(profile: Dict[str, Any]) -> str:
    """生成增强版紧凑表格行 — 包含决策 + 生命周期列（用于 TRUTH 报告 v4.0）"""
    ts_code = profile.get("ts_code", "")
    name = (profile.get("name") or "")[:8]
    industry = (profile.get("industry") or "")[:6]
    final_score = profile.get("final_score", 0) or 0
    grade = profile.get("grade", "-")
    signal = profile.get("signal", "-")

    # 决策 & 生命周期（由 report_truth 预注入）
    decision = profile.get("_decision", "")
    lifecycle = profile.get("_lifecycle", "")
    dec_str = DECISION_EMOJI.get(decision, "") + decision
    lc_str = LIFECYCLE_LABELS.get(lifecycle, lifecycle)

    # 提取关键因子（精简显示）
    factors = profile.get("factors", {})
    def _get_f(fid):
        fd = factors.get(fid) or factors.get(fid.upper(), {})
        return fd.get("score", 0) if isinstance(fd, dict) else (fd or 0)

    gamma = _get_f("gamma")
    d_fraud = _get_f("delta_fraud")
    d_decay = _get_f("delta_decay")
    verif = _get_f("verification")

    grade_emoji = GRADE_EMOJI.get(grade, "")
    signal_emoji = SIGNAL_EMOJI.get(signal, "")

    factor_str = f"γ:{gamma:.2f} δf:{d_fraud:.2f} δd:{d_decay:.2f} V:{verif:.2f}"

    return f"| {ts_code} | {name} | {industry} | {final_score:.1%} | {grade_emoji}{grade} | {signal_emoji}{signal} | {dec_str} | {lc_str} | {factor_str} |"


def _get_top_warning(profile: Dict[str, Any]) -> str:
    """获取最重要的警告信息"""
    warnings = profile.get("warnings", [])
    if not warnings:
        return "-"
    # 取第一个警告的标题
    w = warnings[0]
    title = w.get("title", "") if isinstance(w, dict) else str(w)
    return title[:20] + "..." if len(title) > 20 else title


# ═══════════════════════════════════════════════════════════════════════════════
# TRUTH 决策 & 生命周期推断 — 从八维基因因子推导，与 Evaluator 对齐但独立实现
# ═══════════════════════════════════════════════════════════════════════════════

def _infer_decision_from_truth(profile: Dict[str, Any]) -> str:
    """从 TRUTH 的 grade/signal + 因子细节映射到可比较的决策类别.

    映射逻辑:
        quality  ← A+, A     (真正优质)
        average  ← B+, B     (良好)
        poor     ← C, D      (一般/较差)
        veto     ← F / fraud / 结构性崩溃  (否决)

    v4.3: 新增因子驱动的否决条件，缩小与 Evaluator 的 veto 差距
    """
    grade = (profile.get("grade") or "").upper()
    signal = (profile.get("signal") or "").lower()
    factors = profile.get("factors", {})

    # 1. 熔断/欺诈信号直接否决
    if signal in ("fraud_alert", "meltdown", "strong_sell"):
        return "veto"

    # 2. F 评级直接否决
    if grade == "F":
        return "veto"

    # 3. 因子驱动的否决  (v4.3 新增)
    #    用因子细节判断结构性崩溃，弥补纯评级映射的盲区
    decay_fd = factors.get("delta_decay") or factors.get("DELTA_DECAY", {})
    decay_score = decay_fd.get("score", 0) if isinstance(decay_fd, dict) else 0
    fraud_fd = factors.get("delta_fraud") or factors.get("DELTA_FRAUD", {})
    fraud_score = fraud_fd.get("score", 0) if isinstance(fraud_fd, dict) else 0
    gamma_fd = factors.get("gamma") or factors.get("GAMMA", {})
    gamma_score = gamma_fd.get("score", 0.5) if isinstance(gamma_fd, dict) else 0.5

    consec_decline = _get_factor_detail(factors, "delta_decay", "consecutive_decline_years", 0) or 0
    decay_severity = _get_factor_detail(factors, "delta_decay", "decay_severity", "none")
    growth_type = _get_factor_detail(factors, "gamma", "growth_type", "")

    # 3a. D 评级 + 严重衰退 + 低成长 → 结构性崩溃
    if grade == "D" and decay_severity == "severe" and gamma_score < 0.30:
        return "veto"

    # 3b. δ_fraud 高分 (>0.40) + C/D 评级 → 欺诈风险否决
    if grade in ("C", "D") and fraud_score > 0.40:
        return "veto"

    # 3c. 连续衰退≥5年 + 低评级 → 长期结构性失败
    if consec_decline >= 5 and grade in ("C", "D"):
        return "veto"

    # 3d. C 评级 + 同时满足 severe 衰退 + decline 增长 + γ低 → 全面恶化
    if grade == "C" and decay_severity == "severe" and growth_type == "decline" and gamma_score < 0.40:
        return "veto"

    # 4. 标准评级映射
    if grade in ("A+", "A"):
        # v5.3: δ_decay 衰退惩罚已移至 truth engine._cross_sectional_normalize
        # 在百分位评级之前施加，score/grade/decision 三者一致
        # 不再在 reporter 层用绝对阈值覆写百分位评级结果
        return "quality"
    elif grade in ("B+", "B"):
        return "average"
    elif grade in ("C", "D"):
        return "poor"

    # 兜底: 用信号补判
    if signal in ("strong_buy", "buy"):
        return "average"
    return "poor"


def _get_factor_detail(factors: Dict, factor_id: str, key: str, default=None):
    """安全提取因子的 components/details 子字段"""
    fd = factors.get(factor_id) or factors.get(factor_id.upper(), {})
    if not isinstance(fd, dict):
        return default
    # 先找 details, 再找 components
    details = fd.get("details", {})
    if isinstance(details, dict) and key in details:
        return details[key]
    comps = fd.get("components", {})
    if isinstance(comps, dict) and key in comps:
        return comps[key]
    return default


def _infer_lifecycle_from_truth(profile: Dict[str, Any]) -> Tuple[str, float]:
    """从 TRUTH 七维基因因子推断公司生命周期阶段.

    使用因子的 components/details 层数据（比 score 更丰富），
    与 Evaluator 的 _infer_lifecycle 保持相同状态空间但独立推断。

    状态空间:
        turnaround  — 困境反转期（衰退中但近期改善）
        distressed  — 严重困境（多年连续衰退）
        growth      — 高增长期（真成长 + 高CAGR）
        emerging    — 新兴/高速扩张（高成长但质量未验证）
        cash_cow    — 现金牛（稳定 + 低衰退 + 高alpha）
        mature      — 成熟期（稳健经营）
        slowing     — 增速放缓（温和衰退迹象）
        declining   — 明确衰退（持续下降）
    """
    factors = profile.get("factors", {})

    # ── 提取关键判断维度 ──
    growth_type = _get_factor_detail(factors, "gamma", "growth_type", "")
    decay_severity = _get_factor_detail(factors, "delta_decay", "decay_severity", "none")
    growth_quality = _get_factor_detail(factors, "verification", "growth_quality", "")

    cagr = _get_factor_detail(factors, "gamma", "cagr", 0) or 0
    recent_3y = _get_factor_detail(factors, "gamma", "recent_3y_slope", 0) or 0
    consec_decline = _get_factor_detail(factors, "delta_decay", "consecutive_decline_years", 0) or 0
    has_deterioration = _get_factor_detail(factors, "delta_decay", "has_deterioration", False)

    # α score: 高=周期性强(不稳定), 低=稳定
    alpha_fd = factors.get("alpha") or factors.get("ALPHA", {})
    alpha_score = alpha_fd.get("score", 0.5) if isinstance(alpha_fd, dict) else 0.5

    # δ_fraud 熔断
    is_meltdown = _get_factor_detail(factors, "delta_fraud", "is_meltdown", False)

    # δ_decay score (0~1, 越高越差)
    decay_fd = factors.get("delta_decay") or factors.get("DELTA_DECAY", {})
    decay_score = decay_fd.get("score", 0) if isinstance(decay_fd, dict) else 0

    # γ score (成长动能)
    gamma_fd = factors.get("gamma") or factors.get("GAMMA", {})
    gamma_score = gamma_fd.get("score", 0.5) if isinstance(gamma_fd, dict) else 0.5

    # ── 决策瀑布（优先级从高到低） ──
    # A股实际: decay_severity=severe 覆盖54%, growth_type=decline 覆盖51%
    # 因此需要多因子交叉确认，避免过度分类为 distressed/declining

    # 1. 困境反转: 有衰退但近期趋势明显改善
    if has_deterioration and recent_3y > cagr and recent_3y > 0.02:
        return "turnaround", 0.70

    # 2. 严重困境: δ_fraud熔断 OR (severe衰退 + decline增长 + γ极低)
    if is_meltdown:
        return "distressed", 0.85
    if decay_severity == "severe" and growth_type == "decline" and gamma_score < 0.3:
        return "distressed", 0.80

    # 3. 高增长（已验证质量）
    if growth_type == "high_growth" and growth_quality in ("true_growth", "moderate_quality"):
        return "growth", 0.75

    # 4. 新兴/快速扩张（高成长但质量未验证或低质量）
    if growth_type == "high_growth":
        return "emerging", 0.65

    # 5. 现金牛: 稳定(低alpha) + 衰退可控 + 适度或低增长
    if growth_type in ("moderate_growth", "low_growth") and alpha_score <= 0.35 and decay_severity in ("none", "mild"):
        return "cash_cow", 0.75

    # 6. 成熟: 非衰退增长 + 衰退≤moderate
    if growth_type in ("moderate_growth", "low_growth") and decay_severity in ("none", "mild", "moderate"):
        return "mature", 0.70

    # 7. 增速放缓 vs 成熟边界: moderate/low增长 + severe衰退
    #    γ>0.50 说明成长动能仍在 → 成熟(带风险); γ≤0.50 → 真正放缓
    if growth_type in ("moderate_growth", "low_growth") and decay_severity == "severe":
        if gamma_score > 0.50:
            return "mature", 0.55  # 成熟但有结构性风险
        return "slowing", 0.60

    # 8. 早期衰退信号: 增长转负但结构性衰退不严重
    #    decay=none → 可能只是短期波动，归为成熟; mild → 温和放缓
    if growth_type == "decline" and decay_severity == "none":
        return "mature", 0.50  # 增长略负但无结构性衰退 → 成熟(保守)
    if growth_type == "decline" and decay_severity == "mild":
        return "slowing", 0.55

    # 9. 明确衰退: 增长转负 + 中度结构衰退 → declining (已确认)
    if growth_type == "decline" and decay_severity == "moderate":
        return "declining", 0.65

    # 10. 深度衰退: 增长转负 + 严重衰退 + γ≥0.3 → declining (严重但未至困境)
    if growth_type == "decline" and decay_severity == "severe":
        return "declining", 0.75

    # 11. 默认成熟（兜底: growth_type=unknown 等边缘情况）
    return "mature", 0.50


# 生命周期标签映射（与 Evaluator 报告统一）
LIFECYCLE_LABELS = {
    "emerging": "🚀 新兴扩张",
    "growth": "📈 高增长",
    "mature": "🏔️ 成熟稳健",
    "cash_cow": "💰 现金牛",
    "slowing": "📊 增速放缓",
    "declining": "📉 衰退期",
    "turnaround": "🔄 困境反转",
    "distressed": "⚠️ 严重困境",
}

DECISION_LABELS = {
    "quality": "⭐ 优质",
    "average": "🟡 良好",
    "poor": "🟠 一般",
    "veto": "❌ 否决",
}

DECISION_EMOJI = {"quality": "⭐", "average": "🟡", "poor": "🟠", "veto": "❌"}


@register_method(
    engine_name="report_truth",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. report from run_truth results (v3.1: layered display like evaluators)",
)
def report_truth(
    truth_result: Dict[str, Any],
    evaluator_result: Optional[Dict[str, Any]] = None,
    output_path: str = "data/truth_analysis_report.md",
) -> str:
    """基于 T.R.U.T.H. v4.2 结果生成专业 Markdown 报告 + Evaluator 交叉验证.

    v3.1 改进（参照 evaluators 报告格式）：
    - 分层展示：汇总 → 完整列表(表格) → Top10详细 → 按评级分组
    - 只对精选(A+/A)和Top10展开详细八维基因
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
    lines.append("> 八维基因测序 × 三大物理求解器 × 动态阈值决策系统")
    lines.append("")

    # ============================================================
    # 为每个 profile 注入 决策 + 生命周期（供后续所有表格使用）
    # ============================================================

    for p in profiles:
        p["_decision"] = _infer_decision_from_truth(p)
        lifecycle, life_conf = _infer_lifecycle_from_truth(p)
        p["_lifecycle"] = lifecycle
        p["_life_confidence"] = life_conf

    # 按评分排序
    sorted_profiles = sorted(
        profiles,
        key=lambda p: p.get("final_score", 0) or 0,
        reverse=True,
    )

    # 按决策分类 (对齐 Evaluator)
    quality_profiles = [p for p in sorted_profiles if p.get("_decision") == "quality"]
    average_profiles = [p for p in sorted_profiles if p.get("_decision") == "average"]
    poor_profiles = [p for p in sorted_profiles if p.get("_decision") == "poor"]
    veto_profiles = [p for p in sorted_profiles if p.get("_decision") == "veto"]
    total_prof = len(profiles) or 1

    # ============================================================
    # 元数据 (对齐 Evaluator: 优质/否决数)
    # ============================================================

    lines.append("## 📋 报告概要")
    lines.append("")
    lines.append(f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **算法版本**: {metadata.get('algo_version', '4.1.0')} (T.R.U.T.H.)")
    lines.append(f"- **分析股票数**: {metadata.get('universe_size', len(profiles))}")
    lines.append(f"- **优质公司数**: {len(quality_profiles)}")
    lines.append(f"- **否决公司数**: {len(veto_profiles)}")
    lines.append("")

    # ============================================================
    # 汇总统计 (对齐 Evaluator: 决策→生命周期→评级信号)
    # ============================================================

    lines.append("## 📊 汇总统计")
    lines.append("")

    # --- 决策分布 (主, 与 Evaluator 一致) ---
    lines.append("### 决策分布")
    lines.append("")
    lines.append("| 决策 | 数量 | 占比 |")
    lines.append("|------|------|------|")
    for dec, label in [("quality", "✅ 优质"), ("average", "🟡 一般"),
                       ("poor", "🟠 较差"), ("veto", "❌ 否决")]:
        cnt = sum(1 for p in profiles if p.get("_decision") == dec)
        lines.append(f"| {label} | {cnt} | {cnt / total_prof * 100:.1f}% |")
    lines.append("")

    # --- 生命周期分布 (与 Evaluator 一致) ---
    lifecycle_dist: Dict[str, int] = {}
    for p in profiles:
        lc = p.get("_lifecycle", "mature")
        lifecycle_dist[lc] = lifecycle_dist.get(lc, 0) + 1

    lines.append("### 生命周期分布")
    lines.append("")
    lines.append("| 状态 | 数量 |")
    lines.append("|------|------|")
    lifecycle_order = ["growth", "emerging", "cash_cow", "mature", "slowing", "declining", "turnaround", "distressed"]
    for lc in sorted(lifecycle_dist.keys(), key=lambda x: -lifecycle_dist[x]):
        label = LIFECYCLE_LABELS.get(lc, lc)
        lines.append(f"| {label} | {lifecycle_dist[lc]} |")
    lines.append("")

    # --- 评级 & 信号 (TRUTH 独有, 折叠为子标题) ---
    if summary:
        grade_dist = summary.get("grade_distribution", {})
        signal_dist = summary.get("signal_distribution", {})
        if grade_dist or signal_dist:
            lines.append("### 评级 & 信号详情")
            lines.append("")
            if grade_dist:
                total_g = sum(grade_dist.values())
                grade_parts = []
                for g in ["A+", "A", "B+", "B", "C", "D", "F"]:
                    cnt = grade_dist.get(g, 0)
                    if cnt > 0:
                        grade_parts.append(f"{GRADE_EMOJI.get(g, '')}{g}:{cnt}")
                lines.append(f"- **评级**: {' | '.join(grade_parts)}")
            if signal_dist:
                sig_parts = []
                for sig, cnt in sorted(signal_dist.items(), key=lambda x: -x[1]):
                    if cnt > 0:
                        sig_parts.append(f"{SIGNAL_EMOJI.get(sig, '')}{sig}:{cnt}")
                lines.append(f"- **信号**: {' | '.join(sig_parts)}")
            lines.append(f"- **平均分数**: {summary.get('average_score', 0):.2%}")
            lines.append(f"- **熔断警报**: {summary.get('meltdown_count', 0)}")
            lines.append("")

    # ============================================================
    # helper: 从TRUTH因子构建 Evaluator 风格的 "主要驱动因素" 字符串
    # ============================================================

    def _truth_driver_str(p: Dict) -> str:
        """从 TRUTH 六维因子选取 top3 构建 ↑γ成长:0.84 格式"""
        factors = p.get("factors", {})
        items = []
        factor_display = {
            "gamma": ("γ成长", True),   "GAMMA": ("γ成长", True),
            "verification": ("V验证", True), "VERIFICATION": ("V验证", True),
            "alpha": ("α周期", False),  "ALPHA": ("α周期", False),
            "beta": ("β资本", False),   "BETA": ("β资本", False),
            "delta_fraud": ("δ欺诈", False), "DELTA_FRAUD": ("δ欺诈", False),
            "delta_decay": ("δ衰退", False), "DELTA_DECAY": ("δ衰退", False),
            "lambda_leverage": ("λ杠杆", False), "LAMBDA": ("λ杠杆", False),
        }
        seen_names = set()
        for fid, fd in factors.items():
            if not isinstance(fd, dict):
                continue
            display_info = factor_display.get(fid)
            if not display_info:
                continue
            name, is_positive = display_info
            if name in seen_names:
                continue
            seen_names.add(name)
            score = fd.get("score", 0)
            # 正向因子: 分高=好(↑), 负向因子: 分低=好(↑分低), 分高=差(↓)
            if is_positive:
                arrow = "↑" if score > 0.5 else "↓"
            else:
                arrow = "↑" if score < 0.3 else "↓"
            importance = abs(score - 0.5) if is_positive else score
            items.append((importance, f"{arrow}{name}:{score:.2f}"))
        items.sort(key=lambda x: -x[0])
        return ", ".join(x[1] for x in items[:3])

    # ============================================================
    # ⭐ 优质公司完整列表 (QUALITY) — 对齐 Evaluator
    # ============================================================

    if quality_profiles:
        lines.append("## ⭐ 优质公司完整列表 (QUALITY)")
        lines.append("")
        lines.append(f"> 共 {len(quality_profiles)} 家公司通过 T.R.U.T.H. 评估（优质）")
        lines.append("")
        lines.append("| 代码 | 名称 | 行业 | 得分 | 置信度 | 生命周期 | 主要驱动因素 |")
        lines.append("|------|------|------|------|--------|----------|-------------|")

        for p in quality_profiles:
            ts_code = p.get("ts_code", "")
            name = (p.get("name") or "")[:6]
            industry = (p.get("industry") or "")[:6]
            score = p.get("final_score", 0) or 0
            conf = p.get("confidence", 0) or 0
            lc = p.get("_lifecycle", "")
            lc_label = LIFECYCLE_LABELS.get(lc, lc)
            driver = _truth_driver_str(p)
            lines.append(f"| {ts_code} | {name} | {industry} | {score:.1%} | {conf:.0%} | {lc_label} | {driver} |")
        lines.append("")

        # Top 10 详细基因分析
        lines.append("### 🏆 Top 10 详细分析")
        lines.append("")
        for profile in quality_profiles[:10]:
            lines.extend(_generate_profile_section(profile))

    # ============================================================
    # 🟡 一般公司 (AVERAGE) — 对齐 Evaluator
    # ============================================================

    if average_profiles:
        lines.append("## 🟡 一般公司 (AVERAGE)")
        lines.append("")
        lines.append(f"> 共 {len(average_profiles)} 家（良好）")
        lines.append("")
        lines.append("| 代码 | 名称 | 得分 | 置信度 | 生命周期 | 主要因素 |")
        lines.append("|------|------|------|--------|----------|----------|")
        for p in average_profiles[:80]:
            name = (p.get("name") or "")[:6]
            score = p.get("final_score", 0) or 0
            conf = p.get("confidence", 0) or 0
            lc = p.get("_lifecycle", "")
            lc_short = {"growth": "📈", "emerging": "🚀", "mature": "🏔️",
                        "declining": "📉", "slowing": "📊", "turnaround": "🔄",
                        "distressed": "⚠️", "cash_cow": "💰"}.get(lc, "")
            driver = _truth_driver_str(p)
            lines.append(f"| {p.get('ts_code', '')} | {name} | {score:.1%} | {conf:.0%} | {lc_short} | {driver} |")
        if len(average_profiles) > 80:
            lines.append(f"| ... | | | | | 还有 {len(average_profiles) - 80} 家 |")
        lines.append("")

    # ============================================================
    # 📊 按生命周期分组的非否决公司（对齐 Evaluator）
    # ============================================================

    lines.append("## 📊 按生命周期分组（非否决）")
    lines.append("")

    non_veto_profiles = [p for p in sorted_profiles if p.get("_decision") != "veto"]
    lc_groups: Dict[str, list] = {}
    for p in non_veto_profiles:
        lc = p.get("_lifecycle", "mature")
        if lc not in lc_groups:
            lc_groups[lc] = []
        lc_groups[lc].append(p)

    lc_order = ["cash_cow", "mature", "growth", "emerging", "turnaround", "slowing", "declining", "distressed"]

    for lc in lc_order:
        if lc in lc_groups:
            group = lc_groups[lc]
            label = LIFECYCLE_LABELS.get(lc, lc)
            lines.append(f"### {label} ({len(group)} 家)")
            lines.append("")
            lines.append("| 代码 | 名称 | 决策 | 得分 | 置信度 |")
            lines.append("|------|------|------|------|--------|")
            for p in sorted(group, key=lambda x: -(x.get("final_score", 0) or 0))[:20]:
                dec = p.get("_decision", "")
                dec_str = DECISION_EMOJI.get(dec, "") + dec
                name = (p.get("name") or "")[:6]
                score = p.get("final_score", 0) or 0
                conf = p.get("confidence", 0) or 0
                lines.append(f"| {p.get('ts_code', '')} | {name} | {dec_str} | {score:.1%} | {conf:.0%} |")
            if len(group) > 20:
                lines.append(f"| ... | | | | 还有 {len(group) - 20} 家 |")
            lines.append("")

    # ============================================================
    # 🏭 行业分析 (对齐 Evaluator, TRUTH 新增)
    # ============================================================

    industry_stats: Dict[str, Dict] = {}
    for p in profiles:
        ind = p.get("industry") or "未知"
        if ind not in industry_stats:
            industry_stats[ind] = {"count": 0, "quality": 0, "veto": 0, "total_score": 0.0}
        industry_stats[ind]["count"] += 1
        industry_stats[ind]["total_score"] += (p.get("final_score", 0) or 0)
        if p.get("_decision") == "quality":
            industry_stats[ind]["quality"] += 1
        elif p.get("_decision") == "veto":
            industry_stats[ind]["veto"] += 1

    if len(industry_stats) > 1:
        lines.append("## 🏭 行业分析")
        lines.append("")
        lines.append("| 行业 | 总数 | 优质 | 否决 | 优质率 | 平均分 |")
        lines.append("|------|------|------|------|--------|--------|")
        for ind, stats in sorted(industry_stats.items(), key=lambda x: -x[1]["quality"]):
            avg = stats["total_score"] / stats["count"] * 100 if stats["count"] > 0 else 0
            qr = stats["quality"] / stats["count"] * 100 if stats["count"] > 0 else 0
            lines.append(f"| {ind[:8]} | {stats['count']} | {stats['quality']} | {stats['veto']} | {qr:.0f}% | {avg:.1f} |")
        lines.append("")

    # ============================================================
    # ❌ 否决公司 (VETO) — 合并 F评级+熔断 (对齐 Evaluator)
    # ============================================================

    if veto_profiles:
        lines.append("## ❌ 否决公司 (VETO)")
        lines.append("")
        lines.append(f"> 共 {len(veto_profiles)} 家公司被否决（F 评级 / 欺诈熵熔断）")
        lines.append("")
        lines.append("| 代码 | 名称 | 行业 | 得分 | 置信度 | 否决原因 |")
        lines.append("|------|------|------|------|--------|--------|")
        for p in veto_profiles[:50]:
            ts_code = p.get("ts_code", "")
            name = (p.get("name") or "")[:6]
            industry = (p.get("industry") or "")[:6]
            score = p.get("final_score", 0) or 0
            conf = p.get("confidence", 0) or 0
            signal = (p.get("signal") or "").lower()
            if signal in ("fraud_alert", "meltdown"):
                reason = "欺诈熵熔断"
            else:
                reason = "评分极低(F级)"
            lines.append(f"| {ts_code} | {name} | {industry} | {score:.1%} | {conf:.0%} | {reason} |")
        if len(veto_profiles) > 50:
            lines.append(f"| ... | | | | | 还有 {len(veto_profiles) - 50} 家 |")
        lines.append("")

    # ============================================================
    # 🔬 双引擎交叉验证摘要 (v4.2)
    # ============================================================

    if evaluator_result:
        eval_list = evaluator_result.get("evaluations", [])
        eval_map = {e.get("ts_code", ""): e for e in eval_list}

        lines.append("## 🔬 双引擎交叉验证")
        lines.append("")
        lines.append("> 对比 T.R.U.T.H. (数据驱动) vs Evaluator (规则驱动) 的选股结论")
        lines.append("")

        consensus_quality = []
        t_quality_e_poor = []
        for p in sorted_profiles:
            if p.get("_decision") != "quality":
                continue
            ts = p.get("ts_code", "")
            e = eval_map.get(ts, {})
            if not e:
                continue
            e_dec = e.get("decision", "")
            if e_dec == "quality":
                consensus_quality.append({
                    "ts_code": ts,
                    "name": (p.get("name") or e.get("name", ""))[:6],
                    "industry": (p.get("industry") or e.get("industry", ""))[:6],
                    "t_score": p.get("final_score", 0),
                    "t_grade": p.get("grade", ""),
                    "e_score": e.get("score", 0),
                    "e_lifecycle": e.get("company_state", ""),
                    "t_lifecycle": p.get("_lifecycle", ""),
                })
            elif e_dec == "poor":
                t_quality_e_poor.append({
                    "ts_code": ts,
                    "name": (p.get("name") or "")[:6],
                    "t_score": p.get("final_score", 0),
                    "t_grade": p.get("grade", ""),
                    "e_score": e.get("score", 0),
                })

        t_q_count = len(quality_profiles)
        e_q_count = sum(1 for e in eval_list if e.get("decision") == "quality")

        lines.append(f"- T.R.U.T.H. 优质: **{t_q_count}** 家")
        lines.append(f"- Evaluator 优质: **{e_q_count}** 家")
        lines.append(f"- **双引擎共识**: **{len(consensus_quality)}** 家 ({len(consensus_quality)/max(1,t_q_count)*100:.0f}% of T.R.U.T.H.)")
        lines.append(f"- TRUTH优质但Evaluator较差: {len(t_quality_e_poor)} 家")
        lines.append("")

        if consensus_quality:
            consensus_quality.sort(key=lambda x: -(x["e_score"] + x["t_score"] * 100) / 2)
            lines.append("### ⭐ 双引擎共识优质 (最高信度)")
            lines.append("")
            lines.append("| 代码 | 名称 | 行业 | T评分 | T评级 | E评分 | E周期 | T周期 |")
            lines.append("|------|------|------|-------|-------|-------|-------|-------|")
            for item in consensus_quality:
                lines.append(f"| {item['ts_code']} | {item['name']} | {item['industry']} | "
                           f"{item['t_score']:.1%} | {item['t_grade']} | "
                           f"{item['e_score']:.1f} | {item['e_lifecycle']} | {item['t_lifecycle']} |")
            lines.append("")

        if t_quality_e_poor:
            lines.append("### ⚠️ 分歧警告 (TRUTH优质 / Evaluator较差)")
            lines.append("")
            lines.append("| 代码 | 名称 | T评分 | T评级 | E评分 | 提示 |")
            lines.append("|------|------|-------|-------|-------|------|")
            for item in sorted(t_quality_e_poor, key=lambda x: -x["t_score"])[:10]:
                lines.append(f"| {item['ts_code']} | {item['name']} | "
                           f"{item['t_score']:.1%} | {item['t_grade']} | "
                           f"{item['e_score']:.1f} | 成长好但规则引擎扣分多 |")
            lines.append("")

    # ============================================================
    # 方法论说明
    # ============================================================

    lines.append("## 📖 方法论说明")
    lines.append("")
    lines.append("### T.R.U.T.H. 八维基因模型")
    lines.append("")
    lines.append("```")
    lines.append("┌─────────────────────────────────────────────────────────┐")
    lines.append("│                  八维基因图谱                            │")
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
    lines.append("| ⭐⭐⭐ A+ | ≥78% | 极度优质，强烈推荐 |")
    lines.append("| ⭐⭐ A | 68-78% | 优质标的，建议关注 |")
    lines.append("| ⭐ B+ | 58-68% | 良好标的，可考虑 |")
    lines.append("| ✅ B | 48-58% | 中等偏上，观察 |")
    lines.append("| ➖ C | 38-48% | 一般，持有观望 |")
    lines.append("| ⚠️ D | 28-38% | 较差，谨慎 |")
    lines.append("| ❌ F | <28% | 否决，回避 |")
    lines.append("")

    lines.append("### 决策映射 (对齐 Evaluator)")
    lines.append("")
    lines.append("| 决策 | 映射来源 | 含义 |")
    lines.append("|------|----------|------|")
    lines.append("| ⭐ 优质 (quality) | A+ / A 评级 | 基因优秀，值得重点关注 |")
    lines.append("| 🟡 良好 (average) | B+ / B 评级 | 基因尚可，可纳入观察池 |")
    lines.append("| 🟠 一般 (poor) | C / D 评级 | 基因平庸或较差 |")
    lines.append("| ❌ 否决 (veto) | F / fraud_alert / meltdown | 基因严重缺陷，回避 |")
    lines.append("")

    lines.append("### 公司生命周期状态")
    lines.append("")
    lines.append("| 状态 | 推断因子 | 投资含义 |")
    lines.append("|------|----------|----------|")
    lines.append("| 📈 高增长 (growth) | γ=高增长 + V=真成长 | 成长股机会 |")
    lines.append("| 🚀 新兴 (emerging) | γ=高增长 (质量未验证) | 高弹性/高风险 |")
    lines.append("| 💰 现金牛 (cash_cow) | γ=适度 + α稳定 + 无衰退 | 稳定分红 |")
    lines.append("| 🏔️ 成熟 (mature) | γ=适度 + 无严重衰退 | 价值股/蓝筹 |")
    lines.append("| 📊 放缓 (slowing) | δ_decay=mild | 观望/择机 |")
    lines.append("| 📉 衰退 (declining) | γ=decline 或 δ_decay=moderate | 规避风险 |")
    lines.append("| 🔄 反转 (turnaround) | 衰退中+近期改善 | 逆向投资机会 |")
    lines.append("| ⚠️ 困境 (distressed) | δ_decay=severe / 连跌4年+ | 高度警惕 |")
    lines.append("")

    # ============================================================
    # 页脚
    # ============================================================

    lines.append("---")
    lines.append("")
    lines.append("*报告由 T.R.U.T.H. v4.0 系统自动生成（决策+生命周期对齐 Evaluator）*")
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

            # T.R.U.T.H. 八维基因
            factors = tp.get("factors", {})
            if factors:
                lines.append("**八维基因图谱:**")
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
    # 因子-评分矛盾分析 (Phase 8: TRUTH↔Eval 深度交叉)
    # ============================================================

    lines.append("## 因子-评分矛盾分析")
    lines.append("")
    lines.append("> 检测 TRUTH 因子揭示的风险是否被 Evaluator 评分忽略")
    lines.append("")

    contradiction_list = []
    for ts_code in common_ts:
        tp = truth_by_ts[ts_code]
        ep = eval_by_ts[ts_code]
        factors = tp.get("factors", {})

        e_score = ep.get("score", 50)
        e_decision = ep.get("decision", "uncertain")

        contradictions = []

        # 检查 λ (杠杆) vs Evaluator: 高杠杆但评分不低
        for fid in ("lambda_leverage", "LAMBDA"):
            fd = factors.get(fid)
            if isinstance(fd, dict):
                lam_score = fd.get("score", 0)
                if lam_score > 0.6 and e_score > 60:
                    contradictions.append(f"λ={lam_score:.2f}(高杠杆) vs Eval={e_score:.0f}分")
                break

        # 检查 V (验证) vs Evaluator: 假成长但评分高
        for fid in ("verification", "VERIFICATION"):
            fd = factors.get(fid)
            if isinstance(fd, dict):
                v_score = fd.get("score", 0.5)
                details_v = fd.get("details", {})
                quality = details_v.get("growth_quality", "")
                if quality in ("fake_growth", "low_quality") and e_score > 60:
                    contradictions.append(f"V={v_score:.2f}({quality}) vs Eval={e_score:.0f}分")
                break

        # 检查 δ_fraud vs Evaluator: 高欺诈但评分高
        for fid in ("delta_fraud", "DELTA_FRAUD"):
            fd = factors.get(fid)
            if isinstance(fd, dict):
                fraud_score = fd.get("score", 0)
                if fraud_score > 0.4 and e_score > 50:
                    contradictions.append(f"δ_fraud={fraud_score:.2f} vs Eval={e_score:.0f}分")
                break

        if contradictions:
            contradiction_list.append({
                "ts_code": ts_code,
                "contradictions": contradictions,
                "eval_score": e_score,
                "eval_decision": e_decision,
            })

    if contradiction_list:
        contradiction_list.sort(key=lambda x: -len(x["contradictions"]))
        lines.append(f"> 共发现 {len(contradiction_list)} 家公司存在因子-评分矛盾")
        lines.append("")
        lines.append("| 代码 | Eval 决策 | 矛盾点 |")
        lines.append("|------|-----------|--------|")
        for c in contradiction_list[:25]:
            lines.append(f"| {c['ts_code']} | {c['eval_decision']}({c['eval_score']:.0f}) | {'；'.join(c['contradictions'])} |")
        lines.append("")
    else:
        lines.append("> 未发现显著因子-评分矛盾，两引擎结论基本一致")
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

