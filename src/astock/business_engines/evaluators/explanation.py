"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 决策解释模块
═══════════════════════════════════════════════════════════════════════════════

生成人类可读的评估解释。
核心原则：可解释AI (Explainable AI) - 每个决策都有清晰的因果链条。

功能：
- 生成多层次解释（摘要、详细、专家级）
- 使用因果语言而非相关性描述
- 量化各因素的贡献度
- 提供可操作的建议

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class ExplanationLevel(Enum):
    """解释详细程度"""
    SUMMARY = "summary"       # 一句话摘要
    STANDARD = "standard"     # 标准解释
    DETAILED = "detailed"     # 详细分析
    EXPERT = "expert"         # 专家级（含技术细节）


class DecisionType(Enum):
    """决策类型"""
    QUALITY = "quality"       # 优质公司
    AVERAGE = "average"       # 一般公司
    POOR = "poor"            # 劣质公司
    VETO = "veto"            # 一票否决
    UNCERTAIN = "uncertain"   # 不确定


@dataclass
class Factor:
    """影响因素"""

    name: str                 # 因素名称
    display_name: str         # 显示名称
    value: float             # 观测值
    contribution: float      # 对决策的贡献度 (-1 到 1)
    direction: str           # "positive" | "negative" | "neutral"
    threshold: Optional[float] = None  # 参考阈值
    percentile: Optional[float] = None  # 在同业中的百分位
    explanation: Optional[str] = None  # 该因素的具体解释

    @property
    def is_positive(self) -> bool:
        return self.direction == "positive"

    @property
    def is_significant(self) -> bool:
        return abs(self.contribution) > 0.1

    def format_value(self, as_percent: bool = False) -> str:
        """格式化值"""
        if as_percent:
            return f"{self.value * 100:.1f}%"
        return f"{self.value:.2f}"


@dataclass
class ExplanationResult:
    """解释结果"""

    decision: DecisionType
    confidence: float
    summary: str
    factors: List[Factor]
    causal_chain: str
    recommendations: List[str]
    caveats: List[str]  # 注意事项/警告
    score: float
    score_breakdown: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "decision": self.decision.value,
            "confidence": self.confidence,
            "summary": self.summary,
            "factors": [
                {
                    "name": f.name,
                    "display_name": f.display_name,
                    "value": f.value,
                    "contribution": f.contribution,
                    "direction": f.direction
                }
                for f in self.factors
            ],
            "causal_chain": self.causal_chain,
            "recommendations": self.recommendations,
            "caveats": self.caveats,
            "score": self.score,
            "score_breakdown": self.score_breakdown
        }


# 因素名称映射
FACTOR_DISPLAY_NAMES = {
    "roic_trend": "ROIC趋势",
    "roic_level": "ROIC水平",
    "roe_trend": "ROE趋势",
    "roe_level": "ROE水平",
    "roiic_trend": "增量资本回报趋势",
    "revenue_trend": "营收趋势",
    "revenue_growth": "营收增速",
    "gross_margin_trend": "毛利率趋势",
    "gross_margin_level": "毛利率水平",
    "net_margin_trend": "净利率趋势",
    "net_margin_level": "净利率水平",
    "ocf_trend": "经营现金流趋势",
    "ocf_ratio": "现金流/利润比",
    "company_state": "公司状态",
    "volatility": "波动性",
}

# 因素解释模板
FACTOR_EXPLANATIONS = {
    "roic_trend": {
        "positive": "ROIC呈上升趋势，表明资本效率持续改善",
        "negative": "ROIC呈下降趋势，资本效率正在恶化",
        "neutral": "ROIC基本稳定"
    },
    "roe_trend": {
        "positive": "ROE持续提升，股东回报能力增强",
        "negative": "ROE走低，股东回报能力减弱",
        "neutral": "ROE保持稳定"
    },
    "revenue_trend": {
        "positive": "营收持续增长，业务规模扩张",
        "negative": "营收下滑，业务可能面临挑战",
        "neutral": "营收基本持平"
    },
    "gross_margin_trend": {
        "positive": "毛利率提升，产品竞争力或定价能力增强",
        "negative": "毛利率下滑，可能面临成本压力或竞争加剧",
        "neutral": "毛利率稳定"
    },
    "ocf_trend": {
        "positive": "经营现金流改善，盈利质量提升",
        "negative": "经营现金流恶化，需关注盈利质量",
        "neutral": "现金流基本稳定"
    },
}


class DecisionExplainer:
    """
    决策解释器

    生成清晰、可操作的评估解释。

    Example:
        >>> explainer = DecisionExplainer()
        >>> factors = [
        ...     Factor("roic_trend", "ROIC趋势", 0.03, 0.35, "positive"),
        ...     Factor("revenue_trend", "营收趋势", 0.15, 0.25, "positive"),
        ... ]
        >>> result = explainer.explain(
        ...     decision=DecisionType.QUALITY,
        ...     confidence=0.85,
        ...     factors=factors,
        ...     score=82.5
        ... )
    """

    def __init__(
        self,
        company_name: Optional[str] = None,
        industry: Optional[str] = None,
        level: ExplanationLevel = ExplanationLevel.STANDARD
    ):
        self.company_name = company_name or "该公司"
        self.industry = industry
        self.level = level

    def explain(
        self,
        decision: DecisionType,
        confidence: float,
        factors: List[Factor],
        score: float,
        score_breakdown: Optional[Dict[str, float]] = None,
        state_info: Optional[Dict[str, Any]] = None,
        causal_diagnosis: Optional[Dict[str, Any]] = None
    ) -> ExplanationResult:
        """
        生成决策解释

        Args:
            decision: 决策类型
            confidence: 置信度
            factors: 影响因素列表
            score: 综合评分
            score_breakdown: 分数分解
            state_info: 状态机信息
            causal_diagnosis: 因果诊断信息

        Returns:
            ExplanationResult 完整解释
        """
        # 丰富因素信息
        factors = self._enrich_factors(factors)

        # 排序因素（按贡献度绝对值）
        factors = sorted(factors, key=lambda f: abs(f.contribution), reverse=True)

        # 生成摘要
        summary = self._generate_summary(decision, confidence, factors, score)

        # 生成因果链条
        causal_chain = self._generate_causal_chain(factors, causal_diagnosis)

        # 生成建议
        recommendations = self._generate_recommendations(decision, factors)

        # 生成注意事项
        caveats = self._generate_caveats(confidence, factors, state_info)

        return ExplanationResult(
            decision=decision,
            confidence=confidence,
            summary=summary,
            factors=factors,
            causal_chain=causal_chain,
            recommendations=recommendations,
            caveats=caveats,
            score=score,
            score_breakdown=score_breakdown or {}
        )

    def _enrich_factors(self, factors: List[Factor]) -> List[Factor]:
        """丰富因素信息"""
        enriched = []

        for f in factors:
            # 添加显示名称
            if not f.display_name or f.display_name == f.name:
                f.display_name = FACTOR_DISPLAY_NAMES.get(f.name, f.name)

            # 添加解释
            if not f.explanation:
                templates = FACTOR_EXPLANATIONS.get(f.name, {})
                f.explanation = templates.get(f.direction, "")

            enriched.append(f)

        return enriched

    def _generate_summary(
        self,
        decision: DecisionType,
        confidence: float,
        factors: List[Factor],
        score: float
    ) -> str:
        """生成一句话摘要"""
        # 决策描述
        decision_text = {
            DecisionType.QUALITY: "优质公司",
            DecisionType.AVERAGE: "一般公司",
            DecisionType.POOR: "劣质公司",
            DecisionType.VETO: "存在重大风险",
            DecisionType.UNCERTAIN: "无法确定质量"
        }[decision]

        # 置信度描述
        if confidence >= 0.8:
            conf_text = "高置信度"
        elif confidence >= 0.6:
            conf_text = "中等置信度"
        else:
            conf_text = "较低置信度"

        # 主要因素
        top_positive = [f for f in factors if f.is_positive and f.is_significant][:2]
        top_negative = [f for f in factors if not f.is_positive and f.is_significant][:2]

        # 构建摘要
        summary = f"{self.company_name}被评估为【{decision_text}】（{conf_text}，评分{score:.1f}）。"

        if top_positive:
            pos_names = "、".join([f.display_name for f in top_positive])
            summary += f"主要优势：{pos_names}。"

        if top_negative and decision != DecisionType.QUALITY:
            neg_names = "、".join([f.display_name for f in top_negative])
            summary += f"主要风险：{neg_names}。"

        return summary

    def _generate_causal_chain(
        self,
        factors: List[Factor],
        causal_diagnosis: Optional[Dict[str, Any]]
    ) -> str:
        """生成因果链条解释"""
        parts = []

        # 如果有因果诊断信息
        if causal_diagnosis and "explanation" in causal_diagnosis:
            parts.append(causal_diagnosis["explanation"])

        # 从因素构建因果链
        significant_factors = [f for f in factors if f.is_significant]

        if significant_factors:
            # 按贡献方向分组
            positive_factors = [f for f in significant_factors if f.is_positive]
            negative_factors = [f for f in significant_factors if not f.is_positive]

            if positive_factors:
                chain = " → ".join([f.display_name for f in positive_factors[:3]])
                parts.append(f"正向因果链：{chain} → 公司质量提升")

            if negative_factors:
                chain = " → ".join([f.display_name for f in negative_factors[:3]])
                parts.append(f"负向因果链：{chain} → 公司质量下降风险")

        if not parts:
            parts.append("因果关系不明显，建议谨慎解读。")

        return "\n".join(parts)

    def _generate_recommendations(
        self,
        decision: DecisionType,
        factors: List[Factor]
    ) -> List[str]:
        """生成可操作建议"""
        recommendations = []

        if decision == DecisionType.QUALITY:
            recommendations.append("可以作为长期投资候选，建议进一步研究估值水平")

            # 检查是否有潜在风险
            weak_factors = [f for f in factors if not f.is_positive and f.is_significant]
            if weak_factors:
                names = "、".join([f.display_name for f in weak_factors[:2]])
                recommendations.append(f"关注潜在风险点：{names}")

        elif decision == DecisionType.AVERAGE:
            recommendations.append("需要更多研究才能做出投资决策")

            # 找出最强和最弱的因素
            strong = [f for f in factors if f.is_positive and f.is_significant]
            weak = [f for f in factors if not f.is_positive and f.is_significant]

            if strong:
                names = "、".join([f.display_name for f in strong[:2]])
                recommendations.append(f"优势领域：{names}，可作为投资论点")

            if weak:
                names = "、".join([f.display_name for f in weak[:2]])
                recommendations.append(f"需改善领域：{names}，建议持续跟踪")

        elif decision == DecisionType.POOR:
            recommendations.append("建议回避，除非有明确的反转迹象")
            recommendations.append("如已持有，建议评估是否需要止损")

        elif decision == DecisionType.VETO:
            recommendations.append("存在重大风险，强烈建议回避")
            # 找出触发一票否决的因素
            veto_factors = [f for f in factors if f.contribution < -0.3]
            if veto_factors:
                names = "、".join([f.display_name for f in veto_factors])
                recommendations.append(f"一票否决触发因素：{names}")

        else:  # UNCERTAIN
            recommendations.append("信息不足，建议收集更多数据后再评估")
            recommendations.append("可以先加入观察列表，持续跟踪")

        return recommendations

    def _generate_caveats(
        self,
        confidence: float,
        factors: List[Factor],
        state_info: Optional[Dict[str, Any]]
    ) -> List[str]:
        """生成注意事项"""
        caveats = []

        # 置信度警告
        if confidence < 0.6:
            caveats.append(f"置信度较低（{confidence:.0%}），结论可能不可靠")

        # 数据质量警告
        factors_with_high_uncertainty = [
            f for f in factors if f.contribution != 0 and abs(f.value) < 0.001
        ]
        if factors_with_high_uncertainty:
            caveats.append("部分指标数值异常，可能存在数据质量问题")

        # 周期性警告
        if state_info:
            state = state_info.get("state", "")
            if "cyclical" in state.lower():
                caveats.append("公司处于周期性行业，当前评估可能受周期位置影响")
            elif state == "turnaround":
                caveats.append("公司处于反转期，需验证反转是否可持续")

        # 行业特殊性
        if self.industry:
            if "金融" in self.industry or "银行" in self.industry:
                caveats.append("金融行业有特殊评估标准，ROIC等指标参考意义有限")
            elif "房地产" in self.industry:
                caveats.append("房地产行业受政策影响大，需关注行业政策变化")

        # 通用警告
        caveats.append("本评估基于历史财务数据，不构成投资建议")

        return caveats

    def format_markdown(self, result: ExplanationResult) -> str:
        """格式化为 Markdown"""
        lines = []

        # 标题
        lines.append(f"## {self.company_name} 质量评估报告")
        lines.append("")

        # 摘要
        lines.append("### 📋 评估摘要")
        lines.append(f"> {result.summary}")
        lines.append("")

        # 评分
        lines.append("### 📊 评分详情")
        lines.append(f"- **综合评分**: {result.score:.1f}/100")
        lines.append(f"- **决策**: {result.decision.value}")
        lines.append(f"- **置信度**: {result.confidence:.0%}")
        lines.append("")

        # 因素分析
        lines.append("### 🔍 关键因素分析")
        lines.append("")
        lines.append("| 因素 | 观测值 | 贡献 | 方向 |")
        lines.append("|------|--------|------|------|")

        for f in result.factors[:8]:  # 最多显示8个因素
            direction_emoji = "🟢" if f.is_positive else ("🔴" if f.contribution < -0.1 else "🟡")
            lines.append(
                f"| {f.display_name} | {f.format_value()} | {f.contribution:+.2f} | {direction_emoji} |"
            )
        lines.append("")

        # 因果链条
        lines.append("### 🔗 因果分析")
        lines.append(result.causal_chain)
        lines.append("")

        # 建议
        lines.append("### 💡 投资建议")
        for rec in result.recommendations:
            lines.append(f"- {rec}")
        lines.append("")

        # 注意事项
        lines.append("### ⚠️ 注意事项")
        for caveat in result.caveats:
            lines.append(f"- {caveat}")

        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

def quick_explain(
    decision: str,
    score: float,
    factors: List[Dict[str, Any]],
    company_name: str = "该公司"
) -> str:
    """
    快速生成解释

    Args:
        decision: "quality" | "average" | "poor" | "veto"
        score: 评分
        factors: [{"name": str, "value": float, "contribution": float, "direction": str}, ...]
        company_name: 公司名称

    Returns:
        Markdown 格式的解释
    """
    explainer = DecisionExplainer(company_name=company_name)

    factor_objs = [
        Factor(
            name=f["name"],
            display_name=FACTOR_DISPLAY_NAMES.get(f["name"], f["name"]),
            value=f["value"],
            contribution=f["contribution"],
            direction=f.get("direction", "neutral")
        )
        for f in factors
    ]

    result = explainer.explain(
        decision=DecisionType(decision),
        confidence=0.75,
        factors=factor_objs,
        score=score
    )

    return explainer.format_markdown(result)
