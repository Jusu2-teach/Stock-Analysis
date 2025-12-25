"""
规则引擎 (Rule Engine)
======================

统一的规则评估引擎，执行规则链并输出评估结果。

重构说明:
- 删除了原来的双引擎 (ThresholdEvaluator + TrendRuleEngine)
- 统一为单一的 RuleEngine
- 保持向后兼容的 TrendEvaluator 接口

作者: AStock Analysis System
日期: 2025-12-19
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .rule_config import RuleConfig, DEFAULT_CONFIG, RuleCategory
from .rules import (
    Rule, RuleResult, TrendContext,
    VETO_RULES, PENALTY_RULES, BONUS_RULES, VALIDATION_RULES, ALL_RULES
)
from .strategies import TrendStrategy, StrategyResult, get_default_strategies

if TYPE_CHECKING:
    from ...trend.models import TrendVector

logger = logging.getLogger(__name__)


# ============================================================================
# 规则执行结果
# ============================================================================

@dataclass
class RuleOutcome:
    """规则链执行结果"""
    passes: bool = True
    elimination_reason: str = ""
    penalty: float = 0.0
    penalty_details: List[str] = field(default_factory=list)
    bonus_details: List[str] = field(default_factory=list)
    auxiliary_notes: List[str] = field(default_factory=list)


@dataclass
class EvaluationResult:
    """
    完整评估结果

    Attributes:
        passes: 是否通过
        score: 最终得分 (0-100)
        grade: 评级 (A/B/C/D/F)
        elimination_reason: 淘汰原因 (如果被否决)
        penalty: 总扣分
        penalty_details: 扣分明细
        bonus_details: 加分明细
        auxiliary_notes: 辅助说明
        strategies: 命中的策略
        strategy_reasons: 策略命中原因
    """
    passes: bool = True
    score: float = 100.0
    grade: str = "B"
    elimination_reason: str = ""
    penalty: float = 0.0
    penalty_details: List[str] = field(default_factory=list)
    bonus_details: List[str] = field(default_factory=list)
    auxiliary_notes: List[str] = field(default_factory=list)
    strategies: List[str] = field(default_factory=list)
    strategy_reasons: List[str] = field(default_factory=list)

    def compute_grade(self) -> str:
        """根据分数计算等级"""
        if self.score >= 90:
            return "A"
        elif self.score >= 80:
            return "B"
        elif self.score >= 70:
            return "C"
        elif self.score >= 60:
            return "D"
        else:
            return "F"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passes": self.passes,
            "score": round(self.score, 1),
            "grade": self.grade,
            "elimination_reason": self.elimination_reason,
            "penalty": round(self.penalty, 1),
            "penalty_details": self.penalty_details,
            "bonus_details": self.bonus_details,
            "auxiliary_notes": self.auxiliary_notes,
            "strategies": self.strategies,
            "strategy_reasons": self.strategy_reasons,
        }


# ============================================================================
# 规则引擎
# ============================================================================

class RuleEngine:
    """
    规则引擎

    执行规则链并输出评估结果。

    规则执行顺序:
    1. Veto Rules (否决规则) - 触发即失败
    2. Penalty Rules (扣分规则) - 累计扣分
    3. Bonus Rules (加分规则) - 累计加分
    4. Validation Rules (验证规则) - 交叉验证扣分

    Example:
        engine = RuleEngine()
        outcome = engine.run(context, config)
    """

    def __init__(
        self,
        rules: Optional[List[Rule]] = None,
        enable_validation: bool = True,
    ):
        """
        初始化规则引擎

        Args:
            rules: 自定义规则列表，默认使用 ALL_RULES
            enable_validation: 是否启用交叉验证规则
        """
        if rules is None:
            self.rules = list(ALL_RULES)
            if not enable_validation:
                self.rules = [r for r in self.rules if r.category != RuleCategory.VALIDATION]
        else:
            self.rules = rules

        # 按优先级排序
        self.rules.sort(key=lambda r: r.priority)

        # 按类别分组
        self._rules_by_category = self._group_by_category()

    def _group_by_category(self) -> Dict[RuleCategory, List[Rule]]:
        """按类别分组规则"""
        grouped = {cat: [] for cat in RuleCategory}
        for rule in self.rules:
            grouped[rule.category].append(rule)
        return grouped

    def run(
        self,
        context: TrendContext,
        config: Optional[RuleConfig] = None,
        is_auxiliary: bool = False,
    ) -> RuleOutcome:
        """
        执行规则链

        Args:
            context: 趋势上下文
            config: 规则配置
            is_auxiliary: 是否为辅助指标 (如 ROIIC)

        Returns:
            RuleOutcome: 规则执行结果
        """
        config = config or DEFAULT_CONFIG

        penalty = 0.0
        penalty_details: List[str] = []
        bonus_details: List[str] = []
        auxiliary_notes: List[str] = []

        # 辅助指标判断
        metric_label = f"【{context.metric_name.upper()}辅助】" if is_auxiliary else ""

        # === Phase 1: 否决规则 ===
        for rule in self._rules_by_category[RuleCategory.VETO]:
            result = rule.execute(context, config)
            if result is None:
                continue

            if result.kind == "veto":
                if is_auxiliary:
                    logger.info(f"⚠️ {metric_label}{context.group_key}: {result.message}")
                    auxiliary_notes.append(result.message)
                    continue

                logger.info(f"❌ {result.log_prefix}: {context.group_key} - {result.message}")
                return RuleOutcome(
                    passes=False,
                    elimination_reason=result.message,
                    penalty=penalty,
                    penalty_details=penalty_details,
                    bonus_details=bonus_details,
                    auxiliary_notes=auxiliary_notes,
                )

        # === Phase 2: 扣分规则 ===
        for rule in self._rules_by_category[RuleCategory.PENALTY]:
            result = rule.execute(context, config)
            if result is None:
                continue

            if result.kind == "penalty":
                if is_auxiliary:
                    auxiliary_notes.append(result.message)
                    continue

                penalty += result.value
                penalty_details.append(result.message)

        # === Phase 3: 加分规则 ===
        for rule in self._rules_by_category[RuleCategory.BONUS]:
            result = rule.execute(context, config)
            if result is None:
                continue

            if result.kind == "bonus":
                if is_auxiliary:
                    auxiliary_notes.append(result.message)
                    continue

                penalty = max(0.0, penalty - result.value)
                bonus_details.append(result.message)

            # 周期规则可能返回 penalty
            elif result.kind == "penalty":
                if is_auxiliary:
                    auxiliary_notes.append(result.message)
                    continue
                penalty += result.value
                penalty_details.append(result.message)

        # === Phase 4: 验证规则 ===
        for rule in self._rules_by_category[RuleCategory.VALIDATION]:
            result = rule.execute(context, config)
            if result is None:
                continue

            if result.kind == "penalty":
                if is_auxiliary:
                    auxiliary_notes.append(result.message)
                    continue

                penalty += result.value
                penalty_details.append(result.message)

        return RuleOutcome(
            passes=True,
            elimination_reason="",
            penalty=penalty,
            penalty_details=penalty_details,
            bonus_details=bonus_details,
            auxiliary_notes=auxiliary_notes,
        )


# ============================================================================
# 趋势评估器 (向后兼容接口)
# ============================================================================

class TrendEvaluator:
    """
    趋势评估器

    对 TrendVector 执行规则和策略评估。

    这是向后兼容的接口，内部使用 RuleEngine。

    Example:
        evaluator = TrendEvaluator()
        result = evaluator.evaluate(
            group_key="000001.SZ",
            metric_name="roic",
            config={...},
            trend_vector=vector
        )
    """

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        strategies: Optional[List[TrendStrategy]] = None,
        enable_validation: bool = True,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.strategies = strategies or get_default_strategies()
        self.engine = RuleEngine(enable_validation=enable_validation)

    def evaluate(
        self,
        group_key: str,
        metric_name: str,
        config: Dict[str, Any],
        trend_vector: "TrendVector",
    ) -> EvaluationResult:
        """
        评估趋势向量

        Args:
            group_key: 分组键（如公司代码）
            metric_name: 指标名称
            config: 规则配置字典
            trend_vector: 趋势向量

        Returns:
            EvaluationResult: 评估结果
        """
        # 转换配置
        rule_config = RuleConfig.from_dict(config) if config else DEFAULT_CONFIG

        # 创建上下文
        context = TrendContext.from_vector(group_key, metric_name, trend_vector)

        # 判断是否辅助指标
        is_auxiliary = metric_name.lower() == "roiic"

        # 执行规则引擎
        outcome = self.engine.run(context, rule_config, is_auxiliary=is_auxiliary)

        # 执行策略
        matched_strategies = []
        strategy_reasons = []
        strategy_bonus = 0.0

        for strategy in self.strategies:
            result = strategy.evaluate(context)
            if result.matched:
                matched_strategies.append(result.name)
                strategy_reasons.append(result.reason)
                strategy_bonus += result.score_boost
                self.logger.info(f"🎯 {group_key} 命中策略 [{strategy.name}]: {result.reason}")

        # 计算最终得分
        base_score = rule_config.scoring.base_score
        final_score = max(0.0, base_score - outcome.penalty)
        final_score += strategy_bonus
        final_score = min(100.0, final_score)

        # 创建结果
        result = EvaluationResult(
            passes=outcome.passes,
            score=final_score,
            elimination_reason=outcome.elimination_reason,
            penalty=outcome.penalty,
            penalty_details=outcome.penalty_details,
            bonus_details=outcome.bonus_details,
            auxiliary_notes=outcome.auxiliary_notes,
            strategies=matched_strategies,
            strategy_reasons=strategy_reasons,
        )
        result.grade = result.compute_grade()

        return result


# ============================================================================
# 默认实例
# ============================================================================

# 默认规则引擎
default_rule_engine = RuleEngine()

# 默认评估器
default_evaluator = TrendEvaluator()


# ============================================================================
# 向后兼容别名
# ============================================================================

# 旧名称别名 (向后兼容)
ThresholdEvaluator = TrendEvaluator
ThresholdEvaluatorConfig = RuleConfig
TrendRuleEngine = RuleEngine
trend_rule_engine = default_rule_engine
DEFAULT_TREND_RULES = ALL_RULES


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 新 API
    'RuleEngine',
    'RuleOutcome',
    'TrendEvaluator',
    'EvaluationResult',
    'default_rule_engine',
    'default_evaluator',
    # 向后兼容
    'ThresholdEvaluator',
    'ThresholdEvaluatorConfig',
    'TrendRuleEngine',
    'trend_rule_engine',
    'DEFAULT_TREND_RULES',
]
