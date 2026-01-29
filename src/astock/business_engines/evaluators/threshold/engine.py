"""
规则引擎 v2.0 (Rule Engine - Refactored)
========================================

使用 Protocol-based 架构和工厂模式的规则引擎。

设计原则:
- 使用 RuleFactory 创建规则
- 责任链模式执行规则
- 不可变结果对象
- 类型安全

作者: AStock Analysis System
日期: 2026-01-10
版本: 2.0.0
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
import logging

from .domain_models import TrendContext
from .protocols import RuleProtocol, RuleEngineProtocol
from .results import RuleResultImpl, EvaluationResultImpl
from .factories import RuleFactory, get_default_factory
from .rule_config import RuleConfig, DEFAULT_CONFIG, RuleCategory

logger = logging.getLogger(__name__)


@dataclass
class RuleExecutionSummary:
    """规则执行汇总"""
    veto_triggered: bool = False
    veto_reason: str = ""
    total_penalty: float = 0.0
    total_bonus: float = 0.0
    penalty_results: List[RuleResultImpl] = field(default_factory=list)
    bonus_results: List[RuleResultImpl] = field(default_factory=list)
    validation_results: List[RuleResultImpl] = field(default_factory=list)


class RuleEngine:
    """
    规则引擎 v2.0

    职责:
    - 使用工厂创建规则
    - 按优先级执行规则链
    - 汇总评估结果

    Examples:
        >>> engine = RuleEngine()
        >>> result = engine.evaluate(context)
        >>> print(f"Score: {result.score}, Grade: {result.grade}")
    """

    def __init__(
        self,
        factory: Optional[RuleFactory] = None,
        config: Optional[RuleConfig] = None
    ):
        """
        初始化规则引擎

        Args:
            factory: 规则工厂 (可选，默认使用全局工厂)
            config: 规则配置 (可选，默认使用 DEFAULT_CONFIG)
        """
        self.factory = factory or get_default_factory()
        self.config = config or DEFAULT_CONFIG

        # 缓存规则实例
        self._veto_rules: Optional[List[RuleProtocol]] = None
        self._penalty_rules: Optional[List[RuleProtocol]] = None
        self._bonus_rules: Optional[List[RuleProtocol]] = None
        self._validation_rules: Optional[List[RuleProtocol]] = None

    def _get_veto_rules(self) -> List[RuleProtocol]:
        """获取否决规则 (懒加载)"""
        if self._veto_rules is None:
            self._veto_rules = self.factory.create_veto_rules()
        return self._veto_rules

    def _get_penalty_rules(self) -> List[RuleProtocol]:
        """获取扣分规则 (懒加载)"""
        if self._penalty_rules is None:
            self._penalty_rules = self.factory.create_penalty_rules()
        return self._penalty_rules

    def _get_bonus_rules(self) -> List[RuleProtocol]:
        """获取加分规则 (懒加载)"""
        if self._bonus_rules is None:
            self._bonus_rules = self.factory.create_bonus_rules()
        return self._bonus_rules

    def _get_validation_rules(self) -> List[RuleProtocol]:
        """获取验证规则 (懒加载)"""
        if self._validation_rules is None:
            self._validation_rules = self.factory.create_validation_rules()
        return self._validation_rules

    def evaluate(
        self,
        context: TrendContext,
        config: Optional[RuleConfig] = None
    ) -> EvaluationResultImpl:
        """
        评估趋势质量

        Args:
            context: 趋势上下文
            config: 规则配置 (可选，默认使用实例配置)

        Returns:
            评估结果
        """
        cfg = config or self.config

        # 执行规则链
        summary = self._execute_rule_chain(context, cfg)

        # 计算最终得分
        final_score = self._calculate_score(summary, cfg)

        # 构建结果
        return self._build_result(summary, final_score, cfg)

    def _execute_rule_chain(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> RuleExecutionSummary:
        """
        执行规则链 (责任链模式)

        执行顺序:
        1. 否决规则 (任一触发则终止)
        2. 扣分规则 (累积扣分)
        3. 加分规则 (累积加分)
        4. 验证规则 (交叉验证)
        """
        summary = RuleExecutionSummary()

        # === 1. 执行否决规则 ===
        for rule in self._get_veto_rules():
            if not rule.enabled:
                continue

            try:
                result = rule.execute(context, config)
                if result:
                    logger.info(f"否决规则触发: {rule.name} - {result.message}")
                    summary.veto_triggered = True
                    summary.veto_reason = result.message
                    return summary  # 立即终止
            except Exception as e:
                logger.error(f"否决规则执行失败: {rule.name}, 错误: {e}")

        # === 2. 执行扣分规则 ===
        for rule in self._get_penalty_rules():
            if not rule.enabled:
                continue

            try:
                result = rule.execute(context, config)
                if result:
                    summary.total_penalty += result.value
                    summary.penalty_results.append(result)
                    logger.debug(f"扣分: {rule.name} - {result.value:.1f}")
            except Exception as e:
                logger.error(f"扣分规则执行失败: {rule.name}, 错误: {e}")

        # === 3. 执行加分规则 ===
        for rule in self._get_bonus_rules():
            if not rule.enabled:
                continue

            try:
                result = rule.execute(context, config)
                if result:
                    summary.total_bonus += result.value
                    summary.bonus_results.append(result)
                    logger.debug(f"加分: {rule.name} + {result.value:.1f}")
            except Exception as e:
                logger.error(f"加分规则执行失败: {rule.name}, 错误: {e}")

        # === 4. 执行验证规则 ===
        for rule in self._get_validation_rules():
            if not rule.enabled:
                continue

            try:
                result = rule.execute(context, config)
                if result:
                    summary.validation_results.append(result)
                    logger.debug(f"验证: {rule.name} - {result.message}")
            except Exception as e:
                logger.error(f"验证规则执行失败: {rule.name}, 错误: {e}")

        return summary

    def _calculate_score(
        self,
        summary: RuleExecutionSummary,
        config: RuleConfig
    ) -> float:
        """
        计算最终得分

        计算逻辑:
        - 基础分: 100分
        - 扣分: min(总扣分, max_penalty)
        - 加分: min(总加分, max_bonus)
        - 最终分: max(0, min(100, 基础分 - 扣分 + 加分))
        """
        if summary.veto_triggered:
            return 0.0

        base_score = config.scoring.base_score
        penalty = min(summary.total_penalty, config.scoring.max_penalty)
        bonus = min(summary.total_bonus, config.scoring.max_bonus)

        final_score = base_score - penalty + bonus

        # 限制在 [0, 100] 区间
        return max(0.0, min(100.0, final_score))

    def _build_result(
        self,
        summary: RuleExecutionSummary,
        final_score: float,
        config: RuleConfig
    ) -> EvaluationResultImpl:
        """构建评估结果"""

        # 计算等级
        grade = self._compute_grade(final_score, config)

        # 提取详细信息
        penalty_details = [r.message for r in summary.penalty_results]
        bonus_details = [r.message for r in summary.bonus_results]
        validation_notes = [r.message for r in summary.validation_results]

        return EvaluationResultImpl(
            passes=not summary.veto_triggered,
            score=final_score,
            grade=grade,
            elimination_reason=summary.veto_reason,
            penalty=summary.total_penalty,
            penalty_details=penalty_details,
            bonus_details=bonus_details,
            auxiliary_notes=validation_notes,
            strategies=[],  # 策略由 StrategyEngine 处理
            strategy_reasons=[],
        )

    def _compute_grade(self, score: float, config: RuleConfig) -> str:
        """根据分数计算等级"""
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"

    def get_rule_statistics(self) -> Dict[str, Any]:
        """
        获取规则统计信息

        Returns:
            规则统计字典
        """
        return {
            "veto_rules": len(self._get_veto_rules()),
            "penalty_rules": len(self._get_penalty_rules()),
            "bonus_rules": len(self._get_bonus_rules()),
            "validation_rules": len(self._get_validation_rules()),
            "total_rules": (
                len(self._get_veto_rules()) +
                len(self._get_penalty_rules()) +
                len(self._get_bonus_rules()) +
                len(self._get_validation_rules())
            ),
        }


# ============================================================================
# 向后兼容接口 (Facade)
# ============================================================================

class TrendEvaluator:
    """
    趋势评估器 (向后兼容接口)

    本质上是 RuleEngine 的 Facade
    """

    def __init__(
        self,
        factory: Optional[RuleFactory] = None,
        config: Optional[RuleConfig] = None
    ):
        self.engine = RuleEngine(factory, config)

    def evaluate(
        self,
        context: TrendContext,
        config: Optional[RuleConfig] = None
    ) -> EvaluationResultImpl:
        """评估趋势质量"""
        return self.engine.evaluate(context, config)


__all__ = [
    'RuleEngine',
    'TrendEvaluator',
    'RuleExecutionSummary',
    'EvaluationResultImpl',
]
