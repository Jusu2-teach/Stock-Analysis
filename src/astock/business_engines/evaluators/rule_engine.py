"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 声明式规则引擎
═══════════════════════════════════════════════════════════════════════════════

从 rules.yaml 加载并执行声明式规则:
- veto_rules: 否决规则（一票否决）
- penalty_rules: 扣分规则
- bonus_rules: 加分规则
- strategies: 投资策略识别

版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 数据模型
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class RuleResult:
    """单条规则的执行结果"""
    rule_id: str
    rule_name: str
    triggered: bool
    score_delta: float = 0.0  # 分数变化 (负=扣分, 正=加分)
    message: str = ""
    exempted: bool = False
    exemption_reason: str = ""


@dataclass
class RuleEngineResult:
    """规则引擎的完整结果"""
    base_score: float
    final_score: float
    grade: str
    vetoed: bool = False
    veto_reason: str = ""
    veto_rules: List[RuleResult] = field(default_factory=list)
    penalty_rules: List[RuleResult] = field(default_factory=list)
    bonus_rules: List[RuleResult] = field(default_factory=list)
    strategies: List[Dict[str, Any]] = field(default_factory=list)
    total_penalty: float = 0.0
    total_bonus: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 规则引擎
# ═══════════════════════════════════════════════════════════════════════════════

class RuleEngine:
    """声明式规则引擎

    从 YAML 加载规则并执行评估。

    Example:
        >>> engine = RuleEngine.from_config("config/rules.yaml")
        >>> features = {"log_slope": -0.15, "r_squared": 0.6, "cv": 0.3, ...}
        >>> result = engine.evaluate(features, metric_name="roic")
        >>> print(f"Score: {result.final_score}, Grade: {result.grade}")
    """

    def __init__(
        self,
        scoring_config: Dict[str, Any],
        veto_rules: List[Dict],
        penalty_rules: List[Dict],
        bonus_rules: List[Dict],
        strategies: List[Dict],
        exemption_definitions: Dict[str, Dict],
    ):
        self._scoring = scoring_config
        self._veto_rules = veto_rules
        self._penalty_rules = penalty_rules
        self._bonus_rules = bonus_rules
        self._strategies = strategies
        self._exemptions = exemption_definitions

        self._base_score = scoring_config.get("base_score", 100.0)
        self._max_penalty = scoring_config.get("max_penalty", 50.0)
        self._max_bonus = scoring_config.get("max_bonus", 30.0)
        self._grades = scoring_config.get("grades", {"A": 90, "B": 80, "C": 70, "D": 60, "F": 0})

    @classmethod
    def from_config(cls, config_path: str | Path) -> 'RuleEngine':
        """从 YAML 配置文件加载"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        return cls(
            scoring_config=config.get("scoring", {}),
            veto_rules=config.get("veto_rules", []),
            penalty_rules=config.get("penalty_rules", []),
            bonus_rules=config.get("bonus_rules", []),
            strategies=config.get("strategies", []),
            exemption_definitions=config.get("exemption_definitions", {}),
        )

    @classmethod
    def with_defaults(cls) -> 'RuleEngine':
        """使用默认规则创建"""
        return cls(
            scoring_config={"base_score": 100.0, "max_penalty": 50.0, "max_bonus": 30.0,
                           "grades": {"A": 90, "B": 80, "C": 70, "D": 60, "F": 0}},
            veto_rules=[],
            penalty_rules=[],
            bonus_rules=[],
            strategies=[],
            exemption_definitions={},
        )

    def evaluate(
        self,
        features: Dict[str, Any],
        metric_name: str = "",
        thresholds: Optional[Dict[str, float]] = None,
    ) -> RuleEngineResult:
        """执行规则评估

        Args:
            features: 特征字典 (log_slope, r_squared, cv, latest_value, etc.)
            metric_name: 指标名称 (roic, roe, etc.)
            thresholds: 阈值字典 (high_value, moat_min, etc.)

        Returns:
            RuleEngineResult: 评估结果
        """
        thresholds = thresholds or {}
        result = RuleEngineResult(
            base_score=self._base_score,
            final_score=self._base_score,
            grade="C",
        )

        # 1. 检查否决规则
        for rule in self._veto_rules:
            if not self._rule_applies(rule, metric_name):
                continue

            rule_result = self._check_veto_rule(rule, features, thresholds)
            if rule_result.triggered:
                # 检查豁免
                if rule_result.exempted:
                    result.veto_rules.append(rule_result)
                else:
                    result.vetoed = True
                    result.veto_reason = rule_result.message
                    result.veto_rules.append(rule_result)
                    result.final_score = 0
                    result.grade = "F"
                    return result

        # 2. 应用扣分规则
        total_penalty = 0.0
        for rule in self._penalty_rules:
            if not self._rule_applies(rule, metric_name):
                continue

            rule_result = self._check_penalty_rule(rule, features, thresholds)
            if rule_result.triggered:
                total_penalty += rule_result.score_delta
                result.penalty_rules.append(rule_result)

        # 限制最大扣分
        total_penalty = min(total_penalty, self._max_penalty)
        result.total_penalty = total_penalty

        # 3. 应用加分规则
        total_bonus = 0.0
        for rule in self._bonus_rules:
            if not self._rule_applies(rule, metric_name):
                continue

            rule_result = self._check_bonus_rule(rule, features, thresholds)
            if rule_result.triggered:
                total_bonus += rule_result.score_delta
                result.bonus_rules.append(rule_result)

        # 限制最大加分
        total_bonus = min(total_bonus, self._max_bonus)
        result.total_bonus = total_bonus

        # 4. 计算最终分数
        result.final_score = max(0, min(100, self._base_score - total_penalty + total_bonus))
        result.grade = self._score_to_grade(result.final_score)

        # 5. 识别投资策略
        for strategy in self._strategies:
            if not self._rule_applies(strategy, metric_name):
                continue

            matched, confidence = self._check_strategy(strategy, features, thresholds)
            if matched:
                result.strategies.append({
                    "id": strategy.get("id"),
                    "name": strategy.get("name"),
                    "description": strategy.get("description"),
                    "confidence": confidence,
                    "tags": strategy.get("tags", []),
                })

        return result

    def _rule_applies(self, rule: Dict, metric_name: str) -> bool:
        """检查规则是否适用于当前指标"""
        applies_to = rule.get("applies_to")
        if applies_to is None:
            return True  # 无限制，适用于所有指标
        return metric_name in applies_to

    def _check_condition(self, condition: Dict, features: Dict, thresholds: Dict) -> bool:
        """检查单个条件是否满足"""
        for key, expected in condition.items():
            # 解析条件键: field_operator (e.g., log_slope_lt, cv_gt)
            match = re.match(r'^(.+?)_(lt|lte|gt|gte|eq|ne|in)$', key)
            if match:
                field_name = match.group(1)
                operator = match.group(2)
                actual = features.get(field_name)

                if actual is None:
                    return False

                # 处理阈值引用 (e.g., "high_value")
                if isinstance(expected, str) and expected in thresholds:
                    expected = thresholds[expected]

                if not self._compare(actual, operator, expected):
                    return False

            # 特殊条件: field_gt_threshold (阈值比较)
            elif key.endswith("_gt_threshold"):
                field_name = key.replace("_gt_threshold", "")
                threshold_name = expected
                actual = features.get(field_name)
                threshold_value = thresholds.get(threshold_name, 0)
                if actual is None or actual <= threshold_value:
                    return False

            # 布尔条件
            elif key in features:
                if features[key] != expected:
                    return False

        return True

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        """比较操作"""
        if operator == "lt":
            return actual < expected
        elif operator == "lte":
            return actual <= expected
        elif operator == "gt":
            return actual > expected
        elif operator == "gte":
            return actual >= expected
        elif operator == "eq":
            return actual == expected
        elif operator == "ne":
            return actual != expected
        elif operator == "in":
            return actual in expected
        return False

    def _check_exemption(self, rule: Dict, features: Dict, thresholds: Dict) -> Tuple[bool, str]:
        """检查是否满足豁免条件"""
        exemptions = rule.get("exemptions", [])
        for exemption in exemptions:
            exemption_type = exemption.get("type")
            if exemption_type in self._exemptions:
                exemption_def = self._exemptions[exemption_type]
                conditions = exemption_def.get("conditions", [])
                all_met = all(
                    self._check_condition({k: v}, features, thresholds)
                    for cond in conditions
                    for k, v in (cond.items() if isinstance(cond, dict) else [(cond, True)])
                )
                if all_met:
                    return True, exemption_def.get("description", exemption_type)
        return False, ""

    def _check_veto_rule(self, rule: Dict, features: Dict, thresholds: Dict) -> RuleResult:
        """检查否决规则"""
        condition = rule.get("condition", {})
        triggered = self._check_condition(condition, features, thresholds)

        result = RuleResult(
            rule_id=rule.get("id", "unknown"),
            rule_name=rule.get("name", ""),
            triggered=triggered,
        )

        if triggered:
            # 检查豁免
            exempted, reason = self._check_exemption(rule, features, thresholds)
            result.exempted = exempted
            result.exemption_reason = reason

            # 格式化消息
            template = rule.get("message_template", rule.get("name", ""))
            try:
                result.message = template.format(**features)
            except (KeyError, ValueError):
                result.message = template

        return result

    def _check_penalty_rule(self, rule: Dict, features: Dict, thresholds: Dict) -> RuleResult:
        """检查扣分规则"""
        condition = rule.get("condition", {})
        triggered = self._check_condition(condition, features, thresholds)

        result = RuleResult(
            rule_id=rule.get("id", "unknown"),
            rule_name=rule.get("name", ""),
            triggered=triggered,
        )

        if triggered:
            # 计算扣分
            score = self._calculate_rule_score(rule, features)
            result.score_delta = score

            # 格式化消息
            template = rule.get("message_template", rule.get("name", ""))
            try:
                result.message = template.format(**features)
            except (KeyError, ValueError):
                result.message = template

        return result

    def _check_bonus_rule(self, rule: Dict, features: Dict, thresholds: Dict) -> RuleResult:
        """检查加分规则"""
        condition = rule.get("condition", {})
        triggered = self._check_condition(condition, features, thresholds)

        result = RuleResult(
            rule_id=rule.get("id", "unknown"),
            rule_name=rule.get("name", ""),
            triggered=triggered,
        )

        if triggered:
            # 计算加分
            score = self._calculate_rule_score(rule, features)
            result.score_delta = score

            # 格式化消息
            template = rule.get("message_template", rule.get("name", ""))
            try:
                result.message = template.format(**features)
            except (KeyError, ValueError):
                result.message = template

        return result

    def _calculate_rule_score(self, rule: Dict, features: Dict) -> float:
        """计算规则分数"""
        # 固定分数
        if "score" in rule:
            return float(rule["score"])

        # 公式计算
        if "score_formula" in rule:
            try:
                # 安全执行简单公式
                formula = rule["score_formula"]
                # 创建安全的执行环境
                safe_dict = {
                    "min": min, "max": max, "abs": abs,
                    **{k: v for k, v in features.items() if isinstance(v, (int, float))}
                }
                score = eval(formula, {"__builtins__": {}}, safe_dict)
                max_score = rule.get("max_score", float('inf'))
                return min(float(score), max_score)
            except Exception as e:
                logger.warning(f"Failed to evaluate formula '{formula}': {e}")
                return 0.0

        # 分数映射
        if "score_map" in rule:
            score_key = rule.get("score_key", "")
            value = features.get(score_key, "")
            return float(rule["score_map"].get(value, 0))

        # 分层分数
        if "score_tiers" in rule:
            for tier in rule["score_tiers"]:
                for key, threshold in tier.items():
                    if key.endswith("_gte"):
                        field = key.replace("_gte", "")
                        if features.get(field, 0) >= threshold:
                            return float(tier.get("score", 0))

        return 0.0

    def _check_strategy(self, strategy: Dict, features: Dict, thresholds: Dict) -> Tuple[bool, float]:
        """检查投资策略"""
        conditions = strategy.get("conditions", [])

        # 检查所有条件
        all_met = True
        for cond in conditions:
            if isinstance(cond, dict):
                if not self._check_condition(cond, features, thresholds):
                    all_met = False
                    break

        if not all_met:
            return False, 0.0

        # 计算置信度
        confidence = 0.5
        formula = strategy.get("confidence_formula")
        if formula:
            try:
                safe_dict = {
                    "min": min, "max": max, "abs": abs,
                    "threshold": thresholds.get(strategy.get("applies_to", [""])[0] + "_threshold", 10),
                    **{k: v for k, v in features.items() if isinstance(v, (int, float))}
                }
                confidence = eval(formula, {"__builtins__": {}}, safe_dict)
                confidence = max(0.0, min(1.0, float(confidence)))
            except Exception:
                confidence = 0.5

        return True, confidence

    def _score_to_grade(self, score: float) -> str:
        """分数转评级"""
        for grade, threshold in sorted(self._grades.items(), key=lambda x: -x[1]):
            if score >= threshold:
                return grade
        return "F"


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_ENGINE: Optional[RuleEngine] = None


def get_default_rule_engine() -> RuleEngine:
    """获取默认规则引擎（单例）"""
    global _DEFAULT_ENGINE
    if _DEFAULT_ENGINE is None:
        config_path = Path(__file__).parent / "config" / "rules.yaml"
        if config_path.exists():
            _DEFAULT_ENGINE = RuleEngine.from_config(config_path)
            logger.info(f"Loaded rule engine from {config_path}")
        else:
            _DEFAULT_ENGINE = RuleEngine.with_defaults()
            logger.warning("rules.yaml not found, using empty rule engine")
    return _DEFAULT_ENGINE


__all__ = [
    "RuleEngine",
    "RuleResult",
    "RuleEngineResult",
    "get_default_rule_engine",
]
