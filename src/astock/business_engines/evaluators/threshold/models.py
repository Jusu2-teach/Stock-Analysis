"""
数据模型 (Models)
==================

评估系统的数据模型定义。

重构说明:
- 精简了原有的重复模型
- 统一使用 TrendContext 作为上下文
- 保持向后兼容

作者: AStock Analysis System
日期: 2025-12-19
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# 从 rule_config 导入枚举
from .rule_config import RuleCategory


# ============================================================================
# 策略匹配结果
# ============================================================================

@dataclass
class StrategyMatchResult:
    """
    策略匹配结果

    Attributes:
        name: 策略名称
        matched: 是否匹配
        reason: 匹配原因
        score_boost: 额外加分
        confidence: 匹配置信度 (0-1)
        recommendations: 投资建议
    """
    name: str
    matched: bool = False
    reason: str = ""
    score_boost: float = 0.0
    confidence: float = 0.0
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "matched": self.matched,
            "reason": self.reason,
            "score_boost": self.score_boost,
            "confidence": self.confidence,
            "recommendations": self.recommendations,
        }


# ============================================================================
# 向后兼容导出
# ============================================================================

# 这些类型从其他地方导入，保持向后兼容
from .rules.base import RuleResult, Rule
from .engine import RuleOutcome, EvaluationResult

# 旧模型的别名 (向后兼容)
ThresholdRule = Rule
ThresholdEvaluationResult = EvaluationResult


__all__ = [
    'RuleCategory',
    'RuleResult',
    'Rule',
    'StrategyMatchResult',
    'RuleOutcome',
    'EvaluationResult',
    # 向后兼容别名
    'ThresholdRule',
    'ThresholdEvaluationResult',
]
