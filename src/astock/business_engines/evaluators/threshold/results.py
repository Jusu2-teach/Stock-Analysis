"""
Evaluators Results - 结果数据类
================================

实现 Protocol 定义的结果接口

设计原则：
1. 不可变性 - frozen dataclass 保证线程安全
2. Builder 模式 - 提供便捷的创建方法
3. 序列化支持 - to_dict() 方法

版本: 2.0.0
作者: AStock Analysis System
日期: 2026-01-10
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from enum import Enum


# ============================================================================
# 规则结果
# ============================================================================

@dataclass(frozen=True)
class RuleResultImpl:
    """规则执行结果实现

    实现 RuleResult Protocol
    """
    name: str                                   # 规则名称
    kind: str                                   # 结果类型
    message: str                                # 结果消息
    value: float = 0.0                          # 分值变化
    metadata: Dict[str, Any] = field(default_factory=dict)  # 附加元数据

    @classmethod
    def veto(cls, name: str, message: str, **metadata) -> "RuleResultImpl":
        """创建否决结果"""
        return cls(
            name=name,
            kind="veto",
            message=message,
            value=0.0,
            metadata=metadata,
        )

    @classmethod
    def penalty(cls, name: str, message: str, value: float, **metadata) -> "RuleResultImpl":
        """创建扣分结果"""
        return cls(
            name=name,
            kind="penalty",
            message=message,
            value=abs(value),
            metadata=metadata,
        )

    @classmethod
    def bonus(cls, name: str, message: str, value: float, **metadata) -> "RuleResultImpl":
        """创建加分结果"""
        return cls(
            name=name,
            kind="bonus",
            message=message,
            value=abs(value),
            metadata=metadata,
        )

    @classmethod
    def info(cls, name: str, message: str, **metadata) -> "RuleResultImpl":
        """创建信息结果 (不影响分数)"""
        return cls(
            name=name,
            kind="info",
            message=message,
            value=0.0,
            metadata=metadata,
        )

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return asdict(self)


# ============================================================================
# 策略结果
# ============================================================================

@dataclass(frozen=True)
class StrategyResultImpl:
    """策略评估结果实现

    实现 StrategyResult Protocol
    """
    name: str                                   # 策略名称
    matched: bool                               # 是否匹配
    reason: str = ""                            # 匹配原因
    score_boost: float = 0.0                    # 额外加分
    confidence: float = 0.0                     # 匹配置信度 (0-1)
    recommendations: List[str] = field(default_factory=list)  # 投资建议
    metadata: Dict[str, Any] = field(default_factory=dict)    # 附加元数据

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            "name": self.name,
            "matched": self.matched,
            "reason": self.reason,
            "score_boost": round(self.score_boost, 1),
            "confidence": round(self.confidence, 2),
            "recommendations": self.recommendations,
            "metadata": self.metadata,
        }


# ============================================================================
# 评估结果
# ============================================================================

class EvaluationGrade(str, Enum):
    """评级枚举"""
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    F = "F"

    @classmethod
    def from_score(cls, score: float) -> "EvaluationGrade":
        """根据分数计算评级"""
        if score >= 90:
            return cls.A
        elif score >= 80:
            return cls.B
        elif score >= 70:
            return cls.C
        elif score >= 60:
            return cls.D
        else:
            return cls.F


@dataclass
class EvaluationResultImpl:
    """评估结果实现

    实现 EvaluationResult Protocol

    注意: 非 frozen，支持动态计算 grade
    """
    passes: bool = True                         # 是否通过
    score: float = 100.0                        # 最终得分
    grade: str = "B"                            # 评级
    elimination_reason: str = ""                # 淘汰原因
    penalty: float = 0.0                        # 总扣分
    penalty_details: List[str] = field(default_factory=list)  # 扣分明细
    bonus_details: List[str] = field(default_factory=list)    # 加分明细
    auxiliary_notes: List[str] = field(default_factory=list)  # 辅助说明
    strategies: List[str] = field(default_factory=list)       # 命中策略
    strategy_reasons: List[str] = field(default_factory=list) # 策略原因

    def compute_grade(self) -> str:
        """根据分数计算等级"""
        return EvaluationGrade.from_score(self.score).value

    def update_grade(self) -> None:
        """更新评级"""
        self.grade = self.compute_grade()

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
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
# 规则链执行结果 (内部使用)
# ============================================================================

@dataclass
class RuleChainOutcome:
    """规则链执行结果 (内部使用)

    在引擎内部使用，记录规则链执行的中间状态
    """
    passes: bool = True
    elimination_reason: str = ""
    penalty: float = 0.0
    penalty_details: List[str] = field(default_factory=list)
    bonus_details: List[str] = field(default_factory=list)
    auxiliary_notes: List[str] = field(default_factory=list)

    def apply_veto(self, reason: str) -> None:
        """应用否决"""
        self.passes = False
        self.elimination_reason = reason

    def apply_penalty(self, value: float, detail: str) -> None:
        """应用扣分"""
        self.penalty += value
        self.penalty_details.append(detail)

    def apply_bonus(self, detail: str) -> None:
        """应用加分"""
        self.bonus_details.append(detail)

    def add_note(self, note: str) -> None:
        """添加辅助说明"""
        self.auxiliary_notes.append(note)

    def to_evaluation_result(self, score: float) -> EvaluationResultImpl:
        """转换为评估结果"""
        result = EvaluationResultImpl(
            passes=self.passes,
            score=score,
            elimination_reason=self.elimination_reason,
            penalty=self.penalty,
            penalty_details=self.penalty_details,
            bonus_details=self.bonus_details,
            auxiliary_notes=self.auxiliary_notes,
        )
        result.update_grade()
        return result


# ============================================================================
# Builder 模式 - 便捷创建方法
# ============================================================================

def create_veto_result(name: str, message: str, **metadata) -> RuleResultImpl:
    """创建否决结果"""
    return RuleResultImpl.veto(name, message, **metadata)


def create_penalty_result(name: str, message: str, value: float, **metadata) -> RuleResultImpl:
    """创建扣分结果"""
    return RuleResultImpl.penalty(name, message, value, **metadata)


def create_bonus_result(name: str, message: str, value: float, **metadata) -> RuleResultImpl:
    """创建加分结果"""
    return RuleResultImpl.bonus(name, message, value, **metadata)


def create_info_result(name: str, message: str, **metadata) -> RuleResultImpl:
    """创建信息结果"""
    return RuleResultImpl.info(name, message, **metadata)


def create_strategy_result(
    name: str,
    matched: bool,
    reason: str = "",
    score_boost: float = 0.0,
    confidence: float = 0.0,
    recommendations: Optional[List[str]] = None,
    **metadata
) -> StrategyResultImpl:
    """创建策略结果"""
    return StrategyResultImpl(
        name=name,
        matched=matched,
        reason=reason,
        score_boost=score_boost,
        confidence=confidence,
        recommendations=recommendations or [],
        metadata=metadata,
    )


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 结果实现类
    'RuleResultImpl',
    'StrategyResultImpl',
    'EvaluationResultImpl',
    'RuleChainOutcome',
    'EvaluationGrade',
    # Builder 函数
    'create_veto_result',
    'create_penalty_result',
    'create_bonus_result',
    'create_info_result',
    'create_strategy_result',
]
