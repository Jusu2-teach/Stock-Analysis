"""
Evaluators Protocols - 接口定义
================================

使用 typing.Protocol 定义规则、策略、引擎的接口约束
遵循依赖倒置原则 (DIP)，实现接口与实现的解耦

设计原则：
1. 接口隔离原则 (ISP) - 细粒度接口
2. 依赖倒置原则 (DIP) - 依赖抽象而非具体
3. 里氏替换原则 (LSP) - 任何实现 Protocol 的类都可替换
4. 鸭子类型 (Duck Typing) - 无需显式继承，实现接口即可

版本: 2.0.0
作者: AStock Analysis System
日期: 2026-01-10
"""

from __future__ import annotations

from typing import Protocol, Optional, List, Dict, Any, runtime_checkable
from .domain_models import TrendContext
from .rule_config import RuleConfig, RuleCategory


# ============================================================================
# 规则结果 (共享数据结构)
# ============================================================================

@runtime_checkable
class RuleResult(Protocol):
    """规则执行结果接口

    任何实现此 Protocol 的类都可作为规则结果使用
    """
    name: str                    # 规则名称
    kind: str                    # 结果类型: veto | penalty | bonus | info
    message: str                 # 结果消息
    value: float                 # 分值变化
    metadata: Dict[str, Any]     # 附加元数据


# ============================================================================
# 规则协议 (Rule Protocol)
# ============================================================================

@runtime_checkable
class RuleProtocol(Protocol):
    """规则接口协议

    所有规则类必须实现此 Protocol：
    - 属性: name, category, priority, enabled, description
    - 方法: execute(context, config)

    优势:
    1. 编译时类型检查 - mypy 可检查签名
    2. 无需继承 ABC - 鸭子类型更灵活
    3. 自动发现 - 可通过 Protocol 自动发现规则类

    Example:
        >>> class MyVetoRule:
        ...     name = "my_veto"
        ...     category = RuleCategory.VETO
        ...     priority = 100
        ...     enabled = True
        ...     description = "My custom veto rule"
        ...
        ...     def execute(self, context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
        ...         if context.quality.latest_value < 0:
        ...             return create_veto_result("negative_value", "Value is negative")
        ...         return None

        >>> # 类型检查通过
        >>> assert isinstance(MyVetoRule(), RuleProtocol)  # True
    """

    # 规则元数据
    name: str                    # 规则唯一标识
    category: RuleCategory       # 规则类别
    priority: int                # 执行优先级 (越小越先执行)
    enabled: bool                # 是否启用
    description: str             # 规则描述

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResult]:
        """执行规则

        Args:
            context: 趋势分析上下文
            config: 规则配置

        Returns:
            RuleResult: 规则执行结果，None 表示规则不触发
        """
        ...


# ============================================================================
# 策略协议 (Strategy Protocol)
# ============================================================================

@runtime_checkable
class StrategyProtocol(Protocol):
    """投资策略接口协议

    所有策略类必须实现此 Protocol

    Example:
        >>> class HighGrowthStrategy:
        ...     name = "high_growth"
        ...     description = "高成长策略"
        ...
        ...     def evaluate(self, context: TrendContext) -> StrategyResult:
        ...         if context.cagr_approx > 0.20:
        ...             return StrategyResult(self.name, True, "高速成长")
        ...         return StrategyResult(self.name, False)
    """

    # 策略元数据
    name: str                    # 策略唯一标识
    description: str             # 策略描述

    def evaluate(
        self,
        context: TrendContext
    ) -> "StrategyResult":
        """评估策略是否匹配

        Args:
            context: 趋势分析上下文

        Returns:
            StrategyResult: 策略评估结果
        """
        ...


# ============================================================================
# 引擎协议 (Engine Protocol)
# ============================================================================

@runtime_checkable
class RuleEngineProtocol(Protocol):
    """规则引擎接口协议

    规则引擎负责编排规则执行流程

    Example:
        >>> class MyRuleEngine:
        ...     def __init__(self, rules: List[RuleProtocol]):
        ...         self.rules = rules
        ...
        ...     def evaluate(self, context: TrendContext, config: RuleConfig) -> EvaluationResult:
        ...         # 执行规则链
        ...         ...
    """

    def evaluate(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> "EvaluationResult":
        """执行规则链评估

        Args:
            context: 趋势分析上下文
            config: 规则配置

        Returns:
            EvaluationResult: 完整评估结果
        """
        ...


@runtime_checkable
class StrategyEngineProtocol(Protocol):
    """策略引擎接口协议

    策略引擎负责策略匹配和推荐
    """

    def match_strategies(
        self,
        context: TrendContext
    ) -> List["StrategyResult"]:
        """匹配所有策略

        Args:
            context: 趋势分析上下文

        Returns:
            List[StrategyResult]: 匹配的策略列表
        """
        ...


# ============================================================================
# 结果协议 (Result Protocols)
# ============================================================================

@runtime_checkable
class StrategyResult(Protocol):
    """策略评估结果接口"""
    name: str                    # 策略名称
    matched: bool                # 是否匹配
    reason: str                  # 匹配原因
    score_boost: float           # 额外加分
    confidence: float            # 匹配置信度
    recommendations: List[str]   # 投资建议


@runtime_checkable
class EvaluationResult(Protocol):
    """评估结果接口"""
    passes: bool                 # 是否通过
    score: float                 # 最终得分
    grade: str                   # 评级
    elimination_reason: str      # 淘汰原因
    penalty: float               # 总扣分
    penalty_details: List[str]   # 扣分明细
    bonus_details: List[str]     # 加分明细
    auxiliary_notes: List[str]   # 辅助说明
    strategies: List[str]        # 命中策略
    strategy_reasons: List[str]  # 策略原因


# ============================================================================
# 工厂协议 (Factory Protocol)
# ============================================================================

@runtime_checkable
class RuleFactoryProtocol(Protocol):
    """规则工厂接口协议

    负责创建和管理规则实例

    Example:
        >>> class RuleFactory:
        ...     def create_veto_rules(self) -> List[RuleProtocol]:
        ...         return [MinLatestValueVetoRule(), SevereTrendDeclineVetoRule()]
        ...
        ...     def create_all_rules(self) -> List[RuleProtocol]:
        ...         return self.create_veto_rules() + self.create_penalty_rules() + ...
    """

    def create_veto_rules(self) -> List[RuleProtocol]:
        """创建所有否决规则"""
        ...

    def create_penalty_rules(self) -> List[RuleProtocol]:
        """创建所有扣分规则"""
        ...

    def create_bonus_rules(self) -> List[RuleProtocol]:
        """创建所有加分规则"""
        ...

    def create_validation_rules(self) -> List[RuleProtocol]:
        """创建所有验证规则"""
        ...

    def create_all_rules(self) -> List[RuleProtocol]:
        """创建所有规则"""
        ...


@runtime_checkable
class StrategyFactoryProtocol(Protocol):
    """策略工厂接口协议"""

    def create_all_strategies(self) -> List[StrategyProtocol]:
        """创建所有策略"""
        ...


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 规则相关
    'RuleResult',
    'RuleProtocol',
    'RuleEngineProtocol',
    'RuleFactoryProtocol',
    # 策略相关
    'StrategyProtocol',
    'StrategyResult',
    'StrategyEngineProtocol',
    'StrategyFactoryProtocol',
    # 结果相关
    'EvaluationResult',
]
