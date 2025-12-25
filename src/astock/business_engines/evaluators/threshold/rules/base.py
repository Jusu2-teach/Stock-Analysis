"""
规则基础模块 (Rule Base)
=========================

提供规则函数的基础类型和工具函数。
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable

# 从 analyzers/trend 导入数据模型 (统一使用 TrendContext)
from ....analyzers.trend.models import TrendContext

# 从 rule_config 导入配置
from ..rule_config import RuleConfig, DEFAULT_CONFIG, RuleCategory

logger = logging.getLogger(__name__)


@dataclass
class RuleResult:
    """
    规则执行结果

    Attributes:
        name: 规则名称
        kind: 结果类型 (veto/penalty/bonus/info)
        message: 结果消息
        value: 分值变化 (扣分为正，加分为正)
        log_level: 日志级别
        log_prefix: 日志前缀
        metadata: 附加元数据
    """
    name: str
    kind: str  # veto | penalty | bonus | info
    message: str
    value: float = 0.0
    log_level: int = logging.DEBUG
    log_prefix: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def veto(cls, name: str, message: str, **metadata) -> "RuleResult":
        """创建否决结果"""
        return cls(
            name=name,
            kind="veto",
            message=message,
            value=0.0,
            log_level=logging.INFO,
            log_prefix="【一票否决】",
            metadata=metadata,
        )

    @classmethod
    def penalty(cls, name: str, message: str, value: float, **metadata) -> "RuleResult":
        """创建扣分结果"""
        return cls(
            name=name,
            kind="penalty",
            message=message,
            value=abs(value),
            log_level=logging.DEBUG,
            metadata=metadata,
        )

    @classmethod
    def bonus(cls, name: str, message: str, value: float, **metadata) -> "RuleResult":
        """创建加分结果"""
        return cls(
            name=name,
            kind="bonus",
            message=message,
            value=abs(value),
            log_level=logging.DEBUG,
            metadata=metadata,
        )

    @classmethod
    def info(cls, name: str, message: str, **metadata) -> "RuleResult":
        """创建信息结果 (不影响分数)"""
        return cls(
            name=name,
            kind="info",
            message=message,
            value=0.0,
            log_level=logging.DEBUG,
            metadata=metadata,
        )


# 规则函数签名
RuleFunc = Callable[[TrendContext, RuleConfig], Optional[RuleResult]]


@dataclass
class Rule:
    """
    规则定义

    Attributes:
        name: 规则唯一名称
        category: 规则分类
        func: 规则函数
        description: 规则描述
        enabled: 是否启用
        priority: 优先级 (数字越小越先执行)
    """
    name: str
    category: RuleCategory
    func: RuleFunc
    description: str = ""
    enabled: bool = True
    priority: int = 100

    def execute(self, context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
        """执行规则"""
        if not self.enabled:
            return None
        try:
            return self.func(context, config)
        except Exception as e:
            logger.warning(f"规则 {self.name} 执行异常: {e}")
            return None


# ============================================================================
# 工具函数
# ============================================================================

def is_roiic_metric(context: TrendContext) -> bool:
    """判断是否为 ROIIC 指标"""
    return context.metric_name.lower() == "roiic"


def get_reference_metric(context: TrendContext, metric: str) -> Optional[Dict[str, float]]:
    """获取参考指标数据"""
    metrics = context.reference_metrics or {}
    return metrics.get(metric.lower())


def is_cyclical_exemption(context: TrendContext) -> bool:
    """
    判断是否应用周期底部豁免

    周期股在谷底或回升期，应放宽否决条件
    """
    if not context.is_cyclical:
        return False
    return context.current_phase in ("trough", "recovery", "rising")


def is_turnaround_exemption(context: TrendContext) -> bool:
    """
    判断是否应用困境反转豁免

    V型反转或强势加速的情况下，应放宽条件
    """
    # V型反转
    if context.inflection_type == "deterioration_to_recovery":
        return True
    # 强势加速
    if context.is_accelerating and context.trend_acceleration > 0.1:
        return True
    return False


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    'RuleResult',
    'Rule',
    'RuleFunc',
    'TrendContext',
    'RuleConfig',
    'DEFAULT_CONFIG',
    'RuleCategory',
    'is_roiic_metric',
    'get_reference_metric',
    'is_cyclical_exemption',
    'is_turnaround_exemption',
    'logger',
]
