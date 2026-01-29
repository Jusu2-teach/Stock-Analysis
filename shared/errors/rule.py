"""
规则相关异常
==============

定义规则引擎执行、违背相关的异常。

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

from typing import Optional

from .base import AStockError, ErrorContext, ErrorSeverity


class RuleError(AStockError):
    """规则相关异常基类"""

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message, severity, context, cause)


class RuleExecutionError(RuleError):
    """规则执行异常

    当规则执行过程中发生错误时抛出。

    Examples:
        >>> raise RuleExecutionError(
        ...     "MinLatestValueVetoRule执行失败",
        ...     context=ErrorContext(
        ...         module="veto_rules",
        ...         function="execute",
        ...         ts_code="000001.SZ",
        ...         metric_name="roic",
        ...         metadata={"rule_name": "MinLatestValueVetoRule"}
        ...     )
        ... )
    """
    pass


class RuleViolationError(RuleError):
    """规则违背异常

    当业务规则被触发时抛出（通常是一票否决规则）。

    这是一个业务异常，表示数据不符合投资标准，而非系统错误。

    Examples:
        >>> raise RuleViolationError(
        ...     "触发一票否决: 最新ROIC值过低",
        ...     severity=ErrorSeverity.WARNING,
        ...     context=ErrorContext(
        ...         module="rule_engine",
        ...         function="evaluate",
        ...         ts_code="000001.SZ",
        ...         metric_name="roic",
        ...         metadata={
        ...             "rule_name": "MinLatestValueVeto",
        ...             "latest_value": 0.02,
        ...             "threshold": 0.05
        ...         }
        ...     )
        ... )
    """
    pass
