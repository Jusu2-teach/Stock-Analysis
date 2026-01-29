"""
探针相关异常
==============

定义探针执行、验证、配置相关的异常。

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

from typing import Optional

from .base import AStockError, ErrorContext, ErrorSeverity


class ProbeError(AStockError):
    """探针相关异常基类"""

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message, severity, context, cause)


class ProbeExecutionError(ProbeError):
    """探针执行异常

    当探针计算过程中发生错误时抛出。

    Examples:
        >>> raise ProbeExecutionError(
        ...     "LogTrendProbe计算失败: 矩阵奇异",
        ...     context=ErrorContext(
        ...         module="log_trend_probe",
        ...         function="compute",
        ...         ts_code="000001.SZ",
        ...         metric_name="roic",
        ...         metadata={"probe_name": "LogTrendProbe"}
        ...     )
        ... )
    """
    pass


class ProbeValidationError(ProbeError):
    """探针验证异常

    当输入数据不满足探针要求时抛出。

    Examples:
        >>> raise ProbeValidationError(
        ...     "数据点数量不足，需要至少3个数据点",
        ...     severity=ErrorSeverity.WARNING,
        ...     context=ErrorContext(
        ...         module="cyclical_probe",
        ...         function="validate",
        ...         ts_code="000001.SZ",
        ...         metric_name="roic",
        ...         metadata={"data_points": 2, "required": 3}
        ...     )
        ... )
    """
    pass


class ProbeConfigurationError(ProbeError):
    """探针配置异常

    当探针配置参数不合法时抛出。

    Examples:
        >>> raise ProbeConfigurationError(
        ...     "window_size必须为正整数",
        ...     context=ErrorContext(
        ...         module="rolling_probe",
        ...         function="__init__",
        ...         metadata={"window_size": -5}
        ...     )
        ... )
    """
    pass
