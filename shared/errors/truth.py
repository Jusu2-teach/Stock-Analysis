"""
T.R.U.T.H.相关异常
===================

定义六维基因计算、求解器执行相关的异常。

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

from typing import Optional

from .base import AStockError, ErrorContext, ErrorSeverity


class TruthError(AStockError):
    """T.R.U.T.H.相关异常基类"""

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message, severity, context, cause)


class GeneCalculationError(TruthError):
    """基因计算异常

    当六维基因计算失败时抛出。

    Examples:
        >>> raise GeneCalculationError(
        ...     "Alpha基因计算失败: 缺少周期性数据",
        ...     context=ErrorContext(
        ...         module="alpha_gene",
        ...         function="compute_alpha_from_probes",
        ...         ts_code="000001.SZ",
        ...         metadata={"gene_type": "alpha"}
        ...     )
        ... )
    """
    pass


class SolverExecutionError(TruthError):
    """求解器执行异常

    当三大求解器执行失败时抛出。

    Examples:
        >>> raise SolverExecutionError(
        ...     "GravitySolver执行失败: 基因值超出有效范围",
        ...     context=ErrorContext(
        ...         module="gravity_solver",
        ...         function="solve",
        ...         ts_code="000001.SZ",
        ...         metadata={
        ...             "solver_type": "gravity",
        ...             "alpha": 1.5,
        ...             "beta": 0.8
        ...         }
        ...     )
        ... )
    """
    pass
