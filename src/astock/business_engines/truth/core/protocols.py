"""T.R.U.T.H. 协议定义 - 结构化子类型 (鸭子类型)

使用 typing.Protocol 替代 ABC，实现更 Pythonic 的接口定义。

优势:
    1. 结构化子类型: 只要实现了协议方法就是有效实现，无需显式继承
    2. 更好的类型检查: 支持 mypy 静态类型检查
    3. 运行时检查: 支持 isinstance() 检查 (需要 @runtime_checkable)
    4. 组合优于继承: 更灵活的类型组合

版本: 3.3.0
日期: 2026-01-06
"""

from __future__ import annotations

from typing import (
    Dict,
    List,
    Mapping,
    Protocol,
    Sequence,
    Tuple,
    runtime_checkable,
)

from ..domain import (
    FactorId,
    FactorResult,
    ProbeInput,
    SolverId,
    SolverResult,
    TruthWarning,
)
from ..config import TruthConfig


# ============================================================================
# 因子协议
# ============================================================================

@runtime_checkable
class FactorProtocol(Protocol):
    """因子计算协议

    任何实现了 factor_id 属性和 evaluate 方法的类都是有效因子。

    Example:
        >>> class MyCustomFactor:
        ...     factor_id = FactorId.ALPHA
        ...     def evaluate(self, ts_code, probes, config):
        ...         return FactorResult(...), []
        ...     def explain(self, result):
        ...         return "My explanation"
        >>> isinstance(MyCustomFactor(), FactorProtocol)
        True
    """

    @property
    def factor_id(self) -> FactorId:
        """因子标识"""
        ...

    def evaluate(
        self,
        ts_code: str,
        probes: Sequence[ProbeInput],
        config: TruthConfig,
    ) -> Tuple[FactorResult, List[TruthWarning]]:
        """计算因子

        Args:
            ts_code: 股票代码
            probes: 探针输入列表
            config: 配置

        Returns:
            (FactorResult, warnings) 元组
        """
        ...

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本

        Args:
            result: 因子计算结果

        Returns:
            解释文本
        """
        ...


# ============================================================================
# 求解器协议
# ============================================================================

@runtime_checkable
class SolverProtocol(Protocol):
    """求解器协议

    任何实现了 solver_id 属性和 solve 方法的类都是有效求解器。
    """

    @property
    def solver_id(self) -> SolverId:
        """求解器标识"""
        ...

    def solve(
        self,
        ts_code: str,
        factors: Mapping[FactorId, FactorResult],
        config: TruthConfig,
    ) -> Tuple[SolverResult, List[TruthWarning]]:
        """求解动态阈值

        Args:
            ts_code: 股票代码
            factors: 因子计算结果映射
            config: 配置

        Returns:
            (SolverResult, warnings) 元组
        """
        ...

    def explain(self, result: SolverResult) -> str:
        """生成人类可读的解释文本

        Args:
            result: 求解器结果

        Returns:
            解释文本
        """
        ...


# ============================================================================
# 工厂协议 (依赖注入)
# ============================================================================

@runtime_checkable
class FactorFactoryProtocol(Protocol):
    """因子工厂协议 - 用于依赖注入"""

    def create_factors(self, config: TruthConfig) -> List[FactorProtocol]:
        """创建因子实例列表"""
        ...


@runtime_checkable
class SolverFactoryProtocol(Protocol):
    """求解器工厂协议 - 用于依赖注入"""

    def create_solvers(self, config: TruthConfig) -> List[SolverProtocol]:
        """创建求解器实例列表"""
        ...


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    "FactorProtocol",
    "SolverProtocol",
    "FactorFactoryProtocol",
    "SolverFactoryProtocol",
]
