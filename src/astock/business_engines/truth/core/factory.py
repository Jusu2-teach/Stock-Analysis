"""T.R.U.T.H. 工厂模块 - 依赖注入实现

提供因子和求解器的可配置工厂，支持:
    1. 默认工厂: 返回全部内置因子/求解器
    2. 可配置工厂: 根据配置选择性启用
    3. 自定义工厂: 用户注入自定义实现 (便于测试)

使用方式:
    # 默认工厂
    factory = DefaultFactorFactory()
    factors = factory.create_factors(config)

    # 可配置工厂
    factory = ConfigurableFactorFactory(
        enabled_factors=[FactorId.ALPHA, FactorId.BETA, FactorId.GAMMA]
    )
    factors = factory.create_factors(config)

    # 自定义工厂 (测试用)
    factory = CustomFactorFactory(factors=[MockAlphaFactor()])
    factors = factory.create_factors(config)

版本: 3.3.0
日期: 2026-01-06
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Type

from ..domain import FactorId, SolverId
from ..config import TruthConfig
from .protocols import FactorProtocol, SolverProtocol


# ============================================================================
# 因子工厂
# ============================================================================

@dataclass
class DefaultFactorFactory:
    """默认因子工厂 - 返回所有内置因子"""

    def create_factors(self, config: TruthConfig) -> List[FactorProtocol]:
        """创建所有因子实例"""
        # 延迟导入避免循环依赖
        from .factors import (
            AlphaFactor,
            BetaFactor,
            GammaFactor,
            DeltaFraudFactor,
            DeltaDecayFactor,
            VerificationFactor,
        )

        return [
            AlphaFactor(),
            BetaFactor(),
            GammaFactor(),
            DeltaFraudFactor(),
            DeltaDecayFactor(),
            VerificationFactor(),
        ]


@dataclass
class ConfigurableFactorFactory:
    """可配置因子工厂 - 选择性启用因子

    Example:
        >>> factory = ConfigurableFactorFactory(
        ...     enabled_factors=[FactorId.ALPHA, FactorId.BETA]
        ... )
        >>> factors = factory.create_factors(config)  # 只返回 α 和 β
    """

    enabled_factors: Optional[List[FactorId]] = None
    """启用的因子列表 (None = 全部启用)"""

    def create_factors(self, config: TruthConfig) -> List[FactorProtocol]:
        """创建启用的因子实例"""
        from .factors import (
            AlphaFactor,
            BetaFactor,
            GammaFactor,
            DeltaFraudFactor,
            DeltaDecayFactor,
            VerificationFactor,
        )

        # 因子映射
        factor_map: Dict[FactorId, Type] = {
            FactorId.ALPHA: AlphaFactor,
            FactorId.BETA: BetaFactor,
            FactorId.GAMMA: GammaFactor,
            FactorId.DELTA_FRAUD: DeltaFraudFactor,
            FactorId.DELTA_DECAY: DeltaDecayFactor,
            FactorId.VERIFICATION: VerificationFactor,
        }

        # 如果未指定，返回全部
        if self.enabled_factors is None:
            return [cls() for cls in factor_map.values()]

        # 返回启用的因子
        return [
            factor_map[fid]()
            for fid in self.enabled_factors
            if fid in factor_map
        ]


@dataclass
class CustomFactorFactory:
    """自定义因子工厂 - 用于测试注入 mock 因子

    Example:
        >>> class MockAlphaFactor:
        ...     factor_id = FactorId.ALPHA
        ...     def evaluate(self, ts_code, probes, config):
        ...         return FactorResult(factor_id=self.factor_id, score=0.5, ...), []
        ...
        >>> factory = CustomFactorFactory(factors=[MockAlphaFactor()])
        >>> factors = factory.create_factors(config)
    """

    factors: List[FactorProtocol] = field(default_factory=list)
    """注入的因子实例列表"""

    def create_factors(self, config: TruthConfig) -> List[FactorProtocol]:
        """返回注入的因子"""
        return list(self.factors)


# ============================================================================
# 求解器工厂
# ============================================================================

@dataclass
class DefaultSolverFactory:
    """默认求解器工厂 - 返回所有内置求解器"""

    def create_solvers(self, config: TruthConfig) -> List[SolverProtocol]:
        """创建所有求解器实例"""
        from .solvers import (
            GravitySolver,
            VelocitySolver,
            StructureSolver,
        )

        return [
            GravitySolver(),
            VelocitySolver(),
            StructureSolver(),
        ]


@dataclass
class ConfigurableSolverFactory:
    """可配置求解器工厂 - 选择性启用求解器

    Example:
        >>> factory = ConfigurableSolverFactory(
        ...     enabled_solvers=[SolverId.GRAVITY, SolverId.VELOCITY]
        ... )
        >>> solvers = factory.create_solvers(config)  # 只返回 Gravity 和 Velocity
    """

    enabled_solvers: Optional[List[SolverId]] = None
    """启用的求解器列表 (None = 全部启用)"""

    def create_solvers(self, config: TruthConfig) -> List[SolverProtocol]:
        """创建启用的求解器实例"""
        from .solvers import (
            GravitySolver,
            VelocitySolver,
            StructureSolver,
        )

        # 求解器映射
        solver_map: Dict[SolverId, Type] = {
            SolverId.GRAVITY: GravitySolver,
            SolverId.VELOCITY: VelocitySolver,
            SolverId.STRUCTURE: StructureSolver,
        }

        # 如果未指定，返回全部
        if self.enabled_solvers is None:
            return [cls() for cls in solver_map.values()]

        # 返回启用的求解器
        return [
            solver_map[sid]()
            for sid in self.enabled_solvers
            if sid in solver_map
        ]


@dataclass
class CustomSolverFactory:
    """自定义求解器工厂 - 用于测试注入 mock 求解器

    Example:
        >>> class MockGravitySolver:
        ...     solver_id = SolverId.GRAVITY
        ...     def solve(self, ts_code, factors, config):
        ...         return SolverResult(solver_id=self.solver_id, score=0.8, ...), []
        ...
        >>> factory = CustomSolverFactory(solvers=[MockGravitySolver()])
        >>> solvers = factory.create_solvers(config)
    """

    solvers: List[SolverProtocol] = field(default_factory=list)
    """注入的求解器实例列表"""

    def create_solvers(self, config: TruthConfig) -> List[SolverProtocol]:
        """返回注入的求解器"""
        return list(self.solvers)


# ============================================================================
# 组合工厂 (Pipeline 使用)
# ============================================================================

@dataclass
class TruthComponentFactory:
    """组合工厂 - 同时提供因子和求解器

    这是 Pipeline 使用的主要入口点。

    Example:
        >>> factory = TruthComponentFactory()
        >>> factors = factory.create_factors(config)
        >>> solvers = factory.create_solvers(config)

        # 或者使用自定义工厂
        >>> factory = TruthComponentFactory(
        ...     factor_factory=ConfigurableFactorFactory(enabled_factors=[FactorId.ALPHA]),
        ...     solver_factory=DefaultSolverFactory(),
        ... )
    """

    factor_factory: Optional[DefaultFactorFactory | ConfigurableFactorFactory | CustomFactorFactory] = None
    solver_factory: Optional[DefaultSolverFactory | ConfigurableSolverFactory | CustomSolverFactory] = None

    def __post_init__(self):
        if self.factor_factory is None:
            self.factor_factory = DefaultFactorFactory()
        if self.solver_factory is None:
            self.solver_factory = DefaultSolverFactory()

    def create_factors(self, config: TruthConfig) -> List[FactorProtocol]:
        """创建因子实例列表"""
        return self.factor_factory.create_factors(config)

    def create_solvers(self, config: TruthConfig) -> List[SolverProtocol]:
        """创建求解器实例列表"""
        return self.solver_factory.create_solvers(config)


# ============================================================================
# 便捷函数
# ============================================================================

def create_default_factory() -> TruthComponentFactory:
    """创建默认工厂"""
    return TruthComponentFactory()


def create_test_factory(
    mock_factors: Optional[List[FactorProtocol]] = None,
    mock_solvers: Optional[List[SolverProtocol]] = None,
) -> TruthComponentFactory:
    """创建测试用工厂 - 注入 mock 实现

    Example:
        >>> factory = create_test_factory(
        ...     mock_factors=[MockAlphaFactor()],
        ...     mock_solvers=[MockGravitySolver()],
        ... )
    """
    factor_factory = CustomFactorFactory(factors=mock_factors or []) if mock_factors else DefaultFactorFactory()
    solver_factory = CustomSolverFactory(solvers=mock_solvers or []) if mock_solvers else DefaultSolverFactory()

    return TruthComponentFactory(
        factor_factory=factor_factory,
        solver_factory=solver_factory,
    )


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 因子工厂
    "DefaultFactorFactory",
    "ConfigurableFactorFactory",
    "CustomFactorFactory",
    # 求解器工厂
    "DefaultSolverFactory",
    "ConfigurableSolverFactory",
    "CustomSolverFactory",
    # 组合工厂
    "TruthComponentFactory",
    # 便捷函数
    "create_default_factory",
    "create_test_factory",
]
