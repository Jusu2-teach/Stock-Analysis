from __future__ import annotations
from typing import List, Protocol, Optional
from ..models import MethodRegistration
from ..errors import RegistryMethodNotFound, RegistryStrategyError
from ..utils_version import parse_version


class SelectionStrategy(Protocol):
    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration: ...


class DefaultStrategy:
    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration:
        if not candidates:
            raise RegistryMethodNotFound("no candidates")
        return max(candidates, key=lambda c: (c.priority, not c.deprecated, parse_version(c.version)))


class LatestVersionStrategy:
    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration:
        return max(candidates, key=lambda c: (parse_version(c.version), -int(c.deprecated)))


class StableStrategy:
    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration:
        stable = [c for c in candidates if not c.deprecated]
        base = stable if stable else candidates
        return max(base, key=lambda c: parse_version(c.version))


class HighestPriorityStrategy:
    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration:
        return max(candidates, key=lambda c: (c.priority, parse_version(c.version)))


class EngineOverrideStrategy:
    def __init__(self, engine_type: str):
        self.engine_type = engine_type

    def select(self, candidates: List[MethodRegistration]) -> MethodRegistration:
        for c in candidates:
            if c.engine_type == self.engine_type:
                return c
        raise RegistryMethodNotFound(f"engine {self.engine_type} not found among candidates")


def resolve_strategy(name: str = 'default', *, preferred_engine: Optional[str] = None):
    """解析选择策略

    Args:
        name: 策略名称
        preferred_engine: 优先引擎（仅当 name='engine_override' 时使用）

    Returns:
        SelectionStrategy: 策略实例
    """
    if name == 'default':
        return DefaultStrategy()
    if name == 'prefer_latest':
        return LatestVersionStrategy()
    if name == 'prefer_stable':
        return StableStrategy()
    if name == 'highest_priority':
        return HighestPriorityStrategy()
    if name == 'engine_override':
        if not preferred_engine:
            raise RegistryStrategyError('engine_override requires preferred_engine parameter')
        return EngineOverrideStrategy(preferred_engine)
    raise RegistryStrategyError(f'unknown strategy: {name}')
