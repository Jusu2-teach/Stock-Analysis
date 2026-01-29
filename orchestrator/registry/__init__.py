# namespace package for registry internals

from .registry import Registry
from .index import RegistryIndex
from .strategies import resolve_strategy, parse_version
from .discovery import ModuleLoader, Scanner
from .executor import MethodExecutor
from .metrics import MetricsService


def get_registry() -> Registry:
    """Compatibility helper for external callers (e.g. pipeline CLI)."""
    registry = Registry.get()
    if not registry.index.by_full_key:
        registry.auto_load(hot_reload=False)
    return registry

__all__ = [
    'Registry',
    'get_registry',
    'RegistryIndex',
    'resolve_strategy',
    'parse_version',
    'ModuleLoader',
    'Scanner',
    'MethodExecutor',
    'MetricsService',
]
