from __future__ import annotations
import inspect
from typing import Optional, List
from ..models import MethodRegistration

def register_method(
    component_type: str,
    engine_type: str,
    engine_name: Optional[str] = None,
    version: str = "1.0.0",
    priority: int = 0,
    deprecated: bool = False,
    tags: Optional[List[str]] = None,
    description: str = ""
):
    """
    Decorator to register a method with the Orchestrator Registry.
    """
    def decorator(func):
        # Lazy import to avoid circular dependency
        from ..registry.registry import Registry

        nonlocal engine_name, description
        if engine_name is None:
            engine_name = func.__name__
        if not description:
            description = func.__doc__ or ""

        module_path = func.__module__
        try:
            sig = str(inspect.signature(func))
        except ValueError:
            sig = "()"

        reg = MethodRegistration(
            component_type=component_type,
            engine_type=engine_type,
            engine_name=engine_name,
            description=description.strip(),
            version=version,
            callable=func,
            tags=tuple(tags or []),
            deprecated=deprecated,
            priority=priority,
            signature=sig,
            module_path=module_path
        )

        Registry.get().register(reg)
        return func
    return decorator
