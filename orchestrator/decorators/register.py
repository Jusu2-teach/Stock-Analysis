from __future__ import annotations
import inspect
from typing import Callable, Optional, List
from ..models import MethodRegistration
from ..registry.registry import Registry


def register_method(*, engine_name: str, component_type: str, engine_type: str,
                    description: str = "", version: str = "1.0.0", tags: Optional[List[str]] = None,
                    deprecated: bool = False, priority: int = 0):
    """新的方法注册装饰器（不支持 lazy 以简化首轮重构）"""
    def deco(func: Callable) -> Callable:
        sig = ""
        try:
            sig = str(inspect.signature(func))
        except Exception:
            sig = "( *args, **kwargs )"
        reg = MethodRegistration(
            component_type=component_type,
            engine_type=engine_type,
            engine_name=engine_name,
            description=description,
            version=version,
            callable=func,
            tags=tuple(tags or []),
            deprecated=deprecated,
            priority=priority,
            signature=sig,
            module_path=func.__module__,
        )
        Registry.get().register(reg)
        return func
    return deco
