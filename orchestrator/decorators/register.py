from __future__ import annotations
import inspect
from typing import Callable, Optional, List
from ..models import MethodRegistration
from ..registry.registry import Registry


def register_method(*, engine_name: str, component_type: str, engine_type: str,
                    description: str = "", version: str = "1.0.0", tags: Optional[List[str]] = None,
                    deprecated: bool = False, priority: int = 0):
    """方法注册装饰器

    Args:
        engine_name: 方法名（用户调用）
        component_type: 组件类型（datahub/data_engine/business_engine等）
        engine_type: 引擎类型（pandas/polars/duckdb等）
        description: 方法描述
        version: 语义版本号
        tags: 标签列表
        deprecated: 是否弃用
        priority: 优先级（越大越优先）
    """
    def deco(func: Callable) -> Callable:
        sig = "( *args, **kwargs )"
        try:
            sig = str(inspect.signature(func))
        except (ValueError, TypeError):
            # 某些内建函数或特殊对象无法获取签名
            pass

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
