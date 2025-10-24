"""RuntimeParamService: 负责运行时参数引用解析

将 ExecuteManager._resolve_runtime_params 拆分为独立服务，便于单元测试与职责单一化。
"""
from __future__ import annotations
from typing import Any, Dict, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pipeline.core.execute_manager import ExecuteManager


class RuntimeParamService:
    class ReferenceResolutionError(Exception):
        pass

    def __init__(self, manager: 'ExecuteManager'):
        self.mgr = manager
        self.logger = manager.logger

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        def walk(v: Any):
            if isinstance(v, dict) and '__ref__' in v:
                ref = v['__ref__']
                h = v.get('hash')
                if h in self.mgr.global_registry:
                    return self.mgr.global_registry[h]
                if ref in self.mgr.reference_values:  # 冗余映射
                    return self.mgr.reference_values[ref]
                raise self.ReferenceResolutionError(
                    f"参数引用未找到: {ref} -> 请确认上游 step 输出名称与引用一致 (pattern: steps.<step>.outputs.parameters.<output>)"
                )
            if isinstance(v, list):
                return [walk(x) for x in v]
            if isinstance(v, dict):
                # 保留字典结构，但忽略内部 __ref__ 元素的键值对已经处理过
                return {k: walk(val) for k, val in v.items() if k != '__ref__'}
            return v
        return {k: walk(v) for k, v in params.items()}

__all__ = ["RuntimeParamService"]
