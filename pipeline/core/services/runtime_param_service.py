"""RuntimeParamService: 负责运行时参数引用解析

将 ExecuteManager._resolve_runtime_params 拆分为独立服务，便于单元测试与职责单一化。

重构为依赖 PipelineContext，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict
import logging

from ..context import PipelineContext


class RuntimeParamService:
    """运行时参数解析服务（解耦版本）

    通过 PipelineContext 访问共享状态，而非直接依赖 ExecuteManager。
    """

    __slots__ = ('ctx', 'logger')

    class ReferenceResolutionError(Exception):
        """引用解析失败异常"""
        pass

    def __init__(self, context: PipelineContext, logger: logging.Logger | None = None):
        self.ctx = context
        self.logger = logger or logging.getLogger(__name__)

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """解析运行时参数中的引用

        Args:
            params: 包含引用的参数字典

        Returns:
            解析后的参数字典

        Raises:
            ReferenceResolutionError: 引用解析失败
        """
        def walk(v: Any):
            if isinstance(v, dict) and '__ref__' in v:
                ref = v['__ref__']
                h = v.get('hash')
                # 优先通过哈希查找（更快）
                if h and h in self.ctx.global_registry:
                    return self.ctx.global_registry[h]
                # 降级到引用名查找
                if ref in self.ctx.reference_values:
                    return self.ctx.reference_values[ref]
                raise self.ReferenceResolutionError(
                    f"参数引用未找到: {ref} -> 请确认上游 step 输出名称与引用一致 "
                    f"(pattern: steps.<step>.outputs.parameters.<output>)"
                )
            if isinstance(v, list):
                return [walk(x) for x in v]
            if isinstance(v, dict):
                # 递归处理字典，但排除 __ref__ 键
                return {k: walk(val) for k, val in v.items() if k != '__ref__'}
            return v

        return {k: walk(v) for k, v in params.items()}

__all__ = ["RuntimeParamService"]
