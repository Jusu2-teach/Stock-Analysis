"""RuntimeParamService: 负责运行时参数引用解析

将 ExecuteManager._resolve_runtime_params 拆分为独立服务，便于单元测试与职责单一化。

v3.0 重构 (2025-12-27)：
- 使用 PipelineContext.resolver (ReferenceResolver) 进行统一解析
- 移除对 global_registry/reference_values 的直接访问
"""
from __future__ import annotations
from typing import Any, Dict
import logging

from ..context import PipelineContext


class RuntimeParamService:
    """运行时参数解析服务

    v3.0: 统一使用 ReferenceResolver 进行引用解析
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

        v3.0: 统一使用 ReferenceResolver，支持递归解析。

        Args:
            params: 包含引用的参数字典

        Returns:
            解析后的参数字典

        Raises:
            ReferenceResolutionError: 引用解析失败
        """
        try:
            # 使用 PipelineContext 的 resolver 进行统一解析
            # strict=True 会在引用不存在时抛出 ReferenceNotFoundError
            return self.ctx.resolve_references(params)
        except Exception as e:
            # 转换为自定义异常，保持 API 兼容性
            raise self.ReferenceResolutionError(
                f"参数引用解析失败: {e} -> 请确认上游 step 输出名称与引用一致 "
                f"(pattern: steps.<step>.outputs.parameters.<output>)"
            ) from e

__all__ = ["RuntimeParamService"]
