"""Pipeline Config - Reference Resolver
=======================================

数据引用解析器。

版本: 2.0.0
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Set

from ..core.dag import DataReference

logger = logging.getLogger(__name__)


class ReferenceResolver:
    """引用解析器

    解析 YAML 中的数据引用表达式，将其转换为实际值。

    Example:
        resolver = ReferenceResolver()
        resolver.set_output("load_data", "raw_data", df)

        resolved = resolver.resolve("steps.load_data.outputs.parameters.raw_data")
        # resolved == df
    """

    def __init__(self):
        self._outputs: Dict[str, Dict[str, Any]] = {}

    def set_output(self, task_name: str, output_name: str, value: Any) -> None:
        """设置任务输出"""
        if task_name not in self._outputs:
            self._outputs[task_name] = {}
        self._outputs[task_name][output_name] = value

    def set_outputs(self, task_name: str, outputs: Dict[str, Any]) -> None:
        """批量设置任务输出"""
        self._outputs[task_name] = outputs

    def get_output(self, task_name: str, output_name: str) -> Optional[Any]:
        """获取任务输出"""
        task_outputs = self._outputs.get(task_name, {})
        return task_outputs.get(output_name)

    def resolve(self, expr: str) -> Optional[Any]:
        """解析引用表达式

        Args:
            expr: 引用表达式 (如 "steps.load_data.outputs.parameters.raw_data")

        Returns:
            解析后的值，如果无法解析返回 None
        """
        ref = DataReference.parse(expr)
        if ref is None:
            return None

        return self.get_output(ref.source_task, ref.output_name)

    def resolve_params(
        self,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """解析参数字典中的所有引用

        递归处理嵌套结构。
        """
        return self._resolve_value(params)

    def _resolve_value(self, value: Any) -> Any:
        """递归解析值中的引用"""
        if isinstance(value, str):
            resolved = self.resolve(value)
            if resolved is not None:
                return resolved
            return value
        elif isinstance(value, dict):
            return {k: self._resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_value(v) for v in value]
        return value

    def get_dependencies(self, params: Dict[str, Any]) -> Set[str]:
        """从参数中提取依赖的任务名称"""
        deps = set()
        self._extract_deps(params, deps)
        return deps

    def _extract_deps(self, value: Any, deps: Set[str]) -> None:
        """递归提取依赖"""
        if isinstance(value, str):
            ref = DataReference.parse(value)
            if ref:
                deps.add(ref.source_task)
        elif isinstance(value, dict):
            for v in value.values():
                self._extract_deps(v, deps)
        elif isinstance(value, list):
            for v in value:
                self._extract_deps(v, deps)

    def clear(self) -> None:
        """清空所有输出"""
        self._outputs.clear()

    def list_outputs(self) -> Dict[str, Dict[str, str]]:
        """列出所有可用输出"""
        return {
            task: {name: type(val).__name__ for name, val in outputs.items()}
            for task, outputs in self._outputs.items()
        }
