"""ResultAssembler: 封装输出注册与最终结果字典构建逻辑

重构为依赖 PipelineContext，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict
import logging
import hashlib

from ..context import PipelineContext


class ResultAssembler:
    """结果组装服务（解耦版本）

    通过 PipelineContext 访问共享状态，而非直接依赖 ExecuteManager。
    """

    __slots__ = ('ctx', 'logger')

    def __init__(self, context: PipelineContext, logger: logging.Logger | None = None):
        self.ctx = context
        self.logger = logger or logging.getLogger(__name__)

    def register_catalog(self, catalog: Dict[str, Any]):
        """注册 catalog 输出到上下文

        Args:
            catalog: Kedro catalog 输出字典（key=dataset_name, value=数据对象）
        """
        for ds_name, obj in catalog.items():
            if '__' in ds_name:
                step, out = ds_name.split('__', 1)
                ref = f"steps.{step}.outputs.parameters.{out}"
                h = self._hash_reference(ref)
                if h not in self.ctx.global_registry:
                    self.ctx.reference_to_hash.setdefault(ref, h)
                    self.ctx.reference_values[ref] = obj
                    self.ctx.global_registry[h] = obj

    @staticmethod
    def _hash_reference(ref: str) -> str:
        """生成引用的哈希值"""
        return hashlib.sha256(ref.encode()).hexdigest()[:16]

    def assemble(
        self,
        raw: Dict[str, Any],
        started_at: str,
        kedro_engine: Any = None,
        cache_stats: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """组装执行结果

        Args:
            raw: 原始执行结果
            started_at: 开始时间
            kedro_engine: Kedro 引擎实例（可选，用于获取 lineage/metrics）
            cache_stats: 缓存统计（可选）

        Returns:
            完整的执行结果字典
        """
        from datetime import datetime

        raw.setdefault('mode', 'prefect')
        raw.setdefault('executed_steps', self.ctx.execution_order)
        raw['started_at'] = started_at
        raw['finished_at'] = datetime.now().isoformat()
        raw['outputs'] = {
            'by_reference': list(self.ctx.reference_values.keys()),
            'registry_size': len(self.ctx.global_registry)
        }

        # 附加: 缓存统计（如果可用）
        if cache_stats:
            raw.setdefault('metrics', {})['cache'] = cache_stats

        # 附加: lineage & node_metrics (供外部分析)
        if kedro_engine:
            try:
                raw.setdefault('lineage', kedro_engine.lineage)
                raw.setdefault('node_metrics', kedro_engine.node_metrics)
            except Exception:
                pass

        return raw

__all__ = ["ResultAssembler"]
