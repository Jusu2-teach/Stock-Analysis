"""ResultAssembler: 封装输出注册与最终结果字典构建逻辑

v3.0 重构 (2025-12-27)：
- 使用 DataStore.refs() 获取引用列表
- 移除对旧字典的直接访问
"""
from __future__ import annotations
from typing import Any, Dict
import logging

from ..context import PipelineContext


class ResultAssembler:
    """结果组装服务

    v3.0: 统一使用 DataStore 进行数据访问
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
                # 使用统一的注册 API
                self.ctx.register_reference(ref, obj)

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

        # v3.0: 使用 DataStore API 获取统计信息
        raw['outputs'] = {
            'by_reference': list(self.ctx.data_store.refs()),
            'registry_size': len(self.ctx.data_store),
        }

        # 附加: 缓存统计（如果可用）
        if cache_stats:
            raw.setdefault('metrics', {})['cache'] = cache_stats

        # 附加: lineage & node_metrics (供外部分析)
        if kedro_engine:
            try:
                raw.setdefault('lineage', kedro_engine.lineage)
                raw.setdefault('node_metrics', kedro_engine.node_metrics)
            except AttributeError:
                # kedro_engine 可能没有这些属性，静默忽略
                pass

        return raw

__all__ = ["ResultAssembler"]
