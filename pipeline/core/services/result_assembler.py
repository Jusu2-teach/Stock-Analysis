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
        raw.setdefault('mode', 'prefect')
        raw.setdefault('executed_steps', self.ctx.execution_order)
        raw['started_at'] = started_at

        # 如果 FlowRun 已存在，则优先使用其中的 finished_at，否则使用当前时间
        flow_run = self.ctx.get_flow_run()
        if flow_run and flow_run.finished_at:
            raw['finished_at'] = flow_run.finished_at
        else:
            from datetime import datetime as _dt
            raw['finished_at'] = _dt.now().isoformat()

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

        # 附加: 运行时视图 (flow_run / step_runs)
        try:
            runtime_view: Dict[str, Any] = {}
            if flow_run:
                runtime_view['flow'] = {
                    'run_id': flow_run.run_id,
                    'status': flow_run.status.name,
                    'started_at': flow_run.started_at,
                    'finished_at': flow_run.finished_at,
                    'duration_ms': flow_run.duration_ms,
                    'step_order': flow_run.step_order,
                    'error': flow_run.error,
                    'metrics': flow_run.metrics,
                }
            step_runs = self.ctx.get_step_runs()
            if step_runs:
                runtime_view['steps'] = {
                    name: {
                        'status': sr.status.name,
                        'started_at': sr.started_at,
                        'finished_at': sr.finished_at,
                        'duration_ms': sr.duration_ms,
                        'attempts': sr.attempts,
                        'cached': sr.cached,
                        'error': sr.error,
                        'metadata': sr.metadata,
                    }
                    for name, sr in step_runs.items()
                }
            if runtime_view:
                raw['runtime'] = runtime_view
        except Exception as e:  # 运行视图附加失败不应影响主流程
            self.logger.debug(f"附加运行时视图失败（已忽略）: {e}")

        return raw

__all__ = ["ResultAssembler"]
