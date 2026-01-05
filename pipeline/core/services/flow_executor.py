"""FlowExecutor: 负责 Prefect Hybrid 流执行与结果组装

从 ExecuteManager._run_prefect 拆分，隔离执行流程（构建 -> 运行 -> 收集结果）。

重构为依赖 PipelineContext，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict
from datetime import datetime
import logging

from ..context import PipelineContext
from ..runtime_models import RunStatus
from ..execution_backend import PrefectBackend, ExecutionBackend


class FlowExecutor:
    """Flow 执行服务（解耦版本）

    协调 PrefectEngine 执行，但依赖 Context 而非 Manager。
    需要传入其他服务实例来完成完整流程。
    """

    __slots__ = ('ctx', 'result_assembler', 'cache_stats_service', 'logger', 'backend')

    def __init__(
        self,
        context: PipelineContext,
        result_assembler: Any,  # ResultAssembler 实例
        cache_stats_service: Any = None,  # CacheStatsService 实例（可选）
        logger: logging.Logger | None = None,
        backend: ExecutionBackend | None = None,
    ):
        self.ctx = context
        self.result_assembler = result_assembler
        self.cache_stats_service = cache_stats_service
        self.logger = logger or logging.getLogger(__name__)
        self.backend: ExecutionBackend = backend or PrefectBackend(self.logger)

    def _emit_hook(self, event: str, data: Any = None):
        """安全触发 Hook 事件（非关键功能，忽略错误但记录日志）"""
        try:
            from pipeline.core.services.hook_manager import HookManager
            HookManager.get().emit(event, data)
        except Exception as e:
            # Hook 失败不应影响主流程，仅记录 debug 日志
            self.logger.debug(f"Hook '{event}' 触发失败（已忽略）: {e}")

    def run(self, auto_info: Dict[str, Any], manager: Any) -> Dict[str, Any]:
        """运行 Hybrid Flow（Prefect + Kedro）

        Args:
            auto_info: 自动构建的节点配置（保留参数，供后续扩展）
            manager: ExecuteManager 实例（传递给 PrefectEngine，避免循环依赖）

        Returns:
            执行结果字典
        """
        started_dt = datetime.now()
        started = started_dt.isoformat()

        # 初始化 FlowRun 运行视图
        flow_run = self.ctx.start_flow_run(run_id=self.ctx.get_runtime_value('flow_run_id'))

        try:
            # 使用抽象执行后端运行工作流
            self._emit_hook('before_flow', {'started_at': started})
            res, kedro_engine = self.backend.run(self.ctx, auto_info, manager)

            # 注册 catalog 输出
            if kedro_engine is not None and hasattr(kedro_engine, 'global_catalog'):
                self.result_assembler.register_catalog(kedro_engine.global_catalog)

            # 获取缓存统计（传递 kedro_engine）
            cache_stats = None
            if self.cache_stats_service:
                try:
                    cache_stats = self.cache_stats_service.summary(kedro_engine)
                except Exception as e:
                    # 缓存统计是可选功能，失败不影响主流程
                    self.logger.debug(f"缓存统计获取失败（已忽略）: {e}")

            # 组装结果
            assembled = self.result_assembler.assemble(
                res,
                started,
                kedro_engine=kedro_engine,
                cache_stats=cache_stats
            )

            # 根据后端返回的状态更新 FlowRun / StepRun
            try:
                overall_status = res.get('overall_status') or res.get('status') or 'completed'
                if isinstance(overall_status, str):
                    key = overall_status.lower()
                    if key in {'failed', 'error'}:
                        flow_status = RunStatus.FAILED
                    else:
                        flow_status = RunStatus.SUCCESS
                else:
                    flow_status = RunStatus.SUCCESS

                # 从结果中提取节点/任务级状态，填充 StepRun 视图
                step_collections = []
                if isinstance(res, dict):
                    if 'node_results' in res and isinstance(res['node_results'], dict):
                        step_collections.append(('node', res['node_results']))
                    if 'task_results' in res and isinstance(res['task_results'], dict):
                        step_collections.append(('task', res['task_results']))

                for kind, collection in step_collections:
                    for name, info in collection.items():
                        sr = self.ctx.get_or_create_step_run(name)
                        status = str(info.get('status', 'completed')).lower()
                        if status in {'completed', 'success', 'succeeded'}:
                            sr.status = RunStatus.SUCCESS
                        elif status in {'failed', 'error'}:
                            sr.status = RunStatus.FAILED
                        elif status in {'skipped'}:
                            sr.status = RunStatus.SKIPPED
                        else:
                            sr.status = RunStatus.RUNNING
                        if 'cached' in info:
                            sr.cached = bool(info.get('cached'))
                        if 'error' in info:
                            sr.error = str(info.get('error'))
                        # 将 kind / soft_fail 等附加到 metadata
                        meta = sr.metadata
                        meta['kind'] = kind
                        if 'soft_fail' in info:
                            meta['soft_fail'] = bool(info.get('soft_fail'))
                        if 'reason' in info:
                            meta['reason'] = info.get('reason')

                # 标记 FlowRun 状态
                self.ctx.finish_flow_run(flow_status)
            except Exception as e:  # 运行视图更新失败不影响主流程
                self.logger.debug(f"更新运行视图失败（已忽略）: {e}")
            self._emit_hook('after_flow', assembled)

            return assembled

        except Exception as e:
            self.logger.error(f"Flow execution failed: {e}", exc_info=True)
            # 标记 FlowRun 失败
            self.ctx.finish_flow_run(RunStatus.FAILED, error=str(e))
            return {
                'status': 'failed',
                'mode': 'prefect',
                'error': str(e),
                'executed_steps': self.ctx.execution_order,
                'started_at': started,
                'finished_at': datetime.now().isoformat()
            }


__all__ = ["FlowExecutor"]
