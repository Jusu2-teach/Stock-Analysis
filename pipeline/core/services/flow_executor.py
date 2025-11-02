"""FlowExecutor: 负责 Prefect Hybrid 流执行与结果组装

从 ExecuteManager._run_prefect 拆分，隔离执行流程（构建 -> 运行 -> 收集结果）。

重构为依赖 PipelineContext，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict
from datetime import datetime
import logging

from ..context import PipelineContext


class FlowExecutor:
    """Flow 执行服务（解耦版本）

    协调 PrefectEngine 执行，但依赖 Context 而非 Manager。
    需要传入其他服务实例来完成完整流程。
    """

    __slots__ = ('ctx', 'result_assembler', 'cache_stats_service', 'logger')

    def __init__(
        self,
        context: PipelineContext,
        result_assembler: Any,  # ResultAssembler 实例
        cache_stats_service: Any = None,  # CacheStatsService 实例（可选）
        logger: logging.Logger | None = None
    ):
        self.ctx = context
        self.result_assembler = result_assembler
        self.cache_stats_service = cache_stats_service
        self.logger = logger or logging.getLogger(__name__)

    def _emit_hook(self, event: str, data: Any = None):
        """安全触发 Hook 事件（忽略错误）"""
        try:
            from pipeline.core.services.hook_manager import HookManager
            HookManager.get().emit(event, data)
        except Exception:
            pass

    def run(self, auto_info: Dict[str, Any], manager: Any) -> Dict[str, Any]:
        """运行 Hybrid Flow（Prefect + Kedro）

        Args:
            auto_info: 自动构建的节点配置（保留参数，供后续扩展）
            manager: ExecuteManager 实例（传递给 PrefectEngine，避免循环依赖）

        Returns:
            执行结果字典
        """
        started = datetime.now().isoformat()

        try:
            from pipeline.engines.prefect_engine import PrefectEngine

            # PrefectEngine 仍需要 manager（因为它要协调 orchestrator 等）
            prefect_engine = PrefectEngine(manager)

            # 构建并执行 Flow
            flow = prefect_engine.build_hybrid_flow(self.ctx.config)
            self._emit_hook('before_flow', {'started_at': started})

            flow_result = flow()
            res = flow_result.result() if hasattr(flow_result, 'result') else flow_result

            # 注册 catalog 输出
            kedro_engine = None
            if hasattr(prefect_engine, 'kedro_engine') and prefect_engine.kedro_engine:
                kedro_engine = prefect_engine.kedro_engine
                self.result_assembler.register_catalog(kedro_engine.global_catalog)

            # 获取缓存统计（传递 kedro_engine）
            cache_stats = None
            if self.cache_stats_service:
                try:
                    cache_stats = self.cache_stats_service.summary(kedro_engine)
                except Exception:
                    pass

            # 组装结果
            assembled = self.result_assembler.assemble(
                res,
                started,
                kedro_engine=kedro_engine,
                cache_stats=cache_stats
            )
            self._emit_hook('after_flow', assembled)

            return assembled

        except Exception as e:
            self.logger.error(f"Flow execution failed: {e}", exc_info=True)
            return {
                'status': 'failed',
                'mode': 'prefect',
                'error': str(e),
                'executed_steps': self.ctx.execution_order,
                'started_at': started,
                'finished_at': datetime.now().isoformat()
            }


__all__ = ["FlowExecutor"]
