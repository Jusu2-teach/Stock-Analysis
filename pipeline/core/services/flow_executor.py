"""FlowExecutor: 负责 Prefect Hybrid 流执行与结果组装

从 ExecuteManager._run_prefect 拆分，隔离执行流程（构建 -> 运行 -> 收集结果）。
"""
from __future__ import annotations
from typing import Any, Dict, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:  # pragma: no cover
    from pipeline.core.execute_manager import ExecuteManager


class FlowExecutor:
    def __init__(self, manager: 'ExecuteManager'):
        self.mgr = manager
        self.logger = manager.logger

    def run(self, auto_info: Dict[str, Any]) -> Dict[str, Any]:  # auto_info 暂未直接使用，为后续扩展保留
        started = datetime.now().isoformat()
        try:
            from pipeline.engines.prefect_engine import PrefectEngine
            prefect_engine = PrefectEngine(self.mgr)
            # 挂载引用，便于结果阶段访问 lineage / metrics / cache stats
            try:
                if getattr(prefect_engine, 'kedro_engine', None):
                    setattr(self.mgr, 'kedro_engine', prefect_engine.kedro_engine)
            except Exception:
                pass
            flow = prefect_engine.build_hybrid_flow(self.mgr.config)
            # flow 级 hook(before)
            try:
                from pipeline.core.services.hook_manager import HookManager
                HookManager.get().emit('before_flow', {'started_at': started})
            except Exception:
                pass
            flow_result = flow()
            if hasattr(flow_result, 'result'):
                res = flow_result.result()
            else:
                res = flow_result
            if getattr(prefect_engine, 'kedro_engine', None):
                self.mgr._result_assembler.register_catalog(prefect_engine.kedro_engine.global_catalog)
            assembled = self.mgr._result_assembler.assemble(res, started)
            try:
                from pipeline.core.services.hook_manager import HookManager
                HookManager.get().emit('after_flow', assembled)
            except Exception:
                pass
            return assembled
        except Exception as e:  # pragma: no cover - 错误路径
            from datetime import datetime as _dt
            return {
                'status': 'failed',
                'mode': 'prefect',
                'error': str(e),
                'executed_steps': self.mgr.execution_order,
                'started_at': started,
                'finished_at': _dt.now().isoformat()
            }

__all__ = ["FlowExecutor"]
