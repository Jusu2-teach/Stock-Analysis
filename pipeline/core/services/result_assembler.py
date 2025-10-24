"""ResultAssembler: 封装输出注册与最终结果字典构建逻辑"""
from __future__ import annotations
from typing import Any, Dict, TYPE_CHECKING
if TYPE_CHECKING:  # 避免循环导入
    from pipeline.core.execute_manager import ExecuteManager

class ResultAssembler:
    def __init__(self, manager: 'ExecuteManager'):
        self.mgr = manager
        self.logger = manager.logger

    def register_catalog(self, catalog: Dict[str, Any]):
        for ds_name, obj in catalog.items():
            if '__' in ds_name:
                step, out = ds_name.split('__', 1)
                ref = f"steps.{step}.outputs.parameters.{out}"
                h = self.mgr._hash_reference(ref)
                if h not in self.mgr.global_registry:
                    self.mgr.reference_to_hash.setdefault(ref, h)
                    self.mgr.reference_values[ref] = obj
                    self.mgr.global_registry[h] = obj

    def assemble(self, raw: Dict[str, Any], started_at: str) -> Dict[str, Any]:
        raw.setdefault('mode', 'prefect')
        raw.setdefault('executed_steps', self.mgr.execution_order)
        raw['started_at'] = started_at
        from datetime import datetime
        raw['finished_at'] = datetime.now().isoformat()
        raw['outputs'] = {
            'by_reference': list(self.mgr.reference_values.keys()),
            'registry_size': len(self.mgr.global_registry)
        }
        # 附加: 缓存统计（如果可用）
        if hasattr(self.mgr, '_cache_stats_service'):
            try:
                stats = self.mgr._cache_stats_service.summary()
                if stats:
                    raw.setdefault('metrics', {})['cache'] = stats
            except Exception:
                pass
        # 附加: lineage & node_metrics (供外部分析)
        kedro_engine = None
        try:
            from pipeline.engines.prefect_engine import PrefectEngine  # lazy import
            # FlowExecutor 未保留 PrefectEngine 引用，这里只尝试在执行后 manager 上查找
        except Exception:
            pass
        for attr in ('kedro_engine',):
            if hasattr(self.mgr, attr):
                kedro_engine = getattr(self.mgr, attr)
        # PrefectEngine 情况下由其内部注入 (当前未直接暴露)；保留 future 接口
        if kedro_engine:
            try:
                raw.setdefault('lineage', kedro_engine.lineage)
                raw.setdefault('node_metrics', kedro_engine.node_metrics)
            except Exception:
                pass
        return raw

__all__ = ["ResultAssembler"]
