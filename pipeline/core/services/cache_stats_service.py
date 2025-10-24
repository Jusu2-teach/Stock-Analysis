"""CacheStatsService: 汇总缓存相关统计信息

来源: KedroEngine
- node_metrics: 包含每个 step 的 duration_sec, cached 标志
- node_signatures: 当前签名
- dataset_fingerprints: 数据指纹数量

提供:
- summary(): 总缓存命中次数、未命中次数、命中率、总执行耗时（不含缓存跳过）、节点数量
- enrich_result(result_dict): 将统计写入结果
"""
from __future__ import annotations
from typing import Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pipeline.core.execute_manager import ExecuteManager

class CacheStatsService:
    def __init__(self, manager: 'ExecuteManager'):
        self.mgr = manager
        self.logger = manager.logger

    def summary(self) -> Dict[str, Any]:
        kedro_engine = getattr(self._maybe_prefect_kedro(), 'kedro_engine', None)
        if kedro_engine is None:
            kedro_engine = getattr(self.mgr, 'kedro_engine', None)  # fallback
        if kedro_engine is None:
            return {}
        metrics = getattr(kedro_engine, 'node_metrics', {}) or {}
        cached = sum(1 for m in metrics.values() if m.get('cached'))
        total = len(metrics)
        durations = [m.get('duration_sec', 0.0) for m in metrics.values() if not m.get('cached')]
        total_exec_time = round(sum(durations), 4)
        hit_rate = round(cached / total, 4) if total else None
        return {
            'node_total': total,
            'cache_hits': cached,
            'cache_miss': total - cached if total else 0,
            'cache_hit_rate': hit_rate,
            'total_execution_time_sec': total_exec_time,
            'dataset_fingerprint_count': len(getattr(kedro_engine, 'dataset_fingerprints', {}) or {}),
        }

    def enrich_result(self, result: Dict[str, Any]):
        stat = self.summary()
        if stat:
            result.setdefault('metrics', {})['cache'] = stat
        return result

    # 尝试从 FlowExecutor 暴露的 PrefectEngine 获取 kedro_engine
    def _maybe_prefect_kedro(self):
        # FlowExecutor 在运行期间构建 PrefectEngine，但运行后不一定保留引用；此处尽力探测
        # 可在后续版本把 PrefectEngine 引用挂到 ExecuteManager 以便统计
        return None

__all__ = ["CacheStatsService"]
