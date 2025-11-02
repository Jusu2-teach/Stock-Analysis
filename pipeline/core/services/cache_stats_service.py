"""CacheStatsService: 汇总缓存相关统计信息

来源: KedroEngine
- node_metrics: 包含每个 step 的 duration_sec, cached 标志
- node_signatures: 当前签名
- dataset_fingerprints: 数据指纹数量

提供:
- summary(): 总缓存命中次数、未命中次数、命中率、总执行耗时（不含缓存跳过）、节点数量
- enrich_result(result_dict): 将统计写入结果

重构为无状态服务，不依赖 Manager。
"""
from __future__ import annotations
from typing import Dict, Any, Optional
import logging


class CacheStatsService:
    """缓存统计服务（无状态版本）

    不持有任何状态，所有方法接受 kedro_engine 参数。
    """

    __slots__ = ('logger',)

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def summary(self, kedro_engine: Any = None) -> Dict[str, Any]:
        """计算缓存统计摘要

        Args:
            kedro_engine: KedroEngine 实例（可选）

        Returns:
            缓存统计字典，如果无法获取则返回空字典
        """
        if kedro_engine is None:
            return {}

        metrics = getattr(kedro_engine, 'node_metrics', {}) or {}
        if not metrics:
            return {}

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

    def enrich_result(self, result: Dict[str, Any], kedro_engine: Any = None) -> Dict[str, Any]:
        """将缓存统计添加到结果字典

        Args:
            result: 执行结果字典
            kedro_engine: KedroEngine 实例（可选）

        Returns:
            enriched 结果字典
        """
        stat = self.summary(kedro_engine)
        if stat:
            result.setdefault('metrics', {})['cache'] = stat
        return result

__all__ = ["CacheStatsService"]
