from __future__ import annotations
from typing import Dict, Any
from time import perf_counter


class MetricsService:
    def __init__(self):
        self.stats: Dict[str, Dict[str, Any]] = {}

    def ensure(self, full_key: str, engine_name: str):
        self.stats.setdefault(engine_name, {
            'success': 0, 'error': 0, 'total_calls': 0,
            'success_calls': 0, 'failed_calls': 0, 'total_time': 0.0,
            'avg_time': 0.0, 'last_error': None, 'last_duration': 0.0,
            'full_keys': set()
        })['full_keys'].add(full_key)

    def wrap_execute(self, full_key: str, engine_name: str, func, *args, **kwargs):
        self.ensure(full_key, engine_name)
        stat = self.stats[engine_name]
        start = perf_counter()
        try:
            result = func(*args, **kwargs)
            dt = perf_counter() - start
            stat['success'] += 1
            stat['success_calls'] += 1
            stat['total_calls'] += 1
            stat['total_time'] += dt
            stat['last_duration'] = dt
            stat['avg_time'] = stat['total_time'] / stat['total_calls']
            return result, dt, None
        except Exception as e:  # rethrow after metrics
            dt = perf_counter() - start
            stat['error'] += 1
            stat['failed_calls'] += 1
            stat['total_calls'] += 1
            stat['total_time'] += dt
            stat['last_duration'] = dt
            stat['last_error'] = str(e)
            stat['avg_time'] = stat['total_time'] / stat['total_calls']
            return None, dt, e

    def export(self) -> Dict[str, Any]:
        total_calls = sum(s['total_calls'] for s in self.stats.values())
        success_calls = sum(s['success_calls'] for s in self.stats.values())
        return {
            'execution_stats': self.stats,
            'total_calls': total_calls,
            'success_rate': (success_calls / total_calls * 100) if total_calls else 0
        }
