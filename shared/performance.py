"""
性能监控装饰器 (Performance Monitoring Decorators)
==================================================

提供细粒度的性能追踪能力，用于：
- 探针执行时间统计
- 方法调用计数
- 异常监控
- 性能瓶颈识别

设计原则:
- 零侵入：通过装饰器透明添加
- 统计驱动：聚合统计信息
- EventBus集成：性能事件发布

版本: 1.0.0
日期: 2026-01-17
"""

import time
import logging
import functools
from typing import Callable, Optional, Dict, Any
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock

logger = logging.getLogger(__name__)

# 全局统计存储
_performance_stats: Dict[str, 'PerformanceStats'] = {}
_stats_lock = Lock()


@dataclass
class PerformanceStats:
    """性能统计数据"""
    name: str
    total_calls: int = 0
    total_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    error_count: int = 0
    last_duration: float = 0.0

    @property
    def avg_time(self) -> float:
        """平均执行时间"""
        return self.total_time / self.total_calls if self.total_calls > 0 else 0.0

    def record(self, duration: float, has_error: bool = False):
        """记录一次执行"""
        self.total_calls += 1
        self.total_time += duration
        self.last_duration = duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)
        if has_error:
            self.error_count += 1

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'total_calls': self.total_calls,
            'total_time_ms': self.total_time * 1000,
            'avg_time_ms': self.avg_time * 1000,
            'min_time_ms': self.min_time * 1000 if self.min_time != float('inf') else 0.0,
            'max_time_ms': self.max_time * 1000,
            'error_count': self.error_count,
            'error_rate': self.error_count / self.total_calls if self.total_calls > 0 else 0.0,
        }


def probe_timing(
    name: Optional[str] = None,
    log_threshold_ms: float = 100.0,
    enable_logging: bool = True,
    publish_event: bool = False,
) -> Callable:
    """探针执行时间统计装饰器

    Args:
        name: 统计名称（默认使用函数名）
        log_threshold_ms: 超过此阈值时记录警告日志（毫秒）
        enable_logging: 是否启用日志记录
        publish_event: 是否发布性能事件到EventBus

    Example:
        ```python
        @probe_timing(name="LogTrendProbe", log_threshold_ms=50.0)
        def analyze_trend(data):
            ...
        ```
    """
    def decorator(func: Callable) -> Callable:
        stats_name = name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            has_error = False

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                has_error = True
                raise
            finally:
                end_time = time.perf_counter()
                duration = end_time - start_time
                duration_ms = duration * 1000

                # 更新统计
                with _stats_lock:
                    if stats_name not in _performance_stats:
                        _performance_stats[stats_name] = PerformanceStats(name=stats_name)
                    _performance_stats[stats_name].record(duration, has_error)

                # 日志记录
                if enable_logging:
                    if has_error:
                        logger.error(
                            f"[PERF] {stats_name} FAILED after {duration_ms:.2f}ms"
                        )
                    elif duration_ms > log_threshold_ms:
                        logger.warning(
                            f"[PERF] {stats_name} took {duration_ms:.2f}ms "
                            f"(threshold: {log_threshold_ms}ms)"
                        )
                    else:
                        logger.debug(
                            f"[PERF] {stats_name} took {duration_ms:.2f}ms"
                        )

                # 发布事件（可选）
                if publish_event:
                    try:
                        from shared.event_bus import EventBus, EventType
                        EventBus.publish(
                            EventType.CUSTOM,
                            data={
                                'name': stats_name,
                                'duration_ms': duration_ms,
                                'has_error': has_error,
                            },
                            metadata={'type': 'performance_metric'}
                        )
                    except ImportError:
                        pass  # EventBus 不可用时静默失败

        return wrapper
    return decorator


def method_timing(
    log_threshold_ms: float = 200.0,
    enable_logging: bool = True,
) -> Callable:
    """通用方法执行时间统计装饰器

    适用于非探针的普通业务方法。

    Args:
        log_threshold_ms: 超过此阈值时记录警告日志（毫秒）
        enable_logging: 是否启用日志记录

    Example:
        ```python
        @method_timing(log_threshold_ms=100.0)
        def calculate_truth_genes(data):
            ...
        ```
    """
    return probe_timing(
        name=None,
        log_threshold_ms=log_threshold_ms,
        enable_logging=enable_logging,
        publish_event=False,
    )


def get_performance_stats(reset: bool = False) -> Dict[str, Dict[str, Any]]:
    """获取所有性能统计

    Args:
        reset: 是否在获取后重置统计

    Returns:
        性能统计字典
    """
    with _stats_lock:
        stats = {name: s.to_dict() for name, s in _performance_stats.items()}
        if reset:
            _performance_stats.clear()
        return stats


def print_performance_report(top_n: int = 20, sort_by: str = 'total_time'):
    """打印性能报告

    Args:
        top_n: 显示前N个最慢的方法
        sort_by: 排序字段（total_time, avg_time, total_calls, error_count）
    """
    stats = get_performance_stats(reset=False)

    if not stats:
        print("No performance data available.")
        return

    # 转换为列表并排序
    stats_list = list(stats.values())
    sort_key = {
        'total_time': lambda x: x['total_time_ms'],
        'avg_time': lambda x: x['avg_time_ms'],
        'total_calls': lambda x: x['total_calls'],
        'error_count': lambda x: x['error_count'],
    }.get(sort_by, lambda x: x['total_time_ms'])

    stats_list.sort(key=sort_key, reverse=True)

    # 打印报告
    print("\n" + "=" * 100)
    print(f"Performance Report (Top {top_n} by {sort_by})")
    print("=" * 100)
    print(f"{'Method':<50} {'Calls':>8} {'Total(ms)':>12} {'Avg(ms)':>10} {'Min(ms)':>10} {'Max(ms)':>10} {'Errors':>8}")
    print("-" * 100)

    for stat in stats_list[:top_n]:
        print(
            f"{stat['name']:<50} "
            f"{stat['total_calls']:>8} "
            f"{stat['total_time_ms']:>12.2f} "
            f"{stat['avg_time_ms']:>10.2f} "
            f"{stat['min_time_ms']:>10.2f} "
            f"{stat['max_time_ms']:>10.2f} "
            f"{stat['error_count']:>8}"
        )

    print("=" * 100 + "\n")


def reset_performance_stats():
    """重置所有性能统计"""
    with _stats_lock:
        _performance_stats.clear()
    logger.info("Performance statistics reset.")


# 导出的API
__all__ = [
    'probe_timing',
    'method_timing',
    'get_performance_stats',
    'print_performance_report',
    'reset_performance_stats',
    'PerformanceStats',
]
