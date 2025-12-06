"""HookManager: 事件钩子系统 (Phase: Enterprise Observability Foundation)

提供最小可用钩子:
 - before_node(step_name, context)
 - after_node(step_name, result, metrics)
 - on_cache_hit(step_name, metrics)

使用:
   from pipeline.core.services.hook_manager import HookManager
   hooks = HookManager.get()
   hooks.register('before_node', callable)

后续可扩展: flow 级、异常钩子、插件自动发现。
"""
from __future__ import annotations
from typing import Callable, Dict, List, Any, ClassVar, Optional
import threading
import logging
import time


class HookManager:
    """事件钩子管理器（单例模式）

    提供事件驱动的扩展机制，支持：
    - 插件注册
    - 事件分发
    - 执行统计
    - 错误隔离
    """
    _instance: ClassVar[Optional['HookManager']] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    # 支持的事件类型（可扩展）
    SUPPORTED_EVENTS = frozenset([
        'before_flow',
        'after_flow',
        'before_node',
        'after_node',
        'on_cache_hit',
        'on_failure',
    ])

    def __init__(self):
        self._hooks: Dict[str, List[Callable]] = {event: [] for event in self.SUPPORTED_EVENTS}
        self._logger = logging.getLogger(__name__)
        self._stats: Dict[str, Dict[str, Any]] = {
            event: {'call_count': 0, 'error_count': 0, 'total_time_ms': 0.0}
            for event in self.SUPPORTED_EVENTS
        }
        self._debug_mode = False

    @classmethod
    def get(cls) -> 'HookManager':
        """获取单例实例（线程安全）"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = HookManager()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置单例（用于测试）"""
        with cls._lock:
            cls._instance = None

    def set_debug(self, enabled: bool = True) -> None:
        """启用/禁用调试模式

        调试模式下会记录更详细的钩子执行日志。
        """
        self._debug_mode = enabled

    def register(self, event: str, func: Callable) -> Callable:
        """注册事件处理器

        Args:
            event: 事件名称
            func: 处理函数

        Returns:
            传入的函数（支持装饰器用法）

        Raises:
            ValueError: 事件类型不支持
        """
        if event not in self._hooks:
            raise ValueError(
                f"未支持的 hook 事件: {event}。"
                f"支持的事件: {sorted(self.SUPPORTED_EVENTS)}"
            )
        self._hooks[event].append(func)
        self._logger.debug(f"🔗 Hook 注册: {event} <- {func.__name__}")
        return func

    def unregister(self, event: str, func: Callable) -> bool:
        """注销事件处理器

        Args:
            event: 事件名称
            func: 要移除的处理函数

        Returns:
            是否成功移除
        """
        if event not in self._hooks:
            return False
        try:
            self._hooks[event].remove(func)
            return True
        except ValueError:
            return False

    def emit(self, event: str, *args, **kwargs) -> int:
        """触发事件

        安全执行所有注册的处理器，单个处理器失败不影响其他处理器。

        Args:
            event: 事件名称
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            成功执行的处理器数量
        """
        handlers = self._hooks.get(event, [])
        if not handlers:
            return 0

        success_count = 0
        stats = self._stats[event]

        for handler in handlers:
            start = time.perf_counter()
            try:
                handler(*args, **kwargs)
                success_count += 1
                if self._debug_mode:
                    self._logger.debug(f"✅ Hook {event}.{handler.__name__} 执行成功")
            except Exception as e:
                stats['error_count'] += 1
                # 钩子失败不影响主流程，但记录日志
                self._logger.debug(
                    f"⚠️ Hook '{event}.{handler.__name__}' 执行失败（已忽略）: {e}"
                )
            finally:
                elapsed_ms = (time.perf_counter() - start) * 1000
                stats['total_time_ms'] += elapsed_ms
                stats['call_count'] += 1

        return success_count

    def get_handlers(self, event: str) -> List[Callable]:
        """获取事件的所有处理器（只读）"""
        return list(self._hooks.get(event, []))

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取钩子执行统计

        Returns:
            事件 -> 统计信息的映射
        """
        return {
            event: {
                'handler_count': len(self._hooks[event]),
                'call_count': stats['call_count'],
                'error_count': stats['error_count'],
                'total_time_ms': round(stats['total_time_ms'], 2),
                'avg_time_ms': round(stats['total_time_ms'] / max(1, stats['call_count']), 2),
            }
            for event, stats in self._stats.items()
        }

    def clear(self, event: Optional[str] = None) -> None:
        """清除钩子

        Args:
            event: 指定事件，None 则清除所有
        """
        if event:
            if event in self._hooks:
                self._hooks[event].clear()
        else:
            for handlers in self._hooks.values():
                handlers.clear()


__all__ = ["HookManager"]