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
from typing import Callable, Dict, List, Any, ClassVar
import threading


class HookManager:
    _instance: ClassVar['HookManager' | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self):
        self._hooks: Dict[str, List[Callable]] = {
            'before_flow': [],
            'after_flow': [],
            'before_node': [],
            'after_node': [],
            'on_cache_hit': [],
            'on_failure': [],
        }

    @classmethod
    def get(cls) -> 'HookManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = HookManager()
        return cls._instance

    def register(self, event: str, func: Callable):
        if event not in self._hooks:
            raise ValueError(f"未支持的 hook 事件: {event}")
        self._hooks[event].append(func)
        return func

    def emit(self, event: str, *args, **kwargs):
        for fn in self._hooks.get(event, []):
            try:
                fn(*args, **kwargs)
            except Exception:
                # 钩子失败不影响主流程
                continue

__all__ = ["HookManager"]