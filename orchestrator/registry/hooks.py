from __future__ import annotations
from typing import Callable, Dict, List


class HookBus:
    def __init__(self):
        self._hooks: Dict[str, List[Callable]] = {}

    def on(self, event: str, fn: Callable):
        self._hooks.setdefault(event, []).append(fn)
        return fn

    def emit(self, event: str, *args, **kwargs):
        for fn in self._hooks.get(event, []):
            try:
                fn(*args, **kwargs)
            except Exception:
                continue
