from __future__ import annotations
from typing import Any, List, Callable, Dict
import logging
from .registry.registry import Registry

# Middleware type definition
# func(component, method, args, kwargs, next_call) -> result
Middleware = Callable[[str, str, tuple, dict, Callable], Any]

class ComponentProxy:
    def __init__(self, orchestrator: 'AStockOrchestrator', component_type: str):
        self.o = orchestrator
        self.component_type = component_type

    def __getattr__(self, method_name: str):
        def caller(*args, **kwargs):
            return self.o.execute(self.component_type, method_name, *args, **kwargs)
        return caller

    def __dir__(self):
        return self.o.get_component_methods(self.component_type)


class AStockOrchestrator:
    def __init__(self, *, auto_discover: bool = True):
        self.logger = logging.getLogger(__name__)
        self.registry = Registry.get()
        self._middlewares: List[Middleware] = []

        if auto_discover:
            count = self.registry.auto_load(hot_reload=False)
            self.logger.info(f"[orchestrator] auto_discover loaded modules={count} registered_methods={len(self.registry.index.by_full_key)} legacy_methods={len(getattr(self.registry,'methods',{}))}")
        self._build_interfaces()

    def add_middleware(self, middleware: Middleware):
        """注册中间件 (Middleware)

        中间件可以在方法执行前后添加逻辑（如日志、计时、错误处理）。
        执行顺序：先注册的先执行（洋葱模型外层）。
        """
        self._middlewares.append(middleware)

    # --------- dynamic interface ---------
    def _build_interfaces(self):
        comps = set(self.registry.index.by_component.keys())
        for c in comps:
            setattr(self, c, ComponentProxy(self, c))

    def get_component_methods(self, component_type: str) -> List[str]:
        bucket = self.registry.index.by_component.get(component_type, {})
        return sorted(bucket.keys())

    # --------- execution facades ---------
    def execute(self, component_type: str, method_name: str, *args, **kwargs) -> Any:
        """执行方法（支持策略选择、引擎偏好和中间件）

        特殊参数（从 kwargs 中提取）：
            _strategy: 选择策略 ('default' | 'prefer_latest' | 'prefer_stable' | 'highest_priority')
            _preferred_engine: 优先使用的引擎类型
        """
        strategy = kwargs.pop('_strategy', 'default')
        preferred_engine = kwargs.pop('_preferred_engine', None)

        # 定义核心执行函数
        def core_execution(*a, **kw):
            return self.registry.execute(
                component_type,
                method_name,
                *a,
                strategy=strategy,
                preferred_engine=preferred_engine,
                **kw
            )

        # 构建中间件链
        # 链式调用：middleware1(..., next=middleware2(..., next=core))
        chain = core_execution

        # 反向遍历，从最内层（最后注册的）开始包装
        for mw in reversed(self._middlewares):
            # 闭包捕获当前的 next_call (chain) 和当前中间件 (mw)
            def next_step(c=chain, m=mw):
                def wrapper(*a, **kw):
                    return m(component_type, method_name, a, kw, c)
                return wrapper
            chain = next_step()

        return chain(*args, **kwargs)

    def execute_with_engine(self, component_type: str, engine_type: str, method_name: str, *args, **kwargs) -> Any:
        return self.registry.execute_with_engine(component_type, engine_type, method_name, *args, **kwargs)

    # --------- metadata ---------
    def describe(self, component_type: str, method_name: str):
        cands = self.registry.index.method_candidates(component_type, method_name)
        if not cands:
            return {'status': 'not_found', 'component': component_type, 'method': method_name}
        chosen = self.registry.select(component_type, method_name)
        return {
            'status': 'ok',
            'component': component_type,
            'method': method_name,
            'implementations': [
                {
                    'engine_type': r.engine_type,
                    'version': r.version,
                    'priority': r.priority,
                    'deprecated': r.deprecated,
                    'tags': list(r.tags),
                    'description': r.description
                } for r in cands
            ],
            'selected': {
                'engine_type': chosen.engine_type,
                'version': chosen.version,
                'priority': chosen.priority,
                'deprecated': chosen.deprecated,
                'tags': list(chosen.tags),
                'description': chosen.description
            }
        }

    def resolve_engine(self, component_type: str, method_name: str) -> str:
        chosen = self.registry.select(component_type, method_name)
        return chosen.engine_type

    def list_methods(self, component_type: str | None = None, engine_type: str | None = None):
        return self.registry.list_methods(component_type=component_type, engine_type=engine_type)

    def get_system_status(self):
        stats = self.registry.stats()
        return {
            'orchestrator': {'status': 'active', 'version': '4.0-refactor'},
            'registry': stats,
            'components': list(self.registry.index.by_component.keys())
        }
