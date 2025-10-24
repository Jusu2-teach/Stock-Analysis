from __future__ import annotations
from typing import Any, List
import logging
from .registry.registry import Registry


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
        if auto_discover:
            count = self.registry.auto_load(hot_reload=False)
            self.logger.info(f"[orchestrator] auto_discover loaded modules={count} registered_methods={len(self.registry.index.by_full_key)} legacy_methods={len(getattr(self.registry,'methods',{}))}")
        self._build_interfaces()

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
        strategy = kwargs.pop('_strategy', 'default')
        prefer = kwargs.pop('_engine_type', None) or kwargs.pop('_prefer_engine', None)
        return self.registry.execute(component_type, method_name, *args, strategy=strategy, preferred_engine=prefer, **kwargs)

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
