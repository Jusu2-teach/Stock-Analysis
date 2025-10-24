from __future__ import annotations
from typing import Dict, List
from ..models import MethodRegistration


class RegistryIndex:
    """集中管理多重索引：
    - by_component[component][method][engine_type] -> registration
    - by_full_key[full_key] -> registration
    - flat list methods
    """

    def __init__(self) -> None:
        self.by_component: Dict[str, Dict[str, Dict[str, MethodRegistration]]] = {}
        self.by_full_key: Dict[str, MethodRegistration] = {}
        self.methods: Dict[str, MethodRegistration] = {}

    def add(self, reg: MethodRegistration) -> None:
        comp_bucket = self.by_component.setdefault(reg.component_type, {})
        meth_bucket = comp_bucket.setdefault(reg.engine_name, {})
        meth_bucket[reg.engine_type] = reg
        self.by_full_key[reg.full_key] = reg
        legacy_key = f"{reg.component_type}_{reg.engine_type}_{reg.engine_name}"
        self.methods[legacy_key] = reg

    def get_all(self) -> Dict[str, MethodRegistration]:
        return dict(self.methods)

    def find_by_component(self, component: str) -> Dict[str, MethodRegistration]:
        result: Dict[str, MethodRegistration] = {}
        bucket = self.by_component.get(component, {})
        for _, eng_map in bucket.items():
            for reg in eng_map.values():
                result[f"{reg.component_type}_{reg.engine_type}_{reg.engine_name}"] = reg
        return result

    def get_full(self, full_key: str) -> MethodRegistration | None:
        return self.by_full_key.get(full_key)

    def method_candidates(self, component: str, method_name: str) -> List[MethodRegistration]:
        bucket = self.by_component.get(component, {})
        eng_map = bucket.get(method_name, {})
        return list(eng_map.values())

    def clear(self):
        self.by_component.clear()
        self.by_full_key.clear()
        self.methods.clear()
