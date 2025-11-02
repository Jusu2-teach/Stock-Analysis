from __future__ import annotations
from typing import Dict, List
from ..models import MethodRegistration


class RegistryIndex:
    """注册中心索引（统一使用 full_key 格式）

    索引结构：
    - by_component[component][method][engine_type] -> MethodRegistration
    - by_full_key[full_key] -> MethodRegistration

    full_key 格式: "component::engine::method"
    """

    def __init__(self) -> None:
        self.by_component: Dict[str, Dict[str, Dict[str, MethodRegistration]]] = {}
        self.by_full_key: Dict[str, MethodRegistration] = {}

    def add(self, reg: MethodRegistration) -> None:
        """添加方法注册到索引"""
        # 组件 -> 方法 -> 引擎 三层索引
        comp_bucket = self.by_component.setdefault(reg.component_type, {})
        meth_bucket = comp_bucket.setdefault(reg.engine_name, {})
        meth_bucket[reg.engine_type] = reg

        # Full key 索引
        self.by_full_key[reg.full_key] = reg

    def get_all(self) -> Dict[str, MethodRegistration]:
        """获取所有注册（以 full_key 为键）"""
        return dict(self.by_full_key)

    def find_by_component(self, component: str) -> Dict[str, MethodRegistration]:
        """查找指定组件的所有方法（以 full_key 为键）"""
        result: Dict[str, MethodRegistration] = {}
        bucket = self.by_component.get(component, {})
        for _, eng_map in bucket.items():
            for reg in eng_map.values():
                result[reg.full_key] = reg
        return result

    def get_full(self, full_key: str) -> MethodRegistration | None:
        """根据 full_key 获取注册"""
        return self.by_full_key.get(full_key)

    def method_candidates(self, component: str, method_name: str) -> List[MethodRegistration]:
        """获取指定组件和方法的所有候选实现"""
        bucket = self.by_component.get(component, {})
        eng_map = bucket.get(method_name, {})
        return list(eng_map.values())

    def clear(self) -> None:
        """清空所有索引"""
        self.by_component.clear()
        self.by_full_key.clear()
