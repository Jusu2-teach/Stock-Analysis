from __future__ import annotations
import inspect
import threading
from typing import Any, Dict, Optional
from ..models import MethodRegistration
from ..errors import (
    RegistryConflictError, RegistryMethodNotFound
)
from ..config import RegistryConfig
from .index import RegistryIndex
from .strategies import resolve_strategy
from .metrics import MetricsService
from .executor import MethodExecutor
from .hooks import HookBus
from .loader import ModuleLoader
from .scanner import Scanner


class Registry:
    """方法注册中心（线程安全的单例）

    职责：
    - 方法注册与索引管理
    - 策略选择与执行
    - 指标收集与钩子管理
    - 自动发现与模块加载
    """

    _instance: Optional['Registry'] = None
    _lock = threading.Lock()

    def __init__(self, config: Optional[RegistryConfig] = None):
        self.config = config or RegistryConfig()
        self.index = RegistryIndex()
        self.metrics = MetricsService()
        self.executor = MethodExecutor(self.metrics)
        self.hooks = HookBus()
        self.loader = ModuleLoader(self.index, self.config)
        self.scanner = Scanner(self)

    # ---------------- Singleton (Thread-Safe) -----------------
    @classmethod
    def get(cls) -> 'Registry':
        """获取单例实例（线程安全）"""
        if cls._instance is None:
            with cls._lock:
                # Double-check locking
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置单例（主要用于测试）"""
        with cls._lock:
            cls._instance = None

    # ---------------- Registration --------------
    def register(self, reg: MethodRegistration) -> bool:
        full_key = reg.full_key
        if full_key in self.index.by_full_key:
            mode = self.config.conflict_mode
            if mode == 'error':
                raise RegistryConflictError(f'conflict: {full_key}')
            if mode == 'ignore':
                return False
            # warn: 覆盖
        self.index.add(reg)
        self.hooks.emit('after_method_registered', full_key=full_key, component=reg.component_type,
                        engine_type=reg.engine_type, engine_name=reg.engine_name, version=reg.version)
        return True

    def scan(self, module: Any, component_type: str, engine_type: str, **kwargs) -> int:
        """扫描模块并自动注册方法"""
        return self.scanner.scan(module, component_type, engine_type, **kwargs)

    # ---------------- Discover / Load -----------
    def auto_load(self, hot_reload: bool = False) -> int:
        return self.loader.load_all(hot_reload=hot_reload)

    def refresh(self, hot_reload: bool = True):
        # 简化：全量重载 (清空索引 -> 重新加载)。装饰器再次执行完成重建。
        self.index.clear()
        self.auto_load(hot_reload=hot_reload)
        self.hooks.emit('after_registry_refresh', mode='full')

    # ---------------- Selection -----------------
    def select(self, component_type: str, method_name: str, *, strategy: str = 'default', preferred_engine: Optional[str] = None) -> MethodRegistration:
        """选择最佳方法实现

        Args:
            component_type: 组件类型
            method_name: 方法名
            strategy: 选择策略
            preferred_engine: 优先引擎（当strategy='engine_override'时必需）
        """
        candidates = self.index.method_candidates(component_type, method_name)
        if not candidates:
            raise RegistryMethodNotFound(f'{component_type}.{method_name} not found')
        strat = resolve_strategy(strategy, preferred_engine=preferred_engine)
        return strat.select(candidates)

    # ---------------- Execute -------------------
    def execute(self, component_type: str, method_name: str, *args, strategy: str = 'default', preferred_engine: Optional[str] = None, **kwargs):
        reg = self.select(component_type, method_name, strategy=strategy, preferred_engine=preferred_engine)
        return self.executor.execute(reg, *args, **kwargs)

    def execute_with_engine(self, component_type: str, engine_type: str, method_name: str, *args, **kwargs):
        full_key = f"{component_type}::{engine_type}::{method_name}"
        reg = self.index.get_full(full_key)
        if not reg:
            raise RegistryMethodNotFound(full_key)
        return self.executor.execute(reg, *args, **kwargs)

    # ---------------- Introspection -------------
    def list_methods(self, component_type: Optional[str] = None, engine_type: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        if component_type:
            bucket = self.index.by_component.get(component_type, {})
        else:
            bucket = self.index.by_component
        result: Dict[str, Dict[str, Any]] = {}
        for comp, meth_map in bucket.items():
            for meth, engs in meth_map.items():
                for eng, reg in engs.items():
                    if engine_type and eng != engine_type:
                        continue
                    result[reg.full_key] = {
                        'component_type': comp,
                        'engine_type': eng,
                        'engine_name': reg.engine_name,
                        'description': reg.description,
                        'version': reg.version,
                        'priority': reg.priority,
                        'deprecated': reg.deprecated,
                        'module': reg.module_path,
                        'signature': reg.signature,
                        'tags': list(reg.tags),
                    }
        return result

    def stats(self) -> Dict[str, Any]:
        return self.metrics.export()
