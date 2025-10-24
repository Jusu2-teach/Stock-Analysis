from __future__ import annotations
import importlib
import pkgutil
from pathlib import Path
from typing import List
from ..config import RegistryConfig
from ..models import MethodRegistration
from .index import RegistryIndex


class ModuleLoader:
    def __init__(self, index: RegistryIndex, config: RegistryConfig):
        self.index = index
        self.config = config
        self.module_files: dict[str, float] = {}

    def discover_components(self) -> dict:
        base_module = importlib.import_module(self.config.base_package)
        base_path = Path(base_module.__file__).parent
        discovered = {}
        for item in base_path.iterdir():
            if item.is_dir() and not item.name.startswith('_'):
                component_name = item.name
                modules = self._discover_modules(f"{self.config.base_package}.{component_name}")
                engines = self._discover_engines(component_name)
                if modules or engines:
                    discovered[component_name] = {
                        'modules': modules,
                        'engines': engines,
                        'path': str(item)
                    }
        return discovered

    def _discover_modules(self, component_package: str) -> List[str]:
        try:
            pkg = importlib.import_module(component_package)
            mods = []
            for _, modname, ispkg in pkgutil.iter_modules(pkg.__path__):
                if ispkg or modname.startswith('_'):
                    continue
                if any(p in modname.lower() for p in self.config.skip_patterns):
                    continue
                mods.append(modname)
            return mods
        except ImportError:
            return []

    def _discover_engines(self, component_name: str) -> List[str]:
        engines_pkg_name = f"{self.config.base_package}.{component_name}.engines"
        try:
            eng_pkg = importlib.import_module(engines_pkg_name)
            mods = []
            for _, modname, ispkg in pkgutil.iter_modules(eng_pkg.__path__):
                if ispkg or modname.startswith('_'):
                    continue
                if any(p in modname.lower() for p in self.config.skip_patterns):
                    continue
                mods.append(modname)
            return mods
        except ImportError:
            return []

    def load_all(self, hot_reload: bool = False) -> int:
        discovered = self.discover_components()
        count = 0
        for comp, info in discovered.items():
            for m in info['modules']:
                self._import(f"{self.config.base_package}.{comp}.{m}", hot_reload)
                count += 1
            for m in info['engines']:
                self._import(f"{self.config.base_package}.{comp}.engines.{m}", hot_reload)
                count += 1
        return count

    def _import(self, module_path: str, hot_reload: bool):
        try:
            mod = importlib.import_module(module_path)
            if hot_reload:
                importlib.reload(mod)
        except Exception:
            pass
