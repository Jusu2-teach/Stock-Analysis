"""
Module Discovery and Scanning
=============================

Merged from loader.py and scanner.py for cohesive module discovery functionality.

This module provides:
1. ModuleLoader - Automatic discovery and loading of component modules
2. Scanner - Scanning and registering functions from modules
"""

from __future__ import annotations
import importlib
import inspect
import logging
import pkgutil
from pathlib import Path
from types import ModuleType
from typing import Any, List, Optional, Tuple, Callable, TYPE_CHECKING

from ..config import RegistryConfig
from ..models import MethodRegistration

if TYPE_CHECKING:
    from .index import RegistryIndex
    from .registry import Registry

logger = logging.getLogger(__name__)


# =============================================================================
# Module Loader
# =============================================================================

class ModuleLoader:
    """Automatic module discovery and loading.

    Discovers and loads component modules from the configured base package.
    Supports hot-reloading for development.
    """

    def __init__(self, index: 'RegistryIndex', config: RegistryConfig):
        self.index = index
        self.config = config
        self.module_files: dict[str, float] = {}

    def discover_components(self) -> dict:
        """Discover all components under the base package.

        Returns:
            Dict mapping component names to their module info
        """
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
        """Discover modules within a component package."""
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
        """Discover engine modules within a component."""
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
        """Load all discovered modules.

        Args:
            hot_reload: If True, reload already imported modules

        Returns:
            Number of modules loaded
        """
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
        """Import a module, optionally reloading it."""
        try:
            mod = importlib.import_module(module_path)
            if hot_reload:
                importlib.reload(mod)
        except Exception as e:
            logger.error(f"Failed to import module {module_path}: {e}")


# =============================================================================
# Module Scanner
# =============================================================================

class Scanner:
    """Automatic function scanning and registration.

    Scans modules for functions and registers them with the registry.
    Useful for bulk registration of functions that follow naming conventions.
    """

    def __init__(self, registry: 'Registry'):
        self.registry = registry

    def scan(
        self,
        module: ModuleType,
        component_type: str,
        engine_type: str,
        tags: Tuple[str, ...] = tuple(),
        include_private: bool = False,
        pattern: Optional[str] = None
    ) -> int:
        """Scan a module and register its functions.

        Args:
            module: Target module to scan
            component_type: Component type for registration
            engine_type: Engine type for registration
            tags: Additional tags to apply
            include_private: Whether to include underscore-prefixed functions
            pattern: Optional name pattern filter (simple contains match)

        Returns:
            Number of methods registered
        """
        count = 0
        for name, obj in inspect.getmembers(module):
            if not inspect.isfunction(obj):
                continue

            if name.startswith('_') and not include_private:
                continue

            if pattern and pattern not in name:
                continue

            # Ensure function is defined in current module (avoid re-registering imports)
            if obj.__module__ != module.__name__:
                continue

            # Extract first line of docstring as description
            description = (inspect.getdoc(obj) or "").split('\n')[0]

            reg = MethodRegistration(
                component_type=component_type,
                engine_type=engine_type,
                engine_name=name,
                callable=obj,
                description=description,
                tags=tags,
                module_path=module.__name__
            )

            try:
                if self.registry.register(reg):
                    count += 1
                    logger.debug(f"Scanned and registered: {reg.full_key}")
            except Exception as e:
                logger.warning(f"Failed to register scanned function {name}: {e}")

        return count

    def scan_module_by_name(
        self,
        module_name: str,
        component_type: str,
        engine_type: str,
        **kwargs
    ) -> int:
        """Scan a module by its import name.

        Args:
            module_name: Full module import path (e.g., "src.astock.engines.polars")
            component_type: Component type for registration
            engine_type: Engine type for registration
            **kwargs: Additional arguments passed to scan()

        Returns:
            Number of methods registered
        """
        try:
            module = importlib.import_module(module_name)
            return self.scan(module, component_type, engine_type, **kwargs)
        except ImportError as e:
            logger.error(f"Failed to import module {module_name}: {e}")
            return 0
