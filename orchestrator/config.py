import importlib
import os
from dataclasses import dataclass


def _default_base_package() -> str:
    """Choose a sensible default base package.

    This repo uses a `src/` layout (import path: `src.astock`). In other
    environments the package might be installed as `astock`.

    Env var ASTOCK_COMPONENT_BASE always wins.
    """
    explicit = os.getenv('ASTOCK_COMPONENT_BASE')
    if explicit:
        return explicit

    try:
        importlib.import_module('astock')
        return 'astock'
    except Exception:
        try:
            importlib.import_module('src.astock')
            return 'src.astock'
        except Exception:
            return 'astock'


@dataclass(frozen=True)
class RegistryConfig:
    """注册中心配置

    conflict_mode: 方法冲突处理模式 ('error' | 'warn' | 'ignore')
    skip_patterns: 自动加载时跳过的模块名模式
    base_package: 组件基础包名
    """
    conflict_mode: str = os.getenv('ASTOCK_REGISTRY_CONFLICT', 'warn').lower()
    skip_patterns: tuple[str, ...] = ('backup', 'bak', 'tmp', 'deprecated')
    base_package: str = _default_base_package()
