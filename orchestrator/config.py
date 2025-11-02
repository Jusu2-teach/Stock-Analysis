import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RegistryConfig:
    """注册中心配置

    conflict_mode: 方法冲突处理模式 ('error' | 'warn' | 'ignore')
    skip_patterns: 自动加载时跳过的模块名模式
    base_package: 组件基础包名
    """
    conflict_mode: str = os.getenv('ASTOCK_REGISTRY_CONFLICT', 'warn').lower()
    skip_patterns: tuple[str, ...] = ('backup', 'bak', 'tmp', 'deprecated')
    base_package: str = os.getenv('ASTOCK_COMPONENT_BASE', 'astock')
