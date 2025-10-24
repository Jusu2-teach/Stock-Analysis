import os
from dataclasses import dataclass


@dataclass(frozen=True)
class RegistryConfig:
    conflict_mode: str = os.getenv('ASTOCK_REGISTRY_CONFLICT', 'warn').lower()
    bridge_pipeline: str = os.getenv('ASTOCK_REGISTRY_BRIDGE_PIPELINE', 'auto')
    lazy_enabled: bool = os.getenv('ASTOCK_REGISTRY_LAZY', '1') == '1'
    skip_patterns: tuple[str, ...] = ('backup', 'bak', 'tmp', 'deprecated')
    base_package: str = os.getenv('ASTOCK_COMPONENT_BASE', 'astock')
