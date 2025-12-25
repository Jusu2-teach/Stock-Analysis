"""AStock Orchestrator v5.0

基于统一 EventBus 架构的方法注册与执行系统。

v5.0 新架构:
- 完全使用 shared.EventBus 进行事件通信
- 移除旧版 HookBus
- 简化依赖关系
"""

from .orchestrator import AStockOrchestrator  # 精简 facade
from .decorators.register import register_method  # 方法注册装饰器
from .registry.registry import Registry
from .models import MethodRegistration
from .protocols import (
    hookspec,
    HookSpecRegistry,
    SignatureValidator,
    BusinessEngineFunction,
    DataEngineFunction,
    DataHubFunction,
)

__all__ = [
    # Core
    'AStockOrchestrator',
    'Registry',
    'MethodRegistration',
    'register_method',
    # Protocols & Validation
    'hookspec',
    'HookSpecRegistry',
    'SignatureValidator',
    'BusinessEngineFunction',
    'DataEngineFunction',
    'DataHubFunction',
]
