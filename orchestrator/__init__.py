"""AStock Orchestrator

方法注册与执行系统。

可观测性采用 Observer 端口（生命周期信号）设计：
- orchestrator 核心不硬依赖 shared/pipeline 的 EventBus
- 默认在本仓库中仍会通过 shared.EventBus 发布兼容事件
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

from .telemetry import OrchestratorObserver, NullObserver, CompositeObserver

# 与项目版本/作者信息对齐（见 pyproject.toml）
__version__ = "1.0.0"
__author__ = "Your Name"

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
    # Observability
    'OrchestratorObserver',
    'NullObserver',
    'CompositeObserver',
]
