"""AStock Orchestrator v4.0

仅暴露新版模块化 orchestrator 与注册装饰器。
旧版 intelligent_registry / core 已弃用且不再导出，避免混用。
"""

from .orchestrator import AStockOrchestrator  # 新版精简 facade
from .decorators.register import register_method

__all__ = ['AStockOrchestrator', 'register_method']
