import inspect
import logging
from typing import Any, List, Optional, Tuple, Callable
from types import ModuleType
from ..models import MethodRegistration

logger = logging.getLogger(__name__)

class Scanner:
    """自动扫描模块并注册方法的工具"""

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
        """扫描模块中的函数并注册

        Args:
            module: 目标模块
            component_type: 注册的组件类型
            engine_type: 注册的引擎类型
            tags: 附加标签
            include_private: 是否包含下划线开头的函数
            pattern: 可选的名称匹配模式（简单包含匹配）

        Returns:
            注册的方法数量
        """
        count = 0
        for name, obj in inspect.getmembers(module):
            if not inspect.isfunction(obj):
                continue

            if name.startswith('_') and not include_private:
                continue

            if pattern and pattern not in name:
                continue

            # 确保函数定义在当前模块中（避免导入的函数被重复注册）
            if obj.__module__ != module.__name__:
                continue

            # 提取文档字符串作为描述
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
