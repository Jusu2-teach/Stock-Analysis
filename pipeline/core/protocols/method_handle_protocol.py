"""Method Handle Protocol - 避免循环导入的接口抽象

定义 MethodHandle 的接口契约，允许其他模块声明依赖而不直接导入实现。
"""
from __future__ import annotations
from typing import Protocol, Any, Dict, runtime_checkable


@runtime_checkable
class IMethodHandle(Protocol):
    """方法句柄接口

    职责：延迟绑定引擎，在运行时根据策略选择最佳实现

    Note:
        属性和方法与 MethodHandle 实现对齐：
        - component: 组件名（如 'data_loader'）
        - method: 方法名（如 'load_csv'）
        - prefer: 引擎偏好（'auto' | 'fixed'）
        - resolve(): 解析引擎类型
    """

    component: str
    method: str
    prefer: str  # 'auto' | 'fixed'
    fixed_engine: str | None

    def resolve(self, orchestrator: Any) -> str:
        """解析引擎类型

        Args:
            orchestrator: Orchestrator 实例

        Returns:
            解析后的引擎类型字符串
        """
        ...


def create_method_handle(
    component: str,
    method: str,
    prefer: str = 'auto',
    fixed_engine: str | None = None
) -> IMethodHandle:
    """工厂方法：创建 MethodHandle 实例

    通过工厂方法创建，避免直接导入 MethodHandle 类。

    Args:
        component: 组件类型（如 'data_loader'）
        method: 方法名（如 'load_csv'）
        prefer: 偏好模式（'auto' | 'fixed'）
        fixed_engine: 固定引擎（仅当 prefer='fixed' 时使用）

    Returns:
        IMethodHandle 实例

    Example:
        >>> handle = create_method_handle('data_loader', 'load_csv', prefer='auto')
        >>> result = handle.execute(orchestrator, file='data.csv')
    """
    # 延迟导入实现类，避免循环依赖
    from pipeline.core.handles.method_handle import MethodHandle

    return MethodHandle(
        component=component,
        method=method,
        prefer=prefer,
        fixed_engine=fixed_engine
    )


__all__ = ['IMethodHandle', 'create_method_handle']
