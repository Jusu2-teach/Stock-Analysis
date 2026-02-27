"""Pipeline Protocols
====================

定义业务代码需要遵循的协议 (Protocol)。
使用 typing.Protocol 实现结构化子类型 (Structural Subtyping)。

设计原则：
- 协议定义行为契约，不强制继承
- 业务代码只需实现协议方法即可
- 支持运行时类型检查 (runtime_checkable)

版本: 2.0.0
"""

from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

# 类型检查时才导入，避免循环导入
if TYPE_CHECKING:
    from ..execution.middleware import MiddlewareContext

logger = logging.getLogger(__name__)

# =============================================================================
# 泛型类型变量
# =============================================================================

K = TypeVar('K')  # 聚合键类型
V = TypeVar('V')  # 聚合值类型
T = TypeVar('T')  # 通用类型


# =============================================================================
# 聚合协议 (原 PDDA)
# =============================================================================

# AggregatableResult 从 aggregation 模块统一导入
from ..aggregation.core import AggregatableResult


@runtime_checkable
class Aggregatable(Protocol[K, V]):
    """可聚合协议

    任何实现此协议的对象都可被 Pipeline 聚合系统收集。
    支持鸭子类型 (Duck Typing)。
    """

    def get_aggregation_key(self) -> K:
        """返回聚合键"""
        ...

    def get_aggregation_value(self) -> V:
        """返回聚合值"""
        ...

    def get_aggregation_namespace(self) -> str:
        """返回命名空间 (可选，默认 'default')"""
        ...


# =============================================================================
# 数据集协议
# =============================================================================

@runtime_checkable
class Dataset(Protocol[T]):
    """数据集协议

    定义数据集的读写接口，支持多种后端 (Memory, File, DuckDB...)

    Example:
        class MemoryDataset(Dataset[pd.DataFrame]):
            def load(self) -> pd.DataFrame: ...
            def save(self, data: pd.DataFrame) -> None: ...
    """

    def load(self) -> T:
        """加载数据"""
        ...

    def save(self, data: T) -> None:
        """保存数据"""
        ...

    def exists(self) -> bool:
        """检查数据是否存在"""
        ...

    def release(self) -> None:
        """释放资源 (可选)"""
        ...


# =============================================================================
# 执行协议
# =============================================================================

@runtime_checkable
class TaskCallable(Protocol):
    """任务可调用协议

    定义任务函数的签名要求。
    """

    def __call__(self, **kwargs: Any) -> Any:
        """执行任务，返回结果"""
        ...


@runtime_checkable
class MiddlewareProtocol(Protocol):
    """中间件协议

    定义执行中间件的接口。
    """

    async def __call__(
        self,
        context: 'TaskContext',
        next_middleware: Callable[['TaskContext'], Any]
    ) -> Any:
        """执行中间件逻辑"""
        ...


@runtime_checkable
class RunnerBackend(Protocol):
    """执行后端协议

    定义执行后端的接口 (Sequential, Parallel, Distributed...)
    """

    def run_tasks(
        self,
        tasks: list,
        context: 'ExecutionContext'
    ) -> list:
        """执行一批任务"""
        ...


@runtime_checkable
class CacheBackend(Protocol):
    """缓存后端协议

    定义缓存后端的接口 (Memory, File, Redis...)
    """

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        ...

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        ...

    def delete(self, key: str) -> None:
        """删除缓存"""
        ...

    def exists(self, key: str) -> bool:
        """检查缓存是否存在"""
        ...

    def clear(self) -> None:
        """清空所有缓存"""
        ...


# =============================================================================
# 前向引用类型
# =============================================================================

# 这些类型在运行时为 Any，但在类型检查时会解析为实际类型
# 使用 TYPE_CHECKING 保护的导入确保类型检查器能够解析
if TYPE_CHECKING:
    # 类型检查时使用实际类型
    TaskContext = MiddlewareContext
    ExecutionContext = MiddlewareContext
else:
    # 运行时使用 Any 避免循环导入
    TaskContext = Any
    ExecutionContext = Any


# =============================================================================
# 工具函数
# =============================================================================

def is_aggregatable(obj: Any) -> bool:
    """检查对象是否可聚合

    支持两种方式：
    1. 返回 AggregatableResult 实例
    2. 实现 Aggregatable 协议
    """
    if isinstance(obj, AggregatableResult):
        return True
    if isinstance(obj, Aggregatable):
        return True
    return False


def extract_aggregation_data(obj: Any) -> tuple[Any, Any, str]:
    """提取聚合数据

    Returns:
        (key, value, namespace) 三元组

    Raises:
        TypeError: 如果对象不可聚合
    """
    if isinstance(obj, AggregatableResult):
        return (obj.key, obj.value, obj.namespace)

    if isinstance(obj, Aggregatable):
        namespace = "default"
        if hasattr(obj, 'get_aggregation_namespace'):
            try:
                namespace = obj.get_aggregation_namespace()
            except (TypeError, AttributeError) as e:
                # Fall back to default namespace if method fails
                logger.debug(f"Failed to get aggregation namespace: {e}")
        return (
            obj.get_aggregation_key(),
            obj.get_aggregation_value(),
            namespace
        )

    raise TypeError(f"Object is not aggregatable: {type(obj)}")
