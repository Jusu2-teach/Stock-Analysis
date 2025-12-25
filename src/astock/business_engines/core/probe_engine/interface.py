"""
Probe Interface Protocol
========================

定义探针的标准接口协议，所有探针必须实现此协议。

设计原则：
1. 纯函数式：相同输入 → 相同输出
2. 无业务逻辑：不包含阈值判断，不做 pass/fail 决策
3. 类型安全：使用 Protocol 定义接口，支持静态类型检查
4. 可组合：探针可自由组合

使用示例：
    class MyProbe:
        name = "my_probe"

        def compute(self, values: np.ndarray, **kwargs) -> MyResult:
            # 纯数学计算
            return MyResult(...)

        def default(self) -> MyResult:
            return MyResult.empty()

        def validate(self, values: np.ndarray) -> bool:
            return len(values) >= 3
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    TypeVar,
    runtime_checkable,
)

import numpy as np

# 泛型类型变量，表示探针结果类型
T = TypeVar("T")


@dataclass(frozen=True)
class ProbeResult:
    """
    探针结果基类

    所有探针结果都应该继承此类或实现相同的接口。
    使用 frozen=True 确保结果不可变，支持缓存。

    Attributes:
        is_valid: 结果是否有效
        warnings: 计算过程中产生的警告
        metadata: 附加元数据
    """
    is_valid: bool = True
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "is_valid": self.is_valid,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }

    @classmethod
    def empty(cls) -> "ProbeResult":
        """创建空结果"""
        return cls(is_valid=False, warnings=["No data available"])


@runtime_checkable
class ProbeProtocol(Protocol[T]):
    """
    探针协议（Protocol 定义）

    所有探针必须实现此协议。使用 Protocol 而非抽象基类，
    允许鸭子类型和更灵活的实现。

    Type Parameters:
        T: 探针结果类型

    Attributes:
        name: 探针唯一名称
        description: 探针描述
        fatal: 计算失败是否致命（阻止后续探针）

    Methods:
        compute: 执行探针计算
        default: 返回默认结果
        validate: 验证输入数据
    """

    @property
    def name(self) -> str:
        """探针唯一名称"""
        ...

    @property
    def description(self) -> str:
        """探针描述"""
        ...

    @property
    def fatal(self) -> bool:
        """计算失败是否致命"""
        ...

    def compute(self, values: np.ndarray, **kwargs) -> T:
        """
        执行探针计算

        Args:
            values: 时间序列数据 (numpy array)
            **kwargs: 额外参数（如配置、上下文）

        Returns:
            T: 探针结果

        Raises:
            ValueError: 数据不合法
            RuntimeError: 计算失败
        """
        ...

    def default(self) -> T:
        """
        返回默认结果

        当数据不足或计算失败时使用。

        Returns:
            T: 默认探针结果（通常标记为无效）
        """
        ...

    def validate(self, values: np.ndarray) -> bool:
        """
        验证输入数据是否满足计算要求

        Args:
            values: 时间序列数据

        Returns:
            bool: True 表示数据有效，可以计算
        """
        ...


class BaseProbe(ABC, Generic[T]):
    """
    探针抽象基类

    提供探针的基本实现框架。推荐继承此类来创建新探针，
    而不是直接实现 ProbeProtocol。

    Type Parameters:
        T: 探针结果类型

    Attributes:
        _name: 探针名称
        _description: 探针描述
        _fatal: 是否致命
        _min_data_points: 最小数据点数

    Example:
        class MyProbe(BaseProbe[MyResult]):
            def __init__(self):
                super().__init__(
                    name="my_probe",
                    description="My custom probe",
                    min_data_points=5,
                )

            def _compute_impl(self, values: np.ndarray, **kwargs) -> MyResult:
                # 实现具体计算逻辑
                return MyResult(...)

            def _create_default(self) -> MyResult:
                return MyResult.empty()
    """

    def __init__(
        self,
        name: str,
        description: str = "",
        fatal: bool = False,
        min_data_points: int = 3,
    ):
        self._name = name
        self._description = description
        self._fatal = fatal
        self._min_data_points = min_data_points

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def fatal(self) -> bool:
        return self._fatal

    @property
    def min_data_points(self) -> int:
        return self._min_data_points

    def validate(self, values: np.ndarray) -> bool:
        """默认验证：检查数据点数量"""
        if values is None:
            return False
        if not isinstance(values, np.ndarray):
            return False
        if len(values) < self._min_data_points:
            return False
        # 检查是否全为 NaN
        if np.all(np.isnan(values)):
            return False
        return True

    def compute(self, values: np.ndarray, **kwargs) -> T:
        """
        执行探针计算（带验证和异常处理）

        这是对外的主入口，内部调用 _compute_impl。
        """
        if not self.validate(values):
            return self.default()

        return self._compute_impl(values, **kwargs)

    @abstractmethod
    def _compute_impl(self, values: np.ndarray, **kwargs) -> T:
        """
        实际的计算实现（子类必须实现）

        此方法被 compute() 调用，已通过验证。
        """
        ...

    def default(self) -> T:
        """返回默认结果"""
        return self._create_default()

    @abstractmethod
    def _create_default(self) -> T:
        """创建默认结果（子类必须实现）"""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"


@dataclass
class ProbeExecutionContext:
    """
    探针执行上下文

    传递给探针的额外信息，不影响计算结果，但可用于：
    - 日志记录
    - 性能追踪
    - 调试信息

    Attributes:
        group_key: 分组键（如公司代码）
        metric_name: 指标名称
        config: 探针配置
        metadata: 其他元数据
    """
    group_key: str = ""
    metric_name: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProbeExecutionResult(Generic[T]):
    """
    探针执行结果包装器

    包含探针结果和执行信息。

    Attributes:
        probe_name: 探针名称
        result: 探针结果
        success: 是否成功
        error: 错误信息（如果失败）
        execution_time_ms: 执行时间（毫秒）
    """
    probe_name: str
    result: T
    success: bool = True
    error: Optional[str] = None
    execution_time_ms: float = 0.0

    @classmethod
    def from_error(cls, probe_name: str, error: str, default_result: T) -> "ProbeExecutionResult[T]":
        """从错误创建失败结果"""
        return cls(
            probe_name=probe_name,
            result=default_result,
            success=False,
            error=error,
        )


# ============================================================================
# 探针分类标签
# ============================================================================

class ProbeCategory:
    """探针分类常量"""

    # 趋势类探针
    TREND = "trend"
    # 波动性探针
    VOLATILITY = "volatility"
    # 周期性探针
    CYCLICAL = "cyclical"
    # 恶化检测探针
    DETERIORATION = "deterioration"
    # 拐点检测探针
    INFLECTION = "inflection"
    # 滚动窗口探针
    ROLLING = "rolling"
    # 稳健性探针
    ROBUST = "robust"
    # 多视野探针
    MULTI_HORIZON = "multi_horizon"


@dataclass(frozen=True)
class ProbeMetadata:
    """
    探针元数据

    用于探针注册和查询。

    Attributes:
        name: 探针名称
        category: 探针分类
        version: 探针版本
        author: 作者
        requires: 依赖的探针名称列表
        tags: 标签列表
    """
    name: str
    category: str
    version: str = "1.0.0"
    author: str = "AStock Analysis System"
    requires: tuple = field(default_factory=tuple)
    tags: tuple = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "version": self.version,
            "author": self.author,
            "requires": list(self.requires),
            "tags": list(self.tags),
        }
