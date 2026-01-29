"""Pipeline Protocols - Layer 1: Core Protocols
==============================================

最核心的抽象协议，定义系统中所有组件必须遵循的基础契约。

这些协议是**无状态的纯接口**，不包含任何业务逻辑。

设计原则：
    - 最小接口原则 (ISP): 每个协议只定义单一职责
    - 泛型优先: 支持类型推导和静态检查
    - 运行时可检查: 支持 isinstance() 检查
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    runtime_checkable,
)

__all__ = [
    # 执行协议
    "ExecutableProtocol",
    "ExecutionResult",
    "ExecutionStatus",

    # 解析协议
    "ResolvableProtocol",
    "ResolveResult",

    # 配置协议
    "ConfigurableProtocol",
    "ConfigSchema",

    # 序列化协议
    "SerializableProtocol",
]


# =============================================================================
# 类型变量
# =============================================================================

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ConfigT = TypeVar("ConfigT")


# =============================================================================
# 执行状态
# =============================================================================

class ExecutionStatus(Enum):
    """执行状态枚举

    类似 Airflow 的 TaskInstanceState，但更通用。
    """
    # 初始状态
    PENDING = auto()      # 等待执行
    QUEUED = auto()       # 已排队

    # 执行中
    RUNNING = auto()      # 执行中
    RETRYING = auto()     # 重试中

    # 终态
    SUCCESS = auto()      # 成功
    FAILED = auto()       # 失败
    SKIPPED = auto()      # 跳过
    CANCELLED = auto()    # 取消
    TIMEOUT = auto()      # 超时

    # 特殊状态
    UPSTREAM_FAILED = auto()  # 上游失败

    @property
    def is_terminal(self) -> bool:
        """是否是终态"""
        return self in {
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.SKIPPED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.TIMEOUT,
            ExecutionStatus.UPSTREAM_FAILED,
        }

    @property
    def is_success(self) -> bool:
        """是否成功"""
        return self == ExecutionStatus.SUCCESS


# =============================================================================
# 执行结果
# =============================================================================

@dataclass
class ExecutionResult(Generic[T]):
    """执行结果

    封装任务执行的结果、状态和元数据。

    设计参考:
        - Dagster: OpResult
        - Prefect: State
        - Airflow: TaskInstanceState + XCom

    泛型 T 表示结果数据的类型。

    Example:
        result = ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            value=my_dataframe,
            metadata={"rows": 1000, "columns": 10}
        )
    """
    status: ExecutionStatus
    value: Optional[T] = None
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # 性能指标
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None

    # 追踪信息
    trace_id: Optional[str] = None
    attempt: int = 1

    @property
    def is_success(self) -> bool:
        return self.status.is_success

    @property
    def is_failed(self) -> bool:
        return self.status == ExecutionStatus.FAILED

    def unwrap(self) -> T:
        """解包结果，失败时抛出异常"""
        if not self.is_success:
            if self.error:
                raise self.error
            raise RuntimeError(f"Execution failed with status: {self.status}")
        return self.value  # type: ignore

    def map(self, fn: Callable[[T], OutputT]) -> "ExecutionResult[OutputT]":
        """函数式映射"""
        if self.is_success and self.value is not None:
            return ExecutionResult(
                status=self.status,
                value=fn(self.value),
                metadata=self.metadata,
                start_time=self.start_time,
                end_time=self.end_time,
                duration_ms=self.duration_ms,
                trace_id=self.trace_id,
                attempt=self.attempt,
            )
        return ExecutionResult(
            status=self.status,
            error=self.error,
            metadata=self.metadata,
            trace_id=self.trace_id,
            attempt=self.attempt,
        )


# =============================================================================
# 可执行协议
# =============================================================================

@runtime_checkable
class ExecutableProtocol(Protocol[InputT, OutputT]):
    """可执行协议

    定义任何可执行单元的标准接口。

    设计参考:
        - Dagster: Op (execute method)
        - Luigi: Task (run method)
        - Airflow: BaseOperator (execute method)

    这是 Pipeline 中最核心的协议，任何可执行的任务、操作、函数
    都应该实现这个协议。

    Example:
        class MyTask(ExecutableProtocol[Dict, DataFrame]):
            def execute(self, input: Dict, context=None) -> ExecutionResult[DataFrame]:
                # 执行逻辑
                return ExecutionResult(status=ExecutionStatus.SUCCESS, value=df)
    """

    def execute(
        self,
        input: InputT,
        context: Optional[Any] = None,
    ) -> ExecutionResult[OutputT]:
        """执行任务

        Args:
            input: 输入数据
            context: 执行上下文 (可选)

        Returns:
            ExecutionResult 包装的输出
        """
        ...

    def validate_input(self, input: InputT) -> bool:
        """验证输入

        Args:
            input: 输入数据

        Returns:
            是否有效
        """
        ...


# =============================================================================
# 解析结果
# =============================================================================

@dataclass
class ResolveResult(Generic[T]):
    """解析结果

    封装解析操作的结果。
    """
    success: bool
    value: Optional[T] = None
    error: Optional[str] = None
    source: Optional[str] = None  # 解析来源

    @classmethod
    def ok(cls, value: T, source: Optional[str] = None) -> "ResolveResult[T]":
        """创建成功结果"""
        return cls(success=True, value=value, source=source)

    @classmethod
    def fail(cls, error: str) -> "ResolveResult[T]":
        """创建失败结果"""
        return cls(success=False, error=error)

    def unwrap(self) -> T:
        """解包结果，失败时抛出异常"""
        if not self.success:
            raise ValueError(self.error or "Resolution failed")
        return self.value  # type: ignore


# =============================================================================
# 可解析协议
# =============================================================================

@runtime_checkable
class ResolvableProtocol(Protocol[T]):
    """可解析协议

    定义可以被解析为具体值的对象的接口。

    设计参考:
        - Dagster: ConfigMapping, Resource resolution
        - Luigi: Parameter resolution
        - Prefect: Result resolution

    典型应用:
        - 方法名 → 可调用对象
        - 配置键 → 配置值
        - 引用表达式 → 实际数据

    Example:
        class ConfigResolver(ResolvableProtocol[Dict]):
            def resolve(self, key: str) -> ResolveResult[Dict]:
                config = load_config(key)
                return ResolveResult.ok(config)
    """

    def resolve(self, key: str, **hints: Any) -> ResolveResult[T]:
        """解析键到值

        Args:
            key: 要解析的键
            **hints: 解析提示 (如类型提示、上下文等)

        Returns:
            ResolveResult 包装的解析结果
        """
        ...

    def can_resolve(self, key: str) -> bool:
        """检查是否可以解析该键

        Args:
            key: 要检查的键

        Returns:
            是否可以解析
        """
        ...


# =============================================================================
# 配置模式
# =============================================================================

@dataclass
class ConfigSchema:
    """配置模式定义

    描述配置项的结构、类型和约束。

    设计参考:
        - Dagster: Config schema with Pydantic
        - Prefect: Parameter validation
    """
    name: str
    type: Type
    required: bool = True
    default: Any = None
    description: str = ""
    validators: List[Callable[[Any], bool]] = field(default_factory=list)

    def validate(self, value: Any) -> bool:
        """验证值是否符合模式"""
        if value is None:
            return not self.required

        if not isinstance(value, self.type):
            return False

        return all(v(value) for v in self.validators)


# =============================================================================
# 可配置协议
# =============================================================================

@runtime_checkable
class ConfigurableProtocol(Protocol[ConfigT]):
    """可配置协议

    定义可以接受和验证配置的对象的接口。

    设计参考:
        - Dagster: Configurable resources and ops
        - Prefect: Task configuration
        - Airflow: Operator kwargs

    Example:
        class MyProcessor(ConfigurableProtocol[ProcessorConfig]):
            def configure(self, config: ProcessorConfig) -> None:
                self._threshold = config.threshold

            def get_config_schema(self) -> List[ConfigSchema]:
                return [
                    ConfigSchema(name="threshold", type=float, default=0.5)
                ]
    """

    def configure(self, config: ConfigT) -> None:
        """应用配置

        Args:
            config: 配置对象
        """
        ...

    def get_config_schema(self) -> List[ConfigSchema]:
        """获取配置模式

        Returns:
            配置项模式列表
        """
        ...

    def validate_config(self, config: ConfigT) -> bool:
        """验证配置

        Args:
            config: 配置对象

        Returns:
            是否有效
        """
        ...


# =============================================================================
# 可序列化协议
# =============================================================================

@runtime_checkable
class SerializableProtocol(Protocol):
    """可序列化协议

    定义可以被序列化和反序列化的对象的接口。

    设计参考:
        - Airflow: XCom serialization
        - Prefect: Result serialization
        - Dagster: IOManager serialization

    Example:
        class MyState(SerializableProtocol):
            def to_dict(self) -> Dict[str, Any]:
                return {"value": self.value}

            @classmethod
            def from_dict(cls, data: Dict[str, Any]) -> "MyState":
                return cls(value=data["value"])
    """

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典

        Returns:
            字典表示
        """
        ...

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """从字典反序列化

        Args:
            data: 字典数据

        Returns:
            反序列化的对象
        """
        ...

    def to_json(self) -> str:
        """序列化为 JSON 字符串

        Returns:
            JSON 字符串
        """
        ...

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """从 JSON 字符串反序列化

        Args:
            json_str: JSON 字符串

        Returns:
            反序列化的对象
        """
        ...
