"""Pipeline Protocols - Layer 2: Domain Protocols
================================================

面向特定领域的协议，组合 Core Protocols 形成更高级的抽象。

这些协议定义了 Pipeline 系统中的核心领域对象：
- Task: 工作流的基本单元
- IO: 输入输出管理
- Resource: 外部资源抽象
- Context: 执行上下文

设计原则：
    - 组合优于继承
    - 每个协议可独立使用
    - 支持渐进式实现
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
    Iterator,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    runtime_checkable,
)

from .core import (
    ExecutableProtocol,
    ExecutionResult,
    ExecutionStatus,
    ConfigurableProtocol,
    ConfigSchema,
    SerializableProtocol,
)

__all__ = [
    # 任务协议
    "TaskProtocol",
    "TaskInfo",
    "TaskCapabilities",

    # IO 协议
    "IOProtocol",
    "InputSpec",
    "OutputSpec",
    "TargetProtocol",

    # 资源协议
    "ResourceProtocol",
    "ResourceSpec",
    "ResourceLifecycle",

    # 上下文协议
    "ContextProtocol",
    "ExecutionContext",
]


# =============================================================================
# 类型变量
# =============================================================================

T = TypeVar("T")
InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
ResourceT = TypeVar("ResourceT")


# =============================================================================
# 任务能力标记
# =============================================================================

class TaskCapabilities(Enum):
    """任务能力枚举

    标记任务支持的特性，用于运行时能力发现。

    设计参考:
        - Airflow: Operator capabilities (template_fields, etc.)
        - Dagster: Op tags
    """
    # 执行特性
    RETRYABLE = auto()       # 支持重试
    TIMEOUT = auto()         # 支持超时
    CANCELLABLE = auto()     # 支持取消

    # 数据特性
    AGGREGATABLE = auto()    # 支持聚合 (PDDA)
    STREAMABLE = auto()      # 支持流式处理
    BATCHABLE = auto()       # 支持批处理

    # 缓存特性
    CACHEABLE = auto()       # 支持缓存
    IDEMPOTENT = auto()      # 幂等

    # 依赖特性
    DYNAMIC_DEPS = auto()    # 动态依赖
    PARALLEL = auto()        # 可并行执行


# =============================================================================
# 任务信息
# =============================================================================

@dataclass
class TaskInfo:
    """任务元数据

    描述任务的静态信息，不包含执行状态。

    设计参考:
        - Dagster: OpDefinition
        - Airflow: BaseOperator metadata
        - Prefect: Task metadata
    """
    # 标识
    name: str
    version: str = "1.0.0"

    # 分类
    category: str = "default"
    tags: Set[str] = field(default_factory=set)

    # 描述
    description: str = ""

    # 能力
    capabilities: Set[TaskCapabilities] = field(default_factory=set)

    # 配置
    retry_count: int = 0
    timeout_seconds: Optional[int] = None
    priority: int = 0

    # 来源
    source_module: Optional[str] = None
    source_callable: Optional[str] = None

    # 扩展元数据
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        """完整名称"""
        return f"{self.category}::{self.name}"

    def has_capability(self, cap: TaskCapabilities) -> bool:
        """检查是否具有某能力"""
        return cap in self.capabilities


# =============================================================================
# 输入输出规格
# =============================================================================

@dataclass
class InputSpec:
    """输入规格

    描述任务输入的期望格式。

    设计参考:
        - Dagster: In definition
        - Prefect: Parameter
        - Luigi: Parameter
    """
    name: str
    type: Type = Any
    required: bool = True
    default: Any = None
    description: str = ""

    # 来源追踪
    source_task: Optional[str] = None
    source_output: Optional[str] = None

    def validate(self, value: Any) -> bool:
        """验证输入值"""
        if value is None:
            return not self.required
        if self.type is not Any and not isinstance(value, self.type):
            return False
        return True


@dataclass
class OutputSpec:
    """输出规格

    描述任务输出的格式。

    设计参考:
        - Dagster: Out definition
        - Luigi: Target
    """
    name: str
    type: Type = Any
    description: str = ""

    # 持久化
    persist: bool = False
    storage_key: Optional[str] = None

    # PDDA 支持
    aggregation_key: Optional[str] = None
    is_aggregatable: bool = False


# =============================================================================
# Target 协议 (Luigi 风格)
# =============================================================================

@runtime_checkable
class TargetProtocol(Protocol):
    """目标协议

    定义任务输出的目标位置，支持存在性检查。

    设计参考:
        - Luigi: Target (核心抽象)
        - Dagster: IOManager output

    Target 是工作流系统中极其重要的抽象：
    - 它定义了输出的"位置"（文件、数据库、内存等）
    - 它支持存在性检查，实现智能缓存和跳过
    - 它解耦了任务执行和数据存储

    Example:
        class FileTarget(TargetProtocol):
            def __init__(self, path: str):
                self.path = path

            def exists(self) -> bool:
                return os.path.exists(self.path)

            def open(self, mode: str = 'r'):
                return open(self.path, mode)
    """

    def exists(self) -> bool:
        """检查目标是否存在

        Returns:
            是否存在
        """
        ...

    def path(self) -> str:
        """获取目标路径

        Returns:
            路径字符串
        """
        ...


# =============================================================================
# IO 协议
# =============================================================================

@runtime_checkable
class IOProtocol(Protocol[InputT, OutputT]):
    """IO 协议

    定义任务的输入输出管理接口。

    设计参考:
        - Dagster: IOManager (核心抽象)
        - Prefect: Result persistence
        - Luigi: Target management

    IOProtocol 是数据流的核心：
    - 统一管理任务的输入输出
    - 支持任意存储后端
    - 实现数据血缘追踪

    Example:
        class DuckDBIOManager(IOProtocol[Dict, DataFrame]):
            def load_input(self, spec: InputSpec, context=None) -> Dict:
                return duckdb.query(...).to_dict()

            def handle_output(self, spec: OutputSpec, value: DataFrame, context=None):
                duckdb.register("result", value)
    """

    def load_input(
        self,
        spec: InputSpec,
        context: Optional[Any] = None,
    ) -> InputT:
        """加载输入

        Args:
            spec: 输入规格
            context: 执行上下文

        Returns:
            输入数据
        """
        ...

    def handle_output(
        self,
        spec: OutputSpec,
        value: OutputT,
        context: Optional[Any] = None,
    ) -> None:
        """处理输出

        Args:
            spec: 输出规格
            value: 输出值
            context: 执行上下文
        """
        ...

    def get_output_target(self, spec: OutputSpec) -> Optional[TargetProtocol]:
        """获取输出目标

        Args:
            spec: 输出规格

        Returns:
            目标对象（如果支持）
        """
        ...


# =============================================================================
# 任务协议
# =============================================================================

@runtime_checkable
class TaskProtocol(Protocol[InputT, OutputT]):
    """任务协议

    定义工作流任务的完整接口。

    设计参考:
        - Dagster: Op (execution unit)
        - Luigi: Task (with requires/output/run)
        - Airflow: BaseOperator
        - Prefect: Task

    这是 Pipeline 中最重要的协议，它定义了任务的：
    - 元数据 (info)
    - 输入输出 (inputs/outputs)
    - 依赖 (requires)
    - 执行 (run)

    Example:
        class AnalyzeMetricTask(TaskProtocol[DataFrame, Dict]):
            @property
            def info(self) -> TaskInfo:
                return TaskInfo(
                    name="analyze_metric",
                    category="business_engine",
                    capabilities={TaskCapabilities.CACHEABLE}
                )

            def requires(self) -> List[str]:
                return ["load_data"]

            def run(self, input: DataFrame) -> ExecutionResult[Dict]:
                result = analyze(input)
                return ExecutionResult(status=ExecutionStatus.SUCCESS, value=result)
    """

    @property
    def info(self) -> TaskInfo:
        """获取任务元数据

        Returns:
            TaskInfo 对象
        """
        ...

    def inputs(self) -> List[InputSpec]:
        """获取输入规格

        Returns:
            输入规格列表
        """
        ...

    def outputs(self) -> List[OutputSpec]:
        """获取输出规格

        Returns:
            输出规格列表
        """
        ...

    def requires(self) -> List[str]:
        """获取依赖的任务名称

        类似 Luigi 的 requires() 方法。

        Returns:
            依赖任务名称列表
        """
        ...

    def run(
        self,
        input: InputT,
        context: Optional[Any] = None,
    ) -> ExecutionResult[OutputT]:
        """执行任务

        Args:
            input: 输入数据
            context: 执行上下文

        Returns:
            ExecutionResult 包装的输出
        """
        ...

    def output_target(self) -> Optional[TargetProtocol]:
        """获取输出目标

        类似 Luigi 的 output() 方法。
        用于检查任务是否需要执行（如果输出已存在则跳过）。

        Returns:
            目标对象，或 None（表示总是执行）
        """
        ...


# =============================================================================
# 资源生命周期
# =============================================================================

class ResourceLifecycle(Enum):
    """资源生命周期"""
    SINGLETON = auto()   # 全局单例
    FLOW = auto()        # 流程级别
    TASK = auto()        # 任务级别


# =============================================================================
# 资源规格
# =============================================================================

@dataclass
class ResourceSpec:
    """资源规格

    描述外部资源的配置。
    """
    name: str
    type: Type
    lifecycle: ResourceLifecycle = ResourceLifecycle.SINGLETON
    config: Dict[str, Any] = field(default_factory=dict)
    description: str = ""


# =============================================================================
# 资源协议
# =============================================================================

@runtime_checkable
class ResourceProtocol(Protocol[ResourceT]):
    """资源协议

    定义外部资源的管理接口。

    设计参考:
        - Dagster: Resources (核心抽象)
        - Airflow: Hooks/Connections
        - Prefect: Blocks

    资源是连接外部系统的桥梁：
    - 数据库连接
    - 文件系统
    - API 客户端
    - 消息队列

    Example:
        class DuckDBResource(ResourceProtocol[duckdb.DuckDBPyConnection]):
            def setup(self) -> duckdb.DuckDBPyConnection:
                return duckdb.connect(self.config.get("path", ":memory:"))

            def teardown(self, resource: duckdb.DuckDBPyConnection) -> None:
                resource.close()
    """

    @property
    def spec(self) -> ResourceSpec:
        """获取资源规格

        Returns:
            ResourceSpec 对象
        """
        ...

    def setup(self, config: Optional[Dict[str, Any]] = None) -> ResourceT:
        """初始化资源

        Args:
            config: 配置字典

        Returns:
            资源实例
        """
        ...

    def teardown(self, resource: ResourceT) -> None:
        """清理资源

        Args:
            resource: 资源实例
        """
        ...

    def health_check(self, resource: ResourceT) -> bool:
        """检查资源健康状态

        Args:
            resource: 资源实例

        Returns:
            是否健康
        """
        ...


# =============================================================================
# 执行上下文
# =============================================================================

@dataclass
class ExecutionContext:
    """执行上下文

    封装任务执行时需要的所有上下文信息。

    设计参考:
        - Dagster: OpExecutionContext
        - Airflow: Context dict
        - Prefect: FlowRunContext/TaskRunContext
    """
    # 运行标识
    flow_run_id: str
    task_run_id: str

    # 重试信息
    attempt: int = 1
    max_attempts: int = 1

    # 时间信息
    scheduled_time: Optional[float] = None
    start_time: Optional[float] = None

    # 日志
    log_level: str = "INFO"

    # 资源
    resources: Dict[str, Any] = field(default_factory=dict)

    # 配置
    config: Dict[str, Any] = field(default_factory=dict)

    # 父上下文（用于嵌套执行）
    parent: Optional["ExecutionContext"] = None

    # 扩展数据
    extra: Dict[str, Any] = field(default_factory=dict)

    def get_resource(self, name: str) -> Optional[Any]:
        """获取资源"""
        return self.resources.get(name)

    def with_resource(self, name: str, resource: Any) -> "ExecutionContext":
        """添加资源，返回新上下文"""
        new_resources = {**self.resources, name: resource}
        return ExecutionContext(
            flow_run_id=self.flow_run_id,
            task_run_id=self.task_run_id,
            attempt=self.attempt,
            max_attempts=self.max_attempts,
            scheduled_time=self.scheduled_time,
            start_time=self.start_time,
            log_level=self.log_level,
            resources=new_resources,
            config=self.config,
            parent=self.parent,
            extra=self.extra,
        )


# =============================================================================
# 上下文协议
# =============================================================================

@runtime_checkable
class ContextProtocol(Protocol):
    """上下文协议

    定义执行上下文的提供接口。

    设计参考:
        - Dagster: build_op_context
        - Prefect: get_run_context
        - Airflow: get_current_context

    Example:
        class FlowContextProvider(ContextProtocol):
            def get_context(self) -> ExecutionContext:
                return ExecutionContext(
                    flow_run_id=self._flow_run.id,
                    task_run_id="",
                    resources=self._resources,
                )
    """

    def get_context(self) -> ExecutionContext:
        """获取当前执行上下文

        Returns:
            ExecutionContext 对象
        """
        ...

    def update_context(self, **updates: Any) -> None:
        """更新上下文

        Args:
            **updates: 要更新的字段
        """
        ...

    def push_context(self, context: ExecutionContext) -> None:
        """压入新上下文（进入子任务）

        Args:
            context: 新上下文
        """
        ...

    def pop_context(self) -> Optional[ExecutionContext]:
        """弹出当前上下文（离开子任务）

        Returns:
            被弹出的上下文
        """
        ...
