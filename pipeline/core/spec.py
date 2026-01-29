"""Pipeline Core Models - Specification
======================================

定义不可变的配置规范对象 (Specification)。
Spec 对象从 YAML 配置解析生成，一旦创建不可修改。

设计原则：
- 不可变 (frozen=True) - 配置加载后不应改变
- 完整验证 - 在 __post_init__ 中验证所有约束
- 自描述 - 每个字段都有清晰的文档

版本: 2.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

from .policy import (
    AggregationPolicy,
    CachePolicy,
    FailurePolicy,
    RetryPolicy,
    TaskPolicies,
    TimeoutPolicy,
)


# =============================================================================
# 任务规范
# =============================================================================

@dataclass(frozen=True)
class TaskInputSpec:
    """任务输入规范

    Attributes:
        name: 输入名称 (参数名)
        source: 数据来源 (如 "steps.load_data.outputs.raw_data")
        type_hint: 类型提示
        required: 是否必需
        default: 默认值
    """
    name: str
    source: Optional[str] = None  # None 表示从参数传入
    type_hint: str = "Any"
    required: bool = True
    default: Any = None


@dataclass(frozen=True)
class TaskOutputSpec:
    """任务输出规范

    Attributes:
        name: 输出名称
        type_hint: 类型提示
        primary: 是否为主输出 (用于链式调用)
    """
    name: str
    type_hint: str = "Any"
    primary: bool = False


# 注意: TaskPolicies 从 policy.py 导入，包含完整的策略定义（含 aggregation）


@dataclass(frozen=True)
class TaskSpec:
    """任务配置规范 (不可变)

    完整描述一个任务的配置信息，从 YAML 或装饰器生成。

    Attributes:
        name: 任务唯一名称
        component: 组件类型 (如 "business_engine")
        engine: 执行引擎 (如 "duckdb")
        method: 方法名称
        inputs: 输入规范列表
        outputs: 输出规范列表
        parameters: 静态参数 (不含引用)
        depends_on: 显式依赖的任务名称
        policies: 策略配置
        tags: 标签集合
        description: 任务描述
        metadata: 额外元数据

    Example (YAML):
        - name: analyze_roic
          component: business_engine
          engine: duckdb
          method: analyze_metric_trend
          parameters:
            metric_name: roic
          inputs:
            - name: data
              source: steps.load_data.outputs.raw_data
          outputs:
            - name: trend_result
              primary: true
          policies:
            cache:
              enabled: true
              ttl: 3600
    """
    name: str
    component: str
    engine: str
    method: str
    inputs: Tuple[TaskInputSpec, ...] = field(default_factory=tuple)
    outputs: Tuple[TaskOutputSpec, ...] = field(default_factory=tuple)
    parameters: Mapping[str, Any] = field(default_factory=dict)
    depends_on: FrozenSet[str] = field(default_factory=frozenset)
    policies: TaskPolicies = field(default_factory=TaskPolicies)
    tags: FrozenSet[str] = field(default_factory=frozenset)
    description: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """验证任务规范"""
        if not self.name:
            raise ValueError("TaskSpec.name cannot be empty")
        if not self.component:
            raise ValueError(f"TaskSpec[{self.name}].component cannot be empty")
        if not self.method:
            raise ValueError(f"TaskSpec[{self.name}].method cannot be empty")

        # 确保不可变性
        if not isinstance(self.inputs, tuple):
            object.__setattr__(self, 'inputs', tuple(self.inputs))
        if not isinstance(self.outputs, tuple):
            object.__setattr__(self, 'outputs', tuple(self.outputs))
        if not isinstance(self.depends_on, frozenset):
            object.__setattr__(self, 'depends_on', frozenset(self.depends_on))
        if not isinstance(self.tags, frozenset):
            object.__setattr__(self, 'tags', frozenset(self.tags))

    def get_primary_output(self) -> Optional[str]:
        """获取主输出名称"""
        for out in self.outputs:
            if out.primary:
                return out.name
        # 如果没有标记 primary，返回第一个输出
        return self.outputs[0].name if self.outputs else None

    def get_input_sources(self) -> Dict[str, str]:
        """获取所有输入的数据来源映射"""
        return {
            inp.name: inp.source
            for inp in self.inputs
            if inp.source is not None
        }

    def get_output_names(self) -> List[str]:
        """获取所有输出名称"""
        return [out.name for out in self.outputs]


# =============================================================================
# 流程规范
# =============================================================================

@dataclass(frozen=True)
class FlowDefaults:
    """流程默认配置

    这些配置可被任务级配置覆盖。

    Attributes:
        retry: 默认重试策略
        cache: 默认缓存策略
        timeout: 默认超时策略
        failure: 默认失败策略
    """
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    cache: CachePolicy = field(default_factory=CachePolicy)
    timeout: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    failure: FailurePolicy = field(default_factory=FailurePolicy)


@dataclass(frozen=True)
class FlowOrchestration:
    """流程编排配置

    Attributes:
        granularity: 执行粒度
            - 'task': 按单个任务调度（默认）
            - 'layer': 按 DAG 层级调度（同层并行）
        soft_fail: 是否启用软失败 (任务失败不终止流程)
        max_parallelism: 最大并行度 (0 表示不限制)
        task_runner: 任务运行器类型 ('sequential', 'threaded', 'parallel', 'async')
    """
    granularity: str = "task"
    soft_fail: bool = False
    max_parallelism: int = 4
    task_runner: str = "sequential"

    # 支持的 task_runner 值
    _VALID_TASK_RUNNERS = frozenset({'sequential', 'threaded', 'parallel', 'async'})

    def __post_init__(self):
        if self.granularity not in ('task', 'layer'):
            raise ValueError(
                f"Invalid granularity: '{self.granularity}'. "
                f"Must be 'task' or 'layer'."
            )
        if self.task_runner not in self._VALID_TASK_RUNNERS:
            raise ValueError(
                f"Invalid task_runner: '{self.task_runner}'. "
                f"Must be one of: {sorted(self._VALID_TASK_RUNNERS)}"
            )


@dataclass(frozen=True)
class FlowSpec:
    """流程配置规范 (不可变)

    完整描述一个工作流的配置信息。

    Attributes:
        name: 流程名称
        tasks: 任务规范列表 (按定义顺序)
        defaults: 默认策略配置
        orchestration: 编排配置
        description: 流程描述
        version: 版本号
        metadata: 额外元数据

    Example (YAML):
        apiVersion: astock/v1
        kind: Flow
        metadata:
          name: financial-analysis
        spec:
          defaults:
            cache:
              enabled: true
          orchestration:
            soft_fail: true
            max_parallelism: 4
          tasks:
            - name: load_data
              ...
    """
    name: str
    tasks: Tuple[TaskSpec, ...] = field(default_factory=tuple)
    defaults: FlowDefaults = field(default_factory=FlowDefaults)
    orchestration: FlowOrchestration = field(default_factory=FlowOrchestration)
    description: str = ""
    version: str = "1.0.0"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """验证流程规范"""
        if not self.name:
            raise ValueError("FlowSpec.name cannot be empty")

        # 确保不可变性
        if not isinstance(self.tasks, tuple):
            object.__setattr__(self, 'tasks', tuple(self.tasks))

        # 验证任务名称唯一性
        task_names = [t.name for t in self.tasks]
        if len(task_names) != len(set(task_names)):
            duplicates = [n for n in task_names if task_names.count(n) > 1]
            raise ValueError(f"Duplicate task names: {set(duplicates)}")

    def get_task(self, name: str) -> Optional[TaskSpec]:
        """根据名称获取任务规范"""
        for task in self.tasks:
            if task.name == name:
                return task
        return None

    def get_task_names(self) -> List[str]:
        """获取所有任务名称 (按定义顺序)"""
        return [t.name for t in self.tasks]

    def get_tasks_by_tag(self, tag: str) -> List[TaskSpec]:
        """根据标签筛选任务"""
        return [t for t in self.tasks if tag in t.tags]


# =============================================================================
# 构建器 (Builder Pattern)
# =============================================================================

class TaskSpecBuilder:
    """TaskSpec 构建器

    提供流式 API 构建 TaskSpec。

    Example:
        spec = (TaskSpecBuilder()
            .name("analyze_roic")
            .component("business_engine")
            .engine("duckdb")
            .method("analyze_metric_trend")
            .add_input("data", source="steps.load_data.outputs.raw_data")
            .add_output("trend_result", primary=True)
            .with_cache(enabled=True, ttl=3600)
            .build())
    """

    def __init__(self):
        self._name: str = ""
        self._component: str = ""
        self._engine: str = ""
        self._method: str = ""
        self._inputs: List[TaskInputSpec] = []
        self._outputs: List[TaskOutputSpec] = []
        self._parameters: Dict[str, Any] = {}
        self._depends_on: List[str] = []
        self._retry: RetryPolicy = RetryPolicy()
        self._cache: CachePolicy = CachePolicy()
        self._timeout: TimeoutPolicy = TimeoutPolicy()
        self._failure: FailurePolicy = FailurePolicy()
        self._tags: List[str] = []
        self._description: str = ""
        self._metadata: Dict[str, Any] = {}

    def name(self, value: str) -> 'TaskSpecBuilder':
        self._name = value
        return self

    def component(self, value: str) -> 'TaskSpecBuilder':
        self._component = value
        return self

    def engine(self, value: str) -> 'TaskSpecBuilder':
        self._engine = value
        return self

    def method(self, value: str) -> 'TaskSpecBuilder':
        self._method = value
        return self

    def add_input(
        self,
        name: str,
        source: Optional[str] = None,
        type_hint: str = "Any",
        required: bool = True,
        default: Any = None
    ) -> 'TaskSpecBuilder':
        self._inputs.append(TaskInputSpec(
            name=name,
            source=source,
            type_hint=type_hint,
            required=required,
            default=default,
        ))
        return self

    def add_output(
        self,
        name: str,
        type_hint: str = "Any",
        primary: bool = False
    ) -> 'TaskSpecBuilder':
        self._outputs.append(TaskOutputSpec(
            name=name,
            type_hint=type_hint,
            primary=primary,
        ))
        return self

    def parameter(self, key: str, value: Any) -> 'TaskSpecBuilder':
        self._parameters[key] = value
        return self

    def parameters(self, params: Dict[str, Any]) -> 'TaskSpecBuilder':
        self._parameters.update(params)
        return self

    def depends(self, *task_names: str) -> 'TaskSpecBuilder':
        self._depends_on.extend(task_names)
        return self

    def with_retry(
        self,
        max_attempts: int = 3,
        delay_seconds: float = 1.0,
        backoff: str = "exponential"
    ) -> 'TaskSpecBuilder':
        self._retry = RetryPolicy(
            max_attempts=max_attempts,
            delay_seconds=delay_seconds,
            backoff=backoff,
        )
        return self

    def with_cache(
        self,
        enabled: bool = True,
        ttl: Optional[int] = None,
        backend: str = "memory"
    ) -> 'TaskSpecBuilder':
        self._cache = CachePolicy(
            enabled=enabled,
            ttl_seconds=ttl,
            backend=backend,
        )
        return self

    def with_timeout(self, seconds: int) -> 'TaskSpecBuilder':
        self._timeout = TimeoutPolicy(timeout_seconds=seconds)
        return self

    def tag(self, *tags: str) -> 'TaskSpecBuilder':
        self._tags.extend(tags)
        return self

    def description(self, value: str) -> 'TaskSpecBuilder':
        self._description = value
        return self

    def build(self) -> TaskSpec:
        """构建 TaskSpec"""
        return TaskSpec(
            name=self._name,
            component=self._component,
            engine=self._engine,
            method=self._method,
            inputs=tuple(self._inputs),
            outputs=tuple(self._outputs),
            parameters=self._parameters,
            depends_on=frozenset(self._depends_on),
            policies=TaskPolicies(
                retry=self._retry,
                cache=self._cache,
                timeout=self._timeout,
                failure=self._failure,
            ),
            tags=frozenset(self._tags),
            description=self._description,
            metadata=self._metadata,
        )
