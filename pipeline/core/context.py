"""Pipeline Context - 共享的执行上下文

通过上下文对象减少服务间的紧耦合，实现依赖反转。

职责：
- 封装配置解析和执行过程中的共享状态
- 存储和管理 DependencyGraph（单一构建，多处复用）
- 提供运行时状态存储
- 提供统一的引用注册和解析 API（委托给 contracts.store.DataStore）
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
import os

if TYPE_CHECKING:
    from .dependency_graph import DependencyGraph, ExecutionPlan

# 统一数据存储与引用异常
from shared.contracts.store import DataStore, ReferenceResolver, ReferenceNotFoundError
from .runtime_models import FlowRun, StepRun, RunStatus, RetryPolicy, FailurePolicy


@dataclass
class StepOutput:
    """步骤输出定义"""
    name: str
    source_key: str | None = None
    global_key: str | None = None

@dataclass
class StepSpec:
    """步骤规范"""
    name: str
    component: str
    engine: str
    methods: List[str]
    raw_parameters: Dict[str, Any] = field(default_factory=dict)
    outputs: List[StepOutput] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)  # 显式依赖声明
    retry_policy: RetryPolicy | None = None
    failure_policy: FailurePolicy | None = None


@dataclass
class PipelineContext:
    """Pipeline 执行上下文

    封装配置解析和执行过程中的共享状态，避免服务层直接依赖 ExecuteManager。
    数据存储统一到 DataStore，引用解析统一使用 ReferenceResolver。
    """

    # 配置数据
    config: Dict[str, Any] = field(default_factory=dict)

    # 步骤相关
    steps: Dict[str, StepSpec] = field(default_factory=dict)
    execution_order: List[str] = field(default_factory=list)

    # 运行时状态
    _runtime_state: Dict[str, Any] = field(default_factory=dict)

    # Flow / Step 运行视图（v4.0 运行时模型）
    _flow_run: Optional[FlowRun] = field(default=None, repr=False)
    _step_runs: Dict[str, StepRun] = field(default_factory=dict, repr=False)

    # 统一数据存储（单一真相源）
    _data_store: Optional[DataStore] = field(default=None, repr=False)
    _resolver: Optional[ReferenceResolver] = field(default=None, repr=False)

    def __post_init__(self):
        """初始化 DataStore"""
        if self._data_store is None:
            self._data_store = DataStore()
        if self._resolver is None:
            self._resolver = ReferenceResolver(self._data_store)
            # 注册步骤引用模式
            self._resolver.register_pattern(
                template='steps.{step}.outputs.parameters.{param}',
                handler='step_output',
            )

    @property
    def data_store(self) -> DataStore:
        """获取数据存储"""
        if self._data_store is None:
            self._data_store = DataStore()
        return self._data_store

    @property
    def resolver(self) -> ReferenceResolver:
        """获取引用解析器"""
        if self._resolver is None:
            self._resolver = ReferenceResolver(self.data_store)
            self._resolver.register_pattern(
                template='steps.{step}.outputs.parameters.{param}',
                handler='step_output',
            )
        return self._resolver

    # ================== 依赖图管理 ==================

    def set_dependency_graph(self, graph: 'DependencyGraph') -> None:
        """存储依赖图（由 ConfigService 构建后调用）

        确保依赖图只构建一次，多处复用。

        Args:
            graph: 构建好的 DependencyGraph 实例
        """
        self._runtime_state['_dependency_graph'] = graph

    def get_dependency_graph(self) -> Optional['DependencyGraph']:
        """获取依赖图

        Returns:
            DependencyGraph 实例，如果未设置则返回 None
        """
        return self._runtime_state.get('_dependency_graph')

    def set_execution_plan(self, plan: 'ExecutionPlan') -> None:
        """存储执行计划

        Args:
            plan: 依赖图生成的执行计划
        """
        self._runtime_state['execution_plan'] = plan

    def get_execution_plan(self) -> Optional['ExecutionPlan']:
        """获取执行计划

        Returns:
            ExecutionPlan 实例，如果未设置则返回 None
        """
        return self._runtime_state.get('execution_plan')

    # ================== 通用运行时状态 ==================

    def dataset_name(self, step: str, output: str) -> str:
        """生成数据集名称"""
        return f"{step}__{output}".replace('-', '_')

    def clear_steps(self) -> None:
        """清空步骤相关数据"""
        self.steps.clear()
        self.execution_order.clear()

    # ================== 引用注册 API ==================

    def register_reference(self, ref: str, value: Any) -> str:
        """注册引用值到 DataStore

        Args:
            ref: 引用路径 (如 steps.step1.outputs.parameters.result)
            value: 引用值

        Returns:
            生成的数据指纹
        """
        # 优先通过 ReferenceResolver 的路由解析 ref
        parsed = self.resolver.parse_ref(ref) or {}
        step_id = parsed.get('step')
        param_id = parsed.get('param')

        if step_id and param_id:
            key = self.dataset_name(step_id, param_id)
            entry = self.data_store.put(key, value, ref=ref, producer_step=step_id)
            return entry.fingerprint

        # 非标准格式：直接使用 ref 作为 key
        entry = self.data_store.put(ref, value, ref=ref)
        return entry.fingerprint

    def get_reference(self, ref: str) -> Optional[Any]:
        """获取引用值

        Args:
            ref: 引用路径

        Returns:
            引用值，如果不存在返回 None
        """
        return self.data_store.get_by_ref(ref)

    def get_by_hash(self, h: str) -> Optional[Any]:
        """通过哈希获取引用值

        Args:
            h: 数据指纹/哈希

        Returns:
            引用值，如果不存在返回 None
        """
        return self.data_store.get_by_hash(h)

    def resolve_references(self, params: Dict[str, Any], *, strict: bool = False) -> Dict[str, Any]:
        """解析参数中的引用

        使用 ReferenceResolver + 上下文信息递归解析参数。

        支持三类引用：
        - steps.*: 通过 DataStore 按 ref/hash 查找上游输出
        - config.*: 从 ctx.config 读取配置值
        - env.*: 从环境变量读取值

        Args:
            params: 包含引用的参数字典
            strict: 严格模式；为 True 时，引用未解析将抛出 ReferenceNotFoundError

        Returns:
            解析后的参数字典
        """

        ref_key = self.resolver.REF_KEY
        hash_key_name = self.resolver.HASH_KEY

        def _walk(value: Any) -> Any:
            # 引用字典: {"__ref__": "...", "__hash__": "..."}
            if isinstance(value, dict) and ref_key in value:
                ref = value.get(ref_key, '')
                h = value.get(hash_key_name, '')

                # 1) config.* → 从配置中读取
                if isinstance(ref, str) and ref.startswith('config.'):
                    # 形如 config.section.key[.subkey...]
                    parts = ref.split('.', 2)
                    if len(parts) >= 3:
                        section, key = parts[1], parts[2]
                        section_obj = self.config.get(section) or {}
                        if isinstance(section_obj, dict) and key in section_obj:
                            return section_obj[key]

                # 2) env.* → 环境变量
                if isinstance(ref, str) and ref.startswith('env.'):
                    var_name = ref[len('env.') :]
                    env_val = os.environ.get(var_name)
                    if env_val is not None:
                        return env_val

                # 3) 其它引用统一委托给 ReferenceResolver (DataStore)
                resolved = self.resolver.resolve_ref(str(ref), str(h) if h else '')
                if resolved is None:
                    if strict:
                        raise ReferenceNotFoundError(str(ref))
                    return None
                return resolved

            # 普通 dict / list 递归处理
            if isinstance(value, dict):
                return {k: _walk(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_walk(v) for v in value]

            return value

        return _walk(params)

    def set_runtime_value(self, key: str, value: Any):
        """设置运行时状态值"""
        self._runtime_state[key] = value

    def get_runtime_value(self, key: str, default: Any = None) -> Any:
        """获取运行时状态值"""
        return self._runtime_state.get(key, default)

    # ================== 运行时视图（FlowRun / StepRun） ==================

    def start_flow_run(self, run_id: Optional[str] = None) -> FlowRun:
        """创建并启动 FlowRun

        Args:
            run_id: 运行 ID，可选；未提供时自动生成
        """
        if run_id is None:
            run_id = f"flow-{datetime.now().isoformat()}"
        flow_run = FlowRun(run_id=run_id)
        flow_run.step_order = list(self.execution_order)
        flow_run.mark_started()
        self._flow_run = flow_run
        self._runtime_state['flow_run_id'] = run_id
        return flow_run

    def get_flow_run(self) -> Optional[FlowRun]:
        """获取当前 FlowRun（可能为 None）"""
        return self._flow_run

    def finish_flow_run(self, status: RunStatus, error: Optional[str] = None) -> None:
        """结束 FlowRun 并设置状态"""
        if not self._flow_run:
            # 如果不存在，创建一个最小视图
            self._flow_run = FlowRun(run_id=f"flow-{datetime.now().isoformat()}")
            self._flow_run.mark_started()
        self._flow_run.mark_finished(status=status, error=error)

    def get_or_create_step_run(self, name: str) -> StepRun:
        """获取或创建 StepRun"""
        if name not in self._step_runs:
            self._step_runs[name] = StepRun(name=name)
        return self._step_runs[name]

    def mark_step_started(self, name: str) -> StepRun:
        step = self.get_or_create_step_run(name)
        step.mark_started()
        return step

    def mark_step_finished(
        self,
        name: str,
        status: RunStatus,
        *,
        error: Optional[str] = None,
        duration_ms: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StepRun:
        step = self.get_or_create_step_run(name)
        step.mark_finished(status=status, error=error, duration_ms=duration_ms)
        if metadata:
            step.metadata.update(metadata)
        return step

    def get_step_runs(self) -> Dict[str, StepRun]:
        """获取所有 StepRun 视图"""
        return self._step_runs

    # ================== 高级状态管理 ==================

    def reset(self) -> None:
        """重置上下文到初始状态

        用于清理执行后的状态，便于重新执行或测试。
        """
        self.config.clear()
        self.steps.clear()
        self.execution_order.clear()
        self._runtime_state.clear()
        self._flow_run = None
        self._step_runs.clear()
        if self._data_store is not None:
            self._data_store.clear()

    def clone(self) -> 'PipelineContext':
        """创建上下文的深拷贝

        用于并行执行或快照保存。

        Returns:
            新的 PipelineContext 实例
        """
        import copy
        new_ctx = PipelineContext(
            config=copy.deepcopy(self.config),
            steps=copy.deepcopy(self.steps),
            execution_order=list(self.execution_order),
        )
        new_ctx._runtime_state = copy.deepcopy(self._runtime_state)
        # 运行视图为新的空实例，避免在不同上下文间共享引用
        new_ctx._flow_run = None
        new_ctx._step_runs = {}
        new_ctx._data_store = DataStore()
        new_ctx._resolver = ReferenceResolver(new_ctx._data_store)
        new_ctx._resolver.register_pattern(
            template='steps.{step}.outputs.parameters.{param}',
            handler='step_output',
        )
        return new_ctx

    def get_step_count(self) -> int:
        """获取步骤数量"""
        return len(self.steps)

    def get_stats(self) -> Dict[str, Any]:
        """获取上下文统计信息

        Returns:
            包含各种统计数据的字典
        """
        graph = self.get_dependency_graph()
        plan = self.get_execution_plan()
        return {
            'step_count': len(self.steps),
            'execution_order_length': len(self.execution_order),
            'data_store_size': len(self._data_store) if self._data_store else 0,
            'reference_count': sum(1 for _ in self.data_store.refs()) if self._data_store else 0,
            'runtime_state_keys': list(self._runtime_state.keys()),
            'dependency_graph_nodes': len(graph) if graph else 0,
            'execution_plan_depth': plan.depth if plan else 0,
            'max_parallelism': plan.max_parallelism if plan else 0,
            'has_flow_run': self._flow_run is not None,
            'step_run_count': len(self._step_runs),
        }

__all__ = ['PipelineContext', 'StepSpec', 'StepOutput']
