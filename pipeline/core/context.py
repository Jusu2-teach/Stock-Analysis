"""Pipeline Context - 共享的执行上下文

通过上下文对象减少服务间的紧耦合，实现依赖反转。

职责：
- 封装配置解析和执行过程中的共享状态
- 存储和管理 DependencyGraph（单一构建，多处复用）
- 提供运行时状态存储
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from .dependency_graph import DependencyGraph, ExecutionPlan


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


@dataclass
class PipelineContext:
    """Pipeline 执行上下文

    封装配置解析和执行过程中的共享状态，避免服务层直接依赖 ExecuteManager。
    """

    # 配置数据
    config: Dict[str, Any] = field(default_factory=dict)

    # 步骤相关
    steps: Dict[str, StepSpec] = field(default_factory=dict)
    execution_order: List[str] = field(default_factory=list)

    # 引用相关
    reference_values: Dict[str, Any] = field(default_factory=dict)
    reference_to_hash: Dict[str, str] = field(default_factory=dict)
    global_registry: Dict[str, Any] = field(default_factory=dict)

    # 运行时状态 (新增)
    _runtime_state: Dict[str, Any] = field(default_factory=dict)

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

    def set_runtime_value(self, key: str, value: Any):
        """设置运行时状态值"""
        self._runtime_state[key] = value

    def get_runtime_value(self, key: str, default: Any = None) -> Any:
        """获取运行时状态值"""
        return self._runtime_state.get(key, default)

    # ================== 高级状态管理 ==================

    def reset(self) -> None:
        """重置上下文到初始状态

        用于清理执行后的状态，便于重新执行或测试。
        """
        self.config.clear()
        self.steps.clear()
        self.execution_order.clear()
        self.reference_values.clear()
        self.reference_to_hash.clear()
        self.global_registry.clear()
        self._runtime_state.clear()

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
            reference_values=copy.deepcopy(self.reference_values),
            reference_to_hash=dict(self.reference_to_hash),
            global_registry=copy.deepcopy(self.global_registry),
        )
        new_ctx._runtime_state = copy.deepcopy(self._runtime_state)
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
            'reference_count': len(self.reference_values),
            'global_registry_size': len(self.global_registry),
            'runtime_state_keys': list(self._runtime_state.keys()),
            'dependency_graph_nodes': len(graph) if graph else 0,
            'execution_plan_depth': plan.depth if plan else 0,
            'max_parallelism': plan.max_parallelism if plan else 0,
        }

__all__ = ['PipelineContext', 'StepSpec', 'StepOutput']
