"""Pipeline Context - 共享的执行上下文

通过上下文对象减少服务间的紧耦合，实现依赖反转。
"""
from __future__ import annotations
from typing import Any, Dict, List
from dataclasses import dataclass, field


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


__all__ = ['PipelineContext', 'StepSpec', 'StepOutput']
