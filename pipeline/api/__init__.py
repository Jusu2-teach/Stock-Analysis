"""Pipeline Public API
====================

用户入口模块，提供：
- 协议定义 (业务代码需遵循)
- 公开类型 (TaskResult, FlowResult...)
- 装饰器 (@flow, @task)

使用示例：
    from pipeline.api import AggregatableResult, TaskResult
    from pipeline.api import flow, task

版本: 2.0.0
"""

from .protocols import (
    # 聚合协议
    Aggregatable,
    AggregatableResult,
    # 数据集协议
    Dataset,
    # 执行协议
    TaskCallable,
)

from .types import (
    # 结果类型
    TaskResult,
    FlowResult,
    # 状态类型
    TaskStatus,
    FlowStatus,
    # 输入输出类型
    InputSpec,
    OutputSpec,
)

__all__ = [
    # Protocols
    'Aggregatable',
    'AggregatableResult',
    'Dataset',
    'TaskCallable',
    # Types
    'TaskResult',
    'FlowResult',
    'TaskStatus',
    'FlowStatus',
    'InputSpec',
    'OutputSpec',
]
