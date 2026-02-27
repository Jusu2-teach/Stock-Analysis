"""Pipeline Public Types
=======================

定义 Pipeline 对外暴露的数据类型。
这些类型用于任务输入输出、执行结果等。

设计原则：
- 不可变 (frozen=True) - 保证线程安全
- 类型完备 - 充分利用 Python 类型系统
- 文档清晰 - 每个字段都有说明

版本: 2.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

# =============================================================================
# 状态枚举 - 从 core.state 统一导入，避免重复定义
# =============================================================================

from ..core.state import TaskState, FlowState

# 向后兼容别名：API 层使用 Status 命名约定
TaskStatus = TaskState
FlowStatus = FlowState


# =============================================================================
# 输入输出规范
# =============================================================================

@dataclass(frozen=True)
class InputSpec:
    """输入规范

    Attributes:
        name: 输入名称
        type_hint: 类型提示 (如 'DataFrame', 'str')
        required: 是否必需
        default: 默认值
        description: 描述
    """
    name: str
    type_hint: str = "Any"
    required: bool = True
    default: Any = None
    description: str = ""

    def __post_init__(self):
        if not self.name:
            raise ValueError("InputSpec.name cannot be empty")


@dataclass(frozen=True)
class OutputSpec:
    """输出规范

    Attributes:
        name: 输出名称
        type_hint: 类型提示
        primary: 是否为主输出
        description: 描述
    """
    name: str
    type_hint: str = "Any"
    primary: bool = False
    description: str = ""

    def __post_init__(self):
        if not self.name:
            raise ValueError("OutputSpec.name cannot be empty")


# =============================================================================
# 执行结果
# =============================================================================

@dataclass(frozen=True)
class TaskResult:
    """任务执行结果

    Attributes:
        task_name: 任务名称
        status: 执行状态
        value: 返回值 (成功时)
        error: 错误信息 (失败时)
        started_at: 开始时间
        finished_at: 结束时间
        duration_ms: 耗时 (毫秒)
        cached: 是否缓存命中
        attempt: 重试次数
        metadata: 额外元数据
    """
    task_name: str
    status: TaskStatus
    value: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    cached: bool = False
    attempt: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        """是否成功"""
        return self.status.is_success()

    def is_failed(self) -> bool:
        """是否失败"""
        return self.status == TaskStatus.FAILED

    @classmethod
    def success(
        cls,
        task_name: str,
        value: Any,
        duration_ms: float = 0,
        cached: bool = False,
        **metadata
    ) -> 'TaskResult':
        """创建成功结果的工厂方法"""
        now = datetime.now()
        return cls(
            task_name=task_name,
            status=TaskStatus.CACHED if cached else TaskStatus.SUCCESS,
            value=value,
            finished_at=now,
            duration_ms=duration_ms,
            cached=cached,
            metadata=metadata,
        )

    @classmethod
    def failed(
        cls,
        task_name: str,
        error: Union[str, Exception],
        duration_ms: float = 0,
        attempt: int = 1,
        **metadata
    ) -> 'TaskResult':
        """创建失败结果的工厂方法"""
        now = datetime.now()
        error_str = str(error) if isinstance(error, Exception) else error
        return cls(
            task_name=task_name,
            status=TaskStatus.FAILED,
            error=error_str,
            finished_at=now,
            duration_ms=duration_ms,
            attempt=attempt,
            metadata=metadata,
        )

    @classmethod
    def skipped(cls, task_name: str, reason: str = "") -> 'TaskResult':
        """创建跳过结果的工厂方法"""
        return cls(
            task_name=task_name,
            status=TaskStatus.SKIPPED,
            metadata={'skip_reason': reason},
        )


@dataclass(frozen=True)
class FlowResult:
    """流程执行结果

    Attributes:
        flow_name: 流程名称
        run_id: 运行 ID
        status: 执行状态
        task_results: 各任务结果
        started_at: 开始时间
        finished_at: 结束时间
        total_duration_ms: 总耗时 (毫秒)
        success_count: 成功任务数
        failed_count: 失败任务数
        skipped_count: 跳过任务数
        cached_count: 缓存命中数
        metadata: 额外元数据
    """
    flow_name: str
    run_id: str
    status: FlowStatus
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    success_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    cached_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_success(self) -> bool:
        """是否完全成功"""
        return self.status == FlowStatus.SUCCESS

    def get_task_result(self, task_name: str) -> Optional[TaskResult]:
        """获取指定任务的结果"""
        return self.task_results.get(task_name)

    def get_failed_tasks(self) -> List[str]:
        """获取失败的任务列表"""
        return [
            name for name, result in self.task_results.items()
            if result.status == TaskStatus.FAILED
        ]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典 (便于序列化)"""
        return {
            'flow_name': self.flow_name,
            'run_id': self.run_id,
            'status': self.status.name,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'total_duration_ms': self.total_duration_ms,
            'success_count': self.success_count,
            'failed_count': self.failed_count,
            'skipped_count': self.skipped_count,
            'cached_count': self.cached_count,
            'task_results': {
                name: {
                    'status': r.status.name,
                    'duration_ms': r.duration_ms,
                    'cached': r.cached,
                    'error': r.error,
                }
                for name, r in self.task_results.items()
            },
            'metadata': self.metadata,
        }


# =============================================================================
# 执行计划类型 - 从 core.dag 统一导入，避免重复定义
# =============================================================================


# 向后兼容：api/types.py 曾经定义了 index 属性，但 core/dag.py 使用 level
# 如果需要 index 属性，可以使用 layer.level 替代


# =============================================================================
# 验证错误类型
# =============================================================================

@dataclass(frozen=True)
class ValidationError:
    """验证错误

    Attributes:
        field: 出错字段
        message: 错误信息
        code: 错误代码
    """
    field: str
    message: str
    code: str = "VALIDATION_ERROR"

    def __str__(self) -> str:
        return f"[{self.code}] {self.field}: {self.message}"


@dataclass(frozen=True)
class ValidationResult:
    """验证结果

    Attributes:
        valid: 是否通过验证
        errors: 错误列表
    """
    valid: bool
    errors: Tuple[ValidationError, ...] = field(default_factory=tuple)

    @classmethod
    def ok(cls) -> 'ValidationResult':
        """创建成功结果"""
        return cls(valid=True, errors=tuple())

    @classmethod
    def fail(cls, *errors: ValidationError) -> 'ValidationResult':
        """创建失败结果"""
        return cls(valid=False, errors=errors)
