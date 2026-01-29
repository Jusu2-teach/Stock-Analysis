"""Pipeline Core Models - Run
=============================

定义运行时状态对象 (可变)。
Run 对象在执行过程中持续更新。

设计原则：
- 可变 (与 Spec 不同)
- 包含状态机
- 支持序列化

版本: 2.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
import uuid

from .state import TaskState, FlowState, TaskStateMachine, FlowStateMachine
from .spec import TaskSpec, FlowSpec


# =============================================================================
# 任务运行时
# =============================================================================

@dataclass
class TaskRun:
    """任务运行时状态

    追踪单个任务的执行状态。

    Attributes:
        spec: 任务规范 (不可变引用)
        run_id: 运行唯一 ID
        state_machine: 状态机
        started_at: 开始时间
        finished_at: 结束时间
        duration_ms: 耗时 (毫秒)
        attempt: 当前尝试次数
        result: 执行结果 (成功时)
        error: 错误信息 (失败时)
        error_traceback: 错误堆栈
        cached: 是否缓存命中
        cache_key: 缓存键
        inputs: 实际输入值
        outputs: 实际输出值
        metadata: 运行时元数据
    """
    spec: TaskSpec
    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    state_machine: TaskStateMachine = field(default_factory=TaskStateMachine)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    attempt: int = 0
    result: Any = None
    error: Optional[str] = None
    error_traceback: Optional[str] = None
    cached: bool = False
    cache_key: Optional[str] = None
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def name(self) -> str:
        """任务名称"""
        return self.spec.name

    @property
    def state(self) -> TaskState:
        """当前状态"""
        return self.state_machine.state

    def mark_started(self) -> None:
        """标记开始执行"""
        self.started_at = datetime.now()
        self.attempt += 1
        self.state_machine.transition_to(
            TaskState.RUNNING,
            trigger="executor",
            metadata={'attempt': self.attempt}
        )

    def mark_success(self, result: Any = None, outputs: Optional[Dict[str, Any]] = None) -> None:
        """标记执行成功"""
        self.finished_at = datetime.now()
        self.result = result
        if outputs:
            self.outputs = outputs
        self._calculate_duration()
        self.state_machine.transition_to(
            TaskState.SUCCESS,
            trigger="executor",
            metadata={'duration_ms': self.duration_ms}
        )

    def mark_failed(self, error: str, traceback: Optional[str] = None) -> None:
        """标记执行失败"""
        self.finished_at = datetime.now()
        self.error = error
        self.error_traceback = traceback
        self._calculate_duration()
        self.state_machine.transition_to(
            TaskState.FAILED,
            trigger="executor",
            metadata={'error': error, 'attempt': self.attempt}
        )

    def mark_cached(self, result: Any, cache_key: str) -> None:
        """标记缓存命中"""
        self.cached = True
        self.cache_key = cache_key
        self.result = result
        self.finished_at = datetime.now()
        self.duration_ms = 0  # 缓存命中几乎无耗时
        self.state_machine.transition_to(
            TaskState.CACHED,
            trigger="cache",
            metadata={'cache_key': cache_key}
        )

    def mark_skipped(self, reason: str = "") -> None:
        """标记跳过"""
        self.finished_at = datetime.now()
        self.metadata['skip_reason'] = reason
        self.state_machine.transition_to(
            TaskState.SKIPPED,
            trigger="scheduler",
            metadata={'reason': reason}
        )

    def mark_retrying(self) -> None:
        """标记等待重试"""
        self.state_machine.transition_to(
            TaskState.RETRYING,
            trigger="retry_policy",
            metadata={'attempt': self.attempt}
        )

    def _calculate_duration(self) -> None:
        """计算耗时"""
        if self.started_at and self.finished_at:
            delta = self.finished_at - self.started_at
            self.duration_ms = delta.total_seconds() * 1000

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典 (便于序列化)"""
        return {
            'name': self.name,
            'run_id': self.run_id,
            'state': self.state.name,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'duration_ms': self.duration_ms,
            'attempt': self.attempt,
            'cached': self.cached,
            'error': self.error,
            'outputs': list(self.outputs.keys()),
            'metadata': self.metadata,
        }


# =============================================================================
# 流程运行时
# =============================================================================

@dataclass
class FlowRun:
    """流程运行时状态

    追踪整个流程的执行状态。

    Attributes:
        spec: 流程规范 (不可变引用)
        run_id: 运行唯一 ID
        state_machine: 状态机
        task_runs: 任务运行时映射
        started_at: 开始时间
        finished_at: 结束时间
        total_duration_ms: 总耗时 (毫秒)
        metadata: 运行时元数据
    """
    spec: FlowSpec
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state_machine: FlowStateMachine = field(default=None)
    task_runs: Dict[str, TaskRun] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    total_duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """初始化状态机"""
        if self.state_machine is None:
            self.state_machine = FlowStateMachine(
                soft_fail=self.spec.orchestration.soft_fail
            )

        # 为每个任务创建 TaskRun
        for task_spec in self.spec.tasks:
            if task_spec.name not in self.task_runs:
                self.task_runs[task_spec.name] = TaskRun(spec=task_spec)

    @property
    def name(self) -> str:
        """流程名称"""
        return self.spec.name

    @property
    def state(self) -> FlowState:
        """当前状态"""
        return self.state_machine.state

    def get_task_run(self, task_name: str) -> Optional[TaskRun]:
        """获取任务运行时"""
        return self.task_runs.get(task_name)

    def mark_started(self) -> None:
        """标记开始执行"""
        self.started_at = datetime.now()
        self.state_machine.start()

    def mark_finished(self) -> None:
        """标记执行结束"""
        self.finished_at = datetime.now()
        self._calculate_duration()
        self._update_state()

    def _update_state(self) -> FlowState:
        """根据任务状态更新流程状态"""
        task_states = {name: tr.state for name, tr in self.task_runs.items()}
        return self.state_machine.update_from_tasks(task_states)

    def _calculate_duration(self) -> None:
        """计算总耗时"""
        if self.started_at and self.finished_at:
            delta = self.finished_at - self.started_at
            self.total_duration_ms = delta.total_seconds() * 1000

    def get_statistics(self) -> Dict[str, int]:
        """获取执行统计"""
        stats = {
            'total': len(self.task_runs),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'cached': 0,
            'pending': 0,
            'running': 0,
        }

        for tr in self.task_runs.values():
            state = tr.state
            if state == TaskState.SUCCESS:
                stats['success'] += 1
            elif state == TaskState.FAILED:
                stats['failed'] += 1
            elif state == TaskState.SKIPPED:
                stats['skipped'] += 1
            elif state == TaskState.CACHED:
                stats['cached'] += 1
            elif state == TaskState.PENDING:
                stats['pending'] += 1
            elif state in (TaskState.RUNNING, TaskState.RETRYING):
                stats['running'] += 1

        return stats

    def get_failed_tasks(self) -> List[str]:
        """获取失败的任务列表"""
        return [
            name for name, tr in self.task_runs.items()
            if tr.state == TaskState.FAILED
        ]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典 (便于序列化)"""
        stats = self.get_statistics()
        return {
            'name': self.name,
            'run_id': self.run_id,
            'state': self.state.name,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'finished_at': self.finished_at.isoformat() if self.finished_at else None,
            'total_duration_ms': self.total_duration_ms,
            'statistics': stats,
            'task_runs': {
                name: tr.to_dict() for name, tr in self.task_runs.items()
            },
            'metadata': self.metadata,
        }
