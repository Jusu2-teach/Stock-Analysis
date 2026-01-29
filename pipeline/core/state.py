"""Pipeline Core Models - State
===============================

定义状态机和运行时状态。

设计原则：
- 状态转换明确
- 并发安全
- 可追溯

版本: 2.0.0
"""

from __future__ import annotations

import copy
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from threading import Lock, RLock
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# =============================================================================
# 任务状态
# =============================================================================

class TaskState(Enum):
    """任务状态枚举

    状态转换图：

        PENDING ──┬──→ RUNNING ──┬──→ SUCCESS
                  │              │
                  │              ├──→ FAILED ──→ RETRYING ──→ RUNNING
                  │              │
                  │              └──→ CANCELLED
                  │
                  ├──→ SKIPPED
                  │
                  └──→ CACHED
    """
    PENDING = auto()    # 等待执行
    RUNNING = auto()    # 正在执行
    SUCCESS = auto()    # 执行成功
    FAILED = auto()     # 执行失败
    SKIPPED = auto()    # 跳过执行
    CACHED = auto()     # 缓存命中
    RETRYING = auto()   # 等待重试
    CANCELLED = auto()  # 已取消

    def is_terminal(self) -> bool:
        """是否为终态 (不可再转换)"""
        return self in _TERMINAL_STATES

    def is_success(self) -> bool:
        """是否表示成功完成"""
        return self in (TaskState.SUCCESS, TaskState.CACHED)

    def is_runnable(self) -> bool:
        """是否可开始执行"""
        return self in (TaskState.PENDING, TaskState.RETRYING)


# 终态集合
_TERMINAL_STATES: Set[TaskState] = {
    TaskState.SUCCESS,
    TaskState.FAILED,
    TaskState.SKIPPED,
    TaskState.CACHED,
    TaskState.CANCELLED,
}

# 允许的状态转换
_VALID_TRANSITIONS: Dict[TaskState, Set[TaskState]] = {
    TaskState.PENDING: {TaskState.RUNNING, TaskState.SKIPPED, TaskState.CACHED},
    TaskState.RUNNING: {TaskState.SUCCESS, TaskState.FAILED, TaskState.CANCELLED},
    TaskState.FAILED: {TaskState.RETRYING},
    TaskState.RETRYING: {TaskState.RUNNING, TaskState.CANCELLED},
    # 终态不可转换
    TaskState.SUCCESS: set(),
    TaskState.SKIPPED: set(),
    TaskState.CACHED: set(),
    TaskState.CANCELLED: set(),
}


# =============================================================================
# 流程状态
# =============================================================================

class FlowState(Enum):
    """流程状态枚举"""
    PENDING = auto()         # 等待开始
    RUNNING = auto()         # 正在运行
    SUCCESS = auto()         # 全部成功
    FAILED = auto()          # 执行失败
    PARTIAL_SUCCESS = auto()  # 部分成功 (soft_fail)
    CANCELLED = auto()       # 已取消

    def is_terminal(self) -> bool:
        """是否为终态"""
        return self in (
            FlowState.SUCCESS,
            FlowState.FAILED,
            FlowState.PARTIAL_SUCCESS,
            FlowState.CANCELLED,
        )


# =============================================================================
# 状态机
# =============================================================================

class InvalidStateTransitionError(Exception):
    """无效状态转换错误"""
    def __init__(self, current: TaskState, target: TaskState):
        self.current = current
        self.target = target
        super().__init__(
            f"Invalid state transition: {current.name} -> {target.name}"
        )


@dataclass
class StateTransition:
    """状态转换记录"""
    from_state: TaskState
    to_state: TaskState
    timestamp: datetime
    trigger: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class TaskStateMachine:
    """任务状态机 (线程安全)

    管理单个任务的状态转换，确保：
    - 状态转换合法性
    - 转换历史记录
    - 并发安全

    Example:
        sm = TaskStateMachine()
        sm.transition_to(TaskState.RUNNING, trigger="executor")
        sm.transition_to(TaskState.SUCCESS, trigger="executor")
    """

    def __init__(self, initial_state: TaskState = TaskState.PENDING):
        self._state = initial_state
        self._history: List[StateTransition] = []
        self._lock = RLock()  # 使用可重入锁，支持嵌套调用
        self._listeners: List[Callable[[StateTransition], None]] = []
        self._listeners_lock = Lock()  # 监听器列表专用锁

    @property
    def state(self) -> TaskState:
        """当前状态"""
        return self._state

    @property
    def history(self) -> List[StateTransition]:
        """状态转换历史 (只读副本)"""
        with self._lock:
            return list(self._history)

    def can_transition_to(self, target: TaskState) -> bool:
        """检查是否可以转换到目标状态"""
        return target in _VALID_TRANSITIONS.get(self._state, set())

    def transition_to(
        self,
        target: TaskState,
        trigger: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        force: bool = False
    ) -> StateTransition:
        """转换到目标状态

        Args:
            target: 目标状态
            trigger: 触发者标识
            metadata: 额外元数据
            force: 是否强制转换 (跳过合法性检查)

        Returns:
            状态转换记录

        Raises:
            InvalidStateTransitionError: 如果转换不合法且 force=False
        """
        with self._lock:
            if not force and not self.can_transition_to(target):
                raise InvalidStateTransitionError(self._state, target)

            transition = StateTransition(
                from_state=self._state,
                to_state=target,
                timestamp=datetime.now(),
                trigger=trigger,
                metadata=metadata or {},
            )

            self._state = target
            self._history.append(transition)

        # 通知监听器 (在锁外执行，避免死锁)
        # 获取监听器快照，避免迭代时被修改
        with self._listeners_lock:
            listeners_snapshot = list(self._listeners)

        for listener in listeners_snapshot:
            try:
                listener(transition)
            except Exception as e:
                # 监听器异常不应影响状态机，但需要记录
                logger.warning(
                    f"State listener failed for transition {transition}: {e}"
                )

        return transition

    def add_listener(self, callback: Callable[[StateTransition], None]) -> None:
        """添加状态变化监听器（线程安全）"""
        with self._listeners_lock:
            self._listeners.append(callback)

    def remove_listener(self, callback: Callable[[StateTransition], None]) -> None:
        """移除状态变化监听器（线程安全）"""
        with self._listeners_lock:
            if callback in self._listeners:
                self._listeners.remove(callback)

    def reset(self) -> None:
        """重置状态机

        将状态重置为 PENDING 并清空历史，同时通知监听器。
        """
        old_state = self._state
        with self._lock:
            self._state = TaskState.PENDING
            self._history.clear()

        # 通知监听器 (如果状态确实变化了)
        if old_state != TaskState.PENDING:
            transition = StateTransition(
                from_state=old_state,
                to_state=TaskState.PENDING,
                timestamp=datetime.now(),
                trigger="reset",
            )
            with self._listeners_lock:
                listeners_snapshot = list(self._listeners)
            for listener in listeners_snapshot:
                try:
                    listener(transition)
                except Exception as e:
                    logger.warning(f"State listener failed during reset: {e}")


class FlowStateMachine:
    """流程状态机

    基于所有任务状态聚合计算流程状态。
    """

    def __init__(self, soft_fail: bool = False):
        self._state = FlowState.PENDING
        self._soft_fail = soft_fail
        self._lock = Lock()

    @property
    def state(self) -> FlowState:
        """当前状态"""
        return self._state

    def start(self) -> None:
        """开始执行"""
        with self._lock:
            if self._state == FlowState.PENDING:
                self._state = FlowState.RUNNING

    def update_from_tasks(self, task_states: Dict[str, TaskState]) -> FlowState:
        """根据任务状态更新流程状态

        Args:
            task_states: 任务名称 -> 任务状态 的映射

        Returns:
            更新后的流程状态
        """
        with self._lock:
            if not task_states:
                return self._state

            states = list(task_states.values())

            # 检查是否有任务仍在运行
            running = any(s in (TaskState.RUNNING, TaskState.PENDING, TaskState.RETRYING)
                         for s in states)
            if running:
                self._state = FlowState.RUNNING
                return self._state

            # 所有任务都已完成
            failed_count = sum(1 for s in states if s == TaskState.FAILED)
            success_count = sum(1 for s in states
                               if s in (TaskState.SUCCESS, TaskState.CACHED))

            if failed_count == 0:
                self._state = FlowState.SUCCESS
            elif self._soft_fail:
                self._state = FlowState.PARTIAL_SUCCESS if success_count > 0 else FlowState.FAILED
            else:
                self._state = FlowState.FAILED

            return self._state

    def cancel(self) -> None:
        """取消执行"""
        with self._lock:
            if not self._state.is_terminal():
                self._state = FlowState.CANCELLED

    def set_dry_run_result(self, success: bool) -> None:
        """设置 Dry Run 模式的结果状态

        Dry Run 模式下不执行实际任务，直接设置最终状态。
        这是一个专用方法，避免直接访问私有属性。

        Args:
            success: Dry run 验证是否通过
        """
        with self._lock:
            self._state = FlowState.SUCCESS if success else FlowState.FAILED

    def force_fail(self, reason: str = "") -> None:
        """强制设置为失败状态

        用于内部错误处理场景，绕过正常状态转换验证。
        应谨慎使用，仅在异常处理时调用。

        Args:
            reason: 失败原因 (用于日志记录)
        """
        with self._lock:
            if not self._state.is_terminal():
                if reason:
                    logger.warning(f"Force failing flow: {reason}")
                self._state = FlowState.FAILED
