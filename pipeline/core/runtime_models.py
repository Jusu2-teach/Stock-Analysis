"""Runtime models for professional-grade pipeline orchestration.

定义 Pipeline/Step 级别的运行时模型与策略：
- RunStatus: 统一的运行状态枚举
- RetryPolicy: 重试策略
- FailureStrategy/FailurePolicy: 失败处理策略
- StepRun: 单个步骤的运行视图
- FlowRun: 整个 Pipeline 的运行视图

这些模型与具体执行引擎解耦，只依赖于抽象的字段，
由 FlowExecutor / ResultAssembler / Engines 在运行时填充。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Optional


class RunStatus(Enum):
    """统一的运行状态枚举"""

    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()
    SKIPPED = auto()
    CACHED = auto()


@dataclass
class RetryPolicy:
    """重试策略

    Attributes:
        max_attempts: 最大重试次数（含首次执行）。1 表示不重试。
        delay_seconds: 每次重试间隔秒数。
        backoff_multiplier: 退避系数，大于 1 表示指数退避。
        jitter_seconds: 随机抖动上限，0 表示无抖动。
    """

    max_attempts: int = 1
    delay_seconds: float = 0.0
    backoff_multiplier: float = 1.0
    jitter_seconds: float = 0.0


class FailureStrategy(Enum):
    """失败处理策略"""

    FAIL_PIPELINE = auto()       # 默认：失败即终止整个 Pipeline
    MARK_SKIPPED = auto()        # 将步骤标记为 SKIPPED，后续依赖节点也跳过
    CONTINUE = auto()            # 记录失败但继续执行后续步骤（需依赖图允许）


@dataclass
class FailurePolicy:
    """失败处理策略配置"""

    strategy: FailureStrategy = FailureStrategy.FAIL_PIPELINE
    notify_events: List[str] = field(default_factory=list)


@dataclass
class StepRun:
    """单个步骤的运行视图"""

    name: str
    status: RunStatus = RunStatus.PENDING
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[float] = None
    attempts: int = 0
    cached: bool = False
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def mark_started(self):
        self.status = RunStatus.RUNNING
        self.started_at = datetime.now().isoformat()
        self.attempts += 1

    def mark_finished(self, status: RunStatus, error: str | None = None, duration_ms: float | None = None):
        self.status = status
        self.finished_at = datetime.now().isoformat()
        if duration_ms is not None:
            self.duration_ms = duration_ms
        elif self.started_at and not self.duration_ms:
            try:
                start_dt = datetime.fromisoformat(self.started_at)
                self.duration_ms = (datetime.now() - start_dt).total_seconds() * 1000.0
            except Exception:
                pass
        if error:
            self.error = error


@dataclass
class FlowRun:
    """整个 Pipeline 的运行视图"""

    run_id: str
    status: RunStatus = RunStatus.PENDING
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_ms: Optional[float] = None
    step_order: List[str] = field(default_factory=list)
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    def mark_started(self):
        self.status = RunStatus.RUNNING
        self.started_at = datetime.now().isoformat()

    def mark_finished(self, status: RunStatus, error: str | None = None):
        self.status = status
        self.finished_at = datetime.now().isoformat()
        if self.started_at and not self.duration_ms:
            try:
                start_dt = datetime.fromisoformat(self.started_at)
                self.duration_ms = (datetime.now() - start_dt).total_seconds() * 1000.0
            except Exception:
                pass
        if error:
            self.error = error


__all__ = [
    "RunStatus",
    "RetryPolicy",
    "FailureStrategy",
    "FailurePolicy",
    "StepRun",
    "FlowRun",
]
