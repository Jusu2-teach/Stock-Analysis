"""Pipeline Catalog - Data Entry
================================

定义数据目录中的条目类型。

设计原则：
- 不可变元数据
- 血缘追踪
- 类型安全

版本: 2.0.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Generic, Optional, TypeVar


# =============================================================================
# 数据集类型
# =============================================================================

class DatasetType(Enum):
    """数据集类型"""
    MEMORY = auto()       # 内存数据
    CSV = auto()          # CSV 文件
    PARQUET = auto()      # Parquet 文件
    JSON = auto()         # JSON 文件
    DUCKDB = auto()       # DuckDB 表
    PICKLE = auto()       # Pickle 文件
    CUSTOM = auto()       # 自定义类型


class EntryStatus(Enum):
    """条目状态"""
    PENDING = auto()      # 待填充
    AVAILABLE = auto()    # 可用
    EXPIRED = auto()      # 已过期
    ERROR = auto()        # 加载错误


# =============================================================================
# 血缘信息
# =============================================================================

@dataclass(frozen=True)
class LineageInfo:
    """血缘信息

    追踪数据的来源和依赖关系。

    Attributes:
        source_task: 产生此数据的任务
        source_outputs: 来源输出名称
        upstream_entries: 上游数据条目 ID
        created_at: 创建时间
        run_id: 产生此数据的运行 ID
    """
    source_task: str
    source_outputs: tuple = field(default_factory=tuple)
    upstream_entries: tuple = field(default_factory=tuple)
    created_at: datetime = field(default_factory=datetime.now)
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source_task': self.source_task,
            'source_outputs': list(self.source_outputs),
            'upstream_entries': list(self.upstream_entries),
            'created_at': self.created_at.isoformat(),
            'run_id': self.run_id,
        }


# =============================================================================
# 数据条目
# =============================================================================

T = TypeVar('T')


@dataclass
class DataEntry(Generic[T]):
    """数据目录条目

    表示数据目录中的一个数据项。

    Attributes:
        key: 唯一键 (如 "task_name.output_name")
        value: 数据值
        dataset_type: 数据集类型
        status: 条目状态
        lineage: 血缘信息
        metadata: 额外元数据
        ttl_seconds: 生存时间 (秒)
        created_at: 创建时间
        accessed_at: 最后访问时间
        version: 版本号
    """
    key: str
    value: Optional[T] = None
    dataset_type: DatasetType = DatasetType.MEMORY
    status: EntryStatus = EntryStatus.PENDING
    lineage: Optional[LineageInfo] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    ttl_seconds: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.now)
    accessed_at: Optional[datetime] = None
    version: int = 1

    def is_available(self) -> bool:
        """检查数据是否可用（纯检查，无副作用）

        此方法仅检查当前状态和 TTL，不修改任何状态。
        如需在检查时自动更新过期状态，请使用 check_and_expire()。

        Returns:
            bool: 数据是否可用
        """
        if self.status != EntryStatus.AVAILABLE:
            return False

        # 检查 TTL (纯检查，不修改状态)
        if self.ttl_seconds is not None:
            elapsed = (datetime.now() - self.created_at).total_seconds()
            if elapsed > self.ttl_seconds:
                return False

        return True

    def is_expired(self) -> bool:
        """检查是否已过期（纯检查，无副作用）

        Returns:
            bool: 是否已过期
        """
        if self.status == EntryStatus.EXPIRED:
            return True

        if self.ttl_seconds is not None:
            elapsed = (datetime.now() - self.created_at).total_seconds()
            return elapsed > self.ttl_seconds

        return False

    def check_and_expire(self) -> bool:
        """检查可用性并在需要时更新过期状态

        这是一个有副作用的方法，如果 TTL 已过期，会将状态设置为 EXPIRED。
        适用于需要在检查时同步更新状态的场景。

        Returns:
            bool: 数据是否可用
        """
        if self.status != EntryStatus.AVAILABLE:
            return False

        if self.ttl_seconds is not None:
            elapsed = (datetime.now() - self.created_at).total_seconds()
            if elapsed > self.ttl_seconds:
                self.status = EntryStatus.EXPIRED
                return False

        return True

    def set_value(self, value: T, lineage: Optional[LineageInfo] = None) -> None:
        """设置数据值"""
        self.value = value
        self.status = EntryStatus.AVAILABLE
        self.created_at = datetime.now()
        self.version += 1
        if lineage:
            self.lineage = lineage

    def mark_accessed(self) -> None:
        """标记访问"""
        self.accessed_at = datetime.now()

    def invalidate(self) -> None:
        """使条目无效"""
        self.status = EntryStatus.EXPIRED

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'key': self.key,
            'dataset_type': self.dataset_type.name,
            'status': self.status.name,
            'lineage': self.lineage.to_dict() if self.lineage else None,
            'metadata': self.metadata,
            'created_at': self.created_at.isoformat(),
            'version': self.version,
        }


# =============================================================================
# 键规范
# =============================================================================

@dataclass(frozen=True)
class EntryKey:
    """条目键

    标准化数据条目的键格式。

    格式: {namespace}.{task_name}.{output_name}

    Example:
        key = EntryKey("default", "analyze_roic", "trend_result")
        str(key)  # "default.analyze_roic.trend_result"
    """
    namespace: str
    task_name: str
    output_name: str

    def __str__(self) -> str:
        return f"{self.namespace}.{self.task_name}.{self.output_name}"

    @classmethod
    def parse(cls, key_str: str) -> 'EntryKey':
        """解析键字符串"""
        parts = key_str.split('.', 2)
        if len(parts) == 3:
            return cls(
                namespace=parts[0],
                task_name=parts[1],
                output_name=parts[2],
            )
        elif len(parts) == 2:
            return cls(
                namespace="default",
                task_name=parts[0],
                output_name=parts[1],
            )
        else:
            return cls(
                namespace="default",
                task_name=key_str,
                output_name="result",
            )

    @classmethod
    def from_task(cls, task_name: str, output_name: str, namespace: str = "default") -> 'EntryKey':
        """从任务创建键"""
        return cls(
            namespace=namespace,
            task_name=task_name,
            output_name=output_name,
        )
