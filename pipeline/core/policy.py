"""Pipeline Core Models - Policy
================================

定义任务执行的各类策略配置。

设计原则：
- 不可变 (frozen=True)
- 合理默认值
- 可组合性

版本: 2.0.0
"""

from __future__ import annotations

import random  # 移到顶部，避免每次调用时导入
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


# =============================================================================
# 重试策略
# =============================================================================

class BackoffStrategy(Enum):
    """退避策略"""
    NONE = auto()         # 无退避，固定间隔
    LINEAR = auto()       # 线性退避: delay * attempt
    EXPONENTIAL = auto()  # 指数退避: delay * 2^attempt
    FIBONACCI = auto()    # 斐波那契退避


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略

    Attributes:
        max_attempts: 最大尝试次数 (含首次执行)，1 表示不重试
        delay_seconds: 重试间隔 (秒)
        backoff: 退避策略
        backoff_multiplier: 退避系数 (用于 EXPONENTIAL)
        max_delay_seconds: 最大重试间隔
        retry_on: 仅在这些异常时重试 (None 表示所有异常)
        ignore_on: 忽略这些异常 (不触发重试)

    Example:
        # 指数退避，最多重试 3 次
        policy = RetryPolicy(
            max_attempts=3,
            delay_seconds=1.0,
            backoff="exponential",
            backoff_multiplier=2.0,
        )
    """
    max_attempts: int = 1
    delay_seconds: float = 0.0
    backoff: str = "none"  # none, linear, exponential, fibonacci
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 300.0
    jitter_seconds: float = 0.0
    retry_on: Optional[tuple] = None  # Exception types to retry on
    ignore_on: Optional[tuple] = None  # Exception types to ignore

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        if self.backoff not in ('none', 'linear', 'exponential', 'fibonacci'):
            raise ValueError(f"Invalid backoff strategy: {self.backoff}")

    @property
    def enabled(self) -> bool:
        """是否启用重试"""
        return self.max_attempts > 1

    def get_delay(self, attempt: int) -> float:
        """计算第 N 次重试的延迟时间

        Args:
            attempt: 当前尝试次数 (1-based)

        Returns:
            延迟秒数
        """
        if attempt <= 1 or self.delay_seconds <= 0:
            return 0.0

        retry_count = attempt - 1  # 重试次数 (0-based)

        if self.backoff == 'none':
            delay = self.delay_seconds
        elif self.backoff == 'linear':
            delay = self.delay_seconds * retry_count
        elif self.backoff == 'exponential':
            delay = self.delay_seconds * (self.backoff_multiplier ** retry_count)
        elif self.backoff == 'fibonacci':
            delay = self.delay_seconds * self._fibonacci(retry_count + 1)
        else:
            delay = self.delay_seconds

        # 添加抖动（random 已在模块顶部导入）
        if self.jitter_seconds > 0:
            delay += random.uniform(0, self.jitter_seconds)

        # 限制最大延迟
        return min(delay, self.max_delay_seconds)

    @staticmethod
    def _fibonacci(n: int) -> int:
        """计算斐波那契数"""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b

    def should_retry(self, exception: Exception) -> bool:
        """判断是否应该重试此异常"""
        # 如果在忽略列表中，不重试
        if self.ignore_on and isinstance(exception, self.ignore_on):
            return False

        # 如果指定了重试列表，只重试列表中的异常
        if self.retry_on:
            return isinstance(exception, self.retry_on)

        # 默认：所有异常都重试
        return True


# =============================================================================
# 缓存策略
# =============================================================================

@dataclass(frozen=True)
class CachePolicy:
    """缓存策略

    Attributes:
        enabled: 是否启用缓存
        ttl_seconds: 缓存过期时间 (秒)，None 表示永不过期
        backend: 缓存后端 ('memory', 'file', 'redis')
        key_prefix: 缓存键前缀
        include_params: 计算缓存键时包含的参数名 (None 表示全部)
        exclude_params: 计算缓存键时排除的参数名
        invalidate_on_failure: 失败时是否清除缓存

    Example:
        policy = CachePolicy(
            enabled=True,
            ttl_seconds=3600,
            backend="file",
        )
    """
    enabled: bool = False
    ttl_seconds: Optional[int] = None
    backend: str = "memory"
    key_prefix: str = ""
    include_params: Optional[tuple] = None
    exclude_params: tuple = field(default_factory=tuple)
    invalidate_on_failure: bool = False

    def __post_init__(self):
        if self.ttl_seconds is not None and self.ttl_seconds < 0:
            raise ValueError("ttl_seconds must be >= 0")
        # 与 cache/backends.py 的实现保持一致
        # - tiered: L1 memory + L2 file
        # - none: 显式关闭缓存（等价于 enabled=False）
        if self.backend not in ('memory', 'file', 'tiered', 'redis', 'none'):
            raise ValueError(f"Invalid cache backend: {self.backend}")

        # 语义一致性检查：backend='none' 与 enabled=True 矛盾
        if self.backend == 'none' and self.enabled:
            raise ValueError(
                "Conflicting cache configuration: backend='none' implies cache is disabled, "
                "but enabled=True was specified. Use enabled=False or choose a different backend."
            )


# =============================================================================
# 超时策略
# =============================================================================

@dataclass(frozen=True)
class TimeoutPolicy:
    """超时策略

    Attributes:
        timeout_seconds: 超时时间 (秒)，0 或 None 表示不限制
        soft_timeout: 是否软超时 (记录警告但不终止)
    """
    timeout_seconds: Optional[int] = None
    soft_timeout: bool = False

    @property
    def enabled(self) -> bool:
        """是否启用超时"""
        return self.timeout_seconds is not None and self.timeout_seconds > 0


# =============================================================================
# 失败策略
# =============================================================================

class FailureStrategy(Enum):
    """失败处理策略"""
    FAIL_FLOW = auto()      # 立即终止整个流程
    SKIP_DOWNSTREAM = auto()  # 跳过下游任务，继续其他分支
    CONTINUE = auto()       # 记录失败但继续执行 (需谨慎使用)


@dataclass(frozen=True)
class FailurePolicy:
    """失败处理策略

    Attributes:
        strategy: 失败处理方式
        allowed_failures: 允许的最大失败数 (仅 SKIP_DOWNSTREAM 模式)
        notify_on_failure: 失败时触发的通知事件
    """
    strategy: FailureStrategy = FailureStrategy.FAIL_FLOW
    allowed_failures: int = 0
    notify_on_failure: tuple = field(default_factory=tuple)

    @property
    def soft_fail(self) -> bool:
        """是否为软失败模式"""
        return self.strategy != FailureStrategy.FAIL_FLOW


# =============================================================================
# 聚合策略
# =============================================================================

@dataclass(frozen=True)
class AggregationPolicy:
    """聚合策略 (用于配置 PDDA 行为)

    Attributes:
        enabled: 是否启用聚合
        namespace: 聚合命名空间
        collect_as_producer: 是否作为生产者收集输出
        inject_as_consumer: 是否作为消费者注入参数
        consumer_param_name: 消费者参数名
    """
    enabled: bool = True
    namespace: str = "default"
    collect_as_producer: bool = True
    inject_as_consumer: bool = True
    consumer_param_name: str = "aggregated_data"


# =============================================================================
# 组合策略
# =============================================================================

# 用于标识策略是否被显式配置的哨兵对象
_NOT_CONFIGURED = object()


@dataclass(frozen=True)
class TaskPolicies:
    """任务策略组合

    将所有策略组合在一起，便于统一管理。

    Note:
        使用 _is_explicitly_configured 标记来区分 "未配置" 和 "显式配置为默认值"。
        例如，用户显式设置 retry.max_attempts=1 应该被尊重，而不是被默认值覆盖。
    """
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    cache: CachePolicy = field(default_factory=CachePolicy)
    timeout: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    failure: FailurePolicy = field(default_factory=FailurePolicy)
    aggregation: AggregationPolicy = field(default_factory=AggregationPolicy)
    # 内部标记: 哪些策略被显式配置过
    _configured_policies: frozenset = field(default_factory=frozenset)

    def mark_configured(self, *policy_names: str) -> 'TaskPolicies':
        """标记策略为已配置 (不可变，返回新实例)

        Args:
            policy_names: 策略名称 ('retry', 'cache', 'timeout', 'failure', 'aggregation')

        Returns:
            新的 TaskPolicies 实例
        """
        new_configured = self._configured_policies | frozenset(policy_names)
        return TaskPolicies(
            retry=self.retry,
            cache=self.cache,
            timeout=self.timeout,
            failure=self.failure,
            aggregation=self.aggregation,
            _configured_policies=new_configured,
        )

    def is_configured(self, policy_name: str) -> bool:
        """检查策略是否被显式配置"""
        return policy_name in self._configured_policies

    def merge_with_defaults(self, defaults: 'TaskPolicies') -> 'TaskPolicies':
        """与默认策略合并 (当前策略优先)

        用于实现流程级默认 + 任务级覆盖。

        合并规则 (改进版):
        - 如果策略被显式配置 (在 _configured_policies 中): 使用当前值
        - 否则: 使用默认值

        这确保了用户显式设置 retry.max_attempts=1 (不重试) 不会被默认值覆盖。
        """
        return TaskPolicies(
            retry=self.retry if self.is_configured('retry') else defaults.retry,
            cache=self.cache if self.is_configured('cache') else defaults.cache,
            timeout=self.timeout if self.is_configured('timeout') else defaults.timeout,
            failure=self.failure if self.is_configured('failure') else defaults.failure,
            aggregation=self.aggregation if self.is_configured('aggregation') else defaults.aggregation,
            _configured_policies=self._configured_policies,
        )