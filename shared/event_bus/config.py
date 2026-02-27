"""
EventBus 配置模块
"""
from dataclasses import dataclass
import os


@dataclass
class EventBusConfig:
    """EventBus v6 配置"""
    
    # 死信队列配置
    enable_dead_letter: bool = True
    dead_letter_max_size: int = 1000
    dead_letter_ttl_seconds: int = 3600
    dead_letter_max_retries: int = 3
    
    # 历史事件配置
    enable_historic: bool = True
    historic_max_events_per_type: int = 100
    
    # 中间件配置
    enable_middleware: bool = True  # 是否启用中间件管道
    enable_tracing: bool = True
    enable_logging: bool = True
    enable_retry: bool = False  # 默认关闭重试
    enable_timeout: bool = False
    default_timeout_seconds: float = 30.0
    retry_max_attempts: int = 3
    retry_base_delay: float = 0.1
    
    # HookSpec 验证
    enable_validation: bool = True  # 是否启用 HookSpec 验证
    validation_strict: bool = False  # 严格模式：签名不匹配时抛异常
    strict_spec: bool = False  # 兼容旧名称
    warn_on_spec_mismatch: bool = True
    
    # 异步配置
    async_queue_max_size: int = 1000
    async_default_timeout: float = 30.0
    
    # 调试配置
    debug_mode: bool = False
    max_event_log_size: int = 1000
    
    # 性能配置
    enable_stats: bool = True
    stats_sample_rate: float = 1.0  # 采样率 0.0-1.0
    
    @classmethod
    def from_env(cls) -> 'EventBusConfig':
        """从环境变量加载配置"""
        return cls(
            enable_dead_letter=os.getenv('EVENTBUS_DEAD_LETTER', 'true').lower() == 'true',
            enable_historic=os.getenv('EVENTBUS_HISTORIC', 'true').lower() == 'true',
            enable_tracing=os.getenv('EVENTBUS_TRACING', 'true').lower() == 'true',
            enable_logging=os.getenv('EVENTBUS_LOGGING', 'true').lower() == 'true',
            enable_retry=os.getenv('EVENTBUS_RETRY', 'false').lower() == 'true',
            strict_spec=os.getenv('EVENTBUS_STRICT_SPEC', 'false').lower() == 'true',
            debug_mode=os.getenv('EVENTBUS_DEBUG', 'false').lower() == 'true',
        )
    
    @classmethod
    def production(cls) -> 'EventBusConfig':
        """生产环境配置"""
        return cls(
            enable_dead_letter=True,
            enable_historic=True,
            enable_tracing=True,
            enable_logging=True,
            enable_retry=True,
            strict_spec=False,
            debug_mode=False,
        )
    
    @classmethod
    def development(cls) -> 'EventBusConfig':
        """开发环境配置"""
        return cls(
            enable_dead_letter=True,
            enable_historic=True,
            enable_tracing=True,
            enable_logging=True,
            enable_retry=False,
            strict_spec=True,
            warn_on_spec_mismatch=True,
            debug_mode=True,
        )
    
    @classmethod
    def minimal(cls) -> 'EventBusConfig':
        """最小配置（高性能）"""
        return cls(
            enable_dead_letter=False,
            enable_historic=False,
            enable_tracing=False,
            enable_logging=False,
            enable_retry=False,
            enable_stats=False,
        )
