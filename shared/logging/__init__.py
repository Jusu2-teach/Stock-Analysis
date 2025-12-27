"""
AStock 统一日志系统 (Unified Logging System)
=============================================

集百家之长的日志模块：
- structlog: 结构化日志 + 处理器链
- loguru: 简洁 API + 自动上下文
- Python logging: 标准库兼容性
- Sentry: 错误追踪集成

核心特性:
1. 结构化日志 - JSON 格式，便于日志分析
2. 上下文绑定 - 自动携带追踪信息
3. 多输出源 - Console + File + EventBus
4. 性能优化 - 异步写入 + 采样
5. 装饰器支持 - 自动记录函数调用

Usage:
    from shared.logging import get_logger, LogContext

    # 获取 logger
    logger = get_logger(__name__)
    logger.info("Processing started", step="load_data", count=100)

    # 绑定上下文
    with LogContext(request_id="abc123"):
        logger.debug("In context")

    # 装饰器记录函数调用
    @log_call(level="DEBUG")
    def process_data(df):
        ...
"""
__version__ = "1.0.0"

from .logger import (
    get_logger,
    AStockLogger,
    LogLevel,
    configure_logging,
    reset_logging,
)

from .context import (
    LogContext,
    bind_context,
    clear_context,
    get_context,
)

from .formatters import (
    Formatter,
    ConsoleFormatter,
    JSONFormatter,
    ColoredFormatter,
)

from .handlers import (
    LogHandler,
    ConsoleHandler,
    FileHandler,
    RotatingFileHandler,
    EventBusHandler,
)

from .decorators import (
    log_call,
    log_errors,
    timed,
)

from .config import (
    LogConfig,
    load_log_config,
)

__all__ = [
    # Logger
    'get_logger',
    'AStockLogger',
    'LogLevel',
    'configure_logging',
    'reset_logging',

    # Context
    'LogContext',
    'bind_context',
    'clear_context',
    'get_context',

    # Formatters
    'Formatter',
    'ConsoleFormatter',
    'JSONFormatter',
    'ColoredFormatter',

    # Handlers
    'LogHandler',
    'ConsoleHandler',
    'FileHandler',
    'RotatingFileHandler',
    'EventBusHandler',

    # Decorators
    'log_call',
    'log_errors',
    'timed',

    # Config
    'LogConfig',
    'load_log_config',
]