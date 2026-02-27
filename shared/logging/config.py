"""
日志配置 (Log Configuration)
=============================

参考设计:
- pydantic-settings: 类型化配置
- dynaconf: 多环境支持
- structlog: 处理器配置

支持从文件、环境变量加载日志配置。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
import os
import json

from .logger import configure_logging
from .formatters import Formatter, ConsoleFormatter, ColoredFormatter, JSONFormatter
from .handlers import (
    LogHandler, ConsoleHandler, FileHandler,
    RotatingFileHandler, EventBusHandler, AsyncHandler
)


@dataclass
class HandlerConfig:
    """处理器配置"""
    type: Literal["console", "file", "rotating_file", "eventbus", "null"]
    level: str = "DEBUG"
    formatter: Literal["console", "colored", "json", "compact"] = "colored"

    # File handler options
    file_path: Optional[str] = None
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5
    encoding: str = "utf-8"

    # Async wrapper
    async_mode: bool = False
    queue_size: int = 10000


@dataclass
class LogConfig:
    """日志配置

    Example:
        config = LogConfig(
            level="INFO",
            handlers=[
                HandlerConfig(type="console", formatter="colored"),
                HandlerConfig(type="rotating_file", file_path="logs/app.log"),
            ]
        )
        config.apply()
    """
    level: str = "INFO"
    handlers: List[HandlerConfig] = field(default_factory=list)

    # 全局设置
    include_stdlib: bool = True

    # 默认上下文
    default_context: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # 默认配置：彩色控制台输出
        if not self.handlers:
            self.handlers = [
                HandlerConfig(type="console", formatter="colored")
            ]

    def apply(self) -> None:
        """应用配置"""
        handlers = [self._create_handler(cfg) for cfg in self.handlers]
        configure_logging(
            level=self.level,
            handlers=handlers,
            enable_stdlib=self.include_stdlib,
        )

    def _create_handler(self, cfg: HandlerConfig) -> LogHandler:
        """根据配置创建处理器"""
        formatter = self._create_formatter(cfg.formatter)

        if cfg.type == "console":
            handler = ConsoleHandler(formatter=formatter, min_level=cfg.level)

        elif cfg.type == "file":
            if not cfg.file_path:
                raise ValueError("file_path is required for file handler")
            handler = FileHandler(
                file_path=cfg.file_path,
                formatter=formatter,
                min_level=cfg.level,
                encoding=cfg.encoding,
            )

        elif cfg.type == "rotating_file":
            if not cfg.file_path:
                raise ValueError("file_path is required for rotating_file handler")
            handler = RotatingFileHandler(
                file_path=cfg.file_path,
                max_bytes=cfg.max_bytes,
                backup_count=cfg.backup_count,
                formatter=formatter,
                min_level=cfg.level,
                encoding=cfg.encoding,
            )

        elif cfg.type == "eventbus":
            handler = EventBusHandler(min_level=cfg.level)

        elif cfg.type == "null":
            from .handlers import NullHandler
            handler = NullHandler()

        else:
            raise ValueError(f"Unknown handler type: {cfg.type}")

        # 包装为异步
        if cfg.async_mode and cfg.type not in ("null", "eventbus"):
            handler = AsyncHandler(handler, queue_size=cfg.queue_size)

        return handler

    def _create_formatter(self, fmt_type: str) -> Formatter:
        """创建格式化器"""
        if fmt_type == "console":
            return ConsoleFormatter()
        elif fmt_type == "colored":
            return ColoredFormatter()
        elif fmt_type == "json":
            return JSONFormatter()
        elif fmt_type == "compact":
            from .formatters import CompactFormatter
            return CompactFormatter()
        else:
            return ColoredFormatter()


def load_log_config(
    config_path: Optional[str | Path] = None,
    env_prefix: str = "ASTOCK_LOG_",
) -> LogConfig:
    """加载日志配置

    优先级：环境变量 > 配置文件 > 默认值

    环境变量:
    - ASTOCK_LOG_LEVEL: 日志级别
    - ASTOCK_LOG_FORMAT: 格式化器类型
    - ASTOCK_LOG_FILE: 日志文件路径
    - ASTOCK_LOG_JSON: 是否使用 JSON 格式

    配置文件 (JSON):
    {
        "level": "INFO",
        "handlers": [
            {"type": "console", "formatter": "colored"},
            {"type": "rotating_file", "file_path": "logs/app.log"}
        ]
    }
    """
    config = LogConfig()

    # 从文件加载
    if config_path:
        path = Path(config_path)
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                config = _config_from_dict(data)
            except Exception:
                pass  # 文件加载失败，使用默认值

    # 环境变量覆盖
    env_level = os.environ.get(f"{env_prefix}LEVEL")
    if env_level:
        config.level = env_level.upper()

    env_format = os.environ.get(f"{env_prefix}FORMAT")
    env_file = os.environ.get(f"{env_prefix}FILE")
    env_json = os.environ.get(f"{env_prefix}JSON", "").lower() in ("1", "true", "yes")

    # 根据环境变量重建 handlers
    if env_format or env_file or env_json:
        handlers = []

        # Console handler
        formatter = "json" if env_json else (env_format or "colored")
        handlers.append(HandlerConfig(type="console", formatter=formatter, level=config.level))

        # File handler
        if env_file:
            handlers.append(HandlerConfig(
                type="rotating_file",
                file_path=env_file,
                formatter="json",
                level=config.level,
            ))

        config.handlers = handlers

    return config


def _config_from_dict(data: Dict[str, Any]) -> LogConfig:
    """从字典创建配置"""
    handlers = []
    for h in data.get('handlers', []):
        handlers.append(HandlerConfig(**h))

    return LogConfig(
        level=data.get('level', 'INFO'),
        handlers=handlers,
        include_stdlib=data.get('include_stdlib', True),
        default_context=data.get('default_context', {}),
    )


# 预设配置
PRESET_DEVELOPMENT = LogConfig(
    level="DEBUG",
    handlers=[
        HandlerConfig(type="console", formatter="colored", level="DEBUG"),
    ],
)

PRESET_PRODUCTION = LogConfig(
    level="INFO",
    handlers=[
        HandlerConfig(type="console", formatter="json", level="WARNING"),
        HandlerConfig(
            type="rotating_file",
            file_path="logs/app.log",
            formatter="json",
            level="INFO",
            async_mode=True,
        ),
        HandlerConfig(type="eventbus", level="ERROR"),
    ],
)

PRESET_TESTING = LogConfig(
    level="WARNING",
    handlers=[
        HandlerConfig(type="console", formatter="compact", level="WARNING"),
    ],
)
