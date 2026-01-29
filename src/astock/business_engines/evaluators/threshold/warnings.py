"""
统一错误处理 (Warnings)
========================

结构化的警告和错误处理系统。

设计原则:
- 结构化警告 (代码、级别、消息)
- 分级处理 (INFO/WARNING/ERROR/CRITICAL)
- 可序列化
- 易于追踪

作者: AStock Analysis System
日期: 2026-01-10
版本: 2.0.0
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class WarningLevel(str, Enum):
    """警告级别"""
    INFO = "info"           # 信息性提示
    WARNING = "warning"     # 警告 (需关注)
    ERROR = "error"         # 错误 (影响结果)
    CRITICAL = "critical"   # 严重错误 (系统级)


class WarningCode(str, Enum):
    """警告代码"""
    # 规则执行错误
    RULE_EXECUTION_ERROR = "rule_execution_error"
    RULE_NOT_FOUND = "rule_not_found"
    RULE_DISABLED = "rule_disabled"

    # 数据质量问题
    MISSING_DATA = "missing_data"
    INVALID_DATA = "invalid_data"
    INSUFFICIENT_DATA = "insufficient_data"

    # 配置问题
    INVALID_CONFIG = "invalid_config"
    MISSING_CONFIG = "missing_config"

    # 业务逻辑警告
    METRIC_OUT_OF_RANGE = "metric_out_of_range"
    UNUSUAL_PATTERN = "unusual_pattern"
    THRESHOLD_VIOLATION = "threshold_violation"

    # 系统级错误
    FACTORY_ERROR = "factory_error"
    ENGINE_ERROR = "engine_error"
    UNKNOWN_ERROR = "unknown_error"


@dataclass(frozen=True)
class EvaluatorWarning:
    """
    评估器警告

    不可变警告对象，用于记录评估过程中的问题。

    Attributes:
        code: 警告代码
        level: 警告级别
        message: 警告消息
        context: 上下文信息 (可选)
        exception: 关联异常 (可选)
    """
    code: WarningCode
    level: WarningLevel
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[Exception] = None

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        result = {
            "code": self.code.value,
            "level": self.level.value,
            "message": self.message,
        }

        if self.context:
            result["context"] = self.context

        if self.exception:
            result["exception"] = {
                "type": type(self.exception).__name__,
                "message": str(self.exception),
            }

        return result

    def log(self):
        """记录到日志"""
        log_msg = f"[{self.code.value}] {self.message}"

        if self.context:
            log_msg += f" | Context: {self.context}"

        if self.level == WarningLevel.INFO:
            logger.info(log_msg)
        elif self.level == WarningLevel.WARNING:
            logger.warning(log_msg)
        elif self.level == WarningLevel.ERROR:
            logger.error(log_msg, exc_info=self.exception)
        elif self.level == WarningLevel.CRITICAL:
            logger.critical(log_msg, exc_info=self.exception)


@dataclass
class WarningCollector:
    """
    警告收集器

    用于在评估过程中收集所有警告。

    Examples:
        >>> collector = WarningCollector()
        >>> collector.add_warning(
        ...     WarningCode.RULE_EXECUTION_ERROR,
        ...     "规则执行失败",
        ...     level=WarningLevel.ERROR,
        ...     context={"rule": "veto_rule_1"}
        ... )
        >>> warnings = collector.get_warnings()
    """
    warnings: List[EvaluatorWarning] = field(default_factory=list)

    def add_warning(
        self,
        code: WarningCode,
        message: str,
        level: WarningLevel = WarningLevel.WARNING,
        context: Optional[Dict[str, Any]] = None,
        exception: Optional[Exception] = None,
        auto_log: bool = True
    ):
        """
        添加警告

        Args:
            code: 警告代码
            message: 警告消息
            level: 警告级别
            context: 上下文信息
            exception: 关联异常
            auto_log: 是否自动记录到日志
        """
        warning = EvaluatorWarning(
            code=code,
            level=level,
            message=message,
            context=context or {},
            exception=exception
        )

        self.warnings.append(warning)

        if auto_log:
            warning.log()

    def add_rule_error(
        self,
        rule_name: str,
        exception: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        """添加规则执行错误 (快捷方法)"""
        ctx = {"rule_name": rule_name}
        if context:
            ctx.update(context)

        self.add_warning(
            WarningCode.RULE_EXECUTION_ERROR,
            f"规则执行失败: {rule_name}",
            level=WarningLevel.ERROR,
            context=ctx,
            exception=exception
        )

    def add_data_error(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """添加数据质量错误 (快捷方法)"""
        self.add_warning(
            WarningCode.INVALID_DATA,
            message,
            level=WarningLevel.ERROR,
            context=context
        )

    def add_config_error(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ):
        """添加配置错误 (快捷方法)"""
        self.add_warning(
            WarningCode.INVALID_CONFIG,
            message,
            level=WarningLevel.ERROR,
            context=context
        )

    def get_warnings(
        self,
        level: Optional[WarningLevel] = None
    ) -> List[EvaluatorWarning]:
        """
        获取警告列表

        Args:
            level: 过滤级别 (可选)

        Returns:
            警告列表
        """
        if level is None:
            return self.warnings

        return [w for w in self.warnings if w.level == level]

    def has_errors(self) -> bool:
        """是否有错误级别以上的警告"""
        return any(
            w.level in {WarningLevel.ERROR, WarningLevel.CRITICAL}
            for w in self.warnings
        )

    def has_critical(self) -> bool:
        """是否有严重错误"""
        return any(w.level == WarningLevel.CRITICAL for w in self.warnings)

    def clear(self):
        """清空警告"""
        self.warnings.clear()

    def to_dict(self) -> Dict[str, Any]:
        """序列化为字典"""
        return {
            "total_warnings": len(self.warnings),
            "by_level": {
                "info": len(self.get_warnings(WarningLevel.INFO)),
                "warning": len(self.get_warnings(WarningLevel.WARNING)),
                "error": len(self.get_warnings(WarningLevel.ERROR)),
                "critical": len(self.get_warnings(WarningLevel.CRITICAL)),
            },
            "warnings": [w.to_dict() for w in self.warnings],
        }


# ============================================================================
# 快捷函数
# ============================================================================

def create_warning(
    code: WarningCode,
    message: str,
    level: WarningLevel = WarningLevel.WARNING,
    **context
) -> EvaluatorWarning:
    """
    创建警告 (快捷函数)

    Args:
        code: 警告代码
        message: 警告消息
        level: 警告级别
        **context: 上下文信息

    Returns:
        警告对象
    """
    return EvaluatorWarning(
        code=code,
        level=level,
        message=message,
        context=context
    )


def log_warning(
    code: WarningCode,
    message: str,
    level: WarningLevel = WarningLevel.WARNING,
    **context
):
    """
    创建并记录警告 (快捷函数)

    Args:
        code: 警告代码
        message: 警告消息
        level: 警告级别
        **context: 上下文信息
    """
    warning = create_warning(code, message, level, **context)
    warning.log()


__all__ = [
    # 枚举
    'WarningLevel',
    'WarningCode',
    # 数据类
    'EvaluatorWarning',
    'WarningCollector',
    # 快捷函数
    'create_warning',
    'log_warning',
]
