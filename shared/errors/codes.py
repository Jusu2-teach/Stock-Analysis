"""
错误码定义 (Error Codes)
========================

参考:
- HTTP 状态码分层设计
- gRPC 错误码
- AWS/Azure 错误码命名规范

错误码格式: ASTOCK-{CATEGORY}-{NUMBER}
- CATEGORY: REG(Registry), PIP(Pipeline), VAL(Validation), DAT(Data), CFG(Config), SYS(System)
- NUMBER: 3位数字，同类错误按序编号
"""
from enum import Enum, IntEnum
from typing import NamedTuple


class ErrorSeverity(IntEnum):
    """错误严重程度

    参考 Syslog 严重级别 (RFC 5424)
    """
    DEBUG = 0       # 调试信息
    INFO = 1        # 一般信息
    WARNING = 2     # 警告，可恢复
    ERROR = 3       # 错误，操作失败
    CRITICAL = 4    # 严重，系统不稳定
    FATAL = 5       # 致命，需要立即干预


class ErrorCodeInfo(NamedTuple):
    """错误码元信息"""
    code: str
    message: str
    severity: ErrorSeverity
    category: str
    http_status: int = 500  # 对应的 HTTP 状态码（供 API 使用）


class ErrorCode(Enum):
    """
    统一错误码枚举

    命名规范:
    - {CATEGORY}_{SPECIFIC_ERROR}
    - 全大写，下划线分隔
    """

    # =========================================================================
    # Registry 错误 (REG-0xx)
    # =========================================================================
    REGISTRY_METHOD_NOT_FOUND = ErrorCodeInfo(
        "ASTOCK-REG-001",
        "Requested method not found in registry",
        ErrorSeverity.ERROR,
        "registry",
        404
    )
    REGISTRY_CONFLICT = ErrorCodeInfo(
        "ASTOCK-REG-002",
        "Method registration conflict",
        ErrorSeverity.WARNING,
        "registry",
        409
    )
    REGISTRY_VALIDATION_FAILED = ErrorCodeInfo(
        "ASTOCK-REG-003",
        "Method validation failed",
        ErrorSeverity.ERROR,
        "registry",
        400
    )
    REGISTRY_EXECUTION_FAILED = ErrorCodeInfo(
        "ASTOCK-REG-004",
        "Method execution failed",
        ErrorSeverity.ERROR,
        "registry",
        500
    )
    REGISTRY_STRATEGY_ERROR = ErrorCodeInfo(
        "ASTOCK-REG-005",
        "Strategy selection failed",
        ErrorSeverity.ERROR,
        "registry",
        500
    )

    # =========================================================================
    # Pipeline 错误 (PIP-1xx)
    # =========================================================================
    PIPELINE_CONFIG_INVALID = ErrorCodeInfo(
        "ASTOCK-PIP-101",
        "Pipeline configuration is invalid",
        ErrorSeverity.ERROR,
        "pipeline",
        400
    )
    PIPELINE_STEP_FAILED = ErrorCodeInfo(
        "ASTOCK-PIP-102",
        "Pipeline step execution failed",
        ErrorSeverity.ERROR,
        "pipeline",
        500
    )
    PIPELINE_TIMEOUT = ErrorCodeInfo(
        "ASTOCK-PIP-103",
        "Pipeline execution timeout",
        ErrorSeverity.ERROR,
        "pipeline",
        408
    )
    PIPELINE_DEPENDENCY_ERROR = ErrorCodeInfo(
        "ASTOCK-PIP-104",
        "Pipeline dependency resolution failed",
        ErrorSeverity.ERROR,
        "pipeline",
        400
    )
    PIPELINE_REFERENCE_ERROR = ErrorCodeInfo(
        "ASTOCK-PIP-105",
        "Step reference resolution failed",
        ErrorSeverity.ERROR,
        "pipeline",
        400
    )
    PIPELINE_CYCLE_DETECTED = ErrorCodeInfo(
        "ASTOCK-PIP-106",
        "Circular dependency detected in pipeline",
        ErrorSeverity.CRITICAL,
        "pipeline",
        400
    )

    # =========================================================================
    # Validation 错误 (VAL-2xx)
    # =========================================================================
    VALIDATION_SCHEMA_ERROR = ErrorCodeInfo(
        "ASTOCK-VAL-201",
        "Schema validation failed",
        ErrorSeverity.ERROR,
        "validation",
        400
    )
    VALIDATION_TYPE_ERROR = ErrorCodeInfo(
        "ASTOCK-VAL-202",
        "Type validation failed",
        ErrorSeverity.ERROR,
        "validation",
        400
    )
    VALIDATION_CONSTRAINT_ERROR = ErrorCodeInfo(
        "ASTOCK-VAL-203",
        "Constraint violation",
        ErrorSeverity.ERROR,
        "validation",
        400
    )
    VALIDATION_REQUIRED_MISSING = ErrorCodeInfo(
        "ASTOCK-VAL-204",
        "Required field is missing",
        ErrorSeverity.ERROR,
        "validation",
        400
    )

    # =========================================================================
    # Data 错误 (DAT-3xx)
    # =========================================================================
    DATA_LOAD_ERROR = ErrorCodeInfo(
        "ASTOCK-DAT-301",
        "Failed to load data",
        ErrorSeverity.ERROR,
        "data",
        500
    )
    DATA_TRANSFORM_ERROR = ErrorCodeInfo(
        "ASTOCK-DAT-302",
        "Data transformation failed",
        ErrorSeverity.ERROR,
        "data",
        500
    )
    DATA_SAVE_ERROR = ErrorCodeInfo(
        "ASTOCK-DAT-303",
        "Failed to save data",
        ErrorSeverity.ERROR,
        "data",
        500
    )
    DATA_FORMAT_ERROR = ErrorCodeInfo(
        "ASTOCK-DAT-304",
        "Invalid data format",
        ErrorSeverity.ERROR,
        "data",
        400
    )
    DATA_NOT_FOUND = ErrorCodeInfo(
        "ASTOCK-DAT-305",
        "Data not found",
        ErrorSeverity.ERROR,
        "data",
        404
    )
    DATA_CORRUPTION = ErrorCodeInfo(
        "ASTOCK-DAT-306",
        "Data corruption detected",
        ErrorSeverity.CRITICAL,
        "data",
        500
    )

    # =========================================================================
    # Configuration 错误 (CFG-4xx)
    # =========================================================================
    CONFIG_NOT_FOUND = ErrorCodeInfo(
        "ASTOCK-CFG-401",
        "Configuration file not found",
        ErrorSeverity.ERROR,
        "config",
        404
    )
    CONFIG_PARSE_ERROR = ErrorCodeInfo(
        "ASTOCK-CFG-402",
        "Failed to parse configuration",
        ErrorSeverity.ERROR,
        "config",
        400
    )
    CONFIG_VALIDATION_ERROR = ErrorCodeInfo(
        "ASTOCK-CFG-403",
        "Configuration validation failed",
        ErrorSeverity.ERROR,
        "config",
        400
    )
    CONFIG_MISSING_REQUIRED = ErrorCodeInfo(
        "ASTOCK-CFG-404",
        "Required configuration is missing",
        ErrorSeverity.ERROR,
        "config",
        400
    )

    # =========================================================================
    # System 错误 (SYS-9xx)
    # =========================================================================
    SYSTEM_INTERNAL_ERROR = ErrorCodeInfo(
        "ASTOCK-SYS-901",
        "Internal system error",
        ErrorSeverity.CRITICAL,
        "system",
        500
    )
    SYSTEM_RESOURCE_EXHAUSTED = ErrorCodeInfo(
        "ASTOCK-SYS-902",
        "System resources exhausted",
        ErrorSeverity.CRITICAL,
        "system",
        503
    )
    SYSTEM_DEPENDENCY_UNAVAILABLE = ErrorCodeInfo(
        "ASTOCK-SYS-903",
        "External dependency unavailable",
        ErrorSeverity.ERROR,
        "system",
        503
    )

    # =========================================================================
    # 辅助方法
    # =========================================================================

    @property
    def code(self) -> str:
        """获取错误码字符串"""
        return self.value.code

    @property
    def default_message(self) -> str:
        """获取默认错误消息"""
        return self.value.message

    @property
    def severity(self) -> ErrorSeverity:
        """获取错误严重程度"""
        return self.value.severity

    @property
    def category(self) -> str:
        """获取错误分类"""
        return self.value.category

    @property
    def http_status(self) -> int:
        """获取对应的 HTTP 状态码"""
        return self.value.http_status


# 便捷查询函数
def get_error_by_code(code: str) -> ErrorCode | None:
    """根据错误码字符串查找 ErrorCode"""
    for ec in ErrorCode:
        if ec.code == code:
            return ec
    return None


def list_errors_by_category(category: str) -> list[ErrorCode]:
    """列出指定分类的所有错误码"""
    return [ec for ec in ErrorCode if ec.category == category]


def list_errors_by_severity(severity: ErrorSeverity) -> list[ErrorCode]:
    """列出指定严重程度的所有错误码"""
    return [ec for ec in ErrorCode if ec.severity >= severity]
