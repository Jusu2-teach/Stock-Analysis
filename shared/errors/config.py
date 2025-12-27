"""
配置错误 (Configuration Errors)
================================

配置加载、验证相关的错误定义。
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import AStockError
from .codes import ErrorCode


class ConfigError(AStockError):
    """配置基础错误"""
    default_code = ErrorCode.CONFIG_NOT_FOUND


class ConfigNotFoundError(ConfigError):
    """配置文件未找到错误"""
    default_code = ErrorCode.CONFIG_NOT_FOUND

    def __init__(
        self,
        config_path: str | Path,
        *,
        searched_paths: Optional[List[str]] = None,
        config_type: str = "yaml",
        **kwargs
    ):
        path = Path(config_path)
        message = f"Configuration file not found: '{path}'"

        super().__init__(message, **kwargs)

        self.with_context(
            config_path=str(path),
            config_name=path.name,
            searched_paths=searched_paths,
            config_type=config_type,
        )


class ConfigParseError(ConfigError):
    """配置解析错误"""
    default_code = ErrorCode.CONFIG_PARSE_ERROR

    def __init__(
        self,
        config_path: str | Path,
        *,
        line_number: Optional[int] = None,
        column: Optional[int] = None,
        reason: str = "",
        **kwargs
    ):
        path = Path(config_path)
        message = f"Failed to parse configuration '{path.name}'"

        if line_number:
            message += f" at line {line_number}"
            if column:
                message += f", column {column}"

        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            config_path=str(path),
            line_number=line_number,
            column=column,
            reason=reason,
        )


class ConfigValidationError(ConfigError):
    """配置验证错误"""
    default_code = ErrorCode.CONFIG_VALIDATION_FAILED

    def __init__(
        self,
        config_name: str,
        *,
        invalid_keys: Optional[List[str]] = None,
        missing_keys: Optional[List[str]] = None,
        type_errors: Optional[Dict[str, Dict[str, str]]] = None,
        **kwargs
    ):
        issues = []
        if missing_keys:
            issues.append(f"missing: {missing_keys}")
        if invalid_keys:
            issues.append(f"invalid: {invalid_keys}")
        if type_errors:
            issues.append(f"type errors: {list(type_errors.keys())}")

        message = f"Configuration validation failed for '{config_name}'"
        if issues:
            message += f" ({'; '.join(issues)})"

        super().__init__(message, **kwargs)

        self.with_context(
            config_name=config_name,
            missing_keys=missing_keys,
            invalid_keys=invalid_keys,
            type_errors=type_errors,
        )


class ConfigKeyError(ConfigError):
    """配置键错误"""
    default_code = ErrorCode.CONFIG_KEY_ERROR

    def __init__(
        self,
        key: str,
        *,
        config_name: Optional[str] = None,
        available_keys: Optional[List[str]] = None,
        suggested_key: Optional[str] = None,
        **kwargs
    ):
        message = f"Configuration key not found: '{key}'"
        if config_name:
            message += f" in '{config_name}'"
        if suggested_key:
            message += f" (did you mean '{suggested_key}'?)"

        super().__init__(message, **kwargs)

        self.with_context(
            key=key,
            config_name=config_name,
            available_keys=available_keys,
            suggested_key=suggested_key,
        )


class ConfigTypeError(ConfigError):
    """配置类型错误"""
    default_code = ErrorCode.CONFIG_VALIDATION_FAILED

    def __init__(
        self,
        key: str,
        expected_type: str,
        actual_type: str,
        *,
        value: Optional[Any] = None,
        config_name: Optional[str] = None,
        **kwargs
    ):
        message = f"Invalid type for config key '{key}': expected {expected_type}, got {actual_type}"
        if config_name:
            message += f" in '{config_name}'"

        super().__init__(message, **kwargs)

        self.with_context(
            key=key,
            expected_type=expected_type,
            actual_type=actual_type,
            value=repr(value) if value is not None else None,
            config_name=config_name,
        )


class EnvironmentVariableError(ConfigError):
    """环境变量错误"""
    default_code = ErrorCode.CONFIG_ENV_MISSING

    def __init__(
        self,
        var_name: str,
        *,
        default_available: bool = False,
        required_for: Optional[str] = None,
        **kwargs
    ):
        message = f"Environment variable not set: '{var_name}'"
        if required_for:
            message += f" (required for {required_for})"

        super().__init__(message, **kwargs)

        self.with_context(
            var_name=var_name,
            default_available=default_available,
            required_for=required_for,
        )


class WorkflowConfigError(ConfigError):
    """工作流配置错误

    YAML 工作流定义中的配置问题。
    """
    default_code = ErrorCode.CONFIG_VALIDATION_FAILED

    def __init__(
        self,
        workflow_path: str | Path,
        *,
        step_name: Optional[str] = None,
        field_name: Optional[str] = None,
        reason: str = "",
        **kwargs
    ):
        path = Path(workflow_path)
        message = f"Workflow configuration error in '{path.name}'"

        if step_name:
            message += f" at step '{step_name}'"
        if field_name:
            message += f", field '{field_name}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            workflow_path=str(path),
            step_name=step_name,
            field_name=field_name,
            reason=reason,
        )
