"""
注册表错误 (Registry Errors)
=============================

Orchestrator 方法注册相关的错误定义。
"""
from typing import Any, Dict, List, Optional

from .base import AStockError
from .codes import ErrorCode


class RegistryError(AStockError):
    """注册表基础错误"""
    default_code = ErrorCode.REGISTRY_NOT_FOUND


class MethodNotFoundError(RegistryError):
    """方法未找到错误

    当请求的方法在注册表中不存在时抛出。
    """
    default_code = ErrorCode.REGISTRY_NOT_FOUND

    def __init__(
        self,
        method_name: str,
        *,
        engine_type: Optional[str] = None,
        component_type: Optional[str] = None,
        available_methods: Optional[List[str]] = None,
        **kwargs
    ):
        message = f"Method '{method_name}' not found in registry"
        if engine_type:
            message += f" (engine_type={engine_type})"
        if component_type:
            message += f" (component_type={component_type})"

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            engine_type=engine_type,
            component_type=component_type,
            available_methods=available_methods or [],
        )


class DuplicateMethodError(RegistryError):
    """方法重复注册错误

    当尝试注册已存在的方法时抛出。
    """
    default_code = ErrorCode.REGISTRY_DUPLICATE

    def __init__(
        self,
        method_name: str,
        *,
        existing_version: Optional[str] = None,
        new_version: Optional[str] = None,
        **kwargs
    ):
        message = f"Method '{method_name}' is already registered"
        if existing_version and new_version:
            message += f" (existing: v{existing_version}, new: v{new_version})"

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            existing_version=existing_version,
            new_version=new_version,
        )


class RegistryVersionConflictError(RegistryError):
    """版本冲突错误

    当方法版本不兼容时抛出。
    """
    default_code = ErrorCode.REGISTRY_VERSION_CONFLICT

    def __init__(
        self,
        method_name: str,
        required_version: str,
        available_version: str,
        **kwargs
    ):
        message = (
            f"Version conflict for method '{method_name}': "
            f"required {required_version}, available {available_version}"
        )

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            required_version=required_version,
            available_version=available_version,
        )


class RegistryInitializationError(RegistryError):
    """注册表初始化错误"""
    default_code = ErrorCode.REGISTRY_INITIALIZATION_FAILED

    def __init__(
        self,
        reason: str,
        *,
        registry_name: Optional[str] = None,
        **kwargs
    ):
        message = f"Failed to initialize registry: {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            reason=reason,
            registry_name=registry_name,
        )


class SignatureValidationError(RegistryError):
    """方法签名验证错误

    当方法签名不符合预期时抛出。
    """
    default_code = ErrorCode.VALIDATION_SIGNATURE_MISMATCH

    def __init__(
        self,
        method_name: str,
        *,
        expected_params: Optional[List[str]] = None,
        actual_params: Optional[List[str]] = None,
        missing_params: Optional[List[str]] = None,
        extra_params: Optional[List[str]] = None,
        **kwargs
    ):
        issues = []
        if missing_params:
            issues.append(f"missing: {missing_params}")
        if extra_params:
            issues.append(f"extra: {extra_params}")

        message = f"Signature validation failed for '{method_name}'"
        if issues:
            message += f" ({', '.join(issues)})"

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            expected_params=expected_params,
            actual_params=actual_params,
            missing_params=missing_params,
            extra_params=extra_params,
        )


class MethodExecutionError(RegistryError):
    """方法执行错误

    当注册的方法执行失败时抛出。
    """
    default_code = ErrorCode.PIPELINE_METHOD_EXECUTION_FAILED

    def __init__(
        self,
        method_name: str,
        *,
        error_message: str = "",
        execution_time: Optional[float] = None,
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        message = f"Execution failed for method '{method_name}'"
        if error_message:
            message += f": {error_message}"

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            error_message=error_message,
            execution_time=execution_time,
            parameters=parameters,
        )
