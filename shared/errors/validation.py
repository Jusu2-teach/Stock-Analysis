"""
验证错误 (Validation Errors)
=============================

数据验证、Schema 验证相关的错误定义。
"""
from typing import Any, Dict, List, Optional, Type

from .base import AStockError
from .codes import ErrorCode


class ValidationError(AStockError):
    """验证基础错误"""
    default_code = ErrorCode.VALIDATION_FAILED


class SchemaValidationError(ValidationError):
    """Schema 验证错误

    当数据不符合定义的 Schema 时抛出。
    与 PGCS 契约系统集成。
    """
    default_code = ErrorCode.VALIDATION_SCHEMA_INVALID

    def __init__(
        self,
        schema_name: str,
        *,
        violations: Optional[List[Dict[str, Any]]] = None,
        data_sample: Optional[Any] = None,
        **kwargs
    ):
        violations = violations or []
        violation_count = len(violations)

        message = f"Schema validation failed for '{schema_name}'"
        if violation_count:
            message += f" ({violation_count} violation(s))"

        super().__init__(message, **kwargs)

        self.with_context(
            schema_name=schema_name,
            violations=violations,
            violation_count=violation_count,
            data_sample=data_sample,
        )

        self._violations = violations

    @property
    def violations(self) -> List[Dict[str, Any]]:
        """获取所有违规详情"""
        return self._violations


class TypeValidationError(ValidationError):
    """类型验证错误"""
    default_code = ErrorCode.VALIDATION_TYPE_ERROR

    def __init__(
        self,
        field_name: str,
        expected_type: Type | str,
        actual_type: Type | str,
        *,
        value: Optional[Any] = None,
        **kwargs
    ):
        expected = expected_type.__name__ if isinstance(expected_type, type) else str(expected_type)
        actual = actual_type.__name__ if isinstance(actual_type, type) else str(actual_type)

        message = f"Type error for '{field_name}': expected {expected}, got {actual}"

        super().__init__(message, **kwargs)

        self.with_context(
            field_name=field_name,
            expected_type=expected,
            actual_type=actual,
            value=repr(value) if value is not None else None,
        )


class RequiredFieldError(ValidationError):
    """必填字段缺失错误"""
    default_code = ErrorCode.VALIDATION_REQUIRED_MISSING

    def __init__(
        self,
        field_name: str | List[str],
        *,
        schema_name: Optional[str] = None,
        **kwargs
    ):
        if isinstance(field_name, list):
            fields = field_name
            message = f"Required fields missing: {fields}"
        else:
            fields = [field_name]
            message = f"Required field missing: '{field_name}'"

        if schema_name:
            message += f" in schema '{schema_name}'"

        super().__init__(message, **kwargs)

        self.with_context(
            missing_fields=fields,
            schema_name=schema_name,
        )


class RangeValidationError(ValidationError):
    """范围验证错误"""
    default_code = ErrorCode.VALIDATION_FAILED

    def __init__(
        self,
        field_name: str,
        value: Any,
        *,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        **kwargs
    ):
        constraints = []
        if min_value is not None:
            constraints.append(f"min={min_value}")
        if max_value is not None:
            constraints.append(f"max={max_value}")

        message = f"Value {value} for '{field_name}' out of range"
        if constraints:
            message += f" ({', '.join(constraints)})"

        super().__init__(message, **kwargs)

        self.with_context(
            field_name=field_name,
            value=value,
            min_value=min_value,
            max_value=max_value,
        )


class FormatValidationError(ValidationError):
    """格式验证错误"""
    default_code = ErrorCode.VALIDATION_FAILED

    def __init__(
        self,
        field_name: str,
        value: str,
        expected_format: str,
        *,
        pattern: Optional[str] = None,
        **kwargs
    ):
        message = f"Invalid format for '{field_name}': expected {expected_format}"

        super().__init__(message, **kwargs)

        self.with_context(
            field_name=field_name,
            value=value,
            expected_format=expected_format,
            pattern=pattern,
        )


class SignatureMismatchError(ValidationError):
    """方法签名不匹配错误"""
    default_code = ErrorCode.VALIDATION_SIGNATURE_MISMATCH

    def __init__(
        self,
        method_name: str,
        *,
        missing_args: Optional[List[str]] = None,
        extra_args: Optional[List[str]] = None,
        type_mismatches: Optional[Dict[str, Dict[str, str]]] = None,
        **kwargs
    ):
        issues = []
        if missing_args:
            issues.append(f"missing: {missing_args}")
        if extra_args:
            issues.append(f"unexpected: {extra_args}")
        if type_mismatches:
            issues.append(f"type errors: {list(type_mismatches.keys())}")

        message = f"Signature mismatch for '{method_name}'"
        if issues:
            message += f" ({'; '.join(issues)})"

        super().__init__(message, **kwargs)

        self.with_context(
            method_name=method_name,
            missing_args=missing_args,
            extra_args=extra_args,
            type_mismatches=type_mismatches,
        )


class ContractViolationError(ValidationError):
    """契约违反错误

    PGCS (Proactive Guardrail Contract System) 契约违反。
    """
    default_code = ErrorCode.VALIDATION_CONTRACT_VIOLATION

    def __init__(
        self,
        contract_name: str,
        violation_type: str,  # "precondition", "postcondition", "invariant"
        *,
        condition: Optional[str] = None,
        actual_state: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        message = f"Contract '{contract_name}' {violation_type} violated"
        if condition:
            message += f": {condition}"

        super().__init__(message, **kwargs)

        self.with_context(
            contract_name=contract_name,
            violation_type=violation_type,
            condition=condition,
            actual_state=actual_state,
        )
