"""
PGCS Validation: Built-in Validators
====================================

内置通用验证器集合。

所有验证器都是工厂函数，返回 Validator 实例。
这种设计允许验证器配置化且可重用。
"""

from __future__ import annotations

import re
from typing import Any, Optional, List, Pattern, Callable, TYPE_CHECKING

from .base import Validator, ValidationResult, ValidationContext, FunctionValidator

if TYPE_CHECKING:
    from ..core.field import FieldDescriptor


class RequiredValidator(Validator):
    """必填验证器"""

    def __init__(self, message: str = 'This field is required'):
        self.message = message

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.error(self.message, code='required')

        # 空字符串也视为缺失
        if isinstance(value, str) and not value.strip():
            return ValidationResult.error(self.message, code='required')

        return ValidationResult.ok()


class OptionalValidator(Validator):
    """可选验证器 - 如果值为 None 则跳过后续验证"""

    def __init__(self, inner: Validator):
        self.inner = inner

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()
        return self.inner.validate(value, field, context)


class RangeValidator(Validator):
    """范围验证器"""

    def __init__(
        self,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        inclusive: bool = True,
    ):
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()

        try:
            num = float(value)
        except (TypeError, ValueError):
            return ValidationResult.error(
                f"Value must be a number, got {type(value).__name__}",
                code='type_error',
            )

        if self.min_value is not None:
            if self.inclusive and num < self.min_value:
                return ValidationResult.error(
                    f"Value {num} is less than minimum {self.min_value}",
                    code='min_value',
                )
            if not self.inclusive and num <= self.min_value:
                return ValidationResult.error(
                    f"Value {num} must be greater than {self.min_value}",
                    code='min_value_exclusive',
                )

        if self.max_value is not None:
            if self.inclusive and num > self.max_value:
                return ValidationResult.error(
                    f"Value {num} exceeds maximum {self.max_value}",
                    code='max_value',
                )
            if not self.inclusive and num >= self.max_value:
                return ValidationResult.error(
                    f"Value {num} must be less than {self.max_value}",
                    code='max_value_exclusive',
                )

        return ValidationResult.ok()


class LengthValidator(Validator):
    """长度验证器"""

    def __init__(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ):
        self.min_length = min_length
        self.max_length = max_length

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()

        try:
            length = len(value)
        except TypeError:
            return ValidationResult.error(
                f"Value does not support len(), got {type(value).__name__}",
                code='type_error',
            )

        if self.min_length is not None and length < self.min_length:
            return ValidationResult.error(
                f"Length {length} is less than minimum {self.min_length}",
                code='min_length',
            )

        if self.max_length is not None and length > self.max_length:
            return ValidationResult.error(
                f"Length {length} exceeds maximum {self.max_length}",
                code='max_length',
            )

        return ValidationResult.ok()


class PatternValidator(Validator):
    """正则表达式验证器"""

    def __init__(
        self,
        pattern: str,
        message: Optional[str] = None,
        flags: int = 0,
    ):
        self.pattern_str = pattern
        self.pattern = re.compile(pattern, flags)
        self.message = message or f"Value does not match pattern: {pattern}"

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()

        if not isinstance(value, str):
            return ValidationResult.error(
                f"Pattern validation requires string, got {type(value).__name__}",
                code='type_error',
            )

        if not self.pattern.match(value):
            return ValidationResult.error(self.message, code='pattern')

        return ValidationResult.ok()


class ChoicesValidator(Validator):
    """选项验证器"""

    def __init__(self, choices: List[Any]):
        self.choices = choices

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()

        if value not in self.choices:
            return ValidationResult.error(
                f"Value must be one of {self.choices}, got {value!r}",
                code='choices',
            )

        return ValidationResult.ok()


class TypeValidator(Validator):
    """类型验证器"""

    def __init__(self, expected_type: type, strict: bool = False):
        self.expected_type = expected_type
        self.strict = strict

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        if value is None:
            return ValidationResult.ok()

        if self.strict:
            if type(value) != self.expected_type:
                return ValidationResult.error(
                    f"Expected type {self.expected_type.__name__}, got {type(value).__name__}",
                    code='type_error',
                )
        else:
            if not isinstance(value, self.expected_type):
                return ValidationResult.error(
                    f"Expected instance of {self.expected_type.__name__}, got {type(value).__name__}",
                    code='type_error',
                )

        return ValidationResult.ok()


# ============================================================================
# 工厂函数 (推荐使用方式)
# ============================================================================

def required(message: str = 'This field is required') -> Validator:
    """必填验证器"""
    return RequiredValidator(message)


def optional(inner: Validator) -> Validator:
    """可选验证器 - 值为 None 时跳过内部验证"""
    return OptionalValidator(inner)


def range_check(
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    inclusive: bool = True,
) -> Validator:
    """范围验证器"""
    return RangeValidator(min_value, max_value, inclusive)


def min_value(value: float, inclusive: bool = True) -> Validator:
    """最小值验证器"""
    return RangeValidator(min_value=value, inclusive=inclusive)


def max_value(value: float, inclusive: bool = True) -> Validator:
    """最大值验证器"""
    return RangeValidator(max_value=value, inclusive=inclusive)


def min_length(length: int) -> Validator:
    """最小长度验证器"""
    return LengthValidator(min_length=length)


def max_length(length: int) -> Validator:
    """最大长度验证器"""
    return LengthValidator(max_length=length)


def length(min_len: Optional[int] = None, max_len: Optional[int] = None) -> Validator:
    """长度验证器"""
    return LengthValidator(min_len, max_len)


def pattern(regex: str, message: Optional[str] = None, flags: int = 0) -> Validator:
    """正则表达式验证器"""
    return PatternValidator(regex, message, flags)


def choices(allowed: List[Any]) -> Validator:
    """选项验证器"""
    return ChoicesValidator(allowed)


def type_check(expected: type, strict: bool = False) -> Validator:
    """类型验证器"""
    return TypeValidator(expected, strict)


def custom(
    func: Callable[[Any], bool],
    message: str = 'Custom validation failed',
    code: str = 'custom',
) -> Validator:
    """自定义验证器"""
    return FunctionValidator(func, message, code)


# 常用预置验证器
email = pattern(
    r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    message='Invalid email address',
)

url = pattern(
    r'^https?://[^\s/$.?#].[^\s]*$',
    message='Invalid URL',
)

uuid = pattern(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    message='Invalid UUID',
    flags=re.IGNORECASE,
)


__all__ = [
    # 验证器类
    'RequiredValidator',
    'OptionalValidator',
    'RangeValidator',
    'LengthValidator',
    'PatternValidator',
    'ChoicesValidator',
    'TypeValidator',

    # 工厂函数
    'required',
    'optional',
    'range_check',
    'min_value',
    'max_value',
    'min_length',
    'max_length',
    'length',
    'pattern',
    'choices',
    'type_check',
    'custom',

    # 预置验证器
    'email',
    'url',
    'uuid',
]
