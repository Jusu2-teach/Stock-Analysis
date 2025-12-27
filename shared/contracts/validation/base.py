"""
PGCS Validation: Base
=====================

验证器基础设施。

设计原则:
- 验证器是无状态的纯函数包装
- 支持组合 (AND, OR, NOT)
- 支持上下文传递
- 完全通用，不包含业务逻辑
"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import Any, Optional, Dict, List, Callable, TYPE_CHECKING
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..core.field import FieldDescriptor


@dataclass
class ValidationResult:
    """
    验证结果

    Attributes:
        is_valid: 是否验证通过
        message: 错误消息 (验证失败时)
        code: 错误代码
        context: 额外上下文信息
    """
    is_valid: bool = True
    message: str = ''
    code: str = ''
    context: Dict[str, Any] = dataclass_field(default_factory=dict)

    @classmethod
    def ok(cls) -> 'ValidationResult':
        """创建成功结果"""
        return cls(is_valid=True)

    @classmethod
    def error(cls, message: str, code: str = 'validation_error', **context) -> 'ValidationResult':
        """创建失败结果"""
        return cls(is_valid=False, message=message, code=code, context=context)


@dataclass
class ValidationContext:
    """
    验证上下文

    在验证过程中传递的上下文信息，支持:
    - 字段信息
    - 父对象引用
    - 自定义数据
    """
    field: Optional['FieldDescriptor'] = None
    parent: Any = None
    path: str = ''
    data: Dict[str, Any] = dataclass_field(default_factory=dict)

    def child(self, name: str) -> 'ValidationContext':
        """创建子上下文"""
        return ValidationContext(
            field=self.field,
            parent=self.parent,
            path=f"{self.path}.{name}" if self.path else name,
            data=self.data.copy(),
        )


class Validator(ABC):
    """
    验证器抽象基类

    所有验证器必须实现 validate 方法。验证器应该是:
    - 无状态的
    - 可重用的
    - 可组合的

    Example:
        class MaxLengthValidator(Validator):
            def __init__(self, max_len: int):
                self.max_len = max_len

            def validate(self, value, field, context) -> ValidationResult:
                if value is not None and len(value) > self.max_len:
                    return ValidationResult.error(
                        f"Length exceeds {self.max_len}"
                    )
                return ValidationResult.ok()
    """

    @abstractmethod
    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        """
        执行验证

        Args:
            value: 要验证的值
            field: 字段描述符 (可选)
            context: 验证上下文 (可选)

        Returns:
            ValidationResult
        """
        pass

    def __and__(self, other: 'Validator') -> 'AndValidator':
        """组合验证器 (AND)"""
        return AndValidator(self, other)

    def __or__(self, other: 'Validator') -> 'OrValidator':
        """组合验证器 (OR)"""
        return OrValidator(self, other)

    def __invert__(self) -> 'NotValidator':
        """取反验证器 (NOT)"""
        return NotValidator(self)


class AndValidator(Validator):
    """AND 组合验证器"""

    def __init__(self, *validators: Validator):
        self.validators = validators

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        for v in self.validators:
            result = v.validate(value, field, context)
            if not result.is_valid:
                return result
        return ValidationResult.ok()


class OrValidator(Validator):
    """OR 组合验证器"""

    def __init__(self, *validators: Validator):
        self.validators = validators

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        errors = []
        for v in self.validators:
            result = v.validate(value, field, context)
            if result.is_valid:
                return result
            errors.append(result.message)

        return ValidationResult.error(
            f"None of the validators passed: {'; '.join(errors)}",
            code='or_validation_failed',
        )


class NotValidator(Validator):
    """NOT 取反验证器"""

    def __init__(self, validator: Validator):
        self.validator = validator

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        result = self.validator.validate(value, field, context)
        if result.is_valid:
            return ValidationResult.error(
                "Validation should have failed but passed",
                code='not_validation_failed',
            )
        return ValidationResult.ok()


class FunctionValidator(Validator):
    """
    函数验证器

    将任意函数包装为验证器。

    Example:
        is_even = FunctionValidator(
            lambda x: x % 2 == 0,
            "Value must be even"
        )
    """

    def __init__(
        self,
        func: Callable[[Any], bool],
        message: str = 'Validation failed',
        code: str = 'custom_validation_error',
    ):
        self.func = func
        self.message = message
        self.code = code

    def validate(
        self,
        value: Any,
        field: Optional['FieldDescriptor'] = None,
        context: Optional[ValidationContext] = None,
    ) -> ValidationResult:
        try:
            if self.func(value):
                return ValidationResult.ok()
            return ValidationResult.error(self.message, self.code)
        except Exception as e:
            return ValidationResult.error(
                f"Validation error: {e}",
                code='validation_exception',
            )


__all__ = [
    'Validator',
    'ValidationResult',
    'ValidationContext',
    'AndValidator',
    'OrValidator',
    'NotValidator',
    'FunctionValidator',
]
