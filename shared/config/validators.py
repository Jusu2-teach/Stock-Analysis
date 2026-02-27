"""
配置验证器 (Config Validators)
===============================

参考设计:
- pydantic: 字段验证
- cerberus: Schema 验证
- jsonschema: JSON Schema

提供配置验证功能。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type
import re


@dataclass
class ValidationError:
    """验证错误"""
    path: str
    message: str
    value: Any = None
    expected: Optional[str] = None


@dataclass
class ValidationResult:
    """验证结果"""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)

    def add_error(
        self,
        path: str,
        message: str,
        value: Any = None,
        expected: Optional[str] = None,
    ) -> None:
        self.errors.append(ValidationError(path, message, value, expected))
        self.valid = False

    def merge(self, other: 'ValidationResult') -> None:
        """合并另一个验证结果"""
        self.errors.extend(other.errors)
        if not other.valid:
            self.valid = False


class Validator:
    """验证器基类"""

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        raise NotImplementedError


class TypeValidator(Validator):
    """类型验证器"""

    def __init__(self, expected_type: Type):
        self.expected_type = expected_type

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is not None and not isinstance(value, self.expected_type):
            result.add_error(
                path,
                f"Expected {self.expected_type.__name__}, got {type(value).__name__}",
                value,
                self.expected_type.__name__,
            )

        return result


class RequiredValidator(Validator):
    """必填验证器"""

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is None:
            result.add_error(path, "Required field is missing")

        return result


class RangeValidator(Validator):
    """范围验证器"""

    def __init__(
        self,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ):
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is None:
            return result

        try:
            num_value = float(value)

            if self.min_value is not None and num_value < self.min_value:
                result.add_error(
                    path,
                    f"Value {num_value} is less than minimum {self.min_value}",
                    value,
                )

            if self.max_value is not None and num_value > self.max_value:
                result.add_error(
                    path,
                    f"Value {num_value} is greater than maximum {self.max_value}",
                    value,
                )
        except (TypeError, ValueError):
            result.add_error(path, f"Value {value!r} is not a number", value)

        return result


class PatternValidator(Validator):
    """正则表达式验证器"""

    def __init__(self, pattern: str, message: str = ""):
        self.pattern = re.compile(pattern)
        self.message = message or f"Value does not match pattern {pattern}"

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is None:
            return result

        if not self.pattern.match(str(value)):
            result.add_error(path, self.message, value)

        return result


class EnumValidator(Validator):
    """枚举验证器"""

    def __init__(self, allowed_values: List[Any]):
        self.allowed_values = allowed_values

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is not None and value not in self.allowed_values:
            result.add_error(
                path,
                f"Value {value!r} not in allowed values: {self.allowed_values}",
                value,
            )

        return result


class LengthValidator(Validator):
    """长度验证器"""

    def __init__(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ):
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        if value is None:
            return result

        try:
            length = len(value)

            if self.min_length is not None and length < self.min_length:
                result.add_error(
                    path,
                    f"Length {length} is less than minimum {self.min_length}",
                    value,
                )

            if self.max_length is not None and length > self.max_length:
                result.add_error(
                    path,
                    f"Length {length} is greater than maximum {self.max_length}",
                    value,
                )
        except TypeError:
            result.add_error(path, f"Value {value!r} has no length", value)

        return result


class CompositeValidator(Validator):
    """组合验证器"""

    def __init__(self, validators: List[Validator]):
        self.validators = validators

    def validate(self, value: Any, path: str = "") -> ValidationResult:
        result = ValidationResult(valid=True)

        for validator in self.validators:
            sub_result = validator.validate(value, path)
            result.merge(sub_result)

        return result


@dataclass
class FieldSchema:
    """字段 Schema"""
    type: Optional[Type] = None
    required: bool = False
    default: Any = None
    validators: List[Validator] = field(default_factory=list)
    description: str = ""


class ConfigValidator:
    """配置验证器

    Example:
        schema = {
            "database.host": FieldSchema(type=str, required=True),
            "database.port": FieldSchema(type=int, validators=[RangeValidator(1, 65535)]),
            "log_level": FieldSchema(validators=[EnumValidator(["DEBUG", "INFO", "WARNING"])]),
        }

        validator = ConfigValidator(schema)
        result = validator.validate(config_dict)

        if not result.valid:
            for error in result.errors:
                print(f"{error.path}: {error.message}")
    """

    def __init__(self, schema: Dict[str, FieldSchema]):
        self.schema = schema

    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult(valid=True)

        for path, field_schema in self.schema.items():
            value = self._get_nested(config, path)

            # 必填检查
            if field_schema.required and value is None:
                result.add_error(path, "Required field is missing")
                continue

            # 类型检查
            if field_schema.type and value is not None:
                type_result = TypeValidator(field_schema.type).validate(value, path)
                result.merge(type_result)

            # 自定义验证器
            for validator in field_schema.validators:
                sub_result = validator.validate(value, path)
                result.merge(sub_result)

        return result

    def _get_nested(self, config: Dict, path: str) -> Any:
        """获取嵌套值"""
        if '.' not in path:
            return config.get(path)

        parts = path.split('.')
        current = config

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

            if current is None:
                return None

        return current


def validate_config(
    config: Dict[str, Any],
    schema: Dict[str, FieldSchema],
) -> ValidationResult:
    """便捷验证函数"""
    validator = ConfigValidator(schema)
    return validator.validate(config)
