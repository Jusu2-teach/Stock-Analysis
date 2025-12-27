"""
PGCS Utils: Compatibility
=========================

兼容性检查工具。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from ..core.schema import Schema


class ChangeType(Enum):
    """变更类型"""
    ADDED = 'added'           # 新增
    REMOVED = 'removed'       # 删除
    MODIFIED = 'modified'     # 修改
    UNCHANGED = 'unchanged'   # 无变化


@dataclass
class FieldChange:
    """字段变更记录"""
    field_name: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    message: str = ''


@dataclass
class CompatibilityReport:
    """兼容性报告"""
    is_compatible: bool
    changes: List[FieldChange]
    breaking_changes: List[FieldChange]
    warnings: List[str]

    @property
    def has_breaking_changes(self) -> bool:
        return len(self.breaking_changes) > 0

    def summary(self) -> str:
        """生成摘要"""
        lines = [
            f"Compatible: {self.is_compatible}",
            f"Total changes: {len(self.changes)}",
            f"Breaking changes: {len(self.breaking_changes)}",
        ]

        if self.breaking_changes:
            lines.append("\nBreaking changes:")
            for bc in self.breaking_changes:
                lines.append(f"  - {bc.field_name}: {bc.message}")

        if self.warnings:
            lines.append("\nWarnings:")
            for w in self.warnings:
                lines.append(f"  - {w}")

        return '\n'.join(lines)


def ensure_compatibility(
    old_schema: Type['Schema'],
    new_schema: Type['Schema'],
    backward: bool = True,
    forward: bool = False,
) -> CompatibilityReport:
    """
    检查两个 Schema 的兼容性

    Args:
        old_schema: 旧 Schema
        new_schema: 新 Schema
        backward: 是否检查向后兼容 (新读旧)
        forward: 是否检查向前兼容 (旧读新)

    Returns:
        CompatibilityReport
    """
    old_fields = set(old_schema.field_names())
    new_fields = set(new_schema.field_names())

    changes: List[FieldChange] = []
    breaking: List[FieldChange] = []
    warnings: List[str] = []

    # 检查删除的字段
    removed = old_fields - new_fields
    for name in removed:
        change = FieldChange(
            field_name=name,
            change_type=ChangeType.REMOVED,
            message=f"Field '{name}' was removed",
        )
        changes.append(change)

        if backward:
            breaking.append(change)

    # 检查新增的字段
    added = new_fields - old_fields
    for name in added:
        field = new_schema.get_field(name)
        change = FieldChange(
            field_name=name,
            change_type=ChangeType.ADDED,
            message=f"Field '{name}' was added",
        )
        changes.append(change)

        if forward and field and not field.descriptor.has_default:
            change.message += " without default value"
            breaking.append(change)

    # 检查修改的字段
    common = old_fields & new_fields
    for name in common:
        old_field = old_schema.get_field(name)
        new_field = new_schema.get_field(name)

        if old_field and new_field:
            old_desc = old_field.descriptor
            new_desc = new_field.descriptor

            # 类型变化
            if old_desc.type_info != new_desc.type_info:
                change = FieldChange(
                    field_name=name,
                    change_type=ChangeType.MODIFIED,
                    old_value=str(old_desc.type_info),
                    new_value=str(new_desc.type_info),
                    message=f"Field '{name}' type changed",
                )
                changes.append(change)
                breaking.append(change)

            # 验证器变化
            if len(old_desc.validators) != len(new_desc.validators):
                warnings.append(
                    f"Field '{name}' validators changed: "
                    f"{len(old_desc.validators)} -> {len(new_desc.validators)}"
                )

    is_compatible = len(breaking) == 0

    return CompatibilityReport(
        is_compatible=is_compatible,
        changes=changes,
        breaking_changes=breaking,
        warnings=warnings,
    )


def compare_schemas(
    schema_a: Type['Schema'],
    schema_b: Type['Schema'],
) -> Dict[str, Any]:
    """
    比较两个 Schema

    Returns:
        比较结果字典
    """
    fields_a = set(schema_a.field_names())
    fields_b = set(schema_b.field_names())

    return {
        'only_in_a': list(fields_a - fields_b),
        'only_in_b': list(fields_b - fields_a),
        'in_both': list(fields_a & fields_b),
        'fingerprint_a': schema_a.fingerprint(),
        'fingerprint_b': schema_b.fingerprint(),
        'same_fingerprint': schema_a.fingerprint() == schema_b.fingerprint(),
    }


__all__ = [
    'ensure_compatibility',
    'compare_schemas',
    'CompatibilityReport',
    'FieldChange',
    'ChangeType',
]
