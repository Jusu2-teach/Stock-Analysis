"""
PGCS Registry: Schema Registry
==============================

全局 Schema 注册中心。

设计原则:
- 单例模式确保全局唯一
- 支持版本管理
- 支持指纹变更检测
- 支持兼容性检查
- 完全通用，不包含业务逻辑
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Dict, List, Type, TypeVar, Callable
from enum import Enum

from ..core.schema import Schema, SchemaInfo


T = TypeVar('T', bound=Schema)


class CompatibilityMode(Enum):
    """兼容性检查模式"""
    NONE = 'none'             # 不检查
    BACKWARD = 'backward'     # 向后兼容: 新版本可读旧数据
    FORWARD = 'forward'       # 向前兼容: 旧版本可读新数据
    FULL = 'full'             # 完全兼容: 双向
    STRICT = 'strict'         # 严格模式: 必须完全匹配


@dataclass
class SchemaVersion:
    """Schema 版本记录"""
    version: str
    fingerprint: str
    registered_at: str
    is_latest: bool = True
    changelog: str = ''


@dataclass
class RegisteredSchema:
    """已注册的 Schema"""
    name: str
    schema_cls: Type[Schema]
    info: SchemaInfo
    versions: List[SchemaVersion] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def latest_version(self) -> Optional[SchemaVersion]:
        for v in self.versions:
            if v.is_latest:
                return v
        return self.versions[-1] if self.versions else None

    @property
    def fingerprint(self) -> str:
        return self.schema_cls.fingerprint()


class SchemaRegistry:
    """
    PGCS Schema 注册中心

    全局管理所有注册的 Schema，提供:
    - Schema 注册与发现
    - 版本管理
    - 兼容性检查
    - 变更通知

    Example:
        registry = SchemaRegistry.instance()

        # 注册 Schema
        registry.register(UserSchema)

        # 发现 Schema
        schema = registry.get('user')

        # 检查兼容性
        is_compat, issues = registry.check_compatibility(NewUserSchema)
    """

    _instance: Optional['SchemaRegistry'] = None
    _lock = threading.Lock()

    def __init__(self):
        self._schemas: Dict[str, RegisteredSchema] = {}
        self._by_tag: Dict[str, List[str]] = {}
        self._compatibility_mode = CompatibilityMode.BACKWARD
        self._observers: List[Callable[[str, RegisteredSchema], None]] = []

    @classmethod
    def instance(cls) -> 'SchemaRegistry':
        """获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """重置单例 (仅测试用)"""
        cls._instance = None

    def register(
        self,
        schema_cls: Type[T],
        *,
        name: Optional[str] = None,
        tags: Optional[List[str]] = None,
        changelog: str = '',
        check_compatibility: bool = True,
    ) -> RegisteredSchema:
        """
        注册 Schema

        Args:
            schema_cls: Schema 类
            name: 注册名 (默认使用 schema info 的 name)
            tags: 标签列表
            changelog: 变更日志
            check_compatibility: 是否检查兼容性

        Returns:
            RegisteredSchema
        """
        info = schema_cls.__schema_info__
        reg_name = name or info.name or schema_cls.__name__

        # 兼容性检查
        if check_compatibility and reg_name in self._schemas:
            is_compat, issues = self.check_compatibility(schema_cls)
            if not is_compat:
                raise ValueError(
                    f"Schema '{reg_name}' is not compatible: {issues}"
                )

        # 创建版本
        version = SchemaVersion(
            version=info.version,
            fingerprint=schema_cls.fingerprint(),
            registered_at=datetime.now().isoformat(),
            changelog=changelog,
        )

        # 更新或创建注册
        if reg_name in self._schemas:
            existing = self._schemas[reg_name]
            # 检查指纹是否变化
            if existing.fingerprint == version.fingerprint:
                return existing  # 无变化

            # 标记旧版本
            for v in existing.versions:
                v.is_latest = False
            existing.versions.append(version)
            existing.schema_cls = schema_cls
            existing.info = info
            registered = existing
        else:
            registered = RegisteredSchema(
                name=reg_name,
                schema_cls=schema_cls,
                info=info,
                versions=[version],
                tags=tags or [],
            )
            self._schemas[reg_name] = registered

        # 更新标签索引
        for tag in registered.tags:
            if tag not in self._by_tag:
                self._by_tag[tag] = []
            if reg_name not in self._by_tag[tag]:
                self._by_tag[tag].append(reg_name)

        # 通知观察者
        for observer in self._observers:
            observer(reg_name, registered)

        return registered

    def get(self, name: str) -> Optional[RegisteredSchema]:
        """按名称获取"""
        return self._schemas.get(name)

    def get_schema_class(self, name: str) -> Optional[Type[Schema]]:
        """获取 Schema 类"""
        reg = self._schemas.get(name)
        return reg.schema_cls if reg else None

    def get_by_tag(self, tag: str) -> List[RegisteredSchema]:
        """按标签获取"""
        names = self._by_tag.get(tag, [])
        return [self._schemas[n] for n in names if n in self._schemas]

    def list_all(self) -> List[RegisteredSchema]:
        """列出所有"""
        return list(self._schemas.values())

    def list_names(self) -> List[str]:
        """列出所有名称"""
        return list(self._schemas.keys())

    def unregister(self, name: str):
        """注销 Schema"""
        if name in self._schemas:
            del self._schemas[name]
            # 清理标签索引
            for tag_names in self._by_tag.values():
                if name in tag_names:
                    tag_names.remove(name)

    def check_compatibility(
        self,
        schema_cls: Type[Schema],
        mode: Optional[CompatibilityMode] = None,
    ) -> tuple[bool, List[str]]:
        """
        检查兼容性

        Args:
            schema_cls: 新 Schema 类
            mode: 兼容性模式

        Returns:
            (is_compatible, issues)
        """
        mode = mode or self._compatibility_mode
        name = schema_cls.__schema_info__.name or schema_cls.__name__

        existing = self._schemas.get(name)
        if not existing:
            return True, []  # 新 Schema，总是兼容

        if mode == CompatibilityMode.NONE:
            return True, []

        issues = []
        old_fields = set(existing.schema_cls.field_names())
        new_fields = set(schema_cls.field_names())

        if mode in (CompatibilityMode.BACKWARD, CompatibilityMode.FULL, CompatibilityMode.STRICT):
            # 向后兼容: 不能删除字段
            removed = old_fields - new_fields
            if removed:
                issues.append(f"Removed fields: {removed}")

        if mode in (CompatibilityMode.FORWARD, CompatibilityMode.FULL, CompatibilityMode.STRICT):
            # 向前兼容: 新字段必须有默认值
            added = new_fields - old_fields
            for field_name in added:
                field = schema_cls.get_field(field_name)
                if field and not field.descriptor.has_default:
                    issues.append(f"New required field without default: {field_name}")

        if mode == CompatibilityMode.STRICT:
            # 严格模式: 指纹必须匹配
            if existing.fingerprint != schema_cls.fingerprint():
                issues.append("Schema fingerprint mismatch")

        return len(issues) == 0, issues

    def set_compatibility_mode(self, mode: CompatibilityMode):
        """设置默认兼容性模式"""
        self._compatibility_mode = mode

    def add_observer(self, callback: Callable[[str, RegisteredSchema], None]):
        """添加注册观察者"""
        self._observers.append(callback)

    def remove_observer(self, callback: Callable):
        """移除观察者"""
        if callback in self._observers:
            self._observers.remove(callback)


def get_registry() -> SchemaRegistry:
    """获取全局注册表"""
    return SchemaRegistry.instance()


def register(
    schema_cls: Type[T] = None,
    *,
    name: str = None,
    tags: List[str] = None,
) -> Type[T]:
    """
    注册装饰器

    Example:
        @register(tags=['user'])
        class UserSchema(Schema):
            ...
    """
    def decorator(cls: Type[T]) -> Type[T]:
        get_registry().register(cls, name=name, tags=tags)
        return cls

    if schema_cls is not None:
        return decorator(schema_cls)
    return decorator


__all__ = [
    'SchemaRegistry',
    'RegisteredSchema',
    'SchemaVersion',
    'CompatibilityMode',
    'get_registry',
    'register',
]
