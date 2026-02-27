"""
类型化设置 (Typed Settings)
============================

参考设计:
- pydantic-settings: 类型安全设置
- pydantic: 验证模型

提供类型安全的配置类。
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Optional, TypeVar, get_type_hints
import os

T = TypeVar('T')


class SecretStr:
    """敏感字符串

    在打印时隐藏真实值。
    """

    def __init__(self, value: str):
        self._value = value

    def get_secret_value(self) -> str:
        """获取真实值"""
        return self._value

    def __str__(self) -> str:
        return "***SECRET***"

    def __repr__(self) -> str:
        return "SecretStr('***')"

    def __eq__(self, other) -> bool:
        if isinstance(other, SecretStr):
            return self._value == other._value
        return False

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass
class FieldInfo:
    """字段信息"""
    default: Any = None
    default_factory: Optional[Callable[[], Any]] = None
    env: Optional[str] = None
    description: str = ""
    required: bool = False
    secret: bool = False
    validator: Optional[Callable[[Any], Any]] = None


def Field(
    default: Any = None,
    *,
    default_factory: Optional[Callable[[], Any]] = None,
    env: Optional[str] = None,
    description: str = "",
    required: bool = False,
    secret: bool = False,
    validator: Optional[Callable[[Any], Any]] = None,
) -> Any:
    """定义配置字段

    Example:
        class AppSettings(Settings):
            debug: bool = Field(default=False, env="DEBUG")
            api_key: str = Field(env="API_KEY", required=True, secret=True)
    """
    return FieldInfo(
        default=default,
        default_factory=default_factory,
        env=env,
        description=description,
        required=required,
        secret=secret,
        validator=validator,
    )


class SettingsMeta(type):
    """Settings 元类

    处理字段定义和类型提示。
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict):
        # 收集字段信息
        field_infos: Dict[str, FieldInfo] = {}

        # 从基类继承
        for base in bases:
            if hasattr(base, '_field_infos'):
                field_infos.update(base._field_infos)

        # 处理当前类的注解
        annotations = namespace.get('__annotations__', {})

        for field_name, field_type in annotations.items():
            if field_name.startswith('_'):
                continue

            default_value = namespace.get(field_name)

            if isinstance(default_value, FieldInfo):
                field_infos[field_name] = default_value
            else:
                # 普通默认值
                field_infos[field_name] = FieldInfo(default=default_value)

        namespace['_field_infos'] = field_infos

        return super().__new__(mcs, name, bases, namespace)


class BaseSettings(metaclass=SettingsMeta):
    """设置基类

    提供类型化的配置访问。

    Example:
        class DatabaseSettings(BaseSettings):
            host: str = "localhost"
            port: int = 5432
            password: str = Field(env="DB_PASSWORD", secret=True)

        settings = DatabaseSettings()
    """

    _field_infos: ClassVar[Dict[str, FieldInfo]] = {}

    def __init__(
        self,
        env_prefix: str = "",
        _config: Optional[Dict[str, Any]] = None,
        **overrides
    ):
        self._env_prefix = env_prefix
        self._values: Dict[str, Any] = {}

        # 获取类型提示
        hints = get_type_hints(self.__class__)

        # 处理每个字段
        for name, info in self._field_infos.items():
            field_type = hints.get(name, str)
            value = self._resolve_value(name, info, field_type, _config, overrides)

            # 验证
            if info.validator:
                value = info.validator(value)

            # 类型转换
            value = self._convert_type(value, field_type, info.secret)

            # 必填检查
            if info.required and value is None:
                raise ValueError(f"Required setting '{name}' is not set")

            self._values[name] = value
            setattr(self, name, value)

    def _resolve_value(
        self,
        name: str,
        info: FieldInfo,
        field_type: type,
        config: Optional[Dict[str, Any]],
        overrides: Dict[str, Any],
    ) -> Any:
        """解析配置值

        优先级：overrides > env > config > default
        """
        # 1. 覆盖值
        if name in overrides:
            return overrides[name]

        # 2. 环境变量
        env_name = info.env or f"{self._env_prefix}{name}".upper()
        env_value = os.environ.get(env_name)
        if env_value is not None:
            return env_value

        # 3. 配置字典
        if config and name in config:
            return config[name]

        # 4. 默认值
        if info.default_factory:
            return info.default_factory()

        return info.default

    def _convert_type(
        self,
        value: Any,
        target_type: type,
        is_secret: bool,
    ) -> Any:
        """类型转换"""
        if value is None:
            return None

        # 处理 Optional
        origin = getattr(target_type, '__origin__', None)
        if origin is type(None):
            return None

        # 已经是目标类型
        if isinstance(value, target_type):
            if is_secret and isinstance(value, str):
                return SecretStr(value)
            return value

        # 字符串转换
        if isinstance(value, str):
            if target_type == bool:
                return value.lower() in ('true', '1', 'yes', 'on')
            elif target_type == int:
                return int(value)
            elif target_type == float:
                return float(value)
            elif target_type == list:
                return [s.strip() for s in value.split(',')]
            elif is_secret or target_type == SecretStr:
                return SecretStr(value)

        return value

    def to_dict(self, show_secrets: bool = False) -> Dict[str, Any]:
        """转为字典"""
        result = {}
        for name, value in self._values.items():
            if isinstance(value, SecretStr):
                result[name] = value.get_secret_value() if show_secrets else "***"
            else:
                result[name] = value
        return result

    def __repr__(self) -> str:
        parts = []
        for name in self._field_infos:
            value = getattr(self, name, None)
            parts.append(f"{name}={value!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


# 别名
Settings = BaseSettings


# 常用设置类
class DatabaseSettings(BaseSettings):
    """数据库设置"""
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = Field(env="DB_PASSWORD", secret=True)

    @property
    def url(self) -> str:
        """构建连接 URL"""
        if self.password:
            pwd = self.password.get_secret_value() if isinstance(self.password, SecretStr) else self.password
            return f"postgresql://{self.username}:{pwd}@{self.host}:{self.port}/{self.database}"
        return f"postgresql://{self.username}@{self.host}:{self.port}/{self.database}"


class AppSettings(BaseSettings):
    """应用设置"""
    debug: bool = Field(default=False, env="DEBUG")
    environment: str = Field(default="development", env="ENVIRONMENT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    secret_key: str = Field(env="SECRET_KEY", secret=True)
