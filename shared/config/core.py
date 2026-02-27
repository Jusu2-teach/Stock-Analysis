"""
配置核心 (Config Core)
=======================

参考设计:
- dynaconf: 层叠配置 + 懒加载
- OmegaConf: 结构化访问
- ConfigParser: 标准接口

提供统一的配置访问接口。
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, TypeVar, Union
import copy
import threading

T = TypeVar('T')


class ConfigValue(Generic[T]):
    """配置值包装器

    支持类型转换、默认值、验证。
    """

    def __init__(
        self,
        value: Any,
        path: str = "",
        default: Optional[T] = None,
    ):
        self._raw_value = value
        self._path = path
        self._default = default

    @property
    def raw(self) -> Any:
        """原始值"""
        return self._raw_value

    def get(self, default: T = None) -> T:
        """获取值，支持默认值"""
        if self._raw_value is None:
            return default if default is not None else self._default
        return self._raw_value

    def as_str(self, default: str = "") -> str:
        """转为字符串"""
        if self._raw_value is None:
            return default
        return str(self._raw_value)

    def as_int(self, default: int = 0) -> int:
        """转为整数"""
        if self._raw_value is None:
            return default
        try:
            return int(self._raw_value)
        except (ValueError, TypeError):
            return default

    def as_float(self, default: float = 0.0) -> float:
        """转为浮点数"""
        if self._raw_value is None:
            return default
        try:
            return float(self._raw_value)
        except (ValueError, TypeError):
            return default

    def as_bool(self, default: bool = False) -> bool:
        """转为布尔值"""
        if self._raw_value is None:
            return default
        if isinstance(self._raw_value, bool):
            return self._raw_value
        if isinstance(self._raw_value, str):
            return self._raw_value.lower() in ('true', '1', 'yes', 'on')
        return bool(self._raw_value)

    def as_list(self, default: list = None) -> list:
        """转为列表"""
        if self._raw_value is None:
            return default or []
        if isinstance(self._raw_value, list):
            return self._raw_value
        if isinstance(self._raw_value, str):
            return [s.strip() for s in self._raw_value.split(',')]
        return [self._raw_value]

    def as_dict(self, default: dict = None) -> dict:
        """转为字典"""
        if self._raw_value is None:
            return default or {}
        if isinstance(self._raw_value, dict):
            return self._raw_value
        return default or {}

    def as_path(self, default: Optional[Path] = None) -> Path:
        """转为路径"""
        if self._raw_value is None:
            return default or Path(".")
        return Path(self._raw_value)

    def __bool__(self) -> bool:
        return self._raw_value is not None

    def __str__(self) -> str:
        return str(self._raw_value)

    def __repr__(self) -> str:
        return f"ConfigValue({self._raw_value!r}, path={self._path!r})"


class Config:
    """配置容器

    提供层叠的配置访问，支持点号路径访问。

    Example:
        config = Config({
            "database": {
                "host": "localhost",
                "port": 5432
            }
        })

        # 点号访问
        host = config.get("database.host")
        port = config["database.port"]

        # 链式访问
        db = config.database
        host = db.host.as_str()

        # 带默认值
        timeout = config.get("database.timeout", default=30)
    """

    _default: Optional['Config'] = None
    _lock = threading.Lock()

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        path: str = "",
        parent: Optional['Config'] = None,
    ):
        self._data = data or {}
        self._path = path
        self._parent = parent
        self._cache: Dict[str, Any] = {}

    @classmethod
    def load(
        cls,
        path: Union[str, Path],
        env: Optional[str] = None,
    ) -> 'Config':
        """从文件加载配置

        Args:
            path: 配置文件路径
            env: 环境名称（用于加载环境特定配置）
        """
        from .loaders import ChainLoader

        loader = ChainLoader()
        data = loader.load(path, env=env)

        return cls(data)

    @classmethod
    def from_env(cls, prefix: str = "ASTOCK_") -> 'Config':
        """从环境变量加载"""
        from .loaders import EnvLoader

        loader = EnvLoader(prefix=prefix)
        data = loader.load()

        return cls(data)

    @classmethod
    def get_default(cls) -> 'Config':
        """获取默认配置实例"""
        if cls._default is None:
            with cls._lock:
                if cls._default is None:
                    cls._default = cls()
        return cls._default

    @classmethod
    def set_default(cls, config: 'Config') -> None:
        """设置默认配置"""
        with cls._lock:
            cls._default = config

    def get(
        self,
        key: str,
        default: T = None,
        cast: Optional[Callable[[Any], T]] = None,
    ) -> T:
        """获取配置值

        Args:
            key: 配置键（支持点号路径）
            default: 默认值
            cast: 类型转换函数
        """
        value = self._get_nested(key)

        if value is None:
            return default

        if cast:
            try:
                return cast(value)
            except (ValueError, TypeError):
                return default

        return value

    def _get_nested(self, key: str) -> Any:
        """获取嵌套值"""
        if '.' not in key:
            return self._data.get(key)

        parts = key.split('.')
        current = self._data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

            if current is None:
                return None

        return current

    def __getitem__(self, key: str) -> Any:
        """字典式访问"""
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __getattr__(self, name: str) -> Union['Config', ConfigValue]:
        """属性式访问"""
        if name.startswith('_'):
            raise AttributeError(name)

        value = self._data.get(name)
        new_path = f"{self._path}.{name}" if self._path else name

        if isinstance(value, dict):
            return Config(value, path=new_path, parent=self)

        return ConfigValue(value, path=new_path)

    def __contains__(self, key: str) -> bool:
        """检查键是否存在"""
        return self.get(key) is not None

    def __iter__(self) -> Iterator[str]:
        """迭代键"""
        return iter(self._data.keys())

    def keys(self) -> List[str]:
        """所有键"""
        return list(self._data.keys())

    def values(self) -> List[Any]:
        """所有值"""
        return list(self._data.values())

    def items(self) -> List[tuple]:
        """所有键值对"""
        return list(self._data.items())

    def to_dict(self) -> Dict[str, Any]:
        """转为字典"""
        return copy.deepcopy(self._data)

    def merge(self, other: Union['Config', Dict[str, Any]]) -> 'Config':
        """合并配置（返回新实例）"""
        if isinstance(other, Config):
            other_data = other._data
        else:
            other_data = other

        merged = _deep_merge(self._data, other_data)
        return Config(merged, path=self._path)

    def update(self, other: Union['Config', Dict[str, Any]]) -> None:
        """就地更新配置"""
        if isinstance(other, Config):
            other_data = other._data
        else:
            other_data = other

        self._data = _deep_merge(self._data, other_data)

    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        if '.' not in key:
            self._data[key] = value
            return

        parts = key.split('.')
        current = self._data

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def __repr__(self) -> str:
        return f"Config({self._data!r})"


def get_config() -> Config:
    """获取默认配置"""
    return Config.get_default()


def set_config(config: Config) -> None:
    """设置默认配置"""
    Config.set_default(config)


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """深度合并字典"""
    result = copy.deepcopy(base)

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)

    return result
