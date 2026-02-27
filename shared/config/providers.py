"""
配置提供者 (Config Providers)
==============================

参考设计:
- dynaconf: 多数据源
- Spring Cloud Config: 远程配置

提供可扩展的配置数据源。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Union
import os


class ConfigProvider(ABC):
    """配置提供者基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """提供者名称"""

    @property
    def priority(self) -> int:
        """优先级（越高越优先）"""
        return 0

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""

    @abstractmethod
    def has(self, key: str) -> bool:
        """检查键是否存在"""

    def get_all(self) -> Dict[str, Any]:
        """获取所有配置"""
        return {}


class DefaultProvider(ConfigProvider):
    """默认值提供者"""

    def __init__(self, defaults: Dict[str, Any]):
        self._defaults = defaults

    @property
    def name(self) -> str:
        return "defaults"

    @property
    def priority(self) -> int:
        return 0  # 最低优先级

    def get(self, key: str, default: Any = None) -> Any:
        return self._get_nested(key) or default

    def has(self, key: str) -> bool:
        return self._get_nested(key) is not None

    def get_all(self) -> Dict[str, Any]:
        return self._defaults.copy()

    def _get_nested(self, key: str) -> Any:
        if '.' not in key:
            return self._defaults.get(key)

        parts = key.split('.')
        current = self._defaults

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current


class FileProvider(ConfigProvider):
    """文件配置提供者"""

    def __init__(
        self,
        path: Union[str, Path],
        watch: bool = False,
    ):
        self._path = Path(path)
        self._watch = watch
        self._data: Dict[str, Any] = {}
        self._loaded = False

    @property
    def name(self) -> str:
        return f"file:{self._path.name}"

    @property
    def priority(self) -> int:
        return 10

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return

        if not self._path.exists():
            self._loaded = True
            return

        from .loaders import YAMLLoader, JSONLoader, TOMLLoader

        loaders = [YAMLLoader(), JSONLoader(), TOMLLoader()]

        for loader in loaders:
            if loader.can_load(self._path):
                self._data = loader.load(self._path)
                break

        self._loaded = True

    def get(self, key: str, default: Any = None) -> Any:
        self._ensure_loaded()
        return self._get_nested(key) or default

    def has(self, key: str) -> bool:
        self._ensure_loaded()
        return self._get_nested(key) is not None

    def get_all(self) -> Dict[str, Any]:
        self._ensure_loaded()
        return self._data.copy()

    def reload(self) -> None:
        """重新加载"""
        self._loaded = False
        self._data = {}
        self._ensure_loaded()

    def _get_nested(self, key: str) -> Any:
        if '.' not in key:
            return self._data.get(key)

        parts = key.split('.')
        current = self._data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current


class EnvironmentProvider(ConfigProvider):
    """环境变量提供者"""

    def __init__(
        self,
        prefix: str = "",
        separator: str = "_",
    ):
        self._prefix = prefix.upper()
        self._separator = separator

    @property
    def name(self) -> str:
        return f"env:{self._prefix or '*'}"

    @property
    def priority(self) -> int:
        return 20  # 环境变量优先级较高

    def get(self, key: str, default: Any = None) -> Any:
        env_key = self._to_env_key(key)
        return os.environ.get(env_key, default)

    def has(self, key: str) -> bool:
        env_key = self._to_env_key(key)
        return env_key in os.environ

    def get_all(self) -> Dict[str, Any]:
        result = {}

        for key, value in os.environ.items():
            if self._prefix and not key.startswith(self._prefix):
                continue

            config_key = self._to_config_key(key)
            result[config_key] = value

        return result

    def _to_env_key(self, key: str) -> str:
        """配置键 → 环境变量键"""
        env_key = key.replace('.', self._separator).upper()
        if self._prefix:
            return f"{self._prefix}{self._separator}{env_key}"
        return env_key

    def _to_config_key(self, env_key: str) -> str:
        """环境变量键 → 配置键"""
        if self._prefix:
            env_key = env_key[len(self._prefix) + 1:]
        return env_key.lower().replace(self._separator, '.')


class ChainProvider(ConfigProvider):
    """链式提供者

    按优先级组合多个提供者。

    Example:
        provider = ChainProvider([
            DefaultProvider(defaults),
            FileProvider("config.yaml"),
            EnvironmentProvider(prefix="MYAPP"),
        ])
    """

    def __init__(self, providers: List[ConfigProvider]):
        # 按优先级排序（高优先级在前）
        self._providers = sorted(providers, key=lambda p: p.priority, reverse=True)

    @property
    def name(self) -> str:
        return "chain"

    @property
    def priority(self) -> int:
        return max((p.priority for p in self._providers), default=0)

    def get(self, key: str, default: Any = None) -> Any:
        for provider in self._providers:
            if provider.has(key):
                return provider.get(key)
        return default

    def has(self, key: str) -> bool:
        return any(p.has(key) for p in self._providers)

    def get_all(self) -> Dict[str, Any]:
        """合并所有配置（低优先级先加载）"""
        result = {}

        for provider in reversed(self._providers):
            data = provider.get_all()
            result = _deep_merge(result, data)

        return result

    def add_provider(self, provider: ConfigProvider) -> None:
        """添加提供者"""
        self._providers.append(provider)
        self._providers.sort(key=lambda p: p.priority, reverse=True)


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """深度合并字典"""
    import copy
    result = copy.deepcopy(base)

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)

    return result
