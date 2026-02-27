"""
配置加载器 (Config Loaders)
============================

参考设计:
- dynaconf: 多格式加载
- python-dotenv: .env 文件
- configparser: INI 格式

支持多种配置文件格式。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import os


class ConfigLoader(ABC):
    """配置加载器基类"""

    @abstractmethod
    def load(self, source: Any, **kwargs) -> Dict[str, Any]:
        """加载配置"""

    @abstractmethod
    def can_load(self, source: Any) -> bool:
        """检查是否可以加载此源"""


class YAMLLoader(ConfigLoader):
    """YAML 配置加载器"""

    def load(self, source: Union[str, Path], **kwargs) -> Dict[str, Any]:
        path = Path(source)

        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required for YAML config files")

        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        return data or {}

    def can_load(self, source: Any) -> bool:
        if isinstance(source, (str, Path)):
            path = Path(source)
            return path.suffix.lower() in ('.yaml', '.yml')
        return False


class JSONLoader(ConfigLoader):
    """JSON 配置加载器"""

    def load(self, source: Union[str, Path], **kwargs) -> Dict[str, Any]:
        import json

        path = Path(source)

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return data or {}

    def can_load(self, source: Any) -> bool:
        if isinstance(source, (str, Path)):
            path = Path(source)
            return path.suffix.lower() == '.json'
        return False


class TOMLLoader(ConfigLoader):
    """TOML 配置加载器"""

    def load(self, source: Union[str, Path], **kwargs) -> Dict[str, Any]:
        path = Path(source)

        try:
            import tomllib  # Python 3.11+
        except ImportError:
            try:
                import tomli as tomllib
            except ImportError:
                raise ImportError("tomli is required for TOML config files (Python < 3.11)")

        with open(path, 'rb') as f:
            data = tomllib.load(f)

        return data or {}

    def can_load(self, source: Any) -> bool:
        if isinstance(source, (str, Path)):
            path = Path(source)
            return path.suffix.lower() == '.toml'
        return False


class EnvLoader(ConfigLoader):
    """环境变量加载器

    加载带有指定前缀的环境变量。

    Example:
        # 环境变量: MYAPP_DATABASE_HOST=localhost
        loader = EnvLoader(prefix="MYAPP_")
        config = loader.load()
        # 结果: {"database": {"host": "localhost"}}
    """

    def __init__(
        self,
        prefix: str = "",
        separator: str = "_",
        nested: bool = True,
    ):
        self.prefix = prefix.upper()
        self.separator = separator
        self.nested = nested

    def load(self, source: Any = None, **kwargs) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        for key, value in os.environ.items():
            if self.prefix and not key.upper().startswith(self.prefix):
                continue

            # 移除前缀
            key_name = key[len(self.prefix):] if self.prefix else key

            if self.nested and self.separator in key_name:
                # 嵌套键
                self._set_nested(result, key_name.lower().split(self.separator), value)
            else:
                result[key_name.lower()] = value

        return result

    def _set_nested(self, data: Dict, parts: List[str], value: Any) -> None:
        """设置嵌套值"""
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def can_load(self, source: Any) -> bool:
        return source is None or source == "env"


class DotEnvLoader(ConfigLoader):
    """.env 文件加载器

    参考 python-dotenv。
    """

    def __init__(self, override: bool = False):
        self.override = override

    def load(self, source: Union[str, Path] = ".env", **kwargs) -> Dict[str, Any]:
        path = Path(source)
        result: Dict[str, Any] = {}

        if not path.exists():
            return result

        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()

                # 跳过空行和注释
                if not line or line.startswith('#'):
                    continue

                # 解析键值对
                if '=' not in line:
                    continue

                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # 移除引号
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]

                result[key] = value

                # 设置环境变量
                if self.override or key not in os.environ:
                    os.environ[key] = value

        return result

    def can_load(self, source: Any) -> bool:
        if isinstance(source, (str, Path)):
            path = Path(source)
            return path.name.startswith('.env') or path.suffix == '.env'
        return False


class ChainLoader(ConfigLoader):
    """链式加载器

    组合多个加载器，按优先级加载和合并配置。

    Example:
        loader = ChainLoader([
            DefaultLoader({"debug": False}),
            YAMLLoader(),
            EnvLoader(prefix="MYAPP_"),
        ])
        config = loader.load("config.yaml")
    """

    def __init__(self, loaders: Optional[List[ConfigLoader]] = None):
        self.loaders = loaders or [
            YAMLLoader(),
            JSONLoader(),
            TOMLLoader(),
            EnvLoader(),
        ]

    def load(
        self,
        source: Union[str, Path] = None,
        env: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        if source:
            path = Path(source)

            # 加载基础配置
            for loader in self.loaders:
                if loader.can_load(source):
                    base_data = loader.load(source, **kwargs)
                    result = _deep_merge(result, base_data)
                    break

            # 加载环境特定配置
            if env:
                env_path = path.parent / f"{path.stem}.{env}{path.suffix}"
                if env_path.exists():
                    for loader in self.loaders:
                        if loader.can_load(env_path):
                            env_data = loader.load(env_path, **kwargs)
                            result = _deep_merge(result, env_data)
                            break

        # 加载环境变量
        env_loader = EnvLoader(prefix=kwargs.get('env_prefix', 'ASTOCK_'))
        env_data = env_loader.load()
        result = _deep_merge(result, env_data)

        return result

    def can_load(self, source: Any) -> bool:
        return any(loader.can_load(source) for loader in self.loaders)


class DefaultLoader(ConfigLoader):
    """默认值加载器"""

    def __init__(self, defaults: Dict[str, Any]):
        self.defaults = defaults

    def load(self, source: Any = None, **kwargs) -> Dict[str, Any]:
        return self.defaults.copy()

    def can_load(self, source: Any) -> bool:
        return True


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
