"""
配置加载器 (Config Loader)
=========================

提供统一的配置加载接口，支持：
- YAML配置文件加载
- 环境变量覆盖
- 默认值回退
- 配置验证

作者: AStock Analysis System
日期: 2025-12-06
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class EngineConfig:
    """引擎通用配置基类"""
    name: str = "default"
    enabled: bool = True
    log_level: str = "INFO"
    cache_enabled: bool = True
    timeout_seconds: int = 300
    extra: Dict[str, Any] = field(default_factory=dict)


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    加载YAML配置文件

    Args:
        config_path: 配置文件路径

    Returns:
        配置字典
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def get_config_with_env_override(
    config: Dict[str, Any],
    env_prefix: str = "ASTOCK_"
) -> Dict[str, Any]:
    """
    获取配置，支持环境变量覆盖

    环境变量格式: {env_prefix}{KEY}
    例如: ASTOCK_LOG_LEVEL=DEBUG

    Args:
        config: 原始配置字典
        env_prefix: 环境变量前缀

    Returns:
        合并后的配置
    """
    result = config.copy()

    for key, value in config.items():
        env_key = f"{env_prefix}{key.upper()}"
        env_value = os.environ.get(env_key)

        if env_value is not None:
            # 尝试类型转换
            if isinstance(value, bool):
                result[key] = env_value.lower() in ('true', '1', 'yes')
            elif isinstance(value, int):
                result[key] = int(env_value)
            elif isinstance(value, float):
                result[key] = float(env_value)
            else:
                result[key] = env_value

    return result


def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """
    合并多个配置，后面的覆盖前面的

    Args:
        *configs: 配置字典列表

    Returns:
        合并后的配置
    """
    result: Dict[str, Any] = {}

    for config in configs:
        for key, value in config.items():
            if isinstance(value, dict) and key in result and isinstance(result[key], dict):
                result[key] = merge_configs(result[key], value)
            else:
                result[key] = value

    return result


class ConfigLoader:
    """
    配置加载器

    支持多层配置覆盖:
    1. 默认配置
    2. YAML文件配置
    3. 环境变量配置
    """

    def __init__(
        self,
        default_config: Optional[Dict[str, Any]] = None,
        config_file: Optional[str] = None,
        env_prefix: str = "ASTOCK_"
    ):
        self.default_config = default_config or {}
        self.config_file = config_file
        self.env_prefix = env_prefix
        self._config: Optional[Dict[str, Any]] = None

    def load(self) -> Dict[str, Any]:
        """加载并合并所有配置"""
        if self._config is not None:
            return self._config

        configs = [self.default_config]

        # 加载YAML配置
        if self.config_file:
            try:
                yaml_config = load_yaml_config(self.config_file)
                configs.append(yaml_config)
            except FileNotFoundError:
                pass  # 配置文件可选

        # 合并配置
        merged = merge_configs(*configs)

        # 应用环境变量覆盖
        self._config = get_config_with_env_override(merged, self.env_prefix)

        return self._config

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        config = self.load()
        return config.get(key, default)

    def reload(self) -> Dict[str, Any]:
        """重新加载配置"""
        self._config = None
        return self.load()


# 全局默认配置
DEFAULT_ENGINE_CONFIG: Dict[str, Any] = {
    "log_level": "INFO",
    "cache_enabled": True,
    "timeout_seconds": 300,
    "batch_size": 1000,
    "parallel_workers": 4,
}


def get_engine_config(
    engine_name: str,
    config_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    获取引擎配置的便捷函数

    Args:
        engine_name: 引擎名称
        config_file: 可选的配置文件路径

    Returns:
        引擎配置字典
    """
    loader = ConfigLoader(
        default_config=DEFAULT_ENGINE_CONFIG,
        config_file=config_file,
        env_prefix=f"ASTOCK_{engine_name.upper()}_"
    )
    return loader.load()
