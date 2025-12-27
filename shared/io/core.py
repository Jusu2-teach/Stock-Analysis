"""
核心 I/O 组件 (Core I/O Components)
===================================

参考设计:
- kedro: DataCatalog 统一数据访问
- fsspec: 文件系统抽象

提供数据集抽象和目录管理。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union
import hashlib
import time


T = TypeVar("T")


@dataclass
class DataSetConfig:
    """数据集配置"""

    # 基本配置
    path: Optional[str] = None
    type: str = "memory"

    # 格式配置
    file_format: str = "csv"
    encoding: str = "utf-8"

    # 读取配置
    load_args: Dict[str, Any] = field(default_factory=dict)

    # 写入配置
    save_args: Dict[str, Any] = field(default_factory=dict)

    # 版本控制
    versioned: bool = False
    version: Optional[str] = None

    # 缓存配置
    cached: bool = False
    cache_ttl: Optional[int] = None

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSet(ABC, Generic[T]):
    """数据集抽象基类

    参考 kedro AbstractDataSet 设计。

    Example:
        class MyDataSet(DataSet[pd.DataFrame]):
            def _load(self) -> pd.DataFrame:
                return pd.read_csv(self.path)

            def _save(self, data: pd.DataFrame) -> None:
                data.to_csv(self.path, index=False)
    """

    def __init__(
        self,
        path: Optional[Union[str, Path]] = None,
        config: Optional[DataSetConfig] = None,
    ):
        self._path = Path(path) if path else None
        self._config = config or DataSetConfig(path=str(path) if path else None)
        self._cache: Optional[T] = None
        self._cache_time: Optional[float] = None

    @property
    def path(self) -> Optional[Path]:
        """数据集路径"""
        return self._path

    @property
    def config(self) -> DataSetConfig:
        """数据集配置"""
        return self._config

    @abstractmethod
    def _load(self) -> T:
        """加载数据（子类实现）"""
        pass

    @abstractmethod
    def _save(self, data: T) -> None:
        """保存数据（子类实现）"""
        pass

    def _describe(self) -> Dict[str, Any]:
        """描述数据集"""
        return {
            "type": type(self).__name__,
            "path": str(self._path) if self._path else None,
        }

    def load(self) -> T:
        """加载数据

        支持缓存和版本控制。
        """
        # 检查缓存
        if self._config.cached and self._cache is not None:
            if self._config.cache_ttl is None:
                return self._cache

            if time.time() - self._cache_time < self._config.cache_ttl:
                return self._cache

        # 加载数据
        data = self._load()

        # 更新缓存
        if self._config.cached:
            self._cache = data
            self._cache_time = time.time()

        return data

    def save(self, data: T) -> None:
        """保存数据

        支持版本控制。
        """
        # 确保目录存在
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)

        # 版本控制
        if self._config.versioned and self._path:
            version = self._config.version or self._generate_version()
            versioned_path = self._get_versioned_path(version)

            # 临时修改路径
            original_path = self._path
            self._path = versioned_path

            try:
                self._save(data)
            finally:
                self._path = original_path
        else:
            self._save(data)

        # 清除缓存
        self._cache = None
        self._cache_time = None

    def exists(self) -> bool:
        """检查数据是否存在"""
        if self._path:
            return self._path.exists()
        return False

    def describe(self) -> Dict[str, Any]:
        """获取数据集描述"""
        return self._describe()

    def invalidate_cache(self) -> None:
        """清除缓存"""
        self._cache = None
        self._cache_time = None

    def _generate_version(self) -> str:
        """生成版本号"""
        import datetime
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def _get_versioned_path(self, version: str) -> Path:
        """获取版本化路径"""
        if not self._path:
            raise ValueError("Path is required for versioned dataset")

        stem = self._path.stem
        suffix = self._path.suffix
        parent = self._path.parent

        return parent / f"{stem}_{version}{suffix}"


class MemoryDataSet(DataSet[T]):
    """内存数据集

    用于临时存储或测试。
    """

    def __init__(self, data: Optional[T] = None):
        super().__init__()
        self._data = data

    def _load(self) -> T:
        if self._data is None:
            raise ValueError("No data in memory dataset")
        return self._data

    def _save(self, data: T) -> None:
        self._data = data

    def exists(self) -> bool:
        return self._data is not None


class DataCatalog:
    """数据目录

    参考 kedro DataCatalog 设计，提供统一的数据访问接口。

    Example:
        catalog = DataCatalog()

        # 注册数据集
        catalog.register("raw_data", CSVDataSet("data/raw/input.csv"))
        catalog.register("processed", ParquetDataSet("data/processed/output.parquet"))

        # 加载数据
        raw = catalog.load("raw_data")

        # 保存数据
        catalog.save("processed", processed_data)

        # 从配置加载
        catalog = DataCatalog.from_config({
            "raw_data": {"type": "csv", "path": "data/raw/input.csv"},
            "processed": {"type": "parquet", "path": "data/processed/output.parquet"},
        })
    """

    def __init__(self):
        self._datasets: Dict[str, DataSet] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        name: str,
        dataset: DataSet,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """注册数据集

        Args:
            name: 数据集名称
            dataset: 数据集实例
            metadata: 元数据
        """
        self._datasets[name] = dataset
        if metadata:
            self._metadata[name] = metadata

    def load(self, name: str) -> Any:
        """加载数据

        Args:
            name: 数据集名称

        Returns:
            加载的数据

        Raises:
            KeyError: 数据集不存在
        """
        if name not in self._datasets:
            raise KeyError(f"Dataset '{name}' not found in catalog")

        return self._datasets[name].load()

    def save(self, name: str, data: Any) -> None:
        """保存数据

        Args:
            name: 数据集名称
            data: 要保存的数据

        Raises:
            KeyError: 数据集不存在
        """
        if name not in self._datasets:
            raise KeyError(f"Dataset '{name}' not found in catalog")

        self._datasets[name].save(data)

    def exists(self, name: str) -> bool:
        """检查数据是否存在"""
        if name not in self._datasets:
            return False
        return self._datasets[name].exists()

    def list(self) -> List[str]:
        """列出所有数据集"""
        return list(self._datasets.keys())

    def describe(self, name: Optional[str] = None) -> Dict[str, Any]:
        """描述数据集"""
        if name:
            if name not in self._datasets:
                raise KeyError(f"Dataset '{name}' not found")
            return self._datasets[name].describe()

        return {name: ds.describe() for name, ds in self._datasets.items()}

    def get_metadata(self, name: str) -> Dict[str, Any]:
        """获取数据集元数据"""
        return self._metadata.get(name, {})

    def remove(self, name: str) -> None:
        """移除数据集"""
        if name in self._datasets:
            del self._datasets[name]
        if name in self._metadata:
            del self._metadata[name]

    @classmethod
    def from_config(cls, config: Dict[str, Dict[str, Any]]) -> "DataCatalog":
        """从配置创建目录

        Args:
            config: 数据集配置字典

        Returns:
            DataCatalog 实例
        """
        from .datasets import create_dataset

        catalog = cls()

        for name, dataset_config in config.items():
            dataset = create_dataset(dataset_config)
            catalog.register(name, dataset)

        return catalog

    def __contains__(self, name: str) -> bool:
        return name in self._datasets

    def __len__(self) -> int:
        return len(self._datasets)

    def __iter__(self):
        return iter(self._datasets)
