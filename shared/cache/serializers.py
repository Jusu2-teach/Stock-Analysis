"""
缓存序列化器 (Cache Serializers)
=================================

参考设计:
- joblib: 高效的 numpy/pandas 序列化
- pickle: 通用序列化
- orjson: 高性能 JSON

针对数据分析场景优化的序列化器。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional
import pickle
import json
import io


class Serializer(ABC):
    """序列化器基类"""

    @abstractmethod
    def serialize(self, value: Any) -> bytes:
        """序列化"""

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """反序列化"""

    @property
    def content_type(self) -> str:
        """MIME 类型"""
        return "application/octet-stream"


class PickleSerializer(Serializer):
    """Pickle 序列化器

    通用序列化，支持大多数 Python 对象。
    """

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL):
        self.protocol = protocol

    def serialize(self, value: Any) -> bytes:
        return pickle.dumps(value, protocol=self.protocol)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)

    @property
    def content_type(self) -> str:
        return "application/python-pickle"


class JSONSerializer(Serializer):
    """JSON 序列化器

    适用于简单数据类型。
    """

    def __init__(self, ensure_ascii: bool = False, indent: Optional[int] = None):
        self.ensure_ascii = ensure_ascii
        self.indent = indent

    def serialize(self, value: Any) -> bytes:
        return json.dumps(
            value,
            ensure_ascii=self.ensure_ascii,
            indent=self.indent,
            default=str
        ).encode('utf-8')

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode('utf-8'))

    @property
    def content_type(self) -> str:
        return "application/json"


class ParquetSerializer(Serializer):
    """Parquet 序列化器

    针对 pandas DataFrame 优化，高压缩比。

    依赖: pyarrow
    """

    def __init__(self, compression: str = "snappy"):
        self.compression = compression

    def serialize(self, value: Any) -> bytes:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow is required for ParquetSerializer")

        # 处理 pandas DataFrame
        if hasattr(value, 'to_arrow'):
            # polars DataFrame
            table = value.to_arrow()
        elif hasattr(value, 'columns'):
            # pandas DataFrame
            table = pa.Table.from_pandas(value)
        else:
            raise TypeError(f"Cannot serialize {type(value)} to Parquet")

        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=self.compression)
        return buffer.getvalue()

    def deserialize(self, data: bytes) -> Any:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow and pandas are required for ParquetSerializer")

        buffer = io.BytesIO(data)
        table = pq.read_table(buffer)
        return table.to_pandas()

    @property
    def content_type(self) -> str:
        return "application/vnd.apache.parquet"


class DataFrameSerializer(Serializer):
    """DataFrame 智能序列化器

    自动检测 DataFrame 类型并选择最优序列化方式。

    参考 joblib 的 memory 模块。
    """

    def __init__(self, prefer_parquet: bool = True):
        self.prefer_parquet = prefer_parquet
        self._pickle = PickleSerializer()

    def serialize(self, value: Any) -> bytes:
        type_name = type(value).__name__

        # 检测类型
        if type_name == 'DataFrame':
            # pandas 或 polars DataFrame
            if self.prefer_parquet:
                try:
                    parquet = ParquetSerializer()
                    data = parquet.serialize(value)
                    # 添加类型标记
                    return b'PARQUET:' + data
                except Exception:
                    pass  # fallback to pickle

            # 使用 pickle
            data = self._pickle.serialize(value)
            return b'PICKLE:' + data

        elif type_name == 'ndarray':
            # numpy array
            try:
                import numpy as np
                buffer = io.BytesIO()
                np.save(buffer, value, allow_pickle=False)
                return b'NUMPY:' + buffer.getvalue()
            except Exception:
                pass

        # 默认 pickle
        data = self._pickle.serialize(value)
        return b'PICKLE:' + data

    def deserialize(self, data: bytes) -> Any:
        if data.startswith(b'PARQUET:'):
            parquet = ParquetSerializer()
            return parquet.deserialize(data[8:])

        elif data.startswith(b'NUMPY:'):
            try:
                import numpy as np
                buffer = io.BytesIO(data[6:])
                return np.load(buffer, allow_pickle=False)
            except Exception:
                pass

        elif data.startswith(b'PICKLE:'):
            return self._pickle.deserialize(data[7:])

        # 兼容旧格式
        return self._pickle.deserialize(data)


class CompressedSerializer(Serializer):
    """压缩序列化器

    包装其他序列化器，添加压缩。
    """

    def __init__(
        self,
        wrapped: Serializer,
        compression: str = "gzip",
        level: int = 6,
    ):
        self.wrapped = wrapped
        self.compression = compression
        self.level = level

    def serialize(self, value: Any) -> bytes:
        data = self.wrapped.serialize(value)

        if self.compression == "gzip":
            import gzip
            return gzip.compress(data, compresslevel=self.level)
        elif self.compression == "lz4":
            try:
                import lz4.frame
                return lz4.frame.compress(data)
            except ImportError:
                import gzip
                return gzip.compress(data)
        elif self.compression == "zstd":
            try:
                import zstandard as zstd
                return zstd.compress(data, level=self.level)
            except ImportError:
                import gzip
                return gzip.compress(data)
        else:
            return data

    def deserialize(self, data: bytes) -> Any:
        if self.compression == "gzip":
            import gzip
            data = gzip.decompress(data)
        elif self.compression == "lz4":
            try:
                import lz4.frame
                data = lz4.frame.decompress(data)
            except ImportError:
                import gzip
                data = gzip.decompress(data)
        elif self.compression == "zstd":
            try:
                import zstandard as zstd
                data = zstd.decompress(data)
            except ImportError:
                import gzip
                data = gzip.decompress(data)

        return self.wrapped.deserialize(data)


# 默认序列化器
DEFAULT_SERIALIZER = DataFrameSerializer()


def get_serializer(name: str = "auto") -> Serializer:
    """获取序列化器

    Args:
        name: pickle, json, parquet, dataframe, auto
    """
    if name == "pickle":
        return PickleSerializer()
    elif name == "json":
        return JSONSerializer()
    elif name == "parquet":
        return ParquetSerializer()
    elif name in ("dataframe", "auto"):
        return DataFrameSerializer()
    else:
        return DEFAULT_SERIALIZER
