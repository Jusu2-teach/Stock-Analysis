"""
数据写入器 (Data Writers)
=========================

参考设计:
- pandas: 多格式写入
- polars: 高性能写入

提供多种格式的数据写入器。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


class BaseWriter(ABC):
    """写入器基类"""

    @abstractmethod
    def write(self, data: Any, path: Union[str, Path], **kwargs) -> None:
        """写入数据"""

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        """检查是否支持该文件类型"""
        return False


class CSVWriter(BaseWriter):
    """CSV 写入器

    Example:
        writer = CSVWriter()
        writer.write(df, "output.csv")

        # 自定义选项
        writer.write(df, "output.csv", index=False, encoding="utf-8-sig")
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {"index": False}

    def write(
        self,
        data: Union["pd.DataFrame", "pl.DataFrame"],
        path: Union[str, Path],
        **kwargs,
    ) -> None:
        """写入 CSV 文件

        Args:
            data: DataFrame
            path: 输出路径
            **kwargs: 其他参数
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        options = {**self.default_options, **kwargs}

        # 检测数据类型
        if _is_polars(data):
            data.write_csv(path)
        else:
            data.to_csv(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        return str(path).lower().endswith('.csv')


class ParquetWriter(BaseWriter):
    """Parquet 写入器

    Example:
        writer = ParquetWriter()
        writer.write(df, "output.parquet")

        # 压缩选项
        writer.write(df, "output.parquet", compression="snappy")
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {"compression": "snappy"}

    def write(
        self,
        data: Union["pd.DataFrame", "pl.DataFrame"],
        path: Union[str, Path],
        **kwargs,
    ) -> None:
        """写入 Parquet 文件

        Args:
            data: DataFrame
            path: 输出路径
            **kwargs: 其他参数
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        options = {**self.default_options, **kwargs}

        if _is_polars(data):
            data.write_parquet(path, **options)
        else:
            data.to_parquet(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        return str(path).lower().endswith('.parquet')


class JSONWriter(BaseWriter):
    """JSON 写入器

    Example:
        writer = JSONWriter()
        writer.write(df, "output.json")

        # JSON Lines
        writer.write(df, "output.jsonl", lines=True)
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {"orient": "records"}

    def write(
        self,
        data: Union["pd.DataFrame", "pl.DataFrame", Dict, List],
        path: Union[str, Path],
        lines: bool = False,
        **kwargs,
    ) -> None:
        """写入 JSON 文件

        Args:
            data: 数据
            path: 输出路径
            lines: 是否为 JSON Lines 格式
            **kwargs: 其他参数
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        options = {**self.default_options, **kwargs}

        # 自动检测 JSON Lines
        if not lines and str(path).lower().endswith('.jsonl'):
            lines = True

        if lines:
            options["orient"] = "records"
            options["lines"] = True

        if _is_polars(data):
            if lines:
                data.write_ndjson(path)
            else:
                data.write_json(path)
        elif isinstance(data, (dict, list)):
            import json
            with open(path, 'w', encoding='utf-8') as f:
                if lines and isinstance(data, list):
                    for item in data:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
                else:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        else:
            data.to_json(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        path_str = str(path).lower()
        return path_str.endswith('.json') or path_str.endswith('.jsonl')


class ExcelWriter(BaseWriter):
    """Excel 写入器

    Example:
        writer = ExcelWriter()
        writer.write(df, "output.xlsx")

        # 多工作表
        writer.write({"Sheet1": df1, "Sheet2": df2}, "output.xlsx")
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {"index": False}

    def write(
        self,
        data: Union["pd.DataFrame", Dict[str, "pd.DataFrame"]],
        path: Union[str, Path],
        sheet_name: str = "Sheet1",
        **kwargs,
    ) -> None:
        """写入 Excel 文件

        Args:
            data: DataFrame 或 {sheet_name: DataFrame}
            path: 输出路径
            sheet_name: 工作表名称
            **kwargs: 其他参数
        """
        import pandas as pd

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        options = {**self.default_options, **kwargs}

        if isinstance(data, dict):
            with pd.ExcelWriter(path) as writer:
                for name, df in data.items():
                    df.to_excel(writer, sheet_name=name, **options)
        else:
            # 转换 polars 到 pandas
            if _is_polars(data):
                data = data.to_pandas()
            data.to_excel(path, sheet_name=sheet_name, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        path_str = str(path).lower()
        return path_str.endswith('.xlsx') or path_str.endswith('.xls')


class MarkdownWriter(BaseWriter):
    """Markdown 写入器

    Example:
        writer = MarkdownWriter()
        writer.write(df, "output.md")
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {}

    def write(
        self,
        data: Union["pd.DataFrame", str],
        path: Union[str, Path],
        **kwargs,
    ) -> None:
        """写入 Markdown 文件

        Args:
            data: DataFrame 或字符串
            path: 输出路径
            **kwargs: 其他参数
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(data, str):
            with open(path, 'w', encoding='utf-8') as f:
                f.write(data)
        else:
            # DataFrame to Markdown
            if _is_polars(data):
                data = data.to_pandas()

            options = {**self.default_options, **kwargs}
            markdown = data.to_markdown(**options)

            with open(path, 'w', encoding='utf-8') as f:
                f.write(markdown)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        return str(path).lower().endswith('.md')


def _is_polars(data: Any) -> bool:
    """检查是否为 Polars DataFrame"""
    try:
        import polars as pl
        return isinstance(data, (pl.DataFrame, pl.LazyFrame))
    except ImportError:
        return False


# 便捷函数
def write_csv(
    data: Union["pd.DataFrame", "pl.DataFrame"],
    path: Union[str, Path],
    **kwargs,
) -> None:
    """写入 CSV 文件

    Args:
        data: DataFrame
        path: 输出路径
        **kwargs: 其他参数
    """
    CSVWriter().write(data, path, **kwargs)


def write_parquet(
    data: Union["pd.DataFrame", "pl.DataFrame"],
    path: Union[str, Path],
    **kwargs,
) -> None:
    """写入 Parquet 文件

    Args:
        data: DataFrame
        path: 输出路径
        **kwargs: 其他参数
    """
    ParquetWriter().write(data, path, **kwargs)


def write_json(
    data: Union["pd.DataFrame", "pl.DataFrame", Dict, List],
    path: Union[str, Path],
    **kwargs,
) -> None:
    """写入 JSON 文件

    Args:
        data: 数据
        path: 输出路径
        **kwargs: 其他参数
    """
    JSONWriter().write(data, path, **kwargs)


def write_excel(
    data: Union["pd.DataFrame", Dict[str, "pd.DataFrame"]],
    path: Union[str, Path],
    **kwargs,
) -> None:
    """写入 Excel 文件

    Args:
        data: DataFrame
        path: 输出路径
        **kwargs: 其他参数
    """
    ExcelWriter().write(data, path, **kwargs)


def write_markdown(
    data: Union["pd.DataFrame", str],
    path: Union[str, Path],
    **kwargs,
) -> None:
    """写入 Markdown 文件

    Args:
        data: DataFrame 或字符串
        path: 输出路径
        **kwargs: 其他参数
    """
    MarkdownWriter().write(data, path, **kwargs)
