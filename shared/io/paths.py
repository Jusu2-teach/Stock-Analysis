"""
路径管理 (Path Management)
==========================

参考设计:
- kedro: DataCatalog 路径解析
- pathlib: 路径操作

提供统一的路径管理和解析。
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional, Union
import os


class PathManager:
    """路径管理器

    统一管理项目中的各种路径。

    Example:
        paths = PathManager(base_dir="data")

        # 获取路径
        raw_path = paths.get("raw/input.csv")
        output_path = paths.output("result.parquet")

        # 确保目录存在
        paths.ensure("processed")
    """

    # 默认目录结构
    DEFAULT_DIRS = {
        "raw": "raw",
        "processed": "processed",
        "output": "output",
        "cache": ".cache",
        "logs": "logs",
        "models": "models",
        "reports": "reports",
    }

    def __init__(
        self,
        base_dir: Optional[Union[str, Path]] = None,
        dirs: Optional[Dict[str, str]] = None,
    ):
        """初始化路径管理器

        Args:
            base_dir: 基础目录（默认为项目 data 目录）
            dirs: 自定义目录映射
        """
        self._base_dir = Path(base_dir) if base_dir else self._find_project_data_dir()
        self._dirs = {**self.DEFAULT_DIRS, **(dirs or {})}

    @property
    def base_dir(self) -> Path:
        """基础目录"""
        return self._base_dir

    def get(self, path: Union[str, Path]) -> Path:
        """获取完整路径

        Args:
            path: 相对路径

        Returns:
            绝对路径
        """
        return self._base_dir / path

    def raw(self, path: Union[str, Path]) -> Path:
        """获取原始数据路径"""
        return self._base_dir / self._dirs["raw"] / path

    def processed(self, path: Union[str, Path]) -> Path:
        """获取处理后数据路径"""
        return self._base_dir / self._dirs["processed"] / path

    def output(self, path: Union[str, Path]) -> Path:
        """获取输出路径"""
        return self._base_dir / self._dirs["output"] / path

    def cache(self, path: Union[str, Path]) -> Path:
        """获取缓存路径"""
        return self._base_dir / self._dirs["cache"] / path

    def logs(self, path: Union[str, Path]) -> Path:
        """获取日志路径"""
        return self._base_dir / self._dirs["logs"] / path

    def models(self, path: Union[str, Path]) -> Path:
        """获取模型路径"""
        return self._base_dir / self._dirs["models"] / path

    def reports(self, path: Union[str, Path]) -> Path:
        """获取报告路径"""
        return self._base_dir / self._dirs["reports"] / path

    def ensure(self, *paths: str) -> None:
        """确保目录存在

        Args:
            *paths: 目录名称或相对路径
        """
        for path in paths:
            if path in self._dirs:
                dir_path = self._base_dir / self._dirs[path]
            else:
                dir_path = self._base_dir / path

            dir_path.mkdir(parents=True, exist_ok=True)

    def list_files(
        self,
        pattern: str = "*",
        directory: Optional[str] = None,
        recursive: bool = False,
    ) -> List[Path]:
        """列出文件

        Args:
            pattern: 文件模式（glob 语法）
            directory: 目录名称
            recursive: 是否递归

        Returns:
            文件路径列表
        """
        if directory:
            base = self._base_dir / (self._dirs.get(directory) or directory)
        else:
            base = self._base_dir

        if recursive:
            return list(base.rglob(pattern))
        return list(base.glob(pattern))

    def exists(self, path: Union[str, Path]) -> bool:
        """检查路径是否存在"""
        return (self._base_dir / path).exists()

    def relative(self, path: Union[str, Path]) -> Path:
        """转换为相对路径"""
        abs_path = Path(path).resolve()
        try:
            return abs_path.relative_to(self._base_dir)
        except ValueError:
            return abs_path

    @staticmethod
    def _find_project_data_dir() -> Path:
        """查找项目 data 目录"""
        # 尝试从当前目录向上查找
        current = Path.cwd()

        while current != current.parent:
            data_dir = current / "data"
            if data_dir.exists():
                return data_dir
            current = current.parent

        # 默认使用当前目录的 data 子目录
        return Path.cwd() / "data"


# 全局路径管理器实例
_default_manager: Optional[PathManager] = None


def get_path_manager() -> PathManager:
    """获取默认路径管理器"""
    global _default_manager
    if _default_manager is None:
        _default_manager = PathManager()
    return _default_manager


def set_path_manager(manager: PathManager) -> None:
    """设置默认路径管理器"""
    global _default_manager
    _default_manager = manager


def resolve_path(path: Union[str, Path]) -> Path:
    """解析路径

    支持以下格式：
    - 绝对路径
    - 相对路径（相对于项目 data 目录）
    - 环境变量（如 $DATA_DIR/input.csv）

    Args:
        path: 输入路径

    Returns:
        解析后的绝对路径
    """
    path_str = str(path)

    # 展开环境变量
    path_str = os.path.expandvars(path_str)

    # 展开用户目录
    path_str = os.path.expanduser(path_str)

    result = Path(path_str)

    # 如果是相对路径，基于 data 目录
    if not result.is_absolute():
        result = get_path_manager().get(result)

    return result


def ensure_dir(path: Union[str, Path]) -> Path:
    """确保目录存在

    Args:
        path: 目录路径

    Returns:
        目录路径
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_data_dir() -> Path:
    """获取数据目录

    Returns:
        数据目录路径
    """
    return get_path_manager().base_dir


# AStock 项目专用路径
class AStockPaths(PathManager):
    """AStock 项目路径管理器

    专为 AStock 项目定制的路径管理。

    Example:
        paths = AStockPaths()

        # 10年数据
        raw_10y = paths.raw_10y("20241231_fina_indicator.csv")

        # 中间结果
        middle = paths.filter_middle("roic_trend_analysis.csv")

        # 聚合数据
        polars_data = paths.polars("10yd_final_industry.csv")
    """

    ASTOCK_DIRS = {
        "raw": "raw",
        "raw_10y": "10yd_base",
        "raw_5y": "5yd_base",
        "filter_middle": "filter_middle",
        "polars": "polars",
        "output": ".",
        "cache": ".cache",
        "logs": "logs",
    }

    def __init__(self, base_dir: Optional[Union[str, Path]] = None):
        if base_dir is None:
            # 查找项目根目录
            base_dir = self._find_astock_data_dir()

        super().__init__(base_dir, self.ASTOCK_DIRS)

    def raw_10y(self, path: Union[str, Path]) -> Path:
        """获取 10 年原始数据路径"""
        return self._base_dir / self._dirs["raw_10y"] / path

    def raw_5y(self, path: Union[str, Path]) -> Path:
        """获取 5 年原始数据路径"""
        return self._base_dir / self._dirs["raw_5y"] / path

    def filter_middle(self, path: Union[str, Path]) -> Path:
        """获取中间结果路径"""
        return self._base_dir / self._dirs["filter_middle"] / path

    def polars(self, path: Union[str, Path]) -> Path:
        """获取 Polars 聚合数据路径"""
        return self._base_dir / self._dirs["polars"] / path

    def report(self, name: str) -> Path:
        """获取报告路径"""
        return self._base_dir / f"{name}_report.md"

    def list_raw_files(self, years: int = 10) -> List[Path]:
        """列出原始数据文件

        Args:
            years: 年数（5 或 10）

        Returns:
            文件路径列表
        """
        if years == 5:
            base = self.raw_5y("")
        else:
            base = self.raw_10y("")

        return sorted(base.glob("*_fina_indicator.csv"))

    @staticmethod
    def _find_astock_data_dir() -> Path:
        """查找 AStock 项目的 data 目录"""
        # 尝试从当前目录向上查找
        current = Path.cwd()

        while current != current.parent:
            # 检查是否有 AStock 项目特征
            data_dir = current / "data"
            if data_dir.exists():
                if (data_dir / "10yd_base").exists() or (data_dir / "filter_middle").exists():
                    return data_dir
            current = current.parent

        # 默认使用当前目录的 data 子目录
        return Path.cwd() / "data"
