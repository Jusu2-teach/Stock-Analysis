"""TRUTH 特征注册表 - 元数据识别.

核心职责：识别元数据列，避免将非特征列纳入因子计算。

参考: docs/TRUTH_SYSTEM_DESIGN.md
"""

from __future__ import annotations

from typing import FrozenSet


# ============================================================
# 元数据字段 - 这些不应参与因子计算
# ============================================================
METADATA_COLUMNS: FrozenSet[str] = frozenset({
    # 标识类
    "ts_code",
    "metric_name",
    "name",
    "industry",
    # 时间类
    "ann_date",
    "end_date",
    # 数据质量类
    "full_data_years",
    "trend_window_years",
    "data_regime",
    "break_year_index",
})


def is_metadata_column(column_name: str) -> bool:
    """检查列是否为元数据."""
    # 直接匹配
    if column_name in METADATA_COLUMNS:
        return True
    # 检查后缀模式
    metadata_suffixes = (
        "_full_data_years",
        "_trend_window_years",
        "_data_regime",
        "_break_year_index",
    )
    return any(column_name.endswith(suffix) for suffix in metadata_suffixes)


__all__ = [
    "METADATA_COLUMNS",
    "is_metadata_column",
]
