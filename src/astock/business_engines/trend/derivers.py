"""
指标派生器模块（重构合并版）
============================

合并原 derivers/ 目录：
- base.py: 派生器接口
- roiic_deriver.py: ROIIC计算器

原文件：
- derivers/base.py
- derivers/roiic_deriver.py
- derivers/__init__.py
"""

import logging
from typing import Protocol, Set, runtime_checkable

logger = logging.getLogger(__name__)


# ============================================================================
# 派生器接口
# ============================================================================

@runtime_checkable
class MetricDeriver(Protocol):
    """指标派生器插件接口

    每个派生器负责将基础指标转换为派生指标。
    例如：ROIIC = ΔNOPAT / Δ投入资本
    """

    @property
    def metric_name(self) -> str:
        """返回派生指标名称（小写）"""
        ...

    @property
    def required_columns(self) -> Set[str]:
        """返回派生所需的源数据列"""
        ...

    @property
    def description(self) -> str:
        """派生器描述"""
        return f"{self.metric_name.upper()} 派生器"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """判断是否能派生指定指标"""
        if metric_name.lower() != self.metric_name.lower():
            return False
        return self.required_columns.issubset(available_cols)

    def derive(
        self,
        con,              # DuckDB 连接对象
        source_sql: str,  # 源数据 SQL
        group_column: str # 分组列名
    ) -> str:
        """执行派生逻辑，返回新视图名称"""
        ...


# ============================================================================
# ROIIC 派生器
# ============================================================================

class ROIICDeriver:
    """ROIIC (Return on Incremental Invested Capital) 派生器

    计算增量资本回报率：ROIIC = ΔNOPAT / Δ投入资本
    其中：NOPAT ≈ ROIC × 投入资本
    """

    @property
    def metric_name(self) -> str:
        return "roiic"

    @property
    def required_columns(self) -> Set[str]:
        return {"roic", "invest_capital", "end_date"}

    @property
    def description(self) -> str:
        return "增量资本回报率 (ROIIC): 衡量新增投资的回报效率"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """判断是否能派生 ROIIC"""
        if metric_name.lower() != self.metric_name:
            return False
        return self.required_columns.issubset(available_cols)

    def derive(self, con, source_sql: str, group_column: str) -> str:
        """派生 ROIIC 指标

        步骤：
        1. 估算 NOPAT = ROIC × 投入资本
        2. 计算 ΔNOPAT（使用 LAG 窗口函数）
        3. 计算 Δ投入资本
        4. ROIIC = ΔNOPAT / Δ投入资本
        """
        from ..engines.duckdb_core import _q

        group_col_q = _q(group_column)
        view_name = "trend_with_roiic"

        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            WITH base AS (
                SELECT *,
                    CASE
                        WHEN roic IS NULL OR invest_capital IS NULL THEN NULL
                        ELSE (roic / 100.0) * invest_capital
                    END AS nopat_est
                FROM {source_sql}
            ),
            lagged AS (
                SELECT base.*,
                    LAG(nopat_est) OVER (
                        PARTITION BY {group_col_q}
                        ORDER BY end_date
                    ) AS nopat_prev,
                    LAG(invest_capital) OVER (
                        PARTITION BY {group_col_q}
                        ORDER BY end_date
                    ) AS invest_prev
                FROM base
            )
            SELECT
                lagged.* EXCLUDE (nopat_est, nopat_prev, invest_prev),
                CASE
                    WHEN nopat_est IS NULL OR nopat_prev IS NULL THEN NULL
                    WHEN invest_prev IS NULL OR invest_capital IS NULL THEN NULL
                    WHEN ABS(invest_capital - invest_prev) < 1e-6 THEN NULL
                    ELSE ((nopat_est - nopat_prev) /
                          (invest_capital - invest_prev)) * 100.0
                END AS roiic
            FROM lagged
        """

        logger.info("🔌 ROIIC 插件: 派生 ROIIC = ΔNOPAT / Δ投入资本")
        con.execute(sql)
        return view_name


# ============================================================================
# 派生器注册系统（简化版）
# ============================================================================

_REGISTERED_DERIVERS = []


def register_deriver(deriver: MetricDeriver):
    """注册派生器"""
    if deriver not in _REGISTERED_DERIVERS:
        _REGISTERED_DERIVERS.append(deriver)
        logger.info(f"✅ 已注册派生器: {deriver.metric_name}")


def get_registered_derivers():
    """获取所有已注册的派生器"""
    return _REGISTERED_DERIVERS.copy()


def list_available_metrics():
    """列出所有可派生的指标名"""
    return [d.metric_name for d in _REGISTERED_DERIVERS]


def find_deriver(metric_name: str, available_cols: Set[str]) -> MetricDeriver:
    """查找可用的派生器

    Args:
        metric_name: 指标名
        available_cols: 可用列集合

    Returns:
        派生器实例，如果没找到则返回 None
    """
    for deriver in _REGISTERED_DERIVERS:
        if deriver.can_derive(metric_name, available_cols):
            return deriver
    return None


def check_derivable(metric_name: str, available_cols: Set[str]) -> bool:
    """检查是否可派生某指标

    Args:
        metric_name: 指标名
        available_cols: 可用列集合

    Returns:
        True 如果可派生
    """
    return find_deriver(metric_name, available_cols) is not None


# 自动注册内置派生器
register_deriver(ROIICDeriver())


# 导出
__all__ = [
    'MetricDeriver',
    'ROIICDeriver',
    'register_deriver',
    'get_registered_derivers',
    'find_deriver',
    'list_available_metrics',
    'check_derivable',
]
