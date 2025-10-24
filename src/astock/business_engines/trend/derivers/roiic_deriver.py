"""
ROIIC (Return on Incremental Invested Capital) 派生器
====================================================

计算增量资本回报率：衡量新增投资的回报效率。

公式：ROIIC = ΔNOPAT / Δ投入资本
其中：NOPAT ≈ ROIC × 投入资本

依赖列：
- roic: 投入资本回报率 (%)
- invest_capital: 投入资本
- end_date: 时间列（用于计算增量）
"""

import logging
from typing import Set

logger = logging.getLogger(__name__)


class ROIICDeriver:
    """
    ROIIC 派生器

    将 ROIC 和投入资本转换为增量资本回报率。
    这个指标比 ROIC 更能反映企业扩张质量。
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
        """
        派生 ROIIC 指标

        实现步骤：
        1. 估算 NOPAT = ROIC × 投入资本
        2. 计算 ΔNOPAT（使用 LAG 窗口函数）
        3. 计算 Δ投入资本
        4. ROIIC = ΔNOPAT / Δ投入资本
        """
        # 导入 DuckDB 工具函数
        from ...engines.duckdb_utils import _q

        group_col_q = _q(group_column)
        view_name = "trend_with_roiic"

        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            WITH base AS (
                -- 第一步：估算 NOPAT
                SELECT *,
                    CASE
                        WHEN roic IS NULL OR invest_capital IS NULL THEN NULL
                        ELSE (roic / 100.0) * invest_capital
                    END AS nopat_est
                FROM {source_sql}
            ),
            lagged AS (
                -- 第二步：使用 LAG 获取上期数据
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
            -- 第三步：计算 ROIIC
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
