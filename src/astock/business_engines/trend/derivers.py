"""
指标派生器模块（重构合并版）
============================

提供可扩展的指标派生框架，支持从基础指标动态计算派生指标。

设计原则:
1. 插件化: 新派生器只需实现 MetricDeriver 协议并注册
2. 零侵入: 使用 DuckDB 临时视图，不修改原始数据
3. 可组合: 支持链式派生（派生指标依赖另一个派生指标）
4. 可验证: 派生结果自动校验

使用示例:
    # 注册自定义派生器
    @dataclass
    class MyDeriver:
        metric_name = "my_metric"
        required_columns = {"col_a", "col_b"}
        ...
    register_deriver(MyDeriver())

    # 自动派生
    deriver = find_deriver("my_metric", available_cols)
    if deriver:
        new_source = deriver.derive(con, source_sql, group_col)
"""

import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol, Set, Tuple, runtime_checkable

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
        ...

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """判断是否能派生指定指标"""
        ...

    def derive(
        self,
        con: Any,         # DuckDB 连接对象
        source_sql: str,  # 源数据 SQL/视图名
        group_column: str # 分组列名
    ) -> str:
        """执行派生逻辑，返回新视图名称"""
        ...


# ============================================================================
# 派生器基类（推荐继承）
# ============================================================================

class BaseDeriver(ABC):
    """派生器基类，提供通用实现"""

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """返回派生指标名称（小写）"""

    @property
    @abstractmethod
    def required_columns(self) -> Set[str]:
        """返回派生所需的源数据列"""

    @property
    def description(self) -> str:
        """派生器描述"""
        return f"{self.metric_name.upper()} 派生器"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """判断是否能派生指定指标"""
        if metric_name.lower() != self.metric_name.lower():
            return False
        return self.required_columns.issubset(available_cols)

    def get_missing_columns(self, available_cols: Set[str]) -> Set[str]:
        """获取缺失的必需列"""
        return self.required_columns - available_cols

    def _generate_view_name(self) -> str:
        """生成唯一视图名，避免冲突"""
        short_id = uuid.uuid4().hex[:8]
        return f"derived_{self.metric_name}_{short_id}"

    @abstractmethod
    def derive(
        self,
        con: Any,
        source_sql: str,
        group_column: str
    ) -> str:
        """执行派生逻辑，返回新视图名称"""


# ============================================================================
# ROIIC 派生器
# ============================================================================

class ROIICDeriver(BaseDeriver):
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

    def derive(self, con: Any, source_sql: str, group_column: str) -> str:
        """派生 ROIIC 指标

        步骤：
        1. 估算 NOPAT = ROIC × 投入资本
        2. 计算 ΔNOPAT（使用 LAG 窗口函数）
        3. 计算 Δ投入资本
        4. ROIIC = ΔNOPAT / Δ投入资本
        """
        def _q(col: str) -> str:
            """对列名加双引号"""
            return f'"{col}"'

        group_col_q = _q(group_column)
        view_name = self._generate_view_name()

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

        logger.info(f"🔌 ROIIC 派生器: ROIIC = ΔNOPAT / Δ投入资本 → {view_name}")
        con.execute(sql)
        return view_name


# ============================================================================
# FCF Margin 派生器
# ============================================================================

class FCFMarginDeriver(BaseDeriver):
    """FCF Margin (Free Cash Flow Margin) 派生器

    计算自由现金流利润率：FCF Margin = (OCF - Capex) / Revenue
    其中：
    - OCF = n_cashflow_act (经营活动产生的现金流量净额)
    - Capex = c_pay_acq_const_fiolta (购建固定资产等支付的现金)
    - Revenue = revenue (营业收入)

    意义：衡量公司将收入转化为自由现金流的能力，
    是比净利率更难操纵的盈利质量指标。
    """

    @property
    def metric_name(self) -> str:
        return "fcf_margin"

    @property
    def required_columns(self) -> Set[str]:
        return {"n_cashflow_act", "c_pay_acq_const_fiolta", "revenue"}

    @property
    def description(self) -> str:
        return "自由现金流利润率 (FCF Margin): (OCF - Capex) / Revenue"

    def derive(self, con: Any, source_sql: str, group_column: str) -> str:
        def _q(col: str) -> str:
            return f'"{col}"'

        view_name = self._generate_view_name()

        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN revenue IS NULL OR revenue = 0 THEN NULL
                    WHEN n_cashflow_act IS NULL THEN NULL
                    ELSE (
                        COALESCE(n_cashflow_act, 0)
                        - COALESCE(c_pay_acq_const_fiolta, 0)
                    ) / NULLIF(revenue, 0) * 100.0
                END AS fcf_margin
            FROM {source_sql}
        """

        logger.info(f"🔌 FCF Margin 派生器: (OCF - Capex) / Revenue → {view_name}")
        con.execute(sql)
        return view_name


# ============================================================================
# Asset Turnover 派生器 (DuPont 分解组分)
# ============================================================================

class AssetTurnoverDeriver(BaseDeriver):
    """Asset Turnover (资产周转率) 派生器

    计算资产周转率：Asset Turnover = Revenue / Total Assets
    DuPont 分解要素：ROIC = Net Margin × Asset Turnover

    意义：衡量公司利用资产创造收入的效率，
    是区分 "利润型" 与 "效率型" 公司的关键指标。
    """

    @property
    def metric_name(self) -> str:
        return "asset_turnover"

    @property
    def required_columns(self) -> Set[str]:
        return {"total_revenue", "total_assets"}

    @property
    def description(self) -> str:
        return "资产周转率 (Asset Turnover): Revenue / Total Assets (DuPont 组分)"

    def derive(self, con: Any, source_sql: str, group_column: str) -> str:
        view_name = self._generate_view_name()

        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN total_assets IS NULL OR total_assets = 0 THEN NULL
                    WHEN total_revenue IS NULL THEN NULL
                    ELSE total_revenue / NULLIF(total_assets, 0)
                END AS asset_turnover
            FROM {source_sql}
        """

        logger.info(f"🔌 Asset Turnover 派生器: Revenue / Total Assets → {view_name}")
        con.execute(sql)
        return view_name


# ============================================================================
# Interest Coverage 派生器
# ============================================================================

class InterestCoverageDeriver(BaseDeriver):
    """Interest Coverage (利息覆盖倍数) 派生器

    计算利息保障倍数：Interest Coverage = EBIT / Interest Expense
    其中：
    - EBIT ≈ 营业利润 + 利息支出 (简化估计)
    - Interest Expense = int_exp (利息费用)

    意义：衡量公司偿付利息的能力，
    是评估财务安全性的核心杠杆指标。
    < 1.5 = 危险, > 5.0 = 健康
    """

    @property
    def metric_name(self) -> str:
        return "interest_coverage"

    @property
    def required_columns(self) -> Set[str]:
        return {"operate_profit", "int_exp"}

    @property
    def description(self) -> str:
        return "利息覆盖倍数 (Interest Coverage): EBIT / Interest Expense"

    def derive(self, con: Any, source_sql: str, group_column: str) -> str:
        view_name = self._generate_view_name()

        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN int_exp IS NULL OR int_exp = 0 THEN NULL
                    ELSE (COALESCE(operate_profit, 0) + COALESCE(int_exp, 0))
                         / NULLIF(ABS(int_exp), 0)
                END AS interest_coverage
            FROM {source_sql}
        """

        logger.info(f"🔌 Interest Coverage 派生器: EBIT / Interest Expense → {view_name}")
        con.execute(sql)
        return view_name


# ============================================================================
# 派生器注册系统
# ============================================================================

@dataclass
class DeriverRegistry:
    """派生器注册表（单例模式）"""
    _derivers: Dict[str, MetricDeriver] = field(default_factory=dict)

    def register(self, deriver: MetricDeriver) -> None:
        """注册派生器"""
        name = deriver.metric_name.lower()
        if name in self._derivers:
            logger.warning(f"⚠️ 派生器 {name} 已存在，将被覆盖")
        self._derivers[name] = deriver
        logger.debug(f"✅ 已注册派生器: {name}")

    def unregister(self, metric_name: str) -> bool:
        """注销派生器"""
        name = metric_name.lower()
        if name in self._derivers:
            del self._derivers[name]
            logger.debug(f"🗑️ 已注销派生器: {name}")
            return True
        return False

    def get(self, metric_name: str) -> Optional[MetricDeriver]:
        """获取指定派生器"""
        return self._derivers.get(metric_name.lower())

    def find(self, metric_name: str, available_cols: Set[str]) -> Optional[MetricDeriver]:
        """查找可用的派生器"""
        deriver = self.get(metric_name)
        if deriver and deriver.can_derive(metric_name, available_cols):
            return deriver
        return None

    def list_all(self) -> Dict[str, MetricDeriver]:
        """获取所有派生器"""
        return self._derivers.copy()

    def list_names(self) -> list:
        """列出所有可派生的指标名"""
        return list(self._derivers.keys())

    def clear(self) -> None:
        """清空所有派生器"""
        self._derivers.clear()


# 全局单例
_registry = DeriverRegistry()


# ============================================================================
# 便捷函数 API
# ============================================================================

def register_deriver(deriver: MetricDeriver) -> None:
    """注册派生器"""
    _registry.register(deriver)


def unregister_deriver(metric_name: str) -> bool:
    """注销派生器"""
    return _registry.unregister(metric_name)


def get_registered_derivers() -> Dict[str, MetricDeriver]:
    """获取所有已注册的派生器"""
    return _registry.list_all()


def list_available_metrics() -> list:
    """列出所有可派生的指标名"""
    return _registry.list_names()


def find_deriver(metric_name: str, available_cols: Set[str]) -> Optional[MetricDeriver]:
    """查找可用的派生器

    Args:
        metric_name: 指标名
        available_cols: 可用列集合

    Returns:
        派生器实例，如果没找到则返回 None
    """
    return _registry.find(metric_name, available_cols)


def check_derivable(metric_name: str, available_cols: Set[str]) -> Tuple[bool, Set[str]]:
    """检查是否可派生某指标

    Args:
        metric_name: 指标名
        available_cols: 可用列集合

    Returns:
        (是否可派生, 缺失的列集合)
    """
    deriver = _registry.get(metric_name)
    if deriver is None:
        return False, set()

    if deriver.can_derive(metric_name, available_cols):
        return True, set()

    # 计算缺失列
    if isinstance(deriver, BaseDeriver):
        missing = deriver.get_missing_columns(available_cols)
    else:
        missing = deriver.required_columns - available_cols

    return False, missing


def get_deriver_info(metric_name: str) -> Optional[Dict[str, Any]]:
    """获取派生器详细信息

    Args:
        metric_name: 指标名

    Returns:
        派生器信息字典，或 None
    """
    deriver = _registry.get(metric_name)
    if deriver is None:
        return None

    return {
        'metric_name': deriver.metric_name,
        'required_columns': list(deriver.required_columns),
        'description': deriver.description,
        'class': deriver.__class__.__name__,
    }


# ============================================================================
# 自动注册内置派生器
# ============================================================================

register_deriver(ROIICDeriver())
register_deriver(FCFMarginDeriver())
register_deriver(AssetTurnoverDeriver())
register_deriver(InterestCoverageDeriver())


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 接口与基类
    'MetricDeriver',
    'BaseDeriver',
    # 内置派生器
    'ROIICDeriver',
    'FCFMarginDeriver',
    'AssetTurnoverDeriver',
    'InterestCoverageDeriver',
    # 注册系统
    'DeriverRegistry',
    'register_deriver',
    'unregister_deriver',
    'get_registered_derivers',
    # 查询函数
    'find_deriver',
    'list_available_metrics',
    'check_derivable',
    'get_deriver_info',
]
