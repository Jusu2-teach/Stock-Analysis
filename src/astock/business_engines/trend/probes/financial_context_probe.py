"""财务上下文探针 (Financial Context Probe)

专为 T.R.U.T.H. β/δ_fraud 因子设计的数据提取探针。

职责：
    1. 从原始财务数据提取资产结构字段（用于 β 因子）
    2. 从原始财务数据提取风险检测字段（用于 δ_fraud 因子）
    3. 计算衍生比率（hard_asset_ratio, goodwill_to_equity 等）

设计原则：
    - 完全遵循现有 Probe 协议
    - 输入: Dict[str, float] (原始财务字段)
    - 输出: FinancialContextResult (结构化结果)
    - 与其他 Probe 统一处理流程

使用场景：
    - 在 TrendAnalyzer 处理原始财务数据时调用
    - 输出与其他 Probe 一起传递给 TRUTH 层

版本: 1.1.0
日期: 2026-01-06
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class FinancialContextResult:
    """财务上下文探针输出结果

    包含两类数据：
    1. 原始财务字段 (raw_*): 直接从财务报表提取
    2. 衍生比率 (ratio_*): 计算得出的财务比率

    用于 β 因子的字段:
        - raw_fix_assets: 固定资产
        - raw_cip: 在建工程
        - raw_total_assets: 总资产
        - ratio_hard_asset: (固定资产+在建工程) / 总资产
        - ratio_intang_asset: 无形资产 / 总资产

    用于 δ_fraud 因子的字段:
        - raw_goodwill: 商誉
        - raw_equity: 股东权益
        - ratio_goodwill_to_equity: 商誉 / 股东权益 (>0.4 高风险)
        - ratio_cash_to_assets: 货币资金 / 总资产
        - ratio_debt_to_assets: 总负债 / 总资产
        - flag_cash_loan_anomaly: 存贷双高标记 (cash>30% 且 debt>60%)
        - ratio_receivable_to_revenue: 应收账款 / 营收
    """

    # ========== 原始字段 (用于 β 因子) ==========
    raw_fix_assets: float = 0.0           # 固定资产
    raw_cip: float = 0.0                  # 在建工程 (Construction in Progress)
    raw_total_assets: float = 0.0         # 总资产
    raw_total_cur_assets: float = 0.0     # 流动资产
    raw_total_nca: float = 0.0            # 非流动资产
    raw_intang_assets: float = 0.0        # 无形资产

    # ========== 原始字段 (用于 δ_fraud 因子) ==========
    raw_goodwill: float = 0.0             # 商誉
    raw_equity: float = 0.0               # 股东权益 (不含少数股东)
    raw_money_cap: float = 0.0            # 货币资金
    raw_total_liab: float = 0.0           # 总负债
    raw_accounts_receiv: float = 0.0      # 应收账款
    raw_inventories: float = 0.0          # 存货
    raw_lt_borr: float = 0.0              # 长期借款
    raw_st_borr: float = 0.0              # 短期借款
    raw_total_revenue: float = 0.0        # 营业总收入

    # ========== β 因子衍生比率 ==========
    ratio_hard_asset: float = 0.0         # 硬资产比率 = (固定+在建) / 总资产
    ratio_intang_asset: float = 0.0       # 无形资产比率 = 无形 / 总资产
    ratio_nca: float = 0.0                # 非流动资产比率 = 非流动 / 总资产
    ratio_working_capital: float = 0.0    # 营运资本比率 = (流动资产-流动负债) / 总资产

    # ========== δ_fraud 因子衍生比率 ==========
    ratio_goodwill_to_equity: float = 0.0 # 商誉/权益比 (>0.4 触发商誉爆雷预警)
    ratio_cash_to_assets: float = 0.0     # 现金/资产比
    ratio_debt_to_assets: float = 0.0     # 负债/资产比 (资产负债率)
    ratio_receivable_to_revenue: float = 0.0  # 应收/营收比 (>0.5 可能有问题)
    ratio_inventory_to_assets: float = 0.0    # 存货/资产比

    # ========== 风险标记 ==========
    flag_cash_loan_anomaly: bool = False  # 存贷双高标记
    flag_goodwill_risk: bool = False      # 商誉爆雷风险标记
    flag_high_receivable: bool = False    # 应收账款过高标记

    # ========== 数据质量 ==========
    data_completeness: float = 0.0        # 数据完整度 [0, 1]
    missing_fields: tuple = field(default_factory=tuple)  # 缺失字段列表

    def to_features_dict(self) -> Dict[str, float]:
        """转换为 TRUTH ProbeInput.features 格式

        将所有数值字段展平为 {field_name: value} 字典
        """
        features = {}

        # 原始字段
        features["raw_fix_assets"] = self.raw_fix_assets
        features["raw_cip"] = self.raw_cip
        features["raw_total_assets"] = self.raw_total_assets
        features["raw_total_cur_assets"] = self.raw_total_cur_assets
        features["raw_total_nca"] = self.raw_total_nca
        features["raw_intang_assets"] = self.raw_intang_assets
        features["raw_goodwill"] = self.raw_goodwill
        features["raw_equity"] = self.raw_equity
        features["raw_money_cap"] = self.raw_money_cap
        features["raw_total_liab"] = self.raw_total_liab
        features["raw_accounts_receiv"] = self.raw_accounts_receiv
        features["raw_inventories"] = self.raw_inventories
        features["raw_lt_borr"] = self.raw_lt_borr
        features["raw_st_borr"] = self.raw_st_borr
        features["raw_total_revenue"] = self.raw_total_revenue

        # 衍生比率
        features["ratio_hard_asset"] = self.ratio_hard_asset
        features["ratio_intang_asset"] = self.ratio_intang_asset
        features["ratio_nca"] = self.ratio_nca
        features["ratio_working_capital"] = self.ratio_working_capital
        features["ratio_goodwill_to_equity"] = self.ratio_goodwill_to_equity
        features["ratio_cash_to_assets"] = self.ratio_cash_to_assets
        features["ratio_debt_to_assets"] = self.ratio_debt_to_assets
        features["ratio_receivable_to_revenue"] = self.ratio_receivable_to_revenue
        features["ratio_inventory_to_assets"] = self.ratio_inventory_to_assets

        # 风险标记 (转为 0/1)
        features["flag_cash_loan_anomaly"] = 1.0 if self.flag_cash_loan_anomaly else 0.0
        features["flag_goodwill_risk"] = 1.0 if self.flag_goodwill_risk else 0.0
        features["flag_high_receivable"] = 1.0 if self.flag_high_receivable else 0.0

        # 数据质量
        features["data_completeness"] = self.data_completeness

        return features


class FinancialContextProbe:
    """财务上下文探针

    从原始财务数据提取资产结构和风险检测字段。

    与其他 Probe 的区别：
        - 其他 Probe: 输入时间序列 List[float]，输出趋势/波动特征
        - 本 Probe: 输入单期财务快照 Dict[str, float]，输出结构性特征

    使用方式：
        probe = FinancialContextProbe()
        result = probe.compute(financial_data)

    Example:
        >>> probe = FinancialContextProbe()
        >>> data = {
        ...     "fix_assets": 1000000,
        ...     "total_assets": 5000000,
        ...     "goodwill": 500000,
        ...     "total_hldr_eqy_exc_min_int": 2000000,
        ... }
        >>> result = probe.compute(data)
        >>> print(result.ratio_hard_asset)
        0.2
        >>> print(result.ratio_goodwill_to_equity)
        0.25
    """

    # 探针元数据 (遵循 ProbeProtocol)
    name: str = "financial_context"
    description: str = "Extract financial structure features for β and δ_fraud factors"
    category: str = "context"  # 新类别

    # 字段映射：标准名 -> 可能的 CSV 列名 (原始报表字段)
    FIELD_ALIASES: Dict[str, tuple] = {
        "fix_assets": ("fix_assets", "fixed_assets", "ppe"),
        "cip": ("cip", "construction_in_progress"),
        "total_assets": ("total_assets", "assets"),
        "total_cur_assets": ("total_cur_assets", "current_assets"),
        "total_cur_liab": ("total_cur_liab", "current_liabilities"),
        "total_nca": ("total_nca", "non_current_assets"),
        "intang_assets": ("intang_assets", "intangible_assets"),
        "goodwill": ("goodwill",),
        "equity": ("total_hldr_eqy_exc_min_int", "equity", "shareholders_equity"),
        "money_cap": ("money_cap", "cash", "cash_and_equivalents"),
        "total_liab": ("total_liab", "total_liabilities"),
        "accounts_receiv": ("accounts_receiv", "receivables", "ar"),
        "inventories": ("inventories", "inventory"),
        "lt_borr": ("lt_borr", "long_term_debt"),
        "st_borr": ("st_borr", "short_term_debt"),
        "total_revenue": ("total_revenue", "revenue", "operating_revenue"),
    }

    # 财务指标比率字段映射 (直接使用，无需计算)
    # 这些来自 tushare fina_indicator 表
    RATIO_ALIASES: Dict[str, tuple] = {
        "ca_to_assets": ("ca_to_assets",),           # 流动资产/总资产
        "nca_to_assets": ("nca_to_assets",),         # 非流动资产/总资产
        "debt_to_assets": ("debt_to_assets",),       # 负债/总资产 (资产负债率)
        "debt_to_eqt": ("debt_to_eqt",),             # 负债/权益
        "eqt_to_debt": ("eqt_to_debt",),             # 权益/负债
        "tangible_asset": ("tangible_asset",),       # 有形资产
        "working_capital": ("working_capital",),     # 营运资本
        "fixed_assets_ratio": ("fixed_assets",),     # 固定资产 (可能是比率)
    }

    # β 因子必需字段
    BETA_REQUIRED_FIELDS = {"fix_assets", "total_assets"}

    # δ_fraud 因子必需字段
    FRAUD_REQUIRED_FIELDS = {"goodwill", "equity", "money_cap", "total_liab"}

    def __init__(self, strict_mode: bool = False):
        """
        Args:
            strict_mode: 严格模式下，缺少必需字段会抛出异常
        """
        self.strict_mode = strict_mode

    def compute(self, financial_data: Dict[str, Any], **kwargs) -> FinancialContextResult:
        """计算财务上下文特征

        Args:
            financial_data: 原始财务数据字典，键为字段名，值为数值
            **kwargs: 保留参数，兼容其他探针接口

        Returns:
            FinancialContextResult 结构化结果
        """
        # 提取原始值
        raw = self._extract_raw_values(financial_data)

        # 计算衍生比率
        ratios = self._calculate_ratios(raw)

        # 检测风险标记
        flags = self._detect_risk_flags(raw, ratios)

        # 计算数据完整度
        completeness, missing = self._calculate_completeness(raw)

        return FinancialContextResult(
            # 原始字段
            raw_fix_assets=raw.get("fix_assets", 0.0),
            raw_cip=raw.get("cip", 0.0),
            raw_total_assets=raw.get("total_assets", 0.0),
            raw_total_cur_assets=raw.get("total_cur_assets", 0.0),
            raw_total_nca=raw.get("total_nca", 0.0),
            raw_intang_assets=raw.get("intang_assets", 0.0),
            raw_goodwill=raw.get("goodwill", 0.0),
            raw_equity=raw.get("equity", 0.0),
            raw_money_cap=raw.get("money_cap", 0.0),
            raw_total_liab=raw.get("total_liab", 0.0),
            raw_accounts_receiv=raw.get("accounts_receiv", 0.0),
            raw_inventories=raw.get("inventories", 0.0),
            raw_lt_borr=raw.get("lt_borr", 0.0),
            raw_st_borr=raw.get("st_borr", 0.0),
            raw_total_revenue=raw.get("total_revenue", 0.0),
            # 衍生比率
            ratio_hard_asset=ratios.get("hard_asset", 0.0),
            ratio_intang_asset=ratios.get("intang_asset", 0.0),
            ratio_nca=ratios.get("nca", 0.0),
            ratio_working_capital=ratios.get("working_capital", 0.0),
            ratio_goodwill_to_equity=ratios.get("goodwill_to_equity", 0.0),
            ratio_cash_to_assets=ratios.get("cash_to_assets", 0.0),
            ratio_debt_to_assets=ratios.get("debt_to_assets", 0.0),
            ratio_receivable_to_revenue=ratios.get("receivable_to_revenue", 0.0),
            ratio_inventory_to_assets=ratios.get("inventory_to_assets", 0.0),
            # 风险标记
            flag_cash_loan_anomaly=flags.get("cash_loan_anomaly", False),
            flag_goodwill_risk=flags.get("goodwill_risk", False),
            flag_high_receivable=flags.get("high_receivable", False),
            # 数据质量
            data_completeness=completeness,
            missing_fields=tuple(missing),
        )

    def default(self) -> FinancialContextResult:
        """返回默认结果（数据不足时）"""
        return FinancialContextResult(
            data_completeness=0.0,
            missing_fields=tuple(self.FIELD_ALIASES.keys()),
        )

    def _extract_raw_values(self, data: Dict[str, Any]) -> Dict[str, float]:
        """从原始数据提取标准化字段值"""
        raw = {}

        # 提取原始报表字段
        for standard_name, aliases in self.FIELD_ALIASES.items():
            value = None
            for alias in aliases:
                if alias in data:
                    val = data[alias]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        value = float(val)
                        break

            if value is not None:
                raw[standard_name] = value

        # 提取财务指标比率 (直接可用)
        for standard_name, aliases in self.RATIO_ALIASES.items():
            if standard_name in raw:
                continue  # 已有数据，跳过
            for alias in aliases:
                if alias in data:
                    val = data[alias]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        raw[standard_name] = float(val)
                        break

        return raw

    def _calculate_ratios(self, raw: Dict[str, float]) -> Dict[str, float]:
        """计算衍生财务比率

        支持两种数据源:
        1. 原始报表数据 (fix_assets, total_assets 等) -> 计算比率
        2. 财务指标比率 (ca_to_assets, nca_to_assets 等) -> 直接使用

        注意: tushare 财务指标比率是百分比格式 (0-100)，需要转换为小数
        """
        ratios = {}
        total_assets = raw.get("total_assets", 0.0)
        equity = raw.get("equity", 0.0)
        revenue = raw.get("total_revenue", 0.0)

        # 避免除以零
        def safe_divide(num: float, denom: float) -> float:
            if denom == 0 or abs(denom) < 1e-10:
                return 0.0
            return num / denom

        # 百分比转小数 (tushare 比率是百分比格式)
        def pct_to_decimal(pct: float) -> float:
            if pct > 1.0:  # 是百分比格式
                return pct / 100.0
            return pct

        # ================================================================
        # β 因子比率 (优先使用直接比率，否则计算)
        # ================================================================

        # 硬资产比率
        fix_assets = raw.get("fix_assets", 0.0)
        cip = raw.get("cip", 0.0)
        if fix_assets > 0 and total_assets > 0:
            ratios["hard_asset"] = safe_divide(fix_assets + cip, total_assets)
        else:
            # 无原始数据，使用 nca_to_assets 近似
            nca_to_assets = pct_to_decimal(raw.get("nca_to_assets", 0.0))
            ratios["hard_asset"] = nca_to_assets * 0.6  # 硬资产约占非流动资产60%

        # 无形资产比率
        intang = raw.get("intang_assets", 0.0)
        if intang > 0 and total_assets > 0:
            ratios["intang_asset"] = safe_divide(intang, total_assets)
        else:
            # 近似: 非流动资产的一小部分
            nca_to_assets = pct_to_decimal(raw.get("nca_to_assets", 0.0))
            ratios["intang_asset"] = nca_to_assets * 0.1  # 假设无形资产占非流动10%

        # 非流动资产比率
        nca = raw.get("total_nca", 0.0)
        if nca > 0 and total_assets > 0:
            ratios["nca"] = safe_divide(nca, total_assets)
        else:
            # 直接使用财务指标比率 (转换为小数)
            ratios["nca"] = pct_to_decimal(raw.get("nca_to_assets", 0.0))

        # 营运资本比率
        working_capital = raw.get("working_capital", 0.0)
        cur_assets = raw.get("total_cur_assets", 0.0)
        st_borr = raw.get("st_borr", 0.0)
        cur_liab = raw.get("total_cur_liab", st_borr)

        if working_capital != 0 and total_assets > 0:
            ratios["working_capital"] = safe_divide(working_capital, total_assets)
        elif cur_assets > 0 and total_assets > 0:
            net_working_capital = cur_assets - cur_liab
            ratios["working_capital"] = safe_divide(net_working_capital, total_assets)
        else:
            # 使用流动资产比率近似
            ca_to_assets = pct_to_decimal(raw.get("ca_to_assets", 0.0))
            debt_to_assets = pct_to_decimal(raw.get("debt_to_assets", 0.0))
            # 营运资本 ≈ 流动资产 - 流动负债 ≈ ca_to_assets - debt_to_assets * 0.4
            ratios["working_capital"] = max(0, ca_to_assets - debt_to_assets * 0.4)

        # ================================================================
        # δ_fraud 因子比率
        # ================================================================

        goodwill = raw.get("goodwill", 0.0)
        ratios["goodwill_to_equity"] = safe_divide(goodwill, equity)

        money_cap = raw.get("money_cap", 0.0)
        ratios["cash_to_assets"] = safe_divide(money_cap, total_assets)

        total_liab = raw.get("total_liab", 0.0)
        if total_liab > 0 and total_assets > 0:
            ratios["debt_to_assets"] = safe_divide(total_liab, total_assets)
        else:
            ratios["debt_to_assets"] = pct_to_decimal(raw.get("debt_to_assets", 0.0))

        accounts_receiv = raw.get("accounts_receiv", 0.0)
        ratios["receivable_to_revenue"] = safe_divide(accounts_receiv, revenue)

        inventories = raw.get("inventories", 0.0)
        ratios["inventory_to_assets"] = safe_divide(inventories, total_assets)

        return ratios

    def _detect_risk_flags(self, raw: Dict[str, float], ratios: Dict[str, float]) -> Dict[str, bool]:
        """检测风险标记"""
        flags = {}

        # 存贷双高检测: 货币资金 > 30% 且 负债 > 60%
        cash_ratio = ratios.get("cash_to_assets", 0.0)
        debt_ratio = ratios.get("debt_to_assets", 0.0)
        flags["cash_loan_anomaly"] = cash_ratio > 0.30 and debt_ratio > 0.60

        # 商誉爆雷风险: 商誉/权益 > 40%
        goodwill_ratio = ratios.get("goodwill_to_equity", 0.0)
        flags["goodwill_risk"] = goodwill_ratio > 0.40

        # 应收账款过高: 应收/营收 > 50%
        receivable_ratio = ratios.get("receivable_to_revenue", 0.0)
        flags["high_receivable"] = receivable_ratio > 0.50

        return flags

    def _calculate_completeness(self, raw: Dict[str, float]) -> tuple:
        """计算数据完整度"""
        all_fields = set(self.FIELD_ALIASES.keys())
        present_fields = set(raw.keys())
        missing_fields = all_fields - present_fields

        completeness = len(present_fields) / len(all_fields) if all_fields else 0.0

        return completeness, list(missing_fields)


# ============================================================================
# 便捷工厂函数
# ============================================================================

def create_financial_context_probe(strict_mode: bool = False) -> FinancialContextProbe:
    """创建财务上下文探针实例"""
    return FinancialContextProbe(strict_mode=strict_mode)


def compute_financial_context(financial_data: Dict[str, Any]) -> FinancialContextResult:
    """便捷函数：直接计算财务上下文"""
    probe = FinancialContextProbe()
    return probe.compute(financial_data)


__all__ = [
    "FinancialContextResult",
    "FinancialContextProbe",
    "create_financial_context_probe",
    "compute_financial_context",
]
