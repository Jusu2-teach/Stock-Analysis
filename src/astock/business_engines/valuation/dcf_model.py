"""
DCF现金流折现估值模型
========================

实现专业的DCF估值，包括：
1. 两阶段DCF模型（高增长期 + 永续增长期）
2. WACC加权平均资本成本计算
3. 终值计算（Gordon Growth Model）
4. 敏感性分析

理论基础:
    PV = Σ FCF_t / (1 + WACC)^t + Terminal Value / (1 + WACC)^n

    其中:
    - FCF_t: 第t年自由现金流
    - WACC: 加权平均资本成本
    - Terminal Value: 终值（永续增长模型）

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


@dataclass
class DCFParameters:
    """DCF模型参数

    Attributes:
        risk_free_rate: 无风险利率 (如10年期国债)
        market_return: 市场预期回报率
        beta: 公司贝塔系数
        debt_ratio: 债务比率 (债务/总资本)
        tax_rate: 企业所得税率
        cost_of_debt: 债务成本
        high_growth_period: 高增长期年数 (通常3-5年)
        terminal_growth_rate: 永续增长率 (通常2-3%)
    """
    risk_free_rate: float = 0.03          # 3% 无风险利率
    market_return: float = 0.10           # 10% 市场回报率
    beta: float = 1.0                     # 贝塔系数
    debt_ratio: float = 0.3               # 30% 债务比率
    tax_rate: float = 0.25                # 25% 所得税
    cost_of_debt: float = 0.05            # 5% 债务成本
    high_growth_period: int = 5           # 5年高增长期
    terminal_growth_rate: float = 0.03    # 3% 永续增长率

    def calculate_wacc(self) -> float:
        """计算WACC (加权平均资本成本)

        WACC = Ke * (E/V) + Kd * (D/V) * (1 - T)

        其中:
        - Ke: 股权成本 (CAPM: Rf + β(Rm - Rf))
        - Kd: 债务成本
        - E/V: 股权占比
        - D/V: 债务占比
        - T: 税率
        """
        # 计算股权成本 (CAPM)
        cost_of_equity = self.risk_free_rate + self.beta * (
            self.market_return - self.risk_free_rate
        )

        # 计算WACC
        equity_ratio = 1 - self.debt_ratio
        wacc = (
            cost_of_equity * equity_ratio
            + self.cost_of_debt * self.debt_ratio * (1 - self.tax_rate)
        )

        return wacc


@dataclass
class DCFResult:
    """DCF估值结果

    Attributes:
        enterprise_value: 企业价值
        equity_value: 股权价值 (企业价值 - 净债务)
        fair_value_per_share: 每股公允价值
        current_price: 当前股价
        upside_potential: 上涨空间 (%)
        wacc: WACC
        terminal_value: 终值
        pv_cash_flows: 现金流现值列表
        sensitivity: 敏感性分析结果
    """
    enterprise_value: float
    equity_value: float
    fair_value_per_share: float
    current_price: Optional[float] = None
    upside_potential: Optional[float] = None
    wacc: float = 0.0
    terminal_value: float = 0.0
    pv_cash_flows: List[float] = field(default_factory=list)
    sensitivity: Dict[str, Dict[float, float]] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "enterprise_value": self.enterprise_value,
            "equity_value": self.equity_value,
            "fair_value_per_share": self.fair_value_per_share,
            "current_price": self.current_price,
            "upside_potential": self.upside_potential,
            "wacc": self.wacc,
            "terminal_value": self.terminal_value,
            "pv_cash_flows": self.pv_cash_flows,
        }


class DCFValuationModel:
    """DCF估值模型

    实现两阶段DCF模型：
    1. 第一阶段：预测高增长期的自由现金流
    2. 第二阶段：计算永续增长期的终值
    3. 折现所有现金流到当前

    Examples:
        >>> model = DCFValuationModel()
        >>> params = DCFParameters(beta=1.2, debt_ratio=0.4)
        >>> result = model.value_company(
        ...     base_fcf=100_000_000,
        ...     growth_rates=[0.20, 0.15, 0.10, 0.08, 0.05],
        ...     shares_outstanding=100_000_000,
        ...     net_debt=50_000_000,
        ...     current_price=10.0,
        ...     params=params
        ... )
        >>> print(f"公允价值: {result.fair_value_per_share:.2f}")
        >>> print(f"上涨空间: {result.upside_potential:.1f}%")
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def value_company(
        self,
        base_fcf: float,
        growth_rates: List[float],
        shares_outstanding: float,
        net_debt: float = 0.0,
        current_price: Optional[float] = None,
        params: Optional[DCFParameters] = None,
    ) -> DCFResult:
        """计算公司估值

        Args:
            base_fcf: 基准自由现金流（通常使用最近一年的FCF）
            growth_rates: 增长率列表（高增长期每年的增长率）
            shares_outstanding: 总股本
            net_debt: 净债务（债务 - 现金）
            current_price: 当前股价（用于计算上涨空间）
            params: DCF参数

        Returns:
            DCFResult: 估值结果
        """
        params = params or DCFParameters()

        # 计算WACC
        wacc = params.calculate_wacc()
        self.logger.info(f"WACC: {wacc:.2%}")

        # 第一阶段：高增长期现金流
        pv_cash_flows, fcf_terminal_year = self._calculate_high_growth_phase(
            base_fcf, growth_rates, wacc
        )

        # 第二阶段：终值计算
        terminal_value = self._calculate_terminal_value(
            fcf_terminal_year, params.terminal_growth_rate, wacc
        )

        # 终值的现值
        terminal_year = len(growth_rates)
        pv_terminal_value = terminal_value / ((1 + wacc) ** terminal_year)

        # 企业价值 = 高增长期现金流现值 + 终值现值
        enterprise_value = sum(pv_cash_flows) + pv_terminal_value

        # 股权价值 = 企业价值 - 净债务
        equity_value = enterprise_value - net_debt

        # 每股公允价值
        fair_value_per_share = equity_value / shares_outstanding

        # 计算上涨空间
        upside_potential = None
        if current_price is not None and current_price > 0:
            upside_potential = (fair_value_per_share / current_price - 1) * 100

        # 敏感性分析
        sensitivity = self._sensitivity_analysis(
            base_fcf, growth_rates, shares_outstanding, net_debt, params
        )

        return DCFResult(
            enterprise_value=enterprise_value,
            equity_value=equity_value,
            fair_value_per_share=fair_value_per_share,
            current_price=current_price,
            upside_potential=upside_potential,
            wacc=wacc,
            terminal_value=terminal_value,
            pv_cash_flows=pv_cash_flows,
            sensitivity=sensitivity,
        )

    def _calculate_high_growth_phase(
        self,
        base_fcf: float,
        growth_rates: List[float],
        wacc: float,
    ) -> Tuple[List[float], float]:
        """计算高增长期现金流现值

        Returns:
            (pv_cash_flows, fcf_terminal_year)
        """
        pv_cash_flows = []
        fcf = base_fcf

        for year, growth_rate in enumerate(growth_rates, start=1):
            # 预测当年FCF
            fcf = fcf * (1 + growth_rate)

            # 折现到当前
            pv = fcf / ((1 + wacc) ** year)
            pv_cash_flows.append(pv)

        return pv_cash_flows, fcf

    def _calculate_terminal_value(
        self,
        fcf_terminal_year: float,
        terminal_growth_rate: float,
        wacc: float,
    ) -> float:
        """计算终值 (Gordon Growth Model)

        TV = FCF_n+1 / (WACC - g)
        其中 FCF_n+1 = FCF_n * (1 + g)
        """
        if wacc <= terminal_growth_rate:
            # WACC必须大于永续增长率
            self.logger.warning(
                f"WACC ({wacc:.2%}) <= 永续增长率 ({terminal_growth_rate:.2%}), "
                "调整永续增长率"
            )
            terminal_growth_rate = wacc * 0.5

        fcf_perpetuity = fcf_terminal_year * (1 + terminal_growth_rate)
        terminal_value = fcf_perpetuity / (wacc - terminal_growth_rate)

        return terminal_value

    def _sensitivity_analysis(
        self,
        base_fcf: float,
        growth_rates: List[float],
        shares_outstanding: float,
        net_debt: float,
        params: DCFParameters,
    ) -> Dict[str, Dict[float, float]]:
        """敏感性分析

        分析关键参数变化对估值的影响：
        - WACC: ±1%
        - 永续增长率: ±0.5%

        Returns:
            {
                "wacc": {0.08: 12.5, 0.09: 11.2, 0.10: 10.0, ...},
                "terminal_growth": {0.025: 9.8, 0.03: 10.0, 0.035: 10.2, ...}
            }
        """
        sensitivity = {"wacc": {}, "terminal_growth": {}}

        # WACC敏感性
        base_wacc = params.calculate_wacc()
        for delta in [-0.02, -0.01, 0.00, 0.01, 0.02]:
            temp_params = DCFParameters(**params.__dict__)
            # 调整贝塔以改变WACC
            temp_params.beta = params.beta + delta / (params.market_return - params.risk_free_rate)

            result = self.value_company(
                base_fcf, growth_rates, shares_outstanding, net_debt, None, temp_params
            )
            sensitivity["wacc"][base_wacc + delta] = result.fair_value_per_share

        # 永续增长率敏感性
        for delta in [-0.01, -0.005, 0.00, 0.005, 0.01]:
            temp_params = DCFParameters(**params.__dict__)
            temp_params.terminal_growth_rate = params.terminal_growth_rate + delta

            result = self.value_company(
                base_fcf, growth_rates, shares_outstanding, net_debt, None, temp_params
            )
            sensitivity["terminal_growth"][params.terminal_growth_rate + delta] = (
                result.fair_value_per_share
            )

        return sensitivity
