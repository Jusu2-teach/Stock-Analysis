"""
估值引擎 - Orchestrator注册
=============================

将估值模块注册到Orchestrator系统。

提供的方法:
- evaluate_dcf_valuation: DCF现金流折现估值
- evaluate_pe_band: PE-Band历史估值带分析
- evaluate_relative_valuation: 相对估值分析
- evaluate_fair_value: 综合公允价值评估

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from orchestrator.decorators.register import register_method

from .dcf_model import DCFValuationModel, DCFParameters
from .pe_band import PEBandModel
from .relative_valuation import RelativeValuationModel
from .fair_value_engine import FairValueEngine

logger = logging.getLogger(__name__)


@register_method(
    component_type="business_engine",
    engine_type="valuation",
    engine_name="evaluate_dcf_valuation",
    description="DCF现金流折现估值"
)
def evaluate_dcf_valuation(
    base_fcf: float,
    growth_rates: List[float],
    shares_outstanding: float,
    net_debt: float = 0.0,
    current_price: Optional[float] = None,
    risk_free_rate: float = 0.03,
    market_return: float = 0.10,
    beta: float = 1.0,
    debt_ratio: float = 0.3,
    tax_rate: float = 0.25,
    cost_of_debt: float = 0.05,
    terminal_growth_rate: float = 0.03,
    **kwargs
) -> pd.DataFrame:
    """DCF估值

    Args:
        base_fcf: 基准自由现金流
        growth_rates: 增长率列表（如 [0.20, 0.15, 0.10, 0.08, 0.05]）
        shares_outstanding: 总股本
        net_debt: 净债务
        current_price: 当前股价
        risk_free_rate: 无风险利率
        market_return: 市场回报率
        beta: 贝塔系数
        debt_ratio: 债务比率
        tax_rate: 税率
        cost_of_debt: 债务成本
        terminal_growth_rate: 永续增长率

    Returns:
        DataFrame包含估值结果
    """
    logger.info(f"执行DCF估值: base_fcf={base_fcf}, shares={shares_outstanding}")

    params = DCFParameters(
        risk_free_rate=risk_free_rate,
        market_return=market_return,
        beta=beta,
        debt_ratio=debt_ratio,
        tax_rate=tax_rate,
        cost_of_debt=cost_of_debt,
        high_growth_period=len(growth_rates),
        terminal_growth_rate=terminal_growth_rate,
    )

    model = DCFValuationModel()
    result = model.value_company(
        base_fcf=base_fcf,
        growth_rates=growth_rates,
        shares_outstanding=shares_outstanding,
        net_debt=net_debt,
        current_price=current_price,
        params=params,
    )

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(f"DCF估值完成: 公允价值={result.fair_value_per_share:.2f}")

    return df


@register_method(
    component_type="business_engine",
    engine_type="valuation",
    engine_name="evaluate_pe_band",
    description="PE-Band历史估值带分析"
)
def evaluate_pe_band(
    current_pe: float,
    historical_pes: List[float],
    filter_outliers: bool = True,
    min_years: int = 3,
    **kwargs
) -> pd.DataFrame:
    """PE-Band分析

    Args:
        current_pe: 当前PE
        historical_pes: 历史PE列表
        filter_outliers: 是否过滤离群值
        min_years: 最低历史年限

    Returns:
        DataFrame包含PE-Band分析结果
    """
    logger.info(f"执行PE-Band分析: current_pe={current_pe}, history_years={len(historical_pes)}")

    model = PEBandModel(min_years=min_years)
    result = model.analyze(
        current_pe=current_pe,
        historical_pes=historical_pes,
        filter_outliers=filter_outliers,
    )

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(f"PE-Band分析完成: 分位数={result.percentile:.1f}%, 区域={result.zone.display_name}")

    return df


@register_method(
    component_type="business_engine",
    engine_type="valuation",
    engine_name="evaluate_relative_valuation",
    description="相对估值分析"
)
def evaluate_relative_valuation(
    market_cap: float,
    net_profit: float,
    book_value: float,
    revenue: float,
    growth_rate: Optional[float] = None,
    industry_pe: Optional[float] = None,
    industry_pb: Optional[float] = None,
    **kwargs
) -> pd.DataFrame:
    """相对估值分析

    Args:
        market_cap: 市值
        net_profit: 净利润
        book_value: 净资产
        revenue: 营业收入
        growth_rate: 增长率
        industry_pe: 行业PE
        industry_pb: 行业PB

    Returns:
        DataFrame包含相对估值结果
    """
    logger.info(f"执行相对估值分析: market_cap={market_cap}, net_profit={net_profit}")

    model = RelativeValuationModel()
    result = model.analyze(
        market_cap=market_cap,
        net_profit=net_profit,
        book_value=book_value,
        revenue=revenue,
        growth_rate=growth_rate,
        industry_pe=industry_pe,
        industry_pb=industry_pb,
    )

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(f"相对估值分析完成: PE={result.pe:.1f}, 推荐={result.recommendation}")

    return df


@register_method(
    component_type="business_engine",
    engine_type="valuation",
    engine_name="evaluate_fair_value",
    description="综合公允价值评估"
)
def evaluate_fair_value(
    current_price: float,
    # DCF参数（可选）
    base_fcf: Optional[float] = None,
    growth_rates: Optional[List[float]] = None,
    shares_outstanding: Optional[float] = None,
    net_debt: Optional[float] = None,
    # PE-Band参数（可选）
    current_pe: Optional[float] = None,
    historical_pes: Optional[List[float]] = None,
    # 相对估值参数（可选）
    market_cap: Optional[float] = None,
    net_profit: Optional[float] = None,
    book_value: Optional[float] = None,
    revenue: Optional[float] = None,
    growth_rate: Optional[float] = None,
    industry_pe: Optional[float] = None,
    industry_pb: Optional[float] = None,
    # 权重配置（可选）
    dcf_weight: float = 0.4,
    pe_band_weight: float = 0.3,
    relative_weight: float = 0.3,
    **kwargs
) -> pd.DataFrame:
    """综合公允价值评估

    融合DCF、PE-Band、相对估值三种方法，生成综合判断。

    Args:
        current_price: 当前价格
        base_fcf: DCF基准现金流
        growth_rates: DCF增长率列表
        shares_outstanding: 总股本
        net_debt: 净债务
        current_pe: 当前PE
        historical_pes: 历史PE列表
        market_cap: 市值
        net_profit: 净利润
        book_value: 净资产
        revenue: 营业收入
        growth_rate: 增长率
        industry_pe: 行业PE
        industry_pb: 行业PB
        dcf_weight: DCF权重
        pe_band_weight: PE-Band权重
        relative_weight: 相对估值权重

    Returns:
        DataFrame包含综合估值结果
    """
    logger.info(f"执行综合估值: current_price={current_price}")

    engine = FairValueEngine(
        dcf_weight=dcf_weight,
        pe_band_weight=pe_band_weight,
        relative_weight=relative_weight,
    )

    result = engine.evaluate(
        # DCF参数
        base_fcf=base_fcf,
        growth_rates=growth_rates,
        shares_outstanding=shares_outstanding,
        net_debt=net_debt,
        # PE-Band参数
        current_pe=current_pe,
        historical_pes=historical_pes,
        # 相对估值参数
        market_cap=market_cap,
        net_profit=net_profit,
        book_value=book_value,
        revenue=revenue,
        growth_rate=growth_rate,
        industry_pe=industry_pe,
        industry_pb=industry_pb,
        # 当前价格
        current_price=current_price,
    )

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(
        f"综合估值完成: 信号={result.signal.display_name}, "
        f"置信度={result.confidence:.1f}%, 上涨空间={result.upside_potential:.1f}%"
    )

    return df
