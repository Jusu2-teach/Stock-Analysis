"""
估值模块 (Valuation Module)
============================

提供专业的股票估值分析能力，补充基本面分析的价格维度。

核心组件:
- DCF模型: 现金流折现估值
- 相对估值: PE/PB/PS/PEG 多维度估值
- PE-Band: 历史分位数估值带
- 公允价值引擎: 综合多种估值方法

设计原则:
- 与基本面分析解耦：估值模块独立运行
- 数据驱动：基于历史数据统计，非主观判断
- 多方法融合：避免单一估值方法偏差

版本: 1.0.0
日期: 2026-01-17
"""

from .dcf_model import DCFValuationModel, DCFParameters, DCFResult
from .relative_valuation import RelativeValuationModel, RelativeValuationResult
from .pe_band import PEBandModel, PEBandResult, ValuationZone
from .fair_value_engine import FairValueEngine, FairValueResult, ValuationSignal
from .engine import (
    evaluate_dcf_valuation,
    evaluate_pe_band,
    evaluate_relative_valuation,
    evaluate_fair_value,
)

__all__ = [
    # DCF模型
    "DCFValuationModel",
    "DCFParameters",
    "DCFResult",

    # 相对估值
    "RelativeValuationModel",
    "RelativeValuationResult",

    # PE-Band
    "PEBandModel",
    "PEBandResult",
    "ValuationZone",

    # 综合引擎
    "FairValueEngine",
    "FairValueResult",
    "ValuationSignal",

    # 注册方法
    "evaluate_dcf_valuation",
    "evaluate_pe_band",
    "evaluate_relative_valuation",
    "evaluate_fair_value",
]
