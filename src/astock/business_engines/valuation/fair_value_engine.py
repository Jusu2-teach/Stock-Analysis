"""
公允价值综合引擎
==================

融合多种估值方法，生成综合的估值判断和投资信号。

估值方法权重:
- DCF模型: 40% (内在价值法，理论最严谨)
- PE-Band: 30% (历史估值法，捕捉均值回归)
- 相对估值: 30% (市场比较法，反映市场情绪)

投资信号分级:
- STRONG_BUY: 多个估值方法一致认为低估
- BUY: 综合估值偏低
- HOLD: 估值合理
- SELL: 综合估值偏高
- STRONG_SELL: 多个估值方法一致认为高估

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from .dcf_model import DCFValuationModel, DCFParameters, DCFResult
from .pe_band import PEBandModel, PEBandResult, ValuationZone
from .relative_valuation import RelativeValuationModel, RelativeValuationResult

logger = logging.getLogger(__name__)


class ValuationSignal(str, Enum):
    """估值信号"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    HOLD = "hold"
    SELL = "sell"
    STRONG_SELL = "strong_sell"

    @property
    def display_name(self) -> str:
        """中文显示名"""
        names = {
            "strong_buy": "强烈买入",
            "buy": "买入",
            "hold": "持有",
            "sell": "卖出",
            "strong_sell": "强烈卖出",
        }
        return names[self.value]

    @property
    def emoji(self) -> str:
        """表情符号"""
        emojis = {
            "strong_buy": "🚀",
            "buy": "📈",
            "hold": "➡️",
            "sell": "📉",
            "strong_sell": "⚠️",
        }
        return emojis[self.value]


@dataclass
class FairValueResult:
    """公允价值综合结果

    Attributes:
        signal: 投资信号
        confidence: 信号置信度 (0-100)
        fair_value_range: 公允价值区间 (low, high)
        current_price: 当前价格
        upside_potential: 上涨空间 (%)
        dcf_result: DCF估值结果
        pe_band_result: PE-Band结果
        relative_result: 相对估值结果
        consensus: 一致性描述
        key_insights: 关键洞察
        warnings: 警告信息
    """
    signal: ValuationSignal
    confidence: float
    fair_value_range: tuple[float, float]
    current_price: float
    upside_potential: float
    dcf_result: Optional[DCFResult] = None
    pe_band_result: Optional[PEBandResult] = None
    relative_result: Optional[RelativeValuationResult] = None
    consensus: str = ""
    key_insights: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "signal": self.signal.value,
            "signal_display": self.signal.display_name,
            "confidence": self.confidence,
            "fair_value_range": self.fair_value_range,
            "current_price": self.current_price,
            "upside_potential": self.upside_potential,
            "dcf": self.dcf_result.to_dict() if self.dcf_result else None,
            "pe_band": self.pe_band_result.to_dict() if self.pe_band_result else None,
            "relative": self.relative_result.to_dict() if self.relative_result else None,
            "consensus": self.consensus,
            "key_insights": self.key_insights,
            "warnings": self.warnings,
        }


class FairValueEngine:
    """公允价值综合引擎

    融合多种估值方法，生成综合判断。

    核心特性:
    - 多方法融合：DCF + PE-Band + 相对估值
    - 权重分配：可自定义各方法权重
    - 一致性检验：检查各方法结论是否一致
    - 置信度评估：根据一致性生成置信度

    Examples:
        >>> engine = FairValueEngine()
        >>> result = engine.evaluate(
        ...     # DCF参数
        ...     base_fcf=1_000_000_000,
        ...     growth_rates=[0.20, 0.15, 0.10, 0.08, 0.05],
        ...     shares_outstanding=100_000_000,
        ...     net_debt=500_000_000,
        ...     # PE-Band参数
        ...     current_pe=15.0,
        ...     historical_pes=[10, 12, 14, 16, 18, 20, 22],
        ...     # 相对估值参数
        ...     market_cap=10_000_000_000,
        ...     net_profit=666_666_667,
        ...     book_value=5_000_000_000,
        ...     revenue=20_000_000_000,
        ...     growth_rate=0.15,
        ...     industry_pe=18.0,
        ...     industry_pb=2.5,
        ...     # 当前价格
        ...     current_price=100.0
        ... )
        >>> print(f"信号: {result.signal.display_name}")
        >>> print(f"置信度: {result.confidence:.1f}%")
        >>> print(f"公允价值区间: {result.fair_value_range}")
    """

    def __init__(
        self,
        dcf_weight: float = 0.4,
        pe_band_weight: float = 0.3,
        relative_weight: float = 0.3,
    ):
        """初始化

        Args:
            dcf_weight: DCF模型权重
            pe_band_weight: PE-Band权重
            relative_weight: 相对估值权重
        """
        # 权重归一化
        total_weight = dcf_weight + pe_band_weight + relative_weight
        self.dcf_weight = dcf_weight / total_weight
        self.pe_band_weight = pe_band_weight / total_weight
        self.relative_weight = relative_weight / total_weight

        # 初始化子模型
        self.dcf_model = DCFValuationModel()
        self.pe_band_model = PEBandModel()
        self.relative_model = RelativeValuationModel()

        self.logger = logging.getLogger(self.__class__.__name__)

    def evaluate(
        self,
        # DCF参数
        base_fcf: Optional[float] = None,
        growth_rates: Optional[List[float]] = None,
        shares_outstanding: Optional[float] = None,
        net_debt: Optional[float] = None,
        dcf_params: Optional[DCFParameters] = None,
        # PE-Band参数
        current_pe: Optional[float] = None,
        historical_pes: Optional[List[float]] = None,
        # 相对估值参数
        market_cap: Optional[float] = None,
        net_profit: Optional[float] = None,
        book_value: Optional[float] = None,
        revenue: Optional[float] = None,
        growth_rate: Optional[float] = None,
        industry_pe: Optional[float] = None,
        industry_pb: Optional[float] = None,
        # 当前价格
        current_price: float = 0.0,
    ) -> FairValueResult:
        """综合估值评估

        至少需要提供其中一种估值方法的参数。

        Returns:
            FairValueResult: 综合估值结果
        """
        warnings = []
        key_insights = []

        # === 1. DCF估值 ===
        dcf_result = None
        dcf_signal_score = 0

        if all([base_fcf, growth_rates, shares_outstanding]):
            try:
                dcf_result = self.dcf_model.value_company(
                    base_fcf=base_fcf,
                    growth_rates=growth_rates,
                    shares_outstanding=shares_outstanding,
                    net_debt=net_debt or 0.0,
                    current_price=current_price,
                    params=dcf_params,
                )

                # DCF信号评分
                if dcf_result.upside_potential is not None:
                    if dcf_result.upside_potential > 50:
                        dcf_signal_score = 2
                        key_insights.append(f"DCF模型显示50%+上涨空间")
                    elif dcf_result.upside_potential > 20:
                        dcf_signal_score = 1
                        key_insights.append(f"DCF模型显示20%+上涨空间")
                    elif dcf_result.upside_potential < -20:
                        dcf_signal_score = -2
                        warnings.append("DCF模型显示严重高估")
                    elif dcf_result.upside_potential < 0:
                        dcf_signal_score = -1
            except Exception as e:
                self.logger.warning(f"DCF估值失败: {e}")
                warnings.append(f"DCF估值失败: {str(e)}")

        # === 2. PE-Band估值 ===
        pe_band_result = None
        pe_band_signal_score = 0

        if current_pe and historical_pes:
            try:
                pe_band_result = self.pe_band_model.analyze(
                    current_pe=current_pe,
                    historical_pes=historical_pes,
                )

                if pe_band_result.is_valid:
                    # PE-Band信号评分
                    zone = pe_band_result.zone
                    if zone == ValuationZone.EXTREMELY_UNDERVALUED:
                        pe_band_signal_score = 2
                        key_insights.append("历史PE分位数极低")
                    elif zone == ValuationZone.UNDERVALUED:
                        pe_band_signal_score = 1
                        key_insights.append("历史PE分位数偏低")
                    elif zone == ValuationZone.OVERVALUED:
                        pe_band_signal_score = -1
                    elif zone == ValuationZone.EXTREMELY_OVERVALUED:
                        pe_band_signal_score = -2
                        warnings.append("历史PE分位数过高")

                warnings.extend(pe_band_result.warnings)
            except Exception as e:
                self.logger.warning(f"PE-Band分析失败: {e}")
                warnings.append(f"PE-Band分析失败: {str(e)}")

        # === 3. 相对估值 ===
        relative_result = None
        relative_signal_score = 0

        if all([market_cap, net_profit, book_value, revenue]):
            try:
                relative_result = self.relative_model.analyze(
                    market_cap=market_cap,
                    net_profit=net_profit,
                    book_value=book_value,
                    revenue=revenue,
                    growth_rate=growth_rate,
                    industry_pe=industry_pe,
                    industry_pb=industry_pb,
                )

                # 相对估值信号评分
                if relative_result.recommendation == "STRONG_BUY":
                    relative_signal_score = 2
                elif relative_result.recommendation == "BUY":
                    relative_signal_score = 1
                elif relative_result.recommendation == "SELL":
                    relative_signal_score = -1
                elif relative_result.recommendation == "STRONG_SELL":
                    relative_signal_score = -2

                if relative_result.peg and relative_result.peg < 1:
                    key_insights.append(f"PEG={relative_result.peg:.2f}<1，增长性价比高")

                warnings.extend(relative_result.warnings)
            except Exception as e:
                self.logger.warning(f"相对估值分析失败: {e}")
                warnings.append(f"相对估值分析失败: {str(e)}")

        # === 4. 综合评分 ===
        total_score = (
            dcf_signal_score * self.dcf_weight
            + pe_band_signal_score * self.pe_band_weight
            + relative_signal_score * self.relative_weight
        )

        # 转换为信号
        signal = self._score_to_signal(total_score)

        # === 5. 计算置信度 ===
        confidence = self._calculate_confidence(
            dcf_signal_score,
            pe_band_signal_score,
            relative_signal_score,
        )

        # === 6. 计算公允价值区间 ===
        fair_value_range = self._calculate_fair_value_range(
            dcf_result, pe_band_result, current_price
        )

        # === 7. 计算综合上涨空间 ===
        mid_fair_value = (fair_value_range[0] + fair_value_range[1]) / 2
        upside_potential = (mid_fair_value / current_price - 1) * 100 if current_price > 0 else 0.0

        # === 8. 生成一致性描述 ===
        consensus = self._generate_consensus(
            dcf_signal_score, pe_band_signal_score, relative_signal_score
        )

        return FairValueResult(
            signal=signal,
            confidence=confidence,
            fair_value_range=fair_value_range,
            current_price=current_price,
            upside_potential=upside_potential,
            dcf_result=dcf_result,
            pe_band_result=pe_band_result,
            relative_result=relative_result,
            consensus=consensus,
            key_insights=key_insights,
            warnings=warnings,
        )

    def _score_to_signal(self, score: float) -> ValuationSignal:
        """评分转换为信号"""
        if score >= 1.5:
            return ValuationSignal.STRONG_BUY
        elif score >= 0.5:
            return ValuationSignal.BUY
        elif score >= -0.5:
            return ValuationSignal.HOLD
        elif score >= -1.5:
            return ValuationSignal.SELL
        else:
            return ValuationSignal.STRONG_SELL

    def _calculate_confidence(
        self,
        dcf_score: float,
        pe_band_score: float,
        relative_score: float,
    ) -> float:
        """计算置信度 (0-100)

        置信度取决于各方法的一致性
        """
        scores = [dcf_score, pe_band_score, relative_score]
        valid_scores = [s for s in scores if s != 0]

        if not valid_scores:
            return 50.0  # 无有效信号，置信度50%

        # 计算标准差（一致性指标）
        mean_score = sum(valid_scores) / len(valid_scores)
        variance = sum((s - mean_score) ** 2 for s in valid_scores) / len(valid_scores)
        std_dev = variance ** 0.5

        # 标准差越小，一致性越高，置信度越高
        # 标准差范围 [0, 2]，映射到置信度 [100, 50]
        confidence = max(50, 100 - std_dev * 25)

        return confidence

    def _calculate_fair_value_range(
        self,
        dcf_result: Optional[DCFResult],
        pe_band_result: Optional[PEBandResult],
        current_price: float,
    ) -> tuple[float, float]:
        """计算公允价值区间

        综合DCF和PE-Band的估值结果
        """
        estimates = []

        if dcf_result and dcf_result.fair_value_per_share > 0:
            estimates.append(dcf_result.fair_value_per_share)

        if pe_band_result and pe_band_result.is_valid:
            # 使用PE中位数作为合理估值
            # 假设当前EPS = current_price / current_pe
            if pe_band_result.current_pe > 0:
                eps = current_price / pe_band_result.current_pe
                pe_fair_value = eps * pe_band_result.historical_median
                estimates.append(pe_fair_value)

        if not estimates:
            # 如果无估值数据，使用当前价格±20%作为区间
            return (current_price * 0.8, current_price * 1.2)

        # 使用所有估值的±10%作为区间
        avg_estimate = sum(estimates) / len(estimates)
        return (avg_estimate * 0.9, avg_estimate * 1.1)

    def _generate_consensus(
        self,
        dcf_score: float,
        pe_band_score: float,
        relative_score: float,
    ) -> str:
        """生成一致性描述"""
        valid_scores = [s for s in [dcf_score, pe_band_score, relative_score] if s != 0]

        if not valid_scores:
            return "缺少估值数据"

        # 判断一致性
        all_positive = all(s > 0 for s in valid_scores)
        all_negative = all(s < 0 for s in valid_scores)

        if all_positive:
            return "多种估值方法一致认为低估"
        elif all_negative:
            return "多种估值方法一致认为高估"
        else:
            return "估值方法存在分歧，建议谨慎"
