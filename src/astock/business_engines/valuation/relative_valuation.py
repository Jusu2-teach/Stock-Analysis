"""
相对估值模型
==============

提供多维度的相对估值分析：PE、PB、PS、PEG等。

核心思想:
- 行业比较：与同行业公司对比
- 历史比较：与自身历史估值对比
- 市场比较：与市场整体估值对比

常用估值指标:
- PE (Price/Earnings): 市盈率
- PB (Price/Book): 市净率
- PS (Price/Sales): 市销率
- PEG (PE/Growth): PE相对盈利增长率
- EV/EBITDA: 企业价值/EBITDA

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class RelativeValuationResult:
    """相对估值结果

    Attributes:
        pe: 市盈率
        pb: 市净率
        ps: 市销率
        peg: PEG比率 (PE/增长率)
        industry_pe: 行业平均PE
        industry_pb: 行业平均PB
        pe_premium: PE溢价率 (相对行业)
        pb_premium: PB溢价率 (相对行业)
        peg_rating: PEG评级 (<1优秀, 1-2合理, >2高估)
        recommendation: 综合推荐
        warnings: 警告信息
    """
    pe: float
    pb: float
    ps: float
    peg: Optional[float] = None
    industry_pe: Optional[float] = None
    industry_pb: Optional[float] = None
    pe_premium: Optional[float] = None
    pb_premium: Optional[float] = None
    peg_rating: Optional[str] = None
    recommendation: str = "HOLD"
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "pe": self.pe,
            "pb": self.pb,
            "ps": self.ps,
            "peg": self.peg,
            "industry_pe": self.industry_pe,
            "industry_pb": self.industry_pb,
            "pe_premium": self.pe_premium,
            "pb_premium": self.pb_premium,
            "peg_rating": self.peg_rating,
            "recommendation": self.recommendation,
            "warnings": self.warnings,
        }


class RelativeValuationModel:
    """相对估值模型

    提供多维度相对估值分析。

    PEG评级标准:
        - PEG < 1: 优秀 (增长相对估值低)
        - PEG 1-2: 合理
        - PEG > 2: 高估 (增长不足以支撑估值)

    Examples:
        >>> model = RelativeValuationModel()
        >>> result = model.analyze(
        ...     market_cap=100_000_000_000,  # 1000亿市值
        ...     net_profit=10_000_000_000,   # 100亿净利润
        ...     book_value=50_000_000_000,   # 500亿净资产
        ...     revenue=200_000_000_000,     # 2000亿营收
        ...     growth_rate=0.20,            # 20%增长率
        ...     industry_pe=15.0,            # 行业PE
        ...     industry_pb=2.0              # 行业PB
        ... )
        >>> print(f"PE: {result.pe:.1f}, PEG: {result.peg:.2f}")
        >>> print(f"推荐: {result.recommendation}")
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def analyze(
        self,
        market_cap: float,
        net_profit: float,
        book_value: float,
        revenue: float,
        growth_rate: Optional[float] = None,
        industry_pe: Optional[float] = None,
        industry_pb: Optional[float] = None,
    ) -> RelativeValuationResult:
        """相对估值分析

        Args:
            market_cap: 市值
            net_profit: 净利润（年化）
            book_value: 净资产（账面价值）
            revenue: 营业收入（年化）
            growth_rate: 预期增长率（年化，如0.20表示20%）
            industry_pe: 行业平均PE
            industry_pb: 行业平均PB

        Returns:
            RelativeValuationResult: 相对估值结果
        """
        warnings = []

        # 计算PE
        if net_profit > 0:
            pe = market_cap / net_profit
        else:
            pe = float('inf')
            warnings.append("净利润为负，PE无意义")

        # 计算PB
        if book_value > 0:
            pb = market_cap / book_value
        else:
            pb = float('inf')
            warnings.append("净资产为负，PB无意义")

        # 计算PS
        if revenue > 0:
            ps = market_cap / revenue
        else:
            ps = float('inf')
            warnings.append("营收为负，PS无意义")

        # 计算PEG
        peg = None
        peg_rating = None
        if growth_rate is not None and growth_rate > 0 and pe > 0 and pe != float('inf'):
            peg = pe / (growth_rate * 100)  # growth_rate转换为百分比
            peg_rating = self._rate_peg(peg)

        # 计算行业溢价/折价
        pe_premium = None
        if industry_pe is not None and industry_pe > 0 and pe > 0 and pe != float('inf'):
            pe_premium = (pe / industry_pe - 1) * 100

        pb_premium = None
        if industry_pb is not None and industry_pb > 0 and pb > 0 and pb != float('inf'):
            pb_premium = (pb / industry_pb - 1) * 100

        # 综合推荐
        recommendation = self._generate_recommendation(
            pe, pb, peg, pe_premium, pb_premium, peg_rating
        )

        return RelativeValuationResult(
            pe=pe,
            pb=pb,
            ps=ps,
            peg=peg,
            industry_pe=industry_pe,
            industry_pb=industry_pb,
            pe_premium=pe_premium,
            pb_premium=pb_premium,
            peg_rating=peg_rating,
            recommendation=recommendation,
            warnings=warnings,
        )

    def _rate_peg(self, peg: float) -> str:
        """PEG评级

        Args:
            peg: PEG比率

        Returns:
            rating: "EXCELLENT" | "GOOD" | "FAIR" | "EXPENSIVE"
        """
        if peg < 0.5:
            return "EXCELLENT"  # 优秀
        elif peg < 1.0:
            return "GOOD"       # 良好
        elif peg < 2.0:
            return "FAIR"       # 一般
        else:
            return "EXPENSIVE"  # 昂贵

    def _generate_recommendation(
        self,
        pe: float,
        pb: float,
        peg: Optional[float],
        pe_premium: Optional[float],
        pb_premium: Optional[float],
        peg_rating: Optional[str],
    ) -> str:
        """生成综合推荐

        Returns:
            "STRONG_BUY" | "BUY" | "HOLD" | "SELL" | "STRONG_SELL"
        """
        # 计分系统 (5个信号，每个-2到+2分)
        score = 0

        # 1. PEG评级
        if peg_rating == "EXCELLENT":
            score += 2
        elif peg_rating == "GOOD":
            score += 1
        elif peg_rating == "FAIR":
            score += 0
        elif peg_rating == "EXPENSIVE":
            score -= 1

        # 2. PE溢价
        if pe_premium is not None:
            if pe_premium < -30:
                score += 2  # 大幅折价
            elif pe_premium < -10:
                score += 1  # 轻微折价
            elif pe_premium > 50:
                score -= 2  # 大幅溢价
            elif pe_premium > 20:
                score -= 1  # 轻微溢价

        # 3. PB溢价
        if pb_premium is not None:
            if pb_premium < -30:
                score += 1  # 折价
            elif pb_premium > 50:
                score -= 1  # 溢价

        # 4. 绝对PE水平
        if pe > 0 and pe != float('inf'):
            if pe < 10:
                score += 1  # 低PE
            elif pe > 30:
                score -= 1  # 高PE

        # 5. 绝对PB水平
        if pb > 0 and pb != float('inf'):
            if pb < 1:
                score += 1  # 破净
            elif pb > 5:
                score -= 1  # 高PB

        # 根据得分生成推荐
        if score >= 4:
            return "STRONG_BUY"
        elif score >= 2:
            return "BUY"
        elif score >= -1:
            return "HOLD"
        elif score >= -3:
            return "SELL"
        else:
            return "STRONG_SELL"
