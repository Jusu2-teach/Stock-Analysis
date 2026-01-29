"""
经济周期检测 (Economic Cycle Detector)
======================================

识别经济周期的四个阶段：
- 扩张期 (Expansion): 经济加速增长
- 繁荣期 (Peak): 增长见顶
- 收缩期 (Contraction): 经济减速
- 衰退期 (Trough): 触底反弹

方法论:
- 多指标综合判断（GDP、PMI、就业、信贷）
- 领先指标预警（PMI、利率曲线）
- 马尔可夫状态转移模型

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class CyclePhase(Enum):
    """经济周期阶段"""
    EXPANSION = "expansion"          # 扩张期：经济加速增长
    PEAK = "peak"                    # 繁荣期：增长见顶
    CONTRACTION = "contraction"      # 收缩期：经济减速
    TROUGH = "trough"                # 衰退期：触底反弹
    UNCERTAIN = "uncertain"          # 不确定：数据不足或混合信号

    @property
    def display_name(self) -> str:
        return {
            "expansion": "扩张期",
            "peak": "繁荣期",
            "contraction": "收缩期",
            "trough": "衰退期",
            "uncertain": "不确定",
        }[self.value]

    @property
    def investment_strategy(self) -> str:
        """推荐的投资策略"""
        return {
            "expansion": "进攻型：增配成长股、周期股",
            "peak": "平衡型：减仓周期股，增配防御股",
            "contraction": "防御型：配置公用事业、消费必需品",
            "trough": "逆向型：布局优质周期股，等待反转",
            "uncertain": "谨慎型：保持观望，分散配置",
        }[self.value]


@dataclass(frozen=True)
class CycleResult:
    """经济周期检测结果"""

    # 当前周期阶段
    current_phase: CyclePhase

    # 置信度
    confidence: float  # 0-1之间，表示判断的置信度

    # 周期持续时间（月数）
    duration_months: Optional[int] = None

    # 领先指标信号
    leading_signals: Dict[str, str] = field(default_factory=dict)

    # 同步指标信号
    coincident_signals: Dict[str, str] = field(default_factory=dict)

    # 滞后指标信号
    lagging_signals: Dict[str, str] = field(default_factory=dict)

    # 下一阶段概率（马尔可夫转移）
    transition_probabilities: Dict[str, float] = field(default_factory=dict)

    # 投资建议
    investment_strategy: Optional[str] = None

    # 风险提示
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'current_phase': self.current_phase.value,
            'current_phase_cn': self.current_phase.display_name,
            'confidence': self.confidence,
            'duration_months': self.duration_months,
            'leading_signals': self.leading_signals,
            'coincident_signals': self.coincident_signals,
            'lagging_signals': self.lagging_signals,
            'transition_probabilities': self.transition_probabilities,
            'investment_strategy': self.investment_strategy or self.current_phase.investment_strategy,
            'warnings': self.warnings,
        }


class EconomicCycleDetector:
    """经济周期检测器"""

    # 阈值配置
    GDP_EXPANSION = 6.0       # GDP扩张阈值
    GDP_CONTRACTION = 5.0     # GDP收缩阈值

    PMI_EXPANSION = 52.0      # PMI扩张阈值
    PMI_CONTRACTION = 48.0    # PMI收缩阈值

    YIELD_CURVE_INVERSION = -0.2  # 收益率曲线倒挂阈值

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def detect(
        self,
        macro_data: pd.DataFrame,
        reference_date: Optional[str] = None,
        lookback_months: int = 12,
    ) -> CycleResult:
        """检测经济周期

        Args:
            macro_data: DataFrame包含宏观数据，列名示例：
                - date: 日期
                - gdp_yoy: GDP同比增速
                - pmi: 制造业PMI
                - m2_yoy: M2同比增速
                - bond_yield_10y: 10年期国债收益率
                - bond_yield_1y: 1年期国债收益率
                - unemployment_rate: 失业率
            reference_date: 参考日期（默认使用最新数据）
            lookback_months: 回溯月数

        Returns:
            CycleResult
        """
        self.logger.info(f"检测经济周期: reference_date={reference_date}")

        if macro_data.empty:
            return self._create_uncertain_result("输入数据为空")

        # 获取回溯期数据
        if reference_date:
            data = macro_data[macro_data['date'] <= reference_date].tail(lookback_months)
        else:
            data = macro_data.tail(lookback_months)
            reference_date = str(data.iloc[-1]['date'])

        if data.empty:
            return self._create_uncertain_result("回溯期数据为空")

        warnings = []

        # 分析各类指标
        leading_signals = self._analyze_leading_indicators(data, warnings)
        coincident_signals = self._analyze_coincident_indicators(data, warnings)
        lagging_signals = self._analyze_lagging_indicators(data, warnings)

        # 综合判断周期阶段
        phase, confidence = self._determine_cycle_phase(
            leading_signals, coincident_signals, lagging_signals
        )

        # 计算周期持续时间
        duration_months = self._estimate_cycle_duration(data, phase)

        # 马尔可夫转移概率
        transition_probs = self._calculate_transition_probabilities(
            phase, leading_signals, coincident_signals
        )

        return CycleResult(
            current_phase=phase,
            confidence=confidence,
            duration_months=duration_months,
            leading_signals=leading_signals,
            coincident_signals=coincident_signals,
            lagging_signals=lagging_signals,
            transition_probabilities=transition_probs,
            warnings=warnings,
        )

    def _analyze_leading_indicators(
        self, data: pd.DataFrame, warnings: List[str]
    ) -> Dict[str, str]:
        """分析领先指标"""
        signals = {}

        # PMI（领先3-6个月）
        if 'pmi' in data.columns:
            pmi_series = data['pmi'].dropna()
            if len(pmi_series) >= 3:
                recent_pmi = pmi_series.iloc[-3:].values
                avg_pmi = np.mean(recent_pmi)

                if avg_pmi >= self.PMI_EXPANSION:
                    signals['pmi'] = "expansion"
                elif avg_pmi <= self.PMI_CONTRACTION:
                    signals['pmi'] = "contraction"
                else:
                    signals['pmi'] = "neutral"
        else:
            warnings.append("PMI数据缺失")

        # 收益率曲线（领先6-12个月）
        if 'bond_yield_10y' in data.columns and 'bond_yield_1y' in data.columns:
            yield_10y = data['bond_yield_10y'].iloc[-1]
            yield_1y = data['bond_yield_1y'].iloc[-1]

            if pd.notna(yield_10y) and pd.notna(yield_1y):
                slope = yield_10y - yield_1y

                if slope < self.YIELD_CURVE_INVERSION:
                    signals['yield_curve'] = "recession_warning"  # 倒挂预警衰退
                elif slope > 1.0:
                    signals['yield_curve'] = "expansion"
                else:
                    signals['yield_curve'] = "neutral"
        else:
            warnings.append("国债收益率数据缺失")

        # M2增速（领先指标）
        if 'm2_yoy' in data.columns:
            m2_series = data['m2_yoy'].dropna()
            if len(m2_series) >= 3:
                recent_m2 = m2_series.iloc[-3:].values
                m2_trend = recent_m2[-1] - recent_m2[0]

                if m2_trend > 1.0:
                    signals['m2_growth'] = "expansion"
                elif m2_trend < -1.0:
                    signals['m2_growth'] = "contraction"
                else:
                    signals['m2_growth'] = "neutral"

        return signals

    def _analyze_coincident_indicators(
        self, data: pd.DataFrame, warnings: List[str]
    ) -> Dict[str, str]:
        """分析同步指标"""
        signals = {}

        # GDP增速（同步指标）
        if 'gdp_yoy' in data.columns:
            gdp_series = data['gdp_yoy'].dropna()
            if len(gdp_series) >= 2:
                recent_gdp = gdp_series.iloc[-2:].values

                # 增速加速
                if recent_gdp[-1] > recent_gdp[-2] and recent_gdp[-1] >= self.GDP_EXPANSION:
                    signals['gdp'] = "expansion"
                # 增速减速
                elif recent_gdp[-1] < recent_gdp[-2] and recent_gdp[-1] <= self.GDP_CONTRACTION:
                    signals['gdp'] = "contraction"
                else:
                    signals['gdp'] = "neutral"
        else:
            warnings.append("GDP数据缺失")

        # 工业增加值（同步指标）
        if 'industrial_production_yoy' in data.columns:
            ip_series = data['industrial_production_yoy'].dropna()
            if len(ip_series) >= 2:
                recent_ip = ip_series.iloc[-2:].values

                if recent_ip[-1] > recent_ip[-2]:
                    signals['industrial_production'] = "expansion"
                elif recent_ip[-1] < recent_ip[-2]:
                    signals['industrial_production'] = "contraction"
                else:
                    signals['industrial_production'] = "neutral"

        return signals

    def _analyze_lagging_indicators(
        self, data: pd.DataFrame, warnings: List[str]
    ) -> Dict[str, str]:
        """分析滞后指标"""
        signals = {}

        # 失业率（滞后3-6个月）
        if 'unemployment_rate' in data.columns:
            unemp_series = data['unemployment_rate'].dropna()
            if len(unemp_series) >= 3:
                recent_unemp = unemp_series.iloc[-3:].values
                unemp_trend = recent_unemp[-1] - recent_unemp[0]

                if unemp_trend < -0.2:
                    signals['unemployment'] = "expansion"  # 失业率下降=扩张
                elif unemp_trend > 0.2:
                    signals['unemployment'] = "contraction"  # 失业率上升=收缩
                else:
                    signals['unemployment'] = "neutral"

        # CPI（滞后指标）
        if 'cpi_yoy' in data.columns:
            cpi_series = data['cpi_yoy'].dropna()
            if len(cpi_series) >= 3:
                recent_cpi = cpi_series.iloc[-3:].values
                avg_cpi = np.mean(recent_cpi)

                if avg_cpi > 3.0:
                    signals['inflation'] = "high"
                elif avg_cpi < 1.0:
                    signals['inflation'] = "low"
                else:
                    signals['inflation'] = "moderate"

        return signals

    def _determine_cycle_phase(
        self,
        leading: Dict[str, str],
        coincident: Dict[str, str],
        lagging: Dict[str, str],
    ) -> Tuple[CyclePhase, float]:
        """综合判断周期阶段

        Returns:
            (CyclePhase, confidence)
        """
        # 信号计分
        expansion_score = 0
        contraction_score = 0
        total_signals = 0

        # 领先指标（权重更高）
        for signal in leading.values():
            total_signals += 1
            if signal == "expansion":
                expansion_score += 2
            elif signal == "contraction" or signal == "recession_warning":
                contraction_score += 2

        # 同步指标
        for signal in coincident.values():
            total_signals += 1
            if signal == "expansion":
                expansion_score += 1.5
            elif signal == "contraction":
                contraction_score += 1.5

        # 滞后指标（权重最低）
        for signal in lagging.values():
            total_signals += 1
            if signal == "expansion":
                expansion_score += 1
            elif signal == "contraction":
                contraction_score += 1

        if total_signals == 0:
            return CyclePhase.UNCERTAIN, 0.0

        # 判断阶段
        max_score = max(expansion_score, contraction_score)
        confidence = max_score / (expansion_score + contraction_score) if (expansion_score + contraction_score) > 0 else 0.5

        if expansion_score > contraction_score * 1.5:
            return CyclePhase.EXPANSION, min(confidence, 0.95)
        elif contraction_score > expansion_score * 1.5:
            return CyclePhase.CONTRACTION, min(confidence, 0.95)
        elif expansion_score > contraction_score:
            # 扩张但信号不强，可能是见顶
            if leading.get('yield_curve') == 'recession_warning':
                return CyclePhase.PEAK, min(confidence * 0.8, 0.85)
            return CyclePhase.EXPANSION, min(confidence * 0.7, 0.75)
        elif contraction_score > expansion_score:
            # 收缩但信号不强，可能是触底
            if any(s == "expansion" for s in leading.values()):
                return CyclePhase.TROUGH, min(confidence * 0.8, 0.85)
            return CyclePhase.CONTRACTION, min(confidence * 0.7, 0.75)
        else:
            return CyclePhase.UNCERTAIN, 0.5

    def _estimate_cycle_duration(
        self, data: pd.DataFrame, phase: CyclePhase
    ) -> Optional[int]:
        """估算周期持续时间"""
        # 简化版：返回当前可用数据的长度
        return len(data)

    def _calculate_transition_probabilities(
        self,
        current_phase: CyclePhase,
        leading: Dict[str, str],
        coincident: Dict[str, str],
    ) -> Dict[str, float]:
        """计算马尔可夫转移概率"""
        # 简化版马尔可夫模型
        # 基于领先指标和同步指标预测下一阶段

        if current_phase == CyclePhase.EXPANSION:
            # 扩张期 → 繁荣期/继续扩张
            if leading.get('yield_curve') == 'recession_warning':
                return {'peak': 0.6, 'expansion': 0.3, 'contraction': 0.1}
            else:
                return {'expansion': 0.7, 'peak': 0.2, 'contraction': 0.1}

        elif current_phase == CyclePhase.PEAK:
            # 繁荣期 → 收缩期
            return {'contraction': 0.6, 'peak': 0.3, 'expansion': 0.1}

        elif current_phase == CyclePhase.CONTRACTION:
            # 收缩期 → 衰退期/继续收缩
            expansion_signals = sum(1 for s in leading.values() if s == "expansion")
            if expansion_signals >= 2:
                return {'trough': 0.5, 'contraction': 0.3, 'expansion': 0.2}
            else:
                return {'contraction': 0.6, 'trough': 0.3, 'expansion': 0.1}

        elif current_phase == CyclePhase.TROUGH:
            # 衰退期 → 扩张期
            return {'expansion': 0.6, 'trough': 0.3, 'contraction': 0.1}

        else:
            # 不确定
            return {'uncertain': 1.0}

    def _create_uncertain_result(self, reason: str) -> CycleResult:
        """创建不确定结果"""
        return CycleResult(
            current_phase=CyclePhase.UNCERTAIN,
            confidence=0.0,
            warnings=[reason],
        )
