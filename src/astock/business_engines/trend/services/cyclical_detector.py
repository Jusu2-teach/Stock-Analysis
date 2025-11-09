"""
周期性模式检测服务
================

使用评分器模式重构周期性检测逻辑，提高可维护性和可扩展性。
"""

import logging
import numpy as np
from abc import ABC, abstractmethod
from scipy import stats
from typing import List, Dict, Optional
from ..trend_models import CyclicalPatternResult, TrendWarning
from ..config import TrendAnalysisConfig, get_cyclical_thresholds

logger = logging.getLogger(__name__)


class CyclicalScorer(ABC):
    """周期性评分器基类"""

    def __init__(self, config: TrendAnalysisConfig = None):
        """初始化评分器

        Args:
            config: 趋势分析配置
        """
        from ..config import get_default_config
        self.config = config or get_default_config()

    @property
    @abstractmethod
    def name(self) -> str:
        """评分器名称"""
        pass

    @abstractmethod
    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """计算得分

        Args:
            values: 数据数组
            industry: 行业名称
            context: 上下文信息（用于共享计算结果）

        Returns:
            得分 [0.0, 1.0]
        """
        pass

    @staticmethod
    def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        """限制值在指定范围内"""
        return max(lower, min(upper, value))


class IndustryScorer(CyclicalScorer):
    """行业评分器 - 判断是否属于周期性行业"""

    @property
    def name(self) -> str:
        return "industry"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """判断是否为周期性行业"""
        if not industry:
            return 0.0

        # 判断是否在周期性行业列表中
        is_cyclical = self.config.is_cyclical_industry(industry)

        # 保存到上下文
        if context is not None:
            context['industry_cyclical'] = is_cyclical

        return 1.0 if is_cyclical else 0.0


class PeakToTroughScorer(CyclicalScorer):
    """峰谷比评分器 - 评估波动幅度"""

    @property
    def name(self) -> str:
        return "peak_to_trough"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """计算峰谷比得分"""
        # 获取阈值
        threshold = self._get_threshold(industry)

        # 计算峰谷比（使用绝对值）
        abs_values = np.abs(values)
        positive_abs = abs_values[abs_values > self.config.mean_near_zero_eps]

        if positive_abs.size == 0:
            ratio = 0.0
        else:
            min_abs = np.min(positive_abs)
            max_abs = np.max(abs_values)
            ratio = max_abs / min_abs if min_abs > 0 else float('inf')

        # 保存到上下文
        if context is not None:
            context['peak_to_trough_ratio'] = ratio
            context['peak_to_trough_threshold'] = threshold

        # 归一化得分（对数缩放）
        if ratio <= threshold:
            return 0.0

        ratio_excess = ratio / threshold
        saturation = max(self.config.peak_to_trough_saturation, 1.0001)
        score = np.log(ratio_excess) / np.log(saturation)

        return self._clamp(score)

    def _get_threshold(self, industry: str = None) -> float:
        """获取峰谷比阈值"""
        if industry:
            try:
                thresholds = get_cyclical_thresholds(industry)
                return thresholds["peak_to_trough_ratio"]
            except Exception as e:
                logger.debug(f"获取行业阈值失败: {e}")
        return 3.0  # 默认阈值


class TrendScorer(CyclicalScorer):
    """趋势性评分器 - R²越低，周期性越强"""

    @property
    def name(self) -> str:
        return "low_r_squared"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """计算趋势性得分（R²越低得分越高）"""
        # 获取阈值
        r_squared_max = self._get_threshold(industry)

        # 计算R²
        years = np.arange(values.size)
        try:
            _, _, r_value, _, _ = stats.linregress(years, values)
            r_squared = r_value ** 2
        except:
            r_squared = 0.0

        # 保存到上下文
        if context is not None:
            context['trend_r_squared'] = r_squared
            context['trend_r_squared_max'] = r_squared_max

        # 归一化得分（R²越低得分越高）
        if r_squared_max <= 0:
            return 0.0

        score = 1.0 - self._clamp(r_squared / r_squared_max)
        return self._clamp(score)

    def _get_threshold(self, industry: str = None) -> float:
        """获取R²最大值阈值"""
        if industry:
            try:
                thresholds = get_cyclical_thresholds(industry)
                return thresholds["trend_r_squared_max"]
            except Exception as e:
                logger.debug(f"获取行业阈值失败: {e}")
        return 0.7  # 默认阈值


class WavePatternScorer(CyclicalScorer):
    """波动模式评分器 - 检测"峰-谷-峰"模式"""

    @property
    def name(self) -> str:
        return "wave_pattern"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """检测波动模式"""
        # 计算一阶差分
        diffs = np.diff(values)

        # 检测方向变化
        sign_changes = np.diff(np.sign(diffs))
        direction_changes = np.sum(sign_changes != 0)

        # 有2次以上方向变化 → 有波动模式
        has_wave_pattern = direction_changes >= 2

        # 保存到上下文
        if context is not None:
            context['has_wave_pattern'] = has_wave_pattern
            context['direction_changes'] = int(direction_changes)

        return 1.0 if has_wave_pattern else 0.0


class CVScorer(CyclicalScorer):
    """CV（变异系数）评分器 - 评估波动程度"""

    @property
    def name(self) -> str:
        return "high_cv"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """计算CV得分"""
        # 获取阈值
        cv_threshold = self._get_threshold(industry)

        # 计算CV
        mean_val = np.mean(values)
        mean_abs = abs(mean_val)
        std_val = np.std(values, ddof=1)

        if mean_abs <= self.config.mean_near_zero_eps:
            cv = float('inf')
        else:
            cv = std_val / mean_abs

        # 保存到上下文
        if context is not None:
            context['cv'] = cv
            context['cv_threshold'] = cv_threshold

        # 归一化得分（对数缩放）
        if cv <= cv_threshold:
            return 0.0

        cv_excess = cv / cv_threshold
        saturation = max(self.config.cv_saturation, 1.0001)
        score = np.log(cv_excess) / np.log(saturation)

        return self._clamp(score)

    def _get_threshold(self, industry: str = None) -> float:
        """获取CV阈值"""
        if industry:
            try:
                thresholds = get_cyclical_thresholds(industry)
                return thresholds["cv_min"]
            except Exception as e:
                logger.debug(f"获取行业阈值失败: {e}")
        return 0.25  # 默认阈值


class MiddlePeakScorer(CyclicalScorer):
    """中间波峰评分器 - 检测中间年份是否有波峰"""

    @property
    def name(self) -> str:
        return "middle_peak"

    def score(
        self,
        values: np.ndarray,
        industry: str = None,
        context: Dict = None
    ) -> float:
        """检测中间波峰"""
        if len(values) < 5:
            return 0.0

        # 中间3年（索引1,2,3）vs 边缘2年（索引0,4）
        middle_values = values[1:4]
        edge_values = [values[0], values[4]]

        middle_max = np.max(middle_values)
        edge_max = np.max(edge_values)

        # 中间峰值比边缘高20%以上
        has_middle_peak = middle_max > edge_max * 1.2

        # 保存到上下文
        if context is not None:
            context['has_middle_peak'] = has_middle_peak

        return 1.0 if has_middle_peak else 0.0


class CyclicalPatternDetector:
    """周期性模式检测器

    使用评分器模式，将300行的复杂函数拆分为多个独立的评分器。
    """

    def __init__(self, config: TrendAnalysisConfig = None):
        """初始化检测器

        Args:
            config: 趋势分析配置
        """
        from ..config import get_default_config
        self.config = config or get_default_config()

        # 初始化所有评分器
        self.scorers = [
            IndustryScorer(self.config),
            PeakToTroughScorer(self.config),
            TrendScorer(self.config),
            WavePatternScorer(self.config),
            CVScorer(self.config),
            MiddlePeakScorer(self.config),
        ]

    def detect(
        self,
        values: List[float],
        industry: str = None
    ) -> CyclicalPatternResult:
        """检测周期性模式

        Args:
            values: 数据列表（至少5年）
            industry: 行业名称

        Returns:
            周期性检测结果
        """
        # 转换为数组并取最后5年
        values_array = np.asarray(values, dtype=float)
        if values_array.size > 5:
            values_array = values_array[-5:]

        if values_array.size < 5:
            raise ValueError(f"需要至少5期数据, 实际仅{values_array.size}期")

        # 上下文用于共享计算结果
        context = {}

        # 计算各维度得分
        scores = {}
        for scorer in self.scorers:
            score = scorer.score(values_array, industry, context)
            scores[scorer.name] = score

        # 计算加权置信度
        confidence = self._calculate_confidence(scores)

        # 排除高成长企业
        confidence = self._exclude_high_growth(
            values_array, context.get('trend_r_squared', 0.0), confidence
        )

        # 判断周期性
        is_cyclical = confidence >= 0.5

        # 判断当前阶段
        current_phase = self._detect_phase(values_array, context)

        # 生成置信度因子说明
        confidence_factors = self._build_confidence_factors(scores)

        # 生成警告
        warnings = self._generate_warnings(is_cyclical, confidence, current_phase)

        return CyclicalPatternResult(
            is_cyclical=bool(is_cyclical),
            peak_to_trough_ratio=float(context.get('peak_to_trough_ratio', 0.0)),
            has_middle_peak=bool(context.get('has_middle_peak', False)),
            has_wave_pattern=bool(context.get('has_wave_pattern', False)),
            trend_r_squared=float(context.get('trend_r_squared', 0.0)),
            cv=float(context.get('cv', 0.0)),
            current_phase=current_phase,
            industry_cyclical=bool(context.get('industry_cyclical', False)),
            cyclical_confidence=float(confidence),
            peak_to_trough_threshold=float(context.get('peak_to_trough_threshold', 3.0)),
            trend_r_squared_max=float(context.get('trend_r_squared_max', 0.7)),
            cv_threshold=float(context.get('cv_threshold', 0.25)),
            industry=industry if industry else "default",
            confidence_factors=confidence_factors,
            warnings=warnings,
        )

    def _calculate_confidence(self, scores: Dict[str, float]) -> float:
        """计算加权置信度"""
        weights = self.config.factor_weights

        total = sum(
            scores.get(key, 0.0) * weights.get(key, 0.0)
            for key in scores
        )

        return np.clip(total, 0.0, 1.0)

    def _exclude_high_growth(
        self,
        values: np.ndarray,
        r_squared: float,
        confidence: float
    ) -> float:
        """排除稳定高成长企业"""
        # 如果R²>0.85且斜率>0，说明是稳定高成长，非周期
        if r_squared > 0.85:
            try:
                years = np.arange(values.size)
                slope, _, _, _, _ = stats.linregress(years, values)
                if slope > 0:  # 稳定上升趋势
                    return 0.0  # 完全排除
            except:
                pass

        return confidence

    def _detect_phase(
        self,
        values: np.ndarray,
        context: Dict
    ) -> str:
        """判断当前所处阶段"""
        abs_values = np.abs(values)
        latest_value = values[-1]
        second_latest = values[-2]
        latest_abs = abs(latest_value)

        # 获取峰谷值
        max_abs = np.max(abs_values)
        positive_abs = abs_values[abs_values > self.config.mean_near_zero_eps]
        min_abs = np.min(positive_abs) if positive_abs.size > 0 else self.config.mean_near_zero_eps

        # 判断是否在波峰/波谷
        is_at_peak = max_abs > 0 and latest_abs >= max_abs * 0.9
        is_at_trough = latest_abs <= min_abs * 1.2

        if is_at_peak:
            return "peak"
        elif is_at_trough:
            return "trough"
        elif latest_value > second_latest:
            return "rising"
        elif latest_value < second_latest:
            return "falling"
        else:
            return "unknown"

    def _build_confidence_factors(self, scores: Dict[str, float]) -> List[str]:
        """构建置信度因子说明"""
        weights = self.config.factor_weights
        factors = []

        for name, score in scores.items():
            weight = weights.get(name, 0.0)
            factors.append(f"{name}={score:.2f}×{weight:.2f}")

        return factors

    def _generate_warnings(
        self,
        is_cyclical: bool,
        confidence: float,
        current_phase: str
    ) -> List[TrendWarning]:
        """生成警告信息"""
        warnings = []

        if is_cyclical:
            warnings.append(
                TrendWarning(
                    code="CYCICAL_PATTERN",
                    level="info",
                    message="识别到周期性特征，需结合行业阶段判断",
                    context={
                        "phase": current_phase,
                        "confidence": float(confidence),
                    },
                )
            )

        return warnings
