"""
异常值检测服务
==============

使用策略模式实现多种异常值检测方法。
"""

import logging
import numpy as np
from abc import ABC, abstractmethod
from typing import List, Optional
from ..trend_models import OutlierDetectionResult, TrendWarning
from ..config import TrendAnalysisConfig

logger = logging.getLogger(__name__)


class OutlierDetector(ABC):
    """异常值检测器基类"""

    def __init__(self, config: TrendAnalysisConfig = None):
        """初始化检测器

        Args:
            config: 趋势分析配置
        """
        from ..config import get_default_config
        self.config = config or get_default_config()

    @property
    @abstractmethod
    def method_name(self) -> str:
        """检测方法名称"""
        pass

    @abstractmethod
    def _calculate_threshold(self, values: np.ndarray) -> float:
        """计算阈值"""
        pass

    @abstractmethod
    def _detect_outliers(
        self,
        values: np.ndarray,
        threshold: float
    ) -> np.ndarray:
        """检测异常值，返回布尔掩码"""
        pass

    def detect(self, values: List[float]) -> OutlierDetectionResult:
        """检测异常值

        Args:
            values: 数据列表

        Returns:
            异常值检测结果
        """
        values_array = np.asarray(values, dtype=float)

        # 计算阈值
        threshold = self._calculate_threshold(values_array)

        # 检测异常值
        outlier_mask = self._detect_outliers(values_array, threshold)

        # 收集异常值信息
        outlier_indices = np.where(outlier_mask)[0].tolist()
        outlier_values = values_array[outlier_mask].tolist()
        outlier_count = len(outlier_indices)
        has_outliers = outlier_count > 0

        # 清洗数据（用中位数替换）
        cleaned_values = values_array.copy()
        cleaning_applied = False
        cleaning_ratio = 0.0

        if has_outliers:
            median_val = float(np.median(values_array[~outlier_mask]))
            cleaned_values[outlier_mask] = median_val
            cleaning_applied = True
            cleaning_ratio = outlier_count / values_array.size

        # 评估污染程度
        if cleaning_ratio >= 0.4:
            data_contamination = "high"
            risk_level = "high"
        elif cleaning_ratio >= 0.2:
            data_contamination = "moderate"
            risk_level = "medium"
        else:
            data_contamination = "low"
            risk_level = "low"

        # 生成警告
        warnings = self._generate_warnings(
            has_outliers, outlier_count, data_contamination
        )

        return OutlierDetectionResult(
            method=self.method_name,
            threshold=float(threshold),
            has_outliers=has_outliers,
            indices=outlier_indices,
            values=outlier_values,
            cleaned_values=cleaned_values.tolist(),
            cleaning_ratio=cleaning_ratio,
            cleaning_applied=cleaning_applied,
            data_contamination=data_contamination,
            risk_level=risk_level,
            warnings=warnings,
        )

    def _generate_warnings(
        self,
        has_outliers: bool,
        outlier_count: int,
        contamination: str
    ) -> List[TrendWarning]:
        """生成警告信息"""
        warnings = []

        if contamination == "high":
            warnings.append(
                TrendWarning(
                    code="OUTLIER_HEAVY_CONTAMINATION",
                    level="warn",
                    message="存在多个异常值, 结果依赖中位数替换",
                    context={
                        "count": outlier_count,
                        "method": self.method_name
                    },
                )
            )
        elif has_outliers:
            warnings.append(
                TrendWarning(
                    code="OUTLIER_DETECTED",
                    level="info",
                    message="检测到异常值并使用中位数替换",
                    context={
                        "count": outlier_count,
                        "method": self.method_name
                    },
                )
            )

        return warnings


class MADOutlierDetector(OutlierDetector):
    """Modified Z-Score (MAD) 异常值检测器

    使用中位数绝对偏差（MAD）作为尺度估计，
    对极端值更鲁棒。
    """

    @property
    def method_name(self) -> str:
        return "mad"

    def _calculate_threshold(self, values: np.ndarray) -> float:
        """计算 MAD 阈值"""
        return self.config.mad_z_threshold

    def _detect_outliers(
        self,
        values: np.ndarray,
        threshold: float
    ) -> np.ndarray:
        """使用 Modified Z-Score 检测"""
        median = np.median(values)
        mad = np.median(np.abs(values - median))

        # 避免除以0
        if mad < 1e-10:
            return np.zeros(values.shape, dtype=bool)

        # 计算 Modified Z-Score
        modified_z_scores = (
            self.config.mad_normalizer * (values - median) / mad
        )

        return np.abs(modified_z_scores) > threshold


class ZScoreOutlierDetector(OutlierDetector):
    """标准 Z-Score 异常值检测器

    使用标准差作为尺度估计。
    """

    @property
    def method_name(self) -> str:
        return "z_score"

    def _calculate_threshold(self, values: np.ndarray) -> float:
        """计算 Z-Score 阈值"""
        return self.config.z_score_threshold

    def _detect_outliers(
        self,
        values: np.ndarray,
        threshold: float
    ) -> np.ndarray:
        """使用标准 Z-Score 检测"""
        mean = np.mean(values)
        std = np.std(values, ddof=1)

        # 避免除以0
        if std < 1e-10:
            return np.zeros(values.shape, dtype=bool)

        z_scores = (values - mean) / std
        return np.abs(z_scores) > threshold


class IQROutlierDetector(OutlierDetector):
    """IQR (Interquartile Range) 异常值检测器

    使用四分位距作为尺度估计，
    适合非正态分布。
    """

    @property
    def method_name(self) -> str:
        return "iqr"

    def _calculate_threshold(self, values: np.ndarray) -> float:
        """计算 IQR 倍数"""
        return self.config.iqr_multiplier

    def _detect_outliers(
        self,
        values: np.ndarray,
        threshold: float
    ) -> np.ndarray:
        """使用 IQR 检测"""
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1

        # 避免 IQR 为0
        if iqr < 1e-10:
            return np.zeros(values.shape, dtype=bool)

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        return (values < lower_bound) | (values > upper_bound)


class OutlierDetectorFactory:
    """异常值检测器工厂"""

    _detectors = {
        'mad': MADOutlierDetector,
        'z_score': ZScoreOutlierDetector,
        'iqr': IQROutlierDetector,
    }

    @classmethod
    def create(
        cls,
        method: str,
        config: TrendAnalysisConfig = None
    ) -> OutlierDetector:
        """创建检测器实例

        Args:
            method: 检测方法名称
            config: 配置对象

        Returns:
            检测器实例

        Raises:
            ValueError: 未知的检测方法
        """
        # 归一化方法名
        method_normalized = method.lower().strip()

        # 别名映射
        if method_normalized in ('zscore', 'z'):
            method_normalized = 'z_score'

        detector_class = cls._detectors.get(method_normalized)
        if detector_class is None:
            raise ValueError(
                f"未知的异常值检测方法: {method}. "
                f"支持的方法: {list(cls._detectors.keys())}"
            )

        return detector_class(config)

    @classmethod
    def get_available_methods(cls) -> List[str]:
        """获取所有可用的检测方法"""
        return list(cls._detectors.keys())
