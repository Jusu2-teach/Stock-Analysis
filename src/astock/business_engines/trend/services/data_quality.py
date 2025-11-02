"""
数据质量检查服务
================

统一的数据质量检查逻辑，避免代码重复。
"""

import logging
import numpy as np
from typing import List, Tuple, NamedTuple
from ..trend_models import DataQualitySummary
from ..config import TrendAnalysisConfig

logger = logging.getLogger(__name__)


class SimpleQualityResult(NamedTuple):
    """简化的质量评估结果"""
    quality: str
    has_loss_years: bool
    loss_year_count: int
    has_near_zero_years: bool
    near_zero_count: int


class DataQualityChecker:
    """统一的数据质量检查服务

    职责：
    1. 窗口数据验证
    2. 数据质量评估
    3. 缺失值检测
    4. 异常值初步筛查
    """

    def __init__(self, config: TrendAnalysisConfig = None):
        """初始化数据质量检查器

        Args:
            config: 趋势分析配置，None则使用默认配置
        """
        from ..config import get_default_config
        self.config = config or get_default_config()

    def ensure_window(
        self,
        values: List[float],
        window: int = None
    ) -> np.ndarray:
        """验证并返回窗口数据

        Args:
            values: 数据列表
            window: 窗口大小，None则使用配置中的默认值

        Returns:
            窗口数据的 NumPy 数组

        Raises:
            ValueError: 数据量不足
        """
        window = window or self.config.default_window_size
        arr = np.asarray(values, dtype=float)

        if arr.size < window:
            raise ValueError(
                f"需要至少{window}期数据, 实际仅{arr.size}期"
            )

        # 如果数据多于窗口大小，取最新的数据
        if arr.size > window:
            arr = arr[-window:]

        return arr

    def classify_quality(
        self,
        values: np.ndarray
    ) -> DataQualitySummary:
        """评估数据质量

        根据亏损/接近0的期数判断数据质量。

        Args:
            values: 数据数组

        Returns:
            数据质量摘要
        """
        # 计算亏损和接近0的情况
        loss_mask = values < 0
        near_zero_mask = (values >= 0) & (values < self.config.near_zero_threshold)

        loss_count = int(np.sum(loss_mask))
        near_zero_count = int(np.sum(near_zero_mask))

        # 判断质量等级
        if loss_count >= self.config.poor_quality_threshold or \
           near_zero_count >= self.config.poor_quality_threshold:
            quality = "poor"
        elif loss_count == 1:
            quality = "has_loss"
        elif near_zero_count == 1:
            quality = "has_near_zero"
        else:
            quality = "good"

        return SimpleQualityResult(
            quality=quality,
            has_loss_years=loss_count > 0,
            loss_year_count=loss_count,
            has_near_zero_years=near_zero_count > 0,
            near_zero_count=near_zero_count,
        )

    def check_finite(
        self,
        values: np.ndarray,
        drop_non_finite: bool = True
    ) -> Tuple[np.ndarray, int, int]:
        """检查有限值（非NaN/Inf）

        Args:
            values: 数据数组
            drop_non_finite: 是否排除Inf，True则只排除NaN

        Returns:
            (有效值掩码, 总数, 有效数)
        """
        if drop_non_finite:
            finite_mask = np.isfinite(values)
        else:
            finite_mask = ~np.isnan(values)

        total_count = values.size
        valid_count = int(finite_mask.sum())

        return finite_mask, total_count, valid_count

    def validate_data(
        self,
        values: np.ndarray,
        allow_partial: bool = True,
        min_ratio: float = None
    ) -> Tuple[bool, str]:
        """验证数据有效性

        Args:
            values: 数据数组
            allow_partial: 是否允许部分缺失
            min_ratio: 最小有效数据比例，None则使用配置默认值

        Returns:
            (是否有效, 错误原因)
        """
        min_ratio = min_ratio or self.config.min_valid_ratio

        finite_mask, total_count, valid_count = self.check_finite(values)

        # 全部缺失
        if valid_count == 0:
            return False, "全部为缺失值"

        # 不允许部分缺失
        if not allow_partial and valid_count < total_count:
            return False, f"存在缺失值({total_count - valid_count}/{total_count}期)"

        # 检查有效数据比例
        valid_ratio = valid_count / total_count
        if valid_ratio < min_ratio:
            return False, f"有效数据不足({valid_count}/{total_count}期, 需要≥{min_ratio:.0%})"

        return True, ""

    def prepare_values(
        self,
        values: List[float],
        window: int = None,
        allow_partial: bool = True
    ) -> np.ndarray:
        """准备并验证数据

        整合窗口验证、有效性检查的便捷方法。

        Args:
            values: 数据列表
            window: 窗口大小
            allow_partial: 是否允许部分缺失

        Returns:
            验证后的数据数组

        Raises:
            ValueError: 数据无效
        """
        # 1. 窗口验证
        arr = self.ensure_window(values, window)

        # 2. 有效性检查
        is_valid, error_msg = self.validate_data(arr, allow_partial)
        if not is_valid:
            raise ValueError(error_msg)

        return arr

    def detect_near_zero_mean(
        self,
        values: np.ndarray,
        eps: float = None
    ) -> bool:
        """检测均值是否接近0

        Args:
            values: 数据数组
            eps: 判定阈值，None则使用配置默认值

        Returns:
            是否接近0
        """
        eps = eps or self.config.mean_near_zero_eps
        mean_val = np.mean(np.abs(values))
        return mean_val < eps

    def safe_log_transform(
        self,
        values: np.ndarray,
        method: str = 'arcsinh'
    ) -> Tuple[np.ndarray, bool]:
        """安全的对数变换

        Args:
            values: 数据数组
            method: 变换方法，'arcsinh' 或 'log_shift'

        Returns:
            (变换后的值, 是否跨越0)
        """
        min_val = np.min(values)
        max_val = np.max(values)
        crosses_zero = (min_val < 0 < max_val)

        if method == 'arcsinh':
            # 反双曲正弦变换（可处理负值和0）
            transformed = np.arcsinh(values)
        elif method == 'log_shift':
            # 平移后取对数
            shift = abs(min_val) + self.config.log_safe_min_value
            transformed = np.log(values + shift)
        else:
            raise ValueError(f"未知的变换方法: {method}")

        return transformed, crosses_zero

    def get_quality_warnings(
        self,
        quality: SimpleQualityResult
    ) -> List[str]:
        """根据数据质量生成警告信息

        Args:
            quality: 数据质量评估结果

        Returns:
            警告信息列表
        """
        warnings = []

        if quality.quality == "poor":
            warnings.append(
                f"数据质量较差: {quality.loss_year_count}期亏损, "
                f"{quality.near_zero_count}期接近0"
            )
        elif quality.has_loss_years:
            warnings.append(f"存在{quality.loss_year_count}期亏损")
        elif quality.has_near_zero_years:
            warnings.append(f"存在{quality.near_zero_count}期接近0")

        return warnings
