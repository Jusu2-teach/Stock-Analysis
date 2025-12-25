"""
趋势分析配置模块 (纯算法参数)
==============================

本模块只包含趋势分析算法所需的数学/统计参数。

⚠️ 重要设计原则：
================
trend/ 目录是 **纯数学层**，不包含任何业务逻辑：
- ✅ 统计阈值 (p_value_threshold, r_squared_threshold)
- ✅ 算法参数 (bootstrap 次数, 窗口大小)
- ✅ 数学常量 (zscore_threshold, iqr_multiplier)
- ❌ 行业分类 → evaluators/threshold/industry_config.py
- ❌ 指标阈值 → evaluators/threshold/metric_thresholds.py
- ❌ 业务规则 → evaluators/threshold/rules.py

业务配置请使用：
    from astock.business_engines.evaluators.threshold import (
        get_industry_category,
        get_cyclical_thresholds,
        get_decline_thresholds,
        get_metric_thresholds,
    )

作者: AStock Analysis System
日期: 2025-01-07
版本: 3.0 (Pure Math Edition)
"""

from dataclasses import dataclass, field
from typing import Dict, Tuple
import numpy as np


# ============================================================================
# 主配置类 (纯算法参数)
# ============================================================================

@dataclass
class TrendAnalysisConfig:
    """
    趋势分析统一配置

    所有参数都是纯数学/统计参数，不涉及业务知识。
    """

    # ==================== 加权方案 ====================
    default_weights: np.ndarray = field(
        default_factory=lambda: np.array([0.1, 0.15, 0.2, 0.25, 0.3])
    )

    # ==================== 对数趋势阈值 ====================
    # 这些是对数空间中的斜率阈值，是数学定义，不是业务定义
    log_severe_decline_slope: float = -0.30  # 对数斜率 < -0.30 视为严重下跌
    log_mild_decline_slope: float = -0.15    # 对数斜率 < -0.15 视为轻微下跌

    # ==================== 数值安全参数 ====================
    log_safe_min_value: float = 0.01         # 对数变换的最小安全值
    mean_near_zero_eps: float = 1e-6         # 均值接近零的阈值
    robust_alpha: float = 0.95               # 鲁棒估计的置信度

    # ==================== 异常值检测参数 ====================
    zscore_threshold: float = 3.0            # Z-score 异常值阈值
    iqr_multiplier: float = 1.5              # IQR 乘数 (1.5 = Tukey's rule)
    mad_z_threshold: float = 3.5             # MAD Z-score 阈值
    mad_normalizer: float = 0.6745           # MAD 正态化常数
    default_outlier_method: str = 'iqr'      # 默认异常值检测方法

    # ==================== 窗口配置 ====================
    min_periods: int = 3                     # 最小数据点数
    default_window_size: int = 5             # 默认窗口大小
    min_valid_ratio: float = 0.6             # 最小有效数据比例

    # ==================== 数据质量参数 ====================
    poor_quality_threshold: int = 2          # 质量差的阈值
    near_zero_threshold: float = 1.0         # 接近零的阈值

    # ==================== 统计显著性阈值 ====================
    # 这些是统计学标准阈值，不是业务阈值
    r_squared_low_threshold: float = 0.5     # R² 低阈值
    r_squared_high_threshold: float = 0.8    # R² 高阈值
    p_value_threshold: float = 0.05          # p 值显著性阈值

    # ==================== 波动率统计参数 ====================
    # 变异系数 (CV) 的统计分类阈值
    high_cv_threshold: float = 0.4           # 高波动阈值
    low_cv_threshold: float = 0.15           # 低波动阈值

    # ==================== 鲁棒性检测参数 ====================
    robust_gap_threshold: float = 0.1        # OLS vs 鲁棒斜率差异阈值
    robust_gap_warn_threshold: float = 0.05  # 差异警告阈值

    # ==================== 拐点检测参数 ====================
    inflection_min_change_ratio: float = 0.2           # CUSUM 最小变化比例
    inflection_significance_threshold: float = 0.05   # 显著性阈值

    # ==================== 恶化检测参数 ====================
    deterioration_recent_years: int = 2      # 近期年数
    deterioration_threshold: float = -0.20   # 默认恶化阈值

    # ==================== 滚动趋势参数 ====================
    rolling_window_size: int = 3             # 滚动窗口大小

    # ==================== Bootstrap 参数 ====================
    bootstrap_n_iterations: int = 1000       # Bootstrap 迭代次数
    bootstrap_confidence_level: float = 0.95 # Bootstrap 置信水平

    # ==================== HP 滤波参数 ====================
    hp_filter_lambda: float = 6.25           # HP 滤波 λ (年度数据推荐值)

    # ==================== Hurst 指数参数 ====================
    hurst_min_chunk_size: int = 4            # Hurst R/S 分析最小块大小
    hurst_random_walk_threshold: float = 0.1 # |H - 0.5| < 0.1 视为随机游走

    # ==================== ACF 分析参数 ====================
    acf_significance_level: float = 0.10     # ACF 显著性水平
    acf_max_lag_ratio: float = 0.5           # 最大滞后 = n * ratio

    def __post_init__(self):
        """验证配置合理性"""
        if not isinstance(self.default_weights, np.ndarray):
            self.default_weights = np.array(self.default_weights)

        weight_sum = self.default_weights.sum()
        if not np.isclose(weight_sum, 1.0):
            raise ValueError(f"权重和应为1.0，当前为{weight_sum}")

    def get_weights(
        self,
        window_size: int = None,
        decay_factor: float = 0.8,
        method: str = "exponential"
    ) -> np.ndarray:
        """
        获取时间加权权重（近期数据权重更高）

        Parameters
        ----------
        window_size : int, optional
            窗口大小，默认使用default_weights的长度
        decay_factor : float, optional
            指数衰减因子，范围(0,1)，越小则近期权重越高。默认0.8
        method : str, optional
            权重计算方法:
            - "exponential": 指数衰减 w_i = decay^(n-1-i) (默认)
            - "linear": 线性递增 w_i = i+1
            - "default": 使用预设的default_weights

        Returns
        -------
        np.ndarray
            归一化的权重数组，和为1
        """
        if window_size is None:
            window_size = len(self.default_weights)

        if method == "default" and window_size == len(self.default_weights):
            return self.default_weights

        if method == "exponential":
            indices = np.arange(window_size)
            weights = np.power(decay_factor, (window_size - 1 - indices))
        elif method == "linear":
            weights = np.arange(1, window_size + 1, dtype=float)
        else:
            if window_size == len(self.default_weights):
                return self.default_weights
            weights = np.arange(1, window_size + 1, dtype=float)

        weights = weights / weights.sum()
        return weights


# ============================================================================
# 默认 CV 阈值 (纯统计分类，不涉及行业)
# ============================================================================
# 这些是统计学意义上的波动分类，调用方可以根据业务需要调整
DEFAULT_CV_THRESHOLDS: Dict[str, float] = {
    'ultra_stable': 0.12,   # CV < 0.12 极稳定
    'stable': 0.20,         # CV < 0.20 稳定
    'moderate': 0.35,       # CV < 0.35 中等波动
    'volatile': 0.55,       # CV < 0.55 高波动
    # CV >= 0.55 极高波动
}


# ============================================================================
# 全局单例
# ============================================================================

_default_config = None


def get_default_config() -> TrendAnalysisConfig:
    """获取全局默认配置"""
    global _default_config
    if _default_config is None:
        _default_config = TrendAnalysisConfig()
    return _default_config


def reset_default_config():
    """重置配置（用于测试）"""
    global _default_config
    _default_config = None
