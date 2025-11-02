"""
趋势分析统一配置类
==================

集中管理所有趋势分析相关的阈值、权重和配置参数。
"""

from dataclasses import dataclass, field
from typing import Dict, List
import numpy as np


@dataclass
class TrendAnalysisConfig:
    """趋势分析统一配置

    集中管理所有硬编码的常量和阈值，便于：
    1. 统一管理和修改
    2. 动态配置
    3. A/B 测试
    4. 单元测试
    """

    # ========== 加权方案 ==========

    default_weights: np.ndarray = field(
        default_factory=lambda: np.array([0.1, 0.15, 0.2, 0.25, 0.3])
    )
    """加权方案: 线性递减(最早→最新)"""

    # ========== Log 斜率阈值 ==========

    log_severe_decline_slope: float = -0.30
    """严重衰退阈值: 每年-30% (5年从10%→2.4%)"""

    log_mild_decline_slope: float = -0.15
    """轻度衰退阈值: 每年-15% (5年从10%→4.4%)"""

    # ========== Log 计算安全值 ==========

    log_safe_min_value: float = 0.01
    """最小值截断(防止log(0)或log(负数))"""

    mean_near_zero_eps: float = 1e-6
    """均值接近0时的稳健判定阈值"""

    robust_alpha: float = 0.95
    """鲁棒斜率计算置信区间水平 (Theil–Sen)"""

    # ========== 异常值检测阈值 ==========

    z_score_threshold: float = 3.0
    """Z-score阈值：超过3.0倍标准差视为异常"""

    iqr_multiplier: float = 1.5
    """IQR倍数：用于箱线图异常检测"""

    mad_z_threshold: float = 3.5
    """Modified Z-score阈值（默认3.5）"""

    mad_normalizer: float = 0.6745
    """将MAD缩放为类标准差的系数"""

    default_outlier_method: str = 'mad'
    """默认异常检测方法"""

    # ========== 周期性判断配置 ==========

    factor_weights: Dict[str, float] = field(default_factory=lambda: {
        'industry': 0.25,
        'peak_to_trough': 0.20,
        'low_r_squared': 0.20,
        'wave_pattern': 0.15,
        'high_cv': 0.15,
        'middle_peak': 0.05,
    })
    """置信度因子权重"""

    peak_to_trough_saturation: float = 9.0
    """峰谷比饱和倍率：达到阈值的9倍即记满分"""

    cv_saturation: float = 4.0
    """CV饱和倍率：达到阈值的4倍即记满分"""

    # ========== 周期性行业列表 ==========

    cyclical_industries: List[str] = field(default_factory=lambda: [
        "小金属", "黄金", "钢铁", "煤炭", "有色金属", "石油石化",
        "化工", "化学制品", "基础化工", "化学纤维",
        "建材", "水泥", "玻璃",
        "航运", "港口", "交运设备",
        "房地产", "建筑", "建筑材料",
        "机械", "专用设备", "通用设备",
        "汽车", "汽车零部件",
        "造纸", "包装印刷",
        "轻工制造", "家居用品",
    ])
    """周期性行业列表"""

    # ========== 窗口配置 ==========

    default_window_size: int = 5
    """默认窗口大小（年数）"""

    min_valid_ratio: float = 0.6
    """最小有效数据比例"""

    # ========== 数据质量阈值 ==========

    poor_quality_threshold: int = 2
    """判定为"差"质量的最小亏损/接近0的期数"""

    near_zero_threshold: float = 1.0
    """判定为"接近0"的阈值"""

    # ========== 趋势判断阈值 ==========

    r_squared_low_threshold: float = 0.5
    """R²低阈值：小于此值认为趋势性弱"""

    r_squared_high_threshold: float = 0.8
    """R²高阈值：大于此值认为趋势性强"""

    p_value_threshold: float = 0.05
    """P值阈值：用于显著性检验"""

    # ========== 波动性阈值 ==========

    high_cv_threshold: float = 0.4
    """高CV阈值：大于此值认为波动性高"""

    low_cv_threshold: float = 0.15
    """低CV阈值：小于此值认为波动性低"""

    # ========== 鲁棒性检查 ==========

    robust_gap_threshold: float = 0.1
    """鲁棒斜率与OLS斜率差异阈值"""

    robust_gap_warn_threshold: float = 0.05
    """鲁棒斜率差异警告阈值"""

    # ========== 拐点检测 ==========

    inflection_min_change_ratio: float = 0.2
    """拐点最小变化率"""

    inflection_significance_threshold: float = 0.05
    """拐点显著性阈值"""

    # ========== 恶化检测 ==========

    deterioration_recent_years: int = 2
    """近期恶化检测的年数"""

    deterioration_threshold: float = -0.20
    """恶化阈值：近期斜率"""

    # ========== 滚动趋势 ==========

    rolling_window_size: int = 3
    """滚动窗口大小"""

    def __post_init__(self):
        """验证配置的合理性"""
        # 验证权重
        if not isinstance(self.default_weights, np.ndarray):
            self.default_weights = np.array(self.default_weights)

        # 验证权重和为1
        weight_sum = self.default_weights.sum()
        if not np.isclose(weight_sum, 1.0):
            raise ValueError(f"权重和应为1.0，当前为{weight_sum}")

        # 验证因子权重和为1
        factor_weight_sum = sum(self.factor_weights.values())
        if not np.isclose(factor_weight_sum, 1.0):
            raise ValueError(f"因子权重和应为1.0，当前为{factor_weight_sum}")

        # 验证阈值合理性
        if self.log_severe_decline_slope >= self.log_mild_decline_slope:
            raise ValueError("严重衰退阈值应小于轻度衰退阈值")

        if self.r_squared_low_threshold >= self.r_squared_high_threshold:
            raise ValueError("R²低阈值应小于高阈值")

        if self.low_cv_threshold >= self.high_cv_threshold:
            raise ValueError("低CV阈值应小于高CV阈值")

    def is_cyclical_industry(self, industry: str) -> bool:
        """判断是否为周期性行业"""
        if not industry:
            return False
        return industry in self.cyclical_industries

    def get_weights(self, window_size: int = None) -> np.ndarray:
        """获取指定窗口大小的权重

        Args:
            window_size: 窗口大小，None则使用默认权重

        Returns:
            权重数组
        """
        if window_size is None or window_size == len(self.default_weights):
            return self.default_weights

        # 生成线性递增权重
        weights = np.arange(1, window_size + 1, dtype=float)
        weights = weights / weights.sum()
        return weights


# 全局默认配置实例
_default_config = None


def get_default_config() -> TrendAnalysisConfig:
    """获取全局默认配置实例（单例模式）"""
    global _default_config
    if _default_config is None:
        _default_config = TrendAnalysisConfig()
    return _default_config


def reset_default_config():
    """重置全局默认配置（主要用于测试）"""
    global _default_config
    _default_config = None
