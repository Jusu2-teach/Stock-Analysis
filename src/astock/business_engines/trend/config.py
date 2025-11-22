"""
趋势分析配置模块（重构合并版）
==============================

合并原 config/ 目录下的所有配置：
- analysis_config.py: 主配置类
- characteristics.py, filters.py, roiic.py: 行业配置

简化策略：
1. 保留 TrendAnalysisConfig 作为主配置类
2. 提供简化的行业配置函数（向后兼容）
3. 删除冗余的旧配置系统
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any
import numpy as np


# ============================================================================
# 主配置类
# ============================================================================

@dataclass
class TrendAnalysisConfig:
    """趋势分析统一配置"""

    # 加权方案
    default_weights: np.ndarray = field(
        default_factory=lambda: np.array([0.1, 0.15, 0.2, 0.25, 0.3])
    )

    # Log斜率阈值
    log_severe_decline_slope: float = -0.30
    log_mild_decline_slope: float = -0.15

    # 安全值
    log_safe_min_value: float = 0.01
    mean_near_zero_eps: float = 1e-6
    robust_alpha: float = 0.95

    # 异常值检测
    zscore_threshold: float = 3.0
    iqr_multiplier: float = 1.5
    mad_z_threshold: float = 3.5
    mad_normalizer: float = 0.6745
    default_outlier_method: str = 'iqr'

    # 周期性配置
    factor_weights: Dict[str, float] = field(default_factory=lambda: {
        'industry': 0.25,
        'peak_to_trough': 0.20,
        'low_r_squared': 0.20,
        'wave_pattern': 0.15,
        'high_cv': 0.15,
        'middle_peak': 0.05,
    })

    peak_to_trough_saturation: float = 9.0
    cv_saturation: float = 4.0

    # 周期性行业
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

    # 窗口配置
    min_periods: int = 3
    default_window_size: int = 5
    min_valid_ratio: float = 0.6

    # 数据质量
    poor_quality_threshold: int = 2
    near_zero_threshold: float = 1.0

    # 趋势判断
    r_squared_low_threshold: float = 0.5
    r_squared_high_threshold: float = 0.8
    p_value_threshold: float = 0.05

    # 波动性
    high_cv_threshold: float = 0.4
    low_cv_threshold: float = 0.15

    # 鲁棒性
    robust_gap_threshold: float = 0.1
    robust_gap_warn_threshold: float = 0.05

    # 拐点检测
    inflection_min_change_ratio: float = 0.2
    inflection_significance_threshold: float = 0.05

    # 恶化检测
    deterioration_recent_years: int = 2
    deterioration_threshold: float = -0.20

    # 滚动趋势
    rolling_window_size: int = 3

    def __post_init__(self):
        """验证配置合理性"""
        if not isinstance(self.default_weights, np.ndarray):
            self.default_weights = np.array(self.default_weights)

        weight_sum = self.default_weights.sum()
        if not np.isclose(weight_sum, 1.0):
            raise ValueError(f"权重和应为1.0，当前为{weight_sum}")

        factor_weight_sum = sum(self.factor_weights.values())
        if not np.isclose(factor_weight_sum, 1.0):
            raise ValueError(f"因子权重和应为1.0，当前为{factor_weight_sum}")

    def is_cyclical_industry(self, industry: str) -> bool:
        """判断是否为周期性行业"""
        if not industry:
            return False
        return industry in self.cyclical_industries

    def get_weights(self, window_size: int = None) -> np.ndarray:
        """获取权重"""
        if window_size is None or window_size == len(self.default_weights):
            return self.default_weights

        weights = np.arange(1, window_size + 1, dtype=float)
        weights = weights / weights.sum()
        return weights


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


# ============================================================================
# 行业配置（向后兼容）
# ============================================================================

# 行业分类映射
_INDUSTRY_CATEGORY_MAP = {
    # 周期性行业
    "小金属": "cyclical",
    "黄金": "cyclical",
    "钢铁": "cyclical",
    "煤炭": "cyclical",
    "有色金属": "cyclical",
    "石油石化": "cyclical",
    "化工": "cyclical",
    "建材": "cyclical",
    "房地产": "cyclical",
    "航运": "cyclical",
    # 成长性行业
    "医药": "growth",
    "生物制药": "growth",
    "电子": "growth",
    "计算机": "growth",
    "软件": "growth",
    # 防御性行业
    "食品饮料": "defensive",
    "公用事业": "defensive",
    "医疗服务": "defensive",
}

# 周期性阈值
_CYCLICAL_THRESHOLDS = {
    "cyclical": {
        "cv_threshold": 0.3,
        "peak_valley_ratio": 2.0,
        "r_squared_low": 0.4,
    },
    "growth": {
        "cv_threshold": 0.5,
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
    },
    "defensive": {
        "cv_threshold": 0.6,
        "peak_valley_ratio": 4.0,
        "r_squared_low": 0.6,
    },
    "default": {
        "cv_threshold": 0.5,
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
    },
}

# 衰退阈值
_DECLINE_THRESHOLDS = {
    "cyclical": {
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "decline_threshold_pct": -5.0,
        "decline_threshold_abs": -2.0,
        "high_level_threshold": 20.0,
    },
    "growth": {
        "severe_decline": -0.35,
        "mild_decline": -0.18,
        "decline_threshold_pct": -5.0,
        "decline_threshold_abs": -2.0,
        "high_level_threshold": 20.0,
    },
    "defensive": {
        "severe_decline": -0.20,
        "mild_decline": -0.10,
        "decline_threshold_pct": -5.0,
        "decline_threshold_abs": -2.0,
        "high_level_threshold": 20.0,
    },
    "default": {
        "severe_decline": -0.30,
        "mild_decline": -0.15,
        "decline_threshold_pct": -5.0,
        "decline_threshold_abs": -2.0,
        "high_level_threshold": 20.0,
    },
}

# ROIC过滤配置
_ROIC_FILTER_CONFIGS = {
    "医药": {"min_roic": 0.08, "min_slope": -0.02},
    "食品饮料": {"min_roic": 0.10, "min_slope": -0.02},
    "电子": {"min_roic": 0.06, "min_slope": -0.03},
    "default": {"min_roic": 0.05, "min_slope": -0.05},
}

# ROIIC过滤配置
_ROIIC_FILTER_CONFIGS = {
    "医药": {"min_roiic": 0.06, "min_slope": -0.02},
    "食品饮料": {"min_roiic": 0.08, "min_slope": -0.02},
    "电子": {"min_roiic": 0.04, "min_slope": -0.03},
    "default": {"min_roiic": 0.03, "min_slope": -0.05},
}


def get_industry_category(industry: str) -> str:
    """获取行业分类"""
    if not industry:
        return "default"
    return _INDUSTRY_CATEGORY_MAP.get(industry, "default")


def get_cyclical_thresholds(industry: str = None) -> Dict[str, float]:
    """获取周期性判断阈值（向后兼容）"""
    category = get_industry_category(industry)
    return _CYCLICAL_THRESHOLDS.get(category, _CYCLICAL_THRESHOLDS["default"]).copy()


def get_decline_thresholds(industry: str = None) -> Dict[str, float]:
    """获取衰退阈值（向后兼容）"""
    category = get_industry_category(industry)
    return _DECLINE_THRESHOLDS.get(category, _DECLINE_THRESHOLDS["default"]).copy()


def get_filter_config(industry: str = None) -> Dict[str, float]:
    """获取ROIC过滤配置"""
    if not industry:
        return _ROIC_FILTER_CONFIGS["default"].copy()
    return _ROIC_FILTER_CONFIGS.get(industry, _ROIC_FILTER_CONFIGS["default"]).copy()


def get_roiic_filter_config(industry: str = None) -> Dict[str, float]:
    """获取ROIIC过滤配置"""
    if not industry:
        return _ROIIC_FILTER_CONFIGS["default"].copy()
    return _ROIIC_FILTER_CONFIGS.get(industry, _ROIIC_FILTER_CONFIGS["default"]).copy()


# 保留旧名称（完全向后兼容）
INDUSTRY_FILTER_CONFIGS = _ROIC_FILTER_CONFIGS
DEFAULT_FILTER_CONFIG = _ROIC_FILTER_CONFIGS["default"]
ROIIC_INDUSTRY_FILTER_CONFIGS = _ROIIC_FILTER_CONFIGS
DEFAULT_ROIIC_FILTER_CONFIG = _ROIIC_FILTER_CONFIGS["default"]


# 导出
__all__ = [
    # 配置类
    'TrendAnalysisConfig',
    'get_default_config',
    'reset_default_config',
    # 行业配置函数
    'get_industry_category',
    'get_cyclical_thresholds',
    'get_decline_thresholds',
    'get_filter_config',
    'get_roiic_filter_config',
    # 配置常量（向后兼容）
    'INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_FILTER_CONFIG',
    'ROIIC_INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_ROIIC_FILTER_CONFIG',
]
