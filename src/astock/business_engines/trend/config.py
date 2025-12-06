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
from typing import Dict, List, Any, Tuple
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
    # --- 强周期性行业 (Cyclical) ---
    "小金属": "cyclical", "黄金": "cyclical", "钢铁": "cyclical", "煤炭": "cyclical",
    "有色金属": "cyclical", "石油石化": "cyclical", "化工": "cyclical", "基础化工": "cyclical",
    "化学纤维": "cyclical", "建材": "cyclical", "水泥": "cyclical", "玻璃": "cyclical",
    "房地产": "cyclical", "航运": "cyclical", "港口": "cyclical", "远洋运输": "cyclical",
    "建筑": "cyclical", "工程机械": "cyclical", "重型机械": "cyclical",
    "证券": "cyclical", "保险": "cyclical", "多元金融": "cyclical", # 金融也是顺周期的

    # --- 成长性行业 (Growth) ---
    "医药": "growth", "生物制药": "growth", "医疗器械": "growth", "医疗服务": "growth",
    "电子": "growth", "半导体": "growth", "元件": "growth", "光学光电子": "growth",
    "计算机": "growth", "软件": "growth", "互联网": "growth", "通信设备": "growth",
    "新能源": "growth", "光伏设备": "growth", "电池": "growth", "风电设备": "growth",
    "航空航天": "growth", "军工": "growth", "自动化设备": "growth",

    # --- 防御性/稳定行业 (Defensive) ---
    "食品饮料": "defensive", "白酒": "defensive", "饮料制造": "defensive", "食品加工": "defensive",
    "农林牧渔": "defensive", "饲料": "defensive",
    "公用事业": "defensive", "电力": "defensive", "水务": "defensive", "燃气": "defensive",
    "环保": "defensive", "交通运输": "defensive", "高速公路": "defensive", "机场": "defensive",
    "银行": "defensive", # 银行相对稳健，虽有周期性但波动率低于券商
    "家电": "defensive", "白色家电": "defensive",
}

# 周期性阈值
_CYCLICAL_THRESHOLDS = {
    "cyclical": {
        "cv_threshold": 0.3,      # 周期股容忍较低的波动率阈值(即更容易被判定为高波动)
        "peak_valley_ratio": 2.0, # 峰谷比 > 2 即视为显著周期
        "r_squared_low": 0.4,     # 允许趋势拟合度较低
    },
    "growth": {
        "cv_threshold": 0.5,      # 成长股波动率中等
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
    },
    "defensive": {
        "cv_threshold": 0.6,      # 防御股应该很稳，CV阈值高(即很难被判定为高波动)
        "peak_valley_ratio": 4.0,
        "r_squared_low": 0.6,     # 要求较高的趋势平滑度
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
        "severe_decline": -0.35, # 成长股容忍更大的回撤(高波动)
        "mild_decline": -0.18,
        "decline_threshold_pct": -5.0,
        "decline_threshold_abs": -2.0,
        "high_level_threshold": 20.0,
    },
    "defensive": {
        "severe_decline": -0.20, # 防御股回撤20%就是大灾难
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

# ROIC过滤配置 (基于WACC资本成本逻辑)
# 一般中国企业的WACC在 8% 左右。长期ROIC < 8% 意味着毁灭价值。
# 但考虑到行业特性，给予一定宽容度或溢价。
_ROIC_FILTER_CONFIGS = {
    # 高壁垒/高周转行业: 要求 > 10% (显著创造价值)
    "食品饮料": {"min_roic": 0.12, "min_slope": -0.02},
    "白酒": {"min_roic": 0.15, "min_slope": -0.02},
    "家电": {"min_roic": 0.10, "min_slope": -0.02},
    "医药": {"min_roic": 0.08, "min_slope": -0.02}, # 研发投入大，净资产可能虚高

    # 资本密集/重资产行业: 要求 > 6% (接近WACC即可，主要看现金流)
    "公用事业": {"min_roic": 0.06, "min_slope": -0.01},
    "电力": {"min_roic": 0.06, "min_slope": -0.01},
    "交通运输": {"min_roic": 0.05, "min_slope": -0.01},
    "钢铁": {"min_roic": 0.05, "min_slope": -0.05}, # 周期底部可能很低
    "煤炭": {"min_roic": 0.08, "min_slope": -0.05}, # 资源属性，ROIC应较高

    # 科技/成长行业: 要求 > 6% (看重增长，对当前回报宽容)
    "电子": {"min_roic": 0.06, "min_slope": -0.03},
    "半导体": {"min_roic": 0.05, "min_slope": -0.03},
    "计算机": {"min_roic": 0.06, "min_slope": -0.03},

    # 默认: 6% (底线思维，至少要覆盖大部分债务成本)
    "default": {"min_roic": 0.06, "min_slope": -0.05},
}

# ROIIC过滤配置 (增量资本回报率)
# 反映新投入资本的效率，波动较大，阈值适当降低
_ROIIC_FILTER_CONFIGS = {
    "医药": {"min_roiic": 0.06, "min_slope": -0.02},
    "食品饮料": {"min_roiic": 0.10, "min_slope": -0.02},
    "电子": {"min_roiic": 0.05, "min_slope": -0.03},
    "default": {"min_roiic": 0.04, "min_slope": -0.05},
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

# ============================================================================
# 字段 Schema 定义
# ============================================================================

from .models import TrendField, TrendSnapshot

def trend_field_schema() -> List[TrendField]:
    """Return the default schema for trend result columns."""

    return [
        # 核心趋势
        TrendField("weighted", "weighted_avg", "5年加权平均", unit="ratio", category="core"),
        TrendField("log_slope", "trend.log_slope", "Log趋势斜率", unit="slope", category="core"),
        TrendField("slope", "trend.slope", "线性斜率", unit="slope", category="core"),
        TrendField("r_squared", "trend.r_squared", "趋势拟合优度", category="core"),
        TrendField("p_value", "trend.p_value", "趋势显著性P值", category="core"),
        TrendField("cagr", "trend.cagr_approx", "CAGR近似", unit="ratio", category="core"),
        TrendField("latest", "latest_value", "最新值", category="core"),
        TrendField("trend_score", "evaluation.trend_score", "趋势评分", category="core"),
        # 鲁棒性指标
        TrendField("robust_slope", "robust.robust_slope", "稳健斜率(Theil-Sen)", unit="slope", category="robust"),
        TrendField("mk_tau", "robust.mann_kendall_tau", "Mann-Kendall Tau", category="robust"),
        TrendField("mk_p_value", "robust.mann_kendall_p_value", "Mann-Kendall P值", category="robust"),
        # 数据质量
        TrendField("data_quality", "quality.effective", "有效数据质量标记", category="quality"),
        TrendField("data_quality_original", "quality.original", "原始数据质量", category="quality"),
        TrendField("data_quality_cleaned", "quality.cleaned", "清洗后数据质量", category="quality"),
        TrendField("has_loss_years", "quality.has_loss_years", "是否存在亏损年", category="quality"),
        TrendField("loss_year_count", "quality.loss_year_count", "亏损年计数", category="quality"),
        TrendField("has_near_zero_years", "quality.has_near_zero_years", "是否存在接近0的年份", category="quality"),
        TrendField("near_zero_count", "quality.near_zero_count", "接近0年份数量", category="quality"),
        TrendField("has_loss_years_cleaned", "quality.has_loss_years_cleaned", "清洗后是否亏损", category="quality"),
        TrendField("loss_year_count_cleaned", "quality.loss_year_count_cleaned", "清洗后亏损年数", category="quality"),
        TrendField("has_near_zero_years_cleaned", "quality.has_near_zero_years_cleaned", "清洗后是否接近0", category="quality"),
        TrendField("near_zero_count_cleaned", "quality.near_zero_count_cleaned", "清洗后接近0年数", category="quality"),
        # 波动率
        TrendField("cv", "volatility.cv", "变异系数", category="volatility"),
        TrendField("std_dev", "volatility.std_dev", "标准差", category="volatility"),
        TrendField("range_ratio", "volatility.range_ratio", "极差比例", category="volatility"),
        TrendField("volatility_type", "volatility.volatility_type", "波动类型", category="volatility"),
        TrendField("vol_mean_near_zero", "volatility.mean_near_zero", "均值是否接近0", category="volatility"),
        # 拐点
        TrendField("has_inflection", "inflection.has_inflection", "是否存在拐点", category="inflection"),
        TrendField("inflection_type", "inflection.inflection_type", "拐点类型", category="inflection"),
        TrendField("early_slope", "inflection.early_slope", "早期斜率", category="inflection"),
        TrendField("middle_slope", "inflection.middle_slope", "中段斜率", category="inflection"),
        TrendField("recent_slope", "inflection.recent_slope", "近年斜率", category="inflection"),
        TrendField("slope_change", "inflection.slope_change", "斜率变化幅度", category="inflection"),
        TrendField("inflection_confidence", "inflection.confidence", "拐点置信度", category="inflection"),
        TrendField("inflection_early_r2", "inflection.early_r_squared", "早期拟合优度", category="inflection"),
        TrendField("inflection_recent_r2", "inflection.recent_r_squared", "近期拟合优度", category="inflection"),
        # 恶化
        TrendField("has_deterioration", "deterioration.has_deterioration", "是否存在恶化", category="deterioration"),
        TrendField("deterioration_severity", "deterioration.severity", "恶化程度", category="deterioration"),
        TrendField("year4_to_5_change", "deterioration.year4_to_5_change", "第4-5年变动", category="deterioration"),
        TrendField("year3_to_4_change", "deterioration.year3_to_4_change", "第3-4年变动", category="deterioration"),
        TrendField("total_decline_pct", "deterioration.total_decline_pct", "总跌幅", unit="ratio", category="deterioration"),
        TrendField("year4_to_5_pct", "deterioration.year4_to_5_pct", "第4-5年跌幅比例", unit="ratio", category="deterioration"),
        TrendField("year3_to_4_pct", "deterioration.year3_to_4_pct", "第3-4年跌幅比例", unit="ratio", category="deterioration"),
        TrendField("is_high_level_stable", "deterioration.is_high_level_stable", "高位稳定", category="deterioration"),
        TrendField("deterioration_industry", "deterioration.industry", "恶化判断行业", category="deterioration"),
        # 周期性
        TrendField("is_cyclical", "cyclical.is_cyclical", "是否周期性", category="cyclical"),
        TrendField("peak_to_trough_ratio", "cyclical.peak_to_trough_ratio", "峰谷比", category="cyclical"),
        TrendField("has_middle_peak", "cyclical.has_middle_peak", "是否中段峰值", category="cyclical"),
        TrendField("current_phase", "cyclical.current_phase", "当前周期阶段", category="cyclical"),
        TrendField("industry_cyclical", "cyclical.industry_cyclical", "行业是否周期性", category="cyclical"),
        TrendField("has_wave_pattern", "cyclical.has_wave_pattern", "是否波浪型", category="cyclical"),
        TrendField("trend_r_squared", "cyclical.trend_r_squared", "周期趋势拟合优度", category="cyclical"),
        TrendField("cyclical_cv", "cyclical.cv", "周期CV", category="cyclical"),
        TrendField("cyclical_confidence", "cyclical.cyclical_confidence", "周期置信度", category="cyclical"),
        TrendField("cyclical_industry", "cyclical.industry", "周期判断行业", category="cyclical"),
        # 阈值曝光
        TrendField("decline_threshold_pct", "deterioration.decline_threshold_pct", "跌幅阈值(%)", unit="ratio", category="threshold"),
        TrendField("decline_threshold_abs", "deterioration.decline_threshold_abs", "跌幅阈值(绝对值)", category="threshold"),
        TrendField("peak_to_trough_threshold", "cyclical.peak_to_trough_threshold", "峰谷阈值", category="threshold"),
        TrendField("trend_r_squared_max", "cyclical.trend_r_squared_max", "趋势R²上限", category="threshold"),
        TrendField("cv_threshold", "cyclical.cv_threshold", "CV阈值", category="threshold"),
        # 滚动趋势
        TrendField("recent_3y_slope", "rolling.recent_3y_slope", "近3年斜率", category="rolling"),
        TrendField("recent_3y_r_squared", "rolling.recent_3y_r_squared", "近3年拟合优度", category="rolling"),
        TrendField("trend_acceleration", "rolling.trend_acceleration", "趋势加速度", category="rolling"),
        TrendField("is_accelerating", "rolling.is_accelerating", "是否加速", category="rolling"),
        TrendField("is_decelerating", "rolling.is_decelerating", "是否放缓", category="rolling"),
        TrendField("full_5y_slope", "rolling.full_5y_slope", "5年全样本斜率", category="rolling"),
        TrendField("full_5y_r_squared", "rolling.full_5y_r_squared", "5年全样本拟合优度", category="rolling"),
    ]

def get_default_fields() -> Tuple[TrendField, ...]:
    return tuple(trend_field_schema())
