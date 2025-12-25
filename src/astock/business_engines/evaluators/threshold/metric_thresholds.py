"""
指标阈值配置 (Metric Thresholds)
================================

各财务指标的评估阈值配置。

此模块属于 ThresholdEvaluator 层，定义了:
- 各指标的最低要求值
- 衰退判断阈值
- 扣分系数
- 交叉验证阈值

设计原则:
- 这些是业务判断阈值，不是探针的算法参数
- 探针只计算数值，这里决定"好"还是"坏"

作者: AStock Analysis System
日期: 2025-12-12
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
import re


# ============================================================================
# 指标类别
# ============================================================================

class MetricCategory(Enum):
    """指标类别"""
    SCALE = "scale"              # 规模型（营收、利润）
    EFFICIENCY = "efficiency"    # 效率型（ROE、毛利率）
    CASH_FLOW = "cash_flow"      # 现金流型（OCF、FCF）
    INCREMENTAL = "incremental"  # 增量型（ROIIC）
    UNKNOWN = "unknown"


# ============================================================================
# 指标阈值配置
# ============================================================================

@dataclass
class MetricThresholdConfig:
    """
    单个指标的阈值配置

    定义了该指标的所有评估阈值和规则参数。
    """
    # === 基本信息 ===
    name: str
    display_name: str
    category: MetricCategory

    # === 最低要求 ===
    min_latest_value: Optional[float] = None  # 最新值底线

    # === 衰退阈值 ===
    severe_decline_slope: float = -0.30       # 严重衰退斜率
    mild_decline_slope: float = -0.15         # 轻度衰退斜率
    max_decline_pct: float = 60.0             # 最大允许跌幅 (非周期)
    max_decline_pct_cyclical: float = 75.0    # 最大允许跌幅 (周期)
    max_loss_years: int = 3                   # 最大允许亏损年数

    # === 扣分参数 ===
    penalty_factor: float = 20.0              # 扣分系数
    max_penalty: float = 25.0                 # 最大扣分

    # === 波动性阈值 ===
    cv_threshold_high: float = 0.5            # 高波动阈值
    cv_threshold_low: float = 0.15            # 低波动阈值

    # === 策略阈值 ===
    high_growth_threshold: float = 0.20       # 高增长斜率阈值
    moat_threshold: Optional[float] = None    # 护城河门槛
    min_value_for_growth: Optional[float] = None  # 增长有效的最低值

    # === 交叉验证 ===
    reference_metrics: List[str] = field(default_factory=list)
    quality_check_metrics: List[str] = field(default_factory=list)

    # === 特殊标记 ===
    is_auxiliary: bool = False                # 辅助指标（否决变警告）
    allow_negative: bool = False              # 是否允许负值
    use_log_transform: bool = True            # 是否使用对数变换
    cyclical_sensitive: bool = False          # 是否对周期敏感


# ============================================================================
# 预定义指标阈值库
# ============================================================================

METRIC_THRESHOLDS: Dict[str, MetricThresholdConfig] = {

    # ========== 效率指标 ==========

    "roic": MetricThresholdConfig(
        name="roic",
        display_name="投入资本回报率",
        category=MetricCategory.EFFICIENCY,
        min_latest_value=6.0,             # WACC约8%，底线6%
        severe_decline_slope=-0.25,
        mild_decline_slope=-0.10,
        cv_threshold_high=0.4,
        moat_threshold=15.0,              # ROIC>15%是护城河
        reference_metrics=["roe", "roiic"],
        quality_check_metrics=["ocfps"],
        use_log_transform=False,
        cyclical_sensitive=True,
    ),

    "roe": MetricThresholdConfig(
        name="roe",
        display_name="净资产收益率",
        category=MetricCategory.EFFICIENCY,
        min_latest_value=8.0,
        severe_decline_slope=-0.20,
        mild_decline_slope=-0.08,
        cv_threshold_high=0.35,
        moat_threshold=15.0,
        reference_metrics=["roic", "netprofit_margin"],
        quality_check_metrics=["ocfps", "grossprofit_margin"],
        use_log_transform=False,
    ),

    "grossprofit_margin": MetricThresholdConfig(
        name="grossprofit_margin",
        display_name="毛利率",
        category=MetricCategory.EFFICIENCY,
        min_latest_value=15.0,
        severe_decline_slope=-0.15,
        mild_decline_slope=-0.05,
        cv_threshold_high=0.25,
        cv_threshold_low=0.08,
        high_growth_threshold=0.05,
        moat_threshold=40.0,              # 毛利率>40%是强护城河
        reference_metrics=["netprofit_margin"],
        use_log_transform=False,
    ),

    "netprofit_margin": MetricThresholdConfig(
        name="netprofit_margin",
        display_name="净利率",
        category=MetricCategory.EFFICIENCY,
        min_latest_value=5.0,
        severe_decline_slope=-0.20,
        mild_decline_slope=-0.10,
        cv_threshold_high=0.4,
        moat_threshold=15.0,
        reference_metrics=["grossprofit_margin", "roe"],
        quality_check_metrics=["ocfps"],
        use_log_transform=False,
        allow_negative=True,
    ),

    # ========== 规模指标 ==========

    "total_revenue_ps": MetricThresholdConfig(
        name="total_revenue_ps",
        display_name="每股营业收入",
        category=MetricCategory.SCALE,
        min_latest_value=None,            # 营收无绝对门槛
        severe_decline_slope=-0.35,
        mild_decline_slope=-0.15,
        cv_threshold_high=0.6,
        high_growth_threshold=0.15,
        min_value_for_growth=0.5,
        reference_metrics=["roe", "eps"],
        quality_check_metrics=["ocfps"],
        use_log_transform=True,
    ),

    "eps": MetricThresholdConfig(
        name="eps",
        display_name="每股收益",
        category=MetricCategory.SCALE,
        min_latest_value=0.1,
        max_loss_years=2,
        severe_decline_slope=-0.40,
        mild_decline_slope=-0.20,
        cv_threshold_high=0.7,
        high_growth_threshold=0.20,
        reference_metrics=["ocfps"],
        quality_check_metrics=["ocfps", "grossprofit_margin"],
        use_log_transform=True,
        allow_negative=True,
    ),

    # ========== 现金流指标 ==========

    "ocfps": MetricThresholdConfig(
        name="ocfps",
        display_name="每股经营现金流",
        category=MetricCategory.CASH_FLOW,
        min_latest_value=0.0,             # OCF必须为正
        severe_decline_slope=-0.50,
        mild_decline_slope=-0.20,
        cv_threshold_high=0.8,
        high_growth_threshold=0.15,
        reference_metrics=["eps"],
        use_log_transform=True,
        allow_negative=True,
        cyclical_sensitive=True,
    ),

    # ========== 增量指标 ==========

    "roiic": MetricThresholdConfig(
        name="roiic",
        display_name="增量资本回报率",
        category=MetricCategory.INCREMENTAL,
        min_latest_value=None,            # ROIIC无绝对门槛
        severe_decline_slope=-0.50,
        mild_decline_slope=-0.20,
        cv_threshold_high=1.0,
        penalty_factor=10.0,              # 降低扣分权重
        reference_metrics=["roic"],
        is_auxiliary=True,                # 辅助指标
        use_log_transform=False,
        allow_negative=True,
    ),
}


# ============================================================================
# 简化配置格式 (兼容旧代码)
# ============================================================================

METRIC_FILTER_CONFIGS: Dict[str, Dict[str, Any]] = {
    "roic": {
        "min_latest_value": 0.06,
        "severe_decline": -0.30,
        "mild_decline": -0.15,
        "is_auxiliary": False,
    },
    "roiic": {
        "min_latest_value": None,
        "severe_decline": -0.35,
        "mild_decline": -0.20,
        "is_auxiliary": True,
    },
    "roe": {
        "min_latest_value": 0.08,
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
    },
    "grossprofit_margin": {
        "min_latest_value": 0.15,
        "severe_decline": -0.20,
        "mild_decline": -0.10,
        "is_auxiliary": False,
    },
    "netprofit_margin": {
        "min_latest_value": 0.03,
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
    },
    "total_revenue_ps": {
        "min_latest_value": None,
        "severe_decline": -0.20,
        "mild_decline": -0.08,
        "is_auxiliary": False,
    },
    "eps": {
        "min_latest_value": 0.0,
        "severe_decline": -0.30,
        "mild_decline": -0.15,
        "is_auxiliary": False,
    },
    "ocfps": {
        "min_latest_value": None,
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
    },
}

DEFAULT_METRIC_CONFIG: Dict[str, Any] = {
    "min_latest_value": None,
    "severe_decline": -0.30,
    "mild_decline": -0.15,
    "is_auxiliary": False,
}


# ============================================================================
# 指标类型检测
# ============================================================================

_PATTERN_RULES = [
    # 效率指标
    (r"roic", MetricCategory.EFFICIENCY),
    (r"roe", MetricCategory.EFFICIENCY),
    (r"roa", MetricCategory.EFFICIENCY),
    (r"margin", MetricCategory.EFFICIENCY),
    (r"rate", MetricCategory.EFFICIENCY),
    (r"ratio", MetricCategory.EFFICIENCY),
    (r"turnover", MetricCategory.EFFICIENCY),

    # 现金流指标
    (r"ocf", MetricCategory.CASH_FLOW),
    (r"fcf", MetricCategory.CASH_FLOW),
    (r"cash", MetricCategory.CASH_FLOW),

    # 增量指标
    (r"roiic", MetricCategory.INCREMENTAL),
    (r"incremental", MetricCategory.INCREMENTAL),
    (r"delta", MetricCategory.INCREMENTAL),

    # 规模指标
    (r"revenue", MetricCategory.SCALE),
    (r"profit", MetricCategory.SCALE),
    (r"eps", MetricCategory.SCALE),
    (r"income", MetricCategory.SCALE),
    (r"sales", MetricCategory.SCALE),
]


def detect_metric_category(metric_name: str) -> MetricCategory:
    """根据指标名称自动识别类别"""
    name_lower = metric_name.lower()

    for pattern, category in _PATTERN_RULES:
        if re.search(pattern, name_lower):
            return category

    return MetricCategory.UNKNOWN


# ============================================================================
# 查询函数
# ============================================================================

def get_metric_thresholds(metric_name: str) -> MetricThresholdConfig:
    """
    获取指标阈值配置

    优先精确匹配，然后模糊匹配，最后根据类别生成默认配置。
    """
    name_lower = metric_name.lower()

    # 精确匹配
    if name_lower in METRIC_THRESHOLDS:
        return METRIC_THRESHOLDS[name_lower]

    # 模糊匹配
    for key, config in METRIC_THRESHOLDS.items():
        if key in name_lower or name_lower in key:
            return config

    # 根据类别生成默认配置
    category = detect_metric_category(metric_name)

    if category == MetricCategory.EFFICIENCY:
        return MetricThresholdConfig(
            name=metric_name,
            display_name=metric_name,
            category=category,
            min_latest_value=5.0,
            moat_threshold=15.0,
            use_log_transform=False,
        )
    elif category == MetricCategory.CASH_FLOW:
        return MetricThresholdConfig(
            name=metric_name,
            display_name=metric_name,
            category=category,
            cv_threshold_high=0.8,
            allow_negative=True,
        )
    elif category == MetricCategory.INCREMENTAL:
        return MetricThresholdConfig(
            name=metric_name,
            display_name=metric_name,
            category=category,
            is_auxiliary=True,
            penalty_factor=10.0,
            allow_negative=True,
            use_log_transform=False,
        )
    else:
        return MetricThresholdConfig(
            name=metric_name,
            display_name=metric_name,
            category=MetricCategory.SCALE,
            high_growth_threshold=0.15,
        )


def get_metric_filter_config(metric_name: str) -> Dict[str, Any]:
    """获取指标过滤配置（简化格式，兼容旧代码）"""
    metric_lower = metric_name.lower().strip()
    return METRIC_FILTER_CONFIGS.get(metric_lower, DEFAULT_METRIC_CONFIG).copy()


# ============================================================================
# 交叉验证配对
# ============================================================================

def get_cross_validation_pairs() -> List[tuple]:
    """获取所有交叉验证配对关系"""
    return [
        # 利润与现金流（含金量检验）
        ("eps", "ocfps", "cash_quality"),
        ("netprofit_margin", "ocfps", "cash_quality"),

        # 效率指标一致性（杜邦分解）
        ("roe", "netprofit_margin", "dupont"),
        ("roe", "roic", "dupont"),
        ("grossprofit_margin", "netprofit_margin", "margin_chain"),

        # 增长可持续性
        ("total_revenue_ps", "roe", "sustainable_growth"),

        # 资本效率一致性
        ("roic", "roiic", "capital_efficiency"),
    ]


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    'MetricCategory',
    'MetricThresholdConfig',
    'METRIC_THRESHOLDS',
    'METRIC_FILTER_CONFIGS',
    'DEFAULT_METRIC_CONFIG',
    'detect_metric_category',
    'get_metric_thresholds',
    'get_metric_filter_config',
    'get_cross_validation_pairs',
]
