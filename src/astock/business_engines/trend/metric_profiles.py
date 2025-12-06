"""
指标特性配置模块 (Metric Profiles)
===================================

不同财务指标具有截然不同的数学特性和业务含义。
使用统一的分析方法会导致"张冠李戴"的错误判断。

本模块定义了三大指标类别的专属参数：
1. **规模指标 (Scale Metrics)**: 营收、利润、现金流等绝对值指标
2. **效率指标 (Efficiency Metrics)**: ROE、ROIC、毛利率等比率指标
3. **增量指标 (Incremental Metrics)**: ROIIC等边际变化指标

核心理念：
- 规模指标：关注增长率和加速度
- 效率指标：关注绝对水平和稳定性
- 增量指标：关注正负号和趋势方向

作者: AStock Analysis System
日期: 2025-12-06
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import re


class MetricCategory(Enum):
    """指标类别枚举"""
    SCALE = "scale"              # 规模型（营收、利润）
    EFFICIENCY = "efficiency"    # 效率型（ROE、毛利率）
    CASH_FLOW = "cash_flow"      # 现金流型（OCF、FCF）
    INCREMENTAL = "incremental"  # 增量型（ROIIC）
    UNKNOWN = "unknown"


@dataclass
class MetricProfile:
    """
    单个指标的完整配置档案

    包含：
    - 分析参数（权重、阈值）
    - 规则参数（否决条件、扣分系数）
    - 策略参数（高增长门槛、护城河标准）
    - 交叉验证（参考指标列表）
    """

    # === 基本信息 ===
    name: str                           # 指标代码 (如 'roic', 'eps')
    display_name: str                   # 显示名称 (如 '投入资本回报率')
    category: MetricCategory            # 指标类别
    unit: str = "ratio"                 # 单位 ('ratio', 'percent', 'absolute', 'per_share')

    # === 分析参数 ===
    # 趋势分析适用性
    use_log_transform: bool = True      # 是否使用对数变换（规模指标通常True，效率指标可False）
    allow_negative: bool = False        # 是否允许负值（利润/现金流允许，ROE通常不应为负）

    # 加权方案
    time_weights: List[float] = field(default_factory=lambda: [0.1, 0.15, 0.2, 0.25, 0.3])

    # 波动性容忍度
    cv_threshold_high: float = 0.5      # CV超过此值视为高波动
    cv_threshold_low: float = 0.15      # CV低于此值视为超稳定

    # === 规则参数 ===
    # 否决规则
    min_latest_value: Optional[float] = None      # 最新值门槛（如ROE>=8%）
    max_decline_pct: float = 60.0                 # 最大允许跌幅
    max_loss_years: int = 3                       # 最大允许亏损年数

    # 扣分参数
    penalty_factor: float = 20.0                  # 扣分系数
    severe_decline_slope: float = -0.30           # 严重衰退斜率阈值
    mild_decline_slope: float = -0.15             # 轻度衰退斜率阈值

    # === 策略参数 ===
    # 高增长策略
    high_growth_threshold: float = 0.20           # 高增长斜率阈值
    min_value_for_growth: Optional[float] = None  # 增长有效的最低值（防止小基数）

    # 护城河策略（效率指标专用）
    moat_threshold: Optional[float] = None        # 护城河门槛（如毛利率>40%）
    moat_stability_required: bool = False         # 是否要求趋势稳定

    # === 交叉验证 ===
    reference_metrics: List[str] = field(default_factory=list)  # 参考指标列表
    quality_check_metrics: List[str] = field(default_factory=list)  # 质量验证指标

    # === 特殊处理 ===
    is_auxiliary: bool = False          # 是否为辅助指标（如ROIIC不做一票否决）
    cyclical_sensitive: bool = False    # 是否对周期敏感

    def get_analysis_config(self) -> Dict[str, Any]:
        """导出为趋势分析引擎的配置格式"""
        return {
            "min_latest_value": self.min_latest_value,
            "log_severe_decline_slope": self.severe_decline_slope,
            "log_mild_decline_slope": self.mild_decline_slope,
            "penalty_factor": self.penalty_factor,
            "max_penalty": min(self.penalty_factor, 25),
            "reference_metrics": self.reference_metrics,
        }


# ==============================================================================
# 预定义指标档案库 (Metric Profile Library)
# ==============================================================================

METRIC_PROFILES: Dict[str, MetricProfile] = {

    # ========== 效率指标 (Efficiency Metrics) ==========

    "roic": MetricProfile(
        name="roic",
        display_name="投入资本回报率",
        category=MetricCategory.EFFICIENCY,
        unit="percent",
        use_log_transform=False,          # 效率指标不需要对数变换
        allow_negative=False,             # ROIC为负是严重问题
        min_latest_value=6.0,             # WACC约8%，底线6%
        cv_threshold_high=0.4,            # 效率指标波动应较小
        severe_decline_slope=-0.25,       # 效率指标下跌更敏感
        mild_decline_slope=-0.10,
        moat_threshold=15.0,              # ROIC>15%是真正的护城河
        moat_stability_required=True,     # 护城河需要稳定
        reference_metrics=["roe", "roiic"],
        quality_check_metrics=["ocfps"],
        cyclical_sensitive=True,          # ROIC受周期影响
    ),

    "roe": MetricProfile(
        name="roe",
        display_name="净资产收益率",
        category=MetricCategory.EFFICIENCY,
        unit="percent",
        use_log_transform=False,
        allow_negative=False,
        min_latest_value=8.0,             # ROE>8%是底线
        cv_threshold_high=0.35,
        severe_decline_slope=-0.20,       # ROE大跌是严重信号
        mild_decline_slope=-0.08,
        moat_threshold=15.0,              # ROE>15%是优质
        moat_stability_required=True,
        reference_metrics=["roic", "netprofit_margin"],
        quality_check_metrics=["ocfps", "grossprofit_margin"],  # 杜邦分解验证
    ),

    "grossprofit_margin": MetricProfile(
        name="grossprofit_margin",
        display_name="毛利率",
        category=MetricCategory.EFFICIENCY,
        unit="percent",
        use_log_transform=False,
        allow_negative=False,             # 毛利率不应为负
        min_latest_value=15.0,            # 毛利率<15%通常是低端制造
        cv_threshold_high=0.25,           # 毛利率应该非常稳定
        cv_threshold_low=0.08,
        severe_decline_slope=-0.15,       # 毛利率下跌是护城河侵蚀
        mild_decline_slope=-0.05,
        moat_threshold=40.0,              # 毛利率>40%是强护城河
        moat_stability_required=True,
        high_growth_threshold=0.05,       # 毛利率增长5%就很好了
        reference_metrics=["netprofit_margin"],
    ),

    "netprofit_margin": MetricProfile(
        name="netprofit_margin",
        display_name="净利率",
        category=MetricCategory.EFFICIENCY,
        unit="percent",
        use_log_transform=False,
        allow_negative=True,              # 净利率可能为负（亏损）
        min_latest_value=5.0,             # 净利率<5%说明竞争激烈
        cv_threshold_high=0.4,
        severe_decline_slope=-0.20,
        mild_decline_slope=-0.10,
        moat_threshold=15.0,
        reference_metrics=["grossprofit_margin", "roe"],
        quality_check_metrics=["ocfps"],  # 净利率要与现金流交叉验证
    ),

    # ========== 规模指标 (Scale Metrics) ==========

    "total_revenue_ps": MetricProfile(
        name="total_revenue_ps",
        display_name="每股营业收入",
        category=MetricCategory.SCALE,
        unit="per_share",
        use_log_transform=True,           # 规模指标需要对数变换
        allow_negative=False,
        min_latest_value=None,            # 营收没有绝对门槛，看增速
        cv_threshold_high=0.6,            # 规模指标容忍更高波动
        severe_decline_slope=-0.35,       # 营收下跌35%是严重问题
        mild_decline_slope=-0.15,
        high_growth_threshold=0.15,       # 营收增长15%是高成长
        min_value_for_growth=0.5,         # 每股营收>0.5才算有效基数
        reference_metrics=["roe"],        # 营收增长要看ROE是否跟上
        quality_check_metrics=["ocfps"],
    ),

    "eps": MetricProfile(
        name="eps",
        display_name="每股收益",
        category=MetricCategory.SCALE,
        unit="per_share",
        use_log_transform=True,
        allow_negative=True,              # EPS可以为负（亏损）
        min_latest_value=0.1,             # EPS>0.1元是底线
        max_loss_years=2,                 # 最多允许2年亏损
        cv_threshold_high=0.7,            # 利润波动通常较大
        severe_decline_slope=-0.40,
        mild_decline_slope=-0.20,
        high_growth_threshold=0.20,       # 利润增长20%是高成长
        reference_metrics=["ocfps"],      # 利润必须与现金流交叉验证
        quality_check_metrics=["ocfps", "grossprofit_margin"],
    ),

    # ========== 现金流指标 (Cash Flow Metrics) ==========

    "ocfps": MetricProfile(
        name="ocfps",
        display_name="每股经营现金流",
        category=MetricCategory.CASH_FLOW,
        unit="per_share",
        use_log_transform=True,
        allow_negative=True,              # OCF可以为负（但不好）
        min_latest_value=0.0,             # OCF必须为正
        cv_threshold_high=0.8,            # 现金流波动可能很大
        severe_decline_slope=-0.50,       # 现金流恶化是重大风险
        mild_decline_slope=-0.20,
        high_growth_threshold=0.15,
        reference_metrics=["eps"],        # 现金流与利润交叉验证
        cyclical_sensitive=True,          # 现金流受周期影响
    ),

    # ========== 增量指标 (Incremental Metrics) ==========

    "roiic": MetricProfile(
        name="roiic",
        display_name="增量资本回报率",
        category=MetricCategory.INCREMENTAL,
        unit="percent",
        use_log_transform=False,
        allow_negative=True,              # ROIIC经常为负（扩张期）
        min_latest_value=None,            # ROIIC没有绝对门槛
        cv_threshold_high=1.0,            # ROIIC波动极大是正常的
        severe_decline_slope=-0.50,       # ROIIC大跌很常见
        mild_decline_slope=-0.20,
        penalty_factor=10.0,              # 降低ROIIC的扣分权重
        is_auxiliary=True,                # ROIIC是辅助指标，不做一票否决
        reference_metrics=["roic"],       # 与ROIC交叉验证
    ),
}


# ==============================================================================
# 指标类型识别器 (Metric Type Detector)
# ==============================================================================

# 指标名称模式匹配规则
_PATTERN_RULES: List[tuple] = [
    # 效率指标
    (r"roic", MetricCategory.EFFICIENCY),
    (r"roe", MetricCategory.EFFICIENCY),
    (r"roa", MetricCategory.EFFICIENCY),
    (r"margin", MetricCategory.EFFICIENCY),
    (r"rate", MetricCategory.EFFICIENCY),
    (r"ratio", MetricCategory.EFFICIENCY),
    (r"yield", MetricCategory.EFFICIENCY),
    (r"turnover", MetricCategory.EFFICIENCY),

    # 现金流指标
    (r"ocf", MetricCategory.CASH_FLOW),
    (r"fcf", MetricCategory.CASH_FLOW),
    (r"cash", MetricCategory.CASH_FLOW),

    # 增量指标
    (r"roiic", MetricCategory.INCREMENTAL),
    (r"incremental", MetricCategory.INCREMENTAL),
    (r"delta", MetricCategory.INCREMENTAL),

    # 规模指标（默认）
    (r"revenue", MetricCategory.SCALE),
    (r"profit", MetricCategory.SCALE),
    (r"eps", MetricCategory.SCALE),
    (r"income", MetricCategory.SCALE),
    (r"sales", MetricCategory.SCALE),
    (r"asset", MetricCategory.SCALE),
    (r"equity", MetricCategory.SCALE),
]


def detect_metric_category(metric_name: str) -> MetricCategory:
    """
    根据指标名称自动识别类别

    Args:
        metric_name: 指标名称

    Returns:
        MetricCategory 枚举值
    """
    name_lower = metric_name.lower()

    for pattern, category in _PATTERN_RULES:
        if re.search(pattern, name_lower):
            return category

    return MetricCategory.UNKNOWN


def get_metric_profile(metric_name: str) -> MetricProfile:
    """
    获取指标配置档案

    优先从预定义库中查找，否则根据类别自动生成默认配置。

    Args:
        metric_name: 指标名称

    Returns:
        MetricProfile 配置对象
    """
    name_lower = metric_name.lower()

    # 1. 精确匹配预定义档案
    if name_lower in METRIC_PROFILES:
        return METRIC_PROFILES[name_lower]

    # 2. 模糊匹配（处理带前缀/后缀的情况）
    for key, profile in METRIC_PROFILES.items():
        if key in name_lower or name_lower in key:
            return profile

    # 3. 根据类别生成默认档案
    category = detect_metric_category(metric_name)

    if category == MetricCategory.EFFICIENCY:
        return MetricProfile(
            name=metric_name,
            display_name=metric_name,
            category=category,
            use_log_transform=False,
            min_latest_value=5.0,
            moat_threshold=15.0,
        )
    elif category == MetricCategory.CASH_FLOW:
        return MetricProfile(
            name=metric_name,
            display_name=metric_name,
            category=category,
            use_log_transform=True,
            allow_negative=True,
            cv_threshold_high=0.8,
        )
    elif category == MetricCategory.INCREMENTAL:
        return MetricProfile(
            name=metric_name,
            display_name=metric_name,
            category=category,
            use_log_transform=False,
            allow_negative=True,
            is_auxiliary=True,
            penalty_factor=10.0,
        )
    else:  # SCALE or UNKNOWN
        return MetricProfile(
            name=metric_name,
            display_name=metric_name,
            category=MetricCategory.SCALE,
            use_log_transform=True,
            high_growth_threshold=0.15,
        )


def get_cross_validation_pairs() -> List[tuple]:
    """
    获取所有交叉验证配对关系

    返回 (指标A, 指标B, 验证类型) 的列表
    """
    pairs = [
        # 利润与现金流交叉验证（含金量检验）
        ("eps", "ocfps", "cash_quality"),
        ("netprofit_margin", "ocfps", "cash_quality"),

        # 效率指标一致性（杜邦分解）
        ("roe", "netprofit_margin", "dupont"),
        ("roe", "roic", "dupont"),
        ("grossprofit_margin", "netprofit_margin", "margin_chain"),

        # 增长可持续性（ROE vs 营收增速）
        ("total_revenue_ps", "roe", "sustainable_growth"),

        # 资本效率一致性
        ("roic", "roiic", "capital_efficiency"),
    ]
    return pairs


# ==============================================================================
# 导出
# ==============================================================================

__all__ = [
    "MetricCategory",
    "MetricProfile",
    "METRIC_PROFILES",
    "detect_metric_category",
    "get_metric_profile",
    "get_cross_validation_pairs",
]
