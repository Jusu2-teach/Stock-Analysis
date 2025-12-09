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

    # 周期性行业 (按A股实际分类，覆盖申万一级/二级)
    # 注意：证券、养殖等高周期行业也应纳入
    cyclical_industries: List[str] = field(default_factory=lambda: [
        # --- 上游资源 (大宗商品周期) ---
        "小金属", "黄金", "钢铁", "煤炭", "有色金属", "石油石化",
        "铝", "铜", "锌", "稀土", "锂",  # 细分金属

        # --- 化工 (原材料周期) ---
        "化工", "化学制品", "基础化工", "化学纤维", "化肥", "农药",

        # --- 建材地产 (房地产周期) ---
        "建材", "水泥", "玻璃", "房地产", "建筑", "建筑材料",
        "装饰装修", "房地产服务",

        # --- 交通运输 (贸易周期) ---
        "航运", "港口", "交运设备", "远洋运输", "集装箱",

        # --- 机械设备 (资本开支周期) ---
        "机械", "专用设备", "通用设备", "工程机械", "重型机械",

        # --- 汽车 (可选消费周期) ---
        "汽车", "汽车零部件", "乘用车", "商用车",

        # --- 金融 (信用周期) ---
        "证券", "保险", "多元金融",  # 金融是顺周期的

        # --- 农业 (养殖周期/猪周期) ---
        "养殖", "猪肉", "禽畜养殖", "饲料", "农产品加工",

        # --- 其他周期性 ---
        "造纸", "包装印刷", "轻工制造", "家居用品",
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
            # 指数衰减：越近的年份权重越高
            # 例如 window=5, decay=0.8: [0.8^4, 0.8^3, 0.8^2, 0.8^1, 0.8^0] = [0.41, 0.51, 0.64, 0.8, 1.0]
            indices = np.arange(window_size)
            weights = np.power(decay_factor, (window_size - 1 - indices))
        elif method == "linear":
            # 线性递增
            weights = np.arange(1, window_size + 1, dtype=float)
        else:
            # fallback to default behavior
            if window_size == len(self.default_weights):
                return self.default_weights
            weights = np.arange(1, window_size + 1, dtype=float)

        # 归一化
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

# 行业分类映射 (按申万行业分类，映射到四大类别)
# v2.0: 新增 cyclical_growth 类别，细化电力分类
_INDUSTRY_CATEGORY_MAP = {
    # --- 强周期性行业 (Cyclical) ---
    # 上游资源
    "小金属": "cyclical", "黄金": "cyclical", "钢铁": "cyclical", "煤炭": "cyclical",
    "有色金属": "cyclical", "石油石化": "cyclical", "铝": "cyclical", "铜": "cyclical",
    "锌": "cyclical", "稀土": "cyclical",
    # 化工
    "化工": "cyclical", "基础化工": "cyclical", "化学纤维": "cyclical",
    "化肥": "cyclical", "农药": "cyclical", "化学制品": "cyclical",
    # 建材地产
    "建材": "cyclical", "水泥": "cyclical", "玻璃": "cyclical",
    "房地产": "cyclical", "建筑": "cyclical", "装饰装修": "cyclical",
    # 交运（周期部分）
    "航运": "cyclical", "港口": "cyclical", "远洋运输": "cyclical", "集装箱": "cyclical",
    # 机械
    "工程机械": "cyclical", "重型机械": "cyclical", "机械": "cyclical",
    "专用设备": "cyclical", "通用设备": "cyclical",
    # 汽车
    "汽车": "cyclical", "汽车零部件": "cyclical", "乘用车": "cyclical", "商用车": "cyclical",
    # 金融 (顺周期)
    "证券": "cyclical", "保险": "cyclical", "多元金融": "cyclical",
    # 农业周期
    "养殖": "cyclical", "猪肉": "cyclical", "禽畜养殖": "cyclical",
    # 其他周期
    "造纸": "cyclical", "包装印刷": "cyclical",
    # 电力（周期部分）- 火电与煤价反向相关
    "火电": "cyclical", "热电": "cyclical",

    # --- 周期成长行业 (Cyclical Growth) ---
    # 新能源（产能周期 + 成长属性）
    "新能源": "cyclical_growth", "光伏设备": "cyclical_growth",
    "电池": "cyclical_growth", "锂": "cyclical_growth",  # 锂移到周期成长
    "风电设备": "cyclical_growth", "储能": "cyclical_growth",
    "新能源车": "cyclical_growth",
    # 半导体（产能周期 + 成长属性）
    "半导体": "cyclical_growth", "元件": "cyclical_growth",
    # 面板/显示（强周期成长）
    "光学光电子": "cyclical_growth",

    # --- 成长性行业 (Growth) ---
    # 医药
    "医药": "growth", "生物制药": "growth", "医疗器械": "growth", "医疗服务": "growth",
    "创新药": "growth", "CXO": "growth",
    # 科技
    "电子": "growth", "计算机": "growth", "软件": "growth", "互联网": "growth",
    "通信设备": "growth", "人工智能": "growth", "云计算": "growth",
    # 高端制造
    "航空航天": "growth", "军工": "growth", "自动化设备": "growth",
    "机器人": "growth", "工业母机": "growth",

    # --- 防御性/稳定行业 (Defensive) ---
    # 消费
    "食品饮料": "defensive", "白酒": "defensive", "饮料制造": "defensive",
    "食品加工": "defensive", "调味品": "defensive", "乳制品": "defensive",
    # 农业(非周期部分)
    "农林牧渔": "defensive", "种植业": "defensive", "饲料": "defensive",
    # 公用事业（稳定部分）
    "公用事业": "defensive", "水务": "defensive", "燃气": "defensive",
    "环保": "defensive",
    # 电力（稳定部分）- 水电/核电/绿电具有防御性
    "水电": "defensive", "核电": "defensive", "绿色电力": "defensive",
    "电力": "defensive",  # 混合电力默认归defensive，但火电单独归cyclical
    # 交运(稳定部分)
    "交通运输": "defensive", "高速公路": "defensive", "机场": "defensive", "铁路": "defensive",
    # 金融(稳定部分)
    "银行": "defensive",  # 银行相对稳健
    # 家电
    "家电": "defensive", "白色家电": "defensive", "小家电": "defensive",
    # 其他防御
    "纺织服装": "defensive", "商贸零售": "defensive",
}

# ============================================================================
# 周期性阈值（v2.0: 增加去趋势CV区分成长vs周期）
# ============================================================================
#
# 关键洞察：爆发性成长股和强周期股在原始CV上可能相似，但去趋势后差异明显
#
# 区分逻辑：
#   - 高成长股: 原始CV高，但去趋势CV低（波动来自趋势本身）
#   - 周期股: 原始CV高，去趋势CV也高（波动来自周期往复）
#   - 稳定股: 两个CV都低
#
# detrended_cv_ratio = detrended_cv / raw_cv
#   - ratio < 0.5: 波动主要来自趋势 → 成长特征
#   - ratio > 0.7: 波动主要来自周期/噪音 → 周期特征
#
_CYCLICAL_THRESHOLDS = {
    "cyclical": {
        "cv_threshold": 0.3,            # 原始CV阈值（触发周期检测的门槛）
        "detrended_cv_min": 0.15,       # 去趋势CV下限（真周期股应该有）
        "detrended_cv_ratio_min": 0.6,  # 去趋势CV/原始CV > 0.6 才是真周期
        "peak_valley_ratio": 2.0,       # 峰谷比 > 2 即视为显著周期
        "r_squared_low": 0.4,           # 允许趋势拟合度较低
        "deterioration_tolerance": 0.35, # 周期底部容忍更大回撤（-35%不算恶化）
    },
    "growth": {
        "cv_threshold": 0.5,            # 成长股原始CV可以较高
        "detrended_cv_max": 0.20,       # 但去趋势CV应该较低（波动来自趋势）
        "detrended_cv_ratio_max": 0.5,  # 去趋势CV/原始CV < 0.5 才是真成长
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
        "deterioration_tolerance": 0.25, # 成长股回撤容忍度中等
    },
    "cyclical_growth": {
        # 新类别：周期成长（光伏、锂电、半导体等）
        # 同时具有成长属性和周期属性
        "cv_threshold": 0.4,
        "detrended_cv_min": 0.12,
        "detrended_cv_max": 0.30,
        "peak_valley_ratio": 2.5,
        "r_squared_low": 0.45,
        "deterioration_tolerance": 0.30, # 介于周期和成长之间
    },
    "defensive": {
        "cv_threshold": 0.6,            # 防御股应该很稳，CV阈值高
        "detrended_cv_max": 0.15,       # 去趋势CV也应该很低
        "peak_valley_ratio": 4.0,
        "r_squared_low": 0.6,           # 要求较高的趋势平滑度
        "deterioration_tolerance": 0.20, # 防御股-20%就是大问题
    },
    "default": {
        "cv_threshold": 0.5,
        "detrended_cv_min": 0.10,
        "detrended_cv_max": 0.25,
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
        "deterioration_tolerance": 0.25,
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
    # === 高壁垒/品牌溢价行业: 要求 > 12% (显著创造价值) ===
    "食品饮料": {"min_roic": 0.12, "min_slope": -0.02},
    "白酒": {"min_roic": 0.15, "min_slope": -0.02},  # 白酒护城河极深
    "调味品": {"min_roic": 0.12, "min_slope": -0.02},
    "家电": {"min_roic": 0.10, "min_slope": -0.02},
    "白色家电": {"min_roic": 0.12, "min_slope": -0.02},

    # === 医药健康: 分化严重，研发投入大 ===
    "医药": {"min_roic": 0.08, "min_slope": -0.02},
    "医疗器械": {"min_roic": 0.10, "min_slope": -0.02},
    "医疗服务": {"min_roic": 0.08, "min_slope": -0.02},
    "生物制药": {"min_roic": 0.06, "min_slope": -0.03},  # 研发期可能亏损

    # === 科技/成长行业: 看重增长，对当前回报宽容 ===
    "电子": {"min_roic": 0.06, "min_slope": -0.03},
    "半导体": {"min_roic": 0.05, "min_slope": -0.04},  # 重资本开支周期
    "计算机": {"min_roic": 0.06, "min_slope": -0.03},
    "软件": {"min_roic": 0.08, "min_slope": -0.03},    # 轻资产应更高
    "互联网": {"min_roic": 0.08, "min_slope": -0.03},  # 轻资产高回报
    "通信": {"min_roic": 0.06, "min_slope": -0.03},

    # === 新能源: 重资本开支期，阈值降低 ===
    "新能源": {"min_roic": 0.05, "min_slope": -0.04},
    "光伏设备": {"min_roic": 0.06, "min_slope": -0.04},
    "电池": {"min_roic": 0.06, "min_slope": -0.04},
    "风电设备": {"min_roic": 0.05, "min_slope": -0.04},

    # === 资本密集/重资产行业: 要求 > 6% (接近WACC即可) ===
    "公用事业": {"min_roic": 0.06, "min_slope": -0.01},
    "电力": {"min_roic": 0.06, "min_slope": -0.01},
    "水务": {"min_roic": 0.05, "min_slope": -0.01},
    "燃气": {"min_roic": 0.06, "min_slope": -0.01},
    "交通运输": {"min_roic": 0.05, "min_slope": -0.01},
    "高速公路": {"min_roic": 0.06, "min_slope": -0.01},
    "机场": {"min_roic": 0.06, "min_slope": -0.02},

    # === 周期性行业: 底部宽容，斜率重要 ===
    "钢铁": {"min_roic": 0.04, "min_slope": -0.06},  # 周期底部可能很低
    "煤炭": {"min_roic": 0.08, "min_slope": -0.06},  # 资源属性，均值应较高
    "有色金属": {"min_roic": 0.05, "min_slope": -0.06},
    "化工": {"min_roic": 0.06, "min_slope": -0.05},
    "建材": {"min_roic": 0.06, "min_slope": -0.05},
    "房地产": {"min_roic": 0.04, "min_slope": -0.06}, # 当前行业困境

    # === 金融行业: 特殊处理 ===
    "银行": {"min_roic": 0.10, "min_slope": -0.02},   # 用ROE更合适
    "证券": {"min_roic": 0.06, "min_slope": -0.06},   # 强周期
    "保险": {"min_roic": 0.08, "min_slope": -0.03},

    # === 默认: 6% (底线思维，至少要覆盖大部分债务成本) ===
    "default": {"min_roic": 0.06, "min_slope": -0.05},
}

# ROIIC过滤配置 (增量资本回报率)
# v2.0: 增加平滑窗口配置，解决单年ROIIC噪音问题
#
# 关键改进：
# 1. 使用3年滚动累计：Rolling 3-Year ROIIC = Σ(ΔProfit_3y) / Σ(ΔIC_3y)
# 2. 当 ΔIC < 0 或 ΔIC 过小时，ROIIC 无意义，应标记为 NA
# 3. min_delta_ic_ratio: ΔIC/IC_start 的最小阈值，低于此值视为无意义
#
_ROIIC_FILTER_CONFIGS = {
    "医药": {
        "min_roiic": 0.06,
        "min_slope": -0.02,
        "smoothing_window": 3,           # 3年滚动累计
        "min_delta_ic_ratio": 0.05,      # ΔIC/IC_start > 5% 才计算ROIIC
    },
    "食品饮料": {
        "min_roiic": 0.10,
        "min_slope": -0.02,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.05,
    },
    "电子": {
        "min_roiic": 0.05,
        "min_slope": -0.03,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.08,      # 电子行业资本开支波动大，阈值更高
    },
    "半导体": {
        "min_roiic": 0.04,
        "min_slope": -0.04,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.10,      # 半导体重资本开支，阈值最高
    },
    "default": {
        "min_roiic": 0.04,
        "min_slope": -0.05,
        "smoothing_window": 3,           # 默认3年平滑
        "min_delta_ic_ratio": 0.05,      # 默认5%
    },
}

# ============================================================================
# 指标专属过滤配置 (Metric-Specific Filter Configs)
# ============================================================================
# 设计原则:
# 1. 让数据说话 - 不同指标有不同的分布特征和业务含义
# 2. min_latest_value: 最新值的底线门槛
#    - None 表示不设下限（如营收、现金流可以为负或很小）
#    - 比例型指标(毛利率等)使用绝对值
# 3. severe_decline: 严重衰退的log斜率阈值（触发一票否决）
# 4. mild_decline: 轻度衰退的log斜率阈值（触发扣分）
# 5. is_auxiliary: 是否为辅助指标（辅助指标的否决规则变为警告）

METRIC_FILTER_CONFIGS: Dict[str, Dict[str, Any]] = {
    # === 核心盈利能力指标 ===
    "roic": {
        "min_latest_value": 0.06,      # 6% - 至少覆盖债务成本
        "severe_decline": -0.30,       # CAGR约-26%
        "mild_decline": -0.15,         # CAGR约-14%
        "is_auxiliary": False,
        "description": "存量资本回报率，核心质量指标",
    },
    "roiic": {
        "min_latest_value": None,      # 不设下限，增量投资可能暂时为负
        "severe_decline": -0.35,       # 比ROIC更宽松
        "mild_decline": -0.20,
        "is_auxiliary": True,          # 辅助指标，否决变警告
        "description": "增量资本回报率，波动大，作为参考",
    },
    "roe": {
        "min_latest_value": 0.08,      # 8% - ROE通常应高于ROIC
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
        "description": "净资产收益率，股东回报核心指标",
    },

    # === 利润率指标 ===
    "grossprofit_margin": {
        "min_latest_value": 0.15,      # 15% - 毛利率底线
        "severe_decline": -0.20,       # 毛利率通常较稳定，衰退阈值更敏感
        "mild_decline": -0.10,
        "is_auxiliary": False,
        "description": "毛利率，反映产品竞争力和定价权",
    },
    "netprofit_margin": {
        "min_latest_value": 0.03,      # 3% - 净利率可以很低但不能太低
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
        "description": "净利率，反映综合盈利能力",
    },

    # === 增长型指标 ===
    "total_revenue_ps": {
        "min_latest_value": None,      # 营收无下限，不同行业差异巨大
        "severe_decline": -0.20,       # 营收持续下滑是严重信号
        "mild_decline": -0.08,
        "is_auxiliary": False,
        "description": "每股营收，反映规模和增长",
    },
    "eps": {
        "min_latest_value": 0.0,       # EPS不能为负（允许微利）
        "severe_decline": -0.30,       # 利润波动较大
        "mild_decline": -0.15,
        "is_auxiliary": False,
        "description": "每股收益，反映盈利能力",
    },

    # === 现金流指标 ===
    "ocfps": {
        "min_latest_value": None,      # 现金流可以暂时为负（如高增长期）
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        "is_auxiliary": False,
        "description": "每股经营现金流，反映盈利质量",
    },
}

# 默认配置（用于未知指标）
DEFAULT_METRIC_CONFIG: Dict[str, Any] = {
    "min_latest_value": None,
    "severe_decline": -0.30,
    "mild_decline": -0.15,
    "is_auxiliary": False,
    "description": "默认配置",
}


# ============================================================================
# 扣非利润配置 (Deducted Profit Config) - A股特色
# ============================================================================
# 设计原理：
# A股公司经常通过非经常性损益（卖地、政府补贴、投资收益）美化报表
# 扣非净利润更能反映主营业务的真实盈利能力
#
# 应用场景：
# 1. 当 (净利润 - 扣非净利润) / 净利润 > threshold 时，触发警告
# 2. 要求扣非净利率不能为负（排除靠补贴度日的公司）
# 3. 科技/制造行业应特别关注（政府补贴集中区）
#
DEDUCTED_PROFIT_CONFIG: Dict[str, Dict[str, Any]] = {
    # 对扣非要求严格的行业（非经常性损益频繁）
    "strict": {
        "industries": ["软件", "计算机", "互联网", "新能源", "光伏设备", "电池",
                      "半导体", "医药", "生物制药", "CXO", "军工", "航空航天"],
        "max_non_recurring_ratio": 0.30,  # 非经常性损益占比 < 30%
        "min_deducted_margin": 0.02,       # 扣非净利率 > 2%
    },
    # 对扣非要求中等的行业
    "moderate": {
        "industries": ["电子", "通信设备", "机械", "汽车", "家电"],
        "max_non_recurring_ratio": 0.40,
        "min_deducted_margin": 0.01,
    },
    # 对扣非要求宽松的行业（如金融、地产有合理投资收益）
    "lenient": {
        "industries": ["银行", "证券", "保险", "房地产", "公用事业"],
        "max_non_recurring_ratio": 0.60,
        "min_deducted_margin": 0.0,
    },
    # 默认配置
    "default": {
        "max_non_recurring_ratio": 0.40,
        "min_deducted_margin": 0.01,
    },
}


def get_deducted_profit_config(industry: str = None) -> Dict[str, Any]:
    """
    获取扣非利润配置

    Args:
        industry: 行业名称

    Returns:
        包含 max_non_recurring_ratio, min_deducted_margin 的配置
    """
    if not industry:
        return DEDUCTED_PROFIT_CONFIG["default"].copy()

    for strictness, config in DEDUCTED_PROFIT_CONFIG.items():
        if strictness == "default":
            continue
        if industry in config.get("industries", []):
            return {
                "max_non_recurring_ratio": config["max_non_recurring_ratio"],
                "min_deducted_margin": config["min_deducted_margin"],
            }

    return DEDUCTED_PROFIT_CONFIG["default"].copy()


# ============================================================================
# 周期性检测置信度配置 (Cyclicality Confidence Config)
# ============================================================================
# 基于奈奎斯特采样定理：检测周期T，至少需要2T长度的数据
# 5年数据只能可靠检测2.5年以下周期，商业周期(3-7年)需要更长数据
#
# 置信度上限：数据长度决定了分析结论的可信程度上限
# 即使数据完美符合周期特征，置信度也不能超过此上限
#
CYCLICALITY_CONFIDENCE_CONFIG: Dict[str, Any] = {
    # 数据年数 -> 置信度上限
    "confidence_ceiling_by_years": {
        5: 0.55,   # 5年数据：置信度上限55%
        6: 0.60,
        7: 0.70,   # 7年数据：能检测部分商业周期
        8: 0.75,
        9: 0.80,
        10: 0.85,  # 10年数据：较可靠
        15: 0.95,  # 15年数据：可检测完整商业周期
    },
    # 数据年数 -> 可靠检测的最大周期长度
    "max_reliable_period_by_years": {
        5: 2.5,    # 5年数据最多可靠检测2.5年周期
        7: 3.5,
        10: 5.0,
        15: 7.0,
    },
    # 当检测到的周期长度超过可靠范围时，置信度折扣因子
    "period_excess_discount": 0.6,  # 周期超出可靠范围时，置信度乘以0.6
}


def get_cyclicality_confidence_ceiling(n_years: int) -> float:
    """
    获取周期性检测的置信度上限

    Args:
        n_years: 数据年数

    Returns:
        置信度上限 (0-1)
    """
    config = CYCLICALITY_CONFIDENCE_CONFIG["confidence_ceiling_by_years"]
    # 找到不超过n_years的最大配置值
    applicable_years = [y for y in config.keys() if y <= n_years]
    if not applicable_years:
        return 0.30  # 不足5年，置信度很低
    return config[max(applicable_years)]


# ============================================================================
# 高成长 vs 周期 区分函数 (核心创新)
# ============================================================================

def classify_growth_vs_cyclical(
    raw_cv: float,
    detrended_cv: float,
    log_slope: float,
    r_squared: float,
    industry: str = None
) -> Dict[str, Any]:
    """
    区分高成长股和周期股

    核心逻辑：
    - 高成长股：原始CV高，但去趋势CV低（波动来自趋势本身）
    - 周期股：原始CV高，去趋势CV也高（波动来自周期往复）
    - 稳定股：两个CV都低

    关键指标：detrended_cv_ratio = detrended_cv / raw_cv
    - ratio < 0.5: 波动主要来自趋势 → 成长特征
    - ratio > 0.7: 波动主要来自周期/噪音 → 周期特征

    Args:
        raw_cv: 原始变异系数
        detrended_cv: 去趋势变异系数
        log_slope: Log斜率（趋势方向）
        r_squared: R²拟合优度
        industry: 行业（用于获取先验）

    Returns:
        {
            "classification": "high_growth" | "cyclical" | "stable" | "mixed",
            "confidence": float,  # 分类置信度 (0-1)
            "detrended_cv_ratio": float,
            "reasoning": str,
        }
    """
    # 边界情况处理
    if raw_cv == 0 or np.isinf(raw_cv) or np.isnan(raw_cv):
        return {
            "classification": "unknown",
            "confidence": 0.0,
            "detrended_cv_ratio": None,
            "reasoning": "原始CV无效",
        }

    if np.isinf(detrended_cv) or np.isnan(detrended_cv):
        # 去趋势CV无效，只能依赖其他指标
        detrended_cv_ratio = None
    else:
        detrended_cv_ratio = detrended_cv / raw_cv if raw_cv > 0 else None

    # 获取行业阈值
    category = get_industry_category(industry)
    thresholds = _CYCLICAL_THRESHOLDS.get(category, _CYCLICAL_THRESHOLDS["default"])

    classification = "mixed"
    confidence = 0.5
    reasoning_parts = []

    # === 判断逻辑 ===

    # 1. 低波动 + 高R² → 稳定
    if raw_cv < 0.20 and r_squared > 0.7:
        classification = "stable"
        confidence = 0.85
        reasoning_parts.append(f"低波动(CV={raw_cv:.2f})+高拟合度(R²={r_squared:.2f})")

    # 2. 去趋势CV比例低 + 正斜率 → 高成长
    elif detrended_cv_ratio is not None and detrended_cv_ratio < 0.5 and log_slope > 0.05:
        classification = "high_growth"
        confidence = 0.75 + 0.15 * (1 - detrended_cv_ratio)  # 比例越低，置信度越高
        reasoning_parts.append(
            f"去趋势CV比例低({detrended_cv_ratio:.2f})+正趋势(slope={log_slope:.2f})"
        )

    # 3. 去趋势CV比例高 + 低R² → 周期
    elif detrended_cv_ratio is not None and detrended_cv_ratio > 0.7 and r_squared < 0.6:
        classification = "cyclical"
        confidence = 0.70 + 0.20 * (detrended_cv_ratio - 0.7)
        reasoning_parts.append(
            f"去趋势CV比例高({detrended_cv_ratio:.2f})+低拟合度(R²={r_squared:.2f})"
        )

    # 4. 高原始CV + 负斜率 → 周期下行或恶化
    elif raw_cv > 0.4 and log_slope < -0.10:
        classification = "cyclical"
        confidence = 0.65
        reasoning_parts.append(f"高波动(CV={raw_cv:.2f})+负趋势(slope={log_slope:.2f})")

    # 5. 高原始CV + 正斜率 + 中等R² → 需要看去趋势CV
    elif raw_cv > 0.3 and log_slope > 0.05:
        if detrended_cv_ratio is not None:
            if detrended_cv_ratio < 0.6:
                classification = "high_growth"
                confidence = 0.60
                reasoning_parts.append(f"高波动但去趋势CV较低({detrended_cv_ratio:.2f})")
            else:
                classification = "cyclical"
                confidence = 0.55
                reasoning_parts.append(f"高波动且去趋势CV较高({detrended_cv_ratio:.2f})")
        else:
            classification = "mixed"
            confidence = 0.40
            reasoning_parts.append("高波动+正趋势，无法确定")

    else:
        classification = "mixed"
        confidence = 0.40
        reasoning_parts.append("特征不明显，介于成长和周期之间")

    # 行业先验调整
    if category == "cyclical" and classification == "high_growth":
        confidence *= 0.8  # 周期行业被判为成长，置信度打折
        reasoning_parts.append(f"行业({industry})为周期性，置信度下调")
    elif category == "growth" and classification == "cyclical":
        confidence *= 0.8
        reasoning_parts.append(f"行业({industry})为成长性，置信度下调")

    return {
        "classification": classification,
        "confidence": min(confidence, 0.95),  # 置信度上限
        "detrended_cv_ratio": detrended_cv_ratio,
        "reasoning": "; ".join(reasoning_parts),
    }


def get_metric_filter_config(metric_name: str) -> Dict[str, Any]:
    """
    获取指标专属过滤配置

    Args:
        metric_name: 指标名称（如 roic, roe, grossprofit_margin 等）

    Returns:
        包含 min_latest_value, severe_decline, mild_decline, is_auxiliary 的配置字典
    """
    metric_lower = metric_name.lower().strip()
    return METRIC_FILTER_CONFIGS.get(metric_lower, DEFAULT_METRIC_CONFIG).copy()


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
    # 指标专属配置
    'get_metric_filter_config',
    'METRIC_FILTER_CONFIGS',
    'DEFAULT_METRIC_CONFIG',
    # v2.0新增: 扣非利润配置
    'get_deducted_profit_config',
    'DEDUCTED_PROFIT_CONFIG',
    # v2.0新增: 周期性置信度配置
    'get_cyclicality_confidence_ceiling',
    'CYCLICALITY_CONFIDENCE_CONFIG',
    # v2.0新增: 成长vs周期区分
    'classify_growth_vs_cyclical',
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

        # === 高级统计指标 (Advanced Statistical Metrics) ===
        # WLS回归与Bootstrap置信区间
        TrendField("wls_slope", "trend.wls_slope", "WLS加权斜率", unit="slope", category="advanced"),
        TrendField("bootstrap_ci_median", "trend.bootstrap_ci_median", "Bootstrap斜率中位数", unit="slope", category="advanced"),
        TrendField("bootstrap_ci_low", "trend.bootstrap_ci_low", "Bootstrap斜率95%CI下限", unit="slope", category="advanced"),
        TrendField("bootstrap_ci_high", "trend.bootstrap_ci_high", "Bootstrap斜率95%CI上限", unit="slope", category="advanced"),
        TrendField("heteroscedasticity_detected", "trend.heteroscedasticity_detected", "检测到异方差", category="advanced"),
        TrendField("fused_slope", "trend.fused_slope", "融合斜率(OLS/WLS)", unit="slope", category="advanced"),

        # 贝叶斯恶化概率
        TrendField("deterioration_probability", "deterioration.deterioration_probability", "贝叶斯恶化概率", unit="ratio", category="advanced"),
        TrendField("consecutive_decline_years", "deterioration.consecutive_decline_years", "连续恶化年数", category="advanced"),
        TrendField("deterioration_acceleration", "deterioration.deterioration_acceleration", "恶化加速度", category="advanced"),
        TrendField("deterioration_pattern", "deterioration.deterioration_pattern", "恶化模式", category="advanced"),

        # ARCH效应与波动率体制
        TrendField("detrended_cv", "volatility.detrended_cv", "去趋势CV", category="advanced"),
        TrendField("has_arch_effect", "volatility.has_arch_effect", "存在ARCH效应", category="advanced"),
        TrendField("volatility_regime", "volatility.volatility_regime", "波动率体制", category="advanced"),
    ]

def get_default_fields() -> Tuple[TrendField, ...]:
    return tuple(trend_field_schema())
