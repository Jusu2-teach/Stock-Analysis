"""
行业配置 (Industry Configuration)
==================================

行业分类和行业特定阈值配置。

此模块属于 ThresholdEvaluator 层，不属于探针层。

设计原则:
- 行业分类用于确定评估规则的参数
- 不同行业有不同的容忍度和期望值

作者: AStock Analysis System
日期: 2025-12-12
"""

from typing import Dict, Any


# ============================================================================
# 行业分类映射
# ============================================================================

INDUSTRY_CATEGORY_MAP: Dict[str, str] = {
    # === 强周期性行业 (Cyclical) ===
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
    # 电力（周期部分）
    "火电": "cyclical", "热电": "cyclical",

    # === 周期成长行业 (Cyclical Growth) ===
    "新能源": "cyclical_growth", "光伏设备": "cyclical_growth",
    "电池": "cyclical_growth", "锂": "cyclical_growth",
    "风电设备": "cyclical_growth", "储能": "cyclical_growth",
    "新能源车": "cyclical_growth",
    "半导体": "cyclical_growth", "元件": "cyclical_growth",
    "光学光电子": "cyclical_growth",

    # === 成长性行业 (Growth) ===
    # 医药
    "医药": "growth", "生物制药": "growth", "医疗器械": "growth", "医疗服务": "growth",
    "创新药": "growth", "CXO": "growth",
    # 科技
    "电子": "growth", "计算机": "growth", "软件": "growth", "互联网": "growth",
    "通信设备": "growth", "人工智能": "growth", "云计算": "growth",
    # 高端制造
    "航空航天": "growth", "军工": "growth", "自动化设备": "growth",
    "机器人": "growth", "工业母机": "growth",

    # === 防御性/稳定行业 (Defensive) ===
    # 消费
    "食品饮料": "defensive", "白酒": "defensive", "饮料制造": "defensive",
    "食品加工": "defensive", "调味品": "defensive", "乳制品": "defensive",
    # 农业(非周期部分)
    "农林牧渔": "defensive", "种植业": "defensive", "饲料": "defensive",
    # 公用事业
    "公用事业": "defensive", "水务": "defensive", "燃气": "defensive", "环保": "defensive",
    # 电力（稳定部分）
    "水电": "defensive", "核电": "defensive", "绿色电力": "defensive", "电力": "defensive",
    # 交运(稳定部分)
    "交通运输": "defensive", "高速公路": "defensive", "机场": "defensive", "铁路": "defensive",
    # 金融(稳定部分)
    "银行": "defensive",
    # 家电
    "家电": "defensive", "白色家电": "defensive", "小家电": "defensive",
    # 其他防御
    "纺织服装": "defensive", "商贸零售": "defensive",
}


# ============================================================================
# 周期性行业列表 (用于快速判断)
# ============================================================================

CYCLICAL_INDUSTRIES = [
    # 上游资源
    "小金属", "黄金", "钢铁", "煤炭", "有色金属", "石油石化",
    "铝", "铜", "锌", "稀土", "锂",
    # 化工
    "化工", "化学制品", "基础化工", "化学纤维", "化肥", "农药",
    # 建材地产
    "建材", "水泥", "玻璃", "房地产", "建筑", "建筑材料", "装饰装修",
    # 交运
    "航运", "港口", "交运设备", "远洋运输", "集装箱",
    # 机械
    "机械", "专用设备", "通用设备", "工程机械", "重型机械",
    # 汽车
    "汽车", "汽车零部件", "乘用车", "商用车",
    # 金融
    "证券", "保险", "多元金融",
    # 农业
    "养殖", "猪肉", "禽畜养殖", "饲料", "农产品加工",
    # 其他
    "造纸", "包装印刷", "轻工制造", "家居用品",
]


# ============================================================================
# 行业分类阈值
# ============================================================================

INDUSTRY_CATEGORY_THRESHOLDS: Dict[str, Dict[str, Any]] = {
    "cyclical": {
        # 波动容忍度
        "cv_threshold": 0.3,
        "detrended_cv_min": 0.15,
        "detrended_cv_ratio_min": 0.6,
        "peak_valley_ratio": 2.0,
        "r_squared_low": 0.4,
        # 恶化容忍度
        "deterioration_tolerance": 0.35,  # 周期底部容忍更大回撤
        # 衰退阈值
        "severe_decline": -0.25,
        "mild_decline": -0.12,
        # 最大跌幅限制
        "max_decline_limit": 75,  # 周期股允许75%跌幅
    },
    "growth": {
        "cv_threshold": 0.5,
        "detrended_cv_max": 0.20,
        "detrended_cv_ratio_max": 0.5,
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
        "deterioration_tolerance": 0.25,
        "severe_decline": -0.35,  # 成长股容忍更大回撤
        "mild_decline": -0.18,
        "max_decline_limit": 60,
    },
    "cyclical_growth": {
        "cv_threshold": 0.4,
        "detrended_cv_min": 0.12,
        "detrended_cv_max": 0.30,
        "peak_valley_ratio": 2.5,
        "r_squared_low": 0.45,
        "deterioration_tolerance": 0.30,
        "severe_decline": -0.30,
        "mild_decline": -0.15,
        "max_decline_limit": 70,
    },
    "defensive": {
        "cv_threshold": 0.6,
        "detrended_cv_max": 0.15,
        "peak_valley_ratio": 4.0,
        "r_squared_low": 0.6,
        "deterioration_tolerance": 0.20,  # 防御股-20%就是大问题
        "severe_decline": -0.20,
        "mild_decline": -0.10,
        "max_decline_limit": 50,
    },
    "default": {
        "cv_threshold": 0.5,
        "detrended_cv_min": 0.10,
        "detrended_cv_max": 0.25,
        "peak_valley_ratio": 3.0,
        "r_squared_low": 0.5,
        "deterioration_tolerance": 0.25,
        "severe_decline": -0.30,
        "mild_decline": -0.15,
        "max_decline_limit": 60,
    },
}


# ============================================================================
# ROIC 行业阈值配置
# ============================================================================

ROIC_INDUSTRY_THRESHOLDS: Dict[str, Dict[str, float]] = {
    # 高壁垒行业
    "食品饮料": {"min_roic": 0.12, "min_slope": -0.02},
    "白酒": {"min_roic": 0.15, "min_slope": -0.02},
    "调味品": {"min_roic": 0.12, "min_slope": -0.02},
    "家电": {"min_roic": 0.10, "min_slope": -0.02},
    "白色家电": {"min_roic": 0.12, "min_slope": -0.02},

    # 医药
    "医药": {"min_roic": 0.08, "min_slope": -0.02},
    "医疗器械": {"min_roic": 0.10, "min_slope": -0.02},
    "医疗服务": {"min_roic": 0.08, "min_slope": -0.02},
    "生物制药": {"min_roic": 0.06, "min_slope": -0.03},

    # 科技
    "电子": {"min_roic": 0.06, "min_slope": -0.03},
    "半导体": {"min_roic": 0.05, "min_slope": -0.04},
    "计算机": {"min_roic": 0.06, "min_slope": -0.03},
    "软件": {"min_roic": 0.08, "min_slope": -0.03},
    "互联网": {"min_roic": 0.08, "min_slope": -0.03},

    # 新能源
    "新能源": {"min_roic": 0.05, "min_slope": -0.04},
    "光伏设备": {"min_roic": 0.06, "min_slope": -0.04},
    "电池": {"min_roic": 0.06, "min_slope": -0.04},

    # 公用事业
    "公用事业": {"min_roic": 0.06, "min_slope": -0.01},
    "电力": {"min_roic": 0.06, "min_slope": -0.01},
    "水务": {"min_roic": 0.05, "min_slope": -0.01},

    # 周期行业
    "钢铁": {"min_roic": 0.04, "min_slope": -0.06},
    "煤炭": {"min_roic": 0.08, "min_slope": -0.06},
    "有色金属": {"min_roic": 0.05, "min_slope": -0.06},
    "化工": {"min_roic": 0.06, "min_slope": -0.05},
    "房地产": {"min_roic": 0.04, "min_slope": -0.06},

    # 金融
    "银行": {"min_roic": 0.10, "min_slope": -0.02},
    "证券": {"min_roic": 0.06, "min_slope": -0.06},
    "保险": {"min_roic": 0.08, "min_slope": -0.03},

    # 默认
    "default": {"min_roic": 0.06, "min_slope": -0.05},
}


# ============================================================================
# ROIIC 行业阈值配置
# ============================================================================

ROIIC_INDUSTRY_THRESHOLDS: Dict[str, Dict[str, Any]] = {
    "医药": {
        "min_roiic": 0.06,
        "min_slope": -0.02,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.05,
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
        "min_delta_ic_ratio": 0.08,
    },
    "半导体": {
        "min_roiic": 0.04,
        "min_slope": -0.04,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.10,
    },
    "default": {
        "min_roiic": 0.04,
        "min_slope": -0.05,
        "smoothing_window": 3,
        "min_delta_ic_ratio": 0.05,
    },
}


# ============================================================================
# 扣非利润配置
# ============================================================================

DEDUCTED_PROFIT_CONFIG: Dict[str, Dict[str, Any]] = {
    "strict": {
        "industries": ["软件", "计算机", "互联网", "新能源", "光伏设备", "电池",
                      "半导体", "医药", "生物制药", "CXO", "军工", "航空航天"],
        "max_non_recurring_ratio": 0.30,
        "min_deducted_margin": 0.02,
    },
    "moderate": {
        "industries": ["电子", "通信设备", "机械", "汽车", "家电"],
        "max_non_recurring_ratio": 0.40,
        "min_deducted_margin": 0.01,
    },
    "lenient": {
        "industries": ["银行", "证券", "保险", "房地产", "公用事业"],
        "max_non_recurring_ratio": 0.60,
        "min_deducted_margin": 0.0,
    },
    "default": {
        "max_non_recurring_ratio": 0.40,
        "min_deducted_margin": 0.01,
    },
}


# ============================================================================
# 查询函数
# ============================================================================

def get_industry_category(industry: str) -> str:
    """获取行业分类"""
    if not industry:
        return "default"
    return INDUSTRY_CATEGORY_MAP.get(industry, "default")


def is_cyclical_industry(industry: str) -> bool:
    """判断是否为周期性行业"""
    if not industry:
        return False
    return industry in CYCLICAL_INDUSTRIES


def get_category_thresholds(industry: str = None) -> Dict[str, Any]:
    """获取行业分类阈值"""
    category = get_industry_category(industry)
    return INDUSTRY_CATEGORY_THRESHOLDS.get(
        category,
        INDUSTRY_CATEGORY_THRESHOLDS["default"]
    ).copy()


def get_roic_thresholds(industry: str = None) -> Dict[str, float]:
    """获取ROIC行业阈值"""
    if not industry:
        return ROIC_INDUSTRY_THRESHOLDS["default"].copy()
    return ROIC_INDUSTRY_THRESHOLDS.get(
        industry,
        ROIC_INDUSTRY_THRESHOLDS["default"]
    ).copy()


def get_roiic_thresholds(industry: str = None) -> Dict[str, Any]:
    """获取ROIIC行业阈值"""
    if not industry:
        return ROIIC_INDUSTRY_THRESHOLDS["default"].copy()
    return ROIIC_INDUSTRY_THRESHOLDS.get(
        industry,
        ROIIC_INDUSTRY_THRESHOLDS["default"]
    ).copy()


def get_deducted_profit_config(industry: str = None) -> Dict[str, Any]:
    """获取扣非利润配置"""
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
# 导出
# ============================================================================

__all__ = [
    # 映射表
    'INDUSTRY_CATEGORY_MAP',
    'CYCLICAL_INDUSTRIES',
    'INDUSTRY_CATEGORY_THRESHOLDS',
    'ROIC_INDUSTRY_THRESHOLDS',
    'ROIIC_INDUSTRY_THRESHOLDS',
    'DEDUCTED_PROFIT_CONFIG',

    # 查询函数
    'get_industry_category',
    'is_cyclical_industry',
    'get_category_thresholds',
    'get_roic_thresholds',
    'get_roiic_thresholds',
    'get_deducted_profit_config',
]
