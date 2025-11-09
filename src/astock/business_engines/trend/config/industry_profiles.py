"""
统一的行业配置系统
==================

整合了原来分散在 filters.py、roiic.py、characteristics.py 中的配置。
每个行业的所有参数现在都集中在一个 IndustryProfile 中。

核心优势：
1. 一处修改，全局生效
2. 配置一致性保证
3. 类型安全，IDE自动补全
4. 易于扩展和维护
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CyclicalParams:
    """周期性参数"""

    peak_to_trough_ratio: float = 3.0
    """峰谷比阈值"""

    trend_r_squared_max: float = 0.7
    """趋势R²最大值（越低越周期性）"""

    cv_min: float = 0.25
    """变异系数最小值"""

    # 分位数信息（用于分析）
    p50_peak_to_trough: float = 3.0
    p75_peak_to_trough: float = 4.5
    p50_trend_r_squared: float = 0.50
    p25_trend_r_squared: float = 0.30
    p50_cv: float = 0.35
    p75_cv: float = 0.50

    # CV分档阈值
    cv_ultra_stable: float = 0.15
    cv_stable: float = 0.25
    cv_moderate: float = 0.40
    cv_volatile: float = 0.60


@dataclass
class DeteriorationParams:
    """恶化检测参数"""

    decline_threshold_pct: float = -10.0
    """下降百分比阈值（%）"""

    decline_threshold_abs: float = -3.0
    """下降绝对值阈值（百分点）"""

    high_level_threshold: float = 12.0
    """高水平阈值（用于判断是否高位稳定）"""

    reason: str = ""
    """阈值设置原因说明"""


@dataclass
class ROIICConfig:
    """ROIIC专用配置"""

    min_latest_value: float = 8.0
    excellent_roiic: float = 20.0
    log_severe_decline_slope: float = -0.30
    log_mild_decline_slope: float = -0.15
    trend_significance: float = 0.5
    wacc_benchmark: float = 10.0

    # 否决规则
    roiic_veto_latest_threshold: float = -5.0
    roiic_negative_penalty_buffer: float = 0.0
    roiic_negative_penalty_scale: float = 10.0
    roiic_negative_penalty_cap: float = 15.0

    # 背离规则
    roiic_divergence_slope_gap: float = 0.15

    # 奖励规则
    roiic_positive_bonus_threshold: float = 15.0

    # 惩罚参数
    penalty_factor: int = 20
    max_penalty: int = 15
    severe_single_year_decline_pct: float = -30.0
    severe_single_year_penalty: int = 15
    relative_decline_ratio_70: float = 0.70
    relative_decline_penalty_70: int = 10
    relative_decline_ratio_60: float = 0.60
    relative_decline_penalty_60: int = 15
    sustained_decline_threshold: float = -0.15
    sustained_decline_penalty: int = 10

    interpretation: Dict[str, str] = field(default_factory=dict)
    """ROIIC水平解读"""


@dataclass
class IndustryProfile:
    """统一的行业配置

    整合了所有与行业相关的配置参数，包括：
    - 基础阈值（最低值、最新值要求）
    - Log趋势阈值（严重/轻微下降）
    - ROIIC配置（可选）
    - 周期性参数
    - 恶化检测参数
    - 元数据（分类、描述、典型公司）
    """

    # ========== 基础信息 ==========
    name: str
    """行业名称"""

    category: str
    """行业分类：科技成长/稳定消费/周期性/制造工业"""

    description: str
    """行业描述"""

    is_cyclical: bool = False
    """是否为周期性行业"""

    typical_companies: List[str] = field(default_factory=list)
    """典型公司"""

    # ========== 基础阈值 ==========
    min_latest_value: float = 8.0
    """最低最新值要求"""

    latest_threshold: float = 12.0
    """最新值优秀阈值"""

    trend_significance: float = 0.5
    """趋势显著性阈值（R²）"""

    # ========== Log趋势阈值 ==========
    log_severe_decline_slope: float = -0.30
    """严重下降斜率阈值"""

    log_mild_decline_slope: float = -0.15
    """轻微下降斜率阈值"""

    # ========== 通用惩罚参数 ==========
    penalty_factor: int = 20
    """惩罚系数"""

    max_penalty: int = 15
    """最大惩罚分"""

    severe_single_year_decline_pct: float = -30.0
    """单年严重下降百分比"""

    severe_single_year_penalty: int = 15
    """单年严重下降惩罚分"""

    relative_decline_ratio_70: float = 0.70
    """相对下降比例70%"""

    relative_decline_penalty_70: int = 10
    """相对下降惩罚70%"""

    relative_decline_ratio_60: float = 0.60
    """相对下降比例60%"""

    relative_decline_penalty_60: int = 15
    """相对下降惩罚60%"""

    sustained_decline_threshold: float = -0.15
    """持续下降阈值"""

    sustained_decline_penalty: int = 10
    """持续下降惩罚分"""

    # ========== 可选配置 ==========
    roiic_config: Optional[ROIICConfig] = None
    """ROIIC专用配置（可选）"""

    cyclical_params: CyclicalParams = field(default_factory=CyclicalParams)
    """周期性参数"""

    deterioration_params: DeteriorationParams = field(default_factory=DeteriorationParams)
    """恶化检测参数"""


# ========== 默认配置 ==========

DEFAULT_PROFILE = IndustryProfile(
    name="默认",
    category="其他",
    description="默认标准",
    is_cyclical=False,
    min_latest_value=8.0,
    latest_threshold=12.0,
    trend_significance=0.5,
    log_severe_decline_slope=-0.30,
    log_mild_decline_slope=-0.15,
    cyclical_params=CyclicalParams(
        peak_to_trough_ratio=3.0,
        trend_r_squared_max=0.7,
        cv_min=0.25,
        p50_peak_to_trough=2.5,
        p75_peak_to_trough=3.5,
        p50_trend_r_squared=0.55,
        p25_trend_r_squared=0.35,
        p50_cv=0.35,
        p75_cv=0.50,
        cv_ultra_stable=0.15,
        cv_stable=0.25,
        cv_moderate=0.40,
        cv_volatile=0.60,
    ),
    deterioration_params=DeteriorationParams(
        decline_threshold_pct=-10.0,
        decline_threshold_abs=-3.0,
        high_level_threshold=12.0,
        reason="默认标准",
    ),
)


# ========== 行业配置字典 ==========

INDUSTRY_PROFILES: Dict[str, IndustryProfile] = {
    # ========== 科技成长类 ==========

    '软件服务': IndustryProfile(
        name='软件服务',
        category='科技成长',
        description='SaaS/企业软件，高增长，不应衰退',
        is_cyclical=False,
        typical_companies=['用友网络', '金蝶国际', '恒生电子'],
        min_latest_value=12.0,
        latest_threshold=15.0,
        trend_significance=0.6,
        log_severe_decline_slope=-0.20,
        log_mild_decline_slope=-0.10,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=2.5,
            trend_r_squared_max=0.70,
            cv_min=0.25,
            p50_peak_to_trough=2.5,
            p75_peak_to_trough=3.5,
            p50_trend_r_squared=0.55,
            p25_trend_r_squared=0.35,
            p50_cv=0.35,
            p75_cv=0.50,
            cv_ultra_stable=0.15,
            cv_stable=0.25,
            cv_moderate=0.40,
            cv_volatile=0.60,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-8.0,
            decline_threshold_abs=-3.0,
            high_level_threshold=15.0,
            reason='软件服务应保持增长，不应出现显著下降',
        ),
        roiic_config=ROIICConfig(
            min_latest_value=12.0,
            excellent_roiic=25.0,
            log_severe_decline_slope=-0.25,
            log_mild_decline_slope=-0.12,
            trend_significance=0.6,
            wacc_benchmark=12.0,
            roiic_veto_latest_threshold=-3.0,
            interpretation={
                '>25%': '卓越：SaaS企业，订阅收入稳定',
                '15-25%': '优秀：成熟软件企业',
                '8-15%': '及格：传统软件企业',
                '<8%': '警戒：项目制，盈利能力弱',
                '<0%': '危险：亏损或微利',
            },
        ),
    ),

    '半导体': IndustryProfile(
        name='半导体',
        category='科技成长',
        description='研发周期长，产品迭代快，业绩波动大',
        is_cyclical=True,
        typical_companies=['中芯国际', '韦尔股份', '闻泰科技'],
        min_latest_value=10.0,
        latest_threshold=15.0,
        trend_significance=0.5,
        log_severe_decline_slope=-0.30,
        log_mild_decline_slope=-0.15,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=4.0,
            trend_r_squared_max=0.65,
            cv_min=0.35,
            p50_peak_to_trough=3.5,
            p75_peak_to_trough=5.0,
            p50_trend_r_squared=0.45,
            p25_trend_r_squared=0.25,
            p50_cv=0.45,
            p75_cv=0.65,
            cv_ultra_stable=0.18,
            cv_stable=0.30,
            cv_moderate=0.50,
            cv_volatile=0.70,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-15.0,
            decline_threshold_abs=-5.0,
            high_level_threshold=12.0,
            reason='半导体周期性强，允许较大波动',
        ),
        roiic_config=ROIICConfig(
            min_latest_value=10.0,
            excellent_roiic=22.0,
            log_severe_decline_slope=-0.35,
            log_mild_decline_slope=-0.20,
            trend_significance=0.5,
            wacc_benchmark=12.0,
            roiic_veto_latest_threshold=-5.0,
            interpretation={
                '>22%': '卓越：设计或设备龙头',
                '12-22%': '优秀：制造或封测龙头',
                '5-12%': '及格：成熟产品',
                '<5%': '警戒：产能过剩或技术落后',
                '<0%': '危险：新建产能亏损',
            },
        ),
    ),

    # ========== 稳定消费类 ==========

    '医药生物': IndustryProfile(
        name='医药生物',
        category='稳定消费',
        description='创新药+仿制药，集采影响，整体稳定',
        is_cyclical=False,
        typical_companies=['恒瑞医药', '药明康德', '迈瑞医疗'],
        min_latest_value=10.0,
        latest_threshold=15.0,
        trend_significance=0.6,
        log_severe_decline_slope=-0.25,
        log_mild_decline_slope=-0.12,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=2.5,
            trend_r_squared_max=0.75,
            cv_min=0.20,
            p50_peak_to_trough=2.0,
            p75_peak_to_trough=2.8,
            p50_trend_r_squared=0.65,
            p25_trend_r_squared=0.45,
            p50_cv=0.25,
            p75_cv=0.38,
            cv_ultra_stable=0.12,
            cv_stable=0.20,
            cv_moderate=0.32,
            cv_volatile=0.45,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-8.0,
            decline_threshold_abs=-3.0,
            high_level_threshold=15.0,
            reason='医药行业应保持稳定，集采冲击除外',
        ),
        roiic_config=ROIICConfig(
            min_latest_value=10.0,
            excellent_roiic=25.0,
            log_severe_decline_slope=-0.25,
            log_mild_decline_slope=-0.12,
            wacc_benchmark=10.0,
            interpretation={
                '>25%': '卓越：创新药企业',
                '15-25%': '优秀：CXO或医疗器械',
                '8-15%': '及格：仿制药企业',
                '<8%': '警戒：集采冲击严重',
                '<0%': '危险：研发失败或质量问题',
            },
        ),
    ),

    '白酒': IndustryProfile(
        name='白酒',
        category='稳定消费',
        description='高端白酒稳定，次高端承压',
        is_cyclical=False,
        typical_companies=['贵州茅台', '五粮液', '泸州老窖'],
        min_latest_value=10.0,
        latest_threshold=15.0,
        trend_significance=0.65,
        log_severe_decline_slope=-0.22,
        log_mild_decline_slope=-0.10,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=2.0,
            trend_r_squared_max=0.75,
            cv_min=0.18,
            p50_peak_to_trough=1.8,
            p75_peak_to_trough=2.5,
            p50_trend_r_squared=0.70,
            p25_trend_r_squared=0.50,
            p50_cv=0.22,
            p75_cv=0.32,
            cv_ultra_stable=0.10,
            cv_stable=0.18,
            cv_moderate=0.28,
            cv_volatile=0.40,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-6.0,
            decline_threshold_abs=-2.0,
            high_level_threshold=15.0,
            reason='白酒应保持稳定增长',
        ),
    ),

    # ========== 周期性行业 ==========

    '小金属': IndustryProfile(
        name='小金属',
        category='周期性',
        description='价格波动大，周期性极强',
        is_cyclical=True,
        typical_companies=['赣锋锂业', '天齐锂业', '华友钴业'],
        min_latest_value=5.0,
        latest_threshold=10.0,
        trend_significance=0.4,
        log_severe_decline_slope=-0.50,
        log_mild_decline_slope=-0.30,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=6.0,
            trend_r_squared_max=0.60,
            cv_min=0.50,
            p50_peak_to_trough=5.0,
            p75_peak_to_trough=8.0,
            p50_trend_r_squared=0.35,
            p25_trend_r_squared=0.18,
            p50_cv=0.65,
            p75_cv=0.95,
            cv_ultra_stable=0.25,
            cv_stable=0.40,
            cv_moderate=0.70,
            cv_volatile=1.00,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-20.0,
            decline_threshold_abs=-8.0,
            high_level_threshold=8.0,
            reason='小金属周期极强，允许大幅波动',
        ),
        roiic_config=ROIICConfig(
            min_latest_value=5.0,
            excellent_roiic=25.0,
            log_severe_decline_slope=-0.50,
            log_mild_decline_slope=-0.30,
            trend_significance=0.4,
            wacc_benchmark=12.0,
            roiic_veto_latest_threshold=-10.0,
            interpretation={
                '>30%': '卓越：周期顶部，资源稀缺',
                '15-30%': '优秀：周期上行',
                '5-15%': '及格：周期中性',
                '<5%': '警戒：周期底部',
                '<0%': '危险：价格崩盘',
            },
        ),
    ),

    '钢铁': IndustryProfile(
        name='钢铁',
        category='周期性',
        description='重资产，周期性强，产能过剩',
        is_cyclical=True,
        typical_companies=['宝钢股份', '华菱钢铁', '方大特钢'],
        min_latest_value=4.0,
        latest_threshold=8.0,
        trend_significance=0.4,
        log_severe_decline_slope=-0.40,
        log_mild_decline_slope=-0.25,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=5.0,
            trend_r_squared_max=0.60,
            cv_min=0.45,
            p50_peak_to_trough=4.0,
            p75_peak_to_trough=6.5,
            p50_trend_r_squared=0.38,
            p25_trend_r_squared=0.20,
            p50_cv=0.58,
            p75_cv=0.85,
            cv_ultra_stable=0.22,
            cv_stable=0.35,
            cv_moderate=0.60,
            cv_volatile=0.90,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-18.0,
            decline_threshold_abs=-6.0,
            high_level_threshold=8.0,
            reason='钢铁周期性强，允许大幅波动',
        ),
    ),

    # ========== 制造工业类 ==========

    '电气设备': IndustryProfile(
        name='电气设备',
        category='制造工业',
        description='新能源+电力设备，成长性与周期性并存',
        is_cyclical=True,
        typical_companies=['宁德时代', '比亚迪', '阳光电源'],
        min_latest_value=8.0,
        latest_threshold=12.0,
        trend_significance=0.5,
        log_severe_decline_slope=-0.28,
        log_mild_decline_slope=-0.15,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=3.5,
            trend_r_squared_max=0.68,
            cv_min=0.30,
            p50_peak_to_trough=3.0,
            p75_peak_to_trough=4.5,
            p50_trend_r_squared=0.52,
            p25_trend_r_squared=0.32,
            p50_cv=0.38,
            p75_cv=0.55,
            cv_ultra_stable=0.16,
            cv_stable=0.26,
            cv_moderate=0.40,
            cv_volatile=0.55,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-10.0,
            decline_threshold_abs=-3.5,
            high_level_threshold=12.0,
            reason='电气设备周期性，允许中等波动',
        ),
    ),

    '机械设备': IndustryProfile(
        name='机械设备',
        category='制造工业',
        description='工程机械+专用设备，受投资周期影响',
        is_cyclical=True,
        typical_companies=['三一重工', '中联重科', '徐工机械'],
        min_latest_value=7.0,
        latest_threshold=12.0,
        trend_significance=0.5,
        log_severe_decline_slope=-0.30,
        log_mild_decline_slope=-0.18,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=3.5,
            trend_r_squared_max=0.68,
            cv_min=0.28,
            p50_peak_to_trough=3.0,
            p75_peak_to_trough=4.5,
            p50_trend_r_squared=0.50,
            p25_trend_r_squared=0.30,
            p50_cv=0.38,
            p75_cv=0.55,
            cv_ultra_stable=0.18,
            cv_stable=0.28,
            cv_moderate=0.45,
            cv_volatile=0.65,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-12.0,
            decline_threshold_abs=-3.5,
            high_level_threshold=12.0,
            reason='机械设备受宏观周期影响',
        ),
    ),

    '汽车': IndustryProfile(
        name='汽车',
        category='制造工业',
        description='周期性强，产业链长，波动大',
        is_cyclical=True,
        typical_companies=['比亚迪', '长城汽车', '广汽集团'],
        min_latest_value=6.0,
        latest_threshold=10.0,
        trend_significance=0.45,
        log_severe_decline_slope=-0.35,
        log_mild_decline_slope=-0.20,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=4.0,
            trend_r_squared_max=0.65,
            cv_min=0.35,
            p50_peak_to_trough=3.5,
            p75_peak_to_trough=5.5,
            p50_trend_r_squared=0.48,
            p25_trend_r_squared=0.28,
            p50_cv=0.45,
            p75_cv=0.65,
            cv_ultra_stable=0.18,
            cv_stable=0.30,
            cv_moderate=0.50,
            cv_volatile=0.75,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-15.0,
            decline_threshold_abs=-5.0,
            high_level_threshold=10.0,
            reason='汽车周期性强，允许大幅波动',
        ),
    ),

    # ========== 金融类 ==========

    '银行': IndustryProfile(
        name='银行',
        category='金融',
        description='稳定但增长慢，资产质量是核心',
        is_cyclical=False,
        typical_companies=['招商银行', '宁波银行', '平安银行'],
        min_latest_value=12.0,
        latest_threshold=15.0,
        trend_significance=0.6,
        log_severe_decline_slope=-0.20,
        log_mild_decline_slope=-0.10,
        cyclical_params=CyclicalParams(
            peak_to_trough_ratio=1.8,
            trend_r_squared_max=0.75,
            cv_min=0.15,
            p50_peak_to_trough=1.6,
            p75_peak_to_trough=2.2,
            p50_trend_r_squared=0.70,
            p25_trend_r_squared=0.50,
            p50_cv=0.18,
            p75_cv=0.28,
            cv_ultra_stable=0.08,
            cv_stable=0.15,
            cv_moderate=0.25,
            cv_volatile=0.35,
        ),
        deterioration_params=DeteriorationParams(
            decline_threshold_pct=-6.0,
            decline_threshold_abs=-2.0,
            high_level_threshold=12.0,
            reason='银行应保持稳定，资产质量是核心',
        ),
    ),
}


# ========== 行业名称映射 ==========

INDUSTRY_NAME_MAPPING: Dict[str, List[str]] = {
    '电子': ['半导体', '元器件', 'IT设备'],
    '计算机': ['软件服务'],
    '电气设备': ['电气设备', '新型电力'],
    '汽车': ['汽车零部件', '汽车整车'],
    '机械设备': ['机械设备', '专用设备', '专用机械'],
    '医药生物': ['生物制药', '化学制药', '医疗保健', '中药'],
    '食品饮料': ['食品饮料', '白酒'],
    '有色金属': ['小金属', '有色金属'],
    '化工': ['化工'],
    '煤炭': ['煤炭'],
    '钢铁': ['钢铁'],
    '房地产': ['房地产'],
    '建筑装饰': ['建筑装饰'],
    '建筑材料': ['建筑材料'],
    '银行': ['银行'],
}


# ========== 辅助函数 ==========

def normalize_industry_name(industry: str) -> str:
    """标准化行业名称（细分行业 → 申万一级）"""
    if not industry:
        return industry

    # 如果已经是一级行业，直接返回
    if industry in INDUSTRY_PROFILES:
        return industry

    # 查找映射
    for sw_industry, sub_industries in INDUSTRY_NAME_MAPPING.items():
        if industry in sub_industries:
            return sw_industry

    return industry


def get_industry_profile(industry: str) -> IndustryProfile:
    """获取行业配置

    Args:
        industry: 行业名称（支持细分行业，会自动映射到申万一级）

    Returns:
        行业配置对象
    """
    if not industry:
        return DEFAULT_PROFILE

    # 标准化行业名称
    normalized = normalize_industry_name(industry)

    # 获取配置
    return INDUSTRY_PROFILES.get(normalized, DEFAULT_PROFILE)


def get_all_cyclical_industries() -> List[str]:
    """获取所有周期性行业列表"""
    return [
        name for name, profile in INDUSTRY_PROFILES.items()
        if profile.is_cyclical
    ]


def get_industry_category(industry: str) -> str:
    """获取行业所属大类"""
    profile = get_industry_profile(industry)
    return profile.category


# ========== 导出 ==========

__all__ = [
    # 数据类
    'CyclicalParams',
    'DeteriorationParams',
    'ROIICConfig',
    'IndustryProfile',
    # 配置字典
    'DEFAULT_PROFILE',
    'INDUSTRY_PROFILES',
    'INDUSTRY_NAME_MAPPING',
    # 辅助函数
    'normalize_industry_name',
    'get_industry_profile',
    'get_all_cyclical_industries',
    'get_industry_category',
]
