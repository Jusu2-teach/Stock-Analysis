"""行业滤波参数：按行业区分的一般趋势阈值。"""

from __future__ import annotations

from typing import Dict

# ========== 行业差异化参数配置 ==========
# 注意：数值基于 2019-2023 年 A 股样本的复合衰退率实证。

INDUSTRY_FILTER_CONFIGS: Dict[str, Dict[str, float]] = {
    # 🚀 科技/成长型行业 - 高要求,不应衰退
    '软件服务': {
        'min_latest_value': 12.0,
        'log_severe_decline_slope': -0.20,
        'log_mild_decline_slope': -0.10,
        'trend_significance': 0.6,
        'latest_threshold': 15.0,
    },
    '半导体': {
        'min_latest_value': 10.0,
        'log_severe_decline_slope': -0.20,
        'log_mild_decline_slope': -0.10,
        'trend_significance': 0.6,
        'latest_threshold': 15.0,
    },
    '元器件': {
        'min_latest_value': 10.0,
        'log_severe_decline_slope': -0.20,
        'log_mild_decline_slope': -0.10,
        'trend_significance': 0.6,
        'latest_threshold': 12.0,
    },
    '电气设备': {
        'min_latest_value': 9.0,
        'log_severe_decline_slope': -0.22,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.55,
        'latest_threshold': 12.0,
    },
    'IT设备': {
        'min_latest_value': 9.0,
        'log_severe_decline_slope': -0.22,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.55,
        'latest_threshold': 12.0,
    },
    '新型电力': {
        'min_latest_value': 6.0,
        'log_severe_decline_slope': -0.35,
        'log_mild_decline_slope': -0.20,
        'trend_significance': 0.45,
        'latest_threshold': 9.0,
    },
    # 🏥 稳定/消费型行业 - 标准要求
    '生物制药': {
        'min_latest_value': 8.0,
        'log_severe_decline_slope': -0.25,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.6,
        'latest_threshold': 12.0,
    },
    '化学制药': {
        'min_latest_value': 8.0,
        'log_severe_decline_slope': -0.25,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.6,
        'latest_threshold': 12.0,
    },
    '医疗保健': {
        'min_latest_value': 9.0,
        'log_severe_decline_slope': -0.25,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.6,
        'latest_threshold': 12.0,
    },
    '中药': {
        'min_latest_value': 8.0,
        'log_severe_decline_slope': -0.25,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.6,
        'latest_threshold': 12.0,
    },
    '食品饮料': {
        'min_latest_value': 8.0,
        'log_severe_decline_slope': -0.25,
        'log_mild_decline_slope': -0.12,
        'trend_significance': 0.65,
        'latest_threshold': 12.0,
    },
    '白酒': {
        'min_latest_value': 10.0,
        'log_severe_decline_slope': -0.22,
        'log_mild_decline_slope': -0.10,
        'trend_significance': 0.65,
        'latest_threshold': 15.0,
    },
    # 🏭 制造/工业行业 - 中等要求
    '汽车零部件': {
        'min_latest_value': 7.0,
        'log_severe_decline_slope': -0.30,
        'log_mild_decline_slope': -0.15,
        'trend_significance': 0.5,
        'latest_threshold': 10.0,
    },
    '汽车整车': {
        'min_latest_value': 6.0,
        'log_severe_decline_slope': -0.35,
        'log_mild_decline_slope': -0.18,
        'trend_significance': 0.45,
        'latest_threshold': 10.0,
    },
    '机械设备': {
        'min_latest_value': 7.0,
        'log_severe_decline_slope': -0.30,
        'log_mild_decline_slope': -0.15,
        'trend_significance': 0.5,
        'latest_threshold': 10.0,
    },
    '专用设备': {
        'min_latest_value': 7.0,
        'log_severe_decline_slope': -0.30,
        'log_mild_decline_slope': -0.15,
        'trend_significance': 0.5,
        'latest_threshold': 10.0,
    },
    '专用机械': {
        'min_latest_value': 7.0,
        'log_severe_decline_slope': -0.30,
        'log_mild_decline_slope': -0.15,
        'trend_significance': 0.5,
        'latest_threshold': 10.0,
    },
    # 🔄 周期性行业 - 宽松要求
    '小金属': {
        'min_latest_value': 5.0,
        'log_severe_decline_slope': -0.35,
        'log_mild_decline_slope': -0.20,
        'trend_significance': 0.3,
        'latest_threshold': 8.0,
    },
    '钢铁': {
        'min_latest_value': 5.0,
        'log_severe_decline_slope': -0.40,
        'log_mild_decline_slope': -0.22,
        'trend_significance': 0.3,
        'latest_threshold': 8.0,
    },
    '有色金属': {
        'min_latest_value': 5.0,
        'log_severe_decline_slope': -0.35,
        'log_mild_decline_slope': -0.20,
        'trend_significance': 0.3,
        'latest_threshold': 8.0,
    },
    '化工': {
        'min_latest_value': 6.0,
        'log_severe_decline_slope': -0.35,
        'log_mild_decline_slope': -0.20,
        'trend_significance': 0.4,
        'latest_threshold': 9.0,
    },
    '煤炭': {
        'min_latest_value': 5.0,
        'log_severe_decline_slope': -0.55,
        'log_mild_decline_slope': -0.35,
        'trend_significance': 0.3,
        'latest_threshold': 8.0,
    },
    # 🏗️ 重资产行业 - 低要求
    '房地产': {
        'min_latest_value': 4.0,
        'log_severe_decline_slope': -0.60,
        'log_mild_decline_slope': -0.35,
        'trend_significance': 0.4,
        'latest_threshold': 7.0,
    },
    '建筑装饰': {
        'min_latest_value': 5.0,
        'log_severe_decline_slope': -0.50,
        'log_mild_decline_slope': -0.30,
        'trend_significance': 0.4,
        'latest_threshold': 8.0,
    },
    '建筑材料': {
        'min_latest_value': 6.0,
        'log_severe_decline_slope': -0.45,
        'log_mild_decline_slope': -0.25,
        'trend_significance': 0.45,
        'latest_threshold': 9.0,
    },
}


DEFAULT_FILTER_CONFIG: Dict[str, float] = {
    'min_latest_value': 8.0,
    'log_severe_decline_slope': -0.30,
    'log_mild_decline_slope': -0.15,
    'trend_significance': 0.5,
    'latest_threshold': 12.0,
    'penalty_factor': 20,
    'max_penalty': 15,
    'severe_single_year_decline_pct': -30.0,
    'severe_single_year_penalty': 15,
    'relative_decline_ratio_70': 0.70,
    'relative_decline_penalty_70': 10,
    'relative_decline_ratio_60': 0.60,
    'relative_decline_penalty_60': 15,
    'sustained_decline_threshold': -0.15,
    'sustained_decline_penalty': 10,
}


INDUSTRY_CATEGORIES: Dict[str, list[str]] = {
    '科技成长': ['软件服务', '半导体', '元器件', '电气设备', 'IT设备'],
    '稳定消费': ['生物制药', '化学制药', '医疗保健', '中药', '食品饮料', '白酒'],
    '制造工业': ['汽车零部件', '汽车整车', '机械设备', '专用设备', '专用机械'],
    '周期性': ['小金属', '钢铁', '有色金属', '化工', '煤炭'],
    '重资产': ['房地产', '建筑装饰', '建筑材料', '新型电力'],
}


def get_filter_config(industry: str) -> Dict[str, float]:
    """获取指定行业的过滤配置。"""

    return INDUSTRY_FILTER_CONFIGS.get(industry, DEFAULT_FILTER_CONFIG.copy())


def get_industry_category(industry: str) -> str:
    """返回行业所属大类。"""

    for category, industries in INDUSTRY_CATEGORIES.items():
        if industry in industries:
            return category
    return '其他'


__all__ = [
    'INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_FILTER_CONFIG',
    'INDUSTRY_CATEGORIES',
    'get_filter_config',
    'get_industry_category',
]
