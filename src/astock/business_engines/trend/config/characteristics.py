"""行业特征参数：周期性、恶化阈值等辅助信息。"""

from __future__ import annotations

from typing import Any, Dict

INDUSTRY_NAME_MAPPING: Dict[str, list[str]] = {
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


INDUSTRY_CHARACTERISTICS: Dict[str, Dict[str, Any]] = {
    '半导体': {
        'category': '科技成长',
        'description': '研发周期长，产品迭代快，业绩波动大',
        'peak_to_trough_ratio': {'p50': 3.5, 'p75': 5.0, 'threshold': 4.0},
        'trend_r_squared': {'p50': 0.45, 'p25': 0.25, 'threshold_max': 0.60},
        'cv': {
            'p50': 0.45,
            'p75': 0.65,
            'ultra_stable': 0.20,
            'stable': 0.35,
            'moderate': 0.55,
            'volatile': 0.75,
        },
        'deterioration': {
            'decline_threshold_pct': -15.0,
            'decline_threshold_abs': -5.0,
            'high_level_threshold': 15.0,
            'reason': '半导体周期性强，允许较大波动',
        },
        'is_cyclical': True,
        'typical_companies': ['中芯国际', '韦尔股份', '兆易创新'],
    },
    '计算机': {
        'category': '科技成长',
        'description': '软硬件结合，项目制，业绩波动中等',
        'peak_to_trough_ratio': {'p50': 2.5, 'p75': 3.5, 'threshold': 3.0},
        'trend_r_squared': {'p50': 0.55, 'p25': 0.35, 'threshold_max': 0.70},
        'cv': {
            'p50': 0.35,
            'p75': 0.50,
            'ultra_stable': 0.15,
            'stable': 0.25,
            'moderate': 0.40,
            'volatile': 0.60,
        },
        'deterioration': {
            'decline_threshold_pct': -12.0,
            'decline_threshold_abs': -4.0,
            'high_level_threshold': 12.0,
            'reason': '软件企业项目周期影响，允许中等波动',
        },
        'is_cyclical': False,
        'typical_companies': ['恒生电子', '用友网络', '广联达'],
    },
    '电子': {
        'category': '科技成长',
        'description': '消费电子+工业电子，受下游影响大',
        'peak_to_trough_ratio': {'p50': 3.0, 'p75': 4.5, 'threshold': 3.5},
        'trend_r_squared': {'p50': 0.50, 'p25': 0.30, 'threshold_max': 0.65},
        'cv': {
            'p50': 0.40,
            'p75': 0.60,
            'ultra_stable': 0.18,
            'stable': 0.30,
            'moderate': 0.50,
            'volatile': 0.70,
        },
        'deterioration': {
            'decline_threshold_pct': -12.0,
            'decline_threshold_abs': -4.0,
            'high_level_threshold': 12.0,
            'reason': '电子行业周期性，允许中等波动',
        },
        'is_cyclical': True,
        'typical_companies': ['立讯精密', '歌尔股份', '京东方A'],
    },
    '医药生物': {
        'category': '稳定消费',
        'description': '创新药+仿制药，集采影响，整体稳定',
        'peak_to_trough_ratio': {'p50': 2.0, 'p75': 2.8, 'threshold': 2.5},
        'trend_r_squared': {'p50': 0.65, 'p25': 0.45, 'threshold_max': 0.75},
        'cv': {
            'p50': 0.25,
            'p75': 0.38,
            'ultra_stable': 0.12,
            'stable': 0.20,
            'moderate': 0.32,
            'volatile': 0.45,
        },
        'deterioration': {
            'decline_threshold_pct': -8.0,
            'decline_threshold_abs': -3.0,
            'high_level_threshold': 15.0,
            'reason': '医药行业应保持稳定，集采冲击除外',
        },
        'is_cyclical': False,
        'typical_companies': ['恒瑞医药', '药明康德', '迈瑞医疗'],
    },
    '食品饮料': {
        'category': '稳定消费',
        'description': '需求刚性，现金流好，波动极小',
        'peak_to_trough_ratio': {'p50': 1.5, 'p75': 2.0, 'threshold': 2.0},
        'trend_r_squared': {'p50': 0.70, 'p25': 0.55, 'threshold_max': 0.80},
        'cv': {
            'p50': 0.18,
            'p75': 0.28,
            'ultra_stable': 0.10,
            'stable': 0.15,
            'moderate': 0.25,
            'volatile': 0.35,
        },
        'deterioration': {
            'decline_threshold_pct': -5.0,
            'decline_threshold_abs': -2.0,
            'high_level_threshold': 20.0,
            'reason': '消费品应极度稳定，下滑是重大信号',
        },
        'is_cyclical': False,
        'typical_companies': ['贵州茅台', '海天味业', '伊利股份'],
    },
    '电气设备': {
        'category': '制造工业',
        'description': '新能源+传统电气，受政策影响',
        'peak_to_trough_ratio': {'p50': 2.8, 'p75': 4.0, 'threshold': 3.5},
        'trend_r_squared': {'p50': 0.55, 'p25': 0.35, 'threshold_max': 0.70},
        'cv': {
            'p50': 0.32,
            'p75': 0.48,
            'ultra_stable': 0.15,
            'stable': 0.25,
            'moderate': 0.40,
            'volatile': 0.55,
        },
        'deterioration': {
            'decline_threshold_pct': -10.0,
            'decline_threshold_abs': -3.5,
            'high_level_threshold': 12.0,
            'reason': '电气设备周期性，允许中等波动',
        },
        'is_cyclical': True,
        'typical_companies': ['宁德时代', '比亚迪', '阳光电源'],
    },
    '汽车': {
        'category': '制造工业',
        'description': '周期性强，产业链长，波动大',
        'peak_to_trough_ratio': {'p50': 3.5, 'p75': 5.5, 'threshold': 4.0},
        'trend_r_squared': {'p50': 0.45, 'p25': 0.25, 'threshold_max': 0.65},
        'cv': {
            'p50': 0.48,
            'p75': 0.70,
            'ultra_stable': 0.20,
            'stable': 0.32,
            'moderate': 0.50,
            'volatile': 0.70,
        },
        'deterioration': {
            'decline_threshold_pct': -15.0,
            'decline_threshold_abs': -4.0,
            'high_level_threshold': 10.0,
            'reason': '汽车行业周期性极强，波动正常',
        },
        'is_cyclical': True,
        'typical_companies': ['比亚迪', '长城汽车', '广汽集团'],
    },
    '机械设备': {
        'category': '制造工业',
        'description': '工程机械+专用设备，受投资周期影响',
        'peak_to_trough_ratio': {'p50': 3.0, 'p75': 4.5, 'threshold': 3.5},
        'trend_r_squared': {'p50': 0.50, 'p25': 0.30, 'threshold_max': 0.68},
        'cv': {
            'p50': 0.38,
            'p75': 0.55,
            'ultra_stable': 0.18,
            'stable': 0.28,
            'moderate': 0.45,
            'volatile': 0.65,
        },
        'deterioration': {
            'decline_threshold_pct': -12.0,
            'decline_threshold_abs': -3.5,
            'high_level_threshold': 12.0,
            'reason': '机械设备受宏观周期影响',
        },
        'is_cyclical': True,
        'typical_companies': ['三一重工', '中联重科', '徐工机械'],
    },
    '有色金属': {
        'category': '周期性',
        'description': '商品价格驱动，周期性极强',
        'peak_to_trough_ratio': {'p50': 5.0, 'p75': 8.0, 'threshold': 4.0},
        'trend_r_squared': {'p50': 0.30, 'p25': 0.15, 'threshold_max': 0.55},
        'cv': {
            'p50': 0.65,
            'p75': 0.95,
            'ultra_stable': 0.25,
            'stable': 0.40,
            'moderate': 0.60,
            'volatile': 0.85,
        },
        'deterioration': {
            'decline_threshold_pct': -25.0,
            'decline_threshold_abs': -8.0,
            'high_level_threshold': 8.0,
            'reason': '周期股波动是常态，只看周期位置',
        },
        'is_cyclical': True,
        'typical_companies': ['紫金矿业', '赣锋锂业', '华友钴业'],
    },
    '化工': {
        'category': '周期性',
        'description': '石化+精细化工，价格波动大',
        'peak_to_trough_ratio': {'p50': 4.0, 'p75': 6.5, 'threshold': 3.5},
        'trend_r_squared': {'p50': 0.35, 'p25': 0.20, 'threshold_max': 0.60},
        'cv': {
            'p50': 0.55,
            'p75': 0.80,
            'ultra_stable': 0.22,
            'stable': 0.35,
            'moderate': 0.55,
            'volatile': 0.75,
        },
        'deterioration': {
            'decline_threshold_pct': -20.0,
            'decline_threshold_abs': -6.0,
            'high_level_threshold': 10.0,
            'reason': '化工周期性强，价格主导',
        },
        'is_cyclical': True,
        'typical_companies': ['万华化学', '华鲁恒升', '扬农化工'],
    },
    '煤炭': {
        'category': '周期性',
        'description': '能源商品，政策+需求双重影响',
        'peak_to_trough_ratio': {'p50': 4.5, 'p75': 7.0, 'threshold': 4.0},
        'trend_r_squared': {'p50': 0.32, 'p25': 0.18, 'threshold_max': 0.58},
        'cv': {
            'p50': 0.60,
            'p75': 0.85,
            'ultra_stable': 0.25,
            'stable': 0.38,
            'moderate': 0.58,
            'volatile': 0.78,
        },
        'deterioration': {
            'decline_threshold_pct': -22.0,
            'decline_threshold_abs': -7.0,
            'high_level_threshold': 12.0,
            'reason': '煤炭周期性极强，波动正常',
        },
        'is_cyclical': True,
        'typical_companies': ['中国神华', '陕西煤业', '兖矿能源'],
    },
    '银行': {
        'category': '金融',
        'description': '受宏观经济和货币政策影响，稳定性高',
        'peak_to_trough_ratio': {'p50': 1.8, 'p75': 2.5, 'threshold': 2.2},
        'trend_r_squared': {'p50': 0.68, 'p25': 0.50, 'threshold_max': 0.78},
        'cv': {
            'p50': 0.22,
            'p75': 0.32,
            'ultra_stable': 0.12,
            'stable': 0.18,
            'moderate': 0.28,
            'volatile': 0.40,
        },
        'deterioration': {
            'decline_threshold_pct': -6.0,
            'decline_threshold_abs': -2.0,
            'high_level_threshold': 12.0,
            'reason': '银行应保持稳定，资产质量是核心',
        },
        'is_cyclical': False,
        'typical_companies': ['招商银行', '宁波银行', '平安银行'],
    },
    '房地产': {
        'category': '周期性',
        'description': '政策敏感，周期性强，当前下行周期',
        'peak_to_trough_ratio': {'p50': 6.0, 'p75': 10.0, 'threshold': 5.0},
        'trend_r_squared': {'p50': 0.40, 'p25': 0.22, 'threshold_max': 0.62},
        'cv': {
            'p50': 0.70,
            'p75': 1.05,
            'ultra_stable': 0.28,
            'stable': 0.42,
            'moderate': 0.65,
            'volatile': 0.90,
        },
        'deterioration': {
            'decline_threshold_pct': -30.0,
            'decline_threshold_abs': -10.0,
            'high_level_threshold': 8.0,
            'reason': '房地产周期极长，波动极大',
        },
        'is_cyclical': True,
        'typical_companies': ['万科A', '保利发展', '招商蛇口'],
    },
}


DEFAULT_CHARACTERISTICS: Dict[str, Any] = {
    'category': '其他',
    'description': '未分类行业，使用保守默认值',
    'peak_to_trough_ratio': {'p50': 2.5, 'p75': 3.5, 'threshold': 3.0},
    'trend_r_squared': {'p50': 0.55, 'p25': 0.35, 'threshold_max': 0.70},
    'cv': {
        'p50': 0.35,
        'p75': 0.50,
        'ultra_stable': 0.15,
        'stable': 0.25,
        'moderate': 0.40,
        'volatile': 0.60,
    },
    'deterioration': {
        'decline_threshold_pct': -10.0,
        'decline_threshold_abs': -3.0,
        'high_level_threshold': 12.0,
        'reason': '默认标准',
    },
    'is_cyclical': False,
}


def normalize_industry_name(industry: str) -> str:
    """标准化行业名称（细分行业 → 申万一级）。"""

    if industry in INDUSTRY_NAME_MAPPING:
        return industry

    for sw_industry, sub_industries in INDUSTRY_NAME_MAPPING.items():
        if industry in sub_industries:
            return sw_industry

    return industry


def get_industry_characteristics(industry: str) -> Dict[str, Any]:
    """获取指定行业的财务特征参数。"""

    return INDUSTRY_CHARACTERISTICS.get(industry, DEFAULT_CHARACTERISTICS)


def get_deterioration_thresholds(industry: str) -> Dict[str, float]:
    """获取近期恶化判断的行业差异化阈值。"""

    config = get_industry_characteristics(industry)
    return config['deterioration']


def get_cyclical_thresholds(industry: str) -> Dict[str, float]:
    """获取周期性判断的行业差异化阈值。"""

    sw_industry = normalize_industry_name(industry)
    config = get_industry_characteristics(sw_industry)

    return {
        'peak_to_trough_ratio': config['peak_to_trough_ratio']['threshold'],
        'trend_r_squared_max': config['trend_r_squared']['threshold_max'],
        'cv_min': config['cv']['moderate'],
    }


def get_decline_thresholds(industry: str) -> Dict[str, float]:
    """获取近期恶化判断的行业差异化阈值。"""

    sw_industry = normalize_industry_name(industry)
    deterioration = get_deterioration_thresholds(sw_industry)

    return {
        'decline_threshold_pct': deterioration['decline_threshold_pct'],
        'decline_threshold_abs': deterioration['decline_threshold_abs'],
        'high_level_threshold': deterioration['high_level_threshold'],
    }


__all__ = [
    'INDUSTRY_NAME_MAPPING',
    'INDUSTRY_CHARACTERISTICS',
    'DEFAULT_CHARACTERISTICS',
    'normalize_industry_name',
    'get_industry_characteristics',
    'get_deterioration_thresholds',
    'get_cyclical_thresholds',
    'get_decline_thresholds',
]
