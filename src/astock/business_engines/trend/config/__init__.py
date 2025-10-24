"""趋势配置子包：根据用途拆分行业阈值与特征定义。"""

from .filters import (  # noqa: F401
    DEFAULT_FILTER_CONFIG,
    INDUSTRY_CATEGORIES,
    INDUSTRY_FILTER_CONFIGS,
    get_filter_config,
    get_industry_category,
)
from .roiic import (  # noqa: F401
    DEFAULT_ROIIC_FILTER_CONFIG,
    ROIIC_INDUSTRY_FILTER_CONFIGS,
    get_roiic_filter_config,
)
from .characteristics import (  # noqa: F401
    DEFAULT_CHARACTERISTICS,
    INDUSTRY_CHARACTERISTICS,
    INDUSTRY_NAME_MAPPING,
    get_cyclical_thresholds,
    get_decline_thresholds,
    get_deterioration_thresholds,
    get_industry_characteristics,
    normalize_industry_name,
)

__all__ = [
    # filters
    'INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_FILTER_CONFIG',
    'INDUSTRY_CATEGORIES',
    'get_filter_config',
    'get_industry_category',
    # ROIIC
    'ROIIC_INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_ROIIC_FILTER_CONFIG',
    'get_roiic_filter_config',
    # characteristics
    'INDUSTRY_NAME_MAPPING',
    'INDUSTRY_CHARACTERISTICS',
    'DEFAULT_CHARACTERISTICS',
    'normalize_industry_name',
    'get_industry_characteristics',
    'get_deterioration_thresholds',
    'get_cyclical_thresholds',
    'get_decline_thresholds',
]
