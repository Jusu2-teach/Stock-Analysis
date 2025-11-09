"""趋势配置子包：统一的行业配置系统。

架构:
- industry_profiles.py: 统一的行业配置
- analysis_config.py: 趋势分析配置
- characteristics.py: 行业特征配置(保留,trend_analysis.py需要)
- filters.py: 过滤配置(保留,duckdb_trend.py需要)
- roiic.py: ROIIC配置(保留,duckdb_trend.py需要)
"""

# ========== 行业配置系统 ==========
from .industry_profiles import (  # noqa: F401
    # 数据类
    CyclicalParams,
    DeteriorationParams,
    IndustryProfile,
    ROIICConfig,
    # 配置
    DEFAULT_PROFILE,
    INDUSTRY_PROFILES,
    INDUSTRY_NAME_MAPPING,
    # 函数
    get_all_cyclical_industries,
    get_industry_category,
    get_industry_profile,
    normalize_industry_name,
)

# ========== 分析配置 ==========
from .analysis_config import (  # noqa: F401
    TrendAnalysisConfig,
    get_default_config,
    reset_default_config,
)

# ========== 行业特征配置(保留,供trend_analysis使用) ==========
from .characteristics import (  # noqa: F401
    get_cyclical_thresholds,
    get_decline_thresholds,
)

# ========== 过滤配置(保留,供duckdb_trend使用) ==========
from .filters import (  # noqa: F401
    INDUSTRY_FILTER_CONFIGS,
    DEFAULT_FILTER_CONFIG,
)

from .roiic import (  # noqa: F401
    ROIIC_INDUSTRY_FILTER_CONFIGS,
    DEFAULT_ROIIC_FILTER_CONFIG,
)


__all__ = [
    # 行业配置
    'IndustryProfile',
    'CyclicalParams',
    'DeteriorationParams',
    'ROIICConfig',
    'INDUSTRY_PROFILES',
    'DEFAULT_PROFILE',
    'INDUSTRY_NAME_MAPPING',
    'get_industry_profile',
    'normalize_industry_name',
    'get_industry_category',
    'get_all_cyclical_industries',

    # 分析配置
    'TrendAnalysisConfig',
    'get_default_config',
    'reset_default_config',

    # 行业特征(保留)
    'get_cyclical_thresholds',
    'get_decline_thresholds',

    # 过滤配置(保留)
    'INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_FILTER_CONFIG',
    'ROIIC_INDUSTRY_FILTER_CONFIGS',
    'DEFAULT_ROIIC_FILTER_CONFIG',
]
