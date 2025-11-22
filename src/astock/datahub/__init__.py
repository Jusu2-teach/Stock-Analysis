"""
数据中心模块
提供数据获取功能
"""

from orchestrator import Registry
from . import akshare, tushare

# Scan Akshare
Registry.get().scan(
    module=akshare,
    component_type="datahub",
    engine_type="akshare",
    tags=("akshare", "data-source")
)

# Scan Tushare
Registry.get().scan(
    module=tushare,
    component_type="datahub",
    engine_type="tushare",
    tags=("tushare", "data-source")
)

__all__ = []