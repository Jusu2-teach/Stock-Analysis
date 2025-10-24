"""
数据中心模块
提供数据获取功能
"""

# 导入数据源以触发注册
from . import akshare
from . import tushare

# 简单的注册表
class DataHub:
    pass

class DataSourceRegistry:
    pass

__all__ = [
    'DataHub',
    'DataSourceRegistry'
]