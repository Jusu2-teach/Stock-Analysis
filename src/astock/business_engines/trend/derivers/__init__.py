"""
派生器注册中心
==============

管理所有已注册的派生器插件，提供查询和匹配功能。
"""

from typing import List, Optional, Set, Dict, Any
import logging

from .base import MetricDeriver
from .roiic_deriver import ROIICDeriver

logger = logging.getLogger(__name__)

# 🔌 全局插件注册表
# 要添加新的派生器，只需在这里注册即可
_REGISTERED_DERIVERS: List[MetricDeriver] = [
    ROIICDeriver(),
    # 未来添加更多派生器：
    # ROADeriver(),
    # FCFROICDeriver(),
    # AssetTurnoverDeriver(),
    # ...
]


def get_registered_derivers() -> List[MetricDeriver]:
    """
    获取所有已注册的派生器

    Returns:
        派生器列表
    """
    return list(_REGISTERED_DERIVERS)


def list_available_metrics() -> List[str]:
    """
    列出所有可派生的指标名称

    Returns:
        指标名列表，如 ['roiic', 'roa', 'fcfroic']
    """
    return [d.metric_name for d in _REGISTERED_DERIVERS]


def find_deriver(
    metric_name: str,
    available_cols: Set[str]
) -> Optional[MetricDeriver]:
    """
    查找能派生指定指标的派生器

    Args:
        metric_name: 目标指标名
        available_cols: 数据中可用的列名集合

    Returns:
        匹配的派生器，如果没有则返回 None

    Examples:
        >>> deriver = find_deriver('roiic', {'roic', 'invest_capital', 'end_date'})
        >>> if deriver:
        ...     view_name = deriver.derive(con, source_sql, 'ts_code')
    """
    for deriver in _REGISTERED_DERIVERS:
        if deriver.can_derive(metric_name, available_cols):
            logger.debug(
                f"找到派生器: {deriver.__class__.__name__} "
                f"for metric '{metric_name}'"
            )
            return deriver

    logger.debug(f"未找到派生器 for metric '{metric_name}'")
    return None


def get_deriver_info(metric_name: str) -> Optional[Dict[str, Any]]:
    """
    获取派生器的详细信息

    Args:
        metric_name: 指标名

    Returns:
        派生器信息字典，如果不存在则返回 None

    Examples:
        >>> info = get_deriver_info('roiic')
        >>> print(info['description'])
        增量资本回报率 (ROIIC): 衡量新增投资的回报效率
    """
    for deriver in _REGISTERED_DERIVERS:
        if deriver.metric_name.lower() == metric_name.lower():
            return {
                "name": deriver.metric_name,
                "description": deriver.description,
                "required_columns": sorted(deriver.required_columns),
                "plugin_class": deriver.__class__.__name__,
            }
    return None


def check_derivable(metric_name: str, available_cols: Set[str]) -> Dict[str, Any]:
    """
    检查指标是否可派生，并返回详细信息

    Args:
        metric_name: 目标指标名
        available_cols: 可用列集合

    Returns:
        检查结果字典，包含：
        - can_derive: bool
        - deriver: Optional[str] - 派生器类名
        - missing_columns: List[str] - 缺失的列
        - message: str - 描述信息
    """
    deriver = find_deriver(metric_name, available_cols)

    if deriver:
        return {
            "can_derive": True,
            "deriver": deriver.__class__.__name__,
            "missing_columns": [],
            "message": f"可以派生 {metric_name}",
        }

    # 检查是否有对应的派生器但缺少列
    for d in _REGISTERED_DERIVERS:
        if d.metric_name.lower() == metric_name.lower():
            missing = sorted(d.required_columns - available_cols)
            return {
                "can_derive": False,
                "deriver": d.__class__.__name__,
                "missing_columns": missing,
                "message": f"缺少列: {', '.join(missing)}",
            }

    # 没有对应的派生器
    available = list_available_metrics()
    return {
        "can_derive": False,
        "deriver": None,
        "missing_columns": [],
        "message": f"未注册 {metric_name} 的派生器。可用: {available}",
    }


def register_deriver(deriver: MetricDeriver) -> None:
    """
    动态注册新的派生器（高级用法）

    Args:
        deriver: 派生器实例

    Raises:
        ValueError: 如果派生器已存在
    """
    metric_name = deriver.metric_name.lower()

    # 检查是否已注册
    for existing in _REGISTERED_DERIVERS:
        if existing.metric_name.lower() == metric_name:
            raise ValueError(
                f"派生器 '{metric_name}' 已注册为 "
                f"{existing.__class__.__name__}"
            )

    _REGISTERED_DERIVERS.append(deriver)
    logger.info(
        f"✅ 动态注册派生器: {deriver.__class__.__name__} "
        f"for metric '{metric_name}'"
    )


# 导出接口
__all__ = [
    'MetricDeriver',
    'get_registered_derivers',
    'list_available_metrics',
    'find_deriver',
    'get_deriver_info',
    'check_derivable',
    'register_deriver',
]
