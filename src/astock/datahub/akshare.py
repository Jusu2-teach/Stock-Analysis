"""
数据源 - Akshare实现
=================

基于akshare的数据获取引擎
"""

import sys
from pathlib import Path
import logging
import pandas as pd
from typing import Dict, Any, Optional, Union

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from orchestrator import register_method

logger = logging.getLogger(__name__)

def _get_akshare_module():
    """获取akshare模块"""
    try:
        import akshare as ak
        return ak
    except ImportError:
        logger.error("akshare模块未安装")
        return None

@register_method(
    engine_name="stock_financial_debt_ths",
    component_type="datahub",
    engine_type="akshare",
    description="获取同花顺资产负债表数据"
)
def stock_financial_debt_ths(symbol: str = "",
                             indicator: str = "",
                             data: Optional[pd.DataFrame] = None,
                             **kwargs) -> pd.DataFrame:
    """
    获取同花顺资产负债表数据

    参数:
        symbol (str): 股票代码，如 "000063"
        indicator (str): 指标类型，可选值: "按报告期", "按年度", "按单季度"
        data (Optional[pd.DataFrame]): 可选的输入数据，支持pipeline传入
        **kwargs: 其他参数

    返回:
        pd.DataFrame: 资产负债表数据
    """
    # 如果有传入数据，可以基于数据进行处理
    if data is not None:
        logger.info("基于传入数据进行AKShare查询处理")
        # 这里可以基于传入数据做一些处理，比如批量查询
        # 暂时直接返回原数据，实际应用中可以扩展
        return data

    # 严格参数验证 - 绝无硬编码
    if not symbol:
        logger.error("stock_financial_debt_ths缺少必需的symbol参数")
        return pd.DataFrame()

    if not indicator:
        logger.error("stock_financial_debt_ths缺少必需的indicator参数")
        return pd.DataFrame()

    # 验证indicator参数有效性
    valid_indicators = ["按报告期", "按年度", "按单季度"]
    if indicator not in valid_indicators:
        logger.error(f"stock_financial_debt_ths indicator参数无效: {indicator}，有效值: {valid_indicators}")
        return pd.DataFrame()

    logger.info(f"获取股票{symbol}资产负债表数据，指标类型: {indicator}")

    ak = _get_akshare_module()
    if not ak:
        return pd.DataFrame()

    try:
        # 严格按照官方API调用，无任何硬编码
        data = ak.stock_financial_debt_ths(symbol=symbol, indicator=indicator)

        if data is not None and not data.empty:
            logger.info(f"获取股票{symbol}资产负债表成功，共 {len(data)} 条记录")
            return data
        else:
            logger.warning(f"股票{symbol}资产负债表数据为空")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"获取股票{symbol}资产负债表失败: {e}")
        return pd.DataFrame()

@register_method(
    engine_name="store",
    component_type="datahub",
    engine_type="akshare",
    description="Akshare数据存储 - 保存DataFrame到CSV"
)
def store(data: Optional[pd.DataFrame] = None,
         path: str = "",
         format: str = "csv",
         append_mode: bool = False,
         **kwargs) -> pd.DataFrame:
    """Akshare引擎通用存储方法 - 支持多种格式和模式"""
    logger.info(f"Akshare引擎保存数据到: {path}")

    # 参数验证
    if data is None:
        raise ValueError("store方法需要输入数据")
    if not path:
        raise ValueError("必须指定存储路径")

    try:
        if not isinstance(data, pd.DataFrame):
            logger.warning(f"数据类型不是DataFrame: {type(data)}，跳过存储")
            return data

        # 确保目录存在
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # 根据格式保存
        if format.lower() == "csv":
            mode = "a" if append_mode else "w"
            header = not append_mode or not Path(path).exists()
            data.to_csv(path, index=False, encoding='utf-8', mode=mode, header=header)
        elif format.lower() == "excel":
            data.to_excel(path, index=False)
        elif format.lower() == "parquet":
            data.to_parquet(path, index=False)
        elif format.lower() == "json":
            data.to_json(path, orient='records', force_ascii=False, indent=2)
        else:
            logger.warning(f"不支持的格式: {format}，使用CSV格式")
            data.to_csv(path, index=False, encoding='utf-8')

        logger.info(f"Akshare引擎成功保存 {len(data)} 行数据到: {path} (格式: {format})")
        return data  # 返回原数据以供管道继续使用

    except Exception as e:
        logger.error(f"Akshare引擎保存数据失败: {e}")
        return data  # 即使保存失败也返回原数据

    except Exception as e:
        logger.error(f"Akshare引擎保存数据失败: {e}")
        return data  # 即使保存失败也返回原数据