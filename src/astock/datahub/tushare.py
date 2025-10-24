"""
数据源 - Tushare实现
=================

基于tushare pro的数据获取引擎
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

# Tushare Pro API Token
TUSHARE_TOKEN = "60d29499510471150805842b1c7fc97e3a7ece2676b4ead1707f94d0"

def _get_tushare_pro():
    """获取tushare pro实例"""
    try:
        import tushare as ts
        pro = ts.pro_api(TUSHARE_TOKEN)
        return pro
    except ImportError:
        logger.error("tushare模块未安装，请使用 pip install tushare 安装")
        return None
    except Exception as e:
        logger.error(f"初始化tushare pro失败: {e}")
        return None

@register_method(
    engine_name="stock_basic",
    component_type="datahub",
    engine_type="tushare",
    description="获取当前正常上市交易股票基本信息 (stock_basic)"
)
def stock_basic(exchange: str = "",
                list_status: str = "L",
                fields: str = "ts_code,symbol,name,area,industry,list_date",
                is_hs: str = "",
                data: Optional[pd.DataFrame] = None,
                **kwargs) -> pd.DataFrame:
    """获取股票基础信息列表。

    参数:
        exchange: 交易所代码 (SSE 上交所 / SZSE 深交所 / 或留空全部)
        list_status: 上市状态 (L 上市, D 退市, P 暂停) 默认 L
        fields: 返回字段列表 (逗号分隔)
        is_hs: 是否沪深港通标的 (N 否 H 沪股通 S 深股通)

    返回:
        DataFrame: 股票基础信息列表
    """
    logger.info(f"获取股票基础信息 list_status={list_status} exchange={exchange} is_hs={is_hs}")
    pro = _get_tushare_pro()
    if not pro:
        return pd.DataFrame()
    try:
        df = pro.stock_basic(exchange=exchange, list_status=list_status, fields=fields, is_hs=is_hs)
        if df is not None and not df.empty:
            logger.info(f"stock_basic 获取成功: {len(df)} 行")
            return df
        logger.warning("stock_basic 返回空数据")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"调用 stock_basic 失败: {e}")
        return pd.DataFrame()

@register_method(
    engine_name="income",
    component_type="datahub",
    engine_type="tushare",
    description="获取利润表数据"
)
def income(ts_code: str = "",
           ann_date: str = "",
           start_date: str = "",
           end_date: str = "",
           period: str = "",
           report_type: str = "1",
           data: Optional[pd.DataFrame] = None,
           **kwargs) -> pd.DataFrame:
    """
    获取利润表数据

    参数:
        ts_code (str): 股票代码
        ann_date (str): 公告日期（YYYYMMDD格式）
        start_date (str): 开始日期
        end_date (str): 结束日期
        period (str): 报告期（YYYYMMDD格式）
        report_type (str): 报告类型：1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表

    返回:
        pd.DataFrame: 利润表数据
    """
    logger.info(f"获取利润表数据，股票: {ts_code}, 报告类型: {report_type}")

    pro = _get_tushare_pro()
    if not pro:
        return pd.DataFrame()

    try:
        # 调用tushare pro API
        data = pro.income(ts_code=ts_code, ann_date=ann_date,
                         start_date=start_date, end_date=end_date,
                         period=period, report_type=report_type)

        if data is not None and not data.empty:
            logger.info(f"获取利润表数据成功，共 {len(data)} 条记录")
            return data
        else:
            logger.warning("利润表数据为空")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"获取利润表数据失败: {e}")
        return pd.DataFrame()

@register_method(
    engine_name="balancesheet",
    component_type="datahub",
    engine_type="tushare",
    description="获取资产负债表数据"
)
def balancesheet(ts_code: str = "",
                 ann_date: str = "",
                 start_date: str = "",
                 end_date: str = "",
                 period: str = "",
                 report_type: str = "1",
                 data: Optional[pd.DataFrame] = None,
                 **kwargs) -> pd.DataFrame:
    """
    获取资产负债表数据

    参数:
        ts_code (str): 股票代码
        ann_date (str): 公告日期（YYYYMMDD格式）
        start_date (str): 开始日期
        end_date (str): 结束日期
        period (str): 报告期（YYYYMMDD格式）
        report_type (str): 报告类型：1合并报表 2单季合并 3调整单季合并表 4调整合并报表 5调整前合并报表

    返回:
        pd.DataFrame: 资产负债表数据
    """
    logger.info(f"获取资产负债表数据，股票: {ts_code}, 报告类型: {report_type}")

    pro = _get_tushare_pro()
    if not pro:
        return pd.DataFrame()

    try:
        # 调用tushare pro API
        data = pro.balancesheet(ts_code=ts_code, ann_date=ann_date,
                               start_date=start_date, end_date=end_date,
                               period=period, report_type=report_type)

        if data is not None and not data.empty:
            logger.info(f"获取资产负债表数据成功，共 {len(data)} 条记录")
            return data
        else:
            logger.warning("资产负债表数据为空")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"获取资产负债表数据失败: {e}")
        return pd.DataFrame()

@register_method(
    engine_name="fina_indicator_vip",
    component_type="datahub",
    engine_type="tushare",
    description="获取财务指标数据 - VIP接口，支持季度全量数据"
)
def fina_indicator_vip(ts_code: str = "",
                       ann_date: str = "",
                       start_date: str = "",
                       end_date: str = "",
                       period: str = "",
                       data: Optional[pd.DataFrame] = None,
                       **kwargs) -> pd.DataFrame:
    """
    获取财务指标数据（VIP接口）

    参数:
        ts_code (str): TS股票代码，如 600001.SH/000001.SZ
        ann_date (str): 公告日期（YYYYMMDD格式）
        start_date (str): 报告期开始日期（YYYYMMDD格式）
        end_date (str): 报告期结束日期（YYYYMMDD格式）
        period (str): 报告期（每个季度最后一天的日期，如20171231表示年报）

    返回:
        pd.DataFrame: 财务指标数据

    说明:
        - 需要获取某一季度全部上市公司数据时使用此接口
        - VIP接口，数据更全面，更新更及时
    """
    logger.info(f"获取财务指标数据，股票: {ts_code}, 报告期: {period}")

    pro = _get_tushare_pro()
    if not pro:
        return pd.DataFrame()

    try:
        # 调用tushare pro VIP API
        data = pro.fina_indicator_vip(ts_code=ts_code, ann_date=ann_date,
                                     start_date=start_date, end_date=end_date,
                                     period=period)

        if data is not None and not data.empty:
            logger.info(f"获取财务指标数据成功，共 {len(data)} 条记录")
            return data
        else:
            logger.warning("财务指标数据为空")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"获取财务指标数据失败: {e}")
        return pd.DataFrame()

@register_method(
    engine_name="store",
    component_type="datahub",
    engine_type="tushare",
    description="Tushare数据存储 - 保存DataFrame到CSV"
)
def store(data: Optional[pd.DataFrame] = None,
         path: str = "",
         format: str = "csv",
         append_mode: bool = False,
         **kwargs) -> pd.DataFrame:
    """Tushare引擎通用存储方法 - 支持多种格式和模式"""
    logger.info(f"Tushare引擎保存数据到: {path}")

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

        logger.info(f"Tushare引擎成功保存 {len(data)} 行数据到: {path} (格式: {format})")
        return data  # 返回原数据以供管道继续使用

    except Exception as e:
        logger.error(f"Tushare引擎保存数据失败: {e}")
        return data  # 即使保存失败也返回原数据