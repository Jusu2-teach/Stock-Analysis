"""
数据引擎 - Pandas实现
==================

专门负责数据清理工作：去重、检验、标准化等基础数据处理功能
"""

import logging
import pandas as pd
import numpy as np
import re
from typing import Dict, Any, List, Union, Optional

from .schema_utils import ensure_columns

logger = logging.getLogger(__name__)

def store(data: Optional[pd.DataFrame] = None,
         path: str = "",
         format: str = "csv",
         append_mode: bool = False,
         **kwargs) -> pd.DataFrame:
    """Pandas引擎通用存储方法 - 支持多种格式和模式"""
    logger.info(f"Pandas引擎保存数据到: {path}")
    logger.info(f"🔍 Store函数接收的数据类型: {type(data)}")

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

        logger.info(f"Pandas引擎成功保存 {len(data)} 行数据到: {path} (格式: {format})")
        logger.info(f"🔍 Store函数返回的数据类型: {type(data)}")
        return data  # 返回原数据以供管道继续使用

    except Exception as e:
        logger.error(f"Pandas引擎保存数据失败: {e}")
        return data  # 即使保存失败也返回原数据


def clean_financial_data(data: Optional[Union[pd.DataFrame, str]] = None,
                        file_path: Optional[str] = None,
                        output_path: Optional[str] = None,
                        report_path: Optional[str] = None,
                        validate_accounting: bool = True,
                        **kwargs) -> pd.DataFrame:
    """
    财务数据标准化清洗主函数

    Args:
        data: DataFrame或CSV文件路径或None
        file_path: 可选的文件路径参数
        output_path: 清洗后数据保存路径（可选）
        report_path: 清洗报告保存路径（可选）
        validate_accounting: 是否验证会计恒等式
        **kwargs: 其他参数

    Returns:
        清洗后的DataFrame
    """
    logger.info("🧹 开始财务数据标准化清洗")
    logger.info(f"🔍 输入数据类型: {type(data)}")

    try:
        # 1. 智能数据加载和验证
        if data is not None and isinstance(data, pd.DataFrame):
            # 如果是DataFrame，直接复制
            df = data.copy()
            logger.info("📋 从管道接收DataFrame数据")
        elif data is not None and isinstance(data, str):
            # 如果是文件路径字符串，从文件加载
            df = pd.read_csv(data)
            logger.info(f"📁 从传入路径加载数据: {data}")
        elif file_path:
            # 使用file_path参数加载
            df = pd.read_csv(file_path)
            logger.info(f"� 从file_path加载数据: {file_path}")
        else:
            raise ValueError("必须提供data（DataFrame或文件路径）或file_path参数")

        logger.info(f"📊 原始数据: {df.shape[0]}行 × {df.shape[1]}列")

        # 2. 基础清洗流程
        df = _standardize_column_names(df)
        df = _convert_currency_to_numeric(df)
        df = _handle_boolean_false_values(df)
        df = _handle_missing_values(df)
        df = _standardize_time_index(df)

        # 3. 数据验证
        if validate_accounting:
            df = _validate_accounting_equations(df)

        # 4. 异常值处理
        df = _detect_and_handle_outliers(df)

        # 5. 保存清洗后数据
        if output_path:
            df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info(f"💾 清洗后数据已保存: {output_path}")

        # 6. 生成并保存清洗报告
        if report_path:
            summary = financial_data_summary(df)
            import json
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"📋 清洗报告已保存: {report_path}")

        logger.info(f"✅ 数据清洗完成: {df.shape[0]}行 × {df.shape[1]}列")
        return df

    except Exception as e:
        logger.error(f"❌ 数据清洗失败: {e}")
        raise


def _standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """标准化列名 - 简化中文列名"""
    logger.info("📝 标准化列名")

    # 核心财务指标映射
    column_mapping = {
        '报告期': 'period',
        '*所有者权益（或股东权益）合计': 'total_equity',
        '*资产合计': 'total_assets',
        '*负债合计': 'total_liabilities',
        '*归属于母公司所有者权益合计': 'shareholders_equity',
        '流动资产': 'current_assets',
        '货币资金': 'cash_and_equivalents',
        '交易性金融资产': 'trading_securities',
        '应收票据及应收账款': 'accounts_receivable_total',
        '应收账款': 'accounts_receivable',
        '存货': 'inventory',
        '固定资产合计': 'fixed_assets_total',
        '短期借款': 'short_term_debt',
        '长期借款': 'long_term_debt',
        '实收资本（或股本）': 'share_capital',
        '资本公积': 'capital_surplus',
        '未分配利润': 'retained_earnings'
    }

    # 重命名核心列
    df = df.rename(columns=column_mapping)

    # 为其他列生成英文名
    for col in df.columns:
        if col not in column_mapping.values() and col in column_mapping.keys():
            continue
        elif '：' in col:
            # 处理子项目，如 "其中：应收票据"
            df = df.rename(columns={col: f"sub_{col.split('：')[1]}"})

    logger.info(f"📝 列名标准化完成，核心指标: {len(column_mapping)} 个")
    return df


def _convert_currency_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """转换货币字符串为数值"""
    logger.info("💰 转换货币单位为数值")

    def convert_currency_value(value):
        """转换单个货币值"""
        if pd.isna(value) or value == '':
            return np.nan

        if isinstance(value, (int, float)):
            return value

        if not isinstance(value, str):
            return np.nan

        # 移除空格
        value = str(value).strip()

        # 处理负数
        is_negative = value.startswith('-')
        if is_negative:
            value = value[1:]

        # 提取数字和单位
        if '亿' in value:
            number = re.findall(r'[\d.]+', value)
            if number:
                result = float(number[0]) * 100000000  # 亿 = 1e8
            else:
                return np.nan
        elif '万' in value:
            number = re.findall(r'[\d.]+', value)
            if number:
                result = float(number[0]) * 10000  # 万 = 1e4
            else:
                return np.nan
        else:
            # 尝试直接转换为数字
            try:
                result = float(value)
            except:
                return np.nan

        return -result if is_negative else result

    # 识别需要转换的列（包含货币单位的列）
    currency_columns = []
    for col in df.columns:
        if col in ['period']:  # 跳过时间列
            continue
        # 检查是否包含货币单位
        sample_values = df[col].dropna().astype(str).head(5)
        if any('亿' in str(val) or '万' in str(val) for val in sample_values):
            currency_columns.append(col)

    logger.info(f"💰 发现货币列: {len(currency_columns)} 个")

    # 转换货币列
    for col in currency_columns:
        original_type = df[col].dtype
        df[col] = df[col].apply(convert_currency_value)
        logger.debug(f"  {col}: {original_type} → float64")

    return df


def _handle_boolean_false_values(df: pd.DataFrame) -> pd.DataFrame:
    """处理布尔False值 - 转换为NaN或0"""
    logger.info("🔄 处理布尔False值")

    false_count = 0
    for col in df.columns:
        if col == 'period':  # 跳过时间列
            continue

        # 统计False值
        false_mask = df[col].astype(str) == 'False'
        col_false_count = false_mask.sum()

        if col_false_count > 0:
            false_count += col_false_count
            # 将False转换为NaN，先转换为object类型避免警告
            df[col] = df[col].astype('object')
            df.loc[false_mask, col] = np.nan
            logger.debug(f"  {col}: {col_false_count} 个False值转换为NaN")

    logger.info(f"🔄 总计处理 {false_count} 个False值")
    return df


def _handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """处理缺失值 - 使用前向填充和0填充策略"""
    logger.info("🕳️ 处理缺失值")

    # 计算缺失值统计
    missing_before = df.isnull().sum().sum()

    # 对于财务数据，不同类型指标使用不同策略
    asset_liability_cols = [col for col in df.columns if any(keyword in col.lower()
                           for keyword in ['asset', 'liability', 'debt', 'equity', 'cash'])]

    # 资产负债类：使用前向填充
    for col in asset_liability_cols:
        if col in df.columns:
            df[col] = df[col].ffill()

    # 其他数值列：填充0（表示该年度无此项目）
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if col not in asset_liability_cols:
            df[col] = df[col].fillna(0)

    missing_after = df.isnull().sum().sum()
    logger.info(f"🕳️ 缺失值处理: {missing_before} → {missing_after}")

    return df


def _standardize_time_index(df: pd.DataFrame) -> pd.DataFrame:
    """标准化时间索引"""
    logger.info("📅 标准化时间索引")

    if 'period' in df.columns:
        # 转换为datetime
        df['period'] = pd.to_datetime(df['period'], format='%Y', errors='coerce')

        # 设置为索引
        df = df.set_index('period').sort_index()

        logger.info(f"📅 时间索引设置完成: {df.index.min()} - {df.index.max()}")

    return df


def _validate_accounting_equations(df: pd.DataFrame) -> pd.DataFrame:
    """验证会计恒等式: 资产 = 负债 + 所有者权益"""
    logger.info("⚖️ 验证会计恒等式")

    if all(col in df.columns for col in ['total_assets', 'total_liabilities', 'total_equity']):
        # 计算差额
        df['accounting_diff'] = df['total_assets'] - (df['total_liabilities'] + df['total_equity'])

        # 计算相对误差
        df['accounting_error_pct'] = (df['accounting_diff'] / df['total_assets'] * 100).round(2)

        # 统计验证结果
        tolerance = 0.01  # 1%容忍度
        valid_count = (abs(df['accounting_error_pct']) <= tolerance).sum()
        total_count = len(df)

        logger.info(f"⚖️ 会计等式验证: {valid_count}/{total_count} 条记录在容忍范围内")

        # 报告异常记录
        invalid_records = df[abs(df['accounting_error_pct']) > tolerance]
        if len(invalid_records) > 0:
            logger.warning(f"⚠️ 发现 {len(invalid_records)} 条记录存在会计等式偏差")
            for idx, row in invalid_records.iterrows():
                logger.warning(f"  {idx}: 偏差 {row['accounting_error_pct']:.2f}%")

    return df


def _detect_and_handle_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """检测和处理异常值"""
    logger.info("🔍 检测异常值")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    outlier_count = 0

    for col in numeric_cols:
        if col.endswith('_diff') or col.endswith('_pct'):  # 跳过计算列
            continue

        # 使用IQR方法检测异常值
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1

        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)) & df[col].notna()
        outlier_count += outliers.sum()

        if outliers.sum() > 0:
            logger.warning(f"🔍 {col}: 发现 {outliers.sum()} 个异常值")

    logger.info(f"🔍 异常值检测完成，总计发现: {outlier_count} 个")
    return df


def financial_data_summary(data: pd.DataFrame, output_path: Optional[str] = None) -> Dict[str, Any]:
    """生成财务数据清洗报告"""
    logger.info("📊 生成数据清洗报告")

    try:
        summary = {
            "数据概览": {
                "总行数": int(len(data)),
                "总列数": int(len(data.columns)),
                "时间跨度": f"{data.index.min()} 至 {data.index.max()}" if hasattr(data.index, 'min') else "未设置时间索引",
                "数据类型分布": {str(k): int(v) for k, v in data.dtypes.value_counts().items()}
            },
            "数据质量": {
                "缺失值总数": int(data.isnull().sum().sum()),
                "缺失值比例": f"{(data.isnull().sum().sum() / (len(data) * len(data.columns)) * 100):.2f}%",
                "数值列数量": int(len(data.select_dtypes(include=[np.number]).columns)),
                "完整记录数": int(len(data.dropna()))
            },
            "财务指标统计": {}
        }

        # 核心财务指标统计
        key_metrics = ['total_assets', 'total_liabilities', 'total_equity', 'cash_and_equivalents']
        for metric in key_metrics:
            if metric in data.columns:
                col_data = data[metric].dropna()
                summary["财务指标统计"][metric] = {
                    "最小值": float(col_data.min()),
                    "最大值": float(col_data.max()),
                    "平均值": float(col_data.mean()),
                    "标准差": float(col_data.std()),
                    "有效数据点": len(col_data)
                }

        # 会计等式验证结果
        if 'accounting_error_pct' in data.columns:
            valid_records = (abs(data['accounting_error_pct']) <= 1.0).sum()
            summary["会计验证"] = {
                "验证通过记录": int(valid_records),
                "验证通过率": f"{(valid_records / len(data) * 100):.2f}%",
                "平均偏差": f"{data['accounting_error_pct'].abs().mean():.4f}%"
            }

        # 保存报告
        if output_path:
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            logger.info(f"📊 清洗报告已保存: {output_path}")

        logger.info("📊 数据清洗报告生成完成")
        return summary

    except Exception as e:
        logger.error(f"❌ 生成清洗报告失败: {e}")
        raise


@ensure_columns(required_columns=["total_assets"], output_keys=["merged", "stats"], strict=False)
def join_and_summarize(inputs: List[pd.DataFrame] = None,
                       how: str = 'inner',
                       on: Optional[str] = None,
                       limit: int = 0) -> Dict[str, Any]:
    """示例：展示多输入 + dict 多输出 + schema 校验。

    Args:
        inputs: 来自上游的多个 DataFrame（engine 已注入）
        how: 合并方式
        on: 指定 join 键（若未提供则尝试公共列）
        limit: 可选截断行数（用于调试）
    Returns:
        {"merged": DataFrame, "stats": {列/行信息}}
    """
    logger.info("🔗 join_and_summarize: 开始处理多输入")
    if not inputs or len(inputs) < 2:
        raise ValueError("join_and_summarize 需要至少两个输入 DataFrame")
    left, right = inputs[0], inputs[1]
    # 自动选择 join 列
    if on is None:
        common = [c for c in left.columns if c in right.columns]
        if not common:
            raise ValueError("未找到公共列用于 join，可通过参数 on 指定")
        on = common[0]
        logger.info(f"🔑 自动选择公共列 '{on}' 作为 join 键")
    merged = left.merge(right, how=how, on=on, suffixes=("_l", "_r"))
    if limit and limit > 0:
        merged = merged.head(limit)
    stats = {
        'rows': int(len(merged)),
        'cols': int(len(merged.columns)),
        'columns_sample': merged.columns[:10].tolist(),
        'join_key': on,
        'inputs_shapes': [list(df.shape) for df in inputs[:2]]
    }
    logger.info(f"🔗 join_and_summarize 完成: {stats['rows']} 行, {stats['cols']} 列")
    return {"merged": merged, "stats": stats}


def double_split_demo(data: pd.DataFrame, top: int = 5, sample: int = 5):
    """演示无需 dict，直接返回 (head_df, tail_df) 也可通过 outputs 映射。

    Args:
        data: 上游 DataFrame（引擎自动注入）
        top: 取前多少行
        sample: 从后部随机抽样多少行
    Returns:
        (head_df, tail_sample_df)
    """
    if not isinstance(data, pd.DataFrame):
        raise ValueError("double_split_demo 需要 DataFrame 输入")
    head_part = data.head(top).copy()
    tail_part = data.tail(max(sample, 1)).sample(min(sample, len(data.tail(max(sample,1)))), random_state=42).copy()
    head_part['__subset'] = 'head'
    tail_part['__subset'] = 'tail_sample'
    return head_part, tail_part