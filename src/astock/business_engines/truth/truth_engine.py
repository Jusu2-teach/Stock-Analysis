"""
T.R.U.T.H. Processing Engine Entry Point
========================================

注册 T.R.U.T.H. 处理方法到业务引擎。

数据流：
    8个探针分析结果 → process_truth → 处理后的基因组数据 → 报告生成

架构升级 (v5.0):
- 完全基于 EventBus 架构
- 发布数据处理事件

作者: AStock Analysis System
日期: 2025-01
"""

import time
from typing import Dict, Any, Optional
import pandas as pd
import logging
from orchestrator.decorators.register import register_method

# 统一事件总线
from shared import EventBus, DataLoadedEvent, DataTransformedEvent

from .processor import TruthProcessor, TruthProcessResult, BatchProcessResult
from .config import get_default_truth_config

logger = logging.getLogger(__name__)
_event_bus = EventBus.get()


def _emit_data_event(event_class, **kwargs):
    """发布数据事件"""
    try:
        _event_bus.emit(event_class(**kwargs, source='truth_engine'))
    except Exception as e:
        logger.debug(f"EventBus emit failed: {e}")


@register_method(
    engine_name="process_truth",
    component_type="business_engine",
    engine_type="truth",
    description="Process probe results through T.R.U.T.H. system with professional gene-indicator mapping"
)
def process_truth(
    roic_data: pd.DataFrame = None,
    roe_data: pd.DataFrame = None,
    roiic_data: pd.DataFrame = None,
    gross_margin_data: pd.DataFrame = None,
    net_margin_data: pd.DataFrame = None,
    revenue_data: pd.DataFrame = None,
    profit_data: pd.DataFrame = None,
    ocf_data: pd.DataFrame = None,
) -> Dict[str, Any]:
    """
    通过 T.R.U.T.H. 系统处理探针数据

    专业处理流程：
    1. 基因-指标映射
       - α (周期性): ROIC, ROE → max聚合
       - β (资本密度): ROIC, OCF → 加权计算
       - γ (成长动能): 营收, 利润 → 调和平均
       - δ_fraud (欺诈熵): 利润vs现金流 → 逻辑OR
       - δ_decay (衰退熵): 效率指标 → max
       - V (验证): 现金流

    2. 三大求解器
       - 重力求解器: 计算动态ROIC阈值
       - 速度求解器: 评估增长边界
       - 结构求解器: 检测护城河侵蚀

    3. 因果网络验证
       - 营收→利润→现金流一致性
       - ROE vs ROIC杠杆检测
       - 周期性传导验证

    Args:
        roic_data ~ ocf_data: 各指标探针分析结果 DataFrame

    Returns:
        Dict包含:
        - processed_results: BatchProcessResult 对象
        - results_df: 处理结果DataFrame（用于报告生成）
        - summary: 处理摘要
    """
    probe_data = {
        'roic': roic_data,
        'roe': roe_data,
        'roiic': roiic_data,
        'gross_margin': gross_margin_data,
        'net_margin': net_margin_data,
        'revenue': revenue_data,
        'profit': profit_data,
        'ocf': ocf_data,
    }

    # 过滤掉空数据
    probe_data = {k: v for k, v in probe_data.items() if v is not None and not v.empty}

    if not probe_data:
        logger.warning("No valid probe data provided")
        return {
            'processed_results': None,
            'results_df': pd.DataFrame(),
            'summary': {'error': 'No valid probe data'},
        }

    # 发布数据加载事件
    total_rows = sum(len(v) for v in probe_data.values())
    _emit_data_event(
        DataLoadedEvent,
        dataset_name='truth_probe_data',
        source='probe_analysis',
        row_count=total_rows,
        column_count=len(probe_data)
    )

    # 创建处理器
    start_time = time.perf_counter()
    processor = TruthProcessor()

    # 批量处理
    batch_result = processor.process_batch(probe_data)

    # 转换为DataFrame
    results_df = processor.get_results_dataframe(batch_result)
    duration_ms = (time.perf_counter() - start_time) * 1000

    # 发布数据转换事件
    _emit_data_event(
        DataTransformedEvent,
        dataset_name='truth_results',
        transformation='process_truth',
        input_rows=total_rows,
        output_rows=len(results_df),
        duration_ms=duration_ms
    )

    logger.info(
        f"T.R.U.T.H. processing complete: "
        f"{batch_result.processing_stats['success']}/{batch_result.processing_stats['total']} companies"
    )

    return {
        'processed_results': batch_result,
        'results_df': results_df,
        'summary': batch_result.summary,
        'probe_data': probe_data,  # 传递原始探针数据供报告使用
    }


@register_method(
    engine_name="process_truth_single",
    component_type="business_engine",
    engine_type="truth",
    description="Process single company through T.R.U.T.H. system"
)
def process_truth_single(
    ts_code: str,
    roic_data: pd.DataFrame = None,
    roe_data: pd.DataFrame = None,
    roiic_data: pd.DataFrame = None,
    gross_margin_data: pd.DataFrame = None,
    net_margin_data: pd.DataFrame = None,
    revenue_data: pd.DataFrame = None,
    profit_data: pd.DataFrame = None,
    ocf_data: pd.DataFrame = None,
    company_name: str = "",
) -> TruthProcessResult:
    """
    处理单个公司

    Args:
        ts_code: 股票代码
        roic_data ~ ocf_data: 各指标探针分析结果
        company_name: 公司名称

    Returns:
        TruthProcessResult: 单个公司的处理结果
    """
    probe_data = {
        'roic': roic_data,
        'roe': roe_data,
        'roiic': roiic_data,
        'gross_margin': gross_margin_data,
        'net_margin': net_margin_data,
        'revenue': revenue_data,
        'profit': profit_data,
        'ocf': ocf_data,
    }

    processor = TruthProcessor()
    return processor.process_company(ts_code, probe_data, company_name)
