"""
Reporting Engine Entry Point
============================

报告生成引擎入口，提供两种报告模式：
1. report_comprehensive: 规则驱动的综合分析报告
2. report_truth: T.R.U.T.H. 数据驱动报告（六维基因+三大求解器）

数据输入：直接接收探针分析结果 DataFrame
"""
import logging
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

from orchestrator.decorators.register import register_method
from .comprehensive_generator import ComprehensiveReportGenerator
from .truth_report_generator import TruthReportGenerator


@register_method(
    engine_name="report_comprehensive",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate comprehensive trend analysis report from probe results"
)
def report_comprehensive(
    roic_data: pd.DataFrame = None,
    roe_data: pd.DataFrame = None,
    roiic_data: pd.DataFrame = None,
    gross_margin_data: pd.DataFrame = None,
    net_margin_data: pd.DataFrame = None,
    revenue_data: pd.DataFrame = None,
    profit_data: pd.DataFrame = None,
    ocf_data: pd.DataFrame = None,
    output_path: str = "data/comprehensive_analysis_report.md"
) -> str:
    """
    生成综合趋势分析报告（规则驱动，基于预设阈值）

    Args:
        roic_data: ROIC趋势分析结果
        roe_data: ROE趋势分析结果
        roiic_data: ROIIC趋势分析结果
        gross_margin_data: 毛利率趋势分析结果
        net_margin_data: 净利率趋势分析结果
        revenue_data: 营收趋势分析结果
        profit_data: 利润趋势分析结果
        ocf_data: 现金流趋势分析结果
        output_path: 输出报告路径
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

    generator = ComprehensiveReportGenerator(probe_data=probe_data)
    return generator.generate_report(output_path=output_path)


# ═══════════════════════════════════════════════════════════════════════════════
# T.R.U.T.H. 报告引擎 - 纯数据驱动，无阈值
# ═══════════════════════════════════════════════════════════════════════════════

@register_method(
    engine_name="report_truth",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. data-driven report from processed results (no thresholds, pure data speaks)"
)
def report_truth(
    truth_processed: Dict[str, Any],
    output_path: str = "data/truth_analysis_report.md"
) -> str:
    """生成 T.R.U.T.H. 纯数据驱动报告（仅支持基于 process_truth 的新架构）

    Args:
        truth_processed: T.R.U.T.H. 处理后的结果（来自 process_truth 步骤）
        output_path: 输出报告路径

    Returns:
        生成的报告内容
    """
    if truth_processed is None:
        raise ValueError("truth_processed 不能为空，请先通过 process_truth 步骤获取处理结果")

    # 标准化 truth_processed 结构
    if isinstance(truth_processed, dict):
        generator = TruthReportGenerator(
            truth_processed=truth_processed,
        )
    elif hasattr(truth_processed, 'results') and hasattr(truth_processed, 'summary'):
        # BatchProcessResult 对象（可能从缓存恢复时类型变化）
        logger.debug("truth_processed 是 BatchProcessResult 对象，正在适配")
        generator = TruthReportGenerator(
            truth_processed={
                'processed_results': truth_processed,
                'results_df': pd.DataFrame(),
                'summary': truth_processed.summary,
            },
        )
    else:
        raise TypeError(f"不支持的 truth_processed 类型: {type(truth_processed)}")

    return generator.generate_report(output_path=output_path)


@register_method(
    engine_name="report_truth_single",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. report for a single stock"
)
def report_truth_single(
    stock_code: str,
    truth_processed: Dict[str, Any],
    output_path: str = None
) -> str:
    """
    生成单只股票的 T.R.U.T.H. 深度分析报告

    Args:
        stock_code: 股票代码
        truth_processed: T.R.U.T.H. 处理后的结果（来自 process_truth 步骤）
        output_path: 输出路径（可选）

    Returns:
        生成的报告内容
    """
    if truth_processed is None:
        raise ValueError("truth_processed 不能为空，请先通过 process_truth 步骤获取处理结果")

    # 与多股报告保持同一数据入口，仅在报告层筛选单只股票
    generator = TruthReportGenerator(truth_processed=truth_processed)
    return generator.generate_single_stock_report(
        stock_code=stock_code,
        output_path=output_path
    )

