"""
Reporting Engine Entry Point
============================

报告生成引擎入口，提供两种报告模式：
1. report_comprehensive: 规则驱动的综合分析报告
2. report_truth: T.R.U.T.H. 数据驱动报告（六维基因+三大求解器）

数据输入：直接接收探针分析结果 DataFrame
"""
import sys
import logging
from pathlib import Path
from typing import Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
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
    description="Generate T.R.U.T.H. data-driven report (no thresholds, pure data speaks)"
)
def report_truth(
    truth_processed: Dict[str, Any] = None,  # 新增：T.R.U.T.H. 处理后的结果
    roic_data: pd.DataFrame = None,
    roe_data: pd.DataFrame = None,
    roiic_data: pd.DataFrame = None,
    gross_margin_data: pd.DataFrame = None,
    net_margin_data: pd.DataFrame = None,
    revenue_data: pd.DataFrame = None,
    profit_data: pd.DataFrame = None,
    ocf_data: pd.DataFrame = None,
    output_path: str = "data/truth_analysis_report.md"
) -> str:
    """
    生成 T.R.U.T.H. 纯数据驱动报告

    支持两种模式:
    1. 使用 truth_processed（推荐）: 从 process_truth 步骤获取专业处理后的数据
    2. 直接使用探针数据（兼容旧版）

    特点：
    - 🧬 六维基因测序（Alpha/Beta/Gamma/Delta欺诈/Delta衰退/Verification）
    - ⚙️ 三大求解器动态评分（重力/速度/结构）
    - 📊 无预设阈值，纯数据说话
    - 🔬 多维度全面分析每家公司

    Args:
        truth_processed: T.R.U.T.H. 处理后的结果（来自 process_truth 步骤）
        roic_data ~ ocf_data: 各指标探针分析结果（兼容旧版）
        output_path: 输出报告路径

    Returns:
        生成的报告内容
    """
    # 构建备用探针数据
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

    # 优先使用 truth_processed
    if truth_processed is not None:
        # 检查 truth_processed 的类型
        if isinstance(truth_processed, dict):
            # 正常的字典返回
            generator = TruthReportGenerator(
                truth_processed=truth_processed,
                probe_data=truth_processed.get('probe_data', probe_data)
            )
        elif hasattr(truth_processed, 'results') and hasattr(truth_processed, 'summary'):
            # BatchProcessResult 对象（可能从缓存恢复时类型变化）
            # 静默处理，不再输出警告
            logger.debug(f"truth_processed 是 BatchProcessResult 对象，正在适配")
            generator = TruthReportGenerator(
                truth_processed={
                    'processed_results': truth_processed,
                    'results_df': pd.DataFrame(),
                    'summary': truth_processed.summary,
                    'probe_data': probe_data,
                },
                probe_data=probe_data
            )
        else:
            # 其他未知类型，回退到探针数据
            logger.debug(f"truth_processed 类型未知 ({type(truth_processed)}), 使用探针数据")
            generator = TruthReportGenerator(probe_data=probe_data)
    else:
        # 兼容旧版：直接使用探针数据
        generator = TruthReportGenerator(probe_data=probe_data)

    return generator.generate_report(output_path=output_path)


@register_method(
    engine_name="report_truth_single",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate T.R.U.T.H. report for a single stock"
)
def report_truth_single(
    stock_code: str,
    roic_data: pd.DataFrame = None,
    roe_data: pd.DataFrame = None,
    roiic_data: pd.DataFrame = None,
    gross_margin_data: pd.DataFrame = None,
    net_margin_data: pd.DataFrame = None,
    revenue_data: pd.DataFrame = None,
    profit_data: pd.DataFrame = None,
    ocf_data: pd.DataFrame = None,
    output_path: str = None
) -> str:
    """
    生成单只股票的 T.R.U.T.H. 深度分析报告

    Args:
        stock_code: 股票代码
        roic_data ~ ocf_data: 各指标探针分析结果
        output_path: 输出路径（可选）

    Returns:
        生成的报告内容
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

    generator = TruthReportGenerator(probe_data=probe_data)
    return generator.generate_single_stock_report(
        stock_code=stock_code,
        output_path=output_path
    )

