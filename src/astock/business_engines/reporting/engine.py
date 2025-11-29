"""
Reporting Engine Entry Point
============================
Registers the generic reporting method.
"""
import sys
from pathlib import Path
from typing import Dict, Any

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from orchestrator.decorators.register import register_method
from ..core.interfaces import ScoreResult
from .generic_reporter import GenericReporter
from .comprehensive_generator import ComprehensiveReportGenerator

@register_method(
    engine_name="report_generic",
    component_type="business_engine",
    engine_type="reporting",
    description="Generic reporting entry point"
)
def report_generic(result: ScoreResult, config: Dict[str, Any] = None, output_path: str = None) -> str:
    """
    Generic reporting function using the new architecture.
    """
    reporter = GenericReporter()
    report_text = reporter.generate(result, config)

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(report_text, encoding='utf-8')

    return report_text

@register_method(
    engine_name="report_comprehensive",
    component_type="business_engine",
    engine_type="reporting",
    description="Generate comprehensive trend analysis report from multiple metrics"
)
def report_comprehensive(data_dir: str = "data/filter_middle", output_path: str = "data/comprehensive_analysis_report.md") -> str:
    """
    生成综合趋势分析报告
    """
    generator = ComprehensiveReportGenerator(data_dir=data_dir)
    return generator.generate_report(output_path=output_path)

