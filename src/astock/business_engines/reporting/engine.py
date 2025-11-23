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
