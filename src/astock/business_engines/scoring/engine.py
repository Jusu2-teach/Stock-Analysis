"""
Scoring Engine Entry Point
==========================
Registers the generic scoring method.
"""
import sys
from pathlib import Path
from typing import Dict, Any

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from orchestrator.decorators.register import register_method
from ..core.interfaces import AnalysisResult, ScoreResult
from .generic_scorer import GenericQualityScorer

@register_method(
    engine_name="score_quality",
    component_type="business_engine",
    engine_type="scoring",
    description="Generic scoring entry point"
)
def score_quality(result: AnalysisResult = None, data: AnalysisResult = None, config: Dict[str, Any] = None, **kwargs) -> ScoreResult:
    """
    Generic scoring function using the new architecture.
    """
    target_result = result if result is not None else data
    if target_result is None:
        raise ValueError("Either 'result' or 'data' must be provided")

    scorer = GenericQualityScorer()
    return scorer.score(target_result, config)
