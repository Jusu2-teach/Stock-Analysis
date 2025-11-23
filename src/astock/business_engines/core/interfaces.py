from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union, List
from pathlib import Path
import pandas as pd
from dataclasses import dataclass

@dataclass
class AnalysisResult:
    """Standardized result from an analysis engine."""
    data: pd.DataFrame
    metric_name: str
    metadata: Dict[str, Any]

    def get_column(self, suffix: str) -> pd.Series:
        """Helper to get a column based on metric name and suffix."""
        col_name = f"{self.metric_name}_{suffix}"
        if col_name not in self.data.columns:
            # Try without metric name if generic
            if suffix in self.data.columns:
                return self.data[suffix]
            return pd.Series(index=self.data.index, dtype=float) # Return empty or raise?
        return self.data[col_name]

@dataclass
class ScoreResult:
    """Standardized result from a scoring engine."""
    data: pd.DataFrame
    score_col: str
    grade_col: str
    metadata: Dict[str, Any]

class IAnalyzer(ABC):
    """Interface for business analysis engines."""

    @abstractmethod
    def analyze(self, data: Any, config: Dict[str, Any]) -> AnalysisResult:
        """Perform analysis and return standardized result."""
        pass

class IScorer(ABC):
    """Interface for scoring engines."""

    @abstractmethod
    def score(self, result: AnalysisResult, config: Dict[str, Any]) -> ScoreResult:
        """Apply scoring rules to analysis result."""
        pass

class IReporter(ABC):
    """Interface for reporting engines."""

    @abstractmethod
    def generate(self, result: ScoreResult, config: Dict[str, Any]) -> str:
        """Generate a human-readable report."""
        pass
