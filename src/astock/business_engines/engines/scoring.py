"""业务引擎 - 质量评分实现"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Union

import pandas as pd

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from orchestrator import register_method
from ..scoring.quality_score import calculate_quality_score, generate_quality_report


def _ensure_dataframe(data: Union[str, Path, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.copy()
    path = Path(data)
    if not path.exists():
        raise FileNotFoundError(f"数据文件不存在: {path}")
    return pd.read_csv(path)


@register_method(
    engine_name="score_quality",
    component_type="business_engine",
    engine_type="scoring",
    description="对趋势分析结果进行质量评分 (0-100)"
)
def score_quality(
    data: Union[str, Path, pd.DataFrame],
    output_path: Optional[Union[str, Path]] = None,
    report_path: Optional[Union[str, Path]] = None
) -> pd.DataFrame:
    """根据 ROIC 趋势结果计算质量评分。

    Args:
    data: ROIC 趋势分析结果 (DataFrame 或 CSV 路径)
    output_path: 若提供, 额外将评分结果写入该路径
    report_path: 若提供, 将评分报告写入该路径

    Returns:
        带有质量评分、评级等字段的 DataFrame
    """

    df_input = _ensure_dataframe(data)
    scored_df = calculate_quality_score(df_input)

    if report_path:
        generate_quality_report(scored_df, output_path=report_path)

    if output_path:
        out_path = Path(output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scored_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    return scored_df
