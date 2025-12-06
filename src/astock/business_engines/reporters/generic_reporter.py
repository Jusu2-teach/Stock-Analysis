from __future__ import annotations
from typing import Any, Dict
import pandas as pd
from ..core.interfaces import IReporter, ScoreResult

class GenericReporter(IReporter):

    def generate(self, result: ScoreResult, config: Dict[str, Any] = None) -> str:
        df = result.data
        score_col = result.score_col
        grade_col = result.grade_col

        lines = []
        def add(s=""): lines.append(s)

        add("=" * 80)
        add("通用质量评分报告")
        add("=" * 80)
        add()

        # Grade Distribution
        add("【评级分布】")
        if grade_col in df.columns:
            dist = df[grade_col].value_counts().sort_index()
            for g in ['S', 'A', 'B', 'C', 'D', 'F']:
                count = dist.get(g, 0)
                pct = count / len(df) * 100 if len(df) > 0 else 0
                add(f"  {g}级: {count:3d}家 ({pct:5.1f}%)")
        add()

        # Score Stats
        add("【得分统计】")
        if score_col in df.columns:
            add(f"  平均分: {df[score_col].mean():.2f}")
            add(f"  中位数: {df[score_col].median():.2f}")
            add(f"  最高分: {df[score_col].max():.2f}")
            add(f"  最低分: {df[score_col].min():.2f}")
        add()

        # Top/Bottom Lists
        sorted_df = df.sort_values(score_col, ascending=False)

        add("【Top 20 最高分】")
        for i, row in enumerate(sorted_df.head(20).itertuples(), 1):
            name = getattr(row, 'name', 'Unknown')
            code = getattr(row, 'ts_code', 'Unknown')
            score = getattr(row, score_col, 0)
            grade = getattr(row, grade_col, '-')
            rec = getattr(row, 'recommendation', '')
            add(f"  {i:2d}. {code} {name} | 分数 {score:.2f} | 评级 {grade} | {rec}")

        add()
        add("【Bottom 20 最低分】")
        for i, row in enumerate(sorted_df.tail(20).itertuples(), 1):
            name = getattr(row, 'name', 'Unknown')
            code = getattr(row, 'ts_code', 'Unknown')
            score = getattr(row, score_col, 0)
            grade = getattr(row, grade_col, '-')
            add(f"  {i:2d}. {code} {name} | 分数 {score:.2f} | 评级 {grade}")

        add()
        add("=" * 80)

        return "\n".join(lines)
