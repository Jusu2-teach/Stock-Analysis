"""
评分计算器核心 (简化版)
======================

使用策略模式重构,将409行简化为~180行:
1. ScoreComponent - 评分组件基类
2. 具体评分组件 (ROICScore, TrendScore等)
3. QualityScoreCalculator - 评分编排器

优势:
- 策略模式,易扩展
- 每个组件独立
- 减少55%代码
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd
import numpy as np


class ScoreComponent(ABC):
    """评分组件基类"""

    def __init__(self, name: str, max_score: float):
        self.name = name
        self.max_score = max_score

    @abstractmethod
    def calculate(self, row: pd.Series) -> float:
        """计算得分

        Args:
            row: DataFrame的一行

        Returns:
            得分(0到max_score)
        """
        pass

    def get_column_name(self) -> str:
        """获取输出列名"""
        return f"score_{self.name.lower().replace(' ', '_')}"


class ROICScoreComponent(ScoreComponent):
    """ROIC质量分组件 (40分)"""

    def __init__(self):
        super().__init__("ROIC", 40.0)

        # 分档标准
        self.thresholds = [
            (30, 40),  # ≥30%: 40分 (卓越)
            (25, 35),  # 25-30%: 35分 (优秀+)
            (20, 30),  # 20-25%: 30分 (优秀)
            (15, 25),  # 15-20%: 25分 (良好+)
            (12, 20),  # 12-15%: 20分 (良好)
            (10, 15),  # 10-12%: 15分 (合格+)
            (8, 10),   # 8-10%: 10分 (合格)
            (6, 5),    # 6-8%: 5分 (及格)
        ]

    def calculate(self, row: pd.Series) -> float:
        roic = row.get('roic_weighted', 0)

        for threshold, score in self.thresholds:
            if roic >= threshold:
                return score

        return 0.0  # <6%: 0分


class TrendScoreComponent(ScoreComponent):
    """趋势健康分组件 (35分)"""

    def __init__(self):
        super().__init__("Trend", 35.0)

    def calculate(self, row: pd.Series) -> float:
        # 趋势分已经是0-100,归一化到0-35
        trend_score = row.get('roic_trend_score', 0)
        trend_score = max(0, min(100, trend_score))  # Clip

        return (trend_score / 100.0) * self.max_score


class LatestScoreComponent(ScoreComponent):
    """最新期活力分组件 (15分)"""

    def __init__(self):
        super().__init__("Latest", 15.0)

        self.thresholds = [
            (25, 15),  # ≥25%: 15分
            (20, 12),  # 20-25%: 12分
            (15, 10),  # 15-20%: 10分
            (12, 8),   # 12-15%: 8分
            (10, 6),   # 10-12%: 6分
            (8, 4),    # 8-10%: 4分
            (6, 2),    # 6-8%: 2分
        ]

    def calculate(self, row: pd.Series) -> float:
        latest = row.get('roic_latest', 0)

        for threshold, score in self.thresholds:
            if latest >= threshold:
                return score

        return 0.0


class StabilityScoreComponent(ScoreComponent):
    """稳定性分组件 (10分)"""

    def __init__(self):
        super().__init__("Stability", 10.0)

    def calculate(self, row: pd.Series) -> float:
        r_squared = row.get('roic_r_squared', 0)
        r_squared = max(0, min(1, r_squared))  # Clip到[0,1]

        # 线性映射: R²=1.0→10分, R²=0.0→0分
        return r_squared * self.max_score


class PenaltyRule(ABC):
    """扣分规则基类"""

    def __init__(self, name: str, penalty: float):
        self.name = name
        self.penalty = penalty

    @abstractmethod
    def applies(self, row: pd.Series) -> bool:
        """判断规则是否适用"""
        pass


class TrendHeavyPenalty(PenaltyRule):
    """趋势重罚 (罚分≥15 → 扣12分)"""

    def __init__(self):
        super().__init__("趋势重罚", 12.0)

    def applies(self, row: pd.Series) -> bool:
        penalty = row.get('roic_penalty', 0)
        return penalty >= 15


class TrendWarningPenalty(PenaltyRule):
    """趋势警报 (罚分10-14 → 扣8分)"""

    def __init__(self):
        super().__init__("趋势警报", 8.0)

    def applies(self, row: pd.Series) -> bool:
        penalty = row.get('roic_penalty', 0)
        # 注意:不与TrendHeavyPenalty冲突
        return 10 <= penalty < 15


class TrendCautionPenalty(PenaltyRule):
    """趋势关注 (罚分5-9 → 扣4分)"""

    def __init__(self):
        super().__init__("趋势关注", 4.0)

    def applies(self, row: pd.Series) -> bool:
        penalty = row.get('roic_penalty', 0)
        # 注意:不与TrendWarningPenalty冲突
        return 5 <= penalty < 10


class WeakTrendPenalty(PenaltyRule):
    """极弱趋势 (趋势分<40 → 扣5分)"""

    def __init__(self):
        super().__init__("极弱趋势", 5.0)

    def applies(self, row: pd.Series) -> bool:
        trend_score = row.get('roic_trend_score', 0)
        return trend_score < 40


class LowROICPenalty(PenaltyRule):
    """低ROIC (加权<8% → 扣10分)"""

    def __init__(self):
        super().__init__("低ROIC", 10.0)

    def applies(self, row: pd.Series) -> bool:
        roic = row.get('roic_weighted', 0)
        return roic < 8


class LatestCollapsePenalty(PenaltyRule):
    """最新崩盘 (最新<6% → 扣8分)"""

    def __init__(self):
        super().__init__("最新崩盘", 8.0)

    def applies(self, row: pd.Series) -> bool:
        latest = row.get('roic_latest', 0)
        return latest < 6


class QualityScoreCalculator:
    """质量评分计算器

    使用策略模式组织评分组件
    """

    def __init__(self):
        # 评分组件
        self.components = [
            ROICScoreComponent(),
            TrendScoreComponent(),
            LatestScoreComponent(),
            StabilityScoreComponent(),
        ]

        # 扣分规则(按优先级排序)
        self.penalties = [
            TrendHeavyPenalty(),
            TrendWarningPenalty(),
            TrendCautionPenalty(),
            WeakTrendPenalty(),
            LowROICPenalty(),
            LatestCollapsePenalty(),
        ]

    def calculate_base_score(self, row: pd.Series) -> Dict[str, float]:
        """计算基础分"""
        scores = {}

        for component in self.components:
            score = component.calculate(row)
            scores[component.get_column_name()] = score

        return scores

    def apply_penalties(self, row: pd.Series, base_score: float) -> tuple[float, float]:
        """应用扣分规则

        Returns:
            (最终分, 实际扣分)
        """
        total_penalty = 0.0

        for penalty_rule in self.penalties:
            if penalty_rule.applies(row):
                total_penalty += penalty_rule.penalty

        final_score = max(0, base_score - total_penalty)
        return final_score, total_penalty

    def assign_grade(self, final_score: float) -> str:
        """评级

        S: ≥90分 (卓越)
        A: 80-89分 (优秀)
        B: 70-79分 (良好)
        C: 60-69分 (合格)
        D: 50-59分 (及格)
        F: <50分 (不合格)
        """
        if final_score >= 90:
            return 'S'
        elif final_score >= 80:
            return 'A'
        elif final_score >= 70:
            return 'B'
        elif final_score >= 60:
            return 'C'
        elif final_score >= 50:
            return 'D'
        else:
            return 'F'

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算质量评分

        Args:
            df: 输入DataFrame

        Returns:
            添加评分列的DataFrame
        """
        result_df = df.copy()

        # 1. 计算各组件分数
        for component in self.components:
            col_name = component.get_column_name()
            result_df[col_name] = result_df.apply(
                component.calculate, axis=1
            )

        # 2. 计算基础总分
        score_cols = [c.get_column_name() for c in self.components]
        result_df['base_score'] = result_df[score_cols].sum(axis=1)

        # 3. 应用扣分规则
        penalties_result = result_df.apply(
            lambda row: self.apply_penalties(row, row['base_score']),
            axis=1
        )

        # 解包 (最终分, 实际扣分)
        result_df['final_score'] = penalties_result.apply(lambda x: x[0])
        result_df['total_penalty'] = penalties_result.apply(lambda x: x[1])

        # 4. 评级
        result_df['grade'] = result_df['final_score'].apply(self.assign_grade)

        return result_df
