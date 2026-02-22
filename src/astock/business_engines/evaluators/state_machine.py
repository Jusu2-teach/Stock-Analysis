"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 状态机模块
═══════════════════════════════════════════════════════════════════════════════

基于隐马尔可夫模型（HMM）的公司生命周期状态推断。
状态: EMERGING → GROWTH → MATURE → CASH_COW → DECLINING → DISTRESSED

关键特性：
- 从财务特征推断潜在状态
- 预测未来状态转移概率
- 不同状态应用不同评估标准

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
import numpy as np
from numpy.typing import NDArray


class CompanyState(Enum):
    """公司生命周期状态"""
    EMERGING = "emerging"           # 新兴成长
    GROWTH = "growth"               # 快速成长
    MATURE = "mature"               # 成熟稳定
    CASH_COW = "cash_cow"           # 现金牛
    SLOWING = "slowing"             # 增速放缓（过渡）
    DECLINING = "declining"         # 衰退期
    DISTRESSED = "distressed"       # 困境期
    TURNAROUND = "turnaround"       # 反转期
    CYCLICAL_PEAK = "cyclical_peak" # 周期顶
    CYCLICAL_TROUGH = "cyclical_trough"  # 周期底


class QualityClass(Enum):
    """状态质量分类"""
    QUALITY = "quality"      # 优质状态
    UNCERTAIN = "uncertain"  # 不确定状态
    POOR = "poor"           # 劣质状态


@dataclass
class StateProfile:
    """状态特征描述"""

    state: CompanyState
    name: str
    description: str
    characteristics: Dict[str, Tuple[float, float]]  # metric -> (min, max) expected range
    investment_implication: str
    typical_duration_years: Tuple[int, int]
    quality_class: QualityClass

    def matches_observation(
        self,
        observed_features: Dict[str, float]
    ) -> float:
        """
        计算观测特征与状态特征的匹配度

        Returns:
            0-1 范围的匹配分数
        """
        if not self.characteristics:
            return 0.5  # 无特征定义时返回中性值

        scores = []
        for metric, (min_val, max_val) in self.characteristics.items():
            if metric in observed_features:
                value = observed_features[metric]
                # 计算值在范围内的程度
                if min_val <= value <= max_val:
                    # 完全在范围内
                    range_width = max_val - min_val
                    if range_width > 0:
                        # 距离中心越近得分越高
                        center = (min_val + max_val) / 2
                        distance_from_center = abs(value - center) / (range_width / 2)
                        score = 1.0 - 0.3 * distance_from_center
                    else:
                        score = 1.0
                else:
                    # 范围外，计算距离惩罚
                    if value < min_val:
                        distance = (min_val - value) / max(abs(min_val), 0.01)
                    else:
                        distance = (value - max_val) / max(abs(max_val), 0.01)
                    score = max(0.0, 1.0 - distance)

                scores.append(score)

        return np.mean(scores) if scores else 0.5


@dataclass
class StateInference:
    """状态推断结果"""

    most_likely_state: CompanyState
    state_probabilities: Dict[CompanyState, float]
    confidence: float
    quality_class: QualityClass
    next_state_prediction: Dict[CompanyState, float]
    expected_duration_years: float

    def __repr__(self) -> str:
        return (
            f"StateInference(state={self.most_likely_state.value}, "
            f"prob={self.state_probabilities[self.most_likely_state]:.2%}, "
            f"quality={self.quality_class.value})"
        )


class CompanyStateMachine:
    """
    公司生命周期状态机

    基于 HMM 思想推断公司当前状态和预测未来转移。

    Example:
        >>> machine = CompanyStateMachine()
        >>> features = {
        ...     "revenue_growth": 0.25,
        ...     "roic_level": 18.0,
        ...     "roic_trend": 0.03,
        ...     "volatility": 0.20
        ... }
        >>> inference = machine.infer_state(features)
        >>> print(inference.most_likely_state)  # GROWTH
    """

    def __init__(self):
        self._profiles: Dict[CompanyState, StateProfile] = {}
        self._transition_matrix: Dict[CompanyState, Dict[CompanyState, float]] = {}
        self._quality_mapping: Dict[QualityClass, List[CompanyState]] = {
            QualityClass.QUALITY: [],
            QualityClass.UNCERTAIN: [],
            QualityClass.POOR: []
        }

    @classmethod
    def from_config(cls, config_path: str | Path) -> 'CompanyStateMachine':
        """从 YAML 配置加载状态机"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        machine = cls()

        # 加载状态描述
        for state_name, state_config in config.get('states', {}).items():
            try:
                state = CompanyState(state_name)
            except ValueError:
                continue

            # 解析特征范围
            characteristics = {}
            for metric, range_vals in state_config.get('characteristics', {}).items():
                if isinstance(range_vals, list) and len(range_vals) == 2:
                    characteristics[metric] = (range_vals[0], range_vals[1])

            # 解析持续时间
            duration = state_config.get('typical_duration_years', [2, 5])
            if isinstance(duration, list) and len(duration) == 2:
                duration_tuple = (duration[0], duration[1])
            else:
                duration_tuple = (2, 5)

            # 解析质量分类
            quality_str = state_config.get('quality_class', 'uncertain')
            try:
                quality_class = QualityClass(quality_str)
            except ValueError:
                quality_class = QualityClass.UNCERTAIN

            profile = StateProfile(
                state=state,
                name=state_config.get('name', state_name),
                description=state_config.get('description', ''),
                characteristics=characteristics,
                investment_implication=state_config.get('investment_implication', ''),
                typical_duration_years=duration_tuple,
                quality_class=quality_class
            )

            machine._profiles[state] = profile
            machine._quality_mapping[quality_class].append(state)

        # 加载转移矩阵
        for from_state_name, transitions in config.get('transition_matrix', {}).items():
            try:
                from_state = CompanyState(from_state_name)
            except ValueError:
                continue

            machine._transition_matrix[from_state] = {}
            for to_state_name, prob in transitions.items():
                try:
                    to_state = CompanyState(to_state_name)
                    machine._transition_matrix[from_state][to_state] = prob
                except ValueError:
                    continue

        return machine

    @classmethod
    def with_defaults(cls) -> 'CompanyStateMachine':
        """使用默认配置创建状态机"""
        machine = cls()

        # 定义默认状态描述
        default_profiles = [
            StateProfile(
                CompanyState.EMERGING, "新兴成长", "早期高增长阶段",
                {"revenue_growth": (0.30, 1.00), "roic_level": (-5.0, 10.0), "volatility": (0.30, 0.70)},
                "高风险高回报", (2, 5), QualityClass.UNCERTAIN
            ),
            StateProfile(
                CompanyState.GROWTH, "快速成长", "成长期",
                {"revenue_growth": (0.15, 0.50), "roic_level": (8.0, 25.0), "roic_trend": (0.0, 0.15)},
                "成长股投资", (3, 7), QualityClass.QUALITY
            ),
            StateProfile(
                CompanyState.MATURE, "成熟稳定", "成熟期",
                {"revenue_growth": (0.0, 0.15), "roic_level": (10.0, 20.0), "volatility": (0.05, 0.20)},
                "价值股投资", (5, 15), QualityClass.QUALITY
            ),
            StateProfile(
                CompanyState.CASH_COW, "现金牛", "稳定高分红",
                {"revenue_growth": (-0.05, 0.10), "roic_level": (12.0, 25.0), "volatility": (0.03, 0.15)},
                "长期持有收息", (5, 20), QualityClass.QUALITY
            ),
            StateProfile(
                CompanyState.SLOWING, "增速放缓", "过渡期",
                {"revenue_growth": (0.05, 0.20), "roic_trend": (-0.05, 0.02)},
                "关注转型", (1, 3), QualityClass.UNCERTAIN
            ),
            StateProfile(
                CompanyState.DECLINING, "衰退期", "负增长",
                {"revenue_growth": (-0.15, 0.0), "roic_trend": (-0.10, -0.02)},
                "风险较高", (2, 5), QualityClass.POOR
            ),
            StateProfile(
                CompanyState.DISTRESSED, "困境期", "危机状态",
                {"revenue_growth": (-0.30, 0.0), "roic_level": (-20.0, 3.0)},
                "高风险", (1, 3), QualityClass.POOR
            ),
            StateProfile(
                CompanyState.TURNAROUND, "反转期", "触底回升",
                {"revenue_growth": (0.0, 0.30), "roic_trend": (0.05, 0.25)},
                "逆向投资", (1, 3), QualityClass.UNCERTAIN
            ),
        ]

        for profile in default_profiles:
            machine._profiles[profile.state] = profile
            machine._quality_mapping[profile.quality_class].append(profile.state)

        # 默认转移矩阵（简化版）
        machine._transition_matrix = {
            CompanyState.EMERGING: {CompanyState.EMERGING: 0.4, CompanyState.GROWTH: 0.35, CompanyState.DISTRESSED: 0.15, CompanyState.SLOWING: 0.1},
            CompanyState.GROWTH: {CompanyState.GROWTH: 0.55, CompanyState.MATURE: 0.2, CompanyState.SLOWING: 0.15, CompanyState.DISTRESSED: 0.05, CompanyState.CASH_COW: 0.05},
            CompanyState.MATURE: {CompanyState.MATURE: 0.65, CompanyState.CASH_COW: 0.15, CompanyState.DECLINING: 0.1, CompanyState.GROWTH: 0.05, CompanyState.SLOWING: 0.05},
            CompanyState.CASH_COW: {CompanyState.CASH_COW: 0.7, CompanyState.MATURE: 0.15, CompanyState.DECLINING: 0.1, CompanyState.DISTRESSED: 0.05},
            CompanyState.SLOWING: {CompanyState.MATURE: 0.4, CompanyState.GROWTH: 0.25, CompanyState.DECLINING: 0.2, CompanyState.SLOWING: 0.15},
            CompanyState.DECLINING: {CompanyState.DECLINING: 0.45, CompanyState.DISTRESSED: 0.25, CompanyState.TURNAROUND: 0.15, CompanyState.MATURE: 0.1, CompanyState.CASH_COW: 0.05},
            CompanyState.DISTRESSED: {CompanyState.DISTRESSED: 0.35, CompanyState.TURNAROUND: 0.3, CompanyState.DECLINING: 0.35},
            CompanyState.TURNAROUND: {CompanyState.GROWTH: 0.35, CompanyState.MATURE: 0.25, CompanyState.TURNAROUND: 0.2, CompanyState.DISTRESSED: 0.15, CompanyState.SLOWING: 0.05},
        }

        return machine

    def infer_state(
        self,
        observed_features: Dict[str, float],
        prior_state: Optional[CompanyState] = None
    ) -> StateInference:
        """
        从观测特征推断当前状态

        Args:
            observed_features: 观测到的特征字典
                - revenue_growth: 营收增速
                - roic_level: ROIC水平
                - roic_trend: ROIC趋势
                - volatility: 波动率
                - 等等
            prior_state: 先验状态（如果已知上一期状态）

        Returns:
            StateInference 包含状态概率分布和预测
        """
        # 计算每个状态的似然度（基于特征匹配）
        likelihoods: Dict[CompanyState, float] = {}

        for state, profile in self._profiles.items():
            likelihood = profile.matches_observation(observed_features)

            # 如果有先验状态，应用转移概率作为先验
            if prior_state and prior_state in self._transition_matrix:
                transition_prob = self._transition_matrix[prior_state].get(state, 0.01)
                # 贝叶斯更新: P(state|obs) ∝ P(obs|state) × P(state|prior)
                likelihood *= transition_prob

            likelihoods[state] = likelihood

        # 归一化得到后验概率
        total_likelihood = sum(likelihoods.values())
        if total_likelihood > 0:
            probabilities = {s: l / total_likelihood for s, l in likelihoods.items()}
        else:
            # 均匀分布作为兜底
            n_states = len(self._profiles)
            probabilities = {s: 1.0 / n_states for s in self._profiles}

        # 找到最可能的状态
        most_likely = max(probabilities.items(), key=lambda x: x[1])
        most_likely_state = most_likely[0]
        max_prob = most_likely[1]

        # 计算置信度（基于概率分布的熵）
        probs_array = np.array(list(probabilities.values()))
        entropy = -np.sum(probs_array * np.log(probs_array + 1e-10))
        max_entropy = np.log(min(len(self._profiles), 5))
        confidence = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 0.5
        confidence = max(0.1, min(1.0, confidence))

        # 获取质量分类
        quality_class = self._profiles[most_likely_state].quality_class

        # 预测下一状态
        next_state_prediction = self._predict_next_state(most_likely_state)

        # 预期持续时间
        profile = self._profiles[most_likely_state]
        expected_duration = (profile.typical_duration_years[0] + profile.typical_duration_years[1]) / 2

        return StateInference(
            most_likely_state=most_likely_state,
            state_probabilities=probabilities,
            confidence=confidence,
            quality_class=quality_class,
            next_state_prediction=next_state_prediction,
            expected_duration_years=expected_duration
        )

    def _predict_next_state(
        self,
        current_state: CompanyState
    ) -> Dict[CompanyState, float]:
        """预测下一状态的概率分布"""
        if current_state in self._transition_matrix:
            return self._transition_matrix[current_state].copy()

        # 默认：保持当前状态
        return {current_state: 1.0}

    def multi_step_forecast(
        self,
        initial_state: CompanyState,
        steps: int = 5
    ) -> List[Dict[CompanyState, float]]:
        """
        多步状态预测

        Args:
            initial_state: 初始状态
            steps: 预测步数

        Returns:
            每一步的状态概率分布列表
        """
        forecasts = []
        current_dist = {initial_state: 1.0}

        for _ in range(steps):
            next_dist: Dict[CompanyState, float] = {}

            for state, prob in current_dist.items():
                if state in self._transition_matrix:
                    for next_state, trans_prob in self._transition_matrix[state].items():
                        if next_state not in next_dist:
                            next_dist[next_state] = 0.0
                        next_dist[next_state] += prob * trans_prob

            # 归一化
            total = sum(next_dist.values())
            if total > 0:
                next_dist = {s: p / total for s, p in next_dist.items()}

            forecasts.append(next_dist)
            current_dist = next_dist

        return forecasts

    def get_quality_score_adjustment(
        self,
        state: CompanyState
    ) -> float:
        """
        根据状态获取质量分数调整值

        Returns:
            正数为加分，负数为扣分
        """
        profile = self._profiles.get(state)
        if not profile:
            return 0.0

        quality_adjustments = {
            QualityClass.QUALITY: 10.0,
            QualityClass.UNCERTAIN: 0.0,
            QualityClass.POOR: -15.0
        }

        return quality_adjustments.get(profile.quality_class, 0.0)

    def is_quality_state(self, state: CompanyState) -> bool:
        """判断是否为优质状态"""
        return state in self._quality_mapping[QualityClass.QUALITY]

    def get_investment_implication(self, state: CompanyState) -> str:
        """获取投资含义"""
        profile = self._profiles.get(state)
        return profile.investment_implication if profile else "未知"


# ═══════════════════════════════════════════════════════════════════════════════
# 工厂函数
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_MACHINE: Optional[CompanyStateMachine] = None


def get_default_state_machine() -> CompanyStateMachine:
    """获取默认状态机（单例）"""
    global _DEFAULT_MACHINE
    if _DEFAULT_MACHINE is None:
        _DEFAULT_MACHINE = CompanyStateMachine.with_defaults()
    return _DEFAULT_MACHINE


def infer_company_state(
    revenue_growth: float,
    roic_level: float,
    roic_trend: float = 0.0,
    volatility: float = 0.2
) -> StateInference:
    """便捷状态推断函数"""
    machine = get_default_state_machine()
    features = {
        "revenue_growth": revenue_growth,
        "roic_level": roic_level,
        "roic_trend": roic_trend,
        "volatility": volatility
    }
    return machine.infer_state(features)
