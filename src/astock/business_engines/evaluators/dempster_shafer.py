"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - Dempster-Shafer 证据理论模块
═══════════════════════════════════════════════════════════════════════════════

实现 Dempster-Shafer 证据理论的信度融合。
核心优势：显式处理"不确定性"，区分"不知道"和"反对"。

关键概念：
- 信度函数 (Belief): 完全支持某假设的证据强度
- 似然函数 (Plausibility): 不反对某假设的证据强度
- 不确定性区间: [Belief, Plausibility]
- Dempster 组合规则: 融合多个独立证据源

处理冲突：
- 使用 Yager 修正规则处理高冲突情况
- 冲突系数 K 过高时发出警告

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Set, Tuple, Union

import numpy as np


# 假设类型
Hypothesis = str
HypothesisSet = FrozenSet[str]


def make_hypothesis_set(*hypotheses: str) -> HypothesisSet:
    """创建假设集合"""
    return frozenset(hypotheses)


@dataclass
class MassFunction:
    """
    基本概率赋值 (Basic Probability Assignment, BPA)

    m: 2^Θ → [0, 1]
    对于识别框架 Θ 的所有子集赋予概率质量
    """

    frame: HypothesisSet  # 识别框架 Θ
    masses: Dict[HypothesisSet, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # 确保质量和为1
        total = sum(self.masses.values())
        if total > 0 and not np.isclose(total, 1.0, atol=0.01):
            self.masses = {k: v / total for k, v in self.masses.items()}

        # 移除零质量
        self.masses = {k: v for k, v in self.masses.items() if v > 1e-10}

    @classmethod
    def from_belief_disbelief(
        cls,
        frame: HypothesisSet,
        target: Hypothesis,
        belief: float,
        disbelief: float,
        uncertainty: float
    ) -> 'MassFunction':
        """
        从信度/不信度/不确定性创建 BPA

        Args:
            frame: 识别框架（所有可能假设）
            target: 目标假设
            belief: 支持目标假设的信度
            disbelief: 反对目标假设的信度
            uncertainty: 不确定性（赋予整个框架）
        """
        masses: Dict[HypothesisSet, float] = {}

        # 支持目标假设
        if belief > 0:
            masses[frozenset([target])] = belief

        # 反对目标假设 = 支持补集
        complement = frame - frozenset([target])
        if disbelief > 0 and complement:
            masses[complement] = disbelief

        # 不确定性赋予整个框架
        if uncertainty > 0:
            masses[frame] = uncertainty

        return cls(frame=frame, masses=masses)

    @classmethod
    def vacuous(cls, frame: HypothesisSet) -> 'MassFunction':
        """创建空洞 BPA（完全不确定）"""
        return cls(frame=frame, masses={frame: 1.0})

    def belief(self, hypothesis_set: HypothesisSet) -> float:
        """
        计算信度函数 Bel(A)

        Bel(A) = Σ m(B), for all B ⊆ A
        """
        return sum(
            mass for focal, mass in self.masses.items()
            if focal <= hypothesis_set  # B ⊆ A
        )

    def plausibility(self, hypothesis_set: HypothesisSet) -> float:
        """
        计算似然函数 Pl(A)

        Pl(A) = Σ m(B), for all B ∩ A ≠ ∅
        """
        return sum(
            mass for focal, mass in self.masses.items()
            if focal & hypothesis_set  # B ∩ A ≠ ∅
        )

    def uncertainty_interval(self, hypothesis_set: HypothesisSet) -> Tuple[float, float]:
        """返回不确定性区间 [Bel(A), Pl(A)]"""
        return (self.belief(hypothesis_set), self.plausibility(hypothesis_set))

    def pignistic_probability(self, hypothesis: Hypothesis) -> float:
        """
        计算 Pignistic 概率（用于决策）

        BetP(x) = Σ m(A) / |A|, for all A ∋ x
        """
        total = 0.0
        for focal, mass in self.masses.items():
            if hypothesis in focal:
                total += mass / len(focal)
        return total

    @property
    def focal_elements(self) -> List[HypothesisSet]:
        """返回所有焦元（有正质量的子集）"""
        return list(self.masses.keys())

    @property
    def specificity(self) -> float:
        """
        特异性度量（焦元越小，特异性越高）

        S = 1 - Σ m(A) * (|A| - 1) / (|Θ| - 1)
        """
        n = len(self.frame)
        if n <= 1:
            return 1.0

        weighted_sum = sum(
            mass * (len(focal) - 1) / (n - 1)
            for focal, mass in self.masses.items()
        )
        return 1.0 - weighted_sum


@dataclass
class CombinationResult:
    """Dempster 组合结果"""

    combined_mass: MassFunction
    conflict: float  # 冲突系数 K
    is_high_conflict: bool
    warning_message: Optional[str] = None

    def __repr__(self) -> str:
        return (
            f"CombinationResult(conflict={self.conflict:.3f}, "
            f"focals={len(self.combined_mass.focal_elements)})"
        )


class DempsterShaferCombiner:
    """
    Dempster-Shafer 证据组合器

    实现 Dempster 组合规则和 Yager 修正规则。

    Dempster 规则:
        m(A) = [Σ m1(B)×m2(C)] / (1-K), for B∩C=A, A≠∅
        K = Σ m1(B)×m2(C), for B∩C=∅ (冲突系数)

    Example:
        >>> combiner = DempsterShaferCombiner()
        >>> frame = make_hypothesis_set("quality", "average", "poor")
        >>> m1 = MassFunction.from_belief_disbelief(frame, "quality", 0.7, 0.1, 0.2)
        >>> m2 = MassFunction.from_belief_disbelief(frame, "quality", 0.8, 0.05, 0.15)
        >>> result = combiner.combine(m1, m2)
    """

    def __init__(
        self,
        conflict_threshold: float = 0.8,
        use_yager_rule: bool = True
    ):
        """
        Args:
            conflict_threshold: 冲突系数阈值，超过此值视为高冲突
            use_yager_rule: 高冲突时是否使用 Yager 修正规则
        """
        self._conflict_threshold = conflict_threshold
        self._use_yager_rule = use_yager_rule

    def combine(
        self,
        m1: MassFunction,
        m2: MassFunction
    ) -> CombinationResult:
        """
        组合两个质量函数

        Args:
            m1: 第一个质量函数
            m2: 第二个质量函数

        Returns:
            CombinationResult 包含组合后的质量函数和冲突信息
        """
        if m1.frame != m2.frame:
            raise ValueError("两个质量函数必须有相同的识别框架")

        frame = m1.frame

        # 计算所有交集的质量
        new_masses: Dict[HypothesisSet, float] = {}
        conflict = 0.0

        for focal1, mass1 in m1.masses.items():
            for focal2, mass2 in m2.masses.items():
                intersection = focal1 & focal2
                product = mass1 * mass2

                if not intersection:
                    # 空交集 = 冲突
                    conflict += product
                else:
                    if intersection not in new_masses:
                        new_masses[intersection] = 0.0
                    new_masses[intersection] += product

        # 检查冲突
        is_high_conflict = conflict > self._conflict_threshold
        warning = None

        if conflict >= 1.0 - 1e-10:
            # 完全冲突
            warning = "完全冲突：两个证据源完全矛盾"
            return CombinationResult(
                combined_mass=MassFunction.vacuous(frame),
                conflict=1.0,
                is_high_conflict=True,
                warning_message=warning
            )

        if is_high_conflict:
            warning = f"高冲突警告：K={conflict:.2f}，证据源可能不可靠"

            if self._use_yager_rule:
                # Yager 规则：将冲突分配给整个框架
                if frame not in new_masses:
                    new_masses[frame] = 0.0
                new_masses[frame] += conflict
            else:
                # 标准 Dempster 规则：归一化
                normalization = 1.0 - conflict
                new_masses = {k: v / normalization for k, v in new_masses.items()}
        else:
            # 标准归一化
            normalization = 1.0 - conflict
            if normalization > 0:
                new_masses = {k: v / normalization for k, v in new_masses.items()}

        return CombinationResult(
            combined_mass=MassFunction(frame=frame, masses=new_masses),
            conflict=conflict,
            is_high_conflict=is_high_conflict,
            warning_message=warning
        )

    def combine_multiple(
        self,
        mass_functions: List[MassFunction]
    ) -> CombinationResult:
        """
        组合多个质量函数

        按顺序依次组合（结合律保证结果一致）
        """
        if not mass_functions:
            raise ValueError("至少需要一个质量函数")

        if len(mass_functions) == 1:
            return CombinationResult(
                combined_mass=mass_functions[0],
                conflict=0.0,
                is_high_conflict=False
            )

        # 依次组合
        result = self.combine(mass_functions[0], mass_functions[1])
        total_conflict = result.conflict
        any_high_conflict = result.is_high_conflict

        for mf in mass_functions[2:]:
            result = self.combine(result.combined_mass, mf)
            # 累积冲突使用对数和避免乘法爆炸
            total_conflict = 1 - (1 - total_conflict) * (1 - result.conflict)
            any_high_conflict = any_high_conflict or result.is_high_conflict

        warning = result.warning_message
        if any_high_conflict and not warning:
            warning = f"累积冲突较高：K={total_conflict:.2f}"

        return CombinationResult(
            combined_mass=result.combined_mass,
            conflict=total_conflict,
            is_high_conflict=any_high_conflict,
            warning_message=warning
        )


@dataclass
class DSEvaluationResult:
    """DS证据评估结果"""

    target_hypothesis: str
    belief: float         # 确定支持
    plausibility: float   # 可能支持
    disbelief: float      # 确定反对
    uncertainty: float    # 不确定性 = Pl - Bel
    pignistic_prob: float # 决策概率
    conflict: float
    confidence: float     # = 1 - uncertainty
    decision: str         # "accept" | "reject" | "uncertain"

    def __repr__(self) -> str:
        return (
            f"DSResult({self.target_hypothesis}: "
            f"Bel={self.belief:.2f}, Pl={self.plausibility:.2f}, "
            f"decision={self.decision})"
        )


class DSEvidenceEvaluator:
    """
    基于 DS 理论的证据评估器

    专门用于公司质量评估的 DS 封装。

    Example:
        >>> evaluator = DSEvidenceEvaluator()
        >>> evaluator.add_evidence("roic", belief=0.7, disbelief=0.1, uncertainty=0.2)
        >>> evaluator.add_evidence("roe", belief=0.6, disbelief=0.15, uncertainty=0.25)
        >>> result = evaluator.evaluate("quality")
    """

    # 标准识别框架
    DEFAULT_FRAME = make_hypothesis_set("quality", "average", "poor")

    def __init__(
        self,
        frame: Optional[HypothesisSet] = None,
        conflict_threshold: float = 0.7
    ):
        self._frame = frame or self.DEFAULT_FRAME
        self._combiner = DempsterShaferCombiner(conflict_threshold=conflict_threshold)
        self._evidences: List[MassFunction] = []
        self._evidence_names: List[str] = []

    def add_evidence(
        self,
        name: str,
        target: str = "quality",
        belief: float = 0.5,
        disbelief: float = 0.1,
        uncertainty: float = 0.4
    ) -> None:
        """
        添加一条证据

        Args:
            name: 证据名称（如 "roic_trend"）
            target: 支持的目标假设
            belief: 支持度
            disbelief: 反对度
            uncertainty: 不确定性
        """
        # 归一化
        total = belief + disbelief + uncertainty
        if total > 0:
            belief /= total
            disbelief /= total
            uncertainty /= total

        mf = MassFunction.from_belief_disbelief(
            frame=self._frame,
            target=target,
            belief=belief,
            disbelief=disbelief,
            uncertainty=uncertainty
        )

        self._evidences.append(mf)
        self._evidence_names.append(name)

    def add_evidence_from_probability(
        self,
        name: str,
        prob_quality: float,
        confidence: float = 0.7
    ) -> None:
        """
        从概率和置信度添加证据

        Args:
            name: 证据名称
            prob_quality: 高质量的概率
            confidence: 置信度
        """
        belief = prob_quality * confidence
        disbelief = (1 - prob_quality) * confidence
        uncertainty = 1 - confidence

        self.add_evidence(name, "quality", belief, disbelief, uncertainty)

    def evaluate(
        self,
        target: str = "quality",
        decision_threshold: float = 0.6
    ) -> DSEvaluationResult:
        """
        评估目标假设

        Args:
            target: 目标假设
            decision_threshold: 决策阈值

        Returns:
            DSEvaluationResult 评估结果
        """
        if not self._evidences:
            # 无证据时返回完全不确定
            return DSEvaluationResult(
                target_hypothesis=target,
                belief=0.0,
                plausibility=1.0,
                disbelief=0.0,
                uncertainty=1.0,
                pignistic_prob=1.0 / len(self._frame),
                conflict=0.0,
                confidence=0.0,
                decision="uncertain"
            )

        # 组合所有证据
        if len(self._evidences) == 1:
            combined = self._evidences[0]
            conflict = 0.0
        else:
            result = self._combiner.combine_multiple(self._evidences)
            combined = result.combined_mass
            conflict = result.conflict

        # 计算信度和似然
        target_set = frozenset([target])
        bel = combined.belief(target_set)
        pl = combined.plausibility(target_set)

        # 计算不信度（补集的信度）
        complement = self._frame - target_set
        dis = combined.belief(complement) if complement else 0.0

        # 不确定性
        uncertainty = pl - bel

        # Pignistic 概率
        pignistic = combined.pignistic_probability(target)

        # 决策
        if pignistic >= decision_threshold and bel > 0.3:
            decision = "accept"
        elif pignistic < (1 - decision_threshold) or dis > 0.3:
            decision = "reject"
        else:
            decision = "uncertain"

        return DSEvaluationResult(
            target_hypothesis=target,
            belief=bel,
            plausibility=pl,
            disbelief=dis,
            uncertainty=uncertainty,
            pignistic_prob=pignistic,
            conflict=conflict,
            confidence=1.0 - uncertainty,
            decision=decision
        )

    def clear(self) -> None:
        """清除所有证据"""
        self._evidences.clear()
        self._evidence_names.clear()

    def get_evidence_summary(self) -> Dict[str, Dict[str, float]]:
        """获取所有证据的摘要"""
        summary = {}
        for name, mf in zip(self._evidence_names, self._evidences):
            target_set = frozenset(["quality"])
            summary[name] = {
                "belief": mf.belief(target_set),
                "plausibility": mf.plausibility(target_set),
                "specificity": mf.specificity
            }
        return summary


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

def quick_ds_evaluate(
    evidences: List[Tuple[str, float, float]],
    target: str = "quality"
) -> DSEvaluationResult:
    """
    快速 DS 评估

    Args:
        evidences: [(name, prob_quality, confidence), ...]
        target: 目标假设

    Returns:
        DSEvaluationResult
    """
    evaluator = DSEvidenceEvaluator()

    for name, prob, conf in evidences:
        evaluator.add_evidence_from_probability(name, prob, conf)

    return evaluator.evaluate(target)
