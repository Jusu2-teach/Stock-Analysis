"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - Copula 融合模块
═══════════════════════════════════════════════════════════════════════════════

处理证据间的相关性问题。
传统方法假设证据独立，但财务指标高度相关（如ROE↔ROIC）。
Copula 通过 Sklar 定理分离边际分布和相关结构。

关键创新：
- 避免独立性假设导致的置信度过高/过低
- 使用 Gaussian Copula 建模相关结构
- 输出"有效证据数"而非简单累加

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
from numpy.typing import NDArray
from scipy import stats


@dataclass
class Evidence:
    """单条证据"""

    name: str
    value: float           # 观测值
    belief: float         # 支持假设H的概率
    disbelief: float      # 反对假设H的概率
    uncertainty: float    # 不确定性
    source_metric: Optional[str] = None

    def __post_init__(self) -> None:
        # 验证概率和为1
        total = self.belief + self.disbelief + self.uncertainty
        if not np.isclose(total, 1.0, atol=0.01):
            # 归一化
            self.belief /= total
            self.disbelief /= total
            self.uncertainty /= total

    @classmethod
    def from_probability(
        cls,
        name: str,
        value: float,
        prob_positive: float,
        confidence: float = 0.8
    ) -> 'Evidence':
        """从概率和置信度创建证据"""
        belief = prob_positive * confidence
        disbelief = (1 - prob_positive) * confidence
        uncertainty = 1 - confidence
        return cls(name, value, belief, disbelief, uncertainty)


@dataclass
class CopulaFusionResult:
    """Copula 融合结果"""

    fused_belief: float
    fused_disbelief: float
    fused_uncertainty: float
    effective_evidence_count: float  # 有效证据数（考虑相关性后）
    correlation_matrix: NDArray[np.float64]
    individual_contributions: Dict[str, float]

    @property
    def fused_probability(self) -> float:
        """归一化后的正向概率"""
        if self.fused_belief + self.fused_disbelief > 0:
            return self.fused_belief / (self.fused_belief + self.fused_disbelief)
        return 0.5

    @property
    def confidence(self) -> float:
        """置信度 = 1 - 不确定性"""
        return 1.0 - self.fused_uncertainty

    def __repr__(self) -> str:
        return (
            f"CopulaFusion(prob={self.fused_probability:.2%}, "
            f"conf={self.confidence:.2%}, "
            f"eff_n={self.effective_evidence_count:.2f})"
        )


class GaussianCopula:
    """
    Gaussian Copula 相关性建模

    Sklar 定理：任何多变量分布都可以分解为边际分布和 Copula 函数
    F(x1,...,xn) = C(F1(x1),...,Fn(xn))

    Gaussian Copula 使用正态分布的相关结构。
    """

    def __init__(self, correlation_matrix: NDArray[np.float64]):
        """
        Args:
            correlation_matrix: n×n 相关系数矩阵
        """
        self._correlation = correlation_matrix
        self._n = correlation_matrix.shape[0]

        # 验证是否为有效相关矩阵
        if not self._is_valid_correlation_matrix():
            # 尝试修复为最近的有效相关矩阵
            self._correlation = self._nearest_correlation_matrix()

    def _is_valid_correlation_matrix(self) -> bool:
        """检验是否为有效相关矩阵"""
        # 对称性
        if not np.allclose(self._correlation, self._correlation.T):
            return False
        # 对角线为1
        if not np.allclose(np.diag(self._correlation), 1.0):
            return False
        # 正定性
        eigenvalues = np.linalg.eigvalsh(self._correlation)
        if np.any(eigenvalues < -1e-10):
            return False
        return True

    def _nearest_correlation_matrix(self) -> NDArray[np.float64]:
        """
        找到最近的有效相关矩阵（Higham算法简化版）
        """
        # 简化实现：特征值截断法
        eigenvalues, eigenvectors = np.linalg.eigh(self._correlation)
        eigenvalues = np.maximum(eigenvalues, 1e-8)  # 确保正定

        # 重构
        result = eigenvectors @ np.diag(eigenvalues) @ eigenvectors.T

        # 归一化对角线
        d = np.sqrt(np.diag(result))
        result = result / np.outer(d, d)

        return result

    def compute_copula_density(
        self,
        uniform_marginals: NDArray[np.float64]
    ) -> float:
        """
        计算 Copula 密度

        Args:
            uniform_marginals: n维向量，每个元素在[0,1]，表示边际CDF值

        Returns:
            Copula 密度值
        """
        # 转换为正态分位数
        z = stats.norm.ppf(np.clip(uniform_marginals, 0.001, 0.999))

        # 计算 Gaussian Copula 密度
        # c(u) = |R|^(-1/2) * exp(-0.5 * z^T * (R^-1 - I) * z)

        try:
            R_inv = np.linalg.inv(self._correlation)
            det_R = np.linalg.det(self._correlation)

            if det_R <= 0:
                return 1.0  # 退化情况

            quadratic_form = z.T @ (R_inv - np.eye(self._n)) @ z
            density = (det_R ** -0.5) * np.exp(-0.5 * quadratic_form)

            return max(density, 1e-10)
        except np.linalg.LinAlgError:
            return 1.0  # 数值问题时返回独立假设

    def effective_sample_size(self) -> float:
        """
        计算有效样本量

        当证据完全独立时，ESS = n
        当证据完全相关时，ESS → 1

        使用公式: ESS = n / (1 + (n-1) * avg_correlation)
        """
        n = self._n
        if n <= 1:
            return float(n)

        # 提取非对角线元素的平均相关系数
        off_diag_mask = ~np.eye(n, dtype=bool)
        avg_corr = np.abs(self._correlation[off_diag_mask]).mean()

        ess = n / (1 + (n - 1) * avg_corr)
        return max(1.0, ess)


class CopulaEvidenceFusion:
    """
    基于 Copula 的证据融合器

    核心思想：
    1. 估计证据间的相关矩阵
    2. 使用 Copula 调整联合概率
    3. 输出考虑相关性的融合结果

    Example:
        >>> fusion = CopulaEvidenceFusion()
        >>> evidences = [
        ...     Evidence("roic_trend", 0.03, belief=0.8, disbelief=0.1, uncertainty=0.1),
        ...     Evidence("roe_trend", 0.02, belief=0.7, disbelief=0.15, uncertainty=0.15),
        ... ]
        >>> result = fusion.fuse(evidences, correlation_matrix=np.array([[1, 0.7], [0.7, 1]]))
    """

    def __init__(
        self,
        default_correlation: float = 0.3,
        known_correlations: Optional[Dict[Tuple[str, str], float]] = None
    ):
        """
        Args:
            default_correlation: 默认相关系数（当未知时使用）
            known_correlations: 已知的成对相关系数
        """
        self._default_correlation = default_correlation
        self._known_correlations = known_correlations or {}

        # 预设的财务指标相关性
        self._preset_correlations: Dict[Tuple[str, str], float] = {
            ("roic", "roe"): 0.75,
            ("roic", "roiic"): 0.50,
            ("roe", "roiic"): 0.45,
            ("gross_margin", "net_margin"): 0.65,
            ("revenue", "profit"): 0.60,
            ("profit", "ocf"): 0.55,
            ("roic", "gross_margin"): 0.40,
            ("roe", "net_margin"): 0.50,
        }

    def _build_correlation_matrix(
        self,
        evidence_names: List[str],
        provided_matrix: Optional[NDArray[np.float64]] = None
    ) -> NDArray[np.float64]:
        """构建相关矩阵"""
        n = len(evidence_names)

        if provided_matrix is not None and provided_matrix.shape == (n, n):
            return provided_matrix

        # 从已知相关性构建
        matrix = np.eye(n)

        for i, name_i in enumerate(evidence_names):
            for j, name_j in enumerate(evidence_names):
                if i >= j:
                    continue

                # 提取指标名（去除后缀）
                metric_i = name_i.replace("_trend", "").replace("_level", "")
                metric_j = name_j.replace("_trend", "").replace("_level", "")

                # 查找相关系数
                key1 = (metric_i, metric_j)
                key2 = (metric_j, metric_i)

                if key1 in self._known_correlations:
                    corr = self._known_correlations[key1]
                elif key2 in self._known_correlations:
                    corr = self._known_correlations[key2]
                elif key1 in self._preset_correlations:
                    corr = self._preset_correlations[key1]
                elif key2 in self._preset_correlations:
                    corr = self._preset_correlations[key2]
                else:
                    corr = self._default_correlation

                matrix[i, j] = corr
                matrix[j, i] = corr

        return matrix

    def fuse(
        self,
        evidences: List[Evidence],
        correlation_matrix: Optional[NDArray[np.float64]] = None
    ) -> CopulaFusionResult:
        """
        融合多条证据

        Args:
            evidences: 证据列表
            correlation_matrix: 可选的相关矩阵

        Returns:
            CopulaFusionResult 融合结果
        """
        if not evidences:
            return CopulaFusionResult(
                fused_belief=0.0,
                fused_disbelief=0.0,
                fused_uncertainty=1.0,
                effective_evidence_count=0.0,
                correlation_matrix=np.array([]),
                individual_contributions={}
            )

        if len(evidences) == 1:
            e = evidences[0]
            return CopulaFusionResult(
                fused_belief=e.belief,
                fused_disbelief=e.disbelief,
                fused_uncertainty=e.uncertainty,
                effective_evidence_count=1.0,
                correlation_matrix=np.array([[1.0]]),
                individual_contributions={e.name: 1.0}
            )

        # 构建相关矩阵
        names = [e.name for e in evidences]
        corr_matrix = self._build_correlation_matrix(names, correlation_matrix)

        # 创建 Copula
        copula = GaussianCopula(corr_matrix)

        # 计算有效证据数
        ess = copula.effective_sample_size()

        # 使用 Copula 调整后的融合
        # 简化方法：根据有效证据数调整权重
        weight_adjustment = ess / len(evidences)

        # 加权融合
        beliefs = np.array([e.belief for e in evidences])
        disbeliefs = np.array([e.disbelief for e in evidences])
        uncertainties = np.array([e.uncertainty for e in evidences])

        # 使用几何平均（更适合概率）而非算术平均
        # 调整后的融合公式
        fused_belief = self._copula_weighted_fusion(beliefs, weight_adjustment)
        fused_disbelief = self._copula_weighted_fusion(disbeliefs, weight_adjustment)
        fused_uncertainty = self._copula_weighted_fusion(uncertainties, weight_adjustment)

        # 归一化
        total = fused_belief + fused_disbelief + fused_uncertainty
        if total > 0:
            fused_belief /= total
            fused_disbelief /= total
            fused_uncertainty /= total

        # 计算各证据贡献度
        contributions = self._compute_contributions(evidences, corr_matrix)

        return CopulaFusionResult(
            fused_belief=fused_belief,
            fused_disbelief=fused_disbelief,
            fused_uncertainty=fused_uncertainty,
            effective_evidence_count=ess,
            correlation_matrix=corr_matrix,
            individual_contributions=contributions
        )

    def _copula_weighted_fusion(
        self,
        values: NDArray[np.float64],
        weight_adjustment: float
    ) -> float:
        """
        Copula 加权融合

        使用调整后的几何平均
        """
        # 避免零值
        values = np.maximum(values, 1e-10)

        # 几何平均
        log_mean = np.mean(np.log(values))

        # 应用权重调整
        # 当相关性高时，结果更接近单个值而非乘积
        adjusted = np.exp(log_mean * weight_adjustment)

        return float(adjusted)

    def _compute_contributions(
        self,
        evidences: List[Evidence],
        corr_matrix: NDArray[np.float64]
    ) -> Dict[str, float]:
        """计算各证据的贡献度"""
        n = len(evidences)
        contributions = {}

        # 基于相关性的贡献度计算
        # 与其他证据相关性越高，独特贡献越低
        for i, e in enumerate(evidences):
            # 计算与其他证据的平均相关性
            other_corrs = [corr_matrix[i, j] for j in range(n) if i != j]
            avg_corr = np.mean(np.abs(other_corrs)) if other_corrs else 0

            # 独特性 = 1 - 平均相关性
            uniqueness = 1.0 - avg_corr

            # 贡献度 = 独特性 × (1 - 不确定性)
            contribution = uniqueness * (1 - e.uncertainty)
            contributions[e.name] = contribution

        # 归一化
        total = sum(contributions.values())
        if total > 0:
            contributions = {k: v / total for k, v in contributions.items()}

        return contributions


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_FUSION: Optional[CopulaEvidenceFusion] = None


def get_default_fusion() -> CopulaEvidenceFusion:
    """获取默认融合器（单例）"""
    global _DEFAULT_FUSION
    if _DEFAULT_FUSION is None:
        _DEFAULT_FUSION = CopulaEvidenceFusion()
    return _DEFAULT_FUSION


def fuse_evidences(
    evidences: List[Evidence],
    correlation_matrix: Optional[NDArray[np.float64]] = None
) -> CopulaFusionResult:
    """便捷融合函数"""
    fusion = get_default_fusion()
    return fusion.fuse(evidences, correlation_matrix)
