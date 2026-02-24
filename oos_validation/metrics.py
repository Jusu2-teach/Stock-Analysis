"""
OOS Validation — 统计指标模块

提供排名稳定性、相关性、一致性等指标计算。
所有函数接受 {ts_code: score} 字典，保持接口统一。
"""

from __future__ import annotations

import numpy as np
from scipy import stats
from typing import Dict, List, Tuple


def spearman_rho(scores_a: Dict[str, float], scores_b: Dict[str, float]) -> float:
    """两组评分的 Spearman 秩相关系数。"""
    common = sorted(set(scores_a.keys()) & set(scores_b.keys()))
    if len(common) < 10:
        return float("nan")
    a = [scores_a[k] for k in common]
    b = [scores_b[k] for k in common]
    rho, _ = stats.spearmanr(a, b)
    return float(rho)


def kendall_tau(scores_a: Dict[str, float], scores_b: Dict[str, float]) -> float:
    """两组评分的 Kendall τ 秩相关系数。"""
    common = sorted(set(scores_a.keys()) & set(scores_b.keys()))
    if len(common) < 10:
        return float("nan")
    a = [scores_a[k] for k in common]
    b = [scores_b[k] for k in common]
    tau, _ = stats.kendalltau(a, b)
    return float(tau)


def top_k_overlap(
    scores_a: Dict[str, float], scores_b: Dict[str, float], k: int
) -> float:
    """Top-K 股票的 Jaccard 相似度。

    取两组中评分最高的 K 只股票，计算交集 / 并集。
    """
    top_a = set(sorted(scores_a, key=lambda x: -scores_a[x])[:k])
    top_b = set(sorted(scores_b, key=lambda x: -scores_b[x])[:k])
    if not top_a or not top_b:
        return 0.0
    return len(top_a & top_b) / len(top_a | top_b)


def max_rank_shift(
    scores_a: Dict[str, float], scores_b: Dict[str, float]
) -> Tuple[int, str]:
    """最大排名变动幅度及对应公司。"""
    common = sorted(set(scores_a.keys()) & set(scores_b.keys()))
    if not common:
        return 0, ""
    ranked_a = sorted(common, key=lambda x: -scores_a[x])
    ranked_b = sorted(common, key=lambda x: -scores_b[x])
    rank_a = {k: i for i, k in enumerate(ranked_a)}
    rank_b = {k: i for i, k in enumerate(ranked_b)}
    worst_shift, worst_company = 0, ""
    for k in common:
        shift = abs(rank_a[k] - rank_b[k])
        if shift > worst_shift:
            worst_shift = shift
            worst_company = k
    return worst_shift, worst_company


def grade_consistency(
    grades_a: Dict[str, str], grades_b: Dict[str, str]
) -> float:
    """等级/决策一致率 (完全匹配的比例)。"""
    common = set(grades_a.keys()) & set(grades_b.keys())
    if not common:
        return 0.0
    return sum(1 for k in common if grades_a[k] == grades_b[k]) / len(common)


def aggregate_stability(values: List[float]) -> Dict[str, float]:
    """聚合多轮运行的稳定性统计量。"""
    valid = [v for v in values if not np.isnan(v)]
    if not valid:
        return {
            "mean": float("nan"),
            "std": float("nan"),
            "min": float("nan"),
            "max": float("nan"),
            "ci_95_lower": float("nan"),
            "ci_95_upper": float("nan"),
        }
    return {
        "mean": float(np.mean(valid)),
        "std": float(np.std(valid)),
        "min": float(np.min(valid)),
        "max": float(np.max(valid)),
        "ci_95_lower": float(np.percentile(valid, 2.5)),
        "ci_95_upper": float(np.percentile(valid, 97.5)),
    }
