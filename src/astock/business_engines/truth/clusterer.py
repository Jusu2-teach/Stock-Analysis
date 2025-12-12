"""
T.R.U.T.H. System - Genome Clusterer
=====================================

基于六维基因的无监督聚类模块：
1. KMeans聚类（替代申万行业分类）
2. 匈牙利算法跨期匹配（聚类稳定性）
3. 聚类画像生成
4. 原型推断

设计原则：
1. 完全数据驱动，无人工标签
2. 跨期稳定（聚类ID有意义）
3. 可解释的聚类画像
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import numpy as np
from scipy.spatial.distance import cdist
from scipy.optimize import linear_sum_assignment
import logging

from .models import CompanyGenome, ClusterProfile

logger = logging.getLogger(__name__)


# ============================================================================
# 聚类原型定义
# ============================================================================

ARCHETYPE_DEFINITIONS = {
    # 格式: (条件函数, 名称, emoji)
    'cash_machine': {
        'name': '💰 印钞机型',
        'description': '低周期、轻资产、高质量现金流',
        'condition': lambda c: c['alpha'] < 0.3 and c['beta'] < 0.3 and c['delta_fraud'] < 0.3,
    },
    'heavy_cyclical': {
        'name': '🏭 重周期型',
        'description': '高周期、重资产',
        'condition': lambda c: c['alpha'] > 0.6 and c['beta'] > 0.5,
    },
    'true_growth': {
        'name': '🚀 真成长型',
        'description': '高成长动能、低衰退风险',
        'condition': lambda c: c['gamma'] > 0.6 and c['delta_decay'] < 0.3,
    },
    'fake_growth': {
        'name': '⚠️ 伪成长型',
        'description': '高成长但高欺诈风险',
        'condition': lambda c: c['gamma'] > 0.5 and c['delta_fraud'] > 0.4,
    },
    'decay_trap': {
        'name': '🔻 衰退陷阱',
        'description': '高衰退熵',
        'condition': lambda c: c['delta_decay'] > 0.6,
    },
    'defensive': {
        'name': '🛡️ 防御稳健型',
        'description': '低周期、稳定、低风险',
        'condition': lambda c: c['alpha'] < 0.4 and c['delta_decay'] < 0.3 and c['delta_fraud'] < 0.3,
    },
    'mixed_cyclical': {
        'name': '🔄 周期成长混合型',
        'description': '中等周期、有成长',
        'condition': lambda c: 0.3 < c['alpha'] < 0.6 and c['gamma'] > 0.4,
    },
    'neutral': {
        'name': '📊 中性混合型',
        'description': '各维度均衡',
        'condition': lambda c: True,  # 默认
    },
}


# ============================================================================
# 核心聚类器
# ============================================================================

class GenomeClusterer:
    """
    基因组聚类器

    使用KMeans对公司基因进行无监督聚类，
    并提供跨期匹配以保持聚类ID的稳定性。
    """

    def __init__(
        self,
        n_clusters: int = 20,
        random_state: int = 42,
        n_init: int = 10,
    ):
        """
        初始化聚类器

        Args:
            n_clusters: 聚类数量
            random_state: 随机种子（保证可复现）
            n_init: KMeans初始化次数
        """
        self.n_clusters = n_clusters
        self.random_state = random_state
        self.n_init = n_init

        # 聚类结果
        self.centroids: Optional[np.ndarray] = None
        self.labels: Optional[np.ndarray] = None
        self.cluster_profiles: Dict[int, ClusterProfile] = {}

        # 历史centroids（用于跨期匹配）
        self._previous_centroids: Optional[np.ndarray] = None

        # 拟合状态
        self._is_fitted = False

    def fit(
        self,
        genomes: List[CompanyGenome],
        match_previous: bool = True,
    ) -> "GenomeClusterer":
        """
        拟合聚类模型

        Args:
            genomes: 公司基因组列表
            match_previous: 是否与上期聚类匹配

        Returns:
            self
        """
        if len(genomes) < self.n_clusters:
            logger.warning(
                f"样本数 ({len(genomes)}) < 聚类数 ({self.n_clusters})，"
                f"自动调整聚类数为 {len(genomes) // 2}"
            )
            self.n_clusters = max(2, len(genomes) // 2)

        # 提取基因向量（5维，不含V因子）
        X = np.array([g.to_vector(include_verification=False) for g in genomes])

        # 标准化（Z-score）
        X_mean = X.mean(axis=0)
        X_std = X.std(axis=0)
        X_std[X_std < 1e-6] = 1  # 避免除零
        X_normalized = (X - X_mean) / X_std

        # KMeans聚类
        try:
            from sklearn.cluster import KMeans
        except ImportError:
            raise ImportError("请安装 scikit-learn: pip install scikit-learn")

        kmeans = KMeans(
            n_clusters=self.n_clusters,
            random_state=self.random_state,
            n_init=self.n_init,
            init='k-means++',
        )
        kmeans.fit(X_normalized)

        # 反标准化centroids
        self.centroids = kmeans.cluster_centers_ * X_std + X_mean
        self.labels = kmeans.labels_

        # 跨期匹配
        if match_previous and self._previous_centroids is not None:
            self._match_with_previous()

        # 生成聚类画像
        self._generate_cluster_profiles(genomes)

        # 保存当前centroids供下次匹配
        self._previous_centroids = self.centroids.copy()
        self._is_fitted = True

        logger.info(f"聚类完成: {self.n_clusters} clusters, {len(genomes)} samples")
        return self

    def _match_with_previous(self) -> None:
        """
        使用匈牙利算法匹配新旧聚类

        确保跨期聚类ID的稳定性
        """
        if self._previous_centroids is None or self.centroids is None:
            return

        # 计算成本矩阵（欧氏距离）
        cost_matrix = cdist(self._previous_centroids, self.centroids)

        # 匈牙利算法求最优匹配
        old_idx, new_idx = linear_sum_assignment(cost_matrix)

        # 创建映射：新ID -> 旧ID
        id_mapping = dict(zip(new_idx, old_idx))

        # 重新排列centroids和labels
        new_centroids = np.zeros_like(self.centroids)
        new_labels = np.zeros_like(self.labels)

        for new_id, old_id in id_mapping.items():
            new_centroids[old_id] = self.centroids[new_id]
            new_labels[self.labels == new_id] = old_id

        self.centroids = new_centroids
        self.labels = new_labels

        logger.info(f"跨期匹配完成: {len(id_mapping)} clusters matched")

    def _generate_cluster_profiles(self, genomes: List[CompanyGenome]) -> None:
        """生成聚类画像"""
        self.cluster_profiles = {}
        gene_names = ['alpha', 'beta', 'gamma', 'delta_fraud', 'delta_decay']

        for cluster_id in range(self.n_clusters):
            # 该聚类的成员
            mask = self.labels == cluster_id
            member_indices = np.where(mask)[0]
            members = [genomes[i] for i in member_indices]

            if len(members) == 0:
                continue

            # 聚类中心
            centroid = self.centroids[cluster_id]
            centroid_dict = dict(zip(gene_names, centroid))

            # 推断原型
            archetype = self._infer_archetype(centroid_dict)

            # 基因统计
            gene_stats = {}
            for i, gene_name in enumerate(gene_names):
                gene_values = [g.to_vector()[i] for g in members]
                gene_stats[gene_name] = {
                    'mean': float(np.mean(gene_values)),
                    'std': float(np.std(gene_values)),
                    'min': float(np.min(gene_values)),
                    'max': float(np.max(gene_values)),
                }

            # 计算成员的ROIC统计（用于残差校准）
            roic_values = []
            for g in members:
                if len(g.roic_series) > 0:
                    roic_values.append(np.mean(g.roic_series))

            if roic_values:
                # Top 20% ROIC中位数
                sorted_roic = sorted(roic_values, reverse=True)
                top20_count = max(1, int(len(sorted_roic) * 0.2))
                top20_median = np.median(sorted_roic[:top20_count])
            else:
                top20_median = 0.0

            self.cluster_profiles[cluster_id] = ClusterProfile(
                cluster_id=cluster_id,
                archetype=archetype,
                centroid=centroid_dict,
                count=len(members),
                member_codes=[g.ts_code for g in members],
                top20_median_roic=float(top20_median),
                gene_stats=gene_stats,
            )

    def _infer_archetype(self, centroid: Dict[str, float]) -> str:
        """
        推断聚类原型

        按优先级检查各原型条件
        """
        for archetype_id, definition in ARCHETYPE_DEFINITIONS.items():
            if archetype_id == 'neutral':
                continue  # 跳过默认
            try:
                if definition['condition'](centroid):
                    return definition['name']
            except Exception:
                continue

        # 默认
        return ARCHETYPE_DEFINITIONS['neutral']['name']

    def predict(self, genome: CompanyGenome) -> int:
        """
        预测单个公司的聚类ID

        Args:
            genome: 公司基因组

        Returns:
            聚类ID
        """
        if not self._is_fitted:
            raise RuntimeError("聚类器未拟合，请先调用 fit()")

        # 提取向量
        x = genome.to_vector(include_verification=False).reshape(1, -1)

        # 计算到各centroid的距离
        distances = cdist(x, self.centroids)[0]

        return int(np.argmin(distances))

    def predict_batch(self, genomes: List[CompanyGenome]) -> List[int]:
        """批量预测聚类ID"""
        return [self.predict(g) for g in genomes]

    def get_cluster_profile(self, cluster_id: int) -> Optional[ClusterProfile]:
        """获取聚类画像"""
        return self.cluster_profiles.get(cluster_id)

    def get_cluster_residual_target(self, cluster_id: int) -> float:
        """
        获取聚类的残差修正目标

        基于聚类内Top20%公司的实际表现
        """
        profile = self.cluster_profiles.get(cluster_id)
        if profile is None:
            return 0.0
        return profile.residual_target

    def get_archetype(self, cluster_id: int) -> str:
        """获取聚类原型名称"""
        profile = self.cluster_profiles.get(cluster_id)
        if profile is None:
            return "未知"
        return profile.archetype

    def save_centroids(self, path: str) -> None:
        """保存centroids到文件（用于跨期匹配）"""
        if self.centroids is None:
            raise RuntimeError("无centroids可保存")
        np.save(path, self.centroids)
        logger.info(f"Centroids saved to {path}")

    def load_centroids(self, path: str) -> None:
        """加载历史centroids"""
        self._previous_centroids = np.load(path)
        logger.info(f"Centroids loaded from {path}, shape={self._previous_centroids.shape}")

    def visualize_text(self) -> str:
        """生成文本可视化报告"""
        if not self._is_fitted:
            return "聚类器未拟合"

        lines = []
        lines.append("=" * 60)
        lines.append("T.R.U.T.H. 基因聚类画像")
        lines.append("=" * 60)

        for cluster_id, profile in sorted(self.cluster_profiles.items()):
            lines.append(f"\n📊 Cluster {cluster_id}: {profile.archetype}")
            lines.append(f"   公司数量: {profile.count}")
            lines.append(f"   Top20% ROIC中位数: {profile.top20_median_roic:.1%}")
            lines.append("   基因中心:")
            for gene, value in profile.centroid.items():
                bar = "█" * int(value * 10) + "░" * (10 - int(value * 10))
                lines.append(f"      {gene:15s}: {bar} {value:.2f}")

        return "\n".join(lines)


# ============================================================================
# 辅助函数
# ============================================================================

def compute_optimal_clusters(n_samples: int) -> int:
    """
    计算最优聚类数

    经验法则: n = sqrt(N/2)
    """
    import math
    optimal = int(math.sqrt(n_samples / 2))
    return max(5, min(optimal, 50))  # 限制在 [5, 50]


def silhouette_analysis(
    genomes: List[CompanyGenome],
    n_clusters_range: Tuple[int, int] = (5, 30),
) -> Dict[int, float]:
    """
    轮廓系数分析，用于选择最优聚类数

    Args:
        genomes: 基因组列表
        n_clusters_range: 聚类数范围

    Returns:
        {n_clusters: silhouette_score}
    """
    try:
        from sklearn.cluster import KMeans
        from sklearn.metrics import silhouette_score
    except ImportError:
        raise ImportError("请安装 scikit-learn")

    X = np.array([g.to_vector() for g in genomes])

    results = {}
    for n in range(n_clusters_range[0], n_clusters_range[1] + 1):
        if n >= len(genomes):
            break

        kmeans = KMeans(n_clusters=n, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)

        if len(set(labels)) > 1:
            score = silhouette_score(X, labels)
            results[n] = score

    return results


def match_clusters_across_periods(
    old_centroids: np.ndarray,
    new_centroids: np.ndarray,
) -> Dict[int, int]:
    """
    用匈牙利算法匹配新旧聚类

    Args:
        old_centroids: 旧期聚类中心
        new_centroids: 新期聚类中心

    Returns:
        {new_cluster_id: old_cluster_id}
    """
    cost_matrix = cdist(old_centroids, new_centroids)
    old_idx, new_idx = linear_sum_assignment(cost_matrix)
    return dict(zip(new_idx, old_idx))


def robust_normalize(
    values: np.ndarray,
    lower_pct: float = 0.05,
    upper_pct: float = 0.95,
) -> np.ndarray:
    """
    分位数归一化（对异常值更鲁棒）

    Args:
        values: 输入数组
        lower_pct: 下分位数
        upper_pct: 上分位数

    Returns:
        归一化后的数组 [0, 1]
    """
    lower = np.percentile(values, lower_pct * 100)
    upper = np.percentile(values, upper_pct * 100)

    if upper - lower < 1e-6:
        return np.zeros_like(values)

    return np.clip((values - lower) / (upper - lower), 0, 1)
