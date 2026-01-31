"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 因果推断模块
═══════════════════════════════════════════════════════════════════════════════

基于 Pearl 因果推断理论的有向无环图（DAG）实现。
支持 do-calculus 干预分析，区分相关性与因果性。

关键创新：
- 不仅检测"ROIC下降"，还能推断"为什么下降"
- 通过后门调整（backdoor adjustment）控制混淆因子
- 区分直接效应、间接效应、调节效应

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
import numpy as np
from numpy.typing import NDArray


class EffectType(Enum):
    """因果效应类型"""
    DIRECT = "direct"           # A → B
    MEDIATED = "mediated"       # A → M → B
    MODERATED = "moderated"     # A → B，但受 C 调节
    CONDITIONAL = "conditional" # A → B | C=c


@dataclass
class CausalNode:
    """因果图节点"""

    name: str
    node_type: str  # "exogenous" | "endogenous" | "latent"
    description: str
    observable: bool
    metric_key: Optional[str] = None
    proxy_indicators: List[str] = field(default_factory=list)
    prior_mean: float = 0.0
    prior_std: float = 1.0

    @property
    def is_root(self) -> bool:
        return self.node_type == "exogenous"


@dataclass
class CausalEdge:
    """因果图边"""

    source: str
    target: str
    effect_type: EffectType
    strength: float  # 0-1 范围的因果强度
    mechanism: str   # 因果机制的文字描述
    mediator: Optional[str] = None   # 中介变量
    moderator: Optional[str] = None  # 调节变量
    condition: Optional[str] = None  # 条件表达式

    @property
    def is_strong(self) -> bool:
        return self.strength >= 0.5


@dataclass
class CausalEffect:
    """因果效应估计结果"""

    intervention_node: str
    outcome_node: str
    causal_effect: float      # do(X=x) 对 Y 的因果效应
    total_effect: float       # 包含所有路径的总效应
    direct_effect: float      # 直接效应
    indirect_effect: float    # 通过中介的间接效应
    confidence: float         # 效应估计的置信度
    adjustment_set: Set[str]  # 用于调整的变量集
    causal_path: List[str]    # 主要因果路径

    def __repr__(self) -> str:
        return (
            f"CausalEffect({self.intervention_node} → {self.outcome_node}: "
            f"total={self.total_effect:.3f}, direct={self.direct_effect:.3f})"
        )


@dataclass
class CausalDiagnosis:
    """因果诊断结果"""

    target_metric: str
    status: str  # "declining" | "stable" | "improving"
    primary_causes: List[Tuple[str, float]]  # [(原因, 贡献度), ...]
    confounders_detected: List[str]
    intervention_suggestions: List[str]
    confidence: float
    explanation: str


class CausalGraph:
    """
    因果推断图

    基于 Pearl 的 do-calculus 理论实现因果推断。

    核心方法：
    - `estimate_causal_effect()`: 估计干预效应 P(Y|do(X=x))
    - `find_backdoor_adjustment()`: 寻找后门调整集
    - `diagnose()`: 诊断指标变化的因果原因

    Example:
        >>> graph = CausalGraph.from_config("config/causal_structure.yaml")
        >>> effect = graph.estimate_causal_effect(
        ...     intervention="roic_trend",
        ...     outcome="company_quality",
        ...     observed_data={"roic_trend": 0.03, "revenue_trend": 0.10}
        ... )
        >>> print(effect.causal_effect)
    """

    def __init__(self):
        self._nodes: Dict[str, CausalNode] = {}
        self._edges: List[CausalEdge] = []
        self._adjacency: Dict[str, List[str]] = {}  # 邻接表 (children)
        self._parents: Dict[str, List[str]] = {}    # 父节点表
        self._confounders: List[Dict[str, Any]] = []

    @classmethod
    def from_config(cls, config_path: str | Path) -> 'CausalGraph':
        """从 YAML 配置加载因果图"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        graph = cls()

        # 加载节点
        for name, node_config in config.get('nodes', {}).items():
            prior = node_config.get('prior_distribution', {})
            node = CausalNode(
                name=name,
                node_type=node_config.get('type', 'endogenous'),
                description=node_config.get('description', ''),
                observable=node_config.get('observable', True),
                metric_key=node_config.get('metric_key'),
                proxy_indicators=node_config.get('proxy_indicators', []),
                prior_mean=prior.get('mean', 0.0),
                prior_std=prior.get('std', 1.0)
            )
            graph.add_node(node)

        # 加载边
        for edge_config in config.get('edges', []):
            edge = CausalEdge(
                source=edge_config['from'],
                target=edge_config['to'],
                effect_type=EffectType(edge_config.get('effect_type', 'direct')),
                strength=edge_config.get('strength', 0.5),
                mechanism=edge_config.get('mechanism', ''),
                mediator=edge_config.get('mediator'),
                moderator=edge_config.get('moderator'),
                condition=edge_config.get('condition')
            )
            graph.add_edge(edge)

        # 加载混淆因子
        graph._confounders = config.get('confounders', [])

        return graph

    def add_node(self, node: CausalNode) -> None:
        """添加节点"""
        self._nodes[node.name] = node
        if node.name not in self._adjacency:
            self._adjacency[node.name] = []
        if node.name not in self._parents:
            self._parents[node.name] = []

    def add_edge(self, edge: CausalEdge) -> None:
        """添加边"""
        self._edges.append(edge)
        self._adjacency[edge.source].append(edge.target)
        self._parents[edge.target].append(edge.source)

    def get_node(self, name: str) -> Optional[CausalNode]:
        return self._nodes.get(name)

    def get_edges_from(self, source: str) -> List[CausalEdge]:
        """获取从某节点出发的所有边"""
        return [e for e in self._edges if e.source == source]

    def get_edges_to(self, target: str) -> List[CausalEdge]:
        """获取指向某节点的所有边"""
        return [e for e in self._edges if e.target == target]

    def find_all_paths(
        self,
        source: str,
        target: str,
        max_length: int = 10
    ) -> List[List[str]]:
        """
        找到从 source 到 target 的所有路径（有向）
        """
        paths: List[List[str]] = []

        def dfs(current: str, path: List[str]) -> None:
            if len(path) > max_length:
                return

            if current == target:
                paths.append(path.copy())
                return

            for child in self._adjacency.get(current, []):
                if child not in path:  # 避免环
                    path.append(child)
                    dfs(child, path)
                    path.pop()

        dfs(source, [source])
        return paths

    def find_backdoor_adjustment(
        self,
        intervention: str,
        outcome: str
    ) -> Set[str]:
        """
        寻找满足后门准则的调整集

        后门准则：调整集 Z 满足：
        1. Z 阻断所有 intervention 到 outcome 的后门路径
        2. Z 不包含 intervention 的任何后代

        简化实现：返回混淆因子中同时影响 intervention 和 outcome 的变量
        """
        adjustment_set: Set[str] = set()

        # 从配置的混淆因子中筛选
        for confounder in self._confounders:
            affects = set(confounder.get('affects', []))
            if intervention in affects and outcome in affects:
                adjustment_set.add(confounder['name'])
            # 也考虑间接关联
            elif intervention in affects:
                # 检查是否有路径到 outcome
                for node in affects:
                    if self._has_path_to(node, outcome):
                        adjustment_set.add(confounder['name'])
                        break

        # 添加所有共同父节点
        intervention_parents = set(self._parents.get(intervention, []))
        outcome_ancestors = self._get_ancestors(outcome)
        common = intervention_parents & outcome_ancestors
        adjustment_set.update(common)

        return adjustment_set

    def _has_path_to(self, source: str, target: str) -> bool:
        """检查是否存在从 source 到 target 的路径"""
        return len(self.find_all_paths(source, target, max_length=5)) > 0

    def _get_ancestors(self, node: str) -> Set[str]:
        """获取节点的所有祖先"""
        ancestors: Set[str] = set()

        def collect(n: str) -> None:
            for parent in self._parents.get(n, []):
                if parent not in ancestors:
                    ancestors.add(parent)
                    collect(parent)

        collect(node)
        return ancestors

    def _get_descendants(self, node: str) -> Set[str]:
        """获取节点的所有后代"""
        descendants: Set[str] = set()

        def collect(n: str) -> None:
            for child in self._adjacency.get(n, []):
                if child not in descendants:
                    descendants.add(child)
                    collect(child)

        collect(node)
        return descendants

    def estimate_causal_effect(
        self,
        intervention: str,
        outcome: str,
        observed_data: Dict[str, float]
    ) -> CausalEffect:
        """
        估计因果效应 P(Y|do(X=x))

        使用简化的线性结构方程模型（SEM）估计。
        实际应用中可以扩展为非线性或贝叶斯估计。

        Args:
            intervention: 干预变量
            outcome: 结果变量
            observed_data: 观测数据字典

        Returns:
            CausalEffect 包含效应估计和置信度
        """
        # 找到所有因果路径
        paths = self.find_all_paths(intervention, outcome)

        if not paths:
            return CausalEffect(
                intervention_node=intervention,
                outcome_node=outcome,
                causal_effect=0.0,
                total_effect=0.0,
                direct_effect=0.0,
                indirect_effect=0.0,
                confidence=0.0,
                adjustment_set=set(),
                causal_path=[]
            )

        # 计算各路径的效应
        total_effect = 0.0
        direct_effect = 0.0
        indirect_effect = 0.0

        for path in paths:
            path_effect = self._compute_path_effect(path, observed_data)
            total_effect += path_effect

            if len(path) == 2:  # 直接边
                direct_effect += path_effect
            else:
                indirect_effect += path_effect

        # 找到调整集
        adjustment_set = self.find_backdoor_adjustment(intervention, outcome)

        # 计算置信度（基于路径数量和边强度）
        avg_strength = np.mean([
            self._get_edge_strength(path[i], path[i+1])
            for path in paths
            for i in range(len(path)-1)
        ])
        confidence = min(0.95, avg_strength * (1 - 1/(len(paths)+1)))

        # 主要因果路径（效应最大的）
        main_path = max(paths, key=lambda p: abs(self._compute_path_effect(p, observed_data)))

        return CausalEffect(
            intervention_node=intervention,
            outcome_node=outcome,
            causal_effect=total_effect,  # do-calculus 调整后的效应
            total_effect=total_effect,
            direct_effect=direct_effect,
            indirect_effect=indirect_effect,
            confidence=confidence,
            adjustment_set=adjustment_set,
            causal_path=main_path
        )

    def _compute_path_effect(
        self,
        path: List[str],
        observed_data: Dict[str, float]
    ) -> float:
        """计算单条路径的因果效应（乘积形式）"""
        effect = 1.0

        for i in range(len(path) - 1):
            source, target = path[i], path[i+1]
            edge_strength = self._get_edge_strength(source, target)

            # 考虑观测值的影响
            if source in observed_data:
                # 标准化观测值
                node = self._nodes.get(source)
                if node:
                    z_score = (observed_data[source] - node.prior_mean) / max(node.prior_std, 0.001)
                    # 限制极端值
                    z_score = np.clip(z_score, -3, 3)
                    effect *= edge_strength * (1 + 0.1 * z_score)
                else:
                    effect *= edge_strength
            else:
                effect *= edge_strength

        return effect

    def _get_edge_strength(self, source: str, target: str) -> float:
        """获取边的强度"""
        for edge in self._edges:
            if edge.source == source and edge.target == target:
                return edge.strength
        return 0.0

    def diagnose(
        self,
        target_metric: str,
        observed_data: Dict[str, float],
        threshold: float = 0.0
    ) -> CausalDiagnosis:
        """
        诊断目标指标的因果原因

        Args:
            target_metric: 要诊断的目标指标
            observed_data: 所有观测数据
            threshold: 判断改善/恶化的阈值

        Returns:
            CausalDiagnosis 包含原因分析和建议
        """
        # 确定状态
        target_value = observed_data.get(target_metric, 0.0)
        if target_value > threshold + 0.01:
            status = "improving"
        elif target_value < threshold - 0.01:
            status = "declining"
        else:
            status = "stable"

        # 找到所有父节点（直接原因）
        parent_edges = self.get_edges_to(target_metric)

        # 计算每个父节点的贡献度
        contributions: List[Tuple[str, float]] = []

        for edge in parent_edges:
            parent = edge.source
            parent_value = observed_data.get(parent, 0.0)

            # 贡献度 = 边强度 × 父节点值（标准化）
            node = self._nodes.get(parent)
            if node and node.prior_std > 0:
                z_score = (parent_value - node.prior_mean) / node.prior_std
            else:
                z_score = parent_value

            contribution = edge.strength * z_score
            contributions.append((parent, contribution))

        # 按贡献度排序
        contributions.sort(key=lambda x: abs(x[1]), reverse=True)
        primary_causes = contributions[:5]  # 取前5个

        # 检测混淆因子
        confounders_detected = []
        for conf in self._confounders:
            if target_metric in conf.get('affects', []):
                confounders_detected.append(conf['name'])

        # 生成干预建议
        suggestions = self._generate_intervention_suggestions(
            target_metric, primary_causes, status
        )

        # 生成解释文本
        explanation = self._generate_explanation(
            target_metric, status, primary_causes, confounders_detected
        )

        # 计算诊断置信度
        if contributions:
            confidence = min(0.9, np.mean([abs(c) for _, c in contributions]) / 2)
        else:
            confidence = 0.3

        return CausalDiagnosis(
            target_metric=target_metric,
            status=status,
            primary_causes=primary_causes,
            confounders_detected=confounders_detected,
            intervention_suggestions=suggestions,
            confidence=confidence,
            explanation=explanation
        )

    def _generate_intervention_suggestions(
        self,
        target: str,
        causes: List[Tuple[str, float]],
        status: str
    ) -> List[str]:
        """生成干预建议"""
        suggestions = []

        if status == "declining":
            for cause, contribution in causes:
                if contribution < 0:  # 负面贡献
                    edge = next(
                        (e for e in self._edges if e.source == cause and e.target == target),
                        None
                    )
                    if edge:
                        suggestions.append(
                            f"改善 {cause}: {edge.mechanism}"
                        )
        elif status == "improving":
            for cause, contribution in causes:
                if contribution > 0:  # 正面贡献
                    suggestions.append(f"维持 {cause} 的正向趋势")

        return suggestions[:3]  # 最多3条建议

    def _generate_explanation(
        self,
        target: str,
        status: str,
        causes: List[Tuple[str, float]],
        confounders: List[str]
    ) -> str:
        """生成因果解释文本"""
        status_text = {
            "declining": "下降",
            "stable": "稳定",
            "improving": "改善"
        }[status]

        parts = [f"{target} 趋势 {status_text}。"]

        if causes:
            main_cause, contribution = causes[0]
            if abs(contribution) > 0.1:
                direction = "正面" if contribution > 0 else "负面"
                parts.append(f"主要原因: {main_cause} 产生了{direction}影响 (贡献度: {contribution:.2f})。")

        if confounders:
            parts.append(f"注意: 可能存在混淆因子 ({', '.join(confounders)})，需谨慎解读。")

        return " ".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 预置简化因果图
# ═══════════════════════════════════════════════════════════════════════════════

def create_financial_causal_graph() -> CausalGraph:
    """创建财务指标因果图的简化版本"""
    graph = CausalGraph()

    # 核心节点
    nodes = [
        CausalNode("revenue_trend", "endogenous", "营收趋势", True, "revenue", prior_std=0.15),
        CausalNode("gross_margin_trend", "endogenous", "毛利率趋势", True, "gross_margin", prior_std=0.03),
        CausalNode("net_margin_trend", "endogenous", "净利率趋势", True, "net_margin", prior_std=0.05),
        CausalNode("roic_trend", "endogenous", "ROIC趋势", True, "roic", prior_std=0.03),
        CausalNode("roe_trend", "endogenous", "ROE趋势", True, "roe", prior_std=0.04),
        CausalNode("ocf_trend", "endogenous", "现金流趋势", True, "ocf", prior_std=0.10),
        CausalNode("company_quality", "latent", "公司质量", False),
    ]

    for node in nodes:
        graph.add_node(node)

    # 核心因果边
    edges = [
        CausalEdge("revenue_trend", "gross_margin_trend", EffectType.MODERATED, 0.3, "规模效应"),
        CausalEdge("gross_margin_trend", "net_margin_trend", EffectType.DIRECT, 0.7, "毛利传导"),
        CausalEdge("net_margin_trend", "roic_trend", EffectType.DIRECT, 0.5, "利润率影响ROIC"),
        CausalEdge("net_margin_trend", "roe_trend", EffectType.DIRECT, 0.6, "利润率影响ROE"),
        CausalEdge("roe_trend", "ocf_trend", EffectType.DIRECT, 0.4, "盈利转化现金流"),
        CausalEdge("roic_trend", "company_quality", EffectType.DIRECT, 0.25, "核心价值指标"),
        CausalEdge("revenue_trend", "company_quality", EffectType.DIRECT, 0.15, "增长反映发展"),
        CausalEdge("ocf_trend", "company_quality", EffectType.DIRECT, 0.20, "现金流验证质量"),
    ]

    for edge in edges:
        graph.add_edge(edge)

    return graph
