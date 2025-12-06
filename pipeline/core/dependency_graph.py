"""DependencyGraph - 专业级依赖图管理

提供统一的依赖关系建模、拓扑排序、循环检测和层次化执行计划。

设计原则：
- Single Responsibility: 只负责依赖关系管理
- Open/Closed: 通过依赖源接口扩展新的依赖类型
- Dependency Inversion: 依赖抽象而非具体实现

参考：
- Apache Airflow DAG 实现
- Prefect Flow 依赖模型
- Kedro Pipeline 拓扑排序
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, Generic, Iterable, List, Optional, Set, TypeVar
from collections import defaultdict, deque
import logging

__all__ = [
    'DependencyType',
    'DependencyEdge',
    'DependencyGraph',
    'DependencySource',
    'DataDependencySource',
    'ExplicitDependencySource',
    'ExecutionPlan',
    'ExecutionLayer',
    'CyclicDependencyError',
    'MissingDependencyError',
]


class DependencyType(Enum):
    """依赖类型枚举

    区分不同来源的依赖，便于调试和可视化。
    """
    DATA = auto()       # 数据依赖：通过输入/输出数据集推导
    EXPLICIT = auto()   # 显式依赖：通过 depends_on 声明
    RESOURCE = auto()   # 资源依赖：共享资源（如数据库连接）
    TEMPORAL = auto()   # 时序依赖：时间窗口约束


@dataclass(frozen=True)
class DependencyEdge:
    """依赖边（不可变）

    表示 from_node -> to_node 的依赖关系，
    即 to_node 依赖于 from_node，from_node 必须先执行。

    Attributes:
        from_node: 上游节点（被依赖方）
        to_node: 下游节点（依赖方）
        dep_type: 依赖类型
        metadata: 额外元数据（如数据集名称）
    """
    from_node: str
    to_node: str
    dep_type: DependencyType
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash((self.from_node, self.to_node, self.dep_type))


class CyclicDependencyError(Exception):
    """循环依赖错误"""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        super().__init__(f"检测到循环依赖: {' -> '.join(cycle)}")


class MissingDependencyError(Exception):
    """缺失依赖错误"""
    def __init__(self, node: str, missing_deps: List[str]):
        self.node = node
        self.missing_deps = missing_deps
        super().__init__(f"节点 '{node}' 依赖的节点不存在: {missing_deps}")


class DependencySource(ABC):
    """依赖源抽象基类

    定义如何从特定数据源提取依赖关系。
    遵循策略模式，允许插入不同的依赖解析策略。
    """

    @abstractmethod
    def extract_dependencies(self, node_name: str, node_config: Dict[str, Any],
                            all_nodes: Dict[str, Any]) -> Iterable[DependencyEdge]:
        """从节点配置中提取依赖边

        Args:
            node_name: 当前节点名称
            node_config: 当前节点配置
            all_nodes: 所有节点配置（用于验证依赖是否存在）

        Yields:
            DependencyEdge 实例
        """
        pass


class DataDependencySource(DependencySource):
    """数据依赖源

    从数据集的生产者-消费者关系推导依赖。
    """

    def extract_dependencies(self, node_name: str, node_config: Dict[str, Any],
                            all_nodes: Dict[str, Any]) -> Iterable[DependencyEdge]:
        # 构建数据集到生产者的映射
        dataset_producer: Dict[str, str] = {}
        for name, cfg in all_nodes.items():
            outputs = cfg.get('outputs', [])
            if isinstance(outputs, str):
                outputs = [outputs]
            for out in outputs:
                dataset_producer[out] = name

        # 当前节点的输入数据集
        inputs = node_config.get('inputs', [])
        if isinstance(inputs, str):
            inputs = [inputs]

        for input_ds in inputs:
            if input_ds in dataset_producer:
                producer = dataset_producer[input_ds]
                if producer != node_name:  # 避免自依赖
                    yield DependencyEdge(
                        from_node=producer,
                        to_node=node_name,
                        dep_type=DependencyType.DATA,
                        metadata={'dataset': input_ds}
                    )


class ExplicitDependencySource(DependencySource):
    """显式依赖源

    从 depends_on 字段解析显式声明的依赖。
    """

    def extract_dependencies(self, node_name: str, node_config: Dict[str, Any],
                            all_nodes: Dict[str, Any]) -> Iterable[DependencyEdge]:
        depends_on = node_config.get('depends_on', [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]

        for dep in depends_on:
            yield DependencyEdge(
                from_node=dep,
                to_node=node_name,
                dep_type=DependencyType.EXPLICIT,
                metadata={'declared_in': 'depends_on'}
            )


@dataclass
class ExecutionLayer:
    """执行层

    表示可并行执行的一组节点。
    """
    index: int
    nodes: List[str]

    def __len__(self) -> int:
        return len(self.nodes)

    def __iter__(self):
        return iter(self.nodes)


@dataclass
class ExecutionPlan:
    """执行计划

    包含完整的执行顺序和层次信息。
    """
    layers: List[ExecutionLayer]
    total_nodes: int
    critical_path: List[str] = field(default_factory=list)

    @property
    def max_parallelism(self) -> int:
        """最大并行度"""
        return max(len(layer) for layer in self.layers) if self.layers else 0

    @property
    def depth(self) -> int:
        """执行深度（层数）"""
        return len(self.layers)

    def flatten(self) -> List[str]:
        """扁平化为顺序执行列表"""
        result = []
        for layer in self.layers:
            result.extend(layer.nodes)
        return result

    def __repr__(self) -> str:
        return f"ExecutionPlan(layers={self.depth}, nodes={self.total_nodes}, max_parallelism={self.max_parallelism})"


class DependencyGraph:
    """依赖图

    核心类，管理节点间的依赖关系，提供：
    - 依赖添加/删除
    - 循环检测
    - 拓扑排序
    - 层次化执行计划生成
    - 依赖可视化

    线程安全：本类不是线程安全的，需要外部同步。
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._logger = logger or logging.getLogger(__name__)

        # 节点集合
        self._nodes: Set[str] = set()

        # 邻接表：node -> set of downstream nodes (依赖于该节点的节点)
        self._successors: Dict[str, Set[str]] = defaultdict(set)

        # 反向邻接表：node -> set of upstream nodes (该节点依赖的节点)
        self._predecessors: Dict[str, Set[str]] = defaultdict(set)

        # 边集合（用于元数据查询）
        self._edges: Dict[tuple, DependencyEdge] = {}

        # 依赖源
        self._sources: List[DependencySource] = [
            DataDependencySource(),
            ExplicitDependencySource(),
        ]

    def add_node(self, name: str) -> None:
        """添加节点"""
        self._nodes.add(name)

    def add_nodes(self, names: Iterable[str]) -> None:
        """批量添加节点"""
        self._nodes.update(names)

    def add_edge(self, edge: DependencyEdge) -> None:
        """添加依赖边

        Args:
            edge: 依赖边，from_node -> to_node 表示 to_node 依赖 from_node
        """
        self._nodes.add(edge.from_node)
        self._nodes.add(edge.to_node)
        self._successors[edge.from_node].add(edge.to_node)
        self._predecessors[edge.to_node].add(edge.from_node)
        self._edges[(edge.from_node, edge.to_node)] = edge

    def add_dependency(self, from_node: str, to_node: str,
                       dep_type: DependencyType = DependencyType.EXPLICIT,
                       metadata: Optional[Dict[str, Any]] = None) -> None:
        """添加依赖关系的便捷方法

        Args:
            from_node: 上游节点（被依赖）
            to_node: 下游节点（依赖方）
            dep_type: 依赖类型
            metadata: 元数据
        """
        edge = DependencyEdge(
            from_node=from_node,
            to_node=to_node,
            dep_type=dep_type,
            metadata=metadata or {}
        )
        self.add_edge(edge)

    def get_predecessors(self, node: str) -> FrozenSet[str]:
        """获取节点的所有前驱（上游依赖）"""
        return frozenset(self._predecessors.get(node, set()))

    def get_successors(self, node: str) -> FrozenSet[str]:
        """获取节点的所有后继（下游依赖）"""
        return frozenset(self._successors.get(node, set()))

    def has_cycle(self) -> bool:
        """检测是否存在循环依赖"""
        try:
            self._topological_sort()
            return False
        except CyclicDependencyError:
            return True

    def find_cycle(self) -> Optional[List[str]]:
        """查找循环依赖路径（如果存在）"""
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for succ in self._successors.get(node, set()):
                if succ not in visited:
                    result = dfs(succ)
                    if result:
                        return result
                elif succ in rec_stack:
                    # 找到循环
                    cycle_start = path.index(succ)
                    return path[cycle_start:] + [succ]

            path.pop()
            rec_stack.remove(node)
            return None

        for node in self._nodes:
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    return cycle
        return None

    def _topological_sort(self) -> List[str]:
        """Kahn's 算法实现拓扑排序

        Returns:
            排序后的节点列表

        Raises:
            CyclicDependencyError: 如果存在循环依赖
        """
        # 计算入度
        in_degree = {node: len(self._predecessors.get(node, set()))
                    for node in self._nodes}

        # 入度为0的节点入队
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node = queue.popleft()
            result.append(node)

            for succ in self._successors.get(node, set()):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    queue.append(succ)

        if len(result) != len(self._nodes):
            # 存在循环
            cycle = self.find_cycle()
            raise CyclicDependencyError(cycle or list(self._nodes - set(result)))

        return result

    def build_execution_plan(self) -> ExecutionPlan:
        """构建层次化执行计划

        将节点分组为可并行执行的层次。

        Returns:
            ExecutionPlan 实例

        Raises:
            CyclicDependencyError: 如果存在循环依赖
        """
        if not self._nodes:
            return ExecutionPlan(layers=[], total_nodes=0)

        # 计算入度
        in_degree = {node: len(self._predecessors.get(node, set()))
                    for node in self._nodes}

        layers = []
        remaining = set(self._nodes)
        completed = set()

        layer_idx = 0
        while remaining:
            # 当前层：所有依赖已完成的节点
            current_layer = [
                node for node in remaining
                if all(pred in completed for pred in self._predecessors.get(node, set()))
            ]

            if not current_layer:
                # 无法继续 -> 存在循环
                cycle = self.find_cycle()
                raise CyclicDependencyError(cycle or list(remaining))

            layers.append(ExecutionLayer(index=layer_idx, nodes=sorted(current_layer)))
            completed.update(current_layer)
            remaining -= set(current_layer)
            layer_idx += 1

        # 计算关键路径（最长路径）
        critical_path = self._compute_critical_path()

        return ExecutionPlan(
            layers=layers,
            total_nodes=len(self._nodes),
            critical_path=critical_path
        )

    def _compute_critical_path(self) -> List[str]:
        """计算关键路径（DAG中的最长路径）"""
        if not self._nodes:
            return []

        # 动态规划计算最长路径
        order = self._topological_sort()
        dist = {node: 0 for node in self._nodes}
        prev = {node: None for node in self._nodes}

        for node in order:
            for succ in self._successors.get(node, set()):
                if dist[node] + 1 > dist[succ]:
                    dist[succ] = dist[node] + 1
                    prev[succ] = node

        # 找到终点（最大距离的节点）
        end_node = max(dist.keys(), key=lambda n: dist[n])

        # 回溯构建路径
        path = []
        current = end_node
        while current is not None:
            path.append(current)
            current = prev[current]

        return list(reversed(path))

    def validate(self, strict: bool = True) -> List[str]:
        """验证依赖图的完整性

        Args:
            strict: 严格模式下，缺失依赖会抛出异常

        Returns:
            警告信息列表

        Raises:
            MissingDependencyError: strict=True 且存在缺失依赖时
        """
        warnings = []

        for node in self._nodes:
            for pred in self._predecessors.get(node, set()):
                if pred not in self._nodes:
                    msg = f"节点 '{node}' 依赖的 '{pred}' 不存在"
                    if strict:
                        raise MissingDependencyError(node, [pred])
                    warnings.append(msg)

        return warnings

    @classmethod
    def from_node_configs(cls, node_configs: Dict[str, Dict[str, Any]],
                         sources: Optional[List[DependencySource]] = None,
                         logger: Optional[logging.Logger] = None) -> 'DependencyGraph':
        """从节点配置构建依赖图

        工厂方法，使用依赖源从配置中提取依赖关系。

        Args:
            node_configs: 节点名称到配置的映射
            sources: 依赖源列表（None 使用默认源）
            logger: 日志器

        Returns:
            DependencyGraph 实例
        """
        graph = cls(logger=logger)
        graph.add_nodes(node_configs.keys())

        if sources is None:
            sources = graph._sources

        # 从每个源提取依赖
        for node_name, node_config in node_configs.items():
            for source in sources:
                for edge in source.extract_dependencies(node_name, node_config, node_configs):
                    # 验证依赖目标存在
                    if edge.from_node in node_configs:
                        graph.add_edge(edge)
                    elif graph._logger:
                        graph._logger.warning(
                            f"⚠️ 依赖 '{edge.from_node}' 不存在 (来自 {node_name})"
                        )

        return graph

    def to_dict(self) -> Dict[str, Any]:
        """导出为字典格式（便于序列化）"""
        return {
            'nodes': list(self._nodes),
            'edges': [
                {
                    'from': edge.from_node,
                    'to': edge.to_node,
                    'type': edge.dep_type.name,
                    'metadata': edge.metadata
                }
                for edge in self._edges.values()
            ]
        }

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node: str) -> bool:
        return node in self._nodes

    def __repr__(self) -> str:
        return f"DependencyGraph(nodes={len(self._nodes)}, edges={len(self._edges)})"
