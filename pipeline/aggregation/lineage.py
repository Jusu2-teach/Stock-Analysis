"""
Data Lineage - 数据血缘追踪
===========================

轻量级数据血缘追踪，自动记录数据在 Pipeline 中的流动路径。

特性：
1. 自动追踪：装饰器自动记录生产/消费关系
2. DAG 表示：简洁的有向无环图数据结构
3. 可视化：Mermaid/DOT 格式导出
4. 影响分析：上下游依赖查询
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set
import functools

__all__ = [
    # 核心类型
    "DataLineage",
    "LineageNode",
    "LineageEdge",
    "NodeType",
    # 追踪器
    "LineageTracker",
    # 查询 API
    "LineageQuery",
    # 装饰器
    "track_lineage",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class NodeType(Enum):
    """血缘节点类型"""
    SOURCE = auto()      # 数据源 (外部输入)
    PRODUCER = auto()    # 生产者 (中间节点)
    CONSUMER = auto()    # 消费者 (汇聚节点)
    OUTPUT = auto()      # 输出 (最终结果)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class LineageNode:
    """血缘节点 - 数据流中的一个点

    Attributes:
        node_id: 唯一标识
        node_type: 节点类型
        name: 显示名称 (通常是 step_name)
        namespace: 命名空间
        key: 数据键 (可选)
        metadata: 额外元数据
    """
    node_id: str
    node_type: NodeType
    name: str
    namespace: str = "default"
    key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    @property
    def full_key(self) -> str:
        """完整键: namespace.key 或 namespace"""
        if self.key:
            return f"{self.namespace}.{self.key}"
        return self.namespace

    def __hash__(self):
        return hash(self.node_id)

    def __eq__(self, other):
        if isinstance(other, LineageNode):
            return self.node_id == other.node_id
        return False


@dataclass
class LineageEdge:
    """血缘边 - 数据流中的一条依赖关系

    Attributes:
        source_id: 源节点 ID
        target_id: 目标节点 ID
        relationship: 关系类型 (produces/consumes/transforms)
    """
    source_id: str
    target_id: str
    relationship: str = "produces"
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def edge_id(self) -> str:
        return f"{self.source_id}->{self.target_id}"


# =============================================================================
# Data Lineage Graph
# =============================================================================

@dataclass
class DataLineage:
    """数据血缘图 - 完整的数据流 DAG

    Examples:
        lineage = DataLineage()

        # 添加节点
        lineage.add_node(LineageNode("step1", NodeType.PRODUCER, "Load_Data"))
        lineage.add_node(LineageNode("step2", NodeType.CONSUMER, "Analyze"))

        # 添加边
        lineage.add_edge(LineageEdge("step1", "step2"))

        # 查询
        upstream = lineage.get_upstream("step2")
        downstream = lineage.get_downstream("step1")

        # 可视化
        print(lineage.to_mermaid())
    """
    lineage_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    flow_id: Optional[str] = None
    nodes: Dict[str, LineageNode] = field(default_factory=dict)
    edges: List[LineageEdge] = field(default_factory=list)

    # 内部索引 (懒加载)
    _upstream_index: Dict[str, Set[str]] = field(default_factory=dict, repr=False)
    _downstream_index: Dict[str, Set[str]] = field(default_factory=dict, repr=False)

    # -------------------------------------------------------------------------
    # Mutation
    # -------------------------------------------------------------------------

    def add_node(self, node: LineageNode) -> None:
        """添加节点"""
        self.nodes[node.node_id] = node
        # 初始化索引
        if node.node_id not in self._upstream_index:
            self._upstream_index[node.node_id] = set()
        if node.node_id not in self._downstream_index:
            self._downstream_index[node.node_id] = set()

    def add_edge(self, edge: LineageEdge) -> None:
        """添加边并更新索引"""
        # 确保节点存在
        if edge.source_id not in self.nodes:
            logger.warning(f"Source node {edge.source_id} not found, creating placeholder")
            self.add_node(LineageNode(edge.source_id, NodeType.PRODUCER, edge.source_id))

        if edge.target_id not in self.nodes:
            logger.warning(f"Target node {edge.target_id} not found, creating placeholder")
            self.add_node(LineageNode(edge.target_id, NodeType.CONSUMER, edge.target_id))

        self.edges.append(edge)

        # 更新索引
        self._downstream_index[edge.source_id].add(edge.target_id)
        self._upstream_index[edge.target_id].add(edge.source_id)

    def connect(self, source_id: str, target_id: str, relationship: str = "produces") -> None:
        """便捷方法: 连接两个节点"""
        self.add_edge(LineageEdge(source_id, target_id, relationship))

    # -------------------------------------------------------------------------
    # Query (Direct)
    # -------------------------------------------------------------------------

    def get_upstream(self, node_id: str) -> Set[str]:
        """获取直接上游节点"""
        return self._upstream_index.get(node_id, set()).copy()

    def get_downstream(self, node_id: str) -> Set[str]:
        """获取直接下游节点"""
        return self._downstream_index.get(node_id, set()).copy()

    # -------------------------------------------------------------------------
    # Query (Recursive)
    # -------------------------------------------------------------------------

    def get_all_upstream(self, node_id: str) -> Set[str]:
        """递归获取所有上游节点"""
        result = set()
        stack = list(self.get_upstream(node_id))

        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(self.get_upstream(current))

        return result

    def get_all_downstream(self, node_id: str) -> Set[str]:
        """递归获取所有下游节点"""
        result = set()
        stack = list(self.get_downstream(node_id))

        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(self.get_downstream(current))

        return result

    def get_path(self, from_id: str, to_id: str) -> List[str]:
        """获取两节点间的路径 (BFS)"""
        if from_id == to_id:
            return [from_id]

        from collections import deque
        queue = deque([(from_id, [from_id])])
        visited = {from_id}

        while queue:
            current, path = queue.popleft()

            for next_id in self.get_downstream(current):
                if next_id == to_id:
                    return path + [next_id]

                if next_id not in visited:
                    visited.add(next_id)
                    queue.append((next_id, path + [next_id]))

        return []  # 无路径

    # -------------------------------------------------------------------------
    # Export
    # -------------------------------------------------------------------------

    def to_mermaid(self) -> str:
        """导出为 Mermaid 格式

        Returns:
            Mermaid 图定义字符串
        """
        lines = ["graph LR"]

        # 节点样式映射
        style_map = {
            NodeType.SOURCE: ("[(", ")]"),     # 圆角矩形
            NodeType.PRODUCER: ("([", "])"),   # 椭圆
            NodeType.CONSUMER: ("[[", "]]"),   # 双边矩形
            NodeType.OUTPUT: ("{{", "}}"),     # 六边形
        }

        # 渲染节点
        for node_id, node in self.nodes.items():
            start, end = style_map.get(node.node_type, ("(", ")"))
            label = node.name
            if node.key:
                label = f"{node.name}\\n{node.key}"
            lines.append(f"    {node_id}{start}\"{label}\"{end}")

        # 渲染边
        for edge in self.edges:
            if edge.relationship == "produces":
                lines.append(f"    {edge.source_id} --> {edge.target_id}")
            else:
                lines.append(f"    {edge.source_id} -.->|{edge.relationship}| {edge.target_id}")

        return "\n".join(lines)

    def to_dot(self) -> str:
        """导出为 DOT (Graphviz) 格式"""
        lines = [
            "digraph Lineage {",
            "    rankdir=LR;",
            "    node [shape=box];",
        ]

        # 节点
        for node_id, node in self.nodes.items():
            shape = "box"
            if node.node_type == NodeType.SOURCE:
                shape = "cylinder"
            elif node.node_type == NodeType.OUTPUT:
                shape = "doubleoctagon"

            label = f"{node.name}\\n{node.key}" if node.key else node.name
            lines.append(f'    {node_id} [label="{label}", shape={shape}];')

        # 边
        for edge in self.edges:
            style = ""
            if edge.relationship != "produces":
                style = f' [label="{edge.relationship}", style=dashed]'
            lines.append(f"    {edge.source_id} -> {edge.target_id}{style};")

        lines.append("}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "lineage_id": self.lineage_id,
            "flow_id": self.flow_id,
            "nodes": {
                nid: {
                    "node_id": n.node_id,
                    "node_type": n.node_type.name,
                    "name": n.name,
                    "namespace": n.namespace,
                    "key": n.key,
                }
                for nid, n in self.nodes.items()
            },
            "edges": [
                {
                    "source": e.source_id,
                    "target": e.target_id,
                    "relationship": e.relationship,
                }
                for e in self.edges
            ],
        }


# =============================================================================
# Lineage Tracker
# =============================================================================

class LineageTracker:
    """血缘追踪器 - 自动追踪数据流

    Examples:
        tracker = LineageTracker()

        # 开始追踪
        tracker.start_flow("flow-123")

        # 记录生产
        tracker.track_produce("Load_Data", "raw", "data", {"rows": 1000})

        # 记录消费
        tracker.track_consume("Analyze", ["raw.data"])

        # 获取血缘
        lineage = tracker.get_lineage()
        print(lineage.to_mermaid())
    """

    _instance: Optional["LineageTracker"] = None

    def __init__(self):
        self._lineages: Dict[str, DataLineage] = {}
        self._current_flow_id: Optional[str] = None

    @classmethod
    def instance(cls) -> "LineageTracker":
        """获取单例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # -------------------------------------------------------------------------
    # Flow Lifecycle
    # -------------------------------------------------------------------------

    def start_flow(self, flow_id: str) -> DataLineage:
        """开始追踪 Flow"""
        lineage = DataLineage(flow_id=flow_id)
        self._lineages[flow_id] = lineage
        self._current_flow_id = flow_id
        logger.debug(f"🔍 Lineage tracking started: {flow_id}")
        return lineage

    def end_flow(self, flow_id: str = None) -> Optional[DataLineage]:
        """结束追踪"""
        fid = flow_id or self._current_flow_id
        if fid:
            if fid == self._current_flow_id:
                self._current_flow_id = None
            return self._lineages.get(fid)
        return None

    def get_lineage(self, flow_id: str = None) -> Optional[DataLineage]:
        """获取血缘图"""
        fid = flow_id or self._current_flow_id
        return self._lineages.get(fid) if fid else None

    # -------------------------------------------------------------------------
    # Tracking API
    # -------------------------------------------------------------------------

    def track_produce(
        self,
        step_name: str,
        namespace: str,
        key: str,
        metadata: Dict[str, Any] = None,
    ) -> str:
        """记录数据生产

        Args:
            step_name: 步骤名称
            namespace: 命名空间
            key: 数据键
            metadata: 额外元数据

        Returns:
            生成的节点 ID
        """
        lineage = self.get_lineage()
        if lineage is None:
            return ""

        # 创建数据节点
        node_id = f"{step_name}:{namespace}.{key}"
        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.PRODUCER,
            name=step_name,
            namespace=namespace,
            key=key,
            metadata=metadata or {},
        )
        lineage.add_node(node)

        logger.debug(f"📊 Tracked produce: {node_id}")
        return node_id

    def track_consume(
        self,
        step_name: str,
        sources: List[str],
        output_namespace: str = None,
        output_key: str = None,
    ) -> str:
        """记录数据消费

        Args:
            step_name: 步骤名称
            sources: 消费的数据源列表 (格式: "namespace.key" 或节点ID)
            output_namespace: 输出命名空间 (可选)
            output_key: 输出键 (可选)

        Returns:
            消费者节点 ID
        """
        lineage = self.get_lineage()
        if lineage is None:
            return ""

        # 创建消费者节点
        node_id = f"{step_name}:consumer"
        if output_namespace and output_key:
            node_id = f"{step_name}:{output_namespace}.{output_key}"

        node = LineageNode(
            node_id=node_id,
            node_type=NodeType.CONSUMER,
            name=step_name,
            namespace=output_namespace or "default",
            key=output_key,
        )
        lineage.add_node(node)

        # 连接到源
        for source in sources:
            # 查找匹配的源节点
            source_node_id = self._find_source_node(lineage, source)
            if source_node_id:
                lineage.connect(source_node_id, node_id, "consumes")

        logger.debug(f"📊 Tracked consume: {node_id} <- {sources}")
        return node_id

    def _find_source_node(self, lineage: DataLineage, source: str) -> Optional[str]:
        """查找源节点 ID"""
        # 直接匹配
        if source in lineage.nodes:
            return source

        # 按 full_key 匹配
        for node_id, node in lineage.nodes.items():
            if node.full_key == source:
                return node_id

        # 按 namespace.key 后缀匹配
        for node_id, node in lineage.nodes.items():
            if node_id.endswith(f":{source}"):
                return node_id

        return None


# =============================================================================
# Query Helper
# =============================================================================

class LineageQuery:
    """血缘查询助手 - 提供便捷的查询 API

    Examples:
        query = LineageQuery(lineage)

        # 影响分析
        affected = query.impact_analysis("Load_Data:raw.data")

        # 依赖分析
        deps = query.dependency_analysis("Evaluate:consumer")

        # 根节点
        roots = query.find_roots()
    """

    def __init__(self, lineage: DataLineage):
        self._lineage = lineage

    def impact_analysis(self, node_id: str) -> Dict[str, Any]:
        """影响分析: 修改此节点会影响哪些下游

        Returns:
            {
                "affected_nodes": [...],
                "affected_count": N,
                "critical_path": [...],
            }
        """
        affected = self._lineage.get_all_downstream(node_id)

        # 找到最远的叶子节点
        leaves = [n for n in affected if not self._lineage.get_downstream(n)]

        # 计算关键路径 (到最远叶子)
        critical_path = []
        if leaves:
            # 选择最长路径
            for leaf in leaves:
                path = self._lineage.get_path(node_id, leaf)
                if len(path) > len(critical_path):
                    critical_path = path

        return {
            "affected_nodes": list(affected),
            "affected_count": len(affected),
            "critical_path": critical_path,
        }

    def dependency_analysis(self, node_id: str) -> Dict[str, Any]:
        """依赖分析: 此节点依赖哪些上游

        Returns:
            {
                "dependencies": [...],
                "dependency_count": N,
                "root_sources": [...],
            }
        """
        deps = self._lineage.get_all_upstream(node_id)

        # 找到根节点 (无上游的节点)
        roots = [n for n in deps if not self._lineage.get_upstream(n)]

        return {
            "dependencies": list(deps),
            "dependency_count": len(deps),
            "root_sources": roots,
        }

    def find_roots(self) -> List[str]:
        """查找所有根节点 (数据源)"""
        roots = []
        for node_id in self._lineage.nodes:
            if not self._lineage.get_upstream(node_id):
                roots.append(node_id)
        return roots

    def find_leaves(self) -> List[str]:
        """查找所有叶子节点 (最终输出)"""
        leaves = []
        for node_id in self._lineage.nodes:
            if not self._lineage.get_downstream(node_id):
                leaves.append(node_id)
        return leaves


# =============================================================================
# Decorator
# =============================================================================

def track_lineage(
    namespace: str = "default",
    key: str = None,
    consumes: List[str] = None,
) -> Callable:
    """装饰器: 自动追踪函数的血缘

    Args:
        namespace: 输出命名空间
        key: 输出键 (None 时使用函数名)
        consumes: 消费的数据源列表

    Examples:
        @track_lineage(namespace="trends", key="roic", consumes=["raw.data"])
        def analyze_roic(data):
            return result
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracker = LineageTracker.instance()
            step_name = func.__name__
            output_key = key or step_name

            # 记录消费
            if consumes:
                tracker.track_consume(
                    step_name=step_name,
                    sources=consumes,
                    output_namespace=namespace,
                    output_key=output_key,
                )
            else:
                # 只记录生产
                tracker.track_produce(
                    step_name=step_name,
                    namespace=namespace,
                    key=output_key,
                )

            return func(*args, **kwargs)

        return wrapper

    return decorator
