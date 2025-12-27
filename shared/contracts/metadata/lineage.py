"""
PGCS Metadata: Lineage
======================

数据血缘追踪系统。

设计原则:
- 记录数据的来源和转换历史
- 支持 DAG 结构
- 完全通用
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Dict, List, Set
from enum import Enum


class NodeType(Enum):
    """节点类型"""
    SOURCE = 'source'           # 数据源
    TRANSFORM = 'transform'     # 转换
    SINK = 'sink'               # 数据目标
    FIELD = 'field'             # 字段
    SCHEMA = 'schema'           # Schema


@dataclass
class LineageNode:
    """
    血缘节点

    表示数据流中的一个节点。

    Attributes:
        id: 节点 ID
        name: 节点名称
        node_type: 节点类型
        metadata: 节点元数据
        upstream: 上游节点 ID 列表
        downstream: 下游节点 ID 列表
    """
    id: str
    name: str
    node_type: NodeType = NodeType.TRANSFORM
    metadata: Dict[str, Any] = field(default_factory=dict)
    upstream: List[str] = field(default_factory=list)
    downstream: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def add_upstream(self, node_id: str):
        """添加上游节点"""
        if node_id not in self.upstream:
            self.upstream.append(node_id)

    def add_downstream(self, node_id: str):
        """添加下游节点"""
        if node_id not in self.downstream:
            self.downstream.append(node_id)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'name': self.name,
            'type': self.node_type.value,
            'metadata': self.metadata,
            'upstream': self.upstream,
            'downstream': self.downstream,
            'created_at': self.created_at,
        }


class Lineage:
    """
    数据血缘图

    管理数据流的 DAG 结构。

    Example:
        lineage = Lineage()

        # 添加节点
        lineage.add_node('source', 'Raw Data', NodeType.SOURCE)
        lineage.add_node('transform', 'Clean', NodeType.TRANSFORM)
        lineage.add_node('sink', 'Output', NodeType.SINK)

        # 连接
        lineage.connect('source', 'transform')
        lineage.connect('transform', 'sink')

        # 查询
        upstream = lineage.get_upstream('sink')
        downstream = lineage.get_downstream('source')
    """

    def __init__(self):
        self._nodes: Dict[str, LineageNode] = {}

    @property
    def nodes(self) -> Dict[str, LineageNode]:
        """获取所有节点 (只读副本)"""
        return self._nodes.copy()

    def add_node(
        self,
        node_id: str,
        name: str,
        node_type: NodeType = NodeType.TRANSFORM,
        **metadata,
    ) -> LineageNode:
        """
        添加节点

        Args:
            node_id: 节点 ID
            name: 节点名称
            node_type: 节点类型
            **metadata: 节点元数据

        Returns:
            LineageNode
        """
        node = LineageNode(
            id=node_id,
            name=name,
            node_type=node_type,
            metadata=metadata,
        )
        self._nodes[node_id] = node
        return node

    def get_node(self, node_id: str) -> Optional[LineageNode]:
        """获取节点"""
        return self._nodes.get(node_id)

    def remove_node(self, node_id: str):
        """删除节点"""
        if node_id in self._nodes:
            node = self._nodes[node_id]

            # 清理关联
            for up_id in node.upstream:
                if up_id in self._nodes:
                    up_node = self._nodes[up_id]
                    if node_id in up_node.downstream:
                        up_node.downstream.remove(node_id)

            for down_id in node.downstream:
                if down_id in self._nodes:
                    down_node = self._nodes[down_id]
                    if node_id in down_node.upstream:
                        down_node.upstream.remove(node_id)

            del self._nodes[node_id]

    def connect(self, from_id: str, to_id: str):
        """
        连接两个节点

        Args:
            from_id: 上游节点 ID
            to_id: 下游节点 ID
        """
        if from_id not in self._nodes:
            raise ValueError(f"Node not found: {from_id}")
        if to_id not in self._nodes:
            raise ValueError(f"Node not found: {to_id}")

        self._nodes[from_id].add_downstream(to_id)
        self._nodes[to_id].add_upstream(from_id)

    def disconnect(self, from_id: str, to_id: str):
        """断开连接"""
        if from_id in self._nodes:
            node = self._nodes[from_id]
            if to_id in node.downstream:
                node.downstream.remove(to_id)

        if to_id in self._nodes:
            node = self._nodes[to_id]
            if from_id in node.upstream:
                node.upstream.remove(from_id)

    def get_upstream(self, node_id: str, recursive: bool = False) -> List[LineageNode]:
        """
        获取上游节点

        Args:
            node_id: 节点 ID
            recursive: 是否递归获取所有上游

        Returns:
            上游节点列表
        """
        node = self._nodes.get(node_id)
        if not node:
            return []

        if not recursive:
            return [self._nodes[uid] for uid in node.upstream if uid in self._nodes]

        # 递归获取
        visited: Set[str] = set()
        result = []

        def collect(nid: str):
            if nid in visited:
                return
            visited.add(nid)

            n = self._nodes.get(nid)
            if n:
                for uid in n.upstream:
                    if uid in self._nodes:
                        result.append(self._nodes[uid])
                        collect(uid)

        collect(node_id)
        return result

    def get_downstream(self, node_id: str, recursive: bool = False) -> List[LineageNode]:
        """
        获取下游节点

        Args:
            node_id: 节点 ID
            recursive: 是否递归获取所有下游

        Returns:
            下游节点列表
        """
        node = self._nodes.get(node_id)
        if not node:
            return []

        if not recursive:
            return [self._nodes[did] for did in node.downstream if did in self._nodes]

        # 递归获取
        visited: Set[str] = set()
        result = []

        def collect(nid: str):
            if nid in visited:
                return
            visited.add(nid)

            n = self._nodes.get(nid)
            if n:
                for did in n.downstream:
                    if did in self._nodes:
                        result.append(self._nodes[did])
                        collect(did)

        collect(node_id)
        return result

    def get_sources(self) -> List[LineageNode]:
        """获取所有源节点 (无上游)"""
        return [n for n in self._nodes.values() if not n.upstream]

    def get_sinks(self) -> List[LineageNode]:
        """获取所有目标节点 (无下游)"""
        return [n for n in self._nodes.values() if not n.downstream]

    def get_path(self, from_id: str, to_id: str) -> Optional[List[str]]:
        """
        获取两节点间的路径

        Returns:
            路径节点 ID 列表，如果不存在返回 None
        """
        if from_id not in self._nodes or to_id not in self._nodes:
            return None

        # BFS
        from collections import deque

        queue = deque([(from_id, [from_id])])
        visited = {from_id}

        while queue:
            current, path = queue.popleft()

            if current == to_id:
                return path

            node = self._nodes.get(current)
            if node:
                for next_id in node.downstream:
                    if next_id not in visited:
                        visited.add(next_id)
                        queue.append((next_id, path + [next_id]))

        return None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'nodes': {nid: n.to_dict() for nid, n in self._nodes.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Lineage':
        """从字典创建"""
        lineage = cls()

        nodes_data = data.get('nodes', {})

        # 先创建所有节点
        for nid, node_data in nodes_data.items():
            lineage.add_node(
                node_id=nid,
                name=node_data['name'],
                node_type=NodeType(node_data['type']),
                **node_data.get('metadata', {}),
            )

        # 然后建立连接
        for nid, node_data in nodes_data.items():
            for down_id in node_data.get('downstream', []):
                if down_id in lineage._nodes:
                    lineage.connect(nid, down_id)

        return lineage

    def visualize_ascii(self) -> str:
        """生成 ASCII 可视化"""
        lines = ["Lineage Graph", "=" * 40, ""]

        sources = self.get_sources()
        if sources:
            lines.append("Sources:")
            for s in sources:
                lines.append(f"  [{s.name}] ({s.id})")

        lines.append("")
        lines.append("Connections:")
        for node in self._nodes.values():
            if node.downstream:
                for did in node.downstream:
                    dn = self._nodes.get(did)
                    if dn:
                        lines.append(f"  {node.name} -> {dn.name}")

        sinks = self.get_sinks()
        if sinks:
            lines.append("")
            lines.append("Sinks:")
            for s in sinks:
                lines.append(f"  [{s.name}] ({s.id})")

        return '\n'.join(lines)


__all__ = [
    'Lineage',
    'LineageNode',
    'NodeType',
]
