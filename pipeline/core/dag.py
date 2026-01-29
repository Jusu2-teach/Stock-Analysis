"""Pipeline Core Models - DAG (Directed Acyclic Graph)
=====================================================

提供依赖图构建、拓扑排序和执行计划生成。

设计原则：
- 单次构建 - 避免重复解析
- 懒加载 - 仅在需要时计算
- 验证完整性 - 检测循环依赖

版本: 2.0.0
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)
import re

from .spec import FlowSpec, TaskSpec


# =============================================================================
# 数据引用解析
# =============================================================================

# 引用模式: steps.{task_name}.outputs.parameters.{param_name}
#         或: steps.{task_name}.outputs.{param_name}
_REFERENCE_PATTERN = re.compile(
    r'^steps\.(?P<task>[a-zA-Z_][a-zA-Z0-9_]*)\.outputs(?:\.parameters)?\.(?P<param>[a-zA-Z_][a-zA-Z0-9_]*)$'
)


@dataclass(frozen=True)
class DataReference:
    """数据引用

    解析 YAML 中的数据引用表达式。

    Attributes:
        source_task: 来源任务名称
        output_name: 输出参数名称
        raw: 原始表达式

    Example:
        ref = DataReference.parse("steps.load_data.outputs.raw_data")
        # ref.source_task = "load_data"
        # ref.output_name = "raw_data"
    """
    source_task: str
    output_name: str
    raw: str

    @classmethod
    def parse(cls, expr: str) -> Optional['DataReference']:
        """解析引用表达式

        Args:
            expr: 引用表达式字符串

        Returns:
            DataReference 或 None (如果不是有效引用)
        """
        if not isinstance(expr, str):
            return None

        match = _REFERENCE_PATTERN.match(expr)
        if not match:
            return None

        return cls(
            source_task=match.group('task'),
            output_name=match.group('param'),
            raw=expr,
        )

    @classmethod
    def is_reference(cls, expr: str) -> bool:
        """检查是否为引用表达式"""
        return cls.parse(expr) is not None


# =============================================================================
# 执行层
# =============================================================================

@dataclass(frozen=True)
class ExecutionLayer:
    """执行层

    同一层内的任务可以并行执行。

    Attributes:
        level: 层级号 (0-based)
        tasks: 该层的任务名称
    """
    level: int
    tasks: Tuple[str, ...]

    def __len__(self) -> int:
        return len(self.tasks)

    def __iter__(self) -> Iterator[str]:
        return iter(self.tasks)


@dataclass
class ExecutionPlan:
    """执行计划

    包含拓扑排序后的执行层。

    Attributes:
        layers: 执行层列表 (按顺序)
        total_tasks: 总任务数
        total_layers: 总层数
        critical_path: 关键路径上的任务
    """
    layers: List[ExecutionLayer]
    total_tasks: int = 0
    total_layers: int = 0
    critical_path: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.total_tasks = sum(len(layer) for layer in self.layers)
        self.total_layers = len(self.layers)

    def get_task_layer(self, task_name: str) -> int:
        """获取任务所在层级"""
        for layer in self.layers:
            if task_name in layer.tasks:
                return layer.level
        return -1

    def get_execution_order(self) -> List[str]:
        """获取串行执行顺序"""
        return [task for layer in self.layers for task in layer.tasks]

    def __iter__(self) -> Iterator[ExecutionLayer]:
        return iter(self.layers)


# =============================================================================
# 依赖图
# =============================================================================

class CyclicDependencyError(Exception):
    """循环依赖错误"""
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        cycle_str = ' -> '.join(cycle + [cycle[0]])
        super().__init__(f"Cyclic dependency detected: {cycle_str}")


class MissingDependencyError(Exception):
    """缺失依赖错误"""
    def __init__(self, task: str, missing: str):
        self.task = task
        self.missing = missing
        super().__init__(f"Task '{task}' depends on non-existent task '{missing}'")


@dataclass
class DependencyInfo:
    """依赖信息"""
    upstream: FrozenSet[str]   # 上游任务 (依赖)
    downstream: FrozenSet[str]  # 下游任务 (被依赖)
    level: int = -1             # 拓扑层级


class DAG:
    """依赖图

    构建和管理任务间的依赖关系。

    核心功能：
    - 解析 YAML 引用表达式提取隐式依赖
    - 合并显式 depends_on 声明
    - 拓扑排序生成执行计划
    - 循环依赖检测

    Example:
        dag = DAG.from_flow_spec(flow_spec)
        plan = dag.get_execution_plan()

        for layer in plan:
            # 同层任务可并行
            for task in layer:
                run(task)
    """

    def __init__(self):
        self._tasks: Set[str] = set()
        self._edges: Dict[str, Set[str]] = defaultdict(set)  # task -> upstream tasks
        self._reverse_edges: Dict[str, Set[str]] = defaultdict(set)  # task -> downstream tasks
        self._dependency_info: Dict[str, DependencyInfo] = {}
        self._execution_plan: Optional[ExecutionPlan] = None

    @classmethod
    def from_flow_spec(cls, flow_spec: FlowSpec) -> 'DAG':
        """从流程规范构建 DAG

        Args:
            flow_spec: 流程规范

        Returns:
            构建好的 DAG

        Raises:
            CyclicDependencyError: 如果存在循环依赖
            MissingDependencyError: 如果引用了不存在的任务
        """
        dag = cls()
        task_names = {t.name for t in flow_spec.tasks}

        for task_spec in flow_spec.tasks:
            dag._tasks.add(task_spec.name)

            # 收集依赖：显式 + 隐式
            dependencies = set(task_spec.depends_on)

            # 从输入引用中提取隐式依赖
            for inp in task_spec.inputs:
                if inp.source:
                    ref = DataReference.parse(inp.source)
                    if ref:
                        dependencies.add(ref.source_task)

            # 从参数中提取隐式依赖
            for value in task_spec.parameters.values():
                dag._extract_references_from_value(value, dependencies)

            # 验证依赖存在性
            for dep in dependencies:
                if dep not in task_names:
                    raise MissingDependencyError(task_spec.name, dep)

            # 添加边
            for dep in dependencies:
                dag._edges[task_spec.name].add(dep)
                dag._reverse_edges[dep].add(task_spec.name)

        # 检测循环依赖
        cycle = dag._detect_cycle()
        if cycle:
            raise CyclicDependencyError(cycle)

        # 计算拓扑层级
        dag._compute_levels()

        return dag

    def _extract_references_from_value(self, value, dependencies: Set[str]) -> None:
        """从参数值中递归提取引用"""
        if isinstance(value, str):
            ref = DataReference.parse(value)
            if ref:
                dependencies.add(ref.source_task)
        elif isinstance(value, dict):
            for v in value.values():
                self._extract_references_from_value(v, dependencies)
        elif isinstance(value, (list, tuple)):
            for v in value:
                self._extract_references_from_value(v, dependencies)

    def _detect_cycle(self) -> Optional[List[str]]:
        """检测循环依赖

        使用迭代式 DFS 检测，避免大型 DAG 栈溢出。
        返回循环路径 (如有)。
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {task: WHITE for task in self._tasks}

        # 迭代顺序稳定化：避免 set 迭代导致循环路径报告不稳定
        for start in sorted(self._tasks):
            if color[start] != WHITE:
                continue

            # 迭代式 DFS: (task, neighbors_iterator)
            # 邻接边迭代顺序稳定化
            stack = [(start, iter(sorted(self._edges.get(start, []))))]
            path = [start]
            color[start] = GRAY

            while stack:
                task, neighbors = stack[-1]
                try:
                    neighbor = next(neighbors)
                    if color[neighbor] == GRAY:
                        # 找到循环: 返回从 neighbor 开始到当前的路径
                        # path[cycle_start:] 已包含从 neighbor 到 task 的路径
                        # 不需要再添加 neighbor，因为 CyclicDependencyError 会自动添加首节点形成闭环
                        cycle_start = path.index(neighbor)
                        return path[cycle_start:]
                    elif color[neighbor] == WHITE:
                        color[neighbor] = GRAY
                        path.append(neighbor)
                        stack.append((neighbor, iter(sorted(self._edges.get(neighbor, [])))))
                except StopIteration:
                    color[task] = BLACK
                    path.pop()
                    stack.pop()

        return None

    def _compute_levels(self) -> None:
        """计算拓扑层级 (Kahn 算法)"""
        # 计算入度
        in_degree = {task: len(self._edges[task]) for task in self._tasks}

        # 队列初始化
        # 稳定化：同层初始节点排序，保证层级分配可复现
        queue = deque(sorted([task for task, deg in in_degree.items() if deg == 0]))
        level_map: Dict[str, int] = {}
        current_level = 0

        while queue:
            # 当前层的任务
            layer_size = len(queue)
            for _ in range(layer_size):
                task = queue.popleft()
                level_map[task] = current_level

                # 更新下游任务的入度
                # 稳定化：下游遍历排序，避免入队顺序随机
                for downstream in sorted(self._reverse_edges[task]):
                    in_degree[downstream] -= 1
                    if in_degree[downstream] == 0:
                        queue.append(downstream)

            current_level += 1

        # 构建依赖信息
        for task in self._tasks:
            self._dependency_info[task] = DependencyInfo(
                upstream=frozenset(self._edges.get(task, set())),
                downstream=frozenset(self._reverse_edges.get(task, set())),
                level=level_map.get(task, 0),
            )

    def get_execution_plan(self) -> ExecutionPlan:
        """获取执行计划

        Returns:
            拓扑排序后的执行计划
        """
        if self._execution_plan is not None:
            return self._execution_plan

        # 按层级分组
        level_tasks: Dict[int, Set[str]] = defaultdict(set)
        for task, info in self._dependency_info.items():
            level_tasks[info.level].add(task)

        # 构建执行层
        layers = []
        for level in sorted(level_tasks.keys()):
            layers.append(ExecutionLayer(
                level=level,
                tasks=tuple(sorted(level_tasks[level])),
            ))

        self._execution_plan = ExecutionPlan(
            layers=layers,
            critical_path=self._compute_critical_path(),
        )

        return self._execution_plan

    def _compute_critical_path(self) -> List[str]:
        """计算关键路径

        关键路径是 DAG 中最长的路径。
        """
        if not self._tasks:
            return []

        # 动态规划求最长路径
        max_length = {task: 0 for task in self._tasks}
        predecessors = {task: None for task in self._tasks}

        # 按拓扑顺序处理
        for task in self.get_topological_order():
            for upstream in self._edges.get(task, []):
                if max_length[upstream] + 1 > max_length[task]:
                    max_length[task] = max_length[upstream] + 1
                    predecessors[task] = upstream

        # 找到终点 (最长路径的末端)
        end_task = max(self._tasks, key=lambda t: max_length[t])

        # 回溯构建路径
        path = []
        current = end_task
        while current is not None:
            path.append(current)
            current = predecessors[current]

        return list(reversed(path))

    def get_topological_order(self) -> List[str]:
        """获取拓扑排序 (串行执行顺序)"""
        # 稳定化：同层级按任务名排序，保证顺序可复现
        return sorted(self._tasks, key=lambda t: (self._dependency_info[t].level, t))

    @property
    def tasks(self) -> Set[str]:
        """DAG 中包含的任务集合（只读视图）。"""
        return set(self._tasks)

    def get_dependencies(self, task: str) -> FrozenSet[str]:
        """获取任务的直接依赖"""
        info = self._dependency_info.get(task)
        return info.upstream if info else frozenset()

    def get_dependents(self, task: str) -> FrozenSet[str]:
        """获取依赖此任务的任务列表"""
        info = self._dependency_info.get(task)
        return info.downstream if info else frozenset()

    def get_all_upstream(self, task: str) -> Set[str]:
        """获取所有上游任务 (传递闭包)"""
        result = set()
        stack = list(self._edges.get(task, []))

        while stack:
            upstream = stack.pop()
            if upstream not in result:
                result.add(upstream)
                stack.extend(self._edges.get(upstream, []))

        return result

    def get_all_downstream(self, task: str) -> Set[str]:
        """获取所有下游任务 (传递闭包)"""
        result = set()
        stack = list(self._reverse_edges.get(task, []))

        while stack:
            downstream = stack.pop()
            if downstream not in result:
                result.add(downstream)
                stack.extend(self._reverse_edges.get(downstream, []))

        return result

    def subgraph(self, tasks: Set[str]) -> 'DAG':
        """提取子图

        Args:
            tasks: 要包含的任务集合

        Returns:
            只包含指定任务的子图
        """
        sub = DAG()
        sub._tasks = tasks.copy()

        for task in tasks:
            # 只保留指向子图内任务的边
            sub._edges[task] = {t for t in self._edges.get(task, []) if t in tasks}
            sub._reverse_edges[task] = {t for t in self._reverse_edges.get(task, []) if t in tasks}

        sub._compute_levels()
        return sub

    def filter_by_selection(
        self,
        only: Optional[Set[str]] = None,
        exclude: Optional[Set[str]] = None,
        include_upstream: bool = True,
        include_downstream: bool = False,
    ) -> 'DAG':
        """根据选择条件过滤 DAG

        用于 --only 和 --exclude 命令行参数。

        Args:
            only: 只执行这些任务
            exclude: 排除这些任务
            include_upstream: 是否包含上游任务
            include_downstream: 是否包含下游任务

        Returns:
            过滤后的 DAG
        """
        selected = set(self._tasks)

        if only:
            selected = only.copy()
            if include_upstream:
                for task in only:
                    selected.update(self.get_all_upstream(task))
            if include_downstream:
                for task in only:
                    selected.update(self.get_all_downstream(task))

        if exclude:
            selected -= exclude
            # 移除依赖被排除任务的任务
            to_remove = set()
            for task in selected:
                if self._edges.get(task, set()) & exclude:
                    to_remove.add(task)
            selected -= to_remove

        return self.subgraph(selected)

    def visualize_ascii(self) -> str:
        """生成 ASCII 依赖图"""
        lines = ["DAG Visualization:", "=" * 40]

        plan = self.get_execution_plan()
        for layer in plan:
            lines.append(f"\nLayer {layer.level}:")
            for task in sorted(layer.tasks):
                deps = self.get_dependencies(task)
                if deps:
                    dep_str = ", ".join(sorted(deps))
                    lines.append(f"  └── {task} (deps: {dep_str})")
                else:
                    lines.append(f"  └── {task} (root)")

        return "\n".join(lines)

    def to_mermaid(self, direction: str = "TB") -> str:
        """生成 Mermaid 流程图

        Args:
            direction: 图方向 (TB=上到下, LR=左到右, BT=下到上, RL=右到左)

        Returns:
            Mermaid 流程图语法字符串

        Examples:
            # 生成并打印
            print(dag.to_mermaid())

            # 保存到文件
            with open('dag.md', 'w') as f:
                f.write(f'```mermaid\\n{dag.to_mermaid()}\\n```')
        """
        lines = [f"flowchart {direction}"]

        # 定义节点样式
        plan = self.get_execution_plan()
        for layer in plan:
            for task in sorted(layer.tasks):
                # 根节点用圆角矩形，其他用矩形
                deps = self.get_dependencies(task)
                if not deps:
                    lines.append(f"    {task}([{task}])")
                else:
                    lines.append(f"    {task}[{task}]")

        lines.append("")  # 空行分隔

        # 定义边
        for task in sorted(self._tasks):
            for upstream in sorted(self._edges.get(task, [])):
                lines.append(f"    {upstream} --> {task}")

        # 添加层级子图 (可选，让图更清晰)
        lines.append("")
        for layer in plan:
            if len(layer.tasks) > 1:
                task_list = " & ".join(sorted(layer.tasks))
                lines.append(f"    %% Layer {layer.level}: {task_list}")

        return "\n".join(lines)

    def to_graphviz(self, rankdir: str = "TB") -> str:
        """生成 Graphviz DOT 格式

        Args:
            rankdir: 排列方向 (TB, LR, BT, RL)

        Returns:
            DOT 格式字符串

        Examples:
            # 使用 graphviz 渲染
            import graphviz
            dot = graphviz.Source(dag.to_graphviz())
            dot.render('dag', format='png')
        """
        lines = [
            "digraph DAG {",
            f"    rankdir={rankdir};",
            "    node [shape=box, style=filled, fillcolor=lightblue];",
            "",
        ]

        # 节点定义
        plan = self.get_execution_plan()
        critical_path = set(plan.critical_path) if plan.critical_path else set()

        for task in sorted(self._tasks):
            color = "lightcoral" if task in critical_path else "lightblue"
            deps = self.get_dependencies(task)
            shape = "ellipse" if not deps else "box"
            lines.append(f'    "{task}" [shape={shape}, fillcolor={color}];')

        lines.append("")

        # 边定义
        for task in sorted(self._tasks):
            for upstream in sorted(self._edges.get(task, [])):
                lines.append(f'    "{upstream}" -> "{task}";')

        # 同层节点放在同一 rank
        lines.append("")
        for layer in plan:
            if len(layer.tasks) > 1:
                task_list = "; ".join(f'"{t}"' for t in sorted(layer.tasks))
                lines.append(f"    {{ rank=same; {task_list} }}")

        lines.append("}")
        return "\n".join(lines)

    def to_dict(self) -> Dict:
        """转换为字典 (便于 JSON 序列化)"""
        return {
            'tasks': list(self._tasks),
            'edges': {task: list(deps) for task, deps in self._edges.items()},
            'execution_plan': {
                'layers': [
                    {'level': layer.level, 'tasks': list(layer.tasks)}
                    for layer in self.get_execution_plan().layers
                ],
                'critical_path': self.get_execution_plan().critical_path,
            },
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'DAG':
        """从字典恢复 DAG (JSON 反序列化)

        Args:
            data: 由 to_dict() 生成的字典

        Returns:
            恢复的 DAG 对象
        """
        dag = cls()
        dag._tasks = set(data.get('tasks', []))

        for task, deps in data.get('edges', {}).items():
            for dep in deps:
                dag._edges[task].add(dep)
                dag._reverse_edges[dep].add(task)

        dag._compute_levels()
        return dag

    def get_execution_subplan(
        self,
        start_from: Optional[str] = None,
        stop_at: Optional[str] = None,
    ) -> ExecutionPlan:
        """获取部分执行计划

        用于断点续跑或部分执行场景。

        Args:
            start_from: 从此任务开始 (包含此任务及其所有下游)
            stop_at: 在此任务结束 (包含此任务及其所有上游)

        Returns:
            过滤后的执行计划

        Examples:
            # 从某任务开始执行到结束
            plan = dag.get_execution_subplan(start_from="transform")

            # 执行到某任务为止
            plan = dag.get_execution_subplan(stop_at="validate")

            # 执行两个任务之间的部分
            plan = dag.get_execution_subplan(start_from="extract", stop_at="load")
        """
        selected = set(self._tasks)

        if start_from:
            if start_from not in self._tasks:
                raise ValueError(f"Task '{start_from}' not found in DAG")
            # 包含 start_from 及其所有下游
            selected = {start_from}
            selected.update(self.get_all_downstream(start_from))

        if stop_at:
            if stop_at not in self._tasks:
                raise ValueError(f"Task '{stop_at}' not found in DAG")
            # 取交集: stop_at 及其所有上游
            stop_tasks = {stop_at}
            stop_tasks.update(self.get_all_upstream(stop_at))
            selected = selected & stop_tasks

        if not selected:
            return ExecutionPlan(layers=[], critical_path=[])

        return self.subgraph(selected).get_execution_plan()

    def validate(self) -> List[str]:
        """验证 DAG 完整性

        Returns:
            错误消息列表 (空列表表示验证通过)
        """
        errors = []

        # 检查空 DAG
        if not self._tasks:
            errors.append("DAG is empty")
            return errors

        # 检查孤立节点 (无上游也无下游)
        for task in self._tasks:
            upstream = self._edges.get(task, set())
            downstream = self._reverse_edges.get(task, set())
            if not upstream and not downstream and len(self._tasks) > 1:
                errors.append(f"Task '{task}' is isolated (no dependencies)")

        # 检查引用完整性
        for task in self._tasks:
            for dep in self._edges.get(task, []):
                if dep not in self._tasks:
                    errors.append(f"Task '{task}' depends on non-existent task '{dep}'")

        # 检查循环 (应该在构建时已检测，这里是额外保险)
        cycle = self._detect_cycle()
        if cycle:
            cycle_str = ' -> '.join(cycle + [cycle[0]])
            errors.append(f"Cyclic dependency detected: {cycle_str}")

        return errors

    def get_stats(self) -> Dict[str, Any]:
        """获取 DAG 统计信息"""
        plan = self.get_execution_plan()

        return {
            'total_tasks': len(self._tasks),
            'total_edges': sum(len(deps) for deps in self._edges.values()),
            'total_layers': plan.total_layers,
            'max_parallelism': max((len(layer) for layer in plan.layers), default=0),
            'root_tasks': [t for t in self._tasks if not self._edges.get(t)],
            'leaf_tasks': [t for t in self._tasks if not self._reverse_edges.get(t)],
            'critical_path_length': len(plan.critical_path),
            'critical_path': plan.critical_path,
        }
