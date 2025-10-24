"""I/O 抽象层

集中管理节点输入/输出解析、命名策略、映射与后续可插拔扩展。
"""

from .io_manager import IOManager, NodeIOConfig, InputSpec, OutputSpec

__all__ = [
    'IOManager', 'NodeIOConfig', 'InputSpec', 'OutputSpec'
]
