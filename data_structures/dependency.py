# Dependency类定义 - 存储节点间依赖关系
from dataclasses import dataclass

@dataclass
class Dependency:
    """节点间依赖关系"""
    source_node_id: str
    target_node_id: str
    reason: str  # 依赖原因描述