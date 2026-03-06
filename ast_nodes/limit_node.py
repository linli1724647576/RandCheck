from typing import Set
from .ast_node import ASTNode
from data_structures.node_type import NodeType

class LimitNode(ASTNode):
    """LIMIT子句节点"""

    def __init__(self, value: int):
        super().__init__(NodeType.LIMIT)
        self.value = value
        self.metadata = {'value': value}

    def to_sql(self) -> str:
        return str(self.value)
    
    def collect_table_aliases(self) -> Set[str]:
        """收集节点中引用的所有表别名"""
        aliases = set()
        # 递归收集所有子节点的表别名引用
        for child in self.children:
            aliases.update(child.collect_table_aliases())
        return aliases