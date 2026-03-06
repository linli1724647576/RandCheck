# OrderByNode类定义 - ORDER BY子句节点
from typing import List, Tuple
from .ast_node import ASTNode
from data_structures.node_type import NodeType

class OrderByNode(ASTNode):
    """ORDER BY子句节点"""

    def __init__(self):
        super().__init__(NodeType.ORDER_BY)
        self.expressions: List[Tuple[ASTNode, str]] = []  # (expression, direction)

    def add_expression(self, expr: ASTNode, direction: str = 'ASC') -> None:
        self.expressions.append((expr, direction))
        self.add_child(expr)

    def to_sql(self) -> str:
        if not self.expressions:
            return ""
        parts = []
        for expr, direction in self.expressions:
            parts.append(f"{expr.to_sql()} {direction}")
        return ", ".join(parts)
    # 收集ORDER BY子句中使用的表别名
    def collect_table_aliases(self) -> set:
        aliases = set()
        for expr, _ in self.expressions:
            aliases.update(expr.collect_table_aliases())
        return aliases