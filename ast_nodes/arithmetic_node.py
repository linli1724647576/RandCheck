# ArithmeticNode类定义 - 算术表达式节点
from typing import Set, Tuple, List
from .ast_node import ASTNode
from data_structures.node_type import NodeType

class ArithmeticNode(ASTNode):
    """算术表达式节点（添加除零保护）"""

    def __init__(self, operator: str):
        super().__init__(NodeType.ARITHMETIC)
        self.operator = operator
        self.metadata = {
            'operator': operator,
            'is_aggregate': False  # 算术表达式不是聚合
        }

    def to_sql(self) -> str:
        if len(self.children) != 2:
            return ""

        left = self.children[0].to_sql()
        right = self.children[1].to_sql()

        # 除法和取模运算添加除零保护
        if self.operator in ['/', '%']:
            return f"({left} {self.operator} NULLIF({right}, 0))"
        return f"({left} {self.operator} {right})"

    def collect_column_aliases(self) -> Set[str]:
        """收集算术表达式中引用的列别名"""
        aliases = set()
        for child in self.children:
            aliases.update(child.collect_column_aliases())
        return aliases

    def validate_columns(self, from_node: 'FromNode') -> Tuple[bool, List[str]]:
        """验证算术表达式中的列引用是否有效"""
        errors = []
        for child in self.children:
            if hasattr(child, 'validate_columns'):
                valid, child_errors = child.validate_columns(from_node)
                if not valid:
                    errors.extend(child_errors)
            elif isinstance(child, ColumnReferenceNode):
                if not child.is_valid(from_node):
                    errors.append(f"无效的列引用: {child.to_sql()}")
        return (len(errors) == 0, errors)

    def repair_columns(self, from_node: 'FromNode') -> None:
        """修复算术表达式中的无效列引用"""
        for i, child in enumerate(self.children):
            if hasattr(child, 'repair_columns'):
                child.repair_columns(from_node)
            elif isinstance(child, ColumnReferenceNode) and not child.is_valid(from_node):
                replacement = child.find_replacement(from_node)
                if replacement:
                    self.children[i] = replacement