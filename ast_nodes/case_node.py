# CaseNode类定义 - CASE表达式节点
from typing import Set, Tuple, List, Optional
from .ast_node import ASTNode
from data_structures.node_type import NodeType

class CaseNode(ASTNode):
    """CASE表达式节点"""

    def __init__(self):
        super().__init__(NodeType.CASE)
        self.when_clauses: List[Tuple[ASTNode, ASTNode]] = []  # (condition, result)
        self.else_clause: Optional[ASTNode] = None
        self.metadata = {
            'is_aggregate': False  # 默认不是聚合，会根据子节点更新
        }

    def add_when_clause(self, condition: ASTNode, result: ASTNode) -> None:
        # 检查条件是否重复，避免逻辑冗余
        condition_sql = condition.to_sql()
        for existing_condition, _ in self.when_clauses:
            if existing_condition.to_sql() == condition_sql:
                return  # 跳过重复条件
        self.when_clauses.append((condition, result))
        self.add_child(condition)
        self.add_child(result)

        # 更新聚合标记
        self._update_aggregate_status()

    def set_else_clause(self, else_node: ASTNode) -> None:
        self.else_clause = else_node
        if else_node:
            self.add_child(else_node)
            self._update_aggregate_status()

    def _update_aggregate_status(self) -> None:
        """更新CASE表达式是否包含聚合函数的状态"""
        for _, result in self.when_clauses:
            if result.metadata.get('is_aggregate', False):
                self.metadata['is_aggregate'] = True
                return
        if self.else_clause and self.else_clause.metadata.get('is_aggregate', False):
            self.metadata['is_aggregate'] = True
            return
        self.metadata['is_aggregate'] = False

    def to_sql(self) -> str:
        parts = ["CASE"]

        for condition, result in self.when_clauses:
            parts.append(f"WHEN {condition.to_sql()} THEN {result.to_sql()}")

        if self.else_clause:
            parts.append(f"ELSE {self.else_clause.to_sql()}")

        parts.append("END")
        return " ".join(parts)

    def collect_table_aliases(self) -> Set[str]:
        """收集CASE表达式中所有引用的表别名"""
        aliases = set()
        for condition, result in self.when_clauses:
            aliases.update(condition.collect_table_aliases())
            aliases.update(result.collect_table_aliases())
        if self.else_clause:
            aliases.update(self.else_clause.collect_table_aliases())
        return aliases

    def collect_column_aliases(self) -> Set[str]:
        """收集CASE表达式中引用的列别名"""
        aliases = set()
        for condition, result in self.when_clauses:
            aliases.update(condition.collect_column_aliases())
            aliases.update(result.collect_column_aliases())
        if self.else_clause:
            aliases.update(self.else_clause.collect_column_aliases())
        return aliases

    def validate_columns(self, from_node: 'FromNode') -> Tuple[bool, List[str]]:
        """验证CASE表达式中的列引用是否有效"""
        errors = []
        for condition, result in self.when_clauses:
            if hasattr(condition, 'validate_columns'):
                valid, cond_errors = condition.validate_columns(from_node)
                if not valid:
                    errors.extend(cond_errors)
            if hasattr(result, 'validate_columns'):
                valid, res_errors = result.validate_columns(from_node)
                if not valid:
                    errors.extend(res_errors)
        if self.else_clause and hasattr(self.else_clause, 'validate_columns'):
            valid, else_errors = self.else_clause.validate_columns(from_node)
            if not valid:
                errors.extend(else_errors)
        return (len(errors) == 0, errors)

    def repair_columns(self, from_node: 'FromNode') -> None:
        """修复CASE表达式中的无效列引用"""
        for i, (condition, result) in enumerate(self.when_clauses):
            if hasattr(condition, 'repair_columns'):
                condition.repair_columns(from_node)
            if hasattr(result, 'repair_columns'):
                result.repair_columns(from_node)
        if self.else_clause and hasattr(self.else_clause, 'repair_columns'):
            self.else_clause.repair_columns(from_node)