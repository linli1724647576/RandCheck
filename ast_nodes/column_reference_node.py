# ColumnReferenceNode类定义 - 列引用节点
from typing import Set, Optional
import random
from .ast_node import ASTNode
from data_structures.node_type import NodeType
from data_structures.column import Column
from data_structures.table import Table

class ColumnReferenceNode(ASTNode):
    """列引用节点"""

    def __init__(self, column: Column, table_alias: str):
        super().__init__(NodeType.COLUMN_REFERENCE)
        self.column = column
        self.table_alias = table_alias
        self.metadata = {
            'column_name': column.name,
            'table_name': column.table_name,
            'table_alias': table_alias,
            'data_type': column.data_type,
            'category': column.category,
            'is_aggregate': False  # 列引用不是聚合
        }

    def to_sql(self) -> str:
        return f"{self.table_alias}.{self.column.name}"

    def collect_table_aliases(self) -> Set[str]:
        """返回此列引用使用的表别名"""
        return {self.table_alias}

    def collect_column_aliases(self) -> Set[str]:
        """返回此列引用使用的列名（可能是别名）"""
        return {self.column.name}

    def is_valid(self, from_node: 'FromNode') -> bool:
        """检查列引用是否有效"""
        table_ref = from_node.get_table_for_alias(self.table_alias)
        if not table_ref:
            return False  # 表别名无效

        if isinstance(table_ref, Table):
            # 检查表是否包含该列
            return table_ref.has_column(self.column.name)
        else:
            # 在方法内部局部导入，避免循环导入
            from .subquery_node import SubqueryNode
            if isinstance(table_ref, SubqueryNode):
                # 检查子查询是否包含该列别名
                return table_ref.has_column_alias(self.column.name)
        return False

    def find_replacement(self, from_node: 'FromNode') -> Optional['ColumnReferenceNode']:
        """寻找有效的替代列引用，支持子查询列别名"""
        table_ref = from_node.get_table_for_alias(self.table_alias)
        if not table_ref:
            # 表别名无效，尝试找到有效的表别名替换
            valid_aliases = list(from_node.get_all_aliases())
            if not valid_aliases:
                return None
            new_alias = random.choice(valid_aliases)
            table_ref = from_node.get_table_for_alias(new_alias)
            if not table_ref:
                return None
            self.table_alias = new_alias

        if isinstance(table_ref, Table):
            # 从表中找相似列
            similar_cols = table_ref.get_similar_columns(self.column.name)
            if similar_cols:
                return ColumnReferenceNode(random.choice(similar_cols), self.table_alias)
            return ColumnReferenceNode(table_ref.get_random_column(), self.table_alias)

        else:
            # 在方法内部局部导入，避免循环导入
            from .subquery_node import SubqueryNode
            if isinstance(table_ref, SubqueryNode):
                # 修复：只使用子查询中实际定义的别名
                if table_ref.column_alias_map:
                    # 获取所有可用的列别名
                    valid_aliases = list(table_ref.column_alias_map.keys())
                    if valid_aliases:
                        alias = random.choice(valid_aliases)
                        col_name, data_type, category = table_ref.column_alias_map[alias]
                        virtual_col = Column(
                            name=alias,
                            data_type=data_type,
                            category=category,
                            is_nullable=False,
                            table_name=table_ref.alias
                        )
                        return ColumnReferenceNode(virtual_col, self.table_alias)

                # 如果没有别名或别名无效，使用默认列
                virtual_col = Column(
                    name="id",
                    data_type="INT",
                    category="numeric",
                    is_nullable=False,
                    table_name=table_ref.alias
                )
                return ColumnReferenceNode(virtual_col, self.table_alias)

        return None