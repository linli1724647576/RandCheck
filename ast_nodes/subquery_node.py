# SubqueryNode类定义 - 子查询节点
from typing import Set, Optional, Dict, Tuple, List
from .ast_node import ASTNode
from data_structures.node_type import NodeType
from .column_reference_node import ColumnReferenceNode
from .function_call_node import FunctionCallNode
from .case_node import CaseNode
from .arithmetic_node import ArithmeticNode

class SubqueryNode(ASTNode):
    """子查询节点 - 增强版，管理内部列别名映射"""

    def __init__(self, select_node: 'SelectNode', alias: str):
        super().__init__(NodeType.SUBQUERY)
        self.select_node = select_node
        self.alias = alias
        self.add_child(select_node)
        self.metadata = {
            'alias': alias,
            'is_aggregate': False  # 子查询本身不是聚合
        }

        # 创建子查询列别名映射：alias -> (column_name, data_type, category)
        self.column_alias_map = self._build_column_alias_map(select_node)

    def _build_column_alias_map(self, select_node: 'SelectNode') -> Dict[str, Tuple[str, str, str]]:
        """构建完整的子查询列别名映射，确保所有表达式都有别名"""
        alias_map = {}
        for i, (expr, expr_alias) in enumerate(select_node.select_expressions):
            # 为没有显式别名的表达式生成默认别名
            alias = expr_alias if expr_alias else f"col_{i + 1}"

            if isinstance(expr, ColumnReferenceNode):
                alias_map[alias] = (
                    expr.column.name,
                    expr.column.data_type,
                    expr.column.category
                )
            elif isinstance(expr, FunctionCallNode):
                alias_map[alias] = (
                    alias,  # 函数结果没有原始列名
                    expr.metadata.get('return_type', 'unknown'),
                    self._get_category_from_type(expr.metadata.get('return_type', 'unknown'))
                )
            elif isinstance(expr, CaseNode):
                # 尝试从CASE结果推断类型
                result_type = 'unknown'
                result_category = 'any'
                if expr.when_clauses:
                    first_result = expr.when_clauses[0][1]
                    result_type = first_result.metadata.get('data_type', 'unknown')
                    result_category = first_result.metadata.get('category', 'any')
                alias_map[alias] = (alias, result_type, result_category)
            elif isinstance(expr, ArithmeticNode):
                # 算术运算结果通常为数值型
                alias_map[alias] = (alias, 'numeric', 'numeric')
            else:
                alias_map[alias] = (
                    alias,
                    expr.metadata.get('data_type', 'unknown'),
                    expr.metadata.get('category', 'any')
                )
        return alias_map

    def _get_category_from_type(self, data_type: str) -> str:
        """从数据类型推断类别"""
        data_type = data_type.lower()
        if data_type in ['int', 'integer', 'bigint', 'smallint', 'tinyint', 'float', 'double', 'decimal', 'numeric', 'real']:
            return 'numeric'
        elif data_type in ['varchar', 'string', 'char', 'text', 'longtext', 'mediumtext', 'tinytext']:
            return 'string'
        elif data_type in ['date', 'datetime', 'timestamp', 'time']:
            return 'datetime'
        elif data_type in ['json']:
            return 'json'
        elif data_type in [
            'binary', 'varbinary', 'blob', 'longblob', 'mediumblob', 'tinyblob',
            'geometry', 'point', 'linestring', 'polygon', 'multipoint',
            'multilinestring', 'multipolygon', 'geometrycollection'
        ]:
            return 'binary'
        elif data_type in ['boolean', 'bool']:
            return 'boolean'
        return 'any'

    def to_sql(self) -> str:
        # 只有当alias不为空时才添加AS子句
        if self.alias:
            return f"({self.select_node.to_sql()}) AS {self.alias}"
        else:
            return f"({self.select_node.to_sql()})"

    def get_defined_aliases(self) -> Set[str]:
        """获取此子查询中定义的所有表别名（包括内部子查询）"""
        return {self.alias} | self.select_node.get_defined_aliases()

    def has_column_alias(self, alias: str) -> bool:
        """检查子查询是否包含指定的列别名"""
        return alias in self.column_alias_map

    def get_column_alias_info(self, alias: str) -> Optional[Tuple[str, str, str]]:
        """获取列别名的信息：(原始列名, 数据类型, 类别)"""
        return self.column_alias_map.get(alias)

    def validate_inner_columns(self) -> Tuple[bool, List[str]]:
        """验证子查询内部的列引用是否有效"""
        return self.select_node.validate_all_columns()
    
    def collect_table_aliases(self) -> Set[str]:
        """收集子查询中引用的所有表别名"""
        return self.select_node.collect_table_aliases()
        
    def columns(self):
        """提供columns方法，避免AttributeError错误"""
        return self.column_alias_map.values()
    
    def repair_columns(self, from_node: 'FromNode') -> None:
        """修复子查询内部的列引用
        注意：这个方法不使用传入的from_node参数，而是使用子查询自身的FROM子句
        以避免外部修改错误地影响子查询内部列引用
        """
        # 完全隔离子查询的列引用修复过程
        # 确保只使用子查询自己的FROM子句，不使用外部查询的任何信息
        self.select_node.repair_invalid_columns()
