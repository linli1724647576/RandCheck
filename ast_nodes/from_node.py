# FromNode类定义 - FROM子句节点
import random
from typing import List, Union, Dict, Set, Optional, Tuple
from .ast_node import ASTNode
from data_structures.node_type import NodeType
from data_structures.table import Table
from data_structures.db_dialect import get_current_dialect
from .subquery_node import SubqueryNode
from .comparison_node import ComparisonNode
from .column_reference_node import ColumnReferenceNode


class FromNode(ASTNode):
    """FROM子句节点，处理表引用和连接"""

    def __init__(self):
        super().__init__(NodeType.FROM)
        # 保留原有列表用于向后兼容
        self.table_references: List[Union[Table, SubqueryNode]] = []
        self.aliases: List[str] = []
        self.joins: List[Dict] = []  # 连接信息: {type, table, alias, condition}
        
        # 新增字典用于更可靠的表-别名映射管理
        self.alias_to_table: Dict[str, Union[Table, SubqueryNode]] = {}
        self.table_to_alias: Dict[int, str] = {}  # 使用id作为键，避免对象比较问题

    def add_table(self, table: Union[Table, SubqueryNode], alias: str) -> None:
        """添加表或子查询引用，同时更新所有映射结构"""
        self.table_references.append(table)
        self.aliases.append(alias)
        
        # 更新新的字典映射
        self.alias_to_table[alias] = table
        self.table_to_alias[id(table)] = alias

    def add_join(self, join_type: str, table: Union[Table, SubqueryNode], alias: str, condition: ASTNode) -> None:
        """添加连接，同时更新所有映射结构"""
        self.joins.append({
            'type': join_type,
            'table': table,
            'alias': alias,
            'condition': condition
        })
        self.table_references.append(table)
        self.aliases.append(alias)
        self.add_child(condition)
        
        # 更新新的字典映射
        self.alias_to_table[alias] = table
        self.table_to_alias[id(table)] = alias

    def get_table_for_alias(self, alias: str) -> Optional[Union[Table, SubqueryNode]]:
        """根据别名获取表或子查询，优先使用字典映射"""
        # 优先从新的字典映射中获取
        if alias in self.alias_to_table:
            return self.alias_to_table[alias]
        
        # 保留原有逻辑作为后备方案
        try:
            index = self.aliases.index(alias)
            return self.table_references[index]
        except ValueError:
            return None

    def get_alias_for_table(self, table_or_name: Union[Table, SubqueryNode, str]) -> Optional[str]:
        """根据表对象或表名获取别名，支持更多输入类型并优先使用字典映射"""
        # 如果输入是表对象，优先使用新的字典映射
        if isinstance(table_or_name, (Table, SubqueryNode)):
            table_id = id(table_or_name)
            if table_id in self.table_to_alias:
                return self.table_to_alias[table_id]
        
        # 如果输入是字符串（表名或子查询别名）
        elif isinstance(table_or_name, str):
            # 遍历字典映射查找匹配
            for alias, table in self.alias_to_table.items():
                if isinstance(table, Table) and table.name == table_or_name:
                    return alias
                elif isinstance(table, SubqueryNode) and table.alias == table_or_name:
                    return alias
        
        # 保留原有逻辑作为后备方案
        table_name = table_or_name if isinstance(table_or_name, str) else None
        if table_name:
            for i, ref in enumerate(self.table_references):
                if isinstance(ref, Table) and ref.name == table_name:
                    return self.aliases[i]
                elif isinstance(ref, SubqueryNode) and ref.alias == table_name:
                    return self.aliases[i]
        
        return None

    def get_all_aliases(self) -> Set[str]:
        """获取所有表别名"""
        return set(self.aliases)

    def get_table_alias_map(self) -> Dict[str, str]:
        """获取表名到别名的映射，利用新的字典映射实现"""
        mapping = {}
        
        # 优先使用新的字典映射构建结果
        for alias, table in self.alias_to_table.items():
            if isinstance(table, Table):
                mapping[table.name] = alias
            elif isinstance(table, SubqueryNode):
                mapping[table.alias] = alias
        
        # 如果新映射中没有数据，使用原有逻辑作为后备
        if not mapping:
            for i, ref in enumerate(self.table_references):
                if isinstance(ref, Table):
                    mapping[ref.name] = self.aliases[i]
                elif isinstance(ref, SubqueryNode):
                    mapping[ref.alias] = self.aliases[i]
        
        return mapping

    def to_sql(self) -> str:
        if not self.table_references:
            return ""

        # 主表
        parts = []
        first_table = self.table_references[0]
        first_alias = self.aliases[0]

        if isinstance(first_table, Table):
            table_sql = f"{first_table.name} AS {first_alias}"
            # 添加索引提示
            table_sql_with_hint = self._add_index_hint(table_sql, first_table)
            parts.append(table_sql_with_hint)
        else:
            parts.append(first_table.to_sql())

        # 连接部分
        for join in self.joins:
            join_type = join['type'].upper()
            table = join['table']
            alias = join['alias']
            condition = join.get('condition')
            
            # 支持USING子句
            use_using = False
            if random.random() < 0.2 and isinstance(condition, ComparisonNode) and condition.operator == '=':
                # 检查是否可以转换为USING子句
                if len(condition.children) == 2 and all(isinstance(child, ColumnReferenceNode) for child in condition.children):
                    left_col = condition.children[0]
                    right_col = condition.children[1]
                    # 只有当列名相同且属于不同表时，才使用USING子句
                    if hasattr(left_col, 'column') and hasattr(right_col, 'column') and \
                       left_col.column.name == right_col.column.name and \
                       left_col.table_alias != right_col.table_alias:
                        use_using = True
                        using_col = left_col.column.name
            
            if isinstance(table, Table):
                table_sql = f"{table.name} AS {alias}"
                # 添加索引提示
                table_sql = self._add_index_hint(table_sql, table)
            else:
                table_sql = table.to_sql()
            
            if use_using:
                # 使用USING子句
                parts.append(f"{join_type} JOIN {table_sql} USING ({using_col})")
            elif condition:
                # 标准ON条件
                condition_sql = condition.to_sql()
                
                # 检查当前方言是否支持JOIN条件中的子查询
                dialect = get_current_dialect()
                if hasattr(dialect, 'supports_subqueries_in_join') and not dialect.supports_subqueries_in_join():
                    # 如果方言不支持JOIN中的子查询，避免生成包含子查询的ON条件
                    # 简单检查：如果条件SQL中包含SELECT语句（可能是子查询），则使用简单的条件
                    if 'SELECT' in condition_sql.upper() or '(' in condition_sql and ')' in condition_sql:
                        # 使用一个简单的条件替代，例如1=1或表的主键=主键
                        # 这里我们使用1=1作为替代
                        parts.append(f"{join_type} JOIN {table_sql} ON 1=1")
                    else:
                        parts.append(f"{join_type} JOIN {table_sql} ON {condition_sql}")
                else:
                    # 方言支持JOIN中的子查询，正常生成
                    parts.append(f"{join_type} JOIN {table_sql} ON {condition_sql}")
            else:
                # 没有条件（如CROSS JOIN）
                parts.append(f"{join_type} JOIN {table_sql}")

        return " ".join(parts)
        
    def add_outer_join(self, table: Union[Table, SubqueryNode], alias: str, condition: ASTNode) -> None:
        """添加外连接（LEFT/RIGHT/FULL）"""
        outer_join_type = random.choice(['LEFT OUTER', 'RIGHT OUTER', 'FULL OUTER'])
        self.add_join(outer_join_type, table, alias, condition)
        
    def add_cross_join(self, table: Union[Table, SubqueryNode], alias: str) -> None:
        """添加交叉连接"""
        self.joins.append({
            'type': 'CROSS',
            'table': table,
            'alias': alias,
            'condition': None
        })
        self.table_references.append(table)
        self.aliases.append(alias)

    def validate_table_references(self) -> Tuple[bool, List[str]]:
        """验证表引用是否有效"""
        errors = []
        # 这里可以添加表引用验证逻辑
        return (len(errors) == 0, errors)

    def _add_index_hint(self, table_sql: str, table: Table) -> str:
        """为表添加索引提示
        
        Args:
            table_sql: 表的SQL表示
            table: 表对象
            
        Returns:
            添加索引提示后的SQL
        """
        # 根据需求，不再添加任何索引提示
        return table_sql
    
    def repair_table_references(self) -> None:
        """修复无效的表引用"""
        # 这里可以添加表引用修复逻辑
        pass

    def get_defined_aliases(self) -> Set[str]:
        """获取所有定义的别名"""
        aliases = set(self.aliases)
        # 收集子查询中的别名
        for ref in self.table_references:
            if isinstance(ref, SubqueryNode):
                aliases.update(ref.get_defined_aliases())
        return aliases