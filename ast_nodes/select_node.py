from typing import List, Tuple, Dict, Optional, Set
import random
from .ast_node import ASTNode
from .from_node import FromNode
from .group_by_node import GroupByNode
from .order_by_node import OrderByNode
from .limit_node import LimitNode
from .function_call_node import FunctionCallNode
from .column_reference_node import ColumnReferenceNode
from .comparison_node import ComparisonNode
from .literal_node import LiteralNode
from .subquery_node import SubqueryNode
from data_structures.db_dialect import get_current_dialect
from data_structures.node_type import NodeType
from data_structures.table import Table
from data_structures.function import Function
# 导入ColumnUsageTracker和get_random_column_with_tracker用于跟踪列使用情况
from data_structures.column_usage_tracker import ColumnUsageTracker, get_random_column_with_tracker

class SelectNode(ASTNode):
    """SELECT语句节点"""

    def __init__(self):
        super().__init__(NodeType.SELECT)
        self.distinct = False
        self.select_expressions: List[Tuple[ASTNode, str]] = []  # (expression, alias)
        self.from_clause: Optional[FromNode] = None
        self.where_clause: Optional[ASTNode] = None
        self.group_by_clause: Optional[GroupByNode] = None
        self.having_clause: Optional[ASTNode] = None
        self.order_by_clause: Optional[OrderByNode] = None
        self.limit_clause: Optional[LimitNode] = None
        self.for_update: Optional[str] = None  # 支持多种锁定模式: 'update', 'share', 'no key update', 'key share'
        self.tables: List[Table] = []  # 关联的表信息
        self.functions: List[Function] = []  # 可用函数
        self.column_tracker: Optional[ColumnUsageTracker] = None  # 列使用跟踪器

    def add_select_expression(self, expr_node: ASTNode, alias: str = "") -> None:
        # 检查别名是否重复
        used_aliases = self.get_select_column_aliases()
        base_alias = alias if alias else f"col_{len(self.select_expressions) + 1}"
        final_alias = base_alias
        #print(f"添加SELECT表达式: {expr_node.to_sql()} 别名: {final_alias}")
        count = 1
        
        # 处理重复别名
        while final_alias in used_aliases:
            final_alias = f"{base_alias}_{count}"
            count += 1
        #print(f"最终SELECT表达式别名: {final_alias}")
        self.select_expressions.append((expr_node, final_alias))
        self.add_child(expr_node)

    def get_expression_alias_map(self) -> Dict[str, ASTNode]:
        """创建表达式到别名的映射"""
        return {alias: expr for expr, alias in self.select_expressions if alias}

    def get_alias_for_expression(self, expr_node: ASTNode) -> Optional[str]:
        """获取表达式对应的别名"""
        for expr, alias in self.select_expressions:
            if expr == expr_node:
                return alias
        return None

    def get_select_column_aliases(self) -> Set[str]:
        """获取SELECT子句中定义的所有列别名"""
        aliases = set()
        for i, (_, alias) in enumerate(self.select_expressions):
            # 包含自动生成的默认别名
            aliases.add(alias if alias else f"col_{i + 1}")
        return aliases

    def set_from_clause(self, from_node: FromNode) -> None:
        self.from_clause = from_node
        if from_node:
            self.add_child(from_node)
            # 验证FROM子句中的表引用
            if hasattr(from_node, 'validate_table_references'):
                valid, errors = from_node.validate_table_references()
                if not valid and hasattr(from_node, 'repair_table_references'):
                    from_node.repair_table_references()

    def set_where_clause(self, where_node: ASTNode) -> None:
        # 关键检查1：确保WHERE子句不包含窗口函数
        if where_node.contains_window_function():
            # 创建一个简单的WHERE条件替代
            table_alias_map = self.from_clause.get_table_alias_map() if (
                    self.from_clause and hasattr(self.from_clause, 'get_table_alias_map')) else {}
            if table_alias_map:
                table_name = random.choice(list(table_alias_map.keys())) if table_alias_map else None
                if table_name:
                    table = next((t for t in self.tables if t.name == table_name), None) if hasattr(self,
                                                                                                    'tables') else None
                    if table:
                        col_node = ColumnReferenceNode(random.choice(table.columns), table_alias_map[table_name])
                        where_node = ComparisonNode('IS NOT NULL')
                        where_node.add_child(col_node)

        # 关键检查2：确保WHERE子句不包含聚合函数（SQL语法规则）
        if where_node.contains_aggregate_function():
            # 创建一个简单的WHERE条件替代（不包含聚合函数）
            table_alias_map = self.from_clause.get_table_alias_map() if (
                    self.from_clause and hasattr(self.from_clause, 'get_table_alias_map')) else {}
            if table_alias_map:
                table_name = random.choice(list(table_alias_map.keys())) if table_alias_map else None
                if table_name:
                    table = next((t for t in self.tables if t.name == table_name), None) if hasattr(self,
                                                                                                    'tables') else None
                    if table:
                        col_node = ColumnReferenceNode(random.choice(table.columns), table_alias_map[table_name])
                        # 使用简单比较运算符和字面量
                        operator = random.choice(['=', '<>', '<', '>', '<=', '>=', 'IS NOT NULL'])
                        where_node = ComparisonNode(operator)
                        where_node.add_child(col_node)
                        
                        # 添加右侧操作数（如果需要）
                        if operator not in ['IS NULL', 'IS NOT NULL']:
                            if col_node.column.category == 'numeric':
                                where_node.add_child(LiteralNode(random.randint(0, 100), col_node.column.data_type))
                            elif col_node.column.category == 'string':
                                where_node.add_child(LiteralNode(f"sample_{random.randint(1, 100)}", 'STRING'))
                            elif col_node.column.category == 'datetime':
                                where_node.add_child(LiteralNode('2023-01-01', col_node.column.data_type))

        # 关键检查3：确保WHERE子句返回布尔类型
        # 已在文件顶部导入所有必要的类，无需局部导入
        
        # 如果WHERE子句是函数调用，检查返回类型是否为boolean
        if isinstance(where_node, FunctionCallNode):
            return_type = where_node.metadata.get('return_type', 'unknown')
            if return_type.lower() != 'boolean' and return_type.lower() != 'bool':
                # 返回类型不是boolean，需要包装成比较表达式
                operator = random.choice(['=', '<>', '!='])
                new_where_node = ComparisonNode(operator)
                new_where_node.add_child(where_node)
                
                # 根据返回类型创建适当的字面量
                # 理想情况下，应该使用全局表结构信息和Column对象的category属性来判断类型
                # 但由于此方法直接处理返回类型字符串，暂时保留基于关键词的判断
                if return_type.lower() in ['string', 'text', 'varchar', 'char']:
                    new_where_node.add_child(LiteralNode(f"sample_value", 'STRING'))
                elif return_type.lower() in ['numeric', 'int', 'integer', 'float', 'decimal']:
                    new_where_node.add_child(LiteralNode(random.randint(0, 100), 'INT'))
                elif return_type.lower() in ['date', 'datetime', 'timestamp']:
                    new_where_node.add_child(LiteralNode('2023-01-01', 'DATE'))
                else:
                    # 默认使用字符串类型
                    new_where_node.add_child(LiteralNode(f"sample_value", 'STRING'))
                
                where_node = new_where_node

        self.where_clause = where_node
        if where_node:
            self.add_child(where_node)

    def set_group_by_clause(self, group_by_node: GroupByNode) -> None:
        self.group_by_clause = group_by_node
        if group_by_node:
            self.add_child(group_by_node)

    def set_having_clause(self, having_node: ASTNode) -> None:
        # 关键检查：确保HAVING子句不包含窗口函数
        if having_node and having_node.contains_window_function():
            # 创建一个简单的HAVING条件替代（使用聚合函数）
            count_func = next(f for f in self.functions if f.name == "COUNT")
            func_node = FunctionCallNode(count_func)
            col_ref = self._get_random_column_reference()
            if col_ref:
                func_node.add_child(col_ref)
            else:
                # 如果无法获取列引用，使用COUNT(*)
                func_node.add_child(LiteralNode('*', "STRING"))
            having_node = ComparisonNode('>')
            having_node.add_child(func_node)
            having_node.add_child(LiteralNode(0, "INT"))

        # 验证HAVING子句的合法性
        if not self._is_valid_having_clause(having_node):
            # 创建一个合法的HAVING条件
            if self.group_by_clause and hasattr(self.group_by_clause,
                                                'expressions') and self.group_by_clause.expressions:
                # 使用GROUP BY中的表达式创建条件
                expr = random.choice(self.group_by_clause.expressions)
                having_node = ComparisonNode(random.choice(['=', '<>', '<', '>']))
                having_node.add_child(expr)
                having_node.add_child(LiteralNode(random.randint(1, 100), "INT"))
            else:
                # 创建一个基于聚合函数的条件
                count_func = next(f for f in self.functions if f.name == "COUNT")
                func_node = FunctionCallNode(count_func)
                col_ref = self._get_random_column_reference()
                if col_ref:
                    func_node.add_child(col_ref)
                else:
                    # 如果无法获取列引用，使用COUNT(*)
                    func_node.add_child(LiteralNode('*', "STRING"))
                having_node = ComparisonNode('>')
                having_node.add_child(func_node)
                having_node.add_child(LiteralNode(0, "INT"))

        self.having_clause = having_node
        if having_node:
            self.add_child(having_node)

    def _is_valid_having_clause(self, having_node: ASTNode) -> bool:
        """验证HAVING子句是否合法"""
        if not having_node:
            return True

        # HAVING子句必须包含聚合函数或引用GROUP BY中的列
        has_aggregate = having_node.contains_aggregate_function()

        if not has_aggregate and self.group_by_clause and hasattr(self.group_by_clause, 'expressions'):
            # 获取GROUP BY中的所有列引用
            group_by_columns = set()
            for expr in self.group_by_clause.expressions:
                group_by_columns.update(expr.get_referenced_columns())

            # 获取HAVING子句中的所有列引用
            having_columns = having_node.get_referenced_columns()

            # 检查HAVING中的所有列是否都在GROUP BY中
            if not having_columns.issubset(group_by_columns):
                return False

        return has_aggregate or (self.group_by_clause and hasattr(self.group_by_clause, 'expressions') and len(
            self.group_by_clause.expressions) > 0)

    def _get_random_column_reference(self) -> Optional[ColumnReferenceNode]:
        """获取一个随机的列引用，优先使用column_tracker选择未使用的列"""
        if not self.from_clause or not hasattr(self.from_clause, 'get_table_alias_map'):
            print(f"[SelectNode] 无法获取表别名映射，返回None")
            return None

        table_alias_map = self.from_clause.get_table_alias_map()
        if not table_alias_map:
            print(f"[SelectNode] 表别名映射为空，返回None")
            return None

        table_name = random.choice(list(table_alias_map.keys()))
        table = next((t for t in self.tables if t.name == table_name), None)
        if not table:
            print(f"[SelectNode] 找不到表: {table_name}，返回None")
            return None

        table_alias = table_alias_map[table_name]
        
        # 如果有column_tracker，使用它来选择未使用的列
        if self.column_tracker:
            print(f"[SelectNode] 使用列追踪器选择表 {table_alias} 的列")
            column = get_random_column_with_tracker(table, table_alias, self.column_tracker, for_select=True)
            if column:
                print(f"[SelectNode] 成功通过列追踪器选择列: {table_alias}.{column.name}")
                return ColumnReferenceNode(column, table_alias)
            print(f"[SelectNode] 列追踪器未能选择有效列，将使用回退逻辑")
        else:
            print(f"[SelectNode] 未设置列追踪器，使用原始逻辑")
        
        # 如果没有column_tracker或者没有可用列，回退到原来的逻辑
        column = random.choice(table.columns)
        print(f"[SelectNode] 使用回退逻辑选择列: {table_alias}.{column.name}")
        return ColumnReferenceNode(column, table_alias)

    def set_order_by_clause(self, order_by_node: OrderByNode) -> None:
        self.order_by_clause = order_by_node
        if order_by_node:
            self.add_child(order_by_node)

    def set_limit_clause(self, limit_node: LimitNode) -> None:
        self.limit_clause = limit_node
        if limit_node:
            self.add_child(limit_node)
    
    def set_for_update(self, mode) -> None:
        """设置锁定模式
        参数:
            mode: 锁定模式，可选值: 'update', 'share', 'no key update', 'key share'
        """
        valid_modes = ['update', 'share', 'no key update', 'key share']
        if mode in valid_modes:
            self.for_update = mode
        else:
            # 默认使用FOR UPDATE
            self.for_update = 'update'

    def to_sql(self) -> str:
        parts = []

        # SELECT部分 - 确保至少有一个表达式
        select_parts = []
        for expr, alias in self.select_expressions:
            expr_sql = expr.to_sql()
            # 使用自动生成的别名（如果没有显式别名）
            if alias:
                select_parts.append(f"{expr_sql} AS {alias}")
            else:
                # 为没有别名的表达式生成默认别名
                idx = self.select_expressions.index((expr, alias)) + 1
                default_alias = f"col_{idx}"
                select_parts.append(f"{expr_sql} AS {default_alias}")

        # 防止SELECT子句为空
        if not select_parts:
            select_parts.append("1 AS col_1")  # 添加默认表达式避免语法错误

        distinct_str = "DISTINCT " if self.distinct else ""
        parts.append(f"SELECT {distinct_str}{', '.join(select_parts)}")

        # FROM部分
        if self.from_clause:
            parts.append(f"FROM {self.from_clause.to_sql()}")
        else:
            parts.append("FROM DUAL")

        # WHERE部分
        if self.where_clause:
            where_sql = self.where_clause.to_sql()
            if where_sql:  # 仅添加有效条件
                parts.append(f"WHERE {where_sql}")

        # GROUP BY部分（确保不为空）
        if self.group_by_clause:
            group_by_sql = self.group_by_clause.to_sql()
            if group_by_sql.strip():  # 过滤空的GROUP BY
                parts.append(f"GROUP BY {group_by_sql}")

        # HAVING部分 - 仅在有GROUP BY时添加
        if self.having_clause and self.group_by_clause:
            having_sql = self.having_clause.to_sql()
            if having_sql:  # 仅添加有效条件
                parts.append(f"HAVING {having_sql}")

        # ORDER BY部分
        if self.order_by_clause:
            order_by_sql = self.order_by_clause.to_sql()
            if order_by_sql:  # 仅添加有效排序
                parts.append(f"ORDER BY {order_by_sql}")

        # LIMIT部分
        if self.limit_clause:
            parts.append(f"LIMIT {self.limit_clause.to_sql()}")
            
        # 锁定模式部分
        if self.for_update:
            # 获取当前数据库方言实例
            current_dialect = get_current_dialect()
            dialect_name = current_dialect.name.lower()
            lock_clause = ""
            # 检查是否为Percona方言
            is_percona = 'percona' in dialect_name or (hasattr(current_dialect, '__class__') and 'percona' in current_dialect.__class__.__name__.lower())
            
            # 对于Percona 5.7，只支持FOR UPDATE和LOCK IN SHARE MODE，不支持FOR NO KEY UPDATE等高级锁定模式
            if is_percona or dialect_name in ['mysql', 'mariadb', 'tidb', 'oceanbase', 'polardb']:
                # Percona/MySQL/MariaDB/PolarDB等方言只支持FOR UPDATE和LOCK IN SHARE MODE
                if self.for_update == 'share':
                    # 检查当前方言是否支持SHARE锁定模式
                    if hasattr(current_dialect, 'supports_share_lock_mode'):
                        if current_dialect.supports_share_lock_mode():
                            lock_clause = "LOCK IN SHARE MODE"
                        # 如果不支持share锁定模式，则不添加锁定子句
                    else:
                        # 如果方言没有实现此方法，默认添加LOCK IN SHARE MODE
                        lock_clause = "LOCK IN SHARE MODE"
                elif self.for_update == 'key share':
                    # 对于key share模式，在不支持的方言中使用lock in share mode作为替代
                    if hasattr(current_dialect, 'supports_share_lock_mode'):
                        if current_dialect.supports_share_lock_mode():
                            lock_clause = "LOCK IN SHARE MODE"
                        # 如果不支持share锁定模式，则不添加锁定子句
                    else:
                        # 如果方言没有实现此方法，默认添加LOCK IN SHARE MODE
                        lock_clause = "LOCK IN SHARE MODE"
                elif self.for_update == 'no key update':
                    # 对于no key update模式，在不支持的方言中使用for update作为替代
                    lock_clause = "FOR UPDATE"
                else:
                    # 对于update模式，使用FOR UPDATE
                    lock_clause = "FOR UPDATE"
            else:
                # PostgreSQL等其他方言支持所有模式
                lock_clause = f"FOR {self.for_update.upper()}"
                
            if lock_clause:
                parts.append(lock_clause)
                
        return " ".join(parts)

    def collect_table_aliases(self) -> Set[str]:
        """收集SELECT语句中所有引用的表别名"""
        aliases = set()

        # 收集SELECT表达式中的表别名
        for expr, _ in self.select_expressions:
            aliases.update(expr.collect_table_aliases())

        # 收集WHERE子句中的表别名
        if self.where_clause:
            aliases.update(self.where_clause.collect_table_aliases())

        # 收集GROUP BY子句中的表别名
        if self.group_by_clause:
            aliases.update(self.group_by_clause.collect_table_aliases())

        # 收集HAVING子句中的表别名
        if self.having_clause:
            aliases.update(self.having_clause.collect_table_aliases())

        # 收集ORDER BY子句中的表别名
        if self.order_by_clause:
            aliases.update(self.order_by_clause.collect_table_aliases())

        return aliases

    def get_defined_aliases(self) -> Set[str]:
        """获取此SELECT语句中定义的所有表别名（包括子查询）"""
        aliases = set()

        # 从FROM子句获取定义的表别名
        if self.from_clause and hasattr(self.from_clause, 'get_defined_aliases'):
            aliases.update(self.from_clause.get_defined_aliases())

        return aliases

    def validate_all_columns(self) -> Tuple[bool, List[str]]:
        """验证所有列引用是否有效"""
        errors = []

        if not self.from_clause:
            return (False, ["缺少FROM子句"])

        # 收集所有引用的表别名
        referenced_aliases = set()
        for expr, _ in self.select_expressions:
            referenced_aliases.update(expr.collect_table_aliases())

        # 收集FROM子句中定义的表别名
        defined_aliases = self.from_clause.get_all_aliases()
        # 检查是否有引用的表别名未在FROM子句中定义
        undefined_aliases = referenced_aliases - defined_aliases
        if undefined_aliases:
            errors.extend([f"在SELECT子句中引用的表别名'{alias}'未在FROM子句中定义" for alias in undefined_aliases])

        # 验证SELECT表达式
        for expr, _ in self.select_expressions:
            if hasattr(expr, 'validate_columns'):
                valid, expr_errors = expr.validate_columns(self.from_clause)
                if not valid:
                    errors.extend(expr_errors)
            elif isinstance(expr, ColumnReferenceNode):
                if not expr.is_valid(self.from_clause):
                    errors.append(f"无效的列引用: {expr.to_sql()}")

        # 验证WHERE子句
        if self.where_clause and hasattr(self.where_clause, 'validate_columns'):
            # 检查WHERE子句是否包含子查询
            has_subquery = False
            subquery_node = None
            subquery_nodes = []
            
            # 检查是否为EXISTS/NOT EXISTS子查询
            if hasattr(self.where_clause, 'operator') and self.where_clause.operator in ['EXISTS', 'NOT EXISTS']:
                has_subquery = True
                if len(self.where_clause.children) > 0:
                    subquery_node = self.where_clause.children[0]
                    subquery_nodes = [subquery_node]
            # 检查是否为比较操作符中的子查询（如 <>, =, <, > 等）
            elif hasattr(self.where_clause, 'children'):
                for child in self.where_clause.children:
                    if isinstance(child, SubqueryNode):
                        has_subquery = True
                        subquery_node = child
                        subquery_nodes.append(child)
                # 允许多个子查询，不提前退出
            
            # 如果包含子查询，让子查询自己验证其内部的列引用
            if has_subquery and subquery_nodes:
                for sq in subquery_nodes:
                    if hasattr(sq, 'validate_inner_columns'):
                        valid, where_errors = sq.validate_inner_columns()
                        if not valid:
                            errors.extend(where_errors)
                # 继续验证子查询以外的部分，避免漏掉外层列引用
                for child in self.where_clause.children:
                    if isinstance(child, SubqueryNode):
                        continue
                    if hasattr(child, 'validate_columns'):
                        valid, child_errors = child.validate_columns(self.from_clause)
                        if not valid:
                            errors.extend(child_errors)
                    elif isinstance(child, ColumnReferenceNode):
                        if not child.is_valid(self.from_clause):
                            errors.append(f"无效的列引用: {child.to_sql()}")
            else:
                # 常规WHERE子句验证
                valid, where_errors = self.where_clause.validate_columns(self.from_clause)
                if not valid:
                    errors.extend(where_errors)

        # 验证GROUP BY子句
        if self.group_by_clause:
            for expr in self.group_by_clause.expressions:
                if hasattr(expr, 'validate_columns'):
                    valid, expr_errors = expr.validate_columns(self.from_clause)
                    if not valid:
                        errors.extend(expr_errors)
                elif isinstance(expr, ColumnReferenceNode):
                    if not expr.is_valid(self.from_clause):
                        errors.append(f"无效的列引用: {expr.to_sql()}")

        # 验证HAVING子句
        if self.having_clause:
            if hasattr(self.having_clause, 'validate_columns'):
                valid, having_errors = self.having_clause.validate_columns(self.from_clause)
                if not valid:
                    errors.extend(having_errors)
            elif isinstance(self.having_clause, ColumnReferenceNode):
                if not self.having_clause.is_valid(self.from_clause):
                    errors.append(f"无效的列引用: {self.having_clause.to_sql()}")

        # 验证ORDER BY子句
        if self.order_by_clause:
            for expr, _ in self.order_by_clause.expressions:
                if hasattr(expr, 'validate_columns'):
                    valid, expr_errors = expr.validate_columns(self.from_clause)
                    if not valid:
                        errors.extend(expr_errors)
                elif isinstance(expr, ColumnReferenceNode):
                    if not expr.is_valid(self.from_clause):
                        errors.append(f"无效的列引用: {expr.to_sql()}")

        # 验证ON子句中的列引用
        if hasattr(self.from_clause, 'joins'):
            for join in self.from_clause.joins:
                condition = join.get('condition')
                if condition and hasattr(condition, 'validate_columns'):
                    valid, join_errors = condition.validate_columns(self.from_clause)
                    if not valid:
                        errors.extend([f"JOIN条件错误: {err}" for err in join_errors])

        return (len(errors) == 0, errors)

    def repair_invalid_columns(self) -> None:
        """修复所有无效的列引用"""
        if not self.from_clause:
            return

        # 修复重复的列别名
        aliases = []
        alias_count = {}
        new_select_expressions = []
        for i, (expr, alias) in enumerate(self.select_expressions):
            # 为没有显式别名的表达式生成默认别名
            current_alias = alias if alias else f"col_{i + 1}"
            
            # 检查别名是否重复
            if current_alias in alias_count:
                # 生成新的唯一别名
                alias_count[current_alias] += 1
                new_alias = f"{current_alias}_{alias_count[current_alias]}"
            else:
                alias_count[current_alias] = 1
                new_alias = current_alias
            
            new_select_expressions.append((expr, new_alias))
            aliases.append(new_alias)
        
        # 更新SELECT表达式
        self.select_expressions = new_select_expressions

        # 修复SELECT表达式
        for i, (expr, alias) in enumerate(self.select_expressions):
            if hasattr(expr, 'repair_columns'):
                expr.repair_columns(self.from_clause)
            elif isinstance(expr, ColumnReferenceNode) and hasattr(expr, 'is_valid') and not expr.is_valid(self.from_clause):
                replacement = expr.find_replacement(self.from_clause)
                if replacement:
                    self.select_expressions[i] = (replacement, alias)

        # 修复WHERE子句
        if self.where_clause and hasattr(self.where_clause, 'repair_columns'):
            # 检查WHERE子句是否包含子查询，类似于validate_all_columns中的逻辑
            has_subquery = False
            subquery_node = None
            
            # 检查是否为EXISTS/NOT EXISTS子查询
            if hasattr(self.where_clause, 'operator') and self.where_clause.operator in ['EXISTS', 'NOT EXISTS']:
                has_subquery = True
                if len(self.where_clause.children) > 0:
                    subquery_node = self.where_clause.children[0]
            # 检查是否为比较操作符中的子查询（如 <>, =, <, > 等）
            elif hasattr(self.where_clause, 'children'):
                for child in self.where_clause.children:
                    if isinstance(child, SubqueryNode):
                        has_subquery = True
                        subquery_node = child
                        break
            
            # 如果包含子查询，让子查询自己修复其内部的列引用
            if has_subquery and subquery_node and hasattr(subquery_node, 'repair_columns'):
                # 重要：传递None作为from_node参数，确保子查询的repair_columns方法完全隔离
                # 使用子查询自身的FROM子句进行内部列引用修复，不使用外部查询的FROM子句
                subquery_node.repair_columns(None)
                # 对于WHERE子句的其他部分（非子查询部分），仍然使用外部from_clause进行修复
                remaining_children = [child for child in self.where_clause.children if child != subquery_node]
                for child in remaining_children:
                    if hasattr(child, 'repair_columns'):
                        child.repair_columns(self.from_clause)
                    elif isinstance(child, ColumnReferenceNode) and hasattr(child, 'is_valid') and not child.is_valid(self.from_clause):
                        if hasattr(child, 'find_replacement'):
                            replacement = child.find_replacement(self.from_clause)
                            if replacement:
                                for i, c in enumerate(self.where_clause.children):
                                    if c == child:
                                        self.where_clause.children[i] = replacement
                                        break
            else:
                # 常规WHERE子句修复
                self.where_clause.repair_columns(self.from_clause)

        # 修复GROUP BY子句
        if self.group_by_clause:
            for i, expr in enumerate(self.group_by_clause.expressions):
                if hasattr(expr, 'repair_columns'):
                    expr.repair_columns(self.from_clause)
                elif isinstance(expr, ColumnReferenceNode) and hasattr(expr, 'is_valid') and not expr.is_valid(self.from_clause):
                    replacement = expr.find_replacement(self.from_clause)
                    if replacement:
                        self.group_by_clause.expressions[i] = replacement

        # 修复HAVING子句
        if self.having_clause:
            if hasattr(self.having_clause, 'repair_columns'):
                self.having_clause.repair_columns(self.from_clause)
            elif isinstance(self.having_clause, ColumnReferenceNode) and hasattr(self.having_clause, 'is_valid') and not self.having_clause.is_valid(self.from_clause):
                replacement = self.having_clause.find_replacement(self.from_clause)
                if replacement:
                    self.having_clause = replacement

        # 修复ORDER BY子句
        if self.order_by_clause:
            repaired_order_by = []
            for expr, direction in self.order_by_clause.expressions:
                if hasattr(expr, 'repair_columns'):
                    expr.repair_columns(self.from_clause)
                    repaired_order_by.append((expr, direction))
                elif isinstance(expr, ColumnReferenceNode) and hasattr(expr, 'is_valid') and not expr.is_valid(self.from_clause):
                    replacement = expr.find_replacement(self.from_clause)
                    if replacement:
                        repaired_order_by.append((replacement, direction))
                else:
                    repaired_order_by.append((expr, direction))
            self.order_by_clause.expressions = repaired_order_by

        # 修复ON子句中的列引用
        if hasattr(self.from_clause, 'joins'):
            for join in self.from_clause.joins:
                condition = join.get('condition')
                if condition and hasattr(condition, 'repair_columns'):
                    condition.repair_columns(self.from_clause)
    
    def contains_window_function(self) -> bool:
        """检查是否包含窗口函数"""
        # 检查SELECT表达式
        for expr, _ in self.select_expressions:
            if hasattr(expr, 'contains_window_function') and expr.contains_window_function():
                return True
        
        # 检查WHERE子句
        if self.where_clause and hasattr(self.where_clause, 'contains_window_function') and self.where_clause.contains_window_function():
            return True
        
        # 检查HAVING子句
        if self.having_clause and hasattr(self.having_clause, 'contains_window_function') and self.having_clause.contains_window_function():
            return True
        
        return False
        
    def contains_aggregate_function(self) -> bool:
        """检查是否包含聚合函数"""
        # 检查SELECT表达式
        for expr, _ in self.select_expressions:
            if hasattr(expr, 'contains_aggregate_function') and expr.contains_aggregate_function():
                return True
            # 检查函数调用节点
            if isinstance(expr, FunctionCallNode) and expr.metadata.get('func_type') == 'aggregate':
                return True
        
        # 检查HAVING子句
        if self.having_clause:
            if hasattr(self.having_clause, 'contains_aggregate_function') and self.having_clause.contains_aggregate_function():
                return True
            # 检查函数调用节点
            if isinstance(self.having_clause, FunctionCallNode) and self.having_clause.metadata.get('func_type') == 'aggregate':
                return True
        
        return False
        
    def get_referenced_columns(self) -> Set[str]:
        """获取所有引用的列"""
        columns = set()
        # 收集SELECT表达式中的列引用
        for expr, _ in self.select_expressions:
            if hasattr(expr, 'get_referenced_columns'):
                columns.update(expr.get_referenced_columns())
        
        # 收集WHERE子句中的列引用
        if self.where_clause and hasattr(self.where_clause, 'get_referenced_columns'):
            columns.update(self.where_clause.get_referenced_columns())
        
        # 收集GROUP BY子句中的列引用
        if self.group_by_clause:
            for expr in self.group_by_clause.expressions:
                if hasattr(expr, 'get_referenced_columns'):
                    columns.update(expr.get_referenced_columns())
        
        # 收集HAVING子句中的列引用
        if self.having_clause and hasattr(self.having_clause, 'get_referenced_columns'):
            columns.update(self.having_clause.get_referenced_columns())
        
        # 收集ORDER BY子句中的列引用
        if self.order_by_clause:
            for expr in self.order_by_clause.expressions:
                if hasattr(expr, 'get_referenced_columns'):
                    columns.update(expr.get_referenced_columns())
        
        return columns
