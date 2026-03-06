import random
from typing import List, Optional

from ast_nodes import (
    ASTNode,
    ColumnReferenceNode,
    ComparisonNode,
    FromNode,
    LiteralNode,
    LogicalNode,
    SelectNode,
    SubqueryNode,
)
from data_structures.function import Function
from data_structures.table import Table
from sql_generation.random_sql.column_tracker import ColumnUsageTracker, get_random_column_with_tracker
from sql_generation.random_sql.expressions import create_complex_expression
from sql_generation.random_sql.type_utils import (
    ORDERABLE_CATEGORIES,
    get_comparison_operators,
    get_safe_comparison_category,
    map_return_type_to_category,
)

def create_simple_where_condition(table: Table, alias: str) -> Optional[ASTNode]:
    """为子查询创建简单的WHERE条件
    
    Args:
        table: 表对象
        alias: 表别名
    
    Returns:
        ASTNode: WHERE条件节点
    """
    try:
        # 选择一个列
        col = table.get_random_column()
        col_ref = ColumnReferenceNode(col, alias)
        
        # 根据列类型生成不同的条件
        if col.category == 'numeric':
            # 数值类型列的比较条件
            operators = ['=', '<', '>', '<=', '>=', '<>']
            operator = random.choice(operators)
            condition = ComparisonNode(operator)
            condition.add_child(col_ref)
            
            # 生成随机数值
            if col.data_type in ['INT', 'INTEGER', 'BIGINT', 'TINYINT', 'SMALLINT']:
                condition.add_child(LiteralNode(random.randint(0, 100), col.data_type))
            else:
                condition.add_child(LiteralNode(random.uniform(0, 100), col.data_type))
            
        elif col.category == 'string':
            # 字符串类型列的比较条件
            operators = ['=', '<>', 'LIKE']
            operator = random.choice(operators)
            condition = ComparisonNode(operator)
            condition.add_child(col_ref)
            
            # 生成随机字符串
            if operator == 'LIKE':
                condition.add_child(LiteralNode(f"'sample%'", col.data_type))
            else:
                condition.add_child(LiteralNode(f"'sample_{random.randint(1, 100)}'", col.data_type))
            
        elif col.category == 'datetime':
            # 日期时间类型列的比较条件
            operators = ['=', '<', '>', '<=', '>=', '<>']
            operator = random.choice(operators)
            condition = ComparisonNode(operator)
            condition.add_child(col_ref)
            
            # 生成随机日期
            year = random.randint(2020, 2024)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            date_str = f'{year}-{month:02d}-{day:02d}'
            condition.add_child(LiteralNode(date_str, col.data_type))
            
        else:
            # 其他类型使用IS NULL/IS NOT NULL避免无效比较
            operator = random.choice(['IS NULL', 'IS NOT NULL'])
            condition = ComparisonNode(operator)
            condition.add_child(col_ref)
            
        return condition
    except Exception:
        return None

def create_in_subquery(tables: List[Table], functions: List[Function], 
                      from_node: FromNode, main_table: Table, main_alias: str, 
                      join_table: Optional[Table] = None, join_alias: Optional[str] = None, 
                      column_tracker: Optional[ColumnUsageTracker] = None) -> ASTNode:
    """创建IN/NOT IN子查询，包括多种anti-join形式"""
    if random.random() < 0.3 and len(tables) > 1:
        # 选择一个不同的表用于子查询
        subquery_table = random.choice([t for t in tables if t != main_table])
        
        # 创建子查询
        subquery = SelectNode()
        subquery.tables = [subquery_table]
        subquery.functions = functions
        
        # 添加简单的SELECT表达式
        subquery_col = subquery_table.get_random_column('numeric')
        subquery_expr = ColumnReferenceNode(subquery_col, 'subq')
        subquery.add_select_expression(subquery_expr)
        
        # 创建FROM子句
        subquery_from = FromNode()
        subquery_from.add_table(subquery_table, 'subq')
        subquery.set_from_clause(subquery_from)
        
        # 添加WHERE条件（可选）
        if random.random() > 0.5:
            subquery_category = get_safe_comparison_category(subquery_col)
            if subquery_category in ORDERABLE_CATEGORIES:
                subquery_where = ComparisonNode('>')
                subquery_where.add_child(ColumnReferenceNode(subquery_col, 'subq'))
                if subquery_category == 'numeric':
                    subquery_where.add_child(LiteralNode(random.randint(0, 50), 'INT'))
                else:
                    subquery_where.add_child(LiteralNode('2023-01-01 12:00:00', 'DATETIME'))
                subquery.set_where_clause(subquery_where)
        
        # 左侧列引用 - 使用列追踪器选择未在select、having和on中使用的列
        main_col = get_random_column_with_tracker(main_table, main_alias, column_tracker, for_select=False)
        left_col_ref = ColumnReferenceNode(main_col, main_alias)
        
        # 右侧子查询
        subquery_node = SubqueryNode(subquery, '')
        
        # 随机选择子查询形式
        subquery_form = random.random()
        # 随机选择IN/NOT IN形式
        comp_node = ComparisonNode('IN' if random.random() < 0.5 else 'NOT IN')
        comp_node.add_child(left_col_ref)
        comp_node.add_child(subquery_node)
        return comp_node
    else:
        # 回退到简单比较
        return create_where_condition(tables, functions, from_node, main_table, main_alias, join_table, join_alias, column_tracker=column_tracker)

def create_exists_subquery(tables: List[Table], functions: List[Function], 
                           from_node: FromNode, main_table: Table, main_alias: str, 
                           join_table: Optional[Table] = None, join_alias: Optional[str] = None, 
                           column_tracker: Optional[ColumnUsageTracker] = None) -> ASTNode:
    """创建EXISTS/NOT EXISTS子查询，确保只引用子查询中实际选择的列"""
    if random.random() < 0.2 and len(tables) > 1:
        # 选择关联表
        rel_table = random.choice(tables)
        # 创建相关子查询
        subquery = SelectNode()
        subquery.tables = [rel_table]  # 只包含实际添加到FROM子句的表
        subquery.functions = functions
        
        # 参考generate_random_sql中的from子句生成过程
        # 创建FROM子句
        subquery_from = FromNode()
        
        # 生成安全的表别名，避免SQL关键字冲突
        sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
        base_rel_alias = rel_table.name[:3].lower()
        rel_alias = base_rel_alias if base_rel_alias not in sql_keywords else rel_table.name[:2].lower() + str(random.randint(0, 9))
        # 添加关联表到子查询的FROM子句
        subquery_from.add_table(rel_table, rel_alias)
        subquery.set_from_clause(subquery_from)
        
        # 选择关联表中的列 - 使用列追踪器选择未在select、having和on中使用的列
        rel_col = get_random_column_with_tracker(rel_table, rel_alias, column_tracker, for_select=True)
        
        # 添加SELECT表达式，使用关联表中的随机列并设置别名
        if rel_col:
            subquery_expr = ColumnReferenceNode(rel_col, rel_alias)
            # 设置列别名，确保子查询返回的列有明确的引用
            subquery.add_select_expression(subquery_expr, rel_col.name)
        else:
            # 如果没有找到列，使用默认的常量
            subquery_expr = LiteralNode(1, 'INT')
            subquery.add_select_expression(subquery_expr, 'default_col')
        
        # 创建WHERE条件（相关子查询的关联条件）
        where_node = ComparisonNode('=')
        
        if rel_col:
            left_col_ref = ColumnReferenceNode(rel_col, rel_alias)
            
            # 根据列类型选择右侧操作数（直接使用常量）
            if rel_col.category == 'numeric':
                # 数值类型：直接使用数值常量
                numeric_value = random.randint(0, 100)
                where_node.add_child(left_col_ref)
                where_node.add_child(LiteralNode(numeric_value, rel_col.data_type))
            elif rel_col.category == 'string':
                # 字符串类型：直接使用字符串常量
                string_value = f"sample_{random.randint(1, 100)}"
                where_node.add_child(left_col_ref)
                where_node.add_child(LiteralNode(string_value, 'STRING'))
            elif rel_col.category == 'datetime':
                # 日期时间类型：生成带时间部分的日期时间常量
                # 确保使用明确的DATETIME类型以确保引号被正确添加
                year = 2023
                month = random.randint(1, 12)
                day = random.randint(1, 28)  # 简单处理，避免月末问题
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                # 构建完整的日期时间字符串
                datetime_value = f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"
                
                # 使用明确的DATETIME类型确保引号被添加
                datetime_type = 'DATETIME'  # 明确使用DATETIME类型确保引号被添加
                where_node.add_child(left_col_ref)
                where_node.add_child(LiteralNode(datetime_value, datetime_type))
            else:
                # 其他类型：直接使用默认字符串常量
                default_value = "default_value"
                where_node.add_child(left_col_ref)
                where_node.add_child(LiteralNode(default_value, 'STRING'))
        else:
            # 如果没有找到合适的列，使用默认条件
            where_node.add_child(LiteralNode(1, 'INT'))
            where_node.add_child(LiteralNode(1, 'INT'))
        
        if where_node.children:
            subquery.set_where_clause(where_node)
        
        # 验证子查询内部的列引用，确保不引用不存在的列
        valid, invalid_columns = subquery.validate_all_columns()
        if not valid and invalid_columns:
            # 如果存在无效的列引用，简化WHERE条件
            where_node = ComparisonNode('=')
            where_node.add_child(LiteralNode(1, 'INT'))
            where_node.add_child(LiteralNode(1, 'INT'))
            subquery.set_where_clause(where_node)
        
        # 注意：EXISTS/NOT EXISTS子查询不应该有别名
        subquery_node = SubqueryNode(subquery, '')
        
        # 随机选择子查询形式
        subquery_form = random.random()
        # 随机选择EXISTS/NOT EXISTS形式
        exists_node = ComparisonNode('EXISTS' if random.random() < 0.5 else 'NOT EXISTS')
        exists_node.add_child(subquery_node)
        return exists_node
    else:
        return create_where_condition(tables, functions, from_node, main_table, main_alias, join_table, join_alias, column_tracker=column_tracker)

def create_where_condition(tables: List[Table], functions: List[Function], 
                          from_node: FromNode, main_table: Table, main_alias: str, 
                          join_table: Optional[Table] = None, join_alias: Optional[str] = None, 
                          use_subquery: bool = True, column_tracker: Optional[ColumnUsageTracker] = None) -> ASTNode:
    """创建WHERE条件，支持多种条件类型"""
    
    # 过滤掉聚合函数和窗口函数，确保WHERE条件中不包含这些函数
    non_aggregate_functions = [f for f in functions if f.func_type != 'aggregate' and f.func_type != 'window']
    
    # 根据概率选择条件类型
    condition_type = random.random()
    

    # 15%概率使用子查询条件（IN/NOT IN或EXISTS/NOT EXISTS）
    if use_subquery and condition_type < 0.15:
        # 50%概率使用IN/NOT IN子查询，50%概率使用EXISTS/NOT EXISTS子查询
        if random.random() < 0.5:
            return create_in_subquery(tables, non_aggregate_functions, from_node, main_table, main_alias, join_table, join_alias, column_tracker)
        else:
            return create_exists_subquery(tables, non_aggregate_functions, from_node, main_table, main_alias, join_table, join_alias, column_tracker)
    
    # 15%概率使用带ANY/ALL的子查询（新谓词1）
    elif use_subquery and condition_type < 0.3:
        # 选择表和列
        tables_to_choose = [main_table] + ([join_table] if join_table else [])
        table = random.choice(tables_to_choose)
        alias = main_alias if table == main_table else join_alias
        
        # 优先选择数值型列
        numeric_cols = []
        if column_tracker:
            available_columns = column_tracker.get_available_columns(table, alias)
            numeric_cols = [col for col in available_columns if get_safe_comparison_category(col) in ORDERABLE_CATEGORIES]
        else:
            numeric_cols = [col for col in table.columns if get_safe_comparison_category(col) in ORDERABLE_CATEGORIES]
            
        if numeric_cols:
            col = random.choice(numeric_cols)
            if column_tracker:
                column_tracker.mark_column_as_used(alias, col.name)
            safe_category = get_safe_comparison_category(col)
            
            col_ref = ColumnReferenceNode(col, alias)
            
            # 创建子查询
            subquery = SelectNode()
            subquery_table = random.choice([t for t in tables if t != table]) if len(tables) > 1 else table
            safe_category = get_safe_comparison_category(col)
            subquery_col = subquery_table.get_random_column(safe_category)
            subquery.add_select_expression(ColumnReferenceNode(subquery_col, 'subq'))
            subquery_from = FromNode()
            subquery_from.add_table(subquery_table, 'subq')
            subquery.set_from_clause(subquery_from)
            
            # 选择ANY/ALL操作符和比较操作符
            any_all = random.choice(['ANY', 'ALL'])
            # 为二进制类型选择合适的比较运算符
            operator = random.choice(get_comparison_operators(safe_category))
            
            comp_node = ComparisonNode(f'{operator} {any_all}')
            comp_node.add_child(col_ref)
            comp_node.add_child(SubqueryNode(subquery, ''))
            
            return comp_node
        
        # 如果没有数值列，回退到简单比较
        return create_where_condition(
            tables, non_aggregate_functions, from_node, main_table, main_alias,
            join_table, join_alias, use_subquery=False, column_tracker=column_tracker
        )
    
    # 15%概率使用复杂嵌套表达式
    elif condition_type < 0.45:
        return create_complex_expression(tables, non_aggregate_functions, from_node, main_table, main_alias, join_table, join_alias, column_tracker=column_tracker, for_select=False)
    
    # 15%概率使用BETWEEN条件
    elif condition_type < 0.6:
        # 选择表和列
        tables_to_choose = [main_table] + ([join_table] if join_table else [])
        table = random.choice(tables_to_choose)
        alias = main_alias if table == main_table else join_alias
        
        # 优先选择数值型或日期型列
        numeric_cols = []
        if column_tracker:
            # 获取未使用的数值型或日期型列
            available_columns = column_tracker.get_available_columns(table, alias)
            numeric_cols = [col for col in available_columns if get_safe_comparison_category(col) in ['numeric', 'datetime']]
        else:
            # 如果没有跟踪器，使用所有数值型或日期型列
            numeric_cols = [col for col in table.columns if get_safe_comparison_category(col) in ['numeric', 'datetime']]
            
        if numeric_cols:
            col = random.choice(numeric_cols)
            if column_tracker:
                column_tracker.mark_column_as_used(alias, col.name)
            safe_category = get_safe_comparison_category(col)
            
            col_ref = ColumnReferenceNode(col, alias)
            
            # 创建BETWEEN条件
            between_node = ComparisonNode('BETWEEN' if random.random() < 0.5 else 'NOT BETWEEN')
            between_node.add_child(col_ref)
            
            # 添加低值和高值
            if safe_category == 'numeric':
                low_value = random.randint(0, 50)
                high_value = low_value + random.randint(10, 50)
                low_node = LiteralNode(low_value, col.data_type)
                high_node = LiteralNode(high_value, col.data_type)
                
                # 直接添加低值和高值
                between_node.add_child(low_node)
                between_node.add_child(high_node)
            elif safe_category == 'datetime':
                # 生成包含年月日时分秒的完整日期时间字符串
                start_datetime = f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
                # 确保结束日期时间大于开始日期时间
                end_month = random.randint(1, 12)
                end_day = random.randint(1, 28)
                end_hour = random.randint(0, 23)
                end_minute = random.randint(0, 59)
                end_second = random.randint(0, 59)
                end_datetime = f"2023-{end_month:02d}-{end_day:02d} {end_hour:02d}:{end_minute:02d}:{end_second:02d}"
                
                # 使用明确的'DATETIME'类型确保引号添加
                start_node = LiteralNode(start_datetime, 'DATETIME')
                end_node = LiteralNode(end_datetime, 'DATETIME')
                
                # 直接添加开始值和结束值
                between_node.add_child(start_node)
                between_node.add_child(end_node)
            
            return between_node
        else:
            # 如果没有数值或日期类型的列，回退到适合其他类型的条件
            col = get_random_column_with_tracker(table, alias, column_tracker, for_select=False)
            col_ref = ColumnReferenceNode(col, alias)
            
            # 根据列类型选择合适的操作符
            if col.category == 'string':
                operator = random.choice(['=', '<>', 'LIKE', 'NOT LIKE'])
                comp_node = ComparisonNode(operator)
                comp_node.add_child(col_ref)
                
                if operator in ['LIKE', 'NOT LIKE']:
                    patterns = [
                        f"sample_{random.randint(1, 100)}",
                        f"%sample_{random.randint(1, 100)}",
                        f"sample_{random.randint(1, 100)}%",
                        f"%sample_{random.randint(1, 100)}%"
                    ]
                    selected_pattern = random.choice(patterns)
                    comp_node.add_child(LiteralNode(selected_pattern, 'STRING'))
                else:
                    sample_value = f"sample_{random.randint(1, 100)}"
                    comp_node.add_child(LiteralNode(sample_value, 'STRING'))
            else:
                # 对于其他类型（如几何类型），使用IS NULL/IS NOT NULL操作符
                operator = random.choice(['IS NULL', 'IS NOT NULL'])
                comp_node = ComparisonNode(operator)
                comp_node.add_child(col_ref)
                
                # IS NULL/IS NOT NULL不需要右侧操作数
                if operator not in ['IS NULL', 'IS NOT NULL']:
                    # 为了安全起见，使用字符串字面量作为默认值
                    sample_value = f"sample_{random.randint(1, 100)}"
                    comp_node.add_child(LiteralNode(sample_value, 'STRING'))
            
            return comp_node
    
    # 15%概率使用增强的正则表达式模式（新谓词2）
    elif condition_type < 0.75 and join_table:
        # 选择表和列
        tables_to_choose = [main_table, join_table]
        table = random.choice(tables_to_choose)
        alias = main_alias if table == main_table else join_alias
        
        # 优先选择字符串列
        string_cols = []
        if column_tracker:
            # 获取未使用的字符串列
            available_columns = column_tracker.get_available_columns(table, alias)
            string_cols = [col for col in available_columns if col.category == 'string']
        else:
            # 如果没有跟踪器，使用所有字符串列
            string_cols = [col for col in table.columns if col.category == 'string']
            
        if string_cols:
            col = random.choice(string_cols)
            if column_tracker:
                column_tracker.mark_column_as_used(alias, col.name)
            col_ref = ColumnReferenceNode(col, alias)
            
            # 随机选择操作符
            operator = random.choice(['LIKE', 'NOT LIKE', 'RLIKE', 'REGEXP', 'NOT REGEXP'])
            
            comp_node = ComparisonNode(operator)
            comp_node.add_child(col_ref)
            
            # 添加增强的正则表达式模式
            if operator in ['RLIKE', 'REGEXP', 'NOT REGEXP']:
                # 更复杂的正则表达式模式
                complex_patterns = [
                    r'[a-zA-Z0-9]{5,10}',
                    r'[A-Z][a-z]{2,4}[0-9]{2}',
                    r'[0-9]{3}-[0-9]{2}-[0-9]{4}',
                    r'[a-z]+@[a-z]+\.[a-z]{2,3}',
                    r'^sample_\d+$',
                    r'.*[0-9]{3}.*'
                ]
                selected_pattern = random.choice(complex_patterns)
            else:
                patterns = [
                    f"sample_{random.randint(1, 100)}",
                    f"%sample_{random.randint(1, 100)}",
                    f"sample_{random.randint(1, 100)}%",
                    f"%sample_{random.randint(1, 100)}%"
                ]
                selected_pattern = random.choice(patterns)
                
            # 确保字符串字面量使用正确的数据类型标识
            string_type = 'STRING'
            pattern_node = LiteralNode(selected_pattern, string_type)
            
            comp_node.add_child(pattern_node)
            
            return comp_node
        
        # 如果没有字符串列，回退到简单比较
        return create_where_condition(
            tables, non_aggregate_functions, from_node, main_table, main_alias,
            join_table, join_alias, use_subquery=False, column_tracker=column_tracker
        )
            
        if string_cols:
            col = random.choice(string_cols)
            if column_tracker:
                column_tracker.mark_column_as_used(alias, col.name)
            col_ref = ColumnReferenceNode(col, alias)
            
            # 随机选择操作符
            operator = random.choice(['LIKE', 'NOT LIKE', 'RLIKE', 'REGEXP', 'NOT REGEXP'])
            
            comp_node = ComparisonNode(operator)
            comp_node.add_child(col_ref)
            
            # 添加模式
            patterns = [
                f"sample_{random.randint(1, 100)}",
                f"%sample_{random.randint(1, 100)}",
                f"sample_{random.randint(1, 100)}%",
                f"%sample_{random.randint(1, 100)}%",
                f"[a-z]{{3,5}}" if operator in ['RLIKE', 'REGEXP', 'NOT REGEXP'] else None
            ]
            # 过滤掉None值
            valid_patterns = [p for p in patterns if p is not None]
            selected_pattern = random.choice(valid_patterns)
            # 确保字符串字面量使用正确的数据类型标识，确保引号处理正常
            string_type = 'STRING'  # 明确使用STRING类型确保引号被添加
            pattern_node = LiteralNode(selected_pattern, string_type)
            
            comp_node.add_child(pattern_node)
            
            return comp_node
    
    # 15%概率使用NULL安全比较（新谓词3）
    elif condition_type < 0.9:
        # 选择表和列
        tables_to_choose = [main_table] + ([join_table] if join_table else [])
        table = random.choice(tables_to_choose)
        alias = main_alias if table == main_table else join_alias
        col = get_random_column_with_tracker(table, alias, column_tracker, for_select=False)
        safe_category = get_safe_comparison_category(col)
        
        # 避免使用不支持的NULL安全比较操作符，改用标准比较操作符和IS NULL结合的方式
        # 根据右侧是否为NULL选择不同的比较方式
        if random.random() < 0.5:
            # 右侧为NULL时使用IS NULL
            comp_node = ComparisonNode('IS NULL')
            comp_node.add_child(ColumnReferenceNode(col, alias))
            return comp_node
        else:
            # 右侧为具体值时使用标准比较操作符
            operator = random.choice(get_comparison_operators(safe_category))
            comp_node = ComparisonNode(operator)
            comp_node.add_child(ColumnReferenceNode(col, alias))
        
        # 随机选择右侧操作数为NULL或具体值
        if random.random() < 0.5:
            # 右侧为NULL
            comp_node.add_child(LiteralNode(None, 'NULL'))
        else:
            # 右侧为具体值
            if safe_category == 'numeric':
                value = random.randint(0, 100)
                comp_node.add_child(LiteralNode(value, col.data_type))
            elif safe_category == 'string':
                value = f"sample_{random.randint(1, 100)}"
                comp_node.add_child(LiteralNode(value, 'STRING'))
            elif safe_category == 'datetime':
                year = 2023
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                datetime_value = f"{year:04d}-{month:02d}-{day:02d}"
                comp_node.add_child(LiteralNode(datetime_value, 'DATE'))
            elif safe_category == 'binary':
                # 为binary类型生成随机的十六进制值
                hex_value = ''.join(random.choices('0123456789ABCDEF', k=8))
                comp_node.add_child(LiteralNode(f"X'{hex_value}'", 'BINARY'))
            elif safe_category == 'json':
                comp_node.add_child(LiteralNode('{"key": "value"}', 'JSON'))
            elif safe_category == 'boolean':
                comp_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
        
        return comp_node
    
    # 10%概率使用多列范围比较（新谓词4）
    else:
        # 只有当有连接表时才能使用多列范围比较
        if join_table:
            # 选择两个相关的数值列，确保通过列追踪器选择
                main_numeric_cols = []
                join_numeric_cols = []
                
                # 使用列追踪器获取可用的数值列
                if column_tracker:
                    main_available_columns = column_tracker.get_available_columns(main_table, main_alias)
                    main_numeric_cols = [col for col in main_available_columns if get_safe_comparison_category(col) == 'numeric']
                    
                    join_available_columns = column_tracker.get_available_columns(join_table, join_alias)
                    join_numeric_cols = [col for col in join_available_columns if get_safe_comparison_category(col) == 'numeric']
                else:
                    # 如果没有列追踪器，使用所有数值列
                    main_numeric_cols = [col for col in main_table.columns if get_safe_comparison_category(col) == 'numeric']
                join_numeric_cols = [col for col in join_table.columns if get_safe_comparison_category(col) == 'numeric']
                
                if main_numeric_cols and join_numeric_cols:
                    # 创建多列范围比较（例如：(a, b) BETWEEN (x, y) AND (p, q)）
                    logic_node = LogicalNode('AND')
                    
                    # 选择2-3对列进行比较
                    num_pairs = random.randint(2, 3)
                    for _ in range(num_pairs):
                        main_col = random.choice(main_numeric_cols)
                        join_col = random.choice(join_numeric_cols)
                        if column_tracker:
                            column_tracker.mark_column_as_used(main_alias, main_col.name)
                            column_tracker.mark_column_as_used(join_alias, join_col.name)
                        
                        between_node = ComparisonNode('BETWEEN')
                        between_node.add_child(ColumnReferenceNode(join_col, join_alias))
                        
                        low_value = random.randint(0, 30)
                        high_value = low_value + random.randint(10, 50)
                        
                        between_node.add_child(LiteralNode(low_value, join_col.data_type))
                        between_node.add_child(LiteralNode(high_value, join_col.data_type))
                        
                        logic_node.add_child(between_node)
                    
                    return logic_node
        
        # 默认：使用简单比较条件
        # 选择表和列
        tables_to_choose = [main_table] + ([join_table] if join_table else [])
        table = random.choice(tables_to_choose)
        alias = main_alias if table == main_table else join_alias
        col = get_random_column_with_tracker(table, alias, column_tracker, for_select=False)
        col_ref = ColumnReferenceNode(col, alias)
        safe_category = get_safe_comparison_category(col)
        is_string_category = str(col.category).lower() == 'string' or map_return_type_to_category(col.data_type) == 'string'
        # 根据列类型选择操作符
        if safe_category == 'string':
            if is_string_category:
                operator = random.choice(['=', '<>', 'LIKE', 'NOT LIKE', 'IS NULL', 'IS NOT NULL'])
            else:
                operator = random.choice(['=', '<>', 'IS NULL', 'IS NOT NULL'])
        elif safe_category == 'binary':
            # 二进制类型使用等于、不等于或IS NULL/IS NOT NULL操作符
            operator = random.choice(['=', '<>', 'IS NULL', 'IS NOT NULL'])
        else:
            operators = get_comparison_operators(safe_category)
            operators.extend(['IS NULL', 'IS NOT NULL'])
            operator = random.choice(operators)
        comp_node = ComparisonNode(operator)
        comp_node.add_child(col_ref)
        
        # 添加右侧操作数（确保类型兼容）
        if operator not in ['IS NULL', 'IS NOT NULL']:
            if safe_category == 'numeric':
                numeric_value = random.randint(0, 100)
                # 创建与列类型兼容的数值字面量
                right_node = LiteralNode(numeric_value, col.data_type)
                comp_node.add_child(right_node)
            elif safe_category == 'binary':
                # 为二进制类型生成兼容的十六进制值
                hex_value = ''.join(random.choices('0123456789ABCDEF', k=8))
                right_node = LiteralNode(f"X'{hex_value}'", 'BINARY')
                comp_node.add_child(right_node)
            elif safe_category == 'string':
                # 创建与列类型兼容的字符串字面量
                if operator in ['LIKE', 'NOT LIKE']:
                    # 生成包含通配符的模式
                    patterns = [
                        f"sample_{random.randint(1, 100)}",
                        f"%sample_{random.randint(1, 100)}",
                        f"sample_{random.randint(1, 100)}%",
                        f"%sample_{random.randint(1, 100)}%"
                    ]
                    selected_pattern = random.choice(patterns)
                    # 确保字符串字面量正确被引号包裹
                    right_node = LiteralNode(selected_pattern, 'STRING')
                else:
                    string_value = f"sample_{random.randint(1, 100)}"
                    # 明确使用'STRING'类型确保字符串被正确添加引号
                    right_node = LiteralNode(string_value, 'STRING')
                
                comp_node.add_child(right_node)
            elif safe_category == 'datetime':
                # 创建与列类型兼容的日期时间字面量，使用适当的格式化方式避免语法错误
                from data_structures.db_dialect import get_dialect_config
                dialect = get_dialect_config()
                
                # 生成更具体的日期时间值，包括时间部分
                # 直接使用已经导入的random模块
                year = 2023
                month = random.randint(1, 12)
                day = random.randint(1, 28)  # 简单处理，避免月末问题
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                # 构建完整的日期时间字符串
                datetime_value = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
                
                # 使用明确的日期时间类型确保引号被正确添加
                # 即使col.data_type不是标准的DATE/DATETIME/TIMESTAMP，也使用明确的类型
                datetime_type = 'DATETIME'  # 明确使用DATETIME类型确保引号被添加
                right_node = LiteralNode(datetime_value, datetime_type)
                
                comp_node.add_child(right_node)
            elif safe_category == 'json':
                comp_node.add_child(LiteralNode('{"key": "value"}', 'JSON'))
            elif safe_category == 'boolean':
                comp_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
        return comp_node
