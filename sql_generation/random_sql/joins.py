import random
import string
from typing import Optional, Union

from ast_nodes import (
    ASTNode,
    ColumnReferenceNode,
    ComparisonNode,
    FunctionCallNode,
    LiteralNode,
    LogicalNode,
    FromNode,
    SelectNode,
    SubqueryNode,
)
from data_structures.column import Column
from data_structures.db_dialect import DBDialectFactory
from data_structures.table import Table
from sql_generation.random_sql.expressions import create_compatible_literal, is_type_compatible
from sql_generation.random_sql.type_utils import (
    ORDERABLE_CATEGORIES,
    get_safe_comparison_category,
    normalize_category,
)

def generate_table_alias() -> str:
    """生成唯一的表别名"""
    sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
    # 使用随机字符串生成别名，避免关键字冲突
    base_alias = ''.join(random.choices(string.ascii_lowercase, k=3))
    # 添加随机数字确保唯一性
    alias = base_alias + str(random.randint(1, 99))
    return alias

def create_join_condition(main_table: Table, main_alias: str, join_table: Union[Table, 'SubqueryNode'], join_alias: str) -> ASTNode:
    """创建连接条件，支持多种高级连接类型，确保类型匹配"""
    # 处理子查询作为连接表的情况
    is_subquery_join = hasattr(join_table, 'column_alias_map')
    
    # 获取当前数据库方言
    current_dialect = DBDialectFactory.get_current_dialect()
    
    # 检查当前方言是否支持在ON条件中使用子查询
    supports_subqueries_in_join = True
    if hasattr(current_dialect, 'supports_subqueries_in_join_condition'):
        supports_subqueries_in_join = current_dialect.supports_subqueries_in_join_condition()
    
    # 连接条件类型概率分布
    condition_types = ['fk', 'simple_eq', 'composite', 'range', 'like', 'null_check', 'in_condition', 'exists_condition', 'expression_based']
    weights = [0.1, 0.1, 0.1, 0.05, 0.1, 0.1, 0.15, 0.2, 0.1]
    
    # 对于子查询连接，根据方言支持情况选择条件类型
    if is_subquery_join:
        if supports_subqueries_in_join:
            condition_type = random.choices(['simple_eq', 'in_condition', 'exists_condition', 'expression_based'], 
                                          weights=[0.3, 0.25, 0.25, 0.2], k=1)[0]
        else:
            # 不支持子查询连接条件时，强制使用simple_eq
            condition_type = 'simple_eq'
    else:
        if not supports_subqueries_in_join:
            # 对于非子查询连接，也避免使用包含子查询的条件类型
            condition_types_no_subquery = [t for t in condition_types if t not in ['in_condition', 'exists_condition']]
            weights_no_subquery = [weights[i] for i, t in enumerate(condition_types) if t in condition_types_no_subquery]
            # 归一化权重
            total_weight = sum(weights_no_subquery)
            weights_no_subquery = [w / total_weight for w in weights_no_subquery]
            condition_type = random.choices(condition_types_no_subquery, weights=weights_no_subquery, k=1)[0]
        else:
            condition_type = random.choices(condition_types, weights=weights, k=1)[0]
    
    log_message = f"[连接条件选择] 表1: {main_table.name}({main_alias}), 表2: {'子查询' if is_subquery_join else join_table.name}({join_alias}), 选择的条件类型: {condition_type}\n"
    
    
    
    if condition_type == 'fk' and not is_subquery_join:
        # 尝试找到外键关系
        fk = next((fk for fk in join_table.foreign_keys if fk["ref_table"] == main_table.name), None)
        if fk:
            # 有外键关系，使用外键连接
            # 查找实际列获取正确的数据类型
            fk_column = next((col for col in join_table.columns if col.name == fk["column"]), None)
            ref_column = next((col for col in main_table.columns if col.name == fk["ref_column"]), None)
            
            # 使用实际的列类型而不是硬编码的'numeric'
            left_col = ColumnReferenceNode(
                fk_column if fk_column else Column(
                    fk["column"], 
                    "", 
                    fk.get("data_type", "INT"), 
                    False, 
                    join_table.name
                ),
                join_alias
            )
            right_col = ColumnReferenceNode(
                ref_column if ref_column else Column(
                    fk["ref_column"], 
                    "", 
                    fk.get("ref_data_type", "INT"), 
                    False, 
                    main_table.name
                ),
                main_alias
            )
            condition = ComparisonNode("=")
            condition.add_child(left_col)
            condition.add_child(right_col)
            return condition
        else:
            # 回退到简单相等连接
            log_message = f"[回退] in_condition: 主表中找不到数值类型的列，回退到simple_eq\n"
            condition_type = 'simple_eq'
    
    if condition_type == 'simple_eq':
        # 简单相等连接条件 - 确保类型匹配
        left_col = None
        right_col = None
        
        # 尝试找到兼容的列对
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            
            # 从主表选择随机列
            left_col_candidate = main_table.get_random_column()
            
            # 根据连接表类型选择合适的右侧列
            if is_subquery_join:
                # 从子查询的列别名映射中选择列
                valid_aliases = list(join_table.column_alias_map.keys())
                selected_alias = random.choice(valid_aliases)
                col_name, data_type, category = join_table.column_alias_map[selected_alias]
                
                # 如果主表是orders表且列是user_id，确保子查询列也是数值类型
                if main_table.name == 'orders' and left_col_candidate.name == 'user_id':
                    # 筛选数值类型的子查询列
                    numeric_aliases = [alias for alias, (_, dt, cat) in join_table.column_alias_map.items() 
                                       if cat == 'numeric' or dt.startswith(('INT', 'BIGINT', 'DECIMAL'))]
                    if numeric_aliases:
                        selected_alias = random.choice(numeric_aliases)
                        col_name, data_type, category = join_table.column_alias_map[selected_alias]
                
                # 创建子查询列引用
                right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
            else:
                # 常规表连接
                right_col_candidate = join_table.get_random_column()
            if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
                left_col = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                break
            
            
            attempts += 1
        
        # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
        if left_col is None or right_col is None:
            if is_subquery_join:
                # 子查询连接：遍历主表的每一列，尝试找到类型兼容的子查询列
                for col1 in main_table.columns:
                    # 如果主表是orders表且列是user_id，优先寻找数值类型的子查询列
                    if main_table.name == 'orders' and col1.name == 'user_id':
                        numeric_aliases = [alias for alias, (_, dt, cat) in join_table.column_alias_map.items() 
                                          if cat == 'numeric' or dt.startswith(('INT', 'BIGINT', 'DECIMAL'))]
                        if numeric_aliases:
                            selected_alias = random.choice(numeric_aliases)
                            col_name, data_type, category = join_table.column_alias_map[selected_alias]
                            left_col = ColumnReferenceNode(col1, main_alias)
                            right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
                            right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                            break
                    
                    # 否则尝试所有子查询列
                    for alias, (col_name, data_type, category) in join_table.column_alias_map.items():
                        if is_type_compatible(col1.data_type, data_type):
                            left_col = ColumnReferenceNode(col1, main_alias)
                            right_col_candidate = Column(alias, join_table.alias, data_type, False, join_table.alias)
                            right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                            break
                    if left_col and right_col:
                        break
            else:
                # 常规表连接
                for col1 in main_table.columns:
                    for col2 in join_table.columns:
                        if is_type_compatible(col1.data_type, col2.data_type):
                            left_col = ColumnReferenceNode(col1, main_alias)
                            right_col = ColumnReferenceNode(col2, join_alias)
                            break
                    if left_col and right_col:
                        break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col is None or right_col is None:
                left_col = ColumnReferenceNode(main_table.columns[0], main_alias)
                if is_subquery_join:
                    # 子查询连接：为数值类型的主表列选择数值类型的子查询列
                    if main_table.columns[0].category == 'numeric':
                        numeric_aliases = [alias for alias, (_, dt, cat) in join_table.column_alias_map.items() 
                                          if cat == 'numeric' or dt.startswith(('INT', 'BIGINT', 'DECIMAL'))]
                        if numeric_aliases:
                            selected_alias = random.choice(numeric_aliases)
                            col_name, data_type, category = join_table.column_alias_map[selected_alias]
                            right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
                            right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                        else:
                            # 如果子查询没有数值列，使用数值字面量
                            right_col = create_compatible_literal(main_table.columns[0].data_type)
                    else:
                        # 非数值类型，选择任意子查询列
                        valid_aliases = list(join_table.column_alias_map.keys())
                        selected_alias = random.choice(valid_aliases)
                        col_name, data_type, category = join_table.column_alias_map[selected_alias]
                        right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
                        right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                else:
                    # 对于数值类型，使用数值字面量而不是列引用
                    if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                        right_col = create_compatible_literal(main_table.columns[0].data_type)
                    else:
                        right_col = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        condition = ComparisonNode("=")
        condition.add_child(left_col)
        condition.add_child(right_col)
        return condition
    
    elif condition_type == 'composite':
        # 复合连接条件（AND组合）
        composite = LogicalNode("AND")
        
        # 第一个条件（相等连接）- 确保类型匹配
        left_col1 = None
        right_col1 = None
        
        # 尝试找到兼容的列对
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            left_col_candidate = main_table.get_random_column()
            right_col_candidate = join_table.get_random_column()
            
            if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
                left_col1 = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col1 = ColumnReferenceNode(right_col_candidate, join_alias)
                break
            
            attempts += 1
        
        # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
        if left_col1 is None or right_col1 is None:
            for col1 in main_table.columns:
                for col2 in join_table.columns:
                    if is_type_compatible(col1.data_type, col2.data_type):
                        left_col1 = ColumnReferenceNode(col1, main_alias)
                        right_col1 = ColumnReferenceNode(col2, join_alias)
                        break
                if left_col1 and right_col1:
                    break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col1 is None or right_col1 is None:
                left_col1 = ColumnReferenceNode(main_table.columns[0], main_alias)
                # 对于数值类型，使用数值字面量而不是列引用
                if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                    right_col1 = create_compatible_literal(main_table.columns[0].data_type)
                else:
                    right_col1 = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        cond1 = ComparisonNode("=")
        cond1.add_child(left_col1)
        cond1.add_child(right_col1)
        composite.add_child(cond1)
        
        # 第二个条件（可能是其他比较操作）- 确保类型匹配
        operators = ["<", ">", "<=", ">=", "!="]
        op = random.choice(operators)
        left_col2 = None
        right_col2 = None
        
        attempts = 0
        while attempts < max_attempts:
            left_col_candidate = main_table.get_random_column()
            right_col_candidate = join_table.get_random_column()
            
            if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
                left_col2 = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col2 = ColumnReferenceNode(right_col_candidate, join_alias)
                break
            
            attempts += 1
        
        # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
        if left_col2 is None or right_col2 is None:
            for col1 in main_table.columns:
                for col2 in join_table.columns:
                    if is_type_compatible(col1.data_type, col2.data_type):
                        left_col2 = ColumnReferenceNode(col1, main_alias)
                        right_col2 = ColumnReferenceNode(col2, join_alias)
                        break
                if left_col2 and right_col2:
                    break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col2 is None or right_col2 is None:
                left_col2 = ColumnReferenceNode(main_table.columns[0], main_alias)
                # 对于数值类型，使用数值字面量而不是列引用
                if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                    right_col2 = create_compatible_literal(main_table.columns[0].data_type)
                else:
                    right_col2 = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        cond2 = ComparisonNode(op)
        cond2.add_child(left_col2)
        cond2.add_child(right_col2)
        composite.add_child(cond2)
        return composite
    
    elif condition_type == 'range':
        # 范围连接条件（使用ComparisonNode的BETWEEN操作符）- 确保类型匹配
        left_col = None
        right_col_low = None
        right_col_high = None
        
        # 尝试找到兼容的列
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            left_col_candidate = main_table.get_random_column()
            
            # 优先选择数值或日期类型的列
            if left_col_candidate.category not in ['numeric', 'datetime']:
                attempts += 1
                continue
            
            # 寻找与左侧列类型兼容的两个右侧列
            right_col_low_candidate = None
            right_col_high_candidate = None
            
            # 先找到一个与左侧列类型兼容的列
            for _ in range(5):
                candidate = join_table.get_random_column()
                if is_type_compatible(left_col_candidate.data_type, candidate.data_type):
                    right_col_low_candidate = candidate
                    break
            
            if right_col_low_candidate:
                # 再找另一个与左侧列类型兼容的列（可以是同一个列）
                for _ in range(5):
                    candidate = join_table.get_random_column()
                    if is_type_compatible(left_col_candidate.data_type, candidate.data_type):
                        right_col_high_candidate = candidate
                        break
            
            if right_col_low_candidate and right_col_high_candidate:
                left_col = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col_low = ColumnReferenceNode(right_col_low_candidate, join_alias)
                right_col_high = ColumnReferenceNode(right_col_high_candidate, join_alias)
                break
            
            attempts += 1
        
        # 如果找不到兼容的列，查找两个表中第一个兼容的列对
        if left_col is None or right_col_low is None or right_col_high is None:
            # 尝试为left_col找到兼容的列
            for col1 in main_table.columns:
                for col2 in join_table.columns:
                    if is_type_compatible(col1.data_type, col2.data_type):
                        left_col = ColumnReferenceNode(col1, main_alias)
                        right_col_low = ColumnReferenceNode(col2, join_alias)
                        # 尝试找到另一个兼容的列或使用同一个列
                        for col3 in join_table.columns:
                            if is_type_compatible(col1.data_type, col3.data_type):
                                right_col_high = ColumnReferenceNode(col3, join_alias)
                                break
                        if right_col_high is None:
                            right_col_high = right_col_low
                        break
                if left_col and right_col_low and right_col_high:
                    break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col is None or right_col_low is None or right_col_high is None:
                left_col = ColumnReferenceNode(main_table.columns[0], main_alias)
                # 对于不可比较或不兼容的类型，使用字面量而不是列引用
                left_fallback_category = get_safe_comparison_category(main_table.columns[0])
                right_fallback_category = get_safe_comparison_category(join_table.columns[0])
                if left_fallback_category in ORDERABLE_CATEGORIES and left_fallback_category != right_fallback_category:
                    right_col_low = create_compatible_literal(main_table.columns[0].data_type)
                    right_col_high = create_compatible_literal(main_table.columns[0].data_type)
                else:
                    right_col_low = ColumnReferenceNode(join_table.columns[0], join_alias)
                    right_col_high = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        # 创建BETWEEN表达式
        between_expr = ComparisonNode("BETWEEN")
        between_expr.add_child(left_col)
        between_expr.add_child(right_col_low)
        between_expr.add_child(right_col_high)
        return between_expr
    
    elif condition_type == 'like':
        # LIKE模式匹配连接
        # 选择合适的字符串列
        string_col_main = None
        for col in main_table.columns:
            if col.data_type.startswith('VARCHAR') or col.data_type == 'TEXT':
                string_col_main = col
                break
        
        string_col_join = None
        for col in join_table.columns:
            if col.data_type.startswith('VARCHAR') or col.data_type == 'TEXT':
                string_col_join = col
                break
        
        if string_col_main and string_col_join:
            left_col = ColumnReferenceNode(string_col_main, main_alias)
            right_col = ColumnReferenceNode(string_col_join, join_alias)
            condition = ComparisonNode("LIKE")
            condition.add_child(left_col)
            condition.add_child(right_col)
            return condition
        else:
            # 回退到简单相等连接
            condition_type = 'simple_eq'
            left_col = ColumnReferenceNode(
                main_table.get_random_column(),
                main_alias
            )
            right_col = ColumnReferenceNode(
                join_table.get_random_column(),
                join_alias
            )
            condition = ComparisonNode("=")
            condition.add_child(left_col)
            condition.add_child(right_col)
            return condition
    
    elif condition_type == 'null_check':
        # NULL检查连接条件
        composite = LogicalNode("AND")
        
        # 第一个条件（相等连接）- 确保类型匹配
        left_col1 = None
        right_col1 = None
        
        # 尝试找到兼容的列对
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            left_col_candidate = main_table.get_random_column()
            right_col_candidate = join_table.get_random_column()
            
            if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
                left_col1 = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col1 = ColumnReferenceNode(right_col_candidate, join_alias)
                break
            
            attempts += 1
        
        # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
        if left_col1 is None or right_col1 is None:
            for col1 in main_table.columns:
                for col2 in join_table.columns:
                    if is_type_compatible(col1.data_type, col2.data_type):
                        left_col1 = ColumnReferenceNode(col1, main_alias)
                        right_col1 = ColumnReferenceNode(col2, join_alias)
                        break
                if left_col1 and right_col1:
                    break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col1 is None or right_col1 is None:
                left_col1 = ColumnReferenceNode(main_table.columns[0], main_alias)
                # 对于数值类型，使用数值字面量而不是列引用
                if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                    right_col1 = create_compatible_literal(main_table.columns[0].data_type)
                else:
                    right_col1 = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        cond1 = ComparisonNode("=")
        cond1.add_child(left_col1)
        cond1.add_child(right_col1)
        composite.add_child(cond1)
        
        # 第二个条件（NULL检查）
        null_col = ColumnReferenceNode(
            join_table.get_random_column(),
            join_alias
        )
        # 使用ComparisonNode的IS NULL/IS NOT NULL操作符
        null_op = "IS NOT NULL" if random.random() > 0.5 else "IS NULL"
        cond2 = ComparisonNode(null_op)
        cond2.add_child(null_col)
        composite.add_child(cond2)
          
        return composite
    
    elif condition_type == 'in_condition':
        # IN子查询连接条件
        # 优先使用数值类型的列
        numeric_cols_main = [col for col in main_table.columns if col.category == 'numeric']
        if numeric_cols_main:
            selected_col_main = random.choice(numeric_cols_main)
            left_col = ColumnReferenceNode(selected_col_main, main_alias)
            
            # 创建IN条件
            in_condition = ComparisonNode('IN')
            in_condition.add_child(left_col)
            
            # 创建子查询
            subquery = SelectNode()
            
            # 为子查询选择表和列
            if is_subquery_join:
                # 对于子查询连接，使用join_table作为子查询表
                subquery_table = join_table
                # 生成安全的表别名
                sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
                base_alias = 'sub' + str(random.randint(0, 99))
                sub_alias = base_alias if base_alias not in sql_keywords else base_alias + str(random.randint(0, 9))
            else:
                # 对于常规表连接，使用join_table
                subquery_table = join_table
                # 生成安全的表别名
                sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
                base_alias = subquery_table.name[:3].lower()
                sub_alias = base_alias if base_alias not in sql_keywords else base_alias + str(random.randint(0, 9))
            
            # 创建FROM子句
            sub_from = FromNode()
            sub_from.add_table(subquery_table, sub_alias)
            subquery.set_from_clause(sub_from)
            
            # 从子查询表中选择类型兼容的列
            compatible_cols = [col for col in subquery_table.columns if is_type_compatible(selected_col_main.data_type, col.data_type)]
            if compatible_cols:
                selected_col_sub = random.choice(compatible_cols)
                sub_col_ref = ColumnReferenceNode(selected_col_sub, sub_alias)
                subquery.add_select_expression(sub_col_ref, selected_col_sub.name)
            else:
                # 如果没有兼容的列，使用第一个数值类型的列
                numeric_cols_sub = [col for col in subquery_table.columns if col.category == 'numeric']
                if numeric_cols_sub:
                    selected_col_sub = random.choice(numeric_cols_sub)
                    sub_col_ref = ColumnReferenceNode(selected_col_sub, sub_alias)
                    subquery.add_select_expression(sub_col_ref, selected_col_sub.name)
                else:
                        # 回退到简单相等连接
                        condition_type = 'simple_eq'
                    
            
            # 为子查询添加WHERE条件以限制结果数量
            if selected_col_sub.category == 'numeric':
                where_cond = ComparisonNode('BETWEEN')
                where_cond.add_child(ColumnReferenceNode(selected_col_sub, sub_alias))
                where_cond.add_child(LiteralNode(1, selected_col_sub.data_type))
                where_cond.add_child(LiteralNode(100, selected_col_sub.data_type))
                subquery.set_where_clause(where_cond)
            
            # 将子查询添加到IN条件
            in_condition.add_child(SubqueryNode(subquery, ''))
            return in_condition
        else:
            # 回退到简单相等连接
            condition_type = 'simple_eq'
    
    elif condition_type == 'exists_condition':
        # EXISTS子查询连接条件
        exists_node = ComparisonNode('EXISTS')
        
        # 创建相关子查询
        subquery = SelectNode()
        
        # 选择关联表
        if is_subquery_join:
            # 对于子查询连接，使用join_table作为关联表
            rel_table = join_table
            # 生成安全的表别名
            sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
            base_rel_alias = 'rel' + str(random.randint(0, 99))
            rel_alias = base_rel_alias if base_rel_alias not in sql_keywords else base_rel_alias + str(random.randint(0, 9))
        else:
            # 对于常规表连接，使用join_table
            rel_table = join_table
            # 生成安全的表别名
            sql_keywords = {'use', 'select', 'from', 'where', 'group', 'by', 'order', 'limit', 'join', 'on', 'as'}
            base_rel_alias = rel_table.name[:3].lower()
            rel_alias = base_rel_alias if base_rel_alias not in sql_keywords else base_rel_alias + str(random.randint(0, 9))
        
        # 创建FROM子句
        sub_from = FromNode()
        sub_from.add_table(rel_table, rel_alias)
        subquery.set_from_clause(sub_from)
        
        # 选择一个列用于SELECT
        rel_col = rel_table.get_random_column()
        if rel_col:
            sub_col_ref = ColumnReferenceNode(rel_col, rel_alias)
            subquery.add_select_expression(sub_col_ref, rel_col.name)
        else:
            # 回退到简单相等连接
            condition_type = 'simple_eq'
            
        
        # 创建WHERE关联条件
        where_cond = LogicalNode('AND')
        
        # 找到可用于关联的列对
        max_attempts = 10  # 增加尝试次数
        attempts = 0
        join_condition_created = False
        
        # 首先尝试找到类型兼容的列对（优化策略）
        compatible_pairs_found = False
        compatible_pairs = []
        
        # 预处理：找出所有类型兼容的列对
        if not is_subquery_join:
            for main_col in main_table.columns:
                for rel_col in rel_table.columns:
                    if is_type_compatible(main_col.data_type, rel_col.data_type):
                        compatible_pairs.append((main_col, rel_col))
        
        if compatible_pairs:
            compatible_pairs_found = True
        else:
            pass
        
        while attempts < max_attempts:
            if compatible_pairs_found and attempts < len(compatible_pairs):
                # 使用预处理好的兼容列对
                main_col_candidate, rel_col_candidate = compatible_pairs[attempts]
            else:
                # 随机选择列
                main_col_candidate = main_table.get_random_column()
                rel_col_candidate = rel_table.get_random_column()
            
            if is_type_compatible(main_col_candidate.data_type, rel_col_candidate.data_type):
                # 创建相等比较条件
                eq_cond = ComparisonNode('=')
                eq_cond.add_child(ColumnReferenceNode(main_col_candidate, main_alias))
                eq_cond.add_child(ColumnReferenceNode(rel_col_candidate, rel_alias))
                where_cond.add_child(eq_cond)
                join_condition_created = True
                break
            
            attempts += 1
        
        if not join_condition_created:
            # 回退到简单相等连接
            log_message = f"[回退] exists_condition: 找不到可用于关联的列对（尝试了{attempts}次），回退到simple_eq\n"
            condition_type = 'simple_eq'
            # 设置一个标记，表示需要重新处理
            needs_retry = True
        else:
            needs_retry = False
        
        if needs_retry:
            # 如果需要重新处理，设置条件类型为simple_eq
            # 后面的默认回退逻辑会处理这种情况
            pass
        else:
            # 设置子查询的WHERE子句
            subquery.set_where_clause(where_cond)
            
            # 将子查询添加到EXISTS条件
            exists_node.add_child(SubqueryNode(subquery, ''))
            return exists_node
    
    elif condition_type == 'expression_based':
        # 基于表达式的连接（非相等操作符）
        # 选择操作符
        operators = ['<', '>', '<=', '>=', '!=']
        op = random.choice(operators)
        
        left_col = None
        right_col = None
        
        # 尝试找到兼容的列对
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts:
            left_col_candidate = main_table.get_random_column()
            
            if is_subquery_join:
                # 子查询连接：从子查询的列别名映射中选择列
                valid_aliases = list(join_table.column_alias_map.keys())
                selected_alias = random.choice(valid_aliases)
                col_name, data_type, category = join_table.column_alias_map[selected_alias]
                right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
            else:
                # 常规表连接
                right_col_candidate = join_table.get_random_column()

            left_category = get_safe_comparison_category(left_col_candidate)
            right_category = get_safe_comparison_category(right_col_candidate)
            if left_category not in ORDERABLE_CATEGORIES or right_category not in ORDERABLE_CATEGORIES:
                attempts += 1
                continue

            if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
                left_col = ColumnReferenceNode(left_col_candidate, main_alias)
                right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                break
            
            attempts += 1
        
        # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
        if left_col is None or right_col is None:
            if is_subquery_join:
                # 子查询连接
                for col1 in main_table.columns:
                    for alias, (col_name, data_type, category) in join_table.column_alias_map.items():
                        left_category = get_safe_comparison_category(col1)
                        right_category = normalize_category(category, data_type)
                        if str(data_type).lower() in ['any', 'unknown']:
                            continue
                        if left_category not in ORDERABLE_CATEGORIES or right_category not in ORDERABLE_CATEGORIES:
                            continue
                        if is_type_compatible(col1.data_type, data_type):
                            left_col = ColumnReferenceNode(col1, main_alias)
                            right_col_candidate = Column(alias, join_table.alias, data_type, False, join_table.alias)
                            right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                            break
                    if left_col and right_col:
                        break
            else:
                # 常规表连接
                for col1 in main_table.columns:
                    for col2 in join_table.columns:
                        left_category = get_safe_comparison_category(col1)
                        right_category = get_safe_comparison_category(col2)
                        if left_category not in ORDERABLE_CATEGORIES or right_category not in ORDERABLE_CATEGORIES:
                            continue
                        if is_type_compatible(col1.data_type, col2.data_type):
                            left_col = ColumnReferenceNode(col1, main_alias)
                            right_col = ColumnReferenceNode(col2, join_alias)
                            break
                    if left_col and right_col:
                        break
            
            # 万不得已才使用表的第一个列，但尝试进行类型转换
            if left_col is None or right_col is None:
                left_category = get_safe_comparison_category(main_table.columns[0])
                if left_category not in ORDERABLE_CATEGORIES:
                    condition_type = 'simple_eq'
                left_col = ColumnReferenceNode(main_table.columns[0], main_alias)
                if is_subquery_join:
                    # 子查询连接
                    valid_aliases = list(join_table.column_alias_map.keys())
                    selected_alias = random.choice(valid_aliases)
                    col_name, data_type, category = join_table.column_alias_map[selected_alias]
                    right_col_candidate = Column(selected_alias, join_table.alias, data_type, False, join_table.alias)
                    right_col = ColumnReferenceNode(right_col_candidate, join_alias)
                else:
                    # 对于数值类型，使用数值字面量而不是列引用
                    if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                        right_col = create_compatible_literal(main_table.columns[0].data_type)
                    else:
                        right_col = ColumnReferenceNode(join_table.columns[0], join_alias)
        
        if left_col is None or right_col is None or condition_type == 'simple_eq':
            pass
        else:
            condition = ComparisonNode(op)
            condition.add_child(left_col)
            condition.add_child(right_col)
            return condition
          
    # 默认回退到简单相等连接 - 确保类型匹配
    left_col = None
    right_col = None
    
    # 尝试找到兼容的列对
    max_attempts = 10
    attempts = 0
    
    while attempts < max_attempts:
        left_col_candidate = main_table.get_random_column()
        right_col_candidate = join_table.get_random_column()
        
        if is_type_compatible(left_col_candidate.data_type, right_col_candidate.data_type):
            left_col = ColumnReferenceNode(left_col_candidate, main_alias)
            right_col = ColumnReferenceNode(right_col_candidate, join_alias)
            break
        
        attempts += 1
    
    # 如果找不到兼容的列对，查找两个表中第一个兼容的列对
    if left_col is None or right_col is None:
        for col1 in main_table.columns:
            for col2 in join_table.columns:
                if is_type_compatible(col1.data_type, col2.data_type):
                    left_col = ColumnReferenceNode(col1, main_alias)
                    right_col = ColumnReferenceNode(col2, join_alias)
                    break
            if left_col and right_col:
                break
        
        # 万不得已才使用表的第一个列，但尝试进行类型转换
        if left_col is None or right_col is None:
            left_col = ColumnReferenceNode(main_table.columns[0], main_alias)
            # 对于数值类型，使用数值字面量而不是列引用
            if main_table.columns[0].category == 'numeric' and join_table.columns[0].category != 'numeric':
                right_col = create_compatible_literal(main_table.columns[0].data_type)
            else:
                right_col = ColumnReferenceNode(join_table.columns[0], join_alias)
    
    condition = ComparisonNode("=")
    condition.add_child(left_col)
    condition.add_child(right_col)
    
    # 连接条件创建完成
    
    return condition
