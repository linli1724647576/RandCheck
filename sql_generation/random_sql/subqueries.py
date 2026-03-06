import random
from typing import List, Optional

from ast_nodes import (
    ColumnReferenceNode,
    FromNode,
    FunctionCallNode,
    LimitNode,
    LiteralNode,
    OrderByNode,
    SelectNode,
    SubqueryNode,
)
from data_structures.function import Function
from data_structures.table import Table
from sql_generation.random_sql.geometry import (
    create_geometry_literal_node,
    create_wkb_literal_node,
    is_geometry_type,
)

def create_select_subquery(tables: List[Table], functions: List[Function], 
                          current_depth: int = 0, max_depth: int = 2) -> SubqueryNode:
    """创建SELECT子句中的子查询表达式
    
    Args:
        tables: 可用的表列表
        functions: 可用的函数列表
        current_depth: 当前子查询深度
        max_depth: 最大允许的子查询深度
    
    Returns:
        SubqueryNode: 可用于SELECT子句的子查询节点
    """
    if current_depth >= max_depth or not tables:
        return None
    
    # 创建子查询的SELECT节点
    subquery_select = SelectNode()
    subquery_select.tables = tables
    subquery_select.functions = functions
    
    # 为子查询选择一个表
    subquery_table = random.choice(tables)
    
    # 生成子查询的FROM子句
    subquery_from = FromNode()
    subquery_inner_alias = 's' + str(random.randint(100, 999))
    subquery_from.add_table(subquery_table, subquery_inner_alias)
    subquery_select.set_from_clause(subquery_from)
    
    # 为子查询生成SELECT表达式（简单的单列查询）
    subquery_col = subquery_table.get_random_column()

    subquery_expr = ColumnReferenceNode(subquery_col, subquery_inner_alias)
    
    # 50%概率在子查询中使用聚合函数
    is_aggregate = False
    if random.random() > 0.5 and functions:
        agg_funcs = [f for f in functions if f.func_type == 'aggregate']
        if agg_funcs:
            func = random.choice(agg_funcs)
            func_node = FunctionCallNode(func)
            
            current_params = 0
            min_params = func.min_params
            max_params = func.max_params
            
            if max_params is None:
                max_params = current_params + 3
            
            def build_param(param_type: str):
                param_type = (param_type or "").lower()
                if param_type in ["column", "any", "unknown"]:
                    return subquery_expr
                if param_type == "binary":
                    geom_cols = [col for col in subquery_table.columns if is_geometry_type(col.data_type)]
                    if geom_cols:
                        return ColumnReferenceNode(random.choice(geom_cols), subquery_inner_alias)
                    if "FromWKB" in func.name:
                        return create_wkb_literal_node(func.name)
                    return create_geometry_literal_node(func.name)
                if param_type == "numeric":
                    return LiteralNode(random.randint(1, 100), "INT")
                if param_type == "string":
                    return LiteralNode(f"sample_{random.randint(1, 100)}", "STRING")
                if param_type == "datetime":
                    return LiteralNode("2023-01-01 12:00:00", "DATETIME")
                if param_type == "json":
                    return LiteralNode('{"type":"Point","coordinates":[0,0]}', "JSON")
                if param_type == "boolean":
                    return LiteralNode(random.choice([True, False]), "BOOLEAN")
                return subquery_expr
            
            while current_params < min_params or (current_params < max_params and random.random() > 0.5):
                if current_params < len(func.param_types):
                    param_type = func.param_types[current_params]
                else:
                    param_type = func.param_types[0] if func.param_types else "column"
            
                new_param = build_param(param_type)
                func_node.add_child(new_param)
                current_params += 1
            
            subquery_expr = func_node
            is_aggregate = True
    
    # 确保子查询选择列使用子查询自身的别名，避免引用外层别名
    if isinstance(subquery_expr, ColumnReferenceNode):
        subquery_expr = ColumnReferenceNode(subquery_col, subquery_inner_alias)
    elif hasattr(subquery_expr, 'repair_columns'):
        subquery_expr.repair_columns(subquery_from)

    subquery_select.add_select_expression(subquery_expr, 'subq_col')
    
    # 当子查询不是聚合函数时，强行添加LIMIT 1子句和ORDER BY子句
    if not is_aggregate:
        # 添加ORDER BY子句，使用select子句中的表达式
        from ast_nodes.order_by_node import OrderByNode
        # 获取select子句中的第一个表达式作为排序依据
        if hasattr(subquery_select, 'select_expressions') and subquery_select.select_expressions:
            order_by_node = OrderByNode()
            order_by_node.add_expression(ColumnReferenceNode(subquery_col, subquery_inner_alias))
            subquery_select.set_order_by_clause(order_by_node)

        # 添加LIMIT 1子句
        from ast_nodes.limit_node import LimitNode
        limit_node = LimitNode(1)
        subquery_select.set_limit_clause(limit_node)
    
    
    # 创建子查询节点（使用空字符串作为别名，确保不会添加AS子句）
    subquery_node = SubqueryNode(subquery_select, '')
    
    # 设置子查询深度信息
    subquery_node.metadata['depth'] = current_depth + 1
    
    # 验证子查询的有效性
    valid, errors = subquery_select.validate_all_columns()
    if not valid:
        # 尝试修复无效的列引用
        subquery_select.repair_invalid_columns()
        # 再次验证
        valid, errors = subquery_select.validate_all_columns()
        
        # 如果仍然无效，返回None
        if not valid:
            return None
    
    return subquery_node
