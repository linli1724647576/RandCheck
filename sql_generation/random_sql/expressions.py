import random
import re
from typing import List, Optional

from ast_nodes import (
    ASTNode,
    ColumnReferenceNode,
    ComparisonNode,
    FunctionCallNode,
    FromNode,
    LiteralNode,
    LogicalNode,
    SubqueryNode,
)
from data_structures.function import Function
from data_structures.table import Table
from data_structures.column import Column
from sql_generation.random_sql.column_tracker import ColumnUsageTracker, get_random_column_with_tracker
from sql_generation.random_sql.geometry import (
    create_geohash_literal_node,
    create_geojson_literal_node,
    create_geometry_literal_node,
    create_wkb_literal_node,
    create_wkt_literal_node,
    get_required_geometry_type,
    is_geojson_function,
    is_geohash_function,
    is_geometry_type,
    is_wkt_function,
)
from sql_generation.random_sql.subqueries import create_select_subquery
from sql_generation.random_sql.type_utils import (
    ORDERABLE_CATEGORIES,
    adjust_expected_type_for_conditionals,
    adjust_expected_type_for_min_max,
    get_cast_types,
    get_comparison_operators,
    map_param_type_to_category,
    map_return_type_to_category,
    normalize_category,
)

def _matches_required_geometry(col: Column, required: Optional[str]) -> bool:
    if not required:
        return True
    data_type = str(getattr(col, "data_type", "")).upper()
    if required in data_type:
        return True
    return data_type == "GEOMETRY"

def is_type_compatible(type1, type2):
    """检查两个数据类型是否兼容，允许数值类型间的转换
    
    参数:
    - type1: 第一个数据类型
    - type2: 第二个数据类型
    
    返回:
    - bool: 如果两个类型兼容则返回True，否则返回False
    """
    # 使用列的category属性来判断类型兼容性
    # 注意：当type1和type2是Column对象时，直接使用它们的category属性
    # 当type1和type2是字符串时，这表示它们是数据类型名称，我们需要从表结构中查找对应的列
    # 但由于这个函数通常在已经有Column对象的上下文中使用，我们优先使用category属性
    
    # 实际项目中应该从TABLES全局变量中查找列信息
    # 但为了向后兼容，我们保留了基于数据类型字符串的判断逻辑
    
    # 定义类型兼容性规则
    numeric_types = {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}
    string_types = {'VARCHAR', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT'}
    datetime_types = {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}
    
    # 提取基本类型（去掉括号和长度信息）
    base_type1 = type1.split('(')[0].upper() if type1 else 'UNKNOWN'
    base_type2 = type2.split('(')[0].upper() if type2 else 'UNKNOWN'
    
    # 放宽类型匹配规则
    # 1. 相同类型组的类型视为兼容
    if (base_type1 in numeric_types and base_type2 in numeric_types) or \
       (base_type1 in string_types and base_type2 in string_types) or \
       (base_type1 in datetime_types and base_type2 in datetime_types):
        return True
    
    # 2. 完全相同的类型视为兼容
    if base_type1 == base_type2:
        return True
    
    return False

def create_compatible_literal(data_type):
    """创建兼容类型的字面量，确保类型匹配且引号处理正确"""
    base_type = data_type.split('(')[0].upper()
    
    if base_type in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT'}:
        # 整数类型：返回随机整数值
        return LiteralNode(random.randint(1, 1000), data_type)
    elif base_type in {'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}:
        # 数值类型：返回随机浮点值
        return LiteralNode(round(random.uniform(1.0, 100.0), 2), data_type)
    elif base_type in {'VARCHAR', 'CHAR', 'TEXT', 'STRING'}:
        # 字符串类型：返回纯文本字符串，明确使用'STRING'类型确保引号被正确添加
        return LiteralNode(f"sample_{random.randint(1, 100)}", 'STRING')
    elif base_type == 'DATE':
        # 日期类型：返回日期格式的字符串
        return LiteralNode(f"2023-01-01", data_type)
    elif base_type in {'DATETIME', 'TIMESTAMP','timestamp'}:
        # 日期时间类型：返回日期时间格式的字符串
        return LiteralNode(f"2023-01-01 12:00:00", data_type)
    elif base_type == 'BOOLEAN':
        # 布尔类型：返回布尔值
        return LiteralNode(random.choice([True, False]), data_type)

    elif base_type == 'SET':
        # SET类型：从允许的值中随机选择一个
        set_values = re.findall(r"'([^']+)',?", data_type)
        if set_values:
            return LiteralNode("'" + random.choice(set_values) + "'", data_type)
        else:
            return LiteralNode("''", data_type)
    elif base_type == 'ENUM':
        # ENUM类型：从枚举值中随机选择一个
        enum_values = re.findall(r"'([^']+)',?", data_type)
        if enum_values:
            return LiteralNode("'" + random.choice(enum_values) + "'", data_type)
        else:
            return LiteralNode("NULL", data_type)
    elif base_type == 'BIT':
        # BIT类型：生成随机位值
        bit_count = re.search(r"BIT\((\d+)\)", data_type)
        if bit_count:
            bit_count = int(bit_count.group(1))
            max_value = 2 ** bit_count - 1
            return LiteralNode("b'" + bin(random.randint(0, max_value))[2:].zfill(bit_count) + "'", data_type)
        else:
            return LiteralNode("b'0'", data_type)
    elif base_type == 'YEAR':
        # YEAR类型：生成2000-2023之间的随机年份
        return LiteralNode(str(random.randint(2000, 2023)), data_type)
    elif base_type in {'GEOMETRY', 'POINT'}:
        # 几何类型：生成简单的点坐标
        lat = round(random.uniform(-90, 90), 6)
        lng = round(random.uniform(-180, 180), 6)
        return LiteralNode(f"ST_GeomFromText('POINT({lat} {lng})')", data_type)
    elif base_type in ['BLOB', 'TINYBLOB', 'MEDIUMBLOB', 'LONGBLOB']:
        # BLOB类型：生成随机二进制数据（用十六进制表示）
        # 为避免在MariaDB中出现无效的utf8mb4字符错误，使用CONVERT函数确保正确处理
        blob_size = random.randint(1, 510)  # 限制在510字节以内以符合聚合函数要求
        hex_data = ''.join(random.choice('0123456789ABCDEF') for _ in range(blob_size * 2))
        return LiteralNode(f"X'{hex_data}", data_type)
    else:
        # 默认情况：返回整数
        return LiteralNode(random.randint(1, 100), 'INT')

def ensure_boolean_expression(expr: ASTNode, tables: List[Table], functions: List[Function], 
                           from_node: FromNode, main_table: Table, main_alias: str, 
                           join_table: Optional[Table] = None, join_alias: Optional[str] = None) -> ASTNode:
    """确保表达式是布尔类型，如果不是则转换为布尔表达式"""
    # 如果表达式本身就是比较表达式或逻辑表达式，则认为它是布尔类型
    if isinstance(expr, (ComparisonNode, LogicalNode)):
        return expr
    
    # 对于其他类型的表达式（如列引用、函数调用等），将其转换为布尔表达式
    # 选择表和列
    
    if isinstance(expr, FunctionCallNode):
        # 使用传入的表达式作为比较列，而不是从表中随机获取
        col = expr
        return_type = expr.metadata.get('return_type', '').upper()
        
        # 对于返回类型为any的函数，特殊处理CAST和CONVERT函数
        if return_type == 'ANY' and expr.children:
            # 已在文件顶部导入所有必要的类，无需局部导入
            
            # 检查是否是CAST或CONVERT函数
            if expr.function.name in ['CAST', 'CONVERT'] and len(expr.children) == 2:
                # 对于CAST和CONVERT函数，返回类型由第二个参数（目标数据类型）决定
                second_param = expr.children[1]
                if isinstance(second_param, LiteralNode):
                    # 获取目标数据类型
                    target_type = second_param.upper()
                    param_type = None
                    
                    # 根据目标数据类型设置返回类型
                    if target_type in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}:
                        param_type = 'numeric'
                    elif target_type in {'VARCHAR', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT', 'STRING', 'CHARACTER'}:
                        param_type = 'string'
                    elif target_type in {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}:
                        param_type = 'datetime'
                    elif target_type in {'BOOLEAN', 'BOOL'}:
                        param_type = 'boolean'
                    elif target_type in {'JSON'}:
                        param_type = 'json'
                    elif target_type in {'BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB'}:
                        param_type = 'binary'
                    elif target_type in {'BOOLEAN', 'BOOL'}:
                        param_type = 'boolean'
                    else:
                        param_type = 'string'
            # 对于其他返回类型为any的函数（如max、min），获取第一个参数的类型
            else:
                first_param = expr.children[0]
                param_type = None
            
                if isinstance(first_param, ColumnReferenceNode):
                    # 如果第一个参数是列引用，直接使用列的类型
                    param_type = first_param.column.category
                    if param_type in ['int', 'float', 'decimal']:
                        param_type = 'numeric'
                elif isinstance(first_param, LiteralNode):
                    # 如果第一个参数是字面量，根据数据类型判断
                    if hasattr(first_param, 'data_type'):
                        data_type = first_param.data_type.upper()
                        if data_type in ['INT', 'FLOAT', 'DECIMAL', 'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT']:
                            param_type = 'numeric'
                        elif data_type in ['VARCHAR', 'STRING', 'CHAR', 'TEXT']:
                            param_type = 'string'
                        elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP', 'TIME']:
                            param_type = 'datetime'
                        elif data_type in ['JSON']:
                            param_type = 'json'
                        elif data_type in ['BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON']:
                            param_type = 'binary'
                        elif data_type in ['BOOLEAN', 'BOOL']:
                            param_type = 'boolean'
                elif isinstance(first_param, FunctionCallNode):
                    # 如果第一个参数是函数调用，使用其返回类型
                    param_func_return_type = first_param.metadata.get('return_type', '').upper()
                    if param_func_return_type in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}:
                        param_type = 'numeric'
                    elif param_func_return_type in {'VARCHAR', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT'}:
                        param_type = 'string'
                    elif param_func_return_type in {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}:
                        param_type = 'datetime'
                    elif param_func_return_type in {'JSON'}:
                        param_type = 'json'
                    elif param_func_return_type in {'BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON'}:
                        param_type = 'binary'
                    elif param_func_return_type in {'BOOLEAN', 'BOOL'}:
                        param_type = 'boolean'
                    elif param_func_return_type in {'ANY'}:
                        # 如果内部函数返回类型也是ANY，递归获取其第一个参数的类型
                        if first_param.children:
                            inner_first_param = first_param.children[0]
                            if isinstance(inner_first_param, ColumnReferenceNode):
                                # 如果内部第一个参数是列引用，直接使用列的类型
                                param_type = inner_first_param.column.category
                                if param_type in ['int', 'float', 'decimal']:
                                    param_type = 'numeric'
                                elif param_type in ['string', 'char', 'text']:
                                    param_type = 'string'
                                elif param_type in ['datetime', 'time']:
                                    param_type = 'datetime'
                                elif param_type in ['binary']:
                                    param_type = 'binary'
                                elif param_type in ['json']:
                                    param_type = 'json'
                                elif param_type in ['boolean', 'bool']:
                                    param_type = 'boolean'
                                
                            elif isinstance(inner_first_param, LiteralNode):
                                # 如果内部第一个参数是字面量，根据数据类型判断
                                if hasattr(inner_first_param, 'data_type'):
                                    data_type = inner_first_param.data_type.upper()
                                    if data_type in ['INT', 'FLOAT', 'DECIMAL', 'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT']:
                                        param_type = 'numeric'
                                    elif data_type in ['VARCHAR', 'STRING', 'CHAR', 'TEXT']:
                                        param_type = 'string'
                                    elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP', 'TIME']:
                                        param_type = 'datetime'
                                    elif data_type in ['JSON']:
                                        param_type = 'json'
                                    elif data_type in ['BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON']:
                                        param_type = 'binary'
                                    elif data_type in ['BOOLEAN', 'BOOL']:
                                        param_type = 'boolean'
                            
                if param_type:
                    # 设置函数调用节点的实际返回类型
                    col.metadata['return_type'] = param_type
                    col.category = param_type
        else:
            # 处理明确返回类型的函数
            if return_type in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}:
                col.category = 'numeric'
            elif return_type in {'VARCHAR', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT', 'STRING'}:
                col.category = 'string'
            elif return_type in {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}:
                col.category = 'datetime'
            elif return_type in {'JSON'}:
                col.category = 'json'
            elif return_type in {'BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON'}:
                col.category = 'binary'
            elif return_type in {'BOOLEAN', 'BOOL'}:
                col.category = 'boolean'
            else:
                col.category = 'string'

        if not getattr(col, 'category', None):
            col.category = 'string'

    elif type(expr).__name__ == 'ColumnReferenceNode':
        # 使用传入的列引用作为比较列
        col = expr
        col.category = expr.column.category
        col.data_type = expr.column.data_type

    elif type(expr).__name__ == 'SubqueryNode':
        # 对于子查询，假设它返回单一列并设置适当的类型信息
        col = expr
        if hasattr(expr, 'repair_columns'):
            expr.repair_columns(None)
        # 默认假设为字符串类型
        col.category = 'string'
        col.data_type = 'VARCHAR'
        # 尝试从子查询的column_alias_map获取实际类型信息
        if hasattr(expr, 'column_alias_map') and expr.column_alias_map:
            # 获取第一个列的类型信息
            first_col_info = next(iter(expr.column_alias_map.values()))
            if len(first_col_info) >= 3:
                col.data_type = first_col_info[1]  # 数据类型
                col.category = first_col_info[2]   # 类别
    
    elif type(expr).__name__ == 'LiteralNode':
        col = expr
        literal_type = getattr(expr, 'data_type', '')
        if literal_type:
            data_type = str(literal_type).upper()
            if data_type in {'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'}:
                col.category = 'numeric'
            elif data_type in {'VARCHAR', 'STRING', 'CHAR', 'TEXT', 'LONGTEXT', 'MEDIUMTEXT', 'TINYTEXT'}:
                col.category = 'string'
            elif data_type in {'DATE', 'DATETIME', 'TIMESTAMP', 'TIME'}:
                col.category = 'datetime'
            elif data_type in {'JSON'}:
                col.category = 'json'
            elif data_type in {'BINARY', 'VARBINARY', 'BLOB', 'LONGBLOB', 'MEDIUMBLOB', 'TINYBLOB', 'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON'}:
                col.category = 'binary'
            elif data_type in {'BOOLEAN', 'BOOL'}:
                col.category = 'boolean'
            else:
                col.category = 'numeric'
        else:
            col.category = 'numeric'
    else:
        # 对于其他类型的表达式，使用默认类型
        col = expr
        # 默认假设为字符串类型
        col.category = 'string'
        col.data_type = 'VARCHAR'
    safe_category = normalize_category(col.category, col.data_type)
    # 根据列类型选择操作符
    if safe_category in ['string', 'binary', 'json', 'boolean']:
        operator = random.choice(['=', '<>'])
    else:
        operator = random.choice(get_comparison_operators(safe_category))
    
    comp_node = ComparisonNode(operator)
    comp_node.add_child(expr)

    # 添加右侧操作数（确保类型兼容）
    if safe_category == 'numeric':
        value = random.randint(0, 100)
        comp_node.add_child(LiteralNode(value, col.data_type))
    elif safe_category == 'string':
        value = f"sample_{random.randint(1, 100)}"
        # 明确使用'STRING'类型确保字符串被正确添加引号
        comp_node.add_child(LiteralNode(value, 'STRING'))
    elif safe_category == 'datetime':
        # 生成带时间部分的日期时间常量
        year = 2023
        month = random.randint(1, 12)
        day = random.randint(1, 28)  # 简单处理，避免月末问题
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # 构建完整的日期时间字符串
        value = f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"
        
        # 使用明确的DATETIME类型确保引号被添加
        comp_node.add_child(LiteralNode(value, 'DATETIME'))
    elif safe_category == 'binary':
        # 生成随机十六进制值
        hex_value = ''.join(random.choices('0123456789ABCDEF', k=8))
        comp_node.add_child(LiteralNode(f"X'{hex_value}'", 'BINARY'))
    elif safe_category == 'json':
        # 生成随机JSON字面量
        json_templates = [
            '{"key": "value"}',
            '{"id": ' + str(random.randint(1, 100)) + ', "name": "sample_' + str(random.randint(1, 100)) + '"}',
            '{"items": [' + ', '.join([str(random.randint(1, 10)) for _ in range(random.randint(1, 5))]) + ']}',
            '{"user": {"name": "test", "age": ' + str(random.randint(18, 65)) + ', "active": ' + str(random.choice([True, False])).lower() + '}}'
        ]
        json_value = random.choice(json_templates)
        comp_node.add_child(LiteralNode(json_value, 'JSON'))
    elif safe_category == 'boolean':
        comp_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
    else:
        comp_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
        
    return comp_node

def create_complex_expression(tables: List[Table], functions: List[Function],
                           from_node: FromNode, main_table: Table, main_alias: str,
                           join_table: Optional[Table] = None, join_alias: Optional[str] = None,
                           max_depth: int = 3, depth: int = 0, column_tracker: Optional[ColumnUsageTracker] = None, for_select: bool = False) -> ASTNode:
    """创建复杂表达式嵌套，确保生成的逻辑表达式只包含布尔类型的子表达式"""
    
    # 避免表达式嵌套过深
    if depth >= max_depth:
        expr = create_random_expression(tables, functions, from_node, main_table, main_alias, 
                                       join_table, join_alias, use_subquery=True, column_tracker=column_tracker, for_select=for_select)
        # 确保返回的是布尔表达式
        return ensure_boolean_expression(expr, tables, functions, from_node, main_table, main_alias, 
                                       join_table, join_alias)
        
    # 递归地创建嵌套表达式
    if random.random() < 0.4:
        # 创建逻辑表达式组合多个简单表达式
        operator = random.choice(['AND', 'OR'])
        logic_node = LogicalNode(operator)
        
        # 添加2-3个子表达式
        sub_expr_count = random.randint(2, 3)
        
        for i in range(sub_expr_count):
            # 递归创建子表达式
            sub_expr = create_complex_expression(tables, functions, from_node, main_table, main_alias, 
                                                join_table, join_alias, depth + 1, max_depth, column_tracker=column_tracker, for_select=False)
            # 确保子表达式是布尔类型
            logic_node.add_child(sub_expr)
        
        return logic_node
    else:
        # 创建基础表达式
        expr = create_random_expression(tables, functions, from_node, main_table, main_alias, 
                                       join_table, join_alias, use_subquery=True, column_tracker=column_tracker, for_select=for_select)
        # 确保返回的是布尔表达式
        result = ensure_boolean_expression(expr, tables, functions, from_node, main_table, main_alias, 
                                       join_table, join_alias)
        return result

def create_random_expression(tables: List[Table], functions: List[Function], 
                           from_node: FromNode, main_table: Table, main_alias: str, 
                           join_table: Optional[Table] = None, join_alias: Optional[str] = None, 
                           use_subquery: bool = True, column_tracker: ColumnUsageTracker = None, for_select: bool=False) -> ASTNode:
    """创建随机表达式"""
    if random.random() > 0.3 and functions:  # 70%概率使用列，30%概率使用函数
        func = random.choice(functions)
        func_node = FunctionCallNode(func)
        
        # 特殊处理MIN/MAX等聚合函数，确保返回类型与参数类型相同
        if func.name in ['MIN', 'MAX'] and func.return_type == 'any':
            # 先预设为numeric，后面根据参数类型更新
            func_node.metadata['category'] = 'numeric'
        else:
            # 设置函数表达式的类型信息
            func_node.metadata['category'] = map_return_type_to_category(func.return_type)
        
        # 为窗口函数设置必要的窗口子句信息
        if func.func_type == 'window':
            # 随机选择是否添加PARTITION BY子句
            if random.random() > 0.3:
                # 选择一个表来分区
                tables_to_choose = [main_table] + ([join_table] if join_table else [])
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    # 选择一个列用于分区
                    col = table.get_random_column()
                    func_node.metadata['partition_by'] = [f"{alias}.{col.name}"]
            
            # 对于需要ORDER BY的窗口函数，总是添加ORDER BY子句
            if func.name in ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LEAD', 'LAG','PERCENT_RANK','CUME_DIST','FIRST_VALUE','LAST_VALUE']:
                # 选择一个表来排序
                tables_to_choose = [main_table] + ([join_table] if join_table else [])
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    # 选择一个列用于排序
                    col = table.get_random_column()
                    # 随机选择排序方向
                    direction = 'ASC' if random.random() > 0.5 else 'DESC'
                    func_node.metadata['order_by'] = [f"{alias}.{col.name} {direction}"]
                else:
                    # 回退方案，使用常量表达式排序
                    func_node.metadata['order_by'] = ['1=1']
        
        # 为函数添加参数
        param_count = func.min_params
        if func.max_params is not None and func.max_params > func.min_params and not func.name.startswith('ST_'):
            param_count = random.randint(func.min_params, func.max_params)
        
        # 保存第一个参数的类型，用于MIN/MAX等聚合函数
        first_param_category = None
        
        for param_idx in range(param_count):
            expected_type = map_param_type_to_category(func.param_types[param_idx]) if param_idx < len(func.param_types) else 'any'
            expected_type = adjust_expected_type_for_min_max(func.name, param_idx, expected_type)
            expected_type = adjust_expected_type_for_conditionals(func.name, param_idx, expected_type)
            # 统一处理三个函数的特殊逻辑
            # 1. SUBSTRING函数特殊处理：确保参数类型正确
            if func.name == 'SUBSTRING':
                # 第一个参数必须是字符串类型
                if param_idx == 0:
                    # 强制第一个参数为字符串类型
                    tables_to_choose = [main_table] + ([join_table] if join_table else [])
                    if tables_to_choose:
                        # 尝试找到字符串类型的列
                        all_string_cols = []
                        for table in tables_to_choose:
                            string_cols = [col for col in table.columns if col.category == 'string']
                            all_string_cols.extend(string_cols)
                        
                        if all_string_cols:
                            # 直接选择可用列
                            col = random.choice(all_string_cols)
                            table = [t for t in tables_to_choose if col in t.columns][0]
                            alias = main_alias if table == main_table else join_alias
                            # 记录列在select中使用
                            if column_tracker:
                                column_tracker.mark_column_as_select(alias, col.name)
                            col_ref = ColumnReferenceNode(col, alias)
                            func_node.add_child(col_ref)
                        else:
                            # 没有可用字符串列，使用字符串字面量
                            literal = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                            func_node.add_child(literal)
                    else:
                        # 没有可用表，使用字符串字面量
                        literal = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                        func_node.add_child(literal)
                # 第二、三个参数必须是数值类型（位置和长度）
                elif param_idx in [1, 2]:
                    # 生成一个合理的整数值
                    # 对于位置参数，使用1到20之间的随机数
                    # 对于长度参数，使用1到10之间的随机数
                    value = random.randint(1, 20) if param_idx == 1 else random.randint(1, 10)
                    literal = LiteralNode(value, 'INT')
                    func_node.add_child(literal)

            # 2. DATE_FORMAT/TO_CHAR函数特殊处理
            elif (func.name == 'DATE_FORMAT' or func.name == 'TO_CHAR'):
                # 第一个参数：日期时间列
                if param_idx == 0:
                    # 强制第一个参数为日期时间类型
                    tables_to_choose = [main_table] + ([join_table] if join_table else [])
                    if tables_to_choose:
                        # 尝试找到日期时间类型的列
                        all_datetime_cols = []
                        for table in tables_to_choose:
                            datetime_cols = [col for col in table.columns if col.category == 'datetime']
                            all_datetime_cols.extend(datetime_cols)
                        
                        if all_datetime_cols:
                            # 直接选择可用列
                            col = random.choice(all_datetime_cols)
                            table = [t for t in tables_to_choose if col in t.columns][0]
                            alias = main_alias if table == main_table else join_alias
                            # 记录列在select中使用
                            if column_tracker:
                                column_tracker.mark_column_as_select(alias, col.name)
                            col_ref = ColumnReferenceNode(col, alias)
                            func_node.add_child(col_ref)
                        else:
                            # 没有日期时间列，使用日期字面量
                            literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                            func_node.add_child(literal)
                    else:
                        # 没有可用表，使用日期字面量
                        literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        func_node.add_child(literal)
                # 第二个参数：格式字符串字面量
                elif param_idx == 1:
                    # 第二个参数必须是格式字符串
                    if func.name == 'TO_CHAR':
                        # PostgreSQL TO_CHAR格式
                        format_strings = ['YYYY-MM-DD', 'YYYY-MM-DD HH24:MI:SS', 'DD-MON-YYYY', 'HH24:MI:SS']
                    else:
                        # MySQL DATE_FORMAT格式
                        format_strings = ['%Y-%m-%d', '%Y-%m-%d %H:%i:%s', '%d-%b-%Y', '%H:%i:%s']
                    # 使用STRING类型确保引号被正确添加
                    literal = LiteralNode(random.choice(format_strings), 'STRING')
                    func_node.add_child(literal)

            # 3. CONCAT函数特殊处理：确保所有参数都是字符串类型
            elif func.name == 'CONCAT' and expected_type == 'string':
                # 强制参数为字符串类型
                tables_to_choose = [main_table] + ([join_table] if join_table else [])
                if tables_to_choose:
                    # 尝试找到字符串类型的列
                    all_string_cols = []
                    for table in tables_to_choose:
                        string_cols = [col for col in table.columns if col.category == 'string']
                        all_string_cols.extend(string_cols)
                    
                    if all_string_cols:
                        # 直接选择可用列
                        col = random.choice(all_string_cols)
                        table = [t for t in tables_to_choose if col in t.columns][0]
                        alias = main_alias if table == main_table else join_alias
                        # 记录列在select中使用
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        col_ref = ColumnReferenceNode(col, alias)
                        func_node.add_child(col_ref)
                    else:
                        # 没有可用字符串列，使用字符串字面量
                        literal = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                        func_node.add_child(literal)
                else:
                    # 没有可用表，使用字符串字面量
                    literal = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                    func_node.add_child(literal)
            
            # 4. NTILE函数特殊处理：确保参数是正整数，不允许嵌套函数
            elif func.name == 'NTILE':
                # 生成一个1-100之间的随机整数作为参数
                literal = LiteralNode(random.randint(1, 100), 'INT')
                func_node.add_child(literal)
            elif func.name == 'NTH_VALUE' and param_idx == 1:
                # 生成一个1-100之间的随机整数作为参数
                literal = LiteralNode(random.randint(1, 10), 'INT')
                func_node.add_child(literal)
            # 6. CAST函数特殊处理：确保第二个参数是有效的数据类型名称
            elif func.name in ['CAST', 'CONVERT']:
                # 第一个参数可以是任意表达式
                if param_idx == 0:
                    # 使用常规参数处理
                    tables_to_choose = [main_table] + ([join_table] if join_table else [])
                    if tables_to_choose:
                        table = random.choice(tables_to_choose)
                        alias = main_alias if table == main_table else join_alias
                        
                        # 50%概率使用列引用，50%概率使用字面量
                        if True:
                            col = table.get_random_column()
                            # 记录列在select中使用
                            if column_tracker:
                                column_tracker.mark_column_as_select(alias, col.name)
                            col_ref = ColumnReferenceNode(col, alias)
                            func_node.add_child(col_ref)

                    else:
                        # 没有可用表，使用字面量
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
                # 第二个参数必须是有效的数据类型名称
                elif param_idx == 1:
                    # 定义一些常见的数据类型
                    data_types = get_cast_types()
                    
                    data_type = random.choice(data_types)
                    if data_type == 'CHAR':
                        # CHAR类型需要指定长度
                        length = random.randint(1, 255)
                        data_type = f"{data_type}({length})"
                    # 随机选择一个数据类型
                    
                    # 创建一个特殊的LiteralNode，使用'NONE'类型来避免被加上引号
                    literal = LiteralNode(data_type, "STRING")
                    func_node.add_child(literal)

            # DATETIME函数特殊处理：参数必须是1-6的整数
            elif func.name == 'DATETIME':
                # 生成1-6之间的随机整数作为参数
                literal = LiteralNode(random.randint(1, 6), 'INT')
                func_node.add_child(literal)
            
            # LEAD函数特殊处理：第二个参数必须是整数常量
            elif func.name in ['LEAD', 'LAG']:
                if param_idx == 1:  # 第二个参数：偏移量
                    # 生成1-10之间的随机整数作为偏移量
                    literal = LiteralNode(random.randint(1, 10), 'INT')
                    func_node.add_child(literal)
                elif param_idx == 2:  # 第三个参数：默认值
                    # 默认值可以是NULL或与第一个参数类型兼容的值
                    if random.random() < 0.5:
                        literal = LiteralNode('NULL', 'NULL')
                        func_node.add_child(literal)
                    else:
                        # 选择与第一个参数类型兼容的值
                        # 这里简化处理，使用整数常量
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
                else:  # 第一个参数：列引用
                    # 使用常规参数处理，但确保是列引用
                    tables_to_choose = [main_table] + ([join_table] if join_table else [])
                    if tables_to_choose:
                        table = random.choice(tables_to_choose)
                        alias = main_alias if table == main_table else join_alias
                        col = table.get_random_column()
                        # 记录列在select中使用
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        col_ref = ColumnReferenceNode(col, alias)
                        func_node.add_child(col_ref)
                    else:
                        # 没有可用表，使用字面量
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
            
            elif func.name in ['TIMESTAMPDIFF'] and param_idx == 0:
                # 第一个参数：时间单位
                literal = LiteralNode(random.choice(['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND']), 'STRING')
                func_node.add_child(literal)
            elif func.name == 'ST_Transform' and param_idx == 1:
                func_node.add_child(LiteralNode(4326, 'INT'))
            
            elif func.name in ['JSON_VALUE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_SET','JSON_INSERT', 'JSON_REPLACE'] and param_idx == 1:
                # 第一个参数：JSON键
                literal = LiteralNode('$.key', 'STRING')
                func_node.add_child(literal)
            elif func.name.upper() == 'ST_GEOHASH':
                if param_idx == 0:
                    func_node.add_child(LiteralNode(random.uniform(-180, 180), 'DOUBLE'))
                elif param_idx == 1:
                    func_node.add_child(LiteralNode(random.uniform(-90, 90), 'DOUBLE'))
                else:
                    func_node.add_child(LiteralNode(random.randint(1, 12), 'INT'))
            elif expected_type == 'string' and is_geohash_function(func.name):
                func_node.add_child(create_geohash_literal_node())
            elif expected_type in {'string', 'json'} and is_geojson_function(func.name):
                func_node.add_child(create_geojson_literal_node(func.name))
            elif expected_type == 'string' and func.name.startswith('ST_') and is_wkt_function(func.name):
                func_node.add_child(create_wkt_literal_node(func.name))
            else:
                # 常规参数处理
                # 选择表和列
                tables_to_choose = [main_table] + ([join_table] if join_table else [])
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    if expected_type == 'binary' and func.name.startswith('ST_'):
                        if 'FromWKB' in func.name:
                            func_node.add_child(create_wkb_literal_node(func.name))
                            continue
                        required_geom = get_required_geometry_type(func.name)
                        geometry_candidates = []
                        for candidate_table in tables_to_choose:
                            candidate_alias = main_alias if candidate_table == main_table else join_alias
                            if candidate_alias is None:
                                continue
                            for candidate_col in candidate_table.columns:
                                if is_geometry_type(candidate_col.data_type) and _matches_required_geometry(candidate_col, required_geom):
                                    geometry_candidates.append((candidate_alias, candidate_col))
                        if geometry_candidates:
                            geom_alias, geom_col = random.choice(geometry_candidates)
                            if column_tracker:
                                column_tracker.mark_column_as_select(geom_alias, geom_col.name)
                            func_node.add_child(ColumnReferenceNode(geom_col, geom_alias))
                        else:
                            func_node.add_child(create_geometry_literal_node(func.name))
                        continue
                    
                    # 30%概率生成嵌套函数作为参数（仅支持一层嵌套）
                    # 但CAST函数的第二个参数必须是数据类型名称，不能是嵌套函数
                    # 并且聚合函数和特殊处理的函数不能作为内层函数使用
                    # 需要排除的特殊处理函数列表
                    special_functions = ['SUBSTRING', 'DATE_FORMAT', 'TO_CHAR', 'CONCAT', 'NTILE', 'NTH_VALUE', 
                                         'CAST', 'CONVERT', 'DATETIME', 'LEAD', 'LAG', 'TIMESTAMPDIFF', 'DATE', 'YEAR', 'MONTH', 'DAY', 'DATEDIFF', 'TRIM',
                                         'JSON_VALUE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_SET','JSON_INSERT', 'JSON_REPLACE', 'IFNULL','CUME_DIST'
                                         ]
                    if (random.random() < 0.3 and functions and not (func.name == 'CAST' and param_idx == 1)):
                        # 过滤出返回类型与期望参数类型兼容的函数，并且排除聚合函数和特殊处理函数
                        if func.func_type == 'aggregate':
                            special_functions.extend(['FIRST_VALUE', 'LAST_VALUE'])
                            special_functions.extend(['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'CUME_DIST', 
                                               'PERCENT_RANK', 'LAG', 'LEAD', 'NTH_VALUE', 'FIRST_VALUE', 'LAST_VALUE'])
                        elif func.func_type == 'window' or func.name in ['JSON_OBJECTAGG','LOG']:
                        # 将所有窗口函数加入special_functions列表
                            special_functions.extend(['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'CUME_DIST', 
                                               'PERCENT_RANK', 'LAG', 'LEAD', 'NTH_VALUE', 'FIRST_VALUE', 'LAST_VALUE'])
                        # 确保当前窗口函数也被添加
                            if func.name not in special_functions:
                                special_functions.append(func.name)
                        elif func.func_type == 'scalar':
                            special_functions.extend(['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'CUME_DIST', 
                                               'PERCENT_RANK', 'LAG', 'LEAD', 'NTH_VALUE', 'FIRST_VALUE', 'LAST_VALUE'])
                        
                        compatible_funcs = []
                        for f in functions:
                            # 确保函数返回类型与期望参数类型兼容，且不是聚合函数，也不是特殊处理函数
                            if ((f.return_type == expected_type or f.return_type == 'any' or 
                                 expected_type == 'any') and f.func_type != 'aggregate' and f.name not in special_functions):
                                compatible_funcs.append(f)
                        
                        if compatible_funcs:
                            # 选择一个兼容的函数
                            nested_func = random.choice(compatible_funcs)
                            nested_func_node = FunctionCallNode(nested_func)
                            
                            # 设置函数表达式的类型信息
                            nested_func_node.metadata['category'] = map_return_type_to_category(nested_func.return_type)
                            
                            # 为嵌套函数添加参数（注意：嵌套函数的参数不能再是函数，只能是列或字面量）
                            nested_param_count = nested_func.min_params
                            if (nested_func.max_params is not None and nested_func.max_params > nested_func.min_params
                                    and not nested_func.name.startswith('ST_')):
                                nested_param_count = random.randint(nested_func.min_params, nested_func.max_params)
                            
                            for nested_param_idx in range(nested_param_count):
                                # 当嵌套函数返回类型为'any'时，使用外部期望的参数类型来选择内部参数
                                if nested_func.return_type == 'any':
                                    # 使用外部函数期望的参数类型
                                    nested_expected_type = expected_type
                                else:
                                    # 使用嵌套函数自己的参数类型
                                    nested_expected_type = map_param_type_to_category(nested_func.param_types[nested_param_idx])
                                if nested_expected_type == 'binary' and nested_func.name.startswith('ST_'):
                                    if 'FromWKB' in nested_func.name:
                                        nested_func_node.add_child(create_wkb_literal_node(nested_func.name))
                                        continue
                                    required_geom = get_required_geometry_type(nested_func.name)
                                    geometry_candidates = []
                                    for candidate_table in tables_to_choose:
                                        candidate_alias = main_alias if candidate_table == main_table else join_alias
                                        if candidate_alias is None:
                                            continue
                                        for candidate_col in candidate_table.columns:
                                            if is_geometry_type(candidate_col.data_type) and _matches_required_geometry(candidate_col, required_geom):
                                                geometry_candidates.append((candidate_alias, candidate_col))
                                    if geometry_candidates:
                                        geom_alias, geom_col = random.choice(geometry_candidates)
                                        if column_tracker:
                                            column_tracker.mark_column_as_select(geom_alias, geom_col.name)
                                        nested_func_node.add_child(ColumnReferenceNode(geom_col, geom_alias))
                                    else:
                                        nested_func_node.add_child(create_geometry_literal_node(nested_func.name))
                                    continue
                                
                                # 为嵌套函数选择列参数
                                if nested_func.name == 'ST_Transform' and nested_param_idx == 1:
                                    nested_func_node.add_child(LiteralNode(4326, 'INT'))
                                    continue
                                if nested_expected_type == 'string' and is_geohash_function(nested_func.name):
                                    nested_func_node.add_child(create_geohash_literal_node())
                                    continue
                                if nested_expected_type in {'string', 'json'} and is_geojson_function(nested_func.name):
                                    nested_func_node.add_child(create_geojson_literal_node(nested_func.name))
                                    continue
                                if (nested_expected_type == 'string' and nested_func.name.startswith('ST_')
                                        and is_wkt_function(nested_func.name)):
                                    nested_func_node.add_child(create_wkt_literal_node(nested_func.name))
                                    continue
                                if nested_expected_type == 'any' or not tables_to_choose:
                                    nested_col = table.get_random_column()
                                else:
                                    nested_matching_columns = [col for col in table.columns if col.category == nested_expected_type]
                                    if nested_matching_columns:
                                        nested_col = random.choice(nested_matching_columns) 
                                    else:
                                        # 没有匹配的列，生成一个与期望类型匹配的字面量
                                        if nested_expected_type == 'numeric':
                                            # 生成随机数字字面量
                                            nested_func_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
                                            continue
                                        elif nested_expected_type == 'string':
                                            # 生成随机字符串字面量
                                            nested_func_node.add_child(LiteralNode(f'sample_{random.randint(1, 100)}', 'VARCHAR'))
                                            continue
                                        elif nested_expected_type == 'datetime':
                                            # 生成随机日期时间字面量
                                            nested_func_node.add_child(LiteralNode('2023-01-01 12:00:00', 'DATETIME'))
                                            continue
                                        elif nested_expected_type == 'json':
                                            # 生成随机JSON字面量
                                            nested_func_node.add_child(LiteralNode('{"key": "value"}', 'JSON'))
                                            continue
                                        elif nested_expected_type == 'binary':
                                            if nested_func.name.startswith('ST_') and 'FromWKB' in nested_func.name:
                                                nested_func_node.add_child(create_wkb_literal_node(nested_func.name))
                                            else:
                                                func_name = nested_func.name if nested_func.name.startswith('ST_') else None
                                                nested_func_node.add_child(create_geometry_literal_node(func_name))
                                            continue
                                        else:
                                            # 其他类型，默认使用整数字面量
                                            nested_func_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
                                            continue
                                
                                nested_col_ref = ColumnReferenceNode(nested_col, alias)
                                nested_func_node.add_child(nested_col_ref)
                            
                            # 将嵌套函数节点添加为父函数的参数
                            func_node.add_child(nested_func_node)
                            # 继续处理下一个参数
                            continue
                    
                    # 直接选择可用列
                    if expected_type == 'json':
                        json_columns = [col for col in table.columns if col.category == 'json']
                        if json_columns:
                            col = random.choice(json_columns)
                        else:
                            func_node.add_child(LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON'))
                            continue
                    elif expected_type == 'boolean':
                        boolean_columns = [col for col in table.columns if col.category == 'boolean']
                        if boolean_columns:
                            col = random.choice(boolean_columns)
                        else:
                            func_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
                            continue
                    elif expected_type == 'any' or not tables_to_choose:
                        col = table.get_random_column()
                    else:
                        matching_columns = [col for col in table.columns if col.category == expected_type]
                        if matching_columns:
                            col = random.choice(matching_columns)
                        else:
                            if expected_type == 'numeric':
                                func_node.add_child(LiteralNode(random.randint(1, 100), 'INT'))
                                continue
                            elif expected_type == 'string':
                                func_node.add_child(LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING'))
                                continue
                            elif expected_type == 'datetime':
                                func_node.add_child(LiteralNode('2023-01-01 12:00:00', 'DATETIME'))
                                continue
                            elif expected_type == 'json':
                                func_node.add_child(LiteralNode('{"type":"Point","coordinates":[0,0]}', 'JSON'))
                                continue
                            elif expected_type == 'binary':
                                func_node.add_child(create_wkb_literal_node(func.name) if func.name.startswith('ST_') and 'FromWKB' in func.name else create_geometry_literal_node(func.name))
                                continue
                            elif expected_type == 'boolean':
                                func_node.add_child(LiteralNode(random.choice([True, False]), 'BOOLEAN'))
                                continue
                            col = table.get_random_column()
                    # 记录列在select中使用
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                    # 确保参数被成功添加
                    if not func_node.add_child(col_ref):
                        # 如果列引用添加失败，使用字面量
                        if expected_type == 'numeric':
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        elif expected_type == 'string':
                            literal = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                        elif expected_type == 'datetime':
                            literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        elif expected_type == 'json':
                            literal = LiteralNode('{"key": "value"}', 'JSON')
                        elif expected_type == 'binary':
                            lat = round(random.uniform(-90, 90), 6)
                            lng = round(random.uniform(-180, 180), 6)
                            literal = LiteralNode(f"ST_GeomFromText('POINT({lat} {lng})')", 'BINARY')
                        elif expected_type == 'boolean':
                            literal = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        else:
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
                    
                    # 记录第一个参数的类型
                    if param_idx == 0 and func.name in ['MIN', 'MAX'] and func.return_type == 'any':
                        first_param_category = col.category
                else:
                    # 没有可用表，使用字面量
                    if expected_type == 'numeric':
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                    elif expected_type == 'string':
                        literal = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                    elif expected_type == 'datetime':
                        literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                    elif expected_type == 'json':
                        literal = LiteralNode('{"key": "value"}', 'JSON')
                    elif expected_type == 'binary':
                        lat = round(random.uniform(-90, 90), 6)
                        lng = round(random.uniform(-180, 180), 6)
                        literal = LiteralNode(f"ST_GeomFromText('POINT({lat} {lng})')", 'BINARY')
                    elif expected_type == 'boolean':
                        literal = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                    else:
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                    func_node.add_child(literal)
                    
                    # 记录第一个参数的类型（如果是字面量）
                    if param_idx == 0 and func.name in ['MIN', 'MAX'] and func.return_type == 'any':
                        if expected_type == 'numeric':
                            first_param_category = 'numeric'
                        elif expected_type == 'string':
                            first_param_category = 'string'
                        else:
                            first_param_category = 'numeric'
        
        # 对于MIN/MAX函数，根据第一个参数的类型更新返回类型
        if func.name in ['MIN', 'MAX'] and func.return_type == 'any' and first_param_category:
            func_node.metadata['category'] = first_param_category
        
        return func_node
    elif random.random() >0.5:
        # 只传递聚合函数给子查询
        agg_funcs = [f for f in functions if f.func_type == 'aggregate']
        return create_select_subquery(tables, agg_funcs)
    else:
        # 使用简单列引用
        tables_to_choose = [main_table] + ([join_table] if join_table else [])
        if tables_to_choose:
            table = random.choice(tables_to_choose)
            alias = main_alias if table == main_table else join_alias
            # 使用列追踪器选择未在select、having和on中使用的列
            col = get_random_column_with_tracker(table, alias, column_tracker, for_select)
            col_ref = ColumnReferenceNode(col, alias)
            # 设置列引用的类型信息
            col_ref.metadata['category'] = col.category
            return col_ref
        else:
            # 回退方案，使用字面量
            literal = LiteralNode(random.randint(1, 100), 'INT')
            # 设置字面量的类型信息
            literal.metadata['category'] = 'numeric'
            return literal

def create_expression_of_type(expr_type: str, tables: List[Table], functions: List[Function], 
                             from_node: FromNode, main_table: Table, main_alias: str, 
                             join_table: Optional[Table] = None, join_alias: Optional[str] = None, 
                             column_tracker: Optional[ColumnUsageTracker] = None) -> ASTNode:
    """创建特定类型的表达式"""
    # 优先选择匹配类型的列
    tables_to_choose = [main_table] + ([join_table] if join_table else [])
    for table in tables_to_choose:
        matching_columns = [col for col in table.columns if col.category == expr_type]
        if matching_columns:
            alias = main_alias if table == main_table else join_alias
            col = random.choice(matching_columns)
            # 标记列在select中使用
            if column_tracker:
                column_tracker.mark_column_as_select(alias, col.name)
            col_ref = ColumnReferenceNode(col, alias)
            return col_ref
    
    # 如果没有匹配类型的列，创建匹配类型的函数表达式
    if expr_type == 'numeric':
        # 寻找返回数值型的函数
        numeric_funcs = [f for f in functions if f.return_type == 'numeric' or f.return_type == 'any']
        if numeric_funcs:
            func = random.choice(numeric_funcs)
            func_node = FunctionCallNode(func)
            
            # 添加足够数量的参数
            # 特殊处理三个函数
            if func.name == 'SUBSTRING':
                # SUBSTRING函数特殊处理：第一个参数是字符串类型，第二、三个参数是数值类型
                # 第一个参数：字符串类型
                string_columns = []
                for table in tables_to_choose:
                    string_columns.extend([col for col in table.columns if col.category == 'string'])
                
                if string_columns:
                    col = random.choice(string_columns)
                    table = [t for t in tables_to_choose if col in t.columns][0]
                    alias = main_alias if table == main_table else join_alias
                    # 标记列在select中使用
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                else:
                    # 没有字符串类型列，使用字面量字符串
                    col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                func_node.add_child(col_ref)
                
                # 第二、三个参数：数值类型
                for i in range(1, func.min_params):
                    # 位置参数（1-20）和长度参数（1-10）
                    value = random.randint(1, 20) if i == 1 else random.randint(1, 10)
                    literal = LiteralNode(value, 'INT')
                    func_node.add_child(literal)
            elif func.name in ['DATE_FORMAT', 'TO_CHAR']:
                # DATE_FORMAT/TO_CHAR函数特殊处理：第一个参数是日期时间类型，第二个参数是格式字符串
                # 第一个参数：日期时间类型
                datetime_columns = []
                for table in tables_to_choose:
                    datetime_columns.extend([col for col in table.columns if col.category == 'datetime'])
                
                if datetime_columns:
                    col = random.choice(datetime_columns)
                    table = [t for t in tables_to_choose if col in t.columns][0]
                    alias = main_alias if table == main_table else join_alias
                    # 标记列在select中使用
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                else:
                    # 没有日期时间类型列，使用日期字面量
                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                func_node.add_child(col_ref)
                
                # 第二个参数：格式字符串
                if func.min_params >= 2:
                    if func.name == 'TO_CHAR':
                        # PostgreSQL TO_CHAR格式
                        format_strings = ['YYYY-MM-DD', 'YYYY-MM-DD HH24:MI:SS', 'DD-MON-YYYY', 'HH24:MI:SS']
                    else:
                        # MySQL DATE_FORMAT格式
                        format_strings = ['%Y-%m-%d', '%Y-%m-%d %H:%i:%s', '%d-%b-%Y', '%H:%i:%s']
                    literal = LiteralNode(random.choice(format_strings), 'STRING')
                    func_node.add_child(literal)
            elif func.name == 'CONCAT':
                # CONCAT函数特殊处理：确保所有参数都是字符串类型
                for param_idx in range(func.min_params):
                    string_columns = []
                    for table in tables_to_choose:
                        string_columns.extend([col for col in table.columns if col.category == 'string'])
                    if string_columns:
                        col = random.choice(string_columns)
                        table = [t for t in tables_to_choose if col in t.columns][0]
                        alias = main_alias if table == main_table else join_alias
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        col_ref = ColumnReferenceNode(col, alias)
                    else:
                        col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                    func_node.add_child(col_ref)
            # NTILE函数特殊处理：确保参数是正整数
            elif func.name == 'NTILE':
                # 生成一个1-100之间的随机整数作为参数
                literal = LiteralNode(random.randint(1, 100), 'INT')
                func_node.add_child(literal)
            # NTH_VALUE函数特殊处理：第二个参数必须是正整数
            elif func.name == 'NTH_VALUE' and func.min_params >= 2:
                # 第一个参数：列引用
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    col = table.get_random_column()
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                    func_node.add_child(col_ref)
                else:
                    literal = LiteralNode(random.randint(1, 100), 'INT')
                    func_node.add_child(literal)
                # 第二个参数：1-10之间的随机整数
                literal = LiteralNode(random.randint(1, 10), 'INT')
                func_node.add_child(literal)
            # CAST/CONVERT函数特殊处理：确保第二个参数是有效的数据类型名称
            elif func.name in ['CAST', 'CONVERT']:
                # 第一个参数：任意表达式（这里使用列引用）
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    col = table.get_random_column()
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                    func_node.add_child(col_ref)
                else:
                    literal = LiteralNode(random.randint(1, 100), 'INT')
                    func_node.add_child(literal)
                
                # 第二个参数：有效的数据类型名称
                data_types = get_cast_types()
                data_type = random.choice(data_types)
                if data_type == 'CHAR':
                    length = random.randint(1, 255)
                    data_type = f"{data_type}({length})"
                literal = LiteralNode(data_type, "STRING")
                func_node.add_child(literal)
            # DATETIME函数特殊处理：参数必须是1-6的整数
            elif func.name == 'DATETIME':
                literal = LiteralNode(random.randint(1, 6), 'INT')
                func_node.add_child(literal)
            # LEAD/LAG函数特殊处理
            elif func.name in ['LEAD', 'LAG']:
                # 第一个参数：列引用
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    col = table.get_random_column()
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                    func_node.add_child(col_ref)
                else:
                    literal = LiteralNode(random.randint(1, 100), 'INT')
                    func_node.add_child(literal)
                
                # 第二个参数：1-10之间的随机整数作为偏移量
                if func.min_params >= 2:
                    literal = LiteralNode(random.randint(1, 10), 'INT')
                    func_node.add_child(literal)
                
                # 第三个参数：默认值（可选）
                if func.min_params >= 3:
                    if random.random() < 0.5:
                        literal = LiteralNode('NULL', 'NULL')
                    else:
                        literal = LiteralNode(random.randint(1, 100), 'INT')
                    func_node.add_child(literal)
            # DATE/YEAR/MONTH/DAY/DATEDIFF函数特殊处理：参数应为日期时间类型
            elif func.name in ['DATE', 'YEAR', 'MONTH', 'DAY', 'DATEDIFF']:
                datetime_columns = []
                for table in tables_to_choose:
                    datetime_columns.extend([col for col in table.columns if col.category == 'datetime'])
                
                if datetime_columns:
                    col = random.choice(datetime_columns)
                    table = [t for t in tables_to_choose if col in t.columns][0]
                    alias = main_alias if table == main_table else join_alias
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                else:
                    # 没有日期时间列，使用日期字面量
                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                func_node.add_child(col_ref)
            # TIMESTAMPDIFF函数特殊处理：第一个参数是时间单位
            elif func.name == 'TIMESTAMPDIFF' and func.min_params >= 3:
                # 第一个参数：时间单位
                literal = LiteralNode(random.choice(['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND']), 'STRING')
                func_node.add_child(literal)
                
                # 第二、三个参数：日期时间类型
                for i in range(1, 3):
                    datetime_columns = []
                    for table in tables_to_choose:
                        datetime_columns.extend([col for col in table.columns if col.category == 'datetime'])
                    
                    if datetime_columns:
                        col = random.choice(datetime_columns)
                        table = [t for t in tables_to_choose if col in t.columns][0]
                        alias = main_alias if table == main_table else join_alias
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        col_ref = ColumnReferenceNode(col, alias)
                    else:
                        col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                    func_node.add_child(col_ref)
            # JSON相关函数特殊处理：第二个参数是JSON路径
            elif func.name in ['JSON_VALUE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_SET', 'JSON_INSERT', 'JSON_REPLACE'] and func.min_params >= 2:
                # 第一个参数：列引用（假设JSON类型列）
                if tables_to_choose:
                    table = random.choice(tables_to_choose)
                    alias = main_alias if table == main_table else join_alias
                    col = table.get_random_column()
                    if column_tracker:
                        column_tracker.mark_column_as_select(alias, col.name)
                    col_ref = ColumnReferenceNode(col, alias)
                    func_node.add_child(col_ref)
                else:
                    literal = LiteralNode('{"key": "value"}', 'STRING')
                    func_node.add_child(literal)
                
                # 第二个参数：JSON路径
                literal = LiteralNode('$.key', 'STRING')
                func_node.add_child(literal)
            else:
                # 其他函数使用通用逻辑
                for param_idx in range(func.min_params):
                    expected_type = map_param_type_to_category(func.param_types[param_idx]) if param_idx < len(func.param_types) else 'any'
                    expected_type = adjust_expected_type_for_min_max(func.name, param_idx, expected_type)
                    expected_type = adjust_expected_type_for_conditionals(func.name, param_idx, expected_type)
                    if func.name == 'ST_Transform' and param_idx == 1:
                        func_node.add_child(LiteralNode(4326, 'INT'))
                        continue
                    if expected_type == 'string' and is_geohash_function(func.name):
                        func_node.add_child(create_geohash_literal_node())
                        continue
                    if expected_type in {'string', 'json'} and is_geojson_function(func.name):
                        func_node.add_child(create_geojson_literal_node(func.name))
                        continue
                    if expected_type == 'string' and func.name.startswith('ST_') and is_wkt_function(func.name):
                        func_node.add_child(create_wkt_literal_node(func.name))
                        continue
                    if expected_type == 'binary' and func.name.startswith('ST_'):
                        if 'FromWKB' in func.name:
                            func_node.add_child(create_wkb_literal_node(func.name))
                            continue
                        required_geom = get_required_geometry_type(func.name)
                        geometry_candidates = []
                        for candidate_table in tables_to_choose:
                            candidate_alias = main_alias if candidate_table == main_table else join_alias
                            if candidate_alias is None:
                                continue
                            for candidate_col in candidate_table.columns:
                                if is_geometry_type(candidate_col.data_type) and _matches_required_geometry(candidate_col, required_geom):
                                    geometry_candidates.append((candidate_alias, candidate_col))
                        if geometry_candidates:
                            geom_alias, geom_col = random.choice(geometry_candidates)
                            if column_tracker:
                                column_tracker.mark_column_as_select(geom_alias, geom_col.name)
                            func_node.add_child(ColumnReferenceNode(geom_col, geom_alias))
                        else:
                            func_node.add_child(create_geometry_literal_node(func.name))
                        continue
                    if expected_type == 'any':
                        table = random.choice(tables_to_choose)
                        alias = main_alias if table == main_table else join_alias
                        col = get_random_column_with_tracker(table, alias, column_tracker, for_select=True)
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        func_node.add_child(ColumnReferenceNode(col, alias))
                        continue
                    candidates = []
                    for candidate_table in tables_to_choose:
                        candidate_alias = main_alias if candidate_table == main_table else join_alias
                        if candidate_alias is None:
                            continue
                        for candidate_col in candidate_table.columns:
                            if candidate_col.category == expected_type:
                                candidates.append((candidate_alias, candidate_col))
                    if candidates:
                        alias, col = random.choice(candidates)
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        func_node.add_child(ColumnReferenceNode(col, alias))
                    else:
                        if expected_type == 'numeric':
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        elif expected_type == 'string':
                            literal = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                        elif expected_type == 'datetime':
                            literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        elif expected_type == 'json':
                            literal = LiteralNode('{"key": "value"}', 'JSON')
                        elif expected_type == 'binary':
                            literal = create_geometry_literal_node(func.name)
                        elif expected_type == 'boolean':
                            literal = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        else:
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
            
            return func_node
    elif expr_type == 'string':
        # 寻找返回字符串型的函数
        string_funcs = [f for f in functions if f.return_type == 'string']
        if string_funcs:
            func = random.choice(string_funcs)
            func_node = FunctionCallNode(func)
            
            # 添加足够数量的参数
            # 特殊处理三个函数
            if func.name == 'SUBSTRING':
                # SUBSTRING函数特殊处理：第一个参数是字符串类型，第二、三个参数是数值类型
                # 第一个参数：字符串类型
                string_columns = []
                for table in tables_to_choose:
                    string_columns.extend([col for col in table.columns if col.category == 'string'])
                
                if string_columns:
                    col = random.choice(string_columns)
                    table = [t for t in tables_to_choose if col in t.columns][0]
                    alias = main_alias if table == main_table else join_alias
                    col_ref = ColumnReferenceNode(col, alias)
                else:
                    # 没有字符串类型列，使用字面量字符串
                    col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                func_node.add_child(col_ref)
                
                # 第二、三个参数：数值类型
                for i in range(1, func.min_params):
                    # 位置参数（1-20）和长度参数（1-10）
                    value = random.randint(1, 20) if i == 1 else random.randint(1, 10)
                    literal = LiteralNode(value, 'INT')
                    func_node.add_child(literal)
            elif func.name in ['DATE_FORMAT', 'TO_CHAR']:
                # DATE_FORMAT/TO_CHAR函数特殊处理：第一个参数是日期时间类型，第二个参数是格式字符串
                # 第一个参数：日期时间类型
                datetime_columns = []
                for table in tables_to_choose:
                    datetime_columns.extend([col for col in table.columns if col.category == 'datetime'])
                
                if datetime_columns:
                    col = random.choice(datetime_columns)
                    table = [t for t in tables_to_choose if col in t.columns][0]
                    alias = main_alias if table == main_table else join_alias
                    col_ref = ColumnReferenceNode(col, alias)
                else:
                    # 没有日期时间类型列，使用日期字面量
                    col_ref = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                func_node.add_child(col_ref)
                
                # 第二个参数：格式字符串
                if func.min_params >= 2:
                    if func.name == 'TO_CHAR':
                        # PostgreSQL TO_CHAR格式
                        format_strings = ['YYYY-MM-DD', 'YYYY-MM-DD HH24:MI:SS', 'DD-MON-YYYY', 'HH24:MI:SS']
                    else:
                        # MySQL DATE_FORMAT格式
                        format_strings = ['%Y-%m-%d', '%Y-%m-%d %H:%i:%s', '%d-%b-%Y', '%H:%i:%s']
                    literal = LiteralNode(random.choice(format_strings), 'STRING')
                    func_node.add_child(literal)
            elif func.name == 'CONCAT':
                # CONCAT函数特殊处理：确保所有参数都是字符串类型
                for param_idx in range(func.min_params):
                    string_columns = []
                    for table in tables_to_choose:
                        string_columns.extend([col for col in table.columns if col.category == 'string'])
                    if string_columns:
                        col = random.choice(string_columns)
                        table = [t for t in tables_to_choose if col in t.columns][0]
                        alias = main_alias if table == main_table else join_alias
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        col_ref = ColumnReferenceNode(col, alias)
                    else:
                        col_ref = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                    func_node.add_child(col_ref)
            elif func.name.upper() == 'ST_GEOHASH':
                func_node.add_child(LiteralNode(random.uniform(-180, 180), 'DOUBLE'))
                func_node.add_child(LiteralNode(random.uniform(-90, 90), 'DOUBLE'))
                func_node.add_child(LiteralNode(random.randint(1, 12), 'INT'))
            else:
                # 其他函数使用通用逻辑
                for param_idx in range(func.min_params):
                    expected_type = map_param_type_to_category(func.param_types[param_idx]) if param_idx < len(func.param_types) else 'any'
                    expected_type = adjust_expected_type_for_min_max(func.name, param_idx, expected_type)
                    expected_type = adjust_expected_type_for_conditionals(func.name, param_idx, expected_type)
                    if func.name == 'ST_Transform' and param_idx == 1:
                        func_node.add_child(LiteralNode(4326, 'INT'))
                        continue
                    if expected_type == 'string' and is_geohash_function(func.name):
                        func_node.add_child(create_geohash_literal_node())
                        continue
                    if expected_type in {'string', 'json'} and is_geojson_function(func.name):
                        func_node.add_child(create_geojson_literal_node(func.name))
                        continue
                    if expected_type == 'string' and func.name.startswith('ST_') and is_wkt_function(func.name):
                        func_node.add_child(create_wkt_literal_node(func.name))
                        continue
                    if expected_type == 'binary' and func.name.startswith('ST_'):
                        if get_required_geometry_type(func.name):
                            func_node.add_child(create_geometry_literal_node(func.name))
                            continue
                        if 'FromWKB' in func.name:
                            func_node.add_child(create_wkb_literal_node(func.name))
                            continue
                        geometry_candidates = []
                        for candidate_table in tables_to_choose:
                            candidate_alias = main_alias if candidate_table == main_table else join_alias
                            if candidate_alias is None:
                                continue
                            for candidate_col in candidate_table.columns:
                                if is_geometry_type(candidate_col.data_type):
                                    geometry_candidates.append((candidate_alias, candidate_col))
                        if geometry_candidates:
                            geom_alias, geom_col = random.choice(geometry_candidates)
                            if column_tracker:
                                column_tracker.mark_column_as_select(geom_alias, geom_col.name)
                            func_node.add_child(ColumnReferenceNode(geom_col, geom_alias))
                        else:
                            func_node.add_child(create_geometry_literal_node(func.name))
                        continue
                    if expected_type == 'any':
                        table = random.choice(tables_to_choose)
                        alias = main_alias if table == main_table else join_alias
                        col = get_random_column_with_tracker(table, alias, column_tracker, for_select=True)
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        func_node.add_child(ColumnReferenceNode(col, alias))
                        continue
                    candidates = []
                    for candidate_table in tables_to_choose:
                        candidate_alias = main_alias if candidate_table == main_table else join_alias
                        if candidate_alias is None:
                            continue
                        for candidate_col in candidate_table.columns:
                            if candidate_col.category == expected_type:
                                candidates.append((candidate_alias, candidate_col))
                    if candidates:
                        alias, col = random.choice(candidates)
                        if column_tracker:
                            column_tracker.mark_column_as_select(alias, col.name)
                        func_node.add_child(ColumnReferenceNode(col, alias))
                    else:
                        if expected_type == 'numeric':
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        elif expected_type == 'string':
                            literal = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                        elif expected_type == 'datetime':
                            literal = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        elif expected_type == 'json':
                            literal = LiteralNode('{"key": "value"}', 'JSON')
                        elif expected_type == 'binary':
                            literal = create_geometry_literal_node(func.name)
                        elif expected_type == 'boolean':
                            literal = LiteralNode(random.choice([True, False]), 'BOOLEAN')
                        else:
                            literal = LiteralNode(random.randint(1, 100), 'INT')
                        func_node.add_child(literal)
            
            return func_node
    
    # 最终回退方案，使用匹配类型的字面量
    if expr_type == 'numeric':
        return LiteralNode(random.randint(1, 100), 'INT')
    elif expr_type == 'string':
        return LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
    elif expr_type == 'datetime':
        return LiteralNode('2023-01-01 12:00:00', 'DATETIME')
    elif expr_type == 'json':
        return LiteralNode('{"key": "value"}', 'JSON')
    elif expr_type == 'binary':
        lat = round(random.uniform(-90, 90), 6)
        lng = round(random.uniform(-180, 180), 6)
        return LiteralNode(f"ST_GeomFromText('POINT({lat} {lng})')", 'BINARY')
    elif expr_type == 'boolean':
        return LiteralNode(random.choice([True, False]), 'BOOLEAN')
    else:
        return LiteralNode(random.randint(1, 100), 'INT')
