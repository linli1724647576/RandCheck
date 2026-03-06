# FunctionCallNode类定义 - 函数调用节点
from typing import Set, Optional, Tuple, List
import random
from .ast_node import ASTNode
from .column_reference_node import ColumnReferenceNode
from .literal_node import LiteralNode
from .arithmetic_node import ArithmeticNode
from .case_node import CaseNode
from data_structures.node_type import NodeType
from data_structures.function import Function
from data_structures.db_dialect import get_dialect_config

class FunctionCallNode(ASTNode):
    """函数调用节点 - 增强版，添加参数类型验证"""

    def __init__(self, function: Function):
        super().__init__(NodeType.FUNCTION_CALL)
        self.function = function
        self.category = None
        self.metadata = {
            'function_name': function.name,
            'return_type': function.return_type,
            'is_aggregate': function.func_type == 'aggregate',  # 标记是否为聚合函数
            'func_type': function.func_type  # 保存函数类型用于检查
        }
        
    @property
    def data_type(self):
        """提供data_type属性的访问器，避免AttributeError"""
        return self.metadata.get('return_type', 'unknown')

    def collect_table_aliases(self) -> Set[str]:
        """收集函数参数中引用的所有表别名"""
        aliases = set()
        # 递归收集所有参数节点的表别名引用
        for child in self.children:
            aliases.update(child.collect_table_aliases())
        return aliases

    def validate_columns(self, from_node: 'FromNode') -> Tuple[bool, List[str]]:
        """验证函数参数中的列引用是否有效"""
        if not from_node:
            return True, []
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
        """修复函数参数中的无效列引用"""
        if not from_node:
            return
        for i, child in enumerate(self.children):
            if hasattr(child, 'repair_columns'):
                child.repair_columns(from_node)
            elif isinstance(child, ColumnReferenceNode) and not child.is_valid(from_node):
                replacement = child.find_replacement(from_node)
                if replacement:
                    self.children[i] = replacement

    def add_child(self, child: ASTNode) -> bool:
        """添加函数参数并验证类型

        Returns:
            bool: 参数是否添加成功
        """
        # 检查是否达到最大参数数量
        if self.function.max_params is not None and len(self.children) >= self.function.max_params:
            return False

        # 参数类型验证
        param_index = len(self.children)
        if param_index < len(self.function.param_types):
            expected_type = self.function.param_types[param_index]
            if expected_type == 'datetime' and isinstance(child, LiteralNode):
                if str(child.data_type).lower() == 'date':
                    child = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
            
            # SUBSTRING和ROUND函数特殊处理
            if self.function.name == 'SUBSTRING':
                if param_index == 1:  # 第二个参数：起始位置
                    # 确保是正整数（PostgreSQL要求必须是整数类型）
                    if isinstance(child, LiteralNode):
                        if isinstance(child.value, (int, float)):
                            if child.value <= 0:
                                child = LiteralNode(random.randint(1, 10), 'INT')
                            else:
                                # 确保使用整数类型，而不是numeric/float
                                child = LiteralNode(int(child.value), 'INT')
                    elif isinstance(child, ColumnReferenceNode):
                        # 检查列是否为整数类型
                        col_category = child.column.category
                        if col_category != 'int':
                            # 如果列不是整数类型，使用整数字面量
                            child = LiteralNode(random.randint(1, 10), 'INT')
                    elif not self._is_valid_param_type(child, expected_type):
                        child = LiteralNode(random.randint(1, 10), 'INT')
                elif param_index == 2:  # 第三个参数：长度
                    # 确保是非负整数（PostgreSQL要求必须是整数类型）
                    if isinstance(child, LiteralNode):
                        if isinstance(child.value, (int, float)):
                            if child.value < 0:
                                child = LiteralNode(random.randint(1, 20), 'INT')
                            else:
                                # 确保使用整数类型，而不是numeric/float
                                child = LiteralNode(int(child.value), 'INT')
                    elif isinstance(child, ColumnReferenceNode):
                        # 检查列是否为整数类型
                        col_category = child.column.category
                        if col_category != 'int':
                            # 如果列不是整数类型，使用整数字面量
                            child = LiteralNode(random.randint(1, 20), 'INT')
                    elif not self._is_valid_param_type(child, expected_type):
                        child = LiteralNode(random.randint(1, 20), 'INT')
                elif not self._is_valid_param_type(child, expected_type):
                    # 第一个参数或其他参数类型不匹配
                    if expected_type == 'string':
                        child = LiteralNode(f'str_{random.randint(1, 100)}', 'STRING')
                    else:
                        return False
            elif self.function.name == 'DATE_FORMAT':
                # 严格确保DATE_FORMAT函数的第二个参数是有效的格式字符串字面量
                # 这符合MySQL的标准用法要求
                if param_index == 1:  # 第二个参数：格式字符串
                    # 强制使用标准的MySQL日期时间格式字符串
                    # 无论原始参数类型是什么，都替换为预定义的格式字符串字面量
                    format_options = ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%Y-%m-%d %H:%i:%s', '%H:%i:%s']
                    child = LiteralNode(random.choice(format_options), 'STRING')
            elif self.function.name == 'ROUND':
                if param_index == 0:  # 第一个参数：被四舍五入的值
                    # 检查是否为timestamp类型，如果是则转换为数值类型
                    from data_structures.db_dialect import get_dialect_config
                    dialect = get_dialect_config()
                    
                elif param_index == 1:  # 第二个参数：小数位数
                    # PostgreSQL要求ROUND函数的第二个参数必须是整数类型
                    if isinstance(child, LiteralNode):
                        if isinstance(child.value, (int, float)):
                            # 转换为整数并确保类型为INT
                            child = LiteralNode(int(child.value), 'INT')
                    elif isinstance(child, ColumnReferenceNode):
                        # 检查列是否为整数类型
                        col_category = child.column.category
                        if col_category != 'int':
                            # 如果列不是整数类型，使用整数字面量
                            child = LiteralNode(random.randint(0, 5), 'INT')
                    elif not self._is_valid_param_type(child, expected_type):
                        child = LiteralNode(random.randint(0, 5), 'INT')
                
            # DATE_ADD, DATE_SUB, ADDDATE, SUBDATE函数特殊处理
            elif self.function.name in ['DATE_ADD', 'DATE_SUB', 'ADDDATE', 'SUBDATE']:
                if param_index == 0:  # 第一个参数：日期时间值
                    # 确保是日期时间类型
                    if not self._is_valid_param_type(child, 'datetime'):
                        if isinstance(child, ColumnReferenceNode):
                            # 如果不是日期时间类型的列，使用日期字面量
                            child = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        elif isinstance(child, LiteralNode):
                            # 如果字面量不是日期时间类型，转换为日期字面量
                            child = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                        else:
                            # 其他情况，使用日期字面量
                            child = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                elif param_index == 1:  # 第二个参数：时间间隔单位（如DAY、MONTH等）
                    # 确保是有效的时间间隔单位
                    if isinstance(child, LiteralNode):
                        # 检查是否为有效的时间间隔单位
                        valid_units = ['DAY', 'MONTH', 'YEAR', 'HOUR', 'MINUTE', 'SECOND']
                        if not any(unit in str(child.value).upper() for unit in valid_units):
                            # 如果不是有效的时间间隔单位，使用DAY
                            child = LiteralNode('DAY', 'STRING')
                    else:
                        # 非字面量参数，使用DAY作为默认值
                        child = LiteralNode('DAY', 'STRING')
                elif param_index == 2:  # 第三个参数：时间间隔值
                    # 确保是数值类型
                    if not self._is_valid_param_type(child, 'numeric'):
                        # 如果不是数值类型，使用随机整数
                        child = LiteralNode(random.randint(1, 10), 'INT')
                
            # JSON相关函数特殊处理
            elif self.function.name in ['JSON_SET', 'JSON_INSERT', 'JSON_REPLACE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_VALUE','JSON_OBJECT','JSON_ARRAY']:
                if param_index == 0:  # 第一个参数：JSON值
                    # 确保是有效的JSON类型
                    if isinstance(child, FunctionCallNode):
                        # 如果是JSON_OBJECT()或JSON_ARRAY()，确保它们有参数
                        if child.function.name == 'JSON_OBJECT' and len(child.children) == 0:
                            # JSON_OBJECT()至少需要一个键值对
                            child.add_child(LiteralNode('key', 'STRING'))
                            child.add_child(LiteralNode('value', 'STRING'))
                        elif child.function.name == 'JSON_ARRAY' and len(child.children) == 0:
                            # JSON_ARRAY()至少需要一个元素
                            child.add_child(LiteralNode('element', 'STRING'))
                    elif not self._is_valid_param_type(child, 'json'):
                        # 如果不是JSON类型，创建一个简单的JSON对象
                        from data_structures.function import Function
                        json_obj_func = Function('JSON_OBJECT', 2,2,'json', ['string', 'string'], 'scalar')
                        json_obj_node = FunctionCallNode(json_obj_func)
                        json_obj_node.add_child(LiteralNode('key', 'STRING'))
                        json_obj_node.add_child(LiteralNode('value', 'STRING'))
                        child = json_obj_node
                elif param_index >= 1 and self.function.name in ['JSON_SET', 'JSON_INSERT', 'JSON_REPLACE']:
                    if param_index % 2 == 1:  # 奇数参数索引：JSON路径
                        # 确保是字符串类型的路径
                        if not self._is_valid_param_type(child, 'string'):
                            # 如果不是字符串类型，使用默认路径
                            child = LiteralNode('$.key', 'STRING')
                    else:  # 偶数参数索引：值
                        # 可以是任意类型，不需要特殊处理
                        pass
                elif param_index >= 1 and self.function.name in ['JSON_EXTRACT', 'JSON_REMOVE']:
                    # 确保是字符串类型的路径
                    if not self._is_valid_param_type(child, 'string'):
                        # 如果不是字符串类型，使用默认路径
                        child = LiteralNode('$.key', 'STRING')
                
            elif not self._is_valid_param_type(child, expected_type):
                # 其他函数的类型验证 - 确保所有函数都能获得足够的参数
                # 创建匹配类型的字面量作为后备方案
                if expected_type == 'numeric':
                    child = LiteralNode(random.randint(1, 100), 'INT')
                elif expected_type == 'string':
                    child = LiteralNode(f'sample_{random.randint(1, 100)}', 'STRING')
                elif expected_type == 'datetime':
                    child = LiteralNode('2023-01-01 12:00:00', 'DATETIME')
                else:
                    # 默认为数值型
                    child = LiteralNode(random.randint(1, 100), 'INT')

        super().add_child(child)
        return True

    def _is_valid_param_type(self, child: ASTNode, expected_type: str) -> bool:
        """验证参数类型是否匹配

        Args:
            child: 参数节点
            expected_type: 期望的参数类型

        Returns:
            bool: 参数类型是否有效
        """
        if expected_type == 'any':
            # 对于聚合函数，特殊处理几何类型列
            if self.metadata.get('is_aggregate') and isinstance(child, ColumnReferenceNode):
                # 检查列的数据类型是否为几何类型
                if child.column.data_type in ['GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON']:
                    return False  # 聚合函数不支持几何类型
            return True  # 非聚合函数或非几何类型接受任何类型

        if isinstance(child, ColumnReferenceNode):
            # 列引用节点，检查列类型
            col_category = child.column.category
            return col_category == expected_type or (expected_type == 'numeric' and col_category in ['int', 'float', 'decimal'])
        elif isinstance(child, LiteralNode):
            # 字面量节点，检查数据类型
            if hasattr(child, 'data_type'):
                data_type = child.data_type.lower()
                if expected_type == 'numeric':
                    return data_type in ['int', 'float', 'decimal', 'numeric']
                elif expected_type == 'string':
                    return data_type in ['varchar', 'string', 'char']
                elif expected_type == 'datetime':
                    return data_type in ['date', 'datetime', 'timestamp']
                return data_type == expected_type
            # 如果没有data_type属性，返回False
                return data_type in ['int', 'float', 'decimal', 'numeric']
            elif expected_type == 'string':
                return data_type in ['varchar', 'string', 'char']
            elif expected_type == 'datetime':
                return data_type in ['date', 'datetime', 'timestamp']
            return data_type == expected_type
        elif isinstance(child, FunctionCallNode):
            # 函数调用节点，检查返回类型
            return child.metadata.get('return_type') == expected_type
        elif isinstance(child, ArithmeticNode):
            # 算术表达式节点，结果通常为数值型
            return expected_type == 'numeric'
        elif isinstance(child, CaseNode):
            # CASE表达式，检查结果类型
            result_type = child.metadata.get('result_type', 'unknown')
            return result_type == expected_type or expected_type == 'any'

        return False

    def to_sql(self) -> str:
        # 获取当前方言配置
        dialect = get_dialect_config()
        
        # 获取方言特定的函数名称
        function_name = dialect.get_function_name(self.function.name)
        # 函数调用参数SQL
        args = [child.to_sql() for child in self.children]
        # 特殊处理一些函数
        if self.function.name == 'DATE_FORMAT' and dialect.get_function_name('DATE_FORMAT') == 'TO_CHAR':
            # PostgreSQL中，DATE_FORMAT转换为TO_CHAR，参数顺序不同
            if len(self.children) >= 2:
                # TO_CHAR(date, format)
                date_arg = self.children[0].to_sql()
                # 将MySQL格式转换为PostgreSQL格式
                mysql_format = self.children[1].to_sql().strip("'")
                pg_format = mysql_format.replace('%Y', 'YYYY').replace('%m', 'MM').replace('%d', 'DD')
                pg_format = pg_format.replace('%H', 'HH24').replace('%i', 'MI').replace('%s', 'SS')
                return f"TO_CHAR({date_arg}, '{pg_format}')"
        
        # 特殊处理COUNT_DISTINCT函数
        if self.function.name == 'COUNT_DISTINCT':
            # 确保有参数
            if self.children:
                arg_sql = self.children[0].to_sql()
                return f"COUNT(DISTINCT {arg_sql})"
        
        # 特殊处理SUM_DISTINCT函数
        if self.function.name == 'SUM_DISTINCT':
            # 确保有参数
            if self.children:
                arg_sql = self.children[0].to_sql()
                return f"SUM(DISTINCT {arg_sql})"
        
        # DATE_ADD和DATE_SUB函数的特殊处理（使用INTERVAL语法）

        
        
        # 特殊处理GROUP_CONCAT函数，添加ORDER BY子句
        if self.function.name == 'GROUP_CONCAT' and args:
            # 使用第一个参数作为ORDER BY子句的排序依据
            order_by_arg = args[0]
            return f"GROUP_CONCAT({', '.join(args)} ORDER BY {order_by_arg})"
                
        if self.function.name in ['DATE_ADD', 'DATE_SUB', 'ADDDATE', 'SUBDATE'] and len(self.children) >= 3:
            date_expr = self.children[0].to_sql()
            interval_unit = self.children[1].to_sql()
            interval_value = self.children[2].to_sql()
            
            # 移除单位字符串的引号
            if interval_unit.startswith("'") and interval_unit.endswith("'"):
                interval_unit = interval_unit[1:-1]
            
            if self.function.name == 'DATE_ADD':
                return f"{date_expr} + INTERVAL {interval_value} {interval_unit}"
            else:  # DATE_SUB
                return f"{date_expr} - INTERVAL {interval_value} {interval_unit}"
        
        # JSON相关函数的特殊处理
        if self.function.name in ['JSON_SET', 'JSON_INSERT', 'JSON_REPLACE', 'JSON_REMOVE', 'JSON_EXTRACT', 'JSON_VALUE']:
            params_sql = []
            for i, child in enumerate(self.children):
                child_sql = child.to_sql()
                # 对于JSON路径参数，确保它们是字符串字面量
                if (self.function.name in ['JSON_SET', 'JSON_INSERT', 'JSON_REPLACE'] and i % 2 == 1) or \
                   (self.function.name in ['JSON_EXTRACT', 'JSON_REMOVE'] and i >= 1) or \
                   (self.function.name == 'JSON_VALUE' and i == 1):
                    # 如果不是字符串字面量，添加引号
                    if not (child_sql.startswith("'") and child_sql.endswith("'")):
                        child_sql = f"'{child_sql}'"
                params_sql.append(child_sql)
            
            params_str = ', '.join(params_sql)
            return f"{self.function.name}({params_str})"

        # 窗口函数特殊处理
        if self.function.func_type == 'window':
            
            window_parts = []
            partition_by = self.metadata['partition_by']
            if partition_by:
                window_parts.append(f"PARTITION BY {', '.join(partition_by)}")
            
            # 对于需要ORDER BY的窗口函数，如果没有ORDER BY，添加一个默认的ORDER BY子句
            # 包括ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG等函数
            if not order_by and self.function.name in ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LEAD', 'LAG','PERCENT_RANK','CUME_DIST']:
                # 使用常量表达式作为排序依据，避免使用MySQL不支持的位置引用
                order_by = ['1=1']  # 使用逻辑表达式代替位置引用
                window_parts.append(f"ORDER BY {', '.join(order_by)}")
            elif order_by:
                window_parts.append(f"ORDER BY {', '.join(order_by)}")

            window_clause = f"OVER ({' '.join(window_parts)})"
            if args:
                return f"{function_name}({', '.join(args)}) {window_clause}"
            else:
                return f"{function_name}() {window_clause}"  # 无参数窗口函数
        if self.function.name in ['CONVERT']:
            return f"{function_name}({args[0]}, {args[1][1:-1]})"
        if self.function.name in ['CAST']:
            return f"{function_name}({args[0]} AS {args[1][1:-1]})"
        if self.function.name in ['DIV']:
            return f"{args[0]} {function_name} {args[1]}"
        if self.function.name in ['TRIM']:
            return f"{function_name}( BOTH {args[0]} FROM {args[1]})"
        if self.function.name in ['TIMESTAMPDIFF']:
            return f"{function_name}({args[0][1:-1]}, {args[1]}, {args[2]})"
        return f"{function_name}({', '.join(args)})"

    def collect_column_aliases(self) -> Set[str]:
        """收集函数参数中引用的列别名"""
        aliases = set()
        for child in self.children:
            aliases.update(child.collect_column_aliases())
        return aliases
